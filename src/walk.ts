// Walking the graph, for agents: an entity's neighbourhood, the path between two entities, the graph's shape, and
// raw queries in native JSON, the project's SDK, or Cypher.
import * as Sdk from "@helix-db/helix-db"
import { g, Projection, PropertyProjection, readBatch, SourcePredicate, type Traversal, WriteBatch } from "@helix-db/helix-db"
import { Array as Arr, Console, Effect, Option, Order, Schema } from "effect"
import { ENTITY_KINDS, entityKeyOf, HUB, NodeId, TENANT } from "./domain.ts"
import { Helix, type HelixError } from "./helix.ts"
import { fields, idAs, tenantScope, Vars } from "./queries.ts"

export class WalkError extends Schema.TaggedError<WalkError>()("WalkError", { message: Schema.String }) {}

const Entity = Schema.Struct({ id: NodeId, entityKey: Schema.String, kind: Schema.String, name: Schema.String })
type Entity = typeof Entity.Type
const Neighbour = Schema.Struct({ id: NodeId, kind: Schema.String, name: Schema.String, passages: Schema.Finite, share: Schema.Finite })
type Neighbour = typeof Neighbour.Type
const DocHit = Schema.Struct({ documentId: Schema.String, title: Schema.String })

const entityFields = [idAs(), ...fields("entityKey", "kind", "name")]

/** The entity a name means: an exact name in any kind first, then the best BM25 matches on EntityName; within each,
 *  the most mentioned first. The rest are printed as `also`. */
const resolve = Effect.fnUntraced(function*(name: string) {
  const helix = yield* Helix
  const exact = ENTITY_KINDS.map((kind) => entityKeyOf(kind, name))
  const named = yield* helix.read(
    "walk_resolve",
    readBatch().varAs("named", g().textSearchNodes("EntityName", "name", name.slice(0, 200), 5, TENANT).project(fields("entityKey"))).returning(["named"]),
    Schema.Struct({ named: Schema.NullOr(Schema.Array(Schema.Struct({ entityKey: Schema.String }))) }),
  )
  const keys = Arr.dedupe([...exact, ...(named.named ?? []).map((row) => row.entityKey)])
  const rows = yield* helix.read(
    "walk_resolve_entities",
    readBatch().varAs("e", g().nWithLabelWhere("Entity", Sdk.Predicate.isIn("entityKey", keys)).where(tenantScope).project(entityFields)).returning(["e"]),
    Schema.Struct({ e: Schema.NullOr(Schema.Array(Entity)) }),
  )
  const found = rows.e ?? []
  if (!found.length) return yield* new WalkError({ message: `no entity named "${name}"; try \`exo entities --match ${name}\`` })
  const sizes = yield* helix.read(
    "walk_resolve_sizes",
    found.reduce((b, row, index) => b.varAs(`s${index}`, g().n(row.id).inE("MENTIONS").count()), readBatch()).returning(found.map((_, index) => `s${index}`)),
    Schema.Record(Schema.String, Schema.Finite),
  )
  const exactKeys = new Set<string>(exact)
  type Candidate = Entity & { readonly mentions: number }
  const ranked = Arr.sortBy(
    Order.flip(Order.mapInput(Order.Boolean, (row: Candidate) => exactKeys.has(row.entityKey))),
    Order.flip(Order.mapInput(Order.Number, (row: Candidate) => row.mentions)),
  )(found.map((row, index) => ({ ...row, mentions: sizes[`s${index}`] ?? 0 })))
  return { entity: ranked[0]!, also: ranked.slice(1).map((row) => `${row.name} (${row.kind}, ${row.mentions})`) }
})

/** ASSOCIATED_WITH is stored once per pair, so a neighbourhood reads both directions. */
const neighbourhoods = Effect.fnUntraced(function*(ids: ReadonlyArray<number>) {
  const helix = yield* Helix
  if (!ids.length) return new Map<number, ReadonlyArray<Neighbour>>()
  const edge = (end: "to" | "from") => {
    const at = end === "to" ? Projection.toEndpoint : Projection.fromEndpoint
    return [at("$id", "id"), at("kind", "kind"), at("name", "name"), PropertyProjection.new("passages"), PropertyProjection.new("share")]
  }
  const batch = ids.reduce((b, id, index) =>
    b.varAs(`o${index}`, g().n(id).outE("ASSOCIATED_WITH").project(edge("to")))
      .varAs(`i${index}`, g().n(id).inE("ASSOCIATED_WITH").project(edge("from"))), readBatch())
    .returning(ids.flatMap((_, index) => [`o${index}`, `i${index}`]))
  const rows = yield* helix.read("walk_neighbours", batch, Vars(Neighbour))
  const byPassages = Order.flip(Order.mapInput(Order.Number, (row: Neighbour) => row.passages))
  return new Map(ids.map((id, index) => [id, Arr.sort([...(rows[`o${index}`] ?? []), ...(rows[`i${index}`] ?? [])], byPassages)]))
})

/** Documents an entity runs through: passages that mention it, and documents linked to it as a whole. */
const documentsOf = Effect.fnUntraced(function*(ids: ReadonlyArray<number>) {
  const helix = yield* Helix
  const through = (id: number): Traversal<"terminal"> =>
    g().n(id).in("MENTIONS").union([Sdk.sub().hasLabel("Chunk"), Sdk.sub().hasLabel("SourceDocument")]).project(fields("documentId", "title"))
  const rows = yield* helix.read("walk_documents", ids.reduce((b, id, index) => b.varAs(`d${index}`, through(id)), readBatch()).returning(ids.map((_, index) => `d${index}`)), Vars(DocHit))
  return ids.map((_, index) => {
    const tally = new Map<string, { documentId: string; title: string; passages: number }>()
    for (const row of rows[`d${index}`] ?? []) {
      const entry = tally.get(row.documentId) ?? { ...row, passages: 0 }
      tally.set(row.documentId, { ...entry, passages: entry.passages + 1 })
    }
    return Arr.sort([...tally.values()], Order.flip(Order.mapInput(Order.Number, (row: { passages: number }) => row.passages)))
  })
})

/** Breadth-first over ASSOCIATED_WITH from both ends until they meet: the fewest hops between two entities. */
const pathBetween = Effect.fnUntraced(function*(from: Entity, to: Entity, maxHops: number) {
  type Step = { readonly via: number | null; readonly edge: Neighbour | null }
  const sides = [new Map<number, Step>([[from.id, { via: null, edge: null }]]), new Map<number, Step>([[to.id, { via: null, edge: null }]])] as const
  let frontiers: [Array<number>, Array<number>] = [[from.id], [to.id]]
  const names = new Map<number, string>([[from.id, from.name], [to.id, to.name]])
  const chain = (side: Map<number, Step>, id: number) => {
    const steps: Array<{ id: number; edge: Neighbour | null }> = []
    for (let at: number | null = id; at !== null; at = side.get(at)?.via ?? null) steps.push({ id: at, edge: side.get(at)?.edge ?? null })
    return steps
  }
  for (let hop = 0; hop < maxHops; hop++) {
    const side = frontiers[0].length <= frontiers[1].length ? 0 : 1
    const seen = sides[side]
    const next: Array<number> = []
    const around = yield* neighbourhoods(frontiers[side])
    for (const id of frontiers[side]) {
      for (const row of around.get(id) ?? []) {
        names.set(row.id, row.name)
        if (seen.has(row.id)) continue
        seen.set(row.id, { via: id, edge: row })
        next.push(row.id)
        if (sides[1 - side]!.has(row.id)) {
          const left = chain(sides[0], row.id).reverse()
          const right = chain(sides[1], row.id).slice(1)
          return [...left.map((step) => names.get(step.id)!), ...right.map((step) => names.get(step.id)!)]
        }
      }
    }
    frontiers[side] = next
    if (!next.length) break
  }
  return null
})

export interface WalkOptions {
  readonly name: string
  readonly to: Option.Option<string>
  readonly hops: Option.Option<number>
  readonly top: number
  readonly docs: number
  readonly json: boolean
}

/** `exo walk`: who and what an entity is connected to, where it appears, and how far two entities are apart. */
export const walk = Effect.fn("walk")(function*(options: WalkOptions): Effect.fn.Return<void, WalkError | HelixError, Helix> {
  const { entity, also } = yield* resolve(options.name)
  if (Option.isSome(options.to)) {
    const target = (yield* resolve(options.to.value)).entity
    const path = entity.id === target.id ? [entity.name] : yield* pathBetween(entity, target, Math.max(1, Option.getOrElse(options.hops, () => 4)))
    if (options.json) return yield* Console.log(JSON.stringify({ from: entity, to: target, path }, null, 2))
    return yield* Console.log(path ? `${path.join("  →  ")}   (${path.length - 1} hop${path.length === 2 ? "" : "s"})` : `no path between ${entity.name} and ${target.name}`)
  }

  const layers: Array<ReadonlyArray<Neighbour & { readonly via: string }>> = []
  const visited = new Set([entity.id])
  let frontier: ReadonlyArray<{ id: number; name: string }> = [entity]
  for (let hop = 0; hop < Math.max(1, Option.getOrElse(options.hops, () => 1)); hop++) {
    const around = yield* neighbourhoods(frontier.map((row) => row.id))
    const layer = frontier.flatMap((from) => (around.get(from.id) ?? []).map((row) => ({ ...row, via: from.name })))
      .filter((row) => !visited.has(row.id))
    type Reached = Neighbour & { readonly via: string }
    const best = Arr.dedupeWith(Arr.sort(layer, Order.flip(Order.mapInput(Order.Number, (row: Reached) => row.passages * row.share))), (a: Reached, b: Reached) => a.id === b.id)
      .slice(0, options.top)
    best.forEach((row) => visited.add(row.id))
    layers.push(best)
    frontier = best
    if (!best.length) break
  }
  const [documents = []] = yield* documentsOf([entity.id])
  const held = documents.reduce((sum, row) => sum + row.passages, 0)

  if (options.json) return yield* Console.log(JSON.stringify({ entity, also, hops: layers, documents: documents.slice(0, options.docs) }, null, 2))
  yield* Console.log(`${entity.name} (${entity.kind})  ${entity.entityKey}${held > HUB ? `  · hub: ${held} mentions` : ""}`)
  if (also.length) yield* Console.log(`   also: ${also.join(" · ")}`)
  for (const [index, layer] of layers.entries()) {
    yield* Console.log(`\n── ${index === 0 ? "associated with" : `${index + 1} hops out`}  (passages shared · share)`)
    for (const row of layer) yield* Console.log(`   ${String(row.passages).padStart(4)} · ${row.share.toFixed(2)}  ${row.name} (${row.kind})${index ? `  ← ${row.via}` : ""}`)
  }
  yield* Console.log(`\n── appears in ${documents.length} documents (passages mentioning it)`)
  for (const row of documents.slice(0, options.docs)) yield* Console.log(`   ${String(row.passages).padStart(4)}  ${row.title}  [${row.documentId}]`)
})

// The graph's vocabulary, for an agent writing its own query. The labels and edges are domain.ts's.
const SHAPE = `labels
  SourceDocument  documentId kind source title status chunkCount                     tenant_id on every node and edge
  Chunk           chunkId documentId documentKind chunkIndex content category salience aboutOwner boilerplate embedding(1536)
  Entity          entityKey kind name                                                 entityKey = <tenant>:<kind>:<normalised name>
  EntityName      entityKey name                                                      BM25 shadow of Entity.name
  Category        categoryKey name
edges
  SourceDocument -HAS_CHUNK-> Chunk
  Chunk -MENTIONS{confidence, by}-> Entity       SourceDocument -MENTIONS{by:"consolidated"}-> Entity
  Entity -ASSOCIATED_WITH{passages, share}-> Entity   (stored once per pair: walk both directions)
  Chunk -IN_CATEGORY-> Category <-PARENT_OF- Category
  Chunk -RELATES_TO | EXTENDS | UPDATES-> Chunk
indexes   equality: SourceDocument.documentId/kind/source/status, Chunk.documentId/chunkId/documentKind/source/category/aboutOwner,
          Entity.entityKey, EntityName.entityKey(unique), Category.categoryKey(unique)   range: Chunk.salience
          vector: Chunk.embedding (cosine, tenant_id)   BM25: Chunk.content, SourceDocument.title, EntityName.name (tenant_id)
query     exo query --cypher 'MATCH (e:Entity {entityKey: $tenant + ":person:ada-lovelace"})-[r:ASSOCIATED_WITH]-(n) RETURN n.name, r.passages'
          exo query --ts 'readBatch().varAs("n", g().nWithLabelWhere("Entity", SourcePredicate.eq("entityKey", TENANT + ":topic:rag")).in("MENTIONS").count()).returning(["n"])'
          Cypher has no variable-length paths, shortestPath, vector or BM25; use exo walk / exo search for those.`

/** `exo schema`: the shape above and how many of each label and edge the graph holds. */
export const schema = Effect.gen(function*() {
  const helix = yield* Helix
  const labels = ["SourceDocument", "Chunk", "Entity", "EntityName", "Category"]
  const edges = ["HAS_CHUNK", "MENTIONS", "ASSOCIATED_WITH", "IN_CATEGORY", "PARENT_OF", "RELATES_TO", "EXTENDS", "UPDATES"]
  const counts = yield* helix.read(
    "schema_counts",
    edges.reduce((b, label) => b.varAs(label, g().eWithLabel(label).count()), labels.reduce((b, label) => b.varAs(label, g().nWithLabel(label).count()), readBatch()))
      .returning([...labels, ...edges]),
    Schema.Record(Schema.String, Schema.Finite),
  )
  yield* Console.log(`tenant ${TENANT}\n\n${SHAPE}\n\ncounts`)
  for (const name of [...labels, ...edges]) yield* Console.log(`  ${name.padEnd(16)} ${counts[name] ?? 0}`)
})

export interface QueryOptions {
  readonly cypher: Option.Option<string>
  readonly ts: Option.Option<string>
  readonly file: Option.Option<string>
  readonly params: ReadonlyArray<string>
  readonly explain: boolean
  readonly write: boolean
}

/** `exo query`: any read against the live graph. Writes need --write. `$tenant` / `TENANT` are bound for you. */
export const query = Effect.fn("query")(function*(options: QueryOptions): Effect.fn.Return<void, WalkError | HelixError, Helix> {
  const helix = yield* Helix
  const given = [options.cypher, options.ts, options.file].filter(Option.isSome)
  if (given.length !== 1) return yield* new WalkError({ message: "exo query: pass exactly one of --cypher, --ts, --file" })
  if (Option.isSome(options.cypher)) {
    const statement = options.cypher.value
    const parameters: Record<string, string> = { tenant: TENANT }
    for (const pair of options.params) {
      const at = pair.indexOf("=")
      if (at < 1) return yield* new WalkError({ message: `bad --param "${pair}" (use name=value)` })
      parameters[pair.slice(0, at)] = pair.slice(at + 1)
    }
    const body = JSON.stringify({ query: statement, parameters })
    const plan = yield* helix.raw("/v2/cypher/explain", body)
    if (options.explain) return yield* Console.log(plan)
    // Helix's planner says whether the statement writes; a keyword match would refuse `RETURN "CREATE"`.
    const effect = yield* Effect.try({ try: () => (JSON.parse(plan) as { effect?: unknown }).effect, catch: (cause) => new WalkError({ message: `explain: ${String(cause)}` }) })
    if (effect !== "Read" && !options.write) return yield* new WalkError({ message: "that statement writes; add --write" })
    return yield* Console.log(yield* helix.raw("/v2/cypher", body, { write: effect !== "Read" }))
  }
  if (Option.isSome(options.file)) {
    const path = options.file.value
    const body = yield* Effect.tryPromise({ try: () => Bun.file(path).text(), catch: (cause) => new WalkError({ message: String(cause) }) })
    const requestType = yield* Effect.try({ try: () => (JSON.parse(body) as { request_type?: unknown }).request_type, catch: (cause) => new WalkError({ message: `${path}: ${String(cause)}` }) })
    if (requestType !== "read" && !options.write) return yield* new WalkError({ message: "that request writes; add --write" })
    return yield* Console.log(yield* helix.raw("/v2/query", body, { write: requestType !== "read" }))
  }
  const expression = Option.getOrThrow(options.ts)
  const built = yield* Effect.try({
    try: () => new Function(...Object.keys(Sdk), "TENANT", `return (${expression})`)(...Object.values(Sdk), TENANT) as unknown,
    catch: (cause) => new WalkError({ message: `--ts: ${String(cause)}` }),
  })
  if (!(built instanceof Sdk.ReadBatch || built instanceof WriteBatch)) return yield* new WalkError({ message: "--ts must evaluate to readBatch()… or writeBatch()…" })
  if (built instanceof WriteBatch && !options.write) return yield* new WalkError({ message: "that batch writes; add --write" })
  return yield* Console.log(yield* helix.raw("/v2/query", built.toQueryJson({ queryName: "exo_query" }), { write: built instanceof WriteBatch }))
})
