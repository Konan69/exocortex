// Hybrid graph RAG: entity graph + BM25 (+ vectors for questions), fused app-side with RRF, then walked.
import { g, Predicate, PropertyProjection, readBatch, SourcePredicate, sub, type Traversal } from "@helix-db/helix-db"
import { Array as Arr, Clock, Console, Effect, Option, Order, Result, Schema } from "effect"
import { type AiError, Decision } from "effect/ai"
import { Models } from "./ai.ts"
import { ChunkId, DocumentId, ENTITY_KINDS, EntityKey, entityKeyOf, HUB, NodeId, TENANT } from "./domain.ts"
import { Helix, HelixError } from "./helix.ts"
import { chunkFilters, chunksOfDocument, chunkStream, fields, type FilterError, type Filters, idAs, tenantScope } from "./queries.ts"

export type SearchMode = "graph" | "hybrid" | "vector" | "text"

export interface SearchOptions {
  readonly query: string
  readonly filters: Filters
  /** default: graph for lookups of ≤3 words and bookmark-only searches, hybrid otherwise */
  readonly mode: Option.Option<SearchMode>
  /** candidates per channel; default max(24, page × top × 3) */
  readonly k: Option.Option<number>
  readonly top: number
  readonly page: number
  /** one hit per passage instead of one per document */
  readonly chunks: boolean
  /** print neighbouring passages around each hit */
  readonly expand: number
  readonly full: boolean
  /** Jev re-ranks the candidates: one model call per candidate */
  readonly rerank: boolean
  readonly graph: boolean
  readonly json: boolean
}

export class SearchError extends Schema.TaggedError<SearchError>()("SearchError", { message: Schema.String }) {}

// Helix returns a row's properties in key order, and `--json` prints them as they came: rows are declared in that order.
const Hit = Schema.Struct({
  category: Schema.optional(Schema.String),
  chunkId: ChunkId,
  chunkIndex: Schema.Finite,
  content: Schema.String,
  distance: Schema.optional(Schema.Finite),
  documentId: DocumentId,
  id: NodeId,
  salience: Schema.optional(Schema.Finite),
  title: Schema.String,
})
type Hit = typeof Hit.Type
const Hits = Schema.Struct({ hits: Schema.NullOr(Schema.Array(Hit)) })
interface Ranked {
  readonly hit: Hit
  readonly score: number
  readonly via: ReadonlyArray<string>
  readonly relevance?: number | undefined
}
const Passage = Schema.Struct({ chunkId: ChunkId, chunkIndex: Schema.Finite, documentId: DocumentId, title: Schema.String })
type Passage = typeof Passage.Type
const Vars = <S extends Schema.Top>(row: S) => Schema.Record(Schema.String, Schema.NullOr(Schema.Array(row)))

const hitFields = [idAs(), ...fields("chunkId", "documentId", "title", "chunkIndex", "content", "category", "salience")]
const withDistance = [...hitFields, PropertyProjection.renamed("$distance", "distance")]
const passageFields = fields("chunkId", "documentId", "title", "chunkIndex")

const Useful = Decision.make({
  input: Schema.Struct({ request: Schema.String, passage: Schema.String }),
  decisions: { useful: Decision.probability({ instructions: "The passage answers the request or contains information directly useful for it" }) },
})

// WHY: the graph holds what saved things are about; BM25 catches wording with no entity. Query embeddings and
// Jev reranking were cut from bookmark search for speed at the owner's request.
const graphSearch = Effect.fnUntraced(function*(query: string, filters: Filters, k: number) {
  const helix = yield* Helix
  const keys = ENTITY_KINDS.map((kind) => entityKeyOf(kind, query))
  const KeyRows = Schema.NullOr(Schema.Array(Schema.Struct({ entityKey: EntityKey })))
  const found = yield* helix.read(
    "search_graph_names",
    readBatch()
      .varAs("names", g().textSearchNodes("EntityName", "name", query.slice(0, 200), 16, TENANT).project(fields("entityKey")))
      .varAs("exact", g().nWithLabelWhere("EntityName", Predicate.isIn("entityKey", keys)).where(tenantScope).project(fields("entityKey")))
      .returning(["names", "exact"]),
    Schema.Struct({ names: KeyRows, exact: KeyRows }),
  ).pipe(Effect.mapError((error) =>
    error.message.includes("missing Node text index")
      ? new SearchError({ message: "EntityName BM25 index missing — run `exo entities --index-names --yes` (Helix must build the empty-label index first)" })
      : error
  ))
  const orderedKeys = Arr.dedupe([...(found.exact ?? []), ...(found.names ?? [])].map((row) => row.entityKey)).slice(0, 8)
  if (!orderedKeys.length) return []
  const mapped = yield* helix.read(
    "search_graph_entity_keys",
    readBatch().varAs("entities", g().nWithLabelWhere("Entity", Predicate.isIn("entityKey", orderedKeys)).where(tenantScope).project([idAs(), ...fields("entityKey")]))
      .returning(["entities"]),
    Schema.Struct({ entities: Schema.NullOr(Schema.Array(Schema.Struct({ id: NodeId, entityKey: EntityKey }))) }),
  )
  const byKey = new Map((mapped.entities ?? []).map((row) => [row.entityKey, row.id]))
  const entities = Arr.getSomes(orderedKeys.map((key) => Option.fromNullishOr(byKey.get(key))))
  if (!entities.length) return []
  const { anchor, post } = yield* chunkFilters(filters)
  const filter = Predicate.and([post, ...anchor])
  const reached = yield* helix.read(
    "search_graph_passages",
    entities.reduce((batch, id, index) =>
      batch.varAs(
        `p${index}`,
        g().n(id).in("MENTIONS").union([sub().hasLabel("Chunk"), sub().hasLabel("SourceDocument").out("HAS_CHUNK")])
          .where(filter).dedup().limit(Math.max(k * 8, 80)).project(hitFields),
      ), readBatch()).returning(entities.map((_, index) => `p${index}`)),
    Vars(Hit),
  )
  // Each entity's passages count 1/(rank+1), the entity's place in the name match.
  const tally = new Map<number, { hit: Hit; weight: number; matches: number }>()
  entities.forEach((_, rank) => {
    for (const hit of reached[`p${rank}`] ?? []) {
      const entry = tally.get(hit.id) ?? { hit, weight: 0, matches: 0 }
      tally.set(hit.id, { hit: entry.hit, weight: entry.weight + 1 / (rank + 1), matches: entry.matches + 1 })
    }
  })
  return Arr.sortBy(
    Order.flip(Order.mapInput(Order.Number, (row: { weight: number }) => row.weight)),
    Order.flip(Order.mapInput(Order.Number, (row: { matches: number }) => row.matches)),
    Order.flip(Order.mapInput(Order.Number, (row: { hit: Hit }) => row.hit.salience ?? 0)),
  )([...tally.values()]).slice(0, k).map((row) => row.hit)
})

// Helix EXAMPLES §10, bounded: what each hit is about, and the other chunks reached through the same entities.
// An entity a hundred passages share says nothing about any one of them, so a shared entity counts for 1/log2(1+n)
// where n is how many passages hold it, and hubs over HUB are not walked at all (that was the 60-190 s traversal).
const expandChunks = Effect.fnUntraced(function*(chunkIds: ReadonlyArray<ChunkId>) {
  const helix = yield* Helix
  const chunk = (chunkId: ChunkId) => g().nWithLabelWhere("Chunk", SourcePredicate.eq("chunkId", chunkId)).where(tenantScope)
  const vars = <A>(items: ReadonlyArray<A>, name: (item: A, index: number) => string, traversal: (item: A) => Traversal<"terminal">) =>
    items.reduce((batch, item, index) => batch.varAs(name(item, index), traversal(item)), readBatch()).returning(items.map(name))

  const entitiesOf = yield* helix.read(
    "hit_entities",
    vars(chunkIds, (_, index) => `e${index}`, (chunkId) => chunk(chunkId).out("MENTIONS").project([idAs(), ...fields("name")])),
    Vars(Schema.Struct({ id: NodeId, name: Schema.String })),
  )
  const mentioned = chunkIds.map((_, index) => entitiesOf[`e${index}`] ?? [])
  const entityIds = Arr.dedupe(mentioned.flatMap((rows) => rows.map((row) => row.id)))
  const of = Object.fromEntries(chunkIds.map((chunkId, index) => [chunkId, mentioned[index]!.map((row) => row.name)]))

  // direct passage↔passage edges judged by Jev: another record of the same event, a newer statement, a related matter
  const labelled = chunkIds.flatMap((chunkId, index) => ["EXTENDS", "UPDATES", "RELATES_TO"].map((label) => ({ chunkId, label, index })))
  const linked = yield* helix.read(
    "direct_relations",
    vars(labelled, ({ label, index }) => `${label}${index}`, ({ chunkId, label }) => chunk(chunkId).both(label).project(passageFields)),
    Vars(Passage),
  )
  const related = new Map<string, Passage & { relation: string }>()
  for (const [name, rows] of Object.entries(linked)) {
    for (const row of rows ?? []) {
      if (!chunkIds.includes(row.chunkId) && !related.has(row.chunkId)) related.set(row.chunkId, { ...row, relation: name.replace(/\d+$/, "") })
    }
  }
  const none = { of, connected: [] as ReadonlyArray<Passage & { shared: number; weight: number }>, related: [...related.values()] }
  if (!entityIds.length) return none

  const held = yield* helix.read(
    "entity_sizes",
    vars(entityIds, (_, index) => `s${index}`, (id) => g().n(id).inE("MENTIONS").count()),
    Schema.Record(Schema.String, Schema.Finite),
  )
  const walkable = entityIds.map((id, index) => ({ id, size: held[`s${index}`] ?? 0 })).filter((entity) => entity.size > 1 && entity.size <= HUB)
  if (!walkable.length) return none
  const reached = yield* helix.read(
    "expand_chunks_via_entities",
    vars(walkable, (_, index) => `w${index}`, (entity) => g().n(entity.id).in("MENTIONS").hasLabel("Chunk").project(passageFields)),
    Vars(Passage),
  )
  const tally = new Map<string, Passage & { shared: number; weight: number }>()
  walkable.forEach((entity, index) => {
    for (const row of reached[`w${index}`] ?? []) {
      if (chunkIds.includes(row.chunkId)) continue
      const entry = tally.get(row.chunkId) ?? { ...row, shared: 0, weight: 0 }
      tally.set(row.chunkId, { ...entry, shared: entry.shared + 1, weight: entry.weight + 1 / Math.log2(1 + entity.size) })
    }
  })
  const connected = Arr.sortWith([...tally.values()], (row) => row.weight, Order.flip(Order.Number)).slice(0, 8)
  return { ...none, connected }
})

const neighbours = Effect.fnUntraced(function*(hits: ReadonlyArray<Hit>, expand: number) {
  const helix = yield* Helix
  const near = yield* helix.read(
    "expand_chunks",
    hits.reduce((batch, hit, index) =>
      batch.varAs(
        `a${index}`,
        chunksOfDocument(hit.documentId)
          .where(Predicate.between("chunkIndex", Math.max(0, hit.chunkIndex - expand), hit.chunkIndex + expand))
          .project(fields("chunkIndex", "content")),
      ), readBatch()).returning(hits.map((_, index) => `a${index}`)),
    Vars(Schema.Struct({ chunkIndex: Schema.Finite, content: Schema.String })),
  )
  return hits.map((hit, index) => ({
    ...hit,
    content: Arr.sortWith(near[`a${index}`] ?? [], (chunk) => chunk.chunkIndex, Order.Number).map((chunk) => chunk.content).join("\n"),
  }))
})

export const search = Effect.fn("search")(function*(
  options: SearchOptions,
): Effect.fn.Return<void, SearchError | HelixError | FilterError | AiError.AiError, Helix | Models> {
  const helix = yield* Helix
  const models = yield* Models
  const { query, filters, top, page } = options
  if (!query) return yield* new SearchError({ message: "usage: exo search <query> [filters] [--mode graph|hybrid|vector|text] ..." })
  const started = yield* Clock.currentTimeMillis
  const bookmarksOnly = filters.kinds.length > 0 && filters.kinds.every((kind) => kind === "x_bookmark")
  // A bare name holds no context for an embedding: measured 2026-10-12, a nonsense query sat at cosine distance 0.30,
  // real passages naming the person at 0.30-0.33 and unrelated posts at 0.32, so no distance floor separates
  // them. Lookups of up to three words therefore go by entity graph + BM25 only; the vector channel is for questions.
  const lookup = query.trim().split(/\s+/).length <= 3
  const mode = Option.getOrElse(options.mode, (): SearchMode => bookmarksOnly || lookup ? "graph" : "hybrid")
  // deep enough that the asked-for page is ranked, not padded
  const k = Option.getOrElse(options.k, () => Math.max(24, page * top * 3))

  // Filters build the candidate stream first; vector/BM25 then rank only those candidates (Helix prefiltering).
  const stream = yield* chunkStream(filters)
  const { post } = yield* chunkFilters(filters)
  const vectorChannel = Effect.fnUntraced(function*() {
    const vector = (yield* models.embed([query]))[0]!
    const ranked = Option.match(stream, {
      onSome: (candidates) => candidates.vectorSearch("Chunk", "embedding", [...vector], k, TENANT),
      onNone: () => g().vectorSearchNodes("Chunk", "embedding", [...vector], k, TENANT).where(post),
    })
    return ["~", ranked.project(withDistance)] as const
  })
  const channels = [
    ...(mode === "vector" || mode === "hybrid" ? [yield* vectorChannel()] : []),
    ...(mode !== "vector"
      ? [["=", Option.match(stream, {
        onSome: (candidates) => candidates.textSearch("Chunk", "content", query.slice(0, 200), k, TENANT),
        onNone: () => g().textSearchNodes("Chunk", "content", query.slice(0, 200), k, TENANT).where(post),
      }).project(hitFields)] as const]
      : []),
  ]
  const hitsOf = (channel: string, traversal: Traversal<"terminal">) =>
    helix.read(`search_${channel}`, readBatch().varAs("hits", traversal).returning(["hits"]), Hits).pipe(Effect.map((data) => data.hits ?? []))
  // Independent reads: a slow channel must not cost the other its results.
  const [graphList, keywordLists] = yield* Effect.all([
    mode === "graph" || mode === "hybrid"
      ? Effect.map(graphSearch(query, filters, k), (hits) => [["G", hits] as const])
      : Effect.succeed([]),
    Effect.forEach(channels, ([channel, traversal]) =>
      hitsOf(channel, traversal).pipe(
        Effect.catch((error) => Effect.as(Console.error(`(${channel} channel failed: ${error.message.slice(0, 80)})`), [])),
        Effect.map((hits) => [channel, hits] as const),
      ), { concurrency: "unbounded" }),
  ], { concurrency: "unbounded" })
  const lists: Array<readonly [string, ReadonlyArray<Hit>]> = [...graphList, ...keywordLists]
  // no name and no wording matched: meaning is all that is left
  if (Option.isNone(options.mode) && mode === "graph" && !bookmarksOnly && lists.every(([, list]) => !list.length)) {
    const [channel, traversal] = yield* vectorChannel()
    lists.push([channel, yield* hitsOf(channel, traversal)])
  }

  // Reciprocal-rank fusion, app-side: Helix returns separate ranked lists and prescribes RRF to join them.
  const fused = new Map<number, { hit: Hit; score: number; via: Array<string> }>()
  for (const [channel, list] of lists) {
    list.forEach((hit, rank) => {
      const entry = fused.get(hit.id) ?? { hit, score: 0, via: [] }
      fused.set(hit.id, { hit: entry.hit, score: entry.score + 1 / (60 + rank + 1), via: [...entry.via, channel] })
    })
  }
  const byScore = Order.flip(Order.mapInput(Order.Number, (entry: Ranked) => entry.score))
  const rrf = Arr.sort([...fused.values()], byScore)
  const best = rrf[0]?.score ?? 1
  let ranked: ReadonlyArray<Ranked> = rrf.map((entry) => ({ ...entry, score: entry.score / best }))

  // Jev re-ranking is opt-in (owner, 2026-10-12: "dont rerank its wasteful you can page through results"): one model
  // call per candidate on every search. Read further down the list with --page instead.
  if (options.rerank && ranked.length) {
    const pool = ranked.slice(0, Math.max(page * top * 2, 12))
    const judged = yield* Effect.forEach(pool, (entry) =>
      Effect.gen(function*() {
        const score = entry.score + 0.3 * (entry.hit.salience ?? 0) // Helix: w_rrf 1.0, w_sal 0.3
        const outcomes = yield* Effect.option(models.decide(Useful, { request: query, passage: entry.hit.content }))
        const relevance = Option.getOrUndefined(Option.flatMap(outcomes, (answers) => Result.getSuccess(answers.useful)))?.probability
        return { ...entry, score: relevance === undefined ? score : relevance + 0.2 * score, relevance }
      }), { concurrency: "unbounded" })
    ranked = [...Arr.sort(judged, byScore), ...ranked.slice(pool.length)]
  }
  if (!options.chunks) ranked = Arr.dedupeWith(ranked, (a, b) => a.hit.documentId === b.hit.documentId)
  const more = ranked.length > page * top
  const paged = ranked.slice((page - 1) * top, page * top)
  const contents = options.expand && paged.length ? yield* neighbours(paged.map((entry) => entry.hit), options.expand) : paged.map((entry) => entry.hit)
  const hits = paged.map((entry, index) => ({ ...entry, hit: contents[index]! }))
  // chunk ids, so --chunks or not
  const graph = !options.graph || !hits.length ? null : yield* expandChunks(hits.slice(0, 6).map((entry) => entry.hit.chunkId))

  if (options.json) return yield* Console.log(JSON.stringify({ hits, graph, page, more }, null, 2))
  for (const { hit, score, via, relevance } of hits) {
    yield* Console.log(`\n── ${hit.title}  [${hit.documentId} #${hit.chunkIndex}]`)
    yield* Console.log(
      `   ${relevance !== undefined ? `relevance ${relevance.toFixed(2)} · ` : ""}score ${score.toFixed(3)} ${via.join("")}${hit.category ? ` · #${hit.category}` : ""}${
        hit.salience !== undefined ? ` · salience ${hit.salience.toFixed(2)}` : ""
      }`,
    )
    const about = graph?.of[hit.chunkId] ?? []
    if (about.length) yield* Console.log(`   about: ${about.slice(0, 12).join(" · ")}${about.length > 12 ? ` · +${about.length - 12}` : ""}`)
    yield* Console.log(options.full || options.expand ? hit.content : `   ${hit.content.replace(/\s+/g, " ").slice(0, 500)}`)
  }
  if (graph?.connected.length) {
    yield* Console.log("\n── connected through shared entities")
    for (const row of graph.connected) yield* Console.log(`   (${row.shared}) ${row.title}  [${row.documentId} #${row.chunkIndex}]`)
  }
  if (graph?.related.length) {
    yield* Console.log("\n── related passages (judged: same event / newer statement / same matter)")
    for (const row of graph.related.slice(0, 8)) yield* Console.log(`   ${row.relation.toLowerCase().padEnd(10)} ${row.title}  [${row.documentId} #${row.chunkIndex}]`)
  }
  if (!hits.length) yield* Console.log(page > 1 ? "no more results" : "nothing found")
  yield* Console.log(`\npage ${page}${more ? ` · more: --page ${page + 1}` : " · end of results"}`)
  const { decisions } = yield* models.usage
  const elapsed = ((yield* Clock.currentTimeMillis) - started) / 1000
  yield* Console.log(`${elapsed.toFixed(1)}s${decisions ? ` · ${decisions} jev calls` : ""}`)
})
