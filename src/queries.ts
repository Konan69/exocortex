// Traversals and filters shared by more than one command.
import { g, Predicate, PropertyProjection, readBatch, SourcePredicate, sub, type Traversal } from "@helix-db/helix-db"
import { Effect, Option, Schema } from "effect"
import { type DocumentId, ENTITY_KINDS, entityKeyOf, normalise, TENANT } from "./domain.ts"
import { Helix } from "./helix.ts"

export const tenantScope = Predicate.eq("tenant_id", TENANT)
export const fields = (...names: ReadonlyArray<string>) => names.map((name) => PropertyProjection.new(name))
export const idAs = (alias = "id") => PropertyProjection.renamed("$id", alias)

/** A batch's variables, each a list of rows (null when the traversal found nothing). */
export const Vars = <S extends Schema.Top>(row: S) => Schema.Record(Schema.String, Schema.NullOr(Schema.Array(row)))

/** All rows of one traversal. */
export const rowsOf = <S extends Schema.Top>(query: string, traversal: Traversal<any, "read">, row: S) =>
  Effect.gen(function*() {
    const helix = yield* Helix
    const data = yield* helix.read(query, readBatch().varAs("rows", traversal).returning(["rows"]), Schema.Struct({ rows: Schema.NullOr(Schema.Array(row)) }))
    return data.rows ?? []
  })

export const documentById = (documentId: DocumentId) =>
  g().nWithLabelWhere("SourceDocument", SourcePredicate.eq("documentId", documentId)).where(tenantScope)
export const chunksOfDocument = (documentId: DocumentId) =>
  g().nWithLabelWhere("Chunk", SourcePredicate.eq("documentId", documentId)).where(tenantScope)

/** What a command may be narrowed to. Repeating a flag ORs its values; different flags AND. */
export interface Filters {
  readonly kinds: ReadonlyArray<string>
  readonly sources: ReadonlyArray<string>
  readonly documents: ReadonlyArray<string>
  readonly categories: ReadonlyArray<string>
  readonly statuses: ReadonlyArray<string>
  /** chunks Jev judged to be about the owner */
  readonly mine: boolean
  readonly withBoilerplate: boolean
  readonly minSalience: Option.Option<number>
  /** `field=v` | `field!=v` | `field~substring` | `field^prefix` */
  readonly where: ReadonlyArray<string>
  /** chunks about these entities: ANY of them, or EVERY one with `allEntities` */
  readonly entities: ReadonlyArray<string>
  readonly allEntities: boolean
}

export const noFilters: Filters = {
  kinds: [], sources: [], documents: [], categories: [], statuses: [], mine: false, withBoilerplate: false,
  minSalience: Option.none(), where: [], entities: [], allEntities: false,
}

export class FilterError extends Schema.TaggedError<FilterError>()("FilterError", { message: Schema.String }) {}

const wherePredicate = (expression: string, rename: (field: string) => string = (field) => field) => {
  const match = expression.match(/^(\w+)\s*(!=|=|~|\^)\s*(.*)$/)
  if (!match) return Effect.fail(new FilterError({ message: `bad --where "${expression}" (use field=value, field!=value, field~substring, field^prefix)` }))
  const [, raw = "", operator, value = ""] = match
  const field = rename(raw)
  return Effect.succeed(
    operator === "=" ? Predicate.eq(field, value)
      : operator === "!=" ? Predicate.neq(field, value)
      : operator === "~" ? Predicate.contains(field, value)
      : Predicate.startsWith(field, value),
  )
}
const isIn = (property: string, values: ReadonlyArray<string>) =>
  values.length === 1 ? [Predicate.eq(property, values[0]!)] : values.length ? [Predicate.isIn(property, [...values])] : []

/** Documents matching the filters, by indexed top-level properties. */
export const documentStream = Effect.fnUntraced(function*(filters: Filters) {
  const where = yield* Effect.forEach(filters.where, (expression) => wherePredicate(expression))
  return g().nWithLabelWhere("SourceDocument", Predicate.and([
    tenantScope,
    ...isIn("kind", filters.kinds), ...isIn("source", filters.sources), ...isIn("documentId", filters.documents), ...isIn("status", filters.statuses),
    ...where,
  ]))
})

// Two kinds of chunk filter. `anchor` predicates are equalities on indexed properties: they go into the source step,
// where the planner serves them from the index. `post` predicates (ranges, negations, substrings) run on what is left.
const CHUNK_FIELD: Record<string, string> = { kind: "documentKind", doc: "documentId" }
export const chunkFilters = Effect.fnUntraced(function*(filters: Filters) {
  const anchor = [
    ...isIn("documentKind", filters.kinds), ...isIn("source", filters.sources), ...isIn("documentId", filters.documents),
    ...isIn("category", filters.categories.map(normalise)),
    ...(filters.mine ? [Predicate.eq("aboutOwner", true)] : []),
  ]
  const where = yield* Effect.forEach(filters.where, (expression) => wherePredicate(expression, (field) => CHUNK_FIELD[field] ?? field))
  const post = Predicate.and([
    tenantScope,
    ...(filters.withBoilerplate ? [] : [Predicate.neq("boilerplate", true)]),
    ...Option.match(filters.minSalience, { onNone: () => [], onSome: (min) => [Predicate.gte("salience", min)] }),
    ...where,
  ])
  return { anchor, post }
})

const ChunkIds = Schema.Record(Schema.String, Schema.NullOr(Schema.Array(Schema.Struct({ chunkId: Schema.String }))))

/**
 * The chunk stream to rank within, or none when nothing narrows it (then search ranks the whole index, the fast path,
 * and applies `post` to its results). An entity filter is a graph question: start at the entity and walk in, through
 * both levels it is linked at (the passages about it, and documents it runs through).
 */
export const chunkStream = Effect.fn("chunkStream")(function*(filters: Filters) {
  const helix = yield* Helix
  const { anchor, post } = yield* chunkFilters(filters)
  if (!filters.entities.length) {
    return anchor.length ? Option.some(g().nWithLabelWhere("Chunk", Predicate.and([tenantScope, ...anchor])).where(post)) : Option.none()
  }
  const keysOf = (name: string) => ENTITY_KINDS.map((kind) => entityKeyOf(kind, name))
  const through = (keys: ReadonlyArray<string>) =>
    g().nWithLabelWhere("Entity", Predicate.isIn("entityKey", [...keys])).where(tenantScope)
      .union([sub().in("MENTIONS").hasLabel("Chunk"), sub().in("MENTIONS").hasLabel("SourceDocument").out("HAS_CHUNK")]).dedup()
  const narrowed = Predicate.and([post, ...anchor])
  if (!filters.allEntities || filters.entities.length === 1) return Option.some(through(filters.entities.flatMap(keysOf)).where(narrowed))
  const batch = filters.entities.reduce((batch, name, index) => batch.varAs(`c${index}`, through(keysOf(name)).project(fields("chunkId"))), readBatch())
  const reached = yield* helix.read("chunks_about_all", batch.returning(filters.entities.map((_, index) => `c${index}`)), ChunkIds)
  const sets = filters.entities.map((_, index) => new Set((reached[`c${index}`] ?? []).map((row) => row.chunkId)))
  const shared = [...(sets[0] ?? [])].filter((chunkId) => sets.every((set) => set.has(chunkId)))
  return Option.some(g().nWithLabelWhere("Chunk", Predicate.isIn("chunkId", shared)).where(narrowed))
})
