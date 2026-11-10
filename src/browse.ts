// Read-only views of the graph.
import { g, Predicate, readBatch } from "@helix-db/helix-db"
import { Array as Arr, Console, Effect, Option, Order, Schema } from "effect"
import { DocumentId, LINK_VERSION } from "./domain.ts"
import { Helix, type HelixError } from "./helix.ts"
import { leafCategories } from "./pipeline.ts"
import { chunksOfDocument, chunkStream, documentStream, fields, type FilterError, type Filters, tenantScope } from "./queries.ts"

export class NotFound extends Schema.TaggedError<NotFound>()("NotFound", { message: Schema.String }) {}

// Helix returns a row's properties in key order, and `--json` prints them as they came: rows are declared in that order.
const DocRow = Schema.Struct({
  chunkCount: Schema.Finite,
  documentId: Schema.String,
  kind: Schema.String,
  source: Schema.String,
  status: Schema.optional(Schema.String),
  title: Schema.String,
})
// A `groupCount(property)` row: nodes without the property count under `null`.
const KindCount = Schema.Struct({ count: Schema.Finite, kind: Schema.NullOr(Schema.String) })
const StatusCount = Schema.Struct({ count: Schema.Finite, status: Schema.NullOr(Schema.String) })
const TallyCount = Schema.Struct({
  count: Schema.Finite,
  category: Schema.optional(Schema.NullOr(Schema.String)),
  entityKey: Schema.optional(Schema.NullOr(Schema.String)),
})
const byCountDescending = <A extends { readonly count: number }>(rows: ReadonlyArray<A>) =>
  Arr.sort(rows, Order.flip(Order.mapInput(Order.Number, (row: A) => row.count)))

/** `exo docs`: documents matching the filters. */
export const docs = Effect.fn("docs")(function*(options: { readonly filters: Filters; readonly limit: number; readonly json: boolean }): Effect.fn.Return<void, HelixError | FilterError, Helix> {
  const helix = yield* Helix
  const stream = yield* documentStream(options.filters)
  const data = yield* helix.read(
    "list_documents",
    readBatch().varAs("documents", stream.limit(options.limit).project(fields("documentId", "title", "kind", "source", "chunkCount", "status")))
      .returning(["documents"]),
    Schema.Struct({ documents: Schema.NullOr(Schema.Array(DocRow)) }),
  )
  const rows = data.documents ?? []
  if (options.json) return yield* Console.log(JSON.stringify(rows, null, 2))
  yield* Effect.forEach(rows, (row) =>
    Console.log(
      `${row.documentId.padEnd(24)} ${(row.kind || "-").padEnd(16)} ${String(row.chunkCount).padStart(3)}ch ${row.status === "linked" ? "✓" : "·"} ${row.title.slice(0, 80)}`,
    ), { discard: true })
})

/** `exo show <documentId>`: the document's passages in order. */
export const show = Effect.fn("show")(function*(documentId: string): Effect.fn.Return<void, HelixError, Helix> {
  const helix = yield* Helix
  const data = yield* helix.read(
    "show_document",
    readBatch().varAs("chunks", chunksOfDocument(DocumentId.make(documentId)).project(fields("chunkIndex", "content", "title"))).returning(["chunks"]),
    Schema.Struct({ chunks: Schema.NullOr(Schema.Array(Schema.Struct({ chunkIndex: Schema.Finite, content: Schema.String, title: Schema.String }))) }),
  )
  const chunks = Arr.sortWith(data.chunks ?? [], (chunk) => chunk.chunkIndex, Order.Number)
  if (!Arr.isArrayNonEmpty(chunks)) return yield* Console.log(`no document ${documentId}`)
  yield* Console.log(`# ${chunks[0].title}\n`)
  yield* Effect.forEach(chunks, (chunk) => Console.log(chunk.content + "\n"), { discard: true })
})

/** `exo entities` / `exo categories`: what a slice of the corpus is about, or how it is filed. */
export const tally = Effect.fn("tally")(function*(options: {
  readonly what: "entities" | "categories"
  readonly filters: Filters
  readonly entityKinds: ReadonlyArray<string>
  readonly match: string
  readonly top: number
  readonly json: boolean
}): Effect.fn.Return<void, HelixError | FilterError, Helix> {
  const helix = yield* Helix
  const stream = Option.getOrElse(yield* chunkStream(options.filters), () => g().nWithLabel("Chunk").where(tenantScope))
  const traversal = options.what === "entities" ? stream.outE("MENTIONS").groupCount("entityKey") : stream.groupCount("category")
  const data = yield* helix.read(
    `tally_${options.what}`,
    readBatch().varAs("counts", traversal).returning(["counts"]),
    Schema.Struct({ counts: Schema.NullOr(Schema.Array(TallyCount)) }),
  )
  const rows = (data.counts ?? []).map((row) => {
    if (options.what === "categories") return { kind: "", name: row.category ?? "(unlinked)", count: row.count }
    const [, kind = "", ...name] = (row.entityKey ?? "").split(":")
    return { kind, name: name.join(":"), count: row.count }
  }).filter((row) =>
    (!options.entityKinds.length || options.entityKinds.includes(row.kind)) && (!options.match || row.name.includes(options.match))
  )
  const top = byCountDescending(rows).slice(0, options.top)
  if (options.json) return yield* Console.log(JSON.stringify(top, null, 2))
  yield* Effect.forEach(top, (row) => Console.log(`${String(row.count).padStart(6)}  ${row.kind.padEnd(13)} ${row.name}`), { discard: true })
})

/** `exo stats`: documents by kind, linked counts. */
export const stats: Effect.Effect<void, HelixError, Helix> = Effect.gen(function*() {
  const helix = yield* Helix
  const count = (label: string) => g().nWithLabel(label).where(tenantScope).count()
  const data = yield* helix.read(
    "stats",
    readBatch()
      .varAs("documents", g().nWithLabel("SourceDocument").where(tenantScope).groupCount("kind"))
      .varAs("status", g().nWithLabel("SourceDocument").where(tenantScope).groupCount("status"))
      .varAs("chunks", count("Chunk")).varAs("entities", count("Entity")).varAs("categories", count("Category"))
      .varAs("linked", g().nWithLabel("Chunk").where(tenantScope).where(Predicate.eq("linkVersion", LINK_VERSION)).count())
      .returning(["documents", "status", "chunks", "entities", "categories", "linked"]),
    Schema.Struct({
      documents: Schema.NullOr(Schema.Array(KindCount)),
      status: Schema.NullOr(Schema.Array(StatusCount)),
      chunks: Schema.Finite,
      entities: Schema.Finite,
      categories: Schema.Finite,
      linked: Schema.Finite,
    }),
  )
  const documents = data.documents ?? []
  const total = Arr.reduce(documents, 0, (sum, row) => sum + row.count)
  const linkedDocs = Arr.findFirst(data.status ?? [], (row) => row.status === "linked").pipe(Option.match({ onNone: () => 0, onSome: (row) => row.count }))
  yield* Console.log(
    `${total} documents (${linkedDocs} linked) · ${data.chunks} chunks (${data.linked} linked) · ${data.entities} entities · ${data.categories} categories\n`,
  )
  yield* Effect.forEach(byCountDescending(documents), (row) => Console.log(`${String(row.count).padStart(6)}  ${row.kind || "(no kind)"}`), { discard: true })
})

/** `exo profile`: the owner profile written by `exo model`. */
export const profile: Effect.Effect<void, HelixError | NotFound, Helix> = Effect.gen(function*() {
  const helix = yield* Helix
  const data = yield* helix.read(
    "profile",
    readBatch().varAs("rows", g().nWithLabel("UserProfile").where(tenantScope).project(fields("static", "dynamic"))).returning(["rows"]),
    Schema.Struct({ rows: Schema.NullOr(Schema.Array(Schema.Struct({ static: Schema.Array(Schema.String), dynamic: Schema.Array(Schema.String) }))) }),
  )
  const found = data.rows?.[0]
  if (!found) return yield* new NotFound({ message: "no profile — run `exo model --yes`" })
  const list = (lines: ReadonlyArray<string>) => lines.map((line) => `  - ${line}`).join("\n")
  yield* Console.log(`STATIC\n${list(found.static)}\n\nCURRENT\n${list(found.dynamic)}`)
})

/** `exo taxonomy`: the leaf categories (parents group; passages are filed under leaves). */
export const taxonomy = Effect.gen(function*() {
  const leaves = yield* leafCategories
  yield* Effect.forEach(leaves, (category) => Console.log(`${category.name.padEnd(26)} ${category.description}`), { discard: true })
})
