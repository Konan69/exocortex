// X bookmarks from Field Theory: a cheap index of every bookmark, and a graph pass for the ones with an article.
import { Database } from "bun:sqlite"
import { createHash } from "node:crypto"
import { homedir } from "node:os"
import { join } from "node:path"
import { Expr, g, NodeRef, Predicate, PropertyValue, readBatch, writeBatch } from "@helix-db/helix-db"
import { Array as Arr, Clock, Console, Effect, FileSystem, Number as Num, Option, Schema } from "effect"
import type { AiError } from "effect/ai"
import { Models } from "./ai.ts"
import { Settings } from "./config.ts"
import { ChunkId, chunkIdOf, Doc, DocumentId, LINK_VERSION, OWNER, passageHash, TENANT } from "./domain.ts"
import { Helix, type HelixError } from "./helix.ts"
import { leafCategories, LinkWriter, linkChunks, nameChunks, type NoTaxonomy, PACK, settleDocument } from "./pipeline.ts"
import { chunksOfDocument, documentById, fields, tenantScope } from "./queries.ts"

export class BookmarkSourceError extends Schema.TaggedError<BookmarkSourceError>()("BookmarkSourceError", {
  path: Schema.String,
  message: Schema.String,
}) {}

// Field Theory's record shapes (fieldtheory dist/graphql-bookmarks.js): handles and names can be missing, postedAt null.
const Links = Schema.Array(Schema.String)
const Quoted = Schema.Struct({
  text: Schema.String,
  authorHandle: Schema.optional(Schema.String),
  authorName: Schema.optional(Schema.String),
  links: Schema.optional(Links),
  url: Schema.String,
})
type Quoted = typeof Quoted.Type
const Bookmark = Schema.Struct({
  id: Schema.String,
  url: Schema.String,
  text: Schema.String,
  authorHandle: Schema.optional(Schema.String),
  authorName: Schema.optional(Schema.String),
  postedAt: Schema.NullOr(Schema.String),
  links: Schema.optional(Links),
  quotedTweet: Schema.optional(Quoted),
})
type Bookmark = typeof Bookmark.Type
/** classify.mjs writes null when Jev left the question unanswered. */
const Label = Schema.Struct({ id: Schema.String, category: Schema.NullOr(Schema.String), signal: Schema.NullOr(Schema.Number) })
const FtRow = Schema.Struct({
  id: Schema.String,
  links_json: Schema.NullOr(Schema.fromJsonString(Links)),
  quoted_tweet_json: Schema.NullOr(Schema.fromJsonString(Quoted)),
  article_title: Schema.NullOr(Schema.String),
  article_text: Schema.NullOr(Schema.String),
  folder_names: Schema.NullOr(Schema.fromJsonString(Links)),
})
type FtRow = typeof FtRow.Type

const ChunkState = Schema.Struct({ chunkId: ChunkId, documentId: DocumentId, linkVersion: Schema.optional(Schema.Number) })
const Existing = Schema.Struct({
  found: Schema.NullOr(Schema.Array(Schema.Struct({ documentId: DocumentId }))),
  chunks: Schema.NullOr(Schema.Array(Schema.Struct({
    documentId: DocumentId,
    contentHash: Schema.String,
    category: Schema.String,
    salience: Schema.Number,
    folders: Schema.String,
    postedAt: Schema.String,
    title: Schema.String,
    linkVersion: Schema.optional(Schema.Number),
  }))),
})

interface BookmarkRow {
  readonly documentId: DocumentId
  readonly title: string
  readonly url: string
  readonly content: string
  readonly category: string
  readonly salience: number
  readonly folders: string
  readonly postedAt: string
  /** ft's article_text is over 200 characters: the bookmark goes into the graph */
  readonly article: boolean
}


const expandHome = (path: string) => path === "~" ? homedir() : path.startsWith("~/") ? join(homedir(), path.slice(2)) : path
const pathOf = (option: Option.Option<string>, fallback: string) => expandHome(Option.getOrElse(option, () => fallback))
const isTco = (url: string) => {
  const parsed = URL.parse(url)
  return parsed ? parsed.hostname.toLowerCase() === "t.co" : /^https?:\/\/t\.co(?:\/|$)/i.test(url)
}

const decodeAt = <S extends Schema.Top>(path: string, schema: S) => (value: unknown) =>
  Schema.decodeUnknownEffect(schema)(value).pipe(Effect.mapError((error) => new BookmarkSourceError({ path, message: error.message })))

/** A missing file reads as none; only the bookmark JSONL itself is required. */
const readText = Effect.fnUntraced(function*(path: string) {
  const fs = yield* FileSystem.FileSystem
  const fail = (error: { readonly message: string }) => new BookmarkSourceError({ path, message: error.message })
  if (!(yield* fs.exists(path).pipe(Effect.mapError(fail)))) return Option.none<string>()
  return Option.some(yield* fs.readFileString(path).pipe(Effect.mapError(fail)))
})
const jsonLines = <S extends Schema.Top>(path: string, schema: S) =>
  readText(path).pipe(Effect.flatMap(Option.match({
    onNone: () => Effect.succeed<ReadonlyArray<S["Type"]>>([]),
    onSome: (text) => decodeAt(path, Schema.Array(Schema.fromJsonString(schema)))(text.split("\n").filter((line) => line.trim())),
  })))

const ftRows = Effect.fnUntraced(function*(path: string) {
  const fs = yield* FileSystem.FileSystem
  if (!(yield* fs.exists(path).pipe(Effect.mapError((error) => new BookmarkSourceError({ path, message: error.message }))))) return []
  const rows = yield* Effect.acquireUseRelease(
    Effect.try({ try: () => new Database(path, { readonly: true }), catch: (cause) => new BookmarkSourceError({ path, message: String(cause) }) }),
    (db) => Effect.try({
      try: () => db.query("SELECT id, links_json, quoted_tweet_json, article_title, article_text, folder_names FROM bookmarks").all(),
      catch: (cause) => new BookmarkSourceError({ path, message: String(cause) }),
    }),
    (db) => Effect.sync(() => db.close()),
  )
  return yield* decodeAt(path, Schema.Array(FtRow))(rows)
})

const byline = (handle: string | undefined, name: string | undefined) =>
  handle ? `${name || handle} (@${handle})` : name

function bookmarkContent(bookmark: Bookmark, extra: FtRow | undefined): string {
  const quoted: Quoted | undefined = bookmark.quotedTweet ?? extra?.quoted_tweet_json ?? undefined
  const quoteAuthor = byline(quoted?.authorHandle, quoted?.authorName)
  const links = Arr.dedupe([
    ...bookmark.links ?? [], ...quoted?.links ?? [], ...extra?.links_json ?? [], ...quoted?.url ? [quoted.url] : [],
  ]).filter((url) => !isTco(url))
  return [
    `Author: ${byline(bookmark.authorHandle, bookmark.authorName) || "unknown"}`,
    bookmark.text.trim(),
    quoted?.text.trim() ? `Quoted post${quoteAuthor ? ` by ${quoteAuthor}` : ""}:\n${quoted.text.trim()}` : "",
    links.length ? `Links:\n${links.join("\n")}` : "",
    extra?.article_title?.trim() ? `Article: ${extra.article_title.trim()}` : "",
    extra?.article_text?.trim() ? `Article text:\n${extra.article_text.trim().slice(0, 3000)}` : "",
  ].filter(Boolean).join("\n\n")
}

/** Every non-tombstoned bookmark in the JSONL, as the row exo stores for it. */
const bookmarkRows = Effect.fnUntraced(function*(options: {
  readonly jsonl: Option.Option<string>
  readonly labels: Option.Option<string>
  readonly skip: Option.Option<string>
  readonly ftdb: Option.Option<string>
}) {
  const sourcePath = pathOf(options.jsonl, "~/.fieldtheory/bookmarks/bookmarks.jsonl")
  const labelsPath = pathOf(options.labels, "~/.local/share/bookmark-jev/jev-labels.jsonl")
  const skipPath = pathOf(options.skip, "~/.local/share/bookmark-jev/tombstoned.json")
  const source = yield* readText(sourcePath)
  if (Option.isNone(source)) return yield* new BookmarkSourceError({ path: sourcePath, message: "no such file" })
  const bookmarks = yield* decodeAt(sourcePath, Schema.Array(Schema.fromJsonString(Bookmark)))(source.value.split("\n").filter((line) => line.trim()))
  const labels = new Map((yield* jsonLines(labelsPath, Label)).map((label) => [label.id, label]))
  // Bookmarks deleted upstream, as a JSON array of ids; they never enter the index.
  const tombstoned = new Set(yield* readText(skipPath).pipe(Effect.flatMap(Option.match({
    onNone: () => Effect.succeed<ReadonlyArray<string>>([]),
    onSome: decodeAt(skipPath, Schema.fromJsonString(Schema.Array(Schema.String))),
  }))))
  const extras = new Map((yield* ftRows(pathOf(options.ftdb, "~/.fieldtheory/bookmarks/bookmarks.db"))).map((row) => [row.id, row]))
  const kept = bookmarks.filter((bookmark) => !tombstoned.has(bookmark.id))
  // Lone surrogates (emoji cut mid-pair) made Helix reject the JSON body of the first real run (2026-09-26, after 500
  // of 998 bookmarks), so every stored string is made well-formed.
  const rows = kept.map((bookmark): BookmarkRow => {
    const label = labels.get(bookmark.id), extra = extras.get(bookmark.id)
    return {
      documentId: DocumentId.make(`x:${bookmark.id}`),
      url: bookmark.url.toWellFormed(),
      title: `@${bookmark.authorHandle || "unknown"}: ${bookmark.text.trim().replace(/\s+/g, " ").slice(0, 80)}`.toWellFormed(),
      content: bookmarkContent(bookmark, extra).toWellFormed(),
      category: (label?.category ?? "unclassified").toWellFormed(),
      salience: Num.clamp((label?.signal ?? 0) / 3, { minimum: 0, maximum: 1 }),
      folders: (extra?.folder_names ?? []).join(", ").toWellFormed(),
      postedAt: (bookmark.postedAt ?? "").toWellFormed(),
      article: (extra?.article_text?.trim().length ?? 0) > 200,
    }
  })
  return { rows, skipped: bookmarks.length - kept.length }
})

// Intentionally skips Jev linking, LLM naming and settling: bookmarks need a cheap searchable path.
/**
 * `exo bookmarks [jsonl]`: one SourceDocument `x:<id>` + one Chunk `x:<id>:0` per non-tombstoned bookmark, kind
 * `x_bookmark`, source `x`; Jev label category, signal/3 salience, folders, postedAt on the chunk. With `replace`,
 * unchanged chunks (and their links) stay; a graph-linked article whose content changed is reported for `--graph --redo`.
 */
export const importBookmarks = Effect.fn("importBookmarks")(function*(options: {
  readonly jsonl: Option.Option<string>
  readonly labels: Option.Option<string>
  readonly skip: Option.Option<string>
  readonly ftdb: Option.Option<string>
  readonly replace: boolean
  readonly dryRun: boolean
}): Effect.fn.Return<
  void,
  BookmarkSourceError | HelixError | AiError.AiError,
  Helix | Models | Settings | FileSystem.FileSystem
> {
  const started = yield* Clock.currentTimeMillis
  const helix = yield* Helix
  const { rows, skipped } = yield* bookmarkRows(options)

  const pages = yield* Effect.forEach(Arr.chunksOf(rows, 400), (page) =>
    Effect.gen(function*() {
      const ids = page.map((row) => row.documentId)
      const found = yield* helix.read("bookmark_exists", readBatch()
        .varAs("found", g().nWithLabelWhere("SourceDocument", Predicate.isIn("documentId", ids)).where(tenantScope).project(fields("documentId")))
        .varAs("chunks", g().nWithLabelWhere("Chunk", Predicate.isIn("documentId", ids)).where(tenantScope)
          .project(fields("documentId", "contentHash", "category", "salience", "folders", "postedAt", "title", "linkVersion")))
        .returning(["found", "chunks"]), Existing)
      const current = new Map((found.chunks ?? []).map((chunk) => [chunk.documentId, chunk]))
      const compared = Arr.getSomes(page.map((row) => {
        const old = current.get(row.documentId)
        if (!old) return Option.none()
        const sameContent = old.contentHash === passageHash(row.content), linked = (old.linkVersion ?? 0) === LINK_VERSION
        // A graph-linked article's Jev category/salience replace the import labels; preserving its node keeps its edges.
        const unchanged = sameContent && (linked || (old.category === row.category && old.salience === row.salience
          && old.folders === row.folders && old.postedAt === row.postedAt && old.title === row.title))
        return Option.some({ documentId: row.documentId, unchanged, regraph: linked && !sameContent })
      }))
      return {
        existing: (found.found ?? []).map((document) => document.documentId),
        unchanged: compared.filter((row) => row.unchanged).map((row) => row.documentId),
        regraph: compared.filter((row) => row.regraph).length,
      }
    }))
  const existing = new Set(pages.flatMap((page) => page.existing))
  const unchanged = new Set(pages.flatMap((page) => page.unchanged))
  const regraph = Arr.reduce(pages, 0, (total, page) => total + page.regraph)
  const pending = rows.filter((row) => options.replace ? !unchanged.has(row.documentId) : !existing.has(row.documentId))
  const summary = Effect.map(Clock.currentTimeMillis, (now) => JSON.stringify({
    indexed: pending.length,
    skipped,
    existing: existing.size,
    unchanged: options.replace ? unchanged.size : undefined,
    regraph: options.replace ? regraph : undefined,
    ms: now - started,
  }))

  if (options.replace && regraph) {
    yield* Console.error(`${regraph} changed graph-linked articles will be rewritten; run exo bookmarks --graph --redo afterward`)
  }
  if (options.dryRun) {
    yield* Console.log(yield* summary)
    yield* Effect.forEach((pending.length ? pending : rows).slice(0, 2), (row, index) =>
      Console.log(`\n--- sample ${index + 1}: ${row.documentId} ---\n${row.content}`))
    return
  }
  if (pending.length) {
    const models = yield* Models
    const settings = yield* Settings
    yield* helix.ensureIndexes
    const vectors = yield* models.embed(pending.map((row) => row.content))
    yield* Effect.forEach(Arr.chunksOf(Arr.zip(pending, vectors), 50), (page) =>
      helix.write("bookmarks_ingest", page.reduce((batch, [row, vector], offset) => {
        const document = `d${offset}`, chunk = `c${offset}`
        const cleared = options.replace
          ? batch.varAs(`oc${offset}`, chunksOfDocument(row.documentId).drop()).varAs(`od${offset}`, documentById(row.documentId).drop())
          : batch
        const labelled = { category: row.category, salience: row.salience, folders: row.folders, postedAt: row.postedAt }
        return cleared
          .varAs(document, g().addN("SourceDocument", {
            tenant_id: TENANT, userId: OWNER, documentId: row.documentId, customId: row.documentId.slice(2), title: row.title,
            source: "x", sourceType: "x", kind: "x_bookmark", url: row.url, uri: row.url, chunkCount: 1,
            checksum: createHash("sha256").update(row.content).digest("hex").slice(0, 24), status: "indexed", createdAt: Expr.datetime(),
            ...labelled, metadata: labelled,
          }))
          .varAs(chunk, g().addN("Chunk", {
            tenant_id: TENANT, userId: OWNER, chunkId: chunkIdOf(row.documentId, 0), documentId: row.documentId, title: row.title,
            chunkIndex: 0, ordinal: 0, documentKind: "x_bookmark", source: "x", content: row.content, contentHash: passageHash(row.content),
            embedding: PropertyValue.f32Array([...vector]), embeddingModel: `openai:${settings.embeddingModel}`, createdAt: Expr.datetime(),
            ...labelled,
          }))
          .varAs(`h${offset}`, g().n(NodeRef.var(document)).addE("HAS_CHUNK", NodeRef.var(chunk), { tenant_id: TENANT, ordinal: 0 }))
      }, writeBatch()).returning([])))
  }
  yield* Console.log(yield* summary)
})

/** Chunk ids of the bookmarks the graph pipeline must link (article text over 200 chars); `exo check` exempts the rest. */
export const articleChunkIds = bookmarkRows({ jsonl: Option.none(), labels: Option.none(), skip: Option.none(), ftdb: Option.none() }).pipe(
  Effect.map(({ rows }) => new Set<string>(rows.filter((row) => row.article).map((row) => chunkIdOf(row.documentId, 0)))),
)

// One-line posts stay out of the graph: linking them mints entities from banter; articles carry reusable subjects.
/** `exo bookmarks --graph`: link, name and settle indexed bookmarks whose article text exceeds 200 characters. */
export const graphBookmarks = Effect.fn("graphBookmarks")(function*(options: {
  readonly jsonl: Option.Option<string>
  readonly skip: Option.Option<string>
  readonly ftdb: Option.Option<string>
  readonly limit: Option.Option<number>
  readonly dryRun: boolean
  readonly redo: boolean
}): Effect.fn.Return<void, BookmarkSourceError | HelixError | NoTaxonomy | AiError.AiError, Helix | Models | LinkWriter | Settings | FileSystem.FileSystem> {
  const helix = yield* Helix
  const { rows } = yield* bookmarkRows({ ...options, labels: Option.none() })
  const articles = rows.filter((row) => row.article)
  const documents = new Map(articles.map((row) => [row.documentId, new Doc({
    documentId: row.documentId, title: row.title, kind: "x_bookmark", source: "x", url: row.url,
  })]))
  const states = yield* Effect.forEach(Arr.chunksOf(articles, 400), (page) =>
    helix.read("bookmark_graph_states", readBatch()
      .varAs("chunks", g().nWithLabelWhere("Chunk", Predicate.isIn("chunkId", page.map((row) => chunkIdOf(row.documentId, 0))))
        .where(tenantScope).project(fields("chunkId", "documentId", "linkVersion")))
      .returning(["chunks"]), Schema.Struct({ chunks: Schema.NullOr(Schema.Array(ChunkState)) })).pipe(
        Effect.map((data) => data.chunks ?? []),
      )).pipe(Effect.map(Arr.flatten))
  const due = states.filter((chunk) => documents.has(chunk.documentId) && (options.redo || (chunk.linkVersion ?? 0) !== LINK_VERSION))
  const pending = Option.match(options.limit, { onNone: () => due, onSome: (limit) => due.slice(0, limit) })
  yield* Console.log(`${articles.length} article bookmarks, ${states.length} indexed chunks, ${pending.length} to judge → ~${Math.ceil(pending.length / PACK)} Jev requests (${PACK} passages each, ≈ $${(pending.length * 2300 * 0.042 / 1e6).toFixed(2)}), naming + settling extra`)
  if (options.dryRun || !pending.length) return

  const started = yield* Clock.currentTimeMillis
  yield* helix.ensureIndexes
  const chunkIds = pending.map((chunk) => chunk.chunkId)
  const linked = yield* linkChunks({ chunkIds, documents, categories: yield* leafCategories, durable: false })
  const flagged = yield* helix.read("bookmark_graph_flagged", readBatch()
    .varAs("chunks", g().nWithLabelWhere("Chunk", Predicate.isIn("chunkId", chunkIds)).where(tenantScope)
      .where(Predicate.eq("needsNaming", true)).project(fields("chunkId")))
    .returning(["chunks"]), Schema.Struct({ chunks: Schema.NullOr(Schema.Array(Schema.Struct({ chunkId: ChunkId }))) }))
  const named = yield* nameChunks({ chunkIds: (flagged.chunks ?? []).map((row) => row.chunkId), documents })
  if (named.failures) yield* Console.error(`${named.failures} passages failed naming; rerun --graph --redo after fixing the model route`)
  yield* Effect.forEach(pending, (chunk) => settleDocument(chunk.documentId), { discard: true })
  // Durable barrier: a bulk drain may be acknowledged before its commit reaches object storage; this write cannot be.
  yield* helix.write("settle", writeBatch()
    .varAs("settled", g().nWithLabel("Category").where(tenantScope).limit(1).setProperty("settledAt", Expr.datetime()))
    .returning([]))
  const minutes = ((yield* Clock.currentTimeMillis) - started) / 60000
  yield* Console.log(`DONE ${linked.chunks} chunks judged, ${linked.mentions + named.mentions} mentions, ${linked.flagged} flagged for naming, ${named.named} new names in ${minutes.toFixed(1)} min`)
})
