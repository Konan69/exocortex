// Keeping the graph honest. Everything that writes is a dry run until `yes`.
// A backup is a readable copy of the judgement layer (every entity, every link with its endpoints and confidence, every
// chunk's judged properties). prune, dedupe, resolve and model work from the newest one, so nothing is deleted that is
// not already on disk. Node and edge ids are stable, so a backup is also the work list.
import { createHash } from "node:crypto"
import { join } from "node:path"
import { BatchCondition, EdgeRef, Expr, g, NodeRef, Predicate, readBatch, SourcePredicate, type Traversal, writeBatch } from "@helix-db/helix-db"
import { Array as Arr, Clock, Console, Effect, FileSystem, Option, Order, Result, Schema } from "effect"
import { Decision } from "effect/ai"
import { Models } from "./ai.ts"
import { articleChunkIds } from "./bookmarks.ts"
import { Settings } from "./config.ts"
import { DocumentId, EntityKey, entityNameProps, EntityKind, Floors, HUB, LINK_VERSION, MentionEdge, NodeId, normalise, OWNER, OWNER_NAME, TENANT } from "./domain.ts"
import { Helix } from "./helix.ts"
import { leafCategories } from "./pipeline.ts"
import { chunksOfDocument, documentById, fields, idAs, rowsOf, tenantScope, Vars } from "./queries.ts"

export class NoBackup extends Schema.TaggedError<NoBackup>()("NoBackup", { message: Schema.String }) {}
export class BrokenInvariant extends Schema.TaggedError<BrokenInvariant>()("BrokenInvariant", { message: Schema.String }) {}
export class DuplicateKey extends Schema.TaggedError<DuplicateKey>()("DuplicateKey", { label: Schema.String, key: Schema.String }) {}

/** two documents are copies when this share of one's passages is identical in the other */
const DEDUPE_SHARE = 0.8
/** or this share of its running text (sampled 8-word shingles) is in the other */
const RECUT_SHARE = 0.8

// ---- rows as backups hold them

const Entity = Schema.Struct({ $id: NodeId, entityKey: EntityKey, name: Schema.String, kind: Schema.String })
type Entity = typeof Entity.Type
const ChunkRow = Schema.Struct({
  $id: NodeId,
  chunkId: Schema.String,
  documentId: DocumentId,
  category: Schema.optional(Schema.NullOr(Schema.String)),
  salience: Schema.optional(Schema.NullOr(Schema.Finite)),
  aboutOwner: Schema.optional(Schema.NullOr(Schema.Boolean)),
  boilerplate: Schema.optional(Schema.NullOr(Schema.Boolean)),
  needsNaming: Schema.optional(Schema.NullOr(Schema.Boolean)),
  linkVersion: Schema.optional(Schema.NullOr(Schema.Finite)),
})
const DocumentMention = Schema.Struct({
  $id: NodeId, $from: NodeId, $to: NodeId,
  confidence: Schema.optional(Schema.NullOr(Schema.Finite)),
  entityKey: Schema.optional(Schema.NullOr(Schema.String)),
  evidence: Schema.optional(Schema.NullOr(Schema.String)),
})
const CategoryEdge = Schema.Struct({ $id: NodeId, $from: NodeId, $to: NodeId, confidence: Schema.optional(Schema.NullOr(Schema.Finite)) })
const Pairing = Schema.Struct({ a: NodeId, b: NodeId, chunk: NodeId, chunkB: Schema.optional(NodeId), same: Schema.Finite })
type Pairing = typeof Pairing.Type
const RelationType = Schema.Struct({ edge: NodeId, a: NodeId, b: NodeId, type: Schema.String, confidence: Schema.Finite })

const FILES = {
  entities: "entities.jsonl",
  chunks: "chunks.jsonl",
  documentMentions: "document_mentions.jsonl",
  chunkMentions: "chunk_mentions.jsonl",
  chunkCategories: "chunk_categories.jsonl",
} as const

const stamp = Effect.map(Clock.currentTimeMillis, (now) => new Date(now).toISOString().slice(0, 16).replace(/[:T]/g, "-"))
const clock = Effect.map(Clock.currentTimeMillis, (now) => new Date(now).toTimeString().slice(0, 8))
const toLines = (rows: ReadonlyArray<unknown>) => rows.length ? rows.map((row) => JSON.stringify(row)).join("\n") + "\n" : ""

const readJsonl = Effect.fnUntraced(function*<S extends Schema.Top>(path: string, row: S) {
  const fs = yield* FileSystem.FileSystem
  if (!(yield* fs.exists(path))) return [] as ReadonlyArray<S["Type"]>
  const lines = (yield* fs.readFileString(path)).split("\n").filter(Boolean)
  return yield* Schema.decodeUnknownEffect(Schema.Array(Schema.fromJsonString(row)))(lines)
})

/** The newest backup holding links. */
const latestBackup = Effect.gen(function*() {
  const fs = yield* FileSystem.FileSystem
  const settings = yield* Settings
  const names = (yield* fs.exists(settings.backupDir)) ? yield* fs.readDirectory(settings.backupDir) : []
  const dated = yield* Effect.forEach(names, (name) => {
    const dir = join(settings.backupDir, name)
    return Effect.gen(function*() {
      const info = yield* fs.stat(dir)
      if (info.type !== "Directory" || !(yield* fs.exists(join(dir, FILES.chunkMentions)))) return Option.none()
      return Option.some({ dir, at: Option.getOrElse(Option.map(info.mtime, (date) => date.getTime()), () => 0) })
    })
  })
  const newest = Arr.last(Arr.sortWith(Arr.getSomes(dated), (entry) => entry.at, Order.Number))
  if (Option.isNone(newest)) return yield* new NoBackup({ message: "no backup found: run `exo backup` first" })
  return newest.value.dir
})

const fromBackup = {
  entities: (dir: string) => readJsonl(join(dir, FILES.entities), Entity),
  chunks: (dir: string) => readJsonl(join(dir, FILES.chunks), ChunkRow),
  documentMentions: (dir: string) => readJsonl(join(dir, FILES.documentMentions), DocumentMention),
  chunkMentions: (dir: string) => readJsonl(join(dir, FILES.chunkMentions), MentionEdge),
  chunkCategories: (dir: string) => readJsonl(join(dir, FILES.chunkCategories), CategoryEdge),
}

// The links that stand: judged by the current pipeline (they carry `by`), at or over the floor (an unchecked LLM link
// stands until something has read it), one per chunk→entity pair: Jev's own judgement wins over an LLM's, then the surer.
const standingLinks = (edges: ReadonlyArray<MentionEdge>) => {
  const rank = (edge: MentionEdge) => (edge.by === "jev" ? 2 : 1) + edge.confidence
  const best = new Map<string, MentionEdge>()
  for (const edge of edges) {
    if (!edge.by || (edge.by !== "llm" && edge.confidence < Floors.mention)) continue
    const pair = `${edge.$from}>${edge.$to}`, held = best.get(pair)
    if (!held || rank(edge) > rank(held)) best.set(pair, edge)
  }
  return [...best.values()]
}

// Edges go by id, a few thousand a write; nodes go by id too and take their remaining edges (and name shadows) with them.
const dropEdges = Effect.fnUntraced(function*(ids: ReadonlyArray<NodeId>, label: string) {
  const helix = yield* Helix
  let done = 0
  for (const page of Arr.chunksOf(ids, 2000)) {
    yield* helix.write(label, writeBatch().varAs("gone", g().dropEdgeById([...page])).returning([]))
    yield* Console.log(`  ${label}: ${done += page.length}/${ids.length}`)
  }
})
const dropEntities = Effect.fnUntraced(function*(ids: ReadonlyArray<NodeId>, label: string) {
  const helix = yield* Helix
  let done = 0
  for (const page of Arr.chunksOf(ids, 100)) {
    const keys = (yield* rowsOf("entity_keys_to_drop", g().n([...page]).project(fields("entityKey")), Schema.Struct({ entityKey: Schema.optional(Schema.String) })))
      .flatMap((row) => row.entityKey ? [row.entityKey] : [])
    const shadows = keys.length ? writeBatch().varAs("shadows", g().nWithLabelWhere("EntityName", Predicate.isIn("entityKey", keys)).where(tenantScope).drop()) : writeBatch()
    yield* helix.write(label, shadows.varAs("gone", g().n([...page]).drop()).returning([]))
    yield* Console.log(`  ${label}: ${done += page.length}/${ids.length}`)
  }
})
/** Documents go with their chunks; chunks are text-indexed, so a write stays far under 512 mutations. */
const dropDocuments = Effect.fnUntraced(function*(ids: ReadonlyArray<string>, label: string) {
  const helix = yield* Helix
  let done = 0
  const one = (documentId: DocumentId) => Effect.andThen(
    helix.write(`${label}_big`, writeBatch().varAs("c", chunksOfDocument(documentId).limit(400).drop()).returning([])),
    helix.write(`${label}_big_rest`, writeBatch().varAs("c", chunksOfDocument(documentId).drop()).varAs("d", documentById(documentId).drop()).returning([])),
  )
  for (const page of Arr.chunksOf(ids.map((id) => DocumentId.make(id)), 25)) {
    const batch = page.reduce((out, documentId, index) => out.varAs(`c${index}`, chunksOfDocument(documentId).drop()).varAs(`d${index}`, documentById(documentId).drop()), writeBatch())
    yield* helix.write(label, batch.returning([])).pipe(Effect.catch(() => Effect.forEach(page, one, { discard: true })))
    yield* Console.log(`  ${label}: ${done += page.length}/${ids.length} documents`)
  }
})

// ---------------------------------------------------------------- backup

/** `exo backup [dir]`: entities, links with endpoints and confidence, judged chunk props → backup/<stamp>/*.jsonl. */
export const backup = Effect.fn("backup")(function*(dir: Option.Option<string>) {
  const helix = yield* Helix
  const fs = yield* FileSystem.FileSystem
  const settings = yield* Settings
  const target = Option.isSome(dir) ? dir.value : join(settings.backupDir, yield* stamp)
  yield* fs.makeDirectory(target, { recursive: true })
  const dump = (file: string, rows: ReadonlyArray<unknown>) =>
    Effect.andThen(fs.writeFileString(join(target, file), toLines(rows)), Console.log(`${file.padEnd(26)} ${rows.length}`))
  yield* dump(FILES.entities, yield* rowsOf("backup_entities", g().nWithLabel("Entity").where(tenantScope).valueMap(["$id", "entityKey", "name", "kind"]), Entity))
  const chunks = yield* rowsOf("backup_chunks", g().nWithLabel("Chunk").where(tenantScope)
    .valueMap(["$id", "chunkId", "documentId", "category", "salience", "aboutOwner", "boilerplate", "needsNaming", "linkVersion"]), ChunkRow)
  yield* dump(FILES.chunks, chunks)
  yield* dump(FILES.documentMentions, yield* rowsOf("backup_document_mentions",
    g().nWithLabel("SourceDocument").where(tenantScope).outE("MENTIONS").valueMap(["$id", "$from", "$to", "confidence", "entityKey", "evidence"]), DocumentMention))
  // chunk links are the bulk (hundreds of thousands): read a page of documents at a time, appended as they come
  const mentionsFile = join(target, FILES.chunkMentions), categoriesFile = join(target, FILES.chunkCategories)
  yield* fs.writeFileString(mentionsFile, "")
  yield* fs.writeFileString(categoriesFile, "")
  let mentions = 0
  for (const page of Arr.chunksOf([...new Set(chunks.map((chunk) => chunk.documentId))], 60)) {
    const batch = page.reduce((out, documentId, index) => out
      .varAs(`m${index}`, chunksOfDocument(documentId).outE("MENTIONS").valueMap(["$id", "$from", "$to", "confidence", "by", "entityKey"]))
      .varAs(`c${index}`, chunksOfDocument(documentId).outE("IN_CATEGORY").valueMap(["$id", "$from", "$to", "confidence"])), readBatch())
    const names = page.flatMap((_, index) => [`m${index}`, `c${index}`])
    const data = yield* helix.read("backup_links", batch.returning(names), Vars(Schema.Unknown))
    const linkRows = yield* Schema.decodeUnknownEffect(Schema.Array(MentionEdge))(page.flatMap((_, index) => data[`m${index}`] ?? []))
    const categoryRows = yield* Schema.decodeUnknownEffect(Schema.Array(CategoryEdge))(page.flatMap((_, index) => data[`c${index}`] ?? []))
    yield* fs.writeFileString(mentionsFile, toLines(linkRows), { flag: "a" })
    yield* fs.writeFileString(categoriesFile, toLines(categoryRows), { flag: "a" })
    mentions += linkRows.length
  }
  yield* Console.log(`${FILES.chunkMentions.padEnd(26)} ${mentions}\n→ ${target}`)
})

// ---------------------------------------------------------------- check

/** `exo check`: live invariants, one scan after another so a slow one does not hide the rest; fails when one is broken. */
export const check = Effect.gen(function*() {
  const helix = yield* Helix
  const count = (name: string, traversal: Traversal<any, "read">) =>
    Effect.map(helix.read(`check_${name}`, readBatch().varAs("value", traversal).returning(["value"]), Schema.Struct({ value: Schema.Finite })), (data) => data.value)
  const list = <S extends Schema.Top>(name: string, traversal: Traversal<any, "read">, row: S) => rowsOf(`check_${name}`, traversal, row)
  const EntityCount = Schema.Struct({ count: Schema.Finite, entityKey: Schema.NullOr(Schema.String) })
  const DocumentCount = Schema.Struct({ count: Schema.Finite, documentId: Schema.NullOr(Schema.String) })
  let broken = 0
  const line = (label: string, value: number | string, ok = true) => {
    if (!ok) broken++
    return Console.log(`${ok ? " " : "✗"} ${label.padEnd(46)} ${value}`)
  }
  const mentions = () => g().nWithLabel("Chunk").where(tenantScope).outE("MENTIONS")

  const chunks = yield* count("chunks", g().nWithLabel("Chunk").where(tenantScope).count())
  yield* line("chunks", chunks)
  const judged = yield* count("judged", g().nWithLabelWhere("Chunk", Predicate.eq("linkVersion", LINK_VERSION)).where(tenantScope).count())
  // The fast-index one-line posts deliberately skip linking; only article bookmarks must pass the graph pipeline.
  const articles = yield* articleChunkIds.pipe(Effect.catchTag("BookmarkSourceError", () => Effect.succeed(new Set<string>())))
  const bookmarks = yield* list("bookmark_states", g().nWithLabelWhere("Chunk", Predicate.eq("documentKind", "x_bookmark")).where(tenantScope).project(fields("chunkId", "linkVersion")),
    Schema.Struct({ chunkId: Schema.String, linkVersion: Schema.optional(Schema.NullOr(Schema.Finite)) }))
  const exempt = bookmarks.filter((row) => !articles.has(row.chunkId) && (row.linkVersion ?? 0) !== LINK_VERSION).length
  const unjudged = chunks - judged - exempt
  yield* line("  not judged by Jev (article bookmarks included)", unjudged, unjudged === 0)
  yield* line("  index-only bookmark exemptions", exempt)
  yield* line("  flagged, waiting for naming", yield* count("unnamed", g().nWithLabelWhere("Chunk", Predicate.eq("needsNaming", true)).where(tenantScope).count()))
  const links = yield* count("links", mentions().count())
  yield* line("links chunk→entity", links)
  yield* line("  per chunk", (links / Math.max(1, chunks)).toFixed(1))
  const weak = yield* count("weak", mentions().where(Predicate.lt("confidence", Floors.mention)).count())
  yield* line(`  under the ${Floors.mention} floor`, weak, weak === 0)
  const perEntity = yield* list("per_entity", mentions().groupCount("entityKey"), EntityCount)
  const keyed = perEntity.reduce((sum, row) => sum + row.count, 0)
  yield* line("  without an entityKey (written by something else)", links - keyed, links === keyed)
  const unchecked = yield* count("unchecked", mentions().where(Predicate.eq("by", "llm")).count())
  yield* line("  written by an LLM, never checked by Jev", unchecked, unchecked === 0)

  const entities = yield* count("entities", g().nWithLabel("Entity").where(tenantScope).count())
  const rostered = yield* list("rostered", g().nWithLabel("SourceDocument").where(tenantScope).outE("MENTIONS").groupCount("entityKey"), EntityCount)
  const held = new Set([...perEntity, ...rostered].map((row) => row.entityKey))
  const hubs = perEntity.filter((row) => row.count > HUB)
  yield* line("entities", entities)
  const NameRow = Schema.Struct({ entityKey: Schema.String, name: Schema.String, kind: Schema.String })
  const entityRows = yield* list("entity_names", g().nWithLabel("Entity").where(tenantScope).project(fields("entityKey", "name", "kind")), NameRow)
  const shadows = yield* list("name_shadows", g().nWithLabel("EntityName").where(tenantScope).project(fields("entityKey", "name", "kind")), NameRow)
  const shadowsOf = Arr.groupBy(shadows, (row) => row.entityKey)
  const expected = new Set(entityRows.map((row) => row.entityKey))
  yield* line("  Entity keys duplicated", entityRows.length - expected.size, entityRows.length === expected.size)
  const mismatched = entityRows.filter((row) => {
    const copies = shadowsOf[row.entityKey]
    return copies?.length !== 1 || copies[0].name !== row.name || copies[0].kind !== row.kind
  })
  const stray = shadows.filter((row) => !expected.has(row.entityKey))
  // The shadow exists because Helix v0.0.5 cannot build Entity.name text over pre-existing Entity nodes.
  yield* line("  EntityName missing/duplicate/mismatched", mismatched.length, mismatched.length === 0)
  yield* line("  EntityName without Entity", stray.length, stray.length === 0)
  yield* line("  held by no link and no document", Math.max(0, entities - held.size), entities - held.size <= 0)
  yield* line("  held by one passage", perEntity.filter((row) => row.count === 1).length)
  yield* line(`  hubs (over ${HUB} passages, not walked)`, `${hubs.length} holding ${hubs.reduce((sum, row) => sum + row.count, 0)} links`)

  // a document with no passages was half-ingested, two documents under one id make every lookup ambiguous, and a
  // passage whose document is gone can never be found by anything that starts from a document
  const documentIds = (yield* list("documents", g().nWithLabel("SourceDocument").where(tenantScope).project(fields("documentId")), Schema.Struct({ documentId: Schema.String })))
    .map((row) => row.documentId)
  const chunksPer = new Map((yield* list("chunks_per_document", g().nWithLabel("Chunk").where(tenantScope).groupCount("documentId"), DocumentCount))
    .map((row) => [row.documentId ?? "", row.count]))
  const seen = new Set<string>()
  const duplicates = documentIds.filter((id) => seen.size === seen.add(id).size)
  const empty = [...seen].filter((id) => !chunksPer.has(id))
  const orphans = [...chunksPer].filter(([id]) => !seen.has(id))
  const examples = (ids: ReadonlyArray<string>) => ids.length ? ` (e.g. ${ids.slice(0, 3).join(" · ")})` : ""
  yield* line("documents", seen.size)
  yield* line("  with no passages", empty.length + examples(empty), empty.length === 0)
  yield* line("  sharing a documentId with another", duplicates.length + examples(duplicates), duplicates.length === 0)
  yield* line("  passages whose document is gone", orphans.reduce((sum, [, n]) => sum + n, 0) + (orphans.length ? ` in ${orphans.length} missing documents` : ""), orphans.length === 0)
  if (broken) return yield* new BrokenInvariant({ message: `${broken} invariant(s) broken` })
})

// ---------------------------------------------------------------- prune

/** `exo prune`: drop every link that does not stand, every category edge but a chunk's newest, and orphan entities. No model calls. */
export const prune = Effect.fn("prune")(function*(options: { readonly yes: boolean }) {
  const dir = yield* latestBackup
  const all = yield* fromBackup.chunkMentions(dir)
  const standing = new Set(standingLinks(all).map((edge) => edge.$id))
  const fallen = all.filter((edge) => !standing.has(edge.$id))
  const categories = yield* fromBackup.chunkCategories(dir)
  const newestCategory = new Map<NodeId, NodeId>()
  for (const edge of categories) newestCategory.set(edge.$from, Math.max(newestCategory.get(edge.$from) ?? 0, edge.$id))
  const staleCategories = categories.filter((edge) => newestCategory.get(edge.$from) !== edge.$id)
  const old = fallen.filter((edge) => !edge.by).length
  const weak = fallen.filter((edge) => edge.by && edge.confidence < Floors.mention).length
  yield* Console.log(`${dir}\n${all.length} links · ${old} from earlier pipeline versions · ${weak} under ${Floors.mention} · ${fallen.length - old - weak} duplicate pairs → ${standing.size} stand\n${staleCategories.length} stale category edges`)
  // an entity no standing link points at, and no document roster holds, connects nothing
  const held = new Set([...all.filter((edge) => standing.has(edge.$id)).map((edge) => edge.$to), ...(yield* fromBackup.documentMentions(dir)).map((edge) => edge.$to)])
  const orphans = (yield* fromBackup.entities(dir)).filter((entity) => !held.has(entity.$id))
  yield* Console.log(`${orphans.length} orphan entities${orphans.length ? ` (e.g. ${orphans.slice(0, 6).map((entity) => entity.name).join(" · ")})` : ""}`)
  if (!options.yes) return yield* Console.log("dry run — add --yes")
  yield* dropEdges(fallen.map((edge) => edge.$id), "prune_links")
  yield* dropEdges(staleCategories.map((edge) => edge.$id), "prune_category_edges")
  yield* dropEntities(orphans.map((entity) => entity.$id), "prune_orphan_entities")
})

// ---------------------------------------------------------------- dedupe / forget

const wordsOf = (text: string) => text.toLowerCase().replace(/\[\d+:\d+(:\d+)?\]|&\w+;/g, " ").replace(/[^\p{L}\p{N}]+/gu, " ").trim().split(" ")

/**
 * `exo dedupe`: the same document ingested twice (2026-09-20: 1,323 of 3,929 documents were ≥80% passage-for-passage
 * copies of another). Of a group of copies the fullest stays (ties: the one with a kind, then the non-"rec-" id).
 */
export const dedupe = Effect.fn("dedupe")(function*(options: { readonly yes: boolean }) {
  const helix = yield* Helix
  const fs = yield* FileSystem.FileSystem
  const dir = yield* latestBackup
  const chunks = yield* fromBackup.chunks(dir)
  const hashOf = new Map<NodeId, string>(), lengthOf = new Map<NodeId, number>(), words = new Map<NodeId, ReadonlyArray<string>>()
  for (const page of Arr.chunksOf(chunks, 400)) {
    const rows = yield* rowsOf("dedupe_texts", g().n(page.map((chunk) => chunk.$id)).project([idAs(), ...fields("content")]),
      Schema.Struct({ id: NodeId, content: Schema.optional(Schema.NullOr(Schema.String)) }))
    for (const row of rows) {
      const content = row.content ?? ""
      hashOf.set(row.id, createHash("sha256").update(content.replace(/\s+/g, " ").trim()).digest("hex"))
      lengthOf.set(row.id, content.length)
      words.set(row.id, wordsOf(content))
    }
  }
  // The header passage is not content: the same video ingested twice formats its metadata block differently, so 53
  // documents missed a 100% match by that one passage. Headers ran 134-216 characters; a first passage under 500 is ignored.
  const firstOf = new Map<string, NodeId>()
  for (const chunk of chunks) if ((firstOf.get(chunk.documentId) ?? Infinity) > chunk.$id) firstOf.set(chunk.documentId, chunk.$id)
  const isHeader = (chunk: typeof ChunkRow.Type) => firstOf.get(chunk.documentId) === chunk.$id && (lengthOf.get(chunk.$id) ?? 0) < 500
  const documents = yield* rowsOf("dedupe_documents", g().nWithLabel("SourceDocument").where(tenantScope).project(fields("documentId", "title", "kind")),
    Schema.Struct({ documentId: Schema.String, title: Schema.optional(Schema.NullOr(Schema.String)), kind: Schema.optional(Schema.NullOr(Schema.String)) }))
  const docOf = new Map(documents.map((doc) => [doc.documentId, doc]))
  const hashesOf = new Map<string, Set<string>>(), docsWith = new Map<string, Set<string>>()
  for (const chunk of chunks) {
    const hash = hashOf.get(chunk.$id)
    if (!hash || isHeader(chunk)) continue
    hashesOf.set(chunk.documentId, (hashesOf.get(chunk.documentId) ?? new Set()).add(hash))
    docsWith.set(hash, (docsWith.get(hash) ?? new Set()).add(chunk.documentId))
  }
  const standing = (documentId: string) => {
    const kind = docOf.get(documentId)?.kind
    return (kind && kind !== "recovered" ? 2 : 0) + (documentId.startsWith("rec-") ? 0 : 1)
  }
  const rank = (documentId: string) => (hashesOf.get(documentId)?.size ?? 0) * 4 + standing(documentId)
  const copies = new Map<string, string>()   // copy → the document it duplicates
  for (const [documentId, hashes] of [...hashesOf].sort((a, b) => rank(a[0]) - rank(b[0]))) {
    const overlap = new Map<string, number>()
    for (const hash of hashes) for (const other of docsWith.get(hash)!) if (other !== documentId && !copies.has(other)) overlap.set(other, (overlap.get(other) ?? 0) + 1)
    const best = [...overlap].filter(([other, shared]) => shared / hashes.size >= DEDUPE_SHARE && rank(other) >= rank(documentId)).sort((a, b) => rank(b[0]) - rank(a[0]))[0]
    if (best) copies.set(documentId, best[0])
  }
  // The passage test is blind to a copy cut differently (10 of 12 interviews stood twice, 14 against 16 passages).
  // Second test on the running text: 8-word shingles, one in eight sampled by hash; a copy when ≥80% of its sample is in
  // another's. Time stamps are stripped first. Of two copies the properly-kinded one is kept before the larger.
  const textOf = new Map<string, Array<string>>()
  for (const chunk of [...chunks].sort((a, b) => a.$id - b.$id)) {
    if (copies.has(chunk.documentId) || isHeader(chunk)) continue
    textOf.set(chunk.documentId, [...(textOf.get(chunk.documentId) ?? []), ...(words.get(chunk.$id) ?? [])])
  }
  const shinglesOf = new Map<string, Set<string>>(), docsSharing = new Map<string, Array<string>>()
  for (const [documentId, text] of textOf) {
    const sample = new Set<string>()
    for (let index = 0; index + 8 <= text.length; index++) {
      const hash = createHash("md5").update(text.slice(index, index + 8).join(" ")).digest()
      if (hash[0]! % 8 === 0) sample.add(hash.toString("hex", 0, 8))
    }
    if (sample.size < 20) continue   // too short to tell a copy from a shared phrase
    shinglesOf.set(documentId, sample)
    for (const shingle of sample) docsSharing.set(shingle, [...(docsSharing.get(shingle) ?? []), documentId])
  }
  // Shared text alone is not a re-ingest (a compilation holds 80% of a shorter video): between two documents of equal
  // standing the titles must agree as well; a "recovered" or kindless copy of a kinded document needs no such agreement.
  const titleKey = (documentId: string) => (docOf.get(documentId)?.title ?? "").toLowerCase().replace(/^\d+ - (meeting - )?/, "").replace(/—\s*\d{4}-\d\d-\d\d.*$/, "").replace(/[^\p{L}\p{N}]+/gu, "")
  const sameTitle = (a: string, b: string) => {
    const x = titleKey(a), y = titleKey(b)
    return x.length >= 8 && y.length >= 8 && (x.includes(y) || y.includes(x))
  }
  let recut = 0
  for (const [documentId, sample] of [...shinglesOf].sort((a, b) => standing(a[0]) - standing(b[0]) || a[1].size - b[1].size)) {
    const overlap = new Map<string, number>()
    for (const shingle of sample) for (const other of docsSharing.get(shingle)!) if (other !== documentId && !copies.has(other)) overlap.set(other, (overlap.get(other) ?? 0) + 1)
    const outranks = (other: string) => {
      const size = shinglesOf.get(other)!.size
      return standing(other) > standing(documentId)
        || (standing(other) === standing(documentId) && sameTitle(other, documentId) && (size > sample.size || (size === sample.size && other > documentId)))
    }
    const best = [...overlap].filter(([other, shared]) => shared / sample.size >= RECUT_SHARE && outranks(other)).sort((a, b) => standing(b[0]) - standing(a[0]) || b[1] - a[1])[0]
    if (best) {
      copies.set(documentId, best[0])
      recut++
    }
  }
  yield* Console.log(`   ${copies.size - recut} by identical passages · ${recut} by running text (same text, cut differently)`)
  const gone = chunks.filter((chunk) => copies.has(chunk.documentId))
  yield* Console.log(`${dir}\n${documents.length} documents · ${copies.size} are copies (≥${Math.round(DEDUPE_SHARE * 100)}% identical passages) · ${gone.length} of ${chunks.length} passages go with them`)
  const kindOf = (documentId: string) => docOf.get(documentId)?.kind || "(no kind)"
  yield* Console.log(`   copies by kind: ${JSON.stringify(Object.fromEntries(Object.entries(Arr.groupBy([...copies.keys()], kindOf)).map(([kind, ids]) => [kind, ids.length])))}`)
  yield* Effect.forEach([...copies].slice(0, 8), ([copy, original]) =>
    Console.log(`   ${copy} "${(docOf.get(copy)?.title ?? "").slice(0, 50)}" [${docOf.get(copy)?.kind}]  →  kept ${original} "${(docOf.get(original)?.title ?? "").slice(0, 50)}" [${docOf.get(original)?.kind}]`), { discard: true })
  const listFile = join(dir, "dedupe.json")
  yield* fs.writeFileString(listFile, JSON.stringify(Object.fromEntries(copies), null, 1))
  if (!options.yes) return yield* Console.log(`dry run — full list in ${listFile} — add --yes`)
  yield* helix.ensureIndexes
  yield* dropDocuments([...copies.keys()], "dedupe_documents")
})

/** `exo forget <file>`: remove documents ingested by mistake, one documentId per line. */
export const forget = Effect.fn("forget")(function*(options: { readonly file: string; readonly yes: boolean }) {
  const fs = yield* FileSystem.FileSystem
  const wanted = (yield* fs.readFileString(options.file)).split("\n").map((line) => line.trim()).filter(Boolean)
  const present = wanted.length
    ? yield* rowsOf("forget_lookup", g().nWithLabelWhere("SourceDocument", Predicate.isIn("documentId", wanted)).where(tenantScope).project(fields("documentId", "title", "kind")),
      Schema.Struct({ documentId: Schema.String, title: Schema.optional(Schema.NullOr(Schema.String)), kind: Schema.optional(Schema.NullOr(Schema.String)) }))
    : []
  yield* Console.log(`${wanted.length} ids given · ${present.length} found in the graph`)
  yield* Effect.forEach(present.slice(0, 8), (row) => Console.log(`   ${row.documentId}  "${(row.title ?? "").slice(0, 55)}" [${row.kind ?? ""}]`), { discard: true })
  if (!present.length) return
  if (!options.yes) return yield* Console.log("dry run — add --yes")
  yield* dropDocuments(present.map((row) => row.documentId), "forget_documents")
})

// ---------------------------------------------------------------- entities

/** `exo entities --index-names`: EntityName shadow parity (missing / stale / extra). */
export const indexEntityNames = Effect.fn("indexEntityNames")(function*(options: { readonly yes: boolean }) {
  const helix = yield* Helix
  const Row = Schema.Struct({ entityKey: EntityKey, name: Schema.String, kind: EntityKind })
  const project = fields("entityKey", "name", "kind")
  const data = yield* helix.read("entity_name_backfill_state", readBatch()
    .varAs("entities", g().nWithLabel("Entity").where(tenantScope).project(project))
    .varAs("shadows", g().nWithLabel("EntityName").where(tenantScope).project(project))
    .returning(["entities", "shadows"]), Schema.Struct({ entities: Schema.NullOr(Schema.Array(Row)), shadows: Schema.NullOr(Schema.Array(Row)) }))
  const entities = data.entities ?? [], shadows = data.shadows ?? []
  const keys = new Set<string>()
  for (const row of entities) {
    if (keys.has(row.entityKey)) return yield* new DuplicateKey({ label: "Entity", key: row.entityKey })
    keys.add(row.entityKey)
  }
  const existing = new Map<string, typeof Row.Type>()
  for (const row of shadows) {
    if (existing.has(row.entityKey)) return yield* new DuplicateKey({ label: "EntityName", key: row.entityKey })
    existing.set(row.entityKey, row)
  }
  const missing = entities.filter((row) => !existing.has(row.entityKey))
  const changed = entities.filter((row) => {
    const old = existing.get(row.entityKey)
    return old && (old.name !== row.name || old.kind !== row.kind)
  })
  const extra = shadows.filter((row) => !keys.has(row.entityKey))
  yield* Console.log(`${entities.length} entities · ${shadows.length} EntityName nodes · ${missing.length} missing · ${changed.length} stale · ${extra.length} without Entity`)
  if (!options.yes) return yield* Console.log("dry run — add --yes to index names")
  yield* helix.ensureIndexes
  let indexed = 0
  for (const page of Arr.chunksOf(missing, 60)) {
    yield* helix.write("entity_name_backfill", page.reduce((batch, row, index) => batch.varAs(`n${index}`, g().addN("EntityName", entityNameProps(row))), writeBatch()).returning([]))
    yield* Console.log(`  indexed ${indexed += page.length}/${missing.length}`)
  }
  for (const page of Arr.chunksOf(changed, 60)) {
    yield* helix.write("entity_name_sync", page.reduce((batch, row, index) =>
      batch.varAs(`n${index}`, g().nWithLabelWhere("EntityName", SourcePredicate.eq("entityKey", row.entityKey)).where(tenantScope)
        .setProperty("name", row.name).setProperty("kind", row.kind)), writeBatch()).returning([]))
  }
  if (extra.length) yield* Console.error(`${extra.length} stale EntityName nodes remain; inspect before deleting`)
  yield* Console.log(`DONE ${missing.length} indexed, ${changed.length} updated`)
})

const PairState = Schema.Struct({ a: Schema.String, b: Schema.String, passage_using_a: Schema.String, passage_using_b: Schema.String })
const SameThing = Decision.make({
  input: PairState,
  decisions: { same: Decision.probability({ instructions: "a and b are two names for one and the same thing (not merely related, not part and whole, not competitors)" }) },
})

/**
 * `exo entities --resolve`: "BCG" and "Boston Consulting Group" are two nodes for one thing. A bare name carries next to
 * no context, so names are never compared alone. The graph proposes: two entities of one kind that keep turning up in
 * the same passages, the same slug under two kinds, or names equal once spacing and punctuation are gone. Jev decides,
 * reading a passage for each side, one pair a request (packed pairs leak into each other). A confirmed group is merged
 * into its best-held name: links re-pointed, other names kept in `aliases`, emptied nodes dropped.
 */
export const resolveEntities = Effect.fn("resolveEntities")(function*(options: { readonly yes: boolean; readonly limit: Option.Option<number> }) {
  const helix = yield* Helix
  const models = yield* Models
  const fs = yield* FileSystem.FileSystem
  const dir = yield* latestBackup
  const log = join(dir, "resolve.jsonl")
  const entities = new Map((yield* fromBackup.entities(dir)).map((entity) => [entity.$id, entity]))
  const entity = (id: NodeId) => entities.get(id)!
  const slugOf = (id: NodeId) => entity(id).entityKey.split(":")[2] ?? ""
  const squashOf = (id: NodeId) => `${entity(id).kind}|${normalise(entity(id).name).replace(/-/g, "")}`
  const links = standingLinks(yield* fromBackup.chunkMentions(dir)).filter((edge) => entities.has(edge.$to))
  const holders = new Map<NodeId, Set<NodeId>>()
  for (const edge of links) holders.set(edge.$to, (holders.get(edge.$to) ?? new Set()).add(edge.$from))
  const byChunk = Arr.groupBy(links, (edge) => String(edge.$from))
  const together = new Map<string, { a: NodeId; b: NodeId; count: number; chunk: NodeId }>()
  for (const edges of Object.values(byChunk)) {
    const ids = edges.map((edge) => edge.$to)
    for (let i = 0; i < ids.length; i++) {
      for (let j = i + 1; j < ids.length; j++) {
        const a = Math.min(ids[i]!, ids[j]!), b = Math.max(ids[i]!, ids[j]!)
        if (entity(a).kind !== entity(b).kind && slugOf(a) !== slugOf(b)) continue
        const key = `${a}>${b}`, entry = together.get(key) ?? { a, b, count: 0, chunk: edges[0].$from }
        entry.count++
        together.set(key, entry)
      }
    }
  }
  const candidates: Array<{ a: NodeId; b: NodeId; count: number; chunk: NodeId; chunkB: NodeId }> = [...together.values()]
    .filter(({ a, b, count }) => slugOf(a) === slugOf(b) || (count >= 2 && count / Math.min(holders.get(a)!.size, holders.get(b)!.size) >= 0.5))
    .map((pair) => ({ ...pair, chunkB: pair.chunk }))
  // Co-occurrence is blind to a pure alias: a writer picks ONE of "Data Bricks" / "Databricks" per passage. Names equal
  // once spacing and punctuation are out are proposed too, each side carrying a passage of its own (44 groups, 1,039 links).
  const seen = new Set(candidates.map(({ a, b }) => `${a}>${b}`))
  for (const ids of Object.values(Arr.groupBy([...holders.keys()], squashOf))) {
    for (const id of ids.slice(1)) {
      const a = Math.min(ids[0], id), b = Math.max(ids[0], id)
      if (seen.has(`${a}>${b}`)) continue
      seen.add(`${a}>${b}`)
      candidates.push({ a, b, count: 0, chunk: [...holders.get(a)!][0]!, chunkB: [...holders.get(b)!][0]! })
    }
  }
  const done = new Set((yield* readJsonl(log, Pairing)).map((row) => `${row.a}>${row.b}`))
  const due = candidates.filter(({ a, b }) => !done.has(`${a}>${b}`))
  const pending = Option.match(options.limit, { onNone: () => due, onSome: (limit) => due.slice(0, limit) })
  yield* Console.log(`${dir}\n${candidates.length} candidate pairs (${candidates.filter((pair) => pair.count > 0).length} share passages, ${candidates.filter((pair) => pair.count === 0).length} spelling variants) · ${done.size} already judged · ${pending.length} to judge → ${pending.length} Jev requests (one pair each)`)
  let judged = 0
  yield* Effect.forEach(pending, (pair) =>
    Effect.gen(function*() {
      const texts = new Map((yield* rowsOf("passages_for_pairs", g().n([...new Set([pair.chunk, pair.chunkB])]).project([idAs(), ...fields("content")]),
        Schema.Struct({ id: NodeId, content: Schema.optional(Schema.NullOr(Schema.String)) }))).map((row) => [row.id, row.content ?? ""]))
      const outcomes = yield* models.decide(SameThing, {
        a: `${entity(pair.a).name} (${entity(pair.a).kind})`, b: `${entity(pair.b).name} (${entity(pair.b).kind})`,
        passage_using_a: texts.get(pair.chunk) ?? "", passage_using_b: texts.get(pair.chunkB) ?? "",
      })
      const same = Result.isSuccess(outcomes.same) ? outcomes.same.success.probability : 0
      yield* fs.writeFileString(log, JSON.stringify({ a: pair.a, b: pair.b, chunk: pair.chunk, chunkB: pair.chunkB, same }) + "\n", { flag: "a" })
      if (++judged % 200 === 0) yield* Console.log(`[${yield* clock}] ${judged}/${pending.length} pairs`)
    }), { concurrency: 16, discard: true })

  // Identity is the name, not the name plus a kind: shown two kinds, Jev calls them different things (0 of 425 same-name
  // pairs reached 0.9), so the same normalised name merges without asking; Jev decides the rest.
  const sameName: Array<Pairing> = Object.values(Arr.groupBy([...entities.keys()], slugOf))
    .flatMap((ids) => ids.slice(1).map((id) => ({ a: ids[0], b: id, chunk: 0, same: 1 })))
  const isVariant = (row: Pairing) => squashOf(row.a) === squashOf(row.b)
  const verdicts = (yield* readJsonl(log, Pairing)).filter((row) => entities.has(row.a) && entities.has(row.b) && slugOf(row.a) !== slugOf(row.b))
  const confirmed = [...sameName, ...verdicts.filter((row) => row.same >= (isVariant(row) ? Floors.alias : 0.9))]
  yield* Console.log(`${sameName.length} same-name pairs (merged without asking) · ${confirmed.filter((row) => !isVariant(row)).length - sameName.length} confirmed by Jev at 0.9 · ${confirmed.filter(isVariant).length} spelling variants at ${Floors.alias}`)
  // confirmed pairs → groups (a=b, b=c ⇒ one group), each merged into its best-held member
  const parent = new Map<NodeId, NodeId>()
  const find = (id: NodeId): NodeId => {
    let root = id
    while (parent.has(root) && parent.get(root) !== root) root = parent.get(root)!
    return root
  }
  for (const { a, b } of confirmed) parent.set(find(a), find(b))
  const merges = Object.values(Arr.groupBy([...new Set(confirmed.flatMap((row) => [row.a, row.b]))], (id) => String(find(id)))).map((ids) => {
    const sorted = [...ids].sort((x, y) => (holders.get(y)?.size ?? 0) - (holders.get(x)?.size ?? 0) || entity(y).name.length - entity(x).name.length)
    return { keep: sorted[0]!, fold: sorted.slice(1) }
  })
  yield* Effect.forEach(merges.slice(0, 25), (merge) => Console.log(`   ${entity(merge.keep).name}  ⇐  ${merge.fold.map((id) => entity(id).name).join(" · ")}`), { discard: true })
  yield* Console.log(`${confirmed.length} pairs confirmed → ${merges.length} merges folding ${merges.reduce((sum, merge) => sum + merge.fold.length, 0)} entities away`)
  if (!options.yes) return yield* Console.log("dry run — add --yes to merge")
  yield* helix.ensureIndexes
  const documentLinks = yield* fromBackup.documentMentions(dir)
  for (const merge of merges) {
    const keep = entity(merge.keep), folded = new Set(merge.fold)
    const has = new Set(links.filter((edge) => edge.$to === merge.keep).map((edge) => edge.$from))
    const rostered = new Set(documentLinks.filter((edge) => edge.$to === merge.keep).map((edge) => edge.$from))
    let batch = writeBatch().varAs("keep", g().n(merge.keep).setProperty("aliases", merge.fold.map((id) => entity(id).name)))
    let edge = 0
    for (const link of links) {
      if (!folded.has(link.$to) || has.has(link.$from)) continue
      has.add(link.$from)
      batch = batch.varAs(`l${edge++}`, g().n(link.$from).addE("MENTIONS", merge.keep, { tenant_id: TENANT, confidence: link.confidence, entityKey: keep.entityKey, by: link.by ?? "jev" }))
    }
    for (const link of documentLinks) {
      if (!folded.has(link.$to) || rostered.has(link.$from)) continue
      rostered.add(link.$from)
      batch = batch.varAs(`l${edge++}`, g().n(link.$from).addE("MENTIONS", merge.keep, { tenant_id: TENANT, confidence: link.confidence ?? 1, entityKey: keep.entityKey, evidence: link.evidence ?? "" }))
    }
    const foldedKeys = merge.fold.map((id) => entity(id).entityKey)
    yield* helix.write("merge_entities", batch
      .varAs("shadows", g().nWithLabelWhere("EntityName", Predicate.isIn("entityKey", foldedKeys)).where(tenantScope).drop())
      .varAs("gone", g().n(merge.fold).drop()).returning([]))
  }
  yield* Console.log("merged. Take a new backup before running hygiene commands again (this one is now out of date).")
})

// ---------------------------------------------------------------- the rest of Helix's memory model

const Profile = Schema.Struct({ static: Schema.Array(Schema.String), dynamic: Schema.Array(Schema.String) })
const Tree = Schema.Struct({ parents: Schema.Array(Schema.Struct({ name: Schema.String, description: Schema.String, children: Schema.Array(Schema.String) })) })
const RelationKind = Decision.make({
  input: Schema.Struct({ a: Schema.String, b: Schema.String }),
  decisions: {
    type: Decision.classify({
      instructions: "How passages a and b relate",
      criteria: {
        same_event: "two records of the very same meeting, call, talk or document (for example notes and the verbatim transcript)",
        a_is_newer: "a states a later version of facts, plans or decisions that b also states — a supersedes b",
        b_is_newer: "b states a later version of facts, plans or decisions that a also states — b supersedes a",
        related: "about the same specific matter, but neither of the above",
      },
    }),
  },
})

/**
 * `exo model`: Helix's memory-system model mapped onto a corpus whose unit of knowledge is the Chunk, idempotently:
 *   User -OWNS-> SourceDocument · User -HAS_PROFILE-> UserProfile (GLM, from the densest passages about the owner)
 *   Category -PARENT_OF-> Category (GLM groups the leaves)
 *   RELATES_TO typed by Jev: two records of one event become EXTENDS (fuller → briefer), a newer statement UPDATES the older
 *   Entity -ASSOCIATED_WITH{passages, share}-> Entity (counted from the standing links, no model)
 * Not built: DERIVES (nothing is inferred), Session / DERIVED_FROM (no chat sessions), Connector / IngestionJob.
 */
export const model = Effect.fn("model")(function*(options: { readonly yes: boolean }) {
  const helix = yield* Helix
  const models = yield* Models
  const fs = yield* FileSystem.FileSystem
  const dir = yield* latestBackup
  const apply = options.yes
  if (apply) yield* helix.ensureIndexes
  const dropAll = (label: string) => helix.write(`model_drop_${label}`, writeBatch().varAs("old", g().eWithLabel(label)).varAs("gone", g().dropEdgeById(EdgeRef.var("old"))).returning([]))
  const owner = () => g().nWithLabelWhere("User", SourcePredicate.eq("userId", OWNER)).where(tenantScope).limit(1)

  // 1. owner
  const documents = yield* rowsOf("model_documents", g().nWithLabel("SourceDocument").where(tenantScope).project([idAs()]), Schema.Struct({ id: NodeId }))
  yield* Console.log(`owner: User(${OWNER}) -OWNS-> ${documents.length} documents`)
  if (apply) {
    yield* dropAll("OWNS")
    yield* helix.write("model_user", writeBatch().varAs("found", owner())
      .varAsIf("made", BatchCondition.varEmpty("found"), g().addN("User", { tenant_id: TENANT, userId: OWNER, name: OWNER_NAME, createdAt: Expr.datetime() })).returning([]))
    for (const page of Arr.chunksOf(documents, 500)) {
      yield* helix.write("model_owns", page.reduce((batch, doc, index) => batch.varAs(`o${index}`, g().n(NodeRef.var("user")).addE("OWNS", doc.id, { tenant_id: TENANT })),
        writeBatch().varAs("user", owner())).returning([]))
    }
  }

  // 2. profile: GLM writes, from what Jev marked as about the owner and dense
  const mine = yield* rowsOf("model_owner_passages", g().nWithLabelWhere("Chunk", Predicate.and([Predicate.eq("aboutOwner", true), Predicate.gte("salience", 0.99)])).where(tenantScope)
    .limit(400).project(fields("chunkId", "title", "content")), Schema.Struct({ chunkId: Schema.String, title: Schema.optional(Schema.NullOr(Schema.String)), content: Schema.String }))
  yield* Console.log(`profile: ${mine.length} dense passages about the owner → 1 LLM call over the first 16`)
  if (apply && mine.length) {
    const sources = mine.slice(0, 16)
    const profile = yield* models.generate({
      name: "profile",
      system: `You write the profile of ${OWNER_NAME} for their own knowledge base, from passages about them. "static": durable facts (who they are, roles, companies, people they work with, skills, constraints) as short factual sentences. "dynamic": what they are currently working on, deciding, or worried about. Only what the passages support; no advice; no flattery. 8-20 items each.`,
      prompt: sources.map((row) => `=== ${row.title ?? ""}\n${row.content.slice(0, 1800)}`).join("\n\n"),
      schema: Profile,
    })
    yield* helix.write("model_profile_drop", writeBatch().varAs("old", g().nWithLabel("UserProfile").where(tenantScope).drop()).returning([]))
    yield* helix.write("model_profile", writeBatch().varAs("user", owner())
      .varAs("profile", g().addN("UserProfile", {
        tenant_id: TENANT, userId: OWNER, profileId: `${TENANT}:${OWNER}`, static: [...profile.static], dynamic: [...profile.dynamic],
        sources: sources.map((row) => row.chunkId), updatedAt: Expr.datetime(),
      }))
      .varAs("edge", g().n(NodeRef.var("user")).addE("HAS_PROFILE", NodeRef.var("profile"), { tenant_id: TENANT })).returning([]))
    yield* Console.log(`   static: ${profile.static.length} · dynamic: ${profile.dynamic.length} — e.g. ${profile.static.slice(0, 2).join(" | ")}`)
  }

  // 3. category hierarchy: one GLM call
  const leaves = yield* leafCategories
  yield* Console.log(`categories: ${leaves.length} leaves → parents by 1 LLM call`)
  if (apply) {
    const tree = yield* models.generate({
      name: "category_parents",
      system: "Group these categories of a personal knowledge base under 6 to 10 parent categories. Every category goes under exactly one parent, spelled exactly as given. A parent has a short name and a one-sentence description.",
      prompt: leaves.map((leaf) => `${leaf.name}: ${leaf.description}`).join("\n"),
      schema: Tree,
    })
    yield* helix.write("model_parents_drop", writeBatch().varAs("old", g().nWithLabelWhere("Category", Predicate.eq("isParent", true)).where(tenantScope).drop()).returning([]))
    let batch = writeBatch(), placed = 0
    tree.parents.forEach((parent, index) => {
      batch = batch.varAs(`p${index}`, g().addN("Category", { tenant_id: TENANT, categoryKey: `${TENANT}:parent:${normalise(parent.name)}`, name: parent.name, description: parent.description, isParent: true }))
      parent.children.filter((child) => leaves.some((leaf) => leaf.name === child)).forEach((child, c) => {
        placed++
        batch = batch
          .varAs(`p${index}c${c}`, g().nWithLabelWhere("Category", SourcePredicate.eq("categoryKey", `${TENANT}:${normalise(child)}`)).where(tenantScope).limit(1))
          .varAsIf(`p${index}e${c}`, BatchCondition.varNotEmpty(`p${index}c${c}`), g().n(NodeRef.var(`p${index}`)).addE("PARENT_OF", NodeRef.var(`p${index}c${c}`), { tenant_id: TENANT }))
      })
    })
    yield* helix.write("model_parents", batch.returning([]))
    yield* Console.log(`   ${tree.parents.length} parents, ${placed}/${leaves.length} leaves placed: ${tree.parents.map((parent) => `${parent.name}(${parent.children.length})`).join(" · ")}`)
  }

  // 4. RELATES_TO typed by Jev. Settling a new document types its crosslinks as it writes them; this types the rest.
  // Verdicts are cached in the backup, so a dry run judges once and --yes reuses them.
  const typedLog = join(dir, "relate-types.jsonl")
  const related = (yield* rowsOf("model_relations", g().nWithLabel("Chunk").where(tenantScope).outE("RELATES_TO").valueMap(["$id", "$from", "$to", "confidence"]),
    Schema.Struct({ $id: NodeId, $from: NodeId, $to: NodeId, confidence: Schema.optional(Schema.NullOr(Schema.Finite)) })))
    .filter((edge) => (edge.confidence ?? 0) >= Floors.mention)
  const typedBefore = new Set((yield* readJsonl(typedLog, RelationType)).map((row) => row.edge))
  const untyped = related.filter((edge) => !typedBefore.has(edge.$id))
  yield* Console.log(`relations: ${related.length} RELATES_TO pairs · ${untyped.length} to type → ${untyped.length} Jev requests`)
  yield* Effect.forEach(untyped, (edge) =>
    Effect.gen(function*() {
      const texts = new Map((yield* rowsOf("model_pair", g().n([edge.$from, edge.$to]).project([idAs(), ...fields("title", "content")]),
        Schema.Struct({ id: NodeId, title: Schema.optional(Schema.NullOr(Schema.String)), content: Schema.optional(Schema.NullOr(Schema.String)) })))
        .map((row) => [row.id, `${row.title ?? ""}\n${row.content ?? ""}`]))
      const outcomes = yield* models.decide(RelationKind, { a: texts.get(edge.$from) ?? "", b: texts.get(edge.$to) ?? "" })
      const answer = Result.isSuccess(outcomes.type) ? outcomes.type.success : undefined
      yield* fs.writeFileString(typedLog, JSON.stringify({ edge: edge.$id, a: edge.$from, b: edge.$to, type: answer?.label ?? "related", confidence: answer?.confidence ?? 0 }) + "\n", { flag: "a" })
    }), { concurrency: 16, discard: true })
  const standingRelations = new Set(related.map((edge) => edge.$id))
  const types = (yield* readJsonl(typedLog, RelationType)).filter((row) => standingRelations.has(row.edge) && row.type !== "related")
  yield* Console.log(`   ${JSON.stringify(Object.fromEntries(Object.entries(Arr.groupBy(yield* readJsonl(typedLog, RelationType), (row) => row.type)).map(([type, rows]) => [type, rows.length])))}`)
  if (apply && types.length) {
    const chunks = yield* fromBackup.chunks(dir)
    const perDocument = Arr.groupBy(chunks, (chunk) => chunk.documentId)
    const sizeOf = new Map(chunks.map((chunk) => [chunk.$id, perDocument[chunk.documentId]?.length ?? 0]))
    // the typed edge replaces the RELATES_TO it came from, so RELATES_TO keeps meaning "related, nothing more"
    for (const page of Arr.chunksOf(types, 500)) {
      const batch = page.reduce((out, row, index) => {
        const [from, to, label] = row.type === "same_event"
          ? ((sizeOf.get(row.a) ?? 0) >= (sizeOf.get(row.b) ?? 0) ? [row.a, row.b, "EXTENDS"] as const : [row.b, row.a, "EXTENDS"] as const)
          : row.type === "a_is_newer" ? [row.a, row.b, "UPDATES"] as const : [row.b, row.a, "UPDATES"] as const
        return out.varAs(`t${index}`, g().n(from).addE(label, to, { tenant_id: TENANT, confidence: row.confidence, by: "jev" }))
      }, writeBatch().varAs("gone", g().dropEdgeById(page.map((row) => row.edge))))
      yield* helix.write("model_typed_relations", batch.returning([]))
    }
    yield* Console.log(`   ${types.length} RELATES_TO edges retyped as EXTENDS/UPDATES`)
  }

  // 5. entity ↔ entity, counted from the standing links
  const entities = new Map((yield* fromBackup.entities(dir)).map((entity) => [entity.$id, entity]))
  const standing = standingLinks(yield* fromBackup.chunkMentions(dir)).filter((edge) => entities.has(edge.$to))
  const heldBy = new Map<NodeId, number>()
  for (const edge of standing) heldBy.set(edge.$to, (heldBy.get(edge.$to) ?? 0) + 1)
  const together = new Map<string, { a: NodeId; b: NodeId; passages: number }>()
  for (const edges of Object.values(Arr.groupBy(standing, (edge) => String(edge.$from)))) {
    const unique = [...new Set(edges.map((edge) => edge.$to))].sort((x, y) => x - y)
    for (let i = 0; i < unique.length; i++) {
      for (let j = i + 1; j < unique.length; j++) {
        const key = `${unique[i]}>${unique[j]}`, entry = together.get(key) ?? { a: unique[i]!, b: unique[j]!, passages: 0 }
        entry.passages++
        together.set(key, entry)
      }
    }
  }
  // kept when the two share 3+ passages AND that is a real share of the rarer one's passages (a hub is next to everything)
  const strong = [...together.values()]
    .map((pair) => ({ ...pair, share: pair.passages / Math.min(heldBy.get(pair.a)!, heldBy.get(pair.b)!) }))
    .filter((pair) => pair.passages >= 3 && pair.share >= 0.3)
    .sort((x, y) => y.passages - x.passages)
  yield* Console.log(`associations: ${together.size} co-occurring entity pairs · ${strong.length} strong (3+ shared passages, ≥30% of the rarer one's) — e.g. ${strong.slice(0, 6).map((pair) => `${entities.get(pair.a)!.name}~${entities.get(pair.b)!.name}(${pair.passages})`).join(" · ")}`)
  if (apply) {
    yield* dropAll("ASSOCIATED_WITH")
    for (const page of Arr.chunksOf(strong, 1000)) {
      yield* helix.write("model_associations", page.reduce((batch, pair, index) =>
        batch.varAs(`a${index}`, g().n(pair.a).addE("ASSOCIATED_WITH", pair.b, { tenant_id: TENANT, passages: pair.passages, share: Number(pair.share.toFixed(2)) })), writeBatch()).returning([]))
    }
  }
  if (!apply) yield* Console.log("dry run (relation types were judged and cached) — add --yes to write")
})
