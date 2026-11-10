// Ingest and the two-stage linking pipeline: a document enters the graph already clean.
//   link   Helix shortlists ≤30 candidate entities per chunk; Jev answers category, salience, owner, boilerplate,
//          "something discussed that is not in the list", and one yes/no per candidate. Candidate names appear only
//          in the questions, never in the passage state. A mention is written at confidence ≥ Floors.mention.
//   name   only for chunks Jev flagged: GLM proposes names, Jev vets each (discusses, about, node-worthy).
//   settle second reading of every link, document-wide entities → one document link, crosslinks to other documents.
import { createHash } from "node:crypto"
import { join } from "node:path"
import {
  BatchCondition, EdgeRef, Expr, g, NodeRef, Predicate, PropertyProjection, PropertyValue, readBatch, SourcePredicate, writeBatch,
} from "@helix-db/helix-db"
import {
  Array as Arr, Clock, Console, Context, Deferred, Effect, FileSystem, Layer, Option, Queue, Ref, Result, Schema,
} from "effect"
import { type AiError, Decision } from "effect/ai"
import { Models } from "./ai.ts"
import { Settings } from "./config.ts"
import {
  Category, categoryKeyOf, ChunkId, chunkIdOf, chunksOf, Doc, DocumentId, ENTITY_KINDS, EntityKey, entityKeyOf, EntityKind, EntityRow, entityNameProps,
  Floors, HUB, LINK_VERSION, MentionEdge, type NodeId, NodeId as NodeIdSchema, normalise, OWNER, OWNER_NAME, passageHash, TENANT,
} from "./domain.ts"
import { Helix, type HelixError } from "./helix.ts"
import { chunksOfDocument, documentById, documentStream, fields, idAs, noFilters, rowsOf, tenantScope, Vars } from "./queries.ts"

export interface LinkReport {
  readonly chunks: number
  readonly mentions: number
  /** chunks Jev flagged as discussing something unnamed */
  readonly flagged: number
}
export interface NameReport {
  readonly chunks: number
  readonly named: number
  readonly mentions: number
  readonly rejected: number
  readonly failures: number
}
export interface SettleReport {
  readonly links: number
  readonly refuted: number
  readonly documentWide: number
  readonly crosslinks: number
}

export class NoTaxonomy extends Schema.TaggedError<NoTaxonomy>()("NoTaxonomy", { message: Schema.String }) {}
export class IngestFailed extends Schema.TaggedError<IngestFailed>()("IngestFailed", { failed: Schema.Number }) {}

/** Passages a Jev request carries. Measured: with names kept out of the state, 8 a request decide like 1. */
export const PACK = 6
const JEV_TOKEN_BUDGET = 19_000
const SHARED_TOKENS = 1_600
const CANDIDATES = 30
const NEIGHBOURS = 8
const LINK_PAGE = 48
const LINK_PAGES_IN_FLIGHT = 6
const NAMING_PAGE = 12
const CHUNKS_PER_WRITE = 150
/** well under Helix's 512 text-indexed mutations per write */
const MAX_CHUNKS_PER_WRITE = 60
const PROGRESS_EVERY_MILLIS = 30_000

const tokensOf = (text: string) => Math.ceil(text.length / 3.6)
const clock = Effect.map(Clock.currentTimeMillis, (now) => new Date(now).toTimeString().slice(0, 8))

// ---------------------------------------------------------------- Jev questions

type Question = Decision.Probability | Decision.Classify<string> | Decision.Rate<string>
type Questions = Record<string, Question>
const noul = (instructions: string): Question => Decision.probability({ instructions })

/** Reads a settled decision; an unanswered question reads as "no". */
const readAnswers = (outcomes: Decision.Outcomes<Questions>) => {
  const answer = (key: string) => {
    const outcome = outcomes[key]
    return outcome && Result.isSuccess(outcome) ? outcome.success : undefined
  }
  return {
    probability: (key: string) => {
      const found = answer(key)
      return found && "probability" in found ? found.probability : 0
    },
    choice: (key: string) => {
      const found = answer(key)
      return found && "label" in found && !("rating" in found) ? { label: found.label, confidence: found.confidence ?? 0 } : undefined
    },
    rating: (key: string) => {
      const found = answer(key)
      return found && "rating" in found && typeof found.rating === "number" ? found.rating : 0
    },
  }
}

const DEFINITIONS = {
  discusses:
    "a passage discusses something when it gives information about it. When the passage IS a list that carries the information — a tracklist or setlist, a line-up, the participants of a meeting, a bibliography — every item on it is discussed. A name that only appears in passing, among navigation or promotional links, or in a signature is not discussed. A person who is only speaking, being addressed, or credited as author or host is not thereby discussed — they are discussed when the passage tells you something ABOUT them (what they did, decided, own, are responsible for, think, or are like)",
  boilerplate: "sponsor reads, calls to subscribe, link lists, legal footers, navigation, greetings and sign-offs",
  about_owner: `about ${OWNER_NAME} themselves: their own life, work, decisions, plans, money, or the people they personally deal with — not general or third-party material`,
  salience: "how much lasting, reusable information the passage holds",
}
const VET_DEFINITIONS = {
  discusses: DEFINITIONS.discusses,
  node_worthy:
    "the name identifies one particular thing: a person (a first name alone is fine), an organisation, a product, a project, a place, an event, or a concept that has a recognised name of its own (such as 'value-based pricing', 'AEO', 'virtual assistant'). Obscure or private things count. NOT node_worthy: a descriptive phrase that merely summarises what the passage says (such as 'creating a diverse population', 'three phase approach', 'learning from mistakes')",
}
// `about` is the test for famous names dropped in passing ("everything you'd learn from Harvard"): Jev scores those 0.9+
// on `discusses` and 0.06-0.4 here, while tracklist items and real subjects stay high (36 + 20 hand-labelled links).
const ABOUT = (name: string, p: number) =>
  `${name} is one of the things p${p} is about — not merely a name used as an example, a comparison, a simile, or a tool mentioned on the way. (When the passage is itself a tracklist, line-up, participant list or bibliography, each item on it counts as something the passage is about.)`

const JudgeState = Schema.Struct({
  definitions: Schema.Record(Schema.String, Schema.String),
  categories: Schema.Record(Schema.String, Schema.String),
  passages: Schema.Record(Schema.String, Schema.Struct({ document: Schema.String, kind: Schema.String, text: Schema.String })),
})
const VetState = Schema.Struct({
  definitions: Schema.Record(Schema.String, Schema.String),
  passages: Schema.Record(Schema.String, Schema.Struct({ document: Schema.String, text: Schema.String })),
})
const PairState = Schema.Struct({ a: Schema.String, b: Schema.String })
const Crosslink = Decision.make({
  input: PairState,
  decisions: {
    related: Decision.probability({
      instructions:
        "passages a and b are about the same specific matter — the same deal, person's situation, product decision, technique or idea — so that someone reading one should be pointed to the other. Sharing a broad theme is not enough",
    }),
    type: Decision.classify({
      instructions: "How passages a and b relate",
      criteria: {
        same_event: "two records of the very same meeting, call, talk or document",
        a_is_newer: "a states a later version of facts, plans or decisions that b also states",
        b_is_newer: "b states a later version of facts, plans or decisions that a also states",
        related: "about the same specific matter, but none of the above",
      },
    }),
  },
})

// ---------------------------------------------------------------- taxonomy

/** The taxonomy's leaves (parents group; passages are filed under leaves only). Fails when there is no taxonomy. */
export const leafCategories: Effect.Effect<ReadonlyArray<Category>, HelixError | NoTaxonomy, Helix> = Effect.gen(function*() {
  const rows = yield* rowsOf(
    "taxonomy",
    g().nWithLabel("Category").where(tenantScope).project(fields("name", "description", "isParent")),
    Schema.Struct({ name: Schema.String, description: Schema.String, isParent: Schema.optional(Schema.NullOr(Schema.Boolean)) }),
  )
  const leaves = rows.filter((row) => !row.isParent).map((row) => new Category({ name: row.name, description: row.description }))
  if (!leaves.length) return yield* new NoTaxonomy({ message: "no taxonomy yet: the Category nodes are missing" })
  return leaves
})

const Taxonomy = Schema.Struct({ categories: Schema.Array(Schema.Struct({ name: Schema.String, description: Schema.String })) })

// Jev chooses among options it is given, so the options have to exist first. Drafting a taxonomy is writing, and it
// happens once: GLM sees what the corpus holds and proposes the coarse topics. Fine-grained topics are entities.
/** `exo taxonomy --seed`: GLM drafts 30-45 categories from the corpus; `yes` replaces the stored taxonomy with them. */
export const seedTaxonomy = Effect.fn("seedTaxonomy")(function*(options: { readonly yes: boolean }) {
  const helix = yield* Helix
  const models = yield* Models
  const kinds = yield* rowsOf("taxonomy_kinds", g().nWithLabel("SourceDocument").where(tenantScope).groupCount("kind"),
    Schema.Struct({ count: Schema.Finite, kind: Schema.optional(Schema.NullOr(Schema.String)) }))
  const titles = yield* rowsOf("taxonomy_titles", g().nWithLabel("SourceDocument").where(tenantScope).project(fields("title", "kind")),
    Schema.Struct({ title: Schema.optional(Schema.NullOr(Schema.String)), kind: Schema.optional(Schema.NullOr(Schema.String)) }))
  const topics = yield* rowsOf("taxonomy_topics", g().nWithLabel("SourceDocument").where(tenantScope).outE("MENTIONS").groupCount("entityKey"),
    Schema.Struct({ count: Schema.Finite, entityKey: Schema.optional(Schema.NullOr(Schema.String)) }))
  const every = Math.max(1, Math.floor(titles.length / 450))
  const sample = titles.filter((_, index) => index % every === 0).map((row) => `[${row.kind || "note"}] ${row.title ?? ""}`)
  const topTopics = topics
    .map((row) => ({ key: row.entityKey ?? "", count: row.count }))
    .filter((row) => row.key.split(":")[1] === "topic")
    .sort((a, b) => b.count - a.count).slice(0, 250)
    .map((row) => `${row.key.split(":").slice(2).join(":")} (${row.count})`)
  const answer = yield* models.generate({
    name: "taxonomy",
    system: `You are designing the top-level topic taxonomy for one person's knowledge base. You are shown its document kinds, a sample of titles, and its most-mentioned topics. Propose 30 to 45 categories that together cover everything, each clearly distinct from the others. name: 1-3 lower-case words. description: one plain sentence saying what belongs there, concrete enough that a classifier could decide from it. Cover the owner's personal and work material as well as the reference material. Do not include an "other" category.`,
    prompt: `DOCUMENT KINDS\n${kinds.map((row) => `${row.kind || "note"}: ${row.count}`).join("\n")}\n\nSAMPLE TITLES\n${sample.join("\n")}\n\nMOST-MENTIONED TOPICS\n${topTopics.join(", ")}`,
    schema: Taxonomy,
  })
  yield* Effect.forEach(answer.categories, (category) => Console.log(`${category.name.padEnd(26)} ${category.description}`), { discard: true })
  if (!options.yes) return yield* Console.log(`\n${answer.categories.length} categories proposed — re-run with --yes to store them`)
  yield* helix.ensureIndexes
  yield* helix.write("store_taxonomy", answer.categories.reduce((batch, category, index) =>
    batch.varAs(`c${index}`, g().addN("Category", {
      tenant_id: TENANT, categoryKey: categoryKeyOf(category.name), name: category.name.toLowerCase().trim(), description: category.description,
    })), writeBatch().varAs("old", g().nWithLabel("Category").where(tenantScope).drop())).returning([]))
  yield* Console.log(`\nstored ${answer.categories.length} categories`)
})

// ---------------------------------------------------------------- link (Helix proposes, Jev decides)

const Candidate = Schema.Struct({ id: NodeIdSchema, name: Schema.String, kind: Schema.String, entityKey: EntityKey })
type Candidate = typeof Candidate.Type
interface Passage {
  readonly chunkId: ChunkId
  readonly title: string
  readonly kind: string
  readonly source: string
  readonly content: string
  readonly candidates: ReadonlyArray<Candidate>
}
interface Judged {
  readonly passage: Passage
  readonly mentions: ReadonlyArray<Candidate & { readonly confidence: number }>
  readonly category: string
  readonly categoryConfidence: number
  readonly salience: number
  readonly aboutOwner: boolean
  readonly boilerplate: boolean
  readonly needsNaming: boolean
}

// One Jev request = up to PACK passages from any documents. Everything shared is stated once in the state (definitions,
// the taxonomy); each passage carries only its own text; the questions are short and refer to the state by id.
const judgePack = Effect.fnUntraced(function*(pack: ReadonlyArray<Passage>, categories: ReadonlyArray<Category>) {
  const models = yield* Models
  const labels: Record<string, string> = {
    ...Object.fromEntries(categories.map((category) => [normalise(category.name), category.name])),
    other: "none of the categories fits",
  }
  const questions: Questions = {}
  pack.forEach((passage, p) => {
    questions[`p${p}_category`] = Decision.classify({ instructions: `Topic of p${p}, by the category definitions in state`, criteria: labels })
    questions[`p${p}_salience`] = Decision.rate({
      instructions: `Salience of p${p}`,
      criteria: ["none: filler only", "a little useful detail", "useful, loosely stated", "dense, specific, reusable"],
    })
    questions[`p${p}_owner`] = noul(`p${p} is about_owner`)
    questions[`p${p}_boilerplate`] = noul(`p${p} is boilerplate`)
    questions[`p${p}_missing`] = noul(
      `p${p} discusses specific named people, organisations, products, projects or places other than these: ${passage.candidates.map((entity) => entity.name).join("; ") || "(none)"}`,
    )
    passage.candidates.forEach((entity, e) => {
      questions[`p${p}_e${e}`] = noul(`p${p} discusses ${entity.name}`)
    })
  })
  // A passage's state is its text and nothing that could be mistaken for it: candidate names listed here made Jev
  // "find" them in the passage. Names appear only in the questions.
  const state = {
    definitions: DEFINITIONS,
    categories: Object.fromEntries(categories.map((category) => [normalise(category.name), category.description])),
    passages: Object.fromEntries(pack.map((passage, p) => [`p${p}`, { document: passage.title, kind: passage.kind || "note", text: passage.content }])),
  }
  const answers = readAnswers(yield* models.decide(Decision.make({ input: JudgeState, decisions: questions }), state))
  return pack.map((passage, p): Judged => {
    const category = answers.choice(`p${p}_category`)
    return {
      passage,
      mentions: passage.candidates
        .map((entity, e) => ({ ...entity, confidence: answers.probability(`p${p}_e${e}`) }))
        .filter((entity) => entity.confidence >= Floors.mention),
      category: category?.label ?? "other",
      categoryConfidence: category?.confidence ?? 0,
      salience: Math.min(1, Math.max(0, answers.rating(`p${p}_salience`) / 3)),
      aboutOwner: answers.probability(`p${p}_owner`) >= 0.5,
      boilerplate: answers.probability(`p${p}_boilerplate`) >= 0.5,
      needsNaming: answers.probability(`p${p}_missing`) >= 0.5,
    }
  })
})

// Per chunk: its judged properties and its edges, with Jev's probability as the edge confidence. Candidates came out of
// Helix with their node ids, so edges point straight at them: no lookups, no get-or-create.
const buildLinkBatch = (judged: ReadonlyArray<Judged>) => {
  let batch = writeBatch()
  const categoryVar = new Map<string, string>()
  let edge = 0
  judged.forEach((item, index) => {
    const me = `k${index}`
    batch = batch
      .varAs(me, g().nWithLabelWhere("Chunk", SourcePredicate.eq("chunkId", item.passage.chunkId)).where(tenantScope).limit(1))
      // a re-link replaces the previous judgement rather than stacking on it
      // (.drop() is a node mutation and silently does nothing on an edge stream: edges go by reference)
      .varAs(`${me}m`, g().n(NodeRef.var(me)).outE("MENTIONS")).varAs(`${me}md`, g().dropEdgeById(EdgeRef.var(`${me}m`)))
      .varAs(`${me}c`, g().n(NodeRef.var(me)).outE("IN_CATEGORY")).varAs(`${me}cd`, g().dropEdgeById(EdgeRef.var(`${me}c`)))
      .varAs(`${me}p`, g().n(NodeRef.var(me))
        .setProperty("category", item.category).setProperty("salience", item.salience).setProperty("aboutOwner", item.aboutOwner)
        .setProperty("boilerplate", item.boilerplate).setProperty("needsNaming", item.needsNaming)
        .setProperty("documentKind", item.passage.kind).setProperty("source", item.passage.source)
        .setProperty("userId", OWNER).setProperty("linkVersion", LINK_VERSION).setProperty("linkedAt", Expr.datetime()))
    if (item.category !== "other") {
      let category = categoryVar.get(item.category)
      if (!category) {
        category = `c${categoryVar.size}`
        categoryVar.set(item.category, category)
        batch = batch.varAs(category, g().nWithLabelWhere("Category", SourcePredicate.eq("categoryKey", `${TENANT}:${item.category}`)).where(tenantScope).limit(1))
      }
      batch = batch.varAsIf(`l${edge++}`, BatchCondition.varNotEmpty(category),
        g().n(NodeRef.var(me)).addE("IN_CATEGORY", NodeRef.var(category), { tenant_id: TENANT, confidence: item.categoryConfidence }))
    }
    for (const entity of item.mentions) {
      batch = batch.varAs(`l${edge++}`,
        g().n(NodeRef.var(me)).addE("MENTIONS", entity.id, { tenant_id: TENANT, confidence: entity.confidence, entityKey: entity.entityKey, by: "jev" }))
    }
  })
  return batch.returning([])
}

interface WriteUnit {
  readonly judged: ReadonlyArray<Judged>
  readonly durable: boolean
  readonly done: Deferred.Deferred<void, HelixError>
}

/**
 * Helix commits one write at a time and each commit has a fixed durability cost, so whatever is judged by the time the
 * writer frees up goes into the same transaction (≤ MAX_CHUNKS_PER_WRITE chunks). One writer fiber for the whole run,
 * so concurrent documents of an ingest share commits too. A failed commit is retried unit by unit to isolate the bad one.
 */
export class LinkWriter extends Context.Service<LinkWriter, {
  readonly write: (judged: ReadonlyArray<Judged>, options: { readonly durable: boolean }) => Effect.Effect<void, HelixError>
}>()("exo/LinkWriter") {
  static readonly layer = Layer.effect(LinkWriter, Effect.gen(function*() {
    const helix = yield* Helix
    const ready = yield* Queue.unbounded<WriteUnit>()
    const send = (units: ReadonlyArray<WriteUnit>) =>
      helix.write("link_chunks", buildLinkBatch(units.flatMap((unit) => unit.judged)), { durable: units.some((unit) => unit.durable) })
    const commit = (units: ReadonlyArray<WriteUnit>) =>
      send(units).pipe(Effect.matchEffect({
        onSuccess: () => Effect.forEach(units, (unit) => Deferred.succeed(unit.done, undefined), { discard: true }),
        onFailure: (error) =>
          units.length === 1
            ? Deferred.fail(units[0]!.done, error)
            : Effect.forEach(units, (unit) => Effect.flatMap(Effect.exit(send([unit])), (exit) => Deferred.done(unit.done, exit)), { discard: true }),
      }))
    const groups = (units: ReadonlyArray<WriteUnit>) =>
      units.reduce<Array<Array<WriteUnit>>>((out, unit) => {
        const last = out.at(-1)
        if (last && last.reduce((sum, held) => sum + held.judged.length, 0) + unit.judged.length <= MAX_CHUNKS_PER_WRITE) last.push(unit)
        else out.push([unit])
        return out
      }, [])
    yield* Queue.takeAll(ready).pipe(
      Effect.flatMap((units) => Effect.forEach(groups(units), commit, { discard: true })),
      Effect.forever,
      Effect.forkScoped,
    )
    return LinkWriter.of({
      write: (judged, options) =>
        Effect.gen(function*() {
          const done = yield* Deferred.make<void, HelixError>()
          yield* Queue.offer(ready, { judged, durable: options.durable, done })
          return yield* Deferred.await(done)
        }),
    })
  }))
}

// In-memory pool for corpus-wide passes. Local Helix is slow and is also the writer, so every read a bulk pass can
// avoid is throughput. With a pool, Helix is asked one thing per passage (its nearest chunks: the vector index is the
// only part that cannot live here) and "what are those chunks and their documents linked to" is a map lookup.
// Judged passages update the pool, so later passages see them. A single-document ingest skips it.
export interface Pool {
  readonly linksOf: Map<NodeId, ReadonlyArray<Candidate>>
  readonly rosterOf: Map<string, ReadonlyArray<Candidate>>
}
export const loadPool = Effect.fn("loadPool")(function*() {
  const started = yield* Clock.currentTimeMillis
  const entityRows = yield* rowsOf("pool_entities", g().nWithLabel("Entity").where(tenantScope).valueMap(["$id", "name", "kind", "entityKey"]),
    Schema.Struct({ $id: NodeIdSchema, name: Schema.String, kind: Schema.String, entityKey: EntityKey }))
  const entities = new Map(entityRows.map((row): [NodeId, Candidate] => [row.$id, { id: row.$id, name: row.name, kind: row.kind, entityKey: row.entityKey }]))
  const Edge = Schema.Struct({ $from: NodeIdSchema, $to: NodeIdSchema })
  const chunkIds = (yield* rowsOf("pool_chunks", g().nWithLabel("Chunk").where(tenantScope).valueMap(["$id"]), Schema.Struct({ $id: NodeIdSchema })))
    .map((row) => row.$id)
  const links = yield* Effect.forEach(Arr.chunksOf(chunkIds, 1500), (page) => rowsOf("pool_links", g().n([...page]).outE("MENTIONS").valueMap(["$from", "$to"]), Edge))
  const linksOf = new Map<NodeId, ReadonlyArray<Candidate>>()
  for (const edge of links.flat()) {
    const entity = entities.get(edge.$to)
    if (entity) linksOf.set(edge.$from, [...(linksOf.get(edge.$from) ?? []), entity])
  }
  const documents = new Map((yield* rowsOf("pool_documents", g().nWithLabel("SourceDocument").where(tenantScope).valueMap(["$id", "documentId"]),
    Schema.Struct({ $id: NodeIdSchema, documentId: Schema.String }))).map((row) => [row.$id, row.documentId]))
  const rosterOf = new Map<string, ReadonlyArray<Candidate>>()
  for (const edge of yield* rowsOf("pool_rosters", g().nWithLabel("SourceDocument").where(tenantScope).outE("MENTIONS").valueMap(["$from", "$to"]), Edge)) {
    const entity = entities.get(edge.$to), documentId = documents.get(edge.$from)
    if (entity && documentId) rosterOf.set(documentId, [...(rosterOf.get(documentId) ?? []), entity])
  }
  const seconds = ((yield* Clock.currentTimeMillis) - started) / 1000
  yield* Console.log(`pool: ${entities.size} entities · links of ${linksOf.size} chunks · rosters of ${rosterOf.size} documents · ${seconds.toFixed(0)}s`)
  return { linksOf, rosterOf } satisfies Pool
})

const ChunkToLink = Schema.Struct({
  id: NodeIdSchema, chunkId: ChunkId, documentId: DocumentId, title: Schema.optional(Schema.String), content: Schema.optional(Schema.String),
  embedding: Schema.Array(Schema.Finite),
})
const Neighbour = Schema.Struct({ id: NodeIdSchema, documentId: Schema.String })

/** Packs fill a Jev request up to PACK passages or the token budget, whichever comes first. */
const packsOf = (passages: ReadonlyArray<Passage>) => {
  const packs: Array<Array<Passage>> = []
  let used = 0
  for (const passage of passages) {
    const cost = tokensOf(passage.content) + 130 + passage.candidates.length * 22
    const last = packs.at(-1)
    if (!last || last.length >= PACK || used + cost > JEV_TOKEN_BUDGET) {
      packs.push([passage])
      used = SHARED_TOKENS + cost
    } else {
      last.push(passage)
      used += cost
    }
  }
  return packs
}

/** chunk ids → judged and written. Pages keep memory flat; packs fill Jev's context. */
export const linkChunks = Effect.fn("linkChunks")(function*(input: {
  readonly chunkIds: ReadonlyArray<ChunkId>
  readonly documents: ReadonlyMap<DocumentId, Doc>
  readonly categories: ReadonlyArray<Category>
  /** corpus-wide passes: candidates come from memory instead of per-passage traversals */
  readonly pool?: Pool
  /** false during a bulk drain; the caller then ends with one durable write as the barrier */
  readonly durable?: boolean
  readonly onProgress?: (report: LinkReport) => Effect.Effect<void>
}): Effect.fn.Return<LinkReport, HelixError | AiError.AiError, Helix | Models | LinkWriter> {
  const helix = yield* Helix
  const writer = yield* LinkWriter
  const report = yield* Ref.make<LinkReport>({ chunks: 0, mentions: 0, flagged: 0 })
  const memory = input.pool
  const entityFields = [idAs(), ...fields("name", "kind", "entityKey")]

  const linkPage = Effect.fnUntraced(function*(page: ReadonlyArray<ChunkId>) {
    const chunks = (yield* rowsOf("chunks_to_link", g().nWithLabelWhere("Chunk", Predicate.isIn("chunkId", [...page])).where(tenantScope)
      .project([idAs(), ...fields("chunkId", "documentId", "title", "content", "embedding")]), ChunkToLink))
      .filter((chunk) => chunk.content?.trim())
    if (!chunks.length) return
    // Helix proposes. A bare entity name carries next to no context, so names are never compared to passages. The
    // passages most similar to this one are about certain entities: those, ranked by how many neighbours share them,
    // are the candidates. A chunk is its own nearest neighbour, so its document's roster is always in the pool.
    const neighbours = (embedding: ReadonlyArray<number>) => g().vectorSearchNodes("Chunk", "embedding", [...embedding], NEIGHBOURS, TENANT)
    const pools: Array<{ near: ReadonlyArray<Candidate>; roster: ReadonlyArray<Candidate>; own: ReadonlyArray<Candidate> }> = memory
      ? yield* Effect.map(
        helix.read("candidate_entities", chunks.reduce((batch, chunk, index) =>
          batch.varAs(`v${index}`, neighbours(chunk.embedding).project([idAs(), ...fields("documentId")])), readBatch())
          .returning(chunks.map((_, index) => `v${index}`)), Vars(Neighbour)),
        (found) => chunks.map((chunk, index) => {
          const near = found[`v${index}`] ?? []
          return {
            near: near.flatMap((row) => memory.linksOf.get(row.id) ?? []),
            roster: [...new Set(near.map((row) => row.documentId))].flatMap((documentId) => memory.rosterOf.get(documentId) ?? []),
            own: memory.linksOf.get(chunk.id) ?? [],
          }
        }),
      )
      : yield* Effect.map(
        helix.read("candidate_entities", chunks.reduce((batch, chunk, index) =>
          batch
            .varAs(`c${index}`, neighbours(chunk.embedding).out("MENTIONS").project(entityFields))   // what similar passages are about
            .varAs(`d${index}`, neighbours(chunk.embedding).in("HAS_CHUNK").dedup().out("MENTIONS").project(entityFields))   // their documents
            .varAs(`o${index}`, g().nWithLabelWhere("Chunk", SourcePredicate.eq("chunkId", chunk.chunkId)).where(tenantScope).out("MENTIONS").project(entityFields)),
          readBatch()).returning(chunks.flatMap((_, index) => [`c${index}`, `d${index}`, `o${index}`])), Vars(Candidate)),
        (found) => chunks.map((_, index) => ({ near: found[`c${index}`] ?? [], roster: found[`d${index}`] ?? [], own: found[`o${index}`] ?? [] })),
      )
    const passages = chunks.map((chunk, index): Passage => {
      const { near, roster, own } = pools[index]!
      const tally = new Map<string, { entity: Candidate; votes: number }>()
      for (const entity of [...near, ...roster, ...own]) {
        const entry = tally.get(entity.entityKey) ?? { entity, votes: 0 }
        entry.votes++
        tally.set(entity.entityKey, entry)
      }
      // what the chunk is already linked to is always re-judged (a --redo must not orphan an entity by leaving it out)
      const owned = new Set(own.map((entity) => entity.entityKey))
      const candidates = [...tally.values()]
        .sort((a, b) => Number(owned.has(b.entity.entityKey)) - Number(owned.has(a.entity.entityKey)) || b.votes - a.votes)
        .slice(0, Math.max(CANDIDATES, owned.size))
        .map((entry) => entry.entity)
      const doc = input.documents.get(chunk.documentId)
      return { chunkId: chunk.chunkId, title: doc?.title ?? chunk.title ?? "", kind: doc?.kind ?? "", source: doc?.source ?? "", content: chunk.content ?? "", candidates }
    })
    const nodeIdOf = new Map(chunks.map((chunk) => [chunk.chunkId, chunk.id]))
    yield* Effect.forEach(packsOf(passages), (pack) =>
      Effect.gen(function*() {
        const judged = yield* judgePack(pack, input.categories)
        if (memory) for (const item of judged) memory.linksOf.set(nodeIdOf.get(item.passage.chunkId)!, item.mentions)
        yield* writer.write(judged, { durable: input.durable ?? true })
        const next = yield* Ref.updateAndGet(report, (total) => ({
          chunks: total.chunks + judged.length,
          mentions: total.mentions + judged.reduce((sum, item) => sum + item.mentions.length, 0),
          flagged: total.flagged + judged.filter((item) => item.needsNaming).length,
        }))
        if (input.onProgress) yield* input.onProgress(next)
      }), { concurrency: "unbounded", discard: true })
  })

  yield* Effect.forEach(Arr.chunksOf(input.chunkIds, LINK_PAGE), linkPage, { concurrency: LINK_PAGES_IN_FLIGHT, discard: true })
  return yield* Ref.get(report)
})

// ---------------------------------------------------------------- name (GLM writes, only where Jev asked)

interface Verdict {
  readonly discusses: number
  readonly worthy: number
  readonly about: number
}
interface Proposal {
  readonly chunkId: string
  readonly title: string
  readonly content: string
  readonly names: ReadonlyArray<string>
}
const verdictKey = (chunkId: string, name: string) => `${chunkId}\u0000${name}`

// Every proposed name, judged in the passage it came from: is it really discussed there, is the passage about it, and
// does it deserve a node? Names are only in the questions, so passages can share a request.
const vetNames = Effect.fnUntraced(function*(proposals: ReadonlyArray<Proposal>) {
  const models = yield* Models
  const verdicts = new Map<string, Verdict>()
  yield* Effect.forEach(Arr.chunksOf(proposals.filter((proposal) => proposal.names.length), PACK), (pack) =>
    Effect.gen(function*() {
      const questions: Questions = {}
      pack.forEach((proposal, p) =>
        proposal.names.forEach((name, n) => {
          questions[`p${p}_d${n}`] = noul(`p${p} discusses ${name}`)
          questions[`p${p}_w${n}`] = noul(`"${name}", as used in p${p}, is node_worthy`)
          questions[`p${p}_a${n}`] = noul(ABOUT(name, p))
        }))
      const state = {
        definitions: VET_DEFINITIONS,
        passages: Object.fromEntries(pack.map((proposal, p) => [`p${p}`, { document: proposal.title, text: proposal.content }])),
      }
      const answers = readAnswers(yield* models.decide(Decision.make({ input: VetState, decisions: questions }), state))
      pack.forEach((proposal, p) =>
        proposal.names.forEach((name, n) =>
          verdicts.set(verdictKey(proposal.chunkId, name), {
            discusses: answers.probability(`p${p}_d${n}`),
            worthy: answers.probability(`p${p}_w${n}`),
            about: answers.probability(`p${p}_a${n}`),
          })))
    }), { concurrency: "unbounded", discard: true })
  return verdicts
})

const Naming = Schema.Struct({
  passages: Schema.Array(Schema.Struct({
    id: Schema.String,
    entities: Schema.Array(Schema.Struct({ name: Schema.String, kind: EntityKind })),
  })),
})
const NAMING_SYSTEM =
  `Each passage has an id and a list of entities it is ALREADY linked to. For each passage, list what it discusses that is NOT already in its list — and only what passes BOTH tests a second model will apply to every name you give:
(1) discussed: ${VET_DEFINITIONS.discusses}.
(2) node-worthy: ${VET_DEFINITIONS.node_worthy}.
Most passages add zero to three names; when in doubt leave it out — a rejected name is wasted work. Canonical names: the fullest form the text gives, no titles or honorifics, never two spellings of one thing. Topics are specific ("value-based pricing", "drop swap"), not generic ("business", "music"). For DJ material: artists and DJs are persons, labels and channels are organisations, gear and software are products, techniques and genres are topics; individual tracks are not entities, their artists are. A passage with nothing new gets an empty list. Return every passage id.`

const PageChunk = Schema.Struct({ chunkId: Schema.String, documentId: DocumentId, title: Schema.optional(Schema.String), content: Schema.String })
const NameRow = Schema.Struct({ name: Schema.String })

interface Named {
  readonly name: string
  readonly kind: EntityKind
}

// Stage 2. Naming is writing, so GLM does it, for flagged passages only, many per call, keyed by passage id. It is told
// what each passage is already linked to, so it names only what is new; Jev then vets every name in its passage, so an
// LLM link sits on the same confidence scale as any other.
export const nameChunks = Effect.fn("nameChunks")(function*(input: {
  readonly chunkIds: ReadonlyArray<ChunkId>
  readonly documents: ReadonlyMap<DocumentId, Doc>
}): Effect.fn.Return<NameReport, HelixError | AiError.AiError, Helix | Models | Settings> {
  const helix = yield* Helix
  const models = yield* Models
  const settings = yield* Settings
  const stats = yield* Ref.make({ chunks: 0, named: 0, mentions: 0, rejected: 0, failures: 0, notDiscussed: 0, notWorthy: 0 })

  const nameOnePage = Effect.fnUntraced(function*(page: ReadonlyArray<ChunkId>) {
    const data = yield* helix.read("passages_to_name", page.reduce(
      (batch, chunkId, index) => batch.varAs(`k${index}`, g().nWithLabelWhere("Chunk", Predicate.eq("chunkId", chunkId)).where(tenantScope).out("MENTIONS").project(fields("name"))),
      readBatch().varAs("chunks", g().nWithLabelWhere("Chunk", Predicate.isIn("chunkId", [...page])).where(tenantScope).project(fields("chunkId", "documentId", "title", "content"))),
    ).returning(["chunks", ...page.map((_, index) => `k${index}`)]), Vars(Schema.Unknown))
    const chunks = yield* Schema.decodeUnknownEffect(Schema.Array(PageChunk))(data.chunks ?? [])
    const known = new Map(yield* Effect.forEach(page, (chunkId, index) =>
      Effect.map(Schema.decodeUnknownEffect(Schema.Array(NameRow))(data[`k${index}`] ?? []), (rows) => [chunkId as string, rows.map((row) => row.name)] as const)))
    const titleOf = (chunk: typeof PageChunk.Type) => input.documents.get(chunk.documentId)?.title ?? chunk.title ?? ""
    const answer = yield* models.generate({
      name: "naming",
      system: NAMING_SYSTEM,
      prompt: chunks.map((chunk) => `=== ${chunk.chunkId} — ${titleOf(chunk)}\nalready linked: ${(known.get(chunk.chunkId) ?? []).join(", ") || "(nothing)"}\n${chunk.content}`).join("\n\n"),
      schema: Naming,
    })
    const named = new Map<EntityKey, Named>()
    const perChunk = new Map<string, ReadonlyArray<EntityKey>>()
    for (const row of answer.passages) {
      if (!known.has(row.id)) continue
      const keys = row.entities.filter((entity) => normalise(entity.name)).map((entity) => {
        const key = entityKeyOf(entity.kind, entity.name)
        named.set(key, entity)
        return key
      })
      perChunk.set(row.id, [...new Set(keys)])
    }
    const verdicts = yield* vetNames(chunks.filter((chunk) => perChunk.get(chunk.chunkId)?.length).map((chunk) => ({
      chunkId: chunk.chunkId, title: titleOf(chunk), content: chunk.content, names: perChunk.get(chunk.chunkId)!.map((key) => named.get(key)!.name),
    })))
    const confidenceOf = new Map<string, number>()
    let proposed = 0, notDiscussed = 0, notWorthy = 0
    for (const [chunkId, keys] of perChunk) {
      proposed += keys.length
      perChunk.set(chunkId, keys.filter((key) => {
        const entity = named.get(key)!
        const verdict = verdicts.get(verdictKey(chunkId, entity.name))
        const worthyFloor = entity.kind === "topic" ? Floors.topic : Floors.worthy
        if (!verdict || verdict.discusses < Floors.mention || verdict.worthy < worthyFloor || verdict.about < Floors.about) {
          if (verdict && verdict.discusses < Floors.mention) notDiscussed++
          if (verdict && verdict.worthy < worthyFloor) notWorthy++
          return false
        }
        confidenceOf.set(verdictKey(chunkId, key), verdict.discusses)
        return true
      }))
    }
    // One node per name: "Acme" the product and "Acme" the project are one thing filed twice (772 such
    // groups held 10k links on 2026-09-20). A name that already exists under ANY kind is reused under that kind.
    const variants = new Map<EntityKey, EntityKey>()
    for (const [key, entity] of named) for (const kind of ENTITY_KINDS) variants.set(entityKeyOf(kind, entity.name), key)
    const existing = variants.size
      ? yield* rowsOf("existing_names", g().nWithLabelWhere("Entity", Predicate.isIn("entityKey", [...variants.keys()])).where(tenantScope).project(fields("entityKey", "kind")),
        Schema.Struct({ entityKey: EntityKey, kind: EntityKind }))
      : []
    const existingKeys = new Set(existing.map((row) => row.entityKey))
    const rekey = new Map<EntityKey, { to: EntityKey; kind: EntityKind }>()
    for (const row of existing) {
      const from = variants.get(row.entityKey)!
      if (from !== row.entityKey && !rekey.has(from) && !existingKeys.has(from)) rekey.set(from, { to: row.entityKey, kind: row.kind })
    }
    for (const [from, { to, kind }] of rekey) {
      named.set(to, { ...named.get(from)!, kind })
      named.delete(from)
      for (const [chunkId, keys] of perChunk) {
        if (!keys.includes(from)) continue
        perChunk.set(chunkId, [...new Set(keys.map((key) => key === from ? to : key))])
        confidenceOf.set(verdictKey(chunkId, to), confidenceOf.get(verdictKey(chunkId, from))!)
      }
    }
    const kept = new Set([...perChunk.values()].flat())
    for (const key of [...named.keys()]) if (!kept.has(key)) named.delete(key)
    // A new name is linked to the passage it came from, so it becomes a candidate for every similar passage after it.
    const entries = [...named.entries()]
    const varOf = new Map(entries.map(([key], index) => [key, `n${index}`]))
    let batch = entries.reduce((out, [key, entity], index) =>
      out
        .varAs(`n${index}x`, g().nWithLabelWhere("Entity", SourcePredicate.eq("entityKey", key)).where(tenantScope).limit(1))
        .varAsIf(`n${index}n`, BatchCondition.varEmpty(`n${index}x`),
          g().addN("Entity", { tenant_id: TENANT, entityKey: key, name: entity.name.trim(), kind: entity.kind, entityType: entity.kind }))
        .varAsIf(`s${index}`, BatchCondition.varEmpty(`n${index}x`), g().addN("EntityName", entityNameProps({ ...entity, entityKey: key }))),
      writeBatch())
    let edge = 0
    page.forEach((chunkId, index) => {
      const me = `k${index}`
      batch = batch.varAs(me, g().nWithLabelWhere("Chunk", SourcePredicate.eq("chunkId", chunkId)).where(tenantScope).limit(1))
        .varAs(`${me}p`, g().n(NodeRef.var(me)).setProperty("needsNaming", false).setProperty("namedAt", Expr.datetime()))
      for (const key of perChunk.get(chunkId) ?? []) {
        for (const suffix of ["x", "n"]) {
          batch = batch.varAsIf(`l${edge++}`, BatchCondition.varNotEmpty(`${varOf.get(key)}${suffix}`),
            g().n(NodeRef.var(me)).addE("MENTIONS", NodeRef.var(`${varOf.get(key)}${suffix}`), {
              tenant_id: TENANT, confidence: confidenceOf.get(verdictKey(chunkId, key))!, entityKey: key, by: "llm+jev",
            }))
        }
      }
    })
    yield* helix.write("name_chunks", batch.returning([]))
    const total = yield* Ref.updateAndGet(stats, (total) => ({
      ...total,
      chunks: total.chunks + page.length,
      named: total.named + entries.length,
      mentions: total.mentions + kept.size,
      rejected: total.rejected + proposed - kept.size,
      notDiscussed: total.notDiscussed + notDiscussed,
      notWorthy: total.notWorthy + notWorthy,
    }))
    const usage = yield* models.usage
    yield* Console.log(`[${yield* clock}] named ${total.chunks}/${input.chunkIds.length} passages · ${total.named} names kept, ${total.rejected} rejected by Jev (${total.notDiscussed} not discussed, ${total.notWorthy} not node-worthy) · ${usage.generations} llm calls`)
  })

  // A page the model cannot answer (it returns nothing on very long inputs) is halved and retried, down to one passage.
  const namePage = (page: ReadonlyArray<ChunkId>): Effect.Effect<void, never, Helix | Models> =>
    nameOnePage(page).pipe(Effect.catch((error) =>
      page.length === 1
        ? Effect.andThen(Ref.update(stats, (total) => ({ ...total, failures: total.failures + 1 })), Console.error(`FAILED naming ${page[0]}: ${String(error).slice(0, 200)}`))
        : Effect.andThen(namePage(page.slice(0, Math.ceil(page.length / 2))), namePage(page.slice(Math.ceil(page.length / 2))))))

  yield* Effect.forEach(Arr.chunksOf(input.chunkIds, NAMING_PAGE), namePage, { concurrency: settings.writerConcurrency, discard: true })
  const { chunks, named, mentions, rejected, failures } = yield* Ref.get(stats)
  return { chunks, named, mentions, rejected, failures }
})

// ---------------------------------------------------------------- settle (one document, read live)

// Everything the corpus-wide hygiene passes do, for ONE freshly linked document: (1) every link gets its second reading
// (mean of two ≥ floor, and the passage must be ABOUT it); (2) an entity on at least half of its passages moves to one
// document link; (3) its passages are crosslinked to passages of other documents sharing ≥2 walkable entities, typed by Jev.
export const settleDocument = Effect.fn("settleDocument")(function*(documentId: DocumentId): Effect.fn.Return<SettleReport, HelixError | AiError.AiError, Helix | Models> {
  const helix = yield* Helix
  const models = yield* Models
  const links = yield* rowsOf("settle_links", chunksOfDocument(documentId).outE("MENTIONS").valueMap(["$id", "$from", "$to", "confidence", "by", "entityKey"]), MentionEdge)
  if (!links.length) return { links: 0, refuted: 0, documentWide: 0, crosslinks: 0 }
  const passages = yield* rowsOf("settle_passages", chunksOfDocument(documentId).project([idAs(), ...fields("chunkId", "title", "content")]),
    Schema.Struct({ id: NodeIdSchema, chunkId: Schema.String, title: Schema.optional(Schema.String), content: Schema.String }))
  const entityRows = yield* Effect.forEach(Arr.chunksOf([...new Set(links.map((edge) => edge.$to))], 500), (ids) =>
    rowsOf("settle_entities", g().n([...ids]).valueMap(["$id", "entityKey", "name", "kind"]), EntityRow))
  const names = new Map(entityRows.flat().map((row) => [row.$id, row]))
  // (1) second reading
  const byChunk = Arr.groupBy(links.filter((edge) => names.has(edge.$to)), (edge) => String(edge.$from))
  const verdicts = yield* vetNames(passages.filter((row) => byChunk[String(row.id)]).map((row) => ({
    chunkId: row.chunkId, title: row.title ?? "", content: row.content, names: byChunk[String(row.id)]!.map((edge) => names.get(edge.$to)!.name),
  })))
  const chunkIdOfNode = new Map(passages.map((row) => [row.id, row.chunkId]))
  const refuted = links.filter((edge) => {
    const verdict = verdicts.get(verdictKey(chunkIdOfNode.get(edge.$from) ?? "", names.get(edge.$to)?.name ?? ""))
    return !verdict || (edge.confidence + verdict.discusses) / 2 < Floors.mention || verdict.about < Floors.about
  })
  const gone = new Set(refuted.map((edge) => edge.$id))
  const standing = links.filter((edge) => !gone.has(edge.$id))
  // (2) document-wide facts
  const perEntity = Arr.groupBy(standing, (edge) => String(edge.$to))
  const wide = passages.length >= 4 ? Object.values(perEntity).filter((edges) => edges.length / passages.length >= 0.5) : []
  let batch = writeBatch().varAs("document", documentById(documentId).limit(1))
  wide.forEach((edges, index) => {
    const entity = edges[0].$to, entityKey = names.get(entity)!.entityKey
    batch = batch
      .varAs(`h${index}`, g().n(NodeRef.var("document")).outE("MENTIONS").where(Predicate.eq("entityKey", entityKey)))
      .varAsIf(`a${index}`, BatchCondition.varEmpty(`h${index}`), g().n(NodeRef.var("document")).addE("MENTIONS", entity, {
        tenant_id: TENANT, entityKey, confidence: Math.max(...edges.map((edge) => edge.confidence)), by: "consolidated", passages: edges.length,
      }))
    for (const edge of edges) gone.add(edge.$id)
  })
  if (gone.size) batch = batch.varAs("dropped", g().dropEdgeById([...gone]))
  yield* helix.write("settle_document", batch.returning([]))
  // (3) crosslinks
  const specific = links.filter((edge) => !gone.has(edge.$id))
  const mine = new Set(passages.map((row) => row.id))
  const targets = [...new Set(specific.map((edge) => edge.$to))]
  const partners = new Map<string, { from: NodeId; to: NodeId; weight: number; via: Array<string> }>()
  for (const group of Arr.chunksOf(targets, 40)) {
    const held = yield* helix.read("settle_reach", group.reduce((reach, entity, index) =>
      reach.varAs(`r${index}`, g().n(entity).in("MENTIONS").hasLabel("Chunk").limit(HUB + 1).project([idAs()])), readBatch())
      .returning(group.map((_, index) => `r${index}`)), Vars(Schema.Struct({ id: NodeIdSchema })))
    group.forEach((entity, index) => {
      const holders = (held[`r${index}`] ?? []).map((row) => row.id)
      if (holders.length < 2 || holders.length > HUB) return
      for (const edge of specific.filter((link) => link.$to === entity)) {
        for (const other of holders.filter((holder) => !mine.has(holder))) {
          const key = `${edge.$from}>${other}`
          const entry = partners.get(key) ?? { from: edge.$from, to: other, weight: 0, via: [] }
          entry.weight += 1 / Math.log2(1 + holders.length)
          entry.via.push(names.get(entity)!.name)
          partners.set(key, entry)
        }
      }
    })
  }
  const candidates = Object.values(Arr.groupBy([...partners.values()].filter((entry) => entry.via.length >= 2), (entry) => String(entry.from)))
    .flatMap((list) => [...list].sort((x, y) => y.weight - x.weight).slice(0, 3))
  const PairChunk = Schema.Struct({ id: NodeIdSchema, title: Schema.optional(Schema.String), content: Schema.String })
  const crosslinked = yield* Effect.forEach(candidates, (pair) =>
    Effect.gen(function*() {
      const chunks = new Map((yield* rowsOf("settle_pair", g().n([pair.from, pair.to]).project([idAs(), ...fields("title", "content", "documentId")]), PairChunk))
        .map((chunk) => [chunk.id, chunk]))
      const a = chunks.get(pair.from), b = chunks.get(pair.to)
      if (!a || !b) return false
      const outcomes = yield* models.decide(Crosslink, { a: `${a.title ?? ""}\n${a.content}`, b: `${b.title ?? ""}\n${b.content}` })
      const related = Result.isSuccess(outcomes.related) ? outcomes.related.success.probability : 0
      if (related < Floors.mention) return false
      const type = Result.isSuccess(outcomes.type) ? outcomes.type.success.label : "related"
      const [label, from, to] = type === "a_is_newer" ? ["UPDATES", pair.from, pair.to] as const
        : type === "b_is_newer" ? ["UPDATES", pair.to, pair.from] as const
        : type === "same_event" ? ["EXTENDS", pair.from, pair.to] as const
        : ["RELATES_TO", pair.from, pair.to] as const
      yield* helix.write("settle_crosslink", writeBatch()
        .varAs("edge", g().n(from).addE(label, to, { tenant_id: TENANT, confidence: related, via: pair.via, by: "jev" })).returning([]))
      return true
    }), { concurrency: 16 })
  const crosslinks = crosslinked.filter(Boolean).length
  yield* Console.log(`   settled ${documentId}: ${links.length} links → ${links.length - gone.size} specific + ${wide.length} document-wide · ${refuted.length} refuted on second reading · ${crosslinks} crosslinks`)
  return { links: links.length, refuted: refuted.length, documentWide: wide.length, crosslinks }
})

// ---------------------------------------------------------------- commands

const documentMap = Effect.fnUntraced(function*(kinds: ReadonlyArray<string>) {
  const stream = yield* documentStream({ ...noFilters, kinds })
  const rows = yield* rowsOf("document_map", stream.project(fields("documentId", "title", "kind", "source")), Doc)
  return new Map(rows.map((doc) => [doc.documentId, doc]))
})

const minutesSince = (started: number) => Effect.map(Clock.currentTimeMillis, (now) => ((now - started) / 60000).toFixed(1))

/** Durable barrier: a bulk drain may be acknowledged before its commit reaches object storage; this write cannot be. */
const settleBarrier = Effect.gen(function*() {
  const helix = yield* Helix
  yield* helix.write("settle", writeBatch().varAs("settled", g().nWithLabel("Category").where(tenantScope).limit(1).setProperty("settledAt", Expr.datetime())).returning([]))
})

/** `exo link`: stage 1 over chunks not yet linked at LINK_VERSION (or all with `redo`). Helix proposes, Jev decides. */
export const link = Effect.fn("link")(function*(options: {
  readonly kinds: ReadonlyArray<string>
  readonly limit: Option.Option<number>
  readonly dryRun: boolean
  readonly redo: boolean
}) {
  const helix = yield* Helix
  const models = yield* Models
  const documents = yield* documentMap(options.kinds)
  const states = yield* rowsOf("chunk_states", g().nWithLabel("Chunk").where(tenantScope).project(fields("chunkId", "documentId", "linkVersion")),
    Schema.Struct({ chunkId: ChunkId, documentId: DocumentId, linkVersion: Schema.optional(Schema.NullOr(Schema.Finite)) }))
  const due = states.filter((chunk) => documents.has(chunk.documentId) && (options.redo || (chunk.linkVersion ?? 0) !== LINK_VERSION)).map((chunk) => chunk.chunkId)
  const pending = Option.match(options.limit, { onNone: () => due, onSome: (limit) => due.slice(0, limit) })
  yield* Console.log(`${states.length} chunks, ${pending.length} to judge → ~${Math.ceil(pending.length / PACK)} Jev requests (${PACK} passages each, ≈ $${(pending.length * 2300 * 0.042 / 1e6).toFixed(2)}), 0 LLM calls`)
  if (options.dryRun || !pending.length) return
  yield* helix.ensureIndexes
  const categories = yield* leafCategories
  const started = yield* Clock.currentTimeMillis
  const lastLog = yield* Ref.make(0)
  const pool = yield* loadPool()
  const report = yield* linkChunks({
    chunkIds: pending, documents, categories, pool, durable: false,
    onProgress: (progress) => Effect.gen(function*() {
      const now = yield* Clock.currentTimeMillis
      if (now - (yield* Ref.get(lastLog)) < PROGRESS_EVERY_MILLIS) return
      yield* Ref.set(lastLog, now)
      const usage = yield* models.usage
      yield* Console.log(`[${yield* clock}] ${progress.chunks}/${pending.length} chunks · ${usage.decisions} jev (${Math.round(usage.decisionTokens / Math.max(1, usage.decisions))} tok/call) · ${progress.flagged} flagged`)
    }),
  })
  yield* settleBarrier
  yield* Console.log(`DONE ${report.chunks} chunks judged, ${report.mentions} mentions, ${report.flagged} flagged for naming, 0 new names in ${yield* minutesSince(started)} min`)
})

/** `exo name`: stage 2 over chunks flagged `needsNaming`. */
export const name = Effect.fn("name")(function*(options: {
  readonly kinds: ReadonlyArray<string>
  readonly limit: Option.Option<number>
  readonly dryRun: boolean
}) {
  const helix = yield* Helix
  const documents = yield* documentMap(options.kinds)
  const flagged = yield* rowsOf("flagged_chunks", g().nWithLabelWhere("Chunk", Predicate.eq("needsNaming", true)).where(tenantScope).project(fields("chunkId", "documentId")),
    Schema.Struct({ chunkId: ChunkId, documentId: DocumentId }))
  const due = flagged.filter((chunk) => documents.has(chunk.documentId)).map((chunk) => chunk.chunkId)
  const pending = Option.match(options.limit, { onNone: () => due, onSome: (limit) => due.slice(0, limit) })
  yield* Console.log(`${pending.length} passages flagged by Jev → ~${Math.ceil(pending.length / NAMING_PAGE)} LLM calls`)
  if (options.dryRun || !pending.length) return
  yield* helix.ensureIndexes
  const started = yield* Clock.currentTimeMillis
  const report = yield* nameChunks({ chunkIds: pending, documents })
  yield* Console.log(`DONE ${report.chunks} chunks named, ${report.mentions} mentions, ${report.named} new names, ${report.rejected} rejected, ${report.failures} failed in ${yield* minutesSince(started)} min`)
})

const SourceFile = Schema.Struct({
  id: Schema.optional(Schema.String),
  customId: Schema.optional(Schema.String),
  title: Schema.optional(Schema.String),
  content: Schema.optional(Schema.String),
  raw: Schema.optional(Schema.String),
  source: Schema.optional(Schema.String),
  url: Schema.optional(Schema.String),
  metadata: Schema.optional(Schema.Record(Schema.String, Schema.Unknown)),
})
const decodeSourceFile = Schema.decodeUnknownEffect(Schema.fromJsonString(SourceFile))
type Outcome = "ok" | "empty" | "exists" | "copy" | "failed"
const text = (value: unknown) => value === undefined || value === null ? undefined : String(value)

const ingestOne = Effect.fnUntraced(function*(path: string, filename: string, options: { readonly kind: Option.Option<string>; readonly replace: boolean }, categories: ReadonlyArray<Category>) {
  const helix = yield* Helix
  const models = yield* Models
  const settings = yield* Settings
  const fs = yield* FileSystem.FileSystem
  const source = yield* decodeSourceFile(yield* fs.readFileString(path))
  const body = source.content ?? source.raw ?? ""
  if (!body.trim()) return "empty" satisfies Outcome
  const metadata = source.metadata ?? {}
  const documentId = DocumentId.make(source.id ?? filename.replace(/\.json$/, ""))
  const title = (source.title ?? text(metadata.title) ?? "").slice(0, 300)
  const kind = Option.getOrElse(options.kind, () => text(metadata.type) ?? text(metadata.kind) ?? "")
  const sourceName = text(metadata.source) ?? source.source ?? ""
  const url = text(metadata.url) ?? source.url ?? ""
  if (!options.replace) {
    const found = yield* rowsOf("document_exists", documentById(documentId).limit(1).project(fields("documentId")), Schema.Struct({ documentId: Schema.String }))
    if (found.length) return "exists" satisfies Outcome
  }
  const chunks = chunksOf(body), hashes = chunks.map(passageHash), distinct = new Set(hashes)
  // the same document under another id is not a new document (a third of the corpus once was exactly that)
  const twins = yield* rowsOf("copy_check", g().nWithLabelWhere("Chunk", Predicate.isIn("contentHash", [...distinct])).where(tenantScope).project(fields("documentId", "contentHash")),
    Schema.Struct({ documentId: Schema.String, contentHash: Schema.String }))
  const covered = new Map<string, Set<string>>()
  for (const row of twins.filter((row) => row.documentId !== documentId)) covered.set(row.documentId, (covered.get(row.documentId) ?? new Set()).add(row.contentHash))
  const twin = [...covered].find(([, seen]) => seen.size === distinct.size)
  if (twin) {
    yield* Console.log(`${documentId.padEnd(22)} copy of ${twin[0]} — skipped`)
    return "copy" satisfies Outcome
  }
  const vectors = yield* models.embed(chunks)
  const addChunk = (batch: ReturnType<typeof writeBatch>, index: number) => batch
    .varAs(`c${index}`, g().addN("Chunk", {
      tenant_id: TENANT, userId: OWNER, chunkId: chunkIdOf(documentId, index), documentId, title, chunkIndex: index, ordinal: index, documentKind: kind, source: sourceName,
      content: chunks[index]!, contentHash: hashes[index]!, embedding: PropertyValue.f32Array([...vectors[index]!]), embeddingModel: `openai:${settings.embeddingModel}`,
      createdAt: Expr.datetime(),
    }))
    .varAs(`h${index}`, g().n(NodeRef.var("document")).addE("HAS_CHUNK", NodeRef.var(`c${index}`), { tenant_id: TENANT, ordinal: index }))
  const indexes = chunks.map((_, index) => index)
  const [first = [], ...rest] = Arr.chunksOf(indexes, CHUNKS_PER_WRITE)
  const opening = (options.replace
    ? writeBatch().varAs("oldChunks", chunksOfDocument(documentId).drop()).varAs("oldDocument", documentById(documentId).drop())
    : writeBatch())
    .varAs("document", g().addN("SourceDocument", {
      tenant_id: TENANT, userId: OWNER, documentId, customId: source.customId ?? "", title, source: sourceName, sourceType: sourceName, kind, url, uri: url,
      chunkCount: chunks.length, checksum: createHash("sha256").update(body).digest("hex").slice(0, 24), status: "indexed", createdAt: Expr.datetime(),
      metadata: Object.fromEntries(Object.entries(metadata).map(([key, value]) =>
        [key, typeof value === "string" || typeof value === "number" || typeof value === "boolean" ? value : JSON.stringify(value) ?? ""])),
    }))
  yield* helix.write("ingest_document", first.reduce(addChunk, opening).returning([]))
  for (const more of rest) {   // books
    yield* helix.write("ingest_chunks", more.reduce(addChunk, writeBatch().varAs("document", documentById(documentId).limit(1))).returning([]))
  }
  const chunkIds = indexes.map((index) => chunkIdOf(documentId, index))
  const documents = new Map([[documentId, new Doc({ documentId, title, kind, source: sourceName, url })]])
  yield* linkChunks({ chunkIds, documents, categories })
  const flagged = yield* rowsOf("flagged_in_document",
    g().nWithLabelWhere("Chunk", Predicate.and([Predicate.eq("documentId", documentId), Predicate.eq("needsNaming", true)])).where(tenantScope).project(fields("chunkId")),
    Schema.Struct({ chunkId: ChunkId }))
  if (flagged.length) yield* nameChunks({ chunkIds: flagged.map((row) => row.chunkId), documents })
  yield* settleDocument(documentId)
  yield* Console.log(`${documentId.padEnd(22)} ${String(chunks.length).padStart(3)} chunks  ${title.slice(0, 70)}`)
  return "ok" satisfies Outcome
})

/** `exo ingest <dir>`: every *.json source document in dir → document + chunks + link + name + settle. */
export const ingest = Effect.fn("ingest")(function*(options: {
  readonly dir: string
  readonly kind: Option.Option<string>
  readonly replace: boolean
}) {
  const helix = yield* Helix
  const fs = yield* FileSystem.FileSystem
  yield* helix.ensureIndexes
  const categories = yield* leafCategories
  const files = (yield* fs.readDirectory(options.dir)).filter((file) => file.endsWith(".json")).sort()
  const outcomes = yield* Effect.forEach(files, (file) =>
    ingestOne(join(options.dir, file), file, options, categories).pipe(
      Effect.catch((error) => Effect.as(Console.error(`FAILED ${file}: ${String(error).slice(0, 200)}`), "failed" satisfies Outcome)),
    ), { concurrency: 16 })
  const counts = Object.fromEntries(Object.entries(Arr.groupBy(outcomes, (outcome) => outcome)).map(([outcome, list]) => [outcome, list.length]))
  yield* Console.log(JSON.stringify(counts))
  if (counts.failed) return yield* new IngestFailed({ failed: counts.failed })
})
