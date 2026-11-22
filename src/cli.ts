import { join } from "node:path"
import { Console, Effect, Option } from "effect"
import { Argument, Command, Flag } from "effect/cli"
import { ARTICLE_TEXT_SECTION, readBookmarks } from "./bookmarks.ts"
import { Settings } from "./config.ts"
import { Models } from "./ai.ts"
import { checked, draftOntology, extractCorpus, readCorpus, readJSON, readRecords, saveJSON, sourcesIn } from "./extraction.ts"
import * as Graph from "./graph.ts"
import { canonicalJSON, digest, hasExtraction, Ontology, pendingRecord, validateOntology } from "./knowledge.ts"

const json = Flag.Boolean("json").pipe(Flag.withDefault(false))
const selected = Flag.String("generation").pipe(Flag.optional)
const directory = Argument.Directory("directory", { mustExist: true })
const filters = {
  kinds: Flag.String("kind").pipe(Flag.atLeast(0)), sources: Flag.String("source").pipe(Flag.atLeast(0)), documents: Flag.String("doc").pipe(Flag.atLeast(0)),
  categories: Flag.String("category").pipe(Flag.atLeast(0)), minSalience: Flag.Finite("min-salience").pipe(Flag.optional),
}
const print = (value: unknown) => Console.log(JSON.stringify(value, null, 2))
const generationOf = (id: Option.Option<string>) => Option.match(id, { onSome: Effect.succeed, onNone: () => Graph.activeGeneration })
const recordsDir = Effect.gen(function*() {
  const settings = yield* Settings
  const tenant = process.env.EXO_TENANT
  if (!tenant) return yield* Effect.fail(new Error("EXO_TENANT is required"))
  return join(settings.root, ".exo", digest(tenant), "extractions")
})

const extractCommand = Command.make("extract", { directory }, ({ directory }) => Effect.gen(function*() {
  const { records, directory: runDirectory } = yield* extractCorpus(yield* sourcesIn(directory), yield* recordsDir)
  yield* print({ documents: records.length, entities: records.reduce((sum, record) => sum + record.extraction.entities.length, 0), records: runDirectory })
})).pipe(Command.withDescription("Extract observations without ontology mapping; retain text and evidence"))

const rebalanceCommand = Command.make("rebalance", { directory, ontology: Flag.File("ontology", { mustExist: true }) }, ({ directory, ontology }) => Effect.gen(function*() {
  const definition = yield* readJSON(ontology, Ontology)
  yield* checked(() => validateOntology(definition))
  const { records } = yield* extractCorpus(yield* sourcesIn(directory), yield* recordsDir)
  yield* Graph.build(records, definition)
})).pipe(Command.withDescription("Rebuild the supplied complete corpus into an isolated generation; does not activate"))

const ingestCommand = Command.make("ingest", { directory, ontology: Flag.String("ontology").pipe(Flag.optional), replace: Flag.Boolean("replace").pipe(Flag.withDefault(false)) }, input => Effect.gen(function*() {
  const current = yield* Graph.currentGeneration()
  const previous = current ? { generation: yield* Graph.loadGeneration(current), records: yield* Graph.generationRecords(current) } : undefined
  const old = previous?.records ?? []
  const sources = yield* sourcesIn(input.directory)
  for (const source of sources) {
    const previous = old.find(record => record.source.id === source.id)
    if (previous && previous.sourceHash !== digest(source.content) && !input.replace) return yield* Effect.fail(new Error(`${source.id} already exists with different text; use --replace`))
  }
  const definition = Option.isSome(input.ontology) ? yield* readJSON(input.ontology.value, Ontology)
    : previous?.generation.ontology
  if (!definition) return yield* Effect.fail(new Error("The first ingest requires --ontology <file>"))
  const { records: fresh } = yield* extractCorpus(sources, yield* recordsDir)
  const combined = new Map(old.map(record => [record.source.id, record]))
  fresh.forEach(record => combined.set(record.source.id, record))
  const next = yield* Graph.build([...combined.values()], definition, { previous })
  yield* Graph.activate(next.id, current ?? null)
})).pipe(Command.withDescription("Extract, map and verify documents, then activate the merged corpus"))

const mapCommand = Command.make("map", { directory, ontology: Flag.File("ontology", { mustExist: true }) }, ({ directory, ontology }) => Effect.gen(function*() {
  const { records, retired } = yield* readCorpus(directory)
  yield* Graph.build(records, yield* readJSON(ontology, Ontology), { retired })
})).pipe(Command.withDescription("Map saved extractions into an isolated graph generation"))

const draftCommand = Command.make("draft", { directory, output: Flag.String("output") }, input => Effect.gen(function*() {
  const definition = yield* draftOntology(yield* readRecords(input.directory))
  yield* saveJSON(input.output, definition)
  yield* print({ path: input.output, types: definition.types.length, relations: definition.relations.length, status: "draft" })
}))
const reviseCommand = Command.make("revise", { ontology: Flag.File("ontology", { mustExist: true }), generation: selected }, input => Effect.gen(function*() {
  yield* print(yield* Graph.revise(yield* generationOf(input.generation), yield* readJSON(input.ontology, Ontology)))
})).pipe(Command.withDescription("Replay revised rules on frozen observations without extraction calls or writes"))
const ontologyCommand = Command.make("ontology", { generation: selected }, input => Effect.gen(function*() {
  yield* print((yield* Graph.loadGeneration(yield* generationOf(input.generation))).ontology)
})).pipe(Command.withSubcommands([draftCommand, reviseCommand]))

const activateCommand = Command.make("activate", { generation: Argument.String("generation") }, input => Effect.gen(function*() {
  yield* Graph.activate(input.generation, (yield* Graph.currentGeneration()) ?? null)
}))
const statsCommand = Command.make("stats", { generation: selected, json }, input => Effect.gen(function*() {
  const { ontology, mappingVerdicts, ...counts } = yield* Graph.loadGeneration(yield* generationOf(input.generation))
  yield* print({ ...counts, documentsMapped: counts.documentsMapped ?? counts.documents, documentsPending: counts.documentsPending ?? 0,
    retired: counts.retired ?? [], ontology: ontology.name, mappingHeld: mappingVerdicts.filter(verdict => verdict.verdict !== "supported").length })
}))
const docsCommand = Command.make("docs", { generation: selected, json, ...filters }, input => Effect.gen(function*() {
  const records = yield* Graph.generationRecords(yield* generationOf(input.generation), {
    ...input, minSalience: Option.getOrUndefined(input.minSalience),
  })
  yield* print(records.map(record => {
    const { id, title, kind, source, provenance, category, salience } = record.source
    return { id, title, kind, source, sourceHash: record.sourceHash, provenance, category, salience, pending: !hasExtraction(record) }
  }))
}))
const showCommand = Command.make("show", { document: Argument.String("document"), generation: selected, json }, input => Effect.gen(function*() {
  const [record] = yield* Graph.generationRecords(yield* generationOf(input.generation), { kinds: [], sources: [], documents: [input.document] })
  if (!record) return yield* Effect.fail(new Error(`No document ${input.document}`))
  yield* (input.json ? print(record) : Console.log(`${record.source.title}\n\n${record.source.content}`))
}))
const entitiesCommand = Command.make("entities", { generation: selected, match: Flag.String("match").pipe(Flag.optional), json }, input => Effect.gen(function*() {
  yield* print(yield* Graph.entities(yield* generationOf(input.generation), Option.getOrUndefined(input.match)))
}))
const walkCommand = Command.make("walk", { name: Argument.String("name"), generation: selected, json }, input => Effect.gen(function*() {
  const generation = yield* generationOf(input.generation)
  const matches = yield* Graph.entities(generation, input.name)
  if (!matches.length) return yield* Effect.fail(new Error(`No entity named ${input.name}`))
  yield* print({ entities: matches, claims: yield* Graph.claims(generation, matches.map(entity => entity.entityId)) })
})).pipe(Command.withDescription("Supported incoming and outgoing claims, with direction and source evidence"))
const excerpt = (passage: string, query: string) => {
  const text = passage.replace(/\s+/g, " ").trim()
  if (text.length <= 500) return text
  const folded = text.toLocaleLowerCase()
  const exact = folded.indexOf(query.toLocaleLowerCase())
  const word = query.toLocaleLowerCase().match(/[\p{L}\p{N}]{3,}/gu)?.findLast(term => folded.includes(term))
  const match = exact >= 0 ? exact : word ? folded.indexOf(word) : 0
  const start = Math.max(0, match - 100)
  return `${start ? "…" : ""}${text.slice(start, start + 500)}${start + 500 < text.length ? "…" : ""}`
}

const searchCommand = Command.make("search", {
  query: Argument.String("query").pipe(Argument.variadic()), generation: selected, top: Flag.Int("top").pipe(Flag.withDefault(10)),
  entity: Flag.String("entity").pipe(Flag.optional), json,
  ...filters,
  mode: Flag.Literals("mode", ["hybrid", "text", "vector", "graph"]).pipe(Flag.withDefault("hybrid")),
}, input => Effect.gen(function*() {
  if (input.top <= 0) return yield* Effect.fail(new Error("--top must be positive"))
  const generation = Option.isSome(input.generation) ? input.generation.value : yield* Graph.currentGeneration()
  const query = input.query.join(" ").trim()
  const hits = yield* Graph.search(generation, query, input.top, Option.getOrUndefined(input.entity), {
    ...input, minSalience: Option.getOrUndefined(input.minSalience),
  })
  if (input.json) return yield* print({ hits })
  if (!hits.length) return yield* Console.log("Nothing found.")
  for (const hit of hits) {
    yield* Console.log(`\n${hit.title} [${hit.documentId}]  Score ${hit.score.toFixed(3)}`)
    if (hit.entities.length) yield* Console.log(`   About ${hit.entities.join(" · ")}`)
    if (hit.passage) yield* Console.log(`   ${excerpt(hit.passage, query)}`)
  }
}))

const bookmarksCommand = Command.make("bookmarks", {
  jsonl: Argument.String("jsonl").pipe(Argument.optional),
  labels: Flag.String("labels").pipe(Flag.optional),
  skip: Flag.String("skip").pipe(Flag.optional),
  ftdb: Flag.String("ftdb").pipe(Flag.optional),
  replace: Flag.Boolean("replace").pipe(Flag.withDefault(false)),
  graph: Flag.Boolean("graph").pipe(Flag.withDefault(false)),
  dryRun: Flag.Boolean("dry-run").pipe(Flag.withDefault(false)),
  limit: Flag.Int("limit").pipe(Flag.optional),
}, input => Effect.gen(function*() {
  if (Option.isSome(input.limit) && (input.limit.value < 1 || !input.graph)) {
    return yield* Effect.fail(new Error("--limit requires --graph and a positive count"))
  }
  if (input.graph && input.replace) return yield* Effect.fail(new Error("Run bookmarks --replace before bookmarks --graph"))
  const { rows, retired, skipped } = yield* readBookmarks(input)
  const current = yield* Graph.currentGeneration()
  const canExtract = Boolean(process.env.OPENROUTER_API_KEY?.trim())
  const onlyBookmarks = input.dryRun || (input.graph && !canExtract)
  const records = current ? yield* Graph.generationRecords(current, onlyBookmarks ? { kinds: ["x_bookmark"], sources: [], documents: [] } : undefined) : []
  const combined = new Map(records.map(record => [record.source.id, record]))
  if (input.graph) {
    const pending = records.filter(record => !hasExtraction(record) && rows.get(record.source.id)?.article)
    const selected = Option.isSome(input.limit) ? pending.slice(0, input.limit.value) : pending
    if (!current || input.dryRun || !selected.length || !canExtract) {
      yield* print({ pending: pending.length, selected: selected.length, extracted: 0 })
      return
    }
    const generation = yield* Graph.loadGeneration(current)
    const { records: extracted } = yield* extractCorpus(selected.map(record => record.source), yield* recordsDir)
    for (const record of extracted) combined.set(record.source.id, record)
    const next = yield* Graph.build([...combined.values()], generation.ontology, { previous: { generation, records } })
    yield* Graph.activate(next.id, generation.id)
    return
  }

  let added = 0, replaced = 0, updatedMetadata = 0
  for (const { source, enriched, hasArticleText } of rows.values()) {
    const old = combined.get(source.id)
    const canReplace = input.replace && enriched && (hasArticleText || !old?.source.content.includes(ARTICLE_TEXT_SECTION))
    if (!old || (canReplace && old.source.content !== source.content)) {
      combined.set(source.id, pendingRecord(source))
      if (old) replaced++
      else added++
      continue
    }
    const updated = {
      ...old.source,
      ...(input.replace ? { title: source.title, url: source.url } : {}),
      ...(source.category !== undefined && (input.replace || old.source.category === undefined) ? { category: source.category } : {}),
      ...(source.salience !== undefined && (input.replace || old.source.salience === undefined) ? { salience: source.salience } : {}),
    }
    if (canonicalJSON(updated) === canonicalJSON(old.source)) continue
    combined.set(source.id, { ...old, source: updated })
    updatedMetadata++
  }
  const retirements = records.filter(record => record.source.kind === "x_bookmark" && retired.has(record.source.id))
    .map(record => ({ documentId: record.source.id, reason: "tombstoned bookmark" }))
  for (const { documentId } of retirements) combined.delete(documentId)
  yield* print({ bookmarks: rows.size, skipped, added, replaced, updatedMetadata, retired: retirements.length, dryRun: input.dryRun })
  if (input.dryRun || !(added || replaced || updatedMetadata || retirements.length)) return
  if (!current) return yield* Effect.fail(new Error("Activate a corpus generation before importing bookmarks"))
  const generation = yield* Graph.loadGeneration(current)
  const next = yield* Graph.build([...combined.values()], generation.ontology, {
    previous: { generation, records }, retired: retirements,
  })
  yield* Graph.activate(next.id, current)
})).pipe(Command.withDescription("Import X bookmarks and extract pending article bookmarks"))

export const exo = Command.make("exo").pipe(Command.withDescription("Source observations, versioned ontology mapping and evidence-backed knowledge graphs"),
  Command.withSubcommands([extractCommand, mapCommand, rebalanceCommand, ingestCommand, ontologyCommand, activateCommand, statsCommand, docsCommand, showCommand, entitiesCommand, walkCommand, searchCommand, bookmarksCommand]))
export const run = (argv: ReadonlyArray<string>) => Effect.gen(function*() {
  yield* Command.runWith(exo, { version: "3.0.0" })(argv)
  const usage = yield* (yield* Models).usage
  if (usage.generations || usage.decisions || usage.embedded) yield* Console.error(JSON.stringify({ usage }))
})
