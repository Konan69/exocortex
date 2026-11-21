import { join } from "node:path"
import { Console, Effect, Option } from "effect"
import { Argument, Command, Flag } from "effect/cli"
import { Settings } from "./config.ts"
import { Models } from "./ai.ts"
import { checked, draftOntology, extractCorpus, readJSON, readRecords, saveJSON, sourcesIn } from "./extraction.ts"
import * as Graph from "./graph.ts"
import { digest, Ontology, validateOntology } from "./knowledge.ts"

const json = Flag.Boolean("json").pipe(Flag.withDefault(false))
const selected = Flag.String("generation").pipe(Flag.optional)
const directory = Argument.Directory("directory", { mustExist: true })
const filters = { kinds: Flag.String("kind").pipe(Flag.atLeast(0)), sources: Flag.String("source").pipe(Flag.atLeast(0)), documents: Flag.String("doc").pipe(Flag.atLeast(0)) }
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
  const old = current ? yield* Graph.generationRecords(current) : []
  const sources = yield* sourcesIn(input.directory)
  for (const source of sources) {
    const previous = old.find(record => record.source.id === source.id)
    if (previous && previous.sourceHash !== digest(source.content) && !input.replace) return yield* Effect.fail(new Error(`${source.id} already exists with different text; use --replace`))
  }
  const definition = Option.isSome(input.ontology) ? yield* readJSON(input.ontology.value, Ontology)
    : current ? (yield* Graph.loadGeneration(current)).ontology : undefined
  if (!definition) return yield* Effect.fail(new Error("The first ingest requires --ontology <file>"))
  const { records: fresh } = yield* extractCorpus(sources, yield* recordsDir)
  const combined = new Map(old.map(record => [record.source.id, record]))
  fresh.forEach(record => combined.set(record.source.id, record))
  const next = yield* Graph.build([...combined.values()], definition)
  yield* Graph.activate(next.id, current ?? null)
})).pipe(Command.withDescription("Extract, map and verify documents, then activate the merged corpus"))

const mapCommand = Command.make("map", { directory, ontology: Flag.File("ontology", { mustExist: true }) }, ({ directory, ontology }) => Effect.gen(function*() {
  yield* Graph.build(yield* readRecords(directory), yield* readJSON(ontology, Ontology))
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
  yield* print({ ...counts, ontology: ontology.name, mappingHeld: mappingVerdicts.filter(verdict => verdict.verdict !== "supported").length })
}))
const docsCommand = Command.make("docs", { generation: selected, json, ...filters }, input => Effect.gen(function*() {
  const records = yield* Graph.generationRecords(yield* generationOf(input.generation))
  yield* print(records.filter(({ source }) => (!input.kinds.length || input.kinds.includes(source.kind)) && (!input.sources.length || input.sources.includes(source.source)) && (!input.documents.length || input.documents.includes(source.id)))
    .map(record => ({ id: record.source.id, title: record.source.title, kind: record.source.kind, source: record.source.source, sourceHash: record.sourceHash, provenance: record.source.provenance })))
}))
const showCommand = Command.make("show", { document: Argument.String("document"), generation: selected, json }, input => Effect.gen(function*() {
  const record = (yield* Graph.generationRecords(yield* generationOf(input.generation))).find(record => record.source.id === input.document)
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
const searchCommand = Command.make("search", {
  query: Argument.String("query").pipe(Argument.variadic()), generation: selected, top: Flag.Int("top").pipe(Flag.withDefault(10)),
  entity: Flag.String("entity").pipe(Flag.optional), json,
  ...filters,
  mode: Flag.Literals("mode", ["hybrid", "text", "vector", "graph"]).pipe(Flag.withDefault("hybrid")),
}, input => Effect.gen(function*() {
  if (input.top <= 0) return yield* Effect.fail(new Error("--top must be positive"))
  const generation = Option.isSome(input.generation) ? input.generation.value : yield* Graph.currentGeneration()
  yield* print({ hits: yield* Graph.search(generation, input.query.join(" "), input.top, Option.getOrUndefined(input.entity), input) })
}))

export const exo = Command.make("exo").pipe(Command.withDescription("Source observations, versioned ontology mapping and evidence-backed knowledge graphs"),
  Command.withSubcommands([extractCommand, mapCommand, rebalanceCommand, ingestCommand, ontologyCommand, activateCommand, statsCommand, docsCommand, showCommand, entitiesCommand, walkCommand, searchCommand]))
export const run = (argv: ReadonlyArray<string>) => Effect.gen(function*() {
  yield* Command.runWith(exo, { version: "3.0.0" })(argv)
  const usage = yield* (yield* Models).usage
  if (usage.generations || usage.decisions || usage.embedded) yield* Console.error(JSON.stringify({ usage }))
})
