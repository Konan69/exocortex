import { Console, Effect } from "effect"
import { Argument, Command, Flag } from "effect/cli"
import { Models } from "./ai.ts"
import { importBookmarks, graphBookmarks } from "./bookmarks.ts"
import * as Browse from "./browse.ts"
import * as Maintain from "./maintain.ts"
import * as Pipeline from "./pipeline.ts"
import type { Filters } from "./queries.ts"
import { search } from "./search.ts"

const bool = (name: string, description = "") => Flag.Boolean(name).pipe(Flag.withDescription(description), Flag.withDefault(false))
const many = (name: string, description: string) => Flag.String(name).pipe(Flag.withDescription(description), Flag.atLeast(0))
const yes = bool("yes", "write; without it every change is a dry run")
const json = bool("json", "machine-readable output")
const dryRun = bool("dry-run", "count what would run, call no model")
const limit = Flag.Int("limit").pipe(Flag.withDescription("stop after this many"), Flag.optional)

// Filters on indexed chunk properties: repeat a flag for OR, different flags AND.
const filterFlags = {
  kinds: many("kind", "document kind"),
  sources: many("source", "document source"),
  documents: many("doc", "documentId"),
  categories: many("category", "category name"),
  statuses: many("status", "document status (docs only)"),
  mine: bool("mine", "only passages about the owner"),
  withBoilerplate: bool("with-boilerplate", "include boilerplate (hidden by default)"),
  minSalience: Flag.Finite("min-salience").pipe(Flag.withDescription("0..1"), Flag.optional),
  where: many("where", "field=v | field!=v | field~substring | field^prefix"),
  entities: many("entity", "chunks about this entity (repeat = ANY)"),
  allEntities: bool("all-entities", "chunks about EVERY --entity"),
}
const filtersOf = (input: { readonly [K in keyof Filters]: Filters[K] }): Filters => input

const searchCommand = Command.make("search", {
  query: Argument.String("query").pipe(Argument.withDescription("names (≤3 words) go by graph + BM25; questions add vectors"), Argument.variadic()),
  ...filterFlags,
  mode: Flag.Literals("mode", ["graph", "hybrid", "vector", "text"]).pipe(Flag.optional),
  k: Flag.Int("k").pipe(Flag.withDescription("candidates per channel"), Flag.optional),
  top: Flag.Int("top").pipe(Flag.withDefault(8)),
  page: Flag.Int("page").pipe(Flag.withDefault(1)),
  chunks: bool("chunks", "one hit per passage, not per document"),
  expand: Flag.Int("expand").pipe(Flag.withDescription("neighbouring passages to print around a hit"), Flag.withDefault(0)),
  full: bool("full"),
  rerank: bool("rerank", "Jev re-ranks: one model call per candidate"),
  noGraph: bool("no-graph", "skip the 'about' / connected / related walk"),
  json,
}, (input) => search({
  query: input.query.join(" "), filters: filtersOf(input), mode: input.mode, k: input.k, top: input.top, page: Math.max(1, input.page),
  chunks: input.chunks, expand: input.expand, full: input.full, rerank: input.rerank, graph: !input.noGraph, json: input.json,
})).pipe(Command.withDescription("hybrid graph search"))

const docsCommand = Command.make("docs", { ...filterFlags, limit: Flag.Int("limit").pipe(Flag.withDefault(50)), json },
  (input) => Browse.docs({ filters: filtersOf(input), limit: input.limit, json: input.json })).pipe(Command.withDescription("list documents"))

const showCommand = Command.make("show", { documentId: Argument.String("documentId") }, ({ documentId }) => Browse.show(documentId))
  .pipe(Command.withDescription("print a document"))

const entitiesCommand = Command.make("entities", {
  ...filterFlags,
  entityKinds: many("entity-kind", "person | organisation | product | project | place | topic"),
  match: Flag.String("match").pipe(Flag.withDefault("")),
  top: Flag.Int("top").pipe(Flag.withDefault(40)),
  json,
  indexNames: bool("index-names", "EntityName shadow parity; --yes inserts missing names"),
  resolve: bool("resolve", "find duplicate entities, Jev decides, --yes merges"),
  limit,
  yes,
}, (input) => Effect.gen(function*() {
  if (input.indexNames) return yield* Maintain.indexEntityNames({ yes: input.yes })
  if (input.resolve) return yield* Maintain.resolveEntities({ yes: input.yes, limit: input.limit })
  return yield* Browse.tally({ what: "entities", filters: filtersOf(input), entityKinds: input.entityKinds, match: input.match.toLowerCase(), top: input.top, json: input.json })
})).pipe(Command.withDescription("what a slice of the corpus is about"))

const categoriesCommand = Command.make("categories", { ...filterFlags, top: Flag.Int("top").pipe(Flag.withDefault(60)), json },
  (input) => Browse.tally({ what: "categories", filters: filtersOf(input), entityKinds: [], match: "", top: input.top, json: input.json }))
  .pipe(Command.withDescription("how a slice of the corpus is filed"))

const ingestCommand = Command.make("ingest", {
  dir: Argument.Directory("dir", { mustExist: true }),
  kind: Flag.String("kind").pipe(Flag.optional),
  replace: bool("replace"),
}, (input) => Pipeline.ingest(input)).pipe(Command.withDescription("document + chunks + links, for every *.json in dir"))

const linkCommand = Command.make("link", { kinds: many("kind", "document kind"), limit, dryRun, redo: bool("redo") },
  (input) => Pipeline.link(input)).pipe(Command.withDescription("stage 1: Helix proposes entities, Jev decides"))

const nameCommand = Command.make("name", { kinds: many("kind", "document kind"), limit, dryRun },
  (input) => Pipeline.name(input)).pipe(Command.withDescription("stage 2: GLM names what Jev flagged, Jev checks every name"))

const bookmarksCommand = Command.make("bookmarks", {
  jsonl: Argument.String("jsonl").pipe(Argument.optional),
  labels: Flag.String("labels").pipe(Flag.optional),
  skip: Flag.String("skip").pipe(Flag.optional),
  ftdb: Flag.String("ftdb").pipe(Flag.optional),
  replace: bool("replace"),
  graph: bool("graph", "link, name and settle article bookmarks (>200 chars)"),
  redo: bool("redo"),
  limit,
  dryRun,
}, (input) => input.graph
  ? graphBookmarks({ jsonl: input.jsonl, skip: input.skip, ftdb: input.ftdb, limit: input.limit, dryRun: input.dryRun, redo: input.redo })
  : importBookmarks({ jsonl: input.jsonl, labels: input.labels, skip: input.skip, ftdb: input.ftdb, replace: input.replace, dryRun: input.dryRun }),
).pipe(Command.withDescription("one searchable chunk per X bookmark"))

const backupCommand = Command.make("backup", { dir: Argument.String("dir").pipe(Argument.optional) }, ({ dir }) => Maintain.backup(dir))
  .pipe(Command.withDescription("entities, links and judged chunk props → backup/<stamp>/*.jsonl"))
const checkCommand = Command.make("check", {}, () => Maintain.check).pipe(Command.withDescription("live invariants (exit 1 if broken)"))
const dedupeCommand = Command.make("dedupe", { yes }, (input) => Maintain.dedupe(input)).pipe(Command.withDescription("remove documents that are copies of another"))
const forgetCommand = Command.make("forget", { file: Argument.File("file", { mustExist: true }), yes }, (input) => Maintain.forget(input))
  .pipe(Command.withDescription("remove documents listed in file, one documentId per line"))
const pruneCommand = Command.make("prune", { yes }, (input) => Maintain.prune(input))
  .pipe(Command.withDescription("drop links that do not stand and orphan entities, from the newest backup"))
const modelCommand = Command.make("model", { yes }, (input) => Maintain.model(input))
  .pipe(Command.withDescription("owner profile, category parents, EXTENDS/UPDATES, entity associations"))

const statsCommand = Command.make("stats", {}, () => Browse.stats).pipe(Command.withDescription("documents by kind"))
const profileCommand = Command.make("profile", {}, () => Browse.profile).pipe(Command.withDescription("the owner profile"))
const taxonomyCommand = Command.make("taxonomy", { seed: bool("seed", "GLM drafts the categories from the corpus (ingest needs them)"), yes },
  (input) => Effect.gen(function*() {
    if (input.seed) return yield* Pipeline.seedTaxonomy({ yes: input.yes })
    return yield* Browse.taxonomy
  })).pipe(Command.withDescription("the categories"))


/** Prints what the model calls cost once a command that made any finishes. */
const withUsage = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.gen(function*() {
    const result = yield* effect
    const usage = yield* (yield* Models).usage
    if (usage.decisions || usage.generations || usage.embedded) {
      yield* Console.error(`usage: ${usage.decisions} jev calls (${usage.decisionTokens} tokens, $${usage.decisionCost.toFixed(4)}) · ${usage.generations} glm calls (${usage.promptTokens}+${usage.completionTokens} tokens) · ${usage.embedded} embedded`)
    }
    return result
  })

export const exo = Command.make("exo").pipe(
  Command.withDescription("Exocortex: a headless knowledge graph on Helix — hybrid graph RAG over documents, notes and bookmarks"),
  Command.withSubcommands([
    searchCommand, docsCommand, showCommand, entitiesCommand, categoriesCommand, statsCommand, profileCommand, taxonomyCommand,
    ingestCommand, linkCommand, nameCommand, bookmarksCommand,
    backupCommand, checkCommand, pruneCommand, dedupeCommand, forgetCommand, modelCommand,
  ]),
)

export const run = (argv: ReadonlyArray<string>) => withUsage(Command.runWith(exo, { version: "2.0.0" })(argv))
