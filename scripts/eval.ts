// Does the entity graph earn its keep? For each query, Jev grades three arms of `exo search`, run through the real CLI:
//   top    its first 8 passages
//   next   the 8 ranked after them (what you get by simply asking for more, no graph)
//   graph  the 8 passages it reports as "connected through shared entities"
// The graph earns its links when `graph` holds up against `next` AND surfaces useful passages plain search missed.
// 2026-09-19, before the prune (links kept from 0.5): graph 0.51 vs next 0.67, traversals up to 192 s.
// Results are appended to eval.history.jsonl so runs can be compared.   bun scripts/eval.ts "<note>"
import { join } from "node:path"
import { BunRuntime } from "@effect/platform-bun"
import { g, Predicate, readBatch } from "@helix-db/helix-db"
import { Array as Arr, Clock, Console, Effect, FileSystem, Number as Num, Schema } from "effect"
import { Decision } from "effect/ai"
import { ChildProcess, ChildProcessSpawner } from "effect/process"
import { Models } from "../src/ai.ts"
import { Helix } from "../src/helix.ts"
import { ExoLive } from "../src/live.ts"

const QUERIES = [
  "how should I price a consulting proposal", "value based pricing versus hourly billing", "how to ask for the sale without sounding pushy",
  "handling the objection that it is too expensive", "how to negotiate when the other side has more power", "writing a cold outreach message that gets replies",
  "building a lead magnet", "how to structure a retainer", "fractional CTO positioning", "getting referrals from existing clients",
  "how to beatmatch without sync", "transition from house into drum and bass", "how to use loops in a transition", "echo out effect", "what tracks did Knock2 play",
  "preparing tracks and cue points in rekordbox", "retrieval augmented generation evaluation", "structured outputs from language models", "how to run a discovery call", "productized service offer",
]
const ROOT = join(import.meta.dir, "..")

const SearchOutput = Schema.fromJsonString(Schema.Struct({
  hits: Schema.Array(Schema.Struct({ hit: Schema.Struct({ chunkId: Schema.String, content: Schema.String }) })),
  graph: Schema.NullOr(Schema.Struct({ connected: Schema.Array(Schema.Struct({ chunkId: Schema.String })) })),
}))
const Texts = Schema.Struct({ c: Schema.NullOr(Schema.Array(Schema.Struct({ chunkId: Schema.String, content: Schema.String }))) })

// One passage per request: several passages in one Jev request leak into each other (measured 2026-09-20).
const usefulness = Decision.make({
  input: Schema.Struct({ request: Schema.String, passage: Schema.String }),
  decisions: { useful: Decision.probability({ instructions: "The passage answers the request or contains information directly useful for it" }) },
})

interface Arm { readonly grades: ReadonlyArray<number>; readonly fresh: number; readonly millis: number }
const mean = (values: ReadonlyArray<number>) => values.length ? Num.sumAll(values) / values.length : 0
const usefulCount = (values: ReadonlyArray<number>) => values.filter((value) => value >= 0.5).length

const program = Effect.gen(function*() {
  const models = yield* Models
  const helix = yield* Helix
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner
  const fs = yield* FileSystem.FileSystem

  const kbSearch = Effect.fn("kbSearch")(function*(query: string, flags: ReadonlyArray<string>) {
    const started = yield* Clock.currentTimeMillis
    const stdout = yield* spawner.string(ChildProcess.make("bun", [join(ROOT, "exo.ts"), "search", query, "--json", "--chunks", ...flags]))
    const output = yield* Schema.decodeUnknownEffect(SearchOutput)(stdout)
    return { hits: output.hits.map((entry) => entry.hit), connected: output.graph?.connected ?? [], millis: (yield* Clock.currentTimeMillis) - started }
  })
  const grade = (query: string, passages: ReadonlyArray<string>) =>
    Effect.forEach(passages, (passage) =>
      passage.trim()
        ? models.decide(usefulness, { request: query, passage }).pipe(Effect.map((outcomes) => outcomes.useful._tag === "Success" ? outcomes.useful.success.probability : 0))
        : Effect.succeed(0), { concurrency: "unbounded" })

  const runs = yield* Effect.forEach(QUERIES, Effect.fnUntraced(function*(query) {
    const ranked = (yield* kbSearch(query, ["--top", "16", "--no-graph"])).hits
    const pool = new Set((yield* kbSearch(query, ["--top", "48", "--k", "48", "--no-graph"])).hits.map((hit) => hit.chunkId))
    const plain = yield* kbSearch(query, ["--top", "8", "--no-graph"])
    const withGraph = yield* kbSearch(query, ["--top", "8"])
    const ids = withGraph.connected.map((row) => row.chunkId)
    const texts = ids.length
      ? (yield* helix.read("eval_texts", readBatch().varAs("c", g().nWithLabelWhere("Chunk", Predicate.isIn("chunkId", ids)).valueMap(["chunkId", "content"])).returning(["c"]), Texts)).c ?? []
      : []
    const contentOf = new Map(texts.map((row) => [row.chunkId, row.content]))
    const [top, next, graph] = yield* Effect.all([
      grade(query, ranked.slice(0, 8).map((hit) => hit.content)),
      grade(query, ranked.slice(8, 16).map((hit) => hit.content)),
      grade(query, ids.map((id) => contentOf.get(id) ?? "")),
    ], { concurrency: "unbounded" })
    const fresh = ids.filter((id, index) => (graph[index] ?? 0) >= 0.5 && !pool.has(id)).length   // useful AND outside plain search's first 48
    const added = withGraph.millis - plain.millis
    yield* Console.log(`${query}\n   top ${mean(top).toFixed(2)} · next ${mean(next).toFixed(2)} · graph ${mean(graph).toFixed(2)} (${usefulCount(graph)}/${graph.length} useful, ${fresh} fresh, +${added}ms)`)
    return { top: { grades: top, fresh: 0, millis: 0 }, next: { grades: next, fresh: 0, millis: 0 }, graph: { grades: graph, fresh, millis: added } }
  }))

  const arms = (["top", "next", "graph"] as const).map((arm) => {
    const all: ReadonlyArray<Arm> = runs.map((run) => run[arm])
    const grades = Arr.flatMap(all, (run) => run.grades)
    return { arm, mean: Number(mean(grades).toFixed(3)), useful: usefulCount(grades), graded: grades.length, fresh: Num.sumAll(all.map((run) => run.fresh)), msPerQuery: Math.round(Num.sumAll(all.map((run) => run.millis)) / QUERIES.length) }
  })
  yield* Console.log("\narm      mean relevance   useful/graded   useful that plain search missed   added latency")
  for (const row of arms) {
    yield* Console.log(`${row.arm.padEnd(8)} ${row.mean.toFixed(3).padEnd(16)} ${`${row.useful}/${row.graded}`.padEnd(15)} ${String(row.arm === "graph" ? row.fresh : "-").padEnd(33)} ${row.arm === "graph" ? `${row.msPerQuery}ms/query` : "-"}`)
  }
  const at = new Date(yield* Clock.currentTimeMillis).toISOString()
  const line = JSON.stringify({ at, note: process.argv[2] ?? "", arms: Object.fromEntries(arms.map(({ arm, ...rest }) => [arm, rest])) })
  yield* fs.writeFileString(join(ROOT, "eval.history.jsonl"), `${line}\n`, { flag: "a" })
})


program.pipe(Effect.provide(ExoLive), BunRuntime.runMain)
