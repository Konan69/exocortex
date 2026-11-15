// Ranking quality of `exo search` against the graded gold set in fixtures/eval/gold.jsonl, run through the real CLI.
// Grades are per document (2 about it / answers it, 1 partly useful, unlisted 0), so search runs in its default
// one-hit-per-document view. Prints nDCG@10 (primary), recall@10 of grade-2 documents, MRR, errors, and latency.
//   bun scripts/rank-eval.ts <label> [--baseline <label>] [--gold <file>] [-- extra exo search flags]
// Each run is saved to .audit/rank-eval/<label>.json; --baseline prints the per-query change against an earlier run.
import { join } from "node:path"
import { BunRuntime, BunServices } from "@effect/platform-bun"
import { Array as Arr, Clock, Console, Effect, FileSystem, Number as Num, Order, Schema } from "effect"
import { ChildProcess, ChildProcessSpawner } from "effect/process"

const ROOT = join(import.meta.dir, "..")
const K = 10

const Gold = Schema.Struct({
  id: Schema.String,
  query: Schema.String,
  flags: Schema.Array(Schema.String),
  type: Schema.String,
  relevant: Schema.Record(Schema.String, Schema.Number),
})
type Gold = typeof Gold.Type
const Output = Schema.fromJsonString(Schema.Struct({ hits: Schema.Array(Schema.Struct({ hit: Schema.Struct({ documentId: Schema.String }) })) }))
const Run = Schema.Struct({
  label: Schema.String,
  at: Schema.String,
  queries: Schema.Array(Schema.Struct({ id: Schema.String, type: Schema.String, ndcg: Schema.Number, recall: Schema.Number, rr: Schema.Number, ms: Schema.Number, ranked: Schema.Array(Schema.String), error: Schema.NullOr(Schema.String) })),
})
type Run = typeof Run.Type

const dcg = (gains: ReadonlyArray<number>) => gains.reduce((sum, gain, index) => sum + (2 ** gain - 1) / Math.log2(index + 2), 0)
const score = (gold: Gold, ranked: ReadonlyArray<string>) => {
  const grade = (documentId: string) => gold.relevant[documentId] ?? 0
  const ideal = dcg(Arr.sort(Object.values(gold.relevant), Order.flip(Order.Number)).slice(0, K))
  const essential = Object.entries(gold.relevant).filter(([, value]) => value >= 2).map(([id]) => id)
  const first = ranked.findIndex((id) => grade(id) > 0)
  return {
    ndcg: ideal ? dcg(ranked.slice(0, K).map(grade)) / ideal : 0,
    recall: essential.length ? essential.filter((id) => ranked.slice(0, K).includes(id)).length / essential.length : 0,
    rr: first >= 0 && first < K ? 1 / (first + 1) : 0,
  }
}
const mean = (values: ReadonlyArray<number>) => values.length ? Num.sumAll(values) / values.length : 0
const median = (values: ReadonlyArray<number>) => {
  const sorted = Arr.sort(values, Order.Number)
  return sorted.length ? sorted[Math.floor(sorted.length / 2)]! : 0
}

const program = Effect.gen(function*() {
  const fs = yield* FileSystem.FileSystem
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner
  const argv = process.argv.slice(2)
  const split = argv.indexOf("--")
  const own = split >= 0 ? argv.slice(0, split) : argv
  const extra = split >= 0 ? argv.slice(split + 1) : []
  const label = own[0]
  if (!label) return yield* Console.error("usage: bun scripts/rank-eval.ts <label> [--baseline <label>] [-- exo search flags]")
  const baselineAt = own.indexOf("--baseline")
  const baselineLabel = baselineAt >= 0 ? own[baselineAt + 1] : undefined

  const goldAt = own.indexOf("--gold")
  const lines = (yield* fs.readFileString(goldAt >= 0 ? own[goldAt + 1]! : join(ROOT, "fixtures/eval/gold.jsonl"))).split("\n").filter((line) => line.trim())
  const golds = yield* Effect.forEach(lines, (line) => Schema.decodeUnknownEffect(Schema.fromJsonString(Gold))(line))
  const scored = golds.filter((gold) => Object.keys(gold.relevant).length > 0)

  const runOne = Effect.fnUntraced(function*(gold: Gold) {
    const started = yield* Clock.currentTimeMillis
    const result = yield* spawner.string(ChildProcess.make("bun", [join(ROOT, "exo.ts"), "search", gold.query, ...gold.flags, ...extra, "--json", "--top", String(K), "--no-graph"], { cwd: ROOT }))
      .pipe(Effect.flatMap(Schema.decodeUnknownEffect(Output)), Effect.result)
    const ms = (yield* Clock.currentTimeMillis) - started
    if (result._tag === "Failure") return { id: gold.id, type: gold.type, ndcg: 0, recall: 0, rr: 0, ms, ranked: [], error: String(result.failure).slice(0, 200) }
    const ranked = result.success.hits.map((entry) => entry.hit.documentId)
    return { id: gold.id, type: gold.type, ...score(gold, ranked), ms, ranked, error: null }
  })
  const queries = yield* Effect.forEach(scored, runOne, { concurrency: 4 })
  const run: Run = { label, at: new Date(yield* Clock.currentTimeMillis).toISOString(), queries }
  yield* fs.makeDirectory(join(ROOT, ".audit/rank-eval"), { recursive: true })
  yield* fs.writeFileString(join(ROOT, `.audit/rank-eval/${label}.json`), JSON.stringify(run, null, 1))

  const baseline = baselineLabel
    ? yield* fs.readFileString(join(ROOT, `.audit/rank-eval/${baselineLabel}.json`)).pipe(Effect.flatMap(Schema.decodeUnknownEffect(Schema.fromJsonString(Run))))
    : undefined
  const before = new Map((baseline?.queries ?? []).map((row) => [row.id, row]))
  for (const row of queries) {
    const gold = scored.find((entry) => entry.id === row.id)!
    const was = before.get(row.id)
    const delta = was ? ` (${row.ndcg - was.ndcg >= 0 ? "+" : ""}${(row.ndcg - was.ndcg).toFixed(2)})` : ""
    yield* Console.log(`${row.id} ${row.type.padEnd(10)} ndcg ${row.ndcg.toFixed(2)}${delta.padEnd(8)} recall ${row.recall.toFixed(2)}  rr ${row.rr.toFixed(2)}  ${row.error ? `ERROR ${row.error}` : ""} ${gold.query}`)
  }
  const types = Arr.dedupe(queries.map((row) => row.type))
  yield* Console.log("")
  for (const type of types) {
    const rows = queries.filter((row) => row.type === type)
    yield* Console.log(`${type.padEnd(10)} n=${rows.length}  ndcg ${mean(rows.map((row) => row.ndcg)).toFixed(3)}  recall ${mean(rows.map((row) => row.recall)).toFixed(3)}`)
  }
  const errors = queries.filter((row) => row.error).length
  const hits = Num.sumAll(queries.map((row) => row.ranked.length))
  yield* Console.log(`\n${label}: nDCG@${K} ${mean(queries.map((row) => row.ndcg)).toFixed(3)} · recall@${K} ${mean(queries.map((row) => row.recall)).toFixed(3)} · MRR ${mean(queries.map((row) => row.rr)).toFixed(3)}`
    + `${baseline ? ` (baseline ${baseline.label}: nDCG ${mean(baseline.queries.map((row) => row.ndcg)).toFixed(3)})` : ""}`)
  yield* Console.log(`vector channel: ${process.env.OPENAI_API_KEY ? "on" : "OFF (no OPENAI_API_KEY): graph + BM25 only"}`)
  yield* Console.log(`work: ${queries.length} queries scored (${golds.length - scored.length} with no relevant document skipped), ${hits} documents returned · errors ${errors} · median ${median(queries.map((row) => row.ms))} ms/query (CLI start included)`)
})

program.pipe(Effect.provide(BunServices.layer), BunRuntime.runMain)
