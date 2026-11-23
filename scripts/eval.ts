import { Effect, Schema } from "effect"
import { readFile } from "node:fs/promises"
import { ExoLive } from "../src/live.ts"
import { activeGeneration, claims, entities, generationRecords } from "../src/graph.ts"
import { Evidence, lookupName } from "../src/knowledge.ts"
import { io, checked } from "../src/extraction.ts"

const Gold = Schema.Struct({ documentId: Schema.String, subject: Schema.String, relation: Schema.String, object: Schema.String, expected: Schema.Boolean, quote: Schema.String })
const program = Effect.gen(function*() {
  const path = process.argv[2]
  if (!path) return yield* Effect.fail(new Error("Usage: bun scripts/eval.ts <human-labeled-claims.jsonl> [generation]"))
  const generation = process.argv[3] ?? (yield* activeGeneration)
  const lines = (yield* io(() => readFile(path, "utf8"))).split("\n").filter(line => line.trim())
  const gold = yield* Effect.forEach(lines, line => checked(() => Schema.decodeUnknownSync(Gold)(JSON.parse(line))))
  if (!gold.length || !gold.some(row => row.expected)) return yield* Effect.fail(new Error("Evaluation requires at least one positive case; abstaining on everything is not success"))
  const stored = yield* claims(generation)
  const named = yield* entities(generation)
  const sources = new Map((yield* generationRecords(generation)).map(record => [record.source.id, record.source.content]))
  const ids = (name: string) => new Set(named.filter(entity => entity.aliases.some(alias => lookupName(alias) === lookupName(name))).map(entity => entity.entityId))
  const cases = []
  for (const row of gold) {
    const text = sources.get(row.documentId)
    if (text === undefined || !row.quote || !text.includes(row.quote)) return yield* Effect.fail(new Error(`Gold evidence is absent from ${row.documentId}`))
    const subjects = ids(row.subject), objects = ids(row.object)
    const matches = stored.filter(claim => claim.documentId === row.documentId && subjects.has(claim.subject) && claim.relation === row.relation && (claim.object ? objects.has(claim.object) : claim.value === row.object))
    let cited = false
    for (const match of matches) {
      const spans = yield* checked(() => Schema.decodeUnknownSync(Schema.Array(Evidence))(JSON.parse(match.evidence)))
      cited ||= spans.some(span => text.slice(span.start, span.end) === span.quote && span.quote.includes(row.quote))
    }
    cases.push({ ...row, matches: matches.length, cited, passed: row.expected ? matches.length > 0 && cited : matches.length === 0 })
  }
  const positives = cases.filter(row => row.expected)
  const negatives = cases.filter(row => !row.expected)
  console.log(JSON.stringify({ generation, coverage: "Only the supplied human-labeled cases; not corpus-wide accuracy", positiveCases: positives.length,
    capturedWithExpectedEvidence: positives.filter(row => row.passed).length, negativeCases: negatives.length,
    incorrectNegativeClaims: negatives.filter(row => !row.passed).length, failures: cases.filter(row => !row.passed) }, null, 2))
  if (cases.some(row => !row.passed)) return yield* Effect.fail(new Error("Labeled claim evaluation failed"))
})
Effect.runPromise(program.pipe(Effect.provide(ExoLive))).catch(error => { console.error(error); process.exitCode = 1 })
