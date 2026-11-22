// Validate hand extractions and emit them as Exo extraction records for `exo map`.
// bun .audit/rebuild/hand/hand-check.ts check <doc.json...>
// bun .audit/rebuild/hand/hand-check.ts emit <out-dir> [--include-pending]
import { mkdir, readdir } from "node:fs/promises"
import { join } from "node:path"
import { canonicalJSON, digest, evidenceOf, ExtractionRecord, type GenerationRecord, hasExtraction, pendingRecord, Source, validateRecord } from "../../../src/knowledge.ts"
import { Schema } from "effect"

const root = import.meta.dir
const prepared: { documents: { id: string; path: string; sourceHash: string }[] } = await Bun.file(join(root, "../prepared-input.json")).json()
const byId = new Map(prepared.documents.map(row => [row.id, row]))
const titleRows = (await Bun.file(join(root, "titles.tsv")).text()).trimEnd().split("\n")
if (titleRows.shift() !== "documentId\ttitle") throw new Error("Invalid title snapshot header")
const legacyTitles = new Map(titleRows.map(line => {
  const [id, title] = line.split("\t")
  if (!id || !title) throw new Error("Invalid title snapshot row")
  return [id, Schema.decodeUnknownSync(Schema.String)(JSON.parse(title))]
}))

const Hand = Schema.Struct({
  id: Schema.String,
  sourceHash: Schema.String,
  reader: Schema.String,
  entities: Schema.Array(Schema.Struct({
    id: Schema.String, name: Schema.String, kind: Schema.String, description: Schema.String,
    aliases: Schema.Array(Schema.String),
    identifiers: Schema.optional(Schema.Array(Schema.Struct({ namespace: Schema.String, value: Schema.String, quote: Schema.String }))),
    quotes: Schema.Array(Schema.String),
  })),
  claims: Schema.Array(Schema.Struct({
    id: Schema.String, subject: Schema.String, predicate: Schema.String,
    object: Schema.Union([Schema.Struct({ entity: Schema.String }), Schema.Struct({ value: Schema.String })]),
    statement: Schema.String, qualifiers: Schema.Array(Schema.String), quotes: Schema.Array(Schema.String),
  })),
})
const Checks = Schema.Struct({
  id: Schema.String, checker: Schema.String,
  verdicts: Schema.Array(Schema.Struct({ id: Schema.String, verdict: Schema.Literals(["supported", "refuted", "uncertain"]), reason: Schema.String })),
})

export const fileOf = (id: string) => id.replace(/[^A-Za-z0-9._-]/g, "_") + ".json"

const OriginalSource = Schema.Struct({
  content: Schema.String,
  title: Schema.optional(Schema.NullOr(Schema.String)),
  metadata: Schema.optional(Schema.NullOr(Schema.Struct({ title: Schema.optional(Schema.NullOr(Schema.String)) }))),
})

async function preparedSource(location: typeof prepared.documents[number]): Promise<Source> {
  const source = Schema.decodeUnknownSync(Source)(await Bun.file(location.path).json())
  const legacyTitle = legacyTitles.get(source.id)
  if (legacyTitle !== undefined) return { ...source, title: legacyTitle }
  if (source.title.trim() && source.title !== source.id) return source
  if (source.provenance.kind !== "original" || !source.provenance.path.endsWith(".json")) return source
  const file = Bun.file(source.provenance.path)
  if (!(await file.exists())) return source
  const original = Schema.decodeUnknownSync(OriginalSource)(await file.json())
  if (digest(original.content) !== location.sourceHash) return source
  const title = [original.title, original.metadata?.title].find(title => title?.trim() && title !== source.id)
  return title ? { ...source, title } : source
}

async function record(path: string, requireChecks: boolean) {
  const hand = Schema.decodeUnknownSync(Hand)(await Bun.file(path).json())
  const location = byId.get(hand.id)
  if (!location) throw new Error(`${hand.id}: not in the prepared corpus`)
  const source = await preparedSource(location)
  const sourceHash = digest(source.content)
  if (sourceHash !== location.sourceHash || hand.sourceHash !== sourceHash) throw new Error(`${hand.id}: sourceHash differs from the prepared source`)
  const evidence: Record<string, ReturnType<typeof evidenceOf>> = {}
  const where = (label: string, quotes: ReadonlyArray<string>) => {
    try { return evidenceOf(source.content, quotes) } catch (error) { throw new Error(`${hand.id} ${label}: ${(error as Error).message}: ${JSON.stringify(quotes)}`) }
  }
  for (const entity of hand.entities) evidence[`entity:${entity.id}`] = where(entity.id, entity.quotes)
  for (const claim of hand.claims) evidence[`claim:${claim.id}`] = where(claim.id, claim.quotes)
  const names = new Set(hand.entities.map(entity => entity.name))
  if (names.size !== hand.entities.length) throw new Error(`${hand.id}: two entities share a name; merge them`)
  const entityVerdicts = hand.entities.map(({ id }) => id)
  let checks: typeof Checks.Type | undefined
  const checkPath = path.replace(/\.json$/, ".checks.json")
  if (await Bun.file(checkPath).exists()) {
    checks = Schema.decodeUnknownSync(Checks)(await Bun.file(checkPath).json())
    if (checks.id !== hand.id) throw new Error(`${hand.id}: checks belong to ${checks.id}`)
    if (checks.checker === hand.reader) throw new Error(`${hand.id}: the reader cannot check its own extraction`)
  } else if (requireChecks) return undefined
  const verdict = (id: string) => checks?.verdicts.find(row => row.id === id) ?? { id, verdict: "uncertain" as const, reason: "Not checked yet" }
  const built = ExtractionRecord.make({
    source, sourceHash, extractionVersion: "hand-v1", model: `hand:${hand.reader}`, judgeModel: checks ? `hand:${checks.checker}` : "unchecked",
    coverage: [{ start: 0, end: source.content.length }], evidence,
    extraction: {
      complete: true,
      entities: hand.entities.map(entity => ({ ...entity, identifiers: entity.identifiers ?? [] })),
      claims: hand.claims.map(claim => ({ ...claim, object: "entity" in claim.object ? { kind: "entity" as const, id: claim.object.entity } : { kind: "value" as const, value: claim.object.value } })),
    },
    entityVerdicts: entityVerdicts.map(id => ({ ...verdict(`entity:${id}`), id })),
    verdicts: hand.claims.map(({ id }) => ({ ...verdict(`claim:${id}`), id })),
  })
  try { validateRecord(built) } catch (error) { throw new Error(`${hand.id}: ${(error as Error).message}`) }
  return built
}

const [command, ...rest] = process.argv.slice(2)
if (command === "check") {
  let failed = 0
  for (const path of rest) {
    try {
      const built = await record(path, false)
      console.log(`ok ${built!.source.id}: ${built!.extraction.entities.length} entities, ${built!.extraction.claims.length} claims`)
    } catch (error) { failed++; console.log(`FAIL ${path}: ${(error as Error).message}`) }
  }
  process.exit(failed ? 1 : 0)
} else if (command === "emit") {
  const [out, flag, ...extra] = rest
  if (!out || (flag !== undefined && flag !== "--include-pending") || extra.length) {
    throw new Error("usage: emit <out-dir> [--include-pending]")
  }
  const docs = join(root, "docs")
  const files = new Set(await readdir(docs))
  let mapped = 0
  const documents: { id: string; artifact: string; sourceHash: string }[] = []
  await mkdir(out, { recursive: true })
  for (const location of prepared.documents) {
    const name = fileOf(location.id)
    const checked = files.has(name.replace(/\.json$/, ".checks.json"))
    let built: GenerationRecord
    if (checked) {
      const extracted = await record(join(docs, name), true)
      if (!extracted || extracted.source.id !== location.id) throw new Error(`${location.id}: checked extraction is missing or mismatched`)
      built = extracted
    } else {
      if (flag !== "--include-pending") continue
      const source = await preparedSource(location)
      built = pendingRecord(source)
      if (source.id !== location.id || built.sourceHash !== location.sourceHash) throw new Error(`${location.id}: prepared source differs from its manifest`)
    }
    if (hasExtraction(built)) mapped++
    const artifact = digest(canonicalJSON(built)) + ".json"
    await Bun.write(join(out, artifact), JSON.stringify(built))
    documents.push({ id: built.source.id, artifact, sourceHash: built.sourceHash })
  }
  const run = join(out, digest(canonicalJSON(documents.map(row => row.artifact).sort())))
  await mkdir(run, { recursive: true })
  await Bun.write(join(run, "run.json"), JSON.stringify({ expected: documents.length, complete: documents.length, documents, failures: [] }, null, 2))
  console.log(JSON.stringify({ documents: documents.length, mapped, pending: documents.length - mapped, run }))
} else throw new Error("usage: check <files...> | emit <out-dir> [--include-pending]")
