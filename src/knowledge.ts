import { createHash } from "node:crypto"
import { Schema } from "effect"

export const digest = (value: string) => createHash("sha256").update(value).digest("hex")
export function canonicalJSON(value: unknown): string {
  if (value === null || typeof value !== "object") return JSON.stringify(value) ?? "null"
  if (Array.isArray(value)) return `[${value.map(canonicalJSON).join(",")}]`
  return `{${Object.entries(value).sort(([a], [b]) => a.localeCompare(b)).map(([key, item]) => `${JSON.stringify(key)}:${canonicalJSON(item)}`).join(",")}}`
}
export const lookupName = (name: string) => name.normalize("NFC").trim().toLocaleLowerCase()

export const Source = Schema.Struct({
  id: Schema.String,
  title: Schema.String,
  content: Schema.String,
  kind: Schema.String,
  source: Schema.String,
  url: Schema.String,
  category: Schema.optional(Schema.String),
  salience: Schema.optional(Schema.Finite),
  provenance: Schema.Union([
    Schema.Struct({ kind: Schema.Literal("original"), path: Schema.String }),
    Schema.Struct({ kind: Schema.Literal("assembled"), parts: Schema.Array(Schema.Struct({ path: Schema.String, locator: Schema.String, hash: Schema.String })) }),
    Schema.Struct({ kind: Schema.Literal("recovered"), chunkIds: Schema.Array(Schema.String), originalChecksum: Schema.String }),
    Schema.Struct({ kind: Schema.Literal("legacy"), chunkIds: Schema.Array(Schema.String) }),
  ]),
})
export type Source = typeof Source.Type

export const Evidence = Schema.Struct({ quote: Schema.String, start: Schema.Int, end: Schema.Int })
export type Evidence = typeof Evidence.Type
const Identifier = Schema.Struct({ namespace: Schema.String, value: Schema.String, quote: Schema.String })
export const ExtractedEntity = Schema.Struct({
  id: Schema.String,
  name: Schema.String,
  description: Schema.String,
  kind: Schema.String,
  aliases: Schema.Array(Schema.String),
  identifiers: Schema.Array(Identifier),
  quotes: Schema.Array(Schema.String),
})
export type ExtractedEntity = typeof ExtractedEntity.Type
export const ClaimObject = Schema.Union([
  Schema.Struct({ kind: Schema.Literal("entity"), id: Schema.String }),
  Schema.Struct({ kind: Schema.Literal("value"), value: Schema.String }),
])
export const ExtractedClaim = Schema.Struct({
  id: Schema.String,
  subject: Schema.String,
  predicate: Schema.String,
  object: ClaimObject,
  statement: Schema.String,
  qualifiers: Schema.Array(Schema.String),
  quotes: Schema.Array(Schema.String),
})
export type ExtractedClaim = typeof ExtractedClaim.Type
export const Extraction = Schema.Struct({
  entities: Schema.Array(ExtractedEntity),
  claims: Schema.Array(ExtractedClaim),
  complete: Schema.Boolean,
})
export type Extraction = typeof Extraction.Type

export const Verdict = Schema.Struct({
  id: Schema.String,
  verdict: Schema.Literals(["supported", "refuted", "uncertain"]),
  reason: Schema.String,
})
export type Verdict = typeof Verdict.Type
export const ExtractionRecord = Schema.Struct({
  source: Source,
  sourceHash: Schema.String,
  extractionVersion: Schema.String,
  model: Schema.String,
  judgeModel: Schema.String,
  coverage: Schema.Array(Schema.Struct({ start: Schema.Int, end: Schema.Int })),
  evidence: Schema.Record(Schema.String, Schema.Array(Evidence)),
  extraction: Extraction,
  entityVerdicts: Schema.Array(Verdict),
  verdicts: Schema.Array(Verdict),
})
export type ExtractionRecord = typeof ExtractionRecord.Type

const PendingRecord = Schema.Struct({
  source: Source,
  sourceHash: Schema.String,
  pending: Schema.Literal(true),
  extraction: Schema.optional(Schema.Never),
})
export const GenerationRecord = Schema.Union([ExtractionRecord, PendingRecord])
export type GenerationRecord = typeof GenerationRecord.Type
export const hasExtraction = (record: GenerationRecord): record is ExtractionRecord => record.extraction !== undefined
export const pendingRecord = (source: Source): typeof PendingRecord.Type => ({
  source, sourceHash: digest(source.content), pending: true,
})

export const Retirement = Schema.Struct({ documentId: Schema.String, reason: Schema.String })
export type Retirement = typeof Retirement.Type

export const Ontology = Schema.Struct({
  name: Schema.String,
  description: Schema.String,
  types: Schema.Array(Schema.Struct({
    name: Schema.String,
    description: Schema.String,
    match: Schema.Array(Schema.String),
    identityNamespaces: Schema.Array(Schema.String),
  })),
  relations: Schema.Array(Schema.Struct({
    name: Schema.String,
    description: Schema.String,
    match: Schema.Array(Schema.String),
    from: Schema.Array(Schema.String),
    to: Schema.Array(Schema.String),
  })),
})
export type Ontology = typeof Ontology.Type

export const Mapping = Schema.Struct({
  entities: Schema.Array(Schema.Struct({ id: Schema.String, type: Schema.NullOr(Schema.String), reason: Schema.String })),
  claims: Schema.Array(Schema.Struct({ id: Schema.String, relation: Schema.NullOr(Schema.String), reason: Schema.String })),
})
export type Mapping = typeof Mapping.Type
export const MappedRecord = Schema.Struct({ record: ExtractionRecord, ontologyHash: Schema.String, mapping: Mapping })
export type MappedRecord = typeof MappedRecord.Type

export function evidenceOf(content: string, quotes: ReadonlyArray<string>): Evidence[] {
  if (!quotes.length) throw new Error("Missing evidence")
  return quotes.map(quote => {
    const start = content.indexOf(quote)
    if (!quote.trim() || start < 0) throw new Error("Evidence is not an exact source quote")
    if (content.indexOf(quote, start + 1) >= 0) throw new Error("Evidence quote is ambiguous; include more surrounding text")
    return { quote, start, end: start + quote.length }
  })
}

export function validateExtraction(source: Source, extraction: Extraction): void {
  if (!extraction.complete) throw new Error("Extraction reported incomplete coverage")
  const ids = new Set<string>()
  for (const entity of extraction.entities) {
    if (!entity.id || ids.has(entity.id)) throw new Error("Duplicate or empty entity id")
    ids.add(entity.id)
    if (!entity.name.trim()) throw new Error("Entity has no name")
    if (!entity.quotes.length || entity.quotes.some(quote => !quote.trim() || !source.content.includes(quote))) throw new Error("Entity evidence absent from source")
    for (const identifier of entity.identifiers) {
      if (!identifier.quote.trim() || !source.content.includes(identifier.quote)) throw new Error("Identifier evidence absent from source")
      if (!identifier.namespace || !identifier.value || !identifier.quote.includes(identifier.value)) throw new Error("Identifier lacks literal evidence")
    }
    for (const alias of entity.aliases) if (!alias.trim() || !source.content.includes(alias)) throw new Error("Alias absent from source")
  }
  const claims = new Set<string>()
  for (const claim of extraction.claims) {
    if (!claim.id || claims.has(claim.id)) throw new Error("Duplicate or empty claim id")
    claims.add(claim.id)
    if (!ids.has(claim.subject) || (claim.object.kind === "entity" && !ids.has(claim.object.id))) throw new Error("Claim references unknown entity")
    if (!claim.quotes.length || claim.quotes.some(quote => !quote.trim() || !source.content.includes(quote))) throw new Error("Claim evidence absent from source")
  }
}

export function validateRecord(record: ExtractionRecord): void {
  validateExtraction(record.source, record.extraction)
  if (digest(record.source.content) !== record.sourceHash) throw new Error("Source checksum mismatch")
  let cursor = 0
  for (const range of record.coverage) {
    if (range.start !== cursor || range.end <= range.start || range.end > record.source.content.length) throw new Error("Extraction coverage has a gap, overlap or invalid range")
    cursor = range.end
  }
  if (cursor !== record.source.content.length) throw new Error("Extraction does not cover the complete source")
  for (const [prefix, observations] of [["entity", record.extraction.entities], ["claim", record.extraction.claims]] as const) {
    for (const observation of observations) {
      const spans = record.evidence[`${prefix}:${observation.id}`]
      if (!spans || spans.length !== observation.quotes.length) throw new Error("Evidence positions are missing")
      for (const [index, span] of spans.entries()) {
        if (span.quote !== observation.quotes[index] || span.start < 0 || span.end !== span.start + span.quote.length || record.source.content.slice(span.start, span.end) !== span.quote) throw new Error("Evidence position does not match the source")
      }
    }
  }
  const verdicts = new Set(record.verdicts.map(verdict => verdict.id))
  if (verdicts.size !== record.verdicts.length || verdicts.size !== record.extraction.claims.length || record.extraction.claims.some(claim => !verdicts.has(claim.id))) throw new Error("Verification must account for every claim exactly once")
  const entityVerdicts = new Set(record.entityVerdicts.map(verdict => verdict.id))
  if (entityVerdicts.size !== record.entityVerdicts.length || entityVerdicts.size !== record.extraction.entities.length || record.extraction.entities.some(entity => !entityVerdicts.has(entity.id))) throw new Error("Verification must account for every entity exactly once")
}

export function validateOntology(ontology: Ontology): void {
  const types = new Set(ontology.types.map(type => type.name))
  if (!types.size || types.size !== ontology.types.length || types.has("value") || types.has("")) throw new Error("Ontology needs distinct named types; value is reserved for literal objects")
  const relations = new Set(ontology.relations.map(relation => relation.name))
  if (relations.size !== ontology.relations.length || relations.has("")) throw new Error("Relation names must be distinct and nonempty")
  for (const rule of [...ontology.types, ...ontology.relations]) {
    if (!rule.match.length || rule.match.some(value => !value.trim())) throw new Error("Rules must name the extracted kinds or predicates they match")
  }
  for (const relation of ontology.relations) {
    if (!relation.from.length || !relation.to.length) throw new Error("Relations must declare their endpoints")
    if (relation.from.some(type => !types.has(type)) || relation.to.some(type => type !== "value" && !types.has(type))) throw new Error("Relation references unknown type")
  }
}

export function mapExtraction(record: ExtractionRecord, ontology: Ontology): Mapping {
  validateOntology(ontology)
  const entities = record.extraction.entities.map(entity => {
    const matches = ontology.types.filter(type => type.match.includes(entity.kind))
    const supported = record.entityVerdicts.find(verdict => verdict.id === entity.id)?.verdict === "supported"
    return { id: entity.id, type: supported && matches.length === 1 ? matches[0]!.name : null, reason: !supported ? "Entity identity, aliases or description not supported" : matches.length === 1 ? "Matched extracted kind" : matches.length ? "Ambiguous type rules" : "No matching type rule" }
  })
  const typeOf = new Map(entities.map(entity => [entity.id, entity.type]))
  const verdicts = new Map(record.verdicts.map(verdict => [verdict.id, verdict.verdict]))
  const claims = record.extraction.claims.map(claim => {
    const from = typeOf.get(claim.subject)
    const to = claim.object.kind === "value" ? "value" : typeOf.get(claim.object.id)
    const matches = ontology.relations.filter(relation => relation.match.includes(claim.predicate) && !!from && !!to && relation.from.includes(from) && relation.to.includes(to))
    const supported = verdicts.get(claim.id) === "supported"
    return { id: claim.id, relation: supported && matches.length === 1 ? matches[0]!.name : null, reason: !supported ? "Source verification did not support this claim" : matches.length === 1 ? "Matched predicate and endpoints" : matches.length ? "Ambiguous relation rules" : "No matching relation rule" }
  })
  const mapping = { entities, claims }
  validateMapping(record, ontology, mapping)
  return mapping
}

export function validateMapping(record: ExtractionRecord, ontology: Ontology, mapping: Mapping): void {
  validateOntology(ontology)
  const entities = new Map(mapping.entities.map(entity => [entity.id, entity]))
  const claims = new Map(mapping.claims.map(claim => [claim.id, claim]))
  if (entities.size !== mapping.entities.length || entities.size !== record.extraction.entities.length || record.extraction.entities.some(entity => !entities.has(entity.id))) throw new Error("Mapping must account for every extracted entity exactly once")
  if (claims.size !== mapping.claims.length || claims.size !== record.extraction.claims.length || record.extraction.claims.some(claim => !claims.has(claim.id))) throw new Error("Mapping must account for every extracted claim exactly once")
  const types = new Set(ontology.types.map(type => type.name))
  for (const entity of mapping.entities) if (entity.type !== null && !types.has(entity.type)) throw new Error("Mapping invented a type")
  const verdicts = new Map(record.verdicts.map(verdict => [verdict.id, verdict]))
  for (const claim of record.extraction.claims) {
    const mapped = claims.get(claim.id)
    if (!mapped || mapped.relation === null) continue
    if (verdicts.get(claim.id)?.verdict !== "supported") throw new Error("Mapping promoted an unsupported claim")
    const relation = ontology.relations.find(relation => relation.name === mapped.relation)
    const from = entities.get(claim.subject)?.type
    const to = claim.object.kind === "value" ? "value" : entities.get(claim.object.id)?.type
    if (!relation || !from || !to || !relation.from.includes(from) || !relation.to.includes(to)) throw new Error("Mapped relation violates endpoint rules")
  }
}

export function entityKey(record: ExtractionRecord, entity: ExtractedEntity, type: Ontology["types"][number]): string {
  const identifiers = entity.identifiers.filter(identifier => type.identityNamespaces.includes(identifier.namespace))
  const identity = identifiers.sort((a, b) => type.identityNamespaces.indexOf(a.namespace) - type.identityNamespaces.indexOf(b.namespace))[0]
  return digest(JSON.stringify(identity
    ? [type.name, identity.namespace, identity.value]
    : [type.name, record.source.id, record.sourceHash, entity.id]))
}

export function comparability(before: Ontology, after: Ontology): { comparable: boolean; reasons: string[] } {
  const reasons: string[] = []
  for (const old of before.types) {
    const next = after.types.find(type => type.name === old.name)
    if (!next) reasons.push(`Removed type ${old.name}`)
    else if (JSON.stringify(next.identityNamespaces) !== JSON.stringify(old.identityNamespaces)) reasons.push(`Changed identity rule for ${old.name}`)
  }
  for (const old of before.relations) {
    const next = after.relations.find(relation => relation.name === old.name)
    if (!next) reasons.push(`Removed relation ${old.name}`)
    else if (JSON.stringify([next.from, next.to]) !== JSON.stringify([old.from, old.to])) reasons.push(`Changed endpoints for ${old.name}`)
  }
  return { comparable: reasons.length === 0, reasons }
}
