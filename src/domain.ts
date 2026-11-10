// The graph's vocabulary. Every rule here was measured; README.md has the numbers behind each one.
//
//   User ─OWNS→ SourceDocument ─HAS_CHUNK→ Chunk ─MENTIONS{confidence, by}→ Entity ─ASSOCIATED_WITH→ Entity
//   Chunk ─IN_CATEGORY→ Category ←PARENT_OF─ Category        SourceDocument ─MENTIONS{by:"consolidated"}→ Entity
//   Chunk ─RELATES_TO | EXTENDS | UPDATES→ Chunk             EntityName (BM25 shadow of Entity.name)
import { createHash } from "node:crypto"
import { Schema } from "effect"

// Who the store belongs to. Bun loads `.env` from the working directory (copy `.env.example`); every node carries
// the tenant, so a missing value would silently read an empty graph. It fails at start instead.
const identity = (name: string) => {
  const value = process.env[name]?.trim()
  if (!value) throw new Error(`${name} is not set: copy .env.example to .env and run exo from its directory`)
  return value
}
export const TENANT = identity("EXO_TENANT")
export const OWNER = identity("EXO_OWNER")
/** the owner's name as passages spell it; Jev's "about the owner" question and the profile use it */
export const OWNER_NAME = process.env.EXO_OWNER_NAME?.trim() || OWNER

export const EntityKind = Schema.Literals(["person", "organisation", "product", "project", "place", "topic"])
export type EntityKind = typeof EntityKind.Type
export const ENTITY_KINDS = EntityKind.literals

export const DocumentId = Schema.String.pipe(Schema.brand("DocumentId"))
export type DocumentId = typeof DocumentId.Type
export const ChunkId = Schema.String.pipe(Schema.brand("ChunkId"))
export type ChunkId = typeof ChunkId.Type
export const EntityKey = Schema.String.pipe(Schema.brand("EntityKey"))
export type EntityKey = typeof EntityKey.Type
/** Helix's internal node or edge id. */
export const NodeId = Schema.Finite
export type NodeId = number

export const Floors = {
  /** a mention is written only at this confidence; below it is "named in passing" */
  mention: 0.9,
  /** a proposed non-topic name must be node-worthy at least this much */
  worthy: 0.4,
  /** topics need a recognised name: ≥0.8 reads as named concepts, <0.4 as phrases */
  topic: 0.8,
  /** the passage must be ABOUT the entity, not just name it */
  about: 0.4,
  /** two spellings of one name are only vetoed by Jev, so it hedges: every pair from 0.82 up was right by hand */
  alias: 0.8,
} as const

/** 4 = candidate names only in questions, 6 passages a request (2 listed them in the state and leaked). */
export const LINK_VERSION = 4
/** An entity held by more passages than this says nothing about any one of them: not walked, still filterable. */
export const HUB = 60
export const CHUNK_CHARS = 4000

/** Identity is the normalised name; the rule the existing Entity nodes were written with. */
export const normalise = (name: string): string =>
  name.normalize("NFKD").replace(/\p{M}/gu, "").toLowerCase().trim()
    .replace(/[’'`]/g, "")
    .replace(/\b(ltd|limited|inc|llc|plc|gmbh|corp|corporation|co)\b\.?/g, "")
    .replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "")

export const entityKeyOf = (kind: EntityKind, name: string): EntityKey =>
  EntityKey.make(`${TENANT}:${kind}:${normalise(name)}`)
export const categoryKeyOf = (name: string) => `${TENANT}:${normalise(name)}`
export const chunkIdOf = (documentId: DocumentId, index: number): ChunkId => ChunkId.make(`${documentId}:${index}`)

/** Identical text, not similar text: whitespace collapsed, then hashed. */
export const passageHash = (text: string) =>
  createHash("sha256").update(text.replace(/\s+/g, " ").trim()).digest("hex").slice(0, 32)

/** Paragraph-preserving cut into ≤ CHUNK_CHARS passages. */
export const chunksOf = (text: string): ReadonlyArray<string> => {
  const output: Array<string> = []
  let current = ""
  for (let paragraph of text.split("\n")) {
    if (current.length + paragraph.length + 1 > CHUNK_CHARS) {
      if (current) output.push(current)
      while (paragraph.length > CHUNK_CHARS) {
        output.push(paragraph.slice(0, CHUNK_CHARS))
        paragraph = paragraph.slice(CHUNK_CHARS)
      }
      current = paragraph
    } else current = current ? `${current}\n${paragraph}` : paragraph
  }
  if (current.trim()) output.push(current)
  return output.filter((chunk) => chunk.trim())
}

// ---- rows as Helix returns them (decoded at the Helix boundary)

export class Category extends Schema.Class<Category>("Category")({
  name: Schema.String,
  description: Schema.String,
}) {}

export class Doc extends Schema.Class<Doc>("Doc")({
  documentId: DocumentId,
  title: Schema.String,
  kind: Schema.String,
  source: Schema.String,
  url: Schema.optional(Schema.String),
}) {}

/** An edge row as `valueMap(["$id","$from","$to",...])` returns it. */
export const MentionEdge = Schema.Struct({
  $id: NodeId,
  $from: NodeId,
  $to: NodeId,
  confidence: Schema.Finite,
  by: Schema.optional(Schema.String),
  entityKey: Schema.optional(EntityKey),
})
export type MentionEdge = typeof MentionEdge.Type

export const EntityRow = Schema.Struct({
  $id: NodeId,
  entityKey: EntityKey,
  name: Schema.String,
  kind: EntityKind,
})
export type EntityRow = typeof EntityRow.Type

/** The EntityName shadow node written beside every Entity (its BM25 index; Entity.name cannot be text-indexed). */
export const entityNameProps = (entity: { readonly name: string; readonly entityKey: EntityKey; readonly kind: EntityKind }) =>
  ({ tenant_id: TENANT, userId: OWNER, name: entity.name.trim(), entityKey: entity.entityKey, kind: entity.kind })
