# Exocortex

Exo extracts observations from original documents, then maps those retained observations through a versioned ontology into Helix. Entities have source-supported identities. Relationships carry exact source evidence, direction and qualifiers. Co-occurrence is not a relationship.

## Run

Requires Bun, the installed dependencies, Helix, EXO_TENANT and OPENROUTER_API_KEY. Set HELIX_URL to use an existing server. Configuration lives in .env; source text and extraction artifacts are private and gitignored.

```sh
bun exo.ts extract /path/to/source-json
bun exo.ts ontology draft /path/printed/by/extract --output ontology.json
bun exo.ts map /path/printed/by/extract --ontology ontology.json
bun exo.ts stats --generation <id>
bun exo.ts activate <id>
bun exo.ts ingest /path/to/new-source-json
bun exo.ts search 'garden planner' --entity Atlas
bun exo.ts walk 'Garden Planner'
```

Source files contain id, title and content, plus optional kind, source, url and metadata. Source ids must be unique within an input directory. Replacing changed text requires ingest --replace. Extraction does not receive the ontology. Its free-form kinds and predicates remain available even when the current ontology cannot map them.

Ontology types have name, description, match and identityNamespaces. Relation definitions have name, description, match, from and to. Match lists contain exact extracted kinds or predicates. The reserved endpoint type value denotes a literal value. Unmatched or ambiguous observations remain retained and unmapped. Identity namespaces apply only to explicitly evidenced identifiers; without one, entities remain source-local rather than merging on a name. Alias lookup preserves punctuation and non-Latin characters.

## Revise and rebuild

```sh
bun exo.ts ontology revise --ontology revised.json
bun exo.ts rebalance /path/to/complete-corpus --ontology revised.json
bun exo.ts activate <replacement-id>
```

Revision comparison replays rules on the same stored extraction records without model calls or writes. It reports changed claims, entities and comparability. Removed types or relationships, changed identity rules or changed endpoint contracts make the comparison structurally incompatible. Replay counts are still shown, explicitly separate from semantic accuracy. Mapping a new generation verifies candidate mapped claims before storing supported relationships. Neither model verdicts nor structural checks establish a measured accuracy rate.

Rebalance operates on the supplied full corpus, not every file in a historical export directory. It writes an isolated generation; activation is separate and verifies corpus identity and stored counts. Previous generations and the pre-rebuild graph remain retained. A failed extraction run is incomplete, retains successful artifacts and can resume. A failed projection has no finished-generation marker. Each attempt has a fresh scope; retries cannot clear a scope another caller finished. Activation compares the current head with the starting head in its write transaction, rejecting stale publication.

## Evidence and limits

Original text is retained verbatim with a SHA-256 digest. Quotes carry UTF-16 offsets into that exact stored text. Recovered legacy text must identify its provenance; joining old chunks does not prove the original bytes. Full bookmark articles must come from their original source, not the legacy truncated snippets.

Processing windows are sized from the selected provider model's advertised context and output limits, conservatively counting UTF-8 bytes of encoded text. They cover the entire source and are not graph nodes. There is no fixed passage length or entity count. Interrupted, truncated or invalid model output fails the document. Exact quote and full input coverage checks do not prove exhaustive model recall; context across a transport boundary can still affect extraction.

Search combines source/title BM25, exact alias lookup and claim embeddings. Jev verifies observations and mapped relationships; GLM writes extractions and ontology drafts. Embeddings support retrieval without turning retrieval text into knowledge entities. The historical ranking gold set remains available to measure the new retrieval after the corpus is loaded.

## Commands and implementation

extract replaces link/name. ontology draft/revise replaces generated categories and the old model maintenance flow. ingest, docs, show, entities, search and walk read the new generation. Old prune, resolve, taxonomy and bookmark-specific import commands are removed; acquisition supplies complete source JSON. The separate SQLite reader is unchanged. See proofs/README.md for the bounded safety claims and source mapping.

knowledge.ts owns observation schemas and deterministic mapping. extraction.ts owns source loading, extraction artifacts and model verification. graph.ts owns immutable graph generations, revision comparison and readers. Models and Helix remain the external service boundaries.

```sh
bunx tsc --noEmit
python3 scripts/prove.py /path/to/everystate-source
bun scripts/rank-eval.ts rebuilt --baseline baseline
```

A full rebuild is not verified until the source inventory reconciles, live extraction and mapped-claim verification run, the stored graph passes checks, and an independent review passes at the proved and reviewed commit.
