# Exocortex

A headless knowledge graph for everything you read, write and save: documents, transcripts, notes and X bookmarks.
Obsidian without the app, built for people *and* agents. HelixDB holds it all in one database (entity graph, BM25 and
vectors); `exo` is the CLI that fills it and answers from it.

Embeddings don't find names: a nonsense query sat at cosine distance 0.30 from the corpus, the passages that really
named the person at 0.30-0.33, and unrelated posts at 0.32. So Exocortex builds an entity graph. Jev decides which
people, products and topics each passage is really about, a link is written only at confidence 0.9 and read twice,
and a short query walks that graph and fuses it with BM25; longer questions add vector search.

![Explainer: on a cosine-distance axis a nonsense query (0.30), real passages (0.30 to 0.33) and unrelated posts (0.32) overlap, so no cutoff works; then Jev says yes or no to Helix's candidate entities for the fixture notes, MENTIONS edges are drawn, and a search for "solar greenhouse" walks them to two passages](docs/media/hero.gif)

## Install

Bun 1.3, Docker, and the [Helix CLI](https://github.com/HelixDB/helix-db) (`helix` 3.x).

```bash
git clone https://github.com/Konan69/exocortex && cd exocortex
bun install --frozen-lockfile
cp .env.example .env              # owner, tenant; OPENAI_API_KEY (query vectors) and OPENROUTER_API_KEY (Jev, GLM) are read only by the commands that call a model
cp helix.example.toml helix.toml  # the local Helix project; exo runs `helix start dev` when Helix is down
bun exo.ts taxonomy --seed --yes  # GLM drafts the categories once; ingest files passages under them
bun exo.ts ingest fixtures/notes  # chunk, embed, link, name, settle
bun exo.ts search "solar greenhouse"
```

Run `exo` from the repo directory: Bun loads `.env` from there. Setting `HELIX_URL` points exo at a Helix you run
yourself and turns the auto-start off. After a reboot the first queries are slow (tens of seconds) while Helix loads
its data from object storage, then fast.

```bash
bun exo.ts search "agent memory" --kind x_bookmark   # names and short lookups: entity graph + BM25
bun exo.ts search "how should I price a retainer"    # questions add vector search
bun exo.ts --help                                     # every command; `bun exo.ts <command> --help` for flags
```

## Commands

| Ask | Command |
|---|---|
| Find passages | `search "<q>" [filters] [--mode graph\|hybrid\|vector\|text] [--top N] [--page N] [--chunks] [--expand N] [--rerank] [--json]` |
| Walk the graph | `walk "<entity>" [--hops N] [--to "<entity>"] [--top N] [--docs N] [--json]`: neighbours by `ASSOCIATED_WITH`, hops out, the documents it runs through, the fewest-hop path to another entity |
| Any query | `schema` (labels, edges, indexes, counts), then `query --cypher '<stmt>'` (`$tenant` bound), `query --ts 'readBatch()…'` (the project's SDK, `TENANT` bound) or `query --file q.json`; writes need `--write` |
| Browse | `docs`, `show <documentId>`, `entities`, `categories`, `stats`, `profile`, `taxonomy` (`--seed` drafts it) |
| Add documents | `ingest <dir>` (JSON source files), `bookmarks` (X bookmarks), `bookmarks --graph` (link the ones with articles) |
| Repair linking | `link`, `name` (stages 1 and 2 over chunks that missed them) |
| Keep it honest | `backup`, `check`, `prune`, `dedupe`, `forget <file>`, `entities --resolve`, `entities --index-names`, `model` |

Filters (repeat a flag for OR, different flags AND): `--kind`, `--source`, `--doc`, `--category`, `--mine` (about
the owner), `--min-salience 0.6`, `--where 'field=v'` (`!=`, `~` contains, `^` prefix), `--with-boilerplate`, `--entity
"name"` (repeat for ANY, add `--all-entities` for EVERY).

Every command that changes the graph is a dry run until `--yes` (except `ingest`, `link`, `name` and `bookmarks`,
which write unless `--dry-run`). Hygiene commands work from the newest `exo backup`; take a new one after any `--yes`.

## Shape

```
User ─OWNS→ SourceDocument ─HAS_CHUNK→ Chunk ─MENTIONS{confidence, by}→ Entity ─ASSOCIATED_WITH{passages}→ Entity
User ─HAS_PROFILE→ UserProfile          Chunk ─IN_CATEGORY→ Category ←PARENT_OF─ Category
SourceDocument ─MENTIONS{by:"consolidated"}→ Entity            (document-wide facts)
Chunk ─RELATES_TO | EXTENDS | UPDATES→ Chunk                    (passage crosslinks, typed by Jev)
EntityName                                                       (BM25 shadow of Entity.name, see Open)
```

The unit of knowledge is the Chunk (a passage of ≤4,000 characters). Atomic `Memory` nodes were built, measured and
dropped (2026-09-19).

| Who | Does |
|---|---|
| Helix | stores, indexes, vector + BM25 search, proposes candidate entities from a chunk's 8 nearest chunks |
| Jev (`typesafe/jev-1.13`, OpenRouter decisions API, $0.042 / 1M tokens) | every judgement: is X discussed here, category, salience, about the owner, boilerplate, is something unnamed, is a name node-worthy, are two names one thing, does a passage answer a query |
| GLM (`z-ai/glm-5.3-flash`, OpenRouter, reasoning minimal) | the only writing: names what Jev flagged as missing; drafts the profile and category parents |
| OpenAI `text-embedding-3-small` | 1536-dim chunk and query vectors |

## Code

Effect 4 throughout. `exo.ts` builds the layers and runs the CLI.

| File | Owns |
|---|---|
| `src/domain.ts` | owner and tenant (from `.env`), ids, entity kinds, the measured floors, normalisation, chunking, row schemas |
| `src/helix.ts` | the `Helix` service: Schema-decoded reads, one serialized writer, retries, index bootstrap, `helix start dev` |
| `src/ai.ts` | the `Models` service: Jev on `effect/ai` `DecisionModel`, GLM on `LanguageModel`, OpenAI on `EmbeddingModel`; concurrency, pacing, retries, usage |
| `src/queries.ts` | traversals and filters shared by several commands |
| `src/pipeline.ts` | ingest, link, name, settle; the in-memory pool and LinkWriter (coalesced link writes, ≤60 chunks a commit) |
| `src/bookmarks.ts` | the X bookmark import and graph pass |
| `src/search.ts` | hybrid graph search |
| `src/browse.ts` | read-only views |
| `src/walk.ts` | `walk`, `query`, `schema`: entity neighbourhoods, paths, raw Cypher / SDK / JSON queries |
| `src/maintain.ts` | backup, check, prune, dedupe, forget, entity resolution, the profile model |
| `src/cli.ts` | commands and flags |

Configuration is `.env` (see `.env.example`).
`bun scripts/smoke.ts` proves the Helix, Jev, GLM and embedding seams live. `scripts/rank-eval.ts` scores search against
the gold set; `scripts/eval.ts` asks Jev whether the graph's connected passages earn their place.

## Ingest: a document enters the graph already clean

0. **Copy check.** Every passage has a `contentHash` (whitespace-collapsed text, sha256). A document whose every passage
   another document already holds is skipped.
1. **Chunk, embed, write** the document and its chunks in one write.
2. **Link.** Helix shortlists ≤30 candidate entities per chunk (its 8 nearest chunks' links, their documents' rosters,
   its own links). Jev answers per chunk: category, salience, owner, boilerplate, "is something discussed that is not
   in the list", and one yes/no per candidate, 6 passages a request. A mention is written at confidence ≥ 0.9.
3. **Name**, only for chunks Jev flagged: GLM lists what is missing (12 passages a call); Jev checks every proposed
   name in its passage (`discusses ≥ 0.9`, `about ≥ 0.4`, topics `node_worthy ≥ 0.8`, other kinds ≥ 0.4).
4. **Settle.** Every link gets a second reading (mean of the two ≥ 0.9, `about ≥ 0.4`); an entity on ≥50% of a
   document's passages becomes one document link; passages are crosslinked to passages of other documents that share
   2+ walkable entities, typed by Jev.

Measured on a 4-passage document: 0.4 min, $0.001.

**X bookmarks** (from [Field Theory](https://fieldtheory.dev)). `exo bookmarks` makes one document and one chunk per bookmark (post, quote, links, up to 3,000
characters of article text; Jev label category, `signal / 3` salience) with no linking. `exo bookmarks --graph` links,
names and settles only the bookmarks whose article text exceeds 200 characters: linking one-line posts mints spurious
entities. 105 articles cost ~$0.03 and 4 minutes.

## Search

Every query runs every list, and reciprocal-rank fusion (k = 60) joins them per document (per passage with
`--chunks`); each list counts a document's best rank once. Each list ranks 60 candidates.

| List | What it ranks |
|---|---|
| `G` graph | entities the query names: a phrase of the query that is exactly an entity's name, or a name holding every word of the query ("georgiy" finds Georgiy Sekretaryuk). Their passages, ranked by BM25 on the query within them |
| `~` vector | passages nearest the query embedding (needs `OPENAI_API_KEY`; skipped with a note without it) |
| `=` / `≈` keyword | BM25 over passages: the query as typed, and the query with every word's inflections (Helix BM25 does not stem, so "transcribing" never meets "transcribes") |
| `T` / `t` title | BM25 over document titles, plain and inflected; a title says what a document is about ("Georgiy <> Konan: 1-1", a bookmark's post text) |

`--mode graph|text|vector` leaves lists out to compare. Jev rerank is off unless `--rerank` (one model call per
candidate). Each hit is printed with what it is about, the passages connected through shared entities, and the passages
Jev linked to it.

**Measured** with `bun scripts/rank-eval.ts <label>` against `fixtures/eval/gold.jsonl` (40 queries: names, topics,
questions, half-remembered posts; 1,121 graded documents), run through the CLI. 2026-11-15, vector list off:
nDCG@10 0.547 → 0.734, recall@10 0.443 → 0.567, MRR 0.740 → 0.938. What moved it: entities only when the query names
them (0.583), BM25 within an entity's passages (0.597), the inflected keyword list (0.610), the title lists (0.686,
0.696), 60 candidates per list (0.734). Tried and reverted: prefiltering boilerplate (no change, 3x slower), inflections
folded into one query (0.562), RRF k = 20 (a peak beside a cliff at k = 10), summing every passage of a document (0.642),
one keyword vote (0.665).

## Rules that came out of measuring (do not undo without re-measuring)

Eval runs are in `eval.history.jsonl` (`bun scripts/eval.ts "<note>"`).

- **What an entity is.** Named things only: person, organisation, product, project, place, and a topic only when it
  is a concept with a recognised name ("NDAs", "payback period"). Broad themes are the categories' job.
- **Discussed, not mentioned.** The passage gives information about it. A list that IS the information (tracklist,
  line-up, participants) counts: "a name in a list is not discussed" stripped DJ tracklists of their artists (33 → 400
  linked artists after the change). A person only speaking, addressed or credited does not: of 150 links to the 25
  most-linked people, the speaker-aware definition keeps 63 vs 85.
- **About, not just named.** Famous names dropped in passing ("everything you'd learn from Harvard") score 0.9+ on
  "discusses" twice over and 0.06-0.40 on "about"; tracklist items and real subjects stay high. With `about ≥ 0.4`,
  27/30 good links kept, 20/20 tracklist links kept, 2/6 passing mentions kept.
- **Names never go in the passage state.** Listing each passage's candidate names inside its state made Jev "find"
  them: links whose name is absent from the passage scored ≥0.8 in production, 0.30 when judged alone. Names live only
  in the questions. Packing itself is fine: pack 1/2/4/8 → false-link score 0.30/0.30/0.31/0.33. PACK = 6.
- **Jev is noisy near the floor.** Of 150 links at ≥0.9, 85 stayed ≥0.9 when asked again alone: every link is read
  twice and the mean must reach 0.9.
- **Topics need a recognised name.** 11,623 topic entities held 55% of all links, mostly ad-hoc phrases. `node_worthy`
  ≥0.8 reads as real named concepts, <0.4 as phrases; 9,750 deleted. The same test is not reliable for people
  ("Jeremy Howard" 0.22 in a passing mention), so it decides topics only.
- **Check for copies before anything else.** A third of the corpus was the same documents ingested twice. Copies are
  identical passages, not similar ones. A document's first passage under 500 characters is a header (title, speaker,
  URL), written differently by two ingests of the same video: 51 of 53 near-copies differed only there. A second test
  on running text (8-word shingles, ≥80% contained, equal-standing pairs must agree on title) catches the same text cut
  differently: 197 documents.
- **Resolution runs on its own.** `bookmarks-refresh` ends with `backup`, `entities --resolve --yes`, `backup`, and
  `exo check` fails while proposed pairs wait for Jev. Verdicts live in one `backup/resolve.jsonl` across backups, so a
  pair is judged once. A person named by part of their name ("Georgiy" for Georgiy Sekretaryuk) is proposed with that
  person; a part-name two full names claim is left alone. `exo backup` compares its rows with live counts and removes
  itself when short (a backup taken right after a large merge once came back with 13,597 of 21,287 links).
- **Same name = same node.** Identical normalised names merge without asking; Jev decides aliases ("RAG" =
  "retrieval-augmented generation"). Aliases are the pairs that never co-occur (a writer picks one spelling per
  passage), so they are proposed by squashing spacing and punctuation and accepted at 0.8: of 33 such pairs only 9
  reached 0.9 while every one from 0.82 up was right by hand.
- **A name missing from its passage is not a wrong link.** 3.2% of links contain no word of the entity's name; of 37
  read by hand, 33 hold, 4 weak, 0 wrong (transcripts garble names, old translations spell them their own way, roles
  stand in for names). A "the name must appear" rule was rejected.
- **Hubs are not walked.** An entity held by more than 60 passages says nothing about any one of them. Expansion skips
  them and weights a shared entity by `1 / log2(1 + passages holding it)`. Hubs still work as filters.
- **LLM output is never trusted unchecked.** GLM proposes ~3x more names than survive Jev's check.
- **The graph ranks only through entities the query names** (see Search). A boost from every entity a passage shares
  with the query was tested and removed in 2026-09 (top-8 0.845 plain vs 0.842).

Quality, last measured (2026-09-20, 2,595 docs, 20,509 links): search top-8 0.885, next-8 0.772, graph-reached 0.640
(74/112 useful, 43 that plain search never reaches). Hand audit of 40 random links: 34 hold, 4 weak, 2 wrong.

## Helix facts

- Edges are deleted by id: `.drop()` on an edge stream silently does nothing on v0.0.5 (HelixDB/helix-db#1109). Use
  `g().dropEdgeById(...)`. Node `.drop()` cascades its edges. `outN()` of an out-edge is its target, `inN()` its source.
- ≤512 text-indexed node mutations per write; index DDL cannot share a batch with data; one writer (concurrent writes
  answer 409, so `Helix.write` serializes them); a full scan of every link is 20-90 s.
- Storage is SlateDB on the SeaweedFS bucket `helix-db` (CLI 3.7+, image v0.0.16; older MinIO volumes need the copy in Helix's "Migrate MinIO disk data" guide). Text and vector indexes live there too.

## Open

- **Helix v0.0.5 could not build a text index over a populated label** (`Entity.name`, 2026-09-28: looped at
  `validate_manifests` past 8,000 attempts). The `EntityName` shadow label carries the BM25 index instead. On v0.0.16
  the `SourceDocument.title` index built over 4,040 existing documents in ~3 s (2026-11-15), so the shadow can likely
  go; not tried for `Entity.name` yet.
- **2,650+ entities are held by exactly one passage.** Mostly real and specific, with a junk tail. They can never be
  walked or join two passages; their only job is to be a label you can filter by. Cutting them cuts true information,
  so it is the owner's call.
- Sources are JSON files (`id`, `title`, `content`, `metadata.kind|source|url`; see `fixtures/notes`); acquisition
  (downloaders, exporters) lives outside exo and writes those files for `exo ingest`.

## License

MIT
