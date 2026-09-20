# kb — the high-quality graph pipeline

One tool: `~/projects/kb/kb.ts` (bun, `@helix-db/helix-db` v3). Helix v4 on `localhost:6972`, tenant `example`.
This file is the design, the numbers behind it, and the runbook. `kb` with no arguments prints usage.

## Shape

```
User ─OWNS→ SourceDocument ─HAS_CHUNK→ Chunk ─MENTIONS{confidence ≥ 0.9, by}→ Entity ─ASSOCIATED_WITH{passages}→ Entity
User ─HAS_PROFILE→ UserProfile               Chunk ─IN_CATEGORY→ Category ←PARENT_OF─ Category(isParent)
SourceDocument ─MENTIONS{by:"consolidated"}→ Entity      (document-wide facts)
Chunk ─RELATES_TO | EXTENDS | UPDATES{confidence, by:"jev"}→ Chunk      (passage crosslinks)
Chunk props (Jev): category, salience, aboutOwner, boilerplate, needsNaming, linkVersion
```

### Helix's memory-system model, and where each piece stands here

The unit of knowledge is the **Chunk** (atomic `Memory` nodes were built, then dropped on example's call, 2026-09-19).

| Helix model | Here | Built by |
|---|---|---|
| `SourceDocument`, `Chunk`, `HAS_CHUNK` | same | `kb ingest` |
| `Entity`, `MENTIONS` | Chunk→Entity (specific) and SourceDocument→Entity (document-wide) | `kb link`, `kb name`, `kb consolidate` |
| `Category`, `IN_CATEGORY`, `PARENT_OF` | 44 leaves under 10 parents (`isParent: true`; passages are only filed under leaves) | `kb taxonomy --seed`, `kb model` |
| `RELATES_TO` (unit ↔ unit) | Chunk↔Chunk across documents: the graph proposes pairs sharing 2+ walkable entities (3 strongest per passage), Jev decides ≥0.9 | `kb relate` |
| `EXTENDS`, `UPDATES` | Jev types each related pair: two records of one event → the fuller document's passage `EXTENDS` the other; a newer statement `UPDATES` the older | `kb model` |
| `User`, `OWNS`, `UserProfile`, `HAS_PROFILE` | User(example) owns every document; profile = static facts + current focus, written by GLM from the densest passages Jev marked as about the owner (`kb profile` prints it) | `kb model` |
| `DERIVES` | not built — nothing here is inferred from other memories | — |
| `Session`, `DERIVED_FROM`, `Connector`, `IngestionJob` | not applicable — no chat sessions, one local tool | — |
| *(not in Helix's model)* Entity↔Entity | `ASSOCIATED_WITH{passages, share}`: 3+ shared passages and ≥30% of the rarer entity's passages; counted, no model | `kb model` |

| Who | Does | Why |
|---|---|---|
| Helix | stores, vector + BM25 search, proposes candidate entities from the 8 most similar passages | a bare name holds no context, so names are never embedded or compared |
| Jev (`typesafe/jev-1.13` via OpenRouter; Cloudflare gateway `example-dev` as the alternative) | every decision: is X discussed here, category, salience, about example, boilerplate, is something unnamed, is a name node-worthy, are two names one thing, does a passage answer the query | typed answers with probabilities, $0.042 / 1M tokens, ~2 s |
| GLM (`z-ai/glm-5.3-flash`, OpenRouter, `reasoning: minimal`) | the only writing: names entities Jev flagged as missing, drafts the taxonomy | Jev cannot write text. (Auditing Jev's links is done by Claude or sol from `kb audit`'s sample file, not by GLM) |

## Ingest (per document, one command: `kb ingest <dir>`) — a document enters the graph already clean

0. **copy check** — every passage has a `contentHash` (whitespace-collapsed text, sha256). If another document already
   holds every passage of this one, it is skipped as a copy.
1. chunk (4,000 chars) → embed (`text-embedding-3-small`) → one write: document + chunks.
2. **link** — Helix shortlists ≤30 candidate entities per chunk (its 8 nearest chunks' links + their documents'
   rosters + its own links); Jev answers per chunk: category, salience, owner, boilerplate, "is something discussed
   that is not in the list", and one yes/no per candidate. 6 passages per request. **Candidate names appear only in
   the questions, never in the passage state.** A mention is written only at **confidence ≥ 0.9** (`MENTION_FLOOR`).
3. **name** — only for chunks Jev flagged: GLM lists what is missing (12 passages a call; its prompt carries Jev's
   tests verbatim; a name that exists under any kind is reused). Jev checks every proposed name in its passage:
   `discusses ≥ 0.9`, `about ≥ 0.4`, and for **topics** `node_worthy ≥ 0.8` (other kinds ≥ 0.4).
4. **settle** (`settleDocument`) — every link gets its second reading (mean of the two ≥ 0.9 and `about ≥ 0.4`);
   an entity on ≥50% of the passages moves to one document link; passages are crosslinked (`RELATES_TO` / `EXTENDS` /
   `UPDATES`, typed by Jev) to passages of other documents sharing 2+ walkable entities.

Measured on a 4-passage document: 0.4 min, $0.001, 8 links → 5 specific + 1 document-wide, 1 refuted.

Jev runs through OpenRouter (`POST https://openrouter.ai/api/alpha/decisions`, `typesafe/jev-1.13`, same request
shape, same $0.042/1M; it is not in OpenRouter's `/models` list). `KB_JEV_ROUTE=cloudflare` switches to the gateway.

## What an entity is (the tagging rules)

- **Named things only**: person, organisation, product, project, place — and a *topic* only when it is a concept with a
  recognised name ("NDAs", "payback period", "jobs to be done"). Broad themes are the 44 categories' job, and search's.
- **Discussed, not mentioned**: the passage gives information about it. A list that IS the information (tracklist,
  line-up, participants, bibliography) counts. A person only speaking / addressed / credited does not.
- **A document-wide fact lives on the document**: an entity on ≥50% of a document's passages (4+) is ONE
  `SourceDocument -MENTIONS-> Entity` link (`by: "consolidated"`); `--entity` filters look at both levels.
- **A link passes twice**: `kb confirm` re-judges every link in a focused request; the mean of the two readings must be ≥ 0.9.
- **About, not just named**: the second reading also asks whether the passage is ABOUT the entity (`about ≥ 0.4`).
  Famous names dropped in passing ("everything you'd learn from Harvard", "a blend of Robert Redford or Robert Goulet")
  score 0.9+ on "discusses" twice over and 0.06-0.40 here; tracklist items and real subjects stay high.

## Rules that came out of measuring (do not undo without re-measuring)

Experiments are kept in `backup/experiment-*.ts`; eval runs in `eval.history.jsonl`; audits in `backup/audit*.`

- **Names never go in the passage state.** v2 listed each passage's candidate names inside its state object and packed
  ~13 passages a request. Jev then "found" those names: links whose name is absent from their passage scored ≥0.8 in
  production, 0.30 when judged alone, 0.38 with candidates in the state (pack 1), 0.56 (pack 8). 30,774 of 117k named
  links had a name that appears nowhere in the passage or title. (`experiment-candidates-in-state.ts`)
- **Packing itself is fine.** Names out of the state: pack 1 / 2 / 4 / 8 → false-link score 0.30 / 0.30 / 0.31 / 0.33;
  decision flips vs pack 1 are 11-17 of 120 = Jev's own run-to-run noise. (`experiment-pack-size.ts`) PACK = 6.
- **Jev is noisy near the floor.** Of 150 links at ≥0.9, 85 stayed ≥0.9 when asked again alone → `kb confirm`, mean of two.
- **Speakers are not subjects.** Of 150 links to the 25 most-linked people, the speaker-aware definition keeps 63 vs 85.
  (`experiment-speaker-links.ts`)
- **Topics were the bulk of the waste.** 11,623 topic entities held 55% of all links; most were ad-hoc phrases from the
  old document-level LLM extraction ("choosing what business to start"). Jev's `node_worthy` in the passage: ≥0.8 reads
  as real named concepts, <0.4 as phrases, 0.4-0.8 mixed → topics need ≥0.8; 9,750 deleted. The same test is NOT
  reliable for people ("Jeremy Howard" 0.22 in a passing mention), so it decides topics only.
- **Check for copies before anything else.** A third of the corpus was the same documents ingested twice; every pass
  paid for them. `kb dedupe` compares passages by a hash of whitespace-collapsed text (identical, not similar).
- **Same name = same node.** The entity key carried the kind, so "PyHarness" existed as a project and a product. Shown
  the two kinds Jev calls them different (0 of 425 reached 0.9), so identical normalised names merge without asking and
  `kb name` reuses an existing name under whatever kind it has. Jev decides aliases ("RAG" = "retrieval-augmented generation").
- **Lists are information.** "A name in a list is not discussed" stripped DJ tracklists of their artists (33 → 400
  linked artists after the definition said a tracklist / line-up / participants list counts).
- **Hubs are not walked.** An entity held by more than 60 passages says nothing about any one of them. Graph expansion
  skips them and weights a shared entity by `1 / log2(1 + passages holding it)`. Hubs still work as filters.
- **LLM output is never trusted unchecked.** GLM proposes ~3x more names than survive Jev's check.
- **Edges are deleted by id.** `.drop()` is a node mutation; on an edge stream Helix v0.0.5 silently does nothing
  (filed: HelixDB/helix-db#1109). Use `g().dropEdgeById(ids | EdgeRef.var(name))`. Node `.drop()` cascades its edges.
  `outN()` of an out-edge is its target, `inN()` its source.
- **Throughput.** Cloudflare's gateway refuses past ~250 requests/min ("Wholesale rate limit", code 2018); OpenRouter
  took 40 requests/s without refusing (latency climbs; ~8/s effective from Lagos). With Jev unthrottled the bound is
  Helix: one core, reads and writes share it → bulk passes use an in-memory pool (links + rosters loaded once; Helix is
  asked only for nearest chunks) and early-acknowledged writes with one durable barrier at the end.
- Helix limits: ≤512 text-indexed node mutations per write; index DDL cannot share a batch with data; one writer;
  a full scan of every link is 20–90 s, so `kb check` runs its scans one after another.

## Hygiene runbook (everything is a dry run until `--yes`, and works from the newest backup)

```bash
kb backup                      # ~3 min: entities, links with endpoints, judged chunk props → backup/<stamp>/*.jsonl
kb check                       # invariants; exit 1 if one is broken
kb audit --sample 200          # random links + passages → file, for an independent reviewer (Claude / sol — not GLM)
kb prune --yes                 # links under the floor, stale, duplicate pairs; orphan entities        no model calls
kb vet --yes                   # Jev: unchecked LLM links; node-worthiness of every topic + thinly-held entity
kb confirm --yes               # Jev: every link a second time, focused; mean of two readings ≥ 0.9
kb consolidate --yes           # document-wide entities → one document link                         no model calls
kb dedupe --yes                # documents that are 100% identical-passage copies of another       no model calls
kb relate --yes                # passage↔passage RELATES_TO: graph proposes, Jev decides
kb model --yes                 # owner + profile (1 LLM call), category parents (1 LLM call), EXTENDS/UPDATES (Jev), entity associations
kb entities --resolve --yes    # co-occurrence / same slug proposes duplicates, Jev decides (≥0.9), merge; aliases kept
bun eval.ts "<note>"           # 20 queries → eval.history.jsonl
```

After any `--yes` run the backup it used is out of date: take a new one before the next hygiene command.
Full restore: `backup/helix-volume-2026-09-20-preprune.tgz` is the whole Helix volume before any of this.

## History

| When | What | Links | Entities |
|---|---|---|---|
| 2026-09-19 | v2 full run (names in state, pack 13): Jev $2.59, GLM $0.91 | 462,799 | 28,338 |
| 2026-09-20 | prune <0.8, stale, duplicates | 240,013 | 28,338 |
| 2026-09-20 | vet GLM links + thin entities ($0.32) | 219,918 | 21,412 |
| 2026-09-20 | **v4 re-link** (names only in questions, pack 6; Jev $3.3 incl. trials, GLM $0.75) at floor 0.85 | 185,954 | ~24k |
| 2026-09-20 | floor 0.9 | 163,668 | ~24k |
| 2026-09-20 | topics must be node-worthy ≥0.8 ($0.39): −9,750 topics | 90,771 | 15,095 |
| 2026-09-20 | consolidate: 2,770 document-wide pairs, −16,277 passage copies | 74,494 | 15,095 |
| 2026-09-20 | confirm: every link judged twice, mean ≥0.9 ($0.88): −22,888 | 51,605 | 15,095 |
| 2026-09-20 | resolve: same name = one node (1,380 pairs) + 101 Jev-confirmed aliases → 1,216 merges | 46,857 | 13,622 |
| 2026-09-20 | **dedupe: 1,270 documents were 100% passage-for-passage copies** (old "recovered" store) → 2,659 docs · 17,028 passages | 30,772 | 13,622 |
| 2026-09-20 | **about-test** on every link ($0.58): −9,780 (7,704 only mentioned in passing, 3,539 failing the mean) → 20,992 | 20,992 | ~10,400 |
| 2026-09-20 | (before that) prune 3,221 orphan entities; relate: 4,613 pairs judged ($0.42) → 332 RELATES_TO, 290 EXTENDS, 4 UPDATES; 2,634 entity associations; owner + profile + 10 category parents | 30,772 | ~10,400 |

Search eval (graph-reached passages; grading method changed on 09-20 to one passage per request, so compare each
run's `graph` to its own `next`): 09-19 0.51 vs 0.67 · 09-20 (220k links) 0.63 vs 0.67, 61 fresh · 09-20 (74k links)
0.60 vs 0.74, 54 fresh, +540 ms/query.
Final, 09-20 (2,659 docs, 30,772 links): top 0.88 · next 0.77 · graph 0.60 (81/129 useful, 43 plain search missed).
Latency of the graph step is inside Jev-rerank noise: timed idle, a search took 6.8 s with the graph and 9-12 s
without it on the same queries; the eval's per-query "added latency" column is not reliable while anything else runs.
`kb check` passes: 0 unjudged, 0 under the floor, 0 unchecked LLM links, 0 orphans; 1.8 links per passage.

## Audit (Claude, 2026-09-20, 40 random links of the 30,772-link graph, read by hand)

30 hold · 6 are passing mentions (Harvard, Slack, Prague, Robert Goulet, Truett Cathy, the generic phrase "money
models") · 4 could not be judged because the sample file truncated passages (fixed: `kb audit` now writes whole
passages with edge and chunk ids). Those 6 all scored 0.92-0.97 on "discusses" — the reason the `about` test exists.
`backup/experiment-passing-mentions.ts`: with `about ≥ 0.4`, 27/30 good links kept, 20/20 tracklist links kept, 2/6
passing kept.

**Re-audit after the about-test (Claude, 40 random links of the 20,997-link graph, whole passages):** 34 hold (85%) ·
4 weak (two "Org: Example Research" meeting headers, COVID-19 as the backdrop of a story, MTN as one of three names in a
partner list) · 2 wrong (5%: acquisition.com and value-based pricing, neither named nor discussed in the passage).
Before the about-test: 75% held, 15% wrong. Sample: `backup/audit-sample-2026-09-20-18-03.jsonl`.

## Open

1. The 2 wrong links in the re-audit were both absent-name links surviving two readings — worth a targeted look (links whose name appears nowhere in the passage, non-topic kinds).
2. (closed) Ranking boost from the graph was tested and removed: top-8 0.845 plain vs 0.842 boosted over 20 queries, 16 of 160 slots changed (`eval-rank.ts`, 2026-09-20). Ranking stays vector + BM25 + Jev rerank; the graph filters and cross-references.
3. 53 documents are 80-99% copies of another; ~4k entities are held by one passage; the in-memory pool is untimed.
4. Move the pipeline (model calls + orchestration) to a Railway service or Cloudflare Worker.
