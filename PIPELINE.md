# kb — the high-quality graph pipeline

One tool: `kb.ts` (Bun, `@helix-db/helix-db` v3). Set `HELIX_URL` to your own running Helix instance.
The CLI never starts containers. See `config.example.sh` for environment configuration.
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

The unit of knowledge is the **Chunk**.

| Helix model | Here | Built by |
|---|---|---|
| `SourceDocument`, `Chunk`, `HAS_CHUNK` | same | `kb ingest` |
| `Entity`, `MENTIONS` | Chunk→Entity (specific) and SourceDocument→Entity (document-wide) | `kb link`, `kb name`, `kb consolidate` |
| `Category`, `IN_CATEGORY`, `PARENT_OF` | 44 leaves under 10 parents (`isParent: true`; passages are only filed under leaves) | `kb taxonomy --seed`, `kb model` |
| `RELATES_TO` (unit ↔ unit) | Chunk↔Chunk across documents: the graph proposes pairs sharing 2+ walkable entities (3 strongest per passage), Jev decides ≥0.9 | `kb relate` |
| `EXTENDS`, `UPDATES` | Jev types each related pair: two records of one event → the fuller document's passage `EXTENDS` the other; a newer statement `UPDATES` the older | `kb model` |
| `User`, `OWNS`, `UserProfile`, `HAS_PROFILE` | User(owner) owns every document; profile = static facts + current focus, written by GLM from the densest passages Jev marked as about the owner (`kb profile` prints it) | `kb model` |
| `DERIVES` | not built — nothing here is inferred from other memories | — |
| `Session`, `DERIVED_FROM`, `Connector`, `IngestionJob` | not applicable — no chat sessions, one local tool | — |
| *(not in Helix's model)* Entity↔Entity | `ASSOCIATED_WITH{passages, share}`: 3+ shared passages and ≥30% of the rarer entity's passages; counted, no model | `kb model` |

| Who | Does | Why |
|---|---|---|
| Helix | stores, vector + BM25 search, proposes candidate entities from the 8 most similar passages | a bare name holds no context, so names are never embedded or compared |
| Jev (`typesafe/jev-1.13` via OpenRouter; Cloudflare gateway configured with `KB_AI_GATEWAY` as the alternative) | every decision: is X discussed here, category, salience, about the owner, boilerplate, is something unnamed, is a name node-worthy, are two names one thing, does a passage answer the query | typed answers with probabilities, $0.042 / 1M tokens, ~2 s |
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

## X bookmarks (`kb bookmarks`) — cheap index, article-only graph

The basic import still skips linking, naming and settling. Each non-tombstoned bookmark in Field Theory's JSONL becomes one `SourceDocument` `x:<id>` plus one `Chunk` `x:<id>:0`, kind `x_bookmark`, source `x`. Its text includes post, quote, links and up to 3,000 characters of ft SQLite `article_text`; Jev label category, `signal / 3` salience, folders and postedAt are stored on the chunk. `--replace` leaves unchanged chunks in place (including their links); if a graph-linked article's content changes, it warns to rerun `--graph --redo`.

`kb bookmarks --graph [--dry-run] [--limit N] [--redo]` selects **only** source JSONL bookmarks absent from the tombstone file whose ft `article_text` exceeds 200 characters and whose chunk is indexed. It runs the normal `linkChunks` (Helix candidates → Jev, mention floor 0.9), `nameChunks` (flagged only: GLM proposes → Jev vets), then `settleDocument` for each selected article. LINK_VERSION skips previously linked chunks unless `--redo`; `--limit` caps this pass. One-line posts stay out: linking banter mints spurious entities.

Bookmark-only search defaults to `--mode graph`: Helix BM25 over `EntityName.name` (plus exact entity keys) ranks up to 8 names, then equality-index mapping `entityKey → Entity` reaches passages via `Chunk ←MENTIONS Entity` and `SourceDocument ←MENTIONS Entity → HAS_CHUNK`, respecting chunk filters. Passages accumulate weight from matched entities, then graph and Chunk.content BM25 lists fuse via app-side RRF (+0.3 × salience); `via` shows `G`, `=`, or both. `expandChunks` still walks the hits. The graph holds what saved things are about; keyword catches wording with no entity. No query embeddings or Jev rerank on this path; other searches still default to hybrid. **Since 2026-10-12:** every lookup of three words or fewer takes this graph + BM25 path (a bare name gives an embedding little context); it falls back to vector only when both lists are empty. Longer queries run graph + BM25 + vector. Jev rerank is off unless `--rerank` (one model call per candidate on every search); read further with `--page N`. `kb dedupe` has a second test on running text (8-word shingles, timestamps stripped, ≥80% contained, equal-standing pairs must also agree on title) for the same text cut differently: 197 documents removed, 4096 → 3899. Explicit `--mode text|vector|hybrid` remains available. Missing EntityName BM25 is a hard error with the backfill command, not a silent contains-scan fallback.

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

The measurements below describe the original private corpus. Its data, experiments and audit files are not distributed. New eval runs append to `eval.history.jsonl`.

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
- **The header passage is not content.** After the exact-copy sweep, 53 documents still sat at 80-99% identical, and
  every one of them missed a full match by a single passage. In 51 of the 53 that passage was the *first* one — the
  metadata block (title, speaker, source URL), written differently by the two ingests of the same video. Headers ran
  134-216 characters; the only first passage carrying real text was 3,801, so the copy test ignores a document's
  first passage when it is under 500 characters. That found 65 documents (638 passages) whose every content passage
  already existed elsewhere, and left alone the two that differ mid-document and the blog post with a long opening.
- **Same name = same node.** The entity key carried the kind, so "ExampleTool" existed as a project and a product. Shown
  the two kinds Jev calls them different (0 of 425 reached 0.9), so identical normalised names merge without asking and
  `kb name` reuses an existing name under whatever kind it has. Jev decides aliases ("RAG" = "retrieval-augmented generation").
- **Lists are information.** "A name in a list is not discussed" stripped DJ tracklists of their artists (33 → 400
  linked artists after the definition said a tracklist / line-up / participants list counts).
- **Hubs are not walked.** An entity held by more than 60 passages says nothing about any one of them. Graph expansion
  skips them and weights a shared entity by `1 / log2(1 + passages holding it)`. Hubs still work as filters.
- **A name missing from its passage is not a wrong link.** The two wrong links in the re-audit both named something
  the passage never says, so the whole graph was measured for it: 669 of 20,997 links (3.2%) contain no word of the
  entity's name, 336 of them topics. 37 of the non-topic ones were read by hand — **33 hold, 4 weak, 0 wrong**. The
  passage almost always *does* say the name, differently: a transcript garbles it (Gardin/Garden, school/Skool,
  Olama/Ollama, Lyola/Loyola, Traer board/Trello, Sarte/Sartre), a 16th-century
  translation spells it its own way (Romaines, Souldan, Carminiola), it is abbreviated (DDVC = Example Research), the
  subject is named by role ("the duke" = Cesare Borgia) or is the first-person speaker of an interview. **A "the name
  must appear" rule was therefore rejected** — it would delete ~300 links, ~90% of them right, and it is exactly the
  string rule Jev exists to replace. (`experiment-absent-names.ts`; the same run showed spacing and punctuation
  variants must be matched before calling a name absent — "Work Stream" is written Workstream.)
- **There is no error pocket where "absent name" meets "only one passage holds it".** The obvious follow-up to the
  two findings above — that the tail of thin entities is where absent-name links go wrong — is false: the two
  conditions intersect in 29 links, 0.1% of the graph, and the names in it are the same spelling cases as everywhere
  else (Sarte/Sartre, Carminiola/Carmignola, Caesar, Pyrrhus). Nothing to fix.
- **Aliases are the pairs that never co-occur.** Resolution proposed merges from entities that keep turning up in the
  *same* passages, which is blind to a pure alias: a writer picks ONE of "Workstream" / "Work Stream" per passage, so
  substitutes are precisely the names that never appear together. 44 such groups held 1,039 links. They are proposed
  now by squashing spacing and punctuation out of the name, each side carrying a passage of its own, and they get
  their own floor (`ALIAS_FLOOR` 0.8): the string evidence is already in hand and Jev is only asked to veto, so it
  hedges — of 33 pairs only 9 reached 0.9 while every one from 0.82 up was right by hand. An initials detector was
  tried in the same experiment and thrown away: on short names it degenerates into "shares a first letter".
  (`experiment-alias-and-title-entities.ts`)
- **Document-title entities are not a problem.** The same experiment looked for entities whose links all sit in one
  document and cover most of it — 27 entities, 96 links, and most are genuine subjects (a podcast's interviewee, the
  tools a blog post is a list of). Nothing to cut.
- **LLM output is never trusted unchecked.** GLM proposes ~3x more names than survive Jev's check.
- **Edges are deleted by id.** `.drop()` is a node mutation; on an edge stream Helix v0.0.5 silently does nothing
  (filed: HelixDB/helix-db#1109 — HelixDB replied "investigating will fix shortly" on 2026-09-20). Use `g().dropEdgeById(ids | EdgeRef.var(name))`. Node `.drop()` cascades its edges.
  `outN()` of an out-edge is its target, `inN()` its source.
- **The in-memory pool halves Helix's share of a corpus pass.** Timed on the same 288 and 576 passages, reads only,
  no writes and no model calls: 36 ms a passage through the traversals, 19 ms with the pool (vector search still goes
  to Helix; the rest is a map lookup), and both paths gather the identical candidate set — 9,380 / 28,129 candidates,
  exactly equal, which is the correctness check as well as the timing one. Over 16,394 passages that is ~10 minutes
  of Helix reads against ~5, plus a 5-9 s one-off load. Reproduced twice. (`experiment-pool-timing.ts`)
- **Throughput.** Cloudflare's gateway refuses past ~250 requests/min ("Wholesale rate limit", code 2018); OpenRouter
  took 40 requests/s without refusing (latency climbs; ~8/s effective in the original environment). With Jev unthrottled the bound is
  Helix: one core, reads and writes share it → bulk passes use an in-memory pool (links + rosters loaded once; Helix is
  asked only for nearest chunks) and early-acknowledged writes with one durable barrier at the end.
- Helix limits: ≤512 text-indexed node mutations per write; index DDL cannot share a batch with data; one writer;
  a full scan of every link is 20–90 s, so `kb check` runs its scans one after another.

## Hygiene runbook (everything is a dry run until `--yes`, and works from the newest backup)

```bash
kb backup                      # ~3 min: entities, links with endpoints, judged chunk props → backup/<stamp>/*.jsonl
kb check                       # invariants; article bookmarks judged, index-only posts exempt; EntityName parity
kb entities --index-names      # dry run: EntityName backfill (Helix Entity.name build bug); --yes inserts missing names
kb audit --sample 200          # random links + passages → file, for an independent reviewer (Claude / sol — not GLM)
kb prune --yes                 # links under the floor, stale, duplicate pairs; orphan entities        no model calls
kb vet --yes                   # Jev: unchecked LLM links; node-worthiness of every topic + thinly-held entity
kb confirm --yes               # Jev: every link a second time, focused; mean of two readings ≥ 0.9
kb consolidate --yes           # document-wide entities → one document link                         no model calls
kb dedupe --yes                # documents that are 100% identical-passage copies of another       no model calls
kb relate --yes                # passage↔passage RELATES_TO: graph proposes, Jev decides
kb model --yes                 # owner + profile (1 LLM call), category parents (1 LLM call), EXTENDS/UPDATES (Jev), entity associations
kb entities --resolve --yes    # co-occurrence / same slug proposes duplicates, Jev decides (≥0.9), merge; aliases kept
bun eval.ts "<note>"           # queries → eval.history.jsonl
```

After any `--yes` run the backup it used is out of date: take a new one before the next hygiene command.
