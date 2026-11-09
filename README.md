# kb

Search your notes and bookmarks by who and what they are about.

Embeddings don't find names: when I measured, a nonsense query sat at cosine distance 0.30 from the corpus, the passages that really named the entity at 0.30 to 0.33, and unrelated posts at 0.32. So `kb` builds an entity graph on HelixDB. Jev decides which people, products and topics each passage is really about, and a link is written only at confidence 0.9 or higher. A short query walks that graph and adds BM25; longer questions add vector search.

![A terminal starts the kb reader on the bundled fixture notes and lists the notes that mention greenhouse](docs/media/hero.gif)

## Install

Bun 1.3 and a running HelixDB v3 instance (`kb` never starts one). `OPENAI_API_KEY` for embeddings and `OPENROUTER_API_KEY` for Jev; `--mode text` and `--mode graph` search need only Helix.

```bash
git clone <this-repo-url> kb && cd kb
bun install --frozen-lockfile
cp config.example.sh config.local.sh && . ./config.local.sh   # edit first
helix init local --name kb --port 6972 && helix start kb      # local Helix keeps data in memory only
```

## Use

```bash
bun kb.ts taxonomy --seed --yes                  # one LLM call drafts categories; ingest needs them
bun kb.ts ingest fixtures/notes                  # chunk, embed, link entities
bun kb.ts search "solar greenhouse" --mode graph
bun kb.ts stats
bun kb.ts                                        # every command and filter
```

The hygiene commands (`check`, `prune`, `dedupe`, `consolidate` and others) are dry runs until you pass `--yes`. [PIPELINE.md](PIPELINE.md) has the design, the measurements and the order to run them in.

To read notes without Helix or keys, there is a SQLite reader:

```console
$ KB_READER_DB=/tmp/kbrun/kb.db python3 reader/server.py &
Database ready (6 bookmarks imported)
Serving on http://localhost:8421/bookmarks.html
$ curl -s 'localhost:8421/api/articles' | jq -r '.[] | "\(.word_count)  \(.title)"'
31  Compost soil
32  Garden sensors
34  Greenhouse irrigation
...
```

Every environment variable: [docs/configuration.md](docs/configuration.md). What ingest and search do step by step: [docs/internals.md](docs/internals.md).

## Limits

- Ingest, linking and every hygiene command call paid APIs (OpenAI embeddings, Jev and GLM on OpenRouter). The source records about $0.001 for a 4-passage document.
- The fixture ingest and graph search have not been run from this published copy. On an empty Helix instance without API keys, the indexes build, `stats` and `search` answer, and `ingest` stops at the missing taxonomy.
- Helix is single-writer and allows at most 512 text-indexed node mutations per write. A full scan of every link takes 20 to 90 seconds.
- `kb` does not start or manage Helix.
- The reader passes the `sort` query parameter straight into SQL. Bind it to localhost, as the default does.
- `migrate/reindex_vectors.py` targets the old Helix v1 API and drops and recreates chunks. Read it before you run it.

## License

MIT
