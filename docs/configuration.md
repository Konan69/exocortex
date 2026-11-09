# Configuration

Every option is an environment variable. `config.example.sh` sets each one to its default.

| Variable | Default | What it does |
| --- | --- | --- |
| `HELIX_URL` | `http://localhost:6972` | Helix instance that `kb.ts` and `eval.ts` use |
| `KB_TENANT` | `demo` | `tenant_id` on every node; scopes every query |
| `KB_USER` | `owner` | `userId` on documents and chunks |
| `KB_OWNER_NAME` | value of `KB_USER` | Name on the `User` node and in the profile that `kb model` writes |
| `OPENAI_API_KEY` | none | Embeddings. Required for ingest and for `hybrid` and `vector` search |
| `KB_EMBED_URL` | `https://api.openai.com/v1/embeddings` | Embeddings endpoint |
| `OPENROUTER_API_KEY` | none | GLM and Jev calls. Required for taxonomy, ingest, linking and hygiene |
| `KB_MODEL` | `z-ai/glm-5.3-flash` | LLM that names entities and drafts the taxonomy |
| `KB_OPENROUTER_URL` | `https://openrouter.ai/api/v1` | OpenRouter base URL |
| `KB_JEV_ROUTE` | `openrouter` | `openrouter` or `cloudflare` |
| `KB_JEV_URL` | `https://openrouter.ai/api/alpha/decisions` | Jev endpoint on the OpenRouter route |
| `CLOUDFLARE_API_TOKEN`, `CLOUDFLARE_ACCOUNT_ID` | none | Required when `KB_JEV_ROUTE=cloudflare` |
| `CF_AIG_TOKEN` | empty | Optional AI Gateway authorization on the Cloudflare route |
| `KB_CLOUDFLARE_URL` | `https://api.cloudflare.com/client/v4` | Cloudflare API base URL |
| `KB_AI_GATEWAY` | `kb` | AI Gateway name on the Cloudflare route |
| `KB_JEV_CONCURRENCY` | `64` | Jev requests in flight |
| `KB_LLM_CONCURRENCY` | `32` | LLM requests in flight |
| `KB_LLM_TIMEOUT` | `90000` | LLM call timeout in milliseconds |
| `KB_DEDUPE_SHARE` | `1` | Share of a document's passages that must exist elsewhere for `kb dedupe` to remove it |
| `KB_BOOKMARKS` | `./data/bookmarks.jsonl` | Bookmark JSONL for `kb bookmarks` |
| `KB_LABELS` | `./data/labels.jsonl` | Bookmark category labels |
| `KB_SKIP` | `./data/tombstoned.json` | Bookmark ids to skip |
| `KB_BOOKMARK_DB` | `./data/bookmarks.db` | SQLite file with extracted article text |
| `KB_BACKUPS` | `backup/` beside `kb.ts` | Where `kb backup` writes and the hygiene commands read |
| `KB_READER_SOURCE` | `fixtures/notes` | Reader input: a directory of note JSON or a bookmark JSONL file |
| `KB_READER_DB` | `reader/data/bookmarks.db` | Reader SQLite file |
| `KB_READER_HOST` | `127.0.0.1` | Reader bind address |
| `KB_READER_PORT` | `8421` | Reader port |
| `SUPERMEMORY_URL` | `http://127.0.0.1:6767` | `migrate/export_supermemory.py` source server |
| `SUPERMEMORY_TAG` | `demo` | Container tag to export |
| `SUPERMEMORY_EXPORT_DIR` | `./data/supermemory-export` | Export output directory |
| `SUPERMEMORY_EXPORT_LOG` | `./data/export.log` | Export log |
| `SUPERMEMORY_API_KEY` | none | Optional bearer token for the export |
| `HELIX_REINDEX_URL` | `http://localhost:6971/v1/query` | Helix v1 endpoint for `migrate/reindex_vectors.py` |
| `HELIX_REINDEX_LOG` | `./data/reindex.log` | Reindex log |
| `HELIX_API_KEY` | none | Optional bearer token for the reindex |
