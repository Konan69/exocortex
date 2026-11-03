# Copy to config.local.sh, edit, then run: . ./config.local.sh
# Environment values are read by kb.ts. No personal credential files are read.
# Ingesting fixtures/notes requires a running Helix instance and both API keys.
# bun kb.ts ingest fixtures/notes
# bun kb.ts search "solar greenhouse" --mode graph
export HELIX_URL="${HELIX_URL:-http://localhost:6972}"
export KB_TENANT="${KB_TENANT:-demo}"
export KB_USER="${KB_USER:-owner}"
export KB_OWNER_NAME="${KB_OWNER_NAME:-$KB_USER}"
# Required for embeddings. Set OPENAI_API_KEY in your shell.
# Required for naming and Jev decisions. Set OPENROUTER_API_KEY in your shell.
export KB_EMBED_URL="${KB_EMBED_URL:-https://api.openai.com/v1/embeddings}"
export KB_MODEL="${KB_MODEL:-z-ai/glm-5.3-flash}"
export KB_OPENROUTER_URL="${KB_OPENROUTER_URL:-https://openrouter.ai/api/v1}"
export KB_JEV_ROUTE="${KB_JEV_ROUTE:-openrouter}"
export KB_JEV_URL="${KB_JEV_URL:-https://openrouter.ai/api/alpha/decisions}"
# The cloudflare route uses CLOUDFLARE_API_TOKEN and CLOUDFLARE_ACCOUNT_ID.
# CF_AIG_TOKEN is optional gateway authorization.
export KB_CLOUDFLARE_URL="${KB_CLOUDFLARE_URL:-https://api.cloudflare.com/client/v4}"
export KB_AI_GATEWAY="${KB_AI_GATEWAY:-kb}"
export KB_JEV_CONCURRENCY="${KB_JEV_CONCURRENCY:-64}"
export KB_LLM_CONCURRENCY="${KB_LLM_CONCURRENCY:-32}"
export KB_LLM_TIMEOUT="${KB_LLM_TIMEOUT:-90000}"
export KB_DEDUPE_SHARE="${KB_DEDUPE_SHARE:-1}"
# Bookmark inputs are optional for note ingest and search.
# bookmarks accepts a source path and --labels, --skip, --ftdb overrides.
export KB_BOOKMARKS="${KB_BOOKMARKS:-./data/bookmarks.jsonl}"
export KB_LABELS="${KB_LABELS:-./data/labels.jsonl}"
export KB_SKIP="${KB_SKIP:-./data/tombstoned.json}"
export KB_BOOKMARK_DB="${KB_BOOKMARK_DB:-./data/bookmarks.db}"
# KB_BACKUPS defaults to the backup directory beside kb.ts.
