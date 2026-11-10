# Exocortex

Read README.md first: what exocortex is, the graph's shape, and the rules that came out of measuring (do not undo one without re-measuring).

- Bun + Effect 4 (`effect@4.1.2`). Read `node_modules/effect/src` before using an API; do not code Effect from memory. Model calls go through `Models` (`src/ai.ts`), Helix through `Helix` (`src/helix.ts`); nothing else talks to them.
- Helix on `localhost:6972` is the live graph. Test a write path live with a disposable document: `exo ingest <dir with one test JSON>`, check it with `docs`/`entities`/`search --doc`, then `exo forget <file> --yes`, `exo backup` and `exo prune --yes` for the orphans it leaves; finish with `exo check`.
- Before calling a change done: `bunx tsc --noEmit`, `bun scripts/smoke.ts`, and the command you changed run against the live graph.
- Nothing personal in the repo: owner, tenant and keys live in `.env`, the Helix project in `helix.toml`, backups in `backup/` (all gitignored).
