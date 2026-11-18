# Exocortex

Read README.md first for extraction, ontology mapping, evidence and generation activation.

- Bun + Effect 4 (`effect@4.1.2`). Read `node_modules/effect/src` before using an API; do not code Effect from memory. Model calls go through `Models` (`src/ai.ts`), Helix through `Helix` (`src/helix.ts`); nothing else talks to them.
- Helix on `localhost:6972` holds the existing corpus. Verify writes on a separate instance or an explicit disposable `EXO_TENANT`; set `HELIX_URL` to prevent auto-starting the wrong instance. Never activate a partial corpus as a completed rebuild. The legacy graph and previous generations remain until replacement verification passes.
- Before calling a change done: `bunx tsc --noEmit` and the Everystate proofs described in `proofs/README.md`; use proof and trace evidence rather than adding ordinary tests. Model accuracy needs human-labeled source claims via `scripts/eval.ts`; stubbed model outcomes do not prove semantic accuracy. Search changes also run `scripts/rank-eval.ts <label> --baseline <previous label>` against the same corpus and disclose regressions.
- Nothing personal in git: keys and tenant in `.env`, Helix project in `helix.toml`, backups in `backup/`, extraction and mapping artifacts in `.exo/` (all gitignored).
