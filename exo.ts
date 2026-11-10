#!/usr/bin/env bun
// exo — Exocortex, a headless knowledge graph on Helix. `bun exo.ts --help` for commands; README.md for the design and the measured rules.
import { BunRuntime } from "@effect/platform-bun"
import { Effect } from "effect"
import { run } from "./src/cli.ts"
import { ExoLive } from "./src/live.ts"

run(process.argv.slice(2)).pipe(Effect.provide(ExoLive), BunRuntime.runMain)
