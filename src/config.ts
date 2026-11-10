import { join } from "node:path"
import { Config, Context, Effect, Layer } from "effect"

export class Settings extends Context.Service<Settings, {
  readonly helixUrl: string
  /** set when HELIX_URL is given: then exo never starts the local Helix itself */
  readonly helixExternal: boolean
  readonly jevModel: string
  readonly writerModel: string
  readonly embeddingModel: string
  readonly dimensions: number
  /** requests in flight to Jev / the writer model */
  readonly jevConcurrency: number
  readonly writerConcurrency: number
  readonly writerTimeoutMillis: number
  /** the repo root: holds helix.toml (the local Helix project) and backup/ */
  readonly root: string
  readonly backupDir: string
}>()("exo/Settings") {
  static readonly layer = Layer.effect(Settings, Effect.gen(function*() {
    const helixUrl = yield* Config.option(Config.String("HELIX_URL"))
    return Settings.of({
      helixUrl: helixUrl._tag === "Some" ? helixUrl.value : "http://localhost:6972",
      helixExternal: helixUrl._tag === "Some",
      jevModel: "typesafe/jev-1.13",
      writerModel: yield* Config.String("EXO_MODEL").pipe(Config.withDefault("z-ai/glm-5.3-flash")),
      embeddingModel: "text-embedding-3-small",
      dimensions: 1536,
      jevConcurrency: yield* Config.Int("EXO_JEV_CONCURRENCY").pipe(Config.withDefault(64)),
      writerConcurrency: yield* Config.Int("EXO_LLM_CONCURRENCY").pipe(Config.withDefault(32)),
      writerTimeoutMillis: yield* Config.Int("EXO_LLM_TIMEOUT").pipe(Config.withDefault(90_000)),
      root: join(import.meta.dir, ".."),
      backupDir: join(import.meta.dir, "..", "backup"),
    })
  }))
}
