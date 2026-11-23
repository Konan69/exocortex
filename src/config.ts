import { join } from "node:path"
import { Config, Context, Effect, Layer } from "effect"

export class Settings extends Context.Service<Settings, {
  readonly helixUrl: string
  /** set when HELIX_URL is given: then exo never starts the local Helix itself */
  readonly helixExternal: boolean
  readonly jevModel: string
  readonly jevConcurrency: number
  readonly embeddingModel: string
  readonly dimensions: number
  readonly writerModel: string
  /** model requests in flight */
  readonly writerConcurrency: number
  /** the repo root: holds helix.toml (the local Helix project) and backup/ */
  readonly root: string
}>()("exo/Settings") {
  static readonly layer = Layer.effect(Settings, Effect.gen(function*() {
    const helixUrl = yield* Config.option(Config.String("HELIX_URL"))
    return Settings.of({
      helixUrl: helixUrl._tag === "Some" ? helixUrl.value : "http://localhost:6972",
      helixExternal: helixUrl._tag === "Some",
      jevModel: "typesafe/jev-1.13",
      jevConcurrency: yield* Config.Int("EXO_JEV_CONCURRENCY").pipe(Config.withDefault(64)),
      embeddingModel: "text-embedding-3-small",
      dimensions: 1536,
      writerModel: yield* Config.String("EXO_MODEL").pipe(Config.withDefault("z-ai/glm-5.3-flash")),
      writerConcurrency: yield* Config.Int("EXO_LLM_CONCURRENCY").pipe(Config.withDefault(32)),
      root: join(import.meta.dir, ".."),
    })
  }))
}
