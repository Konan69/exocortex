// Every service exo runs on, for the CLI and the scripts.
import { BunServices } from "@effect/platform-bun"
import { Layer } from "effect"
import { FetchHttpClient } from "effect/http"
import { Models } from "./ai.ts"
import { Settings } from "./config.ts"
import { Helix } from "./helix.ts"
import { LinkWriter } from "./pipeline.ts"

export const ExoLive = LinkWriter.layer.pipe(
  Layer.provideMerge(Layer.mergeAll(Helix.layer, Models.live)),
  Layer.provideMerge(Settings.layer),
  Layer.provideMerge(FetchHttpClient.layer),
  Layer.provideMerge(BunServices.layer),
  Layer.provide(BunServices.layer),
)
