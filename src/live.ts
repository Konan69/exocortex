// Every service exo runs on, for the CLI and the scripts.
import { BunServices } from "@effect/platform-bun"
import { Layer } from "effect"
import { FetchHttpClient } from "effect/http"
import { Models } from "./ai.ts"
import { Settings } from "./config.ts"
import { Helix } from "./helix.ts"

export const ExoLive = Layer.mergeAll(Helix.layer, Models.live).pipe(
  Layer.provideMerge(Settings.layer),
  Layer.provideMerge(FetchHttpClient.layer),
  Layer.provideMerge(BunServices.layer),
  Layer.provide(BunServices.layer),
)
