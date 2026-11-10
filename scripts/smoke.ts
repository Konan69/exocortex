// Proves the foundation against the live services: one Helix read, one Jev decision, one GLM object, one embedding.
import { g, readBatch } from "@helix-db/helix-db"
import { Effect, Schema } from "effect"
import { Decision } from "effect/ai"
import { Models } from "../src/ai.ts"
import { Helix } from "../src/helix.ts"
import { ExoLive } from "../src/live.ts"
import { tenantScope } from "../src/queries.ts"

const program = Effect.gen(function*() {
  const helix = yield* Helix
  const models = yield* Models
  const counts = yield* helix.read(
    "smoke_counts",
    readBatch()
      .varAs("chunks", g().nWithLabel("Chunk").where(tenantScope).count())
      .varAs("entities", g().nWithLabel("Entity").where(tenantScope).count())
      .returning(["chunks", "entities"]),
    Schema.Struct({ chunks: Schema.Finite, entities: Schema.Finite }),
  )
  yield* Effect.log(`helix: ${counts.chunks} chunks, ${counts.entities} entities`)

  const judged = yield* models.decide(
    Decision.make({
      input: Schema.Struct({ passage: Schema.String }),
      decisions: {
        aboutTrains: Decision.probability({ instructions: "The passage is about trains" }),
        tone: Decision.classify({ instructions: "The passage's tone", criteria: { calm: null, urgent: null } }),
      },
    }),
    { passage: "The 8:15 to Lagos Island was cancelled again, so we walked." },
  )
  yield* Effect.log(`jev: ${JSON.stringify(judged)}`)

  const named = yield* models.generate({
    name: "names",
    system: "List the named things in the text.",
    prompt: "Ada met Tunde at Paystack in Lagos to discuss Effect.",
    schema: Schema.Struct({ names: Schema.Array(Schema.String) }),
  })
  yield* Effect.log(`glm: ${JSON.stringify(named)}`)

  const [vector] = yield* models.embed(["agent memory"])
  yield* Effect.log(`embed: ${vector?.length} dims`)
  yield* Effect.log(`usage: ${JSON.stringify(yield* models.usage)}`)
})


Effect.runPromise(program.pipe(Effect.provide(ExoLive)))
