// The three model jobs, each on the Effect AI service built for it:
//   decide   Jev (typesafe/jev-1.13 via OpenRouter's decisions API): every judgement, typed answers with probabilities
//   generate GLM via OpenRouter: the only writing (naming what Jev flagged as missing, drafting a profile)
//   embed    OpenAI text-embedding-3-small, 1536 dims
// Models owns the run-wide policy around them: requests in flight, pacing, retries of transient failures, and the
// usage report a command prints at the end.
import { OpenAiClient, OpenAiEmbeddingModel } from "@effect/ai-openai"
import { OpenRouterClient, OpenRouterDecisionModel, OpenRouterLanguageModel } from "@effect/ai-openrouter"
import { Array as Arr, type Cause, Config, Context, Effect, Layer, Ref, Schedule, Schema, Semaphore } from "effect"
import { type AiError, type Decision, DecisionModel, EmbeddingModel, LanguageModel } from "effect/ai"
import { FetchHttpClient } from "effect/http"
import { Settings } from "./config.ts"

export const Usage = Schema.Struct({
  decisions: Schema.Number,
  decisionTokens: Schema.Number,
  decisionCost: Schema.Number,
  generations: Schema.Number,
  promptTokens: Schema.Number,
  completionTokens: Schema.Number,
  embedded: Schema.Number,
})
export type Usage = typeof Usage.Type
const noUsage: Usage = { decisions: 0, decisionTokens: 0, decisionCost: 0, generations: 0, promptTokens: 0, completionTokens: 0, embedded: 0 }

/** OpenRouter takes ~10 decision requests a second from here before latency climbs; starts are spaced this far apart. */
const DECISION_GAP = "100 millis"
const EMBED_BATCH = 64
const retryTransient = { schedule: Schedule.exponential("600 millis"), times: 8, while: (error: AiError.AiError) => error.isRetryable }

export class Models extends Context.Service<Models, {
  /** Answers every decision it can; a refused or invalid decision is an `Unanswered` outcome, not a failure. */
  readonly decide: <Input extends Schema.Top, Decisions extends Record<string, Decision.Any>>(
    definition: Decision.Definition<Input, Decisions>,
    input: Input["Type"],
  ) => Effect.Effect<Decision.Outcomes<Decisions>, AiError.AiError, Input["EncodingServices"]>
  readonly generate: <S extends Schema.Encoder<Record<string, any>, unknown>>(options: {
    readonly name: string
    readonly system: string
    readonly prompt: string
    readonly schema: S
  }) => Effect.Effect<S["Type"], AiError.AiError | Cause.TimeoutError, S["DecodingServices"]>
  /** One vector per text, in order. */
  readonly embed: (texts: ReadonlyArray<string>) => Effect.Effect<ReadonlyArray<ReadonlyArray<number>>, AiError.AiError>
  readonly usage: Effect.Effect<Usage>
}>()("exo/Models") {
  static readonly layer = Layer.effect(Models, Effect.gen(function*() {
    const settings = yield* Settings
    const decisions = yield* DecisionModel.DecisionModel
    const writer = yield* LanguageModel.LanguageModel
    const embeddings = yield* EmbeddingModel.EmbeddingModel
    const decideSlots = yield* Semaphore.make(settings.jevConcurrency)
    const writeSlots = yield* Semaphore.make(settings.writerConcurrency)
    const startGate = yield* Semaphore.make(1)
    const usage = yield* Ref.make(noUsage)
    const add = (delta: Partial<Usage>) =>
      Ref.update(usage, (total) => ({ ...total, ...Object.fromEntries(Object.entries(delta).map(([key, value]) => [key, total[key as keyof Usage] + (value ?? 0)])) }))

    const decide = <Input extends Schema.Top, Decisions extends Record<string, Decision.Any>>(
      definition: Decision.Definition<Input, Decisions>,
      input: Input["Type"],
    ) =>
      Effect.gen(function*() {
        yield* startGate.withPermits(1)(Effect.sleep(DECISION_GAP))
        const response = yield* decisions.decideSettled(definition, { input })
        yield* add({ decisions: 1, decisionTokens: response.usage.inputTokens ?? 0, decisionCost: response.metadata?.openrouter?.cost ?? 0 })
        return response.outcomes
      }).pipe(Effect.retry(retryTransient), decideSlots.withPermits(1))

    const generate = <S extends Schema.Encoder<Record<string, any>, unknown>>(options: {
      readonly name: string
      readonly system: string
      readonly prompt: string
      readonly schema: S
    }) =>
      Effect.gen(function*() {
        const response = yield* writer.generateObject({
          objectName: options.name,
          schema: options.schema,
          prompt: [{ role: "system", content: options.system }, { role: "user", content: options.prompt }],
        })
        yield* add({ generations: 1, promptTokens: response.usage.inputTokens.total ?? 0, completionTokens: response.usage.outputTokens.total ?? 0 })
        return response.value
      }).pipe(Effect.timeout(settings.writerTimeoutMillis), Effect.retry({ times: 2 }), writeSlots.withPermits(1))

    const embed = (texts: ReadonlyArray<string>) =>
      Effect.forEach(Arr.chunksOf(texts, EMBED_BATCH), (batch) =>
        embeddings.embedMany(batch).pipe(
          Effect.retry(retryTransient),
          Effect.tap(() => add({ embedded: batch.length })),
          Effect.map((response) => response.embeddings.map((embedding) => embedding.vector)),
        )).pipe(Effect.map(Arr.flatten))

    return Models.of({ decide, generate, embed, usage: Ref.get(usage) })
  }))

  static readonly live = Layer.unwrap(Effect.gen(function*() {
    const settings = yield* Settings
    return Models.layer.pipe(
      Layer.provide([
        OpenRouterDecisionModel.layer({ model: settings.jevModel }),
        // GLM's reasoning cannot be switched off; minimal effort is 0 reasoning tokens and ~2 s for the same answer
        OpenRouterLanguageModel.layer({ model: settings.writerModel, config: { temperature: 0, reasoning: { effort: "minimal" } } }),
        OpenAiEmbeddingModel.model(settings.embeddingModel, { dimensions: settings.dimensions }),
      ]),
      Layer.provide([
        OpenRouterClient.layerConfig({ apiKey: Config.Redacted("OPENROUTER_API_KEY") }),
        OpenAiClient.layerConfig({ apiKey: Config.Redacted("OPENAI_API_KEY") }),
      ]),
      Layer.provide(FetchHttpClient.layer),
    )
  }))
}
