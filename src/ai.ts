import { OpenAiClient, OpenAiEmbeddingModel } from "@effect/ai-openai"
import { OpenRouterClient, OpenRouterDecisionModel, OpenRouterLanguageModel } from "@effect/ai-openrouter"
import { Array as Arr, Config, Context, Effect, Layer, Option, Redacted, Ref, Schedule, Schema, Semaphore } from "effect"
import { AiError, type Decision, DecisionModel, EmbeddingModel, LanguageModel } from "effect/ai"
import { FetchHttpClient } from "effect/http"
import { Settings } from "./config.ts"

export const Usage = Schema.Struct({
  decisions: Schema.Number, decisionTokens: Schema.Number, decisionCost: Schema.Number, embedded: Schema.Number,
  generations: Schema.Number,
  promptTokens: Schema.Number,
  completionTokens: Schema.Number,
})
export type Usage = typeof Usage.Type
const noUsage: Usage = { generations: 0, promptTokens: 0, completionTokens: 0, decisions: 0, decisionTokens: 0, decisionCost: 0, embedded: 0 }

export class IncompleteGeneration extends Schema.TaggedError<IncompleteGeneration>()("IncompleteGeneration", { reason: Schema.String }) {}

/** OpenRouter takes ~10 decision requests a second from here before latency climbs; starts are spaced this far apart. */
const DECISION_GAP = "100 millis"
export const EMBED_BATCH = 64
const retryTransient = { schedule: Schedule.exponential("600 millis"), times: 8, while: (error: AiError.AiError) => error.isRetryable }

// Readers require no model key.
const optionalKey = (name: string) => Config.Redacted(name).pipe(Config.option, Config.map(value => Option.isSome(value) && Redacted.value(value.value).trim() ? value.value : undefined))
const requireKey = (name: string, method: string) =>
  Effect.gen(function*() {
    if (yield* optionalKey(name).pipe(Effect.orDie)) return
    return yield* AiError.make({ module: "exo/Models", method, reason: new AiError.AuthenticationError({ kind: "MissingKey", description: `set ${name}` }) })
  })

export class Models extends Context.Service<Models, {
  readonly embeddingsAvailable: boolean
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
  }) => Effect.Effect<S["Type"], AiError.AiError | IncompleteGeneration, S["DecodingServices"]>
  readonly capacity: Effect.Effect<{ readonly inputBytes: number; readonly outputTokens: number }, Error>
  /** One vector per text, in order. */
  readonly embed: (texts: ReadonlyArray<string>) => Effect.Effect<ReadonlyArray<ReadonlyArray<number>>, AiError.AiError>
  readonly usage: Effect.Effect<Usage>
}>()("exo/Models") {
  static readonly layer = Layer.effect(Models, Effect.gen(function*() {
    const settings = yield* Settings
    const embeddingsAvailable = (yield* optionalKey("OPENAI_API_KEY")) !== undefined
    const decisions = yield* DecisionModel.DecisionModel
    const embeddings = yield* EmbeddingModel.EmbeddingModel
    const decideSlots = yield* Semaphore.make(settings.jevConcurrency)
    const startGate = yield* Semaphore.make(1)
    const writer = yield* LanguageModel.LanguageModel
    const writeSlots = yield* Semaphore.make(settings.writerConcurrency)
    const usage = yield* Ref.make(noUsage)
    const add = (delta: Partial<Usage>) =>
      Ref.update(usage, (total) => ({ ...total, ...Object.fromEntries(Object.entries(delta).map(([key, value]) => [key, total[key as keyof Usage] + (value ?? 0)])) }))

    const decide = <Input extends Schema.Top, Decisions extends Record<string, Decision.Any>>(
      definition: Decision.Definition<Input, Decisions>,
      input: Input["Type"],
    ) =>
      Effect.gen(function*() {
        yield* requireKey("OPENROUTER_API_KEY", "decide")
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
        yield* requireKey("OPENROUTER_API_KEY", "generate")
        const limits = yield* capacity.pipe(Effect.mapError(error => new IncompleteGeneration({ reason: error.message })))
        const response = yield* writer.generateObject({
          objectName: options.name,
          schema: options.schema,
          prompt: [{ role: "system", content: options.system }, { role: "user", content: options.prompt }],
        }).pipe(OpenRouterLanguageModel.withConfigOverride({ max_tokens: limits.outputTokens }))
        yield* add({ generations: 1, promptTokens: response.usage.inputTokens.total ?? 0, completionTokens: response.usage.outputTokens.total ?? 0 })
        if (response.finishReason !== "stop") return yield* new IncompleteGeneration({ reason: response.finishReason })
        return response.value
      }).pipe(Effect.retry({ times: 2, while: error => error._tag === "AiError" && error.isRetryable }), writeSlots.withPermits(1))

    const embed = (texts: ReadonlyArray<string>) =>
      requireKey("OPENAI_API_KEY", "embed").pipe(Effect.andThen(Effect.forEach(Arr.chunksOf(texts, EMBED_BATCH), (batch) =>
        embeddings.embedMany(batch).pipe(
          Effect.retry(retryTransient),
          Effect.tap(() => add({ embedded: batch.length })),
          Effect.map((response) => response.embeddings.map((embedding) => embedding.vector)),
        )).pipe(Effect.map(Arr.flatten))))

    const capacity = yield* Effect.cached(Effect.tryPromise(async () => {
      const response = await fetch("https://openrouter.ai/api/v1/models")
      if (!response.ok) throw new Error(`Cannot read model context limits: HTTP ${response.status}`)
      const catalog = Schema.decodeUnknownSync(Schema.Struct({ data: Schema.Array(Schema.Struct({
        id: Schema.String, context_length: Schema.Finite,
        top_provider: Schema.Struct({ max_completion_tokens: Schema.NullOr(Schema.Finite) }),
      })) }))(await response.json())
      const model = catalog.data.find(model => model.id === settings.writerModel)
      const output = model?.top_provider.max_completion_tokens
      if (!model || !output || model.context_length <= output) throw new Error(`No usable context/output limits for ${settings.writerModel}`)
      // A UTF-8 byte is a conservative upper bound for one byte-tokenizer token.
      return { inputBytes: model.context_length - output, outputTokens: output }
    }).pipe(Effect.mapError(error => new Error(String(error)))))
    return Models.of({ decide, generate, embed, embeddingsAvailable, capacity, usage: Ref.get(usage) })
  }))

  static readonly live = Layer.unwrap(Effect.gen(function*() {
    const settings = yield* Settings
    return Models.layer.pipe(
      Layer.provide([
        OpenRouterDecisionModel.layer({ model: settings.jevModel }),
        OpenAiEmbeddingModel.model(settings.embeddingModel, { dimensions: settings.dimensions }),
        // GLM's reasoning cannot be switched off; minimal effort is 0 reasoning tokens and ~2 s for the same answer
        OpenRouterLanguageModel.layer({ model: settings.writerModel, config: { temperature: 0, reasoning: { effort: "minimal" } } }),
      ]),
      Layer.provide([
        OpenRouterClient.layerConfig({ apiKey: optionalKey("OPENROUTER_API_KEY") }),
        OpenAiClient.layerConfig({ apiKey: optionalKey("OPENAI_API_KEY") }),
      ]),
      Layer.provide(FetchHttpClient.layer),
    )
  }))
}
