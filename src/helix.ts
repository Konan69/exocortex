// Helix: the store, the indexes, and the only door to them. Every query is a v3 SDK batch posted to /v2/query,
// decoded through the caller's schema here, so nothing inside kb handles raw Helix JSON.
import { g, IndexSpec, parseJson, readBatch, type ReadBatch, VectorDistanceMetric, type WriteBatch, writeBatch } from "@helix-db/helix-db"
import { Context, Effect, Layer, Schedule, Schema, Semaphore } from "effect"
import { HttpClient, HttpClientRequest } from "effect/http"
import { ChildProcess, ChildProcessSpawner } from "effect/process"
import { Settings } from "./config.ts"

export class HelixError extends Schema.TaggedError<HelixError>()("HelixError", {
  query: Schema.String,
  /** HTTP status; absent for transport and decode failures */
  status: Schema.optional(Schema.Number),
  message: Schema.String,
}) {}


const INDEXES: ReadonlyArray<readonly [string, IndexSpec]> = [
  ...["KnowledgeHead", "KnowledgeGeneration", "KnowledgeSource", "KnowledgeEntity", "KnowledgeAlias", "KnowledgeClaim"].flatMap(label => [
    [`${label}_key`, IndexSpec.nodeUniqueEquality(label, "key")] as const,
    [`${label}_scope`, IndexSpec.nodeEquality(label, "scope")] as const,
  ]),
  ["knowledge_source_text", IndexSpec.nodeText("KnowledgeSource", "content", "scope")],
  ["knowledge_source_title", IndexSpec.nodeText("KnowledgeSource", "title", "scope")],
  ["knowledge_alias_name", IndexSpec.nodeEquality("KnowledgeAlias", "name")],
  ["knowledge_claim_vector", IndexSpec.nodeVector("KnowledgeClaim", "embedding", 1536, VectorDistanceMetric.Cosine, "scope")],
]

const IndexReceipts = Schema.Record(Schema.String, Schema.Struct({ kind: Schema.String, operation_id: Schema.optional(Schema.String) }))

/** Helix is one writer: a busy writer answers 409; the server and transport fail transiently. */
const isTransient = (error: HelixError) =>
  error.status === undefined || error.status === 409 || error.status === 429 || error.status >= 500

export class Helix extends Context.Service<Helix, {
  readonly read: <S extends Schema.Top>(
    query: string,
    batch: ReadBatch,
    schema: S,
  ) => Effect.Effect<S["Type"], HelixError, S["DecodingServices"]>
  /** Serialized: one write in flight. `durable: false` acknowledges before the commit reaches object storage. */
  readonly write: (query: string, batch: WriteBatch, options?: { readonly durable?: boolean }) => Effect.Effect<void, HelixError>
  readonly writeReturning: <S extends Schema.Top>(
    query: string,
    batch: WriteBatch,
    schema: S,
  ) => Effect.Effect<S["Type"], HelixError, S["DecodingServices"]>
  /** Posts a body as given to a Helix endpoint and returns the response text: `exo query`, for agents. */
  readonly raw: (path: "/v2/query" | "/v2/cypher" | "/v2/cypher/explain", body: string, options?: { readonly write?: boolean }) => Effect.Effect<string, HelixError>
  /** Creates every index exo relies on and waits until all are active. Run before the first write of a command. */
  readonly ensureIndexes: Effect.Effect<void, HelixError>
}>()("exo/Helix") {
  static readonly layer = Layer.effect(Helix, Effect.gen(function*() {
    const settings = yield* Settings
    const http = yield* HttpClient.HttpClient
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner
    const writer = yield* Semaphore.make(1)

    const reachable = http.get(settings.helixUrl).pipe(
      Effect.timeout("1 second"),
      Effect.as(true),
      Effect.catch(() => Effect.succeed(false)),
    )
    // The local instance is the `dev` instance of the Helix CLI project in helix.toml (disk storage on SeaweedFS). Its
    // containers have restart policy `no`, so they are off after a WSL boot; `helix start` brings them back on the
    // same volume. The CLI can report "not ready" while the server is fine, so readiness is judged by asking Helix.
    const start = Effect.gen(function*() {
      if (settings.helixExternal || (yield* reachable)) return
      yield* Effect.logInfo("Helix is down; running `helix start dev`")
      yield* spawner.exitCode(
        ChildProcess.make("helix", ["start", "dev", "--quiet"], { cwd: settings.root, extendEnv: true, env: { DO_NOT_TRACK: "1" } }),
      ).pipe(Effect.mapError((cause) => new HelixError({ query: "helix start dev", message: String(cause) })))
      const up = yield* reachable.pipe(
        Effect.repeat({ schedule: Schedule.spaced("500 millis"), until: (ok) => ok }),
        Effect.timeoutOrElse({ duration: "60 seconds", orElse: () => Effect.succeed(false) }),
      )
      if (!up) return yield* new HelixError({ query: "helix start dev", message: `Helix did not answer on ${settings.helixUrl} within 60 s; see \`helix logs dev\`` })
    })
    const ensureUp = yield* Effect.cached(start)

    const post = (query: string, body: string, headers: Record<string, string>, path = "/v2/query") =>
      Effect.gen(function*() {
        yield* ensureUp
        const response = yield* http.execute(
          HttpClientRequest.post(`${settings.helixUrl}${path}`).pipe(
            HttpClientRequest.setHeaders({ "content-type": "application/json", ...headers }),
            HttpClientRequest.bodyText(body, "application/json"),
          ),
        ).pipe(Effect.mapError((cause) => new HelixError({ query, message: String(cause) })))
        const text = yield* response.text.pipe(Effect.mapError((cause) => new HelixError({ query, message: String(cause) })))
        if (response.status >= 300) return yield* new HelixError({ query, status: response.status, message: text.slice(0, 300) })
        return text
      }).pipe(Effect.retry({ schedule: Schedule.exponential("600 millis"), times: 8, while: isTransient }))

    const decode = <S extends Schema.Top>(query: string, schema: S) => (text: string) =>
      Effect.try({ try: () => parseJson(text), catch: (cause) => new HelixError({ query, message: `not JSON: ${String(cause)}` }) }).pipe(
        Effect.flatMap(Schema.decodeUnknownEffect(schema)),
        Effect.mapError((cause) => cause instanceof HelixError ? cause : new HelixError({ query, message: String(cause) })),
      )

    const send = (query: string, batch: WriteBatch, durable: boolean) =>
      writer.withPermits(1)(post(query, batch.toQueryJson({ queryName: query }), { "x-helix-await-durable": String(durable) }))

    const read = <S extends Schema.Top>(query: string, batch: ReadBatch, schema: S) =>
      post(query, batch.toQueryJson({ queryName: query }), {}).pipe(Effect.flatMap(decode(query, schema)))
    const write = (query: string, batch: WriteBatch, options?: { readonly durable?: boolean }) =>
      Effect.asVoid(send(query, batch, options?.durable ?? true))
    const writeReturning = <S extends Schema.Top>(query: string, batch: WriteBatch, schema: S) =>
      send(query, batch, true).pipe(Effect.flatMap(decode(query, schema)))

    const createIndexes = INDEXES.reduce(
      (batch, [name, spec]) => batch.varAs(name, g().createIndexIfNotExists(spec)),
      writeBatch(),
    ).returning(INDEXES.map(([name]) => name))
    const ensureIndexes = Effect.gen(function*() {
      for (let attempt = 0; ; attempt++) {
        const receipts = yield* writeReturning("ensure_indexes", createIndexes, IndexReceipts)
        const building = Object.entries(receipts).filter(([, receipt]) => receipt.kind !== "already_active").map(([name]) => name)
        if (!building.length) return
        let pending = readBatch()
        const jobs: string[] = []
        for (const name of building) {
          const operation = receipts[name]?.operation_id
          if (operation) { pending = pending.varAs(name, g().getIndexOperation(operation)); jobs.push(name) }
        }
        if (jobs.length) {
          const states = yield* read("index_progress", pending.returning(jobs), Schema.Record(Schema.String, Schema.Struct({ status: Schema.String, message: Schema.optional(Schema.NullOr(Schema.String)), blocker_code: Schema.optional(Schema.String) })))
          for (const [name, state] of Object.entries(states)) if (state.status === "blocked" || state.status === "aborted") return yield* new HelixError({ query: "ensure_indexes", message: `${name}: ${state.status}: ${state.blocker_code ?? state.message ?? "inspect the index operation"}` })
        }
        if (attempt % 5 === 0) yield* Effect.logInfo(`waiting for indexes: ${building.join(", ")}`)
        yield* Effect.sleep("3 seconds")
      }
    })

    const raw = (path: "/v2/query" | "/v2/cypher" | "/v2/cypher/explain", body: string, options?: { readonly write?: boolean }) =>
      options?.write ? writer.withPermits(1)(post(`raw ${path}`, body, { "x-helix-await-durable": "true" }, path)) : post(`raw ${path}`, body, {}, path)

    return Helix.of({
      read,
      write,
      writeReturning,
      raw,
      ensureIndexes: yield* Effect.cached(ensureIndexes),
    })
  }))
}
