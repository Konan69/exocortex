#!/usr/bin/env bun
// kb — example's HelixDB knowledgebase, one tool. Run `kb` for usage.
//
//   SourceDocument -HAS_CHUNK-> Chunk -MENTIONS{confidence >= 0.8, by: "jev" | "llm+jev"}-> Entity
//                               Chunk -IN_CATEGORY{confidence}-> Category
//
// The chunk is the unit of knowledge (Helix's model allows Chunk -> Entity directly). Two models, by what each is for:
//   - an LLM (GLM) does the only jobs that need WRITING, in bulk and rarely: naming the entities in a stretch of
//     text, and drafting the category taxonomy once
//   - Jev (TypeSafe's System One model, via Cloudflare AI Gateway) does every DECISION: is this chunk really about
//     entity X, which category, how much it matters, is it about example, is it boilerplate, does this passage answer
//     the query. One parallel pass per chunk, typed answers with calibrated probabilities, ~$0.04 per million tokens.
// Helix stores, indexes, searches and walks the graph. Nothing here is decided by string rules.
import {
  Client, g, readBatch, writeBatch, BatchCondition, EdgeRef, Expr, IndexSpec, NodeRef, Predicate,
  PropertyProjection, PropertyValue, SourcePredicate, VectorDistanceMetric, sub,
} from "@helix-db/helix-db";
import { createOpenRouter } from "@openrouter/ai-sdk-provider";
import { generateObject } from "ai";
import { z } from "zod";
import { createHash } from "node:crypto";
import { appendFileSync, existsSync, mkdirSync, readdirSync, readFileSync, statSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";

const TENANT = "example", USER = "example";
const HELIX = process.env.HELIX_URL ?? "http://localhost:6972";
const EMBED_MODEL = "text-embedding-3-small", DIMS = 1536;
const LLM = process.env.KB_MODEL ?? "z-ai/glm-5.3-flash";
const GATEWAY = process.env.KB_AI_GATEWAY ?? "example-dev";
const CHUNK_CHARS = 4000, CHUNKS_PER_WRITE = 150;
// A Jev request is filled to a token budget (its context is 32k): shared definitions once, then as many passages as fit.
// (the estimate runs ~20% under Jev's own count, and its context is 32k)
const JEV_TOKEN_BUDGET = 19000, SHARED_TOKENS = 1600, CANDIDATES = 30, NEIGHBOURS = 8, NAMING_PAGE = 12, PACK = 6;
// Jev answers in ~2s from here and allows 1,200 requests/min: throughput is requests in flight.
const JEV_IN_FLIGHT = Number(process.env.KB_JEV_CONCURRENCY ?? 64), LLM_IN_FLIGHT = Number(process.env.KB_LLM_CONCURRENCY ?? 32);
const ENTITY_KINDS = ["person", "organisation", "product", "project", "place", "topic"] as const;
const LINK_VERSION = 4;   // 4 = candidate names only in questions, 6 passages a request (2 listed them in the state and leaked; 3 = unpacked trial)
// Below the floor a mention is "named in passing" and is not written. Measured: links kept from 0.5 made graph-reached
// passages worse and up to 40x slower (eval.ts); and with one passage per request an independent LLM agrees with 54% of
// Jev's links at 0.80-0.85, 80% at 0.85-0.90, 84% at 0.90-0.95, 96% at 0.95+ (kb audit, 2026-09-20) → 0.85.
const DEDUPE_SHARE = Number(process.env.KB_DEDUPE_SHARE ?? 1);   // 1 = every passage of the copy exists in the kept document: nothing unique is lost
const MENTION_FLOOR = 0.9, WORTHY_FLOOR = 0.4, TOPIC_FLOOR = 0.8, ABOUT_FLOOR = 0.4;
// A pair whose two names are the same string once spacing and punctuation go ("Work Stream" / "Workstream") arrives
// already carrying its evidence — Jev is only asked to veto it, and it reads two passages that never use the same
// spelling, so it hedges: of 33 such pairs only 9 reached 0.9, while every one from 0.82 up was right by hand
// (example/VC OS 0.88, Workstream/Work Stream 0.88, example/Sand Water 0.85). Below 0.8 it starts refusing for a
// reason (Flow Node/FlowNode 0.53, Optic O/Optico 0.22). Measured 2026-09-20.
const ALIAS_FLOOR = 0.8;   // 0.5 also dropped real definitions ("Mamba is a selective state-space model…" 0.47); passing mentions sit at 0.06-0.40   // example set 0.9 on 2026-09-20

const client = Client.server(HELIX);
const tenantScope = Predicate.eq("tenant_id", TENANT);

// ---------------------------------------------------------------- keys

function openaiKey(): string {
  const env = readFileSync(join(homedir(), "work/example/apps/web/.env"), "utf8");
  const match = env.match(/^\s*(?:export\s+)?OPENAI_API_KEY\s*=\s*["']?([^"'\n]+)/m);
  if (!match) throw new Error("no OPENAI_API_KEY in ~/work/example/apps/web/.env");
  return match[1].trim();
}

function openrouterKey(): string {
  if (process.env.OPENROUTER_API_KEY) return process.env.OPENROUTER_API_KEY;
  const auth = JSON.parse(readFileSync(join(homedir(), ".pi/agent/auth.json"), "utf8"));
  if (!auth.openrouter?.key) throw new Error("no OpenRouter key in ~/.pi/agent/auth.json");
  return auth.openrouter.key;
}

function cloudflare(): { token: string; account: string; gatewayToken: string } {
  const env = readFileSync(join(homedir(), "work/example/apps/web/.env"), "utf8");
  const get = (key: string) => process.env[key] ?? env.match(new RegExp(`^\\s*${key}\\s*=\\s*["']?([^"'\\n]+)`, "m"))?.[1]?.trim() ?? "";
  const token = get("CLOUDFLARE_API_TOKEN"), account = get("CLOUDFLARE_ACCOUNT_ID");
  if (!token || !account) throw new Error("no CLOUDFLARE_API_TOKEN / CLOUDFLARE_ACCOUNT_ID");
  return { token, account, gatewayToken: get("CF_AIG_TOKEN") };
}

// ---------------------------------------------------------------- model calls

async function retrying<T>(label: string, run: () => Promise<T>, attempts = 9): Promise<T> {
  for (let attempt = 1; ; attempt++) {
    try {
      return await run();
    } catch (error) {
      if (attempt === attempts) throw new Error(`${label}: ${String(error).slice(0, 200)}`);
      await Bun.sleep(Math.min(20000, 600 * 2 ** attempt));
    }
  }
}

function limiter(max: number) {
  let inFlight = 0;
  const waiting: (() => void)[] = [];
  return async <T>(run: () => Promise<T>): Promise<T> => {
    if (inFlight >= max) await new Promise<void>((resolve) => waiting.push(resolve));
    inFlight++;
    try {
      return await run();
    } finally {
      inFlight--;
      waiting.shift()?.();
    }
  };
}
const jevSlot = limiter(JEV_IN_FLIGHT), llmSlot = limiter(LLM_IN_FLIGHT);
const usage = { jevCalls: 0, jevTokens: 0, llmCalls: 0, llmPrompt: 0, llmCompletion: 0, embedded: 0 };

async function embed(texts: string[]): Promise<number[][]> {
  const vectors: number[][] = [];
  for (let start = 0; start < texts.length; start += 64) {
    const batch = texts.slice(start, start + 64);
    const data = await retrying("embed", async () => {
      const response = await fetch("https://api.openai.com/v1/embeddings", {
        method: "POST", headers: { "Content-Type": "application/json", Authorization: `Bearer ${openaiKey()}` },
        body: JSON.stringify({ model: EMBED_MODEL, input: batch }),
      });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      return (await response.json()) as { data: { embedding: number[] }[] };
    });
    usage.embedded += batch.length;
    vectors.push(...data.data.map((row) => row.embedding));
  }
  return vectors;
}

// LLM: one schema-constrained completion through the AI SDK. Used only where text has to be written.
let openrouter: ReturnType<typeof createOpenRouter> | undefined;
async function llm<T>(system: string, prompt: string, schema: z.ZodType<T>): Promise<T> {
  openrouter ??= createOpenRouter({ apiKey: openrouterKey() });
  return llmSlot(async () => {
    // (the generic schema defeats generateObject's overload inference; the result is still validated against it)
    const result = await generateObject<any>({
      model: openrouter!.chat(LLM), schema: schema as z.ZodType<any>, system, prompt, temperature: 0, maxRetries: 2,
      abortSignal: AbortSignal.timeout(Number(process.env.KB_LLM_TIMEOUT ?? 90_000)),   // a stalled call must fail so the caller can split the page and retry
      // This model's reasoning cannot be switched off and defaults to ~6k thinking tokens and 25-60s per call.
      // Minimal effort is 0 reasoning tokens and ~2s, with the same answer for a reading task.
      providerOptions: { openrouter: { reasoning: { effort: "minimal" } } },
    });
    usage.llmCalls++; usage.llmPrompt += result.usage.inputTokens ?? 0; usage.llmCompletion += result.usage.outputTokens ?? 0;
    return result.object as T;
  });
}

// Jev: typed questions over a block of state, all answered in one parallel pass.
type Question = { type: "noul"; instructions: string } | { type: "choice"; instructions: string; criteria: Record<string, string> } | { type: "score"; instructions: string; criteria: string[] };
type Answer = { type: string; noul?: number; choice?: string; score?: number; confidence?: number; probabilities?: Record<string, number> };
const noul = (instructions: string): Question => ({ type: "noul", instructions });
const choice = (instructions: string, criteria: Record<string, string>): Question => ({ type: "choice", instructions, criteria });
const score = (instructions: string, criteria: string[]): Question => ({ type: "score", instructions, criteria });

// The gateway's limit is on request rate ("Wholesale rate limit exceeded for this gateway", code 2018). Measured
// 2026-09-20 with real-size requests: 10/s sustained → 120/120 ok; 20/s → 72% refused. Request starts are spaced
// GAP_FLOOR apart (~9/s); a 429 widens the gap a little and it recovers quickly. (The first pacer widened x1.6 and
// paused everyone 4-8 s per 429, which braked a run to 0.7/s for no reason.)
const GAP_FLOOR = (process.env.KB_JEV_ROUTE ?? "openrouter") === "openrouter" ? 100 : 250;   // OpenRouter ~10/s; Cloudflare refuses past ~250 requests a minute
const pace = { gap: GAP_FLOOR, next: 0, streak: 0 };
async function paced() {
  const now = Date.now(), at = Math.max(now, pace.next);
  pace.next = at + pace.gap;
  if (at > now) await Bun.sleep(at - now);
}

// Two routes to the same model, same request shape. OpenRouter (`/api/alpha/decisions`, typesafe/jev-1.13) is the
// default: measured 2026-09-20, 160 production-size requests at 40/s → 0 refused (latency climbs to ~16 s, so the
// ceiling is ~8 requests/s ≈ 46 passages/s, likely this machine's uplink). Cloudflare's gateway refuses past ~250
// requests a minute. KB_JEV_ROUTE=cloudflare switches back.
const JEV_ROUTE = process.env.KB_JEV_ROUTE ?? "openrouter";
async function jev(state: string | object, questions: Record<string, Question>): Promise<Record<string, Answer>> {
  return jevSlot(() => retrying("jev", async () => {
    await paced();
    let response: Response;
    if (JEV_ROUTE === "openrouter") {
      response = await fetch("https://openrouter.ai/api/alpha/decisions", {
        method: "POST", headers: { "Content-Type": "application/json", Authorization: `Bearer ${openrouterKey()}` },
        body: JSON.stringify({ model: "typesafe/jev-1.13", state, questions }), signal: AbortSignal.timeout(120_000),
      });
    } else {
      const { token, account, gatewayToken } = cloudflare();
      response = await fetch(`https://api.cloudflare.com/client/v4/accounts/${account}/ai/run`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json", Authorization: `Bearer ${token}`,
          "cf-aig-gateway-id": GATEWAY, ...(gatewayToken ? { "cf-aig-authorization": `Bearer ${gatewayToken}` } : {}),
          "cf-aig-metadata": JSON.stringify({ workload: "example-kb" }),
        },
        body: JSON.stringify({ model: "typesafe/jev", input: { state, questions } }), signal: AbortSignal.timeout(120_000),
      });
    }
    if (response.status === 429) {
      pace.gap = Math.min(400, Math.round(pace.gap * 1.25)); pace.streak = 0;
      pace.next = Math.max(pace.next, Date.now() + 1000);
      throw new Error("HTTP 429");
    }
    if (!response.ok) throw new Error(`HTTP ${response.status} ${(await response.text()).slice(0, 160)}`);
    if (++pace.streak % 20 === 0) pace.gap = Math.max(GAP_FLOOR, Math.round(pace.gap * 0.9));
    const payload = (await response.json()) as any;
    const result = payload.answers ? payload : payload.result?.result ?? payload.result;
    if (!result?.answers) throw new Error(`no answers: ${JSON.stringify(payload).slice(0, 160)}`);
    usage.jevCalls++; usage.jevTokens += result.usage?.input_tokens ?? 0;
    return result.answers as Record<string, Answer>;
  }));
}

// ---------------------------------------------------------------- helix

// Helix is a single writer: concurrent writes come back 409. Every write goes through this queue.
let writeQueue: Promise<unknown> = Promise.resolve();
// `durable: false` lets a bulk drain be acknowledged before the commit reaches object storage; the run ends with one
// durable write (settle), which cannot be acknowledged before everything queued ahead of it.
let drainFast = false;
function write<R>(batch: any, queryName: string): Promise<R> {
  const run = () => retrying(queryName, () =>
    client.requestBuilder<R>().shouldAwaitDurability(!drainFast).query(batch.toQueryRequest({ queryName })).send());
  const result = writeQueue.then(run, run);
  writeQueue = result.catch(() => undefined);
  return result;
}
const read = <R>(batch: any, queryName: string) => client.query<R>(batch.toQueryRequest({ queryName })).send();
const fields = (...names: string[]) => names.map((name) => PropertyProjection.new(name));
const documentById = (documentId: string) => g().nWithLabelWhere("SourceDocument", SourcePredicate.eq("documentId", documentId)).where(tenantScope);
const chunksOfDocument = (documentId: string) => g().nWithLabelWhere("Chunk", SourcePredicate.eq("documentId", documentId)).where(tenantScope);

// Identity is the normalised name under a kind; the same rule the existing ~13k Entity nodes were written with.
function normalise(name: string): string {
  return name.normalize("NFKD").replace(/\p{M}/gu, "").toLowerCase().trim()
    .replace(/[’'`]/g, "")
    .replace(/\b(ltd|limited|inc|llc|plc|gmbh|corp|corporation|co)\b\.?/g, "")
    .replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "");
}
const entityKeyOf = (kind: string, name: string) => `${TENANT}:${kind}:${normalise(name)}`;
const categoryKeyOf = (name: string) => `${TENANT}:${normalise(name)}`;

async function bootstrap() {
  const specs: [string, IndexSpec][] = [
    ["doc_id", IndexSpec.nodeEquality("SourceDocument", "documentId")], ["doc_status", IndexSpec.nodeEquality("SourceDocument", "status")],
    ["doc_kind", IndexSpec.nodeEquality("SourceDocument", "kind")], ["doc_source", IndexSpec.nodeEquality("SourceDocument", "source")],
    ["chunk_doc", IndexSpec.nodeEquality("Chunk", "documentId")], ["chunk_id", IndexSpec.nodeEquality("Chunk", "chunkId")],
    // what search filters on, as top-level indexed chunk properties (Helix: "keep frequently filtered fields top-level")
    ["chunk_kind", IndexSpec.nodeEquality("Chunk", "documentKind")], ["chunk_source", IndexSpec.nodeEquality("Chunk", "source")],
    ["chunk_category", IndexSpec.nodeEquality("Chunk", "category")], ["chunk_owner", IndexSpec.nodeEquality("Chunk", "aboutOwner")],
    ["chunk_salience", IndexSpec.nodeRange("Chunk", "salience")],
    ["chunk_vec", IndexSpec.nodeVector("Chunk", "embedding", DIMS, VectorDistanceMetric.Cosine, "tenant_id")],
    ["chunk_txt", IndexSpec.nodeText("Chunk", "content", "tenant_id")],
    ["chunk_naming", IndexSpec.nodeEquality("Chunk", "needsNaming")], ["chunk_hash", IndexSpec.nodeEquality("Chunk", "contentHash")],
    ["cat_key", IndexSpec.nodeUniqueEquality("Category", "categoryKey")], ["ent_key", IndexSpec.nodeEquality("Entity", "entityKey")],
  ];
  let batch: any = writeBatch();
  for (const [name, spec] of specs) batch = batch.varAs(name, g().createIndexIfNotExists(spec));
  // Index builds are asynchronous (and cannot share a transaction with data): ask until every receipt is already_active.
  for (let attempt = 0; ; attempt++) {
    const receipts = await write<Record<string, any>>(batch.returning(specs.map(([name]) => name)), "bootstrap");
    const building = Object.entries(receipts).filter(([, receipt]) => receipt?.kind !== "already_active").map(([name]) => name);
    if (!building.length) return;
    if (attempt > 200) throw new Error(`indexes never became active: ${building.join(", ")}`);
    if (attempt % 5 === 0) console.log(`waiting for indexes: ${building.join(", ")}`);
    await Bun.sleep(3000);
  }
}

// ---------------------------------------------------------------- taxonomy (LLM, once)

type Category = { name: string; description: string };

// Jev chooses among options it is given, so the options have to exist first. Drafting a taxonomy is writing, and it
// happens once: the LLM sees what the corpus holds and proposes the coarse topics. Fine-grained topics are entities.
async function seedTaxonomy(args: Args) {
  const data = await read<{ kinds: any[]; titles: any[]; topics: any[] }>(readBatch()
    .varAs("kinds", g().nWithLabel("SourceDocument").where(tenantScope).groupCount("kind"))
    .varAs("titles", g().nWithLabel("SourceDocument").where(tenantScope).project(fields("title", "kind")))
    .varAs("topics", g().nWithLabel("SourceDocument").where(tenantScope).outE("MENTIONS").groupCount("entityKey"))
    .returning(["kinds", "titles", "topics"]), "taxonomy_inputs");
  const titles = data.titles.filter((_: any, index: number) => index % Math.max(1, Math.floor(data.titles.length / 450)) === 0).map((row: any) => `[${row.kind || "note"}] ${row.title}`);
  const topics = data.topics.filter((row: any) => String(row.entityKey).split(":")[1] === "topic").sort((a: any, b: any) => Number(b.count) - Number(a.count)).slice(0, 250).map((row: any) => `${String(row.entityKey).split(":").slice(2).join(":")} (${row.count})`);
  const answer = await llm<{ categories: Category[] }>(
    `You are designing the top-level topic taxonomy for one person's knowledge base. You are shown its document kinds, a sample of titles, and its most-mentioned topics. Propose 30 to 45 categories that together cover everything, each clearly distinct from the others. name: 1-3 lower-case words. description: one plain sentence saying what belongs there, concrete enough that a classifier could decide from it. Cover the owner's personal and work material as well as the reference material. Do not include an "other" category.`,
    `DOCUMENT KINDS\n${data.kinds.map((row: any) => `${row.kind || "note"}: ${row.count}`).join("\n")}\n\nSAMPLE TITLES\n${titles.join("\n")}\n\nMOST-MENTIONED TOPICS\n${topics.join(", ")}`,
    z.object({ categories: z.array(z.object({ name: z.string(), description: z.string() })) }));
  for (const category of answer.categories) console.log(`${category.name.padEnd(26)} ${category.description}`);
  if (!args.has("--yes")) return console.log(`\n${answer.categories.length} categories proposed — re-run with --yes to store them`);
  let batch: any = writeBatch().varAs("old", g().nWithLabel("Category").where(tenantScope).drop());
  answer.categories.forEach((category, index) => {
    batch = batch.varAs(`c${index}`, g().addN("Category", { tenant_id: TENANT, categoryKey: categoryKeyOf(category.name), name: category.name.toLowerCase().trim(), description: category.description }));
  });
  await write(batch.returning([]), "store_taxonomy");
  console.log(`\nstored ${answer.categories.length} categories`);
}

async function taxonomy(): Promise<Category[]> {
  const data = await read<{ categories: Category[] | null }>(readBatch().varAs("categories", g().nWithLabel("Category").where(tenantScope).project(fields("name", "description", "isParent"))).returning(["categories"]), "taxonomy");
  const leaves = (data.categories ?? []).filter((category: any) => !category.isParent);   // parents group; passages are filed under leaves
  if (!leaves.length) throw new Error("no taxonomy yet — run `kb taxonomy --seed --yes` first");
  return leaves;
}

type Named = { name: string; kind: string };
type Chunk = { chunkIndex: number; content: string };
type Doc = { documentId: string; title: string; kind: string; source: string; url?: string; chunkCount?: number };

// ---------------------------------------------------------------- link (Jev decides)

type Candidate = { id: number; name: string; kind: string; entityKey: string };
type Passage = { chunkId: string; documentId: string; title: string; kind: string; source: string; content: string; embedding: number[]; candidates: Candidate[] };
type Judged = { passage: Passage; mentions: (Candidate & { confidence: number })[]; category: string; categoryConfidence: number; salience: number; aboutOwner: boolean; boilerplate: boolean; needsNaming: boolean };

const tokensOf = (text: string) => Math.ceil(text.length / 3.6);
const DEFINITIONS = {
  discusses: "a passage discusses something when it gives information about it. When the passage IS a list that carries the information — a tracklist or setlist, a line-up, the participants of a meeting, a bibliography — every item on it is discussed. A name that only appears in passing, among navigation or promotional links, or in a signature is not discussed. A person who is only speaking, being addressed, or credited as author or host is not thereby discussed — they are discussed when the passage tells you something ABOUT them (what they did, decided, own, are responsible for, think, or are like)",
  boilerplate: "sponsor reads, calls to subscribe, link lists, legal footers, navigation, greetings and sign-offs",
  about_owner: `about ${USER} himself: his own life, work, decisions, plans, money, or the people he personally deals with — not general or third-party material`,
  salience: "how much lasting, reusable information the passage holds",
};

// One Jev request = as many passages as fit the token budget, from any documents. Everything shared is stated once in
// `state` (definitions, the taxonomy); each passage carries its own text and its own candidate list; questions are short
// and refer to the state by id.
async function judgePack(pack: Passage[], categories: Category[]): Promise<Judged[]> {
  const labels = Object.fromEntries([...categories.map((category) => [normalise(category.name), category.name]), ["other", "none of the categories fits"]]);
  const questions: Record<string, Question> = {};
  pack.forEach((passage, p) => {
    questions[`p${p}_category`] = choice(`Topic of p${p}, by the category definitions in state`, labels);
    questions[`p${p}_salience`] = score(`Salience of p${p}`, ["none: filler only", "a little useful detail", "useful, loosely stated", "dense, specific, reusable"]);
    questions[`p${p}_owner`] = noul(`p${p} is about_owner`);
    questions[`p${p}_boilerplate`] = noul(`p${p} is boilerplate`);
    questions[`p${p}_missing`] = noul(`p${p} discusses specific named people, organisations, products, projects or places other than these: ${passage.candidates.map((entity) => entity.name).join("; ") || "(none)"}`);
    passage.candidates.forEach((entity, e) => { questions[`p${p}_e${e}`] = noul(`p${p} discusses ${entity.name}`); });
  });
  const state = {
    definitions: DEFINITIONS, categories: Object.fromEntries(categories.map((category) => [normalise(category.name), category.description])),
    // A passage's state is its text and nothing that could be mistaken for it: candidate names listed here made Jev
    // "find" them in the passage (backup/experiment-candidates-in-state.ts) — names appear only in the questions.
    passages: Object.fromEntries(pack.map((passage, p) => [`p${p}`, { document: passage.title, kind: passage.kind || "note", text: passage.content }])),
  };
  const answers = await jev(state, questions);
  return pack.map((passage, p) => ({
    passage,
    mentions: passage.candidates.map((entity, e) => ({ ...entity, confidence: answers[`p${p}_e${e}`]?.noul ?? 0 })).filter((entity) => entity.confidence >= MENTION_FLOOR),
    category: answers[`p${p}_category`]?.choice ?? "other", categoryConfidence: answers[`p${p}_category`]?.confidence ?? 0,
    salience: Math.min(1, Math.max(0, (answers[`p${p}_salience`]?.score ?? 0) / 3)),
    aboutOwner: (answers[`p${p}_owner`]?.noul ?? 0) >= 0.5, boilerplate: (answers[`p${p}_boilerplate`]?.noul ?? 0) >= 0.5,
    needsNaming: (answers[`p${p}_missing`]?.noul ?? 0) >= 0.5,
  }));
}

// Helix commits one write at a time and each commit has a fixed durability cost, so whatever is judged by the time
// the writer frees up goes into the same transaction.
type Unit = { judged: Judged[]; resolve: () => void; reject: (error: unknown) => void };
const readyUnits: Unit[] = [];
let flushing = false;
const MAX_CHUNKS_PER_WRITE = 60;   // well under Helix's 512 text-indexed mutations per write

function writeJudged(judged: Judged[]): Promise<void> {
  return new Promise((resolve, reject) => { readyUnits.push({ judged, resolve, reject }); if (!flushing) void flush(); });
}
async function flush() {
  flushing = true;
  while (readyUnits.length) {
    const units: Unit[] = [];
    let chunks = 0;
    while (readyUnits.length && (units.length === 0 || chunks + readyUnits[0].judged.length <= MAX_CHUNKS_PER_WRITE)) { chunks += readyUnits[0].judged.length; units.push(readyUnits.shift()!); }
    try {
      await write(buildLinkBatch(units.flatMap((unit) => unit.judged)).returning([]), "link_chunks");
      units.forEach((unit) => unit.resolve());
    } catch (error) {
      if (units.length === 1) units[0].reject(error);
      else for (const unit of units) await write(buildLinkBatch(unit.judged).returning([]), "link_chunks").then(unit.resolve, unit.reject);   // isolate the bad unit
    }
  }
  flushing = false;
}

// Per chunk: its judged properties and its edges, with Jev's probability as the edge confidence. Candidates came out of
// Helix with their node ids, so edges point straight at them — no lookups, no get-or-create.
function buildLinkBatch(judged: Judged[]) {
  let batch: any = writeBatch();
  const categoryVar = new Map<string, string>();
  let edge = 0;
  judged.forEach((item, index) => {
    const me = `k${index}`;
    batch = batch
      .varAs(me, g().nWithLabelWhere("Chunk", SourcePredicate.eq("chunkId", item.passage.chunkId)).where(tenantScope).limit(1))
      // a re-link replaces the previous judgement rather than stacking on it
      // (.drop() is a node mutation and silently does nothing on an edge stream — edges go by reference)
      .varAs(`${me}m`, g().n(NodeRef.var(me)).outE("MENTIONS")).varAs(`${me}md`, g().dropEdgeById(EdgeRef.var(`${me}m`)))
      .varAs(`${me}c`, g().n(NodeRef.var(me)).outE("IN_CATEGORY")).varAs(`${me}cd`, g().dropEdgeById(EdgeRef.var(`${me}c`)))
      .varAs(`${me}p`, g().n(NodeRef.var(me))
        .setProperty("category", item.category).setProperty("salience", item.salience).setProperty("aboutOwner", item.aboutOwner)
        .setProperty("boilerplate", item.boilerplate).setProperty("needsNaming", item.needsNaming)
        .setProperty("documentKind", item.passage.kind ?? "").setProperty("source", item.passage.source ?? "")
        .setProperty("userId", USER).setProperty("linkVersion", LINK_VERSION).setProperty("linkedAt", Expr.datetime()));
    if (item.category !== "other") {
      if (!categoryVar.has(item.category)) {
        categoryVar.set(item.category, `c${categoryVar.size}`);
        batch = batch.varAs(categoryVar.get(item.category)!, g().nWithLabelWhere("Category", SourcePredicate.eq("categoryKey", `${TENANT}:${item.category}`)).where(tenantScope).limit(1));
      }
      batch = batch.varAsIf(`l${edge++}`, BatchCondition.varNotEmpty(categoryVar.get(item.category)!), g().n(NodeRef.var(me)).addE("IN_CATEGORY", NodeRef.var(categoryVar.get(item.category)!), { tenant_id: TENANT, confidence: item.categoryConfidence }));
    }
    for (const entity of item.mentions)
      batch = batch.varAs(`l${edge++}`, g().n(NodeRef.var(me)).addE("MENTIONS", entity.id, { tenant_id: TENANT, confidence: entity.confidence, entityKey: entity.entityKey, by: "jev" }));
  });
  return batch;
}

// In-memory pool for corpus-wide passes. Helix runs on one core and is also the writer, so every read a bulk pass can
// avoid is throughput. With a pool, Helix is asked for one thing per passage — its nearest chunks (the vector index is
// the only part that cannot live here) — and "what are those chunks and their documents linked to" is a map lookup.
// Judged passages update the pool, so later passages see them. A single-document ingest skips the pool (loading it
// costs more than the few traversals it saves).
type Pool = { linksOf: Map<number, Candidate[]>; rosterOf: Map<string, Candidate[]> };
async function loadPool(): Promise<Pool> {
  const started = Date.now();
  const all = async <T>(name: string, traversal: any) => (await read<{ rows: T[] | null }>(readBatch().varAs("rows", traversal).returning(["rows"]), name)).rows ?? [];
  const entities = new Map((await all<Candidate & { $id: number }>("pool_entities", g().nWithLabel("Entity").where(tenantScope).valueMap(["$id", "name", "kind", "entityKey"]))).map((row) => [row.$id, { id: row.$id, name: row.name, kind: row.kind, entityKey: row.entityKey } as Candidate]));
  const chunkIds = (await all<{ $id: number }>("pool_chunks", g().nWithLabel("Chunk").where(tenantScope).valueMap(["$id"]))).map((row) => row.$id);
  const linksOf = new Map<number, Candidate[]>();
  for (let start = 0; start < chunkIds.length; start += 1500)
    for (const edge of await all<{ $from: number; $to: number }>("pool_links", g().n(chunkIds.slice(start, start + 1500)).outE("MENTIONS").valueMap(["$from", "$to"]))) {
      const entity = entities.get(edge.$to);
      if (entity) linksOf.set(edge.$from, [...(linksOf.get(edge.$from) ?? []), entity]);
    }
  const documents = new Map((await all<{ $id: number; documentId: string }>("pool_documents", g().nWithLabel("SourceDocument").where(tenantScope).valueMap(["$id", "documentId"]))).map((row) => [row.$id, row.documentId]));
  const rosterOf = new Map<string, Candidate[]>();
  for (const edge of await all<{ $from: number; $to: number }>("pool_rosters", g().nWithLabel("SourceDocument").where(tenantScope).outE("MENTIONS").valueMap(["$from", "$to"]))) {
    const entity = entities.get(edge.$to), documentId = documents.get(edge.$from);
    if (entity && documentId) rosterOf.set(documentId, [...(rosterOf.get(documentId) ?? []), entity]);
  }
  console.log(`pool: ${entities.size} entities · links of ${linksOf.size} chunks · rosters of ${rosterOf.size} documents · ${((Date.now() - started) / 1000).toFixed(0)}s`);
  return { linksOf, rosterOf };
}

type LinkStats = { chunks: number; mentions: number; flagged: number; named: number; rejected?: number; notDiscussed?: number; notWorthy?: number };

// chunk ids -> judged and written. Pages keep memory flat; packs fill Jev's context.
async function linkChunks(chunkIds: string[], docOf: Map<string, Doc>, categories: Category[], stats: LinkStats, onProgress?: () => void, memory?: Pool) {
  const pages: string[][] = [];
  for (let start = 0; start < chunkIds.length; start += 48) pages.push(chunkIds.slice(start, start + 48));
  await pool(pages, 6, async (page) => {
    const data = await read<{ chunks: any[] | null }>(readBatch().varAs("chunks", g().nWithLabelWhere("Chunk", Predicate.isIn("chunkId", page)).where(tenantScope)
      .project([PropertyProjection.renamed("$id", "id"), ...fields("chunkId", "documentId", "title", "content", "embedding")])).returning(["chunks"]), "chunks_to_link");
    const chunks = (data.chunks ?? []).filter((chunk) => chunk.content?.trim());
    // Helix proposes. A bare entity name carries next to no context, so names are never compared to passages. Instead:
    // the passages most similar to this one (full text against full text, on the chunk index that already exists) are
    // about certain entities — those, ranked by how many of the neighbours share them, are the candidates. A chunk is
    // its own nearest neighbour, so its document's roster is always in the pool.
    const entityFields = [PropertyProjection.renamed("$id", "id"), ...fields("name", "kind", "entityKey")];
    let nearest: any = readBatch();
    const names: string[] = [];
    chunks.forEach((chunk, index) => {
      const neighbours = () => g().vectorSearchNodes("Chunk", "embedding", chunk.embedding, NEIGHBOURS, TENANT);
      if (memory) { nearest = nearest.varAs(`v${index}`, neighbours().project([PropertyProjection.renamed("$id", "id"), ...fields("documentId")])); names.push(`v${index}`); return; }
      nearest = nearest.varAs(`c${index}`, neighbours().out("MENTIONS").project(entityFields))                      // what similar passages were judged to be about
        .varAs(`d${index}`, neighbours().in("HAS_CHUNK").dedup().out("MENTIONS").project(entityFields))            // what their documents are about
        .varAs(`o${index}`, g().nWithLabelWhere("Chunk", SourcePredicate.eq("chunkId", chunk.chunkId)).where(tenantScope).out("MENTIONS").project(entityFields));   // its own links
      names.push(`c${index}`, `d${index}`, `o${index}`);
    });
    const pools = chunks.length ? await read<Record<string, any[] | null>>(nearest.returning(names), "candidate_entities") : {};
    if (memory) chunks.forEach((chunk, index) => {
      const near = (pools[`v${index}`] ?? []) as { id: number; documentId: string }[];
      pools[`c${index}`] = near.flatMap((row) => memory.linksOf.get(row.id) ?? []);
      pools[`d${index}`] = [...new Set(near.map((row) => row.documentId))].flatMap((documentId) => memory.rosterOf.get(documentId) ?? []);
      pools[`o${index}`] = memory.linksOf.get(Number(chunk.id)) ?? [];
    });
    const nodeIdOf = new Map(chunks.map((chunk) => [chunk.chunkId as string, Number(chunk.id)]));
    const found: Record<string, Candidate[]> = {};
    chunks.forEach((_, index) => {
      const tally = new Map<string, { entity: Candidate; votes: number }>();
      for (const entity of [...(pools[`c${index}`] ?? []), ...(pools[`d${index}`] ?? []), ...(pools[`o${index}`] ?? [])]) {
        const entry = tally.get(entity.entityKey) ?? { entity, votes: 0 };
        entry.votes++; tally.set(entity.entityKey, entry);
      }
      // what the chunk is already linked to is always re-judged (a --redo must not orphan an entity by leaving it out)
      const own = new Set((pools[`o${index}`] ?? []).map((entity) => entity.entityKey));
      found[`n${index}`] = [...tally.values()].sort((a, b) => Number(own.has(b.entity.entityKey)) - Number(own.has(a.entity.entityKey)) || b.votes - a.votes).slice(0, Math.max(CANDIDATES, own.size)).map((entry) => entry.entity);
    });
    const passages: Passage[] = chunks.map((chunk, index) => {
      const doc = docOf.get(chunk.documentId);
      return { chunkId: chunk.chunkId, documentId: chunk.documentId, title: doc?.title ?? chunk.title ?? "", kind: doc?.kind ?? "", source: doc?.source ?? "", content: chunk.content, embedding: [], candidates: found[`n${index}`] ?? [] };
    });
    // Jev decides, PACK passages to a request. Measured (backup/experiment-pack-size.ts): with names kept out of the
    // state, 8 passages a request decide like 1 (false-link score 0.33 vs 0.30; decision flips = run-to-run noise).
    // The gateway limits request rate, not tokens, so packing is what makes a corpus-wide pass ~40 minutes.
    const packs: Passage[][] = [[]];
    let used = SHARED_TOKENS;
    for (const passage of passages) {
      const cost = tokensOf(passage.content) + 130 + passage.candidates.length * 22;
      if (packs[packs.length - 1].length && (packs[packs.length - 1].length >= PACK || used + cost > JEV_TOKEN_BUDGET)) { packs.push([]); used = SHARED_TOKENS; }
      packs[packs.length - 1].push(passage); used += cost;
    }
    await Promise.all(packs.filter((pack) => pack.length).map(async (pack) => {
      const judged = await judgePack(pack, categories);
      if (memory) for (const item of judged) memory.linksOf.set(nodeIdOf.get(item.passage.chunkId)!, item.mentions);
      await writeJudged(judged);
      stats.chunks += judged.length; stats.mentions += judged.reduce((sum, item) => sum + item.mentions.length, 0); stats.flagged += judged.filter((item) => item.needsNaming).length;
      onProgress?.();
    }));
  });
}

async function documentMap(args: Args): Promise<Map<string, Doc>> {
  const data = await read<{ documents: Doc[] | null }>(readBatch().varAs("documents", documentStream(args).project(fields("documentId", "title", "kind", "source"))).returning(["documents"]), "document_map");
  return new Map((data.documents ?? []).map((doc) => [doc.documentId, doc]));
}

function report(stats: LinkStats, started: number) {
  const decisionCost = usage.jevTokens * 0.042 / 1e6, llmCost = usage.llmPrompt * 0.09 / 1e6 + usage.llmCompletion * 0.30 / 1e6;
  console.log(`DONE ${stats.chunks} chunks judged, ${stats.mentions} mentions, ${stats.flagged} flagged for naming, ${stats.named} new names in ${((Date.now() - started) / 60000).toFixed(1)} min`);
  console.log(`jev: ${usage.jevCalls} calls, ${usage.jevTokens} tokens (${usage.jevCalls ? Math.round(usage.jevTokens / usage.jevCalls) : 0}/call) ≈ $${decisionCost.toFixed(3)} · llm: ${usage.llmCalls} calls, ${usage.llmPrompt}+${usage.llmCompletion} tokens ≈ $${llmCost.toFixed(3)} · embeddings: ${usage.embedded}`);
}

// kb link [--kind K ...] [--limit N] [--dry-run] [--redo] — stage 1: Helix proposes, Jev decides. No LLM.
async function linkCommand(args: Args) {
  const docOf = await documentMap(args);
  const data = await read<{ chunks: { chunkId: string; documentId: string; linkVersion?: number }[] | null }>(readBatch()
    .varAs("chunks", g().nWithLabel("Chunk").where(tenantScope).project(fields("chunkId", "documentId", "linkVersion"))).returning(["chunks"]), "chunk_states");
  let pending = (data.chunks ?? []).filter((chunk) => docOf.has(chunk.documentId) && (args.has("--redo") || Number(chunk.linkVersion ?? 0) !== LINK_VERSION)).map((chunk) => chunk.chunkId);
  if (args.values("--limit").length) pending = pending.slice(0, args.number("--limit", pending.length));
  console.log(`${data.chunks?.length ?? 0} chunks, ${pending.length} to judge → ~${Math.ceil(pending.length / PACK)} Jev requests (${PACK} passages each, ≈ $${(pending.length * 2300 * 0.042 / 1e6).toFixed(2)}), 0 LLM calls`);
  if (args.has("--dry-run") || !pending.length) return;
  await bootstrap();
  const categories = await taxonomy(), stats: LinkStats = { chunks: 0, mentions: 0, flagged: 0, named: 0 }, started = Date.now();
  let lastLog = 0;
  const pool = await loadPool();
  drainFast = true;
  await linkChunks(pending, docOf, categories, stats, () => {
    if (Date.now() - lastLog < 30000) return;
    lastLog = Date.now();
    console.log(`[${new Date().toTimeString().slice(0, 8)}] ${stats.chunks}/${pending.length} chunks · ${usage.jevCalls} jev (gap ${pace.gap}ms, ${Math.round(usage.jevTokens / Math.max(1, usage.jevCalls))} tok/call) · ${stats.flagged} flagged`);
  }, pool);
  drainFast = false;
  await write(writeBatch().varAs("settled", g().nWithLabel("Category").where(tenantScope).limit(1).setProperty("settledAt", Expr.datetime())).returning([]), "settle");   // durable barrier
  report(stats, started);
}

// ---------------------------------------------------------------- name (LLM writes, only where Jev asked)

// Stage 2. Jev flagged the passages that discuss things the graph has no name for yet. Naming is writing, so the LLM
// does it — for those passages only, many per call (any documents), with structured output keyed by passage id. It is
// told what each passage is already linked to, so it names only what is new. Its attribution is per passage, so the
// edges are written directly; nothing goes back to Jev for the same decision.
const NamingSchema = z.object({ passages: z.array(z.object({ id: z.string(), entities: z.array(z.object({ name: z.string(), kind: z.enum(ENTITY_KINDS) })) })) });

type Proposal = { chunkId: string; title: string; content: string; names: string[] };
const VET_DEFINITIONS = {
  discusses: DEFINITIONS.discusses,
  node_worthy: "the name identifies one particular thing: a person (a first name alone is fine), an organisation, a product, a project, a place, an event, or a concept that has a recognised name of its own (such as 'value-based pricing', 'AEO', 'virtual assistant'). Obscure or private things count. NOT node_worthy: a descriptive phrase that merely summarises what the passage says (such as 'creating a diverse population', 'three phase approach', 'learning from mistakes')",
};
// Every proposed name, judged in the passage it came from: is it really discussed there, and does it deserve a node?
// `about` is the test for famous names dropped in passing ("everything you'd learn from Harvard", "a blend of Robert
// Redford or Robert Goulet"): Jev scores those 0.9+ on `discusses` twice over, and 0.06-0.4 here, while tracklist items
// and real subjects stay high (backup/experiment-passing-mentions.ts, 36 hand-labelled links + 20 tracklist links).
const ABOUT = (name: string, p: number) => `${name} is one of the things p${p} is about — not merely a name used as an example, a comparison, a simile, or a tool mentioned on the way. (When the passage is itself a tracklist, line-up, participant list or bibliography, each item on it counts as something the passage is about.)`;
async function vetNames(proposals: Proposal[]): Promise<Map<string, { discusses: number; worthy: number; about: number }>> {
  const packs: Proposal[][] = [];   // names are only ever in the questions, so passages can share a request (see linkChunks)
  for (let start = 0; start < proposals.length; start += PACK) packs.push(proposals.slice(start, start + PACK));
  const verdicts = new Map<string, { discusses: number; worthy: number; about: number }>();
  await Promise.all(packs.filter((pack) => pack.length).map(async (pack) => {
    const questions: Record<string, Question> = {};
    pack.forEach((proposal, p) => proposal.names.forEach((name, n) => {
      questions[`p${p}_d${n}`] = noul(`p${p} discusses ${name}`);
      questions[`p${p}_w${n}`] = noul(`"${name}", as used in p${p}, is node_worthy`);
      questions[`p${p}_a${n}`] = noul(ABOUT(name, p));
    }));
    const answers = await jev({ definitions: VET_DEFINITIONS, passages: Object.fromEntries(pack.map((proposal, p) => [`p${p}`, { document: proposal.title, text: proposal.content }])) }, questions);
    pack.forEach((proposal, p) => proposal.names.forEach((name, n) => verdicts.set(`${proposal.chunkId}\u0000${name}`, { discusses: answers[`p${p}_d${n}`]?.noul ?? 0, worthy: answers[`p${p}_w${n}`]?.noul ?? 0, about: answers[`p${p}_a${n}`]?.noul ?? 0 })));
  }));
  return verdicts;
}

async function nameCommand(args: Args) {
  const docOf = await documentMap(args);
  const data = await read<{ chunks: any[] | null }>(readBatch().varAs("chunks", g().nWithLabelWhere("Chunk", Predicate.eq("needsNaming", true)).where(tenantScope).project(fields("chunkId", "documentId"))).returning(["chunks"]), "flagged_chunks");
  let pending = (data.chunks ?? []).filter((chunk) => docOf.has(chunk.documentId)).map((chunk) => chunk.chunkId as string);
  if (args.values("--limit").length) pending = pending.slice(0, args.number("--limit", pending.length));
  console.log(`${pending.length} passages flagged by Jev → ~${Math.ceil(pending.length / NAMING_PAGE)} LLM calls`);
  if (args.has("--dry-run") || !pending.length) return;
  await bootstrap();
  const stats: LinkStats = { chunks: 0, mentions: 0, flagged: 0, named: 0 }, started = Date.now();
  await nameChunks(pending, docOf, stats);
  report(stats, started);
}

async function nameChunks(chunkIds: string[], docOf: Map<string, Doc>, stats: LinkStats) {
  const pages: string[][] = [];
  for (let start = 0; start < chunkIds.length; start += NAMING_PAGE) pages.push(chunkIds.slice(start, start + NAMING_PAGE));
  // A page the model cannot answer (it returns nothing on very long inputs) is halved and retried, down to one passage.
  const namePage = async (page: string[]): Promise<void> => {
    try { await nameOnePage(page); } catch (error) {
      if (page.length === 1) { console.error(`FAILED naming ${page[0]}: ${String(error).slice(0, 120)}`); return; }
      const half = Math.ceil(page.length / 2);
      await namePage(page.slice(0, half)); await namePage(page.slice(half));
    }
  };
  await pool(pages, LLM_IN_FLIGHT, namePage);

  async function nameOnePage(page: string[]) {
    let batch: any = readBatch().varAs("chunks", g().nWithLabelWhere("Chunk", Predicate.isIn("chunkId", page)).where(tenantScope).project(fields("chunkId", "documentId", "title", "content")));
    page.forEach((chunkId, index) => { batch = batch.varAs(`k${index}`, g().nWithLabelWhere("Chunk", Predicate.eq("chunkId", chunkId)).where(tenantScope).out("MENTIONS").project(fields("name"))); });
    const data = await read<Record<string, any[] | null>>(batch.returning(["chunks", ...page.map((_, index) => `k${index}`)]), "passages_to_name");
    const known = new Map(page.map((chunkId, index) => [chunkId, (data[`k${index}`] ?? []).map((row) => row.name as string)]));
    const chunks = data.chunks ?? [];
    const answer = await llm(
      `Each passage has an id and a list of entities it is ALREADY linked to. For each passage, list what it discusses that is NOT already in its list — and only what passes BOTH tests a second model will apply to every name you give:
(1) discussed: ${VET_DEFINITIONS.discusses}.
(2) node-worthy: ${VET_DEFINITIONS.node_worthy}.
Most passages add zero to three names; when in doubt leave it out — a rejected name is wasted work. Canonical names: the fullest form the text gives, no titles or honorifics, never two spellings of one thing. Topics are specific ("value-based pricing", "drop swap"), not generic ("business", "music"). For DJ material: artists and DJs are persons, labels and channels are organisations, gear and software are products, techniques and genres are topics; individual tracks are not entities, their artists are. A passage with nothing new gets an empty list. Return every passage id.`,
      chunks.map((chunk) => `=== ${chunk.chunkId} — ${docOf.get(chunk.documentId)?.title ?? chunk.title}\nalready linked: ${(known.get(chunk.chunkId) ?? []).join(", ") || "(nothing)"}\n${chunk.content}`).join("\n\n"),
      NamingSchema);
    const named = new Map<string, Named>();
    const perChunk = new Map<string, string[]>();
    for (const row of answer.passages ?? []) {
      if (!known.has(row.id)) continue;
      const keys: string[] = [];
      for (const entity of row.entities ?? []) {
        if (!normalise(entity.name ?? "")) continue;
        const key = entityKeyOf(entity.kind, entity.name);
        named.set(key, entity); keys.push(key);
      }
      perChunk.set(row.id, [...new Set(keys)]);
    }
    // The LLM proposes; Jev checks every proposal against its passage, so an LLM link sits on the same confidence scale
    // as any other and junk names ("learning from mistakes") never become nodes.
    const verdicts = await vetNames(chunks.filter((chunk) => perChunk.get(chunk.chunkId)?.length).map((chunk) => ({
      chunkId: chunk.chunkId, title: docOf.get(chunk.documentId)?.title ?? chunk.title ?? "", content: chunk.content, names: perChunk.get(chunk.chunkId)!.map((key) => named.get(key)!.name) })));
    const confidenceOf = new Map<string, number>();
    let proposed = 0;
    for (const [chunkId, keys] of perChunk) {
      proposed += keys.length;
      perChunk.set(chunkId, keys.filter((key) => {
        const verdict = verdicts.get(`${chunkId}\u0000${named.get(key)!.name}`);
        const worthyFloor = named.get(key)!.kind === "topic" ? TOPIC_FLOOR : WORTHY_FLOOR;
        if (!verdict || verdict.discusses < MENTION_FLOOR || verdict.worthy < worthyFloor || verdict.about < ABOUT_FLOOR) {
          if (verdict && verdict.discusses < MENTION_FLOOR) stats.notDiscussed = (stats.notDiscussed ?? 0) + 1;
          if (verdict && verdict.worthy < worthyFloor) stats.notWorthy = (stats.notWorthy ?? 0) + 1;
          return false;
        }
        confidenceOf.set(`${chunkId}\u0000${key}`, verdict.discusses); return true;
      }));
    }
    // One node per name: "Workstream" the product and "Workstream" the project are one thing filed twice (772 such
    // groups held 10k links on 2026-09-20). A name that already exists under ANY kind is reused under that kind.
    const variants = new Map<string, string>();   // every kind-variant of a proposed key -> the proposed key
    for (const [key, entity] of named) for (const kind of ENTITY_KINDS) variants.set(entityKeyOf(kind, entity.name), key);
    const existing = await read<{ rows: { entityKey: string; kind: string }[] | null }>(readBatch().varAs("rows", g().nWithLabelWhere("Entity", Predicate.isIn("entityKey", [...variants.keys()])).where(tenantScope).project(fields("entityKey", "kind"))).returning(["rows"]), "existing_names");
    const rekey = new Map<string, string>();
    for (const row of existing.rows ?? []) { const proposed = variants.get(row.entityKey)!; if (proposed !== row.entityKey && !rekey.has(proposed) && !(existing.rows ?? []).some((other) => other.entityKey === proposed)) rekey.set(proposed, row.entityKey); }
    for (const [from, to] of rekey) {
      named.set(to, { ...named.get(from)!, kind: to.split(":")[1] }); named.delete(from);
      for (const [chunkId, keys] of perChunk) if (keys.includes(from)) { perChunk.set(chunkId, [...new Set(keys.map((key) => key === from ? to : key))]); confidenceOf.set(`${chunkId}\u0000${to}`, confidenceOf.get(`${chunkId}\u0000${from}`)!); }
    }
    const kept = new Set([...perChunk.values()].flat());
    for (const key of [...named.keys()]) if (!kept.has(key)) named.delete(key);
    // A new name is linked to the passage it came from, so it becomes a candidate for every similar passage after it.
    const entries = [...named.entries()];
    let out: any = writeBatch();
    const varOf = new Map<string, string>();
    entries.forEach(([key, entity], index) => {
      varOf.set(key, `n${index}`);
      out = out
        .varAs(`n${index}x`, g().nWithLabelWhere("Entity", SourcePredicate.eq("entityKey", key)).where(tenantScope).limit(1))
        .varAsIf(`n${index}n`, BatchCondition.varEmpty(`n${index}x`), g().addN("Entity", { tenant_id: TENANT, entityKey: key, name: entity.name.trim(), kind: entity.kind, entityType: entity.kind,
        }));
    });
    let edge = 0;
    page.forEach((chunkId, index) => {
      const me = `k${index}`;
      out = out.varAs(me, g().nWithLabelWhere("Chunk", SourcePredicate.eq("chunkId", chunkId)).where(tenantScope).limit(1))
        .varAs(`${me}p`, g().n(NodeRef.var(me)).setProperty("needsNaming", false).setProperty("namedAt", Expr.datetime()));
      for (const key of perChunk.get(chunkId) ?? []) for (const suffix of ["x", "n"])
        out = out.varAsIf(`l${edge++}`, BatchCondition.varNotEmpty(`${varOf.get(key)}${suffix}`), g().n(NodeRef.var(me)).addE("MENTIONS", NodeRef.var(`${varOf.get(key)}${suffix}`), { tenant_id: TENANT, confidence: confidenceOf.get(`${chunkId}\u0000${key}`)!, entityKey: key, by: "llm+jev" }));
    });
    await write(out.returning([]), "name_chunks");
    stats.named += entries.length; stats.chunks += page.length; stats.mentions += [...perChunk.values()].reduce((sum, keys) => sum + keys.length, 0);
    stats.rejected = (stats.rejected ?? 0) + proposed - kept.size;
    console.log(`[${new Date().toTimeString().slice(0, 8)}] named ${stats.chunks}/${chunkIds.length} passages · ${stats.named} names kept, ${stats.rejected} rejected by Jev (${stats.notDiscussed ?? 0} not discussed, ${stats.notWorthy ?? 0} not node-worthy) · ${usage.llmCalls} llm calls`);
  }
}

async function pool<T>(items: T[], workers: number, run: (item: T) => Promise<void>) {
  let next = 0;
  await Promise.all(Array.from({ length: Math.min(workers, items.length) }, async () => { while (next < items.length) await run(items[next++]); }));
}

// ---------------------------------------------------------------- ingest

function chunksOf(text: string): string[] {
  const output: string[] = [];
  let current = "";
  for (let paragraph of text.split("\n")) {
    if (current.length + paragraph.length + 1 > CHUNK_CHARS) {
      if (current) output.push(current);
      while (paragraph.length > CHUNK_CHARS) {
        output.push(paragraph.slice(0, CHUNK_CHARS));
        paragraph = paragraph.slice(CHUNK_CHARS);
      }
      current = paragraph;
    } else current = current ? `${current}\n${paragraph}` : paragraph;
  }
  if (current.trim()) output.push(current);
  return output.filter((chunk) => chunk.trim());
}


// identical text, not similar text: whitespace collapsed, then hashed
const passageHash = (text: string) => createHash("sha256").update(text.replace(/\s+/g, " ").trim()).digest("hex").slice(0, 32);

// Everything the corpus-wide hygiene commands do, for ONE freshly linked document, read live — so a new document enters
// the graph already clean: (1) every link gets its second reading (mean of two >= floor, and the passage must be ABOUT
// it); (2) an entity on at least half of its passages moves to one document link; (3) its passages are crosslinked to
// passages of other documents that share two or more walkable entities, typed by Jev.
async function settleDocument(documentId: string, stats: LinkStats) {
  const links = (await read<{ rows: EdgeRow[] | null }>(readBatch().varAs("rows", chunksOfDocument(documentId).outE("MENTIONS").valueMap(["$id", "$from", "$to", "confidence", "by", "entityKey"])).returning(["rows"]), "settle_links")).rows ?? [];
  if (!links.length) return;
  const passages = (await read<{ rows: any[] | null }>(readBatch().varAs("rows", chunksOfDocument(documentId).project([PropertyProjection.renamed("$id", "id"), ...fields("chunkId", "title", "content")])).returning(["rows"]), "settle_passages")).rows ?? [];
  const names = new Map<number, EntityRow>();
  const ids = [...new Set(links.map((edge) => edge.$to))];
  for (let start = 0; start < ids.length; start += 500) for (const row of (await read<{ rows: EntityRow[] | null }>(readBatch().varAs("rows", g().n(ids.slice(start, start + 500)).valueMap(["$id", "entityKey", "name", "kind"])).returning(["rows"]), "settle_entities")).rows ?? []) names.set(row.$id, row);
  // (1) second reading
  const byChunk = new Map<number, EdgeRow[]>();
  for (const edge of links) if (names.has(edge.$to)) byChunk.set(edge.$from, [...(byChunk.get(edge.$from) ?? []), edge]);
  const verdicts = await vetNames(passages.filter((row) => byChunk.has(Number(row.id))).map((row) => ({ chunkId: row.chunkId, title: row.title ?? "", content: row.content, names: byChunk.get(Number(row.id))!.map((edge) => names.get(edge.$to)!.name) })));
  const chunkIdOf = new Map(passages.map((row) => [Number(row.id), row.chunkId as string]));
  const refuted = links.filter((edge) => { const verdict = verdicts.get(`${chunkIdOf.get(edge.$from)}\u0000${names.get(edge.$to)?.name}`); return !verdict || (edge.confidence + verdict.discusses) / 2 < MENTION_FLOOR || verdict.about < ABOUT_FLOOR; });
  const gone = new Set(refuted.map((edge) => edge.$id)), standing = links.filter((edge) => !gone.has(edge.$id));
  // (2) document-wide facts
  const per = new Map<number, EdgeRow[]>();
  for (const edge of standing) per.set(edge.$to, [...(per.get(edge.$to) ?? []), edge]);
  const wide = passages.length >= 4 ? [...per].filter(([, edges]) => edges.length / passages.length >= 0.5) : [];
  let batch: any = writeBatch().varAs("document", documentById(documentId).limit(1));
  wide.forEach(([entity, edges], index) => {
    batch = batch.varAs(`h${index}`, g().n(NodeRef.var("document")).outE("MENTIONS").where(Predicate.eq("entityKey", names.get(entity)!.entityKey)))
      .varAsIf(`a${index}`, BatchCondition.varEmpty(`h${index}`), g().n(NodeRef.var("document")).addE("MENTIONS", entity, { tenant_id: TENANT, entityKey: names.get(entity)!.entityKey, confidence: Math.max(...edges.map((edge) => edge.confidence)), by: "consolidated", passages: edges.length }));
    for (const edge of edges) gone.add(edge.$id);
  });
  if (gone.size) batch = batch.varAs("dropped", g().dropEdgeById([...gone]));
  await write(batch.returning([]), "settle_document");
  stats.mentions -= gone.size;
  // (3) crosslinks
  const specific = links.filter((edge) => !gone.has(edge.$id)), mine = new Set(passages.map((row) => Number(row.id)));
  const targets = [...new Set(specific.map((edge) => edge.$to))];
  const partners = new Map<string, { weight: number; via: string[] }>();
  for (let start = 0; start < targets.length; start += 40) {
    let reach: any = readBatch();
    targets.slice(start, start + 40).forEach((entity, index) => { reach = reach.varAs(`r${index}`, g().n(entity).in("MENTIONS").hasLabel("Chunk").limit(HUB + 1).project([PropertyProjection.renamed("$id", "id")])); });
    const held = await read<Record<string, { id: number }[] | null>>(reach.returning(targets.slice(start, start + 40).map((_, index) => `r${index}`)), "settle_reach");
    targets.slice(start, start + 40).forEach((entity, index) => {
      const holders = (held[`r${index}`] ?? []).map((row) => Number(row.id));
      if (holders.length < 2 || holders.length > HUB) return;
      for (const edge of specific.filter((link) => link.$to === entity)) for (const other of holders) if (!mine.has(other)) {
        const key = `${edge.$from}>${other}`, entry = partners.get(key) ?? { weight: 0, via: [] };
        entry.weight += 1 / Math.log2(1 + holders.length); entry.via.push(names.get(entity)!.name); partners.set(key, entry);
      }
    });
  }
  const best = new Map<number, [string, number][]>();
  for (const [key, entry] of partners) if (entry.via.length >= 2) { const from = Number(key.split(">")[0]); best.set(from, [...(best.get(from) ?? []), [key, entry.weight]]); }
  const candidates = [...best.values()].flatMap((list) => list.sort((x, y) => y[1] - x[1]).slice(0, 3).map(([key]) => key));
  let crosslinks = 0;
  await pool(candidates, JEV_IN_FLIGHT, async (key) => {
    const [a, b] = key.split(">").map(Number);
    const data = await read<{ chunks: any[] | null }>(readBatch().varAs("chunks", g().n([a, b]).project([PropertyProjection.renamed("$id", "id"), ...fields("title", "content", "documentId")])).returning(["chunks"]), "settle_pair");
    const of = new Map((data.chunks ?? []).map((chunk) => [Number(chunk.id), chunk]));
    if (!of.get(a) || !of.get(b)) return;
    const answers = await jev({ a: `${of.get(a).title}\n${of.get(a).content}`, b: `${of.get(b).title}\n${of.get(b).content}` }, {
      related: noul("passages a and b are about the same specific matter — the same deal, person's situation, product decision, technique or idea — so that someone reading one should be pointed to the other. Sharing a broad theme is not enough"),
      type: choice("How passages a and b relate", { same_event: "two records of the very same meeting, call, talk or document", a_is_newer: "a states a later version of facts, plans or decisions that b also states", b_is_newer: "b states a later version of facts, plans or decisions that a also states", related: "about the same specific matter, but none of the above" }) });
    if ((answers.related?.noul ?? 0) < MENTION_FLOOR) return;
    const type = answers.type?.choice ?? "related", [label, from, to] = type === "a_is_newer" ? ["UPDATES", a, b] : type === "b_is_newer" ? ["UPDATES", b, a] : type === "same_event" ? ["EXTENDS", a, b] : ["RELATES_TO", a, b];
    await write(writeBatch().varAs("edge", g().n(from).addE(label, to, { tenant_id: TENANT, confidence: answers.related!.noul!, via: partners.get(key)!.via, by: "jev" })).returning([]), "settle_crosslink");
    crosslinks++;
  });
  console.log(`   settled ${documentId}: ${links.length} links → ${links.length - gone.size} specific + ${wide.length} document-wide · ${refuted.length} refuted on second reading · ${crosslinks} crosslinks`);
}

type SourceJson = { id?: string; customId?: string; title?: string; content?: string; raw?: string; source?: string; url?: string; metadata?: Record<string, any> };

async function ingestOne(dir: string, filename: string, kindOverride: string | null, replace: boolean, categories: Category[], stats: LinkStats): Promise<string> {
  const source = JSON.parse(readFileSync(join(dir, filename), "utf8")) as SourceJson;
  const text = source.content ?? source.raw ?? "";
  if (!text.trim()) return "empty";
  const metadata = source.metadata ?? {};
  const documentId = source.id ?? filename.replace(/\.json$/, "");
  const title = String(source.title ?? metadata.title ?? "").slice(0, 300);
  const kind = kindOverride ?? String(metadata.type ?? metadata.kind ?? ""), sourceName = String(metadata.source ?? source.source ?? ""), url = String(metadata.url ?? source.url ?? "");
  if (!replace) {
    const existing = await read<{ found: unknown[] | null }>(readBatch().varAs("found", documentById(documentId).limit(1).project(fields("documentId"))).returning(["found"]), "document_exists");
    if (existing.found?.length) return "exists";
  }
  const chunks = chunksOf(text), hashes = chunks.map(passageHash);
  // the same document under another id is not a new document (a third of the corpus once was exactly that)
  const twins = (await read<{ rows: { documentId: string; contentHash: string }[] | null }>(readBatch().varAs("rows", g().nWithLabelWhere("Chunk", Predicate.isIn("contentHash", [...new Set(hashes)])).where(tenantScope).project(fields("documentId", "contentHash"))).returning(["rows"]), "copy_check")).rows ?? [];
  const covered = new Map<string, Set<string>>();
  for (const row of twins) if (row.documentId !== documentId) (covered.get(row.documentId) ?? covered.set(row.documentId, new Set()).get(row.documentId)!).add(row.contentHash);
  const twin = [...covered].find(([, seen]) => new Set(hashes).size === seen.size);
  if (twin) { console.log(`${documentId.padEnd(22)} copy of ${twin[0]} — skipped`); return "copy"; }
  const vectors = await embed(chunks);
  const addChunk = (into: any, index: number) => into
    .varAs(`c${index}`, g().addN("Chunk", {
      tenant_id: TENANT, userId: USER, chunkId: `${documentId}:${index}`, documentId, title, chunkIndex: index, ordinal: index, documentKind: kind, source: sourceName,
      content: chunks[index], contentHash: hashes[index], embedding: PropertyValue.f32Array(vectors[index]), embeddingModel: `openai:${EMBED_MODEL}`, createdAt: Expr.datetime(),
    }))
    .varAs(`h${index}`, g().n(NodeRef.var("document")).addE("HAS_CHUNK", NodeRef.var(`c${index}`), { tenant_id: TENANT, ordinal: index }));
  let batch: any = writeBatch();
  if (replace) batch = batch.varAs("oldChunks", chunksOfDocument(documentId).drop()).varAs("oldDocument", documentById(documentId).drop());
  batch = batch.varAs("document", g().addN("SourceDocument", {
    tenant_id: TENANT, userId: USER, documentId, customId: source.customId ?? "", title, source: sourceName, sourceType: sourceName, kind, url, uri: url,
    chunkCount: chunks.length, checksum: createHash("sha256").update(text).digest("hex").slice(0, 24), status: "indexed", createdAt: Expr.datetime(),
    metadata: Object.fromEntries(Object.entries(metadata).map(([key, value]) => [key, typeof value === "object" ? JSON.stringify(value) : value])),
  }));
  const first = Math.min(chunks.length, CHUNKS_PER_WRITE);
  for (let index = 0; index < first; index++) batch = addChunk(batch, index);
  await write(batch.returning([]), "ingest_document");
  for (let start = first; start < chunks.length; start += CHUNKS_PER_WRITE) {   // books
    let more: any = writeBatch().varAs("document", documentById(documentId).limit(1));
    for (let index = start; index < Math.min(chunks.length, start + CHUNKS_PER_WRITE); index++) more = addChunk(more, index);
    await write(more.returning([]), "ingest_chunks");
  }
  const ids = chunks.map((_, index) => `${documentId}:${index}`), docOf = new Map([[documentId, { documentId, title, kind, source: sourceName, url } as Doc]]);
  await linkChunks(ids, docOf, categories, stats);
  const flagged = await read<{ chunks: { chunkId: string }[] | null }>(readBatch().varAs("chunks", g().nWithLabelWhere("Chunk", Predicate.and([Predicate.eq("documentId", documentId), Predicate.eq("needsNaming", true)])).where(tenantScope).project(fields("chunkId"))).returning(["chunks"]), "flagged_in_document");
  if (flagged.chunks?.length) await nameChunks(flagged.chunks.map((row) => row.chunkId), docOf, stats);
  await settleDocument(documentId, stats);
  console.log(`${documentId.padEnd(22)} ${String(chunks.length).padStart(3)} chunks  ${title.slice(0, 70)}`);
  return "ok";
}

// kb ingest <dir> [--kind K] [--replace] — document, chunks, and their links, in one go.
async function ingest(args: Args) {
  await bootstrap();
  const dir = args.positional[0], categories = await taxonomy();
  const files = readdirSync(dir).filter((name) => name.endsWith(".json")).sort();
  const counts: Record<string, number> = {}, stats: LinkStats = { chunks: 0, mentions: 0, flagged: 0, named: 0 }, started = Date.now();
  await pool(files, 16, async (filename) => {
    const outcome = await ingestOne(dir, filename, args.values("--kind")[0] ?? null, args.has("--replace"), categories, stats).catch((error) => { console.error(`FAILED ${filename}: ${String(error).slice(0, 200)}`); return "failed"; });
    counts[outcome] = (counts[outcome] ?? 0) + 1;
  });
  console.log(JSON.stringify(counts));
  report(stats, started);
  if (counts.failed) process.exit(1);
}

// ---------------------------------------------------------------- filters

type Args = { positional: string[]; values: (name: string) => string[]; has: (name: string) => boolean; number: (name: string, fallback: number) => number };

const BOOLEAN_FLAGS = new Set(["--replace", "--all-entities", "--chunks", "--full", "--no-graph", "--no-rerank", "--json", "--dry-run", "--redo", "--seed", "--resolve", "--mine", "--with-boilerplate", "--yes"]);
function parseArgs(argv: string[]): Args {
  const positional: string[] = [], flags = new Map<string, string[]>();
  for (let index = 0; index < argv.length; index++) {
    const arg = argv[index];
    if (!arg.startsWith("--")) positional.push(arg);
    else if (BOOLEAN_FLAGS.has(arg)) flags.set(arg, []);
    else flags.set(arg, [...(flags.get(arg) ?? []), argv[++index] ?? ""]);
  }
  return {
    positional,
    values: (name) => flags.get(name) ?? [],
    has: (name) => flags.has(name),
    number: (name, fallback) => (flags.get(name)?.[0] ? Number(flags.get(name)![0]) : fallback),
  };
}


function wherePredicate(expression: string) {
  const match = expression.match(/^(\w+)\s*(!=|=|~|\^)\s*(.*)$/);
  if (!match) throw new Error(`bad --where "${expression}" (use field=value, field!=value, field~substring, field^prefix)`);
  const [, field, operator, value] = match;
  if (operator === "=") return Predicate.eq(field, value);
  if (operator === "!=") return Predicate.neq(field, value);
  if (operator === "~") return Predicate.contains(field, value);
  return Predicate.startsWith(field, value);
}


// Filters are predicates on indexed top-level properties of the node being searched — no walking to find them.
function documentStream(args: Args) {
  const predicates = [tenantScope];
  const isIn = (flag: string, property: string) => { if (args.values(flag).length) predicates.push(Predicate.isIn(property, args.values(flag))); };
  isIn("--kind", "kind"); isIn("--source", "source"); isIn("--doc", "documentId"); isIn("--status", "status");
  for (const expression of args.values("--where")) predicates.push(wherePredicate(expression));
  return g().nWithLabelWhere("SourceDocument", Predicate.and(predicates));
}

const CHUNK_FIELD: Record<string, string> = { kind: "documentKind", doc: "documentId" };
// Two kinds of chunk filter. `anchor` predicates are equality tests on indexed properties: they go into the source
// step, where the planner serves them from the index, so the candidate stream is small before ranking starts.
// `post` predicates cannot be index-served (ranges, negations, substring tests): they run on what is left.
function chunkFilters(args: Args): { anchor: Predicate[]; post: Predicate[] } {
  const anchor: Predicate[] = [], post: Predicate[] = [];
  const isIn = (flag: string, property: string, map = (value: string) => value) => {
    const values = args.values(flag).map(map);
    if (values.length) anchor.push(values.length === 1 ? Predicate.eq(property, values[0]) : Predicate.isIn(property, values));
  };
  isIn("--kind", "documentKind"); isIn("--source", "source"); isIn("--doc", "documentId"); isIn("--category", "category", normalise);
  if (args.has("--mine")) anchor.push(Predicate.eq("aboutOwner", true));
  if (!args.has("--with-boilerplate")) post.push(Predicate.neq("boilerplate", true));
  if (args.values("--min-salience").length) post.push(Predicate.gte("salience", args.number("--min-salience", 0)));
  for (const expression of args.values("--where")) {
    const field = expression.match(/^(\w+)/)?.[1] ?? "";
    post.push(wherePredicate(expression.replace(/^\w+/, CHUNK_FIELD[field] ?? field)));
  }
  return { anchor, post };
}
const postFilter = (args: Args) => Predicate.and([tenantScope, ...chunkFilters(args).post]);

// The candidate stream to rank within, or null when nothing narrows it (then search ranks the whole index directly,
// which is the fast path, and the post filters apply to its results).
// --entity is the one filter that is a graph question: start at the entity and walk in to the chunks that are about it.
async function chunkStream(args: Args): Promise<any | null> {
  const names = args.values("--entity"), { anchor } = chunkFilters(args), post = postFilter(args);
  if (!names.length) return anchor.length ? g().nWithLabelWhere("Chunk", Predicate.and([tenantScope, ...anchor])).where(post) : null;
  const keysOf = (name: string) => ENTITY_KINDS.map((kind) => entityKeyOf(kind, name));
  // an entity is linked at two levels: to the passages that are specifically about it, and to a document it runs through
  const through = (keys: string[]) => g().nWithLabelWhere("Entity", Predicate.isIn("entityKey", keys)).where(tenantScope)
    .union([sub().in("MENTIONS").hasLabel("Chunk"), sub().in("MENTIONS").hasLabel("SourceDocument").out("HAS_CHUNK")]).dedup();
  const narrowed = Predicate.and([post, ...anchor]);
  if (!args.has("--all-entities") || names.length === 1) return through(names.flatMap(keysOf)).where(narrowed);
  let batch: any = readBatch();
  names.forEach((name, index) => { batch = batch.varAs(`c${index}`, through(keysOf(name)).project(fields("chunkId"))); });
  const data = await read<Record<string, { chunkId: string }[] | null>>(batch.returning(names.map((_, index) => `c${index}`)), "chunks_about_all");
  const sets = names.map((_, index) => new Set((data[`c${index}`] ?? []).map((row) => row.chunkId)));
  const shared = [...sets[0]].filter((chunkId) => sets.every((set) => set.has(chunkId)));
  return g().nWithLabelWhere("Chunk", Predicate.isIn("chunkId", shared)).where(narrowed);
}

// ---------------------------------------------------------------- search

type Hit = { id: number; chunkId: string; documentId: string; title: string; chunkIndex: number; content: string; category?: string; salience?: number };
type Ranked = { hit: Hit; score: number; via: string[]; relevance?: number };

//   kb search "<q>" [filters] [--mode hybrid|vector|text] [--k N] [--top N] [--chunks] [--expand N] [--full] [--no-rerank] [--no-graph] [--json]
async function search(args: Args) {
  const query = args.positional.join(" ");
  if (!query) throw new Error(USAGE);
  const mode = args.values("--mode")[0] ?? "hybrid", K = args.number("--k", 24), top = args.number("--top", 8), started = Date.now();
  const project = [PropertyProjection.renamed("$id", "id"), ...fields("chunkId", "documentId", "title", "chunkIndex", "content", "category", "salience")];
  // Filters build the candidate stream first; vector/BM25 then rank only those candidates (Helix prefiltering).
  const channels: [string, any][] = [];
  if (mode !== "text") {
    const [vector] = await embed([query]);
    channels.push(["~", ((await chunkStream(args))?.vectorSearch("Chunk", "embedding", vector, K, TENANT) ?? g().vectorSearchNodes("Chunk", "embedding", vector, K, TENANT).where(postFilter(args))).project(project)]);
  }
  if (mode !== "vector") channels.push(["=", ((await chunkStream(args))?.textSearch("Chunk", "content", query.slice(0, 200), K, TENANT) ?? g().textSearchNodes("Chunk", "content", query.slice(0, 200), K, TENANT).where(postFilter(args))).project(project)]);
  // Independent reads: a slow channel must not cost the other its results.
  const lists = await Promise.all(channels.map(async ([name, traversal]) => {
    const data = await read<{ hits: Hit[] | null }>(readBatch().varAs("hits", traversal).returning(["hits"]), `search_${name}`)
      .catch((error) => { console.error(`(${name} channel failed: ${String(error).slice(0, 80)})`); return { hits: [] as Hit[] }; });
    return [name, data.hits ?? []] as [string, Hit[]];
  }));
  // Reciprocal-rank fusion, app-side: Helix returns separate ranked lists and prescribes RRF to join them.
  const fused = new Map<number, Ranked>();
  for (const [channel, list] of lists) list.forEach((hit, rank) => {
    const entry = fused.get(hit.id) ?? { hit, score: 0, via: [] };
    entry.score += 1 / (60 + rank + 1); entry.via.push(channel); fused.set(hit.id, entry);
  });
  let ranked = [...fused.values()].sort((a, b) => b.score - a.score);
  const best = ranked[0]?.score ?? 1;
  for (const entry of ranked) entry.score = entry.score / best + 0.3 * Number(entry.hit.salience ?? 0);   // Helix: w_rrf 1.0, w_sal 0.3

  // Jev as the cross-encoder in Helix's re-rank formula: reads each candidate against the query, in parallel.
  if (!args.has("--no-rerank") && ranked.length) {
    const pool = ranked.slice(0, Math.max(top * 2, 12));
    await Promise.all(pool.map(async (entry) => {
      const answers = await jev({ request: query, passage: entry.hit.content }, { useful: noul("The passage answers the request or contains information directly useful for it") }).catch(() => null);
      entry.relevance = answers?.useful?.noul;
      if (entry.relevance !== undefined) entry.score = entry.relevance + 0.2 * entry.score;
    }));
    ranked = [...pool.sort((a, b) => b.score - a.score), ...ranked.slice(pool.length)];
  }
  if (!args.has("--chunks")) {
    const perDocument = new Map<string, Ranked>();
    for (const entry of ranked) if (!perDocument.has(entry.hit.documentId)) perDocument.set(entry.hit.documentId, entry);
    ranked = [...perDocument.values()];
  }
  const hits = ranked.slice(0, top);

  const expand = args.number("--expand", 0);
  if (expand && hits.length) {
    let around: any = readBatch();
    hits.forEach(({ hit }, index) => {
      around = around.varAs(`a${index}`, chunksOfDocument(hit.documentId).where(Predicate.between("chunkIndex", Math.max(0, Number(hit.chunkIndex) - expand), Number(hit.chunkIndex) + expand)).project(fields("chunkIndex", "content")));
    });
    const near = await read<Record<string, Chunk[] | null>>(around.returning(hits.map((_, index) => `a${index}`)), "expand_chunks");
    hits.forEach(({ hit }, index) => { hit.content = (near[`a${index}`] ?? []).sort((a, b) => Number(a.chunkIndex) - Number(b.chunkIndex)).map((chunk) => chunk.content).join("\n"); });
  }
  const graph = args.has("--no-graph") || !hits.length ? null : await expandChunks(hits.slice(0, 6).map((entry) => entry.hit.chunkId));   // chunk ids, so --chunks or not

  if (args.has("--json")) return console.log(JSON.stringify({ hits, graph }, null, 2));
  for (const { hit, score, via, relevance } of hits) {
    console.log(`\n── ${hit.title}  [${hit.documentId} #${hit.chunkIndex}]`);
    console.log(`   ${relevance !== undefined ? `relevance ${relevance.toFixed(2)} · ` : ""}score ${score.toFixed(3)} ${via.join("")}${hit.category ? ` · #${hit.category}` : ""}${hit.salience !== undefined ? ` · salience ${Number(hit.salience).toFixed(2)}` : ""}`);
    const about = graph?.of[hit.chunkId] ?? [];
    if (about.length) console.log(`   about: ${about.slice(0, 12).join(" · ")}${about.length > 12 ? ` · +${about.length - 12}` : ""}`);
    console.log(args.has("--full") || expand ? hit.content : `   ${hit.content.replace(/\s+/g, " ").slice(0, 500)}`);
  }
  if (graph?.connected.length) {
    console.log("\n── connected through shared entities");
    for (const row of graph.connected) console.log(`   (${row.shared}) ${row.title}  [${row.documentId} #${row.chunkIndex}]`);
  }
  if (graph?.related.length) {
    console.log("\n── related passages (judged: same event / newer statement / same matter)");
    for (const row of graph.related.slice(0, 8)) console.log(`   ${String(row.relation).toLowerCase().padEnd(10)} ${row.title}  [${row.documentId} #${row.chunkIndex}]`);
  }
  if (!hits.length) console.log("nothing found");
  console.log(`\n${((Date.now() - started) / 1000).toFixed(1)}s${usage.jevCalls ? ` · ${usage.jevCalls} jev calls` : ""}`);
}

// Helix EXAMPLES §10, bounded: what each hit is about, and the other chunks reached through the same entities.
// An entity a hundred passages share says nothing about any one of them, so a shared entity counts for 1/log2(1+n)
// where n is how many passages hold it, and hubs over HUB are not walked at all (that was the 60-190 s traversal).
const HUB = 60;
async function expandChunks(chunkIds: string[]) {
  const chunk = (chunkId: string) => g().nWithLabelWhere("Chunk", SourcePredicate.eq("chunkId", chunkId)).where(tenantScope);
  let about: any = readBatch();
  chunkIds.forEach((chunkId, index) => { about = about.varAs(`e${index}`, chunk(chunkId).out("MENTIONS").project([PropertyProjection.renamed("$id", "id"), ...fields("name")])); });
  const entitiesOf = await read<Record<string, { id: number; name: string }[] | null>>(about.returning(chunkIds.map((_, index) => `e${index}`)), "hit_entities");
  const entityIds = [...new Set(chunkIds.flatMap((_, index) => (entitiesOf[`e${index}`] ?? []).map((row) => row.id)))];
  const of: Record<string, string[]> = {};
  chunkIds.forEach((chunkId, index) => { of[chunkId] = (entitiesOf[`e${index}`] ?? []).map((row) => row.name); });
  // direct passage↔passage edges judged by Jev: another record of the same event, a newer statement, a related matter
  let direct: any = readBatch();
  chunkIds.forEach((chunkId, index) => { for (const label of ["EXTENDS", "UPDATES", "RELATES_TO"]) direct = direct.varAs(`${label}${index}`, chunk(chunkId).both(label).project(fields("chunkId", "documentId", "title", "chunkIndex"))); });
  const linked = await read<Record<string, any[] | null>>(direct.returning(chunkIds.flatMap((_, index) => ["EXTENDS", "UPDATES", "RELATES_TO"].map((label) => `${label}${index}`))), "direct_relations");
  const related = new Map<string, any>();
  for (const [name, rows] of Object.entries(linked)) for (const row of rows ?? []) if (!chunkIds.includes(row.chunkId) && !related.has(row.chunkId)) related.set(row.chunkId, { ...row, relation: name.replace(/\d+$/, "") });
  if (!entityIds.length) return { of, connected: [], related: [...related.values()] };
  let sizes: any = readBatch();
  entityIds.forEach((id, index) => { sizes = sizes.varAs(`s${index}`, g().n(id).inE("MENTIONS").count()); });
  const held = await read<Record<string, number>>(sizes.returning(entityIds.map((_, index) => `s${index}`)), "entity_sizes");
  const walkable = entityIds.map((id, index) => ({ id, size: Number(held[`s${index}`] ?? 0) })).filter((entity) => entity.size > 1 && entity.size <= HUB);
  if (!walkable.length) return { of, connected: [], related: [...related.values()] };
  let walk: any = readBatch();
  walkable.forEach((entity, index) => { walk = walk.varAs(`w${index}`, g().n(entity.id).in("MENTIONS").hasLabel("Chunk").project(fields("chunkId", "documentId", "title", "chunkIndex"))); });
  const reached = await read<Record<string, any[] | null>>(walk.returning(walkable.map((_, index) => `w${index}`)), "expand_chunks_via_entities");
  const tally = new Map<string, any>();
  walkable.forEach((entity, index) => {
    for (const row of reached[`w${index}`] ?? []) {
      if (chunkIds.includes(row.chunkId)) continue;
      const entry = tally.get(row.chunkId) ?? { ...row, shared: 0, weight: 0 };
      entry.shared++; entry.weight += 1 / Math.log2(1 + entity.size); tally.set(row.chunkId, entry);
    }
  });
  return { of, connected: [...tally.values()].sort((a, b) => b.weight - a.weight).slice(0, 8), related: [...related.values()] };
}

// ---------------------------------------------------------------- browse

async function docs(args: Args) {
  const data = await read<{ documents: any[] | null }>(readBatch().varAs("documents", documentStream(args).limit(args.number("--limit", 50)).project(fields("documentId", "title", "kind", "source", "chunkCount", "status"))).returning(["documents"]), "list_documents");
  if (args.has("--json")) return console.log(JSON.stringify(data.documents ?? [], null, 2));
  for (const row of data.documents ?? []) console.log(`${String(row.documentId).padEnd(24)} ${(row.kind || "-").padEnd(16)} ${String(row.chunkCount).padStart(3)}ch ${row.status === "linked" ? "✓" : "·"} ${String(row.title).slice(0, 80)}`);
}

// What a slice of the corpus is about: most-mentioned entities, or most-used categories.
async function tally(args: Args, what: "entities" | "categories") {
  const stream = (await chunkStream(args)) ?? g().nWithLabel("Chunk").where(tenantScope);
  const traversal = what === "entities" ? stream.outE("MENTIONS").groupCount("entityKey") : stream.groupCount("category");
  const data = await read<{ counts: any[] | null }>(readBatch().varAs("counts", traversal).returning(["counts"]), `tally_${what}`);
  const kinds = args.values("--entity-kind"), match = (args.values("--match")[0] ?? "").toLowerCase();
  const rows = (data.counts ?? []).map((row) => {
    if (what === "categories") return { kind: "", name: String(row.category ?? "(unlinked)"), count: Number(row.count) };
    const parts = String(row.entityKey).split(":");
    return { kind: parts[1], name: parts.slice(2).join(":"), count: Number(row.count) };
  }).filter((row) => (!kinds.length || kinds.includes(row.kind)) && (!match || row.name.includes(match))).sort((a, b) => b.count - a.count).slice(0, args.number("--top", 40));
  if (args.has("--json")) return console.log(JSON.stringify(rows, null, 2));
  for (const row of rows) console.log(`${String(row.count).padStart(6)}  ${row.kind.padEnd(13)} ${row.name}`);
}

async function show(documentId: string) {
  const data = await read<{ chunks: (Chunk & { title: string })[] | null }>(readBatch().varAs("chunks", chunksOfDocument(documentId).project(fields("chunkIndex", "content", "title"))).returning(["chunks"]), "show_document");
  const chunks = (data.chunks ?? []).sort((a, b) => Number(a.chunkIndex) - Number(b.chunkIndex));
  if (!chunks.length) return console.log(`no document ${documentId}`);
  console.log(`# ${chunks[0].title}\n`);
  for (const chunk of chunks) console.log(chunk.content + "\n");
}

async function stats() {
  const count = (label: string) => g().nWithLabel(label).where(tenantScope).count();
  const data = await read<Record<string, any>>(readBatch()
    .varAs("documents", g().nWithLabel("SourceDocument").where(tenantScope).groupCount("kind"))
    .varAs("status", g().nWithLabel("SourceDocument").where(tenantScope).groupCount("status"))
    .varAs("chunks", count("Chunk")).varAs("entities", count("Entity")).varAs("categories", count("Category"))
    .varAs("linked", g().nWithLabel("Chunk").where(tenantScope).where(Predicate.eq("linkVersion", LINK_VERSION)).count())
    .returning(["documents", "status", "chunks", "entities", "categories", "linked"]), "stats");
  const total = (rows: { count: number }[]) => (rows ?? []).reduce((sum, row) => sum + Number(row.count), 0);
  const linkedDocs = Number(((data.status ?? []) as any[]).find((row) => row.status === "linked")?.count ?? 0);
  console.log(`${total(data.documents)} documents (${linkedDocs} linked) · ${Number(data.chunks)} chunks (${Number(data.linked)} linked) · ${Number(data.entities)} entities · ${Number(data.categories)} categories\n`);
  for (const row of [...(data.documents ?? [])].sort((a: any, b: any) => Number(b.count) - Number(a.count))) console.log(`${String(row.count).padStart(6)}  ${row.kind || "(no kind)"}`);
}

// ---------------------------------------------------------------- graph hygiene (backup → prune → vet → check)

// A backup is a readable copy of the graph's judgement layer: every entity, every link with its endpoints and
// confidence, every chunk's judged properties. Prune and vet only ever work from one, so nothing is deleted that is
// not already on disk. Node and edge ids are stable, so a backup is also the work list.
const BACKUPS = join(import.meta.dir, "backup");
const jsonl = <T>(path: string): T[] => existsSync(path) ? readFileSync(path, "utf8").split("\n").filter(Boolean).map((line) => JSON.parse(line) as T) : [];
const latestBackup = (args: Args) => {
  const dir = args.values("--from")[0] ?? readdirSync(BACKUPS, { withFileTypes: true }).filter((entry) => entry.isDirectory() && existsSync(join(BACKUPS, entry.name, "chunk_mentions.jsonl"))).map((entry) => join(BACKUPS, entry.name)).sort((x, y) => statSync(x).mtimeMs - statSync(y).mtimeMs).pop();   // newest
  if (!dir) throw new Error("no backup found — run `kb backup` first");
  return dir;
};
type EdgeRow = { $id: number; $from: number; $to: number; confidence: number; by?: string; entityKey: string };
type EntityRow = { $id: number; entityKey: string; name: string; kind: string };

async function backup(args: Args) {
  const dir = args.positional[0] ?? join(BACKUPS, new Date().toISOString().slice(0, 16).replace(/[:T]/g, "-"));
  mkdirSync(dir, { recursive: true });
  const all = async (name: string, traversal: any) => (await read<{ rows: any[] | null }>(readBatch().varAs("rows", traversal).returning(["rows"]), name)).rows ?? [];
  const dump = (file: string, rows: unknown[]) => { writeFileSync(join(dir, file), rows.map((row) => JSON.stringify(row)).join("\n") + "\n"); console.log(`${file.padEnd(26)} ${rows.length}`); };
  dump("entities.jsonl", await all("backup_entities", g().nWithLabel("Entity").where(tenantScope).valueMap(["$id", "entityKey", "name", "kind"])));
  const chunks = await all("backup_chunks", g().nWithLabel("Chunk").where(tenantScope).valueMap(["$id", "chunkId", "documentId", "category", "salience", "aboutOwner", "boilerplate", "needsNaming", "linkVersion"]));
  dump("chunks.jsonl", chunks);
  dump("document_mentions.jsonl", await all("backup_document_mentions", g().nWithLabel("SourceDocument").where(tenantScope).outE("MENTIONS").valueMap(["$id", "$from", "$to", "confidence", "entityKey", "evidence"])));
  // chunk links are the bulk (hundreds of thousands): read them a page of documents at a time
  const documents = [...new Set(chunks.map((chunk) => chunk.documentId as string))];
  for (const file of ["chunk_mentions.jsonl", "chunk_categories.jsonl"]) writeFileSync(join(dir, file), "");
  let mentions = 0;
  for (let start = 0; start < documents.length; start += 60) {
    let batch: any = readBatch();
    const names: string[] = [];
    documents.slice(start, start + 60).forEach((documentId, index) => {
      batch = batch.varAs(`m${index}`, chunksOfDocument(documentId).outE("MENTIONS").valueMap(["$id", "$from", "$to", "confidence", "by", "entityKey"]))
        .varAs(`c${index}`, chunksOfDocument(documentId).outE("IN_CATEGORY").valueMap(["$id", "$from", "$to", "confidence"]));
      names.push(`m${index}`, `c${index}`);
    });
    const data = await read<Record<string, any[] | null>>(batch.returning(names), "backup_links");
    for (const name of names) {
      const rows = data[name] ?? [];
      if (!rows.length) continue;
      appendFileSync(join(dir, name[0] === "m" ? "chunk_mentions.jsonl" : "chunk_categories.jsonl"), rows.map((row) => JSON.stringify(row)).join("\n") + "\n");
      if (name[0] === "m") mentions += rows.length;
    }
  }
  console.log(`${"chunk_mentions.jsonl".padEnd(26)} ${mentions}\n→ ${dir}`);
}

// Edges go by id, a few thousand a write; nodes go by id too and take their remaining edges with them.
async function dropEdges(ids: number[], label: string) {
  for (let start = 0; start < ids.length; start += 2000) {
    await write(writeBatch().varAs("gone", g().dropEdgeById(ids.slice(start, start + 2000))).returning([]), label);
    console.log(`  ${label}: ${Math.min(ids.length, start + 2000)}/${ids.length}`);
  }
}
async function dropNodes(ids: number[], label: string) {
  for (let start = 0; start < ids.length; start += 400) {
    await write(writeBatch().varAs("gone", g().n(ids.slice(start, start + 400)).drop()).returning([]), label);
    console.log(`  ${label}: ${Math.min(ids.length, start + 400)}/${ids.length}`);
  }
}

// The links that stand: judged by the current pipeline (they carry `by`), at or over the floor (an unchecked LLM link
// stands until vet has read it), one per chunk→entity pair — Jev's own judgement wins over an LLM's, then the surer one.
function standingLinks(dir: string): EdgeRow[] {
  const rank = (edge: EdgeRow) => (edge.by === "jev" ? 2 : 1) + edge.confidence;
  const best = new Map<string, EdgeRow>();
  for (const edge of jsonl<EdgeRow>(join(dir, "chunk_mentions.jsonl"))) {
    if (!edge.by || (edge.by !== "llm" && edge.confidence < MENTION_FLOOR)) continue;
    const pair = `${edge.$from}>${edge.$to}`, held = best.get(pair);
    if (!held || rank(edge) > rank(held)) best.set(pair, edge);
  }
  return [...best.values()];
}

// kb prune [--from dir] [--yes] — drop every link that does not stand: under the floor, left behind by an earlier
// pipeline version, or a second copy of the same pair; and every category edge but a chunk's newest. No model calls.
async function prune(args: Args) {
  const dir = latestBackup(args);
  const all = jsonl<EdgeRow>(join(dir, "chunk_mentions.jsonl")), standing = new Set(standingLinks(dir).map((edge) => edge.$id));
  const fallen = all.filter((edge) => !standing.has(edge.$id));
  const newestCategory = new Map<number, number>(), categories = jsonl<EdgeRow>(join(dir, "chunk_categories.jsonl"));
  for (const edge of categories) newestCategory.set(edge.$from, Math.max(newestCategory.get(edge.$from) ?? 0, edge.$id));
  const staleCategories = categories.filter((edge) => newestCategory.get(edge.$from) !== edge.$id);
  console.log(`${dir}\n${all.length} links · ${fallen.filter((edge) => !edge.by).length} from earlier pipeline versions · ${fallen.filter((edge) => edge.by && edge.confidence < MENTION_FLOOR).length} under ${MENTION_FLOOR} · ${fallen.filter((edge) => edge.by && edge.confidence >= MENTION_FLOOR).length} duplicate pairs → ${standing.size} stand\n${staleCategories.length} stale category edges`);
  // an entity no standing link points at, and no document roster holds, connects nothing
  const held = new Set([...all.filter((edge) => standing.has(edge.$id)).map((edge) => edge.$to), ...jsonl<EdgeRow>(join(dir, "document_mentions.jsonl")).map((edge) => edge.$to)]);
  const orphans = jsonl<EntityRow>(join(dir, "entities.jsonl")).filter((entity) => !held.has(entity.$id));
  console.log(`${orphans.length} orphan entities${orphans.length ? ` (e.g. ${orphans.slice(0, 6).map((entity) => entity.name).join(" · ")})` : ""}`);
  if (!args.has("--yes")) return console.log("dry run — add --yes");
  await dropEdges(fallen.map((edge) => edge.$id), "prune_links");
  await dropEdges(staleCategories.map((edge) => edge.$id), "prune_category_edges");
  await dropNodes(orphans.map((entity) => entity.$id), "prune_orphan_entities");
}

// ---------------------------------------------------------------- the rest of Helix's memory model
//
// Helix's memory-system model, mapped onto a corpus whose unit of knowledge is the Chunk (atomic Memory nodes were
// dropped on example's call). kb model [--from dir] [--yes] builds, idempotently:
//   User -OWNS-> SourceDocument                      who the corpus belongs to
//   User -HAS_PROFILE-> UserProfile                  static facts + current focus, written by the LLM from the most salient passages about the owner
//   Category -PARENT_OF-> Category                   the 44 leaf categories grouped under a few parents (one LLM call)
//   Chunk -EXTENDS-> Chunk / Chunk -UPDATES-> Chunk  typed by Jev over the RELATES_TO pairs: two records of the same event (the fuller extends the other); a newer statement of the same facts updates the older
//   Entity -ASSOCIATED_WITH{passages}-> Entity       entities that keep appearing in the same passages (counted, no model) — not in Helix's model, added for entity→entity walks
// Not built, with reasons: DERIVES (nothing here is inferred from other memories), Session / DERIVED_FROM (no chat sessions), Connector / IngestionJob (one local tool).
async function model(args: Args) {
  const dir = latestBackup(args), apply = args.has("--yes");
  const dropAll = async (label: string) => write(writeBatch().varAs("old", g().eWithLabel(label)).varAs("gone", g().dropEdgeById(EdgeRef.var("old"))).returning([]), `model_drop_${label}`);

  // 1. owner
  const documents = (await read<{ rows: { id: number }[] | null }>(readBatch().varAs("rows", g().nWithLabel("SourceDocument").where(tenantScope).project([PropertyProjection.renamed("$id", "id")])).returning(["rows"]), "model_documents")).rows ?? [];
  console.log(`owner: User(${USER}) -OWNS-> ${documents.length} documents`);
  if (apply) {
    await dropAll("OWNS");
    await write(writeBatch().varAs("found", g().nWithLabelWhere("User", SourcePredicate.eq("userId", USER)).where(tenantScope).limit(1))
      .varAsIf("made", BatchCondition.varEmpty("found"), g().addN("User", { tenant_id: TENANT, userId: USER, name: "the owner", createdAt: Expr.datetime() })).returning([]), "model_user");
    for (let start = 0; start < documents.length; start += 500) {
      let batch: any = writeBatch().varAs("user", g().nWithLabelWhere("User", SourcePredicate.eq("userId", USER)).where(tenantScope).limit(1));
      documents.slice(start, start + 500).forEach((doc, index) => { batch = batch.varAs(`o${index}`, g().n(NodeRef.var("user")).addE("OWNS", doc.id, { tenant_id: TENANT })); });
      await write(batch.returning([]), "model_owns");
    }
  }

  // 2. profile — the LLM writes, from what Jev marked as about the owner and dense
  const mine = (await read<{ rows: any[] | null }>(readBatch().varAs("rows", g().nWithLabelWhere("Chunk", Predicate.and([Predicate.eq("aboutOwner", true), Predicate.gte("salience", 0.99)])).where(tenantScope).limit(400).project(fields("chunkId", "title", "content"))).returning(["rows"]), "model_owner_passages")).rows ?? [];
  console.log(`profile: ${mine.length} dense passages about the owner → 1 LLM call over the first 16`);
  if (apply && mine.length) {
    const profile = await llm(`You write the profile of ${USER} (the owner) for his own knowledge base, from passages about him. "static": durable facts (who he is, roles, companies, people he works with, skills, constraints) as short factual sentences. "dynamic": what he is currently working on, deciding, or worried about. Only what the passages support; no advice; no flattery. 8-20 items each.`,
      mine.slice(0, 16).map((row) => `=== ${row.title}\n${String(row.content).slice(0, 1800)}`).join("\n\n"), z.object({ static: z.array(z.string()), dynamic: z.array(z.string()) }));
    await write(writeBatch().varAs("old", g().nWithLabel("UserProfile").where(tenantScope).drop()).returning([]), "model_profile_drop");
    await write(writeBatch().varAs("user", g().nWithLabelWhere("User", SourcePredicate.eq("userId", USER)).where(tenantScope).limit(1))
      .varAs("profile", g().addN("UserProfile", { tenant_id: TENANT, userId: USER, profileId: `${TENANT}:${USER}`, static: profile.static, dynamic: profile.dynamic, sources: mine.slice(0, 16).map((row) => row.chunkId), updatedAt: Expr.datetime() }))
      .varAs("edge", g().n(NodeRef.var("user")).addE("HAS_PROFILE", NodeRef.var("profile"), { tenant_id: TENANT })).returning([]), "model_profile");
    console.log(`   static: ${profile.static.length} · dynamic: ${profile.dynamic.length} — e.g. ${profile.static.slice(0, 2).join(" | ")}`);
  }

  // 3. category hierarchy — one LLM call
  const leaves = await taxonomy();
  console.log(`categories: ${leaves.length} leaves → parents by 1 LLM call`);
  if (apply) {
    const tree = await llm(`Group these categories of a personal knowledge base under 6 to 10 parent categories. Every category goes under exactly one parent, spelled exactly as given. A parent has a short name and a one-sentence description.`,
      leaves.map((leaf) => `${leaf.name}: ${leaf.description}`).join("\n"), z.object({ parents: z.array(z.object({ name: z.string(), description: z.string(), children: z.array(z.string()) })) }));
    await write(writeBatch().varAs("old", g().nWithLabelWhere("Category", Predicate.eq("isParent", true)).where(tenantScope).drop()).returning([]), "model_parents_drop");
    let batch: any = writeBatch(), placed = 0;
    tree.parents.forEach((parent, index) => {
      batch = batch.varAs(`p${index}`, g().addN("Category", { tenant_id: TENANT, categoryKey: `${TENANT}:parent:${normalise(parent.name)}`, name: parent.name, description: parent.description, isParent: true }));
      parent.children.filter((child) => leaves.some((leaf) => leaf.name === child)).forEach((child, c) => { placed++; batch = batch.varAs(`p${index}c${c}`, g().nWithLabelWhere("Category", SourcePredicate.eq("categoryKey", `${TENANT}:${normalise(child)}`)).where(tenantScope).limit(1))
        .varAsIf(`p${index}e${c}`, BatchCondition.varNotEmpty(`p${index}c${c}`), g().n(NodeRef.var(`p${index}`)).addE("PARENT_OF", NodeRef.var(`p${index}c${c}`), { tenant_id: TENANT })); });
    });
    await write(batch.returning([]), "model_parents");
    console.log(`   ${tree.parents.length} parents, ${placed}/${leaves.length} leaves placed: ${tree.parents.map((parent) => `${parent.name}(${parent.children.length})`).join(" · ")}`);
  }

  // 4. EXTENDS / UPDATES — Jev types the pairs it already found related
  const related = jsonl<Relation>(join(dir, "relate.jsonl")).filter((row) => row.related >= MENTION_FLOOR);
  const typedLog = join(dir, "relate-types.jsonl"), typed = new Map(jsonl<{ a: number; b: number; type: string }>(typedLog).map((row) => [`${row.a}>${row.b}`, row.type]));
  const untyped = related.filter((row) => !typed.has(`${row.a}>${row.b}`));
  console.log(`relations: ${related.length} related pairs · ${untyped.length} to type → ${untyped.length} Jev requests`);
  await pool(untyped, JEV_IN_FLIGHT, async (row) => {
    const data = await read<{ chunks: any[] | null }>(readBatch().varAs("chunks", g().n([row.a, row.b]).project([PropertyProjection.renamed("$id", "id"), ...fields("title", "content")])).returning(["chunks"]), "model_pair");
    const textOf = new Map((data.chunks ?? []).map((chunk) => [Number(chunk.id), `${chunk.title ?? ""}\n${chunk.content ?? ""}`]));
    const answers = await jev({ a: textOf.get(row.a) ?? "", b: textOf.get(row.b) ?? "" }, { type: choice("How passages a and b relate", {
      same_event: "two records of the very same meeting, call, talk or document (for example notes and the verbatim transcript)",
      a_is_newer: "a states a later version of facts, plans or decisions that b also states — a supersedes b",
      b_is_newer: "b states a later version of facts, plans or decisions that a also states — b supersedes a",
      related: "about the same specific matter, but neither of the above" }) });
    appendFileSync(typedLog, JSON.stringify({ a: row.a, b: row.b, type: answers.type?.choice ?? "related", confidence: answers.type?.confidence ?? 0 }) + "\n");
  });
  const types = jsonl<{ a: number; b: number; type: string; confidence: number }>(typedLog), tally: Record<string, number> = {};
  for (const row of types) tally[row.type] = (tally[row.type] ?? 0) + 1;
  console.log(`   ${JSON.stringify(tally)}`);
  if (apply) {
    await dropAll("EXTENDS"); await dropAll("UPDATES");
    const sizeOf = new Map<number, number>(), docChunks = new Map<string, number>(), chunkRows = jsonl<{ $id: number; documentId: string }>(join(dir, "chunks.jsonl"));
    for (const chunk of chunkRows) docChunks.set(chunk.documentId, (docChunks.get(chunk.documentId) ?? 0) + 1);
    for (const chunk of chunkRows) sizeOf.set(chunk.$id, docChunks.get(chunk.documentId)!);
    let batch: any = writeBatch(), count = 0;
    for (const row of types) {
      if (row.type === "same_event") { const [full, brief] = (sizeOf.get(row.a) ?? 0) >= (sizeOf.get(row.b) ?? 0) ? [row.a, row.b] : [row.b, row.a]; batch = batch.varAs(`t${count++}`, g().n(full).addE("EXTENDS", brief, { tenant_id: TENANT, confidence: row.confidence, by: "jev" })); }
      if (row.type === "a_is_newer" || row.type === "b_is_newer") { const [newer, older] = row.type === "a_is_newer" ? [row.a, row.b] : [row.b, row.a]; batch = batch.varAs(`t${count++}`, g().n(newer).addE("UPDATES", older, { tenant_id: TENANT, confidence: row.confidence, by: "jev" })); }
    }
    if (count) await write(batch.returning([]), "model_typed_relations");
    console.log(`   ${count} EXTENDS/UPDATES edges written`);
  }

  // 5. entity ↔ entity — counted from the links, no model
  const entities = new Map(jsonl<EntityRow>(join(dir, "entities.jsonl")).map((entity) => [entity.$id, entity])), byChunk = new Map<number, number[]>(), held = new Map<number, number>();
  for (const edge of standingLinks(dir)) if (entities.has(edge.$to)) { byChunk.set(edge.$from, [...(byChunk.get(edge.$from) ?? []), edge.$to]); held.set(edge.$to, (held.get(edge.$to) ?? 0) + 1); }
  const together = new Map<string, number>();
  for (const ids of byChunk.values()) { const unique = [...new Set(ids)].sort((x, y) => x - y); for (let i = 0; i < unique.length; i++) for (let j = i + 1; j < unique.length; j++) together.set(`${unique[i]}>${unique[j]}`, (together.get(`${unique[i]}>${unique[j]}`) ?? 0) + 1); }
  // kept when the two share 3+ passages AND that is a real share of the rarer one's passages (a hub appears next to everything)
  const strong = [...together].map(([key, passages]) => { const [a, b] = key.split(">").map(Number); return { a, b, passages, share: passages / Math.min(held.get(a)!, held.get(b)!) }; }).filter((pair) => pair.passages >= 3 && pair.share >= 0.3);
  console.log(`associations: ${together.size} co-occurring entity pairs · ${strong.length} strong (3+ shared passages, ≥30% of the rarer one's) — e.g. ${strong.sort((x, y) => y.passages - x.passages).slice(0, 6).map((pair) => `${entities.get(pair.a)!.name}~${entities.get(pair.b)!.name}(${pair.passages})`).join(" · ")}`);
  if (apply) {
    await dropAll("ASSOCIATED_WITH");
    for (let start = 0; start < strong.length; start += 1000) {
      let batch: any = writeBatch();
      strong.slice(start, start + 1000).forEach((pair, index) => { batch = batch.varAs(`a${index}`, g().n(pair.a).addE("ASSOCIATED_WITH", pair.b, { tenant_id: TENANT, passages: pair.passages, share: Number(pair.share.toFixed(2)) })); });
      await write(batch.returning([]), "model_associations");
    }
  }
  if (!apply) console.log("dry run (relation types were judged and cached) — add --yes to write");
}

// kb dedupe [--from dir] [--yes] — the same document ingested twice (found 2026-09-20: 1,323 of 3,929 documents were
// >=80% passage-for-passage copies of another, mostly the old "recovered" store next to the supermemory export — a third
// of every embedding, model pass and link was spent on copies). Passages are compared by a hash of their text with
// whitespace collapsed — identical text, not similar text. Of a group of copies the fullest document stays (ties: the
// one with a kind, then the non-"rec-" id); the others go with their chunks and links.
async function dedupe(args: Args) {
  const dir = latestBackup(args);
  const chunks = jsonl<{ $id: number; documentId: string }>(join(dir, "chunks.jsonl"));
  const hashOf = new Map<number, string>(), lengthOf = new Map<number, number>();
  for (let start = 0; start < chunks.length; start += 400) {
    const data = await read<{ rows: { id: number; content: string }[] | null }>(readBatch().varAs("rows", g().n(chunks.slice(start, start + 400).map((chunk) => chunk.$id)).project([PropertyProjection.renamed("$id", "id"), ...fields("content")])).returning(["rows"]), "dedupe_texts");
    for (const row of data.rows ?? []) { hashOf.set(Number(row.id), createHash("sha256").update((row.content ?? "").replace(/\s+/g, " ").trim()).digest("hex")); lengthOf.set(Number(row.id), (row.content ?? "").length); }
  }
  // The header passage is not content. Ingesting the same video twice writes the same transcript under two ids but
  // formats the metadata block differently, so 53 documents missed a 100% match by exactly that one passage (51 of
  // them by the first passage alone, measured 2026-09-20). Headers ran 134-216 characters; the one first passage that
  // carried real text was 3,801, so a document's first passage is ignored by the copy test when it is under 500.
  const firstOf = new Map<string, number>();
  for (const chunk of chunks) if (!firstOf.has(chunk.documentId) || chunk.$id < firstOf.get(chunk.documentId)!) firstOf.set(chunk.documentId, chunk.$id);
  const isHeader = (chunk: { $id: number; documentId: string }) => firstOf.get(chunk.documentId) === chunk.$id && (lengthOf.get(chunk.$id) ?? 0) < 500;
  const documents = (await read<{ rows: Doc[] | null }>(readBatch().varAs("rows", g().nWithLabel("SourceDocument").where(tenantScope).project(fields("documentId", "title", "kind", "source"))).returning(["rows"]), "dedupe_documents")).rows ?? [];
  const docOf = new Map(documents.map((doc) => [doc.documentId, doc]));
  const hashesOf = new Map<string, Set<string>>(), docsWith = new Map<string, Set<string>>();
  for (const chunk of chunks) { const hash = hashOf.get(chunk.$id); if (!hash || isHeader(chunk)) continue;
    (hashesOf.get(chunk.documentId) ?? hashesOf.set(chunk.documentId, new Set()).get(chunk.documentId)!).add(hash);
    (docsWith.get(hash) ?? docsWith.set(hash, new Set()).get(hash)!).add(chunk.documentId); }
  const rank = (documentId: string) => hashesOf.get(documentId)!.size * 4 + (docOf.get(documentId)?.kind && docOf.get(documentId)!.kind !== "recovered" ? 2 : 0) + (documentId.startsWith("rec-") ? 0 : 1);
  const copies = new Map<string, string>();   // copy -> the document it duplicates
  for (const [documentId, hashes] of [...hashesOf].sort((a, b) => rank(a[0]) - rank(b[0]))) {
    const overlap = new Map<string, number>();
    for (const hash of hashes) for (const other of docsWith.get(hash)!) if (other !== documentId && !copies.has(other)) overlap.set(other, (overlap.get(other) ?? 0) + 1);
    const best = [...overlap].filter(([other, shared]) => shared / hashes.size >= DEDUPE_SHARE && rank(other) >= rank(documentId)).sort((a, b) => rank(b[0]) - rank(a[0]))[0];
    if (best) copies.set(documentId, best[0]);
  }
  const gone = chunks.filter((chunk) => copies.has(chunk.documentId));
  console.log(`${dir}\n${documents.length} documents · ${copies.size} are copies (≥${Math.round(DEDUPE_SHARE * 100)}% identical passages) · ${gone.length} of ${chunks.length} passages go with them`);
  const byKind: Record<string, number> = {};
  for (const documentId of copies.keys()) byKind[docOf.get(documentId)?.kind || "(no kind)"] = (byKind[docOf.get(documentId)?.kind || "(no kind)"] ?? 0) + 1;
  console.log(`   copies by kind: ${JSON.stringify(byKind)}`);
  for (const [copy, original] of [...copies].slice(0, 8)) console.log(`   ${copy} "${(docOf.get(copy)?.title ?? "").slice(0, 50)}" [${docOf.get(copy)?.kind}]  →  kept ${original} "${(docOf.get(original)?.title ?? "").slice(0, 50)}" [${docOf.get(original)?.kind}]`);
  writeFileSync(join(dir, "dedupe.json"), JSON.stringify(Object.fromEntries(copies), null, 1));
  if (!args.has("--yes")) return console.log(`dry run — full list in ${join(dir, "dedupe.json")} — add --yes`);
  const ids = [...copies.keys()];
  for (let start = 0; start < ids.length; start += 25) {   // chunks are text-indexed: stay far under 512 mutations a write
    let batch: any = writeBatch();
    ids.slice(start, start + 25).forEach((documentId, index) => { batch = batch.varAs(`c${index}`, chunksOfDocument(documentId).drop()).varAs(`d${index}`, documentById(documentId).drop()); });
    await write(batch.returning([]), "dedupe_documents").catch(async () => { for (const documentId of ids.slice(start, start + 25)) await write(writeBatch().varAs("c", chunksOfDocument(documentId).limit(400).drop()).returning([]), "dedupe_big").then(() => write(writeBatch().varAs("c", chunksOfDocument(documentId).drop()).varAs("d", documentById(documentId).drop()).returning([]), "dedupe_big_rest")); });
    console.log(`  dedupe: ${Math.min(ids.length, start + 25)}/${ids.length} documents`);
  }
}

// kb relate [--from dir] [--limit N] [--yes] — passage↔passage crosslinks (Helix's RELATES_TO: association between units
// of knowledge; here the unit is the Chunk). The graph proposes: two passages in DIFFERENT documents that share at least
// two walkable entities (held by 2-60 passages), each passage keeping its 3 strongest partners by rarity weight. Jev
// reads both passages and decides whether they are about the same specific matter. Verdicts → <backup>/relate.jsonl.
type Relation = { a: number; b: number; via: string[]; related: number };
async function relate(args: Args) {
  const dir = latestBackup(args), log = join(dir, "relate.jsonl");
  const chunks = new Map(jsonl<{ $id: number; documentId: string }>(join(dir, "chunks.jsonl")).map((chunk) => [chunk.$id, chunk.documentId]));
  const entities = new Map(jsonl<EntityRow>(join(dir, "entities.jsonl")).map((entity) => [entity.$id, entity]));
  const holders = new Map<number, number[]>();
  for (const edge of standingLinks(dir)) if (entities.has(edge.$to) && chunks.has(edge.$from)) holders.set(edge.$to, [...(holders.get(edge.$to) ?? []), edge.$from]);
  const shared = new Map<string, { weight: number; via: number[] }>();
  for (const [entity, held] of holders) {
    if (held.length < 2 || held.length > HUB) continue;
    const weight = 1 / Math.log2(1 + held.length);
    for (let i = 0; i < held.length; i++) for (let j = i + 1; j < held.length; j++) {
      if (chunks.get(held[i]) === chunks.get(held[j])) continue;
      const key = held[i] < held[j] ? `${held[i]}>${held[j]}` : `${held[j]}>${held[i]}`, entry = shared.get(key) ?? { weight: 0, via: [] };
      entry.weight += weight; entry.via.push(entity); shared.set(key, entry);
    }
  }
  const partners = new Map<number, [string, number][]>();
  for (const [key, entry] of shared) { if (entry.via.length < 2) continue; for (const id of key.split(">").map(Number)) partners.set(id, [...(partners.get(id) ?? []), [key, entry.weight]]); }
  const candidates = new Set<string>();
  for (const list of partners.values()) for (const [key] of list.sort((x, y) => y[1] - x[1]).slice(0, 3)) candidates.add(key);
  const done = new Set(jsonl<Relation>(log).map((row) => `${row.a}>${row.b}`));
  let pending = [...candidates].filter((key) => !done.has(key));
  if (args.values("--limit").length) pending = pending.slice(0, args.number("--limit", pending.length));
  console.log(`${dir}\n${shared.size} cross-document passage pairs share a walkable entity · ${[...shared.values()].filter((entry) => entry.via.length >= 2).length} share two or more · ${candidates.size} candidates (3 strongest per passage) · ${pending.length} to judge → ~${Math.ceil(pending.length / 3)} Jev requests`);
  const pages: string[][] = [];
  for (let start = 0; start < pending.length; start += 3) pages.push(pending.slice(start, start + 3));
  let judged = 0;
  await pool(pages, JEV_IN_FLIGHT, async (page) => {
    const ids = [...new Set(page.flatMap((key) => key.split(">").map(Number)))];
    const data = await read<{ chunks: any[] | null }>(readBatch().varAs("chunks", g().n(ids).project([PropertyProjection.renamed("$id", "id"), ...fields("title", "content")])).returning(["chunks"]), "passages_to_relate");
    const textOf = new Map((data.chunks ?? []).map((chunk) => [Number(chunk.id), `${chunk.title ?? ""}\n${chunk.content ?? ""}`]));
    const questions: Record<string, Question> = {};
    page.forEach((_, index) => { questions[`q${index}`] = noul(`In q${index}, passages a and b are about the same specific matter — the same deal, person's situation, product decision, technique or idea — so that someone reading one should be pointed to the other. Sharing a broad theme is not enough`); });
    const answers = await jev({ pairs: Object.fromEntries(page.map((key, index) => { const [a, b] = key.split(">").map(Number); return [`q${index}`, { a: textOf.get(a) ?? "", b: textOf.get(b) ?? "" }]; })) }, questions);
    appendFileSync(log, page.map((key, index) => { const [a, b] = key.split(">").map(Number); return JSON.stringify({ a, b, via: shared.get(key)!.via.map((id) => entities.get(id)!.name), related: answers[`q${index}`]?.noul ?? 0 }); }).join("\n") + "\n");
    judged += page.length;
    if (judged % 300 < 3) console.log(`[${new Date().toTimeString().slice(0, 8)}] ${judged}/${pending.length} pairs · ${usage.jevCalls} jev`);
  });
  if (usage.jevCalls) console.log(`jev: ${usage.jevCalls} calls, ${usage.jevTokens} tokens ≈ $${(usage.jevTokens * 0.042 / 1e6).toFixed(3)}`);
  const verdicts = jsonl<Relation>(log), related = verdicts.filter((row) => row.related >= MENTION_FLOOR && chunks.has(row.a) && chunks.has(row.b));
  console.log(`${verdicts.length} pairs judged · ${related.length} related (≥${MENTION_FLOOR}) · touching ${new Set(related.flatMap((row) => [row.a, row.b])).size} passages in ${new Set(related.flatMap((row) => [chunks.get(row.a), chunks.get(row.b)])).size} documents`);
  if (!args.has("--yes")) return console.log("dry run — add --yes to write RELATES_TO edges");
  for (let start = 0; start < related.length; start += 400) {
    let batch: any = writeBatch();
    related.slice(start, start + 400).forEach((row, index) => { batch = batch.varAs(`r${index}`, g().n(row.a).addE("RELATES_TO", row.b, { tenant_id: TENANT, confidence: row.related, via: row.via, by: "jev" })); });
    await write(batch.returning([]), "relate_passages");
  }
  console.log(`${related.length} RELATES_TO edges written`);
}

// kb confirm [--from dir] [--limit N] [--yes] — a link has to pass twice. Jev's score for a borderline mention moves
// between runs (measured 2026-09-20: of 150 links at >=0.9, 85 stayed >=0.9 when asked again on their own), and a
// request that asks about 30 candidates at once is not a focused reading. Here every standing link is asked again in a
// focused request — the passage and only its own links — under the current definitions. Below the floor the second
// time, it goes. Verdicts land in <backup>/confirm.jsonl first.
async function confirm(args: Args) {
  const dir = latestBackup(args), log = join(dir, "confirm.jsonl");
  const entities = new Map(jsonl<EntityRow>(join(dir, "entities.jsonl")).map((entity) => [entity.$id, entity]));
  const standing = standingLinks(dir).filter((edge) => entities.has(edge.$to));
  const done = new Map(jsonl<{ edge: number; again: number; about?: number }>(log).filter((row) => row.about !== undefined).map((row) => [row.edge, row.again]));
  const byChunk = new Map<number, EdgeRow[]>();
  for (const edge of standing) if (!done.has(edge.$id)) byChunk.set(edge.$from, [...(byChunk.get(edge.$from) ?? []), edge]);
  let chunkIds = [...byChunk.keys()];
  if (args.values("--limit").length) chunkIds = chunkIds.slice(0, args.number("--limit", chunkIds.length));
  console.log(`${dir}\n${standing.length} standing links · ${done.size} already confirmed or refuted · ${chunkIds.length} passages to read → ~${Math.ceil(chunkIds.length / PACK)} Jev requests`);
  const pages: number[][] = [];
  for (let start = 0; start < chunkIds.length; start += 60) pages.push(chunkIds.slice(start, start + 60));
  let read_ = 0;
  await pool(pages, 8, async (page) => {
    const data = await read<{ chunks: any[] | null }>(readBatch().varAs("chunks", g().n(page).project([PropertyProjection.renamed("$id", "id"), ...fields("chunkId", "title", "content")])).returning(["chunks"]), "passages_to_confirm");
    const proposals = (data.chunks ?? []).filter((chunk) => chunk.content?.trim()).map((chunk) => ({ chunk, edges: byChunk.get(Number(chunk.id)) ?? [] }));
    const verdicts = await vetNames(proposals.map(({ chunk, edges }) => ({ chunkId: chunk.chunkId, title: chunk.title ?? "", content: chunk.content, names: edges.map((edge) => entities.get(edge.$to)!.name) })));
    const rows = proposals.flatMap(({ chunk, edges }) => edges.map((edge) => ({ edge: edge.$id, entity: edge.$to, name: entities.get(edge.$to)!.name, first: edge.confidence, again: verdicts.get(`${chunk.chunkId}\u0000${entities.get(edge.$to)!.name}`)?.discusses ?? 0, about: verdicts.get(`${chunk.chunkId}\u0000${entities.get(edge.$to)!.name}`)?.about ?? 0 })));
    if (rows.length) appendFileSync(log, rows.map((row) => JSON.stringify(row)).join("\n") + "\n");
    read_ += proposals.length;
    console.log(`[${new Date().toTimeString().slice(0, 8)}] ${read_}/${chunkIds.length} passages · ${usage.jevCalls} jev`);
  });
  if (usage.jevCalls) console.log(`jev: ${usage.jevCalls} calls, ${usage.jevTokens} tokens ≈ $${(usage.jevTokens * 0.042 / 1e6).toFixed(3)}`);
  const readings = jsonl<{ edge: number; again: number; about?: number }>(log).filter((row) => row.about !== undefined);
  const verdict = new Map(readings.map((row) => [row.edge, row.again])), aboutOf = new Map(readings.map((row) => [row.edge, row.about!]));
  // two independent readings, averaged: a 0.95 then 0.88 is one solid link seen through noise; a 0.93 then 0.46 is not
  const mean = (edge: EdgeRow) => (edge.confidence + verdict.get(edge.$id)!) / 2;
  const judged = standing.filter((edge) => verdict.has(edge.$id)), refuted = judged.filter((edge) => mean(edge) < MENTION_FLOOR || aboutOf.get(edge.$id)! < ABOUT_FLOOR);
  const byKind: Record<string, [number, number]> = {};
  for (const edge of judged) { const kind = entities.get(edge.$to)!.kind; byKind[kind] ??= [0, 0]; byKind[kind][0]++; if (mean(edge) < MENTION_FLOOR || aboutOf.get(edge.$id)! < ABOUT_FLOOR) byKind[kind][1]++; }
  console.log(`${judged.length} links judged a second time · ${judged.length - refuted.length} confirmed · ${refuted.length} refuted (${Math.round(100 * refuted.length / Math.max(1, judged.length))}%) — ${judged.filter((edge) => mean(edge) < MENTION_FLOOR).length} not discussed on average, ${judged.filter((edge) => aboutOf.get(edge.$id)! < ABOUT_FLOOR).length} only mentioned in passing`);
  for (const [kind, [total, gone]] of Object.entries(byKind)) console.log(`   ${kind.padEnd(13)} ${total} → ${total - gone}`);
  if (!args.has("--yes")) return console.log("dry run — add --yes to drop the refuted links");
  await dropEdges(refuted.map((edge) => edge.$id), "confirm_refuted_links");
}

// kb consolidate [--from dir] [--yes] — a fact about a whole document belongs on the document. "Alex Hormozi" on every
// passage of an Alex Hormozi talk tells a passage nothing: when an entity is linked from at least half the passages of a
// document (4+ passages), it becomes ONE document→entity link and the per-passage copies go. No model calls.
// Measured 2026-09-20: such repeats were 20% of all links.
async function consolidate(args: Args) {
  const dir = latestBackup(args);
  const chunks = new Map(jsonl<{ $id: number; documentId: string }>(join(dir, "chunks.jsonl")).map((chunk) => [chunk.$id, chunk.documentId]));
  const entities = new Map(jsonl<EntityRow>(join(dir, "entities.jsonl")).map((entity) => [entity.$id, entity]));
  const size = new Map<string, number>();
  for (const documentId of chunks.values()) size.set(documentId, (size.get(documentId) ?? 0) + 1);
  const groups = new Map<string, EdgeRow[]>();
  for (const edge of standingLinks(dir)) { const documentId = chunks.get(edge.$from); if (!documentId || !entities.has(edge.$to)) continue; const key = `${documentId}\u0000${edge.$to}`; groups.set(key, [...(groups.get(key) ?? []), edge]); }
  const wide = [...groups].filter(([key, edges]) => { const n = size.get(key.split("\u0000")[0])!; return n >= 4 && edges.length / n >= 0.5; });
  const copies = wide.flatMap(([, edges]) => edges.map((edge) => edge.$id));
  console.log(`${dir}\n${wide.length} document-wide (document, entity) pairs · ${copies.length} per-passage copies to drop`);
  for (const [key, edges] of wide.slice(0, 8)) console.log(`   ${entities.get(edges[0].$to)!.name}  ×${edges.length}/${size.get(key.split("\u0000")[0])}  ${key.split("\u0000")[0]}`);
  if (!args.has("--yes")) return console.log("dry run — add --yes");
  for (let start = 0; start < wide.length; start += 150) {
    let batch: any = writeBatch();
    wide.slice(start, start + 150).forEach(([key, edges], index) => {
      const documentId = key.split("\u0000")[0], entity = entities.get(edges[0].$to)!;
      batch = batch.varAs(`d${index}`, documentById(documentId).limit(1))
        .varAs(`h${index}`, g().n(NodeRef.var(`d${index}`)).outE("MENTIONS").where(Predicate.eq("entityKey", entity.entityKey)))
        .varAsIf(`a${index}`, BatchCondition.varEmpty(`h${index}`), g().n(NodeRef.var(`d${index}`)).addE("MENTIONS", entity.$id, { tenant_id: TENANT, entityKey: entity.entityKey, confidence: Math.max(...edges.map((edge) => edge.confidence)), by: "consolidated", passages: edges.length }));
    });
    await write(batch.returning([]), "consolidate_documents");
  }
  await dropEdges(copies, "consolidate_passage_copies");
}

// kb vet [--from dir] [--limit N] [--yes] — Jev re-reads two kinds of doubt, in the passage they came from:
//   every link an LLM wrote unchecked (is it really discussed?), and every entity that at most two passages hold up
//   (does it deserve a node at all?). Verdicts land in <backup>/vet.jsonl first; --yes applies them.
type Verdict = { edge: number; chunk: number; entity: number; name: string; discusses: number; worthy: number };
async function vet(args: Args) {
  const dir = latestBackup(args), log = join(dir, "vet.jsonl");
  const entities = new Map(jsonl<EntityRow>(join(dir, "entities.jsonl")).map((entity) => [entity.$id, entity]));
  const standing = standingLinks(dir).filter((edge) => entities.has(edge.$to));
  const degree = new Map<number, number>();
  for (const edge of standing) degree.set(edge.$to, (degree.get(edge.$to) ?? 0) + 1);
  const rostered = new Set(jsonl<EdgeRow>(join(dir, "document_mentions.jsonl")).map((edge) => edge.$to));
  // the work list: unchecked LLM links, plus the single best link of every thinly-held entity not already covered
  const doubt = new Map<number, EdgeRow>();
  for (const edge of standing) if (edge.by === "llm") doubt.set(edge.$id, edge);
  const covered = new Set([...doubt.values()].map((edge) => edge.$to)), best = new Map<number, EdgeRow>();
  // every topic is in doubt, however well held: topics are where ad-hoc phrases pile up ("choosing what business to start")
  for (const edge of standing) if (((degree.get(edge.$to) ?? 0) <= 2 || entities.get(edge.$to)!.kind === "topic") && !covered.has(edge.$to) && (best.get(edge.$to)?.confidence ?? -1) < edge.confidence) best.set(edge.$to, edge);
  for (const edge of best.values()) doubt.set(edge.$id, edge);
  const done = new Set(jsonl<Verdict>(log).map((verdict) => verdict.edge));
  const byChunk = new Map<number, EdgeRow[]>();
  for (const edge of doubt.values()) if (!done.has(edge.$id)) byChunk.set(edge.$from, [...(byChunk.get(edge.$from) ?? []), edge]);
  let chunkIds = [...byChunk.keys()];
  if (args.values("--limit").length) chunkIds = chunkIds.slice(0, args.number("--limit", chunkIds.length));
  console.log(`${dir}\n${doubt.size} links in doubt (${[...doubt.values()].filter((edge) => edge.by === "llm").length} unchecked LLM links, ${best.size} topics and thinly-held entities) · ${done.size} already judged · ${chunkIds.length} passages to read → ~${Math.ceil(chunkIds.length / 14)} Jev requests`);
  let judged = 0;
  const pages: number[][] = [];
  for (let start = 0; start < chunkIds.length; start += 56) pages.push(chunkIds.slice(start, start + 56));
  await pool(pages, 6, async (page) => {
    const data = await read<{ chunks: any[] | null }>(readBatch().varAs("chunks", g().n(page).project([PropertyProjection.renamed("$id", "id"), ...fields("chunkId", "title", "content")])).returning(["chunks"]), "passages_to_vet");
    const proposals = (data.chunks ?? []).filter((chunk) => chunk.content?.trim()).map((chunk) => ({ chunk, edges: byChunk.get(Number(chunk.id)) ?? [] }));
    const verdicts = await vetNames(proposals.map(({ chunk, edges }) => ({ chunkId: chunk.chunkId, title: chunk.title ?? "", content: chunk.content, names: edges.map((edge) => entities.get(edge.$to)!.name) })));
    const rows: Verdict[] = proposals.flatMap(({ chunk, edges }) => edges.map((edge) => {
      const name = entities.get(edge.$to)!.name, verdict = verdicts.get(`${chunk.chunkId}\u0000${name}`) ?? { discusses: 0, worthy: 0 };
      return { edge: edge.$id, chunk: edge.$from, entity: edge.$to, name, ...verdict };
    }));
    if (rows.length) appendFileSync(log, rows.map((row) => JSON.stringify(row)).join("\n") + "\n");
    judged += proposals.length;
    console.log(`[${new Date().toTimeString().slice(0, 8)}] ${judged}/${chunkIds.length} passages · ${usage.jevCalls} jev (${Math.round(usage.jevTokens / Math.max(1, usage.jevCalls))} tok/call)`);
  });
  console.log(`jev: ${usage.jevCalls} calls, ${usage.jevTokens} tokens ≈ $${(usage.jevTokens * 0.042 / 1e6).toFixed(3)}`);

  // verdicts → actions
  const verdicts = jsonl<Verdict>(log);
  const worth = new Map<number, number>();
  for (const verdict of verdicts) worth.set(verdict.entity, Math.max(worth.get(verdict.entity) ?? 0, verdict.worthy));
  // Worthiness decides TOPICS only. A person, organisation, product, project or place is a named thing by kind; a topic
  // has to earn its node: measured 2026-09-20 on 11.6k topics, >=0.8 reads as real named concepts ("NDAs", "payback
  // period", "jobs to be done"), under 0.4 as phrases ("giving feedback to teams"), and 0.4-0.8 is mixed.
  const unworthy = new Set([...worth].filter(([entity, value]) => entities.get(entity)?.kind === "topic" && value < TOPIC_FLOOR).map(([entity]) => entity));
  const llmEdges = verdicts.filter((verdict) => doubt.get(verdict.edge)?.by === "llm" && !unworthy.has(verdict.entity));
  const failed = llmEdges.filter((verdict) => verdict.discusses < MENTION_FLOOR), passed = llmEdges.filter((verdict) => verdict.discusses >= MENTION_FLOOR);
  // an entity with no link left standing and no document roster behind it is an orphan
  const gone = new Set(failed.map((verdict) => verdict.edge)), left = new Map<number, number>();
  for (const edge of standing) if (!gone.has(edge.$id) && !unworthy.has(edge.$to)) left.set(edge.$to, (left.get(edge.$to) ?? 0) + 1);
  const orphans = [...entities.keys()].filter((id) => !unworthy.has(id) && !left.has(id) && !rostered.has(id));
  console.log(`verdicts: ${verdicts.length} · unworthy entities ${unworthy.size} · LLM links confirmed ${passed.length}, refuted ${failed.length} · orphan entities ${orphans.length}`);
  for (const name of [...unworthy].slice(0, 12).map((id) => entities.get(id)?.name)) console.log(`   unworthy: ${name}`);
  if (!args.has("--yes")) return console.log("dry run — add --yes to apply");
  await dropEdges(failed.map((verdict) => verdict.edge), "vet_refuted_links");
  for (let start = 0; start < passed.length; start += 400) {
    let batch: any = writeBatch();
    passed.slice(start, start + 400).forEach((verdict, index) => { batch = batch.varAs(`e${index}`, g().e(verdict.edge).setProperty("confidence", verdict.discusses).setProperty("by", "llm+jev")); });
    await write(batch.returning([]), "vet_confirmed_links");
  }
  await dropNodes([...unworthy], "vet_unworthy_entities");   // orphans are `kb prune`'s job, from a fresh backup
}

// kb check — the invariants of the high-quality graph, read live (each is a scan of every link, ~20-90 s; they run one
// after another so a slow one does not hide the rest). Exit 1 when one is broken.
async function check() {
  const mentions = () => g().nWithLabel("Chunk").where(tenantScope).outE("MENTIONS");
  const one = async <T>(name: string, traversal: any) => (await read<{ value: T }>(readBatch().varAs("value", traversal).returning(["value"]), `check_${name}`)).value;
  let broken = false;
  const line = (label: string, value: number | string, ok = true) => { if (!ok) broken = true; console.log(`${ok ? " " : "✗"} ${label.padEnd(46)} ${value}`); };
  const chunks = Number(await one("chunks", g().nWithLabel("Chunk").where(tenantScope).count()));
  line("chunks", chunks);
  const judged = Number(await one("judged", g().nWithLabelWhere("Chunk", Predicate.eq("linkVersion", LINK_VERSION)).where(tenantScope).count()));
  line("  not judged by Jev", chunks - judged, chunks === judged);
  line("  flagged, waiting for naming", Number(await one("unnamed", g().nWithLabelWhere("Chunk", Predicate.eq("needsNaming", true)).where(tenantScope).count())));
  const links = Number(await one("links", mentions().count()));
  line("links chunk→entity", links); line("  per chunk", (links / chunks).toFixed(1));
  const weak = Number(await one("weak", mentions().where(Predicate.lt("confidence", MENTION_FLOOR)).count()));
  line(`  under the ${MENTION_FLOOR} floor`, weak, weak === 0);
  const perEntity = (await one<{ count: number; entityKey: string }[]>("per_entity", mentions().groupCount("entityKey"))) ?? [];
  const checkedLinks = perEntity.reduce((sum, row) => sum + row.count, 0);
  line("  without an entityKey (written by something else)", links - checkedLinks, links === checkedLinks);
  const unchecked = Number(await one("unchecked", mentions().where(Predicate.eq("by", "llm")).count()));
  line("  written by an LLM, never checked by Jev", unchecked, unchecked === 0);
  const entities = Number(await one("entities", g().nWithLabel("Entity").where(tenantScope).count()));
  const rostered = (await one<{ entityKey: string }[]>("rostered", g().nWithLabel("SourceDocument").where(tenantScope).outE("MENTIONS").groupCount("entityKey"))) ?? [];
  const held = new Set([...perEntity, ...rostered].map((row) => row.entityKey)), hubs = perEntity.filter((row) => row.count > HUB);
  line("entities", entities);
  line("  held by no link and no document", Math.max(0, entities - held.size), entities - held.size <= 0);
  line("  held by one passage", perEntity.filter((row) => row.count === 1).length);
  line(`  hubs (over ${HUB} passages, not walked)`, `${hubs.length} holding ${hubs.reduce((sum, row) => sum + row.count, 0)} links`);
  if (broken) process.exit(1);
}

// kb audit [--sample N] [--kind K | --doc D] — a second opinion. Jev wrote every link; a random sample of them, each
// with its passage, is written out for an independent reviewer. Agreement is the graph's measured precision.
async function audit(args: Args) {
  const size = args.number("--sample", 200);
  let entities: Map<number, EntityRow>, links: EdgeRow[];
  if (args.values("--kind").length || args.values("--doc").length || args.values("--source").length) {
    // a slice, read live (a slice is small; the whole graph is sampled from the latest backup instead)
    const docOf = await documentMap(args), documents = [...docOf.keys()];
    links = [];
    for (let start = 0; start < documents.length; start += 60) {
      let batch: any = readBatch();
      documents.slice(start, start + 60).forEach((documentId, index) => { batch = batch.varAs(`m${index}`, chunksOfDocument(documentId).outE("MENTIONS").valueMap(["$id", "$from", "$to", "confidence", "by", "entityKey"])); });
      const data = await read<Record<string, EdgeRow[] | null>>(batch.returning(documents.slice(start, start + 60).map((_, index) => `m${index}`)), "audit_live_links");
      for (const rows of Object.values(data)) links.push(...(rows ?? []));
    }
    const ids = [...new Set(links.map((edge) => edge.$to))], found: EntityRow[] = [];
    for (let start = 0; start < ids.length; start += 500) found.push(...((await read<{ rows: EntityRow[] | null }>(readBatch().varAs("rows", g().n(ids.slice(start, start + 500)).valueMap(["$id", "entityKey", "name", "kind"])).returning(["rows"]), "audit_live_entities")).rows ?? []));
    entities = new Map(found.map((entity) => [entity.$id, entity]));
    console.log(`live: ${documents.length} documents · ${links.length} links`);
  } else {
    const dir = latestBackup(args);
    entities = new Map(jsonl<EntityRow>(join(dir, "entities.jsonl")).map((entity) => [entity.$id, entity]));
    links = standingLinks(dir);
  }
  links = links.filter((edge) => entities.has(edge.$to));
  const sample = Array.from({ length: size }, () => links[Math.floor(Math.random() * links.length)]);
  const data = await read<{ chunks: any[] | null }>(readBatch().varAs("chunks", g().n([...new Set(sample.map((edge) => edge.$from))]).project([PropertyProjection.renamed("$id", "id"), ...fields("content")])).returning(["chunks"]), "audit_passages");
  const textOf = new Map((data.chunks ?? []).map((chunk) => [Number(chunk.id), chunk.content as string]));
  // The sample goes to a file for a reviewer that shares nothing with Jev — Claude in the session, or sol. (An LLM
  // auditor inside the tool was dropped on example's call, 2026-09-20.) One JSON line per link: the name, Jev's
  // confidence, and the passage with the name's surroundings first. The reviewer writes verdicts next to it.
  const file = join(BACKUPS, `audit-sample-${new Date().toISOString().slice(0, 16).replace(/[:T]/g, "-")}.jsonl`);
  writeFileSync(file, sample.map((edge, index) => {
    const entity = entities.get(edge.$to)!, text = (textOf.get(edge.$from) ?? "").replace(/\s+/g, " "), at = text.toLowerCase().indexOf(entity.name.toLowerCase());
    return JSON.stringify({ n: index, edge: edge.$id, chunk: edge.$from, name: entity.name, kind: entity.kind, confidence: edge.confidence, by: edge.by, verbatim: at >= 0, around: at >= 0 ? text.slice(Math.max(0, at - 300), at + entity.name.length + 300) : "", passage: text });   // the whole passage: a reviewer cannot judge what it cannot see
  }).join("\n") + "\n");
  console.log(`${sample.length} links sampled from ${links.length} → ${file}`);
}

// ---------------------------------------------------------------- entity resolution (Jev)

// "BCG" and "Boston Consulting Group" are two nodes for one thing, and every link to it is split between them. A bare
// name carries next to no context, so names are never compared on their own. The graph proposes: two entities of one
// kind that keep turning up in the same passages (one of them is there at least half the times the other is), or the
// same slug filed under two kinds. Jev decides, reading a passage that holds both. A confirmed pair is merged into the
// better-held name: its links are re-pointed, the other name is kept in `aliases`, the emptied node goes.
// kb entities --resolve [--from dir] [--yes]   (works from the latest backup; verdicts land in <backup>/resolve.jsonl)
type Pairing = { a: number; b: number; chunk: number; chunkB?: number; same: number };
async function resolveEntities(args: Args) {
  const dir = latestBackup(args), log = join(dir, "resolve.jsonl");
  const entities = new Map(jsonl<EntityRow>(join(dir, "entities.jsonl")).map((entity) => [entity.$id, entity]));
  const links = standingLinks(dir).filter((edge) => entities.has(edge.$to));
  const holders = new Map<number, Set<number>>(), byChunk = new Map<number, number[]>();
  for (const edge of links) { if (!holders.has(edge.$to)) holders.set(edge.$to, new Set()); holders.get(edge.$to)!.add(edge.$from); byChunk.set(edge.$from, [...(byChunk.get(edge.$from) ?? []), edge.$to]); }
  const together = new Map<string, { count: number; chunk: number }>();
  for (const [chunk, ids] of byChunk) for (let i = 0; i < ids.length; i++) for (let j = i + 1; j < ids.length; j++) {
    const [a, b] = ids[i] < ids[j] ? [ids[i], ids[j]] : [ids[j], ids[i]];
    if (entities.get(a)!.kind !== entities.get(b)!.kind && entities.get(a)!.entityKey.split(":")[2] !== entities.get(b)!.entityKey.split(":")[2]) continue;
    const key = `${a}>${b}`, entry = together.get(key) ?? { count: 0, chunk };
    entry.count++; together.set(key, entry);
  }
  const candidates = [...together].map(([key, entry]) => { const [a, b] = key.split(">").map(Number); return { a, b, chunkB: entry.chunk, ...entry }; })
    .filter(({ a, b, count }) => entities.get(a)!.entityKey.split(":")[2] === entities.get(b)!.entityKey.split(":")[2] || (count >= 2 && count / Math.min(holders.get(a)!.size, holders.get(b)!.size) >= 0.5));
  // Co-occurrence is blind to a pure alias: a writer picks ONE of "Workstream" / "Work Stream" per passage, so two
  // names for one thing are exactly the pair that never turns up together. Names that are equal once spacing and
  // punctuation are taken out are proposed as well, each side carrying a passage of its own. (44 such groups held
  // 1,039 links — backup/experiment-alias-and-title-entities.ts. An initials detector was tried in the same
  // experiment and thrown away: with short names it degenerates into "shares a first letter".)
  const squashed = new Map<string, number[]>();
  for (const id of holders.keys()) squashed.set(`${entities.get(id)!.kind}|${normalise(entities.get(id)!.name).replace(/-/g, "")}`, [...(squashed.get(`${entities.get(id)!.kind}|${normalise(entities.get(id)!.name).replace(/-/g, "")}`) ?? []), id]);
  const seen = new Set(candidates.map(({ a, b }) => `${a}>${b}`));
  for (const ids of squashed.values()) for (let i = 1; i < ids.length; i++) {
    const [a, b] = ids[0] < ids[i] ? [ids[0], ids[i]] : [ids[i], ids[0]];
    if (seen.has(`${a}>${b}`)) continue;
    seen.add(`${a}>${b}`);
    candidates.push({ a, b, count: 0, chunk: [...holders.get(a)!][0], chunkB: [...holders.get(b)!][0] });
  }
  const done = new Set(jsonl<Pairing>(log).map((row) => `${row.a}>${row.b}`));
  let pending = candidates.filter(({ a, b }) => !done.has(`${a}>${b}`));
  if (args.values("--limit").length) pending = pending.slice(0, args.number("--limit", pending.length));
  console.log(`${dir}\n${candidates.length} candidate pairs (${candidates.filter((pair) => pair.count > 0).length} share passages, ${candidates.filter((pair) => pair.count === 0).length} spelling variants) · ${done.size} already judged · ${pending.length} to judge → ${pending.length} Jev requests (one pair each)`);
  const pages: typeof pending[] = [];
  for (let start = 0; start < pending.length; start += 1) pages.push(pending.slice(start, start + 1));   // one pair per request — packed items leak into each other
  let judged = 0;
  await pool(pages, JEV_IN_FLIGHT, async (page) => {
    const data = await read<{ chunks: any[] | null }>(readBatch().varAs("chunks", g().n([...new Set(page.flatMap((pair) => [pair.chunk, pair.chunkB ?? pair.chunk]))]).project([PropertyProjection.renamed("$id", "id"), ...fields("content")])).returning(["chunks"]), "passages_for_pairs");
    const textOf = new Map((data.chunks ?? []).map((chunk) => [Number(chunk.id), chunk.content as string]));
    const questions: Record<string, Question> = {};
    page.forEach((_, index) => { questions[`q${index}`] = noul(`In q${index}, a and b are two names for one and the same thing (not merely related, not part and whole, not competitors)`); });
    const answers = await jev({ pairs: Object.fromEntries(page.map((pair, index) => [`q${index}`, { a: `${entities.get(pair.a)!.name} (${entities.get(pair.a)!.kind})`, b: `${entities.get(pair.b)!.name} (${entities.get(pair.b)!.kind})`, passage_using_a: textOf.get(pair.chunk) ?? "", passage_using_b: textOf.get(pair.chunkB ?? pair.chunk) ?? "" }])) }, questions);
    appendFileSync(log, page.map((pair, index) => JSON.stringify({ a: pair.a, b: pair.b, chunk: pair.chunk, chunkB: pair.chunkB, same: answers[`q${index}`]?.noul ?? 0 })).join("\n") + "\n");
    judged += page.length;
    if (judged % 200 === 0) console.log(`[${new Date().toTimeString().slice(0, 8)}] ${judged}/${pending.length} pairs · ${usage.jevCalls} jev`);
  });
  // confirmed pairs → groups (a=b, b=c ⇒ one group), each merged into its best-held member
  // Identity is the name, not the name plus a kind: "PyHarness" the project and "PyHarness" the product are one node filed
  // twice (the key scheme carried the kind). Shown the two kinds, Jev calls them different things (0 of 425 same-name
  // pairs reached 0.9, measured 2026-09-20), so same normalised name merges without asking; Jev decides the rest.
  const slugOf = (id: number) => entities.get(id)!.entityKey.split(":")[2];
  const bySlug = new Map<string, number[]>();
  for (const id of entities.keys()) bySlug.set(slugOf(id), [...(bySlug.get(slugOf(id)) ?? []), id]);
  const sameName: Pairing[] = [...bySlug.values()].filter((ids) => ids.length > 1).flatMap((ids) => ids.slice(1).map((id) => ({ a: ids[0], b: id, chunk: 0, same: 1 })));
  const squashOf = (id: number) => normalise(entities.get(id)!.name).replace(/-/g, "");
  const isVariant = (row: Pairing) => squashOf(row.a) === squashOf(row.b);
  const verdicts = jsonl<Pairing>(log).filter((row) => entities.has(row.a) && entities.has(row.b) && slugOf(row.a) !== slugOf(row.b));
  const confirmed = [...sameName, ...verdicts.filter((row) => row.same >= (isVariant(row) ? ALIAS_FLOOR : 0.9))];
  console.log(`${sameName.length} same-name pairs (merged without asking) · ${confirmed.filter((row) => !isVariant(row)).length - sameName.length} confirmed by Jev at 0.9 · ${confirmed.filter(isVariant).length} spelling variants at ${ALIAS_FLOOR}`);
  const parent = new Map<number, number>(), find = (id: number): number => { while (parent.get(id) !== undefined && parent.get(id) !== id) id = parent.get(id)!; return id; };
  for (const { a, b } of confirmed) parent.set(find(a), find(b));
  const groups = new Map<number, number[]>();
  for (const id of new Set(confirmed.flatMap((row) => [row.a, row.b]))) groups.set(find(id), [...(groups.get(find(id)) ?? []), id]);
  const merges = [...groups.values()].map((ids) => { const sorted = [...ids].sort((x, y) => (holders.get(y)?.size ?? 0) - (holders.get(x)?.size ?? 0) || entities.get(y)!.name.length - entities.get(x)!.name.length); return { keep: sorted[0], fold: sorted.slice(1) }; });
  for (const merge of merges.slice(0, 25)) console.log(`   ${entities.get(merge.keep)!.name}  ⇐  ${merge.fold.map((id) => entities.get(id)!.name).join(" · ")}`);
  console.log(`${confirmed.length} pairs confirmed (≥0.9) → ${merges.length} merges folding ${merges.reduce((sum, merge) => sum + merge.fold.length, 0)} entities away`);
  if (!args.has("--yes")) return console.log("dry run — add --yes to merge");
  const documentLinks = jsonl<EdgeRow & { evidence?: string }>(join(dir, "document_mentions.jsonl"));
  for (const merge of merges) {
    const keep = entities.get(merge.keep)!, folded = new Set(merge.fold), has = new Set(links.filter((edge) => edge.$to === merge.keep).map((edge) => edge.$from)), rostered = new Set(documentLinks.filter((edge) => edge.$to === merge.keep).map((edge) => edge.$from));
    let batch: any = writeBatch().varAs("keep", g().n(merge.keep).setProperty("aliases", merge.fold.map((id) => entities.get(id)!.name)));
    let edge = 0;
    for (const link of links) if (folded.has(link.$to) && !has.has(link.$from)) { has.add(link.$from); batch = batch.varAs(`l${edge++}`, g().n(link.$from).addE("MENTIONS", merge.keep, { tenant_id: TENANT, confidence: link.confidence, entityKey: keep.entityKey, by: link.by ?? "jev" })); }
    for (const link of documentLinks) if (folded.has(link.$to) && !rostered.has(link.$from)) { rostered.add(link.$from); batch = batch.varAs(`l${edge++}`, g().n(link.$from).addE("MENTIONS", merge.keep, { tenant_id: TENANT, confidence: link.confidence ?? 1, entityKey: keep.entityKey, evidence: link.evidence ?? "" })); }
    await write(batch.varAs("gone", g().n(merge.fold).drop()).returning([]), "merge_entities");
  }
  console.log(`merged. Take a new backup before running hygiene commands again (this one is now out of date).`);
}

async function raw(path: string | undefined) {
  const body = path && path !== "-" ? readFileSync(path, "utf8") : await Bun.stdin.text();
  const response = await fetch(`${HELIX}/v2/query`, { method: "POST", headers: { "Content-Type": "application/json" }, body });
  console.log(await response.text());
  if (!response.ok) process.exit(1);
}

// ---------------------------------------------------------------- cli

const USAGE = `kb — HelixDB knowledgebase

  kb search "<query>"  [filters] [--mode hybrid|vector|text] [--k N] [--top N] [--chunks] [--expand N] [--full]
                       [--no-rerank] [--no-graph] [--json]          Jev re-ranks the candidates (~2s); --no-rerank skips it
  kb docs              [--kind K] [--source S] [--status S] [--where ...] [--limit N]
  kb entities          [filters] [--entity-kind K] [--match S] [--top N]   what a slice of the corpus is about
  kb categories        [filters]                                           how a slice is filed
  kb show <documentId>        kb stats        kb profile        kb raw [file|-]

  kb taxonomy --seed [--yes]                  LLM drafts the category taxonomy, once            (1 LLM call)
  kb link [--kind K ...] [--limit N] [--dry-run] [--redo]    stage 1: Helix proposes entities, Jev decides; links under 0.8 are not written
  kb name [--kind K ...] [--limit N] [--dry-run]             stage 2: LLM names what Jev flagged as missing, Jev checks every name
  kb ingest <dir> [--kind K] [--replace]      document + chunks + links (both stages)

  graph hygiene — always from a backup, dry run unless --yes:
  kb backup [dir]                             entities, links (with endpoints + confidence), judged chunk properties → JSONL
  kb check                                    live invariants: nothing unjudged, under the floor, unchecked or orphaned   (exit 1 if broken)
  kb audit [--sample 200] [--kind K]          random sample of links with their passages → file, for an independent reviewer
  kb prune [--yes]                            drop links that do not stand: under the floor, stale, duplicate pairs
  kb model [--yes]                            the rest of Helix's model: User/OWNS, UserProfile, category parents, EXTENDS/UPDATES, entity associations
  kb dedupe [--yes]                           documents that are ≥80% identical-passage copies of another → removed        no model calls
  kb relate [--yes]                           passage↔passage RELATES_TO: the graph proposes cross-document pairs, Jev decides
  kb confirm [--yes]                          every link is judged a second time in a focused request; refuted links go
  kb consolidate [--yes]                      entity on ≥50% of a document's passages → one document link, passage copies dropped
  kb vet [--yes]                              Jev re-reads unchecked LLM links and thinly-held entities in their passages
  kb entities --resolve [--yes]               the graph proposes duplicate entities (co-occurrence), Jev decides, confirmed pairs merge
  bun eval.ts "<note>"                        20 queries: does the graph surface useful passages plain search misses? → eval.history.jsonl

  filters (indexed chunk properties; repeat a flag for OR, different flags AND):
    --kind K   --source S   --doc <documentId>   --category "name"   --mine (about example)   --min-salience 0.6
    --where 'field=v' | field!=v | field~contains | field^prefix      --with-boilerplate (hidden by default)
    --entity "name" (graph: chunks about it; repeatable = ANY)   --all-entities (chunks about EVERY one)`;

const [command, ...rest] = process.argv.slice(2);
const args = parseArgs(rest);
try {
  switch (command) {
    case "search": await search(args); break;
    case "docs": await docs(args); break;
    case "entities": await (args.has("--resolve") ? resolveEntities(args) : tally(args, "entities")); break;
    case "categories": await tally(args, "categories"); break;
    case "show": await show(args.positional[0]); break;
    case "stats": await stats(); break;
    case "taxonomy": await (args.has("--seed") ? seedTaxonomy(args) : taxonomy().then((rows) => rows.forEach((row) => console.log(`${row.name.padEnd(26)} ${row.description}`)))); break;
    case "link": await linkCommand(args); break;
    case "name": await nameCommand(args); break;
    case "ingest": await ingest(args); break;
    case "backup": await backup(args); break;
    case "prune": await prune(args); break;
    case "vet": await vet(args); break;
    case "confirm": await confirm(args); break;
    case "relate": await relate(args); break;
    case "dedupe": await dedupe(args); break;
    case "model": await model(args); break;
    case "hashes": {   // one-off: give every existing passage its contentHash so ingest can recognise copies
      await bootstrap();
      const rows = (await read<{ rows: { id: number; contentHash?: string }[] | null }>(readBatch().varAs("rows", g().nWithLabel("Chunk").where(tenantScope).project([PropertyProjection.renamed("$id", "id"), ...fields("contentHash")])).returning(["rows"]), "hash_state")).rows ?? [];
      const missing = rows.filter((row) => !row.contentHash).map((row) => Number(row.id));
      console.log(`${rows.length} passages · ${missing.length} without a contentHash`);
      for (let start = 0; start < missing.length; start += 300) {
        const texts = (await read<{ rows: { id: number; content: string }[] | null }>(readBatch().varAs("rows", g().n(missing.slice(start, start + 300)).project([PropertyProjection.renamed("$id", "id"), ...fields("content")])).returning(["rows"]), "hash_texts")).rows ?? [];
        let batch: any = writeBatch();
        texts.forEach((row, index) => { batch = batch.varAs(`h${index}`, g().n(Number(row.id)).setProperty("contentHash", passageHash(row.content ?? ""))); });
        await write(batch.returning([]), "hash_backfill");
        console.log(`  ${Math.min(missing.length, start + 300)}/${missing.length}`);
      }
      break;
    }
    case "profile": { const data = await read<{ rows: any[] | null }>(readBatch().varAs("rows", g().nWithLabel("UserProfile").where(tenantScope).project(fields("static", "dynamic", "updatedAt"))).returning(["rows"]), "profile"); const profile = data.rows?.[0]; if (!profile) throw new Error("no profile — run `kb model --yes`"); console.log(`STATIC\n${profile.static.map((line: string) => `  - ${line}`).join("\n")}\n\nCURRENT\n${profile.dynamic.map((line: string) => `  - ${line}`).join("\n")}`); break; }
    case "consolidate": await consolidate(args); break;
    case "check": await check(); break;
    case "audit": await audit(args); break;
    case "raw": await raw(args.positional[0]); break;
    default: console.log(USAGE); process.exit(command ? 1 : 0);
  }
} catch (error) {
  console.error(String(error instanceof Error ? error.message : error));
  process.exit(1);
}
