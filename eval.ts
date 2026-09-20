#!/usr/bin/env bun
// eval — does the entity graph earn its keep? For each query, Jev grades:
//   top      the 8 reranked hits of plain hybrid search
//   next     the 8 hits ranked after them (what you get by simply asking for more, no graph)
//   graph    the 8 passages `kb search` reports as "connected through shared entities"
// The graph earns its links when `graph` holds up against `next` AND surfaces useful passages plain search missed.
// 2026-09-19, before the prune (links kept from 0.5): graph 0.51 vs next 0.67, traversals up to 192 s.
// Results are appended to eval.history.jsonl so runs can be compared.
import { Client, g, readBatch, Predicate, SourcePredicate } from "@helix-db/helix-db";
import { appendFileSync, readFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";

const QUERIES = [
  "how should I price a consulting proposal", "value based pricing versus hourly billing", "how to ask for the sale without sounding pushy",
  "handling the objection that it is too expensive", "how to negotiate when the other side has more power", "writing a cold outreach message that gets replies",
  "building a lead magnet", "how to structure a retainer", "fractional CTO positioning", "getting referrals from existing clients",
  "how to beatmatch without sync", "transition from house into drum and bass", "how to use loops in a transition", "echo out effect", "what tracks did Knock2 play",
  "preparing tracks and cue points in rekordbox", "retrieval augmented generation evaluation", "structured outputs from language models", "how to run a discovery call", "productized service offer",
];

const client = Client.server("http://localhost:6972");
const env = readFileSync(join(homedir(), "work/example/apps/web/.env"), "utf8");
const secret = (name: string) => env.match(new RegExp(`^\\s*(?:export\\s+)?${name}\\s*=\\s*["']?([^"'\\n]+)`, "m"))![1].trim();

// One passage per request: several passages in one Jev request leak into each other (measured 2026-09-20).
async function grade(query: string, passages: string[]): Promise<number[]> {
  return Promise.all(passages.map(async (passage) => {
    if (!passage.trim()) return 0;
    for (let attempt = 0; ; attempt++) {
      const response = await fetch(`https://api.cloudflare.com/client/v4/accounts/${secret("CLOUDFLARE_ACCOUNT_ID")}/ai/run`, {
        method: "POST",
        headers: { "Content-Type": "application/json", Authorization: `Bearer ${secret("CLOUDFLARE_API_TOKEN")}`, "cf-aig-gateway-id": "example-dev", "cf-aig-authorization": `Bearer ${secret("CF_AIG_TOKEN")}`, "cf-aig-metadata": JSON.stringify({ workload: "example-kb-eval" }) },
        body: JSON.stringify({ model: "typesafe/jev", input: { state: { request: query, passage }, questions: { useful: { type: "noul", instructions: "The passage answers the request or contains information directly useful for it" } } } }),
      });
      if (response.ok) return (((await response.json()) as any).result.result.answers.useful?.noul ?? 0) as number;
      if (attempt === 8) throw new Error(`jev HTTP ${response.status}`);
      await Bun.sleep(2500 * (attempt + 1));
    }
  }));
}

async function kbSearch(query: string, flags: string[]): Promise<{ hits: any[]; connected: any[]; ms: number }> {
  const started = Date.now();
  const run = Bun.spawn(["bun", join(import.meta.dir, "kb.ts"), "search", query, "--json", "--chunks", ...flags], { stdout: "pipe", stderr: "ignore" });
  const out = JSON.parse(await new Response(run.stdout).text());
  return { hits: out.hits.map((entry: any) => entry.hit), connected: out.graph?.connected ?? [], ms: Date.now() - started };
}

const mean = (values: number[]) => values.length ? values.reduce((a, b) => a + b, 0) / values.length : 0;
const useful = (values: number[]) => values.filter((value) => value >= 0.5).length;
const totals: Record<string, { relevance: number[]; useful: number; graded: number; fresh: number; ms: number }> = {};
const add = (arm: string, grades: number[], fresh = 0, ms = 0) => { const t = totals[arm] ??= { relevance: [], useful: 0, graded: 0, fresh: 0, ms: 0 }; t.relevance.push(...grades); t.useful += useful(grades); t.graded += grades.length; t.fresh += fresh; t.ms += ms; };

for (const query of QUERIES) {
  const ranked = (await kbSearch(query, ["--top", "16", "--no-graph"])).hits;                       // reranked by Jev, 16 deep
  const pool = new Set((await kbSearch(query, ["--top", "48", "--k", "48", "--no-rerank", "--no-graph"])).hits.map((hit) => hit.chunkId));
  const plain = await kbSearch(query, ["--top", "8", "--no-graph"]), withGraph = await kbSearch(query, ["--top", "8"]);
  const ids = withGraph.connected.map((row) => row.chunkId as string);
  const texts: any = ids.length ? await client.query(readBatch().varAs("c", g().nWithLabelWhere("Chunk", Predicate.isIn("chunkId", ids)).valueMap(["chunkId", "content"])).returning(["c"]).toQueryRequest({ queryName: "eval_texts" })).send() : { c: [] };
  const contentOf = new Map((texts.c as any[]).map((row) => [row.chunkId, row.content as string]));
  const top = ranked.slice(0, 8), next = ranked.slice(8, 16);
  const [topGrades, nextGrades, graphGrades] = await Promise.all([grade(query, top.map((hit) => hit.content)), grade(query, next.map((hit) => hit.content)), grade(query, ids.map((id) => contentOf.get(id) ?? ""))]);
  const fresh = ids.filter((id, index) => graphGrades[index] >= 0.5 && !pool.has(id)).length;   // useful AND outside plain search's first 48
  add("top", topGrades); add("next", nextGrades); add("graph", graphGrades, fresh, withGraph.ms - plain.ms);
  console.log(`${query}\n   top ${mean(topGrades).toFixed(2)} · next ${mean(nextGrades).toFixed(2)} · graph ${mean(graphGrades).toFixed(2)} (${useful(graphGrades)}/${graphGrades.length} useful, ${fresh} fresh, +${withGraph.ms - plain.ms}ms)`);
}
console.log("\narm      mean relevance   useful/graded   useful that plain search missed   added latency");
for (const [arm, t] of Object.entries(totals)) console.log(`${arm.padEnd(8)} ${mean(t.relevance).toFixed(3).padEnd(16)} ${`${t.useful}/${t.graded}`.padEnd(15)} ${String(arm === "graph" ? t.fresh : "-").padEnd(33)} ${arm === "graph" ? `${Math.round(t.ms / QUERIES.length)}ms/query` : "-"}`);
appendFileSync(join(import.meta.dir, "eval.history.jsonl"), JSON.stringify({ at: new Date().toISOString(), note: process.argv[2] ?? "", arms: Object.fromEntries(Object.entries(totals).map(([arm, t]) => [arm, { mean: Number(mean(t.relevance).toFixed(3)), useful: t.useful, graded: t.graded, fresh: t.fresh, msPerQuery: Math.round(t.ms / QUERIES.length) }])) }) + "\n");
