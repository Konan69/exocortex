#!/usr/bin/env bun
// Does --graph-rank improve the top 8? Same queries as eval.ts, Jev grades one passage per request.
import { readFileSync, appendFileSync } from "node:fs"; import { join } from "node:path";
const QUERIES = (readFileSync(join(import.meta.dir, "eval.ts"), "utf8").match(/const QUERIES = \[([\s\S]*?)\];/)![1].match(/"[^"]+"/g)!).map((q) => JSON.parse(q));
const key = process.env.OPENROUTER_API_KEY;
if (!key) throw new Error("set OPENROUTER_API_KEY");
async function grade(query: string, passage: string) { for (let a = 0; ; a++) { const r: any = await fetch(process.env.KB_JEV_URL ?? "https://openrouter.ai/api/alpha/decisions", { method: "POST", headers: { "Content-Type": "application/json", Authorization: `Bearer ${key}` }, body: JSON.stringify({ model: "typesafe/jev-1.13", state: { request: query, passage }, questions: { useful: { type: "noul", instructions: "The passage answers the request or contains information directly useful for it" } } }) }).catch(() => ({ ok: false })); if (r.ok) return ((await r.json()) as any).answers.useful.noul as number; if (a === 8) return 0; await Bun.sleep(2000 * (a + 1)); } }
async function top8(query: string, flags: string[]) { const run = Bun.spawn(["bun", join(import.meta.dir, "kb.ts"), "search", query, "--json", "--chunks", "--no-graph", "--top", "8", ...flags], { stdout: "pipe", stderr: "ignore" }); return JSON.parse(await new Response(run.stdout).text()).hits.map((e: any) => e.hit) as any[]; }
const cache = new Map<string, number>(); const g = async (q: string, h: any) => { const k = `${q}|${h.chunkId}`; if (!cache.has(k)) cache.set(k, await grade(q, h.content)); return cache.get(k)!; };
let plain = 0, boosted = 0, changed = 0, n = 0;
for (const q of QUERIES) { const [a, b] = [await top8(q, []), await top8(q, ["--graph-rank"])]; const ga = await Promise.all(a.map((h) => g(q, h))), gb = await Promise.all(b.map((h) => g(q, h)));
  const ma = ga.reduce((x, y) => x + y, 0) / ga.length, mb = gb.reduce((x, y) => x + y, 0) / gb.length; plain += ma; boosted += mb; n++; const diff = b.filter((h) => !a.some((x) => x.chunkId === h.chunkId)).length; changed += diff;
  console.log(`${ma.toFixed(3)} → ${mb.toFixed(3)}  (${diff} of 8 differ)  ${q}`); }
console.log(`\ntop-8 mean: plain ${(plain / n).toFixed(3)} · graph-rank ${(boosted / n).toFixed(3)} · ${changed} of ${n * 8} slots changed`);
appendFileSync(join(import.meta.dir, "eval.history.jsonl"), JSON.stringify({ at: new Date().toISOString(), note: "rank A/B: --graph-rank", plain: plain / n, graphRank: boosted / n, changed }) + "\n");
