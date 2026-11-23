# Exo's bounded safety proofs

Use the full Everystate source checkout at `c1544c8dc536fb3460badab96db354f1165700ed` (0.3.1). Its formal-verification skill, checker, model construction, conformance implementation and CLI were read before use. No Git history is needed.

```sh
python3 scripts/prove.py /path/to/everystate-source
```

The runner builds the native engine locally with Cargo. To use an already-built binary from that source, set `EVERYSTATE_BINARY` to its path. No global install, ordinary test runner, model API or database connection is used. The runner retains engine and application hashes, exact results and counterexamples under `.audit/proofs/`.

## Claims and bounds

| Model | Finite domain | Safety property | Application binding |
|---|---|---|---|
| Publication | Two publishers, one retry each, an existing document and two new document IDs | The active corpus retains existing documents and committed additions | `cli.ingestCommand` captures its base; `graph.activate` requires the candidate to contain the base IDs, then compares that head and updates it in one conditional `writeBatch` |
| Generation | Two builders, one retry each, two sources, one entity, alias and claim with its required links, one activator | A published generation is complete; a stopped build cannot change the old head | `graph.build` gives each attempt a fresh UUID scope, writes rows separately, then writes a finished marker; `graph.activate` validates before publication |
| Mapping | Two entity observations; zero, one or multiple type/relation matches; every supported/unsupported verdict combination; entity/literal objects; valid/invalid evidence | Unmapped observations remain retained; unsupported or ambiguous observations cannot become published claims | `knowledge.validateRecord`, `mapExtraction`, `validateMapping`, `graph.project` and `build`'s mapped-claim verdict filter |
| Revision | Exact corpus, changed hash, changed count and invalid record; compatible/incompatible ontology | A mismatched corpus is rejected, and incompatible structure is not reported as comparable | `graph.revise` checks its frozen corpus hash/count, then replays both definitions on those same records |

The current safe configurations exhaust their reachable states. Every `*Broken.cfg` deliberately removes one required guard or restores shared mutable scope; it must produce its named counterexample. `*Reachable.cfg` asserts that publication never happens and must fail, establishing that the safe model has a publication path. These are model-checking configurations, not ordinary application tests.

The first draft failed Publication: two callers read the old head, A published, then B published from its old snapshot and lost A's addition. It also failed Generation: a delayed builder cleared the shared scope between activation reads and publication. The implementation now uses expected-head comparison and private attempt scopes. Combining a whole corpus into one transaction was rejected because it would not respect Helix's bounded write batches.

## Assumptions and limits

Each Helix `writeBatch` is assumed atomic, isolated and durably visible on acknowledged completion. The model never combines separate read/write calls into one transaction. Source records and completed generation scopes are immutable under this application; arbitrary external database edits are not modeled. UUIDs and application hashes are assumed collision-free. Publication abstracts generation identity as a monotonic version and models complete additions and rejects candidates that omit existing IDs. It does not compare source text revisions or model a legacy writer changing the corpus during first activation.

Failures occur between operations and retries are bounded as above. No liveness, arbitrary concurrency, exactly-once response delivery, storage-engine recovery or universal correctness for all TypeScript inputs is established. A full-corpus rebalance is a separate operation and remains incomplete until actually executed and reconciled.

The supported labels supplied by Jev are inputs to the Mapping model. Everystate proves how the program gates those labels; it cannot prove that a language model understood a transcript, that all facts were extracted, or that a citation entails a claim. Exact source digests, spans and coverage are checked by application validation; its implementation-to-model correspondence remains subject to code review.

Everystate itself is experimental. Its visited-state table uses 64-bit fingerprints without equality fallback, so conclusions depend on engine correctness and no relevant fingerprint collision. These configurations avoid `VIEW`, symmetry, state-pruning constraints, random simulation and side-effecting operators. The runner requires exit 0, `ok`, a nonempty explored state space and an exhausted queue for a positive verdict. Trace conformance would support only a recorded run and is not claimed here as universal proof.
