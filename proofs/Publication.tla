---- MODULE Publication ----
EXTENDS Naturals, FiniteSets
CONSTANTS Writers, CheckParent, MaxRetries
VARIABLES active, version, snapshot, baseVersion, published, pc, retries
vars == <<active, version, snapshot, baseVersion, published, pc, retries>>
Init == /\ active = {} /\ version = 0 /\ published = {}
        /\ snapshot = [w \in Writers |-> {}]
        /\ baseVersion = [w \in Writers |-> 0]
        /\ pc = [w \in Writers |-> "read"]
        /\ retries = [w \in Writers |-> 0]
\* cli.ingestCommand reads currentGeneration and its immutable records before build.
ReadBase(w) == /\ pc[w] = "read"
               /\ snapshot' = [snapshot EXCEPT ![w] = active]
               /\ baseVersion' = [baseVersion EXCEPT ![w] = version]
               /\ pc' = [pc EXCEPT ![w] = "publish"]
               /\ UNCHANGED <<active, version, published, retries>>
\* graph.activate: matching head and conditional update share one writeBatch.
Publish(w) == /\ pc[w] = "publish"
 /\ (IF ~CheckParent \/ baseVersion[w] = version
       THEN /\ active' = snapshot[w] \cup {w}
            /\ version' = version + 1
            /\ published' = published \cup {w}
            /\ pc' = [pc EXCEPT ![w] = "done"]
       ELSE /\ UNCHANGED <<active, version, published>>
            /\ pc' = [pc EXCEPT ![w] = "stale"])
 /\ UNCHANGED <<snapshot, baseVersion, retries>>
\* An explicit rerun reads the new head; it does not publish the stale candidate.
Retry(w) == /\ pc[w] = "stale" /\ retries[w] < MaxRetries
 /\ pc' = [pc EXCEPT ![w] = "read"]
 /\ retries' = [retries EXCEPT ![w] = @ + 1]
 /\ UNCHANGED <<active, version, snapshot, baseVersion, published>>
Stop(w) == /\ pc[w] \in {"read", "publish", "stale"}
 /\ pc' = [pc EXCEPT ![w] = "stopped"]
 /\ UNCHANGED <<active, version, snapshot, baseVersion, published, retries>>
Done == /\ \A w \in Writers : pc[w] \in {"done", "stopped"}
        /\ UNCHANGED vars
Next == (\E w \in Writers : ReadBase(w) \/ Publish(w) \/ Retry(w) \/ Stop(w)) \/ Done
TypeOK == /\ active \in SUBSET Writers /\ published \in SUBSET Writers
 /\ version \in 0..Cardinality(Writers)
 /\ snapshot \in [Writers -> SUBSET Writers]
 /\ baseVersion \in [Writers -> 0..Cardinality(Writers)]
 /\ pc \in [Writers -> {"read", "publish", "done", "stale", "stopped"}]
 /\ retries \in [Writers -> 0..MaxRetries]
KeepsPublished == published \subseteq active
NeverPublishes == published = {}
====
