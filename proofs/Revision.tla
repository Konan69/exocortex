---- MODULE Revision ----
EXTENDS Naturals
CONSTANT CheckCorpus
VARIABLES pc, observed, compatibleInput, result
vars == <<pc, observed, compatibleInput, result>>
Outcomes == {"exact", "wrongHash", "wrongCount", "invalidRecord"}
Init == /\ pc = 0 /\ observed = "exact"
 /\ compatibleInput \in BOOLEAN /\ result = "none"
\* graph.revise:193 loads immutable generation metadata in one read.
LoadGeneration == /\ pc = 0 /\ pc' = 1
 /\ UNCHANGED <<observed, compatibleInput, result>>
\* graph.revise:194 reads records separately. Outcomes abstract storage and parsing.
ReadRecords == /\ pc = 1 /\ pc' = 2 /\ observed' \in Outcomes
 /\ UNCHANGED <<compatibleInput, result>>
\* graph.revise:195 rejects mismatched count/hash before rule replay.
CheckStoredCorpus == /\ pc = 2
 /\ pc' = (IF CheckCorpus /\ observed \in {"wrongHash", "wrongCount"} THEN 5 ELSE 3)
 /\ result' = (IF CheckCorpus /\ observed \in {"wrongHash", "wrongCount"} THEN "rejected" ELSE result)
 /\ UNCHANGED <<observed, compatibleInput>>
\* graph.revise:196-197 project() validates records and both ontologies.
Replay == /\ pc = 3 /\ pc' = (IF observed = "invalidRecord" THEN 5 ELSE 4)
 /\ result' = (IF observed = "invalidRecord" THEN "rejected" ELSE result)
 /\ UNCHANGED <<observed, compatibleInput>>
\* graph.revise:200 returns comparability(), including FALSE; it does not throw on FALSE.
ReturnComparison == /\ pc = 4 /\ pc' = 5
 /\ result' = (IF compatibleInput THEN "comparable" ELSE "incompatible")
 /\ UNCHANGED <<observed, compatibleInput>>
Done == /\ pc = 5 /\ UNCHANGED vars
Next == Done \/ LoadGeneration \/ ReadRecords \/ CheckStoredCorpus \/ Replay \/ ReturnComparison
TypeOK == /\ pc \in 0..5 /\ observed \in Outcomes
 /\ compatibleInput \in BOOLEAN
 /\ result \in {"none", "rejected", "comparable", "incompatible"}
StaleNeverCompared == result \in {"comparable", "incompatible"} => observed = "exact"
IncompatibleNeverLabeledComparable == result = "comparable" => compatibleInput
====
