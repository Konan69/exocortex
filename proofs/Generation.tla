---- MODULE Generation ----
EXTENDS Naturals
CONSTANTS Builders, MaxRetries, Isolated
Scopes == IF Isolated THEN Builders \X (0..MaxRetries) ELSE {<<"shared", 0>>}
Labels == {"source1", "source2", "entity", "alias", "claim", "links"}
VARIABLES pc, retries, rows, marker, head, activation, selected, snapshots
vars == <<pc, retries, rows, marker, head, activation, selected, snapshots>>
Scope(b) == IF Isolated THEN <<b, retries[b]>> ELSE <<"shared", 0>>
Init == /\ pc = [b \in Builders |-> 0]
 /\ retries = [b \in Builders |-> 0]
 /\ rows = [s \in Scopes |-> {}] /\ marker = {}
 /\ head = <<"old", 0>> /\ activation = 0 /\ selected = <<"old", 0>>
 /\ snapshots = [source |-> FALSE, claim |-> FALSE, entity |-> FALSE]
\* build checks for a completed request fingerprint before making any writes.
ReadExisting(b) == /\ pc[b] = 0
 /\ pc' = [pc EXCEPT ![b] = IF marker # {} THEN 12 ELSE 1]
 /\ UNCHANGED <<retries, rows, marker, head, activation, selected, snapshots>>
\* External extraction and Jev review are assumed valid here; Mapping models their gates.
Review(b) == /\ pc[b] = 1
 /\ pc' = [pc EXCEPT ![b] = IF Isolated THEN 6 ELSE 2]
 /\ UNCHANGED <<retries, rows, marker, head, activation, selected, snapshots>>
\* Deliberately broken configuration models the removed shared-scope clearing writes.
Clear(b, step, labels) == /\ ~Isolated /\ pc[b] = step
 /\ rows' = [rows EXCEPT ![Scope(b)] = @ \ labels]
 /\ pc' = [pc EXCEPT ![b] = step + 1]
 /\ UNCHANGED <<retries, marker, head, activation, selected, snapshots>>
\* Each source, entity and alias write is a separate Helix transaction.
Add(b, step, label) == /\ pc[b] = step /\ label \notin rows[Scope(b)]
 /\ rows' = [rows EXCEPT ![Scope(b)] = @ \cup {label}]
 /\ pc' = [pc EXCEPT ![b] = step + 1]
 /\ UNCHANGED <<retries, marker, head, activation, selected, snapshots>>
\* KnowledgeClaim and its SUBJECT/EVIDENCE/OBJECT links are written together.
AddClaim(b) == /\ pc[b] = 10 /\ "claim" \notin rows[Scope(b)]
 /\ {"source1", "source2", "entity"} \subseteq rows[Scope(b)]
 /\ rows' = [rows EXCEPT ![Scope(b)] = @ \cup {"claim", "links"}]
 /\ pc' = [pc EXCEPT ![b] = 11]
 /\ UNCHANGED <<retries, marker, head, activation, selected, snapshots>>
\* finish_generation is separate from the projection writes, not an invented transaction.
Finish(b) == /\ pc[b] = 11 /\ Scope(b) \notin marker
 /\ marker' = marker \cup {Scope(b)} /\ pc' = [pc EXCEPT ![b] = 12]
 /\ UNCHANGED <<retries, rows, head, activation, selected, snapshots>>
Fail(b) == /\ pc[b] \notin {12, 13}
 /\ pc' = [pc EXCEPT ![b] = 13]
 /\ UNCHANGED <<retries, rows, marker, head, activation, selected, snapshots>>
\* build allocates a new UUID on rerun; the previous attempt's scope is never cleared.
Retry(b) == /\ pc[b] = 13 /\ retries[b] < MaxRetries
 /\ retries' = [retries EXCEPT ![b] = @ + 1] /\ pc' = [pc EXCEPT ![b] = 0]
 /\ UNCHANGED <<rows, marker, head, activation, selected, snapshots>>
LoadMarker == /\ activation = 0 /\ marker # {} /\ selected' \in marker /\ activation' = 1
 /\ UNCHANGED <<pc, retries, rows, marker, head, snapshots>>
ReadSources == /\ activation = 1 /\ activation' = 2
 /\ snapshots' = [snapshots EXCEPT !.source = {"source1", "source2"} \subseteq rows[selected]]
 /\ UNCHANGED <<pc, retries, rows, marker, head, selected>>
ValidateSources == /\ activation = 2 /\ activation' = (IF snapshots.source THEN 3 ELSE 8)
 /\ UNCHANGED <<pc, retries, rows, marker, head, selected, snapshots>>
ReadClaims == /\ activation = 3 /\ activation' = 4
 /\ snapshots' = [snapshots EXCEPT !.claim = "claim" \in rows[selected]]
 /\ UNCHANGED <<pc, retries, rows, marker, head, selected>>
ReadEntities == /\ activation = 4 /\ activation' = 5
 /\ snapshots' = [snapshots EXCEPT !.entity = "entity" \in rows[selected]]
 /\ UNCHANGED <<pc, retries, rows, marker, head, selected>>
ValidateProjection == /\ activation = 5
 /\ activation' = (IF snapshots.claim /\ snapshots.entity THEN 6 ELSE 8)
 /\ UNCHANGED <<pc, retries, rows, marker, head, selected, snapshots>>
\* One activator here; Publication separately models competing head updates and CAS.
Publish == /\ activation = 6 /\ head' = selected /\ activation' = 7
 /\ UNCHANGED <<pc, retries, rows, marker, selected, snapshots>>
StopActivation == /\ activation \in 0..6 /\ activation' = 8
 /\ UNCHANGED <<pc, retries, rows, marker, head, selected, snapshots>>
Done == /\ (\A b \in Builders : pc[b] \in {12, 13}) /\ activation \in {7, 8}
 /\ UNCHANGED vars
Next == (\E b \in Builders : ReadExisting(b) \/ Review(b) \/
 Clear(b, 2, {"claim", "links"}) \/ Clear(b, 3, {"alias"}) \/
 Clear(b, 4, {"entity"}) \/ Clear(b, 5, {"source1", "source2"}) \/
 Add(b, 6, "source1") \/ Add(b, 7, "source2") \/ Add(b, 8, "entity") \/ Add(b, 9, "alias") \/
 AddClaim(b) \/ Finish(b) \/ Fail(b) \/ Retry(b)) \/ LoadMarker \/ ReadSources \/ ValidateSources \/ ReadClaims \/ ReadEntities \/ ValidateProjection \/ Publish \/ StopActivation \/ Done
TypeOK == /\ pc \in [Builders -> 0..13] /\ retries \in [Builders -> 0..MaxRetries]
 /\ rows \in [Scopes -> SUBSET Labels] /\ marker \subseteq Scopes
 /\ head \in Scopes \cup {<<"old", 0>>} /\ selected \in Scopes \cup {<<"old", 0>>}
 /\ activation \in 0..8 /\ snapshots \in [{"source", "claim", "entity"} -> BOOLEAN]
ActiveComplete == head # <<"old", 0>> => head \in marker /\ rows[head] = Labels
MarkerComplete == \A scope \in marker : rows[scope] = Labels
OldHeadUntilPublish == activation # 7 => head = <<"old", 0>>
NeverPublishes == head = <<"old", 0>>
====
