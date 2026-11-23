---- MODULE Mapping ----
EXTENDS Naturals
CONSTANT CheckEvidence
Entities == {"subject", "object"}
VARIABLES pc, sourceSupported, typeMatches, claimSupported, relationMatches,
          mappedSupported, evidenceValid, objectKind, raw, mapped, published
vars == <<pc, sourceSupported, typeMatches, claimSupported, relationMatches,
          mappedSupported, evidenceValid, objectKind, raw, mapped, published>>
Init == /\ pc = "validate" /\ sourceSupported \in [Entities -> BOOLEAN]
 /\ typeMatches \in [Entities -> 0..2] /\ claimSupported \in BOOLEAN
 /\ relationMatches \in 0..2 /\ mappedSupported \in BOOLEAN
 /\ evidenceValid \in BOOLEAN /\ objectKind \in {"entity", "value"}
 /\ raw = Entities /\ mapped = {} /\ published = FALSE
\* graph.project invokes validateRecord before any entity/claim is projected.
Validate == /\ pc = "validate"
 /\ pc' = (IF ~CheckEvidence \/ evidenceValid THEN "entities" ELSE "rejected")
 /\ UNCHANGED <<sourceSupported, typeMatches, claimSupported, relationMatches,
                mappedSupported, evidenceValid, objectKind, raw, mapped, published>>
\* knowledge.mapExtraction retains raw observations, and maps only one supported type.
MapEntities == /\ pc = "entities"
 /\ mapped' = {e \in Entities : sourceSupported[e] /\ typeMatches[e] = 1}
 /\ pc' = "claim"
 /\ UNCHANGED <<sourceSupported, typeMatches, claimSupported, relationMatches,
                mappedSupported, evidenceValid, objectKind, raw, published>>
Endpoints == "subject" \in mapped /\ (objectKind = "value" \/ "object" \in mapped)
\* mapExtraction's relation gate and build's independent Jev mapped-claim gate.
MapClaim == /\ pc = "claim"
 /\ published' = (claimSupported /\ relationMatches = 1 /\ Endpoints /\ mappedSupported)
 /\ pc' = "done"
 /\ UNCHANGED <<sourceSupported, typeMatches, claimSupported, relationMatches,
                mappedSupported, evidenceValid, objectKind, raw, mapped>>
Done == /\ pc \in {"done", "rejected"} /\ UNCHANGED vars
Next == Validate \/ MapEntities \/ MapClaim \/ Done
TypeOK == /\ pc \in {"validate", "entities", "claim", "done", "rejected"}
 /\ sourceSupported \in [Entities -> BOOLEAN] /\ typeMatches \in [Entities -> 0..2]
 /\ claimSupported \in BOOLEAN /\ relationMatches \in 0..2 /\ mappedSupported \in BOOLEAN
 /\ evidenceValid \in BOOLEAN /\ objectKind \in {"entity", "value"}
 /\ raw = Entities /\ mapped \subseteq Entities /\ published \in BOOLEAN
RawRetained == raw = Entities
SupportedEntitiesOnly == \A e \in mapped : sourceSupported[e] /\ typeMatches[e] = 1
SupportedClaimsOnly == published => evidenceValid /\ claimSupported /\ relationMatches = 1 /\ Endpoints /\ mappedSupported
NeverPublishes == ~published
====
