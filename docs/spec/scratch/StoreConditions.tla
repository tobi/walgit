------------------------- MODULE StoreConditions -------------------------
(* Conditional storage must check the token at the mutation, not at an earlier
   HEAD or staging step. Versions here are distinct; content-token ABA is modeled
   separately by LogSlotClaim. No network/auth/pack-byte refinement is claimed. *)
EXTENDS Naturals, TLC
CONSTANT Fault
VARIABLES op, bucket, pc, token, probe, raced, accepted, basis
vars == <<op, bucket, pc, token, probe, raced, accepted, basis>>
Ops == {"delete", "create", "update", "invalid"}
Init ==
  /\ op \in Ops
  /\ bucket = IF op = "create" THEN "absent" ELSE "old"
  /\ pc = "capture" /\ token = "none" /\ probe = FALSE
  /\ raced = FALSE /\ accepted = FALSE /\ basis = "none"
Capture ==
  /\ pc = "capture"
  /\ token' = IF op = "invalid" THEN "invalid" ELSE bucket
  /\ pc' = "probe"
  /\ UNCHANGED <<op, bucket, probe, raced, accepted, basis>>
Probe ==
  /\ pc = "probe"
  /\ probe' = (bucket = token)
  /\ pc' = "commit"
  /\ UNCHANGED <<op, bucket, token, raced, accepted, basis>>
Rival ==
  /\ pc \in {"probe", "commit"} /\ ~raced
  /\ bucket' = "rival" /\ raced' = TRUE
  /\ UNCHANGED <<op, pc, token, probe, accepted, basis>>
Allowed ==
  IF Fault = "invalid" /\ op = "invalid" THEN TRUE
  ELSE IF Fault = "delete" /\ op = "delete" THEN probe
  ELSE IF Fault = "staging" /\ op \in {"create", "update"} THEN probe
  ELSE op # "invalid" /\ bucket = token
Commit ==
  /\ pc = "commit"
  /\ accepted' = Allowed /\ basis' = bucket
  /\ bucket' = IF Allowed THEN (IF op = "delete" THEN "absent" ELSE "published") ELSE bucket
  /\ pc' = "done"
  /\ UNCHANGED <<op, token, probe, raced>>
Next == Capture \/ Probe \/ Rival \/ Commit \/ UNCHANGED vars
Spec == Init /\ [][Next]_vars
TypeOK == /\ op \in Ops /\ pc \in {"capture", "probe", "commit", "done"}
          /\ bucket \in {"absent", "old", "rival", "published"}
          /\ probe \in BOOLEAN /\ raced \in BOOLEAN /\ accepted \in BOOLEAN
MutationCondition == accepted => (op # "invalid" /\ basis = token)
RivalSurvives == (pc = "done" /\ raced) => (~accepted /\ bucket = "rival")
NoRejectedRace == ~(pc = "done" /\ raced /\ ~accepted /\ op # "invalid")
NoSuccessfulDelete == ~(pc = "done" /\ op = "delete" /\ accepted)
NoSuccessfulCreate == ~(pc = "done" /\ op = "create" /\ accepted)
NoSuccessfulUpdate == ~(pc = "done" /\ op = "update" /\ accepted)
=============================================================================
