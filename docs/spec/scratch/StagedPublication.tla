------------------------ MODULE StagedPublication ------------------------
(* Generic conditional multipart create. Each attempt owns immutable staging
   names; only the final conditional operation publishes the whole object.
   No bounds on actual bytes, SDK retries or orphan reclamation are proved. *)
EXTENDS Naturals, FiniteSets, Sequences, TLC
CONSTANT Fault
Writers == {"a", "b"}
Parts == 1..2
NoPart == <<"none", 0>>
Keys == Writers \X Parts
VARIABLES blocks, staged, finished, destination, owner, successes, overlap
vars == <<blocks, staged, finished, destination, owner, successes, overlap>>
Key(w, i) == <<IF Fault = "shared-names" THEN "a" ELSE w, i>>
Init ==
  /\ blocks = [k \in Keys |-> NoPart]
  /\ staged = [w \in Writers |-> 0]
  /\ finished = {} /\ destination = <<>> /\ owner = "none"
  /\ successes = {} /\ overlap = FALSE
Stage(w) ==
  /\ w \notin finished /\ staged[w] < 2
  /\ LET i == staged[w] + 1 IN
       /\ blocks' = [blocks EXCEPT ![Key(w, i)] = <<w, i>>]
       /\ staged' = [staged EXCEPT ![w] = i]
  /\ overlap' = (overlap \/ (\E other \in Writers \ {w}: staged[other] > 0))
  /\ UNCHANGED <<finished, destination, owner, successes>>
Commit(w) ==
  /\ w \notin finished
  /\ (staged[w] = 2 \/ (Fault = "early-commit" /\ staged[w] = 1))
  /\ finished' = finished \cup {w}
  /\ IF destination = <<>> \/ Fault = "unconditional"
     THEN /\ destination' = <<blocks[Key(w, 1)], blocks[Key(w, 2)]>>
          /\ owner' = w /\ successes' = successes \cup {w}
     ELSE UNCHANGED <<destination, owner, successes>>
  /\ UNCHANGED <<blocks, staged, overlap>>
Next == (\E w \in Writers: Stage(w) \/ Commit(w)) \/ UNCHANGED vars
Spec == Init /\ [][Next]_vars
WholeAttempt == destination = <<>> \/ destination = <<<<owner, 1>>, <<owner, 2>>>>
OneCreateWinner == Cardinality(successes) <= 1
NoOverlapWinner == ~(overlap /\ destination # <<>>)
NoLoserRejection == ~(finished = Writers /\ Cardinality(successes) = 1)
NoSecondWriterWinner == ~(owner = "b")
=============================================================================
