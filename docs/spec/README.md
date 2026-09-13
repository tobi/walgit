# Bounded contract checks

These models explore finite interleavings of publication, snapshots, replacement, delivery and retirement.
They do not prove a refinement of Rust, validate Git pack bytes, or certify deployment performance. The
executable inventory is [tlc/cases.tsv](tlc/cases.tsv); a checked-in row is not an execution receipt.

Run `just spec` for the matrix plus the bounded reference and its old-value negative control. Use
`just spec-fragments` to omit those two reference runs, or `just spec-full` to add larger reference arms.
Java 11 or newer, Bash, ripgrep, curl, a SHA-256 utility and GNU timeout are required. Defaults are two JVM
jobs, two workers and 2 GiB heap per job. Full reference runs are serial with eight workers and 8 GiB.
Override `WALGIT_TLC_JOBS`, `WALGIT_TLC_WORKERS`, `WALGIT_TLC_HEAP` and `WALGIT_TLC_TIMEOUT` explicitly;
the timeout defaults to 600 seconds per invocation. More jobs multiply the heap budget.

Each invocation retains its own effective configuration, log and verdict under `target/test-logs/spec-*`.
A fixed arm requires exit 0, the completion banner and an exhausted enumeration queue. A mutation or
negated reachability witness requires exit 12 and exactly its named invariant. A temporal boundary requires
exit 13 and its named property. Counterexamples require a trace and completion footer. Unexpected errors,
deadlocks, timeouts, duplicate verdicts and truncated logs fail; `scripts/test-spec-runner.sh` checks these
classifier rules without a JVM. A witness counterexample demonstrates reachability, not a failed guarantee.

Finite fragments explicitly include the stuttering already allowed by their bracketed next-state relation.
This does not change their temporal behavior or weaken their invariants. It lets TLC check with its normal
deadlock detection enabled; it does not establish application deadlock freedom. Progress is a separate
property with explicit fairness assumptions. Liveness configurations must not use symmetry reduction.
Maintenance safety models do not prove eventual scheduling, repair deadlines or sustained convergence.

## Public contract identifiers

| ID | Obligation |
|---|---|
| C1 | An acknowledged publication is durable from the bucket alone. |
| C2/C3 | Reads revalidate and use one committed snapshot; concurrent writers compose without regressing fields. |
| C4 | Ref old values hold at the successful CAS, including retries. |
| C5 | Complete submissions commit all-or-none; independent batch members remain independent. |
| C6a/C6d | Current tips and candidate external dependencies are covered by the exact resulting committed inventory. |
| C6b | Fetch omissions require honest negotiation: common closure, declared shallow boundaries, granted filters, negotiated thin bases, or **negotiated packfile URIs under exact coverage**. The URI case is the fifth lawful class. Never ACK synthetic roots to the client or subtract them on an engine that emits no URLs. |
| C6c | Granted filters preserve on-demand access to reachable omitted objects. |
| C7/C8 | Uncommitted candidates confer no serving authority; receipts distinguish committed, rejected and unknown outcomes. |
| C9 | Ambiguous forwarding never causes local replay; duplicate no-op requests publish no new entry. |
| C10a/C10b | Maintenance preserves objects and active historical reads. Issued URI records and bytes have no arbitrary expiry or cap. |
| C15/C16 | Conditional termination and eventual folding require the stated fairness and finite-workload assumptions. |
| B2 | Each CAS retry derives evidence from its newly captured basis. |
| B5/B6 | Immutable identity is exact; multi-read operations never combine generations. |

The bounded [WALContract](WALContract.tla) is a reference abstraction, not executable validation to install
mechanically. Its fast bound of two pushes cannot reach a successful multi-ref transaction, and its original
transaction pool cannot collect previously published objects. BatchPublication and MCTrim provide dedicated
multi-ref and historical-trim witnesses. `MC_incomplete` intentionally violates `PushTerminates`: incomplete
inputs have no rejection action in that frozen abstraction. Rust must instead reject missing inputs all-or-none
within a bounded deadline; this control does not demonstrate a new Rust defect.

WALCheckpoint assumes contiguous segments. Its `SegAtMin` equality is stronger than the concrete rules for
burned sequence gaps and genesis and must not be copied into runtime validation. Segmented packs also invalidate
an at-most-one-base invariant. ClosureProvenance and PackReplacement include hypothetical pruning mutations;
public maintenance is conserving and exposes no pruning mode.

## Model-to-code map

The exact fixed arms, faults and negated witnesses are named in the matrix. A listed model can expose a
required implementation obligation even while the corresponding Rust audit remains incomplete.

| Contract | Public correction or boundary | Model / precise controls | Deterministic Rust or Git evidence |
|---|---|---|---|
| C2/C3, B5/B6 | Content-addressed coverage snapshots; committed checkpoint pointer; paired manifest/token/ref capture | SnapshotAuthority: guess/splice/cache; crossed/orphan witnesses. PublicationView: split capture | WAL coverage/checkpoint tests and serial simulation suite; further warm-race twins remain required |
| C6b/C6c, B2 | Exact policy/member/dependency selection, shared snapshot budget, native splice and engine gates | PackCoverage: retire/scope/policy/dependency/generation/companion; retry/old/new/concurrent witnesses | `packfile_uri` selector tests; stock-Git SHA-1/SHA-256 filtered clone and checkout; native residual, metadata isolation and gix/protected fallback |
| C6d/C10a | Exact captured inputs, raw indexed-link validation in isolated inputs, conserving disk-spooled families, additive output commits and per-attempt indexed conservation/current-tip checks at final seal | PackReplacement: raw/conserve/replace/supersedes; seal/refusal/race/retry witnesses. ClosureProvenance: boundary/tip/seal/retry/rival controls | `pack_segments` conservation/resume fixtures and WAL exact-seal tests; SHA-1/SHA-256 unreachable broken-link rejection and bounded metadata validation; real-index seal regression rejects lost unreachable objects and post-plan uncovered tips; candidate publication/raw-boundary gates and adversarial CAS-race twins remain open |
| C2/C3 | Checkpoint anti-regression and composition against the current CAS basis | WALCheckpoint fixed/bug; ManifestComposition stale-base/no-checkpoint/no-tips and composite-story witnesses | Delayed-checkpoint test and simulation replay oracle; combined publisher/checkpointer/sealer twin remains required |
| C5/C7/C8 | No-op receipts use seq 0 and publish no entry; empty-ref pack-only requests reject; unknown CAS remains typed unknown | BatchPublication: partial/isolation/retry/phantom/absence/noopphantom; atomic, late, folded and no-op witnesses | `noop_receipts_never_claim_a_siblings_log_entry`; `lost_cas_reply_is_resolved_or_unknown_without_losing_the_commit`; further late/overlaid failure twins remain required |
| C3/C7, B5 | Per-claim nonce prevents content-token aliasing; lost-response resolution checks exact bytes | LogSlotClaim: burned retry/early sweep/ambiguous delete/deterministic bytes; retry/sweep/late witnesses | `recreated_claims_have_distinct_bytes_and_resolution_checks_identity`; orphan WAL and fault simulation tests; delayed-delete ABA twin remains required |
| C9 | Disable redirects and transport retries; only pre-delivery failures permit local fallback | FrontReplay: ambiguous replay/connect taxonomy/no precondition/no-op publishing; fallback/recovery/quiet no-op witnesses | `ambiguous_delivery_and_gateway_responses_never_publish_locally`; no-op WAL receipt test |
| C10b | Exact retired membership and retained bucket bytes | MCTrim: reader mutation and historical-trim witness | Protected retired pack/index download test; explicit pinned historical local-reader twin remains required |

| C2/C3, B2 | Carry readiness only from an already-proven matching inventory; recheck revision-only changes after restart | ReadinessCarry: packs-only, no-carry, annotation/apply waste, stale opener, birth and empty-state controls | `readiness_carries_only_proven_inventory_and_rechecks_revision_only_restart` |
| C7, B5/B6 | Validate index checksum, pack identity and count; replace damaged inodes and return opened mappings even after zero-download reuse | CacheDiscipline: presence/identity/reprove/fail-open/reader/sweep controls and rescue witnesses | `repairs_corrupt_and_wrong_identity_indexes_and_pins_old_readers` (both Git hash formats); publisher evidence integration remains required |
| C10b | Attempt-owned temporary paths; cross-process kernel lock held by blocking workers through cancellation; reclaim only after acquiring that lock | TempReclaim: drop/sweep/dead-owner/interlock/age mutations; induction and ownership/recovery witnesses | `blocking_owner_keeps_lock_after_request_cancellation`; `killed_process_releases_ownership_before_residue_reclaim`; abandoned-temp recovery in the index test |

### Cache abstraction boundaries

CacheDiscipline is an obligation reference with separate probe, proof, repair and sweep windows. The public
index cache serializes name operations across processes and returns mmap handles before releasing its lock;
readers keep those handles across later removal. A stale sweep can discard a reusable name but cannot remove
an active operation's unopened name. This is a different interlock from the model's atomic live-manifest
sweep rule. The model does not establish a refinement of that lock or of future publisher evidence gates.

TempReclaim models a PID registry with two processes and two lanes, including an inductive litter bound and
its negative controls. Public index downloads instead use a kernel file lock and at most four concurrent
attempts per repository cache. Blocking workers retain ownership after their async request is cancelled.
Acquiring the lock permits reclaiming abandoned attempt names without age, PID reuse or namespace guesses.
The reference's PID oracle and exact lane bound are not claims about this implementation. File count is not
a byte-budget or eventual-reclamation proof; pack installation outside this index cache still needs its own
cancellation/temporary-ownership audit. Both reference models retain their safety mutations and witnesses.

## Remaining scope and limits

Publisher cache-proof integration and cancellation/ownership auditing outside the remote index cache remain required.
PolicyPinLocality and its 29 matrix arms are excluded: they require policy blobs addressed through a
committed metadata ref, same-batch metadata writers and quarantine-backed policy resolution. This public
tree loads `policy.json` once in `smart.rs` and saves it separately in `policy.rs`; none of those Git-policy
operations exist. Importing that model would certify a different protocol. Concurrent policy-edit semantics
are therefore not established by this packfile port; adding CAS-bound Git policy would be a separate design
change. Per-submission independence remains covered by BatchPublication and its Rust receipt tests.

Small downloads now reject clean early EOF and oversized bodies, with a deterministic failed-download/retry
test. This does not close the full cold truncated-read readiness boundary. That boundary and
seal-evidence cooldown remain open. Green model reruns do
not close either gap. Producer quarantine/import, namespace recreation, resource limits, a distributable
protected URI client, live backend/edge tests and representative performance measurements are separate gates.
No model proves authentication, pack encoding, index hashes or Git wire correctness; real regression tests
must cover those boundaries. There is no bucket collector in this port: PackCoverage assumes retention,
MCTrim models a reader-safe collector, and neither proves an unimplemented collector.

## Tool provenance

The checksum-pinned jar is upstream TLA+ `v1.8.0`, revision
`867aefb69ffc2452031292587b389d1fc3eb43ff`, built 2026-09-12. Its SHA-256 is
`db131ddb48e7004d823bef4493df7b35694babe37505b9d9fa5685e7a331f1f1`; the downloaded bytes match the
[release asset digest](https://github.com/tlaplus/tlaplus/releases/tag/v1.8.0) and jar manifest. This tag is
rolling. A changed asset must fail checksum verification until provenance is reviewed and checks rerun.
The [source range](https://github.com/tlaplus/tlaplus/compare/b123b22...867aefb) includes parser/diagnostic
changes and empty-set equality, enumeration and fingerprint corrections; it is not a semantics-neutral
update. Historical state counts are not reused as new evidence. The runner does not substitute another
checker, especially one without equivalent result, liveness and input semantics.

### Conditional backend operations

C3/C7/B5 → S3 native conditional delete and multipart completion; GCS rejects invalid,
zero and negative update generations instead of silently changing the operation.
`StoreConditions` checks the mutation point with separate capture, probe/staging, rival
write and commit actions. Its delete-window, staging-window and invalid-token mutations
must each violate `MutationCondition`; separate witnesses reach rejected rivals and
successful delete/create/update. Rust twins are
`conditional_delete_preserves_rivals_and_only_probes_on_failure`,
`staged_put_and_compose_conditions_hold_at_completion`, and
`update_conditions_cannot_turn_into_create_or_overwrite`.
The model uses distinct versions; content-token ABA remains in `LogSlotClaim`.
It does not certify a compatible service's implementation of conditional headers.

C3/C5/C7 → Azure attempt-unique block names, complete-input checks and conditional
block-list commit. `StagedPublication` explores two concurrent two-part creates;
shared-name and early-commit mutations violate `WholeAttempt`, while unconditional
completion violates `OneCreateWinner`. Witnesses reach overlapping staging, a
loser's rejected commit and either writer winning. Rust twins:
`competing_staged_creates_cannot_mix_each_others_blocks`,
`malformed_lengths_and_source_errors_never_commit`, and
`copy_failure_does_not_commit_a_partial_destination`; S3's completion-condition
regression and `multipart_length_mismatch_never_reaches_completion` also apply.
This is a safety abstraction, not a claim about byte
budgets, cancellation cleanup or eventual staging garbage collection.
