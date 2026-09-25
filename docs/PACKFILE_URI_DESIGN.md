# Packfile delivery design

Context: contributors replacing bundle delivery, implementing pack maintenance, or changing fetch and
immutable HTTP serving. This is the design target for the packfile migration. The first review unit removes
bundle runtime; the group, proof, lifecycle, and URI mechanisms below are not certified by that removal.
Implementation and test receipts must accompany each later review unit. See [migration](PACKFILE_MIGRATION.md)
for rollout gates and [round trips](ROUNDTRIPS.md) for the existing cost contract.

## Implemented transport and remaining gates

The native v2 path can deliver ordinary packs to anonymous-read clients that negotiate `http` or `https`
packfile URIs. Exact current-policy certificates, live members and complete same-generation dependencies
select a captured baseline; native Git sends the uncovered graph with thin-pack disabled. URI and optional
index sections precede `packfile`, and synthetic-have ACKs are suppressed. Gix/remote engines stay dynamic.
A protected client gets the same delivery when it sends `server-option=uri-auth=proactive`, promising that
every URI download carries its credential (§6, D50). Other protected clients stay dynamic; an index opt-in
flag alone makes no such promise.

`GET|HEAD /{owner}/{repo}[.git]/packfiles/{checksum}.pack` and `.idx` require ordinary read authorization
and freshly revalidated live/retired manifest membership before conditional responses or byte offload.
Protected responses use private immutable caching; optional loopback edge offload still requires admission
on each request. Retired membership and bytes do not expire. No `.rev` delivery exists.

The `packfile-indexes` fetch argument explicitly opts into an additional section containing
`<pack-hash> <index-hash> <index-URL>`. Missing local index trailers omit that sidecar. This server framing
does not qualify a trusted-index client: it must separately verify index hash, pack mapping, object count,
header and integrity/trust settings. Stock clients do not request this extension.

Selection shares at most three exact snapshot reads per request; the ordinary successful selection reads
one snapshot. Threshold selection requires one worthwhile pack and retains small required companions,
or falls back when the complete set exceeds the URI count cap. URLs sort by descending pack size then
checksum. Shallow requests, unsupported filters, have-only negotiation, unknown wants, tag-only baselines
without branch roots, and missing evidence remain dynamic. `blob:none` delivery selects proven history
members only; it cannot treat mixed object packs as history. No optimal-cost or zero-extra-read claim is made.

Default discovery follows `[refs].advertise`, independently of packing policy. For v2 mirror/backup tools
that need every ordinary ref, use `git clone --mirror --server-option=ref-view=all <URL>` (and pass the same
server option on later fetches). V0 follows the configured advertisement selectors; configure `refs/*`
when full v0 discovery is required. Receive-pack advertisements remain complete.

Model-to-code coverage, backend/edge validation, broader producer races and representative resource/performance
acceptance remain separate release gates. Small stock-Git fixtures do not certify large-repository behavior.

## 1. One repository inventory

The object-store bucket remains the repository. Immutable ordinary Git packs, committed WAL entries,
checkpoint/ref snapshots, and the CAS manifest are the only durable authority. Local indexes, materialized
Git directories, and scratch jobs are caches. A successful push is acknowledged only after manifest CAS.

Raw `wal/<checksum>.pack` objects become reusable delivery artifacts. A v2 fetch can offer HTTPS URLs for a
proven subset and generate the uncovered requested graph through upload-pack. No second catalog, mutable
bundle list, scheduled bundle wrapper, or client catch-up URL is needed. Small, bounded, unsupported, or
unproven requests continue through dynamic Git transfer.

## 2. Discovery and groups are independent

Ref advertisement chooses names to show. Named object groups choose logical reachability. History/blob
segments choose physical object types. These policies must not be conflated or treated as access controls.
All outputs belong to the same ordinary Git object database.
Identical physical packs are stored once and can belong to both code and metadata groups. Shared objects
already reachable through a code group are ordinary code data; metadata-only objects must never enter its
coverage. Such a shared pack carries a code audience label, but only an exact code-group certificate can
make it eligible for ordinary URI delivery. Labels alone grant neither access nor coverage.

Group configuration (available in the coverage foundation; transport wiring follows separately):

```toml
[refs]
advertise = ["refs/heads/*", "refs/tags/*"]

[refs.packfiles.code]
kind = "code"
include = ["refs/*"]
subtract = []

[refs.packfiles.meta]
kind = "meta"
include = ["refs/meta", "refs/meta/*"]
```

Selectors use full names or trailing byte-prefix `*`, ordered `!` exclusions, and last-match-wins semantics.
`HEAD` resolves the symbolic default branch in the captured snapshot. Metadata roots are `refs/meta` and
`refs/meta/*`; an auxiliary ref hidden from discovery is not automatically metadata. Normal code URI
selection excludes metadata-only objects, while permitted explicit metadata requests remain dynamic.

At captured generation `s`, the physical set of group `g` is its reachable closure minus the logical closure
of every subtraction dependency. Dependencies form an acyclic graph between code groups. Selecting a group
therefore requires its complete dependency certificates at that same generation, even if a dependency's pack
is small. Settings merge group tables by name, inherit omitted fields, and replace arrays. New groups require
kind and include selectors. Validate unknown fields, selectors, cycles, cross-kind subtraction and bounded
configuration size. Proposed limits: 32 groups, 256 selectors per list, 16 KiB of definition bytes.

The packing-policy identity includes group definitions/subtraction but excludes advertisement. Policy changes
invalidate old eligibility, not repository data. Unmatched indexed objects and historical code surplus go into
retained families; an empty include stops new group output but does not delete old objects. Narrow groups
require exact scope bounds. Changing discovery must ship with an explicit full ordinary-ref view for wildcard
consumers, so omitted names are not misinterpreted as deletions.

## 3. Exact proofs and durable membership

Extend protobuf fields append-only after auditing public field numbers. Keep old WAL/checkpoint decoders.
Each usable coverage certificate must bind:

- Group name and canonical current packing-policy identity.
- Captured WAL sequence and exact content-addressed refs snapshot key.
- The complete checksum set needed to reconstruct that group's closure.

Snapshot bytes include sequence, object format, sorted refs, peeled targets, and symbolic HEAD. The digest
must be verified. Publication binds the exact pointer; existence of a candidate object is not authority.
Labels, tier 2, a global `covers_seq`, or a broad audience marker cannot replace the proof.

Every publication CAS attempt rechecks prospective live members and scope. Retired members authorize old
URLs only; they cannot support new coverage. Checkpoint trimming preserves snapshot descriptors needed by
surviving proofs. A later push can coexist with an older complete baseline and arrive in the dynamic remainder.
Missing proof triggers bounded coverage repair or dynamic fallback, not an automatic repository-wide cut.

Static routes require ordinary repository read authorization and exact live or recorded-retired manifest
membership. Lost-CAS candidates remain inaccessible even if their bytes exist. Retired checksum records and
objects do not expire and are not count-capped: no general collector proves that every issued download has
finished. Storage/manifest growth is an explicit tradeoff until a separate safe collection protocol exists.

## 4. One conserving lifecycle

Push/follow/import publish fresh inputs through the WAL. Independent geometric folds combine compatible
families into tier-1 buffers, excluding frozen packs. Size or settlement freezes useful buffers into tier-2
history and blob segments. Ratio-driven re-segmentation improves the global layout when sufficient new
frozen material accumulates. A manual base operation uses the same mechanism; there is no weekly trigger.

Lifecycle defaults include factor 2, 16 fresh packs, a one-day age arm requiring at least two packs, 14-day
settlement, a 75% frozen target and a 0.5 re-segmentation ratio. Fresh-byte thresholds scale with repository
size and the delivery floor. Missing scope classification or certificates is repaired independently.

`[packs]` is the lifecycle configuration; `walgit.example.toml` documents every field. No alternate engine
selector or retired-object timeout exists. Delta-search threads are resolved explicitly before invoking Git;
zero configured threads means CPU detection, never an uncontrolled `--threads=0` subprocess. The whole-operation
window-memory budget is clamped to host headroom and divided over the resolved thread count. GNU `sort` and
`comm` provide bounded on-disk inventory operations; packaged runtimes include GNU coreutils.

History contains commits, trees and tags; blobs have separate segments. Each segment is independently
indexable, with all delta bases inside the pack. Git graph dependencies may cross segments and must be proved.
Whole cuts preserve path names, order inputs deterministically and recompute deltas with `--no-reuse-delta`.
Reusing compressed undeltified objects is permitted. Freezing a smaller already-self-contained fold may retain
delta reuse. Never enumerate only reachable tips when conservation requires all indexed input objects.

Budget delta-search memory across an explicit Git thread count, clamp for available host memory, and measure
full RSS separately. A proposed 2 GiB target is 2,147,483,648 bytes, not decimal 2 GB. Resolve the intended cap
before acceptance and test oversized-object behavior; `--max-pack-size` alone does not prove an absolute cap.

Use isolated committed input views with explicit producer ownership. Pin index mappings before repack unlinks
files; never infer a producer's output from a shared-directory before/after comparison. Heavy work stays on
the bulk runtime with separate store transport permits. Refs remain responsive throughout.

A resumable operation captures/pins, plans deterministic chunks, verifies each completed chunk, proves
conservation, then seals. All chunks finish before sealing. Earlier COMPACT publications retire nothing; only
the final publication retires inputs and establishes the complete replacement proof. Intermediate manifests
may contain old and new packs. Per-attempt closure checks reject new holes; pre-existing holes remain visible
for repair. Scratch markers are disposable progress, not durable jobs or cross-host resume authority.

Maintain a real MIDX bitmap for segmented native Git reuse and verify that a bitmap was produced. Set native
`pack.allowPackReuse=multi`; a successful MIDX command or ordinary `true` setting is insufficient evidence.
Side-files accelerate reads but never establish object existence by themselves.

## 5. Fetch negotiation and engine guards

Advertise v2 `packfile-uris` capability independently of whether a repository currently has useful packs.
Offer optional `packfile-indexes` only with the implemented extension contract. Select against wants, real
haves, filter/depth, current policy and exact certificates before changing native negotiation.

Useful sets contain at least one pack reaching a proposed 32 MiB floor, include small required companions,
and remain within a proposed 64-URI cap. Over-cap sets fall back whole. Sort largest first, then checksum.
All groups share at most three snapshot attempts. An older held baseline can remove members only with a
conservative proof of client holdings, not an assumption that arbitrary topic haves cover default-branch history.

| Request | Delivery target |
|---|---|
| Full code clone | Complete useful code/dependency set plus dynamic remainder |
| Fresh full-history `blob:none` | History only; reject proofs needing omitted mixed OBJECTS data |
| Shallow/deepen/other filters | Request-specific dynamic transfer |
| Stale unfiltered fetch | New certified members only when older holdings are proved |
| Metadata, unrelated OID, small or unproven request | Dynamic transfer |
| Have-only negotiation | Real acknowledgments only, no pack or URI selection |

The native splice appends proven roots as synthetic haves, suppresses only synthetic ACKs, and inserts URI
and optional index sections before `packfile`. Preserve real ACKs, delimiters, progress, sideband-all,
ready/done, wait-for-done and no-progress behavior. With any selected URI pack, disable thin encoding for the
remainder: the graph is reduced but the dynamic pack is self-contained.

**Every engine must either deliver the selected URLs or avoid synthetic subtraction.** Native, gix,
remote-served and mount/local dispatch need explicit tests. A native-only first implementation must gate
selection before it modifies requests that will take another engine. Capability advertisement is not a
substitute for that guard.

## 6. HTTP, clients and operational cost

Advertised URLs use the normal repository origin and paths `/{owner}/{repo}/packfiles/<checksum>.pack` and
`.idx`. Authorize before cache admission or offload. Refresh refs once on unknown membership before refusal.
Preserve GET/HEAD, strong ETag, conditional 304, Range/206/416 and If-Range behavior. Weak/mismatched If-Range
validators fall back to full bytes. A trusted edge must authenticate even warm cached requests; protected pack
routes never become public to accommodate a client.

| Client/access | Release contract |
|---|---|
| URI-capable client, intentionally anonymous reads | Standard packfile URI lane can accelerate once implemented/tested |
| Stock git ≥ 2.46 with `http.<url>.proactiveAuth`, protected repository, `uri-auth=proactive` sent | Standard packfile URI lane; every download carries the fetch's credential; no refresh after a 401 inside a download |
| Stock or unsignalled client, protected repository | Dynamic fallback; narrate large full-clone cost |
| Independently verified compatible auth/index client | Protected URI acceleration only after distribution/licensing and client validation gates |
| v0 or no matching URI protocol | Dynamic transfer |

`fetch.uriProtocols=https` enables standard URI negotiation; it does not add authenticated download behavior
to stock clients. `http.<url>.proactiveAuth` (git ≥ 2.46) does: git's URI download sends the helper's
username and password up front (it ignores an `authtype`-only answer), and the protected case of
`stock_git_downloads_static_baseline_and_native_residual_while_gix_stays_dynamic` clones that way. Credential
refresh after 401, concurrency and trusted-index install require separately verified client support. Do not advertise an unknown config key as a stock-client feature.
A trusted-index opt-in must verify pack/index checksums, structural bounds, embedded pack mapping and object
count before atomic install, preserve keep/promisor semantics, and respect fsck restrictions. Missing indexes
use ordinary index-pack. `.rev` transfer is outside this design's current implementation target.

Default policy should not refuse compatible stock unbounded clones. Actual resource/placement limitations
still apply and must produce prompt narrated errors rather than uncontrolled full materialization.

Additional selection costs are normally one exact snapshot GET, at most three shared snapshot attempts, and
possible uncached index-trailer range reads. Known manifest membership adds no separate lookup; an unknown
checksum triggers one revalidation. Count depth and requests in simulator tests. Neither zero-extra-read
selection nor a globally optimal cost selector is claimed.

## 7. Acceptance before release

Reconstruct URI packs plus remainder in empty clients and verify strict indexing/connectivity against ordinary
Git for full, branch, tag, blobless, shallow, deepen, sparse, stale and explicit-OID requests. Negative controls
must reject missing dependencies, stale policy, wrong generations, future members, partial seals and lost CAS.
Inspect indexes to prove metadata exclusion and retained-object conservation. Pause downloads across repeated
replacement/restarts and finish them successfully through retired membership.

Test all auth modes, credential refresh, HTTP verbs/validators/ranges, warm edge cache denial, gix/native/remote
engine dispatch, interrupted scratch jobs, corrupt/truncated side-files, producer isolation, concurrent pushes
and checkpoint/settings races. Pin request budgets and refs responsiveness under stalled bulk work. Run both
S3 and GCS contract suites under the public project's test workflow.

Performance receipts must identify source/client versions, host/cache state, repository snapshot, raw commands
and logs, static/dynamic bytes, client indexing and memory/disk cost. Compare the same graph against ordinary
upload-pack and a global pack. A source port or small fixture pass is not representative scale evidence.
