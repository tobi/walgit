# Bundle removal and packfile migration

Context: operators and reviewers moving existing walgit repositories to the
[packfile design](PACKFILE_URI_DESIGN.md). This document describes a phased review and rollout plan.
The bundle-removal review unit does not establish production packfile delivery, protected-client acceleration,
or scale performance. Each later unit needs its own tests and compatibility decision before enablement.

## Review stack

1. Remove bundle runtime: crate/command, protocol command, list/catch-up routes, schedules, attempt caches,
   configuration, recipes and UI. Keep ordinary dynamic clone/fetch and durable format decoders.
2. Add independent group/discovery policy, exact coverage snapshots/certificates, append-only schema fields,
   live/retired membership and CAS guards. Prove old-data replay before emitting URLs.
3. Add conserving classification/folds/freezes/re-segmentation, repairable proofs, isolated producers,
   resumable plan/step/seal, memory bounds and verified MIDX bitmap construction.
4. Wire request selection, safe engine dispatch/splice, authenticated static serving and retired URL survival.
5. Validate distributable clients, supported backend/edge combinations, complete UI/SDK/operator integration,
   and publish representative correctness/performance receipts and the concrete rollback floor.

Each review unit must build and preserve supported ordinary Git behavior. Stack position is not permission
to leave a half-proved URI path enabled. Actual commits, test results and remaining gates belong in the PRs;
this plan is not a test receipt.

## Host configuration

Remove `[bundles]` and every `[[bundles.strategy]]` table, `server.roles` entries named `bundle`, and
`cache.bundle_list_entries`. The new parser rejects these removed host shapes, and so does the environment
override loader: remove corresponding `WALGIT__BUNDLES__*` and cache overrides from the host's deployment
configuration as well.

The lifecycle layer replaces `[compaction]` with `[packs]`; old host keys and newly submitted old settings
are rejected. The ordinary `compact` role remains. LFS `serve_via = "proxy"` or `"signed_url"` is unchanged.
Use the documented lifecycle fields in `walgit.example.toml`: factor/count/age triggers, settlement freeze,
ratio-driven re-segmentation, and bounded delta-search resources. There is no engine selector (Git performs
packing), flat `trigger_bytes`, or `retention_superseded` knob. Remove corresponding old environment overrides.

Use `walgit config check` with the actual host configuration/environment before restart. Compare rendered
effective placement, auth, maintenance and upstream settings to the intended values.

## Durable repository settings need an explicit transition

Existing `RepoSettings.toml` is durable WAL data. Rejecting new `[bundles]` input must not erase that record or
make log/checkpoint replay undecodable. It must also not silently discard unrelated overrides or widen ref
visibility/object scope by falling back to broad host defaults.

Before replacing old readers/writers, inspect each repository's saved settings and save its full document,
revision and effective configuration. Using the existing administrative settings API/CLI, publish a reviewed
replacement document that removes only bundle-specific sections while preserving maintenance,
upstream and all other still-supported intent. For example:

```toml
# Before: stored by the bundle-serving release.
[bundles]
main_only = true
[maintenance]
checkpoints = false
[upstream]
follow = ["refs/heads/main"]
```

The replacement retains both `[maintenance]` and `[upstream]` exactly. The old bundle `main_only` policy is
not a ref-advertisement policy or an exact future packing certificate. When introducing groups, choose its
replacement deliberately from the saved intent; never reinterpret absence as permission to deliver every
namespace. Preview/validate the replacement, publish through the normal SETTINGS transaction, and reread it.
Coordinate concurrent settings writers during this transition; a whole-document replacement must not overwrite
a newer administrator's change.

The removal phase includes a durable-only read transition: it removes the obsolete bundle table from the
in-memory settings document, warns, and applies the remaining supported overrides. Raw saved TOML and WAL
history are preserved. Host files and newly published settings still reject the removed table. This bounded
exception does not turn `main_only` or bundle ref filters into new group policy. Publishing the reviewed
replacement document remains the explicit durable migration. A warning followed by dropping every override
and using broad host defaults does not meet the gate.
Historical SETTINGS entries stay intact and replayable; no bucket-wide rewriting or log mutation is authorized.
Rollback/replay to a historical settings revision must apply the same transition checks before object work.

## Saved compaction settings

The lifecycle layer has a bounded read transition for old durable documents only. It preserves the raw saved
record and maps supported intent into the effective config:

| Saved compaction key | Effective packs key |
|---|---|
| enabled | enabled |
| factor | geometric_factor |
| trigger_packs | fold_when_fresh_packs_reach |
| lease_ttl | lease_ttl |
| engine = "git" | removed; native Git is the only producer |

Old flat byte triggers do not have the same meaning as proportional fold thresholds. Time-limited retired
pack retention contradicts issued-URL safety. If saved `trigger_bytes` or `retention_superseded` is present,
the transition **disables pack maintenance** and warns until an administrator publishes reviewed `[packs]`
settings. Ordinary serving remains available. It never guesses a size threshold or starts pruning objects.

A document containing both old `[compaction]` and new `[packs]` fails validation rather than overwriting either
one. Mapped fields must pass current validation; unsupported old values require an explicit rewrite. Other
settings, including narrow ref selectors and upstream settings, remain intact. Preview the full replacement,
publish through the settings API/CLI and verify the stored revision and effective values. This is bucket-data
compatibility, not a host-config alias. Historical entries are never rewritten.

## Durable schema and writer cutover

Audit the public protobuf numbering before adding descriptors. Keep old bundle message decoders and checkpoint
bundle-pointer fields, but stop using them for runtime delivery. Existing unclassified packs remain repository
data; they are not static offers until classification and exact current-policy proofs establish eligibility.

All writers that rewrite manifests must preserve exact snapshots and retired membership before those fields
become authority. Stop incompatible readers and push, follow, import, checkpoint and maintenance writers before enabling new
authority. A new format number alone cannot fence old binaries: the pre-migration readers do not validate
`Manifest.format_version`. A future minimum-reader gate protects only readers that implement it; it cannot
retroactively make deployed old readers safe.
An older binary that drops these fields can invalidate issued URLs or proofs even if it can decode the message.
The coverage-foundation writer uses canonical content-addressed ref snapshots and attempt-specific checkpoint
metadata as soon as it runs; there is no separate snapshot-writing toggle. Complete the incompatible-writer
stop before starting this binary against an existing bucket. Old checkpoint snapshots remain readable through
their committed descriptor; readers must not reconstruct an old snapshot address from sequence alone.
The rollback floor is therefore the earliest tested release preserving the new authority, not simply a binary
that accepts the protobuf. A concrete compatible release hash must be recorded before enabling the new writer;
none is certified by the removal unit.

Test old manifest/log/checkpoint fixtures, current-settings migration and rollback/replay behavior. Exercise CAS
retries and concurrent publication so stale plans cannot reintroduce retired members or erase proof descriptors.

## Client transition

New recipes stop writing `transfer.bundleURI`, `fetch.bundleURI`, or `--bundle-uri`. Existing clients may still
have repository-local `fetch.bundleURI` pointing at removed catch-up routes. Explicitly remove that setting
in affected clones; inspect global/origin-scoped configuration too if an operator previously added it. Do not
silently rewrite users' global Git configuration as part of a server upgrade.

Standard clients retain dynamic fetch/clone. `fetch.uriProtocols=https` can negotiate standard URI delivery on
the native engine for anonymous-read repositories. Remote/gix serving stays dynamic. Protected URI acceleration requires a distributable, license-reviewed
client with verified authenticated downloads; version strings alone are insufficient. Until that gate passes,
document protected dynamic fallback honestly. Never open protected static routes to anonymous requests.

Normal discovery advertises configured `refs.advertise` selectors, independently of packing groups. The
default includes heads and tags. A complete v2 mirror or backup must explicitly request all namespaces:
`git -c protocol.version=2 clone --mirror --server-option=ref-view=all <url>`; subsequent complete fetches
also need `--server-option=ref-view=all`. This changes discovery, not authorization. Receive-pack continues
to see the complete ref set. A v0 client uses the configured advertisement selectors.

## Data retention and release evidence

Removing code does not authorize deleting bucket data. Obsolete `bundles/` objects and checkpoint `.bundle`
wrappers can be considered for a separate operator cleanup only after no reader/writer depends on them.
Never delete `wal/`, manifests, logs, refs snapshots or real checkpoint metadata because a bundle wrapped them.
Retired pack data and exact URI membership records are retained without a timeout/count cap in the new design.

Before release, attach graph-equivalence, metadata-isolation, dependency-race, interrupted-download, auth/range,
backend/edge, ordinary-client fallback, and engine-combination results. Include cold/truncated-read recovery,
conserving maintenance and responsive-refs tests. Representative benchmarks must state exact byte units and
measure server and client work, not just reduced application egress. Unfinished gates remain visible in the PR
stack and release notes; the design document itself certifies none of them.
