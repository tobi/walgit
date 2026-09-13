# LFS — objects in the store, history read through from an upstream

Context: **spec + runbook** for Git LFS on walgit. For anyone touching `crates/walgit-server/src/lfs.rs` /
`lfs_upstream.rs`, importing a repository that has LFS history elsewhere, or debugging "(missing)" in a push's
LFS pre-push. `AGENTS.md §1.4` lists LFS as part of the surface; this is the detail.

## 1. Protocol and storage (✅)
- Batch API `POST /{o}/{r}.git/info/lfs/objects/batch` (`operation = upload | download`, transfer `basic`),
  basic transfer `GET|HEAD|PUT /{o}/{r}.git/info/lfs/objects/<oid>`, `POST …/info/lfs/verify`. Auth as
  everything else (`AGENTS.md §1.3`: a bearer token; git-lfs sends it through the same credential helper as git).
- Objects live in the repository's prefix at `lfs/objects/<aa>/<bb>/<oid>` (`walgit_proto::keys::lfs_key`) —
  sha256-addressed, immutable, served by `static_object` with the full static contract (strong ETag, 304,
  Range/If-Range, HEAD; `X-Accel-Redirect` to an edge's cache when one announces it, D23). `PUT` verifies size +
  sha256 before the store write. `lfs.max_object_bytes` (16 GiB) bounds an upload.

## 2. Read-through upstream `upstream.lfs` (✅)
A repository imported from another host keeps its LFS history in that host's LFS server (the import copies packs
and refs, never LFS — without this, `repos/<o>/<r>/` has no `lfs/` prefix and every push with an LFS-tracked file
fails in git-lfs's pre-push with `(missing) <path>`: our batch says 404 for objects that only upstream has).

**Config** (per repository, D24 settings `[upstream]`; host-level in `walgit.toml` works too; `upstream.git` next
to it feeds the maintainer's `repair` unit, `docs/INTEGRITY.md`):
```toml
[upstream]
lfs = "https://github.com/acme/monorepo.git/info/lfs"
token_env = "WALGIT_UPSTREAM_TOKEN"   # env var on the maintaining host that holds the token (never the token itself)
```
`walgit repo settings set acme/monorepo` (or the Settings tab). Settings are published to the bucket, so they name
an environment variable; the host reads it and sends HTTP Basic `x-access-token:<token>` (what GitHub's LFS endpoint
takes). Unset = unauthenticated upstream.

**Behaviour** (`lfs.rs`, `lfs_upstream.rs`):
| Request | Object in our store | Not ours, upstream has it | Nowhere |
|---|---|---|---|
| batch `upload` | **no actions at all** → git-lfs recognizes the object is already present | **no actions at all** → git-lfs treats it as present, so the push proceeds without re-uploading history we never copied | `upload` action (plus `verify` when required) |
| batch `download` | our href (`static_object`) | our href + `?size=N` (the upstream batch demands the exact size; a bare GET has none) | per-object 404 |
| `GET objects/<oid>` | static contract | **stream-through**: one upstream batch (`download`), then the bytes are streamed to the client while being tee'd into a spool file under `cache.dir/lfs-spool/`; after a complete, sha256-verified read the spool is `put` into the store (never on a short or mismatching read; a client that disconnects does not stop the persist). No Range on this first read; the next request is local. | 404 |
| `HEAD objects/<oid>` | static contract | 200 + Content-Length from the upstream batch | 404 |

One upstream batch per request (only the oids we lack), `BATCH_TIMEOUT` 10 s; any upstream failure is logged
and treated as "absent" (today's behaviour), never a 5xx on the batch. Telemetry: spans `lfs.upstream.batch`
{upstream, asked, found} and `lfs.upstream.open` {oid, size}; counter `walgit_lfs_upstream_total{op=batch|
download|persist, result=ok|error|incomplete}`; a `lfs read-through: persisted from upstream` line per object.
Tests: `crates/walgit-server/tests/lfs_upstream.rs` (mock upstream: upload → no actions; download → streamed +
persisted + local afterwards; upstream 404 → 404/upload action; per-repo setting flips it on).

Going forward there is **no sync job**: pushes through walgit upload straight into our store; the upstream
serves only history, and every object it serves once becomes ours.

## 3. One-time bulk backfill (runbook)
When read-through is not enough (you want every historical object local before the upstream goes away), backfill
from a machine with disk and bandwidth: bare clone/fetch of the upstream, `git lfs fetch --all origin`, then upload
`lfs/objects` (git-lfs's on-disk layout is ours) into `repos/<o>/<r>/lfs/objects` with your store's sync tool
(`aws s3 sync`, `gcloud storage rsync`, `rclone`) — create-only, so a second run copies only deltas. Reference: a
118,391-object / 482 GiB history took ≈ 140 min of `lfs fetch --all` and 6 min of upload on a 44-core VM.

## 4. Not done / open
- Upstream `verify` is not called (we only ever ask the upstream for downloads).
- `lfs.serve_via = "signed_url"` hands out presigned store URLs (S3, or GCS with a signer); the default `proxy`
  streams through walgit or the edge.
- Size accounting of LFS bytes per repository in the overview.

## 5. Metrics

`walgit_lfs_handler_seconds{op}` measures batch/get/put/verify handler completion;
a GET body streams afterward. `walgit_lfs_requests_total{op,result}` records
handler outcomes (including HTTP error responses). These do not measure stream
completion, client cancellation or completed downloads; per-object batch errors
still travel in a successful HTTP response.

`walgit_lfs_response_bytes_offered_total` counts Content-Length for successful
GET/Range responses prepared by walgit, excluding HEAD, 304, errors and edge
offload. Interrupted clients may receive less: this is offered volume, not network
throughput. `walgit_lfs_upload_bytes_validated_total` and
`walgit_lfs_upload_object_bytes` count consumed, SHA-256-validated upload bodies,
including ones whose subsequent store write fails. Direct store/edge transfers
need their own byte instrumentation.
