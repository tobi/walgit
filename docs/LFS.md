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

### `lfs.serve_via` — proxy (default) or straight to the bucket
`proxy` streams every byte through walgit (or an edge). `signed_url` hands out store URLs instead, in both
directions, and falls back to the proxying href per object whenever it cannot: signing is never load-bearing and
a signing failure is a WARN line, not a failed push.

| | `proxy` | `signed_url` |
|---|---|---|
| batch `download` | our href (`static_object`) | `signed_get_url` |
| batch `upload` | our href (`PUT objects/<oid>`, size + sha256 checked here) | `signed_put_url`: a presigned `PUT` whose signature covers a header carrying the oid's sha256 |
| `verify` | ours | **ours**, unchanged |
| `authenticated` | absent — git-lfs authenticates our hrefs itself | `true` for the object holding a store URL |

**A signed `PUT` is only ever handed out bound to the oid.** The keyspace is content-addressed and served back to
everyone as immutable, so a `PUT` that accepts any bytes is a write primitive for every oid a client can name.
`ObjectStore::signed_put_url` therefore takes the sha256 and answers `Ok(None)` unless the backend both rejects a
body that does not match it and puts the header carrying it inside the signature (so a client cannot drop it):
- **S3** signs `x-amz-checksum-sha256`; a mismatching body is `BadDigest`, a missing header is
  `SignatureDoesNotMatch`. If the header ever falls outside `X-Amz-SignedHeaders`, the URL is discarded.
- **GCS** cannot: `x-goog-hash` validates only CRC32C/MD5, which walgit does not know for an object it has never
  seen, and `x-goog-content-sha256` is `UNSIGNED-PAYLOAD` on the signed-URL path. Uploads stay proxied there.
- **memory** signs nothing.

Two things follow. `verify` stays walgit's: the store guarantees the content, we still confirm the object arrived
at the size git-lfs promised — and since `authenticated: true` tells git-lfs to add no credential (it applies the
flag to the `verify` POST as well), the `verify` action carries the credential the client used on the batch,
the way `X-Amz-*` rides the upload href. And `lfs.max_object_bytes` can only be enforced where the bytes pass
through, so an object over the cap is not signed; it goes to the proxy href and is refused there with 413.

Tests: `crates/walgit-server/tests/lfs_signed_url.rs` (a store that signs and one that cannot; the cap; and a
real `git lfs push` against a mock bucket that checks the signed checksum the way S3 does, then `verify` here),
`crates/walgit-store/tests/contract.rs` (`signed_put_url` is bound or absent, on every backend) and the S3
signing unit tests in `crates/walgit-store/src/s3.rs` (the checksum header is required *and* inside
`X-Amz-SignedHeaders`; signing needs no bucket).

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
- GCS signs no uploads (§1): the XML API has no sha256 upload checksum to bind them to. A GCS deployment that
  wants signed LFS uploads needs a different lock — a `x-goog-content-length-range` bound plus a maintainer unit
  that re-hashes and drops mismatches would be one, at the cost of a window where a bad object is servable.
- Size accounting of LFS bytes per repository in the overview.
