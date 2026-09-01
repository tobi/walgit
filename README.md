# walgit — a git server that is one binary in front of an object store

walgit hosts git repositories with **no database, no leader and no local state that matters**. You run a
single binary, point it at an S3, GCS or Azure Blob bucket, and you have: smart HTTP (v0/v2) fetch and
push, `bundle-uri` clones served as static files, Git LFS, a browsing web UI, a JSON API with an SDK,
per-repository push policy, webhooks — and a server that scales to repositories **larger than the machine it
runs on**. Every machine that runs walgit is a disposable cache; the bucket is the repository.

```sh
# 1. a bucket (any S3-compatible store, GCS, or an Azure Blob container) and a config
cat > walgit.toml <<'EOF'
[server]
listen = "0.0.0.0:8080"
public_url = "https://git.example.com"
auto_create_on_push = true
[server.auth]
mode = "token"
anonymous_read = false
tokens = [{ principal = "me", token_env = "WALGIT_TOKEN_ME", write = true }]
[store]
backend = "s3"
bucket = "my-walgit"
[store.s3]
endpoint = "https://s3.us-east-1.amazonaws.com"
region = "us-east-1"
EOF

# 2. run it
WALGIT_TOKEN_ME=$(openssl rand -hex 24) walgit serve --config walgit.toml

# 3. use it — a push to a new name creates the repository
git -c http.extraHeader="Authorization: Bearer $WALGIT_TOKEN_ME" push https://git.example.com/acme/app.git main
```

That is the whole deployment. Add more machines pointed at the same bucket and they serve the same repositories,
consistently, with nothing to coordinate. Kill them all and you lose warmth, nothing else.

It is a Rust implementation of the architecture Cursor described in
[*Git at any scale*](https://cursor.com/blog/git-at-any-scale) (the system they call Continuity), with the changes
needed to run it on machines that are smaller than the repository. The post is worth reading first; it is kept
verbatim in `docs/reference/cursor-git-at-any-scale.md`.

---

## Why this shape

Git is distributed, and that makes hosting it miserable for one reason: **packfiles**. Everything in a repository
is compressed into large binary packs laid out to be small, not to be read in order; every git operation is a
random walk over gigabytes. That is fine on a laptop with the file in page cache and catastrophic over a network
filesystem, which is why "just put the repositories on NFS" failed at every large host that tried it. The design
that survived (GitHub's Spokes) keeps real repositories on local NVMe so upstream `git` does the work, and
replicates at the packfile level with strict consistency — paid for with three-phase commit across a fixed replica
set, a database that maps every repository to its machines, and a fleet of pets.

Continuity's insight changes the economics: **make a write-ahead log in object storage the source of truth, and
make every on-disk repository a cache.** A push is stored as an immutable object in the bucket and becomes visible
only when a tiny manifest is rewritten with a compare-and-swap. That CAS *is* the consensus — no election, no
quorum, no primary. Any instance may accept a push; two racing instances cannot both win. A replica that has never
seen a repository reads the log and has it. Reads are consistent without coordination because every read first
asks the store whether anything changed (a conditional GET, usually a 304). Compaction is done once by whoever holds
a lease and published *into the log*, so replicas download compacted packs instead of repacking. And because the
WAL is the truth, there is complete provenance: every push and every repack, replayable to any point.

walgit takes that as-is, and adds what a *monorepo on small machines* needs: serving refs and web pages for a
repository whose packs will never fit on the instance (a **remote reader** over HTTP range requests), keeping
commits and trees local while blobs stay in the bucket (the **history pack**), and moving clone bytes out of the
server entirely (**bundle-uri**: fresh clones and catch-ups are static files the bucket or a CDN hands out).

## What it does

| | |
|---|---|
| **git** | smart HTTP v0/v2: `ls-refs` with prefixes, fetch with filter/shallow/deepen/sideband-all, receive-pack (atomic, deletes, tags, push options, report-status-v2), `<owner>/<repo>` namespaces, sha1 and sha256 repositories. Upstream `git` does upload-pack/repack/bundle; walgit does receive-pack, the WAL and the plumbing. |
| **bundle-uri** | Bundles cut on calendar slots (weekly full, chained dailies, hourlies) as a pure function of the WAL: a fresh clone downloads the newest full plus the chain above it from the bucket and asks the server only for the remainder; a catch-up downloads exactly the slots it missed. Two lists per repo: `bundles/list` for clones, `bundles/catchup` for fetches. Blobless families for `--filter=blob:none`. |
| **LFS** | Batch API + basic transfer, objects in the bucket, optional read-through from an upstream LFS server for imported repositories. |
| **web UI + API** | A React UI (tree, blob, commits, diffs, the WAL's own health page) on a read-mostly JSON API under `/{owner}/{repo}/api/*`; sha-addressed answers are immutable and cached everywhere; long answers stream progress as SSE. `repos.js` is a dependency-free SDK for pages, agents and scripts. |
| **policy** | Per-repository push rules (`policy.json`): protected refs, groups, fast-forward only, bypass lists. `docs/POLICY.md`. |
| **settings** | Per-repository config (bundle schedules, compaction, upstream follow) published into the WAL with history. |
| **events** | A small bridge tails the WAL and POSTs ref events to a webhook, exactly-once per (repo, seq, ref) with a durable cursor. `docs/EVENTS.md`. |
| **maintenance** | Checkpoints, bundle builds, geometric compaction, base rebuilds, connectivity audits and repairs — one loop that computes the desired state from (config, WAL) every pass and does one bounded unit of the most important missing work. Self-healing by construction: an outage leaves no holes; a deleted artefact is "missing" and rebuilt identically. |
| **auth** | `none` (loopback), `token` (static tokens), `oidc` (any OpenID Connect issuer: browser sign-in, ID tokens, and walgit-issued access tokens for git). `/services/public/install.sh` sets a developer's machine up in one idempotent command. |
| **stores** | S3 and S3-compatible (AWS, MinIO, rustfs, R2, Ceph, …) and GCS, first class; Azure Blob (Entra credentials, user-delegation SAS URLs); an in-memory store for tests. |

## How it works, briefly

**The repository is a WAL in the bucket.** Under `repos/<owner>/<repo>/`: `manifest.pb` (tiny, CAS-rewritten:
head sequence, the live pack set, checkpoint pointer, settings — *the linearization point*), `log/<seq>.pb`
(immutable entries: PUSH, COMPACT, CHECKPOINT, SETTINGS), `wal/<checksum>.pack|.idx|.rev|.bitmap|.commit-graph`
(immutable, content-addressed packs with their side-files), `checkpoints/<seq>/` (folded ref snapshot + pack
inventory so a cold start is snapshot + tail), `bundles/`, `leases/` (CAS with TTL — the only cross-instance
mutex), `policy.json`, `lfs/objects/`, `events/cursor.json`.

**A push**: our receive-pack indexes the pack (`git index-pack --fix-thin --rev-index` in a scratch dir), checks
connectivity and policy, uploads `pack ∥ idx ∥ log entry`, then CASes the manifest. On a 412 it re-reads,
re-validates every ref's old value and retries. Concurrent pushes to one repository on one instance are group
committed into one CAS. The client sees `ok` only after the bucket does.

**A read**: one conditional GET of the manifest; 304 → serve from the local copy, 200 → apply the new entries.
What "apply" means depends on what the request needs: **refs** (snapshot + log → `packed-refs`, no packs:
advertisements, the API, bundle lists), **serve** (the pack set *as this machine can hold it*: small packs and the
history pack local, a too-large base read by range), **full** (everything local, for repacks), **objects** (the
remote reader, for the UI on a repository that does not fit). Pack downloads run on their own runtime and never
block a refs request.

**Placement is configuration.** `[placement] serve / maintain` globs say which repositories a host does object
work for; refs-level reads work everywhere. One box: leave the defaults. Several: put the monorepo on the host with
the SSD (`cache.mode = "disk"`), everything else on the small ones, and route by `/<owner>/<repo>` in front.

**Nothing waits silently.** Anything slow is a *task* with an id, a log and a progress stream — narrated to git on
sideband 2 (`remote: * …`) and to the browser as SSE.

`AGENTS.md` is the full architecture and operating manual: constraints, the WAL strategies, every design decision
with its reasoning, the invariants, and the cost model (round trips to the bucket are the budget).

## Running it

```sh
# build (needs rust per rust-toolchain.toml, protoc, node 24 + pnpm for the web UI)
just web-build && cargo build --release -p walgit-cli
# or: nix build .#walgit        or: podman build -t walgit -f Containerfile .

# one box, TLS by walgit itself, a local S3 store (rustfs in a container)
just dev-store
./target/release/walgit-server --config walgit.standalone.toml
open https://walgit.localhost:8080/
```

* `walgit.standalone.toml` — the one-machine shape (self-signed TLS, rustfs, every role). Start here.
* `walgit.example.toml` — every key with its default and a comment.
* `Containerfile`, `flake.nix` — an OCI image and a Nix package/devshell.
* `deploy/nginx.conf.example` — an optional nginx in front: public TLS, one `auth_request` per credential, and
  **byte offload**: walgit answers bundle/LFS downloads with `X-Accel-Redirect` and nginx streams + caches the
  object from the bucket itself (S3 presigned or GCS with walgit's bearer). The file documents the contract.

Roles (`server.roles`): `serve` (git, API, UI, bundles, LFS), `maintain` (checkpoints, bundles, compaction,
fsck/repair), `events` (the webhook bridge). Empty = all. Any number of `serve` hosts may point at one bucket; give
each repository one maintainer (placement globs) and you are done.

### Authentication

| mode | who gets in | how git authenticates |
|---|---|---|
| `none` | everyone is `anon` with write — loopback experiments | nothing |
| `token` | static `tokens` in the config (`token_env` reads the secret from the environment) | `Authorization: Bearer <token>`, or the token as an HTTP Basic password |
| `oidc` | any OpenID Connect issuer (`issuer`, `oauth_client_id/secret`, `allowed_domains`/`allowed_emails`): Google, Entra, Okta, Auth0, Keycloak, Dex, GitLab… | a **walgit access token**: sign in once in the browser, create one at `/_auth/tokens`, paste it into the installer. Stateless (HMAC with `session_secret`, `access_token_ttl`); rotating the secret revokes all. ID tokens from the issuer (`audiences`) and static `tokens` work too. |

Developer setup is one idempotent command — `sh -c "$(curl -fsSL 'https://git.example.com/services/public/install.sh')"` —
which stores the token in a file only the user can read, installs a tiny git credential helper (git ≥ 2.46: it
answers `get` with `authtype=Bearer`, and on a real 401 `erase`s the token and says where a new one comes from),
and turns on `transfer.bundleURI`. `?repo=owner/name` clones right after.

### Developing

```sh
just test          # fast hermetic tier (< 1 min): unit + quick integration, in-memory store, real git
just e2e           # real git against the server (~20 s)
just warnings      # zero rustc warnings across all targets
just ci            # all of the above
cargo test -p walgit-server --test sim     # fault-injection simulation (crashes, partitions, stale reads)
just test-s3       # store contract against local rustfs
```

Code map:

```
crates/
  walgit-proto    protobuf schema (wal.proto), log framing, store keys
  walgit-store    ObjectStore trait (CAS versions, conditional GET, range, compose); backends s3, gcs, azure, memory; leases
  walgit-git      bare repos on disk, receive-pack, pack ingest, refs ↔ packed-refs, advertisements, upload-pack drivers
  walgit-wal      RepoHandle: sync levels, publish (group commit + CAS), checkpoints, log reader, remote reader, tasks
  walgit-bundle   bundle-uri: slots and chains, building, header ∘ pack composition, lists, retention
  walgit-server   axum: smart HTTP, LFS, bundles, auth (none/token/oidc), the maintainer loop, upstream follow,
                  web/ (API, UI, SDK routes, SSE), setup.rs (installer + recipes), events bridge
  walgit-config   walgit.toml (+ WALGIT__ env overrides), per-repo settings merge, fail-closed validation
  walgit-cli      `walgit serve|import|compact|bundle|wal|mirror|synth|config|repo`; `walgit-server` = `walgit serve`
web/              React SPA (Vite) + sdk/repos.ts, built into the binary; the wire contract is web/API.md
docs/             BUNDLE_URI_DESIGN, ROUNDTRIPS (the cost model), POLICY, LFS, INTEGRITY, EVENTS, CONTRACT, patches/
```

## Invariants worth memorising

* The manifest CAS is the only commit point; everything before it is invisible, everything after it is
  idempotent and replayable.
* Immutable objects are content-addressed; nothing is overwritten except the manifest, the bundle list and leases.
* Every read revalidates against the bucket first; there is no "eventually".
* Local disk is a cache. Memory is a cache. The bucket is the repository.
* Placement is configured, never inferred; refs-level reads work everywhere, object work only where placed.
* The maintainer's output is a pure function of (config, WAL); missing is just "not built yet".
* Cost must not scale with ref count on any hot path, nor with pack size on a machine too small for the pack.
* Long work is a task: discoverable, attachable, narrated.
* Correct is not sufficient: every protocol change is judged on round trips to the bucket (`docs/ROUNDTRIPS.md`).

## License

MIT — see `LICENSE`.
