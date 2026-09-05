# The API: repository-prefix lanes, one SDK

Context: **the wire contract of the JSON API + SDK** (D15/D20/D26/D27), normative, for UI/SDK/site authors and for
anyone changing `crates/walgit-server/src/web/*`. Caching rules (§2a), SSE envelope (§2b), tasks (§2c) apply to
every endpoint.

The programmatic surface of walgit (D20, the *Bring Your Own Tool*
shape): everything the bundled UI, another site, an agent or a script needs.
A Git host that implements this surface (JSON over HTTP plus the hosting rule
of §1) can serve the built UI unchanged; nothing in `web/src` depends on
walgit internals except the optional `overview`/`ops`/`tasks` endpoints.

```
https://git.example.com/{owner}/{repo}/api/…           bearer lane   a bearer token or the same-origin session cookie
https://git.example.com/{owner}/{repo}/api-browser/…   browser lane  credentials: "include" (other origins; the cross-origin cookie lane)
https://git.example.com/api/v1                         non-repo      discovery, me, authenticate, owners (+ /api-browser/v1/me|authenticate for the browser lane)
https://git.example.com/repos.js                       the SDK       window.repos — permanent URL on every host
```
Lanes differ by **credential handling** and CORS (D27): the bearer is a walgit access token (`/_auth/tokens`), a
static token, or an ID token from the OIDC issuer (`GET /services/setup.json` returns the setup commands). Browser
calls carry the session cookie (`walgit_session`, minted by `/_auth/login` → the issuer → `/_auth/callback`) with
`credentials: "include"`. No lane-first forms, no aliases.

Reference implementation: `crates/walgit-server/src/web/{v1,api,ui}.rs`.
Client: **`web/sdk/repos.ts`** (the SDK; the SPA's `web/src/api.ts` is a thin
adapter over it — the dogfood rule) and `web/src/use-resolved.ts` (the
resolve-then-fetch-by-sha flow of §2a).

## 0. Lanes and auth (D27)

| Lane | Path | Who | Credentials |
|---|---|---|---|
| **bearer / same-origin** | `/{owner}/{repo}/api/…` (+ `/api/v1/…` for non-repo) | git tooling, CI, agents, scripts, and the bundled same-origin UI | `Authorization: Bearer <token>`, or the same-origin session cookie |
| **browser** | `/{owner}/{repo}/api-browser/…` (+ `/api-browser/v1/me\|authenticate`) | other allowed origins loading `repos.js` | `fetch(…, {credentials: "include"})`; the session cookie from walgit's own sign-in (`/_auth/login`) |

**The repository prefix comes first on every lane** — it is the only routing key (D26: one edge rule sends
`/acme/monorepo*` to its host). Both lanes hit the same handlers; they differ by credential handling and CORS,
never by a rewrite. There are no other forms: the lane-first `/api/v1/repos/…`, `/api-browser/v1/repos/…` and the
pre-D15 `/services/api/{owner}/{repo}/…` are gone (prototyping phase — no aliases, AGENTS banner).
Every request authenticates (§1.3 of AGENTS.md); a missing/expired browser
session answers `401` and the SDK opens **`/api-browser/v1/authenticate`** in a popup —
walgit's own sign-in (`/_auth/login`) runs before the application page `postMessage`s
`{type: "repos:authenticated"}` to its opener and closes — then retries once.
`GET /api/v1/me` → `{principal, write, anonymous}` (`no-store`).

CORS: origins listed in `server.cors_origins` (exact, or one leading `*.`,
e.g. `https://*.docs.example.com`) get `Access-Control-Allow-Origin: <origin>`
+ `Allow-Credentials: true` + `Expose-Headers: ETag, …` + `Vary: Origin` on
`/{owner}/{repo}/api[-browser]/*` and `/api*`; preflights are answered without auth; a state-changing request from
any other origin is refused (403) before reaching a handler. No CORS headers
anywhere else. Empty list (default) = no cross-origin lane at all.

Versioning is app-owned: additive changes ship as they are; a breaking change of the repo surface gets a new
lane segment (e.g. `/{owner}/{repo}/api2/…`) without infrastructure work.

## 1. Hosting contract

| What | Rule |
|---|---|
| Asset base | Built assets are served under **`/_ui/`** (`vite.config.ts` `base: "/_ui/"`, content-hashed filenames, `public, max-age=31536000, immutable`, strong ETag, brotli/gzip precompressed; `index.html` is `no-cache` + ETag and carries the import map; see `web/README.md`). |
| Page routes | Every UI route below MUST return `web/dist/index.html` (`text/html; charset=utf-8`, `Cache-Control: no-cache`) so deep links and reloads work: `/`, `/{owner}`, `/{owner}/{repo}`, `/{owner}/{repo}/tree/*`, `/{owner}/{repo}/blob/*`, `/{owner}/{repo}/commits`, `/{owner}/{repo}/commits/*`, `/{owner}/{repo}/commit/*`, `/{owner}/{repo}/wal`, `/{owner}/{repo}/settings`, and `/api` (the "API" docs page in the UI; `?repo=owner/name` pre-fills its examples; `/api/v1` itself is the JSON discovery document). |
| API base | **`/{owner}/{repo}/api`** (bearer / same-origin cookie) and **`/{owner}/{repo}/api-browser`** (cross-origin browser), D15/D26/D27: one path prefix per repository, so the edge routes e.g. `acme/monorepo` to its host. Non-repo: `/api/v1` (discovery, `me`, `authenticate`, `owners`), `/services/api/owners*`, `/services/api/instance`. The bundled UI fetches same-origin with the session cookie and `Accept: application/json, text/event-stream`. No aliases or application sessions. |
| Public lane | **`/services/public/*`** — the one open prefix besides health and the SDK (nginx skips `auth_request` there; the app never authenticates here and never reads repo data). Today exactly `/services/public/install.sh[?repo=owner/name]` (`text/x-shellscript`, `Cache-Control: public, max-age=300`); plus `/services/public/ca.pem` (the certificate this process presents when it terminates TLS itself, `application/x-pem-file`, D39; 404 behind an edge); anything else under it is 404. |
| Setup | `/services/setup.json[?repo=owner/name]` — the clone/setup recipes (`setup::Recipes`: `token_url`, `install`, `install_url`, `plain_clone`, `blobless_clone`, `bundle_list`, `manual_clone`, `setup_text`, and `ca_url` + `trust` when the host terminates self-signed TLS itself — D39), `no-cache`; `/services/public/install.sh[?repo=]` — the one-time installer (open lane, AGENTS §1.3) (POSIX sh). The Clone menu and the API page render these, never their own copies. |
| Auth | `/_auth/login?next=`, `/_auth/callback`, `/_auth/logout`, `/_auth/me`, `/_auth/check` (an `auth_request` target for an edge), `/_auth/tokens` (GET: the token page; POST: mint a walgit access token for the signed-in principal, `{token, principal, write, expires_at}`, same-origin only) — in-app OIDC sign-in + session cookie. Off until `session_secret` + the OAuth client are set. |
| SDK | `/repos.js` (IIFE, registers `window.repos`) and `/repos.mjs` (ESM), built from `web/sdk/repos.ts` into `web/dist/` by `pnpm run build`; `no-cache` + strong ETag, precompressed. These data-free routes are open at the application. |
| Dev | `vite dev` proxies `/api/`, `/api-browser/`, `/services/api/` and `/{owner}/{repo}/api[-browser]/…` (plus git/bundle paths) to `$WALGIT_URL` (default `http://127.0.0.1:8080`). |

Path segments in URLs are `encodeURIComponent`-encoded per segment by the
client (`enc()` in `api.ts`); `/` between segments is literal. Servers must
decode each segment.

## 2. Conventions

- **Success**: `200`, `Content-Type: application/json`, plus the cache
  headers in §2a. The UI relies on the browser HTTP cache — it does not
  cache in JS — so the headers are the contract. The server must still
  return a **consistent, up-to-date** view on every revalidation (walgit
  reconciles the warm repo against the WAL before every read; another host
  must offer the same "read your own push" guarantee or the UI will show
  stale refs after a push).
- **Errors**: non-2xx with a **plain-text body** (the message is shown
  verbatim in the UI's error box). `404` for unknown owner/repo/ref/path/
  sha (walgit maps git's "Not a valid object name", "not a tree object",
  "unknown revision", "bad revision", "does not exist" to 404); `5xx` for
  server faults. No JSON error envelope.
- **Null safety**: every array field MUST be `[]` when empty, never `null`
  (the UI calls `.length`/`.map` directly).
- **Timestamps**: RFC 3339 strings (`%aI`/`%cI`), e.g.
  `2026-08-19T11:55:42-04:00`.
- **SHAs**: full 40-hex (SHA-1) strings. The UI abbreviates for display and
  uses full SHAs in links.
- **Sizes**: bytes, integers.

### 2a. Caching — the central design rule

Two classes of URL, and the client is built so that almost every request
falls in the first:

| Class | URLs | Headers |
|---|---|---|
| **Sha-addressed, immutable** | `tree/{sha}/…`, `blob/{sha}/…`, `commits?ref={sha}`, `commit/{sha}` where `{sha}` is a full 40-hex sha | `Cache-Control: private, max-age=31536000, immutable`. Content can never change; browsers never re-ask. |
| **Ref-dependent** | `owners`, `owners/{o}`, `refs`, `refs/branches`, `refs/tags`, `resolve/…`, and any tree/blob/commits/commit addressed by a *name* | `Cache-Control: private, max-age=0, stale-while-revalidate=60` and, where there is a natural one, `ETag: "<resolved sha>"` with `If-None-Match` → `304`. |

Flow per navigation: one ref-dependent call (`resolve`, SWR — paints
instantly from cache, revalidates in the background; a `304` costs the
server only the freshness check it would do anyway), then one
sha-addressed call (immutable — usually a cache hit on revisits, back
button, ref switch that lands on the same commit). `refs` is fetched once
per repository visit.

**Guidance for implementers** (this is where a monorepo-scale host wins or
loses):

- Make `resolve`, `refs` and the ref lists fast under *any* ref count: O(k)
  exact lookups for resolve (k = path segments), O(1) for `refs`, bounded
  output for the lists. Never "load all refs, then filter" per request.
- Keep an in-process **LRU of resolved ref → sha** (and of ref-list pages
  keyed by `(repo, kind, prefix, q, after, n)`) invalidated by the repo's
  ref-state version (walgit: the WAL manifest version; elsewhere: the refs
  ETag / packed-refs mtime / reftable generation). Serve `304` straight from
  that cache when `If-None-Match` matches.
- Honour **stale-while-revalidate** server-side too: if you front the API
  with a cache (CDN, nginx, Varnish) let it serve stale ref-dependent
  responses while one request refreshes; the UI is designed to tolerate
  ≤60 s-old ref data because the second, sha-addressed step is always exact.
- Sha-addressed responses are safe to cache **anywhere, forever** (shared
  caches included, if your auth model allows; walgit says `private` because
  a cloud IAM proxy sits in front). An LRU of rendered tree/commit JSON keyed
  by `(sha, path)` turns repeat views into memory reads.
- Use `private` unless every caller is allowed to see every repo; then
  `public` lets a shared cache absorb the load.

### 2b. Long answers — the SSE envelope

Some answers need work that takes seconds or minutes on a cold instance:
downloading a repo's packs (materialize), downloading pack **indexes** for a
repo too large to materialize (objects are then read from the store by
range), reading objects remotely, running a maintenance task. The UI must
never stare at a silent spinner, so every JSON endpoint accepts

```
Accept: application/json, text/event-stream
```

and answers in one of two ways, chosen by the server:

* **Plain JSON** (with the §2a cache headers) whenever it can answer
  immediately — cached render, packs on disk. The browser HTTP cache keeps
  working; this is the normal case.
* **`text/event-stream`** otherwise: a sequence of packets, then exactly one
  terminal packet. Standard packet names (shared by every streaming endpoint,
  including `…/tasks/{id}` and `POST …/ops/{op}`):

| `event:` | `data:` (JSON) | Meaning |
|---|---|---|
| `notice` | `{"text": "Downloading 1 pack index (2.1 GB)"}` | What the server is doing now. Show it. |
| `progress` | `{"label","done","total"?,"unit","percent"?}` | A bar; `total` absent = indeterminate. The latest packet per `label` wins. |
| `task` | `{TaskRecord}` (see §4 Tasks) | A background task this request depends on started/changed/finished. |
| `result` | *exactly the JSON body the plain endpoint returns* | Terminal. |
| `error` | `{"status": 503, "message": "…"}` | Terminal. Same text the plain endpoint would send. |

Comment lines (`: keepalive`) may appear at any time. A client treats the
stream as: render notices/progress live, resolve with `result`, reject with
`error`; if the stream ends without either, that is an error too. Streamed
results are not HTTP-cacheable, but the server keeps them in its immutable
render cache (and, for remotely served repos, in the object store), so the
*next* request for the same sha is plain JSON.

`…/blob/…?raw` is a page navigation and never streams.

### 2c. Tasks

Anything long-running is a **task** with a unique id, discoverable per repo:

* `GET …/tasks` → `{"hostname","running":[TaskRecord],"recent":[TaskRecord]}`
  (`Cache-Control: no-store`). `running` is what is happening to this repo on
  the instance that answered; the UI shows them as a pill in the repo header
  (spinner + job name + percent, click for the full list) and polls while any
  is running. Because routing is random, a task that vanishes from `running`
  counts as finished only when the same instance answered or `recent` lists it
  with a result — not when a different instance answered.
* `GET …/tasks/{id}` with `Accept: text/event-stream` → attach: `task` packet,
  replay of the packets so far, then live, then `result`
  (`{"task": TaskRecord, "value": …}`) or `error`. Without SSE accept → the
  `TaskRecord` as JSON. `404` if unknown here (tasks are per instance; the
  record says `hostname`).
* `POST …/ops/{op}?params` (write permission) → starts a maintenance task and
  returns the same attach stream; if the same `(repo, kind)` is already
  running, the response attaches to it instead of starting a second one.

`TaskRecord`: `{id, kind, repo, hostname, started, finished?, elapsed_ms,
ok?: bool, summary, progress?: {label,done,total?,unit,percent?},
log_tail: [string], params?: {…}}`; `ok` absent = running. Kinds today:
`materialize`, `remote-index`, `fsck`, `compact`, `bundle`, `checkpoint`,
`sync`, `rematerialize`.

## 3. Ref + path resolution (`{rest...}` routes)

Page URLs are GitHub-shaped: `/{ref}/{path}` with no delimiter, so
`feature/x/src/main.go` is ambiguous. The **server** resolves it (the client
never has the ref list) via `GET …/resolve/{rest}`:

1. For `rest = s1/s2/…/sk`, the candidates are the k prefixes `s1`,
   `s1/s2`, … as branch names and as tag names — **2k exact ref lookups**
   (walgit: one `for-each-ref` with those 2k exact patterns; a packed-refs or
   reftable host does k binary searches). Do **not** scan all refs.
2. The **longest** candidate that exists wins; on equal length a branch
   beats a tag. The remainder (leading `/` trimmed) is the path.
3. No match → the **first segment** must be a revision git can resolve to a
   commit (full/abbreviated sha; `HEAD`); `kind: "commit"`. Otherwise `404`.
4. Empty `rest` → the default branch (`refs.head`); `404` if unborn.

Tags resolve to the **peeled** commit. Tree/blob/commits endpoints also
accept a name in `{ref}` and apply the same rule server-side, but the UI
always sends the sha it got from `resolve` so those requests are immutable.
Responses echo `ref`, `sha` and `path` so the client never re-splits.

## 4. Endpoints

### `GET /api/v1/owners`

Top-level namespaces.

```json
["demo", "jane"]
```

Sorted, `[]` if none. Must come from the authoritative repo list (walgit
lists the object store, not local disk, so a cold node sees everything).
Cache: SWR (§2a).

### `GET /api/v1/owners/{owner}/repos`

Repositories under one owner, short names only.

```json
["hello", "walgit"]
```

Sorted, `[]` for an unknown/empty owner (200, not 404). Cache: SWR.

### `GET /api/v1/me`

```json
{ "principal": "jane@example.com", "write": true, "anonymous": false }
```

`401` without credentials. `Cache-Control: no-store`.

### `GET /{owner}/{repo}/api`

```json
{ "owner": "acme", "name": "monorepo", "full_name": "acme/monorepo",
  "head": { "name": "main", "sha": "807d45a6…" }, "branches": 12, "tags": 3,
  "clone_url": "https://git.example.com/acme/monorepo.git",
  "html_url": "https://git.example.com/acme/monorepo",
  "api_url": "https://git.example.com/acme/monorepo/api" }
```

Ref-level summary: head (`null` when unborn), O(1) ref counts from the ref
index, URLs. `404` for an unknown repo. Cache: SWR + `ETag: "<head sha>"`.
`PUT` creates the repository (write permission; `201`/`200`), `DELETE`
removes it (admin permission) — the same handlers as `PUT|DELETE /{owner}/{repo}`.
`GET|PUT|DELETE …/policy` is the push policy document (`docs/POLICY.md`).

`GET|PUT|DELETE /{o}/{r}/api/settings` (D24, 2026-08-21) is the repository's **settings in the WAL**: a TOML document
restricted to `[bundles]`, `[maintenance]`, `[compaction]` and `[upstream]`, merged over the
host's config (`effective config`).
`GET` → `{revision, author, updated_at, message, toml}` (`revision: 0` = none). `PUT` body = the TOML
(`?message=` optional), validated against the serving host's build — 400 with the reason and nothing published
on failure; 200 `{revision}`. `DELETE` publishes an empty document. `GET …/settings/effective` → the effective
`[bundles]`/`[maintenance]`/`[compaction]`/`[upstream]` as TOML (`application/toml`; no host secrets,
no `token_env`); `GET …/settings/history` → `{min_seq, entries:[{seq,revision,author,message,
at,toml}]}` from the live log (older changes are folded into checkpoints). All `no-store`; PUT/DELETE need
**admin** (`tokens[].admin` or oidc `admin_emails`/`admin_domains`; `mode = none` is admin on loopback).
Every instance sees a new revision on its next refs-level sync (no extra round trip: the document rides
inline on `manifest.pb`). CLI: `walgit repo settings show|set|clear|history`.

Settings tab helpers (`/{o}/{r}/api/settings…`, all `no-store`): `GET …/settings/describe` → `{settings, sections,
strategies:[{name,kind,base,schedule,schedule_human,next,keep,backfill_max,min_commits,refs}], bundles, maintenance:
{checkpoints,interval_secs,this_host:{name,serves,maintains,disk,max_pack_bytes,cache_budget_bytes,roles}}, compaction,
upstream:{git,lfs,token_env:bool,follow:[refs],follow_interval_secs,last_round:{at,outcome: in-sync|published|refused|failed,
detail,upstream:{ref:oid},ours:{ref:oid}}|null} (D33; last_round = this instance's last follow round),
fields:[{key,value,host_value,source: host|setting}], head_seq}`; `POST …/settings/validate` (body TOML) → the same
shape for the *would-be* effective config with `ok: true`, or `{ok: false, errors[]}` (nothing published);
`POST …/policy/validate` (body policy JSON) → `{ok, errors[], rules, groups, protect}`; `POST …/policy/dry-run?last=N`
(body policy JSON, empty = the saved policy) → the policy evaluated against the last N PUSH entries of the live log
(`{pushes, allowed, denied, results:[{seq,at,principal,atomic,refs:[{name,ok,reason,force}]}]}`; force = non-ancestor
update when objects are local). SDK: `repo.settings.{get,put,delete,effective,history,describe,validate}`,
`repo.policy.{validate,dryRun}`.

### `GET /{owner}/{repo}/api/refs`

```json
{ "head": { "name": "main", "sha": "807d45a6…" } }
```

**O(1) by design**: only the default branch (`HEAD` symref target and the
commit it points at). `"head": null` when HEAD is unborn → the UI shows the
"Setup" empty-repo box. No branch/tag arrays here — a repo with a
million refs must answer this as fast as one with three. Fetched once per
repository visit. Cache: SWR + `ETag: "<head sha>"` → `304`.

### `GET /{owner}/{repo}/api/refs/{branches|tags}?prefix=&q=&after=&n=`

One **name-sorted page** of one namespace, for the branch/tag picker.

```json
{ "refs": [ { "name": "2-5-stable", "sha": "46b7fd29…" }, … ], "more": true }
```

- `prefix`: path prefix under the namespace (`refs/heads/<prefix>/`), lets a
  server use a pattern instead of a scan. `q`: case-insensitive substring
  filter on the short name (the picker sends this per keystroke,
  debounced). `after`: name cursor — return names strictly greater (byte
  order) than it. `n`: page size (walgit: default 100, max 1000).
- Sorted by name ascending (byte order — `git for-each-ref --sort=refname`);
  tag `sha` is the peeled commit. `more` = at least one further match
  exists. `[]` when none. Unknown namespace → `404`.
- **SSE option**: with `Accept: text/event-stream` the same page streams as
  `event: ref` / `data: {"name","sha"}` per ref, terminated by
  `event: done` / `data: {"more":bool}` (this predates §2b and keeps its own
  packet names), so a client can paint progressively while the server is
  still walking refs. The bundled UI's branch/tag picker uses this SSE form
  (painting matches as they arrive, aborting on the next keystroke); walgit
  writes each event as soon as it is found (no buffering, `X-Accel-Buffering:
  no`, never compressed). Either way the server must stop walking after `n`
  matches.
- Cache: SWR (same ETag rule as `refs` if you can produce one cheaply; walgit
  sends SWR without ETag here).
- Implementers: cache pages in an LRU keyed by the full query + the repo's
  ref-state version; the picker's typical traffic is the same handful of
  queries repeated.

### `GET /{owner}/{repo}/api/resolve/{rest...}`

```json
{ "ref": "feature/x", "sha": "5ed435d9…", "path": "src/main.go", "kind": "branch" }
```

The ref/path split of §3 plus dereference. `kind` is `branch | tag |
commit`. `404` (plain text) if nothing resolves. Cache: SWR + `ETag:
"<sha>"` → `304`. This is the one request the UI makes per navigation
that depends on ref state; make it cheap and cache it hard.

### `GET /{owner}/{repo}/api/tree/{ref}/{path}`

Directory listing (one round trip for the repo home).

```json
{
  "ref": "main",
  "sha": "807d45a6…",
  "path": "proto",
  "entries": [
    { "name": "pb", "type": "tree", "mode": "040000", "size": -1, "sha": "…" },
    { "name": "gitwal.proto", "type": "blob", "mode": "100644", "size": 4821, "sha": "…" }
  ],
  "commit": { …Commit… },
  "readme": { "name": "README.md", "contents": "# …" }
}
```

- `path` is `""` for the root. Trailing/leading slashes ignored.
- `entries`: sorted **directories first**, then by name (byte order).
  `type` is `blob | tree | commit` (submodule). `mode` is the git mode
  string. `size` is `-1` for trees/submodules. `[]` for an empty tree.
- `commit` (optional): the newest commit touching `path` on `ref`
  (`git log -1 ref -- path`); shown in the tree header. Omit if unknown.
- `readme` (optional): contents of the first entry named (case-insensitive)
  `README`, `README.md`, `README.markdown`, `README.txt`, `README.rst`, only
  if valid UTF-8. Rendered as Markdown under the listing.
- `404` if `ref` is unknown or `path` is not a tree (e.g. a blob path).
- `{ref}` may be a name (resolved per §3; response SWR + ETag) or a full sha
  (response **immutable**). The UI always sends the sha. `sha` in the
  response is the resolved commit.

### `GET /{owner}/{repo}/api/blob/{ref}/{path}[?raw]`

```json
{ "ref": "main", "sha": "807d45a6…", "path": "src/main.go", "name": "main.go", "size": 1234, "contents": "package main\n…" }
```

- Exactly one of: `contents` (UTF-8 text, no NUL bytes), `binary: true`,
  or `too_large: true` (walgit's limit is 2 MiB; the limit is the server's
  choice, the UI just shows "too large"/"binary" with `size`).
- `?raw`: when `contents` exists, respond `200 text/plain; charset=utf-8`
  with the bytes instead of JSON (the "Raw" link). Binary/too-large still
  return the JSON shape.
- Markdown (`.md`, `.markdown`) and README blobs get a Preview/Code toggle
  client-side; no server involvement.
- `404` if unknown ref or path. Cache: as tree (`{ref}` sha → immutable;
  name → SWR + ETag). `?raw` follows the same rule.

### `GET /{owner}/{repo}/api/commits?ref=&path=&skip=&n=`

Linear history page.

```json
{ "ref": "main", "sha": "807d45a6…", "commits": [ …Commit… ], "more": true }
```

- `ref`: a full sha (what the UI sends; response **immutable**) or a
  branch/tag/revision name (resolved per §3; response SWR + ETag); default
  `HEAD`. `path`: optional, limits to commits
  touching it (`git log ref -- path`); `""`/absent = whole history.
- `skip` (default 0) and `n` (default 35, server may cap; walgit caps at
  200). `more` is true when at least one more commit exists after this page
  (walgit asks for `n+1`). Pagination in the UI is "Older" → `skip +=
  commits.length`.
- Order: `git log` default (reverse chronological, topo where needed).
- `commits` is `[]` (never null) for an empty result; unknown ref → 404.

`Commit`:

```json
{
  "sha": "33419c76…",
  "parents": ["f6ef950…"],
  "author": "Jane Doe", "author_email": "jane@…", "author_date": "2026-08-19T11:55:42-04:00",
  "committer": "Jane Doe", "commit_date": "2026-08-19T11:55:42-04:00",
  "subject": "first line",
  "body": "rest of the message WITHOUT the trailer block, trailing newlines trimmed, \"\" if none",
  "trailers": [ { "key": "Merge-Queue-Pull-Request", "value": "42" }, { "key": "Co-authored-by", "value": "Jane <jane@…>" } ]
}
```

`parents` is `[]` for a root commit. `trailers` are the git trailers of the message
(`git interpret-trailers --parse` rules: the `Key: value` lines of the last paragraph,
continuation lines folded, in order; `[]` when the last paragraph is prose) — `body` is
the message with that block removed. Repo-agnostic; the UI knows a small table of keys
(merge-queue CI / people) for grouping and linking. The UI groups the
list by `commit_date` day and shows `subject` + `author`.

### `GET /{owner}/{repo}/api/commit/{sha}`

```json
{
  "commit": { …Commit… },
  "stats": [ { "path": "server.go", "additions": 12, "deletions": 3 } ],
  "patch": "diff --git a/server.go b/server.go\n…"
}
```

- `{sha}` may be any revision git resolves to a commit (full/short sha,
  ref). `404` if not found. Full 40-hex sha → **immutable**; anything else
  → SWR + `ETag: "<full sha>"`.
- `stats`: `git show --numstat` order; `additions == deletions == -1` for
  binary files. Renamed files appear once (`-M`) with the new path. `[]`
  for an empty commit.
- `patch`: the **unified diff** of the commit against its first parent
  (against the empty tree for root commits: `--root`), `--no-color`,
  `--no-ext-diff`, rename detection on, default context. Parsed
  client-side by `@pierre/diffs` `parsePatchFiles(patch, sha)` and rendered
  unified/split; the `diff --git a/… b/…` headers must be present so file
  boundaries and names are detected. The UI anchors files by `stats[].path`
  matching the parsed file name, so use the same paths in both.
- Merge commits: diff against the **first parent** (GitHub semantics;
  walgit passes `--diff-merges=first-parent`). Do not return a combined
  (`diff --cc`) patch or an empty patch with non-empty `stats`.

### `GET /{owner}/{repo}/api/overview` — optional, walgit-specific

Backs the "WAL" tab. Not needed by Code/Commits pages; a host without a
WAL should return `404` (the tab then shows the error text). Shape is in
`api.ts#Overview` / `struct Overview` in `crates/walgit-server/src/web/ui.rs`: `repo`, `clone_url`, `hostname`,
`health{status: ok|degraded|error, issues[], deep, suggestions[{op, params?, reason, auto?}]}` — `deep` is the
last connectivity audit as recorded in the store (`fsck.pb`, any maintainer), `auto` says how/when the
maintainer loop performs a suggestion by itself (absent = a human must) — `manifest{version,
next_seq, min_seq, segments[], tail_entries, entries, checkpoint?,
packset?, advertised_bundle_uri?, last_push?}`, `local{version, next_seq,
bootstrap, reconciled, size_bytes}`, `packs{live, live_bytes, pushes}`,
`bundles[{sha, size, at_seq, created, uri, strategy, kind, base_id, creation_token, filter, tips}]` (the chain:
`base_id` is the bundle whose tips are this one's prerequisites, `""` for a full), `bundle_plan{slots[{strategy,
kind, slot, status: built|missing|pending|blocked|unavailable|too-small|skipped|wrong-host, detail, bundle_id}],
upcoming[], maintainers[{host, disk, max_pack_bytes, last_pass_age_secs, alive, passes, last_unit}], orphaned}`,
`compactions[]`, `node{…counters}`. Arrays `[]` when empty.

### Service routes (not for the browser)

`POST /_events/notify` is the events bridge's wake-up (`docs/EVENTS.md`): the
Pub/Sub push envelope of a GCS notification; ID-token authenticated
(`require_read`), `404` on instances without the `events` role, `200` (+ a
JSON report for a manifest finalize), `503` when the sink failed (so Pub/Sub
redelivers). Never cached, never served to the SPA.

## 5. Consistency and cost expectations

- **Every screen is O(1) requests** — exactly two: `resolve` (tiny, SWR +
  ETag, the only ref-dependent call) plus one **sha-addressed, immutable**
  endpoint — tree, blob, or commits page; commit detail is one immutable
  call. `refs` is one extra tiny call per repository visit; owner and repo
  lists are one request each. No per-row fetches: the tree response carries
  its latest commit and README, the commit response carries numstat and
  patch, the commits page carries its own `more` flag. The branch picker
  asks for one server-filtered page per keystroke, never the whole list.
- **Cost must not scale with ref count** anywhere on the hot path: `refs`
  is O(1), `resolve` is O(path segments), ref lists are O(page). The only
  O(refs) work allowed is a bounded scan inside one ref-list page, and
  even that should hide behind an LRU.
- A server should answer each endpoint in a handful of batched git
  invocations (tree = `ls-tree` + `log -1` + maybe `cat-file`; commit =
  `log -1` + `show`), never one process per entry — and should cache
  sha-addressed JSON in an LRU, since it can never go stale.
- Reads must be as fresh as a `git fetch` from the same host would be:
  after a push is acknowledged, the next API call (any node) reflects it.
- Writes on the JSON surface need a token with the matching permission:
  write for `PUT /{o}/{r}/api` (create) and `POST …/ops/{op}`, admin for
  `DELETE /{o}/{r}/api`, `PUT|DELETE …/policy` and `PUT|DELETE …/settings`
  (D24: write is push, not admin). Content moves over git
  (`git-receive-pack`) and LFS, never through JSON.

## 6. Minimal conformance checklist

```
GET /api/v1                                     → 200 {name, version:1, base, browser_base=/api-browser/v1, sdk, docs, auth, endpoints}
GET /api/v1/me                                  → 200 {principal,write,anonymous} | 401; no-store
GET /api/v1/owners                              → 200 [..]   ([] when empty)
GET /api/v1/owners/nobody/repos                 → 200 []
GET /o/r/api                                    → 200 {owner,name,full_name,head,branches,tags,clone_url,html_url,api_url}; SWR + ETag "<head sha>"
GET /o/r/api-browser/refs                           → same handlers (browser lane)
OPTIONS /api/v1/… (Origin ∈ cors_origins)       → 204 + Access-Control-Allow-{Origin,Credentials,Methods,Headers}
GET /api/v1/… (Origin ∉ cors_origins)           → 200, no CORS headers; DELETE/PUT/POST → 403
GET /repos.js                                   → 200 text/javascript; ETag; no-cache; unauthenticated
GET /o/r/api/refs                               → 200 {head:{name,sha}|null}; ETag; If-None-Match → 304   (browser lane: /o/r/api-browser/refs)
GET /o/r/api/refs/branches?n=3                  → 200 {refs:[3],more}; SWR
GET /o/r/api/refs/branches?q=stab&after=1-x&n=2 → filtered, after cursor
GET /o/r/api/refs/tags   (Accept: text/event-stream) → event: ref … event: done
GET /o/r/api/resolve/feature/x/dir              → 200 {ref:"feature/x",sha,path:"dir",kind:"branch"}; SWR+ETag
GET /o/r/api/resolve/nope/x                     → 404 text/plain
GET /o/r/api/tree/<sha>                         → 200 {ref,sha,path:"",entries,commit?,readme?}; immutable
GET /o/r/api/tree/main                          → 200 same shape; SWR + ETag "<sha>"
GET /o/r/api/tree/<sha>/README.md               → 404 (blob, not tree)
GET /o/r/api/blob/<sha>/README.md               → 200 {contents}; immutable
GET /o/r/api/blob/<sha>/README.md?raw           → 200 text/plain; immutable
GET /o/r/api/commits?ref=<sha>&path=&skip=0     → 200 {ref,sha,commits,more}; immutable
GET /o/r/api/commit/<sha>                       → 200 {commit,stats,patch}; immutable
GET /o/r/api/commit/<sha[:8]>                   → 200; SWR + ETag "<full sha>"
GET /o/r/api/commit/deadbeef                    → 404 text/plain
GET /o/r/tree/main/anything                              → 200 index.html
GET /o/r/settings                                        → 200 index.html  (JSON is /o/r/api/settings)
```
