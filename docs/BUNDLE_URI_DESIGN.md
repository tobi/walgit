# bundle-uri design — how walgit moves clone bytes to static files

Context: **design of record for bundle-uri** — the north star of `GOAL.md` (fast clone + fast catch-up through
static bundles). For the agent/human building or reviewing the bundle scheduler, the SSD maintainer's weekly
base rebuild, or the
clone/fetch experience of big repos. Normative where marked; `AGENTS.md §2.6` is the summary, D17/D21/D22 the
decisions. Technical detail for `crates/walgit-bundle`, SSD-backed maintainers, and manual
`walgit compact --base` recovery.

Status: **implemented design of record** (2026-08-21). Calendar slots, deterministic tokens, as-of-slot
content, backfill, two-newest retention, self-contained incrementals, and the blobless family are live.
Remaining work belongs in §8 and `AGENTS.md §6`. Reading order:
AGENTS.md §2.6 (summary) → this file (detail) → `crates/walgit-bundle/` (code) → `web/API.md` (what the UI shows).

## 1. Why bundles exist here

A small instance is 8 vCPU / 32 GiB with a memory-backed disk; `acme/monorepo`'s base pack alone is 32 GB.
The instance must never hold or stream that pack. So **the first fetch of history is never answered by
upload-pack**: the application hands the client URLs of static bundle files, which the edge serves from GCS or
its immutable-object cache without streaming bytes through an application worker. The client applies them and
only then asks upload-pack for the delta between the newest bundle and now. Static files are cacheable,
Range-capable, and CDN-able. This is the same mechanism git uses for `clone --bundle-uri`; we only decide
*what* to cut, *when*, and
*how the chain fits together*.

Everything below is about making that delta small **for the client that matters**: someone on `main` (or a
branch a few commits off main) who cloned days ago.

## 2. Git's side: what a bundle is and how `creationToken` behaves

A git bundle file = header + packfile.
- Header lists the **refs it contains** (`<sha> refs/heads/main`, `HEAD`) and **prerequisites** (`-<sha>`):
  commits the receiver must already have. A *full* bundle has no prerequisites; an *incremental* one says
  "given you have X, here are the objects from X to the tips".
- The server's **bundle list** (`bundles/list`, also the v2 `bundle-uri` command) is git-config text:
  ```
  [bundle]
      version = 1
      mode = all
      heuristic = creationToken
  [bundle "weekly-1787... "]
      uri = https://git.example.com/acme/monorepo/bundles/weekly/<file>.bundle
      creationToken = 1787211600
  ```
- `heuristic = creationToken` algorithm (git ≥ 2.38, what `transfer.bundleURI=true` / `fetch.bundleURI` run):
  1. Sort entries by token **descending**. Download the newest; if its prerequisites are missing, download the
     next older; repeat until one applies (or a full bundle is reached).
  2. Apply them **oldest → newest**.
  3. Record the highest token applied in `fetch.bundleCreationToken`.
  4. On later `git fetch`: download only entries with token **greater** than that, apply, then negotiate the
     rest with the server as usual (`want main, have <newest bundle tip>`).
- Consequences we design around:
  - Tokens must be **monotonic along the chain** and **deterministic** (a backfilled older bundle must get an
    older token than a newer one, even if it is *built* later).
  - Every incremental bundle's prerequisites must be exactly the tips of the bundle below it in the chain, or
    git walks further down (more downloads) — a broken link degrades to "download more", never to "wrong".
  - Git only downloads what it needs: cumulative daily/hourly bundles let a stale client stop at the newest
    applicable bundle whose prerequisites it already has, then fetch roughly the unbundled remainder.

## 3. What we cut — decisions

| Decision | Choice | Why |
|---|---|---|
| **Refs** | For `bundles.require`/big repos: `HEAD` + `refs/heads/main` only (`bundles.main_only`, override per strategy with `refs = [...]`, e.g. add `refs/tags/v*`). Small repos: `refs/heads/*`, `refs/tags/*`, `HEAD` (today's default). | Almost every client wants main + one branch. Branches are tiny per-fetch deltas *on top of* main (`want branch, have main@bundle`); after a rebase the branch's commits sit on newer main that the hourlies already delivered — rebased branches get **cheaper**, not slower. Putting 466 k refs or all branches in bundles would bloat every incremental and help nobody. |
| **Weekly (full)** | One per calendar week, **Sunday 23:00 UTC** slot. For a big repository: a **compose** of the tier-2 base pack + a generated header (GCS `compose` natively, S3 by multipart `UploadPartCopy`; no bytes through walgit, ~2 s for 32 GB on GCS). Token = slot epoch. | Full history once a week; composing from the base means the weekly costs nothing to build and is identical in content to what every instance already serves from. The base rebuild (VM job) happens *with* the weekly cut, never in between, so everything newer than the base is small packs that fit every instance (AGENTS §2.5 invariant). |
| **Daily (incremental)** | One per day, **23:00 UTC** slot; prerequisite = tips of the latest weekly ≤ slot; content = objects from that weekly's main tip to **main as of the slot time**. | Nightly is when CI is quiet and when "how stale is my clone" is measured in days. |
| **Hourly (incremental)** | One per hour, **:00** slot; prerequisite = tips of the latest daily ≤ slot; content = main from that daily to main as of the slot. | Keeps the server-side remainder to < 1 h of pushes. |
| **Content "as of the slot"** | The WAL is timestamped and sequenced: resolve the **highest seq with `created_at` ≤ slot time**; replay refs to that seq (refs-level, the `wal materialize --at-seq` machinery); cut tips from that state. | Makes **backfill correct**: a "Tuesday" bundle built on Thursday contains Tuesday's main, so the chain's prerequisites line up and tokens stay truthful. |
| **Token** | `creationToken = slot epoch seconds` (weekly: Sunday 23:00; daily: 23:00; hourly: :00). | Deterministic, monotonic, independent of when the maintainer happened to run. |
| **Retention** | `keep` newest weeklies (2); **dailies are chained** (`chain = true`, the operator 2026-08-22): every daily since the newest kept weekly, each cut on the previous daily (≤ 7); hourlies on the newest daily, the **2 newest** kept (D21 amended 2026-08-22). Never an orphan. The maintainer applies it every pass, not only on publish. | git downloads *every* listed bundle newer than the newest full on a clone, newest-first, until one whose prerequisites it has (43 downloads / 11.5 s on a 1 MB repo under the old unbounded rule). Chained dailies carry exactly their day: a fresh clone is 1 weekly + ≤ 7 dailies + 2 hourlies, a days-stale catch-up is exactly the days missed + ≤ 2 hourlies, no overlapping bytes (local rig: stale at daily #2 → dailies #4, #3 + 145 B upload-pack). Unchained hourlies keep an hourly catch-up at ≤ 2; the second one keeps a client that read the list a slot ago from a 404 (git never retries a bundle download). A `chain = true` strategy wants at most one base period of slots (`slots::chain_window`). |
| **Naming / storage** | `bundles/<strategy>/<slotUTC>-<sha1 of content>.bundle` (immutable, content-addressed, ETag = checksum), list at `bundles/list.pb` (CAS). Keys never overwritten. | Immutable → `Cache-Control: immutable`, Range, CDN; CAS'd list = atomic publish. |
| **Serving** | `serve_via = proxy` (streamed from the bucket through a serving host with the full static contract: ETag/304/If-Range/Range/HEAD) or `signed_url` (direct object-store URL). Signing may be unavailable or denied by the store; failure falls back to proxy per entry and never fails the listing. | Static contract either way; direct URLs remove the serving process from the byte path when the store permits signing. |
| **Two lists: clone and catch-up** (2026-08-22) | `bundles/list` is the clone list (fulls + chain); **`bundles/catchup`** is the same list **without the fulls**, and it is what every recipe records in `fetch.bundleURI`. Dailies chain *through* the weekly: Sunday's daily and the weekly fire at the same instant and have the same tips, so Monday's daily is cut on Sunday's daily (tie → own chain, `slots::base_for_incremental`), and retention keeps the chain under every kept full (`keep = 2` on the weekly = two weeks of catch-up through bundles). | git's creationToken walk goes newest-first and a full has no prerequisites, so a fetching client downloads **every full newer than its token** — the new weekly, 32 GB from a large repository, on the first fetch after Sunday (measured on the rig: round 1 of `rig/catchup`). A client with history never needs a full; with no fulls in its list and a chain that crosses the week, it walks daily → daily to a link whose prerequisites it has. Fresh clones still take the newest weekly (they have its objects, so Monday's prerequisites hold). e2e `fetch_after_the_recipe_clone_uses_the_bundles` covers the rollover. |
| **Advertising** | v2 capability `bundle-uri` + the `bundle-uri` command; static list at `/{o}/{r}.git/bundles/list`; `/services/public/install.sh` sets `transfer.bundleURI=true` + `fetch.bundleURI`. The narrated fetch echoes each advertised bundle: `* bundle-uri: /acme/monorepo/bundles/weekly/<file> (32.3 GB, full, seq 1, token …)`. | Users see where bytes come from. |
| **Forcing** | `bundles.require = ["acme/monorepo"]` (D17): an **unbounded zero-have** fetch (a full clone that skipped bundles) is refused with the exact fix; `--depth`/`--filter` zero-have fetches (CI) and all fetches with haves proceed. **One-shot fallback** (2026-08-21): a principal that fetched `bundles/list` within the hour *tried* bundle-uri — git does not retry a failed bundle download and then sends exactly this zero-have fetch — so it gets one upload-pack clone per 6 h with a loud band-2 warning; the next one and anyone who never tried are refused, truthfully. | Protects the instances from the one request they cannot serve; keeps CI's shallow/partial clones (the 2075 s benchmark shape) on upload-pack, where they take ~8 s. The fallback trades ≈ 32 GB of egress + minutes of the SSD host (deltas reused from the base: pack-objects is I/O-bound, not CPU-bound) for "`git clone` never fails"; the rate limit keeps a fleet of misconfigured clients from turning the SSD host into a 32 GB-per-clone server — the same request without the list fetch first is still refused. |

## 4. Scheduling: calendar slots with backfill

Each maintainer derives the slot plan from repository settings and WAL state on every pass:

```
for each strategy (weekly → daily → hourly, so bases exist first):
    slots = all slot times from the base's previous slot .. now
    wanted = fulls: every slot; incrementals: the 2 newest slots (older content is subsumed by the newer)
    missing = wanted slots with no bundle in the list
    build missing oldest-first (≤ backfill_max per pass; 1 for weekly)
    each build: seq = highest WAL seq with created_at ≤ slot
                tips = ref state at seq filtered by strategy refs
                prereqs = tips of the newest bundle of `base` with slot ≤ this slot
                token = slot epoch; key = bundles/<strategy>/<slot>-<sha>.bundle
    publish list (CAS) after each bundle, retention applied; retention also applied at the start of every pass
```
Runs on the repository's assigned maintainer (`roles=["serve","maintain"]`) every pass: the SSD host owns
`acme/monorepo`; the push broker owns the remaining assigned repositories. The first pass after downtime fills
the holes. `walgit bundle plan <repo>` prints the slot table and the WAL page shows it.

**Config standard** (`walgit.example.toml` is normative; `walgit config check` enforces it):

```toml
[bundles]
main_only  = true                 # default refs: HEAD + refs/heads/main (false: heads/* + tags/* + HEAD)
extra_refs = []                   # globs added to every bundle's default ref set, e.g. ["refs/tags/v*"]
require    = ["acme/monorepo"]       # D17: unbounded zero-have fetches refused with the fix text
[[bundles.strategy]]
name = "weekly"; kind = "full";        schedule = "0 0 23 * * Sun"; keep = 2; backfill_max = 1
[[bundles.strategy]]
name = "daily";  kind = "incremental"; base = "weekly"; schedule = "0 0 23 * * *"; backfill_max = 0
[[bundles.strategy]]
name = "hourly"; kind = "incremental"; base = "daily";  schedule = "0 0 * * * *";  backfill_max = 48
```
- `schedule`: 6-field cron, **UTC** (`@weekly/@daily/@hourly` = the defaults above). Each fire time is a
  **slot** = the as-of instant of the content; `creationToken` = slot epoch seconds.
- `kind = "full"` has no prerequisites; `incremental` needs `base` (prereqs = tips of the newest **base** bundle
  with slot ≤ this slot — a daily is built on the latest *weekly*, an hourly on the latest *daily*, never on the
  previous bundle of its own kind). So a client applies weekly + ≤ 1 daily + ≤ 1 hourly; intermediate bundles are
  independent of each other (a missing Tuesday daily does not affect Wednesday's). The price: a Saturday daily
  carries six days of objects, the 22:00 hourly carries 22 hours — still small next to the weekly. A new weekly
  restarts the daily chain; the latest daily the hourly chain.
  **Base resolution goes up the chain**: when no bundle of the base strategy exists at/before the slot (a
  repository's first day: weekly cut Sunday, first daily Monday 23:00), the incremental is cut on the nearest
  ancestor that has one — Monday's hourlies on the weekly (prerequisites = its tips, satisfiable by any client that
  has the weekly) — never blocked, and never "the base cut at this slot" (the old fallback would have produced a daily
  with an hourly's slot/token).
- `keep` = weeklies listed (fulls only). Incrementals have no knob: always the 2 newest whose base is kept
  (`INCREMENTALS_KEPT`); setting `keep` on one fails `Config::validate`.
- `backfill_max` = missing slots built per pass (0 = unlimited), oldest first — for incrementals at most the 2 newest
  slots are ever missing.
- **Selection order** (D22): every pass, per assigned repo, the single most important missing unit (a bundle slot that turns out to have nothing to cut — too small, no state as of the slot — is re-planned at once, up to 48 per pass, so stale slots never delay the current hour) — checkpoint-if-due
  → repair (objects the last fsck found missing, from `upstream.git`) → missing weekly → missing dailies (oldest first)
  → missing hourlies (oldest first) → compaction → fsck audit (`maintenance.fsck_interval`; `docs/INTEGRITY.md`).
  **Placement**: `[placement] maintain/maintain_exclude` (D30) + declared capacity; acme/monorepo is assigned to the big host only —
  on any other instance its units are `wrong-host`, never attempted.
- **Minimum-size gate**: `bundles.min_commits` (default 25; per-strategy override; `min_bytes` optional): an
  incremental slot with fewer commits since its base is plan state `too-small` and is not built — the next slot of
  the same strategy is built on the same base, so nothing is lost. Fulls are never gated. Measured by
  `git rev-list --count <tips> --not <base tips>` over the local commit-graph (commits/trees are local on every
  host), after the lease and before any pack work; the measurement is this maintainer's (in-memory) view, so
  `walgit bundle plan` / the WAL page show `too-small` on the host that measured it and `missing` elsewhere.
  **The verdict on a closed slot is final** (given its base bundle): it is recorded in `bundles/list.pb`
  (`BundleList.skipped {strategy, slot, base_id, as_of_seq, reason}`) and planned as `skipped` by every host and across
  restarts in O(1) — no re-measuring (a restart re-walked ~30 closed slots before reaching the live hour, 2026-08-21).
  A slot is **closed** once its as-of instant is 120 s in the past (entries are stamped at publish time, monotonic).
- **Unchanged gate**: an incremental whose tip set (name + oid) equals the newest built incremental of the same
  strategy on the same base is `skipped (unchanged since <id>)` — recorded like too-small. `min_commits` counts since
  the *base*, so without this an idle night cut 23–48 byte-identical 315 MB hourlies a day on the monorepo (2026-08-21
  08:00/09:00/10:00). Clients are unaffected: git stops at the first bundle whose prerequisites it has.
  A new base bundle for the slot re-opens it; the open (current) slot is never recorded. `min_bytes` is parsed, not yet enforced. The same gate for checkpoints is the existing trigger set
  (`wal.snapshot_every_entries` = the entry floor, `checkpoint_interval` = the age override, `checkpoint_tail_bytes`);
  compaction's byte/count triggers likewise.
- **D21 — no monthly layer**: git's heuristic never walks past a full bundle, and it downloads everything listed
  above it, so the list *is* the clone cost: with two-newest retention a fresh clone is newest weekly + ≤ 2 dailies +
  ≤ 2 hourlies = 5 downloads regardless of history length.

### 4b. Self-healing (D22)
The slot plan is recomputed every pass from config + WAL; the maintainer performs one bounded unit of the most
important missing work (weekly → dailies oldest-first → hourlies). Missing, deleted, or corrupt bundles are simply
"missing" and are rebuilt identically (content and token are functions of the slot and the WAL state as of it).
Slots with no WAL state at/before them are *unavailable* (plan shows it). No one-off backfill scripts exist;
replaying older history into the WAL (explicit `created_at`) makes the same loop fill the older slots.

## 5. Building: where the bytes come from

- **Full (weekly) for a repo whose pack set fits** an instance: `git bundle create` / gix from the local copy.
- **Full for a big repository — the Sunday unit**: the maintainer's missing weekly slot on an ssd host
  (the SSD host) first yields `Unit::BaseRebuild` when the newest tier-2 base predates the week (`base.seq ≤ seq as of
  max(previous weekly slot, first state)` — pushes landing *during* the rebuild do not re-trigger it; next Sunday does):
  the `compact base=1` op = `git repack -adb` (bitmap) + D18 history pack + commit-graph layer, published as a tier-2
  COMPACT entry superseding every smaller pack. The weekly slot itself then **composes** header (refs at the base's
  seq — a checkpoint written on the spot when none exists and no ref moved since) ∘ base pack via GCS compose
  (`walgit_server::bundles::compose_full_from_base`, also the CLI's `walgit bundle compose`): zero bytes through the
  host, no index-pack. A Full slot of a repository with a base is never a `pack-objects` of its history. tmpfs hosts
  never rebuild (they compose the base they have). Test: `weekly_slot_rebuilds_the_base_then_composes_it_on_an_ssd_maintainer`.
- **Incremental**: on a host with the packs local (the SSD host), our own header + `git pack-objects --revs
  --delta-base-offset` — **self-contained deltas, never `--thin`** (`git bundle create` always packs thin: deltas
  against the prerequisites' objects, which the client must resolve against its 32 GB base). Measured on the monorepo's
  07:00 hourly, same object set (2026-08-21): thin 226.8 MB, client `index-pack` 48.4 s and 420 MB on disk (193 MB
  of bases appended by `--fix-thin`); self-contained **314.9 MB (+39 %), 31.7 s (−35 %), 315 MB on disk**; server
  pack-objects 8.9 → 19.7 s. Static bytes are the cheap resource. The **gix engine** (`write_bundle_gix`) remains
  for a host whose base is linked/remote: tree-diff enumeration over the commit-graph + history pack, delta bases
  faulted through the remote reader.
- Every build is a **task** (discoverable at `…/tasks`, narrated); the builder materializes what it needs
  first (`BundleSource::prepare_objects`, the "bad object" fix) and skips refs whose tips it cannot resolve with
  a notice rather than failing the repo.
- Checksum streamed (bundles are GBs); upload `Create` (immutable); list updated by CAS; old entries pruned per
  retention with the list generation respected.

### 5a. Resumable `BaseRebuild` (2026-08-22, landed: `crates/walgit-server/src/rebuild.rs`)

Context: D31 drains interrupt the running unit at once and let D22 redo it — fine for an hourly bundle, wrong for
the monorepo's base: `git repack -adb` is 16–30 min of one core plus the 10-min history pack, and a deploy (every few hours
today) threw it away and restarted it on the next pass — and the repack rewrote the *serving copy* in place. The
shape below is what runs now for the weekly `BaseRebuild` unit and for `walgit compact --base` alike:

1. **Scratch dir that outlives the container**: the rebuild runs in `<cache.dir>/_rebuild/<owner>/<repo>.git/` on
   `/data` (bind mount, survives `docker run` restarts; `cp -a --reflink=always` of the serving copy on XFS = seconds,
   no bytes copied until written), never in the serving copy — the serving copy keeps answering fetches from its
   unchanged pack set meanwhile (today the repack rewrites the serving copy's `objects/pack` in place).
2. **Phase marker** `_rebuild/<o>/<r>.json`: `{ started_head_seq, base_checksum?, phase }` with phases
   `repacked → side-files (rev/bitmap/commit-graph) → history-pack → uploaded → published`, written after each phase
   completes (the pack files themselves are the evidence; the marker only says which ones are final).
3. **Resume rule**: the next `BaseRebuild` unit (any pass after the restart, same host — the scratch dir is the host's)
   reads the marker; **iff `manifest.head_seq == started_head_seq`** it continues from the recorded phase, otherwise it
   deletes the scratch dir and starts over (a push landed: the pack would miss objects; AGENTS §2.5 "pushes landing
   during the rebuild do not re-trigger it" stays true because the *planner* still uses the slot rule — only the resume
   check is strict). Partial uploads are immutable `Create`s keyed by checksum: re-running them is a no-op/412-as-exists.
4. **Idempotent publish**: `publish_compact` of a pack already live under the same checksum is skipped (landed 2026-08-22
   in `compact_repo`); a COMPACT entry that supersedes packs already superseded is harmless (`supersedes` of absent
   packs is ignored by readers — to verify with a sim scenario).
5. **Not awaited on drain** (D31 phase 1 unchanged): SIGTERM kills `git repack`; the marker's phase is whatever
   completed; `git` writes packs via a temp name + rename, so a half-written pack never looks complete.
6. Tests: sim `base_rebuild_resumes_after_a_kill_between_any_two_phases` — the unit is killed after *each* phase in
   turn (per-repo test hook `rebuild::TEST_ABORT_AFTER`, standing in for SIGTERM), the instance restarts on the same
   disk, and across all attempts there is exactly **one** `git repack`; the serving copy's pack files are unchanged
   until publish; a push between a kill and the resume forces a fresh start whose base contains the pushed tip.
   Budget: unchanged store round trips on the happy path (the marker is local). Also: the publisher now queues its own
   superseded packs for removal by the next pack sync (the in-place repack used to delete them; the scratch one
   leaves them in the serving copy until readers are gone). Headroom check: the rebuild refuses to start with less
   free space under `cache.dir` than the live pack set (the new pack is about that size). Out of scope: imports
   (`walgit import` resumability is a different shape — staged upload of many packs), and a tmpfs host (tmpfs dies with
   the instance; bases are never rebuilt there).

## 6. What a client experiences

| Client state | What git does | Server work |
|---|---|---|
| Fresh `clone` (bundle-uri on) | weekly (32 GB from bucket) + dailies + hourlies newer than the weekly, then `fetch` remainder | one negotiation: ≤ 1 h of objects from local small packs |
| Fresh `clone --depth=1 --filter=blob:none --single-branch` (CI) | **git still downloads the advertised bundles first** (measured 2026-08-21: the 32 GB weekly, 154 s / 4.4 GB before giving up) — the server cannot see the filter at bundle-uri time. Pass **`-c transfer.bundleURI=false`** (Clone menu + install.sh say so); then upload-pack answers directly | **8.4 s** on the large-repository benchmark (D18 result; 4.4 s / 58 MB on the SSD host 2026-08-21) — commit-graph + history pack local, blobs lazily |
| 3 d 12:30 stale, on main | the newest applicable cumulative daily/hourly newer than its token, then fetch | ~30 min of objects |
| 3 d stale, on a branch | same as above for main, plus `want branch` | the branch's own commits (tiny; unchanged by rebases) |
| Very stale (older than the oldest kept weekly) | the full weekly + chain | as fresh clone |
| bundle-uri off, full clone of the monorepo | refused (D17) with the fix text | none |

## 6b. The blobless family (`filter = "blob:none"`) — developers' clones
A developer's monorepo clone is `--filter=blob:none` with full history (CI's shape is `--depth=1 --sparse`). Git's
bundle-uri client (2.47 … master, `bundle-uri.c`) **never consults `bundle.<id>.filter`** — the key exists only in
`Documentation/technical/bundle-uri.adoc` — so with `transfer.bundleURI=true` a blobless clone downloads and indexes
the full weekly: 32 GB of blobs it never asked for (measured on the local rig). Conversely a filtered bundle
*is* unbundled (`index-pack --promisor=from-bundle`) by any clone that gets it, so a full clone fed a blobless bundle
ends up with promisor packs it cannot complete. Hence:
- Strategies carry `filter = "blob:none"`; a whole chain shares one filter (config validation). **Weekly-history**
  (full, filtered) = header with `@filter=blob:none` (bundle v3) ∘ the D18 **history pack** of the base (commits +
  trees = exactly `--filter=blob:none` of the refs at the base's seq; BaseRebuild builds it anyway) — a compose, no
  bytes through the host. Incrementals pack with `pack-objects --filter=blob:none` (self-contained, as the full
  family); all gates (unchanged, too-small, closed-slot verdicts) apply per strategy.
- **Two lists, never mixed**: `bundles/list` (and the protocol v2 `bundle-uri` advertisement) carry the unfiltered
  chain only; `bundles/list?filter=blob:none` carries the blobless family with `bundle.<id>.filter = blob:none`
  lines. A developer (or `dev clone`) runs
  `git clone --filter=blob:none --sparse --bundle-uri=<list>?filter=blob:none -c fetch.bundleURI=<list>?filter=blob:none …`
  (`<list>` = `https://git.example.com/acme/monorepo.git/bundles/list`); the explicit `fetch.bundleURI` is what makes later
  fetches use the same family — git 2.51 sets it itself only for `--bundle-uri=` clones with a creationToken list and
  **never for an advertised (`transfer.bundleURI`) clone**, which is why every recipe we emit carries it
  (`setup::Recipes`, 2026-08-22). Blobs arrive lazily through upload-pack as
  today (9.2 s for a 15.9 k-file sparse add). **Always with `--sparse` (or `--no-checkout`)**: a plain checkout of
  the monorepo's HEAD asks upload-pack for all 1.47 M blobs of the tree in one promisor fetch — one `pack-objects` on an SSD-backed
  host at 49 GB RSS for > 12 min (2026-08-22; it exits by itself once the client disconnects, no orphan) — while the
  sparse shape fetches blobs per `sparse-checkout add`. The Clone menu and the installer show the sparse form.
- **Why the server cannot choose for the client**: the protocol v2 `bundle-uri` command takes no arguments at all
  (`bundle_uri_command` in `bundle-uri.c` dies on any: "unexpected argument"), and the clone's `filter` travels only
  in the later `fetch` command — at advertisement time walgit cannot know the clone's shape. So the protocol list
  stays the full chain, and the **finding stands: the advertised list is actively harmful for the most common
  developer shape** (`--filter=blob:none` + `transfer.bundleURI=true` = the full 32 GB weekly + its 19-min index-pack
  for nothing); the Clone menu and the setup script (`/services/install.sh`) say so and give the `--bundle-uri=…?filter=blob:none` line, CI/shallow
  clones `-c transfer.bundleURI=false`.
- **The fix at the root** is in git: `docs/patches/0001-bundle-uri-…patch` makes the client match
  `bundle.<id>.filter` against the clone's filter (clone passes its spec via `bundle_uri_set_filter()` because it
  registers the promisor remote only after bundles are fetched; fetch reads `partialclonefilter`). With it
  `bundles.advertise_filtered = true` puts both families on ONE list and each clone takes its own
  (`tests/git-bundle-filter.sh`). Until clients carry it, the separate list URI stays.
- Test: `blobless_bundle_family_is_composed_from_the_history_pack_and_served_on_its_own_list` (real
  `git clone --filter=blob:none --bundle-uri` → promisor packs from the bundles, blobs missing until checkout; a full
  clone via the protocol list sees none of it).

## 6c. Client indexing cost — measured, and what to do about it (2026-08-22, design only)

A full monorepo clone spends **1,063.6 s of 1,499.9 s (71 %) in the client's `index-pack`** of the 32.3 GB weekly; a
blobless one 362.8 s over 43.6 M history objects. `git clone --bundle-uri` feeds every downloaded bundle to
`git index-pack --fix-thin --stdin` (trace2 of a local clone confirms exactly one such child per bundle) — it never
uses an index the server already has, although the weekly *is* `header ∘ wal/<base>.pack` and `wal/<base>.idx`/`.rev`
sit next to it in the bucket.

**Measured** on `git/git` (bundle 317.6 MB, 419,403 objects, 317,662 deltas = 76 %, 16-core laptop, git 2.51):
| | time |
|---|---|
| `bundle unbundle` (= index-pack, `pack.threads` unset → online CPUs) | **5.5 s** |
| of which *receiving* — inflate + SHA-1 of every object + trailer, **single-threaded by design** | 2.7 s ≈ 115 MB/s |
| of which *resolving deltas* | 2.8 s on 16 threads · 4.4 s on 8 · 7.2 s on 2 · 14.9 s on 1 |
| `core.packedGitLimit=4g core.deltaBaseCacheLimit=2g` | no effect (7.6 s, noise) |
| `sha1sum` of the pack (what trusting a shipped index would still verify) | **0.42 s ≈ 750 MB/s** |
| shipped index size | `.idx` 11.7 MB (3.7 %), `.rev` 1.7 MB (0.5 %) |
Scaled to the weekly (32.3 GB, 60.1 M objects): receiving alone is ≥ 32.3 GB ÷ 115 MB/s ≈ **280 s** no matter the core
count; the measured 1,063.6 s on the SSD host's 44 cores means resolving ≈ 780 s (deltas of large blobs; memory-bound, not
core-bound — 44 cores did not make it 3× faster than 16 would). With a shipped index the client hashes the pack once
(≈ 45 s at 750 MB/s, overlappable with the 100–300 MB/s download) and copies it into place: **≈ 1 min instead of
≈ 18 min**; the clone becomes download-bound (≈ 8 min at edge-cache speed). the monorepo's `.idx` is 2.1 GB + `.rev` 240 MB:
+7 % bytes, ≈ 10–20 s.

**Options**
- **(a) Ship `.idx`/`.rev` next to the bundle; the client skips `index-pack` when present and verified.** Server: the
  list gains `bundle.<id>.idx` / `.rev` URIs (static objects under `/bundles/...`, for a composed weekly they *are*
  `wal/<base>.idx`/`.rev` — zero new bytes in the bucket; for incrementals we would run `index-pack` once on the
  maintainer, seconds). Client (a git patch, next to the `bundle.<id>.filter` patch in `docs/patches/`): in
  `bundle-uri.c`, when the list names an index, download pack-portion + idx (+ rev) straight into `objects/pack/`
  (the bundle's pack starts after its text header; `copy_file_range` to strip it), hash the pack and require the
  trailer to equal the idx's embedded pack checksum and the list's `sha`, then install the header's refs as today —
  **no `index-pack`**. Estimate 150–250 lines + tests. **What it trades**: `index-pack` recomputes every object's id
  from its content; a trusted idx moves "object id ⇔ content" onto the origin (authenticated, same origin as the bundle
  and as the refs themselves — the origin already decides which id `main` points at, but today it cannot make id X
  resolve to content Y). Keep `git fsck` the opt-in way to regain it; do **not** default it on.
- **(b) A shallower history family** for the blobless clone (fewer objects to index; `git log` stops at the cut) —
  addresses the 362.8 s, not the 1,063.6 s; needs the D18 history pack to be cut at a depth (new pack kind) and the
  incrementals to chain to it. Separate decision; ~500 lines server-side, no client change.
- **(c) `index.threads`/`pack.threads` via the installer**: nothing to gain — git already uses online CPUs, and the
  floor is the single-threaded receive phase (280 s); memory limits do not move it.
- **(d) Accept ≈ 18 min once per full clone**: the developer shape is the blobless clone (358 s, 6 GB), full clones
  are CI/benchmarks; tolerable but the north star says "bytes move bucket → laptop", not "laptop re-indexes 32 GB".

**Recommendation**: (a) for the weekly full first (the 1,000 s; zero new bucket bytes; reuses the client-patch channel we
already need for `bundle.<id>.filter`), then reassess (b) with the blobless numbers; drop (c) and (d).

## 7. Numbers to keep honest
Measured 2026-08-21 06:05–06:52Z on the SSD host (44 cores, NVMe, same zone as the edge) against real churn on main
(150–260 commits every 3 min); chain = import weekly + header-only daily + hourlies 04/05/06:00. Full detail and the
trace2 breakdown from the local rig.

| client | bytes through the front (`upload-pack`) | static bytes (edge/bucket) | front CPU | wall |
|---|---|---|---|---|
| fresh `clone` (bundle-uri) | **2.77 MB** = 49 min of pushes (**0.0085 %** of 32.70 GB) | 32.70 GB (weekly 32.3 GB from the edge cache at 305 MB/s, 3 hourlies, daily) | 3.3 s | 1,341.8 s — 1,140 s of it the client's `index-pack` of the weekly |
| stale since the daily, `fetch` (bundle-uri) | **52.4 MB** (since the newest hourly) | 172 MB (one hourly: git stops at the first bundle whose prerequisite — the daily's tip — it has) | 5.1 s | 72.0 s (60 s client `index-pack`) |
| same, bundle-uri off | **227 MB** | 0 | 11.7 s | 62.1 s (48.5 s client `index-pack`) |
| sparse checkout `areas/core/storefront` + a branch on top | 0.5 KB | 0 | — | 7.4 s + 2.5 s |
| CI clone `--filter=blob:none --depth=1` (`transfer.bundleURI=false`) | 58 MB | 0 | 4.4 s | 8.4 s (D18) |
| **developer clone `--filter=blob:none`, today's advertised list** (2026-08-21 12:13Z) | — (aborted) | 32.3 GB weekly + hourlies | — | **≥ 1,398 s** (weekly 92 s download + 1,057 s `index-pack` for blobs it never asked for) |
| **developer clone `--filter=blob:none --bundle-uri=…?filter=blob:none`** (the blobless family, 13:28Z) | ~0 (main unchanged since the base) | **6.0 GB** weekly-history | — | **431 s** = 67.6 s download (89 MB/s, one stream) + **362.8 s client `index-pack --promisor=from-bundle`** (43.6 M commits+trees; 15.8 s of it a `pack-objects --exclude-promisor-objects-best-effort`) + 0.3 s |
| same, `--sparse --single-branch`, through the public path `git.example.com` (2026-08-22, laptop) | **26.6 KB** | 6.0 GB weekly-history + 5 incrementals (78 MB) | — | **358 s** = 58.7 s download + 281 s client `index-pack` + ~5 s incrementals; `git fetch` afterwards 0.6 s |
| **full clone + checkout, end of day** (edge-cached measurement) | 75 MB (remainder = one push not yet in an hourly) | 32.31 GB weekly @ 480 MB/s + daily + 7 hourlies, all byte-exact | — | **1,499.9 s**: weekly download 67 s, **client `index-pack` 1,063.6 s (71 %)**, checkout 109 s for 1.47 M files |

**The trade-off, stated plainly.** Bundles move bytes off the front: 4.3× fewer bytes and 2.3× less CPU for the stale
fetch, 12,000× fewer bytes for the clone. They do not make one fast client on a warm NVMe server faster: the plain fetch
won on wall time (62 vs 72 s) because the client's `index-pack` dominates both and resolves one big pack in one pass,
while a bundle adds a second `index-pack` (43 s for 172 MB — thin deltas against the 32 GB base). Bundles win when what
is scarce is the front (many clients, cold or small fronts, a tmpfs host), the path to it (a laptop, not a same-zone VM),
or upload-pack itself (a cold base, a remote-served host); plain wins for a single client next to a warm SSD-backed host.
Both are correct; `transfer.bundleURI` is the client's choice and the default we install. What moves the wall time: (a) **self-contained incrementals** — landed 2026-08-21 07:30Z (§5: −35 % client
`index-pack`, +39 % static bytes; the 72 s stale fetch above was measured with thin bundles); (b) hourlies that are
not cumulative when the daily is fresh (today one hourly = the whole day so far).

Other numbers: weekly compose 32,312,706,666 B in ~2 s (bucket-side); hourly builds 4.8–7 s for 52–172 MB on the SSD host.
**The remaining full-clone cost is the client's `index-pack` of the weekly** (§6 item: measure its phases; idea of
pre-indexed delivery — the bundle's pack is byte-identical to `pack-<base>.pack`, whose `.idx`/`.rev` already sit in the
bucket — needs a git-side change); developers take the blobless family instead (§6b: 431 s, of which 363 s is the same
`index-pack` over 6 GB of commits+trees).
**Where the blobless 431 s goes and the next levers**: 84 % is the client's `index-pack` of 43.6 M commit+tree objects
(6 GB) — the history pack's object *count*, not its bytes; the download is 16 % and would halve with the edge cache
warm / two streams. Levers, in order: (1) a **shallower history family** (e.g. `--filter=blob:none` + the last N months of
commits as the full, older history lazily) cuts both; (2) `index-pack --threads` is already multi-threaded for delta
resolution but the 43.6 M-object SHA-1/`write_idx` phase is single-threaded — nothing to do server-side; (3) ship the
history pack's `.idx`/`.rev` alongside (git cannot use them from a bundle today). A full-worktree `checkout` on the
blobless clone (1.47 M files) is the pathological case git itself warns about — `fetch --stdin` of 1.47 M blob wants
spins a core client-side for > 35 min before any request reaches the server; developers use sparse-checkout (9.2 s for
15.9 k files, §6).

## 8. Open items
- [ ] Land the filter-matching patch in a patched git before advertising full and blobless families together
  (`docs/patches/README.md`, §6b).
- [ ] Reduce client-side indexing of full/history bundles; evaluate pre-indexed delivery or a shallower history family (§7).
- [ ] `packfile-uris` evaluation (serve the base pack itself as a static URI inside upload-pack) — likely
      redundant with bundle-uri + D18; measure before building.
- [ ] CDN in front of `/{o}/{r}/bundles/*` (immutable + Range already in place).
