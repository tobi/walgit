# Object integrity — the invariant, the audit, the repair

Context: **spec + runbook** for the one data invariant the WAL cannot express: *every object reachable from an
advertised ref is in a live pack*. For anyone touching `walgit import`, the maintainer's `fsck`/`repair` units
(`crates/walgit-server/src/ops.rs`, `maintain.rs`, `walgit-git/src/repair.rs`), or debugging `connectivity:
missing object` on a push / `NotFound` on a partial-clone fetch. Born from a large-repository recovery test (the monorepo's 1,952
missing blobs).

## 1. The invariant and where it can break
The WAL guarantees *which* packs are live and *which* refs point where; it does not know whether the packs hold
the refs' closure. Pushes cannot break it (receive-pack checks connectivity before publishing — that check is
what surfaced the hole). What can:
- **Import**: a pack set built from one ref selection and a ref snapshot taken from another. the monorepo: the 32 GB base
  = closure of `main` + tags at `45371258`; the snapshot also carried `refs/remotes/origin/main` 479 commits
  ahead. `import --direct` now refuses this: every published tip (and by default its full closure,
  `--verify-closure=false` to skip) must be in the pack set before anything is uploaded
  (`import_direct::verify_refs_in_packs`, two unit tests).
- A compaction/base rebuild that drops objects (reachability from a stale tip), a superseded pack GC'd too early,
  a corrupt or truncated object in the bucket. None seen; the audit below is the detector for all of them.

## 2. Audit: the `fsck` unit (✅ 2026-08-21)
Lowest-priority maintainer unit (`maintenance.fsck_interval`, default 7 d; 0 = off), only on a host whose copy
holds the whole pack set (`packs_fit()` — the SSD host for a large repository; never over a linked/remote base). Runs
`git fsck --connectivity-only --no-dangling` (`ops.rs` `fsck`, `connectivity=1`) and writes the verdict to
**`repos/<o>/<r>/fsck.pb`** (`FsckReport {seq, at, host, missing[≤100 k], missing_total, problems, elapsed_secs,
repaired_seq}`; overwritten, not WAL). Missing objects are a *finding* (unit ok, plan shows `repair` next), corrupt
objects a failure. Gauge `walgit_repo_missing_objects{repo}` is set from the report on every pass. Due when: never
audited, older than the interval, or a repair landed since (`repaired_seq > 0` → re-verify). Manual: `POST
/{o}/{r}/api/ops/fsck` with `connectivity=1` (WAL page) writes the same report. the monorepo on the SSD host: ~10 min.

## 3. Repair: the `repair` unit (✅ 2026-08-21)
Due right after checkpoint when `fsck.pb` lists missing objects, `repaired_seq == 0` and the repository has
**`upstream.git`** (D24 setting, `[upstream] git = "https://github.com/acme/monorepo.git"`, `token_env` shared with
`upstream.lfs`, `docs/LFS.md`). Steps (`walgit_git::repair::fetch_objects_as_pack`): scratch bare repo under
`cache.dir/repair/`, `git fetch --depth=1 <upstream> <oid>…` in batches of 500 (GitHub serves commit, tree **and
blob** wants by SHA — verified 2026-08-21; walgit enables arbitrary object wants for partial-clone lazy fetches), `pack-objects` of exactly
the requested oids, verify every oid is in the resulting idx (a refused want is an error, never a silent hole),
then `RepoHandle::add_pack(tier 0)` → one **COMPACT entry superseding nothing** (what `walgit wal add-pack … --tier
0` did by hand for a large repository, seq 11). `fsck.pb.repaired_seq` is set so the next pass re-audits instead of repairing
again. Counter `walgit_repair_objects_total{repo}`. Test: `tests/maintain.rs
fsck_unit_records_missing_objects_and_repair_unit_fetches_them_from_upstream` (hole → audit → repair over HTTP from
a second repository → re-audit clean → idle).

This does not widen repository access: a principal with read access can already fetch any stored object by ID,
including objects unreachable from an advertised ref. Removing `git.allow_any_sha1_in_want` only removes a
non-functional configuration switch.

## 4. Runbook (what was done for a large repository, 2026-08-21 03:40Z)
1. Enumerate: `git rev-list --objects --missing=print <advertised tips> --not <known-good tip>` on the complete copy
   (or read `fsck.pb` / `/var/lib/walgit/monorepo-fsck.out`) → 1,952 blobs, 70 MB.
2. Source them: a mirror (`git pack-objects --stdout < oids`) or the upstream (the unit's fetch).
3. Publish: `walgit wal add-pack <o>/<r> pack-<sha>.pack --tier 0` → seq N; every host installs it on its next sync.
4. Fix the cause: refs that should never have been advertised are deleted with a ref-delete push
   (`refs/remotes/origin/main|HEAD`, seq 12); the import now verifies.
5. Re-audit (the unit does it after a repair).

## 5. `walgit import --direct` is resumable and idempotent (2026-08-22)
An import interrupted anywhere (network, SIGTERM, a deploy, a full disk) is re-run **with the same command** and
finishes without redoing finished work; running it again on a completed import changes nothing.
- **Marker** `<pack dir>/../walgit-import/<owner>-<repo>.json`, keyed by the *intent* (repository + a hash of the
  exact ref set being published — a different `--refs` filter is a different import): the target manifest version
  when the import started, the seq it publishes at, the last completed phase (`started → verified → side-files →
  history-pack → uploaded`), the **per-object done set** of uploaded store keys, the history pack path. Written after every phase and after every uploaded object (a kill loses at most one
  object's upload).
- **Resume rule**: the marker is used only while the target's manifest version is still the one recorded; if the
  repository moved meanwhile (someone pushed or imported) the run refuses with the fix — `--force` starts over on the
  current state, and objects whose checksums already sit in the bucket are still skipped.
- **Uploads** (ROUNDTRIPS): an object in the done set costs nothing; the rest of a pack's objects (pack, idx, rev,
  bitmap, commit-graph) are HEADed **in parallel, one round**, and only absent ones are uploaded (`Create`-if-absent by
  checksum; a size mismatch re-uploads). The closure walk (minutes on 60 M objects), the commit-graph layer and the
  history pack are never redone: file presence and the marker are the evidence.
- **Idempotent completion**: if the manifest already lists every object pack of the pack set (and a history pack
  derived from the base when `--history-pack`), the run prints "already holds this import" and exits — 0 uploads,
  0 CAS, no `--replace` needed. The manifest write stays one CAS on the version the import started from.
- Tests (`import_direct::resume_tests`): `decide_resume` (intent/base/force matrix); an import killed after each
  phase in turn and resumed — every object uploaded exactly once across all runs, no second closure walk / history
  pack, one CAS, marker gone, then a second full run = no-op; a moved target → refused, `--force` → fresh start
  reusing the uploads.
