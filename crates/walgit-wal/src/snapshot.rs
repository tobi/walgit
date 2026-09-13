//! Time travel: rebuild a repository as it was at a WAL sequence.
//!
//! The WAL keeps every state a repository has ever been in (AGENTS §2.4), so a
//! rewind is a pure function of (bucket, seq): refs from the newest checkpoint
//! at or before the cut plus the replayed ref transactions, objects from the
//! pack set that was live at the cut. Nothing here publishes, nothing moves a
//! live ref, and the local serving copy is never rewritten — it stays at the
//! manifest head while the rewound copy lives in a directory of its own.
//!
//! Two entry points over the same replay:
//!
//! * [`materialize_at`] rebuilds into a caller-chosen directory
//!   (`walgit wal materialize --at-seq`).
//! * [`snapshot_at`] rebuilds into this instance's
//!   `<cache.dir>/snapshots/<owner>/<name>/<seq>/`, idempotently, and is what
//!   the server's `snapshot` op runs. That tree is a cache like every other
//!   byte on disk: delete it and the next call rebuilds the identical copy.
//!
//! The pack phase (striped downloads, 32 MiB copies, `git index-pack` waits)
//! runs on the bulk runtime, never on the caller's — a rewind must not stall
//! request workers (AGENTS §5, principle VI).

use std::path::{Path, PathBuf};
use std::sync::Arc;

use prost::Message;
use serde::{Deserialize, Serialize};
use tracing::info;
use walgit_git::{LocalRepo, ObjectFormat, RepoId};
use walgit_proto::v1::{LogEntry, PackRef};
use walgit_store::ObjectStoreExt;

use crate::error::WalError;
use crate::handle::RepoHandle;
use crate::registry::Registry;

/// Completion marker of a [`snapshot_at`] tree: the serialized [`Snapshot`],
/// written last, so a tree without it is a partial run.
const MARKER: &str = "snapshot.json";

/// How many refs a [`Snapshot`] carries inline.
pub const REFS_MAX: usize = 1000;

/// Sink for the lines a rewind narrates: the CLI prints them, the server's
/// `snapshot` op turns them into task notices.
pub type Narrator = Arc<dyn Fn(String) + Send + Sync>;

/// What a rewind produced. Also the body of `GET /{o}/{r}/api/snapshot/{seq}`
/// and the `snapshot` op's result value, so field names are wire names.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    pub repo: String,
    /// The WAL sequence this copy was rebuilt at.
    pub at_seq: u64,
    /// The manifest head at build time. The serving copy stays here: a
    /// snapshot never moves a live ref.
    pub head_seq: u64,
    /// Where the replay started: the newest checkpoint at or before `at_seq`,
    /// or 0 (from the first log entry).
    pub from_seq: u64,
    /// Log entries applied in `(from_seq, at_seq]`.
    pub entries: u64,
    /// The bare git directory of the rebuilt copy on the host that built it.
    pub git_dir: String,
    /// How many refs existed at `at_seq`.
    pub ref_count: usize,
    /// The first [`REFS_MAX`] refs, name-sorted. Bounded on purpose: a rewind
    /// of a 466 k-ref repository must not become a 30 MB JSON document on a
    /// path that answers requests. The copy at `git_dir` has all of them.
    pub refs: Vec<SnapshotRef>,
    /// `HEAD`'s symbolic target at `at_seq` (`""` when the log never set one).
    pub head_target: String,
    /// The packs that were live at `at_seq`.
    pub packs: Vec<SnapshotPack>,
    /// Which instance built it (snapshots are per instance, like tasks).
    pub hostname: String,
    /// RFC 3339 build time.
    pub built_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotRef {
    pub name: String,
    pub sha: String,
    /// Peeled commit of an annotated tag, `""` otherwise.
    pub peeled: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotPack {
    pub checksum: String,
    pub tier: u32,
    pub pack_size: u64,
    pub object_count: u64,
    /// `local` (copied from this instance's serving copy) or `store`
    /// (range-downloaded from the bucket).
    pub source: String,
}

/// Directory this instance keeps a rewind of `(id, at_seq)` in:
/// `<cache.dir>/snapshots/<owner>/<name>/<seq>/`. The bare git directory is
/// `<owner>/<name>.git` inside it, as in every other walgit repository root.
pub fn snapshot_dir(cache_dir: &Path, id: &RepoId, at_seq: u64) -> PathBuf {
    cache_dir
        .join("snapshots")
        .join(id.owner())
        .join(id.name())
        .join(at_seq.to_string())
}

/// The completed snapshot in `dir`, or None when it is absent, unreadable, or
/// its git directory is gone (an evicted tree is not a snapshot).
pub fn read_snapshot(dir: &Path) -> Option<Snapshot> {
    let bytes = std::fs::read(dir.join(MARKER)).ok()?;
    let snap: Snapshot = serde_json::from_slice(&bytes).ok()?;
    Path::new(&snap.git_dir)
        .join("HEAD")
        .is_file()
        .then_some(snap)
}

/// Rebuild `id` as it was at `at_seq` under `<cache.dir>/snapshots/…`,
/// idempotently: a tree that already carries its [`MARKER`] is returned as it
/// is (`false`), anything else is (re)built from the WAL (`true`).
///
/// Rejects `at_seq` of 0 and any sequence past the WAL head, and refuses to
/// build a second pack set on an instance whose cache budget cannot hold even
/// the live one (D25: no budget in disk mode).
pub async fn snapshot_at(
    registry: Arc<Registry>,
    id: RepoId,
    at_seq: u64,
    narrate: Narrator,
) -> Result<(Snapshot, bool), WalError> {
    if at_seq == 0 {
        return Err(WalError::Invalid(
            "at_seq must be 1 or greater (there is no WAL state before the first entry)".into(),
        ));
    }
    let handle = registry.open(&id).await?;
    // One conditional GET: validate against a head nobody can call stale.
    drop(handle.sync_refs().await?);
    let head_seq = handle.manifest().head_seq;
    if at_seq > head_seq {
        return Err(WalError::Invalid(format!(
            "at_seq {at_seq} is past the WAL head ({head_seq})"
        )));
    }
    if !handle.packs_fit() {
        let bytes: u64 = handle
            .manifest()
            .packs
            .iter()
            .map(|p| p.pack_size + p.idx_size)
            .sum();
        return Err(WalError::TooLarge {
            bytes,
            max: registry.config().cache_budget_bytes(),
        });
    }

    let dir = snapshot_dir(&registry.config().cache.dir, &id, at_seq);
    if let Some(snap) = read_snapshot(&dir) {
        narrate(format!(
            "seq {at_seq} is already materialized at {}",
            snap.git_dir
        ));
        return Ok((snap, false));
    }
    // No marker: either nothing is there, or an interrupted run left a partial
    // tree behind (a drain interrupts this op — D31 — and D22's rule is that
    // the next pass redoes it identically).
    if dir.exists() {
        narrate(format!(
            "discarding an incomplete snapshot tree at {}",
            dir.display()
        ));
        tokio::fs::remove_dir_all(&dir).await?;
    }

    let snap = materialize_at(registry, id, at_seq, dir.clone(), narrate).await?;
    let body = serde_json::to_vec(&snap)
        .map_err(|e| WalError::Corrupt(format!("encoding {MARKER}: {e}")))?;
    tokio::fs::write(dir.join(MARKER), body).await?;
    Ok((snap, true))
}

/// Rebuild `id` as it was at `at_seq` into `out` (which must not exist): refs
/// from the newest checkpoint at or before `at_seq` (or from seq 0) plus the
/// replayed log entries, packs from this instance's serving copy when it has
/// them (copied, never moved) or from the store. Works on any machine with
/// bucket access — a cold rewind needs nothing local.
pub async fn materialize_at(
    registry: Arc<Registry>,
    id: RepoId,
    at_seq: u64,
    out: PathBuf,
    narrate: Narrator,
) -> Result<Snapshot, WalError> {
    // Downloads, big copies and `git index-pack` waits belong to the bulk
    // runtime; the caller's request workers keep serving refs.
    crate::sync::on_bulk_runtime(async move {
        materialize_inner(&registry, &id, at_seq, &out, &narrate).await
    })
    .await
}

async fn materialize_inner(
    registry: &Registry,
    id: &RepoId,
    at_seq: u64,
    out: &Path,
    narrate: &Narrator,
) -> Result<Snapshot, WalError> {
    let handle = registry.open(id).await?;

    if out.exists() {
        return Err(WalError::Invalid(format!(
            "output directory {} already exists",
            out.display()
        )));
    }
    std::fs::create_dir_all(out)?;

    let manifest = handle.manifest();
    let format = match manifest.object_format.as_str() {
        "sha1" => ObjectFormat::Sha1,
        "sha256" => ObjectFormat::Sha256,
        other => {
            return Err(WalError::Corrupt(format!(
                "unknown object format in manifest: {other}"
            )));
        }
    };
    let local = LocalRepo::init(out, id, format)?;

    // Start from the newest checkpoint at or before `at_seq` (its snapshot is
    // the folded log), else replay from seq 0.
    let mut start_seq = 0u64;
    let mut pack_set: Vec<PackRef> = Vec::new();
    if let Some(cp) = manifest.checkpoint.as_ref().filter(|c| c.seq <= at_seq) {
        let (_, bytes) =
            handle.store().get_bytes(&cp.key).await?.ok_or_else(|| {
                WalError::Corrupt(format!("checkpoint object {} missing", cp.key))
            })?;
        let cpo = walgit_proto::v1::Checkpoint::decode(bytes.as_ref())
            .map_err(|e| WalError::Corrupt(format!("checkpoint decode: {e}")))?;
        let (_, rb) = handle
            .store()
            .get_bytes(&cpo.refs_key)
            .await?
            .ok_or_else(|| {
                WalError::Corrupt(format!("checkpoint refs {} missing", cpo.refs_key))
            })?;
        let snap = walgit_proto::v1::RefSnapshot::decode(rb.as_ref())
            .map_err(|e| WalError::Corrupt(format!("checkpoint refs decode: {e}")))?;
        local.load_ref_snapshot(&snap)?;
        pack_set = cpo.packs.clone();
        start_seq = cp.seq;
        info!(
            seq = cp.seq,
            packs = pack_set.len(),
            refs = snap.refs.len(),
            "starting from checkpoint"
        );
        narrate(format!(
            "starting from the checkpoint at seq {} ({} pack(s), {} refs)",
            cp.seq,
            pack_set.len(),
            snap.refs.len()
        ));
    }

    let entries = read_entries(&handle, &manifest, start_seq, at_seq).await?;
    info!(entries = entries.len(), "replaying log entries");
    narrate(format!(
        "replaying {} log entr{} up to seq {at_seq}",
        entries.len(),
        if entries.len() == 1 { "y" } else { "ies" }
    ));
    let in_range = |e: &&LogEntry| e.seq > start_seq && e.seq <= at_seq;
    let mut applied = 0u64;
    for entry in entries.iter().filter(in_range) {
        applied += 1;
        if let Some(pack) = &entry.pack {
            pack_set.push(pack.clone());
        }
        pack_set.retain(|p| !entry.supersedes.contains(&p.checksum));
    }

    // Packs live at `at_seq`: copy from the serving copy when it has them, else
    // fetch from the store. The serving copy is only ever read.
    let tmp = out.join(".walgit-tmp");
    std::fs::create_dir_all(&tmp)?;
    let mut packs = Vec::with_capacity(pack_set.len());
    for p in &pack_set {
        let checksum = gix_hash::ObjectId::from_hex(p.checksum.as_bytes())
            .map_err(|e| WalError::Corrupt(format!("pack checksum {}: {e}", p.checksum)))?;
        let src = handle.local().pack_path(&checksum);
        let source = if src.is_file() && !src.is_symlink() {
            for ext in ["pack", "idx", "rev", "bitmap", "commit-graph"] {
                let f = src.with_extension(ext);
                if f.is_file() {
                    std::fs::copy(&f, tmp.join(format!("pack-{}.{ext}", p.checksum)))?;
                }
            }
            narrate(format!("pack {}: copied from the local copy", p.checksum));
            "local"
        } else {
            narrate(format!(
                "pack {}: fetching {} bytes from the store",
                p.checksum, p.pack_size
            ));
            handle.fetch_pack_into(p, &tmp).await.map_err(|e| {
                WalError::Corrupt(format!(
                    "pack {} is not in the store any more (superseded and past retention?): {e}",
                    p.checksum
                ))
            })?;
            "store"
        };
        let pack_path = tmp.join(format!("pack-{}.pack", p.checksum));
        let idx_path = tmp.join(format!("pack-{}.idx", p.checksum));
        let extra: Vec<PathBuf> = ["rev", "bitmap", "commit-graph"]
            .iter()
            .map(|e| tmp.join(format!("pack-{}.{e}", p.checksum)))
            .filter(|f| f.is_file())
            .collect();
        local.install_pack(&pack_path, &idx_path, &extra).await?;
        packs.push(SnapshotPack {
            checksum: p.checksum.clone(),
            tier: p.tier,
            pack_size: p.pack_size,
            object_count: p.object_count,
            source: source.to_string(),
        });
    }
    let _ = std::fs::remove_dir_all(&tmp);

    // Refs last (git checks the objects exist).
    for entry in entries.iter().filter(in_range) {
        if let Some(txn) = &entry.txn {
            local.apply_ref_txn(txn, false)?;
        }
    }

    local.refresh()?;
    let refs = local.refs()?;
    info!(out = %out.display(), "materialized at seq {at_seq}");
    narrate(format!(
        "materialized {id} at seq {at_seq} into {}: {} ref(s), {} pack(s)",
        local.path().display(),
        refs.refs.len(),
        packs.len()
    ));

    Ok(Snapshot {
        repo: id.to_string(),
        at_seq,
        head_seq: manifest.head_seq,
        from_seq: start_seq,
        entries: applied,
        git_dir: local.path().display().to_string(),
        ref_count: refs.refs.len(),
        refs: refs
            .refs
            .into_iter()
            .take(REFS_MAX)
            .map(|r| SnapshotRef {
                name: r.name,
                sha: r.oid,
                peeled: r.peeled,
            })
            .collect(),
        head_target: refs.head_target,
        packs,
        hostname: walgit_store::coord::instance_id().to_string(),
        built_at: chrono::Utc::now().to_rfc3339(),
    })
}

/// Log entries in `(start_seq, at_seq]`. Entries older than the manifest's log
/// window (folded into a checkpoint) are probed directly as `log/<seq>.pb`:
/// those objects stay in the bucket until they are GC'd, so a cold rewind past
/// the window usually still works — and says so plainly when it does not.
async fn read_entries(
    handle: &RepoHandle,
    manifest: &walgit_proto::v1::Manifest,
    start_seq: u64,
    at_seq: u64,
) -> Result<Vec<LogEntry>, WalError> {
    if manifest.min_seq == 0 || start_seq + 1 >= manifest.min_seq {
        return handle.read_log(start_seq + 1, Some(at_seq)).await;
    }
    let mut found = Vec::new();
    let mut seq = start_seq + 1;
    while seq <= at_seq.min(manifest.min_seq.saturating_sub(1)) {
        let key = walgit_proto::keys::log_segment_key(seq);
        let Some((_, bytes)) = handle.store().get_bytes(&key).await? else {
            return Err(WalError::Corrupt(format!(
                "history before seq {} is folded into a checkpoint and {key} is gone; the oldest rewindable state is seq {}",
                manifest.min_seq,
                manifest
                    .checkpoint
                    .as_ref()
                    .map_or(manifest.min_seq, |c| c.seq)
            )));
        };
        let (es, _) = walgit_proto::frame::decode_entries(&bytes)
            .map_err(|e| WalError::Corrupt(format!("log segment {key} decode: {e}")))?;
        let last = es.last().map_or(seq, |e| e.seq);
        found.extend(es);
        seq = last + 1;
    }
    if at_seq >= manifest.min_seq {
        found.extend(handle.read_log(manifest.min_seq, Some(at_seq)).await?);
    }
    Ok(found)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use walgit_config::Config;

    fn run_git(dir: &Path, args: &[&str]) -> String {
        let out = std::process::Command::new("git")
            .current_dir(dir)
            .args(args)
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    }

    fn silent() -> Narrator {
        Arc::new(|_| {})
    }

    /// Three pushes on one instance, then a full repack published as a tier-2
    /// base (seq 4) and a checkpoint there.
    struct Fixture {
        cfg: Arc<Config>,
        store: walgit_store::DynStore,
        /// The instance that wrote everything; its local copy keeps its packs.
        writer: Arc<Registry>,
        id: RepoId,
        /// The three commits, oldest first.
        tips: Vec<String>,
        base_seq: u64,
        _cache: tempfile::TempDir,
    }

    impl Fixture {
        /// A second instance on the same store with an empty cache: every pack
        /// has to come from the bucket (the cold-rewind case).
        fn cold(&self) -> (Arc<Registry>, tempfile::TempDir) {
            let cache = tempfile::tempdir().unwrap();
            let mut cfg = (*self.cfg).clone();
            cfg.cache.dir = cache.path().to_path_buf();
            (Registry::new(self.store.clone(), Arc::new(cfg)), cache)
        }
    }

    async fn fixture() -> Fixture {
        let cache = tempfile::tempdir().unwrap();
        let store = walgit_store::memory::MemoryStore::shared();
        let mut cfg = Config::default();
        cfg.cache.dir = cache.path().to_path_buf();
        cfg.store.bucket = "test".into();
        cfg.wal.fsck_objects = false;
        cfg.wal.check_connectivity = false;
        cfg.wal.freshness_ttl = std::time::Duration::ZERO;
        cfg.wal.snapshot_every_entries = 0;
        cfg.wal.checkpoint_interval = std::time::Duration::ZERO;
        cfg.wal.checkpoint_tail_bytes = walgit_config::ByteSize::b(0);
        let cfg = Arc::new(cfg);
        let registry = Registry::new(store.clone(), cfg.clone());
        let id = RepoId::new("t", "rewind").unwrap();
        let handle = registry.create(&id, ObjectFormat::Sha1).await.unwrap();
        let work = tempfile::tempdir().unwrap();
        run_git(work.path(), &["init", "-q", "-b", "main"]);
        run_git(work.path(), &["config", "user.email", "t@t"]);
        run_git(work.path(), &["config", "user.name", "t"]);
        let mut prev = String::new();
        let mut tips = Vec::new();
        for i in 0..3 {
            std::fs::write(work.path().join(format!("f{i}")), format!("{i}\n")).unwrap();
            run_git(work.path(), &["add", "."]);
            run_git(work.path(), &["commit", "-q", "-m", &format!("c{i}")]);
            let c = run_git(work.path(), &["rev-parse", "HEAD"]);
            let out = std::process::Command::new("sh")
                .arg("-c")
                .arg(format!(
                    "git rev-list --objects {c} {} | git pack-objects --stdout",
                    if prev.is_empty() {
                        String::new()
                    } else {
                        format!("^{prev}")
                    }
                ))
                .current_dir(work.path())
                .output()
                .unwrap();
            let ingested = handle
                .local()
                .ingest_pack(
                    std::io::Cursor::new(out.stdout),
                    walgit_git::IngestOptions {
                        fsck: false,
                        max_bytes: None,
                        thin: false,
                    },
                )
                .await
                .unwrap()
                .unwrap();
            handle
                .publish_push(
                    Some(ingested),
                    walgit_proto::v1::RefTransaction {
                        updates: vec![walgit_proto::v1::RefUpdate {
                            name: "refs/heads/main".into(),
                            old_oid: prev.clone(),
                            new_oid: c.clone(),
                            new_symbolic_target: String::new(),
                            new_peeled: String::new(),
                        }],
                        push_options: vec![],
                        atomic: true,
                    },
                    HashMap::new(),
                )
                .await
                .unwrap();
            prev = c.clone();
            tips.push(c);
        }
        // Compact into a base (seq 4) and checkpoint there.
        let repack = handle
            .local()
            .repack(walgit_git::RepackOptions {
                mode: walgit_git::RepackMode::Full,
                write_bitmap: false,
                write_midx: false,
                keep: vec![],
            })
            .await
            .unwrap();
        let base = repack.new_packs[0].clone();
        let base_seq = handle
            .publish_compact(base, repack.removed.clone(), 2)
            .await
            .unwrap();
        assert_eq!(base_seq, 4);
        handle.write_checkpoint().await.unwrap();
        Fixture {
            cfg,
            store,
            writer: registry,
            id,
            tips,
            base_seq,
            _cache: cache,
        }
    }

    /// Cold rewind: a registry with an empty cache materializes seq 2 (before
    /// the compaction: packs fetched from the store) and head (after it) into
    /// fresh directories; refs and objects match, the live copy is untouched.
    #[tokio::test]
    async fn materialize_at_seq_fetches_packs_from_the_store() {
        let f = fixture().await;
        let (cold, _cold_cache) = f.cold();
        let outs = tempfile::tempdir().unwrap();

        // Head: from the checkpoint (base pack fetched from the store).
        let out_head = outs.path().join("head");
        let snap = materialize_at(
            cold.clone(),
            f.id.clone(),
            f.base_seq,
            out_head.clone(),
            silent(),
        )
        .await
        .unwrap();
        let g = out_head.join("t").join("rewind.git");
        assert_eq!(snap.git_dir, g.display().to_string());
        assert_eq!((snap.at_seq, snap.head_seq), (f.base_seq, f.base_seq));
        assert_eq!(snap.from_seq, f.base_seq, "the checkpoint is at the head");
        assert_eq!(run_git(&g, &["rev-parse", "refs/heads/main"]), f.tips[2]);
        assert_eq!(
            snap.refs
                .iter()
                .map(|r| (r.name.as_str(), r.sha.as_str()))
                .collect::<Vec<_>>(),
            vec![("refs/heads/main", f.tips[2].as_str())]
        );
        assert_eq!(snap.packs.len(), 1);
        assert_eq!(snap.packs[0].source, "store");
        assert!(
            std::process::Command::new("git")
                .current_dir(&g)
                .args(["fsck", "--connectivity-only"])
                .status()
                .unwrap()
                .success()
        );
        assert_eq!(
            std::fs::read_dir(g.join("objects/pack"))
                .unwrap()
                .filter(|e| e
                    .as_ref()
                    .unwrap()
                    .path()
                    .extension()
                    .is_some_and(|x| x == "pack"))
                .count(),
            1
        );

        // Seq 2: before the compaction; the two push packs still exist in the
        // store (retention) and refs replay to the second commit.
        let out_2 = outs.path().join("two");
        let snap2 = materialize_at(cold.clone(), f.id.clone(), 2, out_2.clone(), silent())
            .await
            .unwrap();
        let g2 = out_2.join("t").join("rewind.git");
        assert_eq!(run_git(&g2, &["rev-parse", "refs/heads/main"]), f.tips[1]);
        assert_eq!(snap2.from_seq, 0, "no checkpoint at or before seq 2");
        assert_eq!(snap2.entries, 2);
        assert_eq!(snap2.packs.len(), 2);
        assert!(
            std::process::Command::new("git")
                .current_dir(&g2)
                .args(["fsck", "--connectivity-only"])
                .status()
                .unwrap()
                .success()
        );

        // The writer's live copy kept its packs and its head.
        let live = f.writer.open(&f.id).await.unwrap();
        assert!(!live.local().packs().unwrap().is_empty());
        assert_eq!(live.manifest().head_seq, f.base_seq);
        assert_eq!(
            run_git(live.local().path(), &["rev-parse", "refs/heads/main"]),
            f.tips[2]
        );

        // An existing output directory is refused, not overwritten.
        let err = materialize_at(cold, f.id.clone(), 2, out_2, silent())
            .await
            .unwrap_err();
        assert!(matches!(err, WalError::Invalid(_)), "{err}");
    }

    /// `snapshot_at` owns the cache-dir layout: the same seq twice builds once,
    /// a tree without its marker is rebuilt, and a bad `at_seq` is rejected
    /// before anything is written.
    #[tokio::test]
    async fn snapshot_at_is_idempotent_and_validates_the_seq() {
        let f = fixture().await;
        let (cold, cold_cache) = f.cold();

        let (snap, built) = snapshot_at(cold.clone(), f.id.clone(), 2, silent())
            .await
            .unwrap();
        assert!(built);
        assert_eq!(snap.at_seq, 2);
        assert_eq!(snap.head_seq, f.base_seq);
        assert_eq!(snap.refs[0].sha, f.tips[1]);
        let dir = snapshot_dir(cold_cache.path(), &f.id, 2);
        assert!(dir.join(MARKER).is_file());
        assert_eq!(
            run_git(Path::new(&snap.git_dir), &["rev-parse", "HEAD"]),
            f.tips[1]
        );

        // Second call: the marker is trusted, nothing is rebuilt.
        let (again, built) = snapshot_at(cold.clone(), f.id.clone(), 2, silent())
            .await
            .unwrap();
        assert!(!built);
        assert_eq!(again.built_at, snap.built_at);

        // A tree without the marker is a partial run: rebuilt.
        std::fs::remove_file(dir.join(MARKER)).unwrap();
        assert!(read_snapshot(&dir).is_none());
        let (_, built) = snapshot_at(cold.clone(), f.id.clone(), 2, silent())
            .await
            .unwrap();
        assert!(built);

        for bad in [0, f.base_seq + 1] {
            let err = snapshot_at(cold.clone(), f.id.clone(), bad, silent())
                .await
                .unwrap_err();
            assert!(matches!(err, WalError::Invalid(_)), "at_seq {bad}: {err}");
            assert!(!snapshot_dir(cold_cache.path(), &f.id, bad).exists());
        }
    }
}
