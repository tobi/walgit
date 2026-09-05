// Test fixtures use panics to fail the test, including shared helper functions.
#![allow(clippy::expect_used, clippy::indexing_slicing, clippy::unwrap_used)]

//! Simulation tests: safety mode → liveness mode (after `TigerBeetle`'s VOPR,
//! "Simulation Testing For Liveness", 2023).
//!
//! A *cluster* is N walgit instances (one `Registry` + cache dir each) that
//! share one in-memory bucket, each through its own fault-injecting link
//! (`walgit_store::fault::FaultStore`). Safety mode rolls the dice on every
//! store op of every link while pushers hammer the WAL. Liveness mode then
//! picks a **core** of instances, heals their links, **freezes** every other
//! link in whatever broken state it is in (black hole, stale-forever, always
//! 412, crashed-mid-CAS, lease holder gone) and demands that the core still
//! converges within a bound:
//!
//! * a push on a core instance is acknowledged,
//! * every core instance syncs to the same head and the same refs,
//! * compaction and checkpoints complete on the core,
//! * a brand-new instance cold-starts from the bucket and sees everything,
//! * and the **truth** (the bucket itself) stays consistent: every ACK'd push
//!   is in the log at its seq with its txn; every pack/segment/checkpoint the
//!   manifest references exists.
//!
//! The bucket is never touched by faults directly (faults live on links), so
//! the truth store is the oracle. Seeds: `WALGIT_SIM_SEED` (one run) or
//! `WALGIT_SIM_SEEDS` (count, default 2). Size: `WALGIT_SIM_PUSHES` per pusher.
//! Failing runs print the link traces and the seed.

use std::collections::{BTreeMap, HashMap};
use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail, ensure};
use prost::Message;
use walgit_git::{IngestOptions, ObjectFormat, RepoId};
use walgit_proto::v1::{EntryKind, Manifest, RefTransaction, RefUpdate};
use walgit_store::fault::{FaultPlan, FaultStore};
use walgit_store::memory::MemoryStore;
use walgit_store::{DynStore, ObjectStoreExt};
use walgit_wal::{Registry, RepoHandle, WalError};

// ---------------------------------------------------------------------------
// Git work repo (a pusher's clone)
// ---------------------------------------------------------------------------

struct WorkRepo {
    dir: tempfile::TempDir,
}

impl WorkRepo {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        for args in [
            vec!["init", "-q"],
            vec!["config", "user.name", "Sim"],
            vec!["config", "user.email", "sim@test"],
            vec!["config", "commit.gpgsign", "false"],
        ] {
            let o = Command::new("git")
                .args(&args)
                .current_dir(dir.path())
                .output()
                .unwrap();
            assert!(
                o.status.success(),
                "git {args:?}: {}",
                String::from_utf8_lossy(&o.stderr)
            );
        }
        WorkRepo { dir }
    }
    fn path(&self) -> &Path {
        self.dir.path()
    }
    fn commit(&self, n: u64, salt: &str) -> String {
        std::fs::write(
            self.path().join(format!("f{}.txt", n % 7)),
            format!("{salt}-{n}\n"),
        )
        .unwrap();
        for args in [
            vec!["add", "."],
            vec![
                "commit",
                "-q",
                "--allow-empty",
                "-m",
                &format!("{salt} {n}"),
            ],
        ] {
            let o = Command::new("git")
                .args(&args)
                .current_dir(self.path())
                .output()
                .unwrap();
            assert!(
                o.status.success(),
                "git {args:?}: {}",
                String::from_utf8_lossy(&o.stderr)
            );
        }
        let out = Command::new("git")
            .args(["rev-parse", "HEAD"])
            .current_dir(self.path())
            .output()
            .unwrap();
        String::from_utf8(out.stdout).unwrap().trim().to_string()
    }
    /// Self-contained pack with everything reachable from `head` minus `base`.
    fn pack(&self, head: &str, base: Option<&str>) -> Vec<u8> {
        let mut revs = format!("{head}\n");
        if let Some(b) = base {
            {
                let _ = std::fmt::Write::write_fmt(&mut revs, format_args!("^{b}\n"));
            };
        }
        let mut child = Command::new("git")
            .args(["pack-objects", "--stdout", "--revs", "-q"])
            .current_dir(self.path())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        child
            .stdin
            .take()
            .unwrap()
            .write_all(revs.as_bytes())
            .unwrap();
        let out = child.wait_with_output().unwrap();
        assert!(
            out.status.success(),
            "pack-objects: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        out.stdout
    }
}

// ---------------------------------------------------------------------------
// Cluster
// ---------------------------------------------------------------------------

fn sim_config(cache_dir: &Path) -> walgit_config::Config {
    let mut cfg = walgit_config::Config::default();
    cfg.cache.dir = cache_dir.to_path_buf();
    cfg.wal.batch_window = Duration::from_millis(5);
    cfg.wal.freshness_ttl = Duration::ZERO;
    cfg.wal.fsck_objects = false;
    cfg.wal.check_connectivity = false;
    cfg.wal.snapshot_every_entries = 0;
    cfg.wal.checkpoint_interval = Duration::ZERO;
    cfg.wal.checkpoint_tail_bytes = walgit_config::ByteSize::b(0);
    cfg.compaction.lease_ttl = Duration::from_secs(2);
    cfg.compaction.trigger_packs = 4;
    // The simulator exercises WAL publication, not derived-index CPU work.
    // History-pack/commit-graph builders can dominate tiny zero-latency store
    // runs and obscure the liveness bound without injecting another fault.
    cfg.git.history_pack = false;
    cfg.git.commit_graph = false;
    cfg.store.bucket = "sim".to_string();
    cfg
}

struct Instance {
    name: String,
    link: Arc<FaultStore>,
    registry: Arc<Registry>,
    cfg: Arc<walgit_config::Config>,
    cache: tempfile::TempDir,
}

impl Instance {
    fn new(
        truth: &DynStore,
        name: &str,
        seed: u64,
        tweak: &dyn Fn(&mut walgit_config::Config),
    ) -> Self {
        Self::new_at(truth, name, seed, tempfile::tempdir().unwrap(), tweak)
    }
    /// Same, on an existing cache dir (a restart that keeps "disk": the SSD host's /data survives the container).
    fn new_at(
        truth: &DynStore,
        name: &str,
        seed: u64,
        cache: tempfile::TempDir,
        tweak: &dyn Fn(&mut walgit_config::Config),
    ) -> Self {
        let mut cfg = sim_config(cache.path());
        tweak(&mut cfg);
        let cfg = Arc::new(cfg);
        let link = FaultStore::new(truth.clone(), name, seed);
        link.set_trace(true);
        let registry = Registry::new(link.clone() as DynStore, cfg.clone());
        Instance {
            name: name.to_string(),
            link,
            registry,
            cfg,
            cache,
        }
    }
    async fn open(&self, id: &RepoId) -> Result<Arc<RepoHandle>> {
        Ok(self.registry.open(id).await?)
    }
}

struct Cluster {
    #[allow(dead_code)]
    seed: u64,
    truth: DynStore,
    id: RepoId,
    instances: Vec<Instance>,
    next_link_seed: AtomicU64,
}

impl Cluster {
    async fn new(seed: u64, n: usize) -> Result<Self> {
        let truth: DynStore = MemoryStore::shared();
        let id = RepoId::new("sim", format!("r{seed}"))?;
        let mut c = Cluster {
            seed,
            truth,
            id,
            instances: Vec::new(),
            next_link_seed: AtomicU64::new(seed * 1000),
        };
        for i in 0..n {
            c.add_instance(&format!("i{i}"), &|_| {});
        }
        // Create through a healthy link.
        c.instances[0]
            .registry
            .create(&c.id, ObjectFormat::Sha1)
            .await?;
        Ok(c)
    }
    fn add_instance(&mut self, name: &str, tweak: &dyn Fn(&mut walgit_config::Config)) -> usize {
        let s = self.next_link_seed.fetch_add(1, Ordering::Relaxed);
        self.instances
            .push(Instance::new(&self.truth, name, s, tweak));
        self.instances.len() - 1
    }
    /// "Crash" an instance: drop its registry/cache, bring a fresh one up on a
    /// fresh link with the same name.
    fn restart(&mut self, i: usize) {
        let name = self.instances[i].name.clone();
        let s = self.next_link_seed.fetch_add(1, Ordering::Relaxed);
        let fresh = Instance::new(&self.truth, &name, s, &|_| {});
        let old = std::mem::replace(&mut self.instances[i], fresh);
        drop(old);
    }
    /// Restart keeping the cache dir (disk survives) and the config shape: the D31 deploy restart
    /// of a disk-mode host.
    fn restart_keep_disk(&mut self, i: usize, tweak: &dyn Fn(&mut walgit_config::Config)) {
        let name = self.instances[i].name.clone();
        let s = self.next_link_seed.fetch_add(1, Ordering::Relaxed);
        // Take the cache dir out of the old instance without dropping it.
        let placeholder = tempfile::tempdir().unwrap();
        let cache = std::mem::replace(&mut self.instances[i].cache, placeholder);
        let fresh = Instance::new_at(&self.truth, &name, s, cache, tweak);
        let old = std::mem::replace(&mut self.instances[i], fresh);
        drop(old);
    }
    /// A throwaway healthy observer (fresh cache, no faults) for oracles.
    fn observer(&self) -> Instance {
        Instance::new(&self.truth, "observer", 0, &|_| {})
    }
    fn repo_prefix(&self) -> String {
        format!("repos/{}/{}/", self.id.owner(), self.id.name())
    }
    async fn truth_manifest(&self) -> Result<Manifest> {
        let key = format!("{}manifest.pb", self.repo_prefix());
        let (_, b) = self
            .truth
            .get_bytes(&key)
            .await?
            .ok_or_else(|| anyhow!("manifest missing in truth"))?;
        Ok(Manifest::decode(b)?)
    }
    fn dump_traces(&self) -> String {
        let mut s = String::new();
        for i in &self.instances {
            {
                let _ = std::fmt::Write::write_fmt(
                    &mut s,
                    format_args!("--- link {} ({})\n", i.name, i.link.stats().summary()),
                );
            };
            for l in i
                .link
                .take_trace()
                .iter()
                .rev()
                .take(40)
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
            {
                s.push_str(l);
                s.push('\n');
            }
        }
        s
    }
}

/// `BundleSource` adapter used by the bundle-lease liveness scenario.
struct SimBundleSource(Arc<Registry>);

#[async_trait::async_trait]
impl walgit_bundle::BundleSource for SimBundleSource {
    async fn open_repo(
        &self,
        id: &RepoId,
    ) -> Result<walgit_bundle::BundleRepoHandle, walgit_bundle::BundleError> {
        let h = self
            .0
            .open(id)
            .await
            .map_err(|e| walgit_bundle::BundleError::Other(e.to_string()))?;
        Ok(walgit_bundle::BundleRepoHandle {
            local: h.local().clone(),
            store: h.store().clone(),
            head_seq: h.manifest().head_seq,
            engine: walgit_bundle::BundleEngine::Git,
            cfg: Some(h.effective_config()),
        })
    }

    async fn prepare_objects(&self, id: &RepoId) -> Result<(), walgit_bundle::BundleError> {
        let h = self
            .0
            .open(id)
            .await
            .map_err(|e| walgit_bundle::BundleError::Other(e.to_string()))?;
        drop(
            h.sync_full()
                .await
                .map_err(|e| walgit_bundle::BundleError::Other(e.to_string()))?,
        );
        Ok(())
    }

    async fn list_repos(&self) -> Result<Vec<RepoId>, walgit_bundle::BundleError> {
        Ok(Vec::new())
    }
}

// ---------------------------------------------------------------------------
// Pushers
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
struct Acked {
    seq: u64,
    refname: String,
    old: String,
    new: String,
}

struct Pusher {
    idx: usize,
    work: WorkRepo,
    refname: String,
    n: u64,
    tip: String,
    acked: Vec<Acked>,
    errors: Vec<String>,
    rejected: u64,
}

impl Pusher {
    fn new(idx: usize) -> Self {
        Pusher {
            idx,
            work: WorkRepo::new(),
            refname: format!("refs/heads/p{idx}"),
            n: 0,
            tip: String::new(),
            acked: Vec::new(),
            errors: Vec::new(),
            rejected: 0,
        }
    }

    /// One push of one new commit through `inst`. Returns Ok(true) when
    /// acknowledged, Ok(false) when rejected/errored (recorded), Err only for
    /// harness bugs.
    async fn push_once(&mut self, inst: &Instance, id: &RepoId, timeout: Duration) -> Result<bool> {
        self.n += 1;
        let new = self.work.commit(self.n, &format!("p{}", self.idx));
        let handle = match tokio::time::timeout(timeout, inst.open(id)).await {
            Ok(Ok(h)) => h,
            Ok(Err(e)) => {
                self.errors.push(format!("open: {e}"));
                self.n -= 1;
                return Ok(false);
            }
            Err(_) => {
                self.errors.push("open: timeout".into());
                self.n -= 1;
                return Ok(false);
            }
        };
        // What does this instance believe our ref is? (refs-level sync)
        let current = match tokio::time::timeout(timeout, handle.sync_refs()).await {
            Ok(Ok(g)) => {
                drop(g);
                handle
                    .local()
                    .refs()
                    .ok()
                    .and_then(|s| {
                        s.refs
                            .into_iter()
                            .find(|r| r.name == self.refname)
                            .map(|r| r.oid)
                    })
                    .unwrap_or_default()
            }
            Ok(Err(e)) => {
                self.errors.push(format!("sync_refs: {e}"));
                self.n -= 1;
                return Ok(false);
            }
            Err(_) => {
                self.errors.push("sync_refs: timeout".into());
                self.n -= 1;
                return Ok(false);
            }
        };
        let base = if current.is_empty() {
            None
        } else {
            Some(current.as_str())
        };
        let pack = self.work.pack(&new, base);
        let ingested = match tokio::time::timeout(
            timeout,
            handle.local().ingest_pack(
                std::io::Cursor::new(pack),
                IngestOptions {
                    fsck: false,
                    max_bytes: None,
                    thin: false,
                },
            ),
        )
        .await
        {
            Ok(Ok(Some(p))) => Some(p),
            Ok(Ok(None)) => None,
            Ok(Err(e)) => {
                self.errors.push(format!("ingest: {e}"));
                self.n -= 1;
                return Ok(false);
            }
            Err(_) => {
                self.errors.push("ingest: timeout".into());
                self.n -= 1;
                return Ok(false);
            }
        };
        let txn = RefTransaction {
            updates: vec![RefUpdate {
                name: self.refname.clone(),
                old_oid: current.clone(),
                new_oid: new.clone(),
                new_symbolic_target: String::new(),
                new_peeled: String::new(),
            }],
            push_options: vec![],
            atomic: true,
        };
        // We performed the request freshness sync above, exactly like
        // receive-pack. Reuse it: publish_push() would add a simulator-only
        // second conditional manifest GET to every healthy push.
        match tokio::time::timeout(
            timeout,
            handle.publish_push_synced(ingested, txn, HashMap::new()),
        )
        .await
        {
            Ok(Ok(res)) if res.per_ref.iter().all(|(_, r)| r.is_ok()) => {
                self.acked.push(Acked {
                    seq: res.seq,
                    refname: self.refname.clone(),
                    old: current,
                    new: new.clone(),
                });
                self.tip = new;
                Ok(true)
            }
            Ok(Ok(res)) => {
                self.rejected += 1;
                self.errors.push(format!("rejected: {:?}", res.per_ref));
                Ok(false)
            }
            Ok(Err(e)) => {
                self.errors.push(format!("publish: {e}"));
                Ok(false)
            }
            Err(_) => {
                self.errors.push("publish: timeout".into());
                Ok(false)
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Oracles
// ---------------------------------------------------------------------------

/// Truth-level safety: log is dense and consistent with every ACK; every
/// object the manifest references exists in the bucket.
async fn check_truth(c: &Cluster, pushers: &[Pusher]) -> Result<()> {
    let manifest = c.truth_manifest().await?;
    let obs = c.observer();
    let handle = obs.open(&c.id).await?;
    // The checkpoint (if any) folds the log prefix: refs from its RefSnapshot,
    // entries after it from the tail. Both must exist in the bucket.
    let prefix = c.repo_prefix();
    let cp_seq = manifest.checkpoint.as_ref().map_or(0, |cp| cp.seq);
    let mut folded: HashMap<String, String> = HashMap::new();
    if cp_seq > 0 {
        let key = format!(
            "{prefix}{}",
            walgit_proto::keys::checkpoint_refs_key(cp_seq)
        );
        let (_, b) = c
            .truth
            .get_bytes(&key)
            .await?
            .ok_or_else(|| anyhow!("checkpoint refs missing: {key}"))?;
        let snap = walgit_proto::v1::RefSnapshot::decode(b)?;
        for r in snap.refs {
            folded.insert(r.name, r.oid);
        }
    }
    let log = handle
        .read_log(cp_seq + 1, None)
        .await
        .context("observer read_log")?;
    ensure!(
        !log.is_empty() || manifest.head_seq == cp_seq,
        "empty log tail with head_seq {} > checkpoint {cp_seq}",
        manifest.head_seq
    );
    // Strictly increasing (gaps are allowed: a seq is burned when a crashed
    // writer left an orphan segment at the head), reaching head.
    for w in log.windows(2) {
        ensure!(
            w[0].seq < w[1].seq,
            "log not strictly increasing: {} then {}",
            w[0].seq,
            w[1].seq
        );
    }
    ensure!(
        log.first().is_none_or(|e| e.seq > cp_seq),
        "log tail starts at {} <= checkpoint {cp_seq}",
        log[0].seq
    );
    ensure!(
        log.last().map_or(cp_seq, |e| e.seq) == manifest.head_seq,
        "log tail {} != manifest.head_seq {}",
        log.last().map_or(cp_seq, |e| e.seq),
        manifest.head_seq
    );
    // Every ACK after the checkpoint is in the log at its seq with its txn.
    let by_seq: BTreeMap<u64, _> = log.iter().map(|e| (e.seq, e)).collect();
    for p in pushers {
        for a in p.acked.iter().filter(|a| a.seq > cp_seq) {
            let e = by_seq.get(&a.seq).ok_or_else(|| {
                anyhow!("acked seq {} missing from log (pusher {})", a.seq, p.idx)
            })?;
            ensure!(
                e.kind == EntryKind::Push as i32,
                "seq {} is not a PUSH",
                a.seq
            );
            let txn = e
                .txn
                .as_ref()
                .ok_or_else(|| anyhow!("seq {} has no txn", a.seq))?;
            let u = txn
                .updates
                .iter()
                .find(|u| u.name == a.refname)
                .ok_or_else(|| anyhow!("seq {} lacks {}", a.seq, a.refname))?;
            ensure!(
                u.old_oid == a.old && u.new_oid == a.new,
                "seq {} txn {:?} != ack {:?}",
                a.seq,
                u,
                a
            );
        }
    }
    // Folded refs: each pusher's ref at its last ACK'd tip unless a later
    // (errored-but-committed) push moved it further along the same chain.
    for e in &log {
        if let Some(t) = &e.txn {
            for u in &t.updates {
                folded.insert(u.name.clone(), u.new_oid.clone());
            }
        }
    }
    for p in pushers {
        if let Some(last) = p.acked.last() {
            let f = folded.get(&p.refname).cloned().unwrap_or_default();
            ensure!(!f.is_empty(), "ref {} vanished from fold", p.refname);
            // f must be last.new or a commit pushed after it (the ack'd or an
            // errored-but-committed push along the same chain).
            let later = log.iter().filter(|e| e.seq > last.seq).any(|e| {
                e.txn.as_ref().is_some_and(|t| {
                    t.updates
                        .iter()
                        .any(|u| u.name == p.refname && u.new_oid == f)
                })
            }) || last.seq <= cp_seq;
            ensure!(
                f == last.new || later,
                "ref {} folded to {f}, last ack {}",
                p.refname,
                last.new
            );
        }
    }
    // Referenced objects exist in truth.
    for pk in &manifest.packs {
        for key in [
            walgit_proto::keys::pack_key(&pk.checksum),
            walgit_proto::keys::idx_key(&pk.checksum),
        ] {
            ensure!(
                c.truth.exists(&format!("{prefix}{key}")).await?,
                "manifest pack side-file missing: {key}"
            );
        }
    }
    for seg in &manifest.log_segments {
        ensure!(
            c.truth.exists(&format!("{prefix}{}", seg.key)).await?,
            "manifest log segment missing: {}",
            seg.key
        );
    }
    if let Some(cp) = &manifest.checkpoint {
        for key in [
            walgit_proto::keys::checkpoint_key(cp.seq),
            walgit_proto::keys::checkpoint_refs_key(cp.seq),
        ] {
            ensure!(
                c.truth.exists(&format!("{prefix}{key}")).await?,
                "checkpoint object missing: {key}"
            );
        }
    }
    // Every live pack's objects: every folded ref tip is an object the observer
    // can materialize (full sync through a clean link).
    let _g = handle.sync_full().await.context("observer sync_full")?;
    for (name, oid) in &folded {
        let id = gix_hash::ObjectId::from_hex(oid.as_bytes())?;
        ensure!(
            handle.local().has_object(&id),
            "observer lacks tip {oid} of {name} after full sync"
        );
    }
    Ok(())
}

/// Liveness of the core: pushes ACK, syncs converge, maintenance completes,
/// a cold instance comes up. `bound` is the wall-clock budget per step.
async fn check_core_liveness(
    c: &mut Cluster,
    core: &[usize],
    pushers: &mut [Pusher],
    bound: Duration,
) -> Result<()> {
    eprintln!("liveness: step 1 core pushes");
    // 1. A push on every core instance is acknowledged (retrying is allowed: a
    //    core instance may need a moment to resync after its link healed, but
    //    it must get there).
    for (k, &i) in core.iter().enumerate() {
        let p = &mut pushers[k % pushers.len()];
        let t = Instant::now();
        let mut ok = false;
        while t.elapsed() < bound {
            // `push_once` has per-stage timeouts; cap the whole attempt too so
            // retries cannot multiply the liveness bound (sync + ingest +
            // publish) and turn a failing seed into a hung test.
            let remaining = bound.saturating_sub(t.elapsed());
            match tokio::time::timeout(remaining, p.push_once(&c.instances[i], &c.id, remaining))
                .await
            {
                Ok(Ok(true)) => {
                    ok = true;
                    break;
                }
                Ok(Ok(false)) => {}
                Ok(Err(e)) => return Err(e),
                Err(_) => p.errors.push("whole push attempt: timeout".into()),
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        ensure!(
            ok,
            "liveness: push on core instance {} never acknowledged within {bound:?}; last errors: {:?}",
            c.instances[i].name,
            p.errors.iter().rev().take(3).collect::<Vec<_>>()
        );
    }
    eprintln!("liveness: step 2 converge refs");
    // 2. Every core instance syncs to the truth head and agrees on refs.
    let truth = c.truth_manifest().await?;
    let mut views = Vec::new();
    for &i in core {
        let h = c.instances[i].open(&c.id).await?;
        let t = Instant::now();
        loop {
            match tokio::time::timeout(bound, h.sync_refs()).await {
                Ok(Ok(g)) => {
                    drop(g);
                    if h.applied_seq() >= truth.head_seq {
                        break;
                    }
                }
                Ok(Err(e)) => tracing::warn!("core {} sync_refs: {e}", c.instances[i].name),
                Err(_) => bail!(
                    "liveness: core {} sync_refs hung > {bound:?}",
                    c.instances[i].name
                ),
            }
            ensure!(
                t.elapsed() < bound,
                "liveness: core {} stuck at seq {} < truth {}",
                c.instances[i].name,
                h.applied_seq(),
                truth.head_seq
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let refs: BTreeMap<String, String> = h
            .local()
            .refs()?
            .refs
            .into_iter()
            .map(|r| (r.name, r.oid))
            .collect();
        views.push((c.instances[i].name.clone(), refs));
    }
    for w in views.windows(2) {
        ensure!(
            w[0].1 == w[1].1,
            "liveness: core refs diverge between {} and {}",
            w[0].0,
            w[1].0
        );
    }
    eprintln!("liveness: step 3 checkpoint + compaction");
    // 3. Maintenance on the core: checkpoint + forced compaction.
    let h = c.instances[core[0]].open(&c.id).await?;
    tokio::time::timeout(bound, h.write_checkpoint())
        .await
        .map_err(|_| anyhow!("liveness: checkpoint hung"))??;
    eprintln!("liveness: checkpoint complete; starting compaction");
    let t = Instant::now();
    loop {
        let out = tokio::time::timeout(
            bound,
            walgit_server::ops::compact_repo(
                &h,
                &c.instances[core[0]].cfg,
                walgit_server::ops::CompactRequest {
                    force: true,
                    rebuild_base: false,
                },
                &walgit_server::ops::noop_log,
            ),
        )
        .await
        .map_err(|_| anyhow!("liveness: compaction hung > {bound:?}"))?;
        match out {
            Ok(
                walgit_server::ops::CompactOutcome::Published { .. }
                | walgit_server::ops::CompactOutcome::NotTriggered { .. },
            ) => break,
            Ok(walgit_server::ops::CompactOutcome::LeaseHeld) => {
                ensure!(
                    t.elapsed() < bound,
                    "liveness: compaction lease never became available within {bound:?}"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(e) => {
                ensure!(
                    t.elapsed() < bound,
                    "liveness: compaction kept failing within {bound:?}: {e}"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
    eprintln!("liveness: step 4 cold start");
    // 4. A cold instance comes up from the bucket alone.
    let cold = c.add_instance("cold", &|_| {});
    let h = c.instances[cold].open(&c.id).await?;
    tokio::time::timeout(bound, h.sync_full())
        .await
        .map_err(|_| anyhow!("liveness: cold sync_full hung"))??;
    let truth = c.truth_manifest().await?;
    ensure!(
        h.applied_seq() == truth.head_seq,
        "cold instance at {} != truth {}",
        h.applied_seq(),
        truth.head_seq
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Scenarios
// ---------------------------------------------------------------------------

fn seeds() -> Vec<u64> {
    if let Ok(s) = std::env::var("WALGIT_SIM_SEED") {
        return vec![s.parse().expect("WALGIT_SIM_SEED")];
    }
    let n: u64 = std::env::var("WALGIT_SIM_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2);
    (1..=n).map(|i| 0x00C0_FFEE + i * 7_919).collect()
}
fn pushes_per_pusher() -> u64 {
    std::env::var("WALGIT_SIM_PUSHES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(12)
}

struct Lcg(u64);
impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0 >> 33
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
    fn below_usize(&mut self, n: usize) -> usize {
        let bound = u64::try_from(n).expect("usize always fits in u64");
        usize::try_from(self.below(bound)).expect("random value is less than the usize bound")
    }
    fn chance(&mut self, p: f64) -> bool {
        let sample = u32::try_from(self.next()).expect("LCG output is limited to 31 bits");
        (f64::from(sample) / f64::from(1u32 << 31)) < p
    }
}

/// The general run: chaos on every link while P pushers hammer N instances
/// (with crash/restarts), then liveness with a random core.
async fn run_safety_then_liveness(seed: u64) -> Result<()> {
    let n_instances = 4;
    let n_pushers = 3;
    let mut c = Cluster::new(seed, n_instances).await?;
    let mut rng = Lcg(seed);
    let mut pushers: Vec<Pusher> = (0..n_pushers).map(Pusher::new).collect();

    // Safety mode: chaos everywhere (moderate so that pushes still land).
    for inst in &c.instances {
        inst.link.set(FaultPlan::chaos(0.04));
    }
    let per = pushes_per_pusher();
    let op_timeout = Duration::from_secs(10);
    for round in 0..per {
        for p in &mut pushers {
            let i = rng.below_usize(n_instances);
            let _ = p.push_once(&c.instances[i], &c.id, op_timeout).await?;
        }
        // Random crash: replace an instance (its in-flight state is gone).
        if rng.chance(0.2) {
            let i = rng.below_usize(n_instances);
            c.restart(i);
            c.instances[i].link.set(FaultPlan::chaos(0.04));
        }
        // Occasionally somebody checkpoints or compacts under chaos.
        if round % 4 == 3 {
            let i = rng.below_usize(n_instances);
            if let Ok(h) = c.instances[i].open(&c.id).await {
                let _ = tokio::time::timeout(op_timeout, h.write_checkpoint()).await;
                let cfg = c.instances[i].cfg.clone();
                let _ = tokio::time::timeout(
                    op_timeout,
                    walgit_server::ops::compact_repo(
                        &h,
                        &cfg,
                        walgit_server::ops::CompactRequest {
                            force: rng.chance(0.5),
                            rebuild_base: false,
                        },
                        &walgit_server::ops::noop_log,
                    ),
                )
                .await;
            }
        }
    }
    let acked: usize = pushers.iter().map(|p| p.acked.len()).sum();
    let errs: usize = pushers.iter().map(|p| p.errors.len()).sum();
    eprintln!("[seed {seed}] safety phase: {acked} acked, {errs} errored/rejected pushes");
    ensure!(acked > 0, "chaos too strong: nothing was ever acknowledged");

    // Truth must be consistent at the end of safety mode already.
    if let Err(e) = check_truth(&c, &pushers).await {
        eprintln!("{}", c.dump_traces());
        return Err(e.context("truth after safety mode"));
    }

    // Liveness mode: pick a core of 2, heal it, freeze the rest in nasty states.
    let mut idx: Vec<usize> = (0..n_instances).collect();
    for k in (1..idx.len()).rev() {
        let j = rng.below_usize(k + 1);
        idx.swap(k, j);
    }
    let core = &idx[..2];
    for &i in core {
        c.instances[i].link.heal();
    }
    let link_delay = Some((Duration::from_millis(1), Duration::from_millis(2)));
    let frozen: Vec<FaultPlan> = vec![
        FaultPlan::black_hole(),
        FaultPlan {
            p_stale_304: 1.0,
            delay: link_delay,
            ..Default::default()
        },
        FaultPlan {
            p_cas_fail: 1.0,
            delay: link_delay,
            ..Default::default()
        },
        FaultPlan {
            p_err_after: 1.0,
            delay: link_delay,
            ..Default::default()
        },
    ];
    for (k, &i) in idx[2..].iter().enumerate() {
        c.instances[i]
            .link
            .set(frozen[(k + rng.below_usize(4)) % frozen.len()].clone());
    }
    // Non-core pushers keep hammering the frozen links in the background (they
    // may never interfere with the core).
    let bg: Vec<_> = idx[2..]
        .iter()
        .map(|&i| {
            let link = c.instances[i].link.clone();
            let reg = c.instances[i].registry.clone();
            let id = c.id.clone();
            tokio::spawn(async move {
                let mut p = Pusher::new(90 + i);
                let inst = Instance {
                    name: "bg".into(),
                    link,
                    registry: reg,
                    cfg: Arc::new(sim_config(Path::new("/nonexistent"))),
                    cache: tempfile::tempdir().unwrap(),
                };
                for _ in 0..20 {
                    let _ = p.push_once(&inst, &id, Duration::from_millis(500)).await;
                    // MemoryStore completes operations without network latency.
                    // Yield between requests so a stale link models a busy
                    // client, not an impossible zero-latency CPU denial loop
                    // that monopolizes a Tokio worker and the truth mutex.
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
        })
        .collect();

    let bound = Duration::from_secs(20);
    let core_names: Vec<_> = core.iter().map(|&i| c.instances[i].name.clone()).collect();
    eprintln!("[seed {seed}] liveness phase: core = {core_names:?}");
    let res = check_core_liveness(&mut c, core, &mut pushers, bound).await;
    for b in bg {
        b.abort();
    }
    res.with_context(|| format!("liveness with core {core_names:?}"))?;
    eprintln!("[seed {seed}] final truth oracle");
    tokio::time::timeout(bound, check_truth(&c, &pushers))
        .await
        .map_err(|_| anyhow!("truth oracle hung after liveness mode > {bound:?}"))?
        .context("truth after liveness mode")?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sim_safety_then_liveness() {
    for seed in seeds() {
        let t = Instant::now();
        let r = run_safety_then_liveness(seed).await;
        eprintln!(
            "[seed {seed}] {:?} in {:.1}s",
            r.as_ref().map(|()| "ok"),
            t.elapsed().as_secs_f64()
        );
        if let Err(e) = r {
            panic!(
                "seed {seed} failed: {e:#}\nreproduce: WALGIT_SIM_SEED={seed} cargo test -p walgit-server --test sim sim_safety_then_liveness -- --nocapture"
            );
        }
    }
}

/// Liveness 1: the compaction lease holder dies (crash between acquire and
/// release, no heartbeat). The core must compact once the TTL expires.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn liveness_compaction_after_lease_holder_dies() -> Result<()> {
    let mut c = Cluster::new(11, 2).await?;
    let mut p = Pusher::new(0);
    for _ in 0..5 {
        ensure!(
            p.push_once(&c.instances[0], &c.id, Duration::from_secs(10))
                .await?
        );
    }
    // Instance 1 takes the lease and "crashes" (guard leaked, never released).
    let h1 = c.instances[1].open(&c.id).await?;
    let lease_store: DynStore = Arc::new(h1.store().clone());
    let guard = walgit_store::coord::try_acquire(
        lease_store,
        &walgit_proto::keys::lease_key("compact"),
        "dead-instance",
        "compact",
        c.instances[1].cfg.compaction.lease_ttl,
    )
    .await?
    .expect("lease free");
    std::mem::forget(guard);
    c.restart(1);

    let t = Instant::now();
    let h0 = c.instances[0].open(&c.id).await?;
    loop {
        let out = walgit_server::ops::compact_repo(
            &h0,
            &c.instances[0].cfg,
            walgit_server::ops::CompactRequest {
                force: true,
                rebuild_base: false,
            },
            &walgit_server::ops::noop_log,
        )
        .await?;
        match out {
            walgit_server::ops::CompactOutcome::Published { .. } => break,
            walgit_server::ops::CompactOutcome::LeaseHeld => {
                ensure!(
                    t.elapsed() < Duration::from_secs(15),
                    "lease of a dead holder never expired (ttl {:?})",
                    c.instances[0].cfg.compaction.lease_ttl
                );
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            other @ walgit_server::ops::CompactOutcome::NotTriggered { .. } => {
                bail!("unexpected {other:?}")
            }
        }
    }
    eprintln!(
        "compacted {:.1}s after the holder died",
        t.elapsed().as_secs_f64()
    );
    check_truth(&c, std::slice::from_ref(&p)).await?;
    Ok(())
}

/// Liveness 2: a process crash in the middle of a publish (the publisher task
/// panics on the manifest CAS). The instance must keep accepting pushes for
/// that repo afterwards — a dead single-flight publisher must not wedge it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn liveness_publisher_survives_a_crash_mid_publish() -> Result<()> {
    let c = Cluster::new(12, 1).await?;
    let mut p = Pusher::new(0);
    ensure!(
        p.push_once(&c.instances[0], &c.id, Duration::from_secs(10))
            .await?
    );
    c.instances[0].link.set(FaultPlan {
        panic_once_keys: vec!["put:manifest.pb".into()],
        ..Default::default()
    });
    // This push hits the panic inside the publisher task.
    let first = p
        .push_once(&c.instances[0], &c.id, Duration::from_secs(10))
        .await?;
    eprintln!("push during crash acked={first}; errors={:?}", p.errors);
    c.instances[0].link.heal();
    // Now the instance must recover: pushes are acknowledged again.
    let t = Instant::now();
    let mut ok = false;
    while t.elapsed() < Duration::from_secs(10) {
        if p.push_once(&c.instances[0], &c.id, Duration::from_secs(5))
            .await?
        {
            ok = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    ensure!(
        ok,
        "instance never accepted a push again after its publisher crashed; errors: {:?}\n{}",
        p.errors,
        c.dump_traces()
    );
    check_truth(&c, std::slice::from_ref(&p)).await?;
    Ok(())
}

/// Liveness 3: a stale-forever instance (its conditional GETs always answer
/// 304: it never learns of anyone's writes) pushes in a tight loop against the
/// same repo. The healthy core must keep acknowledging pushes quickly and
/// without errors: a non-core instance may not starve the manifest.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn liveness_stale_instance_cannot_starve_the_core() -> Result<()> {
    let c = Cluster::new(13, 2).await?;
    let mut core_p = Pusher::new(0);
    ensure!(
        core_p
            .push_once(&c.instances[0], &c.id, Duration::from_secs(10))
            .await?
    );
    // Let the stale instance see the repo once, then freeze its view.
    let mut stale_p = Pusher::new(1);
    ensure!(
        stale_p
            .push_once(&c.instances[1], &c.id, Duration::from_secs(10))
            .await?
    );
    c.instances[1].link.set(FaultPlan::stale_forever());

    let stale_link = c.instances[1].link.clone();
    let stale_reg = c.instances[1].registry.clone();
    let id = c.id.clone();
    let bg = tokio::spawn(async move {
        let inst = Instance {
            name: "stale".into(),
            link: stale_link,
            registry: stale_reg,
            cfg: Arc::new(sim_config(Path::new("/nonexistent"))),
            cache: tempfile::tempdir().unwrap(),
        };
        let mut n = 0u64;
        loop {
            let _ = stale_p.push_once(&inst, &id, Duration::from_secs(2)).await;
            n += 1;
            if n.is_multiple_of(10) {
                tracing::info!(
                    "stale pusher: {n} attempts, last: {:?}",
                    stale_p.errors.last()
                );
            }
        }
    });

    let mut lat = Vec::new();
    let mut fails = Vec::new();
    for _ in 0..25 {
        let t = Instant::now();
        let ok = core_p
            .push_once(&c.instances[0], &c.id, Duration::from_secs(10))
            .await?;
        lat.push(t.elapsed());
        if !ok {
            fails.push(core_p.errors.last().cloned().unwrap_or_default());
        }
    }
    bg.abort();
    lat.sort();
    let p50 = lat[lat.len() / 2];
    let p99 = lat[lat.len() * 99 / 100];
    eprintln!(
        "core pushes next to a stale hammerer: p50 {p50:?} p99 {p99:?}, failures {}",
        fails.len()
    );
    ensure!(
        fails.is_empty(),
        "core pushes failed next to a stale instance: {fails:?}"
    );
    ensure!(
        p99 < Duration::from_secs(5),
        "core push p99 {p99:?} — starved"
    );
    check_truth(&c, &[core_p]).await?;
    Ok(())
}

/// Liveness 4: the bucket ACKs the manifest CAS but the response is lost.
/// The push errors (fine), but the truth must stay consistent (no committed
/// log segment may be deleted as an "orphan") and everybody must still sync.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn liveness_after_a_lost_cas_response() -> Result<()> {
    let mut c = Cluster::new(14, 2).await?;
    let mut p = Pusher::new(0);
    ensure!(
        p.push_once(&c.instances[0], &c.id, Duration::from_secs(10))
            .await?
    );
    c.instances[0].link.set(
        FaultPlan {
            p_err_after: 1.0,
            ..Default::default()
        }
        .with_only(&["manifest.pb"]),
    );
    let acked = p
        .push_once(&c.instances[0], &c.id, Duration::from_secs(10))
        .await?;
    eprintln!(
        "push with lost CAS response: acked={acked}, last error {:?}",
        p.errors.last()
    );
    c.instances[0].link.heal();
    let head = c.truth_manifest().await?.head_seq;
    eprintln!("truth head after lost response: {head}");
    // Everyone, including a cold instance, must be able to sync.
    let r = check_truth(&c, std::slice::from_ref(&p)).await;
    if let Err(e) = &r {
        eprintln!("{}", c.dump_traces());
        bail!("truth inconsistent after a lost CAS response: {e:#}");
    }
    let cold = c.add_instance("cold", &|_| {});
    let h = c.instances[cold].open(&c.id).await?;
    tokio::time::timeout(Duration::from_secs(10), h.sync_full())
        .await
        .map_err(|_| anyhow!("cold sync hung"))??;
    ensure!(
        h.applied_seq() == head,
        "cold at {} != {head}",
        h.applied_seq()
    );
    // And the writer itself keeps working.
    ensure!(
        p.push_once(&c.instances[0], &c.id, Duration::from_secs(10))
            .await?,
        "writer wedged: {:?}",
        p.errors.last()
    );
    check_truth(&c, std::slice::from_ref(&p)).await?;
    Ok(())
}

/// Liveness 7 (found by `sim_safety_then_liveness`): a writer crashes between
/// its log PUT and its manifest CAS, leaving `log/<head+1>.pb` orphaned. Every
/// later writer used to 412 on that key forever ("retry exhausted"): one crash
/// = no more pushes to the repo, from anyone. The core must publish anyway.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn liveness_orphaned_log_segment_does_not_block_writers() -> Result<()> {
    let mut c = Cluster::new(17, 2).await?;
    let mut p = Pusher::new(0);
    ensure!(
        p.push_once(&c.instances[0], &c.id, Duration::from_secs(10))
            .await?
    );
    // Instance 1 crashes right after its log PUT (the manifest CAS never happens).
    c.instances[1].link.set(FaultPlan {
        panic_once_keys: vec!["put:manifest.pb".into()],
        ..Default::default()
    });
    let mut crasher = Pusher::new(1);
    let _ = crasher
        .push_once(&c.instances[1], &c.id, Duration::from_secs(10))
        .await?;
    let head = c.truth_manifest().await?.head_seq;
    let orphan = format!(
        "{}{}",
        c.repo_prefix(),
        walgit_proto::keys::log_segment_key(head + 1)
    );
    ensure!(
        c.truth.exists(&orphan).await?,
        "setup: expected an orphan at {orphan}"
    );
    c.restart(1);

    // Core: pushes from both instances must land.
    let t = Instant::now();
    for i in 0..2 {
        let mut ok = false;
        while t.elapsed() < Duration::from_secs(20) {
            if p.push_once(&c.instances[i], &c.id, Duration::from_secs(10))
                .await?
            {
                ok = true;
                break;
            }
        }
        ensure!(
            ok,
            "writers blocked by an orphaned log segment: {:?}",
            p.errors.last()
        );
    }
    eprintln!(
        "pushes past the orphan took {:.2}s; errors seen: {:?}",
        t.elapsed().as_secs_f64(),
        p.errors
    );
    // Compaction (its own CAS loop) must get past it too.
    let h = c.instances[0].open(&c.id).await?;
    let out = walgit_server::ops::compact_repo(
        &h,
        &c.instances[0].cfg,
        walgit_server::ops::CompactRequest {
            force: true,
            rebuild_base: false,
        },
        &walgit_server::ops::noop_log,
    )
    .await?;
    ensure!(
        matches!(out, walgit_server::ops::CompactOutcome::Published { .. }),
        "{out:?}"
    );
    // The orphan was swept.
    ensure!(
        !c.truth.exists(&orphan).await?,
        "orphan {orphan} still in the bucket after a commit burned past it"
    );
    check_truth(&c, std::slice::from_ref(&p)).await?;
    Ok(())
}

/// Liveness 5: a cold instance whose pack downloads are truncated for a while.
/// Once its link heals, it must finish syncing — a half-downloaded pack on
/// disk may not poison every later attempt.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::many_single_char_names)]
async fn liveness_cold_start_through_truncated_pack_reads() -> Result<()> {
    let mut c = Cluster::new(15, 1).await?;
    let mut p = Pusher::new(0);
    for _ in 0..4 {
        ensure!(
            p.push_once(&c.instances[0], &c.id, Duration::from_secs(10))
                .await?
        );
    }
    let cold = c.add_instance("cold", &|_| {});
    c.instances[cold].link.set(
        FaultPlan {
            p_truncate: 1.0,
            ..Default::default()
        }
        .with_only(&[".pack", ".idx"]),
    );
    let h = c.instances[cold].open(&c.id).await?;
    let mut failures = 0;
    for _ in 0..3 {
        match tokio::time::timeout(Duration::from_secs(10), h.sync_full()).await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => {
                failures += 1;
                eprintln!("sync_full under truncation: {e}");
            }
            Err(_) => bail!("sync_full hung under truncation"),
        }
    }
    eprintln!("{failures} failed syncs under truncation (expected > 0)");
    c.instances[cold].link.heal();
    let t = Instant::now();
    loop {
        match tokio::time::timeout(Duration::from_secs(10), h.sync_full()).await {
            Ok(Ok(_)) => break,
            Ok(Err(e)) => {
                ensure!(
                    t.elapsed() < Duration::from_secs(10),
                    "healed cold instance never syncs: {e}\n{}",
                    c.dump_traces()
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(_) => bail!("sync_full hung after heal"),
        }
    }
    // Objects really are there.
    for a in &p.acked {
        let id = gix_hash::ObjectId::from_hex(a.new.as_bytes())?;
        ensure!(h.local().has_object(&id), "cold instance lacks {}", a.new);
    }
    // Compaction on the healed instance works on what it downloaded.
    let out = walgit_server::ops::compact_repo(
        &h,
        &c.instances[cold].cfg,
        walgit_server::ops::CompactRequest {
            force: true,
            rebuild_base: false,
        },
        &walgit_server::ops::noop_log,
    )
    .await?;
    ensure!(
        matches!(out, walgit_server::ops::CompactOutcome::Published { .. }),
        "{out:?}"
    );
    check_truth(&c, std::slice::from_ref(&p)).await?;
    Ok(())
}

/// Liveness 6: a black-holed instance holds a read guard / is mid-sync forever;
/// its pushers hang. The rest of the cluster must not notice: pushes on the
/// healthy instance ACK at normal latency, and a checkpoint+compaction go through.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn liveness_black_holed_instance_is_invisible_to_the_core() -> Result<()> {
    let c = Cluster::new(16, 2).await?;
    let mut p0 = Pusher::new(0);
    let mut p1 = Pusher::new(1);
    ensure!(
        p0.push_once(&c.instances[0], &c.id, Duration::from_secs(10))
            .await?
    );
    ensure!(
        p1.push_once(&c.instances[1], &c.id, Duration::from_secs(10))
            .await?
    );
    c.instances[1].link.set(FaultPlan::black_hole());
    let (link, reg, id) = (
        c.instances[1].link.clone(),
        c.instances[1].registry.clone(),
        c.id.clone(),
    );
    let bg = tokio::spawn(async move {
        let inst = Instance {
            name: "hole".into(),
            link,
            registry: reg,
            cfg: Arc::new(sim_config(Path::new("/nonexistent"))),
            cache: tempfile::tempdir().unwrap(),
        };
        for _ in 0..5 {
            let _ = p1.push_once(&inst, &id, Duration::from_secs(30)).await;
        }
    });
    let mut lat = Vec::new();
    for _ in 0..10 {
        let t = Instant::now();
        ensure!(
            p0.push_once(&c.instances[0], &c.id, Duration::from_secs(10))
                .await?,
            "{:?}",
            p0.errors.last()
        );
        lat.push(t.elapsed());
    }
    lat.sort();
    eprintln!(
        "core push p50 {:?} max {:?} next to a black-holed instance",
        lat[lat.len() / 2],
        lat.last().unwrap()
    );
    let h = c.instances[0].open(&c.id).await?;
    tokio::time::timeout(Duration::from_secs(10), h.write_checkpoint())
        .await
        .map_err(|_| anyhow!("checkpoint hung"))??;
    let out = tokio::time::timeout(
        Duration::from_secs(20),
        walgit_server::ops::compact_repo(
            &h,
            &c.instances[0].cfg,
            walgit_server::ops::CompactRequest {
                force: true,
                rebuild_base: false,
            },
            &walgit_server::ops::noop_log,
        ),
    )
    .await
    .map_err(|_| anyhow!("compaction hung"))??;
    ensure!(
        matches!(out, walgit_server::ops::CompactOutcome::Published { .. }),
        "{out:?}"
    );
    bg.abort();
    check_truth(&c, std::slice::from_ref(&p0)).await?;
    Ok(())
}

/// A task owner can be frozen forever on a black-holed store link. A second
/// caller joins that task (rather than duplicating work), but startup readiness
/// must still open after its configured bound instead of joining forever.
#[tokio::test]
async fn liveness_frozen_task_owner_does_not_wedge_readiness() -> Result<()> {
    let c = Cluster::new(18, 1).await?;
    let h = c.instances[0].open(&c.id).await?;
    let owner = match h.begin_task("prewarm", HashMap::new()) {
        walgit_wal::Begin::Started(t) => t,
        walgit_wal::Begin::AlreadyRunning(_) => bail!("setup: task already running"),
    };
    let frozen = owner.state.clone();
    c.instances[0].link.set(FaultPlan::black_hole());
    // Model a live process whose operation will never return: retain the task
    // handle, so its RAII drop cannot release the (repo, kind) lock.
    std::mem::forget(owner);
    match h.begin_task("prewarm", HashMap::new()) {
        walgit_wal::Begin::AlreadyRunning(s) => ensure!(s.id() == frozen.id()),
        walgit_wal::Begin::Started(_) => bail!("duplicate prewarm escaped the task lock"),
    }
    ensure!(!frozen.wait_done(Duration::from_millis(20)).await);

    let ready = walgit_server::prewarm::Readiness::new();
    ready.done.store(false, Ordering::Release);
    ensure!(!ready.ready(Duration::from_millis(50)));
    tokio::time::sleep(Duration::from_millis(60)).await;
    ensure!(
        ready.ready(Duration::from_millis(50)),
        "readiness joined a frozen prewarm forever"
    );
    Ok(())
}

/// A request `ReadGuard` is the pin that promises packs remain on disk. Even a
/// leaked guard must make eviction skip the repo; after it drops, eviction may
/// reclaim the cache.
#[tokio::test]
async fn liveness_leaked_read_guard_pins_cache_until_drop() -> Result<()> {
    let mut c = Cluster::new(19, 1).await?;
    let pinned = c.add_instance("pinned", &|cfg| {
        cfg.cache.max_bytes = walgit_config::ByteSize::b(0);
        cfg.cache.evict_idle_after = Duration::ZERO;
    });
    let h = c.instances[pinned].open(&c.id).await?;
    let guard = h.sync_full().await?;
    let path = h.local().path().to_path_buf();

    let report = c.instances[pinned].registry.evict_idle().await?;
    ensure!(
        report.evicted == 0,
        "evicted a repo under an active ReadGuard"
    );
    ensure!(path.exists(), "deleted a pinned repo directory");

    drop(guard);
    let report = c.instances[pinned].registry.evict_idle().await?;
    ensure!(
        report.evicted == 1,
        "repo was not evictable after guard drop"
    );
    ensure!(!path.exists(), "evicted repo directory remains");
    Ok(())
}

/// Checkpoint and compaction both CAS manifest.pb. Whichever wins first, the
/// loser must re-sync and preserve both the checkpoint and compacted pack set.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn liveness_checkpoint_racing_compaction() -> Result<()> {
    let c = Cluster::new(20, 2).await?;
    let mut p = Pusher::new(0);
    for _ in 0..5 {
        ensure!(
            p.push_once(&c.instances[0], &c.id, Duration::from_secs(10))
                .await?
        );
    }
    let h0 = c.instances[0].open(&c.id).await?;
    let h1 = c.instances[1].open(&c.id).await?;
    drop(h1.sync_full().await?);
    let cfg = c.instances[1].cfg.clone();
    let (cp, compact) = tokio::join!(
        h0.write_checkpoint(),
        walgit_server::ops::compact_repo(
            &h1,
            &cfg,
            walgit_server::ops::CompactRequest {
                force: true,
                rebuild_base: false
            },
            &walgit_server::ops::noop_log,
        )
    );
    cp.context("checkpoint lost its CAS race")?;
    ensure!(
        matches!(
            compact?,
            walgit_server::ops::CompactOutcome::Published { .. }
        ),
        "compaction did not publish"
    );
    check_truth(&c, std::slice::from_ref(&p)).await?;
    Ok(())
}

/// The due-bundle lease is durable, so a dead holder leaves it behind. Once
/// its expiry passes, a healthy maintainer must steal it and build the bundle.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn liveness_bundle_build_after_lease_holder_dies() -> Result<()> {
    let mut c = Cluster::new(21, 1).await?;
    let mut p = Pusher::new(0);
    ensure!(
        p.push_once(&c.instances[0], &c.id, Duration::from_secs(10))
            .await?
    );
    let maint = c.add_instance("bundle-core", &|cfg| {
        cfg.bundles.strategy.truncate(1); // weekly/full only
        // The sim pushes to refs/heads/p<idx>, never main; with the D22 default
        // `bundles.main_only = true` a weekly of this repo has no refs to cut
        // (`NoRefs`) — the test was silently failing since that default landed
        // (2026-08-21 ~22:00Z). Bundle every head here; the liveness under test
        // is the lease, not the ref selection.
        cfg.bundles.main_only = false;
    });
    let h = c.instances[maint].open(&c.id).await?;
    let dead =
        walgit_bundle::ops::try_acquire_lease(h.store(), "weekly", Duration::from_millis(100))
            .await?
            .expect("setup: weekly lease free");
    std::mem::forget(dead);

    let source = Arc::new(SimBundleSource(c.instances[maint].registry.clone()));
    let bundler = walgit_bundle::Bundler::new_with_source(source, c.instances[maint].cfg.clone());
    ensure!(
        bundler
            .run_due(&c.id, std::time::SystemTime::now())
            .await?
            .is_empty(),
        "built while dead holder's lease was live"
    );
    tokio::time::sleep(Duration::from_millis(120)).await;
    let built = tokio::time::timeout(
        Duration::from_secs(20),
        bundler.run_due(&c.id, std::time::SystemTime::now()),
    )
    .await
    .map_err(|_| anyhow!("bundle build did not recover after lease expiry"))??;
    ensure!(built.iter().any(|b| b.strategy == "weekly"));
    check_truth(&c, std::slice::from_ref(&p)).await?;
    Ok(())
}

/// Exact healthy-link request counts defend the critical-path budgets in
/// docs/ROUNDTRIPS.md. `MemoryStore` has no retries, so deltas are deterministic:
/// push = one freshness GET + pack/idx/log PUTs + manifest CAS; warm refs = one
/// conditional GET; cold refs = the open's manifest GET + one log tail GET.
#[tokio::test]
async fn healthy_request_round_trip_budgets() -> Result<()> {
    let mut c = Cluster::new(22, 1).await?;
    let mut p = Pusher::new(0);
    let before = c.instances[0].link.stats().ops.load(Ordering::Relaxed);
    ensure!(
        p.push_once(&c.instances[0], &c.id, Duration::from_secs(10))
            .await?
    );
    let push_ops = c.instances[0].link.stats().ops.load(Ordering::Relaxed) - before;
    ensure!(
        push_ops <= 5,
        "healthy push used {push_ops} requests, budget 5"
    );

    let h = c.instances[0].open(&c.id).await?;
    let before = c.instances[0].link.stats().ops.load(Ordering::Relaxed);
    drop(h.sync_refs().await?);
    let warm_ops = c.instances[0].link.stats().ops.load(Ordering::Relaxed) - before;
    ensure!(
        warm_ops <= 1,
        "warm refs read used {warm_ops} requests, budget 1"
    );

    let cold = c.add_instance("budget-cold", &|_| {});
    let before = c.instances[cold].link.stats().ops.load(Ordering::Relaxed);
    let _h = c.instances[cold].open(&c.id).await?;
    let cold_ops = c.instances[cold].link.stats().ops.load(Ordering::Relaxed) - before;
    ensure!(
        cold_ops <= 2,
        "cold refs sync used {cold_ops} requests, budget 2"
    );
    // Checkpoint: the freshness GET every operation pays, then refs PUT ∥ checkpoint PUT, then
    // the manifest CAS — 4 requests, 3 rounds; the provenance times come from what the writer
    // already applied, never a log GET (2026-08-22: it was 6 requests in 6 rounds with a
    // bundle-list GET and a log GET inside).
    let before = c.instances[0].link.stats().ops.load(Ordering::Relaxed);
    let cp = h.write_checkpoint().await?;
    let cp_ops = c.instances[0].link.stats().ops.load(Ordering::Relaxed) - before;
    ensure!(
        cp_ops <= 4,
        "checkpoint used {cp_ops} requests, budget 4 (3 rounds: cond GET → PUTs ∥ → CAS)"
    );
    ensure!(
        cp.as_of.is_some() && cp.first_state_at.is_some(),
        "provenance times carried without a log read: {cp:?}"
    );
    eprintln!(
        "healthy request counts: push={push_ops}, warm_refs={warm_ops}, cold_refs={cold_ops}, checkpoint={cp_ops}"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Bundle list: two maintainers (one legacy-shaped) + retention under faults
// ---------------------------------------------------------------------------

fn sim_bundle_entry(
    strategy: &str,
    kind: &str,
    slot: u64,
    base_id: &str,
) -> walgit_proto::v1::BundleEntry {
    walgit_proto::v1::BundleEntry {
        id: format!("{strategy}-{slot}"),
        key: format!("bundles/{strategy}/{slot}.bundle"),
        strategy: strategy.into(),
        kind: kind.into(),
        creation_token: slot,
        slot,
        base_id: base_id.into(),
        ..Default::default()
    }
}

/// Oracle over the truth store: every bundle the *current* list references exists (a client
/// that just read the list never 404s on an entry), and — once converged — the list obeys the
/// D21 rule (`keep` fulls + the 2 newest incrementals per strategy, no orphan).
async fn check_bundle_list_truth(c: &Cluster, converged: bool) -> Result<()> {
    let store = walgit_store::Prefixed::new(c.truth.clone(), c.repo_prefix());
    let Some(list) = walgit_bundle::ops::read_list(&store).await? else {
        return Ok(());
    };
    for b in &list.bundles {
        ensure!(
            c.truth
                .head(&format!("{}{}", c.repo_prefix(), b.key))
                .await?
                .is_some(),
            "listed bundle {} is missing from the bucket (a client would 404)",
            b.key
        );
        if !b.base_id.is_empty() {
            ensure!(
                list.bundles.iter().any(|x| x.id == b.base_id),
                "orphan {}: base {} not listed",
                b.id,
                b.base_id
            );
        }
    }
    if converged {
        let mut probe = list.clone();
        let pruned = walgit_bundle::slots::retain(
            &sim_config(Path::new("/nonexistent")).bundles,
            &mut probe,
        );
        ensure!(
            pruned.is_empty(),
            "list not at retention: would still prune {pruned:?}"
        );
        for strat in ["daily", "hourly"] {
            ensure!(
                list.bundles.iter().filter(|b| b.strategy == strat).count()
                    <= walgit_bundle::slots::INCREMENTALS_KEPT,
                "{strat}: more than {} listed",
                walgit_bundle::slots::INCREMENTALS_KEPT
            );
        }
    }
    Ok(())
}

/// A legacy-shaped maintainer (the rule before 2026-08-22: publish = append, prune nothing) and a
/// current one race on `bundles/list.pb` through faulty links (lost responses, spurious 412s,
/// errors before the op) while an oracle watches the truth. The list must converge to the D21
/// rule within a bounded number of healthy passes, no listed bundle may ever be missing, and a
/// steady-state retention pass costs one request (ROUNDTRIPS: 0 extra when nothing is pruned).
async fn run_bundle_retention_race(seed: u64) -> Result<()> {
    let mut c = Cluster::new(seed, 1).await?;
    let store = walgit_store::Prefixed::new(c.truth.clone(), c.repo_prefix());
    // Seed: one weekly, two dailies on it, 8 hourlies on each daily (objects = dummy bytes).
    let mut list = walgit_proto::v1::BundleList {
        mode: "all".into(),
        heuristic: "creationToken".into(),
        ..Default::default()
    };
    let w0 = 1_787_000_400u64;
    list.bundles
        .push(sim_bundle_entry("weekly", "full", w0, ""));
    let mut slot_max = w0;
    for d in 1..=2u64 {
        let ds = w0 + d * 86_400;
        list.bundles.push(sim_bundle_entry(
            "daily",
            "incremental",
            ds,
            &format!("weekly-{w0}"),
        ));
        for h in 1..=8u64 {
            list.bundles.push(sim_bundle_entry(
                "hourly",
                "incremental",
                ds + h * 3600,
                &format!("daily-{ds}"),
            ));
            slot_max = slot_max.max(ds + h * 3600);
        }
    }
    for b in &list.bundles {
        store
            .put_bytes(&b.key, b"bundle".as_slice(), walgit_store::PutMode::Create)
            .await?;
    }
    store
        .put_bytes(
            walgit_proto::keys::BUNDLE_LIST,
            list.encode_to_vec(),
            walgit_store::PutMode::Create,
        )
        .await?;
    check_bundle_list_truth(&c, false).await?;

    // The current maintainer (faulty link) and the legacy one (its own faulty link).
    let cur = c.add_instance("maint-current", &|cfg| cfg.bundles.enabled = true);
    let legacy = c.add_instance("maint-legacy", &|cfg| cfg.bundles.enabled = true);
    let chaos = FaultPlan::chaos(0.15).with_only(&["bundles/"]);
    c.instances[cur].link.set(chaos.clone());
    c.instances[legacy].link.set(chaos);
    let bundler = walgit_bundle::Bundler::new_with_source(
        Arc::new(SimBundleSource(c.instances[cur].registry.clone())),
        c.instances[cur].cfg.clone(),
    );
    let legacy_h = c.instances[legacy].open(&c.id).await?;
    let legacy_store = legacy_h.store().clone();
    let newest_daily = format!("daily-{}", w0 + 2 * 86_400);

    // Safety mode: interleave retention passes with legacy appends (each new hourly's object is
    // written first, then the list entry — the legacy code's order too), checking the oracle.
    let mut appended = 0u64;
    for round in 0..12u64 {
        let _ = bundler.apply_retention(&c.id).await; // faults may fail it: that is the point
        if round % 2 == 0 {
            slot_max += 3600;
            let e = sim_bundle_entry("hourly", "incremental", slot_max, &newest_daily);
            if legacy_store
                .put_bytes(&e.key, b"bundle".as_slice(), walgit_store::PutMode::Create)
                .await
                .is_ok()
            {
                let e2 = e.clone();
                let r = walgit_bundle::ops::cas_update_list(&legacy_store, 8, move |cur| {
                    let mut l = cur.cloned().unwrap_or_default();
                    l.bundles.retain(|b| b.id != e2.id);
                    l.bundles.push(e2.clone());
                    Ok(Some(l))
                })
                .await;
                if r.is_ok() {
                    appended += 1;
                }
            }
        }
        check_bundle_list_truth(&c, false)
            .await
            .with_context(|| format!("round {round}, seed {seed}\n{}", c.dump_traces()))?;
    }

    // Liveness mode: heal the current maintainer; the list converges within a bound.
    c.instances[cur].link.heal();
    let mut converged = false;
    for _ in 0..6 {
        bundler.apply_retention(&c.id).await?;
        if check_bundle_list_truth(&c, true).await.is_ok() {
            converged = true;
            break;
        }
    }
    ensure!(
        converged,
        "list did not converge to retention (seed {seed}, legacy appended {appended}):\n{}",
        c.dump_traces()
    );
    // Steady state: a retention pass with nothing to prune is one request (the list GET).
    let before = c.instances[cur].link.stats().ops.load(Ordering::Relaxed);
    ensure!(bundler.apply_retention(&c.id).await? == 0);
    let ops = c.instances[cur].link.stats().ops.load(Ordering::Relaxed) - before;
    ensure!(
        ops <= 1,
        "steady-state retention pass used {ops} requests, budget 1"
    );
    // And a legacy append after convergence is folded by the next pass, never leaving a gap.
    slot_max += 3600;
    let e = sim_bundle_entry("hourly", "incremental", slot_max, &newest_daily);
    c.instances[legacy].link.heal();
    legacy_store
        .put_bytes(&e.key, b"bundle".as_slice(), walgit_store::PutMode::Create)
        .await?;
    let e2 = e.clone();
    walgit_bundle::ops::cas_update_list(&legacy_store, 8, move |cur| {
        let mut l = cur.cloned().unwrap_or_default();
        l.bundles.push(e2.clone());
        Ok(Some(l))
    })
    .await?;
    check_bundle_list_truth(&c, false).await?;
    bundler.apply_retention(&c.id).await?;
    check_bundle_list_truth(&c, true).await?;
    let final_list = walgit_bundle::ops::read_list(&store).await?.unwrap();
    ensure!(
        final_list.bundles.iter().any(|b| b.id == e.id),
        "the newest legacy hourly is listed"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sim_bundle_retention_under_two_maintainers() {
    for seed in seeds() {
        run_bundle_retention_race(seed)
            .await
            .unwrap_or_else(|e| panic!("seed {seed}: {e:#}"));
    }
}

// ---------------------------------------------------------------------------
// Checkpoint writer crashes between its PUTs and the manifest CAS
// ---------------------------------------------------------------------------

/// The checkpoint protocol is refs PUT → checkpoint PUT → manifest CAS (the CAS is the only
/// commit point). A writer that dies after the PUTs leaves orphan objects and an unchanged
/// manifest: a cold reader never sees a half checkpoint, and the next pass writes the same
/// seq idempotently and commits it.
async fn run_checkpoint_crash(seed: u64, crash_at: &str) -> Result<()> {
    let mut c = Cluster::new(seed, 2).await?;
    let mut p = Pusher::new(0);
    for _ in 0..3 {
        ensure!(
            p.push_once(&c.instances[0], &c.id, Duration::from_secs(10))
                .await?
        );
    }
    let before = c.truth_manifest().await?;
    ensure!(before.checkpoint.is_none());
    // The writer's link panics once on the chosen step.
    c.instances[1].link.set(FaultPlan {
        panic_once_keys: vec![crash_at.to_string()],
        ..Default::default()
    });
    let h = c.instances[1].open(&c.id).await?;
    let crashed = tokio::spawn(async move { h.write_checkpoint().await }).await;
    ensure!(
        crashed.is_err() || crashed.as_ref().unwrap().is_err(),
        "the writer should have died at {crash_at}"
    );
    drop(crashed);
    // Truth: manifest unchanged (no checkpoint), whatever objects landed are orphans.
    let after = c.truth_manifest().await?;
    ensure!(
        after.checkpoint.is_none() && after.head_seq == before.head_seq,
        "manifest moved despite the crash"
    );
    // A cold reader sees exactly the pre-crash state.
    let cold = c.add_instance("cold-after-crash", &|_| {});
    let hc = c.instances[cold].open(&c.id).await?;
    drop(hc.sync_refs().await?);
    ensure!(hc.manifest().checkpoint.is_none());
    ensure!(hc.applied_seq() == before.head_seq);
    // The next pass (fresh writer) completes the checkpoint idempotently.
    c.restart(1);
    let h = c.instances[1].open(&c.id).await?;
    let cp = tokio::time::timeout(Duration::from_secs(20), h.write_checkpoint())
        .await
        .map_err(|_| anyhow!("checkpoint hung"))??;
    ensure!(cp.seq == before.head_seq);
    let m = c.truth_manifest().await?;
    ensure!(m.checkpoint.as_ref().map(|x| x.seq) == Some(before.head_seq));
    // The committed checkpoint's objects exist and a cold start folds from it.
    for key in [
        walgit_proto::keys::checkpoint_key(cp.seq),
        walgit_proto::keys::checkpoint_refs_key(cp.seq),
    ] {
        ensure!(
            c.truth
                .head(&format!("{}{}", c.repo_prefix(), key))
                .await?
                .is_some(),
            "{key} missing after commit"
        );
    }
    let cold2 = c.add_instance("cold-after-repair", &|_| {});
    let h2 = c.instances[cold2].open(&c.id).await?;
    drop(h2.sync_refs().await?);
    ensure!(h2.manifest().checkpoint.as_ref().map(|x| x.seq) == Some(cp.seq));
    check_truth(&c, std::slice::from_ref(&p)).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sim_checkpoint_writer_crash_is_invisible_and_repaired() {
    for seed in seeds() {
        for crash_at in ["put:checkpoint.pb", "put:manifest.pb"] {
            run_checkpoint_crash(seed, crash_at)
                .await
                .unwrap_or_else(|e| panic!("seed {seed} crash_at {crash_at}: {e:#}"));
        }
    }
}

// ---------------------------------------------------------------------------
// Compaction never folds the base / history pack; a full rebuild leaves one full pack
// ---------------------------------------------------------------------------

/// Object ids inside a local pack (`git verify-pack -v`).
fn pack_objects(repo: &Path, checksum: &gix_hash::ObjectId) -> std::collections::HashSet<String> {
    let idx = repo
        .join("objects/pack")
        .join(format!("pack-{}.idx", checksum.to_hex()));
    let out = Command::new("git")
        .args(["verify-pack", "-v", idx.to_str().unwrap()])
        .output()
        .unwrap();
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter_map(|l| {
            let mut it = l.split_whitespace();
            let oid = it.next()?;
            let kind = it.next()?;
            (oid.len() == 40 && matches!(kind, "commit" | "tree" | "blob" | "tag"))
                .then(|| oid.to_string())
        })
        .collect()
}

/// Build a large-repository shape on a disk-mode host: a tier-2 base (full repack + bitmap) with its D18
/// history pack, then several fresh pushes. Returns (base, history) checksums.
#[allow(clippy::many_single_char_names)]
async fn seed_base_and_history(
    c: &Cluster,
    i: usize,
    p: &mut Pusher,
    fresh: usize,
) -> Result<(gix_hash::ObjectId, gix_hash::ObjectId)> {
    for _ in 0..3 {
        ensure!(
            p.push_once(&c.instances[i], &c.id, Duration::from_secs(10))
                .await?
        );
    }
    let h = c.instances[i].open(&c.id).await?;
    drop(h.sync_full().await?);
    let out = walgit_server::ops::compact_repo(
        &h,
        &c.instances[i].cfg,
        walgit_server::ops::CompactRequest {
            force: true,
            rebuild_base: true,
        },
        &walgit_server::ops::noop_log,
    )
    .await?;
    ensure!(
        matches!(
            out,
            walgit_server::ops::CompactOutcome::Published {
                rebuild_base: true,
                ..
            }
        ),
        "{out:?}"
    );
    drop(h.sync_full().await?);
    let m = h.manifest();
    let base = m
        .packs
        .iter()
        .find(|x| x.tier == 2 && x.kind != walgit_proto::v1::PackKind::History as i32)
        .ok_or_else(|| anyhow!("no base: {m:?}"))?;
    let hist = m
        .packs
        .iter()
        .find(|x| x.kind == walgit_proto::v1::PackKind::History as i32)
        .ok_or_else(|| anyhow!("no history pack: {m:?}"))?;
    let base = gix_hash::ObjectId::from_hex(base.checksum.as_bytes())?;
    let hist = gix_hash::ObjectId::from_hex(hist.checksum.as_bytes())?;
    for _ in 0..fresh {
        ensure!(
            p.push_once(&c.instances[i], &c.id, Duration::from_secs(10))
                .await?
        );
    }
    Ok((base, hist))
}

/// Large-repository regression at seq 101: on the SSD host every pack is a real local file, and the geometric
/// fold rolled the 32 GB base and the 6 GB history pack into a tier-1 pack. The fold must keep
/// tier-2 and HISTORY packs out (`--keep-pack`): they stay live, the new pack carries none of
/// the base's objects, and only tier < 2 packs are superseded.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn geometric_fold_never_touches_the_base_or_the_history_pack() -> Result<()> {
    let mut c = Cluster::new(31, 1).await?;
    // Disk-mode host (D25): every pack local, like the SSD host. history_pack on for this scenario.
    let i = c.add_instance("ssd", &|cfg| {
        cfg.cache.mode = walgit_config::CacheMode::Disk;
        cfg.git.history_pack = true;
    });
    let mut p = Pusher::new(0);
    let (base, hist) = seed_base_and_history(&c, i, &mut p, 4).await?;
    let h = c.instances[i].open(&c.id).await?;
    drop(h.sync_full().await?);
    let before = h.manifest();
    let fresh: Vec<String> = before
        .packs
        .iter()
        .filter(|x| x.tier == 0)
        .map(|x| x.checksum.clone())
        .collect();
    ensure!(fresh.len() >= 2, "{before:?}");
    let base_objects = pack_objects(h.local().path(), &base);
    ensure!(!base_objects.is_empty());

    let out = walgit_server::ops::compact_repo(
        &h,
        &c.instances[i].cfg,
        walgit_server::ops::CompactRequest {
            force: true,
            rebuild_base: false,
        },
        &walgit_server::ops::noop_log,
    )
    .await?;
    let dbg = format!("{out:?}");
    let walgit_server::ops::CompactOutcome::Published {
        rebuild_base,
        tier,
        packs,
        superseded,
    } = out
    else {
        bail!("{dbg}")
    };
    ensure!(!rebuild_base && tier == 1, "a fold, not a rebuild: {dbg}");
    drop(h.sync_full().await?);
    let after = h.manifest();
    let live: Vec<&str> = after.packs.iter().map(|x| x.checksum.as_str()).collect();
    ensure!(
        live.contains(&base.to_hex().to_string().as_str()),
        "base folded away: {after:?}"
    );
    ensure!(
        live.contains(&hist.to_hex().to_string().as_str()),
        "history pack folded away: {after:?}"
    );
    ensure!(
        after
            .packs
            .iter()
            .any(|x| x.checksum == base.to_hex().to_string() && x.tier == 2)
    );
    ensure!(
        after
            .packs
            .iter()
            .any(|x| x.checksum == hist.to_hex().to_string()
                && x.kind == walgit_proto::v1::PackKind::History as i32)
    );
    for f in &fresh {
        ensure!(
            !live.contains(&f.as_str()),
            "fresh pack {f} still live after the fold"
        );
    }
    ensure!(
        superseded == fresh.len(),
        "superseded {superseded}, fresh {}",
        fresh.len()
    );
    // The folded pack carries none of the base's objects.
    for new in &packs {
        let objs = pack_objects(
            h.local().path(),
            &gix_hash::ObjectId::from_hex(new.as_bytes())?,
        );
        ensure!(
            objs.is_disjoint(&base_objects),
            "folded pack {new} contains base objects"
        );
    }
    check_truth(&c, std::slice::from_ref(&p)).await?;
    Ok(())
}

/// Large-repository regression at seq 127: a full rebuild superseded only what git happened to delete, so a
/// retained redundant 32 GB pack stayed live next to the new base (and a ratio then re-triggered
/// rebuilds forever). A rebuild must supersede every other live pack: afterwards exactly one
/// base (tier 2, non-history) and its history pack are live, whatever git kept on disk.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn full_rebuild_leaves_exactly_one_base_even_with_a_retained_pack() -> Result<()> {
    let mut c = Cluster::new(32, 1).await?;
    let i = c.add_instance("ssd", &|cfg| {
        cfg.cache.mode = walgit_config::CacheMode::Disk;
        cfg.git.history_pack = true;
    });
    let mut p = Pusher::new(0);
    let (_base1, _hist1) = seed_base_and_history(&c, i, &mut p, 2).await?;
    let h = c.instances[i].open(&c.id).await?;
    drop(h.sync_full().await?);
    // Simulate git retaining the old base (a `.keep` git would honour — as a kept pack it is not
    // deleted by `repack -a -d`; a pack nobody deleted is exactly what stayed live at seq 127).
    // `repack` removes `.keep` markers before a full repack, so retain it another way: copy the
    // old base under a fresh name after the rebuild below would be cheating — instead assert on
    // the manifest rule directly: every pre-rebuild live pack must be superseded.
    let before: Vec<String> = h
        .manifest()
        .packs
        .iter()
        .map(|x| x.checksum.clone())
        .collect();
    ensure!(before.len() >= 3, "{before:?}");
    let log_lines: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new());
    let out = walgit_server::ops::compact_repo(
        &h,
        &c.instances[i].cfg,
        walgit_server::ops::CompactRequest {
            force: true,
            rebuild_base: true,
        },
        &|l: String| log_lines.lock().unwrap().push(l),
    )
    .await?;
    let dbg = format!("{out:?}\nlog:\n{}", log_lines.lock().unwrap().join("\n"));
    let walgit_server::ops::CompactOutcome::Published {
        rebuild_base: true,
        superseded,
        ..
    } = out
    else {
        bail!("{dbg}")
    };
    drop(h.sync_full().await?);
    let after = h.manifest();
    let fulls: Vec<&walgit_proto::v1::PackRef> = after
        .packs
        .iter()
        .filter(|x| x.tier == 2 && x.kind != walgit_proto::v1::PackKind::History as i32)
        .collect();
    ensure!(
        fulls.len() == 1,
        "exactly one base after a rebuild: {after:?}"
    );
    ensure!(
        after
            .packs
            .iter()
            .filter(|x| x.kind == walgit_proto::v1::PackKind::History as i32)
            .count()
            == 1,
        "{after:?}\n{dbg}"
    );
    ensure!(after.packs.len() == 2, "base + history only: {after:?}");
    for old in &before {
        if after.packs.iter().any(|x| &x.checksum == old) {
            // The rebuild may reproduce an identical pack (same objects, same order): then it is the
            // new base, never a stale extra.
            ensure!(
                fulls[0].checksum == *old,
                "old pack {old} still live next to the new base: {after:?}"
            );
        }
    }
    ensure!(
        superseded >= before.len() - 1,
        "rebuild superseded {superseded} of {} live packs",
        before.len()
    );
    check_truth(&c, std::slice::from_ref(&p)).await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Resumable base rebuild (BUNDLE_URI_DESIGN §5a)
// ---------------------------------------------------------------------------

/// Run one rebuild attempt; returns (outcome, log lines).
async fn rebuild_attempt(
    c: &Cluster,
    i: usize,
) -> (Result<walgit_server::ops::CompactOutcome>, Vec<String>) {
    let lines: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new());
    let h = match c.instances[i].open(&c.id).await {
        Ok(h) => h,
        Err(e) => return (Err(e), Vec::new()),
    };
    let out = walgit_server::ops::compact_repo(
        &h,
        &c.instances[i].cfg,
        walgit_server::ops::CompactRequest {
            force: true,
            rebuild_base: true,
        },
        &|l: String| lines.lock().unwrap().push(l),
    )
    .await;
    (out, lines.into_inner().unwrap())
}

/// A deploy (D31) kills the rebuild after any phase: the next unit resumes from the marker —
/// across every phase boundary there is exactly **one** `git repack` in total — the serving copy
/// is never rewritten (its pack files before publish are exactly the pre-rebuild ones), and the
/// result is one base + one history pack. A push between the attempts makes the head move, and
/// the next unit starts over (a second repack) instead of publishing a pack that lacks objects.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::many_single_char_names)]
async fn base_rebuild_resumes_after_a_kill_between_any_two_phases() -> Result<()> {
    use walgit_server::rebuild::{Phase, TEST_ABORT_AFTER};
    let mut c = Cluster::new(33, 1).await?;
    let i = c.add_instance("ssd", &|cfg| {
        cfg.cache.mode = walgit_config::CacheMode::Disk;
        cfg.git.history_pack = true;
        cfg.git.commit_graph = true;
    });
    let mut p = Pusher::new(0);
    for _ in 0..4 {
        ensure!(
            p.push_once(&c.instances[i], &c.id, Duration::from_secs(10))
                .await?
        );
    }
    let h = c.instances[i].open(&c.id).await?;
    drop(h.sync_full().await?);
    let before_files: std::collections::BTreeSet<String> =
        std::fs::read_dir(h.local().path().join("objects/pack"))?
            .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
            .collect();
    let repo_key = c.id.to_string();
    let mut repacks = 0usize;
    // Kill after each phase in turn; every attempt but the last fails by the hook.
    for phase in [
        Phase::Copied,
        Phase::Repacked,
        Phase::HistoryPack,
        Phase::CommitGraph,
    ] {
        *TEST_ABORT_AFTER.lock() = Some((repo_key.clone(), phase));
        let (out, log) = rebuild_attempt(&c, i).await;
        repacks += log.iter().filter(|l| l.starts_with("repack done")).count();
        ensure!(
            out.is_err(),
            "attempt killed after {phase:?} should fail: {out:?}\n{}",
            log.join("\n")
        );
        // The serving copy is untouched while the rebuild is in flight.
        let now_files: std::collections::BTreeSet<String> =
            std::fs::read_dir(h.local().path().join("objects/pack"))?
                .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
                .collect();
        ensure!(
            now_files == before_files,
            "serving copy changed during the rebuild after {phase:?}: {now_files:?} vs {before_files:?}"
        );
        // "Restart": a fresh instance on the same cache dir/link name — the scratch dir on "disk" survives.
        c.restart_keep_disk(i, &|cfg| {
            cfg.cache.mode = walgit_config::CacheMode::Disk;
            cfg.git.history_pack = true;
            cfg.git.commit_graph = true;
        });
    }
    *TEST_ABORT_AFTER.lock() = None;
    let (out, log) = rebuild_attempt(&c, i).await;
    repacks += log.iter().filter(|l| l.starts_with("repack done")).count();
    let out = out.with_context(|| log.join("\n"))?;
    ensure!(
        matches!(
            out,
            walgit_server::ops::CompactOutcome::Published {
                rebuild_base: true,
                ..
            }
        ),
        "{out:?}"
    );
    ensure!(
        repacks == 1,
        "exactly one git repack across all attempts, saw {repacks}:\n{}",
        log.join("\n")
    );
    ensure!(
        log.iter().any(|l| l.starts_with("resuming base rebuild")),
        "{}",
        log.join("\n")
    );
    let h = c.instances[i].open(&c.id).await?;
    drop(h.sync_full().await?);
    let m = h.manifest();
    ensure!(
        m.packs
            .iter()
            .filter(|x| x.tier == 2 && x.kind != walgit_proto::v1::PackKind::History as i32)
            .count()
            == 1,
        "{m:?}"
    );
    ensure!(
        m.packs
            .iter()
            .filter(|x| x.kind == walgit_proto::v1::PackKind::History as i32)
            .count()
            == 1,
        "{m:?}"
    );
    ensure!(m.packs.len() == 2, "{m:?}");
    let scratch = c.instances[i].cfg.cache.dir.join("_rebuild");
    ensure!(
        !scratch.join("sim").exists() || std::fs::read_dir(scratch.join("sim"))?.next().is_none(),
        "scratch dir left behind"
    );
    check_truth(&c, std::slice::from_ref(&p)).await?;

    // A push between a kill and the resume: the head moved, the next attempt starts over.
    *TEST_ABORT_AFTER.lock() = Some((repo_key.clone(), Phase::Repacked));
    let (out, log1) = rebuild_attempt(&c, i).await;
    ensure!(out.is_err());
    ensure!(log1.iter().filter(|l| l.starts_with("repack done")).count() == 1);
    ensure!(
        p.push_once(&c.instances[i], &c.id, Duration::from_secs(10))
            .await?
    );
    *TEST_ABORT_AFTER.lock() = None;
    let (out, log2) = rebuild_attempt(&c, i).await;
    let out = out.with_context(|| log2.join("\n"))?;
    ensure!(matches!(
        out,
        walgit_server::ops::CompactOutcome::Published {
            rebuild_base: true,
            ..
        }
    ));
    ensure!(
        log2.iter()
            .any(|l| l.starts_with("discarding interrupted base rebuild")),
        "{}",
        log2.join("\n")
    );
    ensure!(
        log2.iter().filter(|l| l.starts_with("repack done")).count() == 1,
        "a fresh repack after the head moved"
    );
    let h = c.instances[i].open(&c.id).await?;
    drop(h.sync_full().await?);
    let m = h.manifest();
    ensure!(
        m.packs.len() == 2,
        "one base + one history pack again: {m:?}"
    );
    // The pushed commit is in the new base (not lost to a stale scratch).
    let base = m
        .packs
        .iter()
        .find(|x| x.tier == 2 && x.kind != walgit_proto::v1::PackKind::History as i32)
        .unwrap();
    let objs = pack_objects(
        h.local().path(),
        &gix_hash::ObjectId::from_hex(base.checksum.as_bytes())?,
    );
    ensure!(
        objs.contains(&p.tip),
        "the in-between push's tip {} is in the new base",
        p.tip
    );
    check_truth(&c, std::slice::from_ref(&p)).await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Task ownership under concurrency + an owner crash; cache pressure
// ---------------------------------------------------------------------------

/// A pusher whose commits carry ~`kb` KB of incompressible content (so packs have bytes to
/// download and caches have something to evict).
fn push_blobby(p: &mut Pusher, kb: usize, rng: &mut Lcg) -> String {
    let mut buf = vec![0u8; kb * 1024];
    for b in &mut buf {
        *b = u8::try_from(rng.next() & 0xff).expect("masked random byte fits in u8");
    }
    std::fs::write(p.work.path().join(format!("blob-{}.bin", p.n + 1)), &buf).unwrap();
    p.work.commit(p.n + 1, &format!("p{}", p.idx))
}

/// Task locks are **per instance and in memory** (`Tasks::running`, released by the handle's
/// RAII drop; cross-instance exclusivity is the GCS lease, not this registry), so "two
/// instances" reduces to many concurrent callers on one instance plus a crash of whoever
/// owns the materialize task. Asserted under randomized link faults: never more than one
/// `materialize` running for the repo; an aborted owner releases the lock at once (the next
/// caller starts its own task — nothing blocks forever); a late joiner's `attach()` replays
/// the story so far and sees the outcome; downloads are not multiplied by the callers.
#[allow(clippy::many_single_char_names)]
async fn run_task_ownership(seed: u64) -> Result<()> {
    let mut rng = Lcg(seed);
    let mut c = Cluster::new(seed, 1).await?;
    let mut p = Pusher::new(0);
    for _ in 0..4 {
        let new = push_blobby(&mut p, 64, &mut rng);
        let h = c.instances[0].open(&c.id).await?;
        let pack = p
            .work
            .pack(&new, (!p.tip.is_empty()).then_some(p.tip.as_str()));
        let ingested = h
            .local()
            .ingest_pack(
                std::io::Cursor::new(pack),
                IngestOptions {
                    fsck: false,
                    max_bytes: None,
                    thin: true,
                },
            )
            .await?
            .unwrap();
        let txn = RefTransaction {
            updates: vec![RefUpdate {
                name: p.refname.clone(),
                old_oid: p.tip.clone(),
                new_oid: new.clone(),
                ..Default::default()
            }],
            ..Default::default()
        };
        h.publish_push(Some(ingested), txn, HashMap::new()).await?;
        p.tip = new;
    }
    let live_packs = c.truth_manifest().await?.packs.len();
    ensure!(live_packs >= 4);

    // A fresh instance whose pack reads are slow and flaky.
    let j = c.add_instance("joiners", &|_| {});
    c.instances[j].link.set(
        FaultPlan {
            delay: Some((
                Duration::from_millis(1),
                Duration::from_millis(2 + rng.below(15)),
            )),
            p_err_before: 0.05
                + f64::from(u32::try_from(rng.below(10)).expect("sample is below 10")) / 100.0,
            p_truncate: 0.05,
            ..Default::default()
        }
        .with_only(&["wal/"]),
    );
    let h = c.instances[j].open(&c.id).await?;
    let tasks = c.instances[j].registry.tasks().clone();
    let repo = c.id.to_string();

    // K concurrent object-level syncs; one random caller is aborted after a random delay.
    let k = 4 + rng.below_usize(4);
    let mut joins = Vec::new();
    for _ in 0..k {
        let h = h.clone();
        joins.push(tokio::spawn(async move {
            h.sync().await.map(drop).map_err(|e| e.to_string())
        }));
    }
    let victim = rng.below_usize(k);
    let abort_after = Duration::from_millis(rng.below(40));
    // Watch the task registry while they run: at most one materialize task at a time.
    let watcher = {
        let tasks = tasks.clone();
        let repo = repo.clone();
        tokio::spawn(async move {
            let mut max_running = 0usize;
            for _ in 0..400 {
                let n = tasks
                    .running(&repo)
                    .iter()
                    .filter(|t| t.kind == "materialize")
                    .count();
                max_running = max_running.max(n);
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
            max_running
        })
    };
    tokio::time::sleep(abort_after).await;
    joins[victim].abort();
    let mut errors = 0usize;
    for (i, jh) in joins.into_iter().enumerate() {
        match tokio::time::timeout(Duration::from_secs(30), jh).await {
            Ok(Ok(Ok(()))) => {}
            Ok(Ok(Err(_))) => errors += 1,
            Ok(Err(e)) if e.is_cancelled() && i == victim => errors += 1,
            Ok(Err(e)) => bail!("caller {i} panicked: {e}"),
            Err(_) => bail!(
                "caller {i} hung 30 s: a task lock was never released\n{}",
                c.dump_traces()
            ),
        }
    }
    let max_running = watcher.await?;
    ensure!(
        max_running <= 1,
        "{max_running} materialize tasks ran concurrently for one repo"
    );

    // Afterwards: nothing stuck, and a healed caller completes (or had completed).
    c.instances[j].link.heal();
    tokio::time::timeout(Duration::from_secs(30), h.sync())
        .await
        .map_err(|_| anyhow!("sync hung after the chaos\n{}", c.dump_traces()))??;
    ensure!(h.packs_ready(), "packs not ready after a healthy sync");
    ensure!(
        tasks.running(&repo).is_empty(),
        "a task is still marked running: {:?}",
        tasks.running(&repo)
    );
    let recent = tasks.recent(&repo);
    let materializes: Vec<_> = recent.iter().filter(|t| t.kind == "materialize").collect();
    ensure!(!materializes.is_empty());
    // Every start beyond the first is accounted for by a failure or the abort — never a duplicate.
    ensure!(
        materializes.len() <= 1 + errors + 1,
        "{} materialize tasks for {k} callers with {errors} failures:\n{:?}",
        materializes.len(),
        materializes
            .iter()
            .map(|t| (&t.id, &t.summary))
            .collect::<Vec<_>>()
    );
    // A late joiner attaches to the finished task and gets the replay + outcome.
    let last_ok = materializes
        .iter()
        .find(|t| t.ok == Some(true))
        .ok_or_else(|| anyhow!("no successful materialize: {materializes:?}"))?;
    let state = tasks
        .get(&last_ok.id)
        .ok_or_else(|| anyhow!("task state gone"))?;
    let (replay, _rx, outcome) = state.attach();
    ensure!(!replay.is_empty(), "late joiner got no replay");
    ensure!(
        matches!(outcome, Some(Ok(_))),
        "late joiner did not see the outcome: {outcome:?}"
    );
    // Downloads: every attempt downloads each pack at most once (+ idx); no N-fold traffic.
    let ops = usize::try_from(c.instances[j].link.stats().ops.load(Ordering::Relaxed))
        .context("store operation count does not fit usize")?;
    let attempts = materializes.len();
    let budget = attempts * (live_packs * 4 + 6) + k * 3 + 20;
    ensure!(
        ops <= budget,
        "{ops} store requests for {attempts} materialize attempt(s) over {live_packs} packs (budget {budget})"
    );
    check_truth(&c, std::slice::from_ref(&p)).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sim_task_ownership_under_concurrency_and_owner_crash() {
    for seed in seeds() {
        run_task_ownership(seed)
            .await
            .unwrap_or_else(|e| panic!("seed {seed}: {e:#}"));
    }
}

/// Budget-mode cache pressure: four repositories of which the cache holds about two, a
/// randomized interleaving of refs-level and object-level reads, one repository pinned by a
/// live `ReadGuard` throughout, plus one repository whose pack set exceeds `cache.max_bytes`.
/// Asserted after every step: the pinned repo is never evicted; the too-large repo is refused
/// with `TooLarge` (never materialized, never the cause of evicting the others); the cache
/// stays ≤ `max_bytes` + one pack set; a refs-level read on a cold repo during eviction stays fast.
async fn run_cache_pressure(seed: u64) -> Result<()> {
    let mut rng = Lcg(seed ^ 0x5EED);
    let truth: DynStore = MemoryStore::shared();
    let writer = Instance::new(&truth, "writer", seed, &|_| {});
    let mut ids = Vec::new();
    let mut pushers = Vec::new();
    for r in 0..4u32 {
        let id = RepoId::new("sim", format!("cache{seed}-{r}"))?;
        writer.registry.create(&id, ObjectFormat::Sha1).await?;
        let mut p = Pusher::new(r as usize);
        let new = push_blobby(&mut p, 96, &mut rng);
        let h = writer.registry.open(&id).await?;
        let pack = p.work.pack(&new, None);
        let ingested = h
            .local()
            .ingest_pack(
                std::io::Cursor::new(pack),
                IngestOptions {
                    fsck: false,
                    max_bytes: None,
                    thin: false,
                },
            )
            .await?
            .unwrap();
        let txn = RefTransaction {
            updates: vec![RefUpdate {
                name: "refs/heads/main".into(),
                new_oid: new.clone(),
                ..Default::default()
            }],
            ..Default::default()
        };
        h.publish_push(Some(ingested), txn, HashMap::new()).await?;
        p.tip = new;
        ids.push(id);
        pushers.push(p);
    }
    // The big one: ~5 × a small repo.
    let big = RepoId::new("sim", format!("cache{seed}-big"))?;
    writer.registry.create(&big, ObjectFormat::Sha1).await?;
    {
        let mut p = Pusher::new(9);
        let new = push_blobby(&mut p, 96 * 5, &mut rng);
        let h = writer.registry.open(&big).await?;
        let pack = p.work.pack(&new, None);
        let ingested = h
            .local()
            .ingest_pack(
                std::io::Cursor::new(pack),
                IngestOptions {
                    fsck: false,
                    max_bytes: None,
                    thin: false,
                },
            )
            .await?
            .unwrap();
        let txn = RefTransaction {
            updates: vec![RefUpdate {
                name: "refs/heads/main".into(),
                new_oid: new.clone(),
                ..Default::default()
            }],
            ..Default::default()
        };
        h.publish_push(Some(ingested), txn, HashMap::new()).await?;
    }
    let small_set = {
        let h = writer.registry.open(&ids[0]).await?;
        h.manifest()
            .packs
            .iter()
            .map(|p| p.pack_size + p.idx_size)
            .sum::<u64>()
    };
    let max_bytes = small_set * 2 + small_set / 2;

    // The pressured front: room for about two small repositories.
    let front = Instance::new(&truth, "front", seed + 1, &|cfg| {
        cfg.cache.max_bytes = walgit_config::ByteSize::b(max_bytes);
        cfg.cache.evict_idle_after = Duration::ZERO;
        cfg.wal.prefetch_packs = false;
    });
    front.link.set(FaultPlan {
        delay: Some((Duration::ZERO, Duration::from_millis(3))),
        ..Default::default()
    });

    // Pin repo 0 with a live read guard for the whole run.
    let pinned = front.registry.open(&ids[0]).await?;
    let pinned_path = pinned.local().path().to_path_buf();
    let guard = pinned.sync_full().await?;

    let mut refs_latencies = Vec::new();
    let mut total_evicted = 0usize;
    for step in 0..30u64 {
        let r = 1 + rng.below_usize(3); // repos 1..3
        let id = &ids[r];
        let h = front.registry.open(id).await?;
        match rng.below(3) {
            0 => {
                let t = Instant::now();
                drop(h.sync_refs().await?);
                refs_latencies.push(t.elapsed());
            }
            _ => {
                drop(
                    h.sync()
                        .await
                        .with_context(|| format!("step {step}: object sync of {id}"))?,
                );
            }
        }
        if rng.below(2) == 0 {
            // The too-large repo: refused, never materialized.
            let hb = front.registry.open(&big).await?;
            match hb.sync().await {
                Err(WalError::TooLarge { bytes, max }) => {
                    ensure!(bytes > max && max == max_bytes, "TooLarge {bytes} > {max}");
                }
                Ok(_) => bail!("too-large repo was materialized"),
                Err(e) => bail!("unexpected error for the too-large repo: {e}"),
            }
            ensure!(!hb.packs_ready(), "too-large repo has packs locally");
        }
        // Eviction pass (the registry's periodic sweep) and the invariants.
        let before = Instant::now();
        let report = front.registry.evict_idle().await?;
        total_evicted += report.evicted;
        let evict_took = before.elapsed();
        ensure!(
            pinned_path.join("objects").exists(),
            "step {step}: pinned repo evicted (report {report:?})"
        );
        let on_disk: u64 = ids
            .iter()
            .chain(std::iter::once(&big))
            .map(|i| i.local_dir(&front.cfg.cache.dir))
            .filter(|p| p.exists())
            .map(|p| dir_size_of(&p))
            .sum();
        ensure!(
            on_disk <= max_bytes + small_set * 2,
            "step {step}: {on_disk} bytes on disk > max {max_bytes} + one pack set {small_set} (report {report:?})"
        );
        // A cold refs-level read while/after eviction stays refs-fast.
        let other = &ids[1 + ((r) % 3)];
        let t = Instant::now();
        let ho = front.registry.open(other).await?;
        drop(ho.sync_refs().await?);
        let took = t.elapsed();
        ensure!(
            took < Duration::from_secs(1),
            "step {step}: refs read took {took:?} (eviction took {evict_took:?}; observed ≈ 20 ms)"
        );
        refs_latencies.push(took);
    }
    ensure!(pinned.packs_ready(), "pinned repo lost its packs");
    ensure!(
        total_evicted > 0,
        "the scenario never evicted anything: pressure not reached (max {max_bytes}, set {small_set})"
    );
    drop(guard);
    // Once unpinned, pressure may take it.
    let _ = front.registry.evict_idle().await?;
    let worst = refs_latencies.iter().max().copied().unwrap_or_default();
    eprintln!(
        "cache pressure seed {seed}: max_bytes {max_bytes}, small set {small_set}, worst refs read {worst:?}"
    );
    Ok(())
}

fn dir_size_of(p: &Path) -> u64 {
    let mut total = 0u64;
    if let Ok(rd) = std::fs::read_dir(p) {
        for e in rd.flatten() {
            let path = e.path();
            if path.is_dir() {
                total += dir_size_of(&path);
            } else if let Ok(m) = std::fs::symlink_metadata(&path) {
                total += m.len();
            }
        }
    }
    total
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sim_cache_pressure_keeps_pinned_repos_and_refuses_too_large() {
    for seed in seeds() {
        run_cache_pressure(seed)
            .await
            .unwrap_or_else(|e| panic!("seed {seed}: {e:#}"));
    }
}

#[allow(dead_code)]
fn _unused(_: WalError) {}
