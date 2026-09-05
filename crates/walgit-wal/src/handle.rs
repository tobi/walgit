//! `RepoHandle`: per-repository state, sync, publish, checkpoint.

use std::collections::HashMap;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Instant;

use parking_lot::{Mutex as PLMutex, RwLock as PLRwLock};
use tokio::sync::{Mutex as TokioMutex, RwLock as TokioRwLock, mpsc};
use tracing::Instrument;
use walgit_git::{LocalRepo, RepoId};
use walgit_proto::v1::Manifest;
use walgit_store::{Prefixed, Version};

use crate::error::WalError;
use crate::progress::{ProgressRx, ProgressTx, Reporter};
use crate::publish::{PublishRequest, PublishResult};
use crate::remote::{BlockCache, RemotePacks};
use crate::state::RepoState;
use crate::sync::{PackPlan, SyncLevel};
use crate::tasks::{Begin, Tasks};
use walgit_proto::v1::PackRef;

pub(crate) fn instance_id() -> String {
    walgit_store::coord::instance_id().to_string()
}

/// When a checkpoint's state is from (see `RepoHandle::checkpoint_times`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckpointTimes {
    /// When the checkpoint was written: state exists as of this instant.
    pub created_at: Option<std::time::SystemTime>,
    /// Earliest state the repository ever had (D22, carried forward).
    pub first_state_at: Option<std::time::SystemTime>,
    /// `created_at` of the newest entry folded: the state it holds is as of then.
    pub as_of: Option<std::time::SystemTime>,
}

pub struct RepoHandle {
    pub(crate) id: RepoId,
    pub(crate) local: LocalRepo,
    pub(crate) store: Prefixed,
    pub(crate) cfg: Arc<walgit_config::Config>,

    // Prevents pack removal during reads. Uses tokio::sync::RwLock because
    // guards are held across .await points (freshness check, apply_delta).
    pub(crate) rw: TokioRwLock<()>,
    // Single in-flight sync.
    pub(crate) sync_mutex: TokioMutex<()>,
    /// Serializes pack reconciliation (downloads/links/removals). Held
    /// *without* `sync_mutex`/`rw.write`, so a refs-level request is never
    /// stuck behind a multi-GB materialization (only removals take the write
    /// lock, briefly).
    pub(crate) pack_mutex: TokioMutex<()>,

    // Current manifest (last known). Short critical sections, no await.
    pub(crate) manifest: PLRwLock<Arc<Manifest>>,
    pub(crate) manifest_version: PLMutex<Option<Version>>,

    // Persistent local state.
    pub(crate) state: PLMutex<RepoState>,

    // Freshness TTL.
    pub(crate) last_freshness: PLMutex<Option<Instant>>,

    // Eviction tracking.
    pub(crate) last_access: PLMutex<Instant>,

    // Self-referential Arc for spawning the publisher task.
    pub(crate) self_arc: std::sync::OnceLock<Arc<RepoHandle>>,
    // Single-flight publisher channel.
    pub(crate) publish_tx: PLMutex<Option<mpsc::UnboundedSender<PublishRequest>>>,
    // Number of callers currently waiting for a publish response. Used by
    // the publisher to distinguish a lone push from a concurrent batch.
    pub(crate) publish_waiters: AtomicUsize,
    // Single-flight background pack prefetch after a refs-level sync.
    pub(crate) prefetch_inflight: std::sync::atomic::AtomicBool,
    /// Time of the oldest *live* log entry (the segment holding `min_seq`) this
    /// handle replayed, for `first_state_time`. Never a later segment: a process
    /// that warmed from disk and only fetched the tail must not take the tail's
    /// first entry for the repository's first state (prod 2026-08-21: the SSD host
    /// took seq 8 / 03:22Z and planned every earlier slot `unavailable`).
    pub(crate) first_entry_time: parking_lot::Mutex<Option<std::time::SystemTime>>,
    /// Times of the current checkpoint read from the checkpoint *object* when
    /// the manifest's `CheckpointRef` carries none (checkpoints written before
    /// the ref had `created_at`, e.g. a large repository's import): `(seq, created_at,
    /// first_state_at, as_of)`. One small GET per checkpoint per process.
    pub(crate) checkpoint_times: parking_lot::Mutex<Option<(u64, CheckpointTimes)>>,
    /// Time of the newest log entry seen (replayed or published): the floor
    /// for explicit `created_at` on publish.
    pub(crate) last_entry_time: parking_lot::Mutex<Option<std::time::SystemTime>>,
    /// `created_at` of seq 1 when this process published it: the checkpoint's `first_state_at`
    /// without a log read. Deliberately *not* `first_entry_time` (the slot planner's witness): a
    /// writer's own push today must not turn the current slots `unavailable` — the planner keeps
    /// learning first state from replayed segments and checkpoints only.
    pub(crate) first_seq_published_at: parking_lot::Mutex<Option<std::time::SystemTime>>,
    pub(crate) history_install_inflight: std::sync::atomic::AtomicBool,
    /// Fires when a history-pack install finishes (for `wait_history_pack_installed`).
    pub(crate) history_install_done: tokio::sync::Notify,
    /// D24: effective config cache — (settings revision, host config ⊕ settings).
    pub(crate) effective: parking_lot::Mutex<Option<(u64, Arc<walgit_config::Config>)>>,

    // Progress packets of every task touching this repo (SSE envelope).
    pub(crate) progress: ProgressTx,
    pub(crate) tasks: Arc<Tasks>,
    // Reporter of the task currently running inside sync (None = repo channel only).
    pub(crate) active_reporter: PLMutex<Option<Reporter>>,
    // Remote object access (pack set too large to materialize).
    pub(crate) blocks: Arc<BlockCache>,
    pub(crate) remote: PLMutex<Option<Arc<RemotePacks>>>,
}

/// How a request may read objects after [`RepoHandle::sync_objects`].
#[derive(Clone)]
pub enum ObjectAccess {
    /// Packs are installed in the local repository; run git/gix locally.
    Local,
    /// Pack set too large for this instance: indexes local, data by range read.
    Remote(Arc<RemotePacks>),
}

impl ObjectAccess {
    pub fn is_remote(&self) -> bool {
        matches!(self, ObjectAccess::Remote(_))
    }
}

impl RepoHandle {
    #[expect(
        clippy::too_many_arguments,
        reason = "Repository construction combines the shared services and loaded WAL state"
    )]
    pub(crate) fn new(
        id: RepoId,
        local: LocalRepo,
        store: Prefixed,
        cfg: Arc<walgit_config::Config>,
        manifest: Manifest,
        version: Option<Version>,
        state: RepoState,
        tasks: Arc<Tasks>,
        blocks: Arc<BlockCache>,
    ) -> Self {
        let (progress, _) = tokio::sync::broadcast::channel(1024);
        RepoHandle {
            id,
            local,
            store,
            cfg,
            rw: TokioRwLock::new(()),
            sync_mutex: TokioMutex::new(()),
            pack_mutex: TokioMutex::new(()),
            manifest: PLRwLock::new(Arc::new(manifest)),
            manifest_version: PLMutex::new(version),
            state: PLMutex::new(state),
            last_freshness: PLMutex::new(None),
            last_access: PLMutex::new(Instant::now()),
            self_arc: std::sync::OnceLock::new(),
            publish_tx: PLMutex::new(None),
            publish_waiters: AtomicUsize::new(0),
            prefetch_inflight: std::sync::atomic::AtomicBool::new(false),
            first_entry_time: parking_lot::Mutex::new(None),
            checkpoint_times: parking_lot::Mutex::new(None),
            last_entry_time: parking_lot::Mutex::new(None),
            first_seq_published_at: parking_lot::Mutex::new(None),
            history_install_inflight: std::sync::atomic::AtomicBool::new(false),
            history_install_done: tokio::sync::Notify::new(),
            effective: parking_lot::Mutex::new(None),
            progress,
            tasks,
            active_reporter: PLMutex::new(None),
            blocks,
            remote: PLMutex::new(None),
        }
    }

    /// Live progress of everything happening to this repo on this instance.
    /// Subscribe *before* starting the work you want to watch.
    pub fn subscribe_progress(&self) -> ProgressRx {
        self.progress.subscribe()
    }

    /// Reporter for work done on behalf of this repo: the running sync task's
    /// (so packets land in its record) or the bare repo channel.
    pub fn reporter(&self) -> Reporter {
        self.active_reporter
            .lock()
            .clone()
            .unwrap_or_else(|| Reporter::for_repo(self.progress.clone()))
    }

    pub fn tasks(&self) -> &Arc<Tasks> {
        &self.tasks
    }

    pub fn progress_tx(&self) -> ProgressTx {
        self.progress.clone()
    }

    /// Begin a task of `kind` on this repo (lock per (repo, kind); packets
    /// mirrored into the repo channel).
    pub fn begin_task(&self, kind: &str, params: HashMap<String, String>) -> Begin {
        self.tasks.begin(
            &self.id.to_string(),
            kind,
            params,
            Some(self.progress.clone()),
        )
    }

    /// Whether the live pack set fits this instance's cache as a full local
    /// copy (what the web API's object path needs to go local instead of the
    /// remote reader).
    pub fn packs_fit(&self) -> bool {
        self.check_fits(&self.manifest(), SyncLevel::Full).is_ok()
    }

    /// Whether a serving copy fits: like [`packs_fit`] but base packs that can
    /// be linked from the store mount count only their side-files.
    pub fn serve_fits(&self) -> bool {
        self.check_fits(&self.manifest(), SyncLevel::Serve).is_ok()
    }

    /// Repository root inside the mounted bucket (`cache.store_mount`), if
    /// configured and present: `<mount>/<store.prefix>repos/<o>/<r>/`.
    pub fn mount_dir(&self) -> Option<std::path::PathBuf> {
        let mount = self.cfg.cache.store_mount.as_ref()?;
        let dir = mount
            .join(self.cfg.store_prefix())
            .join(self.id.store_prefix());
        dir.is_dir().then_some(dir)
    }

    /// Whether `checksum`'s pack is readable from the store mount.
    pub fn mounted_pack(&self, checksum: &str) -> Option<std::path::PathBuf> {
        let dir = self.mount_dir()?;
        let p = crate::sync::mount_pack_path(&dir, checksum);
        p.is_file().then_some(p)
    }

    /// Current remote reader (if this repo is served remotely).
    pub fn remote(&self) -> Option<Arc<RemotePacks>> {
        self.remote.lock().clone()
    }

    /// Make objects readable: full sync (packs local) when they fit, otherwise
    /// a refs sync plus the remote reader (indexes local, data by range read).
    /// Long work registers as a task ("materialize" / "remote-index") whose
    /// progress streams on the repo channel.
    pub async fn sync_objects(
        &self,
    ) -> Result<(crate::sync::ReadGuard<'_>, ObjectAccess), WalError> {
        // Common case: packs fit (as far as the last known manifest says) →
        // one full sync, one manifest round trip. TooLarge (manifest grew) falls
        // through to the remote path.
        if !self.cfg.wal.remote_objects || self.packs_fit() {
            match self.sync_level(SyncLevel::Serve).await {
                Ok(guard) => return Ok((guard, ObjectAccess::Local)),
                Err(WalError::TooLarge { .. }) if self.cfg.wal.remote_objects => {}
                Err(e) => return Err(e),
            }
        }
        let guard = self.sync_level(SyncLevel::Refs).await?;
        let manifest = guard.manifest();
        if self.check_fits(&manifest, SyncLevel::Full).is_ok() {
            drop(guard);
            let guard = self.sync_level(SyncLevel::Serve).await?;
            return Ok((guard, ObjectAccess::Local));
        }
        // Remote: reuse the reader for this manifest revision, else (re)open.
        if let Some(r) = self.remote.lock().clone()
            && r.revision == manifest.revision
        {
            return Ok((guard, ObjectAccess::Remote(r)));
        }
        let remote = self.open_remote(&manifest).await?;
        Ok((guard, ObjectAccess::Remote(remote)))
    }

    async fn open_remote(&self, manifest: &Manifest) -> Result<Arc<RemotePacks>, WalError> {
        match self.begin_task("remote-index", HashMap::new()) {
            Begin::Started(task) => {
                let reporter = task.reporter();
                let store = self.store.clone();
                let manifest = manifest.clone();
                let path = self.local.path().to_path_buf();
                let hash = self.local.object_format().kind();
                let blocks = self.blocks.clone();
                let object_cache_bytes = self.cfg.cache.remote_object_bytes.as_u64();
                let reporter_bulk = reporter.clone();
                let res = crate::sync::on_bulk_runtime(async move {
                    RemotePacks::open(
                        store,
                        &manifest,
                        &path,
                        hash,
                        blocks,
                        object_cache_bytes,
                        &reporter_bulk,
                    )
                    .await
                })
                .instrument(task.span())
                .await;
                match res {
                    Ok(r) => {
                        let r = Arc::new(r);
                        *self.remote.lock() = Some(r.clone());
                        task.finish_ok(
                            format!(
                                "{} pack index(es) ready, {} objects addressable",
                                r.pack_count(),
                                r.total_objects()
                            ),
                            None,
                        );
                        Ok(r)
                    }
                    Err(e) => {
                        task.finish_err(503, e.to_string());
                        Err(e)
                    }
                }
            }
            Begin::AlreadyRunning(state) => {
                // Another request is opening it: wait for that task, then reuse.
                let _ = state.wait_done(std::time::Duration::from_mins(10)).await;
                match state.outcome() {
                    Some(Ok(_)) => {}
                    Some(Err((_, m))) => {
                        return Err(WalError::Corrupt(format!("remote index task failed: {m}")));
                    }
                    None => return Err(WalError::Corrupt("remote index task vanished".into())),
                }
                self.remote
                    .lock()
                    .clone()
                    .ok_or_else(|| WalError::Corrupt("remote reader missing after task".into()))
            }
        }
    }

    pub(crate) fn set_self_arc(&self, arc: Arc<RepoHandle>) {
        let _ = self.self_arc.set(arc);
    }

    // ---- public API ----

    pub fn id(&self) -> &RepoId {
        &self.id
    }

    pub fn local(&self) -> &LocalRepo {
        &self.local
    }

    pub fn store(&self) -> &Prefixed {
        &self.store
    }

    pub fn config(&self) -> &Arc<walgit_config::Config> {
        &self.cfg
    }

    pub fn manifest(&self) -> Arc<Manifest> {
        self.manifest.read().clone()
    }

    pub fn manifest_version(&self) -> Option<Version> {
        self.manifest_version.lock().clone()
    }

    /// Last applied log entry sequence (local replay progress).
    pub fn applied_seq(&self) -> u64 {
        self.state.lock().applied_seq
    }

    /// Persisted manifest version string from the local state file.
    pub fn local_version(&self) -> Option<String> {
        self.state.lock().manifest_version.clone()
    }

    pub fn last_access(&self) -> Instant {
        *self.last_access.lock()
    }

    pub fn touch(&self) {
        *self.last_access.lock() = Instant::now();
    }

    /// Freshness check + serving catch-up (refs **and** packs; base packs
    /// linked from the store mount when configured, see [`SyncLevel::Serve`]).
    /// Returns a read guard; while any guard is alive no pack is removed
    /// locally. Required before anything that reads or verifies objects
    /// (upload-pack, receive-pack).
    pub async fn sync(&self) -> Result<crate::sync::ReadGuard<'_>, WalError> {
        self.sync_level(SyncLevel::Serve).await
    }

    /// Like [`sync`] but every pack is a real local copy (base rebuilds,
    /// bundle builds and anything else that streams whole packs). Refused with
    /// `TooLarge` when the set does not fit `cache.max_bytes`.
    pub async fn sync_full(&self) -> Result<crate::sync::ReadGuard<'_>, WalError> {
        self.sync_level(SyncLevel::Full).await
    }

    /// Freshness check + refs-only catch-up: applies the WAL's ref state but
    /// downloads no packs. This is the cheap cold-start path for
    /// `info/refs`, `ls-refs`, `bundle-uri` and the web `refs` endpoint.
    /// When packs are not yet reconciled and `wal.prefetch_packs` is on, a
    /// background Serve sync is kicked off so the first fetch finds them — for
    /// pack sets up to `wal.prefetch_max_bytes` only ([`prefetch_wanted`]).
    pub async fn sync_refs(&self) -> Result<crate::sync::ReadGuard<'_>, WalError> {
        let guard = self.sync_level(SyncLevel::Refs).await?;
        if self.prefetch_wanted() {
            self.spawn_pack_prefetch();
        }
        Ok(guard)
    }

    /// Whether a refs-level sync should pull the serving copy in the background:
    /// configured, not yet reconciled, this host serves the repository's objects
    /// (placement — a host that does not never pulls its packs, not even in the
    /// background), the copy fits the cache, and what lands on disk is at most
    /// `wal.prefetch_max_bytes` (0 = unbounded). Bigger sets are materialized by
    /// the request that needs them, narrated.
    pub fn prefetch_wanted(&self) -> bool {
        if !self.cfg.wal.prefetch_packs || self.packs_ready() {
            return false;
        }
        if !self.cfg.placement.serves(self.id.owner(), self.id.name()) || !self.serve_fits() {
            return false;
        }
        let cap = self.cfg.wal.prefetch_max_bytes.as_u64();
        if cap == 0 {
            return true;
        }
        let manifest = self.manifest();
        let bytes: u64 = self
            .serve_plan(&manifest, SyncLevel::Serve)
            .iter()
            .map(|(p, how)| how.tmpfs_bytes(p))
            .sum();
        if bytes > cap {
            tracing::debug!(repo = %self.id, bytes, cap, "pack prefetch skipped: serving copy above wal.prefetch_max_bytes; materialized on demand");
            return false;
        }
        true
    }

    /// True when the local pack set matches the last applied manifest.
    pub fn packs_ready(&self) -> bool {
        self.state.lock().packs_ready()
    }

    /// Install history packs (D18) in the background: a `history-pack` task
    /// under `pack_mutex`, narrated like any materialization; serving goes on
    /// from the base meanwhile and switches to the local history pack once
    /// the midx is in place.
    pub(crate) fn spawn_history_pack_install(&self, packs: Vec<PackRef>) {
        if self.history_install_inflight.swap(true, Ordering::AcqRel) {
            return;
        }
        let Some(arc) = self.self_arc.get().cloned() else {
            self.history_install_inflight
                .store(false, Ordering::Release);
            return;
        };
        tokio::spawn(async move {
            let span = tracing::info_span!("wal.history_pack_install", repo = %arc.id, packs = packs.len());
            let res = async {
                let _pack_guard = crate::lockwait::timed(
                    "pack_mutex",
                    &arc.id,
                    arc.cfg.telemetry.lock_wait_warn,
                    || arc.pack_mutex.try_lock().ok(),
                    arc.pack_mutex.lock(),
                )
                .await;
                let manifest = arc.manifest();
                let task = match arc.begin_task("history-pack", HashMap::new()) {
                    Begin::Started(t) => Some(t),
                    Begin::AlreadyRunning(_) => None,
                };
                if let Some(t) = &task {
                    *arc.active_reporter.lock() = Some(t.reporter());
                }
                let arc2 = arc.clone();
                let r = crate::sync::on_bulk_runtime(async move {
                    crate::sync::reconcile_packs_inner(&arc2, &manifest, SyncLevel::Serve, true)
                        .await
                })
                .await;
                *arc.active_reporter.lock() = None;
                if let Some(t) = task {
                    match &r {
                        Ok(()) => {
                            t.finish_ok(
                                "history pack installed: commits + trees are local".to_string(),
                                None,
                            );
                        }
                        Err(e) => {
                            t.finish_err(500, e.to_string());
                        }
                    }
                }
                r
            }
            .instrument(span)
            .await;
            if let Err(e) = res {
                tracing::warn!(repo = %arc.id, error = %e, "history pack install failed");
            }
            arc.history_install_inflight.store(false, Ordering::Release);
            arc.history_install_done.notify_waiters();
        });
    }

    /// Whether a background history-pack install (D18) is running now.
    pub fn history_pack_install_inflight(&self) -> bool {
        self.history_install_inflight.load(Ordering::Acquire)
    }

    /// Wait until no history-pack install is in flight (returns immediately
    /// when none is). Prewarm uses it so `/readyz` only flips once the serving
    /// copy is complete — history pack + midx installed — not merely planned.
    pub async fn wait_history_pack_installed(&self) {
        loop {
            let notified = self.history_install_done.notified();
            if !self.history_pack_install_inflight() {
                return;
            }
            notified.await;
        }
    }

    fn spawn_pack_prefetch(&self) {
        if self.prefetch_inflight.swap(true, Ordering::AcqRel) {
            return;
        }
        let Some(arc) = self.self_arc.get().cloned() else {
            self.prefetch_inflight.store(false, Ordering::Release);
            return;
        };
        tokio::spawn(async move {
            let span = tracing::info_span!("wal.prefetch_packs", repo = %arc.id);
            if let Err(e) = arc.sync_impl_level(SyncLevel::Serve).instrument(span).await {
                tracing::warn!(repo = %arc.id, error = ?e, "background pack prefetch failed");
            }
            arc.prefetch_inflight.store(false, Ordering::Release);
        });
    }

    async fn sync_level(&self, level: SyncLevel) -> Result<crate::sync::ReadGuard<'_>, WalError> {
        let span = tracing::info_span!(
            "wal.sync",
            repo = %self.id,
            level = ?level,
            changed = false,
            entries_applied = 0u64,
        );
        self.touch();
        // Phase 1 — refs (manifest freshness + ref state), under sync_mutex
        // only; sub-second. Phase 2 — packs, under pack_mutex only. Neither
        // takes `rw.write()`: that lock exists solely so a superseded pack is
        // not unlinked under an active reader, and it is only ever
        // `try_write()`n (see sync.rs) — a queued writer on a tokio RwLock
        // blocks every *new* reader until all current readers are gone, and a
        // clone's ReadGuard lives for the whole stream (prod 2026-08-20: a
        // 24-minute clone + one queued writer = every info/refs on the
        // instance waited 60–680 s on `rw.read()`).
        self.sync_refs_phase(&span).instrument(span.clone()).await?;
        if level.wants_packs() {
            self.sync_packs_phase(level, &span)
                .instrument(span.clone())
                .await?;
        }
        let read_guard = crate::lockwait::timed(
            "rw.read",
            &self.id,
            self.cfg.telemetry.lock_wait_warn,
            || self.rw.try_read().ok(),
            self.rw.read().instrument(span.clone()),
        )
        .await;
        Ok(crate::sync::ReadGuard {
            _guard: read_guard,
            handle: self,
        })
    }

    /// Manifest freshness check + ref state apply (never packs, never
    /// `rw.write()`: ref files and the gix handle are replaced atomically).
    async fn sync_refs_phase(&self, span: &tracing::Span) -> Result<(), WalError> {
        if self.freshness_ttl_active() {
            return Ok(());
        }
        let _sync_guard = crate::lockwait::timed(
            "sync_mutex",
            &self.id,
            self.cfg.telemetry.lock_wait_warn,
            || self.sync_mutex.try_lock().ok(),
            self.sync_mutex.lock(),
        )
        .await;
        if self.freshness_ttl_active() {
            return Ok(());
        }
        self.sync_locked_inner(span).await
    }

    /// Make the local pack set match the plan for `level` (download / link /
    /// remote-serve / drop superseded). Registers the `materialize` task.
    /// Serialized by `pack_mutex`; concurrent readers keep reading (packs are
    /// only added; removals take the write lock for the rename alone).
    async fn sync_packs_phase(
        &self,
        level: SyncLevel,
        _span: &tracing::Span,
    ) -> Result<(), WalError> {
        if self.level_satisfied(level) {
            return Ok(());
        }
        // A second caller of a running materialize waits here: the task join, measured.
        let _pack_guard = crate::lockwait::timed(
            "pack_mutex",
            &self.id,
            self.cfg.telemetry.lock_wait_warn,
            || self.pack_mutex.try_lock().ok(),
            self.pack_mutex.lock(),
        )
        .await;
        if self.level_satisfied(level) {
            return Ok(());
        }
        let manifest = self.manifest();
        self.check_fits(&manifest, level)?;
        let task = match self.begin_task("materialize", HashMap::new()) {
            Begin::Started(t) => {
                *self.active_reporter.lock() = Some(t.reporter());
                Some(t)
            }
            Begin::AlreadyRunning(_) => None, // cannot happen: pack_mutex is held
        };
        // The whole materialization runs on the bulk runtime (own threads):
        // nothing in it can stall this runtime's request workers.
        let arc = self.self_arc.get().cloned();
        let res = if let Some(arc) = arc {
            let m = manifest.clone();
            let task_span = task.as_ref().map(super::tasks::TaskHandle::span);
            crate::sync::on_bulk_runtime(async move {
                let work = async {
                    crate::sync::reconcile_packs(&arc, &m, level).await?;
                    arc.local.refresh_async().await?;
                    Ok::<(), WalError>(())
                };
                match task_span {
                    Some(sp) => work.instrument(sp).await,
                    None => work.await,
                }
            })
            .await
        } else {
            let res = async {
                crate::sync::reconcile_packs(self, &manifest, level).await?;
                self.local.refresh_async().await?;
                Ok::<(), WalError>(())
            };
            match &task {
                Some(t) => res.instrument(t.span()).await,
                None => res.await,
            }
        };
        *self.active_reporter.lock() = None;
        if let Some(t) = task {
            match &res {
                Ok(()) => {
                    t.finish_ok(
                        format!("local copy complete at seq {}", self.applied_seq()),
                        None,
                    );
                }
                Err(e) => {
                    let status = if matches!(e, WalError::TooLarge { .. }) {
                        503
                    } else {
                        500
                    };
                    t.finish_err(status, e.to_string());
                }
            }
        }
        res
    }

    /// Freshness check + apply, with `sync_mutex` and the write lock held by
    /// the caller.
    /// Never pull a pack set that cannot fit the local cache (tmpfs):
    /// a 32 GB monorepo must not OOM a 20 GiB instance. Refs-level syncs are fine.
    /// Counts only what would land on tmpfs (see [`serve_plan`]).
    fn check_fits(&self, manifest: &Manifest, level: SyncLevel) -> Result<(), WalError> {
        let max = self.cfg.cache_budget_bytes();
        let plan = self.serve_plan(manifest, level);
        let bytes: u64 = plan.iter().map(|(p, how)| how.tmpfs_bytes(p)).sum();
        if max > 0 && bytes > max {
            metrics::counter!("walgit_sync_too_large_total").increment(1);
            return Err(WalError::TooLarge { bytes, max });
        }
        Ok(())
    }

    /// How each live pack is served on this instance at `level`:
    /// * every pack a local copy when the whole set fits `cache.max_bytes`
    ///   (or the level is `Full`);
    /// * otherwise (`Serve`) tier-2 bases are **linked** from the store mount
    ///   when one has them, else **remote-served** (commit-graph layer local,
    ///   data through the remote reader; `wal.remote_objects`), tiers < 2 stay
    ///   local copies — that is the invariant "everything newer than the base
    ///   fits every instance".
    pub fn serve_plan(&self, manifest: &Manifest, level: SyncLevel) -> Vec<(PackRef, PackPlan)> {
        let max = self.cfg.cache_budget_bytes();
        let full: u64 = manifest
            .packs
            .iter()
            .map(|p| p.pack_size + p.idx_size)
            .sum();
        if level != SyncLevel::Serve || max == 0 || full <= max {
            return manifest
                .packs
                .iter()
                .map(|p| (p.clone(), PackPlan::Local))
                .collect();
        }
        let mount = self.mount_dir();
        if mount.is_none()
            && let Some(store_mount) = &self.cfg.cache.store_mount
        {
            tracing::warn!(repo = %self.id, mount = %store_mount.display(), "store mount configured but the repository directory is not visible in it (gcsfuse not up yet?): base packs served remotely until it is");
        }
        manifest
            .packs
            .iter()
            .map(|p| {
                // History packs (commits + trees of a base) are always local:
                // that is the point of them.
                let how = if p.tier != 2 || p.kind == walgit_proto::v1::PackKind::History as i32 {
                    PackPlan::Local
                } else if let Some(m) = mount.as_ref() {
                    let target = crate::sync::mount_pack_path(m, &p.checksum);
                    if target.is_file() {
                        PackPlan::Link(target)
                    } else {
                        tracing::warn!(repo = %self.id, pack = %p.checksum, target = %target.display(), "base pack not readable through the store mount (gcsfuse not up yet?): serving it remotely until it is");
                        if self.cfg.wal.remote_objects { PackPlan::Remote } else { PackPlan::Local }
                    }
                } else if self.cfg.wal.remote_objects {
                    PackPlan::Remote
                } else {
                    PackPlan::Local
                };
                (p.clone(), how)
            })
            .collect()
    }

    /// Tier-2 packs this instance serves remotely (see [`PackPlan::Remote`]).
    /// Non-empty means upload-pack must use the gix engine with an
    /// [`ObjectFaulter`](walgit_git::ObjectFaulter) over [`remote_reader`].
    pub fn remote_served(&self) -> Vec<String> {
        self.state.lock().remote_served.clone()
    }

    /// The remote reader for the current manifest (opened once per manifest
    /// revision; pack indexes local, data by range read). Registers the
    /// `remote-index` task while opening.
    pub async fn remote_reader(&self) -> Result<Arc<RemotePacks>, WalError> {
        let manifest = self.manifest();
        if let Some(r) = self.remote.lock().clone()
            && r.revision == manifest.revision
        {
            return Ok(r);
        }
        self.open_remote(&manifest).await
    }

    /// Freshness check + refs apply, with `sync_mutex` and the write lock held
    /// by the caller. Packs are never touched here (see `sync_packs_phase`).
    async fn sync_locked_inner(&self, span: &tracing::Span) -> Result<(), WalError> {
        let known = self.manifest_version.lock().clone();
        let outcome = crate::sync::freshness_check(&self.store, known.as_ref()).await?;
        match outcome {
            crate::sync::SyncOutcome::Unchanged => self.update_freshness(),
            crate::sync::SyncOutcome::Changed {
                meta_version,
                manifest,
            } => {
                // Monotonic: never apply a manifest older than the one this instance already holds. A
                // publish on this instance commits locally (manifest, version, refs) outside this lock, so
                // a sync that read the manifest just before that CAS can arrive here with the previous
                // revision — applying it rewrote packed-refs to the pre-push state and rolled the known
                // version back, so one `ls-remote` right after an acknowledged push answered the OLD tip
                // (a concurrency regression test; the next request's conditional GET then
                // repaired it). The revision increments on every manifest write.
                let cur = self.manifest();
                let initialised = self.manifest_version.lock().is_some();
                if manifest.revision < cur.revision {
                    tracing::debug!(repo = %self.id, read_rev = manifest.revision, held_rev = cur.revision, "stale manifest read ignored (a local publish is ahead)");
                    self.update_freshness();
                    return Ok(());
                }
                if initialised && manifest.revision == cur.revision {
                    // Same content under a version we did not record (a publish that learned the version
                    // by HEAD): adopt the version so the next check is a 304, apply nothing.
                    *self.manifest_version.lock() = Some(meta_version);
                    self.update_freshness();
                    return Ok(());
                }
                span.record("changed", true);
                let before = self.state.lock().applied_seq;
                crate::sync::apply_delta(self, &manifest, &meta_version).await?;
                span.record("entries_applied", manifest.head_seq.saturating_sub(before));
                *self.manifest.write() = manifest;
                *self.manifest_version.lock() = Some(meta_version);
                self.update_freshness();
            }
        }
        Ok(())
    }

    fn level_satisfied(&self, level: SyncLevel) -> bool {
        match level {
            SyncLevel::Refs => true,
            // A base remote-served only because the mount was not yet
            // readable (gcsfuse comes up after the container) is re-planned
            // on the next sync once the file is visible.
            SyncLevel::Serve => self.packs_ready() && !self.remote_served_but_mountable(),
            SyncLevel::Full => self.packs_ready() && !self.has_linked_packs(),
        }
    }

    /// Remote-served packs whose file is now readable through the mount.
    fn remote_served_but_mountable(&self) -> bool {
        let served = self.remote_served();
        if served.is_empty() {
            return false;
        }
        let Some(m) = self.mount_dir() else {
            return false;
        };
        served
            .iter()
            .any(|c| crate::sync::mount_pack_path(&m, c).is_file())
    }

    /// Any local pack that is a symlink into the store mount.
    fn has_linked_packs(&self) -> bool {
        self.local.packs().is_ok_and(|ps| {
            ps.iter()
                .any(|p| self.local.pack_path(&p.checksum).is_symlink())
        })
    }

    /// Internal serving sync (no read guard). Used by `publish/checkpoint/read_log`.
    pub(crate) async fn sync_impl(&self) -> Result<(), WalError> {
        self.sync_impl_level(SyncLevel::Serve).await
    }

    pub(crate) async fn sync_impl_level(&self, level: SyncLevel) -> Result<(), WalError> {
        let span = tracing::info_span!("wal.sync", repo = %self.id, level = ?level, changed = false, entries_applied = 0u64);
        self.sync_refs_phase(&span).instrument(span.clone()).await?;
        if level.wants_packs() {
            self.sync_packs_phase(level, &span)
                .instrument(span.clone())
                .await?;
        }
        Ok(())
    }

    /// Force full re-materialize from store (repair).
    pub async fn rematerialize(&self) -> Result<(), WalError> {
        let _sync_guard = crate::lockwait::timed(
            "sync_mutex",
            &self.id,
            self.cfg.telemetry.lock_wait_warn,
            || self.sync_mutex.try_lock().ok(),
            self.sync_mutex.lock(),
        )
        .await;
        let _pack_guard = crate::lockwait::timed(
            "pack_mutex",
            &self.id,
            self.cfg.telemetry.lock_wait_warn,
            || self.pack_mutex.try_lock().ok(),
            self.pack_mutex.lock(),
        )
        .await;

        // Read manifest fresh
        let Some((meta, manifest)) =
            crate::store_proto::get_message::<Manifest>(&self.store, walgit_proto::keys::MANIFEST)
                .await?
        else {
            return Err(WalError::NotFound);
        };

        // Reset state and re-materialize
        crate::sync::materialize_from_scratch(self, &manifest, &meta.version).await?;

        *self.manifest.write() = Arc::new(manifest);
        *self.manifest_version.lock() = Some(meta.version);
        self.last_freshness.lock().take();

        Ok(())
    }

    /// Publish a push.
    pub async fn publish_push(
        &self,
        pack: Option<walgit_git::IngestedPack>,
        txn: walgit_proto::v1::RefTransaction,
        meta: HashMap<String, String>,
    ) -> Result<PublishResult, WalError> {
        self.enqueue_publish(pack, txn, meta, false).await
    }

    /// Publish a push when the caller has already completed `sync()`.
    ///
    /// Receive-pack holds a read guard while parsing and ingesting the pack.
    /// Reusing that freshness check avoids a second conditional manifest GET
    /// before the publisher's first CAS attempt. The publisher still syncs
    /// after every CAS conflict.
    pub async fn publish_push_synced(
        &self,
        pack: Option<walgit_git::IngestedPack>,
        txn: walgit_proto::v1::RefTransaction,
        meta: HashMap<String, String>,
    ) -> Result<PublishResult, WalError> {
        self.enqueue_publish(pack, txn, meta, true).await
    }

    /// Publish with an explicit entry time (history replay into the WAL): the
    /// log entry's `created_at` is `at` instead of now, validated monotonic
    /// (≥ the head entry's time; ≥ earlier explicit times in the same batch),
    /// else every ref of the transaction is rejected with the reason. The WAL
    /// itself never enforces fast-forward — that is receive-pack/policy — so a
    /// replay may move `main` non-ancestrally between slots; callers pass
    /// `old_oid` = the current value (or "" to skip the old check).
    pub async fn publish_push_at(
        &self,
        pack: Option<walgit_git::IngestedPack>,
        txn: walgit_proto::v1::RefTransaction,
        meta: HashMap<String, String>,
        at: std::time::SystemTime,
    ) -> Result<PublishResult, WalError> {
        self.enqueue_publish_at(
            pack,
            txn,
            meta,
            false,
            Some(walgit_proto::time::from_system(at)),
        )
        .await
    }

    async fn enqueue_publish(
        &self,
        pack: Option<walgit_git::IngestedPack>,
        txn: walgit_proto::v1::RefTransaction,
        meta: HashMap<String, String>,
        synced: bool,
    ) -> Result<PublishResult, WalError> {
        self.enqueue_publish_at(pack, txn, meta, synced, None).await
    }

    async fn enqueue_publish_at(
        &self,
        pack: Option<walgit_git::IngestedPack>,
        txn: walgit_proto::v1::RefTransaction,
        meta: HashMap<String, String>,
        synced: bool,
        created_at: Option<prost_types::Timestamp>,
    ) -> Result<PublishResult, WalError> {
        let sender = self.get_or_init_publisher()?;
        self.publish_waiters.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = tokio::sync::oneshot::channel();
        let request = PublishRequest {
            pack,
            txn,
            meta,
            synced,
            created_at,
            response: tx,
        };

        if sender.send(request).is_err() {
            self.publish_waiters.fetch_sub(1, Ordering::Relaxed);
            return Err(WalError::Corrupt("publisher channel closed".into()));
        }

        let result = rx
            .await
            .map_err(|_| WalError::Corrupt("publisher dropped response".into()))?;
        self.publish_waiters.fetch_sub(1, Ordering::Relaxed);
        result
    }
    /// Publish a ref-only update (no pack).
    pub async fn publish_ref_update(
        &self,
        txn: walgit_proto::v1::RefTransaction,
        meta: HashMap<String, String>,
    ) -> Result<PublishResult, WalError> {
        self.publish_push(None, txn, meta).await
    }

    /// Publish a compact entry.
    pub async fn publish_compact(
        &self,
        new_pack: walgit_git::PackInfo,
        supersedes: Vec<gix_hash::ObjectId>,
        tier: u32,
    ) -> Result<u64, WalError> {
        crate::publish::publish_compact_impl(self, new_pack, supersedes, tier).await
    }

    /// D24: the repository's settings as last applied (manifest-inline).
    pub fn settings(&self) -> Option<walgit_proto::v1::RepoSettings> {
        self.manifest().settings.clone()
    }

    /// D24: effective configuration = host config ⊕ this repository's
    /// settings, cached per settings revision. Settings that no longer parse
    /// against this build fall back to the host config with a warning
    /// (never a failure on a read path).
    pub fn effective_config(&self) -> Arc<walgit_config::Config> {
        let settings = self.settings();
        let rev = settings.as_ref().map_or(0, |s| s.revision);
        if rev == 0 {
            return self.cfg.clone();
        }
        if let Some((r, c)) = self.effective.lock().as_ref()
            && *r == rev
        {
            return c.clone();
        }
        let toml = settings.as_ref().map_or("", |s| s.toml.as_str());
        let cfg = match self.cfg.with_settings(toml) {
            Ok(c) => Arc::new(c),
            Err(e) => {
                tracing::warn!(repo = %self.id, revision = rev, error = %e, "repo settings do not apply to this build; using the host config");
                self.cfg.clone()
            }
        };
        *self.effective.lock() = Some((rev, cfg.clone()));
        cfg
    }

    /// D24: validate and publish new settings (whole-document replace): a
    /// SETTINGS log entry + the manifest's inline copy, one CAS. Returns the
    /// new revision. `toml` empty = clear.
    pub async fn publish_settings(
        &self,
        toml: &str,
        author: &str,
        message: &str,
    ) -> Result<u64, WalError> {
        // Validate against THIS host's config (the effective config must load).
        self.cfg
            .with_settings(toml)
            .map_err(|e| WalError::Invalid(format!("{e:#}")))?;
        crate::publish::publish_settings_impl(self, toml, author, message).await
    }

    /// Write checkpoint at current head.
    pub async fn write_checkpoint(&self) -> Result<walgit_proto::v1::CheckpointRef, WalError> {
        crate::checkpoint::write_checkpoint_impl(self).await
    }

    /// Attach side-files (rev/bitmap/commit-graph) to a published pack and
    /// advertise them in the manifest (CAS). See `publish::annotate_pack_impl`.
    pub async fn annotate_pack(
        &self,
        checksum: &str,
        rev: Option<std::path::PathBuf>,
        bitmap: Option<std::path::PathBuf>,
        commit_graph: Option<std::path::PathBuf>,
    ) -> Result<walgit_proto::v1::PackRef, WalError> {
        crate::publish::annotate_pack_impl(self, checksum, rev, bitmap, commit_graph).await
    }

    /// Publish an already built pack (`pack-<checksum>.pack` + `.idx`) as a
    /// tier-`tier` COMPACT entry superseding nothing; `history_of = Some(base)`
    /// marks it a history pack of that base (D18).
    pub async fn add_pack(
        &self,
        pack: &std::path::Path,
        idx: &std::path::Path,
        tier: u32,
        history_of: Option<String>,
    ) -> Result<u64, WalError> {
        crate::publish::add_pack_impl(self, pack, idx, tier, history_of).await
    }

    /// Whether the last known manifest wants a checkpoint, and why.
    pub fn checkpoint_due(&self) -> Option<crate::checkpoint::CheckpointTrigger> {
        crate::checkpoint::checkpoint_due(&self.manifest(), &self.cfg.wal)
    }

    /// Download `wal/<checksum>.pack` + `.idx` (+ advertised side-files) from the
    /// store into `dir` as `pack-<checksum>.*` (striped), for tooling that
    /// rebuilds a historical copy elsewhere (`walgit wal materialize`). The
    /// live local copy is never touched.
    pub async fn fetch_pack_into(
        &self,
        pack: &walgit_proto::v1::PackRef,
        dir: &std::path::Path,
    ) -> Result<(), WalError> {
        tokio::fs::create_dir_all(dir).await?;
        let c = &pack.checksum;
        let pack_path = dir.join(format!("pack-{c}.pack"));
        let idx_path = dir.join(format!("pack-{c}.idx"));
        let pack_key = walgit_proto::keys::pack_key(c);
        let idx_key = walgit_proto::keys::idx_key(c);
        let pack_size = pack.pack_size;
        let idx_size = pack.idx_size;
        let pack_fut = async {
            crate::sync::download_object(
                &self.store,
                &pack_key,
                &pack_path,
                (pack_size > 0).then_some(pack_size),
                None,
            )
            .await
        };
        let idx_fut = async {
            crate::sync::download_object(
                &self.store,
                &idx_key,
                &idx_path,
                (idx_size > 0).then_some(idx_size),
                None,
            )
            .await
        };
        let mut side_futs = Vec::new();
        for (flag, ext, key) in [
            (pack.has_rev, "rev", walgit_proto::keys::rev_key(c)),
            (pack.has_bitmap, "bitmap", walgit_proto::keys::bitmap_key(c)),
            (
                pack.has_commit_graph,
                "commit-graph",
                walgit_proto::keys::commit_graph_key(c),
            ),
        ] {
            if flag {
                let store = self.store.clone();
                let path = dir.join(format!("pack-{c}.{ext}"));
                side_futs.push(async move {
                    crate::sync::download_object(&store, &key, &path, None, None).await
                });
            }
        }
        let (pack_r, idx_r, sides) =
            tokio::join!(pack_fut, idx_fut, futures::future::join_all(side_futs));
        pack_r?;
        idx_r?;
        let _ = sides;
        Ok(())
    }

    /// When this repository's WAL history starts (oldest replayable state):
    /// the first live log segment's entry time, else the checkpoint's. Slots
    /// before it are "unavailable" for bundles. None when unknown.
    pub fn first_state_time(&self) -> Option<std::time::SystemTime> {
        // Three witnesses that state existed at a time; the earliest wins:
        // the checkpoint's `first_state_at` (D22: the earliest entry ever, carried
        // forward), the checkpoint's own write time — a checkpoint *is* state as
        // of when it was written, whatever its entries say — and the oldest live
        // log entry this process replayed. Prod 2026-08-21: a large repository's import
        // checkpoint (seq 1, 08-19 21:33Z) was ignored because its manifest ref
        // carries no times; the bundler then cut "08-19 23:00" slots from today's
        // main. The times now come from the checkpoint object when the ref has none.
        let cp = self.checkpoint_times();
        [
            cp.as_ref().and_then(|c| c.first_state_at),
            cp.as_ref().and_then(|c| c.created_at),
        ]
        .into_iter()
        .flatten()
        .chain(*self.first_entry_time.lock())
        .min()
    }

    /// Times of the current checkpoint: from the manifest's `CheckpointRef`, else
    /// from the checkpoint object read during sync (`checkpoint_times` cache).
    /// None = no checkpoint, or its times are not known to this process yet.
    pub fn checkpoint_times(&self) -> Option<CheckpointTimes> {
        let m = self.manifest();
        let cp = m.checkpoint.as_ref()?;
        let from_ref = CheckpointTimes {
            created_at: cp.created_at.as_ref().map(walgit_proto::time::to_system),
            first_state_at: cp
                .first_state_at
                .as_ref()
                .map(walgit_proto::time::to_system),
            as_of: cp.as_of.as_ref().map(walgit_proto::time::to_system),
        };
        if from_ref.created_at.is_some() {
            return Some(from_ref);
        }
        match self.checkpoint_times.lock().as_ref() {
            Some((seq, t)) if *seq == cp.seq => Some(*t),
            _ => Some(from_ref),
        }
    }

    /// Read the checkpoint object's times when the manifest ref has none
    /// (one 240-byte GET per checkpoint per process; no-op otherwise).
    pub(crate) async fn learn_checkpoint_times(&self) -> Result<(), WalError> {
        use walgit_store::ObjectStoreExt;

        use prost::Message;

        let m = self.manifest();
        let Some(cp) = m.checkpoint.as_ref() else {
            return Ok(());
        };
        if cp.created_at.is_some()
            || matches!(self.checkpoint_times.lock().as_ref(), Some((seq, _)) if *seq == cp.seq)
        {
            return Ok(());
        }

        if let Some((_, bytes)) = self.store.get_bytes(&cp.key).await? {
            let cpo = walgit_proto::v1::Checkpoint::decode(bytes.as_ref())
                .map_err(|e| WalError::Corrupt(format!("checkpoint decode: {e}")))?;
            let t = CheckpointTimes {
                created_at: cpo.created_at.as_ref().map(walgit_proto::time::to_system),
                first_state_at: None,
                as_of: None,
            };
            *self.checkpoint_times.lock() = Some((cp.seq, t));
        }
        Ok(())
    }

    /// Ref state as of a point in time (checkpoint + log replay in memory,
    /// refs-level, pure). See `log_reader::refs_as_of`.
    pub async fn refs_as_of(
        &self,
        at: std::time::SystemTime,
    ) -> Result<(walgit_proto::v1::RefSnapshot, u64), WalError> {
        crate::log_reader::refs_as_of(self, at).await
    }

    /// Ref state at WAL `seq` exactly (refs-level, pure). See `log_reader::refs_at_seq`.
    pub async fn refs_at_seq(&self, seq: u64) -> Result<walgit_proto::v1::RefSnapshot, WalError> {
        crate::log_reader::refs_at_seq(self, seq).await
    }

    /// Read log entries [`from_seq`, `to_seq`].
    pub async fn read_log(
        &self,
        from_seq: u64,
        to_seq: Option<u64>,
    ) -> Result<Vec<walgit_proto::v1::LogEntry>, WalError> {
        crate::log_reader::read_log_impl(self, from_seq, to_seq).await
    }

    // ---- internal helpers ----

    fn freshness_ttl_active(&self) -> bool {
        let ttl = self.cfg.wal.freshness_ttl;
        if ttl == std::time::Duration::ZERO {
            return false;
        }
        let last = self.last_freshness.lock();
        match *last {
            Some(t) => t.elapsed() < ttl,
            None => false,
        }
    }

    fn update_freshness(&self) {
        *self.last_freshness.lock() = Some(Instant::now());
    }

    fn get_or_init_publisher(&self) -> Result<mpsc::UnboundedSender<PublishRequest>, WalError> {
        let mut guard = self.publish_tx.lock();
        if let Some(tx) = &*guard {
            // A publisher task that died (panic mid-batch) leaves a sender to
            // a dropped receiver; respawn instead of failing every push on
            // this instance forever.
            if !tx.is_closed() {
                return Ok(tx.clone());
            }
            tracing::warn!(repo = %self.id, "publisher task is gone; respawning");
        }
        let (tx, rx) = mpsc::unbounded_channel();
        let arc = self
            .self_arc
            .get()
            .ok_or_else(|| {
                WalError::Corrupt("publisher repository reference not initialized".into())
            })?
            .clone();
        tokio::spawn(crate::publish::publisher_task(arc, rx));
        *guard = Some(tx.clone());
        Ok(tx)
    }
}
