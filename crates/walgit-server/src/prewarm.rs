//! Startup warm-up: for every repo in `cache.prewarm`, bring refs and objects
//! to this instance (packs when they fit, the remote pack indexes otherwise)
//! and touch the default branch's root tree, so the first user request on a
//! fresh instance finds everything in place. Each repo is a `prewarm` task
//! (discoverable at `…/tasks`); `/readyz` is ready only after every required
//! prewarm succeeds.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Instant;
use tracing::Instrument;

use crate::AppState;

pub struct Readiness {
    /// All configured prewarms finished successfully.
    pub done: AtomicBool,
    pub pending: AtomicUsize,
    /// Number of prewarm tasks that finished with an error in the current run.
    pub failed: AtomicUsize,
    pub started_at: Instant,
}

impl Readiness {
    pub fn new() -> Arc<Self> {
        Arc::new(Readiness {
            done: AtomicBool::new(true),
            pending: AtomicUsize::new(0),
            failed: AtomicUsize::new(0),
            started_at: Instant::now(),
        })
    }
    /// True only when every configured prewarm completed successfully.
    ///
    pub fn ready(&self) -> bool {
        self.done.load(Ordering::Acquire) && self.failed.load(Ordering::Acquire) == 0
    }
}

impl Default for Readiness {
    fn default() -> Self {
        Readiness {
            done: AtomicBool::new(true),
            pending: AtomicUsize::new(0),
            failed: AtomicUsize::new(0),
            started_at: Instant::now(),
        }
    }
}

/// Kick off the prewarm of `cfg.cache.prewarm` (no-op when empty).
pub fn spawn(state: Arc<AppState>) {
    let repos: Vec<String> = state.cfg.cache.prewarm.clone();
    if repos.is_empty() {
        return;
    }
    state.readiness.done.store(false, Ordering::Release);
    state.readiness.failed.store(0, Ordering::Release);
    state
        .readiness
        .pending
        .store(repos.len(), Ordering::Release);
    let par = state.cfg.cache.prewarm_parallelism.max(1);
    tokio::spawn(async move {
        let sem = Arc::new(tokio::sync::Semaphore::new(par));
        let mut handles = Vec::new();
        for r in repos {
            let st = state.clone();
            let sem = sem.clone();
            handles.push(tokio::spawn(async move {
                let _p = sem.acquire().await;
                let t = Instant::now();
                match warm(&st, &r).await {
                    Ok(summary) => tracing::info!(repo = %r, elapsed_ms = t.elapsed().as_millis() as u64, "prewarm: {summary}"),
                    Err(e) => {
                        st.readiness.failed.fetch_add(1, Ordering::AcqRel);
                        tracing::warn!(repo = %r, elapsed_ms = t.elapsed().as_millis() as u64, "prewarm failed: {e}");
                    }
                }
                st.readiness.pending.fetch_sub(1, Ordering::AcqRel);
            }));
        }
        for h in handles {
            let _ = h.await;
        }
        state.readiness.done.store(true, Ordering::Release);
        let failures = state.readiness.failed.load(Ordering::Acquire);
        if failures == 0 {
            tracing::info!("prewarm complete; instance ready");
        } else {
            tracing::error!(
                failures,
                "prewarm complete with failures; instance remains unready"
            );
        }
    });
}

async fn warm(st: &Arc<AppState>, repo: &str) -> Result<String, String> {
    let id: walgit_git::RepoId = repo
        .parse()
        .map_err(|e: walgit_git::GitError| e.to_string())?;
    let handle = st.registry.open(&id).await.map_err(|e| e.to_string())?;
    let task = match handle.begin_task("prewarm", Default::default()) {
        walgit_wal::Begin::Started(t) => t,
        walgit_wal::Begin::AlreadyRunning(_) => return Ok("already warming".into()),
    };
    let reporter = task.reporter();
    reporter.notice("Refs from the WAL");
    let span = task.span();
    let res: Result<String, String> = async {
        let guard = handle.sync_refs().await.map_err(|e| e.to_string())?;
        let head = handle.local().refs().map_err(|e| e.to_string())?;
        drop(guard);
        // Placement: a repository this host does not serve is warm at refs
        // level only (what the read-only fallback needs); no packs, no indexes.
        if !handle
            .config()
            .placement
            .serves(handle.id().owner(), handle.id().name())
        {
            return Ok(format!(
                "warm: refs only (not served here), {} refs",
                head.refs.len()
            ));
        }
        // The serving copy for git (Serve level: packs that fit, base side-
        // files + mount link / remote-serve otherwise) — so the first fetch or
        // push on this instance does not pay for a large repository's ~3 GB of side-files.
        if handle.serve_fits() {
            reporter.notice("Serving copy (packs / base side-files)");
            match handle.sync().await {
                Ok(g) => drop(g),
                Err(e) => reporter.notice(format!("serving copy not ready: {e}")),
            }
            // D18/D19: the history pack + midx install is deferred to the bulk
            // runtime; an instance is not "ready" until it landed (prod
            // 2026-08-21: ready at 21:34, history pack installing until 21:56).
            if handle.history_pack_install_inflight() {
                reporter.notice(
                    "Waiting for the history pack (commits + trees) and the midx to install",
                );
                handle.wait_history_pack_installed().await;
            }
        }
        reporter.notice("Objects (packs when they fit, remote pack indexes otherwise)");
        let (guard, access) = handle.sync_objects().await.map_err(|e| e.to_string())?;
        drop(guard);
        let mode = match &access {
            walgit_wal::ObjectAccess::Local => "local packs",
            walgit_wal::ObjectAccess::Remote(_) => "remote pack indexes",
        };
        // Touch HEAD's root tree so the first page render is warm too.
        let head_sha = head
            .refs
            .iter()
            .find(|r| r.name == head.head_target)
            .map(|r| r.oid.clone());
        if let (Some(sha), walgit_wal::ObjectAccess::Remote(packs)) = (head_sha.as_deref(), &access)
        {
            if let Ok(oid) = gix_hash::ObjectId::from_hex(sha.as_bytes()) {
                reporter.notice(format!(
                    "Reading the root tree of {} from the pack set",
                    &sha[..12]
                ));
                let remote = crate::web::objects::Remote::new(
                    packs.clone(),
                    handle.local().clone(),
                    reporter.clone(),
                );
                let (_c, tree, _m) = remote.fault_path(&oid, "").await.map_err(|e| e.message())?;
                let _ = remote.tree_entries(&tree).await;
            }
        }
        Ok(format!("warm: {mode}"))
    }
    .instrument(span)
    .await;
    match res {
        Ok(s) => {
            task.finish_ok(s.clone(), None);
            Ok(s)
        }
        Err(e) => {
            task.finish_err(500, e.clone());
            Err(e)
        }
    }
}
