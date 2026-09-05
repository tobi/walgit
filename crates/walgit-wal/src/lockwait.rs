//! Lock-wait observability (D19). Every per-repository lock on a request path — `RepoHandle::rw`
//! (readers), `sync_mutex`, `pack_mutex` (also what a second caller of a running `materialize`
//! waits on) — is acquired through [`timed`]: `try_*` first (the fast path costs nothing), and
//! only a wait that actually queued is timed, recorded in `walgit_lock_wait_seconds{lock}`, kept
//! as a per-lock maximum ([`snapshot`], printed by the runtime watchdog) and, past
//! `telemetry.lock_wait_warn`, logged as a WARN `lock wait` line carrying `lock`, `repo`,
//! `wait_ms` and — through the span — the request id. The 2026-08-20 incident (a queued writer
//! on `rw` starving every reader 60–680 s) would have been one grep away.

use std::future::Future;
use std::time::{Duration, Instant};

use parking_lot::Mutex;

/// Per-lock maximum wait since process start (ms) and count of timed waits.
static STATS: Mutex<Vec<(&'static str, u64, u64)>> = Mutex::new(Vec::new());
/// Per-(lock, repo) maximum — bounded by the repositories this instance touched; for tests and
/// the `…/api/ops` page, never a metric label.
static PER_REPO: Mutex<Vec<(&'static str, String, u64)>> = Mutex::new(Vec::new());

/// Record a wait that was not satisfied immediately.
pub fn record(
    lock: &'static str,
    repo: &walgit_git::RepoId,
    waited: Duration,
    warn_after: Duration,
) {
    metrics::histogram!("walgit_lock_wait_seconds", "lock" => lock).record(waited.as_secs_f64());
    let ms = u64::try_from(waited.as_millis()).unwrap_or(u64::MAX);
    {
        let mut s = STATS.lock();
        match s.iter_mut().find(|(l, _, _)| *l == lock) {
            Some(e) => {
                e.1 = e.1.max(ms);
                e.2 += 1;
            }
            None => s.push((lock, ms, 1)),
        }
    }
    {
        let key = repo.to_string();
        let mut s = PER_REPO.lock();
        match s.iter_mut().find(|(l, r, _)| *l == lock && *r == key) {
            Some(e) => e.2 = e.2.max(ms),
            None => s.push((lock, key, ms)),
        }
    }
    if waited >= warn_after {
        tracing::warn!(lock, repo = %repo, wait_ms = ms, "lock wait");
    }
}

/// `(lock, max_wait_ms)` for one repository since process start.
pub fn snapshot_for(repo: &walgit_git::RepoId) -> Vec<(&'static str, u64)> {
    let key = repo.to_string();
    PER_REPO
        .lock()
        .iter()
        .filter(|(_, r, _)| *r == key)
        .map(|(l, _, m)| (*l, *m))
        .collect()
}

/// `(lock, max_wait_ms, waits)` per lock since process start.
pub fn snapshot() -> Vec<(&'static str, u64, u64)> {
    STATS.lock().clone()
}

/// Worst wait on any lock so far (ms) — one number for the watchdog line.
pub fn max_wait_ms() -> u64 {
    STATS.lock().iter().map(|e| e.1).max().unwrap_or(0)
}

/// Acquire through `try_now` when free (no timing, no allocation); otherwise await `slow` and
/// record how long the queue took.
pub async fn timed<T, F>(
    lock: &'static str,
    repo: &walgit_git::RepoId,
    warn_after: Duration,
    try_now: impl FnOnce() -> Option<T>,
    slow: F,
) -> T
where
    F: Future<Output = T>,
{
    if let Some(t) = try_now() {
        return t;
    }
    let t0 = Instant::now();
    let out = slow.await;
    record(lock, repo, t0.elapsed(), warn_after);
    out
}
