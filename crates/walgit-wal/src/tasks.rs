//! Background tasks: anything that makes a request wait or runs detached
//! (pack materialization, remote index download, compaction, bundle builds,
//! fsck, ...). Every task
//!
//! * gets a unique id and a record in a per-instance log (bounded per repo),
//! * holds a lock on `(repo, kind)` — a second start of the same kind on the
//!   same repo joins the running task instead of duplicating the work,
//! * publishes [`Progress`] packets (notices, progress bars, final
//!   result/error) that can be attached to live at any time
//!   (`GET /{o}/{r}/api/tasks/{id}` streams them as SSE; the replay
//!   buffer gives late joiners the story so far),
//! * mirrors its packets into the repo's progress channel so requests blocked
//!   behind it (single-flight sync) stream the same thing.
//!
//! Records are per instance (serverless instances share nothing but the
//! bucket); `hostname` says where a task ran. Cross-instance exclusivity for
//! mutating tasks is still the GCS lease (compaction, bundles); this registry
//! is the local, discoverable half.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Instant;

use parking_lot::Mutex;
use serde::Serialize;

use crate::progress::{Progress, ProgressTx};

const KEEP_RECORDS: usize = 30;
const KEEP_LOG: usize = 60;
const REPLAY: usize = 200;

/// Replayed packets, future packets, and the terminal outcome if already finished.
pub type TaskAttachment = (
    Vec<Progress>,
    tokio::sync::broadcast::Receiver<Progress>,
    Option<Result<TaskOutcome, (u16, String)>>,
);

#[derive(Serialize, Clone, Debug)]
pub struct TaskRecord {
    pub id: String,
    pub kind: String,
    pub repo: String,
    pub hostname: String,
    pub started: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finished: Option<String>,
    pub elapsed_ms: u64,
    /// `None` while running.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ok: Option<bool>,
    pub summary: String,
    /// Latest progress bar, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub progress: Option<Progress>,
    /// Last notice lines (bounded).
    pub log_tail: Vec<String>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub params: HashMap<String, String>,
}

impl TaskRecord {
    pub fn running(&self) -> bool {
        self.ok.is_none()
    }
}

fn now_rfc3339() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

/// Terminal packet of a task stream.
#[derive(Serialize, Clone, Debug)]
pub struct TaskOutcome {
    pub task: TaskRecord,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<serde_json::Value>,
}

pub struct TaskState {
    pub record: Mutex<TaskRecord>,
    started_at: Instant,
    tx: ProgressTx,
    replay: Mutex<VecDeque<Progress>>,
    outcome: Mutex<Option<Result<TaskOutcome, (u16, String)>>>,
    done: tokio::sync::watch::Sender<bool>,
    /// The tokio task running this job, when it runs as one (`set_abort_handle`):
    /// the D31 drain interrupts it at once.
    abort: Mutex<Option<tokio::task::AbortHandle>>,
}

impl TaskState {
    /// Register the tokio task that runs this job so a drain can interrupt it.
    pub fn set_abort_handle(&self, h: tokio::task::AbortHandle) {
        *self.abort.lock() = Some(h);
    }
    /// Interrupt the job (D31): the future is dropped, the handle's Drop records
    /// `interrupted` (D22: the next pass redoes it). Returns whether it could.
    pub fn interrupt(&self) -> bool {
        match self.abort.lock().as_ref() {
            Some(h) => {
                h.abort();
                true
            }
            None => false,
        }
    }
    pub fn id(&self) -> String {
        self.record.lock().id.clone()
    }
    pub fn record(&self) -> TaskRecord {
        self.record.lock().clone()
    }
    /// Subscribe + snapshot of everything so far (no gap, no duplicates).
    pub fn attach(&self) -> TaskAttachment {
        let replay = self.replay.lock();
        let rx = self.tx.subscribe();
        let outcome = self.outcome.lock().clone();
        (replay.iter().cloned().collect(), rx, outcome)
    }
    pub fn outcome(&self) -> Option<Result<TaskOutcome, (u16, String)>> {
        self.outcome.lock().clone()
    }
    /// Completion signal. Late subscribers see `true` immediately (the value
    /// is stored, not just broadcast) — check `*rx.borrow()` before waiting.
    pub fn done_rx(&self) -> tokio::sync::watch::Receiver<bool> {
        self.done.subscribe()
    }

    /// Wait until the task finished (bounded by `timeout`); `Ok(false)` on timeout.
    pub async fn wait_done(&self, timeout: std::time::Duration) -> bool {
        let mut rx = self.done.subscribe();
        if *rx.borrow() {
            return true;
        }
        tokio::time::timeout(timeout, async {
            while !*rx.borrow() {
                if rx.changed().await.is_err() {
                    break;
                }
            }
        })
        .await
        .is_ok()
            && *self.done.borrow()
    }
    pub(crate) fn publish(&self, p: Progress) {
        {
            let mut rec = self.record.lock();
            match &p {
                Progress::Notice { text } => {
                    rec.log_tail.push(text.clone());
                    if rec.log_tail.len() > KEEP_LOG {
                        rec.log_tail.remove(0);
                    }
                }
                Progress::Progress { .. } => rec.progress = Some(p.clone()),
                Progress::Task { .. } => {}
            }
            rec.elapsed_ms =
                u64::try_from(self.started_at.elapsed().as_millis()).unwrap_or(u64::MAX);
        }
        {
            let mut replay = self.replay.lock();
            // Keep only the latest bar with a given label in the replay buffer.
            if let Progress::Progress { label, .. } = &p {
                replay.retain(|q| !matches!(q, Progress::Progress { label: l, .. } if l == label));
            }
            replay.push_back(p.clone());
            while replay.len() > REPLAY {
                replay.pop_front();
            }
        }
        let _ = self.tx.send(p);
    }
}

/// RAII handle of a running task. Dropping it without `finish_*` records a
/// failure ("dropped").
pub struct TaskHandle {
    pub state: Arc<TaskState>,
    tasks: Arc<Tasks>,
    repo_tx: Option<ProgressTx>,
    finished: bool,
    /// `task.run` span: `task_id`, `task_kind`, `repo`. Child of the span
    /// that began the task (a request) or a root with its own trace
    /// (background jobs). Run the task's work `.instrument(task.span())`
    /// so every log line of the job carries these fields and groups by trace.
    span: tracing::Span,
}

impl TaskHandle {
    pub fn id(&self) -> String {
        self.state.id()
    }
    pub fn span(&self) -> tracing::Span {
        self.span.clone()
    }
    pub fn record(&self) -> TaskRecord {
        self.state.record()
    }
    /// A [`crate::progress::Reporter`] that writes into this task (and the
    /// repo channel).
    pub fn reporter(&self) -> crate::progress::Reporter {
        crate::progress::Reporter::for_task(self.state.clone(), self.repo_tx.clone())
    }
    pub fn notice(&self, text: impl Into<String>) {
        self.reporter().notice(text);
    }
    pub fn finish_ok(
        mut self,
        summary: impl Into<String>,
        value: Option<serde_json::Value>,
    ) -> TaskRecord {
        self.finished = true;
        self.span.record("task_ok", true);
        let _g = self.span.enter();
        self.tasks.finish(
            &self.state,
            Ok((summary.into(), value)),
            self.repo_tx.as_ref(),
        )
    }
    pub fn finish_err(mut self, status: u16, message: impl Into<String>) -> TaskRecord {
        self.finished = true;
        self.span.record("task_ok", false);
        let _g = self.span.enter();
        self.tasks.finish(
            &self.state,
            Err((status, message.into())),
            self.repo_tx.as_ref(),
        )
    }
}

/// D31 — shutdown in two phases. **Phase 1** (`begin_drain`, on SIGTERM): the
/// maintenance loop starts no new unit and the running one is **interrupted at
/// once** (D22 redoes it; a unit too expensive to redo is made resumable, not
/// awaited) — while the instance serves everything normally and `/readyz`
/// stays 200 (draining must not stop serving: a 9-min wait behind a rev-index
/// unit took the only server of acme/monorepo offline for 9 min, 2026-08-21).
/// **Phase 2** (`begin_shutdown`, once the unit is gone, seconds): `/readyz`
/// 503, new fetch/push/LFS refused with 503 + Retry-After, in-flight requests
/// get `server.drain_timeout`, exit. Tasks dropped while draining end as
/// **interrupted**, not "dropped".
static DRAINING: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
static SHUTTING_DOWN: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Phase 1: SIGTERM arrived; no new maintenance unit starts. Serving goes on.
pub fn begin_drain() {
    DRAINING.store(true, std::sync::atomic::Ordering::Release);
}

/// Whether `begin_drain` (or `begin_shutdown`) was called.
pub fn draining() -> bool {
    DRAINING.load(std::sync::atomic::Ordering::Acquire) || shutting_down()
}

/// Phase 2: stop serving (readyz 503, object work refused), exit soon.
pub fn begin_shutdown() {
    DRAINING.store(true, std::sync::atomic::Ordering::Release);
    SHUTTING_DOWN.store(true, std::sync::atomic::Ordering::Release);
}

/// Whether `begin_shutdown` was called.
pub fn shutting_down() -> bool {
    SHUTTING_DOWN.load(std::sync::atomic::Ordering::Acquire)
}

/// Summary prefix of a task that was interrupted by an instance shutdown.
pub const INTERRUPTED: &str = "interrupted: instance shut down; will be retried by the next pass";

impl Drop for TaskHandle {
    fn drop(&mut self) {
        if !self.finished {
            self.span.record("task_ok", false);
            let _g = self.span.enter();
            let (status, msg) = if draining() {
                (503u16, INTERRUPTED.to_string())
            } else {
                (500u16, "task dropped before completion".to_string())
            };
            let _ = self
                .tasks
                .finish(&self.state, Err((status, msg)), self.repo_tx.as_ref());
        }
    }
}

/// Result of [`Tasks::begin`]: a fresh task, or the one already running for
/// the same `(repo, kind)`.
pub enum Begin {
    Started(TaskHandle),
    AlreadyRunning(Arc<TaskState>),
}

#[derive(Default)]
pub struct Tasks {
    recent: Mutex<HashMap<String, VecDeque<Arc<TaskState>>>>,
    by_id: Mutex<HashMap<String, Arc<TaskState>>>,
    running: Mutex<HashMap<(String, String), Arc<TaskState>>>,
}

impl Tasks {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Start a task or join the running one. `repo_tx` is the repo's progress
    /// channel (packets are mirrored into it).
    pub fn begin(
        self: &Arc<Self>,
        repo: &str,
        kind: &str,
        params: HashMap<String, String>,
        repo_tx: Option<ProgressTx>,
    ) -> Begin {
        let key = (repo.to_string(), kind.to_string());
        let mut running = self.running.lock();
        if let Some(existing) = running.get(&key) {
            return Begin::AlreadyRunning(existing.clone());
        }
        let record = TaskRecord {
            id: uuid::Uuid::new_v4().to_string(),
            kind: kind.to_string(),
            repo: repo.to_string(),
            hostname: walgit_store::coord::instance_id().to_string(),
            started: now_rfc3339(),
            finished: None,
            elapsed_ms: 0,
            ok: None,
            summary: "running".into(),
            progress: None,
            log_tail: Vec::new(),
            params,
        };
        let (tx, _) = tokio::sync::broadcast::channel(1024);
        let (done, _) = tokio::sync::watch::channel(false);
        let state = Arc::new(TaskState {
            record: Mutex::new(record.clone()),
            started_at: Instant::now(),
            tx,
            replay: Mutex::new(VecDeque::new()),
            outcome: Mutex::new(None),
            done,
            abort: Mutex::new(None),
        });
        running.insert(key, state.clone());
        drop(running);
        self.by_id.lock().insert(record.id.clone(), state.clone());
        {
            let mut recent = self.recent.lock();
            let q = recent.entry(repo.to_string()).or_default();
            q.push_back(state.clone());
            while q.len() > KEEP_RECORDS {
                if let Some(old) = q.pop_front() {
                    self.by_id.lock().remove(&old.id());
                }
            }
        }
        if let Some(tx) = &repo_tx {
            let _ = tx.send(Progress::Task {
                task: Box::new(record),
            });
        }
        let span = tracing::info_span!(
            "task.run",
            repo,
            task_kind = kind,
            task_id = %state.id(),
            task_ok = tracing::field::Empty,
        );
        span.in_scope(|| tracing::info!("task started"));
        metrics::counter!("walgit_tasks_started_total", "kind" => kind.to_string()).increment(1);
        Begin::Started(TaskHandle {
            state,
            tasks: self.clone(),
            repo_tx,
            finished: false,
            span,
        })
    }

    fn finish(
        &self,
        state: &Arc<TaskState>,
        outcome: Result<(String, Option<serde_json::Value>), (u16, String)>,
        repo_tx: Option<&ProgressTx>,
    ) -> TaskRecord {
        let record = {
            let mut rec = state.record.lock();
            rec.finished = Some(now_rfc3339());
            rec.elapsed_ms =
                u64::try_from(state.started_at.elapsed().as_millis()).unwrap_or(u64::MAX);
            match &outcome {
                Ok((summary, _)) => {
                    rec.ok = Some(true);
                    rec.summary.clone_from(summary);
                }
                Err((_, msg)) => {
                    rec.ok = Some(false);
                    rec.summary.clone_from(msg);
                }
            }
            rec.clone()
        };
        self.running
            .lock()
            .remove(&(record.repo.clone(), record.kind.clone()));
        let pick_from = outcome.as_ref().ok().and_then(|(_, v)| v.clone());
        let out = match outcome {
            Ok((_, value)) => Ok(TaskOutcome {
                task: record.clone(),
                value,
            }),
            Err(e) => Err(e),
        };
        *state.outcome.lock() = Some(out);
        // `send` is a no-op when no receiver exists yet (a waiter that
        // subscribes a moment later would then wait forever); `send_replace`
        // always stores the value.
        state.done.send_replace(true);
        let p = Progress::Task {
            task: Box::new(record.clone()),
        };
        state.publish(p.clone());
        if let Some(tx) = repo_tx {
            let _ = tx.send(p);
        }
        // `bytes` / `objects` when the kind's result value carries them
        // (bundle builds, materialize, compaction) — dashboards key on them.
        let pick = |k: &str| {
            pick_from
                .as_ref()
                .and_then(|v| v.get(k))
                .and_then(serde_json::Value::as_u64)
        };
        let (bytes, objects) = (pick("bytes").or_else(|| pick("size")), pick("objects"));
        let ok = record.ok.unwrap_or(false);
        let outcome = if ok {
            "ok"
        } else if record.summary.starts_with("interrupted") {
            "interrupted"
        } else {
            "error"
        };
        tracing::info!(repo = %record.repo, kind = %record.kind, id = %record.id, ok, outcome, elapsed_ms = record.elapsed_ms, bytes, objects, "task finished: {}", record.summary);
        metrics::counter!("walgit_tasks_finished_total", "kind" => record.kind.clone(), "ok" => ok.to_string()).increment(1);
        metrics::histogram!("walgit_task_duration_seconds", "kind" => record.kind.clone(), "ok" => ok.to_string()).record(std::time::Duration::from_millis(record.elapsed_ms).as_secs_f64());
        record
    }

    pub fn get(&self, id: &str) -> Option<Arc<TaskState>> {
        self.by_id.lock().get(id).cloned()
    }

    /// Recent + running tasks of a repo, newest first.
    pub fn recent(&self, repo: &str) -> Vec<TaskRecord> {
        self.recent
            .lock()
            .get(repo)
            .map(|q| q.iter().rev().map(|s| s.record()).collect())
            .unwrap_or_default()
    }

    pub fn running(&self, repo: &str) -> Vec<TaskRecord> {
        self.running
            .lock()
            .iter()
            .filter(|((r, _), _)| r == repo)
            .map(|(_, s)| s.record())
            .collect()
    }

    /// How many tasks run on this instance right now (cheap; the watchdog reads it).
    pub fn running_count(&self) -> usize {
        self.running.lock().len()
    }

    /// Every running task on this instance (all repos).
    pub fn running_all(&self) -> Vec<TaskRecord> {
        self.running.lock().values().map(|s| s.record()).collect()
    }

    /// Interrupt every running task whose kind satisfies `pred` (D31 drain:
    /// the maintenance units). Returns the `(repo, kind)` of those interrupted.
    pub fn interrupt_where(&self, pred: impl Fn(&str) -> bool) -> Vec<(String, String)> {
        let running: Vec<Arc<TaskState>> = self.running.lock().values().cloned().collect();
        let mut out = Vec::new();
        for s in running {
            let r = s.record();
            if pred(&r.kind) && s.interrupt() {
                out.push((r.repo, r.kind));
            }
        }
        out
    }

    /// Latest finished record of `kind` for `repo`.
    pub fn last(&self, repo: &str, kind: &str) -> Option<TaskRecord> {
        self.recent(repo)
            .into_iter()
            .find(|r| r.kind == kind && r.ok.is_some())
    }
}
