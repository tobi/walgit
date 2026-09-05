//! Progress packets for long-running repository work (pack downloads, remote
//! index fetches, object faults, compaction, ...). This is the wire standard of
//! the SSE envelope (web/API.md §2b): the SSE `event:` name is
//! [`Progress::event_name`], the `data:` is the JSON of the variant.
//!
//! Every `RepoHandle` owns a broadcast channel; tasks publish into it
//! regardless of who is listening and request handlers that stream subscribe
//! before they start waiting. Because syncs are single-flight per repo,
//! *every* request blocked behind one sees the same progress.

use std::sync::Arc;

use serde::Serialize;

use crate::tasks::{TaskRecord, TaskState};

#[derive(Clone, Debug, Serialize)]
#[serde(untagged)]
pub enum Progress {
    /// A human-readable line: what the server is doing right now.
    Notice { text: String },
    /// A progress bar. `total` unknown => indeterminate. `unit` is `bytes`,
    /// `objects`, `commits`, ... purely for display.
    Progress {
        label: String,
        done: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        total: Option<u64>,
        unit: &'static str,
        #[serde(skip_serializing_if = "Option::is_none")]
        percent: Option<f64>,
    },
    /// A background task started, changed or finished (its full record).
    Task { task: Box<TaskRecord> },
}

impl Progress {
    pub fn notice(text: impl Into<String>) -> Self {
        Progress::Notice { text: text.into() }
    }
    pub fn bar(
        label: impl Into<String>,
        done: u64,
        total: Option<u64>,
        unit: &'static str,
    ) -> Self {
        let percent = total.filter(|t| *t > 0).map(|t| {
            let tenths = done.min(t).saturating_mul(1_000) / t;
            f64::from(u32::try_from(tenths).unwrap_or(1_000)) / 10.0
        });
        Progress::Progress {
            label: label.into(),
            done,
            total,
            unit,
            percent,
        }
    }
    pub fn event_name(&self) -> &'static str {
        match self {
            Progress::Notice { .. } => "notice",
            Progress::Progress { .. } => "progress",
            Progress::Task { .. } => "task",
        }
    }
}

pub type ProgressTx = tokio::sync::broadcast::Sender<Progress>;
pub type ProgressRx = tokio::sync::broadcast::Receiver<Progress>;

/// Sink handed to work loops. Writes into a task (record + replay + live
/// subscribers) and/or the repo's channel. Cloning is cheap; a reporter with
/// no sinks is a valid no-op.
#[derive(Clone, Default)]
pub struct Reporter {
    task: Option<Arc<TaskState>>,
    repo: Option<ProgressTx>,
}

impl Reporter {
    pub fn none() -> Self {
        Reporter::default()
    }
    pub fn for_repo(tx: ProgressTx) -> Self {
        Reporter {
            task: None,
            repo: Some(tx),
        }
    }
    pub fn for_task(task: Arc<TaskState>, repo: Option<ProgressTx>) -> Self {
        Reporter {
            task: Some(task),
            repo,
        }
    }
    /// Narration ⇄ telemetry: every notice the user sees is also a DEBUG
    /// event in the current span (the task.run span on task futures), so a
    /// trace tells the same story the client was told.
    pub fn notice(&self, text: impl Into<String>) {
        let text = text.into();
        tracing::debug!(narration = "notice", "{text}");
        self.send(Progress::notice(text));
    }
    pub fn bar(&self, label: impl Into<String>, done: u64, total: Option<u64>, unit: &'static str) {
        let label = label.into();
        if tracing::enabled!(tracing::Level::DEBUG) {
            tracing::debug!(
                narration = "progress",
                done,
                total = total.unwrap_or(0),
                unit,
                "{label}"
            );
        }
        self.send(Progress::bar(label, done, total, unit));
    }
    pub fn send(&self, p: Progress) {
        if let Some(t) = &self.task {
            t.publish(p.clone());
        }
        if let Some(tx) = &self.repo {
            let _ = tx.send(p);
        }
    }
}

/// Rate-limits progress-bar updates (at most one per `min_interval`, plus
/// always the forced final one) so a 32 GB download does not emit a packet per
/// chunk.
pub struct Throttle {
    last: std::sync::Mutex<Option<std::time::Instant>>,
    min_interval: std::time::Duration,
}

impl Throttle {
    pub fn new(min_interval: std::time::Duration) -> Self {
        Throttle {
            last: std::sync::Mutex::new(None),
            min_interval,
        }
    }
    /// True when an update should be emitted now.
    pub fn tick(&self, force: bool) -> bool {
        let mut last = self
            .last
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let now = std::time::Instant::now();
        if force
            || last
                .map(|t| now.duration_since(t) >= self.min_interval)
                .unwrap_or(true)
        {
            *last = Some(now);
            true
        } else {
            false
        }
    }
}
