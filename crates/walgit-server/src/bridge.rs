//! The events bridge (`Role::Events`, `docs/EVENTS.md`): the WAL → your webhook.
//! The only producer of events.
//!
//! `catch_up(repo)`: read the durable cursor `events/cursor.json` (last seq
//! published), the fresh manifest, and the log entries `(cursor, head_seq]`;
//! convert to `ref` events; deliver to every sink; CAS the cursor to
//! `head_seq`. A sink failure leaves the cursor where it was, so the next
//! wake-up retries the same range — at-least-once, never a gap, and
//! `head_seq - cursor` is the lag. An event is published iff its entry is
//! durable; nothing on the push path knows events exist.
//!
//! Wake-ups, both idempotent (they only ever call `catch_up`):
//! * `POST /_events/notify` with a bucket notification that names a finalized
//!   `…/manifest.pb` — the commit point itself as a notification. Accepted
//!   shapes: a GCS Pub/Sub push envelope (`message.attributes.objectId`), an S3
//!   event notification (`Records[].s3.object.key`), or a plain `{"key": "…"}`
//!   / `{"repo": "owner/name"}`. A non-2xx here is meant to be redelivered;
//! * the sweep (`events.sweep_interval`): every repo — the backstop. A sweep
//!   that finds unpublished entries means the notifications are not flowing
//!   and says so (`events_bridge_sweep_found_total`, warn).
//!
//! One instance of the service; catch-ups are serialized (volume is tiny).

use std::sync::Arc;

use anyhow::Context;
use chrono::Utc;
use walgit_git::RepoId;
use walgit_store::{ObjectStoreExt, PutMode, StoreError};

use crate::events::{self, RefEvent, Sink};

const CURSOR_KEY: &str = "events/cursor.json";

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct Cursor {
    published_seq: u64,
    updated_at: String,
}

/// What one `catch_up` did.
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct CatchUp {
    pub repo: String,
    /// Cursor before (the last seq already published).
    pub from_seq: u64,
    pub head_seq: u64,
    pub emitted: usize,
    /// `[first, last]` seqs folded into a checkpoint before the bridge read
    /// them: not emitted, counted — consumers backfill from the WAL.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gap: Option<[u64; 2]>,
}

pub struct Bridge {
    registry: Arc<walgit_wal::Registry>,
    /// Global key prefix of the bucket (`prefix/`): GCS object names
    /// carry it, the registry's store strips it.
    store_prefix: String,
    sinks: Vec<Box<dyn Sink>>,
    serial: tokio::sync::Mutex<()>,
}

impl Bridge {
    /// `None` unless this instance has the `events` role and a sink is
    /// configured (`events.webhook_url`).
    pub fn new(
        cfg: &walgit_config::Config,
        registry: Arc<walgit_wal::Registry>,
    ) -> Option<Arc<Bridge>> {
        if !cfg.has_role(walgit_config::Role::Events) {
            return None;
        }
        let mut sinks: Vec<Box<dyn Sink>> = Vec::new();
        if let Some(url) = &cfg.events.webhook_url {
            sinks.push(Box::new(events::WebhookSink::new(
                url.clone(),
                cfg.events.webhook_secret.clone().filter(|s| !s.is_empty()),
            )));
        }
        if sinks.is_empty() {
            return None;
        }
        tracing::info!(
            sinks = ?sinks.iter().map(|s| s.name()).collect::<Vec<_>>(),
            "events bridge enabled"
        );
        Some(Arc::new(Bridge {
            registry,
            store_prefix: cfg.store_prefix(),
            sinks,
            serial: tokio::sync::Mutex::new(()),
        }))
    }

    /// Publish everything committed after the cursor, then advance it.
    pub async fn catch_up(&self, id: &RepoId) -> anyhow::Result<CatchUp> {
        let _serial = self.serial.lock().await;
        let handle = self.registry.open(id).await?;
        let manifest = {
            let guard = handle.sync_refs().await?;
            let m = handle.manifest();
            drop(guard);
            m
        };
        let head = manifest.head_seq;
        // Entries below `min_seq` are folded into a checkpoint and no longer
        // in the manifest's log window; `min_seq - 1` is the newest seq we
        // can treat as "already published" when starting cold.
        let readable_from = manifest.min_seq.saturating_sub(1);
        let store = handle.store();

        let (cursor, version) = match store.get_bytes(CURSOR_KEY).await? {
            Some((meta, bytes)) => {
                let c: Cursor = serde_json::from_slice(&bytes).context("events/cursor.json")?;
                (Some(c.published_seq), Some(meta.version))
            }
            None => (None, None),
        };
        let mut from = cursor.unwrap_or(readable_from);
        let mut gap = None;
        if from < readable_from {
            gap = Some([from + 1, readable_from]);
            metrics::counter!("events_bridge_gap_total", "repo" => id.to_string())
                .increment(readable_from - from);
            tracing::warn!(repo = %id, first = from + 1, last = readable_from,
                "events bridge: entries folded into a checkpoint before they were published; consumers backfill from the WAL");
            from = readable_from;
        }
        metrics::gauge!("events_bridge_lag_entries", "repo" => id.to_string())
            .set((head - from) as f64);
        let mut report = CatchUp {
            repo: id.to_string(),
            from_seq: from,
            head_seq: head,
            emitted: 0,
            gap,
        };

        if head > from {
            let entries = handle.read_log(from + 1, Some(head)).await?;
            let mut batch = Vec::new();
            events::refs_from_entries(id, &entries, &mut batch);
            // A failing sink returns here: cursor untouched, retried on the
            // next wake-up.
            self.publish(&batch).await?;
            report.emitted = batch.len();
        }

        if cursor != Some(head) {
            let body = serde_json::to_vec(&Cursor {
                published_seq: head,
                updated_at: Utc::now().to_rfc3339(),
            })?;
            let mode = match version {
                Some(v) => PutMode::Update(v),
                None => PutMode::Create,
            };
            match store.put_bytes(CURSOR_KEY, body, mode).await {
                Ok(_) => {}
                // Another bridge instance advanced it: our emission was a
                // duplicate (dedup key), theirs stands.
                Err(StoreError::PreconditionFailed { .. }) => {
                    tracing::warn!(repo = %id, "events bridge: cursor CAS lost (two bridges?)");
                }
                Err(e) => return Err(e.into()),
            }
        }
        metrics::gauge!("events_bridge_lag_entries", "repo" => id.to_string()).set(0.0);
        if report.emitted > 0 {
            tracing::info!(repo = %id, from = from, head = head, emitted = report.emitted,
                "events bridge: published");
        }
        Ok(report)
    }

    /// Every repo (a `list` + one conditional manifest GET each): the backstop
    /// behind the notifications, and the health check — finding work here
    /// means they are not flowing.
    pub async fn sweep(&self) {
        let repos = match self.registry.list().await {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(error = %e, "events bridge: sweep list failed");
                return;
            }
        };
        for id in repos {
            match self.catch_up(&id).await {
                Ok(c) if c.emitted > 0 => {
                    metrics::counter!("events_bridge_sweep_found_total")
                        .increment(c.emitted as u64);
                    tracing::warn!(repo = %id, emitted = c.emitted,
                        "events bridge: sweep found unpublished entries — are the GCS notifications flowing?");
                }
                Ok(_) => {}
                Err(e) => {
                    tracing::warn!(repo = %id, error = %e, "events bridge: sweep catch-up failed");
                }
            }
        }
    }

    /// A GCS notification: `…/repos/<o>/<r>/manifest.pb` finalized ⇒ that
    /// repo committed something. Everything else is ignored.
    pub async fn object_finalized(&self, object: &str) -> anyhow::Result<Option<CatchUp>> {
        let Some(id) = self.manifest_repo(object) else {
            return Ok(None);
        };
        match self.catch_up(&id).await {
            // A late notification for a repo deleted since: nothing to do
            // (a 503 here would have Pub/Sub retry it for days).
            Err(e)
                if matches!(
                    e.downcast_ref::<walgit_wal::WalError>(),
                    Some(walgit_wal::WalError::NotFound)
                ) =>
            {
                Ok(None)
            }
            r => r.map(Some),
        }
    }

    /// `prefix/repos/<o>/<r>/manifest.pb` → `o/r`.
    fn manifest_repo(&self, object: &str) -> Option<RepoId> {
        let rel = object
            .strip_prefix(self.store_prefix.as_str())?
            .strip_prefix("repos/")?
            .strip_suffix("/manifest.pb")?;
        let (owner, name) = rel.split_once('/')?;
        if name.contains('/') {
            return None;
        }
        RepoId::new(owner, name).ok()
    }

    async fn publish(&self, batch: &[RefEvent]) -> anyhow::Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        for sink in &self.sinks {
            sink.deliver(batch)
                .await
                .with_context(|| format!("{} sink", sink.name()))?;
            metrics::counter!("events_published_total", "sink" => sink.name())
                .increment(batch.len() as u64);
        }
        // One structured line per published event (Cloud Logging:
        // `jsonPayload.event_type="ref"`).
        for ev in batch {
            if let Ok(body) = serde_json::to_string(ev) {
                tracing::info!(target: "walgit::event",
                    event_type = "ref", repo = %ev.repo, event = %body, "event");
            }
        }
        Ok(())
    }
}

/// The object keys (or repository ids) a notification body names. Store-agnostic.
fn notified_keys(v: &serde_json::Value) -> Vec<String> {
    let mut keys = Vec::new();
    // GCS → Pub/Sub push envelope.
    let attrs = &v["message"]["attributes"];
    if attrs["eventType"] == "OBJECT_FINALIZE"
        && let Some(k) = attrs["objectId"].as_str()
    {
        keys.push(k.to_string());
    }
    // S3 event notification (also what MinIO/rustfs/Ceph emit).
    if let Some(records) = v["Records"].as_array() {
        for r in records {
            if r["eventName"]
                .as_str()
                .is_some_and(|e| e.starts_with("ObjectCreated"))
                && let Some(k) = r["s3"]["object"]["key"].as_str()
            {
                // S3 URL-encodes keys in notifications.
                keys.push(
                    k.replace('+', " ")
                        .split('%')
                        .enumerate()
                        .map(|(i, part)| {
                            if i == 0 {
                                return part.to_string();
                            }
                            match u8::from_str_radix(part.get(..2).unwrap_or(""), 16) {
                                Ok(b) => format!("{}{}", b as char, &part[2..]),
                                Err(_) => format!("%{part}"),
                            }
                        })
                        .collect(),
                );
            }
        }
    }
    // Plain shapes for your own glue.
    if let Some(k) = v["key"].as_str() {
        keys.push(k.to_string());
    }
    if let Some(r) = v["repo"].as_str() {
        keys.push(format!("repos/{r}/manifest.pb"));
    }
    keys
}

/// `POST /_events/notify`: a bucket notification naming a finalized `manifest.pb`.
/// `200` (ack) when handled or ignored, `503` (redeliver) when a sink failed.
pub async fn http_notify(
    st: &crate::AppState,
    headers: &axum::http::HeaderMap,
    body: axum::body::Body,
) -> Result<axum::response::Response, crate::error::ApiError> {
    use crate::error::ApiError;
    use axum::response::IntoResponse;
    let _ = st.auth.require_read(headers).await.map_err(auth_err)?;
    let Some(bridge) = &st.bridge else {
        return Err(ApiError::NotFound(
            "events bridge is not enabled here".into(),
        ));
    };
    let bytes = crate::collect_body(body).await?;
    let v: serde_json::Value = serde_json::from_slice(&bytes)
        .map_err(|e| ApiError::BadRequest(format!("notify body: {e}")))?;
    let mut reports = Vec::new();
    for key in notified_keys(&v) {
        match bridge.object_finalized(&key).await {
            Ok(Some(report)) => reports.push(report),
            Ok(None) => {}
            Err(e) => {
                tracing::warn!(error = %e, key, "events bridge: notify failed");
                return Ok((
                    axum::http::StatusCode::SERVICE_UNAVAILABLE,
                    format!("events bridge: {e:#}"),
                )
                    .into_response());
            }
        }
    }
    Ok(axum::Json(reports).into_response())
}

fn auth_err(e: crate::auth::AuthError) -> crate::error::ApiError {
    use crate::error::ApiError;
    match e {
        crate::auth::AuthError::Invalid | crate::auth::AuthError::Unauthorized => {
            ApiError::Unauthorized
        }
        crate::auth::AuthError::Forbidden => ApiError::Forbidden,
        crate::auth::AuthError::Unavailable => {
            ApiError::ServiceUnavailable("auth provider unavailable".into())
        }
    }
}

/// `events.sweep_interval` timer (0 = off).
pub fn spawn_sweeper(state: Arc<crate::AppState>) {
    let Some(bridge) = state.bridge.clone() else {
        return;
    };
    let every = state.cfg.events.sweep_interval;
    if every.is_zero() {
        return;
    }
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(every).await;
            bridge.sweep().await;
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_object_names() {
        let b = Bridge {
            registry: walgit_wal::Registry::new(
                Arc::new(walgit_store::memory::MemoryStore::new()),
                Arc::new(walgit_config::Config::default()),
            ),
            store_prefix: "prefix/".into(),
            sinks: Vec::new(),
            serial: tokio::sync::Mutex::new(()),
        };
        let id = b
            .manifest_repo("prefix/repos/acme/monorepo/manifest.pb")
            .unwrap();
        assert_eq!(id.to_string(), "acme/monorepo");
        for other in [
            "prefix/repos/t/r/wal/abc.pack",
            "prefix/repos/t/r/events/cursor.json",
            "walgit-go/repos/t/r/manifest.pb",
            "prefix/repos/t/manifest.pb",
            "prefix/repos/t/r/x/manifest.pb",
        ] {
            assert!(b.manifest_repo(other).is_none(), "{other}");
        }
    }
}
