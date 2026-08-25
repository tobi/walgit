//! `ref` event shapes and the WAL → event conversion. Contract:
//! `docs/EVENTS.md`. The only producer is the bridge (`crate::bridge`): it
//! tails each repo's WAL from a durable cursor, converts committed PUSH /
//! REF_UPDATE entries with [`refs_from_entries`], and delivers to every
//! [`Sink`] before advancing the cursor. Nothing on the push path knows events
//! exist.
//!
//! Wire facts consumers rely on (the doc is normative, this is the code):
//! `old`/`new` are always full zero OIDs on create/delete, never empty;
//! dedup key = `(repo, _walgit.seq, ref_name)`; order by `_walgit.seq`.

use serde::Serialize;
use walgit_git::RepoId;
use walgit_proto::v1::{EntryKind, LogEntry};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Action {
    Create,
    Update,
    Delete,
}

impl Action {
    pub fn as_str(self) -> &'static str {
        match self {
            Action::Create => "create",
            Action::Update => "update",
            Action::Delete => "delete",
        }
    }
}

fn absent(oid: &str) -> bool {
    oid.is_empty() || oid.bytes().all(|b| b == b'0')
}

/// `create` / `update` / `delete` from the oid pair; `None` = no-op
/// (`0→0`, `X→X`) which must not emit.
pub fn classify(old: &str, new: &str) -> Option<Action> {
    match (absent(old), absent(new)) {
        (true, true) => None,
        (true, false) => Some(Action::Create),
        (false, true) => Some(Action::Delete),
        (false, false) if old == new => None,
        (false, false) => Some(Action::Update),
    }
}

/// The zero OID of the same length as `other` (sha1: 40, sha256: 64). The
/// WAL records "absent" as `""` (import-built txns) or the wire's zero OID
/// (pushes); the event always carries the zero OID.
fn zero_like(other: &str) -> String {
    "0".repeat(other.len())
}

pub fn ref_type(name: &str) -> &'static str {
    if name.starts_with("refs/heads/") {
        "branch"
    } else if name.starts_with("refs/tags/") {
        "tag"
    } else {
        ""
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct WalgitExt {
    pub schema_version: u32,
    /// WAL seq of the committed entry, as a string (uint64 convention).
    pub seq: String,
    /// `push` | `ref_update`.
    pub entry_kind: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub request_id: String,
}

/// Top level byte-compatible with the common `RefEvent` shape; walgit adds `repo`
/// and `_walgit`.
#[derive(Debug, Clone, Serialize)]
pub struct RefEvent {
    pub action: Action,
    pub ref_type: &'static str,
    pub ref_name: String,
    pub old: String,
    pub new: String,
    pub pusher: String,
    pub correlation_id: String,
    pub repo: String,
    #[serde(rename = "_walgit")]
    pub walgit: WalgitExt,
}

impl RefEvent {
    /// Message key (`<repo>/<ref_name>`): per-ref order where a transport keeps it.
    pub fn key(&self) -> String {
        format!("{}/{}", self.repo, self.ref_name)
    }
    /// Transport headers (`x_walgit_<name>`): filtering without parsing bodies.
    /// Always a strict subset of the body.
    pub fn headers(&self) -> [(&'static str, &str); 7] {
        [
            ("repo", &self.repo),
            ("action", self.action.as_str()),
            ("ref_type", self.ref_type),
            ("ref_name", &self.ref_name),
            ("pusher", &self.pusher),
            ("seq", &self.walgit.seq),
            ("correlation_id", &self.correlation_id),
        ]
    }
}

pub(crate) fn ref_event_count(entry: &LogEntry) -> usize {
    if !matches!(
        EntryKind::try_from(entry.kind),
        Ok(EntryKind::Push | EntryKind::RefUpdate)
    ) {
        return 0;
    }
    entry.txn.as_ref().map_or(0, |txn| {
        txn.updates
            .iter()
            .filter(|update| {
                update.new_symbolic_target.is_empty()
                    && classify(&update.old_oid, &update.new_oid).is_some()
            })
            .count()
    })
}

/// `ref` events for the PUSH / REF_UPDATE entries in `entries`, in seq order.
pub(crate) fn refs_from_entries(repo: &RepoId, entries: &[LogEntry], out: &mut Vec<RefEvent>) {
    let repo = repo.to_string();
    for entry in entries {
        let entry_kind = match EntryKind::try_from(entry.kind) {
            Ok(EntryKind::Push) => "push",
            Ok(EntryKind::RefUpdate) => "ref_update",
            // COMPACT / CHECKPOINT never move refs.
            _ => continue,
        };
        let Some(txn) = &entry.txn else { continue };
        let pusher = entry.meta.get("principal").cloned().unwrap_or_default();
        let request_id = entry.meta.get("request_id").cloned().unwrap_or_default();
        for u in &txn.updates {
            if !u.new_symbolic_target.is_empty() {
                continue; // HEAD retarget: no oid transition to announce
            }
            let Some(action) = classify(&u.old_oid, &u.new_oid) else {
                continue;
            };
            let (old, new) = match action {
                Action::Create => (zero_like(&u.new_oid), u.new_oid.clone()),
                Action::Delete => (u.old_oid.clone(), zero_like(&u.old_oid)),
                Action::Update => (u.old_oid.clone(), u.new_oid.clone()),
            };
            out.push(RefEvent {
                action,
                ref_type: ref_type(&u.name),
                ref_name: u.name.clone(),
                old,
                new,
                pusher: pusher.clone(),
                correlation_id: request_id.clone(),
                repo: repo.clone(),
                walgit: WalgitExt {
                    schema_version: 1,
                    seq: entry.seq.to_string(),
                    entry_kind: entry_kind.to_string(),
                    request_id: request_id.clone(),
                },
            });
        }
    }
}

pub(crate) fn batch_json_len(events: &[RefEvent]) -> anyhow::Result<u64> {
    Ok(u64::try_from(serde_json::to_vec(events)?.len())?)
}

/// Ref transactions are the indivisible delivery unit. Reject one before WAL
/// publication when its eventual webhook body cannot fit the configured bound.
pub(crate) fn ensure_entry_fits(
    repo: &RepoId,
    txn: &walgit_proto::v1::RefTransaction,
    meta: &std::collections::HashMap<String, String>,
    max_events: usize,
    max_bytes: u64,
) -> anyhow::Result<()> {
    let entry = LogEntry {
        seq: u64::MAX,
        kind: EntryKind::Push as i32,
        txn: Some(txn.clone()),
        meta: meta.clone(),
        ..Default::default()
    };
    let event_count = ref_event_count(&entry);
    anyhow::ensure!(
        event_count <= max_events,
        "ref transaction produces {event_count} events; events.max_batch_events is {max_events}"
    );
    let mut events = Vec::with_capacity(event_count);
    refs_from_entries(repo, &[entry], &mut events);
    let bytes = batch_json_len(&events)?;
    anyhow::ensure!(
        bytes <= max_bytes,
        "ref transaction produces a {bytes}-byte event payload; events.max_batch_bytes is {max_bytes} bytes"
    );
    Ok(())
}

/// Where the bridge publishes: a webhook receives each batch as a JSON array,
/// `POST`ed with `Content-Type: application/json`, a `X-Walgit-Delivery` id
/// (sha1 of the body — the consumer's dedup key) and, when `events.webhook_secret`
/// is set, `X-Walgit-Signature: sha256=<hex HMAC-SHA256 of the body>`. A non-2xx
/// answer fails the batch; the cursor does not move and the bridge retries.
#[async_trait::async_trait]
pub(crate) trait Sink: Send + Sync {
    fn name(&self) -> &'static str;
    async fn deliver(&self, batch: &[RefEvent]) -> anyhow::Result<()>;
}

pub(crate) struct WebhookSink {
    url: String,
    secret: Option<Vec<u8>>,
    client: reqwest::Client,
}

impl WebhookSink {
    pub fn new(url: String, secret: Option<String>) -> Self {
        WebhookSink {
            url,
            secret: secret.map(|s| s.into_bytes()),
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(10))
                .build()
                .expect("reqwest client"),
        }
    }

    /// `sha256=<hex>` over `body` with the shared secret.
    pub fn signature(secret: &[u8], body: &[u8]) -> String {
        use hmac::{Hmac, Mac};
        let mut mac = Hmac::<sha2::Sha256>::new_from_slice(secret).expect("hmac key");
        mac.update(body);
        format!("sha256={}", hex::encode(mac.finalize().into_bytes()))
    }
}

#[async_trait::async_trait]
impl Sink for WebhookSink {
    fn name(&self) -> &'static str {
        "webhook"
    }
    async fn deliver(&self, batch: &[RefEvent]) -> anyhow::Result<()> {
        use sha1::Digest;
        let body = serde_json::to_vec(batch)?;
        let delivery = hex::encode(sha1::Sha1::digest(&body));
        let mut req = self
            .client
            .post(&self.url)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .header("x-walgit-delivery", delivery);
        if let Some(secret) = &self.secret {
            req = req.header("x-walgit-signature", Self::signature(secret, &body));
        }
        let resp = req.body(body).send().await?;
        anyhow::ensure!(
            resp.status().is_success(),
            "webhook returned {}",
            resp.status()
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn classify_shapes() {
        assert_eq!(classify("", ""), None);
        assert_eq!(classify("0000", "0000"), None);
        assert_eq!(classify("", "abc"), Some(Action::Create));
        assert_eq!(classify("abc", ""), Some(Action::Delete));
        assert_eq!(classify("abc", "abc"), None);
        assert_eq!(classify("abc", "def"), Some(Action::Update));
        assert_eq!(ref_type("refs/heads/main"), "branch");
        assert_eq!(ref_type("refs/tags/v1"), "tag");
        assert_eq!(ref_type("refs/notes/commits"), "");
    }

    /// The golden body shape (docs/EVENTS.md). Field order and names are the
    /// contract; changing this test is changing the wire.
    #[test]
    fn ref_event_golden() {
        let ev = RefEvent {
            action: Action::Update,
            ref_type: "branch",
            ref_name: "refs/heads/main".into(),
            old: "48a0637".into(),
            new: "cb38da1".into(),
            pusher: "alice@example.com".into(),
            correlation_id: "d1f916f7".into(),
            repo: "acme/monorepo".into(),
            walgit: WalgitExt {
                schema_version: 1,
                seq: "42".into(),
                entry_kind: "push".into(),
                request_id: "d1f916f7".into(),
            },
        };
        let got = serde_json::to_string(&ev).unwrap();
        let want = concat!(
            "{\"action\":\"update\",\"ref_type\":\"branch\",\"ref_name\":\"refs/heads/main\",",
            "\"old\":\"48a0637\",\"new\":\"cb38da1\",\"pusher\":\"alice@example.com\",",
            "\"correlation_id\":\"d1f916f7\",\"repo\":\"acme/monorepo\",",
            "\"_walgit\":{\"schema_version\":1,\"seq\":\"42\",\"entry_kind\":\"push\",",
            "\"request_id\":\"d1f916f7\"}}"
        );
        assert_eq!(got, want);
        assert_eq!(ev.key(), "acme/monorepo/refs/heads/main");
    }

    fn log_entry(kind: EntryKind, name: &str, old: &str, new: &str, sym: &str) -> LogEntry {
        let mut meta = HashMap::new();
        meta.insert("principal".into(), "alice".into());
        meta.insert("request_id".into(), "cid".into());
        LogEntry {
            seq: 9,
            kind: kind as i32,
            txn: Some(walgit_proto::v1::RefTransaction {
                updates: vec![walgit_proto::v1::RefUpdate {
                    name: name.into(),
                    old_oid: old.into(),
                    new_oid: new.into(),
                    new_symbolic_target: sym.into(),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            meta,
            ..Default::default()
        }
    }

    const SHA: &str = "4a06219d6a5ba9d3e3d1a8e1e2b6f0c9d8e7f6a5";

    #[test]
    fn conversion_emits_ref_skips_compact_noop_symbolic() {
        let repo = walgit_git::RepoId::new("t", "r").unwrap();
        let mut out = Vec::new();
        refs_from_entries(
            &repo,
            &[
                log_entry(EntryKind::Compact, "refs/heads/main", "", SHA, ""),
                log_entry(EntryKind::Push, "refs/heads/main", SHA, SHA, ""),
                log_entry(EntryKind::Push, "HEAD", "", "", "refs/heads/main"),
                log_entry(EntryKind::Push, "refs/heads/main", "", SHA, ""),
                log_entry(EntryKind::Push, "refs/heads/main", SHA, &"0".repeat(40), ""),
            ],
            &mut out,
        );
        assert_eq!(out.len(), 2, "compact/noop/symbolic must not emit");
        let r = &out[0];
        assert_eq!(r.action, Action::Create);
        assert_eq!(r.ref_name, "refs/heads/main");
        assert_eq!(
            r.old,
            "0".repeat(40),
            "absent old is the zero OID, never empty"
        );
        assert_eq!(r.new, SHA);
        assert_eq!(
            (r.pusher.as_str(), r.correlation_id.as_str()),
            ("alice", "cid")
        );
        assert_eq!((r.repo.as_str(), r.walgit.seq.as_str()), ("t/r", "9"));
        assert_eq!(r.walgit.entry_kind, "push");
        let d = &out[1];
        assert_eq!(d.action, Action::Delete);
        assert_eq!(
            (d.old.as_str(), d.new.as_str()),
            (SHA, "0".repeat(40).as_str())
        );
    }
}
