//! Upstream follow — ingress through the WAL. Refs of a repository follow the same
//! refs on an upstream git host (`[upstream] git` + `follow`, D24 settings or host
//! config), performed by the host that maintains the repository (D28: its writer):
//!
//! every `maintenance.follow_interval` the loop fetches the delta of the followed
//! refs into a scratch repository over the serving copy's objects
//! (`walgit_git::follow`), and when anything moved runs the `follow` op as a task:
//! stream the pack through `ingest_pack`, connectivity + fast-forward checks, one
//! `publish_push` — the same PUSH entry receive-pack publishes, `principal =
//! upstream`. Every other instance picks it up the way it picks up a push (the next
//! conditional GET of `manifest.pb`); the maintainer's checkpoints/bundles fold it
//! like a push. A rewound upstream is refused (not a fast-forward) and logged every
//! round until a human decides; policy is not evaluated (follow is configuration,
//! not a principal — remove the ref from `follow` to stop it).
//!
//! Its own loop, not a unit of the priority loop (`maintain.rs`): ingress must not
//! wait behind a 30-minute base rebuild, and as the top unit it would starve the
//! derived work of a busy repository. No task for a round that found nothing.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use tracing::{Instrument, debug, info, warn};
use walgit_git::{IngestOptions, RepoId};
use walgit_proto::v1::{RefTransaction, RefUpdate};

use crate::AppState;

/// Run forever: a round every `maintenance.follow_interval` (0 = never).
pub async fn run_loop(state: Arc<AppState>) {
    let interval = state.cfg.maintenance.follow_interval;
    if interval.is_zero() {
        return;
    }
    info!(interval = ?interval, "upstream follow loop started");
    loop {
        tokio::time::sleep(interval).await;
        if walgit_wal::tasks::draining() {
            info!("upstream follow loop: draining, no new round");
            return;
        }
        match run_pass(&state).await {
            Ok(r) if r.behind > 0 || r.failed > 0 => info!(
                repos = r.repos,
                behind = r.behind,
                published = r.published,
                failed = r.failed,
                "upstream follow round"
            ),
            Ok(_) => {}
            Err(e) => warn!(error = %e, "upstream follow round failed"),
        }
    }
}

/// What the last round did for a repository on this instance (the Settings tab
/// shows it next to the configuration; per instance, like tasks — the maintaining
/// host answers the repository's routes).
#[derive(Debug, Clone, serde::Serialize)]
pub struct FollowStatus {
    /// RFC 3339.
    pub at: String,
    /// `in-sync` | `published` | `refused` | `failed`
    pub outcome: &'static str,
    /// Human line: what moved / why it did not.
    pub detail: String,
    /// `ref → oid` upstream has (as fetched this round).
    pub upstream: HashMap<String, String>,
    /// `ref → oid` the WAL had before the round.
    pub ours: HashMap<String, String>,
}

/// Per-repo last-round status on this instance.
#[derive(Default)]
pub struct FollowStatuses(parking_lot::Mutex<HashMap<String, FollowStatus>>);

impl FollowStatuses {
    pub fn get(&self, repo: &str) -> Option<FollowStatus> {
        self.0.lock().get(repo).cloned()
    }
    fn set(
        &self,
        repo: &str,
        outcome: &'static str,
        detail: String,
        upstream: HashMap<String, String>,
        ours: HashMap<String, String>,
    ) {
        let at = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        self.0.lock().insert(
            repo.to_string(),
            FollowStatus {
                at,
                outcome,
                detail,
                upstream,
                ours,
            },
        );
    }
}

#[derive(Debug, Default, Clone, serde::Serialize)]
pub struct FollowReport {
    /// Assigned repositories with `upstream.follow` set.
    pub repos: usize,
    /// Of those, how many had a followed ref that differs from upstream's.
    pub behind: usize,
    /// `follow` ops that published.
    pub published: usize,
    pub failed: usize,
}

/// One round: for every assigned repository that follows an upstream, fetch the
/// delta (no task); when a followed ref moved, run the `follow` op on it.
pub async fn run_pass(state: &Arc<AppState>) -> anyhow::Result<FollowReport> {
    let mut report = FollowReport::default();
    for id in state.registry.list().await? {
        if !state.cfg.placement.maintains(id.owner(), id.name()) {
            continue;
        }
        if walgit_wal::tasks::draining() {
            break;
        }
        let handle = state.registry.open(&id).await?;
        let _refs = handle.sync_refs().await?; // the manifest carries the settings (D24)
        drop(_refs);
        let cfg = handle.effective_config();
        let Some(upstream) = cfg.upstream.git.clone() else {
            continue;
        };
        if cfg.upstream.follow.is_empty() {
            continue;
        }
        report.repos += 1;
        if !handle.packs_fit() {
            warn!(repo = %id, "follow: the whole object set must be local on this host (negotiation + thin-pack bases); skipping");
            continue;
        }
        let t0 = Instant::now();
        let fetched = async {
            // Hold the read guard through the fetch: nothing removes a pack under
            // the scratch's alternates while git reads them.
            let guard = handle.sync().await?;
            let have = current(&handle, &cfg.upstream.follow)?;
            let token = token_for(state, &cfg).await?;
            let delta = walgit_git::follow::fetch_refs(
                &upstream,
                token.as_deref(),
                &handle.local().path().join("objects"),
                &have,
                &cfg.upstream.follow,
                &scratch_dir(state, &id),
            )
            .await?;
            drop(guard);
            let moved = cfg
                .upstream
                .follow
                .iter()
                .any(|r| delta.tips.get(r).is_some_and(|t| have.get(r) != Some(t)));
            if !moved {
                delta.discard_pack().await;
            }
            anyhow::Ok((moved, delta.tips, have))
        }
        .await;
        let repo = id.to_string();
        match fetched {
            Ok((false, tips, have)) => {
                debug!(repo = %id, %upstream, elapsed_ms = u64::try_from(t0.elapsed().as_millis()).unwrap_or(u64::MAX), "follow: in sync");
                state.follow.set(
                    &repo,
                    "in-sync",
                    format!(
                        "{} up to date with {upstream}",
                        cfg.upstream.follow.join(", ")
                    ),
                    tips,
                    have,
                );
            }
            Ok((true, tips, have)) => {
                report.behind += 1;
                let mut params = HashMap::new();
                params.insert("prefetched".to_string(), "1".to_string());
                if let Some(v) = run_op(state, &id, params).await {
                    let n = v
                        .get("published")
                        .and_then(serde_json::Value::as_u64)
                        .unwrap_or(0);
                    if n > 0 {
                        report.published += 1;
                    }
                    let seq = v
                        .get("seq")
                        .and_then(serde_json::Value::as_u64)
                        .unwrap_or(0);
                    let refused: Vec<String> = v
                        .get("refused")
                        .and_then(|r| r.as_array())
                        .map(|a| {
                            a.iter()
                                .filter_map(|x| x.as_str().map(String::from))
                                .collect()
                        })
                        .unwrap_or_default();
                    let detail = if refused.is_empty() {
                        format!("{n} ref(s) published at seq {seq}")
                    } else {
                        format!(
                            "{n} ref(s) published at seq {seq}; refused: {}",
                            refused.join("; ")
                        )
                    };
                    state.follow.set(
                        &repo,
                        if n > 0 { "published" } else { "refused" },
                        detail,
                        tips,
                        have,
                    );
                } else {
                    report.failed += 1;
                    // The task's summary names the reason (rewind, unpack, connectivity, publish).
                    let why = state
                        .registry
                        .tasks()
                        .recent(&repo)
                        .into_iter()
                        .find(|t| t.kind == "follow")
                        .map(|t| t.summary)
                        .unwrap_or_default();
                    state.follow.set(&repo, "refused", why, tips, have);
                }
            }
            Err(e) => {
                report.failed += 1;
                metrics::counter!("walgit_follow_rounds_total", "repo" => id.to_string(), "outcome" => "fetch-failed").increment(1);
                warn!(repo = %id, %upstream, error = format!("{e:#}"), elapsed_ms = u64::try_from(t0.elapsed().as_millis()).unwrap_or(u64::MAX), "follow: fetch from upstream failed");
                state.follow.set(
                    &repo,
                    "failed",
                    format!("fetch from {upstream} failed: {e:#}"),
                    HashMap::new(),
                    HashMap::new(),
                );
            }
        }
    }
    Ok(report)
}

/// Start the `follow` op as a task and wait for it; its result value, `None` = failed.
async fn run_op(
    state: &Arc<AppState>,
    id: &RepoId,
    params: HashMap<String, String>,
) -> Option<serde_json::Value> {
    let task = match crate::ops::start(state.clone(), id.clone(), "follow", params).await {
        Ok(t) | Err(crate::ops::StartError::AlreadyRunning(t)) => t,
        Err(crate::ops::StartError::UnknownOp) => return None,
    };
    if !task.wait_done(std::time::Duration::from_hours(1)).await {
        warn!(repo = %id, "follow: op still running after 1h; moving on");
        return None;
    }
    match task.outcome() {
        Some(Ok(o)) => Some(o.value.unwrap_or(serde_json::Value::Null)),
        _ => None,
    }
}

/// The `follow` op (`ops.rs`): fetch unless `prefetched=1` (the loop just did),
/// then ingest the pack like a push, check connectivity and fast-forward, publish.
pub(crate) async fn op(
    state: &Arc<AppState>,
    handle: &walgit_wal::RepoHandle,
    id: &RepoId,
    params: &HashMap<String, String>,
    log: crate::ops::Log<'_>,
) -> Result<(String, serde_json::Value), String> {
    let cfg = handle.effective_config();
    let upstream = cfg
        .upstream
        .git
        .clone()
        .ok_or("follow: no upstream.git for this repository")?;
    let refs = cfg.upstream.follow.clone();
    if refs.is_empty() {
        return Err("follow: upstream.follow is empty for this repository".into());
    }
    let prefetched = params.get("prefetched").is_some_and(|v| v == "1");
    let t0 = Instant::now();
    let guard = handle.sync().await.map_err(|e| format!("sync: {e}"))?;
    let local = handle.local().clone();
    let have = current(handle, &refs).map_err(|e| format!("{e:#}"))?;
    let scratch = scratch_dir(state, id);
    let delta = if prefetched {
        walgit_git::follow::read_scratch(&scratch)
            .await
            .map_err(|e| format!("reading the fetched delta: {e}"))?
    } else {
        let token = token_for(state, &cfg).await.map_err(|e| format!("{e:#}"))?;
        log(format!("fetching {} from {upstream}", refs.join(", ")));
        walgit_git::follow::fetch_refs(
            &upstream,
            token.as_deref(),
            &local.path().join("objects"),
            &have,
            &refs,
            &scratch,
        )
        .await
        .map_err(|e| format!("fetch from upstream: {e}"))?
    };

    // What moves. A ref upstream no longer has is left alone (deleting is a human's call).
    let mut updates: Vec<RefUpdate> = Vec::new();
    for r in &refs {
        let Some(new) = delta.tips.get(r) else {
            log(format!("{r}: not on upstream; left as is"));
            continue;
        };
        let old = have.get(r).cloned().unwrap_or_default();
        if old != *new {
            updates.push(RefUpdate {
                name: r.clone(),
                old_oid: old,
                new_oid: new.clone(),
                ..Default::default()
            });
        }
    }
    if updates.is_empty() {
        delta.discard_pack().await;
        return Ok((
            "in sync with upstream".into(),
            serde_json::json!({"published": 0}),
        ));
    }

    // Objects: the fetched pack goes through the same ingest as a push (the scratch
    // completed it from our own objects, so it is not thin).
    let ingested = match &delta.pack {
        Some(p) => {
            let bytes = tokio::fs::metadata(p).await.map_or(0, |m| m.len());
            log(format!("ingesting {bytes} bytes of objects from upstream"));
            let file = tokio::fs::File::open(p)
                .await
                .map_err(|e| format!("opening the fetched pack: {e}"))?;
            local
                .ingest_pack(
                    file,
                    IngestOptions {
                        fsck: cfg.wal.fsck_objects,
                        max_bytes: None,
                        thin: false,
                    },
                )
                .await
                .map_err(|e| format!("unpack failed: {e}"))?
        }
        None => None, // a ref moved to objects we already hold (e.g. a rewind)
    };
    if cfg.wal.check_connectivity {
        let tips: Vec<gix_hash::ObjectId> = updates
            .iter()
            .filter_map(|u| gix_hash::ObjectId::from_hex(u.new_oid.as_bytes()).ok())
            .collect();
        local
            .check_connectivity_async(&tips, true)
            .instrument(tracing::info_span!(
                "follow.connectivity",
                tips = tips.len()
            ))
            .await
            .map_err(|e| format!("connectivity: {e}"))?;
    }
    // Fast-forward only: a rewound upstream is a human matter, reported every round.
    let mut refused: Vec<String> = Vec::new();
    let mut publishable: Vec<RefUpdate> = Vec::new();
    for u in updates {
        if !u.old_oid.is_empty() {
            match local.is_ancestor(&u.old_oid, &u.new_oid).await {
                Ok(true) => {}
                Ok(false) => {
                    refused.push(format!(
                        "{}: upstream rewound {}→{} (not a fast-forward)",
                        u.name,
                        short(&u.old_oid),
                        short(&u.new_oid)
                    ));
                    continue;
                }
                Err(e) => {
                    refused.push(format!("{}: {e}", u.name));
                    continue;
                }
            }
        }
        publishable.push(u);
    }
    drop(guard);
    if publishable.is_empty() {
        delta.discard_pack().await;
        metrics::counter!("walgit_follow_rounds_total", "repo" => id.to_string(), "outcome" => "refused").increment(1);
        return Err(format!(
            "follow: nothing publishable — {}",
            refused.join("; ")
        ));
    }
    let mut txn = RefTransaction {
        updates: publishable,
        ..Default::default()
    };
    local.fill_peeled(&mut txn);
    let meta = HashMap::from([
        ("principal".to_string(), "upstream".to_string()),
        ("upstream".to_string(), upstream.clone()),
        ("agent".to_string(), "walgit follow".to_string()),
    ]);
    let planned: Vec<(String, String, String)> = txn
        .updates
        .iter()
        .map(|u| (u.name.clone(), u.old_oid.clone(), u.new_oid.clone()))
        .collect();
    let res = handle
        .publish_push(ingested, txn, meta)
        .await
        .map_err(|e| format!("publish: {e}"))?;
    delta.discard_pack().await;
    let mut published = 0u64;
    for (name, r) in &res.per_ref {
        let (old, new) = planned
            .iter()
            .find(|(n, _, _)| n == name)
            .map_or(("", ""), |(_, o, n)| (o.as_str(), n.as_str()));
        match r {
            Ok(()) => {
                published += 1;
                log(format!(
                    "{name}: {}..{} published at seq {}",
                    short(old),
                    short(new),
                    res.seq
                ));
            }
            Err(e) => refused.push(format!("{name}: {e}")), // moved under us: the next round re-plans
        }
    }
    metrics::counter!("walgit_follow_rounds_total", "repo" => id.to_string(), "outcome" => "published").increment(1);
    metrics::counter!("walgit_follow_refs_total", "repo" => id.to_string()).increment(published);
    info!(repo = %id, seq = res.seq, refs = published, refused = refused.len(), %upstream, elapsed_ms = u64::try_from(t0.elapsed().as_millis()).unwrap_or(u64::MAX), "follow published");
    let summary = format!(
        "{published} ref(s) from upstream published at seq {} in {:.1}s{}",
        res.seq,
        t0.elapsed().as_secs_f64(),
        if refused.is_empty() {
            String::new()
        } else {
            format!("; refused: {}", refused.join("; "))
        }
    );
    Ok((
        summary,
        serde_json::json!({"published": published, "seq": res.seq, "refused": refused}),
    ))
}

/// The WAL's current values of `refs` (from the synced local copy).
fn current(
    handle: &walgit_wal::RepoHandle,
    refs: &[String],
) -> anyhow::Result<HashMap<String, String>> {
    let snapshot = handle.local().refs()?;
    Ok(snapshot
        .refs
        .into_iter()
        .filter(|r| refs.contains(&r.name))
        .map(|r| (r.name, r.oid))
        .collect())
}

async fn token_for(
    state: &AppState,
    cfg: &walgit_config::Config,
) -> anyhow::Result<Option<String>> {
    match cfg.upstream.token_env.as_deref() {
        Some(name) => Ok(Some(
            state
                .lfs_upstream
                .secret(name)
                .await
                .map_err(|e| anyhow::anyhow!("upstream token: {e}"))?,
        )),
        None => Ok(None),
    }
}

/// `cache.dir/follow/<owner>/<name>.git` — the persistent scratch over the serving copy.
fn scratch_dir(state: &AppState, id: &RepoId) -> PathBuf {
    state
        .cfg
        .cache
        .dir
        .join("follow")
        .join(id.owner())
        .join(format!("{}.git", id.name()))
}

fn short(oid: &str) -> &str {
    if oid.is_empty() {
        "(none)"
    } else {
        &oid[..oid.len().min(12)]
    }
}
