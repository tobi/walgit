//! Bounded maintenance from committed state: checkpoints, repair, compaction and audits.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant, SystemTime};

use parking_lot::Mutex;
use prost::Message;
use tracing::{Instrument, info, warn};
use walgit_git::RepoId;
use walgit_store::{PutBody, PutMode, PutOptions};

use crate::AppState;

/// Run forever: a pass every `maintenance.interval`.
pub async fn run_loop(state: Arc<AppState>) {
    let interval = state.cfg.maintenance.interval;
    let started = SystemTime::now();
    let host = host_name(&state);
    info!(interval = ?interval, host = %host, maintain = ?state.cfg.placement.maintain, exclude = ?state.cfg.placement.maintain_exclude, "maintenance loop started");
    let mut passes = 0u64;
    let mut last_unit = String::new();
    loop {
        if walgit_wal::tasks::draining() {
            info!("maintenance loop: draining, no new pass");
            return;
        }
        passes += 1;
        let t0 = Instant::now();
        // `maintain.pass`: one close line per pass with the counts; every unit
        // (and its task.run) is a child, so a trace holds the whole pass.
        let span = tracing::info_span!("maintain.pass", host = %host, pass = passes, repos = tracing::field::Empty, units = tracing::field::Empty, skipped = tracing::field::Empty, outcome = tracing::field::Empty);
        // Heartbeat during long pack and reverse-index work; otherwise it shows the host
        // STALE and `upcoming` as "no live maintainer" while it is working.
        let ticker = {
            let (state, host, last_unit) = (state.clone(), host.clone(), last_unit.clone());
            tokio::spawn(async move {
                let mut t = tokio::time::interval(std::time::Duration::from_mins(2));
                t.tick().await;
                loop {
                    t.tick().await;
                    if let Err(e) = heartbeat(&state, &host, started, passes, &last_unit).await {
                        warn!(error = %e, "maintenance heartbeat (mid-pass) failed");
                    }
                }
            })
        };
        let outcome = run_pass(&state).instrument(span.clone()).await;
        ticker.abort();
        match outcome {
            Ok(r) => {
                if let Some(u) = &r.last_unit {
                    last_unit.clone_from(u);
                }
                span.record("repos", r.repos);
                span.record("units", r.units);
                span.record("skipped", r.skipped);
                span.record("outcome", "ok");
                if r.units > 0 {
                    info!(
                        repos = r.repos,
                        units = r.units,
                        checkpoints = r.checkpoints,
                        compactions = r.compactions,
                        "maintenance pass"
                    );
                }
            }
            Err(e) => {
                span.record("outcome", "error");
                warn!(error = %e, "maintenance pass failed");
            }
        }
        metrics::histogram!("walgit_maintain_pass_seconds", "host" => host.clone())
            .record(t0.elapsed().as_secs_f64());
        if let Err(e) = heartbeat(&state, &host, started, passes, &last_unit).await {
            warn!(error = %e, "maintenance heartbeat failed");
        }
        tokio::time::sleep(interval).await;
    }
}

#[derive(Debug, Default, Clone, serde::Serialize)]
pub struct PassReport {
    pub repos: usize,
    pub units: usize,
    pub checkpoints: usize,
    pub compactions: usize,
    pub last_unit: Option<String>,
    /// Repos that had a unit this host could not run (wrong-host/too-small/blocked are not counted here; planning errors are).
    pub skipped: u64,
}

/// What this host would do for `id` right now (the first unit of the
/// priority order), or why nothing.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub enum Unit {
    Checkpoint(String),
    Compact,
    /// fsck found missing objects and `upstream.git` can supply them.
    Repair(u64),
    /// An installed pack the manifest advertises without a `.rev` (written by
    /// git < 2.41, or imported without one): build it here, upload it as the
    /// side-file, CAS it into the manifest — every other host then downloads
    /// it on its next sync instead of rebuilding 60 M entries per fetch.
    RevIndex(String),
    /// Connectivity audit due (`maintenance.fsck_interval`): none recorded, older than the
    /// interval, or a repair landed since the last audit (re-verify).
    Fsck(String),
    /// Nothing to do.
    Idle,
    /// Not this host's repository (placement).
    NotAssigned,
}

/// Packs with at least this many objects get a `.rev` side-file (≈ 50 ns per
/// object per `pack-objects` without one: 60 M → 2.85 s, 250 k → 12 ms).
pub const REV_INDEX_MIN_OBJECTS: u64 = 250_000;

pub fn host_name(state: &AppState) -> String {
    state
        .cfg
        .maintenance
        .host
        .clone()
        .unwrap_or_else(|| walgit_store::coord::instance_id().to_string())
}

/// Consecutive failures per repository and the time its next unit may run.
///
/// A failing unit would otherwise be retried every pass; the exponential
/// backoff keeps a broken repository from rewriting packs in a tight loop.
static FAILURES: LazyLock<Mutex<HashMap<String, (u32, Instant)>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn backoff_delay(failures: u32) -> Duration {
    let minutes = 1u64 << failures.min(6);
    Duration::from_secs(minutes.saturating_mul(60).min(3_600))
}

fn failure_ready(id: &RepoId) -> bool {
    let now = Instant::now();
    FAILURES
        .lock()
        .get(&id.to_string())
        .is_none_or(|(_, until)| *until <= now)
}

fn record_failure(id: &RepoId) {
    let mut failures = FAILURES.lock();
    let entry = failures
        .entry(id.to_string())
        .or_insert((0, Instant::now()));
    entry.0 = entry.0.saturating_add(1);
    entry.1 = Instant::now() + backoff_delay(entry.0);
    if entry.0.is_power_of_two() {
        warn!(
            repo = %id,
            failures = entry.0,
            retry_in = ?backoff_delay(entry.0),
            "maintenance: backing off after repeated failures"
        );
    }
}

fn clear_failure(id: &RepoId) {
    FAILURES.lock().remove(&id.to_string());
}

pub async fn next_unit(state: &Arc<AppState>, id: &RepoId) -> anyhow::Result<Unit> {
    if !state.cfg.placement.maintains(id.owner(), id.name()) {
        return Ok(Unit::NotAssigned);
    }
    let handle = state.registry.open(id).await?;
    handle.sync_refs().await?;
    // D24: the repository's effective config (host ⊕ settings) decides what is
    // due; host-level facts (roles, assignment, capacity) stay the host's.
    let cfg = handle.validated_effective_config()?;
    {
        // Checkpoint lag/age gauges: how far the fold is behind the head.
        let m = handle.manifest();
        let cp_seq = m.checkpoint.as_ref().map_or(0, |c| c.seq);
        metrics::gauge!("walgit_checkpoint_lag_entries", "repo" => id.to_string())
            .set(m.head_seq.saturating_sub(cp_seq) as f64);
        if let Some(t) = m
            .checkpoint
            .as_ref()
            .and_then(|c| c.created_at.as_ref())
            .map(walgit_proto::time::to_system)
        {
            metrics::gauge!("walgit_checkpoint_age_seconds", "repo" => id.to_string()).set(
                SystemTime::now()
                    .duration_since(t)
                    .unwrap_or_default()
                    .as_secs_f64(),
            );
        }
    }
    if cfg.maintenance.checkpoints
        && let Some(trigger) = handle.checkpoint_due()
    {
        return Ok(Unit::Checkpoint(trigger.to_string()));
    }
    // Integrity before everything else that builds on the object set.
    let fsck = crate::ops::read_fsck(&handle).await.ok().flatten();
    if let Some(f) = &fsck {
        metrics::gauge!("walgit_repo_missing_objects", "repo" => id.to_string()).set(
            if f.repaired_seq > 0 {
                0.0
            } else {
                f.missing_total as f64
            },
        );
        if !f.missing.is_empty() && f.repaired_seq == 0 && cfg.upstream.git.is_some() {
            return Ok(Unit::Repair(f.missing_total));
        }
    }
    if cfg.packs.enabled
        && state.cfg.has_role(walgit_config::Role::Compact)
        && handle.packs_fit()
        && crate::pack_lifecycle::plan(&handle.manifest(), &cfg, std::time::SystemTime::now())
            .is_some()
    {
        return Ok(Unit::Compact);
    }
    // A big pack without its `.rev` side-file, where the pack is local (tmpfs
    // hosts link tier-2 bases from the mount: the maintainer with the disk does
    // it). Push packs (gix ingest, no .rev) stay as they are: git's in-memory
    // reverse index costs ~50 ns/object per pack-objects, nothing below the
    // threshold; a side-file per push would be manifest churn for no gain.
    if handle.packs_fit() && state.cfg.has_role(walgit_config::Role::Compact) {
        let m = handle.manifest();
        if let Some(p) = m
            .packs
            .iter()
            .filter(|p| !p.has_rev && p.object_count >= REV_INDEX_MIN_OBJECTS)
            .min_by_key(|p| p.seq)
            && let Ok(oid) = gix_hash::ObjectId::from_hex(p.checksum.as_bytes())
            && handle.local().pack_path(&oid).exists()
        {
            return Ok(Unit::RevIndex(p.checksum.clone()));
        }
    }
    // Lowest priority: the audit itself. Only where the whole pack set is local
    // (fsck over a linked/remote base would read 32 GB through the mount).
    let interval = cfg.maintenance.fsck_interval;
    if !interval.is_zero() && handle.packs_fit() {
        let due = match &fsck {
            None => Some("never audited".to_string()),
            Some(f) if f.repaired_seq > 0 && handle.manifest().head_seq >= f.repaired_seq => {
                Some(format!("re-verify after repair at seq {}", f.repaired_seq))
            }
            Some(f) => {
                let at =
                    f.at.as_ref()
                        .map_or(SystemTime::UNIX_EPOCH, walgit_proto::time::to_system);
                let age = SystemTime::now().duration_since(at).unwrap_or_default();
                (age >= interval).then(|| format!("last audit {}h ago", age.as_secs() / 3600))
            }
        };
        if let Some(why) = due {
            return Ok(Unit::Fsck(why));
        }
    }
    Ok(Unit::Idle)
}

/// One pass: one unit per assigned repository.
pub async fn run_pass(state: &Arc<AppState>) -> anyhow::Result<PassReport> {
    let mut report = PassReport::default();
    let repos = state.registry.list().await?;
    for id in repos {
        if !state.cfg.placement.maintains(id.owner(), id.name()) {
            continue;
        }
        if walgit_wal::tasks::draining() {
            break;
        }
        report.repos += 1;
        if !failure_ready(&id) {
            report.skipped += 1;
            continue;
        }
        let unit = match next_unit(state, &id).await {
            Ok(u) => u,
            Err(e) => {
                warn!(repo = %id, error = %e, "maintenance: planning failed");
                report.skipped += 1;
                continue;
            }
        };
        if matches!(unit, Unit::Idle | Unit::NotAssigned) {
            continue;
        }
        let kind = match &unit {
            Unit::Checkpoint(_) => "checkpoint",
            Unit::Compact => "compact",
            Unit::Repair(_) => "repair",
            Unit::RevIndex(_) => "rev-index",
            Unit::Fsck(_) => "fsck",
            Unit::Idle | Unit::NotAssigned => unreachable!(),
        };
        let unit_span =
            tracing::info_span!("maintain.unit", repo = %id, kind, outcome = tracing::field::Empty);
        let t_unit = Instant::now();
        let done = async {
            match &unit {
                Unit::Checkpoint(trigger) => {
                    let mut params = HashMap::new();
                    params.insert("trigger".to_string(), trigger.clone());
                    let ok = run_op(state, &id, "checkpoint", params).await;
                    if ok {
                        report.checkpoints += 1;
                    }
                    ok
                }
                Unit::Compact => {
                    let ok = run_op(state, &id, "compact", HashMap::new()).await;
                    if ok {
                        report.compactions += 1;
                    }
                    ok
                }
                Unit::Repair(_) => run_op(state, &id, "repair", HashMap::new()).await,
                Unit::RevIndex(checksum) => {
                    let mut params = HashMap::new();
                    params.insert("pack".to_string(), checksum.clone());
                    run_op(state, &id, "rev-index", params).await
                }
                Unit::Fsck(why) => {
                    let mut params = HashMap::new();
                    params.insert("connectivity".to_string(), "1".to_string());
                    params.insert("why".to_string(), why.clone());
                    run_op(state, &id, "fsck", params).await
                }
                Unit::Idle | Unit::NotAssigned => false,
            }
        }
        .instrument(unit_span.clone())
        .await;
        let outcome = if done { "ok" } else { "failed" };
        unit_span.record("outcome", outcome);
        metrics::counter!("walgit_maintain_units_total", "host" => host_name(state), "kind" => kind, "outcome" => outcome).increment(1);
        metrics::histogram!("walgit_maintain_unit_seconds", "kind" => kind)
            .record(t_unit.elapsed().as_secs_f64());
        if done {
            report.units += 1;
            report.last_unit = Some(format!("{id} {unit:?}"));
            clear_failure(&id);
        } else {
            report.skipped += 1;
            record_failure(&id);
        }
    }
    Ok(report)
}

/// Heartbeats older than this are a departed host, not a stale one.
const HEARTBEAT_EXPIRY: std::time::Duration = std::time::Duration::from_hours(24);

/// Every maintainer heartbeat in the bucket (expired ones purged).
pub async fn heartbeats(
    state: &AppState,
) -> anyhow::Result<Vec<walgit_proto::v1::MaintainerHeartbeat>> {
    use futures::StreamExt;
    use walgit_store::ObjectStoreExt;
    let mut out = Vec::new();
    let mut keys = state.store.list(walgit_proto::keys::MAINTAIN_DIR, None);
    while let Some(m) = keys.next().await {
        let m = m?;
        if let Some((meta, bytes)) = state.store.get_bytes(&m.key).await?
            && let Ok(hb) = walgit_proto::v1::MaintainerHeartbeat::decode(bytes.as_ref())
        {
            // A host that has not passed for a day is gone: purge its
            // heartbeat so the plan shows only live maintainers.
            let age = hb
                .last_pass_at
                .as_ref()
                .map(walgit_proto::time::to_system)
                .and_then(|t| SystemTime::now().duration_since(t).ok());
            if age.is_some_and(|a| a > HEARTBEAT_EXPIRY) {
                if state.cfg.has_role(walgit_config::Role::Maintain) {
                    info!(host = %hb.host, age_secs = age.map_or(0, |a| a.as_secs()), "maintenance: purging expired heartbeat");
                    let _ = state.store.delete(&m.key, Some(meta.version)).await;
                }
                continue;
            }
            out.push(hb);
        }
    }
    Ok(out)
}

async fn heartbeat(
    state: &Arc<AppState>,
    host: &str,
    started: SystemTime,
    passes: u64,
    last_unit: &str,
) -> anyhow::Result<()> {
    metrics::gauge!("walgit_maintainer_heartbeat_timestamp", "host" => host.to_string()).set(
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64(),
    );
    let hb = walgit_proto::v1::MaintainerHeartbeat {
        host: host.to_string(),
        repos: state.cfg.placement.maintain.clone(),
        exclude: state.cfg.placement.maintain_exclude.clone(),
        max_pack_bytes: {
            let c = state.cfg.maintenance.max_pack_bytes.as_u64();
            if c == 0 {
                state.cfg.cache_budget_bytes()
            } else {
                c
            }
        },
        disk: format!("{:?}", state.cfg.maintenance.disk).to_lowercase(),
        started_at: Some(walgit_proto::time::from_system(started)),
        last_pass_at: Some(walgit_proto::time::now()),
        last_unit: last_unit.to_string(),
        passes,
    };
    state
        .store
        .put(
            &walgit_proto::keys::maintainer_key(host),
            PutBody::Bytes(hb.encode_to_vec().into()),
            PutOptions::from(PutMode::Overwrite),
        )
        .await?;
    Ok(())
}

/// Start `op` as a task and wait for it. Returns true when it finished ok.
async fn run_op(
    state: &Arc<AppState>,
    id: &RepoId,
    op: &str,
    params: HashMap<String, String>,
) -> bool {
    run_op_value(state, id, op, params).await.is_some()
}

/// Like [`run_op`], returning the op's result value (`None` = failed / still running).
async fn run_op_value(
    state: &Arc<AppState>,
    id: &RepoId,
    op: &str,
    params: HashMap<String, String>,
) -> Option<serde_json::Value> {
    let started = Instant::now();
    let task = match crate::ops::start(state.clone(), id.clone(), op, params).await {
        Ok(t) | Err(crate::ops::StartError::AlreadyRunning(t)) => t,
        Err(crate::ops::StartError::UnknownOp) => {
            warn!(repo = %id, op, "maintenance: cannot start op");
            return None;
        }
    };
    // Bounded: a maintenance op that runs longer than an hour is reported and
    // left running (it stays discoverable at …/tasks); the pass moves on.
    if !task.wait_done(std::time::Duration::from_hours(1)).await {
        warn!(repo = %id, op, "maintenance: op still running after 1h; moving on");
        return None;
    }
    match task.outcome() {
        Some(Ok(o)) => {
            info!(repo = %id, op, ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX), "maintenance: done");
            Some(o.value.unwrap_or(serde_json::Value::Null))
        }
        Some(Err((_, msg))) => {
            warn!(repo = %id, op, error = %msg, "maintenance: failed");
            None
        }
        None => None,
    }
}

#[cfg(test)]
mod tests {
    use super::backoff_delay;
    use std::time::Duration;

    #[test]
    fn backoff_grows_and_is_capped() {
        assert_eq!(backoff_delay(1), Duration::from_mins(2));
        assert_eq!(backoff_delay(2), Duration::from_mins(4));
        assert_eq!(backoff_delay(10), Duration::from_hours(1));
    }
}
