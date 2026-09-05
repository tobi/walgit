//! The `maintain` role: a **permanent, self-healing priority loop**. Every
//! `maintenance.interval`, for each *assigned* repository (`[maintenance]
//! repos` minus `exclude` — placement by rule), it picks the most important
//! unit of work and does exactly ONE bounded unit as a task (discoverable at
//! `…/tasks`, visible on the WAL page):
//!
//! 1. checkpoint-if-due (refs-level; works for any repo on any host),
//! 2. the missing **weekly** slot (full bundle; compose for repos whose base is
//!    not a local copy here — or `wrong-host`),
//! 3. missing **daily** slots, oldest first (backfill),
//! 4. missing **hourly** slots, oldest first,
//! 5. geometric compaction when triggered.
//!
//! Slot content = the WAL state as of the slot (highest seq with
//! `created_at <= slot`); a slot before the repository's first WAL state is
//! `unavailable` (not an error); a unit this host cannot hold is `wrong-host`
//! and left to the host that can (`[maintenance] max_pack_bytes`, `disk`).
//! Each pass writes a heartbeat (`maintain/<host>.pb`) so the plan shows who
//! maintains a repository and whether that host is alive.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Instant, SystemTime};

use prost::Message;
use tracing::{Instrument, info, warn};
use walgit_bundle::slots::{PlanContext, SlotStatus};
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
        tokio::time::sleep(interval).await;
        if walgit_wal::tasks::draining() {
            info!("maintenance loop: draining, no new pass");
            return;
        }
        passes += 1;
        let t0 = Instant::now();
        // `maintain.pass`: one close line per pass with the counts; every unit
        // (and its task.run) is a child, so a trace holds the whole pass.
        let span = tracing::info_span!("maintain.pass", host = %host, pass = passes, repos = tracing::field::Empty, units = tracing::field::Empty, skipped = tracing::field::Empty, outcome = tracing::field::Empty);
        // Heartbeat DURING the pass too: a long unit (Sunday's 25-min base
        // rebuild, a 1 h rev-index over a 32 GB pack) otherwise shows the host
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
                        bundles = r.bundles,
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
    }
}

#[derive(Debug, Default, Clone, serde::Serialize)]
pub struct PassReport {
    pub repos: usize,
    pub units: usize,
    pub checkpoints: usize,
    pub bundles: usize,
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
    /// `(strategy, slot)`
    BundleSlot(String, u64),
    Compact,
    /// fsck found missing objects and `upstream.git` can supply them.
    Repair(u64),
    /// A full (weekly) slot is missing and pushes landed since the tier-2 base:
    /// rebuild the base first (`compact --base`: repack -adb + history pack +
    /// bitmap + commit-graph, checkpoint) on this ssd host, then the slot
    /// composes it. `(strategy, slot)` of the full slot waiting on it.
    BaseRebuild(String, u64),
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

/// The planner's view of this host for `id`.
pub fn plan_context(state: &AppState, handle: &walgit_wal::RepoHandle) -> PlanContext {
    let cap = state.cfg.maintenance.max_pack_bytes.as_u64();
    let cap = if cap == 0 {
        state.cfg.cache_budget_bytes()
    } else {
        cap
    };
    let full_bytes: u64 = handle
        .manifest()
        .packs
        .iter()
        .map(|p| p.pack_size + p.idx_size)
        .sum();
    let ssd = state.cfg.maintenance.disk == walgit_config::MaintainerDisk::Ssd;
    // A full bundle is a compose of an existing base when one exists (any
    // host), else `git bundle create` on a complete local copy: the set must
    // fit this host's declared capacity (large-repository-sized sets only fit an ssd
    // host; a small repo is fine on tmpfs).
    let has_base = walgit_wal::base_pack(&handle.manifest()).is_some();
    // cap == 0 ⇒ unlimited (disk mode, D25).
    let can_full = has_base || ((cap == 0 || full_bytes <= cap) && (ssd || handle.packs_fit()));
    let can_incremental = handle.serve_fits();
    let first_state = handle.first_state_time();
    PlanContext {
        first_state,
        can_full,
        can_incremental,
        wrong_host_reason: if !can_incremental {
            Some("the serving copy does not fit this host's cache")
        } else if !can_full {
            Some("a full bundle needs the whole pack set on an ssd host")
        } else {
            None
        },
    }
}

/// Plan → span fields + `walgit_bundle_plan_slots{repo,strategy,state}` gauges.
fn record_plan(
    span: &tracing::Span,
    id: &RepoId,
    cfg: &walgit_config::BundlesConfig,
    rows: &[walgit_bundle::slots::SlotPlan],
) {
    let state_of = |s: &SlotStatus| -> &'static str {
        match s {
            SlotStatus::Built { .. } => "built",
            SlotStatus::Missing => "missing",
            SlotStatus::Pending => "pending",
            SlotStatus::Blocked(_) => "blocked",
            SlotStatus::TooSmall { .. } => "too-small",
            SlotStatus::Skipped { .. } => "skipped",
            SlotStatus::Unavailable => "unavailable",
            SlotStatus::WrongHost(_) => "wrong-host",
        }
    };
    let count = |st: &str| rows.iter().filter(|r| state_of(&r.status) == st).count() as u64;
    span.record("slots", rows.len() as u64);
    span.record("built", count("built"));
    span.record("missing", count("missing"));
    span.record("too_small", count("too-small"));
    span.record("wrong_host", count("wrong-host"));
    span.record("unavailable", count("unavailable"));
    for strat in &cfg.strategy {
        for st in [
            "built",
            "missing",
            "blocked",
            "too-small",
            "skipped",
            "unavailable",
            "wrong-host",
        ] {
            let n = rows
                .iter()
                .filter(|r| r.strategy == strat.name && state_of(&r.status) == st)
                .count();
            metrics::gauge!("walgit_bundle_plan_slots", "repo" => id.to_string(), "strategy" => strat.name.clone(), "state" => st).set(n as f64);
        }
    }
}

/// The next unit for `id` on this host (pure w.r.t. side effects except a
/// refs sync and a bundle-list read).
pub async fn next_unit(state: &Arc<AppState>, id: &RepoId) -> anyhow::Result<Unit> {
    if !state.cfg.placement.maintains(id.owner(), id.name()) {
        return Ok(Unit::NotAssigned);
    }
    let handle = state.registry.open(id).await?;
    handle.sync_refs().await?;
    // D24: the repository's effective config (host ⊕ settings) decides what is
    // due; host-level facts (roles, assignment, capacity) stay the host's.
    let cfg = handle.effective_config();
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
    if cfg.bundles.enabled && state.cfg.has_role(walgit_config::Role::Bundle) {
        let ctx = plan_context(state, &handle);
        let span = tracing::info_span!("bundle.plan", repo = %id, slots = tracing::field::Empty, missing = tracing::field::Empty, too_small = tracing::field::Empty, wrong_host = tracing::field::Empty, unavailable = tracing::field::Empty, built = tracing::field::Empty);
        // Settle closed slots first (refs-level, no unit): the plan below then
        // shows them `skipped` and the first missing slot is real work.
        // Retention is a pure function of (config, list): apply it every pass so a list that
        // grew under an older rule — or an idle repository that publishes nothing — shrinks too.
        match state.bundles.apply_retention(id).await {
            Ok(n) if n > 0 => tracing::info!(repo = %id, pruned = n, "bundle retention applied"),
            Ok(_) => {}
            Err(e) => {
                tracing::warn!(repo = %id, error = %e, "bundle retention failed; the next publish applies it");
            }
        }
        match state
            .bundles
            .settle_closed_slots(id, SystemTime::now())
            .await
        {
            Ok(n) if n > 0 => {
                tracing::info!(repo = %id, settled = n, "closed bundle slots settled");
            }
            Ok(_) => {}
            Err(e) => {
                tracing::warn!(repo = %id, error = %e, "settling closed slots failed; units will measure them");
            }
        }
        let rows = state
            .bundles
            .plan(id, SystemTime::now(), ctx)
            .instrument(span.clone())
            .await?;
        record_plan(&span, id, &cfg.bundles, &rows);
        // Strategies in config order = priority (weekly, daily, hourly); slots
        // oldest first inside a strategy.
        for strat in &cfg.bundles.strategy {
            if let Some(r) = rows
                .iter()
                .filter(|r| r.strategy == strat.name && r.status == SlotStatus::Missing)
                .min_by_key(|r| r.slot)
            {
                // The weekly slot is when the base is rebuilt (AGENTS §2.5): so the full bundle
                // carries the week and the chain below it restarts small. On any host whose
                // declared capacity holds the whole pack set (an SSD host for a large repository; a tmpfs host
                // for the long tail), when: there is no bitmap'd base yet (a repo that only ever
                // saw pushes and folds — Health used to ask a human to run `compact base=1`),
                // several tier-2 packs exist (an imported set; the compose needs exactly one), or
                // the base predates the slot's window (pushes landed since it).
                // A base rebuild is compaction: `[compaction] enabled = false` (a large repository, 2026-08-22) stops it too.
                if strat.kind == walgit_config::BundleKind::Full
                    && cfg.compaction.enabled
                    && state.cfg.has_role(walgit_config::Role::Compact)
                    && handle.packs_fit()
                {
                    let m = handle.manifest();
                    let live_bytes: u64 = m.packs.iter().map(|p| p.pack_size + p.idx_size).sum();
                    let cap = state.cfg.maintenance.max_pack_bytes.as_u64();
                    let ssd = state.cfg.maintenance.disk == walgit_config::MaintainerDisk::Ssd;
                    let cap = if cap == 0 {
                        state.cfg.cache_budget_bytes()
                    } else {
                        cap
                    };
                    let holds = ssd || cap == 0 || live_bytes <= cap;
                    let bases = walgit_wal::base_packs(&m);
                    let due = match walgit_wal::base_pack(&m).cloned() {
                        None => !m.packs.is_empty(),
                        Some(base) => {
                            bases.len() > 1
                                || !base.has_bitmap
                                || base_predates_window(&handle, strat, r.slot, base.seq).await
                        }
                    };
                    if holds && due {
                        return Ok(Unit::BaseRebuild(strat.name.clone(), r.slot));
                    }
                }
                return Ok(Unit::BundleSlot(strat.name.clone(), r.slot));
            }
        }
    }
    if cfg.compaction.enabled
        && state.cfg.has_role(walgit_config::Role::Compact)
        && handle.packs_fit()
        && crate::ops::compaction_triggered(&handle, &cfg)
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

/// Stale bundle slots skipped through within one pass before the loop yields.
const SKIP_THROUGH_MAX: u32 = 48;

/// Whether the tier-2 base at `base_seq` was built before the window the full
/// slot `slot` covers: the window starts at the previous slot (or the
/// repository's first state); the WAL seq as of that instant is the bar.
/// `base_seq ≤ bar` ⇒ rebuild. Pushes that land *during* a rebuild do not
/// re-trigger one (the new base's seq is above the bar); next week's slot does.
async fn base_predates_window(
    handle: &walgit_wal::RepoHandle,
    strat: &walgit_config::BundleStrategy,
    slot: u64,
    base_seq: u64,
) -> bool {
    let prev = walgit_bundle::slots::last_slot_at_or_before(
        strat,
        walgit_bundle::slots::from_epoch(slot.saturating_sub(1)),
    )
    .ok()
    .flatten();
    let mut window_start = prev.map(walgit_bundle::slots::from_epoch);
    if let Some(first) = handle.first_state_time() {
        window_start = Some(window_start.map_or(first, |w| w.max(first)));
    }
    let Some(ws) = window_start else { return false };
    match handle.refs_as_of(ws).await {
        Ok((_, bar)) => base_seq <= bar.max(1),
        Err(_) => false,
    }
}

/// What the next slot of each strategy will run — so Sunday's base rebuild is
/// visible in the plan before it happens. Host = the live maintainer that can
/// run it (ssd for a base rebuild). Wall-time estimate for a rebuild scales
/// the 2026-08-21 prod rebuild (32.4 GB base → 31 min end to end incl. the
/// 10-min history pack ≈ 1 min/GB).
#[derive(Debug, Clone, serde::Serialize)]
pub struct Upcoming {
    pub strategy: String,
    pub kind: String,
    /// Next slot epoch (after now).
    pub slot: u64,
    /// e.g. `base rebuild (~25 min on the ssd host) + compose`
    pub unit: String,
    pub host: Option<String>,
}

pub async fn upcoming(
    handle: &walgit_wal::RepoHandle,
    cfg: &walgit_config::Config,
    heartbeats: &[walgit_proto::v1::MaintainerHeartbeat],
    now: SystemTime,
) -> Vec<Upcoming> {
    let id = handle.id();
    let live: Vec<&walgit_proto::v1::MaintainerHeartbeat> = heartbeats
        .iter()
        .filter(|h| {
            walgit_config::repo_listed(&h.repos, id.owner(), id.name())
                && !walgit_config::repo_listed(&h.exclude, id.owner(), id.name())
        })
        .filter(|h| {
            h.last_pass_at
                .as_ref()
                .map(walgit_proto::time::to_system)
                .and_then(|t| now.duration_since(t).ok())
                .is_some_and(|d| d.as_secs() < 600)
        })
        .collect();
    let ssd = live
        .iter()
        .find(|h| h.disk == "ssd")
        .map(|h| h.host.clone());
    let any = live.first().map(|h| h.host.clone());
    let m = handle.manifest();
    let many = walgit_wal::base_packs(&m).len() > 1;
    let base = walgit_wal::base_pack(&m).cloned();
    let mut out = Vec::new();
    for strat in &cfg.bundles.strategy {
        let Ok(sched) = walgit_bundle::schedule::parse_schedule(&strat.schedule) else {
            continue;
        };
        let Some(next) = walgit_bundle::schedule::next_fire_after(&sched, now) else {
            continue;
        };
        let slot = next
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_or(0, |d| d.as_secs());
        let (unit, host) = match strat.kind {
            walgit_config::BundleKind::Full => match &base {
                Some(b) if many || base_predates_window(handle, strat, slot, b.seq).await => {
                    let gib = b.pack_size as f64 / (1u64 << 30) as f64;
                    let mins = (gib * 1.0).max(1.0).round() as u64;
                    match &ssd {
                        Some(h) => (format!("base rebuild (repack {gib:.1} GiB, ~{mins} min on {h}) + compose"), Some(h.clone())),
                        None => ("base rebuild needed — NO ssd maintainer alive; the slot would compose the stale base".to_string(), None),
                    }
                }
                Some(b) => (
                    format!(
                        "compose header ∘ base pack-{} (no push since it)",
                        &b.checksum[..12]
                    ),
                    any.clone(),
                ),
                None => (
                    "full bundle (pack-objects of the history)".to_string(),
                    any.clone(),
                ),
            },
            walgit_config::BundleKind::Incremental => (
                if strat.chain {
                    format!(
                        "incremental on the previous {} (chained; on {}'s newest after a new one) (unless unchanged / under min_commits)",
                        strat.name,
                        strat.base.as_deref().unwrap_or("?")
                    )
                } else {
                    format!(
                        "incremental on {}'s newest (unless unchanged / under min_commits)",
                        strat.base.as_deref().unwrap_or("?")
                    )
                },
                any.clone(),
            ),
        };
        out.push(Upcoming {
            strategy: strat.name.clone(),
            kind: format!("{:?}", strat.kind).to_lowercase(),
            slot,
            unit,
            host,
        });
    }
    out
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
        // One bounded unit of real work per repository per pass. A bundle slot
        // that turns out to have nothing to cut (too small, no state as of the
        // slot) is not work: re-plan at once instead of spending a whole pass
        // per stale slot (a large repository after the 08-21 restart: ~30 such slots stood
        // between the loop and the 05:00 hourly).
        let mut skipped_slots = 0u32;
        loop {
            let before_bundles = report.bundles;
            let unit = match next_unit(state, &id).await {
                Ok(u) => u,
                Err(e) => {
                    warn!(repo = %id, error = %e, "maintenance: planning failed");
                    report.skipped += 1;
                    break;
                }
            };
            if matches!(unit, Unit::Idle | Unit::NotAssigned) {
                break;
            }
            let (kind, strategy, slot) = match &unit {
                Unit::Checkpoint(_) => ("checkpoint", None, None),
                Unit::BundleSlot(s, slot) => ("bundle", Some(s.clone()), Some(*slot)),
                Unit::Compact => ("compact", None, None),
                Unit::Repair(_) => ("repair", None, None),
                Unit::BaseRebuild(s, slot) => ("base-rebuild", Some(s.clone()), Some(*slot)),
                Unit::RevIndex(_) => ("rev-index", None, None),
                Unit::Fsck(_) => ("fsck", None, None),
                Unit::Idle | Unit::NotAssigned => unreachable!(),
            };
            let unit_span = tracing::info_span!("maintain.unit", repo = %id, kind, strategy = strategy.as_deref().unwrap_or(""), slot = slot.unwrap_or(0), outcome = tracing::field::Empty);
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
                    Unit::BundleSlot(strategy, slot) => {
                        let mut params = HashMap::new();
                        params.insert("strategy".to_string(), strategy.clone());
                        params.insert("slot".to_string(), slot.to_string());
                        let value = run_op_value(state, &id, "bundle", params).await;
                        // `bundles` counts bundles that exist now, not slots visited
                        // (a visited slot with nothing to cut is re-planned at once).
                        if value
                            .as_ref()
                            .and_then(|v| v.get("built"))
                            .and_then(serde_json::Value::as_bool)
                            .unwrap_or(false)
                        {
                            report.bundles += 1;
                        }
                        value.is_some()
                    }
                    Unit::Compact => {
                        let ok = run_op(state, &id, "compact", HashMap::new()).await;
                        if ok {
                            report.compactions += 1;
                        }
                        ok
                    }
                    Unit::Repair(_) => run_op(state, &id, "repair", HashMap::new()).await,
                    Unit::BaseRebuild(..) => {
                        let mut params = HashMap::new();
                        params.insert("base".to_string(), "1".to_string());
                        params.insert("force".to_string(), "1".to_string());
                        let ok = run_op(state, &id, "compact", params).await;
                        if ok {
                            report.compactions += 1;
                        }
                        ok
                    }
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
            } else {
                report.skipped += 1;
            }
            let was_bundle = matches!(unit, Unit::BundleSlot(..));
            if done
                && was_bundle
                && report.bundles == before_bundles
                && skipped_slots < SKIP_THROUGH_MAX
            {
                skipped_slots += 1;
                continue;
            }
            break;
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
