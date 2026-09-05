#![allow(clippy::doc_lazy_continuation)]
//! Calendar-slot scheduling with backfill.
//!
//! A strategy's cron expression defines **slots** (its fire times). Each
//! slot gets exactly one bundle whose content is the ref state *as of the
//! slot time* (the WAL seq whose entries were created at or before it) and
//! whose `creation_token` is the slot's epoch seconds — deterministic, so
//! backfilled slots sort correctly even when built out of order, and git's
//! `creationToken` heuristic downloads exactly the bundles newer than what it
//! has. A maintainer pass computes every slot from the chain's anchor to now
//! and builds the **missing** ones oldest first (holes from downtime get
//! filled); `backfill_max` bounds one pass.
//!
//! Chain: an incremental slot's prerequisite is the newest bundle of its base
//! strategy with slot ≤ this slot (a daily after a new weekly restarts from
//! it). Retention is a contiguous chain (see [`retain`]).

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use walgit_config::{BundleKind, BundleStrategy, BundlesConfig};
use walgit_proto::v1::{BundleEntry, BundleList};

use crate::BundleError;
use crate::schedule::parse_schedule;

/// Why a slot is not (to be) built.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SlotStatus {
    /// A bundle exists for this slot.
    Built { id: String, size: u64, seq: u64 },
    /// No bundle yet: the next pass builds it (oldest first).
    Missing,
    /// The slot fired less than `SLOT_CLOSE_GRACE` ago: a writer's entry with `created_at ≤ slot`
    /// may still land, so it is not work yet — the pass after the grace builds or settles it.
    /// (Without this every pass ran a unit for the newest slot that found nothing to build, one
    /// `nothing to build` row per minute on the rig, and starved fsck/compaction.)
    Pending,
    /// Not buildable (yet): e.g. an incremental slot with no base bundle ≤ slot.
    Blocked(String),
    /// Measured under the minimum-size gate (`bundles.min_commits`) by this
    /// maintainer: not cut; the next slot of the strategy covers it.
    TooSmall { commits: u64, min: u64 },
    /// No WAL state exists at or before this slot (the repository was
    /// imported later): nothing exists to cut yet, not an error.
    Unavailable,
    /// Measured on a closed slot and recorded in the list (`BundleList.skipped`):
    /// final for this base — nobody re-measures it.
    Skipped { reason: String },
    /// Buildable, but not on this host: the unit needs a local copy bigger
    /// than the maintainer's declared capacity (or a base repack on tmpfs).
    /// Another maintainer (the big host) owns it.
    WrongHost(String),
}

/// One row of a repository's slot table.
#[derive(Debug, Clone)]
pub struct SlotPlan {
    pub strategy: String,
    pub kind: BundleKind,
    /// Slot time (= `creation_token`).
    pub slot: u64,
    pub status: SlotStatus,
    /// For incrementals: the base bundle id this slot chains to (if any).
    pub base_id: Option<String>,
}

pub fn epoch(t: SystemTime) -> u64 {
    t.duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs())
}
pub fn from_epoch(s: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(s)
}

/// Slot times of `strategy` in `(after, upto]` (fire times of its cron).
pub fn slots_between(
    strategy: &BundleStrategy,
    after: SystemTime,
    upto: SystemTime,
) -> Result<Vec<u64>, BundleError> {
    let sched = parse_schedule(&strategy.schedule)?;
    let mut out = Vec::new();
    let mut cursor = after;
    // Hard cap so a misconfigured schedule cannot loop forever.
    for _ in 0..20_000 {
        let Some(next) = crate::schedule::next_fire_after(&sched, cursor) else {
            break;
        };
        if next > upto {
            break;
        }
        out.push(epoch(next));
        cursor = next;
    }
    Ok(out)
}

/// Whether `slot` is closed at `now`: its as-of instant lies at least
/// [`SLOT_CLOSE_GRACE`] in the past. Entries are stamped `created_at` at publish
/// time, monotonic, so nothing with `created_at ≤ slot` can arrive afterwards
/// (explicit `created_at` replays excepted — they must clear `skipped`). The
/// grace absorbs clock skew between writers. (An earlier rule, "the strategy's
/// next fire has passed", left a daily re-measured every pass for 24 h.)
pub fn slot_closed(_strategy: &BundleStrategy, slot: u64, now: SystemTime) -> bool {
    from_epoch(slot) + SLOT_CLOSE_GRACE <= now
}

/// Clock-skew margin before a slot's verdict is treated as final.
pub const SLOT_CLOSE_GRACE: Duration = Duration::from_mins(2);

/// The newest slot of `strategy` at or before `t` (its most recent fire ≤ t).
pub fn last_slot_at_or_before(
    strategy: &BundleStrategy,
    t: SystemTime,
) -> Result<Option<u64>, BundleError> {
    // Walk back through growing windows ending at `t`, smallest first: the first window with a
    // fire holds the answer. (One 400-day window under the iteration cap of `slots_between`
    // stopped days short of `t` for minute-scale schedules — the rig's "weekly" got a token
    // 5.7 days old, 2026-08-22.)
    for days in [1u64, 8, 40, 400] {
        let start = t
            .checked_sub(Duration::from_secs(days * 86_400))
            .unwrap_or(UNIX_EPOCH);
        if let Some(last) = slots_between(strategy, start, t)?.last() {
            return Ok(Some(*last));
        }
    }
    Ok(None)
}

fn entries_of<'a>(list: &'a BundleList, strategy: &str) -> Vec<&'a BundleEntry> {
    let mut v: Vec<&BundleEntry> = list
        .bundles
        .iter()
        .filter(|b| b.strategy == strategy)
        .collect();
    v.sort_by_key(|b| b.creation_token);
    v
}

/// Newest bundle of `strategy` with token ≤ `slot`.
pub fn base_for_slot<'a>(
    list: &'a BundleList,
    strategy: &str,
    slot: u64,
) -> Option<&'a BundleEntry> {
    entries_of(list, strategy)
        .into_iter()
        .rfind(|b| b.creation_token <= slot)
}

/// The base bundle of an incremental at `slot`, **up the chain**: the newest
/// bundle of `base` at or before the slot, else of the base's base, … up to
/// the chain's full root. A repository's first day has hourlies before its
/// first daily exists; they are cut on the weekly (prerequisites = the
/// weekly's tips, always satisfiable by a client that has the weekly) instead
/// of being blocked — or, worse, of a "daily" being cut at the hourly's slot.
/// (2026-08-21: a large repository's hourlies only existed because a header-only legacy
/// daily happened to be in the list.)
pub fn base_for_slot_chain<'a>(
    cfg: &BundlesConfig,
    list: &'a BundleList,
    base: &str,
    slot: u64,
) -> Option<&'a BundleEntry> {
    let mut name = base;
    loop {
        if let Some(b) = base_for_slot(list, name, slot) {
            return Some(b);
        }
        name = cfg
            .strategy
            .iter()
            .find(|s| s.name == name)?
            .base
            .as_deref()?;
    }
}

/// The base of an incremental of `strat` at `slot` — **the one rule** for both topologies:
/// * `chain = false` (D21): the newest base bundle at or before the slot, up the chain
///   (`base_for_slot_chain`).
/// * `chain = true`: this strategy's own newest bundle before the slot, **if it is newer than
///   that base** (dailies chain from the weekly onwards; hourlies restart from every new daily
///   instead of chaining across it); else the base.
/// `slot = 0` (a manual cut, "now"): the same with the newest bundles overall.
pub fn base_for_incremental<'a>(
    cfg: &BundlesConfig,
    list: &'a BundleList,
    strat: &BundleStrategy,
    slot: u64,
) -> Option<&'a BundleEntry> {
    let base_name = strat.base.as_deref()?;
    let at = if slot == 0 { u64::MAX } else { slot };
    let base = if slot == 0 {
        chain_up(cfg, base_name)
            .into_iter()
            .find_map(|n| entries_of(list, n).last().copied())
    } else {
        base_for_slot_chain(cfg, list, base_name, slot)
    };
    if !strat.chain {
        return base;
    }
    let own = entries_of(list, &strat.name)
        .into_iter()
        .rfind(|b| b.creation_token < at);
    // `>=`: at a tie (Sunday's daily and the weekly fire at the same instant, so their tips are the
    // same objects) the chain continues through its own link. A fresh clone has the weekly's objects
    // and therefore that link's prerequisites; a stale client walks daily → daily straight across
    // the week boundary without ever needing the new full (git would otherwise download it: the
    // creationToken walk passes every newer full, and a full has no prerequisites — rig, 2026-08-22).
    match (own, base) {
        (Some(o), Some(b)) if o.creation_token >= b.creation_token => Some(o),
        (Some(o), None) => Some(o),
        (_, base) => base,
    }
}

/// The chain of strategy names from `base` up to the full root (inclusive).
pub fn chain_up_names<'a>(cfg: &'a BundlesConfig, base: &'a str) -> Vec<&'a str> {
    chain_up(cfg, base)
}

fn chain_up<'a>(cfg: &'a BundlesConfig, base: &'a str) -> Vec<&'a str> {
    let mut out = vec![base];
    let mut name = base;
    while let Some(next) = cfg
        .strategy
        .iter()
        .find(|s| s.name == name)
        .and_then(|s| s.base.as_deref())
    {
        out.push(next);
        name = next;
    }
    out
}

/// The slot table of a repository at `now`: for every strategy, the slots
/// from the chain anchor to now and whether each is built / missing /
/// blocked. The anchor of a full strategy is its newest built slot (nothing
/// older is rebuilt) or, when none exists, the most recent slot ≤ now (a fresh
/// repo gets exactly one full bundle, not a year of history). The anchor of an
/// incremental strategy is the slot of the newest base bundle (a chain starts
/// at its base).
/// What the planner knows about the repository and this host.
#[derive(Debug, Clone, Copy, Default)]
pub struct PlanContext {
    /// Earliest WAL state (`created_at` of the first entry / checkpoint); slots
    /// before it are `Unavailable`. None = unknown → never unavailable.
    pub first_state: Option<SystemTime>,
    /// Whether this host can cut a **full** bundle for the repo (a compose of
    /// an existing base always can; a repack needs the local copy + ssd).
    pub can_full: bool,
    /// Whether this host can cut **incremental** bundles (needs a serving copy
    /// that fits: history/small packs local, base linked/remote is fine).
    pub can_incremental: bool,
    /// Human reason when a kind cannot run here (→ `WrongHost`).
    pub wrong_host_reason: Option<&'static str>,
}

pub fn plan(
    cfg: &BundlesConfig,
    list: &BundleList,
    now: SystemTime,
    can_build_full: bool,
) -> Result<Vec<SlotPlan>, BundleError> {
    plan_with(
        cfg,
        list,
        now,
        PlanContext {
            first_state: None,
            can_full: can_build_full,
            can_incremental: true,
            wrong_host_reason: None,
        },
    )
}

pub fn plan_with(
    cfg: &BundlesConfig,
    list: &BundleList,
    now: SystemTime,
    ctx: PlanContext,
) -> Result<Vec<SlotPlan>, BundleError> {
    let can_build_full = ctx.can_full;
    let mut rows = Vec::new();
    for strat in &cfg.strategy {
        let built = entries_of(list, &strat.name);
        let (anchor_excl, ()): (SystemTime, ()) = match strat.kind {
            BundleKind::Full => match built.last() {
                // Newest built full: everything after it is a candidate.
                Some(b) => (from_epoch(b.creation_token), ()),
                None => {
                    // No full yet: only the latest slot ≤ now.
                    match last_slot_at_or_before(strat, now)? {
                        Some(s) => (from_epoch(s - 1), ()),
                        None => continue,
                    }
                }
            },
            BundleKind::Incremental => {
                let base_name = strat.base.as_deref().unwrap_or("");
                // The newest bundle of the nearest strategy up the chain that has one
                // (daily, else weekly): the anchor of this chain.
                let anchored = chain_up(cfg, base_name)
                    .into_iter()
                    .find_map(|n| entries_of(list, n).last().map(|b| (n, *b)));
                match anchored {
                    // Chained: every slot after the newest base is a delta of its own; nothing
                    // before the base is ever wanted.
                    Some((_, b)) if strat.chain => (from_epoch(b.creation_token), ()),
                    Some((anchor_name, b)) => {
                        // Start at the newest base; but keep older built
                        // incrementals visible via their own anchor: a chain
                        // on the previous base still serves clients there.
                        let oldest_relevant = entries_of(list, anchor_name)
                            .iter()
                            .rev()
                            .nth(1)
                            .map_or(b.creation_token, |prev| prev.creation_token);
                        (from_epoch(oldest_relevant), ())
                    }
                    _ => {
                        rows.push(SlotPlan {
                            strategy: strat.name.clone(),
                            kind: strat.kind,
                            slot: 0,
                            status: SlotStatus::Blocked(format!("no {base_name} bundle yet")),
                            base_id: None,
                        });
                        continue;
                    }
                }
            }
        };
        let slots = slots_between(strat, anchor_excl, now)?;
        // Incrementals: only the newest `INCREMENTALS_KEPT` slots are desired (retention
        // keeps no more; an older slot's content is subsumed by the newer one on the same
        // base). Built older entries stay visible until retention drops them; missing older
        // slots are not work.
        let wanted_from = match strat.kind {
            BundleKind::Incremental if !strat.chain => {
                slots.len().saturating_sub(INCREMENTALS_KEPT)
            }
            // Chained: every slot since the base — bounded to one period of the base strategy
            // (24 hourlies under a daily, 7 dailies under a weekly): a chain under a base that is
            // months old (a stale list, an outage) is not months of work, and anything older than
            // the period would be pruned by retention as soon as the next base is cut.
            BundleKind::Incremental => slots.len().saturating_sub(chain_window(cfg, strat).max(1)),
            BundleKind::Full => 0,
        };
        for (i, slot) in slots.into_iter().enumerate() {
            let existing = built
                .iter()
                .find(|b| b.creation_token == slot || b.slot == slot);
            if existing.is_none() && i < wanted_from {
                continue;
            }
            let base_id = match strat.kind {
                BundleKind::Full => None,
                BundleKind::Incremental => {
                    base_for_incremental(cfg, list, strat, slot).map(|b| b.id.clone())
                }
            };
            // The first full of a chain is never unavailable: a repository that
            // appeared after the slot gets its first full bundle cut from its
            // earliest state (for a large repository, the import) — that is what "weekly =
            // import state" means; later slots are as-of by construction.
            let first_full = strat.kind == BundleKind::Full && built.is_empty();
            let unavailable = !first_full && ctx.first_state.is_some_and(|t| from_epoch(slot) < t);
            let skipped = list.skipped.iter().find(|k| {
                k.strategy == strat.name
                    && k.slot == slot
                    && k.base_id == base_id.clone().unwrap_or_default()
            });
            let status = match existing {
                Some(b) => SlotStatus::Built {
                    id: b.id.clone(),
                    size: b.size,
                    seq: b.seq,
                },
                None if unavailable => SlotStatus::Unavailable,
                None if skipped.is_some() => SlotStatus::Skipped {
                    reason: skipped.map(|k| k.reason.clone()).unwrap_or_default(),
                },
                None if strat.kind == BundleKind::Incremental && !slot_closed(strat, slot, now) => {
                    SlotStatus::Pending
                }
                None => match strat.kind {
                    BundleKind::Full if !can_build_full => SlotStatus::WrongHost(
                        ctx.wrong_host_reason
                            .unwrap_or("full bundles need the base pack locally (ssd host)")
                            .into(),
                    ),
                    BundleKind::Incremental if base_id.is_none() => {
                        SlotStatus::Blocked("no base bundle at or before this slot".into())
                    }
                    BundleKind::Incremental if !ctx.can_incremental => SlotStatus::WrongHost(
                        ctx.wrong_host_reason
                            .unwrap_or("the serving copy does not fit this host")
                            .into(),
                    ),
                    BundleKind::Full | BundleKind::Incremental => SlotStatus::Missing,
                },
            };
            rows.push(SlotPlan {
                strategy: strat.name.clone(),
                kind: strat.kind,
                slot,
                status,
                base_id,
            });
        }
    }
    rows.sort_by_key(|r| (r.slot, r.kind == BundleKind::Incremental));
    Ok(rows)
}

/// How many slots of a chained strategy fit in one period of its base strategy (24 hourlies per
/// daily, 7 dailies per weekly): the most a chain under one base can ever need.
pub fn chain_window(cfg: &BundlesConfig, strat: &BundleStrategy) -> usize {
    let Some(base) = strat
        .base
        .as_deref()
        .and_then(|n| cfg.strategy.iter().find(|s| s.name == n))
    else {
        return usize::MAX;
    };
    let (Ok(bs), Ok(_)) = (
        parse_schedule(&base.schedule),
        parse_schedule(&strat.schedule),
    ) else {
        return usize::MAX;
    };
    // Two consecutive base fires from an arbitrary anchor; the count of our fires between them.
    let anchor = from_epoch(1_700_000_000);
    let Some(b1) = crate::schedule::next_fire_after(&bs, anchor) else {
        return usize::MAX;
    };
    let Some(b2) = crate::schedule::next_fire_after(&bs, b1) else {
        return usize::MAX;
    };
    slots_between(strat, b1, b2).map_or(usize::MAX, |v| v.len())
}

/// How many bundles of an incremental strategy stay listed: the newest, and the one
/// before it. Incrementals are built on their *base* (a daily on the weekly, an hourly
/// on the newest daily), never on their own predecessor, so the newest one subsumes every
/// older one on the same base — and git's `creationToken` heuristic downloads **every**
/// listed bundle newer than the newest full on a clone (descending until a bundle whose
/// prerequisites it has), so anything older than the newest is pure cost: 43 downloads /
/// 11.5 s for a 1 MB repository on 2026-08-22. The second one stays so a client that read
/// the list a slot ago still finds its bundle (git never retries a failed download, D17).
pub const INCREMENTALS_KEPT: usize = 2;

/// Retention (D21 amended 2026-08-22): keep the newest `keep` fulls, and per incremental
/// strategy the `INCREMENTALS_KEPT` newest bundles whose base is kept — never an orphan.
/// Returns the object keys to delete. Fresh clone = 1 full + ≤ 2 dailies + ≤ 2 hourlies;
/// catch-up ≤ 2 bundles.
pub fn retain(cfg: &BundlesConfig, list: &mut BundleList) -> Vec<String> {
    use std::collections::HashSet;
    let mut keep: HashSet<String> = HashSet::new();
    // Fulls: newest `keep`.
    for strat in cfg.strategy.iter().filter(|s| s.kind == BundleKind::Full) {
        let mut v = entries_of(list, &strat.name);
        v.reverse();
        for b in v.into_iter().take(strat.keep.max(1)) {
            keep.insert(b.id.clone());
        }
    }
    // Incrementals, in dependency order (strategies listed base-first), **per kept full**: the
    // week under each kept weekly is a group — tokens in (this full, next full] — and the rule
    // applies inside every group, so `keep = 2` on the weekly is two weeks of catch-up through
    // bundles (a client stale by less than that always meets a link whose prerequisites it has).
    // In a group: D21 (unchained) = the newest `INCREMENTALS_KEPT` whose base survived; chained =
    // every link newer than the newest kept bundle of the base strategy in the group whose own base
    // survived (≤ 7 dailies under a weekly, ≤ 24 hourlies under a daily).
    let mut full_tokens: Vec<u64> = list
        .bundles
        .iter()
        .filter(|b| keep.contains(&b.id) && b.base_id.is_empty())
        .map(|b| b.creation_token)
        .collect();
    full_tokens.sort_unstable();
    full_tokens.dedup();
    // Group i = the kept full F_i and the incrementals with tokens in (F_i, F_{i+1}]: a bundle cut at
    // a full's own instant belongs to the previous week (Sunday's daily under the old weekly — its
    // tips equal the new weekly's). Incrementals before the first kept full fall into group 0 and
    // are pruned there (their base is gone).
    let group_of = |b: &BundleEntry| -> usize {
        let below = full_tokens
            .iter()
            .take_while(|t| **t < b.creation_token)
            .count();
        if b.base_id.is_empty() {
            below
        } else {
            below.saturating_sub(1)
        }
    };
    for strat in cfg
        .strategy
        .iter()
        .filter(|s| s.kind == BundleKind::Incremental)
    {
        let v = entries_of(list, &strat.name);
        let groups = full_tokens.len() + 1;
        for g in 0..groups {
            let in_group: Vec<&BundleEntry> =
                v.iter().copied().filter(|b| group_of(b) == g).collect();
            if strat.chain {
                let base_newest = strat.base.as_deref().map_or(0, |n| {
                    entries_of(list, n)
                        .into_iter()
                        .filter(|b| keep.contains(&b.id) && group_of(b) == g)
                        .map(|b| b.creation_token)
                        .max()
                        .unwrap_or(0)
                });
                // Oldest first so a link's base (the previous link) is decided before it. The first
                // link of a group may point at a pruned link of the previous group (Monday on Sunday's
                // daily): its prerequisites are the group's full's tips, so it stays while the full does.
                let group_full_kept = full_tokens.get(g).is_some();
                let mut first = true;
                for b in in_group {
                    if b.creation_token <= base_newest {
                        continue;
                    }
                    let anchored = b.base_id.is_empty()
                        || keep.contains(&b.base_id)
                        || (first && group_full_kept);
                    first = false;
                    if anchored {
                        keep.insert(b.id.clone());
                    }
                }
            } else {
                let chosen: Vec<String> = in_group
                    .into_iter()
                    .rev()
                    .filter(|b| b.base_id.is_empty() || keep.contains(&b.base_id))
                    .take(INCREMENTALS_KEPT)
                    .map(|b| b.id.clone())
                    .collect();
                keep.extend(chosen);
            }
        }
    }
    let mut pruned = Vec::new();
    let mut kept = Vec::new();
    for b in list.bundles.drain(..) {
        if keep.contains(&b.id) {
            kept.push(b);
        } else {
            pruned.push(b.key.clone());
        }
    }
    list.bundles = kept;
    pruned
}

/// Default ref set for a bundle (see `bundles.main_only` / `extra_refs`).
pub fn default_refs(cfg: &BundlesConfig, strategy: &BundleStrategy) -> Vec<String> {
    if !strategy.refs.is_empty() {
        return strategy.refs.clone();
    }
    let mut v: Vec<String> = if cfg.main_only {
        vec!["HEAD".into(), "refs/heads/main".into()]
    } else {
        vec!["refs/heads/*".into(), "refs/tags/*".into(), "HEAD".into()]
    };
    v.extend(cfg.extra_refs.iter().cloned());
    v
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The D21 shape (every incremental on its base): what the two-newest tests below pin.
    fn cfg() -> BundlesConfig {
        let mut c = BundlesConfig::default();
        for s in &mut c.strategy {
            s.chain = false;
        }
        c
    }

    /// The default shape: weekly full, chained dailies, hourlies on the newest daily.
    #[test]
    fn default_shape_chains_the_dailies_only() {
        let c = BundlesConfig::default();
        let by: std::collections::HashMap<&str, &BundleStrategy> =
            c.strategy.iter().map(|s| (s.name.as_str(), s)).collect();
        assert!(!by["weekly"].chain && by["daily"].chain && !by["hourly"].chain);
        assert_eq!(chain_window(&c, by["daily"]), 7);
    }
    fn t(s: &str) -> SystemTime {
        let dt = chrono::DateTime::parse_from_rfc3339(s).unwrap();
        from_epoch(dt.timestamp().max(0).unsigned_abs())
    }
    fn entry(strategy: &str, slot: u64, base_id: &str) -> BundleEntry {
        BundleEntry {
            id: format!("{strategy}-{slot}"),
            key: format!("bundles/{strategy}/{slot}.bundle"),
            strategy: strategy.into(),
            kind: if strategy == "weekly" {
                "full"
            } else {
                "incremental"
            }
            .into(),
            filter: String::new(),
            creation_token: slot,
            slot,
            seq: 1,
            size: 1,
            base_id: base_id.into(),
            created_at: None,
            version: String::new(),
            tips: vec![],
        }
    }

    #[test]
    fn default_schedules_fire_on_the_calendar() {
        let c = cfg();
        let weekly = &c.strategy[0];
        // Sunday 2026-08-23 23:00Z is a slot; the week before too.
        let s =
            slots_between(weekly, t("2026-08-16T22:59:00Z"), t("2026-08-23T23:00:00Z")).unwrap();
        assert_eq!(
            s,
            vec![
                epoch(t("2026-08-16T23:00:00Z")),
                epoch(t("2026-08-23T23:00:00Z"))
            ]
        );
        let daily = &c.strategy[1];
        assert_eq!(
            slots_between(daily, t("2026-08-20T00:00:00Z"), t("2026-08-21T23:30:00Z"))
                .unwrap()
                .len(),
            2
        );
        let hourly = &c.strategy[2];
        assert_eq!(
            slots_between(hourly, t("2026-08-20T10:30:00Z"), t("2026-08-20T13:00:00Z"))
                .unwrap()
                .len(),
            3
        );
    }

    /// Minute-scale schedules (a test rig) must resolve their newest slot exactly, not a capped
    /// walk from a year ago.
    #[test]
    fn last_slot_is_exact_for_minute_scale_schedules() {
        let mut strat = cfg().strategy[0].clone();
        strat.schedule = "0 */10 * * * *".into();
        let now = t("2026-08-22T16:22:45Z");
        assert_eq!(
            last_slot_at_or_before(&strat, now).unwrap(),
            Some(epoch(t("2026-08-22T16:20:00Z")))
        );
        strat.schedule = "0 * * * * *".into();
        assert_eq!(
            last_slot_at_or_before(&strat, now).unwrap(),
            Some(epoch(t("2026-08-22T16:22:00Z")))
        );
        // And the calendar ones still do.
        let weekly = &cfg().strategy[0];
        assert_eq!(
            last_slot_at_or_before(weekly, now).unwrap(),
            Some(epoch(t("2026-08-16T23:00:00Z")))
        );
    }

    #[test]
    fn fresh_repo_plans_one_full_and_nothing_older() {
        let c = cfg();
        let list = BundleList::default();
        let now = t("2026-08-20T12:30:00Z"); // Thursday
        let rows = plan(&c, &list, now, true).unwrap();
        let fulls: Vec<&SlotPlan> = rows.iter().filter(|r| r.strategy == "weekly").collect();
        assert_eq!(fulls.len(), 1);
        assert_eq!(
            fulls[0].slot,
            epoch(t("2026-08-16T23:00:00Z")),
            "latest Sunday 23:00 ≤ now"
        );
        assert_eq!(fulls[0].status, SlotStatus::Missing);
        // Incrementals are blocked until the weekly exists.
        assert!(
            rows.iter()
                .filter(|r| r.strategy == "daily")
                .all(|r| matches!(r.status, SlotStatus::Blocked(_)))
        );
    }

    /// D21 (2026-08-22): incrementals are independent of their predecessors, so after a gap only
    /// the `INCREMENTALS_KEPT` newest slots of each strategy are work — never a backfill of
    /// slots whose content the newer one subsumes (and retention would drop at once).
    #[test]
    fn backfill_after_a_three_day_gap_is_only_the_two_newest_slots_per_strategy() {
        let c = cfg();
        let mut list = BundleList::default();
        let weekly_slot = epoch(t("2026-08-16T23:00:00Z"));
        list.bundles.push(entry("weekly", weekly_slot, ""));
        // Dailies built through Monday 17th 23:00, hourlies through Tue 18th 00:00.
        let d1 = epoch(t("2026-08-17T23:00:00Z"));
        list.bundles
            .push(entry("daily", d1, "weekly-".to_string().as_str()));
        list.bundles.last_mut().unwrap().base_id = format!("weekly-{weekly_slot}");
        let h0 = epoch(t("2026-08-18T00:00:00Z"));
        list.bundles
            .push(entry("hourly", h0, &format!("daily-{d1}")));
        // Maintainer was down; now it is Friday 21st 12:30.
        let now = t("2026-08-21T12:30:00Z");
        let rows = plan(&c, &list, now, false).unwrap();
        let missing_daily: Vec<u64> = rows
            .iter()
            .filter(|r| r.strategy == "daily" && r.status == SlotStatus::Missing)
            .map(|r| r.slot)
            .collect();
        assert_eq!(
            missing_daily,
            vec![
                epoch(t("2026-08-19T23:00:00Z")),
                epoch(t("2026-08-20T23:00:00Z"))
            ],
            "the 2 newest daily slots, oldest first; Tue 18th is not work"
        );
        // Hourlies missing: only Fri 11:00 and 12:00 (the 2 newest slots ≤ now).
        let missing_hourly: Vec<u64> = rows
            .iter()
            .filter(|r| r.strategy == "hourly" && r.status == SlotStatus::Missing)
            .map(|r| r.slot)
            .collect();
        assert_eq!(
            missing_hourly,
            vec![
                epoch(t("2026-08-21T11:00:00Z")),
                epoch(t("2026-08-21T12:00:00Z"))
            ]
        );
        // The built hourly stays visible; no row for the Tue..Thu slots nobody wants.
        assert!(rows.iter().any(|r| r.strategy == "hourly"
            && r.slot == h0
            && matches!(r.status, SlotStatus::Built { .. })));
        assert!(
            !rows
                .iter()
                .any(|r| r.strategy == "hourly" && r.slot == epoch(t("2026-08-20T12:00:00Z")))
        );
        // Each missing hourly chains to the newest *built* daily ≤ its slot (the builder cuts dailies first).
        let fri_noon = rows
            .iter()
            .find(|r| r.strategy == "hourly" && r.slot == epoch(t("2026-08-21T12:00:00Z")))
            .unwrap();
        assert_eq!(
            fri_noon.base_id.as_deref(),
            Some(format!("daily-{d1}").as_str())
        );
        // Weekly: no new full until the next Sunday 23:00 → no missing weekly (the VM builds fulls anyway).
        assert!(
            rows.iter()
                .filter(|r| r.strategy == "weekly")
                .all(|r| matches!(r.status, SlotStatus::Built { .. }))
        );
        // Tokens are the slots: monotonic in time regardless of build order.
        let mut toks: Vec<u64> = rows.iter().map(|r| r.slot).collect();
        toks.dedup();
        assert!(toks.windows(2).all(|w| w[0] <= w[1]));
    }

    fn chained() -> BundlesConfig {
        let mut c = cfg();
        c.strategy[0].keep = 1;
        c.strategy[1].chain = true;
        c.strategy[2].chain = true;
        c
    }

    /// `chain = true`: a daily's base is the previous daily once one exists after the weekly; an
    /// hourly's base is the previous hourly until a newer daily appears, then that daily.
    #[test]
    fn chained_incrementals_base_on_their_predecessor_until_a_newer_base_appears() {
        let c = chained();
        let (daily, hourly) = (&c.strategy[1], &c.strategy[2]);
        let mut list = BundleList::default();
        let w = epoch(t("2026-08-16T23:00:00Z"));
        list.bundles.push(entry("weekly", w, ""));
        let d1 = epoch(t("2026-08-17T23:00:00Z"));
        // First daily after the weekly: on the weekly.
        assert_eq!(
            base_for_incremental(&c, &list, daily, d1).unwrap().id,
            format!("weekly-{w}")
        );
        list.bundles
            .push(entry("daily", d1, &format!("weekly-{w}")));
        // Second daily: on the first.
        let d2 = epoch(t("2026-08-18T23:00:00Z"));
        assert_eq!(
            base_for_incremental(&c, &list, daily, d2).unwrap().id,
            format!("daily-{d1}")
        );
        // Hourlies after d1: the first on d1, the next on its predecessor.
        let h1 = epoch(t("2026-08-18T00:00:00Z"));
        assert_eq!(
            base_for_incremental(&c, &list, hourly, h1).unwrap().id,
            format!("daily-{d1}")
        );
        list.bundles
            .push(entry("hourly", h1, &format!("daily-{d1}")));
        let h2 = epoch(t("2026-08-18T01:00:00Z"));
        assert_eq!(
            base_for_incremental(&c, &list, hourly, h2).unwrap().id,
            format!("hourly-{h1}")
        );
        list.bundles
            .push(entry("hourly", h2, &format!("hourly-{h1}")));
        // d2 is cut; the first hourly after it restarts from d2, not from h2.
        list.bundles
            .push(entry("daily", d2, &format!("daily-{d1}")));
        let h3 = epoch(t("2026-08-19T00:00:00Z"));
        assert_eq!(
            base_for_incremental(&c, &list, hourly, h3).unwrap().id,
            format!("daily-{d2}")
        );
        // The D21 shape is untouched: without `chain` the same slot is cut on the newest daily.
        let plain = cfg();
        assert_eq!(
            base_for_incremental(&plain, &list, &plain.strategy[2], h2)
                .unwrap()
                .id,
            format!("daily-{d1}")
        );
        // A manual cut (slot 0) chains on the newest link.
        assert_eq!(
            base_for_incremental(&c, &list, hourly, 0).unwrap().id,
            format!("daily-{d2}")
        );
        assert_eq!(
            base_for_incremental(&c, &list, daily, 0).unwrap().id,
            format!("daily-{d2}")
        );
    }

    /// Chained: every slot since the newest base is wanted (each is its own delta), not just the
    /// 2 newest; and nothing before the newest base.
    #[test]
    fn chained_plan_wants_every_slot_since_the_base() {
        let c = chained();
        let mut list = BundleList::default();
        let w = epoch(t("2026-08-16T23:00:00Z"));
        list.bundles.push(entry("weekly", w, ""));
        let d1 = epoch(t("2026-08-17T23:00:00Z"));
        list.bundles
            .push(entry("daily", d1, &format!("weekly-{w}")));
        let now = t("2026-08-21T12:30:00Z");
        let rows = plan(&c, &list, now, false).unwrap();
        let missing_daily: Vec<u64> = rows
            .iter()
            .filter(|r| r.strategy == "daily" && r.status == SlotStatus::Missing)
            .map(|r| r.slot)
            .collect();
        assert_eq!(
            missing_daily,
            vec![
                epoch(t("2026-08-18T23:00:00Z")),
                epoch(t("2026-08-19T23:00:00Z")),
                epoch(t("2026-08-20T23:00:00Z"))
            ],
            "every daily after the built one, oldest first"
        );
        // Planned bases: the first missing daily on d1 (the newest link), the later ones too until
        // that link is built — the builder cuts oldest first, so each then lands on its predecessor.
        let d2 = rows
            .iter()
            .find(|r| r.strategy == "daily" && r.slot == epoch(t("2026-08-18T23:00:00Z")))
            .unwrap();
        assert_eq!(d2.base_id.as_deref(), Some(format!("daily-{d1}").as_str()));
        // Hourlies: every slot since d1 (the newest daily) is wanted, bounded to one daily period
        // (24) — the older ones would be pruned the moment the next daily is cut.
        assert_eq!(chain_window(&c, &c.strategy[2]), 24);
        assert_eq!(chain_window(&c, &c.strategy[1]), 7);
        let missing_hourly = rows
            .iter()
            .filter(|r| r.strategy == "hourly" && r.status == SlotStatus::Missing)
            .count();
        assert_eq!(missing_hourly, 24);
    }

    /// Chained retention: 1 weekly, the dailies after it, the hourlies after the newest daily.
    #[test]
    fn chained_retention_keeps_the_chain_below_the_newest_bases_only() {
        let c = chained();
        let mut list = BundleList::default();
        let w0 = epoch(t("2026-08-09T23:00:00Z"));
        let w1 = epoch(t("2026-08-16T23:00:00Z"));
        list.bundles.push(entry("weekly", w0, ""));
        list.bundles.push(entry("weekly", w1, ""));
        // A daily on the old weekly, then three on the new chain.
        let d_old = epoch(t("2026-08-15T23:00:00Z"));
        list.bundles
            .push(entry("daily", d_old, &format!("weekly-{w0}")));
        let ds: Vec<u64> = [
            "2026-08-17T23:00:00Z",
            "2026-08-18T23:00:00Z",
            "2026-08-19T23:00:00Z",
        ]
        .iter()
        .map(|x| epoch(t(x)))
        .collect();
        list.bundles
            .push(entry("daily", ds[0], &format!("weekly-{w1}")));
        list.bundles
            .push(entry("daily", ds[1], &format!("daily-{}", ds[0])));
        list.bundles
            .push(entry("daily", ds[2], &format!("daily-{}", ds[1])));
        // Hourlies: two under the second daily (obsolete), two after the newest daily.
        let h_old1 = epoch(t("2026-08-19T00:00:00Z"));
        let h_old2 = epoch(t("2026-08-19T01:00:00Z"));
        list.bundles
            .push(entry("hourly", h_old1, &format!("daily-{}", ds[1])));
        list.bundles
            .push(entry("hourly", h_old2, &format!("hourly-{h_old1}")));
        let h1 = epoch(t("2026-08-20T00:00:00Z"));
        let h2 = epoch(t("2026-08-20T01:00:00Z"));
        list.bundles
            .push(entry("hourly", h1, &format!("daily-{}", ds[2])));
        list.bundles
            .push(entry("hourly", h2, &format!("hourly-{h1}")));
        let pruned = retain(&c, &mut list);
        let mut kept: Vec<&str> = list.bundles.iter().map(|b| b.id.as_str()).collect();
        kept.sort_unstable();
        let mut want = [
            format!("weekly-{w1}"),
            format!("daily-{}", ds[0]),
            format!("daily-{}", ds[1]),
            format!("daily-{}", ds[2]),
            format!("hourly-{h1}"),
            format!("hourly-{h2}"),
        ];
        want.sort();
        assert_eq!(kept, want.iter().map(String::as_str).collect::<Vec<_>>());
        assert_eq!(
            pruned.len(),
            4,
            "old weekly, its daily, the two hourlies under an older daily"
        );
    }

    /// The chain crosses the week: Sunday's daily and the weekly share an instant (and tips); Monday's
    /// daily is cut on Sunday's daily, and with `keep = 2` the previous week's chain stays listed,
    /// so a client stale by up to two weeks catches up daily → daily without the new full.
    #[test]
    fn chained_dailies_continue_through_the_weekly_and_keep_two_weeks() {
        let mut c = chained();
        c.strategy[0].keep = 2;
        let (daily, hourly) = (c.strategy[1].clone(), c.strategy[2].clone());
        let mut list = BundleList::default();
        let w1 = epoch(t("2026-08-16T23:00:00Z"));
        list.bundles.push(entry("weekly", w1, ""));
        let mut prev = format!("weekly-{w1}");
        // Mon 17 .. Sun 23 dailies (the Sunday one at the weekly's instant).
        let mut sunday = 0;
        for d in 17..=23 {
            let slot = epoch(t(&format!("2026-08-{d}T23:00:00Z")));
            assert_eq!(
                base_for_incremental(&c, &list, &daily, slot).unwrap().id,
                prev,
                "day {d}"
            );
            list.bundles.push(entry("daily", slot, &prev));
            prev = format!("daily-{slot}");
            sunday = slot;
        }
        let w2 = epoch(t("2026-08-23T23:00:00Z"));
        assert_eq!(sunday, w2);
        list.bundles.push(entry("weekly", w2, ""));
        // Monday 24th: on Sunday's daily (tie → own chain), not on the new weekly.
        let mon = epoch(t("2026-08-24T23:00:00Z"));
        assert_eq!(
            base_for_incremental(&c, &list, &daily, mon).unwrap().id,
            format!("daily-{sunday}")
        );
        list.bundles
            .push(entry("daily", mon, &format!("daily-{sunday}")));
        // An hourly after Monday's daily hangs under it (hourlies are not chained here: D21 shape).
        let mut plain = c.clone();
        plain.strategy[2].chain = false;
        let h = epoch(t("2026-08-25T01:00:00Z"));
        assert_eq!(
            base_for_incremental(&plain, &list, &hourly, h).unwrap().id,
            format!("daily-{mon}")
        );
        list.bundles
            .push(entry("hourly", h, &format!("daily-{mon}")));
        // Retention with keep = 2: both weeklies, all 7 dailies of week 1, Monday, the hourly.
        let mut probe = list.clone();
        let pruned = retain(&c, &mut probe);
        assert!(pruned.is_empty(), "nothing to prune: {pruned:?}");
        assert_eq!(probe.bundles.len(), 2 + 7 + 1 + 1);
        // keep = 1: week 1's chain goes (including Sunday's daily, which belongs to week 1), Monday stays.
        c.strategy[0].keep = 1;
        let mut probe = list.clone();
        let pruned = retain(&c, &mut probe);
        assert_eq!(pruned.len(), 1 + 7, "{pruned:?}");
        assert!(probe.bundles.iter().any(|b| b.id == format!("daily-{mon}")));
        assert!(probe.bundles.iter().any(|b| b.id == format!("weekly-{w2}")));
    }

    #[test]
    fn slots_before_the_first_wal_state_are_unavailable_not_missing() {
        let c = cfg();
        let mut list = BundleList::default();
        let weekly_slot = epoch(t("2026-08-16T23:00:00Z"));
        list.bundles.push(entry("weekly", weekly_slot, ""));
        // Repo imported Wed 19th 21:30 for a large repository), now Fri 00:30: the two wanted daily slots are Wed/Thu, both
        // missing; Mon/Tue are neither wanted nor shown.
        let ctx = PlanContext {
            first_state: Some(t("2026-08-19T21:30:00Z")),
            can_full: true,
            can_incremental: true,
            wrong_host_reason: None,
        };
        let rows = plan_with(&c, &list, t("2026-08-21T00:30:00Z"), ctx).unwrap();
        let daily: Vec<(u64, &SlotStatus)> = rows
            .iter()
            .filter(|r| r.strategy == "daily")
            .map(|r| (r.slot, &r.status))
            .collect();
        assert_eq!(daily.len(), 2, "{daily:?}");
        assert_eq!(
            daily[0],
            (epoch(t("2026-08-19T23:00:00Z")), &SlotStatus::Missing),
            "Wed 19th 23:00"
        );
        assert_eq!(
            daily[1],
            (epoch(t("2026-08-20T23:00:00Z")), &SlotStatus::Missing),
            "Thu 20th 23:00"
        );
        // Imported Thu 22:00 instead: Wed is within the wanted window but predates the first state → unavailable.
        let ctx2 = PlanContext {
            first_state: Some(t("2026-08-20T22:00:00Z")),
            ..ctx
        };
        let rows = plan_with(&c, &list, t("2026-08-21T00:30:00Z"), ctx2).unwrap();
        let daily: Vec<(u64, &SlotStatus)> = rows
            .iter()
            .filter(|r| r.strategy == "daily")
            .map(|r| (r.slot, &r.status))
            .collect();
        assert_eq!(daily[0].1, &SlotStatus::Unavailable, "Wed 19th");
        assert_eq!(daily[1].1, &SlotStatus::Missing, "Thu 20th");
        // A small host: incrementals are wrong-host, visible as such.
        let small = PlanContext {
            can_incremental: false,
            wrong_host_reason: Some("cache too small"),
            ..ctx
        };
        let rows = plan_with(&c, &list, t("2026-08-21T00:30:00Z"), small).unwrap();
        assert!(
            rows.iter()
                .filter(|r| r.strategy == "daily")
                .any(|r| matches!(r.status, SlotStatus::WrongHost(_)))
        );
    }

    #[test]
    fn retention_keeps_two_fulls_and_the_two_newest_incrementals_per_strategy() {
        let mut c = cfg();
        c.strategy[0].keep = 2;
        let mut list = BundleList::default();
        let w = |d: &str| epoch(t(&format!("2026-{d}T23:00:00Z")));
        for day in ["07-26", "08-02", "08-09", "08-16"] {
            list.bundles.push(entry("weekly", w(day), ""));
            // one daily per weekly
            let ds = w(day) + 86_400;
            list.bundles
                .push(entry("daily", ds, &format!("weekly-{}", w(day))));
            let hs = ds + 3600;
            list.bundles
                .push(entry("hourly", hs, &format!("daily-{ds}")));
        }
        let pruned = retain(&c, &mut list);
        let kept: Vec<&str> = list.bundles.iter().map(|b| b.id.as_str()).collect();
        // Two newest weeklies + the two newest dailies/hourlies (which sit on them); the older chains pruned.
        assert_eq!(
            list.bundles
                .iter()
                .filter(|b| b.strategy == "weekly")
                .count(),
            2
        );
        assert_eq!(
            list.bundles
                .iter()
                .filter(|b| b.strategy == "daily")
                .count(),
            2
        );
        assert_eq!(
            list.bundles
                .iter()
                .filter(|b| b.strategy == "hourly")
                .count(),
            2
        );
        assert_eq!(pruned.len(), 6);
        assert!(
            kept.iter()
                .all(|k| !k.contains(&w("07-26").to_string())
                    && !k.contains(&w("08-02").to_string()))
        );
    }

    /// The 2026-08-22 shape: one weekly, three dailies, 39 hourlies spread over them. Only the two
    /// newest of each incremental strategy stay — a fresh clone downloads 5 bundles, not 43 — and the
    /// one that was newest a slot ago is still listed (a client mid-chain never 404s on it).
    #[test]
    fn retention_drops_everything_but_the_two_newest_incrementals_and_keeps_last_slots_bundle() {
        let c = cfg();
        let mut list = BundleList::default();
        let w0 = epoch(t("2026-08-16T23:00:00Z"));
        list.bundles.push(entry("weekly", w0, ""));
        let mut dailies = Vec::new();
        for day in ["08-17", "08-18", "08-19"] {
            let ds = epoch(t(&format!("2026-{day}T23:00:00Z")));
            list.bundles
                .push(entry("daily", ds, &format!("weekly-{w0}")));
            dailies.push(ds);
        }
        // 13 hourlies on each daily (01:00..13:00 the next day).
        for ds in &dailies {
            for h in 2..=14u64 {
                list.bundles
                    .push(entry("hourly", ds + h * 3600, &format!("daily-{ds}")));
            }
        }
        assert_eq!(list.bundles.len(), 1 + 3 + 39);
        let newest_hourly = format!("hourly-{}", dailies[2] + 14 * 3600);
        let previous_hourly = format!("hourly-{}", dailies[2] + 13 * 3600);
        let pruned = retain(&c, &mut list);
        let kept: Vec<&str> = list.bundles.iter().map(|b| b.id.as_str()).collect();
        assert_eq!(
            list.bundles
                .iter()
                .filter(|b| b.strategy == "weekly")
                .count(),
            1
        );
        assert_eq!(
            list.bundles
                .iter()
                .filter(|b| b.strategy == "daily")
                .count(),
            2
        );
        assert_eq!(
            list.bundles
                .iter()
                .filter(|b| b.strategy == "hourly")
                .count(),
            2
        );
        assert_eq!(kept.len(), 5, "{kept:?}");
        assert_eq!(pruned.len(), 38);
        assert!(
            kept.contains(&newest_hourly.as_str()) && kept.contains(&previous_hourly.as_str()),
            "{kept:?}"
        );
        assert!(
            kept.contains(&format!("daily-{}", dailies[2]).as_str())
                && kept.contains(&format!("daily-{}", dailies[1]).as_str())
        );
        // Next slot arrives: the bundle that was newest is still listed, the one before it goes.
        let next = dailies[2] + 15 * 3600;
        list.bundles
            .push(entry("hourly", next, &format!("daily-{}", dailies[2])));
        let pruned = retain(&c, &mut list);
        assert_eq!(
            pruned,
            vec![format!("bundles/hourly/{}.bundle", dailies[2] + 13 * 3600)]
        );
        assert!(list.bundles.iter().any(|b| b.id == newest_hourly));
        // Orphans never survive: an hourly whose daily is gone goes too, even if it is among the newest.
        let mut c1 = cfg();
        c1.strategy[0].keep = 1;
        let w1 = epoch(t("2026-08-23T23:00:00Z"));
        list.bundles.push(entry("weekly", w1, ""));
        list.bundles
            .push(entry("daily", w1 + 86_400, &format!("weekly-{w1}")));
        // keep=1 weekly → weekly-w0 and its dailies go → their hourlies go, whatever their age.
        retain(&c1, &mut list);
        assert!(
            list.bundles.iter().all(|b| b.strategy != "hourly"),
            "{:?}",
            list.bundles.iter().map(|b| &b.id).collect::<Vec<_>>()
        );
        assert_eq!(
            list.bundles
                .iter()
                .filter(|b| b.strategy == "daily")
                .count(),
            1
        );
    }

    #[test]
    fn default_refs_are_main_only_unless_configured() {
        let mut c = cfg();
        assert_eq!(
            default_refs(&c, &c.strategy[0]),
            vec!["HEAD", "refs/heads/main"]
        );
        c.extra_refs = vec!["refs/tags/v*".into()];
        assert_eq!(
            default_refs(&c, &c.strategy[0]),
            vec!["HEAD", "refs/heads/main", "refs/tags/v*"]
        );
        c.main_only = false;
        assert_eq!(
            default_refs(&c, &c.strategy[0]),
            vec!["refs/heads/*", "refs/tags/*", "HEAD", "refs/tags/v*"]
        );
        let mut s = c.strategy[0].clone();
        s.refs = vec!["refs/heads/release-*".into()];
        assert_eq!(default_refs(&c, &s), vec!["refs/heads/release-*"]);
    }

    /// A repository's first day: a weekly exists, no daily yet. Hourlies are not
    /// blocked — they resolve their base up the chain to the weekly (and the
    /// builder would cut them with the weekly's tips as prerequisites); the
    /// missing dailies are planned as usual. Prod 2026-08-21: a large repository's hourlies
    /// only existed because a header-only legacy daily sat in the list.
    #[test]
    fn hourlies_fall_back_to_the_weekly_when_no_daily_exists_yet() {
        let c = cfg();
        let w0 = epoch(t("2026-08-16T23:00:00Z")); // Sunday
        let mut list = BundleList::default();
        list.bundles.push(entry("weekly", w0, ""));
        let now = t("2026-08-17T12:30:00Z"); // Monday midday: no daily slot (23:00) has passed
        let rows = plan(&c, &list, now, true).unwrap();
        let hourly: Vec<&SlotPlan> = rows.iter().filter(|r| r.strategy == "hourly").collect();
        assert_eq!(hourly.len(), INCREMENTALS_KEPT, "{rows:?}");
        assert!(
            hourly.iter().all(|r| r.status == SlotStatus::Missing),
            "{hourly:?}"
        );
        assert!(
            hourly
                .iter()
                .all(|r| r.base_id.as_deref() == Some(format!("weekly-{w0}").as_str())),
            "{hourly:?}"
        );
        assert_eq!(
            hourly.first().unwrap().slot,
            epoch(t("2026-08-17T11:00:00Z"))
        );
        assert_eq!(
            hourly.last().unwrap().slot,
            epoch(t("2026-08-17T12:00:00Z"))
        );
        assert_eq!(
            base_for_slot_chain(&c, &list, "daily", epoch(t("2026-08-17T05:00:00Z")))
                .map(|b| b.id.as_str()),
            Some(format!("weekly-{w0}").as_str())
        );
        // Once Monday's daily exists, Tuesday's hourlies chain to it, Monday's stay on the weekly.
        let d1 = epoch(t("2026-08-17T23:00:00Z"));
        list.bundles
            .push(entry("daily", d1, &format!("weekly-{w0}")));
        let rows = plan(&c, &list, t("2026-08-18T03:30:00Z"), true).unwrap();
        let tue3 = rows
            .iter()
            .find(|r| r.strategy == "hourly" && r.slot == epoch(t("2026-08-18T03:00:00Z")))
            .unwrap();
        assert_eq!(
            tue3.base_id.as_deref(),
            Some(format!("daily-{d1}").as_str())
        );
        // Monday 05:00 is neither built nor among the two newest slots: not planned at all.
        assert!(
            rows.iter()
                .all(|r| !(r.strategy == "hourly" && r.slot == epoch(t("2026-08-17T05:00:00Z"))))
        );
    }
}
