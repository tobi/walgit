#![allow(clippy::unused_self, clippy::doc_lazy_continuation)]
//! bundle-uri: scheduled full/incremental bundle strategies, bundle list.
//! See AGENTS.md Phase 5 and docs/CONTRACT.md `walgit-bundle`.
//!
//! # Architecture
//!
//! The [`Bundler`] is the public entry point. It depends on a [`BundleSource`]
//! trait that provides repo-scoped access (local git repo + [`Prefixed`] store
//! + `head_seq`). When `walgit_wal::Registry` lands it will implement
//! `BundleSource` (impl lives in this crate) and the `new` signature will
//! accept `Arc<Registry>` directly. Until then, [`Bundler::new_with_source`]
//! accepts any `BundleSource` impl (used by tests).
//!
//! The core operations in [`ops`] take a [`walgit_git::LocalRepo`] + [`Prefixed`]
//! store so they are unit-testable with upstream `git` + [`MemoryStore`] without
//! the full WAL stack.

pub mod ops;
pub mod render;
pub mod schedule;
pub mod slots;

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use thiserror::Error;
use tracing::{debug, warn};

use walgit_config::Config;
use walgit_git::{GitError, LocalRepo};
use walgit_proto::time;
use walgit_proto::v1::{BundleEntry, BundleList};
use walgit_store::Prefixed;

pub use ops::LeaseGuard;
pub use walgit_git::{RefSnapshotData, RepoId};
// ---------------------------------------------------------------------------
// Error
// ---------------------------------------------------------------------------

/// Error type for all bundle operations.
#[derive(Debug, Error)]
pub enum BundleError {
    #[error("store error: {0}")]
    Store(#[from] walgit_store::StoreError),
    #[error("decode error: {0}")]
    Decode(#[from] prost::DecodeError),
    #[error("git error: {0}")]
    Git(#[from] GitError),
    #[error("strategy not found: {0}")]
    StrategyNotFound(String),
    #[error("repo not found: {0}")]
    RepoNotFound(String),
    #[error("invalid repo id: {0}")]
    InvalidRepoId(String),
    #[error("invalid schedule: {0}")]
    InvalidSchedule(String),
    #[error("io error: {0}")]
    Io(String),
    #[error("bundle not found: {0}")]
    BundleNotFound(String),
    #[error("no refs to bundle")]
    NoRefs,
    #[error("no new objects since base — bundle would be empty")]
    NoNewObjects,
    /// Minimum-size gate (`bundles.min_commits`): the incremental would carry
    /// fewer commits than the floor — not built, plan state `too-small`.
    #[error("too small: {commits} commits since the base bundle (min {min})")]
    TooSmall { commits: u64, min: u64 },
    /// The tip set equals the newest incremental of the strategy on the same
    /// base: the bundle would be identical to `since` — not built.
    #[error("unchanged since {since}")]
    Unchanged { since: String },
    #[error("CAS retries exhausted")]
    RetriesExhausted,
    #[error("other: {0}")]
    Other(String),
}

// ---------------------------------------------------------------------------
// BundleSource trait + handle
// ---------------------------------------------------------------------------

/// A handle to an open repo for bundle operations.
///
/// When `walgit_wal::RepoHandle` lands, `BundleSource::open_repo` will
/// construct this from `handle.local().clone()`, `handle.store().clone()`,
/// and `handle.manifest().head_seq`.
pub struct BundleRepoHandle {
    /// Local git repository (bare, on disk).
    pub local: LocalRepo,
    /// Repo-scoped store (prefix = `repos/<owner>/<repo>/`).
    pub store: Prefixed,
    /// Current WAL head sequence number.
    pub head_seq: u64,
    /// How packs are produced here (set by `prepare_objects`; see [`BundleEngine`]).
    pub engine: BundleEngine,
    /// D24: the repository's effective config (host ⊕ settings). `None` =
    /// the bundler's own (tests, sources without settings).
    pub cfg: Option<Arc<Config>>,
}

/// Which engine writes a bundle's pack.
#[derive(Clone, Default)]
pub enum BundleEngine {
    /// `git bundle create` on a complete local copy (every pack real).
    #[default]
    Git,
    /// The gix engine (`LocalRepo::write_bundle_gix`): the base pack is
    /// linked from a store mount or served remotely, so enumeration must be
    /// a tree diff (stock git would read every tree of the boundary commits
    /// through the mount, or fail). Only **incremental** bundles are built
    /// this way; full bundles of such repos are the VM job's (compose).
    Gix {
        faulter: Option<std::sync::Arc<dyn walgit_git::ObjectFaulter>>,
    },
}

/// Abstraction over the registry: provides repo access and listing.
///
/// `walgit_wal::Registry` will implement this trait (impl in this crate)
/// when Wal lands.
#[async_trait::async_trait]
pub trait BundleSource: Send + Sync + 'static {
    /// Open a repo for bundle operations (list/render: refs + store only;
    /// must be cheap and must not take the repo's write lock — callers may
    /// hold a read guard). Error if the repo doesn't exist.
    async fn open_repo(&self, id: &RepoId) -> Result<BundleRepoHandle, BundleError>;
    /// Make the repo's objects readable locally before a build (`git bundle
    /// create` streams from the local copy). Called by `build`/`run_due` only,
    /// never while a guard is held. Default: nothing to do.
    async fn prepare_objects(&self, _id: &RepoId) -> Result<(), BundleError> {
        Ok(())
    }
    /// Engine for building `id`'s bundles (after `prepare_objects`). Default: git.
    async fn engine(&self, _id: &RepoId) -> BundleEngine {
        BundleEngine::Git
    }
    /// Ref state of `id` as of `at` (+ the WAL seq it corresponds to), for
    /// cutting a calendar slot's bundle. Default: not supported → the
    /// current refs are used (seq 0).
    async fn refs_as_of(
        &self,
        _id: &RepoId,
        _at: SystemTime,
    ) -> Result<Option<(RefSnapshotData, u64)>, BundleError> {
        Ok(None)
    }
    /// List all known repos.
    async fn list_repos(&self) -> Result<Vec<RepoId>, BundleError>;
}

// ---------------------------------------------------------------------------
// Bundler
// ---------------------------------------------------------------------------

/// The bundle builder. Evaluates schedules, builds full/incremental bundles,
/// manages the bundle list, and renders advertisement data.
pub struct Bundler {
    source: Arc<dyn BundleSource>,
    cfg: Arc<Config>,
    lease_ttl: Duration,
    /// Slots measured under the minimum-size gate: (repo, strategy, slot) →
    /// commits. In-memory (this maintainer's view); the plan shows `too-small`.
    gates: parking_lot::Mutex<std::collections::HashMap<(String, String, u64), u64>>,
}

impl Bundler {
    /// Create a bundler from any [`BundleSource`]. Used by tests and
    /// internally by [`Bundler::new`] (which requires `walgit-wal`).
    pub fn new_with_source(source: Arc<dyn BundleSource>, cfg: Arc<Config>) -> Arc<Self> {
        Arc::new(Self {
            source,
            cfg,
            gates: parking_lot::Mutex::default(),
            lease_ttl: Duration::from_mins(30),
        })
    }

    /// Create a bundler backed by `walgit_wal::Registry`.
    /// Available when the `wal` feature is enabled.
    #[cfg(feature = "wal")]
    pub fn new(registry: Arc<walgit_wal::Registry>, cfg: Arc<Config>) -> Arc<Self> {
        Self::new_with_source(registry, cfg)
    }

    /// Find a strategy config by name.
    /// The config to plan/build `handle`'s repository with (D24).
    fn cfg_for<'a>(&'a self, handle: &'a BundleRepoHandle) -> &'a Config {
        handle.cfg.as_deref().unwrap_or(&self.cfg)
    }

    fn find_strategy<'a>(
        &self,
        cfg: &'a Config,
        name: &str,
    ) -> Result<&'a walgit_config::BundleStrategy, BundleError> {
        cfg.bundles
            .strategy
            .iter()
            .find(|s| s.name == name)
            .ok_or_else(|| BundleError::StrategyNotFound(name.to_string()))
    }

    /// Build a bundle for `strategy` (and its base, if needed) and update the
    /// bundle list.
    pub async fn build(&self, id: &RepoId, strategy: &str) -> Result<BundleEntry, BundleError> {
        self.source.prepare_objects(id).await?;
        let mut handle = self.source.open_repo(id).await?;
        handle.engine = self.source.engine(id).await;
        self.build_with_handle(&handle, strategy).await
    }

    /// Internal build that reuses an open handle (avoids re-opening for the
    /// recursive base-build case).
    async fn build_with_handle(
        &self,
        handle: &BundleRepoHandle,
        strategy_name: &str,
    ) -> Result<BundleEntry, BundleError> {
        let cut = ops::Cut {
            slot: 0,
            snapshot: None,
            seq: handle.head_seq,
        };
        self.build_slot(handle, strategy_name, &cut).await
    }

    /// Build `strategy_name` for `cut` (a calendar slot with its ref state, or
    /// "now" when `cut.slot == 0`). Incremental prerequisites = the newest base
    /// bundle with token ≤ the slot (or the newest at all for "now").
    async fn build_slot(
        &self,
        handle: &BundleRepoHandle,
        strategy_name: &str,
        cut: &ops::Cut,
    ) -> Result<BundleEntry, BundleError> {
        let cfg = self.cfg_for(handle);
        let strat = self.find_strategy(cfg, strategy_name)?;
        let store = &handle.store;
        let refs = slots::default_refs(&cfg.bundles, strat);

        // Determine prerequisites for incremental bundles.
        let (base_id, prerequisites) = match strat.kind {
            walgit_config::BundleKind::Full => (String::new(), Vec::new()),
            walgit_config::BundleKind::Incremental => {
                let base_name = strat.base.as_deref().ok_or_else(|| {
                    BundleError::Other(format!("strategy {strategy_name} has no base"))
                })?;
                let list = ops::read_list(store).await?.unwrap_or_default();
                // One rule for both topologies (`slots::base_for_incremental`): up the chain (daily,
                // else weekly) — never block an incremental on a base strategy that has no bundle
                // yet, and never cut the base at THIS slot (a daily with an hourly's slot/token was
                // the old fallback) — or, `chain = true`, this strategy's own previous bundle.
                let base =
                    slots::base_for_incremental(&cfg.bundles, &list, strat, cut.slot).cloned();
                match base {
                    Some(base) => {
                        let prereqs: Vec<String> =
                            base.tips.iter().map(|t| t.oid.clone()).collect();
                        (base.id, prereqs)
                    }
                    None => {
                        return Err(BundleError::Other(format!(
                            "strategy {strategy_name}: no bundle of {base_name} or anything up its chain exists yet — the full root is cut first (compose for a large repository)"
                        )));
                    }
                }
            }
        };

        // Incrementals only (fulls are never gated): identical to the previous
        // incremental on this base → not built; then the minimum-size gate.
        if strat.kind == walgit_config::BundleKind::Incremental && !prerequisites.is_empty() {
            let snap = match &cut.snapshot {
                Some(s) => s.clone(),
                None => handle.local.refs().map_err(BundleError::Git)?,
            };
            let (_, tips) = ops::filter_refs(&snap, &refs);
            if cut.slot > 0
                && let Some(prev) = ops::unchanged_since(
                    &ops::read_list(store).await?.unwrap_or_default(),
                    strategy_name,
                    &base_id,
                    cut.slot,
                    &tips,
                )
            {
                return Err(BundleError::Unchanged {
                    since: prev.id.clone(),
                });
            }
            let min = strat.min_commits.unwrap_or(cfg.bundles.min_commits);
            if min > 0 {
                let tip_oids: Vec<String> = tips
                    .iter()
                    .filter(|t| {
                        walgit_git::gix_hash::ObjectId::from_hex(t.oid.as_bytes())
                            .is_ok_and(|o| handle.local.has_object(&o))
                    })
                    .map(|t| t.oid.clone())
                    .collect();
                let commits = ops::count_commits(&handle.local, &tip_oids, &prerequisites).await?;
                metrics::histogram!("walgit_bundle_commits", "strategy" => strategy_name.to_string()).record(metric_u64(commits));
                tracing::info!(
                    strategy = strategy_name,
                    slot = cut.slot,
                    commits,
                    min,
                    "bundle: commits since base"
                );
                if commits < min {
                    self.gates.lock().insert(
                        (
                            handle.local.path().display().to_string(),
                            strategy_name.to_string(),
                            cut.slot,
                        ),
                        commits,
                    );
                    return Err(BundleError::TooSmall { commits, min });
                }
            }
        }

        // Compute the next creation token.
        let list = ops::read_list(store).await?.unwrap_or_default();
        let prev_token = ops::max_creation_token(&list);
        let now = SystemTime::now();

        // Build and upload the bundle.
        let entry = ops::build_and_upload(
            &handle.local,
            &handle.engine,
            store,
            strategy_name,
            strat.kind,
            &refs,
            &prerequisites,
            &base_id,
            cut,
            prev_token,
            now,
            strat.filter.as_deref(),
        )
        .await?;

        // CAS update the bundle list (append + prune).
        let old_list = list;
        let new_entry = entry.clone();
        let bcfg = &cfg.bundles;
        let cas_result = ops::cas_update_list(store, cfg.wal.cas_max_retries, |current| {
            let mut list = current.cloned().unwrap_or_default();
            list.mode = "all".into();
            list.heuristic = "creationToken".into();
            // Idempotent per slot: replace an entry for the same strategy+token.
            list.bundles.retain(|b| {
                !(b.strategy == new_entry.strategy && b.creation_token == new_entry.creation_token)
            });
            list.bundles.push(new_entry.clone());
            slots::retain(bcfg, &mut list);
            list.updated_at = Some(time::now());
            Ok(Some(list))
        })
        .await?;

        // Delete pruned objects after CAS succeeds.
        if let Some((_, new_list)) = &cas_result {
            let pruned = ops::pruned_diff(&old_list, new_list);
            if !pruned.is_empty() {
                debug!(count = pruned.len(), "deleting pruned bundle objects");
                ops::delete_pruned(store, &pruned).await;
            }
        }

        Ok(entry)
    }

    /// Read the current bundle list. `Ok(None)` if no list exists.
    pub async fn list(&self, id: &RepoId) -> Result<Option<BundleList>, BundleError> {
        let handle = self.source.open_repo(id).await?;
        ops::read_list(&handle.store).await
    }

    /// Render the bundle list as git config text for `--bundle-uri` usage.
    /// `Ok(None)` if no list exists.
    pub async fn render_list(
        &self,
        id: &RepoId,
        base_url: &str,
        filter: Option<&str>,
        fulls: bool,
    ) -> Result<Option<String>, BundleError> {
        let handle = self.source.open_repo(id).await?;
        let list = ops::read_list(&handle.store).await?;
        match list {
            Some(list) => {
                let text = render::render_list_text(
                    &list,
                    id.owner(),
                    id.name(),
                    base_url,
                    &self.cfg_for(&handle).bundles,
                    &handle.store,
                    filter,
                    fulls,
                )
                .await?;
                Ok(Some(text))
            }
            None => Ok(None),
        }
    }

    /// Protocol v2 `bundle-uri` command response lines.
    pub async fn protocol_v2_lines(
        &self,
        id: &RepoId,
        base_url: &str,
    ) -> Result<Vec<String>, BundleError> {
        let handle = self.source.open_repo(id).await?;
        let list = ops::read_list(&handle.store).await?.unwrap_or_default();
        render::protocol_v2_lines(
            &list,
            id.owner(),
            id.name(),
            base_url,
            &self.cfg_for(&handle).bundles,
            &handle.store,
        )
        .await
    }

    /// The slot table of `id` at `now` (see [`slots::plan_with`]); `ctx`
    /// carries what this host can do and the repo's first WAL state.
    pub async fn plan(
        &self,
        id: &RepoId,
        now: SystemTime,
        ctx: slots::PlanContext,
    ) -> Result<Vec<slots::SlotPlan>, BundleError> {
        let handle = self.source.open_repo(id).await?;
        let list = ops::read_list(&handle.store).await?.unwrap_or_default();
        let cfg = self.cfg_for(&handle);
        let mut rows = slots::plan_with(&cfg.bundles, &list, now, ctx)?;
        // A strategy whose ref patterns match nothing in the repository (a repo
        // without `main` under `bundles.main_only`, a pattern typo) can never be
        // cut: say so in the plan instead of logging `NoRefs` at debug and
        // leaving the slot `missing` forever (the sim's weekly did exactly that).
        if let Ok(current) = handle.local.refs_arc() {
            for strat in &cfg.bundles.strategy {
                let patterns = slots::default_refs(&cfg.bundles, strat);
                let (_, tips) = ops::filter_refs(&current, &patterns);
                if tips.is_empty() {
                    let why = format!(
                        "no refs match {} (bundles.main_only = {}): nothing to cut",
                        patterns.join(", "),
                        cfg.bundles.main_only
                    );
                    for r in rows.iter_mut().filter(|r| {
                        r.strategy == strat.name && r.status == slots::SlotStatus::Missing
                    }) {
                        r.status = slots::SlotStatus::Blocked(why.clone());
                    }
                }
            }
        }
        // Slots this instance measured under the floor since its start: show
        // them as `too-small` (a later measurement or a build replaces it).
        let gates = self.gates.lock();
        if !gates.is_empty() {
            for r in &mut rows {
                if r.status == slots::SlotStatus::Missing
                    && let Some(c) = gates.get(&(
                        handle.local.path().display().to_string(),
                        r.strategy.clone(),
                        r.slot,
                    ))
                {
                    let min = cfg
                        .bundles
                        .strategy
                        .iter()
                        .find(|s| s.name == r.strategy)
                        .and_then(|s| s.min_commits)
                        .unwrap_or(cfg.bundles.min_commits);
                    r.status = slots::SlotStatus::TooSmall { commits: *c, min };
                }
            }
        }
        Ok(rows)
    }

    /// Settle every **closed, missing** incremental slot of `id` at refs level —
    /// no lease, no pack work: resolve the slot's refs as of its time, count
    /// commits over its base's tips on the commit graph, and when the verdict
    /// is final (`too-small`, or no state as of the slot) record it in the list
    /// (`BundleList.skipped`). Slots with real work stay `missing` for the
    /// unit selection. Returns how many verdicts were recorded. After a
    /// restart the SSD host spent one 3–4 s unit per closed slot, ~30 passes,
    /// before reaching the live hour (2026-08-21); this makes that one pass.
    /// Bring the list to what retention says (D21, 2026-08-22) without waiting for the next
    /// publish: an idle repository publishes nothing (unchanged/too-small gates), so a list
    /// that grew under an older rule would otherwise keep its 43 entries forever. Refs-level:
    /// one conditional GET of the list, and only when something is pruned one CAS + the
    /// deletes. Returns how many entries were dropped.
    pub async fn apply_retention(&self, id: &RepoId) -> Result<usize, BundleError> {
        let handle = self.source.open_repo(id).await?;
        let store = handle.store.clone();
        let cfg = self.cfg_for(&handle).clone();
        let Some(list) = ops::read_list(&store).await? else {
            return Ok(0);
        };
        let mut probe = list.clone();
        if slots::retain(&cfg.bundles, &mut probe).is_empty() {
            return Ok(0);
        }
        let bcfg = cfg.bundles.clone();
        let cas = ops::cas_update_list(&store, cfg.wal.cas_max_retries, move |current| {
            let mut next = current.cloned().unwrap_or_default();
            if slots::retain(&bcfg, &mut next).is_empty() {
                return Ok(None);
            }
            next.updated_at = Some(time::now());
            Ok(Some(next))
        })
        .await?;
        let Some((_, new_list)) = cas else {
            return Ok(0);
        };
        let pruned = ops::pruned_diff(&list, &new_list);
        if !pruned.is_empty() {
            tracing::info!(repo = %id, pruned = pruned.len(), listed = new_list.bundles.len(), "bundle list brought to retention");
            ops::delete_pruned(&store, &pruned).await;
        }
        Ok(pruned.len())
    }

    pub async fn settle_closed_slots(
        &self,
        id: &RepoId,
        now: SystemTime,
    ) -> Result<usize, BundleError> {
        let handle = self.source.open_repo(id).await?;
        let store = handle.store.clone();
        let cfg = self.cfg_for(&handle).clone();
        let list = ops::read_list(&store).await?.unwrap_or_default();
        let rows = slots::plan(&cfg.bundles, &list, now, true)?;
        let mut verdicts: Vec<walgit_proto::v1::SkippedSlot> = Vec::new();
        for strat in cfg
            .bundles
            .strategy
            .iter()
            .filter(|s| s.kind == walgit_config::BundleKind::Incremental)
        {
            let min = strat.min_commits.unwrap_or(cfg.bundles.min_commits);
            for r in rows.iter().filter(|r| {
                r.strategy == strat.name
                    && r.status == slots::SlotStatus::Missing
                    && slots::slot_closed(strat, r.slot, now)
            }) {
                let Some(base_id) = r.base_id.clone() else {
                    continue;
                };
                let Some(base) = list.bundles.iter().find(|b| b.id == base_id) else {
                    continue;
                };
                let at = slots::from_epoch(r.slot);
                let verdict = match self.source.refs_as_of(id, at).await? {
                    None => Some((0u64, "no state as of the slot".to_string())),
                    Some((snap, seq)) => {
                        let refs = slots::default_refs(&cfg.bundles, strat);
                        let (_, tips) = ops::filter_refs(&snap, &refs);
                        if let Some(prev) =
                            ops::unchanged_since(&list, &strat.name, &base_id, r.slot, &tips)
                        {
                            Some((seq, format!("unchanged since {}", prev.id)))
                        } else if min == 0 {
                            None
                        } else {
                            let prerequisites: Vec<String> =
                                base.tips.iter().map(|t| t.oid.clone()).collect();
                            let tip_oids: Vec<String> = tips
                                .iter()
                                .filter(|t| {
                                    walgit_git::gix_hash::ObjectId::from_hex(t.oid.as_bytes())
                                        .is_ok_and(|o| handle.local.has_object(&o))
                                })
                                .map(|t| t.oid.clone())
                                .collect();
                            if tip_oids.is_empty() {
                                None // cannot measure here (tips not local): leave it to a unit
                            } else {
                                let commits =
                                    ops::count_commits(&handle.local, &tip_oids, &prerequisites)
                                        .await?;
                                (commits < min).then(|| {
                                    (
                                        seq,
                                        format!(
                                            "too-small: {commits} commits since base (min {min})"
                                        ),
                                    )
                                })
                            }
                        }
                    }
                };
                if let Some((seq, reason)) = verdict {
                    tracing::debug!(repo = %id, strategy = %strat.name, slot = r.slot, %reason, "closed slot settled at plan time");
                    verdicts.push(walgit_proto::v1::SkippedSlot {
                        strategy: strat.name.clone(),
                        slot: r.slot,
                        base_id,
                        as_of_seq: seq,
                        reason,
                        at: Some(time::now()),
                    });
                }
            }
        }
        // One CAS for the whole pass (ROUNDTRIPS): a verdict per slot would be a round trip per slot.
        let recorded = verdicts.len();
        ops::record_skipped_many(&store, verdicts).await?;
        Ok(recorded)
    }

    /// Build exactly **one** slot (`strategy` at `slot`) as a unit of the
    /// maintenance loop: lease on the strategy, objects prepared, content =
    /// WAL state as of the slot. Returns `Ok(None)` when the slot was built by
    /// someone else meanwhile or has no new objects / refs (then it stays
    /// missing; the next slot covers it).
    pub async fn build_slot_unit(
        &self,
        id: &RepoId,
        strategy: &str,
        slot: u64,
    ) -> Result<Option<BundleEntry>, BundleError> {
        let mut handle = self.source.open_repo(id).await?;
        let cfg = self.cfg_for(&handle).clone();
        let strat = self.find_strategy(&cfg, strategy)?.clone();
        let strat = &strat;
        let store = handle.store.clone();
        let Some(lease) = ops::try_acquire_lease(&store, &strat.name, self.lease_ttl).await? else {
            return Ok(None);
        };
        let res: Result<Option<BundleEntry>, BundleError> = async {
            let fresh = ops::read_list(&store).await?.unwrap_or_default();
            if fresh.bundles.iter().any(|b| b.strategy == strat.name && b.creation_token == slot) {
                return Ok(None);
            }
            self.source.prepare_objects(id).await?;
            handle.engine = self.source.engine(id).await;
            if strat.kind == walgit_config::BundleKind::Full && matches!(handle.engine, BundleEngine::Gix { .. }) {
                return Err(BundleError::Other("full bundle needs the base pack as a local copy on this host".into()));
            }
            let at = slots::from_epoch(slot);
            // Content is the state AS OF the slot (D22). No state at that time:
            // the first full of a chain is cut from the earliest state there is
            // ("weekly = import state"); an incremental is NOT built — cutting
            // it from now would put today's main under an old token (prod
            // 2026-08-21: eight "08-19/08-20" bundles carried 04:2xZ content).
            let now = SystemTime::now();
            let list_now = ops::read_list(&store).await?.unwrap_or_default();
            let base_id_now = match strat.kind {
                walgit_config::BundleKind::Full => String::new(),
                walgit_config::BundleKind::Incremental => slots::base_for_incremental(&cfg.bundles, &list_now, strat, slot).map(|b| b.id.clone()).unwrap_or_default(),
            };
            let (snapshot, seq) = match self.source.refs_as_of(id, at).await? {
                Some((snap, seq)) => (Some(snap), seq),
                None if strat.kind == walgit_config::BundleKind::Full => (None, handle.head_seq),
                None => {
                    tracing::info!(repo = %id, strategy = %strat.name, slot, "no WAL state as of this slot — not cut");
                    if slots::slot_closed(strat, slot, now) {
                        ops::record_skipped(&store, &strat.name, slot, &base_id_now, 0, "no state as of the slot").await?;
                    }
                    return Ok(None);
                }
            };
            let cut = ops::Cut { slot, snapshot, seq };
            match self.build_slot(&handle, &strat.name, &cut).await {
                Ok(e) => Ok(Some(e)),
                Err(BundleError::TooSmall { commits, min }) => {
                    tracing::info!(repo = %id, strategy = %strat.name, slot, commits, min, "bundle slot too small — not cut (next slot catches up)");
                    // Final once the window is closed: the as-of state cannot change.
                    if slots::slot_closed(strat, slot, now) {
                        ops::record_skipped(&store, &strat.name, slot, &base_id_now, seq, &format!("too-small: {commits} commits since base (min {min})")).await?;
                    }
                    Ok(None)
                }
                Err(BundleError::Unchanged { since }) => {
                    tracing::info!(repo = %id, strategy = %strat.name, slot, %since, "bundle slot unchanged — not cut");
                    if slots::slot_closed(strat, slot, now) {
                        ops::record_skipped(&store, &strat.name, slot, &base_id_now, seq, &format!("unchanged since {since}")).await?;
                    }
                    Ok(None)
                }
                Err(BundleError::NoNewObjects | BundleError::NoRefs) => Ok(None),
                Err(e) => Err(e),
            }
        }
        .await;
        lease.release().await.ok();
        res
    }

    /// Build every **missing slot** of every strategy for `id` up to `now`,
    /// oldest first (backfill), each with the ref state as of its slot and
    /// `creation_token = slot`. One lease per strategy; `backfill_max` bounds
    /// one pass. Returns the built entries. A slot whose content adds no
    /// objects over its prerequisite is still recorded? No: it is skipped
    /// (`NoNewObjects`) and stays missing — the next slot covers it.
    pub async fn run_due(
        &self,
        id: &RepoId,
        now: SystemTime,
    ) -> Result<Vec<BundleEntry>, BundleError> {
        let mut handle = self.source.open_repo(id).await?;
        let store = handle.store.clone();
        let store = &store;
        let mut built = Vec::new();
        let mut prepared = false;

        let list = ops::read_list(store).await?.unwrap_or_default();
        let can_full_guess = true;
        let cfg = self.cfg_for(&handle).clone();
        let rows = slots::plan(&cfg.bundles, &list, now, can_full_guess)?;
        // Strategies in config order (base before incrementals), slots oldest first.
        for strat in &cfg.bundles.strategy {
            let missing: Vec<&slots::SlotPlan> = rows
                .iter()
                .filter(|r| r.strategy == strat.name && r.status == slots::SlotStatus::Missing)
                .collect();
            if missing.is_empty() {
                continue;
            }
            let Some(lease) = ops::try_acquire_lease(store, &strat.name, self.lease_ttl).await?
            else {
                debug!(strategy = %strat.name, "lease held, skipping");
                continue;
            };
            let res: Result<(), BundleError> = async {
                if !prepared {
                    self.source.prepare_objects(id).await?;
                    handle.engine = self.source.engine(id).await;
                    prepared = true;
                }
                if strat.kind == walgit_config::BundleKind::Full && matches!(handle.engine, BundleEngine::Gix { .. }) {
                    debug!(strategy = %strat.name, "full bundle of a linked/remote-served base: built by the VM job (compose)");
                    return Ok(());
                }
                // Re-read the list under the lease: a sibling may have built some.
                let fresh = ops::read_list(store).await?.unwrap_or_default();
                let rows = slots::plan(&cfg.bundles, &fresh, now, true)?;
                let mut todo: Vec<u64> = rows
                    .iter()
                    .filter(|r| r.strategy == strat.name && r.status == slots::SlotStatus::Missing)
                    .map(|r| r.slot)
                    .collect();
                todo.sort_unstable();
                if strat.backfill_max > 0 && todo.len() > strat.backfill_max {
                    // Oldest first, bounded: the rest next pass.
                    todo.truncate(strat.backfill_max);
                }
                for slot in todo {
                    let at = slots::from_epoch(slot);
                    let (snapshot, seq) = match self.source.refs_as_of(id, at).await? {
                        Some((snap, seq)) => (Some(snap), seq),
                        None => (None, handle.head_seq),
                    };
                    let cut = ops::Cut { slot, snapshot, seq };
                    match self.build_slot(&handle, &strat.name, &cut).await {
                        Ok(entry) => {
                            debug!(strategy = %strat.name, slot, seq, "slot built");
                            built.push(entry);
                        }
                        Err(BundleError::NoNewObjects) => {
                            debug!(strategy = %strat.name, slot, "no new objects for this slot, skipped");
                        }
                        Err(BundleError::NoRefs) => {
                            debug!(strategy = %strat.name, slot, "no refs at this slot, skipped");
                        }
                        Err(BundleError::TooSmall { commits, min }) => {
                            debug!(strategy = %strat.name, slot, commits, min, "too small, skipped");
                        }
                        Err(BundleError::Unchanged { since }) => {
                            debug!(strategy = %strat.name, slot, %since, "unchanged, skipped");
                        }
                        Err(e) => return Err(e),
                    }
                }
                Ok(())
            }
            .await;
            lease.release().await.ok();
            res?;
        }
        Ok(built)
    }

    /// Run `run_due` for every repo in the registry.
    pub async fn run_all_due(&self, now: SystemTime) -> Result<(), BundleError> {
        let repos = self.source.list_repos().await?;
        for id in &repos {
            match self.run_due(id, now).await {
                Ok(entries) => {
                    if !entries.is_empty() {
                        debug!(repo = %id, count = entries.len(), "built bundles");
                    }
                }
                // An empty repository has nothing to bundle; that is not a failure.
                Err(BundleError::NoRefs) => {
                    debug!(repo = %id, "no refs to bundle yet");
                }
                // Pack set too large for this instance: bundles for it are built
                // by the VM job (weekly = compose). Not a failure, not noise.
                Err(e) if e.to_string().contains("larger than this instance") => {
                    debug!(repo = %id, "bundle build skipped: pack set does not fit here");
                }
                Err(e) => {
                    warn!(repo = %id, error = %e, "run_due failed");
                }
            }
        }
        Ok(())
    }
}

/// Metrics use `f64`; values beyond its exact integer range are still useful as
/// approximate counters.
#[allow(clippy::cast_precision_loss)]
fn metric_u64(value: u64) -> f64 {
    value as f64
}

// ---------------------------------------------------------------------------
// BundleSource impl for walgit_wal::Registry (behind 'wal' feature)
// ---------------------------------------------------------------------------

/// Pick the engine for a synced WAL repo: gix (+ remote faulter) when its base
/// is remote-served or linked from a store mount, git otherwise.
#[cfg(feature = "wal")]
pub async fn bundle_engine(handle: &walgit_wal::RepoHandle) -> BundleEngine {
    if !handle.remote_served().is_empty() {
        match handle.remote_reader().await {
            Ok(reader) => {
                return BundleEngine::Gix {
                    faulter: Some(std::sync::Arc::new(walgit_wal::remote::Faulter::new(
                        reader,
                        handle.local().clone(),
                    ))),
                };
            }
            Err(e) => {
                warn!(repo = %handle.id(), error = %e, "remote reader unavailable for bundle build; using git");
            }
        }
    }
    let linked = handle.local().packs().is_ok_and(|ps| {
        ps.iter()
            .any(|p| handle.local().pack_path(&p.checksum).is_symlink())
    });
    if linked {
        return BundleEngine::Gix { faulter: None };
    }
    BundleEngine::Git
}

#[cfg(feature = "wal")]
mod wal_impl {
    use super::{BundleEngine, BundleError, BundleRepoHandle, BundleSource, RepoId, bundle_engine};
    use walgit_wal::{Registry, WalError};

    fn wal_err(e: WalError) -> BundleError {
        match e {
            WalError::NotFound => BundleError::RepoNotFound("not found".into()),
            WalError::Git(e) => BundleError::Git(e),
            WalError::Store(e) => BundleError::Store(e),
            other => BundleError::Other(other.to_string()),
        }
    }

    #[async_trait::async_trait]
    impl BundleSource for Registry {
        async fn prepare_objects(&self, id: &RepoId) -> Result<(), BundleError> {
            // `git bundle create` streams objects from the local copy: bring
            // the packs here first (Serve level; too-large repos surface as
            // "larger than this instance" and are skipped by run_all_due).
            // Registry::open alone is refs-level — building from it produced
            // "fatal: bad object refs/heads/main" on the maintainer.
            let handle = self.open(id).await.map_err(wal_err)?;
            drop(handle.sync().await.map_err(wal_err)?);
            Ok(())
        }

        async fn open_repo(&self, id: &RepoId) -> Result<BundleRepoHandle, BundleError> {
            let handle = self.open(id).await.map_err(wal_err)?;
            Ok(BundleRepoHandle {
                local: handle.local().clone(),
                store: handle.store().clone(),
                head_seq: handle.manifest().head_seq,
                engine: BundleEngine::Git,
                cfg: Some(handle.effective_config()),
            })
        }

        async fn engine(&self, id: &RepoId) -> BundleEngine {
            match self.open(id).await {
                Ok(h) => bundle_engine(&h).await,
                Err(_) => BundleEngine::Git,
            }
        }

        async fn list_repos(&self) -> Result<Vec<RepoId>, BundleError> {
            self.list().await.map_err(wal_err)
        }
    }
}
