//! Fault-injecting store wrapper for simulation tests (TigerBeetle-style
//! "safety mode" + "liveness mode").
//!
//! One [`FaultStore`] is one *instance's link* to the bucket: every simulated
//! instance wraps the same inner store (normally [`crate::memory::MemoryStore`])
//! in its own `FaultStore`, so faults are per instance — an asymmetric
//! partition is "this instance's link black-holes GETs", a stale replica is
//! "this link answers every conditional GET with 304", and so on.
//!
//! Two modes, switched at run time with [`FaultStore::set`] / [`FaultStore::heal`]:
//!
//! * **safety**: a [`FaultPlan`] with non-zero probabilities; every op rolls
//!   the dice (seeded, deterministic per link given the same op sequence),
//! * **liveness**: the harness picks a *core* of instances, heals their links
//!   (`heal()`), and *freezes* the rest (`set` a permanent plan: `black_hole`,
//!   `p_stale_304 = 1.0`, …). The core must then converge; the frozen links
//!   may never interfere with it.
//!
//! Fault kinds (see [`FaultPlan`]): latency, transient error *before* the op
//! (nothing applied), transient error *after* a mutation (applied, response
//! lost — the one that breaks "PUT then CAS" protocols), spurious CAS failure,
//! stale 304, truncated bodies, hanging futures, black hole, missing objects,
//! and panics (a process crash in the middle of a protocol step).
//!
//! Every fault taken is counted in [`Stats`] and, when `trace` is on, logged
//! with the key so a failing seed can be read back.

use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use bytes::Bytes;
use futures::StreamExt;
use parking_lot::Mutex;

use crate::{
    BoxStream, ByteStream, DynStore, GetOptions, GetResult, ObjectMeta, ObjectStore, PutBody,
    PutMode, PutOptions, Result, StoreError, Version,
};

/// Probabilities (0..=1) and switches. `Default` = no faults at all.
#[derive(Clone, Debug, Default)]
pub struct FaultPlan {
    /// Uniform latency added to every op, before it is applied.
    pub delay: Option<(Duration, Duration)>,
    /// Latency added to reads (`get`/`head`) AFTER the inner op: the answer was taken from the store
    /// at the earlier instant and arrives late — a slow network delivering an already-stale snapshot
    /// (what a conditional GET racing a local publish looks like). Honours `only_keys`.
    pub delay_after: Option<Duration>,
    /// `Retryable` before the op is applied (get/head/put/delete/list/compose).
    pub p_err_before: f64,
    /// Mutation applied, then `Retryable` returned (put/delete/compose only).
    pub p_err_after: f64,
    /// Conditional PUT/DELETE answers `PreconditionFailed` without applying.
    pub p_cas_fail: f64,
    /// `get` with `if_none_match` answers `NotModified` regardless of the
    /// real version (a replica that never sees anyone else's writes).
    pub p_stale_304: f64,
    /// Body streams (get) end early with `Retryable` after some bytes.
    pub p_truncate: f64,
    /// The op's future never completes.
    pub p_hang: f64,
    /// Every op hangs forever (hard partition). Pending ops keep hanging.
    pub black_hole: bool,
    /// Keys containing any of these substrings answer `NotFound` on get/head
    /// (object lost / not yet visible). Mutations still go through.
    pub deny_keys: Vec<String>,
    /// Keys containing any of these substrings panic on first touch, once per
    /// pattern (a crash in the middle of a protocol step). A pattern may be
    /// scoped to one op as `"put:manifest.pb"` (ops: get/head/put/delete/compose).
    pub panic_once_keys: Vec<String>,
    /// Restrict every probabilistic fault to keys containing one of these
    /// substrings (`None` = all keys). `black_hole`/`deny`/`panic` are unaffected.
    pub only_keys: Option<Vec<String>>,
}

impl FaultPlan {
    /// Moderate, uniform chaos: the "safety mode" dice.
    pub fn chaos(rate: f64) -> Self {
        FaultPlan {
            delay: Some((Duration::from_millis(0), Duration::from_millis(5))),
            p_err_before: rate,
            p_err_after: rate / 2.0,
            p_cas_fail: rate / 2.0,
            p_stale_304: rate / 2.0,
            p_truncate: rate / 2.0,
            p_hang: 0.0,
            ..Default::default()
        }
    }
    /// Hard partition: nothing ever returns.
    pub fn black_hole() -> Self {
        FaultPlan {
            black_hole: true,
            ..Default::default()
        }
    }
    /// Asymmetric partition of the replica kind: writes go through, but the
    /// instance never learns anything new (every conditional GET is a 304).
    pub fn stale_forever() -> Self {
        FaultPlan {
            p_stale_304: 1.0,
            ..Default::default()
        }
    }
    #[must_use]
    pub fn with_only(mut self, keys: &[&str]) -> Self {
        self.only_keys = Some(keys.iter().map(std::string::ToString::to_string).collect());
        self
    }
}

#[derive(Default, Debug)]
pub struct Stats {
    pub ops: AtomicU64,
    pub err_before: AtomicU64,
    pub err_after: AtomicU64,
    pub cas_fail: AtomicU64,
    pub stale_304: AtomicU64,
    pub truncate: AtomicU64,
    pub hang: AtomicU64,
    pub denied: AtomicU64,
    pub panics: AtomicU64,
}

impl Stats {
    pub fn faults(&self) -> u64 {
        self.err_before.load(Ordering::Relaxed)
            + self.err_after.load(Ordering::Relaxed)
            + self.cas_fail.load(Ordering::Relaxed)
            + self.stale_304.load(Ordering::Relaxed)
            + self.truncate.load(Ordering::Relaxed)
            + self.hang.load(Ordering::Relaxed)
            + self.denied.load(Ordering::Relaxed)
            + self.panics.load(Ordering::Relaxed)
    }
    pub fn summary(&self) -> String {
        format!(
            "ops={} err_before={} err_after={} cas_fail={} stale_304={} truncate={} hang={} denied={} panics={}",
            self.ops.load(Ordering::Relaxed),
            self.err_before.load(Ordering::Relaxed),
            self.err_after.load(Ordering::Relaxed),
            self.cas_fail.load(Ordering::Relaxed),
            self.stale_304.load(Ordering::Relaxed),
            self.truncate.load(Ordering::Relaxed),
            self.hang.load(Ordering::Relaxed),
            self.denied.load(Ordering::Relaxed),
            self.panics.load(Ordering::Relaxed),
        )
    }
}

/// xorshift64*: tiny, seedable, good enough for dice.
struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed.max(1) ^ 0x9E37_79B9_7F4A_7C15)
    }
    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    #[expect(
        clippy::cast_precision_loss,
        reason = "The shifted numerator has 53 bits and the denominator is exactly 2^53"
    )]
    fn f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }
    fn below(&mut self, n: u64) -> u64 {
        if n == 0 { 0 } else { self.next_u64() % n }
    }
}

pub struct FaultStore {
    inner: DynStore,
    name: String,
    plan: Mutex<FaultPlan>,
    rng: Mutex<Rng>,
    stats: Stats,
    /// Patterns from `panic_once_keys` already fired.
    fired_panics: Mutex<Vec<String>>,
    trace: Mutex<Option<Vec<String>>>,
}

enum Decision {
    Proceed,
    ErrBefore,
    ErrAfter,
    CasFail,
    Stale,
    Truncate(usize),
    Hang,
    Denied,
}

impl FaultStore {
    pub fn new(inner: DynStore, name: impl Into<String>, seed: u64) -> Arc<Self> {
        Arc::new(FaultStore {
            inner,
            name: name.into(),
            plan: Mutex::new(FaultPlan::default()),
            rng: Mutex::new(Rng::new(seed)),
            stats: Stats::default(),
            fired_panics: Mutex::new(Vec::new()),
            trace: Mutex::new(None),
        })
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn inner(&self) -> &DynStore {
        &self.inner
    }
    pub fn stats(&self) -> &Stats {
        &self.stats
    }
    /// Replace the plan (takes effect for every op issued from now on).
    pub fn set(&self, plan: FaultPlan) {
        *self.plan.lock() = plan;
    }
    pub fn plan(&self) -> FaultPlan {
        self.plan.lock().clone()
    }
    /// Liveness mode for a core link: no faults from now on. Ops that are
    /// already hanging stay hung (that is the point of a crash).
    pub fn heal(&self) {
        *self.plan.lock() = FaultPlan::default();
    }
    pub fn set_trace(&self, on: bool) {
        *self.trace.lock() = if on { Some(Vec::new()) } else { None };
    }
    pub fn take_trace(&self) -> Vec<String> {
        self.trace
            .lock()
            .as_mut()
            .map(std::mem::take)
            .unwrap_or_default()
    }

    fn log(&self, s: String) {
        if let Some(t) = self.trace.lock().as_mut() {
            t.push(s);
        }
    }

    fn in_scope(plan: &FaultPlan, key: &str) -> bool {
        match &plan.only_keys {
            None => true,
            Some(ks) => ks.iter().any(|k| key.contains(k.as_str())),
        }
    }

    /// Roll the dice for one op. `mutation`: put/delete/compose; `conditional`:
    /// CAS put/delete or if-none-match get; `body_len`: for truncation.
    #[expect(
        clippy::cast_possible_truncation,
        reason = "The truncation offset is sampled below 2^20 and fits usize"
    )]
    #[expect(
        clippy::panic,
        reason = "Explicit crash injection is the purpose of the fault-store test adapter"
    )]
    async fn decide(
        &self,
        op: &str,
        key: &str,
        mutation: bool,
        conditional: bool,
        read_body: bool,
    ) -> Decision {
        self.stats.ops.fetch_add(1, Ordering::Relaxed);
        let plan = self.plan.lock().clone();
        if let Some(p) = plan
            .panic_once_keys
            .iter()
            .find(|p| match p.split_once(':') {
                Some((o, k)) if ["get", "head", "put", "delete", "compose"].contains(&o) => {
                    o == op && key.contains(k)
                }
                _ => key.contains(p.as_str()),
            })
        {
            let mut fired = self.fired_panics.lock();
            if !fired.contains(p) {
                fired.push(p.clone());
                drop(fired);
                self.stats.panics.fetch_add(1, Ordering::Relaxed);
                self.log(format!("{} {op} {key}: PANIC", self.name));
                panic!(
                    "fault-store[{}]: injected crash during {op} {key}",
                    self.name
                );
            }
        }
        if plan.black_hole {
            self.stats.hang.fetch_add(1, Ordering::Relaxed);
            self.log(format!("{} {op} {key}: black-hole", self.name));
            return Decision::Hang;
        }
        if !mutation && plan.deny_keys.iter().any(|p| key.contains(p.as_str())) {
            self.stats.denied.fetch_add(1, Ordering::Relaxed);
            self.log(format!("{} {op} {key}: denied", self.name));
            return Decision::Denied;
        }
        if let Some((lo, hi)) = plan.delay {
            let span = u64::try_from(hi.saturating_sub(lo).as_micros()).unwrap_or(u64::MAX);
            let extra = self.rng.lock().below(span.saturating_add(1));
            tokio::time::sleep(lo + Duration::from_micros(extra)).await;
        }
        if !Self::in_scope(&plan, key) {
            return Decision::Proceed;
        }
        let (roll, cut) = {
            let mut r = self.rng.lock();
            (
                [r.f64(), r.f64(), r.f64(), r.f64(), r.f64(), r.f64()],
                r.below(1 << 20) as usize,
            )
        };
        let d = if roll[0] < plan.p_hang {
            self.stats.hang.fetch_add(1, Ordering::Relaxed);
            Decision::Hang
        } else if roll[1] < plan.p_err_before {
            self.stats.err_before.fetch_add(1, Ordering::Relaxed);
            Decision::ErrBefore
        } else if mutation && conditional && roll[2] < plan.p_cas_fail {
            self.stats.cas_fail.fetch_add(1, Ordering::Relaxed);
            Decision::CasFail
        } else if mutation && roll[3] < plan.p_err_after {
            self.stats.err_after.fetch_add(1, Ordering::Relaxed);
            Decision::ErrAfter
        } else if !mutation && conditional && roll[4] < plan.p_stale_304 {
            self.stats.stale_304.fetch_add(1, Ordering::Relaxed);
            Decision::Stale
        } else if read_body && roll[5] < plan.p_truncate {
            self.stats.truncate.fetch_add(1, Ordering::Relaxed);
            Decision::Truncate(cut)
        } else {
            Decision::Proceed
        };
        if !matches!(d, Decision::Proceed) {
            let what = match &d {
                Decision::Hang => "hang",
                Decision::ErrBefore => "err-before",
                Decision::CasFail => "cas-fail",
                Decision::ErrAfter => "err-after",
                Decision::Stale => "stale-304",
                Decision::Truncate(_) => "truncate",
                _ => "?",
            };
            self.log(format!("{} {op} {key}: {what}", self.name));
        }
        d
    }

    fn retryable(&self, op: &str, key: &str, when: &str) -> StoreError {
        StoreError::Retryable(anyhow::anyhow!(
            "fault-store[{}]: injected transient error {when} {op} {key}",
            self.name
        ))
    }
}

async fn hang_forever<T>() -> T {
    futures::future::pending::<T>().await
}

fn truncate_stream(body: ByteStream, at: usize, msg: String) -> ByteStream {
    let mut sent = 0usize;
    let mut done = false;
    Box::pin(body.flat_map(move |chunk| {
        if done {
            return futures::stream::iter(Vec::new());
        }
        match chunk {
            Ok(b) => {
                let room = at.saturating_sub(sent);
                if b.len() <= room {
                    sent += b.len();
                    futures::stream::iter(vec![Ok(b)])
                } else {
                    done = true;
                    let head = b.slice(0..room);
                    futures::stream::iter(vec![
                        Ok(head),
                        Err(StoreError::Retryable(anyhow::anyhow!(msg.clone()))),
                    ])
                }
            }
            Err(e) => {
                done = true;
                futures::stream::iter(vec![Err(e)])
            }
        }
    }))
}

#[async_trait::async_trait]
impl ObjectStore for FaultStore {
    fn backend(&self) -> &'static str {
        self.inner.backend()
    }
    fn is_prefixed(&self) -> bool {
        // Hide from the Prefixed span logic: we are transparent.
        self.inner.is_prefixed()
    }

    async fn get(&self, key: &str, opts: GetOptions) -> Result<GetResult> {
        let conditional = opts.if_none_match.is_some();
        let late = {
            let plan = self.plan();
            plan.delay_after.filter(|_| Self::in_scope(&plan, key))
        };
        let r = self.get_inner(key, opts, conditional).await;
        if let Some(d) = late {
            self.log(format!(
                "{} get {key}: answer delivered {d:?} late (conditional={conditional})",
                self.name
            ));
            tokio::time::sleep(d).await;
        }
        r
    }

    async fn head(&self, key: &str) -> Result<Option<ObjectMeta>> {
        match self.decide("head", key, false, false, false).await {
            Decision::Hang => hang_forever().await,
            Decision::ErrBefore => Err(self.retryable("head", key, "before")),
            Decision::Denied => Ok(None),
            _ => self.inner.head(key).await,
        }
    }

    async fn put(&self, key: &str, body: PutBody, opts: PutOptions) -> Result<ObjectMeta> {
        let conditional = !matches!(opts.mode, PutMode::Overwrite);
        match self.decide("put", key, true, conditional, false).await {
            Decision::Hang => hang_forever().await,
            Decision::ErrBefore => Err(self.retryable("put", key, "before")),
            Decision::CasFail => Err(StoreError::PreconditionFailed {
                key: key.into(),
                current: None,
            }),
            Decision::ErrAfter => {
                let _ = self.inner.put(key, body, opts).await?;
                Err(self.retryable("put", key, "after (applied)"))
            }
            _ => self.inner.put(key, body, opts).await,
        }
    }

    async fn delete(&self, key: &str, if_version: Option<Version>) -> Result<()> {
        let conditional = if_version.is_some();
        match self.decide("delete", key, true, conditional, false).await {
            Decision::Hang => hang_forever().await,
            Decision::ErrBefore => Err(self.retryable("delete", key, "before")),
            Decision::CasFail => Err(StoreError::PreconditionFailed {
                key: key.into(),
                current: None,
            }),
            Decision::ErrAfter => {
                self.inner.delete(key, if_version).await?;
                Err(self.retryable("delete", key, "after (applied)"))
            }
            _ => self.inner.delete(key, if_version).await,
        }
    }

    fn list(
        &self,
        prefix: &str,
        start_after: Option<&str>,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        // Listing is never on a hot path; only the black hole and err_before apply.
        let plan = self.plan.lock().clone();
        self.stats.ops.fetch_add(1, Ordering::Relaxed);
        if plan.black_hole {
            self.stats.hang.fetch_add(1, Ordering::Relaxed);
            return Box::pin(futures::stream::pending());
        }
        if Self::in_scope(&plan, prefix) && self.rng.lock().f64() < plan.p_err_before {
            self.stats.err_before.fetch_add(1, Ordering::Relaxed);
            let e = self.retryable("list", prefix, "before");
            return Box::pin(futures::stream::once(async move { Err(e) }));
        }
        self.inner.list(prefix, start_after)
    }

    async fn list_prefixes(&self, prefix: &str) -> Result<Vec<String>> {
        let plan = self.plan.lock().clone();
        self.stats.ops.fetch_add(1, Ordering::Relaxed);
        if plan.black_hole {
            self.stats.hang.fetch_add(1, Ordering::Relaxed);
            futures::future::pending::<()>().await;
        }
        if Self::in_scope(&plan, prefix) && self.rng.lock().f64() < plan.p_err_before {
            self.stats.err_before.fetch_add(1, Ordering::Relaxed);
            return Err(self.retryable("list_prefixes", prefix, "before"));
        }
        self.inner.list_prefixes(prefix).await
    }

    async fn signed_get_url(&self, key: &str, ttl: Duration) -> Result<Option<String>> {
        self.inner.signed_get_url(key, ttl).await
    }
    fn supports_compose(&self) -> bool {
        self.inner.supports_compose()
    }
    async fn compose(
        &self,
        dest: &str,
        sources: &[String],
        opts: PutOptions,
    ) -> Result<ObjectMeta> {
        let conditional = !matches!(opts.mode, PutMode::Overwrite);
        match self.decide("compose", dest, true, conditional, false).await {
            Decision::Hang => hang_forever().await,
            Decision::ErrBefore => Err(self.retryable("compose", dest, "before")),
            Decision::CasFail => Err(StoreError::PreconditionFailed {
                key: dest.into(),
                current: None,
            }),
            Decision::ErrAfter => {
                let _ = self.inner.compose(dest, sources, opts).await?;
                Err(self.retryable("compose", dest, "after (applied)"))
            }
            _ => self.inner.compose(dest, sources, opts).await,
        }
    }
}

/// Snapshot of an inner store's keys → sizes (for oracles that read the truth
/// behind every link). Works for any store; intended for `MemoryStore`.
pub async fn snapshot_keys(store: &DynStore, prefix: &str) -> Result<BTreeMap<String, u64>> {
    let mut out = BTreeMap::new();
    let mut s = store.list(prefix, None);
    while let Some(m) = s.next().await {
        let m = m?;
        out.insert(m.key, m.size);
    }
    Ok(out)
}

/// Read a whole object from the truth store (bypassing every link).
pub async fn truth_bytes(store: &DynStore, key: &str) -> Result<Option<Bytes>> {
    use crate::ObjectStoreExt;
    Ok(store.get_bytes(key).await?.map(|(_, b)| b))
}

impl FaultStore {
    async fn get_inner(&self, key: &str, opts: GetOptions, conditional: bool) -> Result<GetResult> {
        match self.decide("get", key, false, conditional, true).await {
            Decision::Hang => hang_forever().await,
            Decision::ErrBefore => Err(self.retryable("get", key, "before")),
            Decision::Denied => Err(StoreError::NotFound { key: key.into() }),
            Decision::Stale => Ok(GetResult::NotModified {
                version: opts.if_none_match.clone().ok_or_else(|| {
                    StoreError::other(anyhow::anyhow!(
                        "stale response needs a conditional version"
                    ))
                })?,
            }),
            Decision::Truncate(at) => match self.inner.get(key, opts).await? {
                GetResult::Object { meta, body } => {
                    let size = usize::try_from(meta.size).map_err(StoreError::other)?;
                    let at = if size == 0 { 0 } else { at % size };
                    let msg = format!(
                        "fault-store[{}]: injected truncation of {key} at {at}/{size}",
                        self.name
                    );
                    Ok(GetResult::Object {
                        meta,
                        body: truncate_stream(body, at, msg),
                    })
                }
                r @ GetResult::NotModified { .. } => Ok(r),
            },
            Decision::Proceed | Decision::ErrAfter | Decision::CasFail => {
                self.inner.get(key, opts).await
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ObjectStoreExt;
    use crate::memory::MemoryStore;

    #[tokio::test]
    async fn err_after_applies_the_write() {
        let truth: DynStore = MemoryStore::shared();
        let link = FaultStore::new(truth.clone(), "a", 1);
        link.set(FaultPlan {
            p_err_after: 1.0,
            ..Default::default()
        });
        let r = link.put_bytes("k", b"v".to_vec(), PutMode::Overwrite).await;
        assert!(matches!(r, Err(StoreError::Retryable(_))));
        assert_eq!(
            truth.get_bytes("k").await.unwrap().unwrap().1.as_ref(),
            b"v"
        );
        assert_eq!(link.stats().err_after.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn stale_304_hides_new_versions() {
        let truth: DynStore = MemoryStore::shared();
        let m1 = truth
            .put_bytes("k", b"1".to_vec(), PutMode::Overwrite)
            .await
            .unwrap();
        truth
            .put_bytes("k", b"2".to_vec(), PutMode::Overwrite)
            .await
            .unwrap();
        let link = FaultStore::new(truth.clone(), "a", 1);
        link.set(FaultPlan::stale_forever());
        let r = link.get_if_changed("k", &m1.version).await.unwrap();
        assert!(r.is_none(), "stale link must answer 304");
        link.heal();
        let r = link.get_if_changed("k", &m1.version).await.unwrap();
        assert_eq!(r.unwrap().1.as_ref(), b"2");
    }

    #[tokio::test]
    async fn truncation_errors_mid_body() {
        let truth: DynStore = MemoryStore::shared();
        truth
            .put_bytes("k", vec![7u8; 4096], PutMode::Overwrite)
            .await
            .unwrap();
        let link = FaultStore::new(truth.clone(), "a", 3);
        link.set(FaultPlan {
            p_truncate: 1.0,
            ..Default::default()
        });
        let r = link.get_bytes("k").await;
        assert!(r.is_err(), "truncated body must surface as an error: {r:?}");
    }

    #[tokio::test]
    async fn black_hole_hangs() {
        let truth: DynStore = MemoryStore::shared();
        let link = FaultStore::new(truth.clone(), "a", 3);
        link.set(FaultPlan::black_hole());
        let r = tokio::time::timeout(Duration::from_millis(50), link.head("k")).await;
        assert!(r.is_err());
    }
}
