//! Coordination primitives (CAS loop, Lease) built on [`crate::ObjectStore`].
//!
//! Every read of a repo starts with a freshness check on `manifest.pb`
//! (conditional GET). Mutations go through [`cas_update`], a generic
//! read-modify-write loop that re-reads on `PreconditionFailed` and backs off
//! on `Retryable`. Leases are protobuf objects under `leases/` acquired by
//! `Create` or by `Update` over an expired lease, renewed by CAS heartbeat.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use prost::Message;
use tokio::sync::Mutex;

use crate::{DynStore, ObjectMeta, ObjectStore, ObjectStoreExt, PutMode, StoreError, Version};
use walgit_proto::time;
use walgit_proto::v1::Lease;

/// Small clock-skew grace: only steal a lease once this much past its expiry,
/// so a holder whose clock is slightly ahead does not have its lease ripped
/// away while it is still legitimately active.
const LEASE_SKEW_TOLERANCE: Duration = Duration::from_secs(2);

#[derive(Debug, thiserror::Error)]
pub enum CoordError {
    #[error("store error: {0}")]
    Store(#[from] StoreError),
    #[error("protobuf decode error: {0}")]
    Decode(#[from] prost::DecodeError),
    #[error("operation aborted")]
    Aborted,
    #[error("lease lost")]
    LeaseLost,
    #[error("retries exhausted on {key} after {attempts} attempts")]
    RetriesExhausted { key: String, attempts: u32 },
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

// ---------------------------------------------------------------------------
// Generic CAS loop
// ---------------------------------------------------------------------------

/// Generic read-modify-write CAS loop on a protobuf object.
///
/// `f(None)` is called when the object is absent. Returning `None` from `f`
/// aborts with `Ok(None)`. Returning `Some(new)` writes `new` with `Create` if
/// the object was absent or `Update(version)` if it existed. On
/// `PreconditionFailed` the loop re-reads and retries (counted); on `Retryable`
/// it sleeps with jittered backoff then retries. After `max_retries` retries
/// the error is [`CoordError::RetriesExhausted`].
pub async fn cas_update<T, F>(
    store: &dyn ObjectStore,
    key: &str,
    max_retries: u32,
    mut f: F,
) -> Result<Option<(ObjectMeta, T)>, CoordError>
where
    T: prost::Message + Default,
    F: FnMut(Option<&T>) -> Result<Option<T>, CoordError>,
{
    let mut attempts: u32 = 0;
    loop {
        let current = get_message::<T>(store, key).await?;
        let new = match f(current.as_ref().map(|(_, t)| t)) {
            Ok(None) => return Ok(None),
            Ok(Some(new)) => new,
            Err(e) => return Err(e),
        };
        let mode = match &current {
            Some((meta, _)) => PutMode::Update(meta.version.clone()),
            None => PutMode::Create,
        };
        let encoded = new.encode_to_vec();
        match store.put_bytes(key, encoded, mode).await {
            Ok(meta) => return Ok(Some((meta, new))),
            Err(StoreError::PreconditionFailed { .. }) => {
                attempts += 1;
                if attempts > max_retries {
                    return Err(CoordError::RetriesExhausted {
                        key: key.to_string(),
                        attempts,
                    });
                }
                // re-read on next iteration
            }
            Err(StoreError::Retryable(_)) => {
                attempts += 1;
                if attempts > max_retries {
                    return Err(CoordError::RetriesExhausted {
                        key: key.to_string(),
                        attempts,
                    });
                }
                let d = crate::util::backoff(
                    attempts - 1,
                    Duration::from_millis(5),
                    Duration::from_millis(100),
                );
                tokio::time::sleep(d).await;
            }
            Err(e) => return Err(CoordError::Store(e)),
        }
    }
}

/// Read a protobuf object with its version. `Ok(None)` if absent.
pub async fn get_message<T>(
    store: &dyn ObjectStore,
    key: &str,
) -> Result<Option<(ObjectMeta, T)>, CoordError>
where
    T: prost::Message + Default,
{
    match store.get_bytes(key).await? {
        None => Ok(None),
        Some((meta, bytes)) => {
            let msg = T::decode(bytes)?;
            Ok(Some((meta, msg)))
        }
    }
}

/// Read a protobuf object only if its version changed since `known`.
/// `Ok(None)` if unchanged or absent.
pub async fn get_message_if_changed<T>(
    store: &dyn ObjectStore,
    key: &str,
    known: &Version,
) -> Result<Option<(ObjectMeta, T)>, CoordError>
where
    T: prost::Message + Default,
{
    match store.get_if_changed(key, known).await {
        Err(StoreError::NotFound { .. }) | Ok(None) => Ok(None),
        Err(e) => Err(CoordError::Store(e)),
        Ok(Some((meta, bytes))) => {
            let msg = T::decode(bytes)?;
            Ok(Some((meta, msg)))
        }
    }
}

// ---------------------------------------------------------------------------
// Lease
// ---------------------------------------------------------------------------

fn make_lease(holder: &str, purpose: &str, now: SystemTime, ttl: Duration, epoch: u64) -> Lease {
    Lease {
        holder: holder.to_string(),
        purpose: purpose.to_string(),
        acquired_at: Some(time::from_system(now)),
        expires_at: Some(time::from_system(now + ttl)),
        epoch,
    }
}

/// A held lease. Drop performs a best-effort release when inside a Tokio
/// runtime; call [`LeaseGuard::release`] for a confirmed release.
pub struct LeaseGuard {
    store: DynStore,
    key: String,
    holder: String,
    purpose: String,
    version: Version,
    expires_at: SystemTime,
    epoch: u64,
    /// Set by `release` / `Drop` so the other path is a no-op. Also read by the
    /// heartbeat task to know when to stop.
    released: AtomicBool,
}

impl LeaseGuard {
    #[expect(
        clippy::too_many_arguments,
        reason = "Lease construction collects its store identity and timing in one place"
    )]
    fn new(
        store: DynStore,
        key: &str,
        holder: &str,
        purpose: &str,
        version: Version,
        now: SystemTime,
        ttl: Duration,
        epoch: u64,
    ) -> Self {
        LeaseGuard {
            store,
            key: key.to_string(),
            holder: holder.to_string(),
            purpose: purpose.to_string(),
            version,
            expires_at: now + ttl,
            epoch,
            released: AtomicBool::new(false),
        }
    }

    /// CAS-extend `expires_at` and increment `epoch`. A `PreconditionFailed`
    /// (someone stole the lease) returns [`CoordError::LeaseLost`].
    pub async fn heartbeat(&mut self, ttl: Duration) -> Result<(), CoordError> {
        let now = SystemTime::now();
        self.epoch += 1;
        let lease = make_lease(&self.holder, &self.purpose, now, ttl, self.epoch);
        let encoded = lease.encode_to_vec();
        match self
            .store
            .put_bytes(&self.key, encoded, PutMode::Update(self.version.clone()))
            .await
        {
            Ok(meta) => {
                self.version = meta.version;
                self.expires_at = now + ttl;
                Ok(())
            }
            Err(StoreError::PreconditionFailed { .. }) => Err(CoordError::LeaseLost),
            Err(e) => Err(CoordError::Store(e)),
        }
    }

    /// Confirmed CAS delete. Consumes the guard. `Ok(())` even if the lease was
    /// already stolen (the desired end state — no one holds it in our name).
    pub async fn release(self) -> Result<(), CoordError> {
        self.released.store(true, Ordering::SeqCst);
        match self
            .store
            .delete(&self.key, Some(self.version.clone()))
            .await
        {
            Ok(()) | Err(StoreError::PreconditionFailed { .. } | StoreError::NotFound { .. }) => {
                Ok(())
            }
            Err(e) => Err(CoordError::Store(e)),
        }
    }

    /// Spawn a background task that calls [`heartbeat`](Self::heartbeat) every
    /// `every` with `ttl` until the guard is released or the lease is lost.
    ///
    /// Called as `LeaseGuard::spawn_heartbeat(guard, every, ttl)`. (Rust does
    /// not allow `Arc<Mutex<Self>>` as a `self` receiver, so this is an
    /// associated function rather than a method.)
    pub fn spawn_heartbeat(
        guard: Arc<Mutex<Self>>,
        every: Duration,
        ttl: Duration,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(every).await;
                let mut g = guard.lock().await;
                if g.released.load(Ordering::SeqCst) {
                    break;
                }
                match g.heartbeat(ttl).await {
                    Ok(()) => {}
                    Err(CoordError::LeaseLost) => {
                        g.released.store(true, Ordering::SeqCst);
                        tracing::warn!(key = %g.key, "lease lost during heartbeat");
                        break;
                    }
                    Err(e) => {
                        // Transient store error: keep trying; the store may recover.
                        tracing::debug!(key = %g.key, error = %e, "heartbeat transient error");
                    }
                }
            }
        })
    }

    pub fn holder(&self) -> &str {
        &self.holder
    }

    pub fn expires_at(&self) -> SystemTime {
        self.expires_at
    }
}

impl Drop for LeaseGuard {
    fn drop(&mut self) {
        if self.released.swap(true, Ordering::SeqCst) {
            return;
        }
        // Best-effort release when inside a Tokio runtime.
        let store = self.store.clone();
        let key = self.key.clone();
        let version = self.version.clone();
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            drop(handle.spawn(async move {
                let _ = store.delete(&key, Some(version)).await;
            }));
        }
    }
}

/// Try to acquire a lease. Returns `Ok(Some(guard))` on success, `Ok(None)` if
/// the lease is held by someone else and not expired.
pub async fn try_acquire(
    store: DynStore,
    key: &str,
    holder: &str,
    purpose: &str,
    ttl: Duration,
) -> Result<Option<LeaseGuard>, CoordError> {
    let now = SystemTime::now();
    let current = get_message::<Lease>(store.as_ref(), key).await?;
    match current {
        None => {
            let lease = make_lease(holder, purpose, now, ttl, 0);
            let encoded = lease.encode_to_vec();
            match store.put_bytes(key, encoded, PutMode::Create).await {
                Ok(meta) => Ok(Some(LeaseGuard::new(
                    store,
                    key,
                    holder,
                    purpose,
                    meta.version,
                    now,
                    ttl,
                    0,
                ))),
                Err(StoreError::PreconditionFailed { .. }) => Ok(None),
                Err(e) => Err(CoordError::Store(e)),
            }
        }
        Some((meta, existing)) => {
            let expires_at = existing
                .expires_at
                .as_ref()
                .map_or(UNIX_EPOCH, time::to_system);
            if now >= expires_at + LEASE_SKEW_TOLERANCE {
                let epoch = existing.epoch + 1;
                let lease = make_lease(holder, purpose, now, ttl, epoch);
                let encoded = lease.encode_to_vec();
                match store
                    .put_bytes(key, encoded, PutMode::Update(meta.version.clone()))
                    .await
                {
                    Ok(new_meta) => Ok(Some(LeaseGuard::new(
                        store,
                        key,
                        holder,
                        purpose,
                        new_meta.version,
                        now,
                        ttl,
                        epoch,
                    ))),
                    Err(StoreError::PreconditionFailed { .. }) => Ok(None),
                    Err(e) => Err(CoordError::Store(e)),
                }
            } else {
                Ok(None)
            }
        }
    }
}

/// Acquire a lease, polling with jittered backoff until `wait_up_to` elapses.
/// Returns `Ok(Some(guard))` on success, `Ok(None)` on timeout.
pub async fn acquire(
    store: DynStore,
    key: &str,
    holder: &str,
    purpose: &str,
    ttl: Duration,
    wait_up_to: Duration,
) -> Result<Option<LeaseGuard>, CoordError> {
    let deadline = tokio::time::Instant::now() + wait_up_to;
    let mut attempt: u32 = 0;
    loop {
        if let Some(guard) = try_acquire(store.clone(), key, holder, purpose, ttl).await? {
            return Ok(Some(guard));
        }
        let now = tokio::time::Instant::now();
        if now >= deadline {
            return Ok(None);
        }
        let sleep = crate::util::backoff(
            attempt,
            Duration::from_millis(10),
            Duration::from_millis(200),
        );
        let remaining = deadline.saturating_duration_since(now);
        tokio::time::sleep(sleep.min(remaining)).await;
        attempt += 1;
    }
}

/// A process-stable identity: explicit `WALGIT_INSTANCE_NAME`/`WALGIT_INSTANCE_ID`,
/// else `HOSTNAME`/pid, else a random UUID. Computed once and cached.
pub fn instance_id() -> &'static str {
    static ID: LazyLock<String> = LazyLock::new(|| {
        if let (Ok(name), Ok(inst)) = (
            std::env::var("WALGIT_INSTANCE_NAME"),
            std::env::var("WALGIT_INSTANCE_ID"),
        ) && !name.is_empty()
            && !inst.is_empty()
        {
            return format!("{name}/{inst}");
        }
        if let Ok(h) = std::env::var("HOSTNAME")
            && !h.is_empty()
        {
            return format!("{h}/{}", std::process::id());
        }
        uuid::Uuid::new_v4().to_string()
    });
    &ID
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::MemoryStore;
    use walgit_proto::v1::RepoCatalog;

    fn dyn_store() -> DynStore {
        MemoryStore::shared() as DynStore
    }

    #[tokio::test]
    async fn cas_update_convergence_64_incrementers() {
        const N: u32 = 64;

        let store = dyn_store();
        let key = "counter.pb";

        let mut handles = Vec::new();
        for i in 0..N {
            let s = store.clone();
            let k = key.to_string();
            handles.push(tokio::spawn(async move {
                let tag = format!("repo-{i}");
                cas_update::<RepoCatalog, _>(s.as_ref(), &k, 500, |current| {
                    let mut cat = current.cloned().unwrap_or_default();
                    cat.repos.push(tag.clone());
                    Ok(Some(cat))
                })
                .await
                .unwrap();
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        let (_, cat) = get_message::<RepoCatalog>(store.as_ref(), key)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(cat.repos.len(), N as usize);
    }

    #[tokio::test]
    async fn cas_update_abort_returns_none() {
        let store = dyn_store();
        let key = "abort.pb";
        let res = cas_update::<RepoCatalog, _>(store.as_ref(), key, 10, |_| Ok(None))
            .await
            .unwrap();
        assert!(res.is_none());
        // object was never created
        assert!(store.get_bytes(key).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn lease_exclusivity_32_concurrent() {
        const N: u32 = 32;

        let store = dyn_store();
        let key = "leases/excl.pb";

        let mut handles = Vec::new();
        for i in 0..N {
            let s = store.clone();
            let k = key.to_string();
            handles.push(tokio::spawn(async move {
                let holder = format!("h{i}");
                try_acquire(s, &k, &holder, "test", Duration::from_mins(1)).await
            }));
        }
        let mut successes = 0;
        for h in handles {
            if h.await.unwrap().unwrap().is_some() {
                successes += 1;
            }
        }
        assert_eq!(successes, 1);
    }

    #[tokio::test]
    async fn lease_steal_after_expiry() {
        let store = dyn_store();
        let key = "leases/steal.pb";

        let g1 = try_acquire(store.clone(), key, "h1", "test", Duration::from_millis(50))
            .await
            .unwrap()
            .unwrap();
        // Wait past expiry + skew tolerance.
        tokio::time::sleep(LEASE_SKEW_TOLERANCE + Duration::from_millis(100)).await;

        let g2 = try_acquire(store.clone(), key, "h2", "test", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(g2.holder(), "h2");

        // h1's CAS should now fail.
        let mut g1 = g1;
        let res = g1.heartbeat(Duration::from_secs(30)).await;
        assert!(matches!(res, Err(CoordError::LeaseLost)));
    }

    #[tokio::test]
    async fn lease_heartbeat_keeps_it() {
        let store = dyn_store();
        let key = "leases/hb.pb";
        let ttl = Duration::from_millis(100);

        let g = try_acquire(store.clone(), key, "h1", "test", ttl)
            .await
            .unwrap()
            .unwrap();
        let g = Arc::new(Mutex::new(g));
        let handle = LeaseGuard::spawn_heartbeat(g.clone(), Duration::from_millis(20), ttl);

        // Wait well past the original ttl.
        tokio::time::sleep(Duration::from_millis(400)).await;

        // Lease should still be held.
        let res = try_acquire(store.clone(), key, "h2", "test", Duration::from_secs(30))
            .await
            .unwrap();
        assert!(res.is_none());

        // Stop heartbeat and release.
        {
            let guard = g.lock().await;
            guard.released.store(true, Ordering::SeqCst);
        }
        handle.await.unwrap();
        let guard = Arc::try_unwrap(g).ok().expect("single ref").into_inner();
        guard.release().await.unwrap();
    }

    #[tokio::test]
    async fn lease_release_frees_it() {
        let store = dyn_store();
        let key = "leases/rel.pb";

        let g = try_acquire(store.clone(), key, "h1", "test", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        g.release().await.unwrap();

        let g2 = try_acquire(store.clone(), key, "h2", "test", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(g2.holder(), "h2");
    }

    #[tokio::test]
    async fn lease_lost_after_external_steal() {
        let store = dyn_store();
        let key = "leases/lost.pb";
        let ttl = Duration::from_millis(50);

        let mut g1 = try_acquire(store.clone(), key, "h1", "test", ttl)
            .await
            .unwrap()
            .unwrap();
        // Wait for expiry + skew, then steal from outside.
        tokio::time::sleep(LEASE_SKEW_TOLERANCE + Duration::from_millis(100)).await;
        let _g2 = try_acquire(store.clone(), key, "h2", "test", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();

        let res = g1.heartbeat(Duration::from_secs(30)).await;
        assert!(matches!(res, Err(CoordError::LeaseLost)));
    }

    #[tokio::test]
    async fn acquire_waits_then_succeeds() {
        let store = dyn_store();
        let key = "leases/acquire.pb";

        let g1 = try_acquire(store.clone(), key, "h1", "test", Duration::from_millis(50))
            .await
            .unwrap()
            .unwrap();
        // g1 holds with 50ms ttl + 2s skew tolerance ≈ 2050ms before stealable.
        // acquire with wait_up_to = 3s should eventually get it.
        let g2 = acquire(
            store.clone(),
            key,
            "h2",
            "test",
            Duration::from_secs(30),
            Duration::from_secs(3),
        )
        .await
        .unwrap()
        .expect("should acquire within 3s");
        assert_eq!(g2.holder(), "h2");
        // g1 is now stale; drop it (best-effort release will no-op on PreconditionFailed).
        drop(g1);
    }

    #[tokio::test]
    async fn acquire_times_out() {
        let store = dyn_store();
        let key = "leases/timeout.pb";

        let _g1 = try_acquire(store.clone(), key, "h1", "test", Duration::from_mins(1))
            .await
            .unwrap()
            .unwrap();
        let g2 = acquire(
            store.clone(),
            key,
            "h2",
            "test",
            Duration::from_secs(30),
            Duration::from_millis(200),
        )
        .await
        .unwrap();
        assert!(g2.is_none());
    }

    #[tokio::test]
    async fn get_message_if_changed_works() {
        let store = dyn_store();
        let key = "catalog.pb";

        // Absent => None.
        let res = get_message_if_changed::<RepoCatalog>(store.as_ref(), key, &Version::new("0"))
            .await
            .unwrap();
        assert!(res.is_none());

        // Create.
        let (meta, _) = cas_update::<RepoCatalog, _>(store.as_ref(), key, 10, |current| {
            assert!(current.is_none());
            let mut c = RepoCatalog::default();
            c.repos.push("a".into());
            Ok(Some(c))
        })
        .await
        .unwrap()
        .unwrap();

        // Same version => None (unchanged).
        let res = get_message_if_changed::<RepoCatalog>(store.as_ref(), key, &meta.version)
            .await
            .unwrap();
        assert!(res.is_none());

        // Different version => Some.
        let res = get_message_if_changed::<RepoCatalog>(store.as_ref(), key, &Version::new("0"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(res.1.repos, vec!["a".to_string()]);
    }

    #[test]
    fn instance_id_is_stable() {
        let a = instance_id();
        let b = instance_id();
        assert_eq!(a, b);
        assert!(!a.is_empty());
    }
}
