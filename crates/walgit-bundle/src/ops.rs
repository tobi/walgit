//! Core bundling operations: ref resolution, bundle creation, store upload,
//! bundle-list CAS management, pruning, and per-strategy leasing.
//!
//! The core functions take a [`LocalRepo`] + [`Prefixed`] store so they are
//! testable without the full WAL/Registry stack.

use std::collections::HashSet;
use std::path::Path;
use std::sync::LazyLock;
use std::time::{Duration, SystemTime};

use prost::Message;
use sha1::{Digest, Sha1};
use tracing::Instrument;
use tracing::{debug, warn};

use walgit_config::BundleKind;
use walgit_git::{GitError, LocalRepo, RefSnapshotData};
use walgit_proto::v1::{BundleEntry, BundleList, Lease, Ref};
use walgit_proto::{keys, time};
use walgit_store::{
    ObjectStore, ObjectStoreExt, Prefixed, PutBody, PutMode, PutOptions, StoreError, Version,
};

use crate::BundleError;

// ---------------------------------------------------------------------------
// Instance identity (for lease holder)
// ---------------------------------------------------------------------------

/// Per-process unique identifier for lease holding.
pub fn instance_id() -> &'static str {
    static ID: LazyLock<String> = LazyLock::new(|| {
        format!(
            "bundle-{}-{}",
            std::process::id(),
            chrono::Utc::now().timestamp()
        )
    });
    ID.as_str()
}

// ---------------------------------------------------------------------------
// Ref resolution / filtering
// ---------------------------------------------------------------------------

/// Check whether a ref name matches a glob-style pattern.
///
/// `refs/heads/*` matches `refs/heads/main`, `refs/heads/feature/x`.
/// `HEAD` matches only `HEAD`. Exact names match exactly.
fn matches_pattern(ref_name: &str, pattern: &str) -> bool {
    if pattern == "HEAD" {
        return ref_name == "HEAD";
    }
    if let Some(prefix) = pattern.strip_suffix("/*") {
        // Glob: prefix matches anything under that path segment.
        return ref_name == prefix || ref_name.starts_with(&format!("{prefix}/"));
    }
    ref_name == pattern
}

/// Filter refs from a [`RefSnapshotData`] by strategy ref patterns.
///
/// Empty patterns default to `refs/heads/*`, `refs/tags/*`, `HEAD`.
/// Returns `(ref_names_for_bundle, tips_for_entry)`.
///
/// `ref_names` includes `HEAD` if appropriate; `tips` are proto `Ref`s with
/// name + oid + peeled.
pub(crate) fn filter_refs(snap: &RefSnapshotData, patterns: &[String]) -> (Vec<String>, Vec<Ref>) {
    let effective: Vec<&str> = if patterns.is_empty() {
        vec!["refs/heads/*", "refs/tags/*", "HEAD"]
    } else {
        patterns.iter().map(|s| s.as_str()).collect()
    };

    let mut ref_names = Vec::new();
    let mut tips = Vec::new();

    for r in &snap.refs {
        if effective.iter().any(|p| matches_pattern(&r.name, p)) {
            ref_names.push(r.name.clone());
            tips.push(Ref {
                name: r.name.clone(),
                oid: r.oid.clone(),
                peeled: r.peeled.clone(),
            });
        }
    }

    // Include HEAD if requested and not already captured as a named ref.
    let want_head = effective.iter().any(|p| *p == "HEAD");
    if want_head && !ref_names.iter().any(|n| n == "HEAD") {
        // HEAD's oid = the oid of head_target (if set).
        if let Some(head_ref) = snap.refs.iter().find(|r| r.name == snap.head_target) {
            ref_names.push("HEAD".into());
            tips.push(Ref {
                name: "HEAD".into(),
                oid: head_ref.oid.clone(),
                peeled: String::new(),
            });
        }
    }

    (ref_names, tips)
}

// ---------------------------------------------------------------------------
// Bundle file creation
// ---------------------------------------------------------------------------

/// Create a bundle at `out`: header (tips, `-<prerequisite>` lines) + a
/// self-contained pack of exactly the objects reachable from `refs` minus the
/// prerequisites (gix engine: `write_bundle_gix`; git: `pack-objects --revs`,
/// never thin — see the comment in the body). Returns the bundle file size.
pub async fn create_bundle(
    local: &LocalRepo,
    engine: &crate::BundleEngine,
    out: &Path,
    refs: &[String],
    tips: &[Ref],
    prerequisites: &[String],
    filter: Option<&str>,
) -> Result<u64, BundleError> {
    if let crate::BundleEngine::Gix { faulter } = engine {
        if filter.is_some() {
            return Err(BundleError::Other(
                "filtered bundles need the stock git engine (gix engine cannot pack --filter)"
                    .into(),
            ));
        }
        let mut ref_oids = Vec::with_capacity(refs.len());
        for name in refs {
            let Some(t) = tips.iter().find(|t| &t.name == name) else {
                continue;
            };
            let oid = walgit_git::gix_hash::ObjectId::from_hex(t.oid.as_bytes())
                .map_err(|e| BundleError::Other(format!("bad tip {}: {e}", t.oid)))?;
            ref_oids.push((name.clone(), oid));
        }
        let mut prereqs = Vec::with_capacity(prerequisites.len());
        for p in prerequisites {
            prereqs.push(
                walgit_git::gix_hash::ObjectId::from_hex(p.as_bytes())
                    .map_err(|e| BundleError::Other(format!("bad prerequisite {p}: {e}")))?,
            );
        }
        let file = tokio::fs::File::create(out)
            .await
            .map_err(|e| BundleError::Io(e.to_string()))?;
        let mut w = tokio::io::BufWriter::new(file);
        let stats = local
            .write_bundle_gix(&mut w, &ref_oids, &prereqs, faulter.as_deref())
            .await
            .map_err(BundleError::Git)?;
        tokio::io::AsyncWriteExt::shutdown(&mut w)
            .await
            .map_err(|e| BundleError::Io(e.to_string()))?;
        if stats.objects == 0 {
            return Err(BundleError::NoNewObjects);
        }
        return Ok(std::fs::metadata(out).map(|m| m.len()).unwrap_or(0));
    }
    // Stock git: our own header + `pack-objects` WITHOUT `--thin`. `git bundle
    // create` always packs thin (deltas against the prerequisites' objects),
    // which is the wrong trade for a static incremental: measured on a large repository's
    // 07:00 hourly (2026-08-21, same object set) thin = 226.8 MB and a 48.4 s
    // client `index-pack` that appended 193 MB of bases (420 MB on disk);
    // self-contained = 314.9 MB (+39 %), 31.7 s (−35 %), 315 MB on disk. Static
    // bytes are the cheap resource (edge cache, CDN, bucket); client seconds and
    // client disk are not. Prerequisites still bound the object set exactly.
    let repo_path = local.path();
    let git = |args: &[&str]| {
        let mut c = tokio::process::Command::new("git");
        c.args(args)
            .current_dir(repo_path)
            .env("GIT_DIR", repo_path);
        c
    };
    // Resolve every ref to write the header (HEAD included, as git would).
    let mut header = bundle_prelude(local.object_format(), filter);
    // Prerequisites are commits (git peels `^<tag>` for `bundle create`; we do
    // the same) — deduped, non-commits dropped.
    let mut prereq_commits: Vec<String> = Vec::with_capacity(prerequisites.len());
    for p in prerequisites {
        let out = git(&["rev-parse", "--verify", "-q", &format!("{p}^{{commit}}")])
            .output()
            .await
            .map_err(|e| BundleError::Io(e.to_string()))?;
        if out.status.success() {
            let c = String::from_utf8_lossy(&out.stdout).trim().to_string();
            if !prereq_commits.contains(&c) {
                prereq_commits.push(c);
            }
        }
    }
    for p in &prereq_commits {
        // `-<oid> <comment>`: git reads the oid and ignores the rest.
        header.extend_from_slice(format!("-{p} walgit\n").as_bytes());
    }
    let mut revs = String::new();
    for r in refs {
        // The ref's own object (an annotated tag stays a tag: clients expect the
        // tag oid under refs/tags/*; the tag object itself is in the pack).
        let out = git(&["rev-parse", "--verify", "-q", r])
            .output()
            .await
            .map_err(|e| BundleError::Io(e.to_string()))?;
        if !out.status.success() {
            continue; // unresolvable tip: skipped (the caller already warned)
        }
        let oid = String::from_utf8_lossy(&out.stdout).trim().to_string();
        header.extend_from_slice(format!("{oid} {r}\n").as_bytes());
        revs.push_str(&oid);
        revs.push('\n');
    }
    header.push(b'\n');
    if revs.is_empty() {
        return Err(BundleError::NoRefs);
    }
    for p in &prereq_commits {
        revs.push('^');
        revs.push_str(p);
        revs.push('\n');
    }
    let mut po_args: Vec<String> = [
        "pack-objects",
        "--revs",
        "--delta-base-offset",
        "-q",
        "--stdout",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    if let Some(f) = filter {
        po_args.push(format!("--filter={f}"));
    }
    let po_args: Vec<&str> = po_args.iter().map(|s| s.as_str()).collect();
    let mut child = git(&po_args)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| BundleError::Io(e.to_string()))?;
    {
        use tokio::io::AsyncWriteExt;
        let mut stdin = child.stdin.take().ok_or_else(|| {
            BundleError::Io("git pack-objects stdin was not piped".to_string())
        })?;
        stdin
            .write_all(revs.as_bytes())
            .await
            .map_err(|e| BundleError::Io(e.to_string()))?;
        drop(stdin);
    }
    let mut file = tokio::io::BufWriter::new(
        tokio::fs::File::create(out)
            .await
            .map_err(|e| BundleError::Io(e.to_string()))?,
    );
    {
        use tokio::io::AsyncWriteExt;
        file.write_all(&header)
            .await
            .map_err(|e| BundleError::Io(e.to_string()))?;
    }
    let mut stdout = child.stdout.take().ok_or_else(|| {
        BundleError::Io("git pack-objects stdout was not piped".to_string())
    })?;
    let mut first = [0u8; 12];
    tokio::io::AsyncReadExt::read_exact(&mut stdout, &mut first)
        .await
        .map_err(|e| BundleError::Io(format!("pack header: {e}")))?;
    let [_, _, _, _, _, _, _, _, b0, b1, b2, b3] = first;
    let objects = u32::from_be_bytes([b0, b1, b2, b3]);
    {
        use tokio::io::AsyncWriteExt;
        file.write_all(&first)
            .await
            .map_err(|e| BundleError::Io(e.to_string()))?;
    }
    tokio::io::copy(&mut stdout, &mut file)
        .await
        .map_err(|e| BundleError::Io(e.to_string()))?;
    {
        use tokio::io::AsyncWriteExt;
        file.flush()
            .await
            .map_err(|e| BundleError::Io(e.to_string()))?;
    }
    let status = child
        .wait_with_output()
        .await
        .map_err(|e| BundleError::Io(e.to_string()))?;
    if !status.status.success() {
        return Err(BundleError::Git(GitError::Subprocess {
            cmd: "git pack-objects --revs (bundle)".into(),
            status: status.status.code(),
            stderr: String::from_utf8_lossy(&status.stderr).to_string(),
        }));
    }
    if objects == 0 {
        let _ = tokio::fs::remove_file(out).await;
        return Err(BundleError::NoNewObjects);
    }
    let meta = tokio::fs::metadata(out)
        .await
        .map_err(|e| BundleError::Io(e.to_string()))?;
    Ok(meta.len())
}

// ---------------------------------------------------------------------------
// Checksum and key generation
// ---------------------------------------------------------------------------

/// SHA-1 hex digest of the bundle file content (content-addressed key).
pub fn bundle_checksum(data: &[u8]) -> String {
    let mut hasher = Sha1::new();
    hasher.update(data);
    hex::encode(hasher.finalize())
}

/// [`bundle_checksum`] over a file, streamed.
pub fn bundle_checksum_file(path: &std::path::Path) -> std::io::Result<String> {
    use std::io::Read;
    let mut f = std::fs::File::open(path)?;
    let mut hasher = Sha1::new();
    let mut buf = vec![0u8; 1 << 20];
    loop {
        let n = f.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hex::encode(hasher.finalize()))
}

/// RFC 3339 compact timestamp: `20260818T120000Z`.
pub fn rfc3339_compact(now: SystemTime) -> String {
    let dt = chrono::DateTime::<chrono::Utc>::from(now);
    dt.format("%Y%m%dT%H%M%SZ").to_string()
}

/// Build the object key: `bundles/<strategy>/<compact>-<checksum>.bundle`.
pub fn bundle_key(strategy: &str, now: SystemTime, checksum: &str) -> String {
    format!(
        "bundles/{strategy}/{}-{checksum}.bundle",
        rfc3339_compact(now)
    )
}

// ---------------------------------------------------------------------------
// Bundle list read / CAS update
// ---------------------------------------------------------------------------

/// Read `bundles/list.pb` from the repo-scoped store. `Ok(None)` if absent.
pub async fn read_list(store: &Prefixed) -> Result<Option<BundleList>, BundleError> {
    match store.get_bytes(keys::BUNDLE_LIST).await? {
        Some((_, data)) => {
            let list = BundleList::decode(data.as_ref())?;
            Ok(Some(list))
        }
        None => Ok(None),
    }
}

/// Record a closed slot's verdict in the list (idempotent per (strategy, slot, base)).
pub async fn record_skipped(
    store: &Prefixed,
    strategy: &str,
    slot: u64,
    base_id: &str,
    as_of_seq: u64,
    reason: &str,
) -> Result<(), BundleError> {
    record_skipped_many(
        store,
        vec![walgit_proto::v1::SkippedSlot {
            strategy: strategy.to_string(),
            slot,
            base_id: base_id.to_string(),
            as_of_seq,
            reason: reason.to_string(),
            at: Some(time::now()),
        }],
    )
    .await
}

/// Upper bound on recorded verdicts: the plan only looks at slots inside the retention window
/// (≤ 1 weekly period of dailies, ≤ 1 daily period of hourlies, per family), so anything older is
/// irrelevant; the newest verdicts (by slot) are the ones kept.
pub const SKIPPED_KEPT: usize = 4096;

/// Record many verdicts in **one** CAS of the list (one settle pass = one round trip, however many
/// closed slots it judged; 2026-08-22 a rig repo settled 9,654 slots per pass, one CAS each, and the
/// verdicts beyond the cap never stuck, so every pass did it again).
pub async fn record_skipped_many(
    store: &Prefixed,
    verdicts: Vec<walgit_proto::v1::SkippedSlot>,
) -> Result<(), BundleError> {
    if verdicts.is_empty() {
        return Ok(());
    }
    let _ = cas_update_list(store, 8, move |cur| {
        let mut next = cur.cloned().unwrap_or_default();
        let mut added = false;
        for v in &verdicts {
            if next
                .skipped
                .iter()
                .any(|k| k.strategy == v.strategy && k.slot == v.slot && k.base_id == v.base_id)
            {
                continue;
            }
            next.skipped.push(v.clone());
            added = true;
        }
        if !added {
            return Ok(None);
        }
        if next.skipped.len() > SKIPPED_KEPT {
            next.skipped.sort_by_key(|k| k.slot);
            let drop = next.skipped.len() - SKIPPED_KEPT;
            next.skipped.drain(0..drop);
        }
        next.updated_at = Some(time::now());
        Ok(Some(next))
    })
    .await?;
    Ok(())
}

/// CAS read-modify-write loop on `bundles/list.pb`.
///
/// `f` receives `None` when the list is absent, `Some(&list)` when present.
/// Returning `None` from `f` aborts the update with `Ok(None)`.
pub async fn cas_update_list<F>(
    store: &Prefixed,
    max_retries: u32,
    mut f: F,
) -> Result<Option<(Version, BundleList)>, BundleError>
where
    F: FnMut(Option<&BundleList>) -> Result<Option<BundleList>, BundleError>,
{
    let key = keys::BUNDLE_LIST;
    for attempt in 0..max_retries {
        let current = store.get_bytes(key).await?;
        match current {
            None => match f(None)? {
                None => return Ok(None),
                Some(new_list) => {
                    let body = new_list.encode_to_vec();
                    match store
                        .put(key, PutBody::from(body), PutOptions::from(PutMode::Create))
                        .await
                    {
                        Ok(meta) => return Ok(Some((meta.version, new_list))),
                        Err(StoreError::PreconditionFailed { .. }) => {
                            debug!(attempt, "cas retry: list created by another writer");
                            continue;
                        }
                        Err(e) => return Err(e.into()),
                    }
                }
            },
            Some((meta, data)) => {
                let list = BundleList::decode(data.as_ref())?;
                match f(Some(&list))? {
                    None => return Ok(None),
                    Some(new_list) => {
                        let body = new_list.encode_to_vec();
                        match store
                            .put(
                                key,
                                PutBody::from(body),
                                PutOptions::from(PutMode::Update(meta.version.clone())),
                            )
                            .await
                        {
                            Ok(new_meta) => return Ok(Some((new_meta.version, new_list))),
                            Err(StoreError::PreconditionFailed { .. }) => {
                                debug!(attempt, "cas retry: list changed by another writer");
                                continue;
                            }
                            Err(e) => return Err(e.into()),
                        }
                    }
                }
            }
        }
    }
    Err(BundleError::RetriesExhausted)
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// Leasing
// ---------------------------------------------------------------------------

/// A held bundle-build lease. Drop is best-effort release; call [`release`]
/// for an awaited release.
pub struct LeaseGuard {
    store: Prefixed,
    key: String,
    version: Version,
    released: bool,
}

impl Drop for LeaseGuard {
    /// Best-effort release when the holder is dropped without `release()`
    /// (task cancelled by an instance shutdown): the next maintainer must not
    /// wait a TTL for a lease nobody holds.
    fn drop(&mut self) {
        if self.released {
            return;
        }
        let store = self.store.clone();
        let key = self.key.clone();
        let version = self.version.clone();
        if let Ok(h) = tokio::runtime::Handle::try_current() {
            h.spawn(async move {
                let _ = store.delete(&key, Some(version)).await;
            });
        }
    }
}

impl LeaseGuard {
    /// Release the lease (CAS delete).
    pub async fn release(mut self) -> Result<(), BundleError> {
        self.released = true;
        match self
            .store
            .delete(&self.key, Some(self.version.clone()))
            .await
        {
            Ok(()) => Ok(()),
            Err(StoreError::PreconditionFailed { .. }) | Err(StoreError::NotFound { .. }) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    /// CAS-extend the lease's expires_at (heartbeat).
    pub async fn heartbeat(&mut self, ttl: Duration) -> Result<(), BundleError> {
        let now = SystemTime::now();
        let expires = now + ttl;
        let lease = Lease {
            holder: instance_id().to_string(),
            purpose: "bundle".into(),
            acquired_at: Some(time::from_system(now)),
            expires_at: Some(time::from_system(expires)),
            epoch: 1,
        };
        match self
            .store
            .put(
                &self.key,
                PutBody::from(lease.encode_to_vec()),
                PutOptions::from(PutMode::Update(self.version.clone())),
            )
            .await
        {
            Ok(meta) => {
                self.version = meta.version;
                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }
}

/// Try to acquire a lease at `leases/bundle-<strategy>.pb`.
///
/// Returns `Ok(Some(guard))` if acquired, `Ok(None)` if held by another
/// non-expired holder.
pub async fn try_acquire_lease(
    store: &Prefixed,
    strategy: &str,
    ttl: Duration,
) -> Result<Option<LeaseGuard>, BundleError> {
    let key = format!("{}bundle-{strategy}.pb", keys::LEASES_DIR);
    let now = SystemTime::now();
    let expires = now + ttl;
    let lease = Lease {
        holder: instance_id().to_string(),
        purpose: format!("bundle-{strategy}"),
        acquired_at: Some(time::from_system(now)),
        expires_at: Some(time::from_system(expires)),
        epoch: 0,
    };
    let body = lease.encode_to_vec();

    // Try create-if-absent.
    match store
        .put(
            &key,
            PutBody::from(body.clone()),
            PutOptions::from(PutMode::Create),
        )
        .await
    {
        Ok(meta) => {
            return Ok(Some(LeaseGuard {
                store: store.clone(),
                key,
                version: meta.version,
                released: false,
            }));
        }
        Err(StoreError::PreconditionFailed { .. }) => {}
        Err(e) => return Err(e.into()),
    }

    // Already exists — check if expired and try to steal.
    match store.get_bytes(&key).await? {
        Some((meta, data)) => {
            let existing = Lease::decode(data.as_ref())?;
            let expired = existing
                .expires_at
                .as_ref()
                .map(|t| time::to_system(t) <= now)
                .unwrap_or(true);
            if !expired {
                return Ok(None);
            }
            match store
                .put(
                    &key,
                    PutBody::from(body),
                    PutOptions::from(PutMode::Update(meta.version.clone())),
                )
                .await
            {
                Ok(new_meta) => Ok(Some(LeaseGuard {
                    store: store.clone(),
                    key,
                    version: new_meta.version,
                    released: false,
                })),
                Err(StoreError::PreconditionFailed { .. }) => Ok(None),
                Err(e) => Err(e.into()),
            }
        }
        None => match store
            .put(&key, PutBody::from(body), PutOptions::from(PutMode::Create))
            .await
        {
            Ok(meta) => Ok(Some(LeaseGuard {
                store: store.clone(),
                key,
                version: meta.version,
                released: false,
            })),
            Err(StoreError::PreconditionFailed { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        },
    }
}

/// Manually hold a lease for a strategy (prevents builds). Used in tests
/// to verify lease skipping.
pub async fn hold_lease(
    store: &Prefixed,
    strategy: &str,
    holder: &str,
    ttl: Duration,
) -> Result<Version, BundleError> {
    let key = format!("{}bundle-{strategy}.pb", keys::LEASES_DIR);
    let now = SystemTime::now();
    let lease = Lease {
        holder: holder.to_string(),
        purpose: format!("bundle-{strategy}"),
        acquired_at: Some(time::from_system(now)),
        expires_at: Some(time::from_system(now + ttl)),
        epoch: 0,
    };
    let meta = store
        .put(
            &key,
            PutBody::from(lease.encode_to_vec()),
            PutOptions::from(PutMode::Create),
        )
        .await
        .map_err(|e| {
            if e.is_precondition_failed() {
                BundleError::Other("lease already held".into())
            } else {
                BundleError::from(e)
            }
        })?;
    Ok(meta.version)
}

// ---------------------------------------------------------------------------
// Full build: create bundle, upload, return entry
// ---------------------------------------------------------------------------

/// Build a bundle from `local`, upload it to `store`, and return the
/// [`BundleEntry`]. Does NOT update `bundles/list.pb` — the caller does that
/// via [`cas_update_list`] so pruning is atomic with the append.
/// What a bundle is cut for: a calendar slot with the ref state as of that
/// slot (`snapshot`, WAL `seq`), or "now" (legacy: token = max(prev+1, now)).
pub struct Cut {
    /// Slot epoch seconds = creation_token (0 = no slot: token from `now`).
    pub slot: u64,
    /// Ref state to cut from (None = the local copy's current refs).
    pub snapshot: Option<RefSnapshotData>,
    /// WAL seq the snapshot corresponds to.
    pub seq: u64,
}

#[allow(clippy::too_many_arguments)]
pub async fn build_and_upload(
    local: &LocalRepo,
    engine: &crate::BundleEngine,
    store: &Prefixed,
    strategy_name: &str,
    kind: BundleKind,
    ref_patterns: &[String],
    prerequisites: &[String],
    base_id: &str,
    cut: &Cut,
    prev_token: u64,
    now: SystemTime,
    filter: Option<&str>,
) -> Result<BundleEntry, BundleError> {
    // 1. Resolve refs (tips): the slot's ref state, or the local copy's.
    let snap = match &cut.snapshot {
        Some(s) => s.clone(),
        None => local.refs().map_err(|e| BundleError::Git(e))?,
    };
    let (ref_names, tips) = filter_refs(&snap, ref_patterns);
    // A tip whose object this copy cannot resolve (a ref published ahead of a
    // pack this instance has not installed, or a dangling ref in the snapshot)
    // must not fail the whole build: bundle what resolves, report the rest.
    let mut resolvable = std::collections::HashSet::new();
    let mut dangling = Vec::new();
    for t in &tips {
        match walgit_git::gix_hash::ObjectId::from_hex(t.oid.as_bytes()) {
            Ok(oid) if local.has_object(&oid) => {
                resolvable.insert(t.name.clone());
            }
            _ => dangling.push(format!("{} -> {}", t.name, t.oid)),
        }
    }
    if !dangling.is_empty() {
        warn!(strategy = strategy_name, count = dangling.len(), refs = ?dangling, "bundle: skipping refs whose tips are not in the local copy");
    }
    let ref_names: Vec<String> = ref_names
        .into_iter()
        .filter(|n| resolvable.contains(n))
        .collect();
    let tips: Vec<Ref> = tips
        .into_iter()
        .filter(|t| resolvable.contains(&t.name))
        .collect();
    if ref_names.is_empty() {
        return Err(BundleError::NoRefs);
    }

    // 2. Create bundle file in a tempdir.
    let tmp = tempfile::tempdir().map_err(|e| BundleError::Io(e.to_string()))?;
    let bundle_path = tmp.path().join("bundle.bundle");

    let engine_name = match engine {
        crate::BundleEngine::Gix { .. } => "gix",
        crate::BundleEngine::Git => "git",
    };
    let build_span = tracing::info_span!(
        "bundle.build",
        strategy = strategy_name,
        kind = match kind {
            BundleKind::Full => "full",
            BundleKind::Incremental => "incremental",
        },
        slot = cut.slot,
        seq = cut.seq,
        base = base_id,
        refs = ref_names.len(),
        prerequisites = prerequisites.len(),
        engine = engine_name,
        bytes = tracing::field::Empty,
        outcome = tracing::field::Empty
    );
    let t_build = std::time::Instant::now();
    let size = match create_bundle(
        local,
        engine,
        &bundle_path,
        &ref_names,
        &tips,
        prerequisites,
        filter,
    )
    .instrument(build_span.clone())
    .await
    {
        Ok(s) => {
            build_span.record("bytes", s);
            build_span.record("outcome", "ok");
            metrics::histogram!("walgit_bundle_build_seconds", "strategy" => strategy_name.to_string(), "kind" => match kind { BundleKind::Full => "full", BundleKind::Incremental => "incremental" }).record(t_build.elapsed().as_secs_f64());
            metrics::histogram!("walgit_bundle_build_bytes", "strategy" => strategy_name.to_string()).record(s);
            s
        }
        Err(BundleError::Git(GitError::Subprocess { stderr, .. }))
            if stderr.contains("empty bundle") =>
        {
            build_span.record("outcome", "empty");
            return Err(BundleError::NoNewObjects);
        }
        Err(e) => {
            build_span.record("outcome", "error");
            return Err(e);
        }
    };

    // 3. Checksum (streamed; a bundle can be GBs — never buffer it whole).
    let checksum = {
        let p = bundle_path.clone();
        tokio::task::spawn_blocking(move || bundle_checksum_file(&p))
            .await
            .map_err(|e| BundleError::Io(e.to_string()))?
            .map_err(|e| BundleError::Io(e.to_string()))?
    };

    // 4. Generate key and upload (immutable, create-if-absent).
    let key = bundle_key(strategy_name, now, &checksum);
    let kind_str = match kind {
        BundleKind::Full => "full",
        BundleKind::Incremental => "incremental",
    };
    let creation_token = if cut.slot > 0 {
        cut.slot
    } else {
        (prev_token + 1).max(crate::schedule::unix_now(now))
    };
    let bundle_id = format!("{strategy_name}-{creation_token}");

    let publish_span = tracing::info_span!("bundle.publish", strategy = strategy_name, key = %key, bytes = size, token = creation_token, slot = cut.slot);
    let version = match store
        .put(
            &key,
            PutBody::File(bundle_path.clone()),
            PutOptions {
                mode: PutMode::Create,
                immutable: true,
                content_type: Some("application/x-git-bundle"),
            },
        )
        .instrument(publish_span)
        .await
    {
        Ok(meta) => meta.version,
        Err(StoreError::PreconditionFailed { .. }) => {
            // Already uploaded (idempotent retry) — fetch existing version.
            store
                .head(&key)
                .await?
                .ok_or_else(|| BundleError::Other(format!("bundle vanished: {key}")))?
                .version
        }
        Err(e) => return Err(e.into()),
    };

    // 5. Build the entry.
    let entry = BundleEntry {
        id: bundle_id,
        key,
        strategy: strategy_name.to_string(),
        kind: kind_str.to_string(),
        creation_token,
        slot: cut.slot,
        seq: cut.seq,
        size,
        base_id: base_id.to_string(),
        filter: filter.unwrap_or("").to_string(),
        created_at: Some(time::from_system(now)),
        version: version.to_string(),
        tips,
    };

    debug!(strategy = strategy_name, kind = kind_str, key = %entry.key, slot = cut.slot, "bundle uploaded");
    Ok(entry)
}

/// Find the most recent bundle entry for `strategy` in `list`.
pub fn last_for_strategy<'a>(list: &'a BundleList, strategy: &str) -> Option<&'a BundleEntry> {
    list.bundles
        .iter()
        .filter(|b| b.strategy == strategy)
        .max_by_key(|b| b.creation_token)
}

/// The newest built incremental of `strategy` on `base_id` at or before
/// `slot` whose tip set equals `tips` (name + oid) — i.e. cutting `slot` would
/// reproduce it byte for byte. Idle nights/weekends otherwise cut 23–48
/// identical 315 MB hourlies a day on a large repository (2026-08-21: 08:00/09:00/10:00).
/// Clients are unaffected: git stops at the first bundle whose prerequisites
/// it has, and a stale `fetch.bundleCreationToken` simply finds nothing newer.
pub fn unchanged_since<'a>(
    list: &'a BundleList,
    strategy: &str,
    base_id: &str,
    slot: u64,
    tips: &[Ref],
) -> Option<&'a BundleEntry> {
    let prev = list
        .bundles
        .iter()
        .filter(|b| b.strategy == strategy && b.base_id == base_id && b.slot > 0 && b.slot <= slot)
        .max_by_key(|b| b.slot)?;
    let mut a: Vec<(&str, &str)> = prev
        .tips
        .iter()
        .map(|t| (t.name.as_str(), t.oid.as_str()))
        .collect();
    let mut b: Vec<(&str, &str)> = tips
        .iter()
        .map(|t| (t.name.as_str(), t.oid.as_str()))
        .collect();
    a.sort_unstable();
    b.sort_unstable();
    (a == b).then_some(prev)
}

/// Max creation_token across all entries in `list` (0 if empty).
pub fn max_creation_token(list: &BundleList) -> u64 {
    list.bundles
        .iter()
        .map(|b| b.creation_token)
        .max()
        .unwrap_or(0)
}

/// Delete objects for pruned bundle keys (best-effort, after CAS succeeds).
pub async fn delete_pruned(store: &Prefixed, keys_to_delete: &[String]) {
    let span = tracing::info_span!("bundle.retention", pruned = keys_to_delete.len());
    delete_pruned_inner(store, keys_to_delete)
        .instrument(span)
        .await
}

async fn delete_pruned_inner(store: &Prefixed, keys_to_delete: &[String]) {
    for key in keys_to_delete {
        if let Err(e) = store.delete(key, None).await {
            warn!(key, error = %e, "failed to delete pruned bundle object");
        }
    }
}

/// Compute the set of keys present in `old` but absent from `new` — i.e.,
/// entries that were pruned during the CAS update.
pub fn pruned_diff(old: &BundleList, new: &BundleList) -> Vec<String> {
    let new_keys: HashSet<&str> = new.bundles.iter().map(|b| b.key.as_str()).collect();
    old.bundles
        .iter()
        .map(|b| b.key.clone())
        .filter(|k| !new_keys.contains(k.as_str()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc3339_compact_format() {
        let t = std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_700_000_000);
        assert_eq!(rfc3339_compact(t), "20231114T221320Z");
    }

    #[test]
    fn checksum_deterministic() {
        let data = b"hello world";
        let c1 = bundle_checksum(data);
        let c2 = bundle_checksum(data);
        assert_eq!(c1, c2);
        assert_eq!(c1.len(), 40);
    }

    #[test]
    fn bundle_key_format() {
        let t = std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_700_000_000);
        assert_eq!(
            bundle_key("weekly", t, "abc123"),
            "bundles/weekly/20231114T221320Z-abc123.bundle"
        );
    }

    #[test]
    fn pattern_matching() {
        assert!(matches_pattern("refs/heads/main", "refs/heads/*"));
        assert!(matches_pattern("refs/heads/feature/x", "refs/heads/*"));
        assert!(!matches_pattern("refs/tags/v1", "refs/heads/*"));
        assert!(matches_pattern("HEAD", "HEAD"));
        assert!(!matches_pattern("refs/heads/main", "HEAD"));
        assert!(matches_pattern("refs/heads/main", "refs/heads/main"));
        assert!(!matches_pattern("refs/heads/dev", "refs/heads/main"));
    }
}

// ---------------------------------------------------------------------------
// Full bundle = header ∘ base pack via server-side compose (no disk, no
// index-pack, no bytes through the builder). Used by `walgit import --direct`
// and the weekly large-repository job (`walgit bundle compose`): the base pack built at
// WAL seq S contains every object reachable from the refs at S, so the header
// must carry the refs *at S* (the checkpoint's snapshot), never today's.
// ---------------------------------------------------------------------------

/// Bundle header bytes + the tips it lists (HEAD, refs/heads/*, refs/tags/*).
/// Bundle header prelude: v2 for sha1 without a filter, v3 with capabilities
/// otherwise (`@object-format=sha256`, `@filter=blob:none` — git unbundles a
/// filtered bundle with `index-pack --promisor=from-bundle`).
pub fn bundle_prelude(format: walgit_git::ObjectFormat, filter: Option<&str>) -> Vec<u8> {
    let sha256 = matches!(format, walgit_git::ObjectFormat::Sha256);
    if !sha256 && filter.is_none() {
        return b"# v2 git bundle\n".to_vec();
    }
    let mut h = b"# v3 git bundle\n".to_vec();
    if sha256 {
        h.extend_from_slice(b"@object-format=sha256\n");
    }
    if let Some(f) = filter {
        h.extend_from_slice(format!("@filter={f}\n").as_bytes());
    }
    h
}

pub fn full_bundle_header(
    snap: &walgit_proto::v1::RefSnapshot,
    format: walgit_git::ObjectFormat,
    filter: Option<&str>,
) -> (Vec<u8>, Vec<Ref>) {
    let mut h = bundle_prelude(format, filter);
    let mut tips = Vec::new();
    if let Some(head) = snap.refs.iter().find(|r| r.name == snap.head_target) {
        h.extend_from_slice(format!("{} HEAD\n", head.oid).as_bytes());
        tips.push(Ref {
            name: "HEAD".into(),
            oid: head.oid.clone(),
            peeled: String::new(),
        });
    }
    for r in &snap.refs {
        if r.name.starts_with("refs/heads/") || r.name.starts_with("refs/tags/") {
            h.extend_from_slice(format!("{} {}\n", r.oid, r.name).as_bytes());
            tips.push(r.clone());
        }
    }
    h.push(b'\n');
    (h, tips)
}

/// Publish `bundles/<strategy>/<stamp>-<pack>.bundle` = header ∘ `wal/<pack>.pack`
/// by compose (falls back to streaming header + `pack_path` when the store
/// cannot compose; then `pack_path` must be a local file) and return the entry
/// (not yet in the list — see [`cas_update_list`]).
pub async fn compose_full(
    store: &Prefixed,
    pack_checksum: &str,
    pack_size: u64,
    pack_path: Option<&Path>,
    snap: &walgit_proto::v1::RefSnapshot,
    format: walgit_git::ObjectFormat,
    strategy: &str,
    seq: u64,
    prev_token: u64,
    now: SystemTime,
    slot: u64,
    filter: Option<&str>,
) -> Result<BundleEntry, BundleError> {
    let (header, tips) = full_bundle_header(snap, format, filter);
    if tips.is_empty() {
        return Err(BundleError::NoRefs);
    }
    let span = tracing::info_span!(
        "bundle.compose",
        strategy,
        slot,
        seq,
        base_checksum = pack_checksum,
        bytes = pack_size,
        refs = tips.len()
    );
    compose_full_inner(
        store,
        pack_checksum,
        pack_size,
        pack_path,
        header,
        tips,
        strategy,
        seq,
        prev_token,
        now,
        slot,
        filter,
    )
    .instrument(span)
    .await
}

#[allow(clippy::too_many_arguments)]
async fn compose_full_inner(
    store: &Prefixed,
    pack_checksum: &str,
    pack_size: u64,
    pack_path: Option<&Path>,
    header: Vec<u8>,
    tips: Vec<Ref>,
    strategy: &str,
    seq: u64,
    prev_token: u64,
    now: SystemTime,
    slot: u64,
    filter: Option<&str>,
) -> Result<BundleEntry, BundleError> {
    let creation_token = if slot > 0 {
        slot
    } else {
        (prev_token + 1).max(crate::schedule::unix_now(now))
    };
    let key = format!(
        "bundles/{strategy}/{}-{pack_checksum}.bundle",
        rfc3339_compact(now)
    );
    let pack_key = keys::pack_key(pack_checksum);
    let opts = PutOptions {
        mode: PutMode::Overwrite,
        immutable: true,
        content_type: Some("application/x-git-bundle"),
    };
    let meta = if store.supports_compose() {
        let hdr_key = format!("{key}.hdr");
        store
            .put(
                &hdr_key,
                PutBody::Bytes(header.clone().into()),
                PutOptions::from(PutMode::Overwrite),
            )
            .await?;
        let m = store
            .compose(&key, &[hdr_key.clone(), pack_key], opts)
            .await?;
        let _ = store.delete(&hdr_key, None).await;
        m
    } else {
        let Some(path) = pack_path else {
            return Err(BundleError::Other(
                "store cannot compose and the base pack is not a local file".into(),
            ));
        };
        let len = header.len() as u64 + pack_size;
        let stream = futures::StreamExt::boxed(futures::StreamExt::chain(
            walgit_store::util::once(header.clone().into()),
            walgit_store::util::file_stream(path.to_path_buf(), None, 1024 * 1024),
        ));
        store
            .put(&key, PutBody::Stream { len, stream }, opts)
            .await?
    };
    Ok(BundleEntry {
        id: format!("{strategy}-{creation_token}"),
        key,
        strategy: strategy.to_string(),
        kind: "full".into(),
        creation_token,
        slot: if slot > 0 { slot } else { 0 },
        seq,
        size: meta.size,
        base_id: String::new(),
        filter: filter.unwrap_or("").to_string(),
        created_at: Some(time::from_system(now)),
        version: meta.version.to_string(),
        tips,
    })
}

/// Commits reachable from `tips` and not from `prerequisites`
/// (`git rev-list --count`): the minimum-size gate's measure. Commits/trees are
/// local on every host (history pack / small packs) and the commit-graph makes
/// this a graph walk, never a pack read.
pub(crate) async fn count_commits(
    local: &LocalRepo,
    tips: &[String],
    prerequisites: &[String],
) -> Result<u64, BundleError> {
    if tips.is_empty() {
        return Ok(0);
    }
    let repo_path = local.path();
    let mut cmd = tokio::process::Command::new("git");
    cmd.arg("rev-list").arg("--count");
    for t in tips {
        cmd.arg(t);
    }
    cmd.arg("--not");
    for p in prerequisites {
        cmd.arg(p);
    }
    cmd.current_dir(repo_path).env("GIT_DIR", repo_path);
    let out = cmd
        .output()
        .await
        .map_err(|e| BundleError::Io(e.to_string()))?;
    if !out.status.success() {
        return Err(BundleError::Git(GitError::Subprocess {
            cmd: "git rev-list --count".into(),
            status: out.status.code(),
            stderr: String::from_utf8_lossy(&out.stderr).to_string(),
        }));
    }
    Ok(String::from_utf8_lossy(&out.stdout)
        .trim()
        .parse()
        .unwrap_or(0))
}
