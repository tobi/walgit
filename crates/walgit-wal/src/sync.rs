//! sync() implementation: freshness check, catch-up, materialization.

use std::sync::Arc;

use crate::error::WalError;
use crate::store_proto::{get_message, get_message_if_changed};
use tracing::Instrument;
use walgit_git::LocalRepo;
use walgit_proto::keys;
use walgit_proto::v1::{EntryKind, LogEntry, Manifest, PackRef, RefSnapshot};
use walgit_store::{GetOptions, GetResult, ObjectStore, Prefixed, Version};

/// A read guard held for the lifetime of a request. While any guard is alive
/// no pack is removed locally (the inner RwLock read guard prevents it).
pub struct ReadGuard<'a> {
    pub(crate) _guard: tokio::sync::RwLockReadGuard<'a, ()>,
    pub(crate) handle: &'a super::handle::RepoHandle,
}

impl<'a> ReadGuard<'a> {
    pub fn manifest(&self) -> Arc<Manifest> {
        self.handle.manifest.read().clone()
    }
    pub fn local(&self) -> &LocalRepo {
        &self.handle.local
    }
}

fn symlink_file(target: &std::path::Path, link: &std::path::Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(target, link)
    }
    #[cfg(windows)]
    {
        if std::os::windows::fs::symlink_file(target, link).is_ok() {
            return Ok(());
        }
        std::fs::copy(target, link).map(|_| ())
    }
}

fn write_all_at(file: &std::fs::File, buf: &[u8], offset: u64) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        file.write_all_at(buf, offset)
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::FileExt;
        let mut written = 0usize;
        while written < buf.len() {
            let n = file.seek_write(&buf[written..], offset + written as u64)?;
            if n == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "write_all_at",
                ));
            }
            written += n;
        }
        Ok(())
    }
}

/// How much of the WAL a sync must bring to the local copy.
///
/// `Refs` applies the checkpoint ref snapshot and every log entry's ref
/// transaction but downloads no packs: enough for `info/refs`, `ls-refs`,
/// `bundle-uri` and the web `refs` endpoint, i.e. everything a cold instance
/// must answer instantly. `Full` additionally reconciles the local pack set
/// with `Manifest.packs` (download missing, drop superseded) and is required
/// before serving or verifying objects (upload-pack, receive-pack, compaction).
///
/// `Serve` is `Full` for everything that fits an instance, except tier-2
/// base packs when `cache.store_mount` is configured: those keep their
/// side-files (idx/rev/bitmap/commit-graph) local and point `pack-<sha>.pack`
/// at the mounted bucket object, so a 32 GB base never lands on tmpfs while
/// git still resolves any object. Without a mount `Serve` == `Full`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum SyncLevel {
    Refs,
    Serve,
    Full,
}

impl SyncLevel {
    /// Whether this level reconciles the local pack set.
    pub fn wants_packs(self) -> bool {
        self >= SyncLevel::Serve
    }
}

/// How one live pack is served on this instance (see `RepoHandle::serve_plan`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PackPlan {
    /// Real local copy of `.pack` + side-files.
    Local,
    /// Side-files local, `.pack` a symlink to this path in the store mount.
    Link(std::path::PathBuf),
    /// Only the commit-graph layer local; data via the remote reader.
    Remote,
}

impl PackPlan {
    /// Bytes this plan puts on tmpfs for `p`.
    pub fn tmpfs_bytes(&self, p: &PackRef) -> u64 {
        match self {
            PackPlan::Local => p.pack_size + p.idx_size,
            PackPlan::Link(_) => p.idx_size,
            PackPlan::Remote => 0,
        }
    }
}

/// Result of a sync operation, holding either a read guard (common case) or
/// indicating the repo was not found.
pub(crate) enum SyncOutcome {
    Unchanged,
    Changed {
        meta_version: Version,
        manifest: Manifest,
    },
}

/// Perform a conditional GET on manifest.pb and return the outcome.
pub(crate) async fn freshness_check(
    store: &Prefixed,
    known: &Option<Version>,
) -> Result<SyncOutcome, WalError> {
    match known {
        Some(v) => match get_message_if_changed::<Manifest>(store, keys::MANIFEST, v).await? {
            None => Ok(SyncOutcome::Unchanged),
            Some((meta, manifest)) => Ok(SyncOutcome::Changed {
                meta_version: meta.version,
                manifest,
            }),
        },
        None => match get_message::<Manifest>(store, keys::MANIFEST).await? {
            None => Err(WalError::NotFound),
            Some((meta, manifest)) => Ok(SyncOutcome::Changed {
                meta_version: meta.version,
                manifest,
            }),
        },
    }
}

/// Download a pack+idx from the store and install it into the local repo.
/// Uses a temp file, then `install_pack` (atomic rename).
pub(crate) async fn download_and_install_pack(
    store: &Prefixed,
    local: &LocalRepo,
    pack: &PackRef,
    tmp_dir: &std::path::Path,
    progress: Option<ProgressFn<'_>>,
) -> Result<(), WalError> {
    let span = tracing::info_span!(
        "wal.download_pack",
        checksum = %pack.checksum,
        bytes = pack.pack_size,
        objects = pack.object_count,
    );
    // Instrument awaited futures instead of carrying a thread-local span guard
    // across await points.

    let checksum = &pack.checksum;
    let pack_key = keys::pack_key(checksum);
    let idx_key = keys::idx_key(checksum);

    // Skip if already installed locally.
    let oid = gix_hash::ObjectId::from_hex(checksum.as_bytes())
        .map_err(|e| WalError::Corrupt(format!("invalid pack checksum {checksum}: {e}")))?;
    if local.pack_path(&oid).exists() {
        // Already installed; only fetch side files the manifest now advertises
        // (a base "promotion" publishes the same pack with a bitmap).
        for (flag, ext, key) in side_files(pack) {
            if !flag {
                continue;
            }
            let dest = local.pack_path(&oid).with_extension(ext);
            if dest.exists() {
                continue;
            }
            let tmp = tmp_dir.join(format!("{checksum}.{ext}"));
            if let Err(e) = download_object(store, &key, &tmp, None, None)
                .instrument(span.clone())
                .await
            {
                tracing::warn!(checksum = %checksum, ext, error = %e, "side file download failed");
                continue;
            }
            if std::fs::rename(&tmp, &dest).is_err() {
                let _ = std::fs::copy(&tmp, &dest);
                let _ = std::fs::remove_file(&tmp);
            }
        }
        return Ok(());
    }

    let pack_path = tmp_dir.join(format!("pack-{checksum}.pack"));
    let idx_path = tmp_dir.join(format!("pack-{checksum}.idx"));

    // Pack + idx + advertised side-files in one round: they are independent
    // immutable objects. Sequential was pack then idx then each side-file
    // (measurements: a 2.1 GB idx at ~10 MB/s *then* the 0.4 GB bitmap at
    // 40 MB/s). Each download is already striped.
    let mut extra = Vec::new();
    let mut side_futs = Vec::new();
    for (flag, ext, key) in side_files(pack) {
        if !flag {
            continue;
        }
        let path = tmp_dir.join(format!("pack-{checksum}.{ext}"));
        extra.push(path.clone());
        side_futs.push(
            async move { download_object(store, &key, &path, None, None).await }
                .instrument(span.clone()),
        );
    }
    let (pack_r, idx_r, side_rs) = tokio::join!(
        download_object(
            store,
            &pack_key,
            &pack_path,
            nonzero(pack.pack_size),
            progress
        )
        .instrument(span.clone()),
        download_object(store, &idx_key, &idx_path, nonzero(pack.idx_size), progress)
            .instrument(span.clone()),
        futures::future::join_all(side_futs),
    );
    pack_r?;
    idx_r?;
    for r in side_rs {
        r?;
    }

    local
        .install_pack(&pack_path, &idx_path, &extra)
        .instrument(span.clone())
        .await?;
    if pack.kind == walgit_proto::v1::PackKind::History as i32 {
        local.mark_history_pack(&oid, &pack.derived_from).await?;
        tracing::info!(checksum = %checksum, base = %pack.derived_from, bytes = pack.pack_size, "history pack installed (commits + trees local)");
    }
    Ok(())
}

/// `<mount>/<store prefix>/<repo prefix>/wal/<sha>.pack` (see `RepoHandle::mount_dir`).
pub(crate) fn mount_pack_path(
    mount_repo_dir: &std::path::Path,
    checksum: &str,
) -> std::path::PathBuf {
    mount_repo_dir.join(keys::pack_key(checksum))
}

/// Serve-level install of a base pack: side-files downloaded onto tmpfs, the
/// `.pack` itself a symlink into the mounted bucket (`target`). If the pack
/// was already installed as a real copy nothing changes.
pub(crate) async fn link_and_install_pack(
    store: &Prefixed,
    local: &LocalRepo,
    pack: &PackRef,
    tmp_dir: &std::path::Path,
    target: &std::path::Path,
) -> Result<(), WalError> {
    let checksum = &pack.checksum;
    let oid = gix_hash::ObjectId::from_hex(checksum.as_bytes())
        .map_err(|e| WalError::Corrupt(format!("invalid pack checksum {checksum}: {e}")))?;
    if local.pack_path(&oid).exists() {
        return Ok(());
    }
    let span =
        tracing::info_span!("wal.link_pack", checksum = %checksum, target = %target.display());
    let idx_path = tmp_dir.join(format!("pack-{checksum}.idx"));
    // The remote reader may already hold this index (web API on the same
    // instance): same bytes, hard-link instead of a second 2 GB download.
    let remote_idx = crate::remote::idx_dir(local.path()).join(format!("{checksum}.idx"));
    let mut extra = Vec::new();
    let mut side_futs = Vec::new();
    // Idx + rev + bitmap + commit-graph in one round (each already striped).
    // Sequential idx-then-sides left the 2.1 GB idx on the critical path
    // alone (measurements: 10 MB/s idx, then 40 MB/s bitmap).
    for (flag, ext, key) in side_files(pack) {
        if !flag {
            continue;
        }
        let path = tmp_dir.join(format!("pack-{checksum}.{ext}"));
        extra.push(path.clone());
        side_futs.push(
            async move { download_object(store, &key, &path, None, None).await }
                .instrument(span.clone()),
        );
    }
    let idx_r = if remote_idx.is_file()
        && (std::fs::hard_link(&remote_idx, &idx_path).is_ok()
            || std::fs::copy(&remote_idx, &idx_path).is_ok())
    {
        tracing::info!(checksum = %checksum, "pack index reused from the remote reader");
        let side_rs = futures::future::join_all(side_futs).await;
        for r in side_rs {
            r?;
        }
        Ok(())
    } else {
        let idx_key = keys::idx_key(checksum);
        let (idx_r, side_rs) = tokio::join!(
            download_object(store, &idx_key, &idx_path, nonzero(pack.idx_size), None)
                .instrument(span.clone()),
            futures::future::join_all(side_futs),
        );
        for r in side_rs {
            r?;
        }
        idx_r
    };
    idx_r?;
    let link = tmp_dir.join(format!("pack-{checksum}.pack"));
    let _ = std::fs::remove_file(&link);
    symlink_file(target, &link)?;
    local
        .install_pack(&link, &idx_path, &extra)
        .instrument(span.clone())
        .await?;
    tracing::info!(checksum = %checksum, target = %target.display(), "base pack linked from store mount");
    // A history pack already installed for this base: the midx must now
    // cover both (history preferred) — see LocalRepo::write_history_midx.
    if local
        .packs()?
        .iter()
        .any(|p| p.history_of.as_deref() == Some(checksum.as_str()))
    {
        local.write_history_midx().await?;
    }
    Ok(())
}

/// `(advertised, extension, store key)` for every side-file a pack may carry.
fn side_files(pack: &PackRef) -> [(bool, &'static str, String); 3] {
    let c = &pack.checksum;
    [
        (pack.has_rev, "rev", keys::rev_key(c)),
        (pack.has_bitmap, "bitmap", keys::bitmap_key(c)),
        (
            pack.has_commit_graph,
            "commit-graph",
            keys::commit_graph_key(c),
        ),
    ]
}

/// Download an object to `dest`. Small objects stream straight to the file;
/// large ones (packs) are fetched as concurrent range reads written at their
/// offsets (object stores deliver ~100 MB/s per connection; striping gets the
/// NIC's worth), with bounded memory (PAR * CHUNK).
/// `progress(delta_bytes, total_bytes)` is called as chunks land (callers
/// throttle). `known_size` skips the happy-path HEAD (ROUNDTRIPS: HEAD ≈ GET;
/// PackRef already carries pack/idx sizes).
pub(crate) type ProgressFn<'a> = &'a (dyn Fn(u64, u64) + Send + Sync);

fn nonzero(n: u64) -> Option<u64> {
    (n > 0).then_some(n)
}

pub(crate) async fn download_object(
    store: &Prefixed,
    key: &str,
    dest: &std::path::Path,
    known_size: Option<u64>,
    progress: Option<ProgressFn<'_>>,
) -> Result<(), WalError> {
    use futures::{StreamExt, TryStreamExt};
    const CHUNK: u64 = 32 * 1024 * 1024;
    // 16 stripes in flight: one gRPC stream tops out around 10–20 MB/s from
    // a serverless host, the NIC well beyond 100 MB/s (a large repository's 2.1 GB idx took 217 s
    // on the broker with 8).
    const PAR: usize = 16;

    let size = match known_size {
        Some(n) => n,
        None => match store.head(key).await? {
            Some(m) => m.size,
            None => {
                return Err(WalError::Store(walgit_store::StoreError::NotFound {
                    key: key.to_string(),
                }));
            }
        },
    };
    let report = |n: u64| {
        if let Some(p) = progress {
            p(n, size);
        }
    };
    if size <= CHUNK {
        let res = store.get(key, GetOptions::default()).await?;
        return match res {
            GetResult::Object { body, .. } => {
                let mut file = tokio::fs::File::create(dest).await?;
                let mut body = body;
                while let Some(chunk) = body.next().await {
                    let chunk = chunk?;
                    tokio::io::AsyncWriteExt::write_all(&mut file, &chunk).await?;
                    report(chunk.len() as u64);
                }
                tokio::io::AsyncWriteExt::flush(&mut file).await?;
                Ok(())
            }
            GetResult::NotModified { .. } => {
                Err(WalError::Corrupt(format!("unexpected 304 for {key}")))
            }
        };
    }
    let file = std::fs::File::create(dest)?;
    file.set_len(size)?;
    let file = std::sync::Arc::new(file);
    let starts: Vec<u64> = (0..size).step_by(CHUNK as usize).collect();
    let report = &report;
    futures::stream::iter(starts)
        .map(|start| {
            let file = file.clone();
            let end = (start + CHUNK).min(size);
            async move {
                let res = store
                    .get(
                        key,
                        GetOptions {
                            range: Some(start..end),
                            ..Default::default()
                        },
                    )
                    .await?;
                let body = match res {
                    GetResult::Object { body, .. } => body,
                    GetResult::NotModified { .. } => {
                        return Err(WalError::Corrupt(format!("unexpected 304 for {key}")));
                    }
                };
                let bytes = walgit_store::util::collect(body, (end - start) as usize).await?;
                if bytes.len() as u64 != end - start {
                    return Err(WalError::Corrupt(format!(
                        "short range read for {key}: {}..{} got {}",
                        start,
                        end,
                        bytes.len()
                    )));
                }
                let n = bytes.len() as u64;
                tokio::task::spawn_blocking(move || write_all_at(&file, &bytes, start))
                    .await
                    .map_err(|e| WalError::Corrupt(format!("write task failed: {e}")))??;
                report(n);
                Ok::<(), WalError>(())
            }
        })
        .buffer_unordered(PAR)
        .try_collect::<Vec<()>>()
        .await?;
    Ok(())
}

/// Apply the delta between the current local state and the new manifest.
/// Downloads missing packs, replays log entries, applies ref transactions.
pub(crate) async fn apply_delta(
    handle: &super::handle::RepoHandle,
    new_manifest: &Manifest,
    new_version: &Version,
) -> Result<(), WalError> {
    let store = &handle.store;
    let local = &handle.local;
    let current_state = handle.state.lock().clone();

    // If we have a checkpoint and haven't loaded it yet, load its refs. Its
    // packs are a subset of `Manifest.packs` and are reconciled below.
    let checkpoint_seq = new_manifest.checkpoint.as_ref().map(|c| c.seq).unwrap_or(0);
    let need_checkpoint_load = checkpoint_seq > 0 && current_state.applied_seq < checkpoint_seq;

    // The checkpoint's times feed `first_state_time` / `refs_as_of`; old refs
    // carry none, the object always does.
    handle.learn_checkpoint_times().await?;
    if need_checkpoint_load {
        let refs_key = keys::checkpoint_refs_key(checkpoint_seq);
        if let Some((_, snap)) = get_message::<RefSnapshot>(store, &refs_key).await? {
            local.load_ref_snapshot(&snap)?;
            handle.state.lock().applied_seq = checkpoint_seq;
        }
    }

    // Replay log entries (refs, and superseded-pack bookkeeping) from
    // applied_seq+1 to head_seq. Packs are never touched here.
    let applied_seq = handle.state.lock().applied_seq;
    let head_seq = new_manifest.head_seq;
    if applied_seq < head_seq {
        replay_log(handle, new_manifest, applied_seq, head_seq).await?;
    }

    {
        let mut state = handle.state.lock();
        state.manifest_version = Some(new_version.as_str().to_string());
        state.applied_seq = head_seq;
        state.revision = new_manifest.revision;
    }
    crate::state::save_state(local.path(), &handle.state.lock().clone())?;
    local.refresh_async().await?;
    Ok(())
}

/// Make the local pack set match `manifest.packs`: download what is missing
/// (bounded concurrency, striped range reads) and remove packs superseded by
/// COMPACT entries applied since the last full sync. Idempotent; records
/// `packs_revision` on success so the next full sync is a no-op check.
pub(crate) async fn reconcile_packs(
    handle: &super::handle::RepoHandle,
    manifest: &Manifest,
    level: SyncLevel,
) -> Result<(), WalError> {
    reconcile_packs_inner(handle, manifest, level, false).await
}

/// `background_history = true`: this call *is* the background history-pack
/// install and must download them instead of deferring.
pub(crate) async fn reconcile_packs_inner(
    handle: &super::handle::RepoHandle,
    manifest: &Manifest,
    level: SyncLevel,
    background_history: bool,
) -> Result<(), WalError> {
    let store = &handle.store;
    let local = &handle.local;
    // Test hook: simulate an unknown blocking call inside the install path
    // (what prod had: 2.6–43 s runtime stalls during materialization). With
    // the bulk runtime this only delays bulk work.
    if let Some(ms) = std::env::var("WALGIT_TEST_BLOCK_INSTALL_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
    {
        std::thread::sleep(std::time::Duration::from_millis(ms));
    }
    let plan: std::collections::HashMap<String, PackPlan> = handle
        .serve_plan(manifest, level)
        .into_iter()
        .map(|(p, how)| (p.checksum, how))
        .collect();
    let span = tracing::info_span!(
        "wal.reconcile_packs",
        repo = %handle.id,
        live = manifest.packs.len(),
        downloaded = 0usize,
        removed = 0usize,
    );

    let remote_served_before: std::collections::HashSet<String> =
        handle.state.lock().remote_served.iter().cloned().collect();
    let installed_packs: std::collections::HashSet<String> = local
        .packs()?
        .into_iter()
        // A Full sync replaces mount symlinks with real copies.
        .filter(|p| level != SyncLevel::Full || !local.pack_path(&p.checksum).is_symlink())
        .map(|p| p.checksum.to_string())
        .collect();
    let tmp_dir = local.path().join(".walgit-tmp");
    tokio::fs::create_dir_all(&tmp_dir).await.ok();

    // Remote-served packs: no data here, only the commit-graph layer (the
    // `remote_served` list is rebuilt from the plan every pass).
    let mut remote_served: Vec<String> = Vec::new();
    for p in &manifest.packs {
        if plan.get(&p.checksum) != Some(&PackPlan::Remote) {
            continue;
        }
        remote_served.push(p.checksum.clone());
        if p.has_commit_graph {
            let oid = gix_hash::ObjectId::from_hex(p.checksum.as_bytes()).map_err(|e| {
                WalError::Corrupt(format!("invalid pack checksum {}: {e}", p.checksum))
            })?;
            let dest = local.pack_path(&oid).with_extension("commit-graph");
            if !dest.exists() {
                let tmp = tmp_dir.join(format!("pack-{}.commit-graph", p.checksum));
                download_object(
                    store,
                    &keys::commit_graph_key(&p.checksum),
                    &tmp,
                    None,
                    None,
                )
                .instrument(span.clone())
                .await?;
                if std::fs::rename(&tmp, &dest).is_err() {
                    std::fs::copy(&tmp, &dest)?;
                    let _ = std::fs::remove_file(&tmp);
                }
            }
        }
        if !remote_served_before.contains(&p.checksum) {
            tracing::info!(repo = %handle.id, pack = %p.checksum, bytes = p.pack_size, "base pack served remotely (no mount, does not fit)");
        }
    }
    {
        let mut st = handle.state.lock();
        st.remote_served = remote_served.clone();
    }
    let remote_set: std::collections::HashSet<&str> =
        remote_served.iter().map(|s| s.as_str()).collect();

    // History packs (D18) are an accelerator, not a requirement: a fetch can
    // be served from the linked/remote base right away. They are installed by
    // a background task (`RepoHandle::spawn_history_pack_install`) so the
    // first request on an instance never waits for a 7.5 GB download.
    let is_history = |p: &PackRef| p.kind == walgit_proto::v1::PackKind::History as i32;
    let deferred_history: Vec<PackRef> = manifest
        .packs
        .iter()
        .filter(|p| is_history(p) && !installed_packs.contains(&p.checksum) && !background_history)
        .cloned()
        .collect();
    if !deferred_history.is_empty() {
        handle.spawn_history_pack_install(deferred_history);
    }
    // Installed packs whose manifest entry now advertises a side-file this
    // host lacks (`annotate-pack` / the rev-index unit retrofitting a `.rev`
    // or a bitmap onto a published pack): fetch just the side-file. Without
    // this the fleet never converged — a large repository's base got its `.rev` in the
    // bucket and every host kept rebuilding the reverse index per fetch.
    for p in manifest
        .packs
        .iter()
        .filter(|p| installed_packs.contains(&p.checksum))
    {
        let Ok(oid) = gix_hash::ObjectId::from_hex(p.checksum.as_bytes()) else {
            continue;
        };
        for (flag, ext, key) in side_files(p) {
            let dest = local.pack_path(&oid).with_extension(ext);
            if !flag || dest.exists() {
                continue;
            }
            let tmp = tmp_dir.join(format!("pack-{}.{ext}", p.checksum));
            match download_object(store, &key, &tmp, None, None)
                .instrument(span.clone())
                .await
            {
                Ok(()) => {
                    if std::fs::rename(&tmp, &dest).is_err() {
                        let _ = std::fs::copy(&tmp, &dest);
                        let _ = std::fs::remove_file(&tmp);
                    }
                    tracing::info!(repo = %handle.id, pack = %p.checksum, ext, "side-file installed for an installed pack");
                }
                Err(e) => {
                    tracing::warn!(repo = %handle.id, pack = %p.checksum, ext, error = %e, "side-file download failed")
                }
            }
        }
    }
    let missing_packs: Vec<&PackRef> = manifest
        .packs
        .iter()
        .filter(|p| {
            !installed_packs.contains(&p.checksum) && !remote_set.contains(p.checksum.as_str())
        })
        .filter(|p| background_history || !is_history(p))
        .collect();
    span.record("downloaded", missing_packs.len());
    let downloaded: Vec<PackRef> = missing_packs
        .iter()
        .map(|p| (*p).clone())
        .chain(
            // Newly remote-served packs count as "installed" for commit-graph
            // maintenance (their layer becomes the chain base).
            manifest
                .packs
                .iter()
                .filter(|p| {
                    remote_set.contains(p.checksum.as_str())
                        && !remote_served_before.contains(&p.checksum)
                })
                .cloned(),
        )
        .collect();

    let reporter = handle.reporter();
    let link_target = |p: &PackRef| match plan.get(&p.checksum) {
        Some(PackPlan::Link(t)) => Some(t.clone()),
        _ => None,
    };
    let total_bytes: u64 = missing_packs
        .iter()
        .map(|p| {
            if link_target(p).is_some() {
                p.idx_size
            } else {
                p.pack_size + p.idx_size
            }
        })
        .sum();
    if !missing_packs.is_empty() {
        reporter.notice(format!(
            "Materializing {} pack(s) ({}) from the WAL onto this instance",
            missing_packs.len(),
            crate::remote::human_bytes(total_bytes)
        ));
    }
    let done_all = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let throttle = Arc::new(crate::progress::Throttle::new(
        std::time::Duration::from_millis(250),
    ));
    let sem = Arc::new(tokio::sync::Semaphore::new(8));
    let mut tasks = Vec::new();
    for p in missing_packs {
        let sem = sem.clone();
        let store = store.clone();
        let local = local.clone();
        let p = p.clone();
        let tmp_dir = tmp_dir.clone();
        let reporter = reporter.clone();
        let done_all = done_all.clone();
        let throttle = throttle.clone();
        let link_to = link_target(&p);
        tasks.push(tokio::spawn(
            async move {
                let _permit = sem.acquire().await.unwrap();
                // Per-object progress arrives as absolute (done,total); turn it
                // into deltas for the shared counter.
                let cb = |delta: u64, _t: u64| {
                    // download_object reports a per-chunk delta so pack+idx+
                    // side-files can run in one round without a shared cursor.
                    let all =
                        done_all.fetch_add(delta, std::sync::atomic::Ordering::Relaxed) + delta;
                    if throttle.tick(false) {
                        reporter.bar("Downloading packs", all, Some(total_bytes), "bytes");
                    }
                };
                match link_to {
                    Some(target) => {
                        link_and_install_pack(&store, &local, &p, &tmp_dir, &target).await
                    }
                    None => {
                        download_and_install_pack(&store, &local, &p, &tmp_dir, Some(&cb)).await
                    }
                }
            }
            .instrument(span.clone()),
        ));
    }
    let had_downloads = !tasks.is_empty();
    for t in tasks {
        t.await.map_err(|e| WalError::Corrupt(e.to_string()))??;
    }
    if had_downloads {
        reporter.bar("Downloading packs", total_bytes, Some(total_bytes), "bytes");
        reporter.notice("Packs installed; local copy is complete");
    }
    maintain_commit_graph(handle, manifest, &downloaded).await;

    // Remove packs superseded by compactions (never a pack that is still
    // live: a later PUSH/COMPACT may have re-listed it). Removal needs no
    // active readers (they hold `rw.read()` through their ReadGuard), so it
    // **tries** the write lock: if any reader is active (a clone streaming
    // for minutes), the removal simply waits for a later pass — never queue
    // as a writer, which would block every new reader on the instance.
    let live: std::collections::HashSet<&str> =
        manifest.packs.iter().map(|p| p.checksum.as_str()).collect();
    let pending = std::mem::take(&mut handle.state.lock().pending_pack_removals);
    let mut removed = 0usize;
    let mut still_pending: Vec<String> = Vec::new();
    let to_remove: Vec<(String, gix_hash::ObjectId)> = pending
        .iter()
        .filter(|s| !live.contains(s.as_str()))
        .map(|s| {
            gix_hash::ObjectId::from_hex(s.as_bytes())
                .map(|o| (s.clone(), o))
                .map_err(|e| WalError::Corrupt(format!("invalid supersedes checksum: {e}")))
        })
        .collect::<Result<_, _>>()?;
    if !to_remove.is_empty() {
        match handle.rw.try_write() {
            Ok(_w) => {
                for (_, oid) in &to_remove {
                    if local.pack_path(oid).exists() {
                        local.remove_pack(oid)?;
                        removed += 1;
                    }
                }
            }
            Err(_) => {
                tracing::info!(repo = %handle.id, packs = to_remove.len(), "superseded packs kept for now: readers active; retried on the next sync");
                still_pending.extend(to_remove.iter().map(|(s, _)| s.clone()));
            }
        }
    }
    span.record("removed", removed);
    if !still_pending.is_empty() {
        handle.state.lock().pending_pack_removals = still_pending;
    }

    {
        let mut state = handle.state.lock();
        state.packs_revision = manifest.revision;
    }
    crate::state::save_state(local.path(), &handle.state.lock().clone())?;
    Ok(())
}

/// Keep the local commit-graph chain current after packs were installed:
/// a tier-2 pack that ships a commit-graph layer becomes the chain base
/// (replacing whatever was there), and every other newly installed pack's
/// commits are folded in as an incremental layer (`--split --stdin-packs`,
/// cheap: generation numbers come from the existing layers). Best effort:
/// the graph is an accelerator, a failure only costs speed.
pub(crate) async fn maintain_commit_graph(
    handle: &super::handle::RepoHandle,
    manifest: &Manifest,
    installed: &[PackRef],
) {
    if !handle.cfg.git.commit_graph {
        return;
    }
    let local = &handle.local;
    let mut base_changed = false;
    for p in installed.iter().filter(|p| p.has_commit_graph) {
        if let Ok(oid) = gix_hash::ObjectId::from_hex(p.checksum.as_bytes()) {
            // Filesystem + gix reopen: off the runtime.
            let l = local.clone();
            let res = tokio::task::spawn_blocking(move || l.install_commit_graph_base(&oid)).await;
            match res {
                Ok(Ok(true)) => base_changed = true,
                Ok(Ok(false)) => {}
                Ok(Err(e)) => {
                    tracing::warn!(pack = %p.checksum, error = %e, "commit-graph base install failed")
                }
                Err(e) => {
                    tracing::warn!(pack = %p.checksum, error = %e, "commit-graph base install task failed")
                }
            }
        }
    }
    // After a base change every non-base pack must be re-added (the old chain
    // layers were dropped); otherwise only what was just installed.
    // History packs hold the base's commits, already covered by its layer.
    let is_history = |p: &&PackRef| p.kind == walgit_proto::v1::PackKind::History as i32;
    let candidates: Vec<&PackRef> = if base_changed {
        manifest
            .packs
            .iter()
            .filter(|p| !p.has_commit_graph && !is_history(p))
            .collect()
    } else {
        installed
            .iter()
            .filter(|p| !p.has_commit_graph && !is_history(p))
            .collect()
    };
    let packs: Vec<gix_hash::ObjectId> = candidates
        .iter()
        .filter_map(|p| gix_hash::ObjectId::from_hex(p.checksum.as_bytes()).ok())
        .filter(|o| local.pack_path(o).exists())
        .collect();
    if packs.is_empty() {
        return;
    }
    let started = std::time::Instant::now();
    if let Err(e) = local
        .update_commit_graph(&packs, handle.cfg.git.commit_graph_changed_paths)
        .await
    {
        tracing::warn!(repo = %handle.id, error = %e, "commit-graph update failed");
    } else {
        tracing::info!(repo = %handle.id, packs = packs.len(), ms = started.elapsed().as_millis() as u64, "commit-graph updated");
    }
}

/// Replay log entries in (from_seq, to_seq] from the manifest's log segments.
pub(crate) async fn replay_log(
    handle: &super::handle::RepoHandle,
    manifest: &Manifest,
    from_seq: u64,
    to_seq: u64,
) -> Result<(), WalError> {
    let store = &handle.store;

    // Find segments that overlap (from_seq, to_seq]
    let segments: Vec<&walgit_proto::v1::LogSegmentRef> = manifest
        .log_segments
        .iter()
        .filter(|s| s.last_seq > from_seq && s.first_seq <= to_seq)
        .collect();

    // Fetch every segment in parallel (order kept), then apply ALL entries in
    // one pass: `apply_ref_txns_offline` rewrites packed-refs, which is O(refs)
    // — per segment that was k × 500 ms on a 500 k-ref repo (2026-08-21,
    // test/refs500k: 2 tail entries = 1 s of every cold refs sync). One
    // rewrite per sync, and the tail's GETs overlap instead of serializing.
    let keys: Vec<String> = segments.iter().map(|s| s.key.clone()).collect();
    let mut fetched: Vec<Option<bytes::Bytes>> = Vec::with_capacity(keys.len());
    for chunk in keys.chunks(16) {
        let futs: Vec<_> = chunk
            .iter()
            .map(|key| {
                let store = store.clone();
                let key = key.clone();
                async move {
                    let res = store.get(&key, GetOptions::default()).await?;
                    Ok::<Option<bytes::Bytes>, WalError>(match res {
                        GetResult::Object { meta, body } => {
                            Some(walgit_store::util::collect(body, meta.size as usize).await?)
                        }
                        GetResult::NotModified { .. } => None,
                    })
                }
            })
            .collect();
        for r in futures::future::join_all(futs).await {
            fetched.push(r?);
        }
    }

    let mut all: Vec<LogEntry> = Vec::new();
    for bytes in fetched {
        let Some(bytes) = bytes else { continue };
        // Decode frames (tolerate partial trailing frame)
        let (entries, _) = walgit_proto::frame::decode_entries(&bytes)
            .map_err(|e| WalError::Corrupt(format!("log segment decode: {e}")))?;
        // Only the oldest live segment witnesses the repository's first state.
        if entries
            .first()
            .is_some_and(|e| e.seq <= manifest.min_seq.max(1))
            && let Some(first) = entries
                .first()
                .and_then(|e| e.created_at.as_ref())
                .map(walgit_proto::time::to_system)
        {
            let mut slot = handle.first_entry_time.lock();
            if slot.map(|t| first < t).unwrap_or(true) {
                *slot = Some(first);
            }
        }
        all.extend(
            entries
                .into_iter()
                .filter(|e| e.seq > from_seq && e.seq <= to_seq),
        );
    }
    all.sort_by_key(|e| e.seq);
    if let Some(last) = all
        .last()
        .and_then(|e| e.created_at.as_ref())
        .map(walgit_proto::time::to_system)
    {
        let mut slot = handle.last_entry_time.lock();
        if slot.map(|t| last > t).unwrap_or(true) {
            *slot = Some(last);
        }
    }
    let wanted: Vec<&LogEntry> = all.iter().collect();
    apply_entries(handle, &wanted)?;

    Ok(())
}

/// Apply a batch of log entries to the local repo (refs level).
///
/// Ref transactions are merged and written once (`apply_ref_txns_offline`:
/// no `git update-ref`, so it works before the packs exist locally).
/// COMPACT entries only record their superseded packs; `reconcile_packs`
/// installs/removes packs against `Manifest.packs` on the next full sync.
pub(crate) fn apply_entries(
    handle: &super::handle::RepoHandle,
    entries: &[&LogEntry],
) -> Result<(), WalError> {
    let local = &handle.local;
    let mut txns: Vec<&walgit_proto::v1::RefTransaction> = Vec::new();
    let mut supersedes: Vec<String> = Vec::new();
    for entry in entries {
        match entry.kind() {
            EntryKind::Push | EntryKind::RefUpdate => {
                if let Some(txn) = &entry.txn {
                    txns.push(txn);
                }
            }
            EntryKind::Compact => {
                supersedes.extend(entry.supersedes.iter().cloned());
            }
            EntryKind::Checkpoint => {}
            // Settings live on the manifest; the entry is history only.
            EntryKind::Settings => {}
            EntryKind::Unspecified => {
                tracing::warn!(seq = entry.seq, "unspecified log entry kind, skipping");
            }
        }
    }
    if !txns.is_empty() {
        local.apply_ref_txns_offline(&txns)?;
    }
    if !supersedes.is_empty() {
        let mut state = handle.state.lock();
        for s in supersedes {
            if !state.pending_pack_removals.contains(&s) {
                state.pending_pack_removals.push(s);
            }
        }
    }
    Ok(())
}

pub(crate) async fn materialize_from_scratch(
    handle: &super::handle::RepoHandle,
    manifest: &Manifest,
    version: &Version,
) -> Result<(), WalError> {
    let span = tracing::info_span!(
        "wal.materialize",
        repo = %handle.id,
        head_seq = manifest.head_seq,
    );
    // Instrument the awaited materialization future; do not hold an enter guard.

    // Reset state
    handle.state.lock().applied_seq = 0;

    // Apply delta from scratch (checkpoint + all log entries), then packs.
    apply_delta(handle, manifest, version)
        .instrument(span.clone())
        .await?;
    reconcile_packs(handle, manifest, SyncLevel::Serve)
        .instrument(span.clone())
        .await
}

#[cfg(test)]
mod download_tests {
    use super::download_object;
    use walgit_store::{ObjectStoreExt, Prefixed, PutMode, memory::MemoryStore};

    #[tokio::test]
    async fn striped_download_matches_source() {
        // > CHUNK (32 MiB) so the ranged/striped path runs, with a ragged tail.
        let size = 70 * 1024 * 1024 + 12345;
        let mut data = vec![0u8; size];
        let mut x: u64 = 0x9E3779B97F4A7C15;
        for b in data.iter_mut() {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            *b = x as u8;
        }
        let store = MemoryStore::shared();
        store
            .put_bytes("p/big.pack", data.clone(), PutMode::Create)
            .await
            .unwrap();
        store
            .put_bytes("p/small.pack", b"tiny".to_vec(), PutMode::Create)
            .await
            .unwrap();
        let prefixed = Prefixed::new(store, "p/");
        let dir = tempfile::tempdir().unwrap();
        let big = dir.path().join("big.pack");
        download_object(&prefixed, "big.pack", &big, Some(size as u64), None)
            .await
            .unwrap();
        assert_eq!(std::fs::read(&big).unwrap(), data);
        let small = dir.path().join("small.pack");
        download_object(&prefixed, "small.pack", &small, None, None)
            .await
            .unwrap();
        assert_eq!(std::fs::read(&small).unwrap(), b"tiny");
    }
}

/// The **bulk runtime**: a small dedicated tokio runtime (own worker threads)
/// that runs pack materialization (striped downloads, 32 MiB chunk copies,
/// tmpfs writes, install renames, gix reopen, commit-graph/midx subprocess
/// waits). Whatever inside that path is CPU-heavy or secretly blocking can
/// only delay other bulk work — request workers on the main runtime keep
/// serving refs in milliseconds (prod 2026-08-20: the main runtime stalled
/// 2.6–43 s repeatedly for the whole duration of one repo's 7.5 GB + another's
/// 12 GB materializations; the watchdog caught it, the cause hid among a dozen
/// candidates; isolation makes the question moot).
static BULK_RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();

fn bulk_runtime() -> &'static tokio::runtime::Runtime {
    BULK_RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .thread_name("walgit-bulk")
            .enable_all()
            .build()
            .expect("bulk runtime")
    })
}

/// Run `fut` on the bulk runtime and await its result from the caller's
/// runtime. The future must be `'static + Send` (use `Arc<RepoHandle>`).
pub(crate) async fn on_bulk_runtime<T: Send + 'static>(
    fut: impl std::future::Future<Output = Result<T, WalError>> + Send + 'static,
) -> Result<T, WalError> {
    let span = tracing::Span::current();
    let (tx, rx) = tokio::sync::oneshot::channel();
    bulk_runtime().spawn(async move {
        let r = fut.instrument(span).await;
        let _ = tx.send(r);
    });
    rx.await
        .map_err(|_| WalError::Corrupt("bulk runtime task dropped".into()))?
}
