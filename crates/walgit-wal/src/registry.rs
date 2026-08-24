//! Registry: process-wide map of RepoId -> Arc<RepoHandle>.

use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;

use dashmap::DashMap;
use futures::StreamExt;
use prost::Message;
use walgit_git::{LocalRepo, ObjectFormat, RepoId};
use walgit_proto::WAL_FORMAT_VERSION;
use walgit_proto::keys;
use walgit_proto::v1::Manifest;
use walgit_store::{DynStore, ObjectStore, Prefixed, PutBody, PutMode, StoreError};

use crate::error::WalError;
use crate::handle::RepoHandle;
use crate::state::{RepoState, load_state, save_state};
use crate::store_proto::get_message;

pub struct Registry {
    store: DynStore,
    cfg: Arc<walgit_config::Config>,
    cache_root: std::path::PathBuf,
    repos: DashMap<RepoId, Arc<RepoHandle>>,
    /// Per-repo single-flight guard for open/create so two concurrent first
    /// requests never both `git init` / materialize the same repo.
    opening: DashMap<RepoId, Arc<tokio::sync::Mutex<()>>>,
    /// Background task log + (repo, kind) locks for this instance.
    tasks: Arc<crate::tasks::Tasks>,
    /// Pack-data block cache shared by every remote reader.
    blocks: Arc<crate::remote::BlockCache>,
    /// `list()` answer + when it was computed (`LIST_TTL`); single-flight refresh.
    listing: tokio::sync::Mutex<Option<(Instant, Arc<Vec<RepoId>>)>>,
}

/// How long a repository listing is served from memory before the bucket is asked again.
/// Owner/repo pages, the maintainer's pass and the bridge all call `list()`; a new repository
/// created on another host appears within this on every instance (on this one immediately).
const LIST_TTL: std::time::Duration = std::time::Duration::from_secs(30);

#[derive(Default, Debug)]
pub struct EvictReport {
    pub evicted: usize,
    pub remaining_bytes: u64,
}

impl Registry {
    pub fn new(store: DynStore, cfg: Arc<walgit_config::Config>) -> Arc<Self> {
        let cache_root = cfg.cache.dir.clone();
        let blocks = crate::remote::BlockCache::new(cfg.cache.remote_block_bytes.as_u64());
        Arc::new(Registry {
            store,
            cfg,
            cache_root,
            repos: DashMap::new(),
            opening: DashMap::new(),
            tasks: crate::tasks::Tasks::new(),
            blocks,
            listing: tokio::sync::Mutex::new(None),
        })
    }

    pub fn store(&self) -> &DynStore {
        &self.store
    }

    pub fn tasks(&self) -> &Arc<crate::tasks::Tasks> {
        &self.tasks
    }

    pub fn blocks(&self) -> &Arc<crate::remote::BlockCache> {
        &self.blocks
    }

    pub fn config(&self) -> &Arc<walgit_config::Config> {
        &self.cfg
    }

    /// Open an existing repo. Err(NotFound) if manifest.pb absent.
    pub async fn open(&self, id: &RepoId) -> Result<Arc<RepoHandle>, WalError> {
        // Every request that touches a repo goes through here: tag the
        // enclosing `http.request` span (no-op outside one).
        tracing::Span::current().record("repo", tracing::field::display(id));
        if let Some(h) = self.repos.get(id) {
            return Ok(h.clone());
        }
        let gate = self.opening.entry(id.clone()).or_default().clone();
        let _g = gate.lock().await;
        if let Some(h) = self.repos.get(id) {
            return Ok(h.clone());
        }

        let prefix = id.store_prefix();
        let prefixed = Prefixed::new(self.store.clone(), prefix);

        // Read manifest (NotFound if absent)
        let (meta, manifest) = match get_message::<Manifest>(&prefixed, keys::MANIFEST).await? {
            Some(v) => v,
            None => return Err(WalError::NotFound),
        };

        // Open or init local repo (LocalRepo joins owner/name.git onto the root).
        let local = match LocalRepo::open(&self.cache_root, id)? {
            Some(l) => l,
            None => {
                let format = parse_object_format(&manifest.object_format);
                LocalRepo::init(&self.cache_root, id, format)?
            }
        };

        // Load state
        let state = load_state(local.path());

        let state_is_behind = state.applied_seq < manifest.head_seq;
        let manifest_version = meta.version.clone();

        let handle = RepoHandle::new(
            id.clone(),
            local,
            prefixed,
            self.cfg.clone(),
            manifest.clone(),
            Some(manifest_version.clone()),
            state,
            self.tasks.clone(),
            self.blocks.clone(),
        );

        let handle = Arc::new(handle);
        handle.set_self_arc(handle.clone());

        // The manifest GET above is already fresh. Apply that exact value
        // directly instead of issuing a second manifest GET merely to learn
        // what we already hold. Cold open remains one manifest round, followed
        // by checkpoint/log objects in parallel/sequence as required.
        if state_is_behind {
            crate::sync::apply_delta(&handle, &manifest, &manifest_version).await?;
        }

        self.repos.insert(id.clone(), handle.clone());
        Ok(handle)
    }

    /// Delete a repository: every object under its store prefix, the cached
    /// handle and the local copy. Err(NotFound) if the manifest does not exist.
    /// Other instances notice on their next freshness check (manifest GET -> 404).
    pub async fn delete(&self, id: &RepoId) -> Result<(), WalError> {
        let prefixed = Prefixed::new(self.store.clone(), id.store_prefix());
        if get_message::<Manifest>(&prefixed, keys::MANIFEST)
            .await?
            .is_none()
        {
            return Err(WalError::NotFound);
        }
        // Drop the handle first so no request on this instance publishes into a
        // prefix that is being removed; in-flight requests hold their own Arc.
        self.repos.remove(id);
        self.invalidate_listing();
        // Manifest first: it is the linearization point, so the repo disappears
        // atomically for readers; remaining objects are unreferenced garbage.
        prefixed.delete(keys::MANIFEST, None).await?;
        let mut after: Option<String> = None;
        loop {
            let mut stream = prefixed.list("", after.as_deref());
            let mut last = None;
            while let Some(res) = stream.next().await {
                let m = res?;
                prefixed.delete(&m.key, None).await?;
                last = Some(m.key);
            }
            match last {
                Some(k) => after = Some(k),
                None => break,
            }
        }
        let local_dir = id.local_dir(&self.cache_root);
        if local_dir.exists() {
            tokio::fs::remove_dir_all(&local_dir)
                .await
                .map_err(WalError::Io)?;
        }
        Ok(())
    }

    /// CAS-create manifest.pb (PutMode::Create). Err(AlreadyExists) on 412.
    pub async fn create(
        &self,
        id: &RepoId,
        format: ObjectFormat,
    ) -> Result<Arc<RepoHandle>, WalError> {
        if let Some(h) = self.repos.get(id) {
            return Ok(h.clone());
        }
        let gate = self.opening.entry(id.clone()).or_default().clone();
        let _g = gate.lock().await;
        if let Some(h) = self.repos.get(id) {
            return Ok(h.clone());
        }

        let prefix = id.store_prefix();
        let prefixed = Prefixed::new(self.store.clone(), prefix);

        // Create manifest with PutMode::Create
        let manifest = Manifest {
            format_version: WAL_FORMAT_VERSION,
            repo: id.to_string(),
            object_format: format.as_str().to_string(),
            head_seq: 0,
            min_seq: 0,
            checkpoint: None,
            log_segments: vec![],
            packs: vec![],
            updated_at: Some(walgit_proto::time::now()),
            writer: crate::handle::instance_id(),
            revision: 1,
            settings: None,
        };

        let buf = manifest.encode_to_vec();
        match prefixed
            .put(
                keys::MANIFEST,
                PutBody::Bytes(bytes::Bytes::from(buf)),
                PutMode::Create.into(),
            )
            .await
        {
            Ok(meta) => {
                // Init local repo
                let local = LocalRepo::init(&self.cache_root, id, format)?;

                let state = RepoState::default();
                save_state(local.path(), &state)?;

                let handle = RepoHandle::new(
                    id.clone(),
                    local,
                    prefixed,
                    self.cfg.clone(),
                    manifest,
                    Some(meta.version),
                    state,
                    self.tasks.clone(),
                    self.blocks.clone(),
                );
                let handle = Arc::new(handle);
                handle.set_self_arc(handle.clone());

                self.repos.insert(id.clone(), handle.clone());
                self.invalidate_listing();
                Ok(handle)
            }
            Err(StoreError::PreconditionFailed { .. }) => Err(WalError::AlreadyExists),
            Err(e) => Err(WalError::Store(e)),
        }
    }

    /// Open or create.
    pub async fn open_or_create(
        &self,
        id: &RepoId,
        format: ObjectFormat,
    ) -> Result<Arc<RepoHandle>, WalError> {
        match self.open(id).await {
            Ok(h) => Ok(h),
            Err(WalError::NotFound) => self.create(id, format).await,
            Err(e) => Err(e),
        }
    }

    /// Every repository (sorted owner, name), from memory for `LIST_TTL`, else recomputed with
    /// **delimited** listings — `repos/` → owners, `repos/<o>/` → names (one round, all owners
    /// in parallel) — and one HEAD of `manifest.pb` per candidate (parallel, one round) so a
    /// prefix whose manifest is gone (deleted repository) is not a repository. Never a walk over
    /// the objects under `repos/`: that was 122 k keys and 8–9 s per owners page (2026-08-22).
    pub async fn list(&self) -> Result<Vec<RepoId>, WalError> {
        let mut slot = self.listing.lock().await;
        if let Some((at, repos)) = slot.as_ref()
            && at.elapsed() < LIST_TTL
        {
            return Ok(repos.as_ref().clone());
        }
        let repos = Arc::new(self.list_uncached().await?);
        *slot = Some((Instant::now(), repos.clone()));
        Ok(repos.as_ref().clone())
    }

    /// Forget the cached listing (after this host created or deleted a repository).
    fn invalidate_listing(&self) {
        if let Ok(mut slot) = self.listing.try_lock() {
            *slot = None;
        }
    }

    async fn list_uncached(&self) -> Result<Vec<RepoId>, WalError> {
        let owners = self.store.list_prefixes("repos/").await?;
        let per_owner = futures::stream::iter(owners)
            .map(|owner_prefix| {
                let store = self.store.clone();
                async move { store.list_prefixes(&owner_prefix).await }
            })
            .buffer_unordered(16)
            .collect::<Vec<_>>()
            .await;
        let mut candidates = Vec::new();
        for r in per_owner {
            for repo_prefix in r? {
                // repos/<owner>/<repo>/
                if let Some(id) = repo_prefix
                    .strip_prefix("repos/")
                    .and_then(|s| s.strip_suffix('/'))
                    .and_then(|s| RepoId::from_str(s).ok())
                {
                    candidates.push(id);
                }
            }
        }
        let present = futures::stream::iter(candidates)
            .map(|id| {
                let store = self.store.clone();
                async move {
                    let key = format!("{}{}", id.store_prefix(), keys::MANIFEST);
                    store.head(&key).await.map(|m| m.map(|_| id))
                }
            })
            .buffer_unordered(32)
            .collect::<Vec<_>>()
            .await;
        let mut repos = Vec::new();
        for r in present {
            if let Some(id) = r? {
                repos.push(id);
            }
        }
        repos.sort_by(|a, b| {
            a.owner()
                .cmp(b.owner())
                .then_with(|| a.name().cmp(b.name()))
        });
        Ok(repos)
    }

    /// Disk cache maintenance: evict idle repos beyond cache.max_bytes / evict_idle_after.
    pub async fn evict_idle(&self) -> Result<EvictReport, WalError> {
        let evict_after = self.cfg.cache.evict_idle_after;
        // D25: budget mode evicts past `cache.max_bytes`; disk mode only under
        // disk pressure (filesystem of `cache.dir` above `disk_high_watermark`)
        // — then down to the low mark (watermark − 10 %), oldest idle first.
        let max_bytes = if self.cfg.cache_is_disk() {
            match disk_usage(&self.cfg.cache.dir) {
                Some((used, total)) if total > 0 && self.cfg.cache.disk_high_watermark > 0.0 => {
                    let frac = used as f64 / total as f64;
                    metrics::gauge!("walgit_cache_disk_used_fraction").set(frac);
                    if frac <= self.cfg.cache.disk_high_watermark {
                        return Ok(EvictReport::default());
                    }
                    let low = ((self.cfg.cache.disk_high_watermark - 0.10).max(0.0) * total as f64)
                        as u64;
                    // Other data on the filesystem counts against us: target =
                    // cache bytes − (used − low).
                    let over = used.saturating_sub(low);
                    let cache_bytes: u64 = self
                        .repos
                        .iter()
                        .map(|e| dir_size(e.value().local.path()))
                        .sum();
                    tracing::warn!(
                        used,
                        total,
                        over,
                        "cache disk above high watermark: evicting idle repositories"
                    );
                    cache_bytes.saturating_sub(over)
                }
                _ => return Ok(EvictReport::default()),
            }
        } else {
            self.cfg.cache.max_bytes.as_u64()
        };
        let now = std::time::Instant::now();

        let mut evicted = 0;
        let mut candidates: Vec<(RepoId, Instant, Arc<RepoHandle>)> = Vec::new();

        // Collect idle repos. In-use checks happen again while evicting: a
        // request may acquire a ReadGuard after this snapshot.
        for entry in self.repos.iter() {
            let handle = entry.value();
            let last_access = handle.last_access();
            if now.duration_since(last_access) > evict_after {
                candidates.push((entry.key().clone(), last_access, handle.clone()));
            }
        }

        // Sort by oldest access first.
        candidates.sort_by_key(|(_, t, _)| *t);

        // Calculate total cache size and evict as needed.
        let mut total_bytes: u64 = candidates
            .iter()
            .map(|(_, _, h)| dir_size(h.local.path()))
            .sum();

        for (id, _, handle) in &candidates {
            if total_bytes <= max_bytes {
                break;
            }
            // Hold both gates through removal. `sync_mutex` excludes a sync
            // beginning between the idle snapshot and removal; `rw.write`
            // excludes leaked/long-lived request ReadGuards. The old code only
            // probed sync_mutex and immediately dropped it, so it could delete
            // packs underneath an active reader.
            let Ok(_sync) = handle.sync_mutex.try_lock() else {
                continue;
            };
            let Ok(_write) = handle.rw.try_write() else {
                continue;
            };
            let path = handle.local.path().to_path_buf();
            let bytes = dir_size(&path);
            self.repos.remove(id);
            let _ = std::fs::remove_dir_all(&path);
            total_bytes = total_bytes.saturating_sub(bytes);
            evicted += 1;
        }

        Ok(EvictReport {
            evicted,
            remaining_bytes: total_bytes,
        })
    }
}

fn parse_object_format(s: &str) -> ObjectFormat {
    match s {
        "sha256" => ObjectFormat::Sha256,
        _ => ObjectFormat::Sha1,
    }
}

/// Bytes a repo directory occupies: symlinks (mount-linked base packs) count
/// as their link size, hard links (the pack index shared between the Serve
/// level and the remote reader) once.
fn dir_size(path: &std::path::Path) -> u64 {
    fn walk(p: &std::path::Path, seen: &mut std::collections::HashSet<(u64, u64)>) -> u64 {
        let mut total = 0;
        if let Ok(entries) = std::fs::read_dir(p) {
            for entry in entries.flatten() {
                let path = entry.path();
                // `DirEntry::metadata` does not follow symlinks: a linked base
                // pack is a few bytes here, as it should be.
                let Ok(meta) = entry.metadata() else { continue };
                if meta.is_dir() {
                    total += walk(&path, seen);
                    continue;
                }
                #[cfg(unix)]
                {
                    use std::os::unix::fs::MetadataExt;
                    if meta.nlink() <= 1 || seen.insert((meta.dev(), meta.ino())) {
                        total += meta.len();
                    }
                }
                #[cfg(not(unix))]
                {
                    let _ = seen;
                    total += meta.len();
                }
            }
        }
        total
    }
    walk(path, &mut std::collections::HashSet::new())
}

/// (used, total) bytes of the filesystem holding `path` (statvfs / GetDiskFreeSpaceEx).
fn disk_usage(path: &std::path::Path) -> Option<(u64, u64)> {
    #[cfg(unix)]
    {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;
        let c = CString::new(path.as_os_str().as_bytes()).ok()?;
        let mut st: libc::statvfs = unsafe { std::mem::zeroed() };
        if unsafe { libc::statvfs(c.as_ptr(), &mut st) } != 0 {
            return None;
        }
        let total = st.f_blocks as u64 * st.f_frsize as u64;
        let avail = st.f_bavail as u64 * st.f_frsize as u64;
        Some((total.saturating_sub(avail), total))
    }
    #[cfg(windows)]
    {
        disk_usage_windows(path)
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = path;
        None
    }
}

#[cfg(windows)]
fn disk_usage_windows(path: &std::path::Path) -> Option<(u64, u64)> {
    use std::os::windows::ffi::OsStrExt;
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn GetDiskFreeSpaceExW(
            lp_directory_name: *const u16,
            lp_free_bytes_available: *mut u64,
            lp_total_number_of_bytes: *mut u64,
            lp_total_number_of_free_bytes: *mut u64,
        ) -> i32;
    }
    let mut wide: Vec<u16> = path.as_os_str().encode_wide().collect();
    wide.push(0);
    let mut avail = 0u64;
    let mut total = 0u64;
    let mut free = 0u64;
    let ok = unsafe { GetDiskFreeSpaceExW(wide.as_ptr(), &mut avail, &mut total, &mut free) };
    if ok == 0 || total == 0 {
        return None;
    }
    Some((total.saturating_sub(avail), total))
}
