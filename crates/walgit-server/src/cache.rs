//! In-process LRU caches for hot render paths.
//!
//! Caches are invalidated by manifest version (never serve stale refs).
//! Each cache exposes hit/miss counters via the `metrics` crate.
//!
//! **Justification for `moka`:** these caches need bounded, concurrent,
//! size-based LRU eviction. Implementing LRU eviction on `DashMap` requires a
//! secondary ordering structure and manual locking — error-prone and slower.
//! `moka::sync::Cache` provides thread-safe, size-bounded LRU out of the box
//! with excellent throughput (bucket-level locking, no global lock on hot
//! path). `DashMap` remains the right choice for unbounded lookup tables
//! (e.g. `RepoSemaphores`); bounded LRU is moka's domain.

use moka::sync::Cache;
use walgit_git::LsRefsLine;
use walgit_store::Version;

// ---------------------------------------------------------------------------
// Ref advertisement cache
// ---------------------------------------------------------------------------

/// Cache key for a ref advertisement. The complete ls-refs request variant is
/// part of the key: prefix hashes alone could collide and serve one request's
/// filtered result to another request.
#[derive(Clone, PartialEq, Eq, Hash)]
struct RefAdvertKey {
    repo: String,
    version: String,
    variant: u8,
    prefixes: Vec<String>,
    symrefs: bool,
    peel: bool,
    unborn: bool,
}

fn v0_key(repo: &str, version: Option<&Version>, service: walgit_git::Service) -> RefAdvertKey {
    RefAdvertKey {
        repo: repo.to_string(),
        version: version.map(|v| v.as_str().to_string()).unwrap_or_default(),
        variant: match service {
            walgit_git::Service::UploadPack => 0,
            walgit_git::Service::ReceivePack => 1,
        },
        prefixes: Vec::new(),
        symrefs: false,
        peel: false,
        unborn: false,
    }
}

fn v2_key(repo: &str, version: Option<&Version>, args: &walgit_git::LsRefsArgs) -> RefAdvertKey {
    RefAdvertKey {
        repo: repo.to_string(),
        version: version.map(|v| v.as_str().to_string()).unwrap_or_default(),
        variant: 2,
        prefixes: args.ref_prefixes.clone(),
        symrefs: args.symrefs,
        peel: args.peel,
        unborn: args.unborn,
    }
}

/// Cache for rendered v0 ref advertisements.
/// Keyed by (repo, `manifest_version`, service).
#[derive(Clone)]
pub struct RefAdvertCache {
    v0: Cache<RefAdvertKey, Vec<u8>>,
    v2_ls_refs: Cache<RefAdvertKey, Vec<LsRefsLine>>,
}

impl RefAdvertCache {
    pub fn new(max_entries: usize) -> Self {
        let v0 = Cache::builder().max_capacity(max_entries as u64).build();
        let v2_ls_refs = Cache::builder().max_capacity(max_entries as u64).build();
        Self { v0, v2_ls_refs }
    }

    /// Get a cached v0 advertisement.
    pub fn get_v0(
        &self,
        repo: &str,
        version: Option<&Version>,
        service: walgit_git::Service,
    ) -> Option<Vec<u8>> {
        let key = v0_key(repo, version, service);
        if let Some(val) = self.v0.get(&key) {
            metrics::counter!("walgit_cache_ref_advert_hit").increment(1);
            Some(val)
        } else {
            metrics::counter!("walgit_cache_ref_advert_miss").increment(1);
            None
        }
    }

    /// Insert a v0 advertisement into the cache.
    pub fn insert_v0(
        &self,
        repo: &str,
        version: Option<&Version>,
        service: walgit_git::Service,
        buf: Vec<u8>,
    ) {
        self.v0.insert(v0_key(repo, version, service), buf);
    }

    /// Get cached v2 ls-refs lines.
    pub fn get_v2_ls_refs(
        &self,
        repo: &str,
        version: Option<&Version>,
        args: &walgit_git::LsRefsArgs,
    ) -> Option<Vec<LsRefsLine>> {
        let key = v2_key(repo, version, args);
        if let Some(val) = self.v2_ls_refs.get(&key) {
            metrics::counter!("walgit_cache_ls_refs_hit").increment(1);
            Some(val)
        } else {
            metrics::counter!("walgit_cache_ls_refs_miss").increment(1);
            None
        }
    }

    /// Insert v2 ls-refs lines into the cache.
    pub fn insert_v2_ls_refs(
        &self,
        repo: &str,
        version: Option<&Version>,
        args: &walgit_git::LsRefsArgs,
        lines: Vec<LsRefsLine>,
    ) {
        self.v2_ls_refs.insert(v2_key(repo, version, args), lines);
    }
}

// ---------------------------------------------------------------------------
// Bundle list render cache
// ---------------------------------------------------------------------------

/// Cache key for the rendered bundle list: the repo (freshness = TTL + the
/// building host's own invalidation, see `BundleListCache`).
#[derive(Clone, PartialEq, Eq, Hash)]
struct BundleListKey {
    repo: String,
    /// Version (generation) of `bundles/list.pb` the text was rendered from.
    list_version: String,
}

/// Cache for rendered bundle list text, keyed by (repo, **version of
/// `bundles/list.pb`**) — the object a bundle publish actually changes (the
/// manifest does not; keyed by manifest version it served a 20-minute-stale
/// list on the very host that had just published, 2026-08-21). One metadata
/// probe per request decides freshness on every host; the building host also
/// invalidates. The TTL only bounds memory for repos nobody asks about.
#[derive(Clone)]
pub struct BundleListCache {
    inner: Cache<BundleListKey, String>,
}

/// Idle lifetime of a rendered list (freshness comes from the version key).
pub const BUNDLE_LIST_TTL: std::time::Duration = std::time::Duration::from_mins(10);

impl BundleListCache {
    pub fn new(max_entries: usize) -> Self {
        Self::with_ttl(max_entries, BUNDLE_LIST_TTL)
    }

    pub fn with_ttl(max_entries: usize, ttl: std::time::Duration) -> Self {
        Self {
            inner: Cache::builder()
                .max_capacity(max_entries as u64)
                .time_to_live(ttl)
                .support_invalidation_closures()
                .build(),
        }
    }

    pub fn get(&self, repo: &str, list_version: &str) -> Option<String> {
        let key = BundleListKey {
            repo: repo.to_string(),
            list_version: list_version.to_string(),
        };
        if let Some(val) = self.inner.get(&key) {
            metrics::counter!("walgit_cache_bundle_list_hit").increment(1);
            Some(val)
        } else {
            metrics::counter!("walgit_cache_bundle_list_miss").increment(1);
            None
        }
    }

    pub fn insert(&self, repo: &str, list_version: &str, text: String) {
        self.inner.insert(
            BundleListKey {
                repo: repo.to_string(),
                list_version: list_version.to_string(),
            },
            text,
        );
    }

    /// This host built/published a bundle for `repo`: drop every render of it
    /// (belt and braces — the version key already misses on the new list).
    pub fn invalidate(&self, repo: &str) {
        let repo = repo.to_string();
        let _ = self.inner.invalidate_entries_if(move |k, _| k.repo == repo);
    }
}

// ---------------------------------------------------------------------------
// ServerCaches — aggregate held by AppState
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Ref index (web API): exact-name lookups + name-sorted namespaces
// ---------------------------------------------------------------------------

/// Indexed view of one repo's refs at one manifest version. Built once per
/// version (O(refs)), then every web API request is O(1)/O(k)/O(page).
pub struct RefIndex {
    /// Full ref name -> (oid, peeled). `peeled` empty unless annotated tag.
    pub by_name: std::collections::HashMap<String, (String, String)>,
    /// Short branch names, byte-sorted, with commit sha.
    pub branches: Vec<(String, String)>,
    /// Short tag names, byte-sorted, with the peeled commit sha.
    pub tags: Vec<(String, String)>,
    /// HEAD symref target (full name) or "".
    pub head_target: String,
}

impl RefIndex {
    pub fn build(snap: &walgit_git::RefSnapshotData) -> Self {
        let mut by_name = std::collections::HashMap::with_capacity(snap.refs.len());
        let mut branches = Vec::new();
        let mut tags = Vec::new();
        for r in &snap.refs {
            if let Some(n) = r.name.strip_prefix("refs/heads/") {
                branches.push((n.to_string(), r.oid.clone()));
            } else if let Some(n) = r.name.strip_prefix("refs/tags/") {
                let sha = if r.peeled.is_empty() {
                    r.oid.clone()
                } else {
                    r.peeled.clone()
                };
                tags.push((n.to_string(), sha));
            }
            by_name.insert(r.name.clone(), (r.oid.clone(), r.peeled.clone()));
        }
        branches.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
        tags.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
        Self {
            by_name,
            branches,
            tags,
            head_target: snap.head_target.clone(),
        }
    }

    /// Default branch (short name, sha) if HEAD points at an existing branch.
    pub fn head(&self) -> Option<(String, String)> {
        let short = self.head_target.strip_prefix("refs/heads/")?;
        let (oid, _) = self.by_name.get(&self.head_target)?;
        Some((short.to_string(), oid.clone()))
    }

    /// Commit sha for a short branch name.
    pub fn branch(&self, short: &str) -> Option<&str> {
        self.by_name
            .get(&format!("refs/heads/{short}"))
            .map(|(oid, _)| oid.as_str())
    }

    /// Peeled commit sha for a short tag name.
    pub fn tag(&self, short: &str) -> Option<&str> {
        self.by_name
            .get(&format!("refs/tags/{short}"))
            .map(|(oid, peeled)| {
                if peeled.is_empty() {
                    oid.as_str()
                } else {
                    peeled.as_str()
                }
            })
    }
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct RefIndexKey {
    repo: String,
    version: String,
}

/// Keyed by (repo, `manifest_version`).
#[derive(Clone)]
pub struct RefIndexCache {
    inner: Cache<RefIndexKey, std::sync::Arc<RefIndex>>,
}

impl RefIndexCache {
    pub fn new(max_entries: usize) -> Self {
        Self {
            inner: Cache::builder().max_capacity(max_entries as u64).build(),
        }
    }

    pub fn get_or_build(
        &self,
        repo: &str,
        version: &str,
        build: impl FnOnce() -> Result<walgit_git::RefSnapshotData, walgit_git::GitError>,
    ) -> Result<std::sync::Arc<RefIndex>, walgit_git::GitError> {
        let key = RefIndexKey {
            repo: repo.to_string(),
            version: version.to_string(),
        };
        if let Some(v) = self.inner.get(&key) {
            metrics::counter!("walgit_cache_ref_index_hit").increment(1);
            return Ok(v);
        }
        metrics::counter!("walgit_cache_ref_index_miss").increment(1);
        let idx = std::sync::Arc::new(RefIndex::build(&build()?));
        self.inner.insert(key, idx.clone());
        Ok(idx)
    }
}

// ---------------------------------------------------------------------------
// ServerCaches — aggregate held by AppState
// ---------------------------------------------------------------------------

/// All in-process caches for the server, held by `AppState`.
#[derive(Clone)]
pub struct ServerCaches {
    pub ref_advert: RefAdvertCache,
    pub bundle_list: BundleListCache,
    pub ref_index: RefIndexCache,
    /// Rendered sha-addressed web API JSON (immutable): key = repo\0kind\0sha\0path.
    pub api_immutable: Cache<String, bytes::Bytes>,
    /// `bundles.require` fallback (D17 amendment): when a principal fetched a
    /// repo's `bundles/list` (`key = repo\0principal` → when), it *tried*
    /// bundle-uri; a zero-have full fetch from it within the hour is a bundle
    /// download that failed, and gets ONE upload-pack clone per
    /// `FALLBACK_EVERY` (the second entry, `repo\0principal\0fallback`).
    pub bundle_attempts: Cache<String, std::time::Instant>,
}

impl ServerCaches {
    pub fn new(cfg: &walgit_config::Config) -> Self {
        Self {
            ref_advert: RefAdvertCache::new(cfg.cache.ref_advert_entries),
            bundle_list: BundleListCache::new(cfg.cache.bundle_list_entries),
            ref_index: RefIndexCache::new(cfg.cache.ref_advert_entries.max(64)),
            api_immutable: Cache::builder()
                .max_capacity(64 * 1024 * 1024)
                .weigher(|k: &String, v: &bytes::Bytes| {
                    (k.len() + v.len()).min(u32::MAX as usize) as u32
                })
                .build(),
            bundle_attempts: Cache::builder()
                .max_capacity(100_000)
                .time_to_live(std::time::Duration::from_hours(6))
                .build(),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::fmt::Write;

    fn make_args(prefixes: &[&str]) -> walgit_git::LsRefsArgs {
        walgit_git::LsRefsArgs {
            ref_prefixes: prefixes
                .iter()
                .map(std::string::ToString::to_string)
                .collect(),
            symrefs: false,
            peel: true,
            unborn: false,
        }
    }

    #[test]
    fn ref_advert_cache_hit_miss() {
        let cache = RefAdvertCache::new(16);
        let version = Version::new("v1");

        assert!(
            cache
                .get_v0(
                    "acme/monorepo",
                    Some(&version),
                    walgit_git::Service::UploadPack
                )
                .is_none()
        );

        cache.insert_v0(
            "acme/monorepo",
            Some(&version),
            walgit_git::Service::UploadPack,
            b"advert".to_vec(),
        );

        assert_eq!(
            cache.get_v0(
                "acme/monorepo",
                Some(&version),
                walgit_git::Service::UploadPack
            ),
            Some(b"advert".to_vec())
        );
    }

    #[test]
    fn ref_advert_cache_invalidated_by_version() {
        let cache = RefAdvertCache::new(16);
        let v1 = Version::new("v1");
        let v2 = Version::new("v2");

        cache.insert_v0(
            "acme/monorepo",
            Some(&v1),
            walgit_git::Service::UploadPack,
            b"old".to_vec(),
        );

        // Different version → miss (stale ref advert not served).
        assert!(
            cache
                .get_v0("acme/monorepo", Some(&v2), walgit_git::Service::UploadPack)
                .is_none()
        );

        // Same version → hit.
        assert_eq!(
            cache.get_v0("acme/monorepo", Some(&v1), walgit_git::Service::UploadPack),
            Some(b"old".to_vec())
        );
    }

    #[test]
    fn ls_refs_cache_different_prefixes() {
        let cache = RefAdvertCache::new(16);
        let version = Version::new("v1");
        let args1 = make_args(&["refs/heads/"]);
        let args2 = make_args(&["refs/tags/"]);

        let lines1 = vec![LsRefsLine {
            name: "refs/heads/main".into(),
            oid: "abc".into(),
            peeled: String::new(),
            symref_target: None,
        }];
        let lines2 = vec![LsRefsLine {
            name: "refs/tags/v1".into(),
            oid: "def".into(),
            peeled: String::new(),
            symref_target: None,
        }];

        cache.insert_v2_ls_refs("acme/monorepo", Some(&version), &args1, lines1.clone());
        cache.insert_v2_ls_refs("acme/monorepo", Some(&version), &args2, lines2.clone());

        assert_eq!(
            cache.get_v2_ls_refs("acme/monorepo", Some(&version), &args1),
            Some(lines1)
        );
        assert_eq!(
            cache.get_v2_ls_refs("acme/monorepo", Some(&version), &args2),
            Some(lines2)
        );
    }

    /// A bundle publish changes list.pb, not the manifest: the rendered list
    /// must expire on its own (prod served a 20-minute-stale list, 2026-08-21).
    #[tokio::test]
    async fn bundle_list_cache_expires_without_a_manifest_change() {
        let c = BundleListCache::with_ttl(8, std::time::Duration::from_millis(60));
        c.insert("o/r", "g1", "[bundle] one".into());
        assert_eq!(c.get("o/r", "g1").as_deref(), Some("[bundle] one"));
        tokio::time::sleep(std::time::Duration::from_millis(120)).await;
        assert!(
            c.get("o/r", "g1").is_none(),
            "idle entry gone after the TTL"
        );
    }

    #[test]
    fn bundle_list_cache_hit_miss() {
        let cache = BundleListCache::new(16);
        assert!(cache.get("acme/monorepo", "g1").is_none());
        cache.insert("acme/monorepo", "g1", "bundle list text".into());
        assert_eq!(
            cache.get("acme/monorepo", "g1"),
            Some("bundle list text".into())
        );
        // A new list.pb generation → miss (the invariant); another repo → miss.
        assert!(cache.get("acme/monorepo", "g2").is_none());
        assert!(cache.get("acme/other", "g1").is_none());
        // This host's build → every render of the repo dropped.
        cache.invalidate("acme/monorepo");
        std::thread::sleep(std::time::Duration::from_millis(50)); // moka applies closure invalidation lazily
        assert!(cache.get("acme/monorepo", "g1").is_none());
    }

    #[test]
    fn cache_eviction_by_size() {
        let cache = RefAdvertCache::new(2);
        let v = Version::new("v1");

        // Insert 3 entries with capacity 2 — the oldest should be evicted.
        cache.insert_v0(
            "repo1",
            Some(&v),
            walgit_git::Service::UploadPack,
            b"1".to_vec(),
        );
        cache.insert_v0(
            "repo2",
            Some(&v),
            walgit_git::Service::UploadPack,
            b"2".to_vec(),
        );
        cache.insert_v0(
            "repo3",
            Some(&v),
            walgit_git::Service::UploadPack,
            b"3".to_vec(),
        );

        // repo1 should have been evicted (LRU).
        // Note: moka may not evict immediately; run all pending tasks.
        cache.v0.run_pending_tasks();

        // At most 2 entries should be present.
        let count = cache.v0.entry_count();
        assert!(
            count <= 2,
            "cache should have at most 2 entries, got {count}"
        );
    }

    /// Benchmark: measure ref advertisement render time with and without cache
    /// for a 50k-ref repo. Run with: cargo test -p walgit-server `bench_ref_advert` -- --nocapture --ignored
    #[test]
    #[ignore = "requires git binary and takes ~10s"]
    fn bench_ref_advert_50k_refs() {
        use std::process::Command;
        use std::time::Instant;
        use walgit_git::{LocalRepo, ObjectFormat, RepoId, Service};

        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();

        // Create a bare repo.
        let id = RepoId::new("bench", "big").unwrap();
        let repo = LocalRepo::init(root, &id, ObjectFormat::Sha1).unwrap();

        // Create an initial commit.
        let repo_path = repo.path();

        // Create an empty tree.
        let empty_tree = Command::new("git")
            .args(["mktree"])
            .current_dir(repo_path)
            .output()
            .unwrap();
        let tree_oid = String::from_utf8_lossy(&empty_tree.stdout)
            .trim()
            .to_string();

        // Create a commit object via commit-tree.
        let commit_result = Command::new("git")
            .args(["commit-tree", &tree_oid, "-m", "initial"])
            .env("GIT_AUTHOR_NAME", "Test")
            .env("GIT_AUTHOR_EMAIL", "test@test.com")
            .env("GIT_COMMITTER_NAME", "Test")
            .env("GIT_COMMITTER_EMAIL", "test@test.com")
            .current_dir(repo_path)
            .output()
            .unwrap();
        assert!(
            commit_result.status.success(),
            "commit-tree failed: {}",
            String::from_utf8_lossy(&commit_result.stderr)
        );
        let commit_oid = String::from_utf8_lossy(&commit_result.stdout)
            .trim()
            .to_string();

        // Create 50k refs.
        let mut ref_input = String::with_capacity(50_000 * 60);
        for i in 0..50_000u32 {
            writeln!(ref_input, "update refs/heads/b{i} {commit_oid}").unwrap();
        }
        Command::new("git")
            .args(["update-ref", "--stdin"])
            .current_dir(repo_path)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .and_then(|mut c| {
                use std::io::Write;
                c.stdin.as_mut().unwrap().write_all(ref_input.as_bytes())?;
                c.wait_with_output()
            })
            .unwrap();

        repo.refresh().unwrap();

        let cache = RefAdvertCache::new(16);
        let version = Version::new("v1");

        // Measure: cache miss (first render).
        let start = Instant::now();
        let mut buf = Vec::with_capacity(4 * 1024 * 1024);
        repo.advertise_refs_v0(Service::UploadPack, &mut buf)
            .unwrap();
        let render_ms = start.elapsed().as_millis();
        let advert_bytes = buf.len();
        cache.insert_v0("bench/big", Some(&version), Service::UploadPack, buf);

        // Measure: cache hit (second call).
        let start = Instant::now();
        let cached = cache
            .get_v0("bench/big", Some(&version), Service::UploadPack)
            .unwrap();
        let cache_ms = start.elapsed().as_millis();

        println!(
            "50k refs: render={render_ms}ms ({advert_bytes} bytes), cache_hit={cache_ms}ms, speedup={:.1}x",
            if cache_ms > 0 {
                render_ms as f64 / cache_ms as f64
            } else {
                render_ms as f64 * 1000.0 // sub-millisecond cache
            }
        );
        assert!(cache_ms <= render_ms, "cache should be faster");
        assert_eq!(cached.len(), advert_bytes);
    }
}
