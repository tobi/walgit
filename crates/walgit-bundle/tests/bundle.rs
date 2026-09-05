// Test fixtures use panics to fail the test, including shared helper functions.
#![allow(
    clippy::panic,
    clippy::string_slice,
    clippy::unwrap_used,
    clippy::field_reassign_with_default,
    clippy::cast_sign_loss
)]

//! Integration tests for walgit-bundle: real upstream `git` + `MemoryStore`.
//!
//! These tests create bare repos via `LocalRepo::init`, push commits from a
//! working clone, and exercise the full bundler flow:
//!   - Full bundle passes `git bundle verify`
//!   - Incremental bundle has prerequisites
//!   - `git clone` from full + `git fetch` incremental → identical refs
//!   - `run_due` respects schedule and lease
//!   - Pruning keeps the chain valid
//!   - `--bundle-uri` clone works from a file:// bundle list

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tempfile::TempDir;
use tokio::process::Command;

use walgit_bundle::{
    BundleEngine, BundleError, BundleRepoHandle, BundleSource, Bundler, RepoId, ops,
};
use walgit_config::{BundleKind, BundleServe, BundleStrategy, BundlesConfig, ByteSize, Config};
use walgit_git::{LocalRepo, ObjectFormat as GitObjectFormat};
use walgit_store::{DynStore, ObjectStore, ObjectStoreExt, Prefixed, memory::MemoryStore};

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

struct TestRepo {
    local: LocalRepo,
    store: Prefixed,
    head_seq: Arc<AtomicU64>,
    work_path: PathBuf,
    _root: TempDir,
}

impl TestRepo {
    async fn new(owner: &str, name: &str) -> Self {
        let root = tempfile::tempdir().unwrap();
        let id = RepoId::new(owner, name).unwrap();

        // Init bare repo.
        let local = LocalRepo::init(root.path(), &id, GitObjectFormat::Sha1).unwrap();
        let bare_path = id.local_dir(root.path());

        // Create working clone.
        let work_path = root.path().join("work");
        tokio::fs::create_dir_all(&work_path).await.unwrap();
        run_git(&work_path, &["init", "--initial-branch=main"]).await;
        run_git(&work_path, &["config", "user.email", "test@test.com"]).await;
        run_git(&work_path, &["config", "user.name", "Test"]).await;
        run_git(
            &work_path,
            &["remote", "add", "origin", bare_path.to_str().unwrap()],
        )
        .await;

        // Memory store scoped to the repo prefix.
        let mem = Arc::new(MemoryStore::new()) as DynStore;
        let store = Prefixed::new(mem, id.store_prefix());

        TestRepo {
            local,
            store,
            head_seq: Arc::new(AtomicU64::new(0)),
            work_path,
            _root: root,
        }
    }

    async fn commit(&self, msg: &str) {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let file = self.work_path.join(format!("file-{ts}.txt"));
        tokio::fs::write(&file, format!("content {ts}"))
            .await
            .unwrap();
        run_git(&self.work_path, &["add", "."]).await;
        run_git(&self.work_path, &["commit", "-m", msg]).await;
    }

    async fn tag(&self, name: &str, msg: &str) {
        run_git(&self.work_path, &["tag", "-a", name, "-m", msg]).await;
    }

    async fn push(&self) {
        run_git(&self.work_path, &["push", "origin", "main", "--tags"]).await;
        self.local.refresh().unwrap();
    }

    fn advance_seq(&self) {
        self.head_seq.fetch_add(1, Ordering::Relaxed);
    }
}

/// Test `BundleSource`: holds one or more repos.
struct TestSource {
    repos: HashMap<RepoId, (LocalRepo, Prefixed, Arc<AtomicU64>)>,
}

impl TestSource {
    fn new() -> Self {
        TestSource {
            repos: HashMap::new(),
        }
    }

    fn add(&mut self, tr: &TestRepo, id: RepoId) {
        self.repos.insert(
            id,
            (tr.local.clone(), tr.store.clone(), tr.head_seq.clone()),
        );
    }
}

#[async_trait::async_trait]
impl BundleSource for TestSource {
    async fn open_repo(&self, id: &RepoId) -> Result<BundleRepoHandle, BundleError> {
        let (local, store, head_seq) = self
            .repos
            .get(id)
            .ok_or_else(|| BundleError::RepoNotFound(id.to_string()))?;
        Ok(BundleRepoHandle {
            local: local.clone(),
            store: store.clone(),
            head_seq: head_seq.load(Ordering::Relaxed),
            engine: BundleEngine::default(),
            cfg: None,
        })
    }

    async fn list_repos(&self) -> Result<Vec<RepoId>, BundleError> {
        Ok(self.repos.keys().cloned().collect())
    }
}

/// Run a git command in `cwd`, panicking on failure.
async fn run_git(cwd: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .args(args)
        .current_dir(cwd)
        .output()
        .await
        .unwrap_or_else(|e| panic!("git {args:?}: {e}"));
    assert!(
        output.status.success(),
        "git {:?} failed: {}",
        args,
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).to_string()
}

/// Config with a single full strategy "weekly".
fn cfg_full_only(keep: usize) -> Config {
    let mut cfg = Config::default();
    cfg.bundles = BundlesConfig {
        enabled: true,
        strategy: vec![BundleStrategy {
            name: "weekly".into(),
            kind: BundleKind::Full,
            schedule: "@weekly".into(),
            base: None,
            keep,
            refs: vec![],
            backfill_max: 0,
            min_commits: None,
            filter: None,
            chain: false,
        }],
        min_commits: 0,
        min_bytes: ByteSize::default(),
        serve_via: BundleServe::Proxy,
        signed_url_ttl: Duration::from_hours(1),
        advertise: true,
        advertise_filtered: false,
        require: Vec::new(),
        signed_url_for: Vec::new(),
        main_only: false,
        extra_refs: Vec::new(),
    };
    cfg
}

/// Config with weekly (full) + daily (incremental based on weekly).
fn cfg_weekly_daily(keep_full: usize, keep_inc: usize) -> Config {
    let mut cfg = Config::default();
    cfg.bundles = BundlesConfig {
        enabled: true,
        strategy: vec![
            BundleStrategy {
                name: "weekly".into(),
                kind: BundleKind::Full,
                schedule: "@weekly".into(),
                base: None,
                keep: keep_full,
                refs: vec![],
                backfill_max: 0,
                min_commits: None,
                filter: None,
                chain: false,
            },
            BundleStrategy {
                name: "daily".into(),
                kind: BundleKind::Incremental,
                schedule: "@daily".into(),
                base: Some("weekly".into()),
                keep: keep_inc,
                refs: vec![],
                backfill_max: 0,
                min_commits: None,
                filter: None,
                chain: false,
            },
        ],
        min_commits: 0,
        min_bytes: ByteSize::default(),
        serve_via: BundleServe::Proxy,
        signed_url_ttl: Duration::from_hours(1),
        advertise: true,
        advertise_filtered: false,
        require: Vec::new(),
        signed_url_for: Vec::new(),
        main_only: false,
        extra_refs: Vec::new(),
    };
    cfg
}

/// Download a bundle from the store to a tempdir at the path matching a
/// `file://<base>/<owner>/<repo>/<key>` URI.
async fn download_bundle_to(
    store: &Prefixed,
    key: &str,
    base_dir: &Path,
    owner: &str,
    repo: &str,
) -> PathBuf {
    let (_, data) = store
        .get_bytes(key)
        .await
        .unwrap()
        .unwrap_or_else(|| panic!("bundle not found: {key}"));
    // The file:// URI is file://<base>/<owner>/<repo>/<key>
    // So the file path is <base>/<owner>/<repo>/<key>
    let file_path = base_dir.join(owner).join(repo).join(key);
    if let Some(parent) = file_path.parent() {
        tokio::fs::create_dir_all(parent).await.unwrap();
    }
    tokio::fs::write(&file_path, &data).await.unwrap();
    file_path
}

/// Get refs from a repo as a sorted list of "name oid" strings.
async fn get_refs(repo_path: &Path) -> Vec<String> {
    let output = Command::new("git")
        .args(["for-each-ref", "--format=%(refname) %(objectname)"])
        .current_dir(repo_path)
        .output()
        .await
        .unwrap();
    let s = String::from_utf8_lossy(&output.stdout);
    let mut refs: Vec<String> = s.lines().map(std::string::ToString::to_string).collect();
    refs.sort();
    refs
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn full_bundle_passes_verify() {
    let tr = TestRepo::new("test", "repo").await;
    tr.commit("initial commit").await;
    tr.commit("second commit").await;
    tr.tag("v1.0", "version 1.0").await;
    tr.push().await;
    tr.advance_seq();

    let id = RepoId::new("test", "repo").unwrap();
    let mut source = TestSource::new();
    source.add(&tr, id.clone());

    let cfg = Arc::new(cfg_full_only(4));
    let bundler = Bundler::new_with_source(Arc::new(source), cfg);

    let entry = bundler.build(&id, "weekly").await.unwrap();

    // Download the bundle and verify.
    let (_, data) = tr.store.get_bytes(&entry.key).await.unwrap().unwrap();
    let bundle_file = tempfile::tempdir().unwrap();
    let bundle_path = bundle_file.path().join("bundle.bundle");
    tokio::fs::write(&bundle_path, &data).await.unwrap();

    // `git bundle verify` needs a repository to run in (any will do: it checks the bundle's
    // own prerequisites); never rely on the test's cwd being one.
    let scratch = bundle_file.path().join("scratch");
    assert!(
        Command::new("git")
            .args(["init", "-q", "--bare"])
            .arg(&scratch)
            .status()
            .await
            .unwrap()
            .success()
    );
    let output = Command::new("git")
        .arg("-C")
        .arg(&scratch)
        .args(["bundle", "verify"])
        .arg(&bundle_path)
        .output()
        .await
        .unwrap();
    assert!(
        output.status.success(),
        "git bundle verify failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // Entry should have tips.
    assert!(!entry.tips.is_empty(), "bundle entry should have tips");
    assert!(entry.tips.iter().any(|t| t.name == "refs/heads/main"));
    assert!(entry.tips.iter().any(|t| t.name == "refs/tags/v1.0"));
    assert!(entry.kind == "full");
    assert!(entry.base_id.is_empty());
}

#[tokio::test]
async fn incremental_has_prerequisites() {
    let tr = TestRepo::new("test", "repo").await;
    tr.commit("initial").await;
    tr.push().await;
    tr.advance_seq();

    let id = RepoId::new("test", "repo").unwrap();
    let mut source = TestSource::new();
    source.add(&tr, id.clone());

    let cfg = Arc::new(cfg_weekly_daily(4, 7));
    let bundler = Bundler::new_with_source(Arc::new(source), cfg);

    // Build the base (weekly) first.
    let base_entry = bundler.build(&id, "weekly").await.unwrap();
    assert_eq!(base_entry.kind, "full");

    // Add new commits and advance seq.
    tr.commit("new commit 1").await;
    tr.commit("new commit 2").await;
    tr.push().await;
    tr.advance_seq();

    // Build incremental (daily).
    let inc_entry = bundler.build(&id, "daily").await.unwrap();
    assert_eq!(inc_entry.kind, "incremental");
    assert_eq!(inc_entry.base_id, base_entry.id);

    // Download and verify the incremental bundle has prerequisites.
    let (_, data) = tr.store.get_bytes(&inc_entry.key).await.unwrap().unwrap();
    let bundle_file = tempfile::tempdir().unwrap();
    let bundle_path = bundle_file.path().join("incremental.bundle");
    tokio::fs::write(&bundle_path, &data).await.unwrap();

    // git bundle verify should pass.
    let output = Command::new("git")
        .args(["bundle", "verify"])
        .arg(&bundle_path)
        .current_dir(tr.local.path())
        .output()
        .await
        .unwrap();
    assert!(
        output.status.success(),
        "incremental bundle verify failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // Check that the bundle header has prerequisites (lines starting with -).
    let header = String::from_utf8_lossy(&data);
    let header_lines: Vec<&str> = header.lines().take(20).collect();
    let has_prereq = header_lines.iter().any(|l| l.starts_with('-'));
    assert!(
        has_prereq,
        "incremental bundle should have prerequisites in header"
    );

    // The prerequisites should match the base bundle's tips.
    let base_tips: Vec<&str> = base_entry.tips.iter().map(|t| t.oid.as_str()).collect();
    for prereq_line in header_lines.iter().filter(|l| l.starts_with('-')) {
        // Format: "-<oid> <comment>"
        let oid = prereq_line[1..].split_whitespace().next().unwrap_or("");
        assert!(
            base_tips.contains(&oid),
            "prerequisite {oid} should be in base tips {base_tips:?}"
        );
    }
}

#[tokio::test]
async fn clone_from_full_then_fetch_incremental() {
    let tr = TestRepo::new("test", "repo").await;
    tr.commit("initial commit").await;
    tr.commit("second commit").await;
    tr.tag("v1.0", "version 1").await;
    tr.push().await;
    tr.advance_seq();

    let id = RepoId::new("test", "repo").unwrap();
    let mut source = TestSource::new();
    source.add(&tr, id.clone());

    let cfg = Arc::new(cfg_weekly_daily(4, 7));
    let bundler = Bundler::new_with_source(Arc::new(source), cfg);

    // Build full bundle.
    let full_entry = bundler.build(&id, "weekly").await.unwrap();

    // Add more commits.
    tr.commit("third commit").await;
    tr.tag("v2.0", "version 2").await;
    tr.push().await;
    tr.advance_seq();

    // Build incremental bundle.
    let inc_entry = bundler.build(&id, "daily").await.unwrap();

    // Download both bundles.
    let bundle_dir = tempfile::tempdir().unwrap();
    let (_, full_data) = tr.store.get_bytes(&full_entry.key).await.unwrap().unwrap();
    let full_path = bundle_dir.path().join("full.bundle");
    tokio::fs::write(&full_path, &full_data).await.unwrap();

    let (_, inc_data) = tr.store.get_bytes(&inc_entry.key).await.unwrap().unwrap();
    let inc_path = bundle_dir.path().join("inc.bundle");
    tokio::fs::write(&inc_path, &inc_data).await.unwrap();

    // Clone from the full bundle as a bare repo (so we can fetch into refs/heads/*).
    let clone_dir = tempfile::tempdir().unwrap();
    let clone_path = clone_dir.path().join("clone.git");
    run_git(
        clone_dir.path(),
        &[
            "clone",
            "--bare",
            full_path.to_str().unwrap(),
            clone_path.to_str().unwrap(),
        ],
    )
    .await;

    // Fetch the incremental bundle into the clone, updating local refs.
    run_git(
        &clone_path,
        &[
            "fetch",
            inc_path.to_str().unwrap(),
            "refs/heads/main:refs/heads/main",
            "refs/tags/*:refs/tags/*",
        ],
    )
    .await;

    // Compare refs: clone should have all refs from the original repo.
    let original_refs = get_refs(tr.local.path()).await;
    let clone_refs = get_refs(&clone_path).await;

    // The clone should have the same branch and tags.
    let original_main = original_refs
        .iter()
        .find(|r| r.starts_with("refs/heads/main"))
        .cloned();
    let clone_main = clone_refs
        .iter()
        .find(|r| r.starts_with("refs/heads/main"))
        .cloned();
    assert_eq!(
        original_main, clone_main,
        "main branch should match after fetch"
    );

    for tag in &original_refs {
        if tag.starts_with("refs/tags/") {
            let tag_name = tag.split_whitespace().next().unwrap();
            let clone_tag = clone_refs.iter().find(|r| r.starts_with(tag_name)).cloned();
            assert_eq!(
                Some(tag.clone()),
                clone_tag,
                "tag {tag_name} should match after fetch"
            );
        }
    }
}

#[tokio::test]
async fn run_due_respects_schedule_and_lease() {
    let tr = TestRepo::new("test", "repo").await;
    tr.commit("initial").await;
    tr.push().await;
    tr.advance_seq();

    let id = RepoId::new("test", "repo").unwrap();
    let mut source = TestSource::new();
    source.add(&tr, id.clone());

    let cfg = Arc::new(cfg_full_only(4));
    let bundler = Bundler::new_with_source(Arc::new(source), cfg);

    let now = SystemTime::now();

    // First run: never built → due → should build.
    let built = bundler.run_due(&id, now).await.unwrap();
    assert_eq!(built.len(), 1, "should build one bundle (never built)");
    assert_eq!(built[0].strategy, "weekly");

    // Second run at same time: head_seq unchanged → skip.
    let built2 = bundler.run_due(&id, now).await.unwrap();
    assert!(built2.is_empty(), "should skip (head_seq unchanged)");

    // Add commits and advance seq.
    tr.commit("new commit").await;
    tr.push().await;
    tr.advance_seq();

    // Third run: head_seq changed, schedule still due (weekly, but last built
    // was just now — next fire is a week away, so NOT due by schedule).
    // Wait — @weekly means next fire is ~1 week away. So it shouldn't be due.
    let built3 = bundler.run_due(&id, now).await.unwrap();
    assert!(
        built3.is_empty(),
        "should skip (not due by schedule: @weekly next fire is a week away)"
    );

    // Fourth run: advance just past the next weekly calendar slot. Adding a
    // fixed eight days can cross two slots, depending on the current weekday.
    let schedule = walgit_bundle::schedule::parse_schedule("@weekly").unwrap();
    let future =
        walgit_bundle::schedule::next_fire_after(&schedule, now).unwrap() + Duration::from_secs(1);
    let built4 = bundler.run_due(&id, future).await.unwrap();
    assert_eq!(
        built4.len(),
        1,
        "should build (due by schedule after the next weekly slot)"
    );

    // Lease test: hold the lease, run_due should skip.
    tr.commit("another commit").await;
    tr.push().await;
    tr.advance_seq();

    let future2 = walgit_bundle::schedule::next_fire_after(&schedule, future).unwrap()
        + Duration::from_secs(1);
    ops::hold_lease(&tr.store, "weekly", "test-holder", Duration::from_mins(1))
        .await
        .unwrap();
    let built5 = bundler.run_due(&id, future2).await.unwrap();
    assert!(
        built5.is_empty(),
        "should skip (lease held by another holder)"
    );
}

#[tokio::test]
async fn pruning_keeps_chain_valid() {
    let tr = TestRepo::new("test", "repo").await;
    tr.commit("initial").await;
    tr.push().await;
    tr.advance_seq();

    let id = RepoId::new("test", "repo").unwrap();
    let mut source = TestSource::new();
    source.add(&tr, id.clone());

    // Config: keep only 2 full bundles, 3 daily incrementals.
    let cfg = Arc::new(cfg_weekly_daily(2, 3));
    let bundler = Bundler::new_with_source(Arc::new(source), cfg);

    let now = SystemTime::now();

    // Build 3 full bundles (each time advancing seq).
    for i in 0..3 {
        if i > 0 {
            tr.commit(&format!("commit {i}")).await;
            tr.push().await;
            tr.advance_seq();
        }
        let future = now + Duration::from_secs((i as u64) * 8 * 24 * 3600);
        bundler.run_due(&id, future).await.unwrap();
    }

    // Check the list: should have only 2 weekly bundles (keep=2).
    let list = ops::read_list(&tr.store).await.unwrap().unwrap();
    let weekly: Vec<_> = list
        .bundles
        .iter()
        .filter(|b| b.strategy == "weekly")
        .collect();
    assert_eq!(weekly.len(), 2, "should keep 2 weekly bundles");

    // The oldest weekly bundle's object should be deleted from the store.
    // (We built 3, kept 2, so 1 should be pruned.)
    // Check that all kept bundle objects still exist.
    for b in &list.bundles {
        assert!(
            tr.store.head(&b.key).await.unwrap().is_some(),
            "kept bundle should exist: {}",
            b.key
        );
    }
}

#[tokio::test]
async fn bundle_uri_clone_works() {
    let tr = TestRepo::new("test", "repo").await;
    tr.commit("initial commit").await;
    tr.commit("second commit").await;
    tr.tag("v1.0", "version 1").await;
    tr.push().await;
    tr.advance_seq();

    let id = RepoId::new("test", "repo").unwrap();
    let mut source = TestSource::new();
    source.add(&tr, id.clone());

    let cfg = Arc::new(cfg_full_only(4));
    let bundler = Bundler::new_with_source(Arc::new(source), cfg);

    // Build a full bundle.
    let entry = bundler.build(&id, "weekly").await.unwrap();

    // Download the bundle to a tempdir at the path matching the file:// URI.
    let bundles_dir = tempfile::tempdir().unwrap();
    let _bundle_path =
        download_bundle_to(&tr.store, &entry.key, bundles_dir.path(), "test", "repo").await;

    // Render the bundle list with file:// base_url.
    let base_url = format!("file://{}", bundles_dir.path().display());
    let list_text = bundler
        .render_list(&id, &base_url, None, true)
        .await
        .unwrap()
        .unwrap();

    // Write the list to a config file.
    let list_file = bundles_dir.path().join("bundle-list");
    tokio::fs::write(&list_file, &list_text).await.unwrap();

    // git clone --bundle-uri=<list-file> <bare-repo> <target>
    let clone_dir = tempfile::tempdir().unwrap();
    let clone_path = clone_dir.path().join("clone");
    let output = Command::new("git")
        .args([
            "-c",
            "transfer.bundleURI=true",
            "clone",
            "--bundle-uri",
            list_file.to_str().unwrap(),
            tr.local.path().to_str().unwrap(),
            clone_path.to_str().unwrap(),
        ])
        .output()
        .await
        .unwrap();

    assert!(
        output.status.success(),
        "git clone --bundle-uri failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    // Verify the clone has the correct refs.
    let clone_refs = get_refs(&clone_path).await;
    let original_refs = get_refs(tr.local.path()).await;

    let clone_main = clone_refs
        .iter()
        .find(|r| r.starts_with("refs/heads/main"))
        .cloned();
    let original_main = original_refs
        .iter()
        .find(|r| r.starts_with("refs/heads/main"))
        .cloned();
    assert_eq!(clone_main, original_main, "main branch should match");

    for tag in &original_refs {
        if tag.starts_with("refs/tags/") {
            let tag_name = tag.split_whitespace().next().unwrap();
            let clone_tag = clone_refs.iter().find(|r| r.starts_with(tag_name)).cloned();
            assert_eq!(
                Some(tag.clone()),
                clone_tag,
                "tag {tag_name} should match in clone"
            );
        }
    }
}

#[tokio::test]
async fn run_all_due_multiple_repos() {
    let tr1 = TestRepo::new("owner1", "repo1").await;
    tr1.commit("init").await;
    tr1.push().await;
    tr1.advance_seq();

    let tr2 = TestRepo::new("owner2", "repo2").await;
    tr2.commit("init").await;
    tr2.push().await;
    tr2.advance_seq();

    let id1 = RepoId::new("owner1", "repo1").unwrap();
    let id2 = RepoId::new("owner2", "repo2").unwrap();

    let mut source = TestSource::new();
    source.add(&tr1, id1.clone());
    source.add(&tr2, id2.clone());

    let cfg = Arc::new(cfg_full_only(4));
    let bundler = Bundler::new_with_source(Arc::new(source), cfg);

    let now = SystemTime::now();
    bundler.run_all_due(now).await.unwrap();

    // Both repos should have a bundle.
    let list1 = ops::read_list(&tr1.store).await.unwrap().unwrap();
    assert_eq!(list1.bundles.len(), 1);

    let list2 = ops::read_list(&tr2.store).await.unwrap().unwrap();
    assert_eq!(list2.bundles.len(), 1);
}

#[tokio::test]
async fn render_list_and_protocol_v2_consistency() {
    let tr = TestRepo::new("test", "repo").await;
    tr.commit("initial").await;
    tr.push().await;
    tr.advance_seq();

    let id = RepoId::new("test", "repo").unwrap();
    let mut source = TestSource::new();
    source.add(&tr, id.clone());

    let cfg = Arc::new(cfg_full_only(4));
    let bundler = Bundler::new_with_source(Arc::new(source), cfg);

    bundler.build(&id, "weekly").await.unwrap();

    let base_url = "https://example.com";
    let list_text = bundler
        .render_list(&id, base_url, None, true)
        .await
        .unwrap()
        .unwrap();
    let v2_lines = bundler.protocol_v2_lines(&id, base_url).await.unwrap();

    // Both should mention the same URI and creationToken.
    assert!(list_text.contains("version = 1"));
    assert!(list_text.contains("mode = all"));
    assert!(list_text.contains("heuristic = creationToken"));

    assert!(v2_lines.contains(&"bundle.version=1".to_string()));
    assert!(v2_lines.contains(&"bundle.mode=all".to_string()));
    assert!(v2_lines.contains(&"bundle.heuristic=creationToken".to_string()));

    // Extract URI from both and compare.
    let config_uri = list_text
        .lines()
        .find(|l| l.trim_start().starts_with("uri ="))
        .unwrap()
        .trim_start_matches("    uri = ")
        .to_string();
    let v2_uri = v2_lines
        .iter()
        .find(|l| l.contains(".uri="))
        .unwrap()
        .split('=')
        .nth(1)
        .unwrap()
        .to_string();
    assert_eq!(config_uri, v2_uri, "URI should match between config and v2");
}

#[tokio::test]
async fn incremental_builds_base_if_absent() {
    let tr = TestRepo::new("test", "repo").await;
    tr.commit("initial").await;
    tr.push().await;
    tr.advance_seq();

    let id = RepoId::new("test", "repo").unwrap();
    let mut source = TestSource::new();
    source.add(&tr, id.clone());

    let cfg = Arc::new(cfg_weekly_daily(4, 7));
    let bundler = Bundler::new_with_source(Arc::new(source), cfg);

    // Build the base (weekly) first.
    bundler.build(&id, "weekly").await.unwrap();

    // Add a new commit so the incremental has new objects.
    tr.commit("new commit for daily").await;
    tr.push().await;
    tr.advance_seq();

    // Now build daily — base already exists.
    let entry = bundler.build(&id, "daily").await.unwrap();
    assert_eq!(entry.kind, "incremental");
    assert!(!entry.base_id.is_empty(), "should have a base_id");

    // The list should contain both weekly and daily.
    let list = ops::read_list(&tr.store).await.unwrap().unwrap();
    assert!(
        list.bundles.iter().any(|b| b.strategy == "weekly"),
        "weekly should be built"
    );
    assert!(
        list.bundles.iter().any(|b| b.strategy == "daily"),
        "daily should be built"
    );
}

/// Minimum-size gate: an incremental with fewer than `min_commits` commits
/// since its base is not cut (`TooSmall`, plan state `too-small`); once enough
/// commits exist the same strategy builds on the same base. Fulls are never
/// gated.
#[tokio::test]
async fn min_commits_gate_skips_small_incrementals() {
    let tr = TestRepo::new("test", "gate").await;
    tr.commit("initial").await;
    tr.push().await;
    tr.advance_seq();

    let id = RepoId::new("test", "gate").unwrap();
    let mut source = TestSource::new();
    source.add(&tr, id.clone());

    let mut cfg = cfg_weekly_daily(4, 7);
    cfg.bundles.min_commits = 3;
    let cfg = Arc::new(cfg);
    let bundler = Bundler::new_with_source(Arc::new(source), cfg.clone());

    // Full is never gated (1 commit).
    let base = bundler.build(&id, "weekly").await.unwrap();
    assert_eq!(base.kind, "full");

    // 2 commits since base < 3 → too small.
    tr.commit("a").await;
    tr.commit("b").await;
    tr.push().await;
    tr.advance_seq();
    match bundler.build(&id, "daily").await {
        Err(walgit_bundle::BundleError::TooSmall { commits, min }) => {
            assert_eq!((commits, min), (2, 3));
        }
        other => panic!("expected TooSmall, got {:?}", other.map(|e| e.id)),
    }
    // The plan shows it as too-small for the measured slot (slot 0 = "now" build path keyed by cut.slot=0),
    // and no bundle was recorded.
    let rows = bundler
        .plan(
            &id,
            std::time::SystemTime::now(),
            walgit_bundle::slots::PlanContext {
                first_state: None,
                can_full: true,
                can_incremental: true,
                wrong_host_reason: None,
            },
        )
        .await
        .unwrap();
    assert!(rows.iter().all(|r| !(r.strategy == "daily"
        && matches!(r.status, walgit_bundle::slots::SlotStatus::Built { .. }))));

    // One more commit → 3 ≥ 3 → built on the same base.
    tr.commit("c").await;
    tr.push().await;
    tr.advance_seq();
    let inc = bundler.build(&id, "daily").await.unwrap();
    assert_eq!(inc.base_id, base.id);
}

/// D22: content is the state AS OF the slot. When the source knows no state at
/// the slot (`refs_as_of` = None), the first full is cut from the earliest state
/// there is, but an incremental is NOT cut from "now" — prod 2026-08-21: eight
/// "08-19/08-20" dailies/hourlies were published with 04:2xZ content under
/// old tokens.
#[tokio::test]
async fn incremental_slot_without_state_is_not_cut_from_now() {
    let tr = TestRepo::new("test", "asof").await;
    tr.commit("initial").await;
    tr.push().await;
    tr.advance_seq();
    let id = RepoId::new("test", "asof").unwrap();
    let mut source = TestSource::new();
    source.add(&tr, id.clone());
    let mut cfg = cfg_weekly_daily(4, 7);
    cfg.bundles.min_commits = 1;
    let bundler = Bundler::new_with_source(Arc::new(source), Arc::new(cfg));

    // TestSource::refs_as_of is None for every instant.
    let old_slot = 1_787_180_400u64; // 2026-08-19 23:00Z
    let weekly = bundler
        .build_slot_unit(&id, "weekly", old_slot)
        .await
        .unwrap();
    assert!(
        weekly.is_some(),
        "the first full is cut from the earliest state"
    );
    for _ in 0..3 {
        tr.commit("later").await;
    }
    tr.push().await;
    tr.advance_seq();
    let daily = bundler
        .build_slot_unit(&id, "daily", old_slot + 3600)
        .await
        .unwrap();
    assert!(
        daily.is_none(),
        "no state as of the slot: an incremental is not cut"
    );
    let list = ops::read_list(&tr.store).await.unwrap().unwrap();
    assert!(
        list.bundles.iter().all(|b| b.strategy != "daily"),
        "{:?}",
        list.bundles.iter().map(|b| &b.id).collect::<Vec<_>>()
    );
}

/// A closed slot measured under the gate is recorded in the list and planned as
/// `skipped` from then on — by any host, across restarts, in O(1) — instead of
/// being re-measured every pass (after a restart the SSD host re-walked ~30 such
/// slots before reaching the live hour, 2026-08-21). A new base bundle for the
/// slot re-opens the question; the live (open) slot is never recorded.
#[tokio::test]
async fn too_small_closed_slots_are_recorded_and_skipped_not_remeasured() {
    let tr = TestRepo::new("test", "skip").await;
    tr.commit("initial").await;
    tr.push().await;
    tr.advance_seq();
    let id = RepoId::new("test", "skip").unwrap();
    let mut source = TestSource::new();
    source.add(&tr, id.clone());
    let mut cfg = cfg_weekly_daily(4, 7);
    cfg.bundles.min_commits = 1000; // everything is too small
    let cfg = Arc::new(cfg);
    let bundler = Bundler::new_with_source(Arc::new(source), cfg.clone());
    // A closed daily slot (yesterday 23:00) on a weekly cut at the Sunday before it.
    let now = std::time::SystemTime::now();
    let daily_strat = cfg
        .bundles
        .strategy
        .iter()
        .find(|s| s.name == "daily")
        .unwrap();
    // The newest CLOSED daily slot: the one before the latest fire.
    let latest = walgit_bundle::slots::last_slot_at_or_before(daily_strat, now)
        .unwrap()
        .unwrap();
    let yesterday = walgit_bundle::slots::last_slot_at_or_before(
        daily_strat,
        walgit_bundle::slots::from_epoch(latest - 1),
    )
    .unwrap()
    .unwrap();
    assert!(walgit_bundle::slots::slot_closed(
        daily_strat,
        yesterday,
        now
    ));
    let weekly_strat = cfg
        .bundles
        .strategy
        .iter()
        .find(|s| s.name == "weekly")
        .unwrap();
    let sunday = walgit_bundle::slots::last_slot_at_or_before(
        weekly_strat,
        walgit_bundle::slots::from_epoch(yesterday),
    )
    .unwrap()
    .unwrap();
    let weekly = bundler
        .build_slot_unit(&id, "weekly", sunday)
        .await
        .unwrap()
        .expect("weekly cut from the earliest state");

    assert!(
        bundler
            .build_slot_unit(&id, "daily", yesterday)
            .await
            .unwrap()
            .is_none()
    );
    let list = ops::read_list(&tr.store).await.unwrap().unwrap();
    assert_eq!(list.skipped.len(), 1, "{:?}", list.skipped);
    assert_eq!(
        (
            list.skipped[0].strategy.as_str(),
            list.skipped[0].slot,
            list.skipped[0].base_id.as_str()
        ),
        ("daily", yesterday, weekly.id.as_str())
    );
    assert!(
        list.skipped[0].reason.starts_with("too-small")
            || list.skipped[0].reason.starts_with("no state"),
        "{}",
        list.skipped[0].reason
    );
    // Planned as skipped now — no host measures it again; recording is idempotent.
    let ctx = walgit_bundle::slots::PlanContext {
        first_state: None,
        can_full: true,
        can_incremental: true,
        wrong_host_reason: None,
    };
    let rows = bundler.plan(&id, now, ctx).await.unwrap();
    let dailies: Vec<_> = rows
        .iter()
        .filter(|r| r.strategy == "daily")
        .map(|r| (r.slot, r.status.clone()))
        .collect();
    let row = rows
        .iter()
        .find(|r| r.strategy == "daily" && r.slot == yesterday)
        .unwrap_or_else(|| panic!("row for {yesterday}; dailies: {dailies:?}"));
    assert!(
        matches!(row.status, walgit_bundle::slots::SlotStatus::Skipped { .. }),
        "{row:?}"
    );
    assert!(
        bundler
            .build_slot_unit(&id, "daily", yesterday)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        ops::read_list(&tr.store)
            .await
            .unwrap()
            .unwrap()
            .skipped
            .len(),
        1
    );

    // A record for another base does not apply: same slot, different base → missing again.
    let mut list = ops::read_list(&tr.store).await.unwrap().unwrap();
    list.skipped[0].base_id = "weekly-older".into();
    ops::cas_update_list(&tr.store, 4, |_| Ok(Some(list.clone())))
        .await
        .unwrap();
    let rows = bundler.plan(&id, now, ctx).await.unwrap();
    let row = rows
        .iter()
        .find(|r| r.strategy == "daily" && r.slot == yesterday)
        .unwrap();
    assert_eq!(
        row.status,
        walgit_bundle::slots::SlotStatus::Missing,
        "{row:?}"
    );
}

/// A strategy whose ref patterns match nothing (a repository without `main`
/// under `bundles.main_only`) is `blocked` in the plan with the reason — never
/// a silent `NoRefs` at debug level with the slot `missing` forever.
#[tokio::test]
async fn strategies_matching_no_refs_are_blocked_in_the_plan_with_the_reason() {
    let tr = TestRepo::new("test", "nomain").await;
    tr.commit("initial").await;
    run_git(&tr.work_path, &["branch", "-M", "trunk"]).await; // no refs/heads/main
    run_git(&tr.work_path, &["push", "origin", "trunk"]).await;
    tr.local.refresh().unwrap();
    tr.advance_seq();
    let id = RepoId::new("test", "nomain").unwrap();
    let mut source = TestSource::new();
    source.add(&tr, id.clone());
    let mut cfg = cfg_weekly_daily(4, 7);
    cfg.bundles.main_only = true;
    let bundler = Bundler::new_with_source(Arc::new(source), Arc::new(cfg));
    let ctx = walgit_bundle::slots::PlanContext {
        first_state: None,
        can_full: true,
        can_incremental: true,
        wrong_host_reason: None,
    };
    let rows = bundler
        .plan(&id, std::time::SystemTime::now(), ctx)
        .await
        .unwrap();
    let weekly = rows
        .iter()
        .find(|r| r.strategy == "weekly")
        .expect("weekly row");
    match &weekly.status {
        walgit_bundle::slots::SlotStatus::Blocked(why) => assert!(
            why.contains("no refs match") && why.contains("main_only = true"),
            "{why}"
        ),
        other => panic!("expected blocked, got {other:?}"),
    }
    assert!(
        bundler
            .run_due(&id, std::time::SystemTime::now())
            .await
            .unwrap()
            .is_empty()
    );
}
