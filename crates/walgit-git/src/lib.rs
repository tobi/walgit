//! Local git repository engine: gix in-process for odb/refs/revwalk/pack
//! generation; upstream git subprocess for ingest (`index-pack`), repack,
//! bundle, and the selectable Engine::Git upload-pack fallback. See AGENTS.md
//! D2 and docs/CONTRACT.md walgit-git.

pub mod follow;
pub mod pkt;
pub mod receive;
pub mod repair;
pub mod upload_gix;
pub use upload_gix::ObjectFaulter;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;

pub use gix_hash::{self, ObjectId};
use gix_object::{FindExt, FindHeader, Kind as ObjKind};
use gix_traverse::tree::Visit as TreeVisit;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::process::Command;
use tracing::Instrument;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[derive(Debug, Error)]
pub enum GitError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("gix error: {0}")]
    Gix(#[from] Box<dyn std::error::Error + Send + Sync>),
    #[error("pack error")]
    Pack,
    #[error("ref conflict on {name}: expected {expected}, actual {actual}")]
    RefConflict {
        name: String,
        expected: String,
        actual: String,
    },
    #[error("missing object {oid}")]
    MissingObject { oid: String },
    #[error("fsck failed: {0}")]
    Fsck(String),
    #[error("subprocess `{cmd}` exited {status:?}: {stderr}")]
    Subprocess {
        cmd: String,
        status: Option<i32>,
        stderr: String,
    },
    #[error("invalid input: {0}")]
    InvalidInput(String),
    #[error("protocol error: {0}")]
    Protocol(String),
}

fn ge<E: std::error::Error + Send + Sync + 'static>(e: E) -> GitError {
    GitError::Gix(Box::new(e))
}

/// Reject ref names that would inject `git update-ref --stdin` commands or
/// poison packed-refs (newlines, NULs, git-illegal bytes).
pub fn validate_ref_name(name: &str) -> Result<(), GitError> {
    if name == "HEAD" {
        return Ok(());
    }
    let bad = name.is_empty()
        || !name.starts_with("refs/")
        || name.bytes().any(|b| {
            matches!(
                b,
                0 | b'\n' | b'\r' | b' ' | b'~' | b'^' | b':' | b'?' | b'*' | b'[' | b'\\'
            )
        })
        || name.contains("..")
        || name.contains("@{")
        || name.contains("//")
        || name.starts_with('/')
        || name.ends_with('/')
        || name.ends_with('.')
        || name.ends_with(".lock");
    if bad {
        Err(GitError::InvalidInput(format!("invalid ref name {name:?}")))
    } else {
        Ok(())
    }
}

/// Full hex oid (sha1 40 or sha256 64). Empty / all-zeros is a delete/create marker.
pub fn validate_oid(oid: &str) -> Result<(), GitError> {
    if oid.is_empty() || oid.bytes().all(|b| b == b'0') {
        return Ok(());
    }
    let n = oid.len();
    if (n == 40 || n == 64) && oid.bytes().all(|b| b.is_ascii_hexdigit()) {
        Ok(())
    } else {
        Err(GitError::InvalidInput(format!("invalid oid {oid:?}")))
    }
}

pub fn validate_ref_update(u: &walgit_proto::v1::RefUpdate) -> Result<(), GitError> {
    if !u.new_symbolic_target.is_empty() {
        if u.name != "HEAD" {
            return Err(GitError::InvalidInput(format!(
                "symbolic update is only allowed for HEAD, got {:?}",
                u.name
            )));
        }
        return validate_ref_name(&u.new_symbolic_target);
    }
    validate_ref_name(&u.name)?;
    validate_oid(&u.old_oid)?;
    validate_oid(&u.new_oid)
}

const WALGIT_VERSION: &str = env!("CARGO_PKG_VERSION");

#[cfg(test)]
mod validate_ref_tests {
    use super::*;

    #[test]
    fn ref_names_reject_injection() {
        assert!(validate_ref_name("HEAD").is_ok());
        assert!(validate_ref_name("refs/heads/main").is_ok());
        assert!(validate_ref_name("refs/heads/foo\nupdate refs/heads/main").is_err());
        assert!(validate_ref_name("refs/heads/foo\0bar").is_err());
        assert!(validate_ref_name("../etc/passwd").is_err());
        assert!(validate_oid(&"0".repeat(40)).is_ok());
        assert!(validate_oid("gg").is_err());
        assert!(validate_oid(&"a".repeat(40)).is_ok());
    }
}

// ---------------------------------------------------------------------------
// RepoId
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RepoId {
    owner: String,
    name: String,
}

impl RepoId {
    pub fn new(owner: impl Into<String>, name: impl Into<String>) -> Result<Self, GitError> {
        let owner = owner.into();
        let name = name.into();
        validate_part(&owner, "owner")?;
        validate_part(&name, "name")?;
        Ok(RepoId { owner, name })
    }

    pub fn owner(&self) -> &str {
        &self.owner
    }
    pub fn name(&self) -> &str {
        &self.name
    }

    /// `repos/<owner>/<repo>/` (walgit_proto::keys::repo_prefix).
    pub fn store_prefix(&self) -> String {
        walgit_proto::keys::repo_prefix(&self.owner, &self.name)
    }

    /// `<root>/<owner>/<name>.git` — the on-disk bare repo path.
    pub fn local_dir(&self, root: &Path) -> PathBuf {
        root.join(&self.owner).join(format!("{}.git", self.name))
    }
}

fn validate_part(s: &str, what: &str) -> Result<(), GitError> {
    if s.is_empty() || s.len() > 100 {
        return Err(GitError::InvalidInput(format!(
            "{what} must be 1..=100 chars"
        )));
    }
    if s == ".." {
        return Err(GitError::InvalidInput(format!("{what} may not be '..'")));
    }
    if s.starts_with('.') {
        return Err(GitError::InvalidInput(format!(
            "{what} may not start with '.'"
        )));
    }
    if !s
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'_' || b == b'-')
    {
        return Err(GitError::InvalidInput(format!(
            "{what} must be ASCII [A-Za-z0-9._-]"
        )));
    }
    Ok(())
}

impl std::str::FromStr for RepoId {
    type Err = GitError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim();
        let s = s.strip_suffix(".git").unwrap_or(s);
        let (owner, name) = s
            .split_once('/')
            .ok_or_else(|| GitError::InvalidInput("RepoId must be 'owner/name'".into()))?;
        if owner.is_empty() || name.is_empty() {
            return Err(GitError::InvalidInput(
                "RepoId parts must be non-empty".into(),
            ));
        }
        RepoId::new(owner, name)
    }
}

impl std::fmt::Display for RepoId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.owner, self.name)
    }
}

// ---------------------------------------------------------------------------
// ObjectFormat
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ObjectFormat {
    Sha1,
    Sha256,
}

impl ObjectFormat {
    pub fn as_str(&self) -> &'static str {
        match self {
            ObjectFormat::Sha1 => "sha1",
            ObjectFormat::Sha256 => "sha256",
        }
    }
    pub fn kind(&self) -> gix_hash::Kind {
        match self {
            ObjectFormat::Sha1 => gix_hash::Kind::Sha1,
            ObjectFormat::Sha256 => gix_hash::Kind::Sha256,
        }
    }
}

impl From<walgit_config::ObjectFormat> for ObjectFormat {
    fn from(f: walgit_config::ObjectFormat) -> Self {
        match f {
            walgit_config::ObjectFormat::Sha1 => ObjectFormat::Sha1,
            walgit_config::ObjectFormat::Sha256 => ObjectFormat::Sha256,
        }
    }
}

impl From<gix_hash::Kind> for ObjectFormat {
    fn from(k: gix_hash::Kind) -> Self {
        match k {
            gix_hash::Kind::Sha1 => ObjectFormat::Sha1,
            gix_hash::Kind::Sha256 => ObjectFormat::Sha256,
            _ => ObjectFormat::Sha1,
        }
    }
}

// ---------------------------------------------------------------------------
// Public data structs
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct IngestOptions {
    pub fsck: bool,
    pub max_bytes: Option<u64>,
    pub thin: bool,
}

#[derive(Debug, Clone)]
pub struct IngestedPack {
    pub checksum: gix_hash::ObjectId,
    pub pack_path: PathBuf,
    pub idx_path: PathBuf,
    pub pack_size: u64,
    pub idx_size: u64,
    pub object_count: u64,
}

#[derive(Debug, Clone)]
pub struct PackInfo {
    pub checksum: gix_hash::ObjectId,
    pub pack_size: u64,
    pub idx_size: u64,
    pub object_count: u64,
    pub has_rev: bool,
    pub has_bitmap: bool,
    /// `pack-<checksum>.commit-graph` side-file: a split commit-graph layer
    /// covering the pack's commits (see [`LocalRepo::write_pack_commit_graph`]).
    pub has_commit_graph: bool,
    /// `Some(base)` when this is a **history pack** (commits + trees derived
    /// from `base`, marker file `pack-<checksum>.history` holding the base
    /// checksum; see [`LocalRepo::write_history_pack`]).
    pub history_of: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ref {
    pub name: String,
    pub oid: String,
    pub peeled: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RefSnapshotData {
    pub refs: Vec<Ref>,
    pub head_target: String,
}

impl From<walgit_proto::v1::RefSnapshot> for RefSnapshotData {
    fn from(s: walgit_proto::v1::RefSnapshot) -> Self {
        let refs = s
            .refs
            .into_iter()
            .map(|r| Ref {
                name: r.name,
                oid: r.oid,
                peeled: r.peeled,
            })
            .collect();
        RefSnapshotData {
            refs,
            head_target: s.head_target,
        }
    }
}

impl From<RefSnapshotData> for walgit_proto::v1::RefSnapshot {
    fn from(d: RefSnapshotData) -> Self {
        let refs = d
            .refs
            .into_iter()
            .map(|r| walgit_proto::v1::Ref {
                name: r.name,
                oid: r.oid,
                peeled: r.peeled,
            })
            .collect();
        walgit_proto::v1::RefSnapshot {
            seq: 0,
            object_format: "sha1".into(),
            refs,
            head_target: d.head_target,
            created_at: None,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct LsRefsArgs {
    pub ref_prefixes: Vec<String>,
    pub symrefs: bool,
    pub peel: bool,
    pub unborn: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LsRefsLine {
    pub name: String,
    pub oid: String,
    pub peeled: String,
    pub symref_target: Option<String>,
}

impl LsRefsLine {
    /// Render the line per protocol-v2 ls-refs format:
    /// `<oid> <name>` optionally followed by ` symref-target:<t>` and/or
    /// ` peeled:<oid>`, then a trailing newline.
    pub fn render(&self, args: &LsRefsArgs) -> String {
        let mut s = format!("{} {}", self.oid, self.name);
        if args.symrefs || self.oid == "unborn" {
            if let Some(t) = &self.symref_target {
                s.push_str(&format!(" symref-target:{t}"));
            }
        }
        if args.peel && !self.peeled.is_empty() {
            s.push_str(&format!(" peeled:{}", self.peeled));
        }
        s.push('\n');
        s
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Service {
    UploadPack,
    ReceivePack,
}

impl std::str::FromStr for Service {
    type Err = GitError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "git-upload-pack" => Ok(Service::UploadPack),
            "git-receive-pack" => Ok(Service::ReceivePack),
            other => Err(GitError::InvalidInput(format!("unknown service {other}"))),
        }
    }
}

impl Service {
    pub fn as_str(&self) -> &'static str {
        match self {
            Service::UploadPack => "git-upload-pack",
            Service::ReceivePack => "git-receive-pack",
        }
    }
}

#[derive(Debug, Clone)]
pub struct UploadPackRequest {
    pub wants: Vec<gix_hash::ObjectId>,
    pub haves: Vec<gix_hash::ObjectId>,
    pub done: bool,
    pub thin_pack: bool,
    pub no_progress: bool,
    pub include_tag: bool,
    pub ofs_delta: bool,
    pub sideband_all: bool,
    pub wait_for_done: bool,
    pub filter: Option<String>,
    pub deepen: Option<u32>,
    pub deepen_since: Option<i64>,
    pub deepen_not: Vec<String>,
    pub shallow: Vec<gix_hash::ObjectId>,
    pub want_refs: Vec<String>,
    pub packfile_uris_protocols: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub struct UploadPackStats {
    pub objects: u64,
    pub bytes: u64,
}

#[derive(Debug, Clone)]
pub enum RepackMode {
    Geometric { factor: u32 },
    Full,
}

#[derive(Debug, Clone)]
pub struct RepackOptions {
    pub mode: RepackMode,
    pub write_bitmap: bool,
    pub write_midx: bool,
    pub keep: Vec<gix_hash::ObjectId>,
}

#[derive(Debug, Clone, Default)]
pub struct RepackResult {
    pub new_packs: Vec<PackInfo>,
    pub removed: Vec<gix_hash::ObjectId>,
}

#[derive(Debug, Clone)]
pub struct BundleInfo {
    pub size: u64,
    pub pack_offset: u64,
}

/// Outcome of [`LocalRepo::fsck_streaming`].
#[derive(Debug, Clone, serde::Serialize)]
pub struct FsckReport {
    pub ok: bool,
    pub exit_code: Option<i32>,
    pub problems: u64,
}

// ---------------------------------------------------------------------------
// LocalRepo
// ---------------------------------------------------------------------------

/// Point lookups over a shared, name-sorted ref snapshot plus an overlay of
/// pending changes: O(log n) instead of building an O(n) `HashMap` per push
/// (500 k refs = a 34 MB map per push on both the verify and the publish path,
/// 2026-08-21). The overlay holds what a batch applied so far.
pub struct RefView {
    base: Arc<RefSnapshotData>,
    overlay: HashMap<String, Option<String>>,
    head_target: Option<String>,
}

impl RefView {
    pub fn new(base: Arc<RefSnapshotData>) -> Self {
        Self {
            base,
            overlay: HashMap::new(),
            head_target: None,
        }
    }
    /// Current oid (or symbolic target for symrefs) of `name`; `None` = absent.
    pub fn get(&self, name: &str) -> Option<String> {
        if let Some(v) = self.overlay.get(name) {
            return v.clone();
        }
        if name == "HEAD" {
            return self.head_oid();
        }
        self.base
            .refs
            .binary_search_by(|r| r.name.as_str().cmp(name))
            .ok()
            .map(|i| self.base.refs[i].oid.clone())
    }
    pub fn head_target(&self) -> &str {
        self.head_target
            .as_deref()
            .unwrap_or(&self.base.head_target)
    }
    /// HEAD's symbolic target as of the overlay (a pending `HEAD` symref update).
    pub fn set_head_target(&mut self, target: String) {
        self.head_target = Some(target);
    }
    /// HEAD's oid through its symbolic target (None when unborn/detached-empty).
    pub fn head_oid(&self) -> Option<String> {
        let target = self.head_target().to_string();
        if target.is_empty() {
            return None;
        }
        self.get(&target)
    }
    pub fn set(&mut self, name: &str, value: String) {
        self.overlay.insert(name.to_string(), Some(value));
    }
    pub fn remove(&mut self, name: &str) {
        self.overlay.insert(name.to_string(), None);
    }
}

struct Inner {
    id: RepoId,
    path: PathBuf,
    format: ObjectFormat,
    tsr: parking_lot::Mutex<gix::ThreadSafeRepository>,
    ingest_lock: tokio::sync::Mutex<()>,
    /// Parsed refs, shared by every reader until a ref write or a change of
    /// `packed-refs`/`HEAD` on disk (see [`LocalRepo::refs_arc`]).
    refs_cache: parking_lot::Mutex<Option<RefsCached>>,
    /// Bumped by every ref writer in this process; part of the cache key.
    refs_gen: std::sync::atomic::AtomicU64,
    /// How often `packed-refs` + loose refs were parsed (tests assert pushes do not add to it).
    refs_parses: std::sync::atomic::AtomicU64,
}

#[derive(Clone)]
struct RefsCached {
    key: RefsKey,
    /// The parsed (or last materialized) snapshot.
    data: Arc<RefSnapshotData>,
    /// Ref transactions this process applied since `data` was built, not yet folded in: a push
    /// records its txn here in O(k) and the next *reader* that needs the full sorted vector
    /// pays one O(n) copy for all of them ([`LocalRepo::refs_arc`]); lookups ([`LocalRepo::ref_view`])
    /// never materialize.
    pending: Vec<walgit_proto::v1::RefTransaction>,
}

#[derive(PartialEq, Eq, Clone, Copy)]
struct RefsKey {
    generation: u64,
    packed_len: u64,
    packed_mtime: Option<std::time::SystemTime>,
    head_mtime: Option<std::time::SystemTime>,
}

fn refs_key(path: &Path, generation: u64) -> RefsKey {
    let packed = std::fs::metadata(path.join("packed-refs")).ok();
    let head = std::fs::metadata(path.join("HEAD")).ok();
    RefsKey {
        generation,
        packed_len: packed.as_ref().map(|m| m.len()).unwrap_or(0),
        packed_mtime: packed.and_then(|m| m.modified().ok()),
        head_mtime: head.and_then(|m| m.modified().ok()),
    }
}

#[derive(Clone)]
pub struct LocalRepo {
    inner: Arc<Inner>,
}

impl LocalRepo {
    /// Create a bare repo at `<root>/<owner>/<name>.git`.
    pub fn init(root: &Path, id: &RepoId, format: ObjectFormat) -> Result<Self, GitError> {
        let path = id.local_dir(root);
        std::fs::create_dir_all(path.parent().unwrap_or(root)).map_err(|e| GitError::Io(e))?;
        // `git init --bare [--object-format=...] <path>`.
        let mut cmd = std::process::Command::new("git");
        cmd.arg("init").arg("--bare");
        if format == ObjectFormat::Sha256 {
            cmd.arg("--object-format=sha256");
        }
        cmd.arg(&path);
        let out = cmd.output().map_err(GitError::Io)?;
        if !out.status.success() {
            return Err(GitError::Subprocess {
                cmd: "git init".into(),
                status: out.status.code(),
                stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
            });
        }
        // Deterministic HEAD -> refs/heads/main regardless of the host's
        // init.defaultBranch (git init writes master/main depending on config).
        std::fs::write(path.join("HEAD"), "ref: refs/heads/main\n").map_err(GitError::Io)?;
        // Permissive upload-pack config so filter / any-sha1 fetches work.
        // `pack.writeReverseIndex`: every pack this repo writes (index-pack on
        // ingest, repack in compaction) gets a `.rev` — git < 2.41 defaults it
        // off, and without one `pack-objects` builds the reverse index of the
        // base in memory on EVERY fetch: 60 M entries, 962 MB, 2.85 s flat on
        // the SSD host (2026-08-21, a large repository's serving copy had no .rev at all).
        for (k, v) in [
            ("uploadpack.allowFilter", "true"),
            ("uploadpack.allowAnySHA1InWant", "true"),
            ("uploadpack.allowSidebandAll", "true"),
            ("pack.writeReverseIndex", "true"),
        ] {
            let _ = std::process::Command::new("git")
                .arg("-C")
                .arg(&path)
                .args(["config", k, v])
                .output();
        }
        let tsr = gix::ThreadSafeRepository::open(&path).map_err(ge)?;
        Ok(LocalRepo {
            inner: Arc::new(Inner {
                id: id.clone(),
                path,
                format,
                tsr: parking_lot::Mutex::new(tsr),
                ingest_lock: tokio::sync::Mutex::new(()),
                refs_cache: parking_lot::Mutex::new(None),
                refs_gen: std::sync::atomic::AtomicU64::new(0),
                refs_parses: std::sync::atomic::AtomicU64::new(0),
            }),
        })
    }

    /// Open an existing bare repo. Returns `Ok(None)` if it does not exist.
    pub fn open(root: &Path, id: &RepoId) -> Result<Option<Self>, GitError> {
        let path = id.local_dir(root);
        if !path.is_dir() || !path.join("HEAD").exists() {
            return Ok(None);
        }
        let tsr = gix::ThreadSafeRepository::open(&path).map_err(ge)?;
        // Detect object format from config.
        let repo = gix::Repository::from(&tsr);
        let kind = repo.object_hash();
        let format = ObjectFormat::from(kind);
        Ok(Some(LocalRepo {
            inner: Arc::new(Inner {
                id: id.clone(),
                path,
                format,
                tsr: parking_lot::Mutex::new(tsr),
                ingest_lock: tokio::sync::Mutex::new(()),
                refs_cache: parking_lot::Mutex::new(None),
                refs_gen: std::sync::atomic::AtomicU64::new(0),
                refs_parses: std::sync::atomic::AtomicU64::new(0),
            }),
        }))
    }

    pub fn id(&self) -> &RepoId {
        &self.inner.id
    }
    pub fn path(&self) -> &Path {
        &self.inner.path
    }
    pub fn object_format(&self) -> ObjectFormat {
        self.inner.format
    }

    /// Per-call gix handle cloned from the shared thread-safe repository.
    pub fn gix(&self) -> gix::Repository {
        let tsr = self.inner.tsr.lock();
        gix::Repository::from(&*tsr)
    }

    /// Re-open so gix sees new packed-refs / HEAD without remapping pack indexes.
    /// Ref-only writers use this; pack installs use [`refresh`].
    pub fn refresh_refs(&self) -> Result<(), GitError> {
        self.refs_changed();
        let tsr = gix::ThreadSafeRepository::open(&self.inner.path).map_err(ge)?;
        *self.inner.tsr.lock() = tsr;
        Ok(())
    }

    /// Re-open the underlying repository so the odb/refs reflect on-disk
    /// changes from pack/ref writes.
    pub fn refresh(&self) -> Result<(), GitError> {
        self.refs_changed();
        let tsr = gix::ThreadSafeRepository::open(&self.inner.path).map_err(ge)?;
        // Load every pack index / the midx NOW, on this (blocking) thread.
        // gix's odb is lazy: without this the first object lookup after a
        // refresh — a request on an async worker — pays for mmapping and
        // reading a 2.5 GB midx + 2.1 GB idx (prod 2026-08-21: the front
        // stalled 20–30 min after /readyz while a large repository's history pack landed).
        {
            let repo = gix::Repository::from(&tsr);
            let t = std::time::Instant::now();
            // `iter()` snapshots the store with all indices loaded.
            let _ = repo.objects.iter();
            let ms = t.elapsed().as_millis() as u64;
            if ms > 200 {
                tracing::info!(repo = %self.inner.path.display(), ms, "odb indices loaded");
            }
        }
        *self.inner.tsr.lock() = tsr;
        Ok(())
    }

    /// [`refresh`] off the async runtime: re-opening a repository with a
    /// multi-GB index / midx is filesystem work that must never run on a
    /// tokio worker (prod: every other request on the instance stalled for
    /// minutes while a history pack was installed).
    pub async fn refresh_async(&self) -> Result<(), GitError> {
        let this = self.clone();
        tokio::task::spawn_blocking(move || this.refresh())
            .await
            .map_err(|e| GitError::Protocol(format!("refresh task: {e}")))?
    }

    fn objects_pack_dir(&self) -> PathBuf {
        self.inner.path.join("objects").join("pack")
    }

    // ---- packs ----

    /// Stream a packfile in, index it with `git index-pack`, and install
    /// `pack-<checksum>.{pack,idx,rev}` into `objects/pack/`. Thin packs
    /// (`opts.thin`, every receive-pack) use `--fix-thin` against this
    /// repo's ODB. Empty input returns Ok(None). `opts.fsck` adds
    /// `--fsck-objects` so object parse happens in the same pass as
    /// indexing (a large repository: 64 k objects used to spend tens of seconds in a
    /// second gix walk after a gix write).
    pub async fn ingest_pack<R: AsyncRead + Unpin + Send + 'static>(
        &self,
        mut pack: R,
        opts: IngestOptions,
    ) -> Result<Option<IngestedPack>, GitError> {
        let span = tracing::info_span!(
            "git.ingest_pack",
            repo = %self.inner.id,
            objects = 0u64,
            bytes = 0u64,
            engine = "git",
            thin = opts.thin,
            feed_ms = 0u64,
            git_ms = 0u64,
        );
        // Instrument each awaited operation rather than carrying a thread-local
        // span guard across await points.
        // Pack installation and repository refresh are not safe concurrently
        // with gix's pack/index readers. Serialize ingestion per repository;
        // callers may still run ingests for different repositories in parallel.
        let _ingest_guard = self.inner.ingest_lock.lock().instrument(span.clone()).await;

        let pack_dir = self.objects_pack_dir();
        std::fs::create_dir_all(&pack_dir).map_err(GitError::Io)?;

        // Reserve the temporary path atomically. A timestamp suffix alone can
        // collide when many ingest calls start in the same scheduler tick.
        let (tmp_path, tmp_file) = loop {
            let candidate = pack_dir.join(format!("tmp-ingest-{}.pack", unique_suffix()));
            match std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&candidate)
            {
                Ok(file) => break (candidate, file),
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(e) => return Err(GitError::Io(e)),
            }
        };
        let mut tmp = tokio::fs::File::from_std(tmp_file);
        let mut total: u64 = 0;
        let mut buf = vec![0u8; 64 * 1024];
        let mut empty_check = true;
        loop {
            let n = pack
                .read(&mut buf)
                .instrument(span.clone())
                .await
                .map_err(GitError::Io)?;
            if n == 0 {
                break;
            }
            empty_check = false;
            total += n as u64;
            if let Some(max) = opts.max_bytes {
                if total > max {
                    drop(
                        tokio::fs::remove_file(&tmp_path)
                            .instrument(span.clone())
                            .await,
                    );
                    return Err(GitError::InvalidInput(format!(
                        "pack exceeds max_bytes {max}"
                    )));
                }
            }
            tmp.write_all(&buf[..n])
                .instrument(span.clone())
                .await
                .map_err(GitError::Io)?;
        }
        // tokio's File buffers writes in a background blocking task and does
        // NOT flush on drop: without this the tail of the pack may be missing
        // when index-pack reads it back (seen as "failed to fill whole buffer"
        // under load).
        tmp.flush()
            .instrument(span.clone())
            .await
            .map_err(GitError::Io)?;
        drop(tmp);
        span.record("bytes", total);
        if empty_check {
            let _ = tokio::fs::remove_file(&tmp_path)
                .instrument(span.clone())
                .await;
            return Ok(None);
        }

        // `git index-pack` is the receive-pack ingest engine: it is the tool
        // that `--fix-thin` + `--threads` + `--fsck-objects` + `--rev-index`
        // were built for. gix-pack 0.73's write path was the previous engine
        // (a second object walk for fsck, no `.rev`, and a parser hole that
        // already fell back here). A large repository: 64,317 objects / 75 MB in 49.1 s
        // on that path (2026-08-21).
        let repo_path = self.inner.path.clone();
        let tmp_for_index = tmp_path.clone();
        let fix_thin = opts.thin;
        let fsck = opts.fsck;
        let index_span = tracing::info_span!(
            parent: &span,
            "git.ingest_pack.index",
            feed_ms = 0u64,
            git_ms = 0u64,
            phases = tracing::field::Empty,
        );
        let indexed = tokio::task::spawn_blocking(move || {
            git_index_pack(&tmp_for_index, &repo_path, fix_thin, fsck)
        })
        .instrument(index_span.clone())
        .await
        .map_err(|e| GitError::Io(std::io::Error::other(e)));
        let _ = tokio::fs::remove_file(&tmp_path)
            .instrument(span.clone())
            .await;
        let outcome = indexed??;
        index_span.record("feed_ms", outcome.feed_ms);
        index_span.record("git_ms", outcome.git_ms);
        index_span.record("phases", outcome.phases.as_str());
        span.record("feed_ms", outcome.feed_ms);
        span.record("git_ms", outcome.git_ms);
        tracing::info!(
            parent: &index_span,
            feed_ms = outcome.feed_ms,
            git_ms = outcome.git_ms,
            phases = %outcome.phases,
            "git.index_pack.trace2"
        );
        let checksum = outcome.checksum;
        let pack_path = outcome.pack_path;
        let idx_path = outcome.idx_path;
        let object_count = outcome.object_count;
        span.record("objects", object_count);
        // A ref-only push (`git push origin main:feature` with nothing new)
        // sends a 32-byte pack with zero objects. Nothing to publish.
        if object_count == 0 {
            let _ = std::fs::remove_file(&pack_path);
            let _ = std::fs::remove_file(&idx_path);
            let _ = std::fs::remove_file(pack_path.with_extension("rev"));
            return Ok(None);
        }
        let pack_size = std::fs::metadata(&pack_path).map(|m| m.len()).unwrap_or(0);
        let idx_size = std::fs::metadata(&idx_path).map(|m| m.len()).unwrap_or(0);
        self.refresh_async()
            .instrument(tracing::info_span!(parent: &span, "git.ingest_pack.refresh"))
            .await?;
        Ok(Some(IngestedPack {
            checksum,
            pack_path,
            idx_path,
            pack_size,
            idx_size,
            object_count,
        }))
    }

    /// Atomically move downloaded files into `objects/pack/`, then refresh.
    pub async fn install_pack(
        &self,
        pack: &Path,
        idx: &Path,
        extra: &[PathBuf],
    ) -> Result<(), GitError> {
        let pack_dir = self.objects_pack_dir();
        std::fs::create_dir_all(&pack_dir).map_err(GitError::Io)?;
        let dst_pack = pack_dir.join(pack.file_name().unwrap());
        let dst_idx = pack_dir.join(idx.file_name().unwrap());
        rename_atomic(pack, &dst_pack)?;
        rename_atomic(idx, &dst_idx)?;
        for e in extra {
            let dst = pack_dir.join(e.file_name().unwrap());
            rename_atomic(e, &dst)?;
        }
        self.refresh_async().await?;
        Ok(())
    }

    /// Write `pack-<checksum>.rev` for an installed pack **from its `.idx`
    /// alone** (no pack bytes read: `git index-pack --rev-index` re-indexes
    /// the whole pack — 4 GB of a large repository's 32 GB in 52 min, 2026-08-21). Without
    /// a `.rev` git rebuilds the reverse index in memory on EVERY
    /// `pack-objects`: a large repository's 60 M-object base cost 2.85 s per fetch
    /// (the original large-repository measurements). The file is a bucket side-file like `.bitmap`:
    /// the caller uploads it (`RepoHandle::annotate_pack`) so the fleet
    /// converges once. Returns the path; a no-op when it already exists.
    pub async fn write_rev_index(&self, checksum: &gix_hash::oid) -> Result<PathBuf, GitError> {
        let rev = self.pack_path(checksum).with_extension("rev");
        if rev.exists() {
            return Ok(rev);
        }
        let idx = self.pack_path(checksum).with_extension("idx");
        let kind = self.object_format().kind();
        let rev_out = rev.clone();
        tokio::task::spawn_blocking(move || write_rev_from_idx(&idx, &rev_out, kind))
            .await
            .map_err(|e| GitError::InvalidInput(format!("rev index task: {e}")))??;
        self.refresh_async().await?;
        Ok(rev)
    }

    /// Delete `.pack/.idx/.rev/.bitmap` for `checksum`. Caller guarantees no
    /// readers.
    pub fn remove_pack(&self, checksum: &gix_hash::oid) -> Result<(), GitError> {
        let hex = checksum.to_hex();
        let pack_dir = self.objects_pack_dir();
        let was_history = pack_dir.join(format!("pack-{hex}.history")).exists();
        for ext in ["pack", "idx", "rev", "bitmap", "commit-graph", "history"] {
            let p = pack_dir.join(format!("pack-{hex}.{ext}"));
            match std::fs::remove_file(&p) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(GitError::Io(e)),
            }
        }
        if was_history {
            // The midx must not name a pack that is gone.
            self.write_history_midx_blocking()?;
        }
        Ok(())
    }

    pub fn packs(&self) -> Result<Vec<PackInfo>, GitError> {
        let pack_dir = self.objects_pack_dir();
        let mut out = Vec::new();
        let rd = match std::fs::read_dir(&pack_dir) {
            Ok(rd) => rd,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(GitError::Io(e)),
        };
        for ent in rd {
            let ent = ent.map_err(GitError::Io)?;
            let name = ent.file_name();
            let name = name.to_string_lossy();
            if !name.starts_with("pack-") || !name.ends_with(".pack") {
                continue;
            }
            let hex = &name["pack-".len()..name.len() - ".pack".len()];
            let checksum = match gix_hash::ObjectId::from_hex(hex.as_bytes()) {
                Ok(o) => o,
                Err(_) => continue,
            };
            let pack_path = ent.path();
            let idx_path = pack_path.with_extension("idx");
            let pack_size = std::fs::metadata(&pack_path).map(|m| m.len()).unwrap_or(0);
            let idx_size = std::fs::metadata(&idx_path).map(|m| m.len()).unwrap_or(0);
            let object_count = idx_object_count(&idx_path).unwrap_or(0);
            let has_rev = pack_path.with_extension("rev").exists();
            let has_bitmap = pack_path.with_extension("bitmap").exists();
            let has_commit_graph = pack_path.with_extension("commit-graph").exists();
            let history_of = std::fs::read_to_string(pack_path.with_extension("history"))
                .ok()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty());
            out.push(PackInfo {
                checksum,
                pack_size,
                idx_size,
                object_count,
                has_rev,
                has_bitmap,
                has_commit_graph,
                history_of,
            });
        }
        out.sort_by_key(|p| p.checksum);
        Ok(out)
    }

    pub fn pack_path(&self, checksum: &gix_hash::oid) -> PathBuf {
        let hex = checksum.to_hex();
        self.objects_pack_dir().join(format!("pack-{hex}.pack"))
    }

    // ---- refs ----

    pub fn refs(&self) -> Result<RefSnapshotData, GitError> {
        Ok((*self.refs_arc()?).clone())
    }

    /// The refs, parsed once and shared: `packed-refs` of a 500 k-ref repo is
    /// 34 MB and read_refs also peels every tag — 1–2 s per call, which every
    /// `ls-refs` (prefix or not) paid (2026-08-21, test/refs500k on a serverless host).
    /// Valid until a ref writer in this process bumps the generation or
    /// `packed-refs`/`HEAD` change on disk (two stats per call). Sorted by name.
    pub fn refs_arc(&self) -> Result<Arc<RefSnapshotData>, GitError> {
        let key = refs_key(
            &self.inner.path,
            self.inner
                .refs_gen
                .load(std::sync::atomic::Ordering::Acquire),
        );
        {
            let mut guard = self.inner.refs_cache.lock();
            if let Some(c) = guard.as_mut()
                && c.key == key
            {
                if !c.pending.is_empty() {
                    // Fold the pushes applied since the last materialization: one copy of the
                    // vector for all of them, no parsing, no object reads.
                    let t = std::time::Instant::now();
                    let patched = self.patch_snapshot(&c.data, &c.pending);
                    c.data = Arc::new(patched);
                    c.pending.clear();
                    if c.data.refs.len() >= 10_000 {
                        tracing::debug!(repo = %self.inner.id, refs = c.data.refs.len(), ms = t.elapsed().as_millis() as u64, "refs cache materialized from pending txns");
                    }
                }
                return Ok(c.data.clone());
            }
        }
        let t = std::time::Instant::now();
        let data = Arc::new(read_refs(&self.inner.path)?);
        self.inner
            .refs_parses
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if data.refs.len() >= 10_000 {
            tracing::debug!(repo = %self.inner.id, refs = data.refs.len(), ms = t.elapsed().as_millis() as u64, "refs parsed into the cache");
        }
        *self.inner.refs_cache.lock() = Some(RefsCached {
            key,
            data: data.clone(),
            pending: Vec::new(),
        });
        Ok(data)
    }

    /// Number of full ref parses so far (`packed-refs` + loose refs + tag peeling): the O(refs)
    /// cost a push must not incur (AGENTS §1.4).
    pub fn refs_parses(&self) -> u64 {
        self.inner
            .refs_parses
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Point lookups over the current refs without materializing: the cached snapshot plus an
    /// overlay of the transactions applied since (O(k)). What the push path uses for
    /// `check_old` and the publisher's working view — a push after a push costs O(k), not a
    /// 500 k-entry copy.
    pub fn ref_view(&self) -> Result<RefView, GitError> {
        let key = refs_key(
            &self.inner.path,
            self.inner
                .refs_gen
                .load(std::sync::atomic::Ordering::Acquire),
        );
        let cached = self
            .inner
            .refs_cache
            .lock()
            .as_ref()
            .filter(|c| c.key == key)
            .cloned();
        let Some(c) = cached else {
            return Ok(RefView::new(self.refs_arc()?));
        };
        let mut view = RefView::new(c.data);
        for txn in &c.pending {
            for u in &txn.updates {
                if !u.new_symbolic_target.is_empty() {
                    if u.name == "HEAD" {
                        view.set_head_target(u.new_symbolic_target.clone());
                    }
                    continue;
                }
                if u.new_oid.is_empty() || u.new_oid.chars().all(|c| c == '0') {
                    view.remove(&u.name);
                } else {
                    view.set(&u.name, u.new_oid.clone());
                }
            }
        }
        Ok(view)
    }

    /// Invalidate the refs cache (every ref writer in this file calls it).
    fn refs_changed(&self) {
        self.inner
            .refs_gen
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
    }

    pub fn apply_ref_txn(
        &self,
        txn: &walgit_proto::v1::RefTransaction,
        check_old: bool,
    ) -> Result<(), GitError> {
        let span = tracing::info_span!(
            "git.apply_ref_txn",
            repo = %self.inner.id,
            n_updates = txn.updates.len(),
            check_old,
        );
        let _enter = span.enter();
        // The cached snapshot as of now (if current): patched and re-installed after the txn
        // instead of thrown away — see the end of this function. Taken before git touches
        // packed-refs (a delete of a packed ref rewrites it and would change the key).
        let before = {
            let key = refs_key(
                &self.inner.path,
                self.inner
                    .refs_gen
                    .load(std::sync::atomic::Ordering::Acquire),
            );
            self.inner
                .refs_cache
                .lock()
                .as_ref()
                .filter(|c| c.key == key)
                .cloned()
        };

        for u in &txn.updates {
            validate_ref_update(u)?;
        }

        // Pre-check old values for clear error reporting.
        if check_old {
            let view = self.ref_view()?;
            for u in &txn.updates {
                if !u.new_symbolic_target.is_empty() {
                    continue;
                }
                let current = view.get(&u.name).unwrap_or_default();
                let old = u.old_oid.trim_start_matches('0');
                let cur = current.trim_start_matches('0');
                if old.is_empty() {
                    // must not exist
                    if !cur.is_empty() {
                        return Err(GitError::RefConflict {
                            name: u.name.clone(),
                            expected: u.old_oid.clone(),
                            actual: current.to_string(),
                        });
                    }
                } else if old != cur {
                    return Err(GitError::RefConflict {
                        name: u.name.clone(),
                        expected: u.old_oid.clone(),
                        actual: current.to_string(),
                    });
                }
            }
        }

        // Build `git update-ref --stdin` transaction for oid updates. Symbolic
        // ref updates (HEAD target) are applied separately by writing the HEAD
        // file directly — the `symref` command is not universally available in
        // update-ref --stdin (e.g. older forks), so we avoid it.
        let mut input = String::new();
        let mut symref_updates: Vec<&walgit_proto::v1::RefUpdate> = Vec::new();
        let mut has_oid_cmds = false;
        input.push_str("start\n");
        for u in &txn.updates {
            if !u.new_symbolic_target.is_empty() {
                symref_updates.push(u);
                continue;
            }
            has_oid_cmds = true;
            let new_zero = u.new_oid.chars().all(|c| c == '0') || u.new_oid.is_empty();
            let old_zero = u.old_oid.chars().all(|c| c == '0') || u.old_oid.is_empty();
            if new_zero {
                // delete
                if check_old && !old_zero {
                    input.push_str(&format!("delete {} {}\n", u.name, u.old_oid));
                } else {
                    input.push_str(&format!("delete {}\n", u.name));
                }
            } else if check_old && old_zero {
                input.push_str(&format!("create {} {}\n", u.name, u.new_oid));
            } else if check_old && !old_zero {
                input.push_str(&format!("update {} {} {}\n", u.name, u.new_oid, u.old_oid));
            } else {
                input.push_str(&format!("update {} {}\n", u.name, u.new_oid));
            }
        }

        if has_oid_cmds {
            input.push_str("prepare\ncommit\n");
            let out = std::process::Command::new("git")
                .current_dir(&self.inner.path)
                .env("GIT_DIR", &self.inner.path)
                .args(["update-ref", "--stdin"])
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .and_then(|mut c| {
                    {
                        let stdin = c.stdin.as_mut().unwrap();
                        stdin.write_all(input.as_bytes())?;
                    }
                    c.wait_with_output()
                })
                .map_err(GitError::Io)?;

            if !out.status.success() {
                if let Some(name) = find_conflict(&String::from_utf8_lossy(&out.stderr)) {
                    let snap = self.refs().unwrap_or_default();
                    let map: HashMap<String, Ref> =
                        snap.refs.into_iter().map(|r| (r.name.clone(), r)).collect();
                    let actual = map.get(&name).map(|r| r.oid.clone()).unwrap_or_default();
                    let expected = txn
                        .updates
                        .iter()
                        .find(|u| u.name == name)
                        .map(|u| u.old_oid.clone())
                        .unwrap_or_default();
                    return Err(GitError::RefConflict {
                        name: name.clone(),
                        expected,
                        actual,
                    });
                }
                return Err(GitError::Subprocess {
                    cmd: "git update-ref --stdin".into(),
                    status: out.status.code(),
                    stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
                });
            }
        }

        // Apply symbolic ref updates by writing the HEAD file directly.
        for u in &symref_updates {
            let head_path = self.inner.path.join("HEAD");
            std::fs::write(&head_path, format!("ref: {}\n", u.new_symbolic_target))
                .map_err(GitError::Io)?;
        }
        // Patch the snapshot we started from instead of throwing it away: re-parsing
        // packed-refs + peeling 100 k tags was the O(refs) term every push handed to the next
        // request (~700 ms debug / ~200 ms release at 500 k refs, AGENTS §1.4). `refresh_refs()`
        // bumps the generation without remapping pack indexes.
        self.refresh_refs()?;
        self.refs_changed();
        if let Some(mut c) = before {
            c.pending.push(txn.clone());
            c.key = refs_key(
                &self.inner.path,
                self.inner
                    .refs_gen
                    .load(std::sync::atomic::Ordering::Acquire),
            );
            *self.inner.refs_cache.lock() = Some(c);
        }
        Ok(())
    }

    /// `base` (name-sorted) with `txns` applied in order: O(k log n) lookups + one O(n) copy of
    /// the vector (no parsing, no object reads except peeling a new annotated tag whose update
    /// did not carry `new_peeled`).
    fn patch_snapshot(
        &self,
        base: &RefSnapshotData,
        txns: &[walgit_proto::v1::RefTransaction],
    ) -> RefSnapshotData {
        let mut refs = base.refs.clone();
        let mut head_target = base.head_target.clone();
        let mut repo: Option<gix::Repository> = None;
        for u in txns.iter().flat_map(|t| t.updates.iter()) {
            if !u.new_symbolic_target.is_empty() {
                if u.name == "HEAD" {
                    head_target = u.new_symbolic_target.clone();
                }
                continue;
            }
            let delete = u.new_oid.is_empty() || u.new_oid.chars().all(|c| c == '0');
            let pos = refs.binary_search_by(|r| r.name.as_str().cmp(u.name.as_str()));
            match (pos, delete) {
                (Ok(i), true) => {
                    refs.remove(i);
                }
                (Err(_), true) => {}
                (pos, false) => {
                    let mut peeled = u.new_peeled.clone();
                    if peeled.is_empty() && u.name.starts_with("refs/tags/") {
                        let r = repo.get_or_insert_with(|| {
                            gix::Repository::from(
                                &gix::ThreadSafeRepository::open(&self.inner.path)
                                    .expect("repo open"),
                            )
                        });
                        if let Ok(oid) = gix_hash::ObjectId::from_hex(u.new_oid.as_bytes()) {
                            peeled = peel_tag(r, oid)
                                .map(|p| p.to_hex().to_string())
                                .unwrap_or_default();
                        }
                    }
                    let entry = Ref {
                        name: u.name.clone(),
                        oid: u.new_oid.clone(),
                        peeled,
                    };
                    match pos {
                        Ok(i) => refs[i] = entry,
                        Err(i) => refs.insert(i, entry),
                    }
                }
            }
        }
        RefSnapshotData { refs, head_target }
    }

    /// Replace ALL refs + HEAD by writing `packed-refs` directly and removing
    /// loose refs. Fast for very large ref sets.
    pub fn load_ref_snapshot(&self, snap: &walgit_proto::v1::RefSnapshot) -> Result<(), GitError> {
        self.refs_changed();
        let path = &self.inner.path;
        let packed = path.join("packed-refs");
        let mut content = String::new();
        content.push_str("# pack-refs with: peeled fully-peeled sorted \n");
        let mut refs = snap.refs.clone();
        refs.sort_by(|a, b| a.name.cmp(&b.name));
        for r in &refs {
            content.push_str(&format!("{} {}\n", r.oid, r.name));
            if !r.peeled.is_empty() {
                content.push_str(&format!("^{}\n", r.peeled));
            }
        }
        // Atomic write.
        let tmp = packed.with_extension("tmp");
        std::fs::write(&tmp, content).map_err(GitError::Io)?;
        rename_atomic(&tmp, &packed)?;

        // Remove loose refs (everything under refs/, keep HEAD).
        let refs_dir = path.join("refs");
        if refs_dir.exists() {
            let _ = std::fs::remove_dir_all(&refs_dir);
            // gix requires the refs directory to exist; recreate the standard
            // skeleton so the repository remains openable.
            let _ = std::fs::create_dir_all(refs_dir.join("heads"));
            let _ = std::fs::create_dir_all(refs_dir.join("tags"));
        }
        // Rewrite HEAD symbolic target.
        if !snap.head_target.is_empty() {
            std::fs::write(path.join("HEAD"), format!("ref: {}\n", snap.head_target))
                .map_err(GitError::Io)?;
        }
        self.refresh_refs()?;
        Ok(())
    }

    /// Apply already-committed WAL ref transactions without `git update-ref`.
    ///
    /// `git update-ref` refuses to point a ref at an object that is not in the
    /// local odb, so it cannot be used by a replica that has applied the WAL's
    /// *refs* but not (yet) downloaded its packs ("refs-first" sync, the cheap
    /// cold-start path). The log is trusted (the publisher verified old values
    /// and connectivity), so this merges the updates into the current ref set
    /// in memory and rewrites `packed-refs` + `HEAD` once, exactly like
    /// `load_ref_snapshot`. Peeled values for new annotated tags are filled in
    /// when the tag object happens to be present locally.
    pub fn apply_ref_txns_offline(
        &self,
        txns: &[&walgit_proto::v1::RefTransaction],
    ) -> Result<(), GitError> {
        let span = tracing::info_span!(
            "git.apply_ref_txns_offline",
            repo = %self.inner.id,
            n_txns = txns.len(),
        );
        let _enter = span.enter();
        let snap = self.refs()?;
        let mut head_target = snap.head_target;
        let mut map: BTreeMap<String, Ref> =
            snap.refs.into_iter().map(|r| (r.name.clone(), r)).collect();
        let repo = self.gix();
        for txn in txns {
            for u in &txn.updates {
                validate_ref_update(u)?;
                if !u.new_symbolic_target.is_empty() {
                    if u.name == "HEAD" {
                        head_target = u.new_symbolic_target.clone();
                    }
                    continue;
                }
                let new_zero = u.new_oid.is_empty() || u.new_oid.chars().all(|c| c == '0');
                if new_zero {
                    map.remove(&u.name);
                    continue;
                }
                let peeled = if !u.new_peeled.is_empty() {
                    u.new_peeled.clone()
                } else if u.name.starts_with("refs/tags/") {
                    gix_hash::ObjectId::from_hex(u.new_oid.as_bytes())
                        .ok()
                        .and_then(|oid| peel_tag(&repo, oid))
                        .map(|p| p.to_hex().to_string())
                        .unwrap_or_default()
                } else {
                    String::new()
                };
                map.insert(
                    u.name.clone(),
                    Ref {
                        name: u.name.clone(),
                        oid: u.new_oid.clone(),
                        peeled,
                    },
                );
            }
        }
        let data = RefSnapshotData {
            refs: map.into_values().collect(),
            head_target,
        };
        self.load_ref_snapshot(&data.into())
    }

    pub fn pack_refs(&self) -> Result<(), GitError> {
        self.git_cmd_sync(&["pack-refs", "--all", "--prune"])?;
        self.refresh_refs()?;
        Ok(())
    }

    // ---- objects ----

    pub fn has_object(&self, oid: &gix_hash::oid) -> bool {
        let repo = self.gix();
        repo.has_object(oid)
    }

    /// Write one object into the loose store (`objects/xx/yyyy…`) with a known
    /// id (no re-hashing). Used to fault objects read from a remote pack into
    /// the local copy so unmodified `git` commands can run against them.
    /// Idempotent: an existing object is left alone.
    pub fn write_loose_object(
        &self,
        kind: gix_object::Kind,
        oid: &gix_hash::oid,
        data: &[u8],
    ) -> Result<(), GitError> {
        use gix_object::Write as _;
        let hex = oid.to_hex().to_string();
        let path = self
            .inner
            .path
            .join("objects")
            .join(&hex[..2])
            .join(&hex[2..]);
        if path.exists() {
            return Ok(());
        }
        let store = gix_odb::loose::Store::at(
            self.inner.path.join("objects"),
            gix_odb::loose::Options {
                object_hash: oid.kind(),
                ..Default::default()
            },
        );
        store
            .write_buf_with_known_id(kind, data, oid.to_owned())
            .map_err(|e| GitError::Gix(e))?;
        Ok(())
    }

    /// Every object reachable from tips exists. When stop_at_existing_refs,
    /// objects already reachable from current refs are assumed present and
    /// only the new set is verified. Uses gix revwalk with .with_hidden(
    /// existing ref tips) for commit traversal and gix_traverse::tree
    /// breadthfirst for tree traversal with a seen-set.
    pub fn check_connectivity(
        &self,
        tips: &[gix_hash::ObjectId],
        stop_at_existing_refs: bool,
    ) -> Result<(), GitError> {
        let span = tracing::info_span!(
            "git.check_connectivity",
            repo = %self.inner.id,
            tips = tips.len(),
            stop_at_existing_refs,
        );
        let _enter = span.enter();

        // Mirror pushes commonly contain thousands of refs at the same tip.
        // Avoid an O(number-of-updates) object lookup and duplicate rev-walk
        // roots for those requests.
        let mut unique_tips = Vec::with_capacity(tips.len());
        let mut tip_set = HashSet::with_capacity(tips.len());
        for tip in tips {
            if tip_set.insert(*tip) {
                unique_tips.push(*tip);
            }
        }
        if unique_tips.is_empty() {
            return Ok(());
        }

        let repo = self.gix();
        let mut seen: HashSet<gix_hash::ObjectId> = HashSet::new();
        let mut buf = Vec::new();
        let mut tree_state = gix_traverse::tree::breadthfirst::State::default();

        // Tips may be commits, annotated tags (possibly nested), trees or blobs.
        // Peel tags (verifying every object in the chain exists), walk trees and
        // check blobs directly; only commits seed the rev-walk.
        let mut commit_tips = Vec::with_capacity(unique_tips.len());
        for t in &unique_tips {
            match peel_tip(&repo, *t, &mut seen, &mut buf)? {
                (gix_object::Kind::Commit, id) => commit_tips.push(id),
                (gix_object::Kind::Tree, id) => {
                    if seen.insert(id) {
                        let tree_iter = repo
                            .objects
                            .find_tree_iter(&id, &mut buf)
                            .map_err(|e| GitError::Gix(Box::new(e)))?;
                        let mut visitor = ConnectivityVisitor {
                            seen: &mut seen,
                            repo: &repo,
                            missing: None,
                        };
                        if let Err(e) = gix_traverse::tree::breadthfirst(
                            tree_iter,
                            &mut tree_state,
                            &repo.objects,
                            &mut visitor,
                        ) {
                            return Err(match visitor.missing {
                                Some(oid) => GitError::MissingObject {
                                    oid: oid.to_hex().to_string(),
                                },
                                None => GitError::Gix(Box::new(e)),
                            });
                        }
                    }
                }
                (_, _) => {}
            }
        }
        if commit_tips.is_empty() {
            return Ok(());
        }

        // Collect hidden tips (existing ref tips, peeled to commits) for stop-at-existing.
        let hidden: Vec<gix_hash::ObjectId> = if stop_at_existing_refs {
            let snap = self.refs()?;
            let mut seen_hidden = HashSet::with_capacity(snap.refs.len());
            let mut out = Vec::with_capacity(snap.refs.len());
            for r in &snap.refs {
                // Prefer the pre-peeled oid for tags; otherwise peel cheaply via the odb.
                let candidate = if !r.peeled.is_empty() {
                    r.peeled.as_str()
                } else {
                    r.oid.as_str()
                };
                let Ok(oid) = gix_hash::ObjectId::from_hex(candidate.as_bytes()) else {
                    continue;
                };
                if !seen_hidden.insert(oid) {
                    continue;
                }
                match repo.objects.try_header(&oid) {
                    Ok(Some(h)) if h.kind == gix_object::Kind::Commit => out.push(oid),
                    Ok(Some(h)) if h.kind == gix_object::Kind::Tag => {
                        let mut ignore = HashSet::new();
                        if let Ok((gix_object::Kind::Commit, id)) =
                            peel_tip(&repo, oid, &mut ignore, &mut buf)
                        {
                            out.push(id);
                        }
                    }
                    _ => {}
                }
            }
            out
        } else {
            Vec::new()
        };

        // Walk commits from tips, hiding existing ref tips.
        let walk = repo
            .rev_walk(commit_tips)
            .with_hidden(hidden.iter().copied())
            .all()
            .map_err(|e| GitError::Gix(Box::new(e)))?;

        for item in walk {
            let info = item.map_err(|e| GitError::Gix(Box::new(e)))?;
            let cid = info.id;
            if !seen.insert(cid) {
                continue;
            }
            if !repo.has_object(&cid) {
                return Err(GitError::MissingObject {
                    oid: cid.to_hex().to_string(),
                });
            }
            // Get the commit's tree id.
            let mut commit = repo
                .objects
                .find_commit_iter(&cid, &mut buf)
                .map_err(|e| GitError::Gix(Box::new(e)))?;
            let tree_id = commit.tree_id().map_err(|e| ge(e))?;
            if seen.insert(tree_id) {
                if !repo.has_object(&tree_id) {
                    return Err(GitError::MissingObject {
                        oid: tree_id.to_hex().to_string(),
                    });
                }
                let tree_iter = repo
                    .objects
                    .find_tree_iter(&tree_id, &mut buf)
                    .map_err(|e| GitError::Gix(Box::new(e)))?;
                let mut visitor = ConnectivityVisitor {
                    seen: &mut seen,
                    repo: &repo,
                    missing: None,
                };
                if let Err(e) = gix_traverse::tree::breadthfirst(
                    tree_iter,
                    &mut tree_state,
                    &repo.objects,
                    &mut visitor,
                ) {
                    return Err(match visitor.missing {
                        Some(oid) => GitError::MissingObject {
                            oid: oid.to_hex().to_string(),
                        },
                        None => GitError::Gix(Box::new(e)),
                    });
                }
            }
        }
        Ok(())
    }

    /// [`check_connectivity`] on a blocking thread. The walk inflates trees
    /// against the pack set (5.5 s on a 2,852-commit push) and
    /// must not sit on a tokio worker.
    pub async fn check_connectivity_async(
        &self,
        tips: &[gix_hash::ObjectId],
        stop_at_existing_refs: bool,
    ) -> Result<(), GitError> {
        let this = self.clone();
        let tips = tips.to_vec();
        tokio::task::spawn_blocking(move || this.check_connectivity(&tips, stop_at_existing_refs))
            .await
            .map_err(|e| GitError::Protocol(format!("connectivity task: {e}")))?
    }

    // ---- protocol, server side ----

    /// protocol v2 fetch with in-process gix pack generation. Handles
    /// negotiation (ACK common haves that exist, NAK, ready), shallow-info
    /// (deepen by depth), wanted-refs, then generates the packfile via
    /// gix_pack::data::output (count + entries with delta reuse from on-disk
    /// packs), framed in sideband-64k (channel 1; progress on 2 unless
    /// no_progress) with a final flush. UploadPackStats populated with object
    /// count and byte count.
    pub async fn upload_pack<W: AsyncWrite + Unpin + Send>(
        &self,
        req: UploadPackRequest,
        out: W,
    ) -> Result<UploadPackStats, GitError> {
        let wants = req.wants.len();
        let haves = req.haves.len();
        let span = tracing::info_span!(
            "git.upload_pack",
            repo = %self.inner.id,
            engine = "gix",
            wants,
            haves,
            objects = 0u64,
            bytes = 0u64,
        );
        let stats = self
            .upload_pack_gix(req, out)
            .instrument(span.clone())
            .await?;
        span.record("objects", stats.objects);
        span.record("bytes", stats.bytes);
        Ok(stats)
    }

    /// Raw passthrough: `git upload-pack --stateless-rpc` with `GIT_PROTOCOL`
    /// set from `protocol`. `body` is the request, `out` receives the response.
    pub async fn upload_pack_raw<R, W>(
        &self,
        protocol: pkt::Protocol,
        body: R,
        out: W,
    ) -> Result<(), GitError>
    where
        R: AsyncRead + Unpin + Send,
        W: AsyncWrite + Unpin + Send,
    {
        let span = tracing::info_span!(
            "git.upload_pack",
            repo = %self.inner.id,
            engine = "git",
        );
        self.run_upload_pack_stateless_io(protocol, body, out)
            .instrument(span.clone())
            .await
    }

    pub fn ls_refs(&self, args: &LsRefsArgs) -> Result<Vec<LsRefsLine>, GitError> {
        let span = tracing::debug_span!(
            "git.ls_refs",
            repo = %self.inner.id,
            prefixes = args.ref_prefixes.len(),
        );
        let _enter = span.enter();

        let snap = self.refs_arc()?;
        let mut lines = Vec::new();
        let head_target = snap.head_target.clone();
        // Resolve HEAD's target before filtering: a ref-prefix that excludes the
        // target ref must not prevent HEAD itself from being advertised.
        let head_oid = if head_target.is_empty() {
            None
        } else {
            snap.refs
                .binary_search_by(|r| r.name.as_str().cmp(head_target.as_str()))
                .ok()
                .map(|i| snap.refs[i].oid.clone())
        };
        // Prefix selection is O(log n + k) over the name-sorted list: each
        // prefix is one range (binary search for its start, scan while it
        // matches); ranges are merged so overlapping prefixes emit once.
        let selected: Vec<&Ref> = if args.ref_prefixes.is_empty() {
            snap.refs.iter().collect()
        } else {
            let mut ranges: Vec<(usize, usize)> = args
                .ref_prefixes
                .iter()
                .map(|p| {
                    let start = snap.refs.partition_point(|r| r.name.as_str() < p.as_str());
                    let mut end = start;
                    while end < snap.refs.len() && snap.refs[end].name.starts_with(p.as_str()) {
                        end += 1;
                    }
                    (start, end)
                })
                .collect();
            ranges.sort_unstable();
            let mut out = Vec::new();
            let mut cursor = 0usize;
            for (a, b) in ranges {
                let a = a.max(cursor);
                if a < b {
                    out.extend(snap.refs[a..b].iter());
                    cursor = b;
                }
            }
            out
        };
        for r in selected {
            if r.name == "HEAD" {
                continue; // rendered below from head_target/head_oid
            }
            lines.push(LsRefsLine {
                name: r.name.clone(),
                oid: r.oid.clone(),
                peeled: r.peeled.clone(),
                symref_target: None,
            });
        }
        // HEAD is advertised whenever a prefix matches it (empty prefixes match
        // all). `symrefs` only controls the `symref-target:` attribute, except for
        // the unborn form which always carries it (protocol-v2 ls-refs).
        let head_matches = args.ref_prefixes.is_empty()
            || args
                .ref_prefixes
                .iter()
                .any(|p| "HEAD".starts_with(p.as_str()));
        if head_matches && !head_target.is_empty() {
            match head_oid {
                Some(oid) => lines.push(LsRefsLine {
                    name: "HEAD".to_string(),
                    oid,
                    peeled: String::new(),
                    symref_target: Some(head_target.clone()),
                }),
                None if args.unborn => lines.push(LsRefsLine {
                    name: "HEAD".to_string(),
                    oid: "unborn".to_string(),
                    peeled: String::new(),
                    symref_target: Some(head_target.clone()),
                }),
                None => {}
            }
        }
        Ok(lines)
    }

    /// v0 advertisement with capabilities. The HTTP server prepends the
    /// `# service=<svc>\n` pkt-line + flush.
    pub fn advertise_refs_v0(&self, service: Service, out: &mut Vec<u8>) -> Result<(), GitError> {
        let snap = self.refs()?;
        let caps = capabilities_for(service, self.inner.format);
        let caps_line = format!("\0{}\n", caps);

        if snap.refs.is_empty() {
            // No refs: emit the capabilities line with a zero id and
            // `capabilities^{}`.
            let zero = zero_hex(self.inner.format);
            let line = format!("{zero} capabilities^{{}}{caps_line}");
            pkt::encode_data(out, line.as_bytes());
        } else {
            let head_target = snap.head_target;
            let mut first = true;
            for r in &snap.refs {
                let mut line = format!("{} {}", r.oid, r.name);
                if first {
                    line.push_str(&caps_line);
                    first = false;
                } else {
                    line.push('\n');
                }
                pkt::encode_data(out, line.as_bytes());
                // Peeled annotated tags (`<oid> refs/tags/x^{}`), like git's
                // `packed-refs`-backed advertisement.
                if !r.peeled.is_empty() && r.peeled != r.oid {
                    let peeled = format!("{} {}^{{}}\n", r.peeled, r.name);
                    pkt::encode_data(out, peeled.as_bytes());
                }
            }
            // Include HEAD if it has a resolvable target and isn't already the
            // first advertised ref (upload-pack advertises HEAD).
            if !head_target.is_empty() && service == Service::UploadPack {
                if let Some(oid) = snap
                    .refs
                    .iter()
                    .find(|r| r.name == head_target)
                    .map(|r| r.oid.clone())
                {
                    let head_line = format!("{oid} HEAD\n");
                    pkt::encode_data(out, head_line.as_bytes());
                }
            }
        }
        pkt::encode_flush(out);
        Ok(())
    }

    // ---- upstream git helpers ----

    /// Run `git` with cwd=repo, `GIT_DIR` set, capturing output.
    pub async fn git(&self, args: &[&str]) -> Result<std::process::Output, GitError> {
        let mut cmd = Command::new("git");
        cmd.current_dir(&self.inner.path)
            .env("GIT_DIR", &self.inner.path)
            .args(args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        cmd.output().await.map_err(GitError::Io)
    }

    /// `git merge-base --is-ancestor old new`: true when `new` is a descendant of `old`.
    /// Missing objects or non-commit oids return `Ok(false)` (treat as not fast-forward).
    pub async fn is_ancestor(&self, old: &str, new: &str) -> Result<bool, GitError> {
        let out = self.git(&["merge-base", "--is-ancestor", old, new]).await?;
        Ok(out.status.success())
    }

    pub async fn repack(&self, opts: RepackOptions) -> Result<RepackResult, GitError> {
        let before: HashSet<gix_hash::ObjectId> =
            self.packs()?.into_iter().map(|p| p.checksum).collect();

        let mut args: Vec<String> = vec!["repack".into()];
        match opts.mode {
            RepackMode::Geometric { factor } => {
                args.push("-d".into());
                args.push(format!("--geometric={factor}"));
                if opts.write_midx {
                    args.push("--write-midx".into());
                }
                if opts.write_bitmap {
                    args.push("--write-bitmap-index".into());
                }
            }
            RepackMode::Full => {
                args.push("-a".into());
                args.push("-d".into());
                // Every core for the delta phase; the write + bitmap phases are
                // single-threaded in git (a large repository's base: 16 min for 32 GB on 44
                // cores, 2026-08-21 dry run — mostly that).
                args.push("--threads=0".into());
                if opts.write_bitmap {
                    args.push("--write-bitmap-index".into());
                }
                if opts.write_midx {
                    args.push("--write-midx".into());
                }
            }
        }
        // `--keep-pack=<file>` excludes a pack from the repack (git's `.keep` semantics for one
        // run). (`--keep=<hex>` was ambiguous between --keep-unreachable and --keep-pack and never
        // worked; no caller passed a non-empty list until 2026-08-22.)
        for k in &opts.keep {
            args.push(format!("--keep-pack=pack-{}.pack", k.to_hex()));
        }
        if matches!(opts.mode, RepackMode::Full) {
            // gix's bundle writer creates a temporary `.keep` marker before
            // installing a pack. Once an explicit full repack is requested,
            // those markers must not prevent git from coalescing/removing the
            // source packs.
            for pack in self.packs()? {
                let keep = self
                    .objects_pack_dir()
                    .join(format!("pack-{}.keep", pack.checksum));
                match std::fs::remove_file(keep) {
                    Ok(()) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => return Err(GitError::Io(e)),
                }
            }
        }
        let arg_refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let out = self.git(&arg_refs).await?;
        if !out.status.success() {
            return Err(GitError::Subprocess {
                cmd: "git repack".into(),
                status: out.status.code(),
                stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
            });
        }
        self.refresh_async().await?;

        let after = self.packs()?;
        let new_packs: Vec<PackInfo> = after
            .iter()
            .filter(|p| !before.contains(&p.checksum))
            .cloned()
            .collect();
        let removed: Vec<gix_hash::ObjectId> = before
            .into_iter()
            .filter(|c| !after.iter().any(|p| &p.checksum == c))
            .collect();
        Ok(RepackResult { new_packs, removed })
    }

    // ---- history pack ----

    /// Derive the **history pack** of `base`: every commit and tree reachable
    /// from the refs (`git pack-objects --filter=blob:none --revs --all`),
    /// written as a normal local pack + a `pack-<hash>.history` marker naming
    /// `base`. Instances install it as a real local pack next to a linked /
    /// remote base, so history walks, tree diffs and depth-1 enumerations
    /// never read the base (blob bytes are all that crosses the mount).
    /// Sized for tmpfs: a large repository ≈ 7 GB (2.4 GB commits + 4.6 GB trees).
    pub async fn write_history_pack(&self, base: &gix_hash::oid) -> Result<PackInfo, GitError> {
        let before: HashSet<gix_hash::ObjectId> =
            self.packs()?.into_iter().map(|p| p.checksum).collect();
        let prefix = self.objects_pack_dir().join("pack");
        // `--revs` takes revision arguments on stdin (no `--all` there): every
        // ref tip.
        let mut revs = String::new();
        for r in self.refs()?.refs {
            if !r.oid.is_empty() {
                revs.push_str(&r.oid);
                revs.push('\n');
            }
        }
        if revs.is_empty() {
            return Err(GitError::InvalidInput(
                "no refs to derive a history pack from".into(),
            ));
        }
        // `--filter` is only allowed with `--stdout` (git ≥ 2.43; the weekly dry
        // run of 2026-08-21 failed here: "cannot use --filter without --stdout"
        // and a large repository's base was published without its history pack): stream
        // the pack into `index-pack --stdin`, which writes pack + idx (+ rev)
        // under the pack dir and prints the checksum.
        let hash = {
            let mut po = tokio::process::Command::new("git")
                .current_dir(&self.inner.path)
                .env("GIT_DIR", &self.inner.path)
                .args([
                    "pack-objects",
                    "--filter=blob:none",
                    "--revs",
                    "--delta-base-offset",
                    "--stdout",
                    "-q",
                ])
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .map_err(GitError::Io)?;
            let po_stdout: Stdio = po
                .stdout
                .take()
                .expect("stdout")
                .try_into()
                .map_err(GitError::Io)?;
            let ip = tokio::process::Command::new("git")
                .current_dir(&self.inner.path)
                .env("GIT_DIR", &self.inner.path)
                .args([
                    "index-pack",
                    "--stdin",
                    "-v",
                    prefix
                        .with_extension("pack")
                        .to_str()
                        .unwrap_or("objects/pack/pack.pack"),
                ])
                .stdin(po_stdout)
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .map_err(GitError::Io)?;
            {
                use tokio::io::AsyncWriteExt;
                let mut stdin = po.stdin.take().expect("stdin");
                stdin
                    .write_all(revs.as_bytes())
                    .await
                    .map_err(GitError::Io)?;
                drop(stdin);
            }
            let (po_out, ip_out) = tokio::join!(po.wait_with_output(), ip.wait_with_output());
            let po_out = po_out.map_err(GitError::Io)?;
            let ip_out = ip_out.map_err(GitError::Io)?;
            if !po_out.status.success() {
                return Err(GitError::Subprocess {
                    cmd: "git pack-objects --filter=blob:none --stdout".into(),
                    status: po_out.status.code(),
                    stderr: String::from_utf8_lossy(&po_out.stderr).into_owned(),
                });
            }
            if !ip_out.status.success() {
                return Err(GitError::Subprocess {
                    cmd: "git index-pack --stdin (history pack)".into(),
                    status: ip_out.status.code(),
                    stderr: String::from_utf8_lossy(&ip_out.stderr).into_owned(),
                });
            }
            // index-pack prints `pack\t<checksum>` (or `keep\t…`) on stdout.
            let text = String::from_utf8_lossy(&ip_out.stdout);
            let checksum = text
                .lines()
                .rev()
                .find_map(|l| l.split_whitespace().nth(1))
                .unwrap_or("")
                .to_string();
            // index-pack wrote `<prefix>.pack/.idx` under the given name: rename to the canonical pack-<sha>.*.
            for ext in ["pack", "idx", "rev"] {
                let from = prefix.with_extension(ext);
                if from.exists() {
                    std::fs::rename(
                        &from,
                        self.objects_pack_dir()
                            .join(format!("pack-{checksum}.{ext}")),
                    )
                    .map_err(GitError::Io)?;
                }
            }
            checksum
        };
        let oid = gix_hash::ObjectId::from_hex(hash.as_bytes()).map_err(|e| {
            GitError::InvalidInput(format!("pack-objects printed no pack hash ({hash:?}): {e}"))
        })?;
        let _ = before; // an identical existing pack is fine: the marker makes it the history pack
        std::fs::write(
            self.pack_path(&oid).with_extension("history"),
            format!("{}\n", base.to_hex()),
        )
        .map_err(GitError::Io)?;
        self.refresh_async().await?;
        self.packs()?
            .into_iter()
            .find(|p| p.checksum == oid)
            .ok_or_else(|| {
                GitError::InvalidInput(format!("history pack {hash} not found after write"))
            })
    }

    /// Mark an installed pack as the history pack of `base` (readers do this
    /// after downloading one) and make it the **first place git and gix look**:
    /// a multi-pack-index covering exactly the history pack(s). git consults
    /// the midx before any pack; gix puts the midx first too (its plain-index
    /// order is biggest-index-first, which would put the 2 GB base idx ahead).
    /// So every commit/tree lookup hits the local history pack; only blobs
    /// fall through to the linked/remote base. Recent push packs are not in
    /// the midx (misses there are idx lookups, no data reads).
    pub async fn mark_history_pack(
        &self,
        checksum: &gix_hash::oid,
        base: &str,
    ) -> Result<(), GitError> {
        std::fs::write(
            self.pack_path(checksum).with_extension("history"),
            format!("{base}\n"),
        )
        .map_err(GitError::Io)?;
        let now = std::fs::FileTimes::new().set_modified(std::time::SystemTime::now());
        for ext in ["pack", "idx"] {
            if let Ok(f) = std::fs::File::options()
                .write(true)
                .open(self.pack_path(checksum).with_extension(ext))
            {
                let _ = f.set_times(now);
            }
        }
        self.write_history_midx().await
    }

    /// (Re)write `objects/pack/multi-pack-index` over the history pack(s) only
    /// (see [`mark_history_pack`]); removes it when there is none. Runs off the
    /// runtime: over a large repository's 43 M objects this is minutes of git.
    pub async fn write_history_midx(&self) -> Result<(), GitError> {
        let this = self.clone();
        tokio::task::spawn_blocking(move || this.write_history_midx_blocking())
            .await
            .map_err(|e| GitError::Protocol(format!("midx task: {e}")))?
    }

    fn write_history_midx_blocking(&self) -> Result<(), GitError> {
        // The midx covers the history pack(s) **and their bases** (when the
        // base idx is installed: linked or local), history first as the
        // preferred pack: an object in both resolves to the history pack, and
        // gix loads the midx *instead of* the covered plain indexes — its
        // plain-index order is biggest-first, which put the 2 GB base idx ahead
        // of the history pack and made pack generation copy 241 k tree
        // entries through the FUSE-linked base (prod: 23-minute clones with a
        // 2 s enumeration). Blobs still resolve to the base.
        let packs = self.packs()?;
        let history: Vec<&PackInfo> = packs.iter().filter(|p| p.history_of.is_some()).collect();
        let midx = self.objects_pack_dir().join("multi-pack-index");
        if history.is_empty() {
            let _ = std::fs::remove_file(&midx);
            return Ok(());
        }
        let mut names: Vec<String> = history
            .iter()
            .map(|p| format!("pack-{}.idx", p.checksum))
            .collect();
        for h in &history {
            if let Some(base) = &h.history_of {
                if let Some(b) = packs.iter().find(|p| &p.checksum.to_string() == base) {
                    let n = format!("pack-{}.idx", b.checksum);
                    if !names.contains(&n) {
                        names.push(n);
                    }
                }
            }
        }
        let preferred = names[0].clone();
        let input = names.iter().map(|n| format!("{n}\n")).collect::<String>();
        let out = std::process::Command::new("git")
            .current_dir(&self.inner.path)
            .env("GIT_DIR", &self.inner.path)
            .args([
                "multi-pack-index",
                "write",
                "--stdin-packs",
                &format!("--preferred-pack={preferred}"),
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .and_then(|mut c| {
                c.stdin.take().unwrap().write_all(input.as_bytes())?;
                c.wait_with_output()
            })
            .map_err(GitError::Io)?;
        if !out.status.success() {
            return Err(GitError::Subprocess {
                cmd: "git multi-pack-index write".into(),
                status: out.status.code(),
                stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
            });
        }
        self.refresh()
    }

    // ---- commit-graph ----

    fn commit_graphs_dir(&self) -> PathBuf {
        self.inner
            .path
            .join("objects")
            .join("info")
            .join("commit-graphs")
    }

    /// Build a single split commit-graph layer for every reachable commit
    /// (`git commit-graph write --reachable --split=replace [--changed-paths]`)
    /// and copy it next to `checksum`'s pack as `pack-<checksum>.commit-graph`,
    /// the side-file published with tier-2 bases so readers walk history
    /// without pack data. Returns the layer size.
    pub async fn write_pack_commit_graph(
        &self,
        checksum: &gix_hash::oid,
        changed_paths: bool,
    ) -> Result<u64, GitError> {
        let mut args = vec!["commit-graph", "write", "--reachable", "--split=replace"];
        if changed_paths {
            args.push("--changed-paths");
        }
        let out = self.git(&args).await?;
        if !out.status.success() {
            return Err(GitError::Subprocess {
                cmd: "git commit-graph write".into(),
                status: out.status.code(),
                stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
            });
        }
        let layers = self.commit_graph_chain()?;
        let Some(hash) = layers.last() else {
            return Err(GitError::InvalidInput(
                "commit-graph write produced no layer".into(),
            ));
        };
        let src = self.commit_graphs_dir().join(format!("graph-{hash}.graph"));
        let dst = self.pack_path(checksum).with_extension("commit-graph");
        let tmp = dst.with_extension("commit-graph.tmp");
        std::fs::copy(&src, &tmp).map_err(GitError::Io)?;
        rename_atomic(&tmp, &dst)?;
        Ok(std::fs::metadata(&dst).map(|m| m.len()).unwrap_or(0))
    }

    /// Hashes listed in `objects/info/commit-graphs/commit-graph-chain`
    /// (base first), empty when there is no chain.
    pub fn commit_graph_chain(&self) -> Result<Vec<String>, GitError> {
        match std::fs::read_to_string(self.commit_graphs_dir().join("commit-graph-chain")) {
            Ok(s) => Ok(s
                .lines()
                .map(|l| l.trim().to_string())
                .filter(|l| !l.is_empty())
                .collect()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
            Err(e) => Err(GitError::Io(e)),
        }
    }

    /// Install `pack-<checksum>.commit-graph` as the *base* layer of the
    /// repository's commit-graph chain: `commit-graphs/graph-<hash>.graph` +
    /// a chain file naming only that layer (a monolithic
    /// `objects/info/commit-graph` is removed — git would otherwise fold it
    /// into a full rewrite on the next `--split` write). Returns false when
    /// the side-file is absent. No-op when it already heads the chain.
    pub fn install_commit_graph_base(&self, checksum: &gix_hash::oid) -> Result<bool, GitError> {
        let side = self.pack_path(checksum).with_extension("commit-graph");
        if !side.exists() {
            return Ok(false);
        }
        let hash = commit_graph_layer_hash(&side)?;
        let chain = self.commit_graph_chain()?;
        if chain.first().map(|h| h == &hash).unwrap_or(false) {
            return Ok(true);
        }
        let dir = self.commit_graphs_dir();
        std::fs::create_dir_all(&dir).map_err(GitError::Io)?;
        let layer = dir.join(format!("graph-{hash}.graph"));
        if !layer.exists() {
            let tmp = dir.join(format!("graph-{hash}.graph.tmp"));
            if std::fs::hard_link(&side, &tmp).is_err() {
                std::fs::copy(&side, &tmp).map_err(GitError::Io)?;
            }
            rename_atomic(&tmp, &layer)?;
        }
        let chain_tmp = dir.join("commit-graph-chain.tmp");
        std::fs::write(&chain_tmp, format!("{hash}\n")).map_err(GitError::Io)?;
        rename_atomic(&chain_tmp, &dir.join("commit-graph-chain"))?;
        // Old layers are unreferenced now; drop them (never the new base).
        for old in chain.iter().filter(|h| **h != hash) {
            let _ = std::fs::remove_file(dir.join(format!("graph-{old}.graph")));
        }
        let mono = self
            .inner
            .path
            .join("objects")
            .join("info")
            .join("commit-graph");
        match std::fs::remove_file(&mono) {
            Ok(()) | Err(_) => {}
        }
        self.refresh()?;
        Ok(true)
    }

    /// Add the commits of `packs` (local pack checksums) to the commit-graph
    /// chain as a new tip layer (`git commit-graph write --split
    /// --stdin-packs`). Commits already in the chain are skipped by git;
    /// generation numbers come from the existing layers, so with a base layer
    /// installed this never reads base pack data (unless `changed_paths`,
    /// which diffs against parent trees). Cheap and incremental; git merges
    /// layers geometrically.
    pub async fn update_commit_graph(
        &self,
        packs: &[gix_hash::ObjectId],
        changed_paths: bool,
    ) -> Result<(), GitError> {
        if packs.is_empty() {
            return Ok(());
        }
        let mut input = String::new();
        for p in packs {
            input.push_str(&format!("pack-{}.idx\n", p.to_hex()));
        }
        let mut args = vec!["write", "--split", "--stdin-packs"];
        if changed_paths {
            args.push("--changed-paths");
        }
        let out = self
            .run_git_stdin("commit-graph", &args, input.as_bytes())
            .await?;
        if !out.status.success() {
            return Err(GitError::Subprocess {
                cmd: "git commit-graph write --split".into(),
                status: out.status.code(),
                stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
            });
        }
        self.refresh_async().await?;
        Ok(())
    }

    pub async fn write_bundle(
        &self,
        out: &Path,
        refs: &[String],
        exclude: &[gix_hash::ObjectId],
    ) -> Result<BundleInfo, GitError> {
        // Build rev args fed to `git bundle create <out> --stdin`.
        let mut input = String::new();
        for r in refs {
            input.push_str(r);
            input.push('\n');
        }
        for e in exclude {
            input.push('^');
            input.push_str(&e.to_hex().to_string());
            input.push('\n');
        }
        let out_str = out.to_string_lossy().to_string();
        let bundle_out = self
            .run_git_stdin(
                "bundle",
                &["create", out_str.as_str(), "--stdin"],
                input.as_bytes(),
            )
            .await?;
        if !bundle_out.status.success() {
            return Err(GitError::Subprocess {
                cmd: "git bundle create".into(),
                status: bundle_out.status.code(),
                stderr: String::from_utf8_lossy(&bundle_out.stderr).into_owned(),
            });
        }
        let size = std::fs::metadata(out).map(|m| m.len()).unwrap_or(0);
        let pack_offset = locate_pack_offset(out).unwrap_or(size);
        Ok(BundleInfo { size, pack_offset })
    }

    /// Full `git fsck` of the local copy, streaming every output line (stdout
    /// and stderr, interleaved by arrival) to `on_line`. `connectivity_only`
    /// skips object content checks (much faster on big repos). Returns the
    /// number of problem lines git printed; `Err` only when git itself failed
    /// to run.
    pub async fn fsck_streaming(
        &self,
        connectivity_only: bool,
        mut on_line: impl FnMut(String) + Send,
    ) -> Result<FsckReport, GitError> {
        use tokio::io::{AsyncBufReadExt, BufReader};
        let mut args = vec![
            "fsck",
            "--full",
            "--strict",
            "--no-progress",
            "--no-dangling",
        ];
        if connectivity_only {
            args.push("--connectivity-only");
        }
        let mut child = tokio::process::Command::new("git")
            .current_dir(&self.inner.path)
            .env("GIT_DIR", &self.inner.path)
            .args(&args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(GitError::Io)?;
        let (tx, mut rx) = tokio::sync::mpsc::channel::<String>(256);
        let mut readers = Vec::new();
        if let Some(out) = child.stdout.take() {
            let tx = tx.clone();
            readers.push(tokio::spawn(async move {
                let mut lines = BufReader::new(out).lines();
                while let Ok(Some(l)) = lines.next_line().await {
                    if tx.send(l).await.is_err() {
                        break;
                    }
                }
            }));
        }
        if let Some(err) = child.stderr.take() {
            readers.push(tokio::spawn(async move {
                let mut lines = BufReader::new(err).lines();
                while let Ok(Some(l)) = lines.next_line().await {
                    if tx.send(l).await.is_err() {
                        break;
                    }
                }
            }));
        }
        let mut problems = 0u64;
        while let Some(line) = rx.recv().await {
            let l = line.trim_end().to_string();
            if l.is_empty() {
                continue;
            }
            let lower = l.to_ascii_lowercase();
            if lower.starts_with("error")
                || lower.starts_with("missing")
                || lower.starts_with("broken")
                || lower.starts_with("unreachable")
                || lower.contains("fatal:")
            {
                problems += 1;
            }
            on_line(l);
        }
        for r in readers {
            let _ = r.await;
        }
        let status = child.wait().await.map_err(GitError::Io)?;
        Ok(FsckReport {
            ok: status.success() && problems == 0,
            exit_code: status.code(),
            problems,
        })
    }

    // ---- internal helpers ----

    /// In-process gix upload-pack for protocol v2 fetch. Builds the response
    /// sections (acknowledgments, shallow-info, wanted-refs, packfile) and
    /// generates the pack using gix_pack::data::output.
    async fn upload_pack_gix<W: AsyncWrite + Unpin + Send>(
        &self,
        req: UploadPackRequest,
        out: W,
    ) -> Result<UploadPackStats, GitError> {
        self.upload_pack_gix_with(req, out, None).await
    }

    fn git_cmd_sync(&self, args: &[&str]) -> Result<std::process::Output, GitError> {
        let mut cmd = std::process::Command::new("git");
        cmd.current_dir(&self.inner.path)
            .env("GIT_DIR", &self.inner.path)
            .args(args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        cmd.output().map_err(GitError::Io)
    }

    async fn run_git_stdin(
        &self,
        cmd_name: &str,
        args: &[&str],
        stdin_bytes: &[u8],
    ) -> Result<std::process::Output, GitError> {
        let path = self.inner.path.clone();
        let args: Vec<String> = args.iter().map(|s| s.to_string()).collect();
        let stdin_bytes: Vec<u8> = stdin_bytes.to_vec();
        let cmd_name = cmd_name.to_string();
        let res = tokio::task::spawn_blocking(move || {
            let mut full_args: Vec<String> = Vec::with_capacity(args.len() + 1);
            full_args.push(cmd_name.clone());
            full_args.extend(args.iter().cloned());
            let mut cmd = std::process::Command::new("git");
            cmd.current_dir(&path)
                .env("GIT_DIR", &path)
                .args(&full_args)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped());
            let mut child = cmd.spawn().map_err(GitError::Io)?;
            {
                let stdin = child.stdin.as_mut().unwrap();
                stdin.write_all(&stdin_bytes).map_err(GitError::Io)?;
            }
            child.wait_with_output().map_err(GitError::Io)
        })
        .await
        .map_err(|e| GitError::Io(std::io::Error::other(e)))??;
        Ok(res)
    }

    async fn run_upload_pack_stateless_io<R, W>(
        &self,
        protocol: pkt::Protocol,
        body: R,
        mut out: W,
    ) -> Result<(), GitError>
    where
        R: AsyncRead + Unpin + Send,
        W: AsyncWrite + Unpin + Send,
    {
        let mut body = body;
        let git_protocol = protocol.git_protocol_env();
        let mut cmd = Command::new("git");
        cmd.current_dir(&self.inner.path)
            .env("GIT_DIR", &self.inner.path)
            .env("GIT_PROTOCOL", git_protocol)
            // sideband-all: the server advertises it so it can narrate before
            // the packfile section; upload-pack only honours the client's
            // request with this config (also set at init, -c covers old copies).
            .args([
                "-c",
                "uploadpack.allowSidebandAll=true",
                "upload-pack",
                "--stateless-rpc",
                ".",
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = cmd.spawn().map_err(GitError::Io)?;
        let mut stdin = child.stdin.take().unwrap();
        let mut stdout = child.stdout.take().unwrap();
        // Copy the request body into stdin first, then close stdin so the
        // subprocess sees EOF and can finish + exit. Only then drain stdout:
        // `copy_out` blocks on stdout EOF (subprocess exit), and the subprocess
        // won't exit until stdin is closed, so they must NOT be joined
        // concurrently (that deadlocks).
        let copy_in = tokio::io::copy(&mut body, &mut stdin);
        let in_res = copy_in.await;
        drop(stdin);
        in_res.map_err(GitError::Io)?;
        let out_res = tokio::io::copy(&mut stdout, &mut out).await;
        out_res.map_err(GitError::Io)?;
        let status = child.wait().await.map_err(GitError::Io)?;
        if !status.success() {
            let stderr = child.stderr.take();
            if let Some(mut e) = stderr {
                let mut s = String::new();
                let _ = e.read_to_string(&mut s).await;
                return Err(GitError::Subprocess {
                    cmd: "git upload-pack".into(),
                    status: status.code(),
                    stderr: s,
                });
            }
            return Err(GitError::Subprocess {
                cmd: "git upload-pack".into(),
                status: status.code(),
                stderr: String::new(),
            });
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Free functions
// ---------------------------------------------------------------------------

/// Render a git bundle v2 header from a ref snapshot and prerequisites, so a
/// full bundle can be assembled as header + existing pack bytes without git.
pub fn bundle_header(
    refs: &RefSnapshotData,
    prerequisites: &[gix_hash::ObjectId],
    format: ObjectFormat,
) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(b"# v2 git bundle\n");
    for p in prerequisites {
        out.extend_from_slice(p.to_hex().to_string().as_bytes());
        out.extend_from_slice(b" \n");
    }
    for r in &refs.refs {
        out.extend_from_slice(r.oid.as_bytes());
        out.push(b' ');
        out.extend_from_slice(r.name.as_bytes());
        out.push(b'\n');
    }
    out.push(b'\n');
    let _ = format;
    out
}

// ---------------------------------------------------------------------------
// Internals
// ---------------------------------------------------------------------------

fn unique_suffix() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{}-{}", std::process::id(), nanos)
}

fn rename_atomic(src: &Path, dst: &Path) -> Result<(), GitError> {
    match std::fs::rename(src, dst) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::CrossesDevices => {
            std::fs::copy(src, dst).map_err(GitError::Io)?;
            std::fs::remove_file(src).map_err(GitError::Io)?;
            Ok(())
        }
        Err(e) => Err(GitError::Io(e)),
    }
}
/// Read the number of objects from a pack .idx file (v1 or v2) by reading the
/// last fanout entry.
fn idx_object_count(idx_path: &Path) -> Result<u64, GitError> {
    use std::io::{Read, Seek};
    let mut f = std::fs::File::open(idx_path).map_err(GitError::Io)?;
    let mut head = [0u8; 8];
    f.read_exact(&mut head).map_err(GitError::Io)?;
    let is_v2 = &head[..4] == b"\xfftOc";
    let fanout_off = if is_v2 { 8 + 255 * 4 } else { 255 * 4 };
    f.seek(std::io::SeekFrom::Start(fanout_off as u64))
        .map_err(GitError::Io)?;
    let mut buf = [0u8; 4];
    f.read_exact(&mut buf).map_err(GitError::Io)?;
    Ok(u32::from_be_bytes(buf) as u64)
}
struct IndexPackOutcome {
    checksum: gix_hash::ObjectId,
    pack_path: PathBuf,
    idx_path: PathBuf,
    object_count: u64,
    /// Time spent copying the pack into index-pack stdin.
    feed_ms: u64,
    /// `exit.t_abs` from GIT_TRACE2_EVENT (whole child). index-pack itself
    /// emits no region_leave events today; any that appear (future git) are
    /// in `phases`.
    git_ms: u64,
    /// Compact `k=ms` list: always `feed` + `git`, plus every TRACE2
    /// `region_leave` (`category:label`).
    phases: String,
}

fn git_index_pack(
    input: &Path,
    repo_path: &Path,
    fix_thin: bool,
    fsck: bool,
) -> Result<IndexPackOutcome, GitError> {
    let file = std::fs::File::open(input).map_err(GitError::Io)?;
    // `--threads=0` = auto (ncpus). `--rev-index` writes `.rev` in the same
    // pass so the next pack-objects does not rebuild a reverse index in RAM.
    // `--fsck-objects` parses commits/trees/tags while resolving — one walk,
    // not a second pass over the new pack through the whole ODB.
    let mut args = vec![
        "index-pack",
        "--stdin",
        "--keep",
        "--rev-index",
        "--threads=0",
    ];
    if fix_thin {
        args.push("--fix-thin");
    }
    if fsck {
        args.push("--fsck-objects");
    }
    let suffix = unique_suffix();
    let trace_path = std::env::temp_dir().join(format!("walgit-index-pack-{suffix}.jsonl"));
    // index-pack runs in a per-ingest scratch git dir whose objects/info/alternates points at the
    // repository: `--fix-thin` and `--fsck-objects` see the ODB, but everything index-pack writes —
    // the finished pack/idx/rev and, on failure, the `tmp_pack_*` it does not clean up (one
    // pack-sized leak per rejected push, on tmpfs) — lands under the scratch dir, which is removed
    // whole; the finished files are renamed into objects/pack (same filesystem, atomic).
    let scratch = repo_path.join(format!("walgit-ingest-{suffix}"));
    let _scratch_guard = ScratchDir(scratch.clone());
    let scratch_pack_dir = scratch.join("objects").join("pack");
    std::fs::create_dir_all(&scratch_pack_dir).map_err(GitError::Io)?;
    std::fs::create_dir_all(scratch.join("objects").join("info")).map_err(GitError::Io)?;
    std::fs::create_dir_all(scratch.join("refs")).map_err(GitError::Io)?;
    std::fs::write(scratch.join("HEAD"), "ref: refs/heads/main\n").map_err(GitError::Io)?;
    // The repository's config: object format (sha256 repos), repositoryformatversion, pack knobs.
    std::fs::copy(repo_path.join("config"), scratch.join("config")).map_err(GitError::Io)?;
    std::fs::write(
        scratch.join("objects").join("info").join("alternates"),
        format!("{}\n", repo_path.join("objects").display()),
    )
    .map_err(GitError::Io)?;
    let mut child = std::process::Command::new("git")
        .current_dir(repo_path)
        .env("GIT_DIR", &scratch)
        .env("GIT_TRACE2_EVENT", &trace_path)
        .args(&args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(GitError::Io)?;
    let feed_started = std::time::Instant::now();
    {
        let mut stdin = child.stdin.take().ok_or_else(|| {
            GitError::Io(std::io::Error::other("git index-pack stdin unavailable"))
        })?;
        let mut file = file;
        std::io::copy(&mut file, &mut stdin).map_err(GitError::Io)?;
    }
    let feed_ms = feed_started.elapsed().as_millis() as u64;
    let output = child.wait_with_output().map_err(GitError::Io)?;
    let trace = std::fs::read_to_string(&trace_path).unwrap_or_default();
    let _ = std::fs::remove_file(&trace_path);
    if !output.status.success() {
        return Err(GitError::Subprocess {
            cmd: "git index-pack".into(),
            status: output.status.code(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        });
    }
    let checksum = String::from_utf8_lossy(&output.stdout)
        .split_whitespace()
        .find_map(|word| gix_hash::ObjectId::from_hex(word.as_bytes()).ok())
        .ok_or_else(|| GitError::Protocol("git index-pack returned no checksum".into()))?;
    let hex = checksum.to_hex();
    let pack_dir = repo_path.join("objects").join("pack");
    std::fs::create_dir_all(&pack_dir).map_err(GitError::Io)?;
    // Move the finished files home: idx and rev first, the pack last, so a reader that sees the
    // pack also finds its index. `.keep` only exists to protect a pack until refs point at it;
    // publish is our commit point, and leaving it would hide the pack from `repack -d`.
    for ext in ["idx", "rev", "pack"] {
        let from = scratch_pack_dir.join(format!("pack-{hex}.{ext}"));
        if from.exists() {
            rename_atomic(&from, &pack_dir.join(format!("pack-{hex}.{ext}")))?;
        }
    }
    let pack_path = pack_dir.join(format!("pack-{hex}.pack"));
    let idx_path = pack_dir.join(format!("pack-{hex}.idx"));
    let object_count = idx_object_count(&idx_path)?;
    let parsed = parse_index_pack_trace2(&trace);
    let git_ms = parsed.git_ms;
    let phases = format_index_pack_phases(feed_ms, git_ms, &parsed.regions);
    Ok(IndexPackOutcome {
        checksum,
        pack_path,
        idx_path,
        object_count,
        feed_ms,
        git_ms,
        phases,
    })
}

/// Removes the per-ingest scratch git dir on every exit path (success, refusal, panic).
struct ScratchDir(PathBuf);
impl Drop for ScratchDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct Trace2Phases {
    git_ms: u64,
    regions: Vec<(String, u64)>,
}

fn secs_to_ms(t: f64) -> u64 {
    let ms = (t * 1000.0).ceil() as u64;
    if t > 0.0 && ms == 0 { 1 } else { ms }
}

/// Pull a JSON string field (`"key":"value"`) out of a TRACE2 event line.
fn json_str_field<'a>(line: &'a str, key: &str) -> Option<&'a str> {
    let pat = format!("\"{key}\":\"");
    let rest = line.split_once(&pat)?.1;
    let end = rest.find('"')?;
    Some(&rest[..end])
}

fn json_f64_field(line: &str, key: &str) -> Option<f64> {
    let pat = format!("\"{key}\":");
    let rest = line.split_once(&pat)?.1.trim_start();
    let token = rest.split([',', '}', ' ']).next()?.trim();
    token.parse().ok()
}

fn parse_index_pack_trace2(text: &str) -> Trace2Phases {
    let mut git_ms = 0u64;
    let mut regions = Vec::new();
    for line in text.lines() {
        let event = json_str_field(line, "event").unwrap_or("");
        match event {
            "exit" => {
                if let Some(t) = json_f64_field(line, "t_abs") {
                    git_ms = secs_to_ms(t);
                }
            }
            "region_leave" => {
                let label = json_str_field(line, "label")
                    .or_else(|| json_str_field(line, "name"))
                    .unwrap_or("");
                if label.is_empty() {
                    continue;
                }
                let name = match json_str_field(line, "category") {
                    Some(c) if !c.is_empty() => format!("{c}:{label}"),
                    _ => label.to_string(),
                };
                let t = json_f64_field(line, "t_rel").or_else(|| json_f64_field(line, "t_abs"));
                if let Some(t) = t {
                    regions.push((name, secs_to_ms(t)));
                }
            }
            _ => {}
        }
    }
    Trace2Phases { git_ms, regions }
}

fn format_index_pack_phases(feed_ms: u64, git_ms: u64, regions: &[(String, u64)]) -> String {
    let mut out = format!("feed={feed_ms},git={git_ms}");
    for (name, ms) in regions {
        out.push(',');
        out.push_str(name);
        out.push('=');
        out.push_str(&ms.to_string());
    }
    out
}

fn zero_hex(format: ObjectFormat) -> String {
    "0".repeat(match format {
        ObjectFormat::Sha1 => 40,
        ObjectFormat::Sha256 => 64,
    })
}

fn capabilities_for(service: Service, format: ObjectFormat) -> String {
    let of = format.as_str();
    let agent = format!("agent=walgit/{WALGIT_VERSION}");
    match service {
        Service::UploadPack => format!(
            "multi_ack_detailed side-band-64k thin-pack ofs-delta shallow \
             no-progress include-tag allow-tip-sha1-in-want allow-reachable-sha1-in-want filter \
             object-format={of} {agent}"
        ),
        Service::ReceivePack => format!(
            "report-status report-status-v2 delete-refs side-band-64k quiet atomic ofs-delta \
             push-options object-format={of} {agent}"
        ),
    }
}

fn find_conflict(stderr: &str) -> Option<String> {
    // git update-ref prints: "cannot lock ref '<name>' ... : ..." or similar.
    // Best-effort: extract a ref name appearing in a quoted context.
    for line in stderr.lines() {
        if let Some((_, rest)) = line.split_once("cannot lock ref '") {
            if let Some((name, _)) = rest.split_once('\'') {
                return Some(name.to_string());
            }
        }
        if let Some((_, rest)) = line.split_once("ref ") {
            // "ref refs/heads/main: expected ..."
            let name = rest.split([':', ' ', ',']).next().unwrap_or("").trim();
            if name.starts_with("refs/") {
                return Some(name.to_string());
            }
        }
    }
    None
}

pub(crate) fn read_refs(repo_path: &Path) -> Result<RefSnapshotData, GitError> {
    // HEAD symbolic target.
    let head_target = match std::fs::read_to_string(repo_path.join("HEAD")) {
        Ok(s) => {
            let s = s.trim();
            if let Some(t) = s.strip_prefix("ref: ") {
                t.trim().to_string()
            } else {
                // Detached HEAD: not symbolic.
                String::new()
            }
        }
        Err(_) => String::new(),
    };

    let mut map: BTreeMap<String, (String, String)> = BTreeMap::new();

    // packed-refs
    let packed_path = repo_path.join("packed-refs");
    if let Ok(content) = std::fs::read_to_string(&packed_path) {
        let mut last: Option<String> = None;
        for line in content.lines() {
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            if let Some(rest) = line.strip_prefix('^') {
                if let Some(name) = &last {
                    if let Some((_, peeled)) = map.get_mut(name) {
                        *peeled = rest.trim().to_string();
                    }
                }
                continue;
            }
            let mut parts = line.splitn(2, ' ');
            let oid = parts.next().unwrap_or("").trim().to_string();
            let name = parts.next().unwrap_or("").trim().to_string();
            if !name.is_empty() {
                map.insert(name.clone(), (oid, String::new()));
                last = Some(name);
            }
        }
    }

    // Loose refs (override packed).
    let refs_dir = repo_path.join("refs");
    walk_loose_refs(&refs_dir, "refs", &mut map);

    // Peel annotated tags that have no packed peel line. Branch refs cannot be
    // tags, so skipping their object lookup keeps mirror pushes linear in the
    // number of distinct tag object IDs rather than all branch refs.
    let refs: Vec<Ref> = map
        .into_iter()
        .map(|(name, (oid, peeled))| Ref { name, oid, peeled })
        .collect();

    let mut data = RefSnapshotData { refs, head_target };
    // Only tags can be annotated. Avoid an object lookup for every branch:
    // mirror pushes routinely contain tens of thousands of branch refs, often
    // all pointing at the same commit.
    if let Ok(tsr) = gix::ThreadSafeRepository::open(repo_path) {
        let repo = gix::Repository::from(&tsr);
        let mut peeled_by_oid: HashMap<String, Option<String>> = HashMap::new();
        for r in &mut data.refs {
            if !r.peeled.is_empty() || !r.name.starts_with("refs/tags/") {
                continue;
            }
            let Some(oid) = gix_hash::ObjectId::from_hex(r.oid.as_bytes()).ok() else {
                continue;
            };
            let peeled = peeled_by_oid
                .entry(r.oid.clone())
                .or_insert_with(|| peel_tag(&repo, oid).map(|p| p.to_hex().to_string()));
            if let Some(peeled) = peeled {
                r.peeled.clone_from(peeled);
            }
        }
    }
    Ok(data)
}

fn walk_loose_refs(dir: &Path, prefix: &str, map: &mut BTreeMap<String, (String, String)>) {
    let rd = match std::fs::read_dir(dir) {
        Ok(rd) => rd,
        Err(_) => return,
    };
    for ent in rd.flatten() {
        let path = ent.path();
        let name = format!("{prefix}/{}", ent.file_name().to_string_lossy());
        if path.is_dir() {
            walk_loose_refs(&path, &name, map);
        } else if path.is_file() {
            if let Ok(content) = std::fs::read_to_string(&path) {
                let s = content.trim();
                if let Some(t) = s.strip_prefix("ref: ") {
                    // Symbolic loose ref: resolve target oid later if present.
                    // We record the target name in the oid slot is wrong;
                    // instead skip (packed-refs usually has the real value, or
                    // the symref target is resolved at read time elsewhere).
                    // For HEAD-only symref we handle separately; loose symrefs
                    // under refs/ are rare. Record empty oid if unresolved.
                    let target = t.trim();
                    if let Some((o, _)) = map.get(target).cloned() {
                        map.insert(name, (o, String::new()));
                    }
                    continue;
                }
                if !s.is_empty() {
                    map.insert(name, (s.to_string(), String::new()));
                }
            }
        }
    }
}

impl LocalRepo {
    /// Record the peeled target of every `refs/tags/*` update in `txn`
    /// (`new_peeled`) so replicas can advertise annotated tags from the WAL
    /// alone. Call on the writer after the pack is installed.
    pub fn fill_peeled(&self, txn: &mut walgit_proto::v1::RefTransaction) {
        let repo = self.gix();
        for u in &mut txn.updates {
            if !u.name.starts_with("refs/tags/") || u.new_oid.is_empty() || !u.new_peeled.is_empty()
            {
                continue;
            }
            if u.new_oid.bytes().all(|b| b == b'0') {
                continue;
            }
            if let Ok(oid) = gix_hash::ObjectId::from_hex(u.new_oid.as_bytes()) {
                if let Some(p) = peel_tag(&repo, oid) {
                    if p != oid {
                        u.new_peeled = p.to_hex().to_string();
                    }
                }
            }
        }
    }
}

fn peel_tag(repo: &gix::Repository, oid: gix_hash::ObjectId) -> Option<gix_hash::ObjectId> {
    let kind = repo.object_hash();
    let mut cur = oid;
    for _ in 0..16 {
        let obj = match repo.find_object(cur) {
            Ok(o) => o,
            Err(_) => return None,
        };
        if obj.kind == gix_object::Kind::Tag {
            let tag = gix_object::TagRef::from_bytes(&obj.data, kind).ok()?;
            cur = tag.target();
        } else {
            return Some(cur);
        }
    }
    None
}

/// Build the v2 fetch command pkt-line request bytes from a typed request.
pub fn build_v2_fetch_request(req: &UploadPackRequest) -> Vec<u8> {
    let mut buf = Vec::new();
    pkt::encode_data(&mut buf, b"command=fetch\n");
    // Git protocol v2 carries all fetch features (thin-pack, want, have, ...)
    // as arguments following the delim-pkt; there is no pre-delim capability
    // section for fetch.
    pkt::encode_delim(&mut buf);
    if req.thin_pack {
        pkt::encode_data(&mut buf, b"thin-pack\n");
    }
    if req.ofs_delta {
        pkt::encode_data(&mut buf, b"ofs-delta\n");
    }
    if req.no_progress {
        pkt::encode_data(&mut buf, b"no-progress\n");
    }
    if req.include_tag {
        pkt::encode_data(&mut buf, b"include-tag\n");
    }
    if req.sideband_all {
        pkt::encode_data(&mut buf, b"sideband-all\n");
    }
    if req.wait_for_done {
        pkt::encode_data(&mut buf, b"wait-for-done\n");
    }
    if let Some(f) = &req.filter {
        pkt::encode_data(&mut buf, format!("filter {f}\n").as_bytes());
    }
    for w in &req.wants {
        pkt::encode_data(&mut buf, format!("want {}\n", w.to_hex()).as_bytes());
    }
    for h in &req.haves {
        pkt::encode_data(&mut buf, format!("have {}\n", h.to_hex()).as_bytes());
    }
    for s in &req.shallow {
        pkt::encode_data(&mut buf, format!("shallow {}\n", s.to_hex()).as_bytes());
    }
    if let Some(d) = req.deepen {
        pkt::encode_data(&mut buf, format!("deepen {d}\n").as_bytes());
    }
    if let Some(ts) = req.deepen_since {
        pkt::encode_data(&mut buf, format!("deepen-since {ts}\n").as_bytes());
    }
    for n in &req.deepen_not {
        pkt::encode_data(&mut buf, format!("deepen-not {n}\n").as_bytes());
    }
    for r in &req.want_refs {
        pkt::encode_data(&mut buf, format!("want-ref {r}\n").as_bytes());
    }
    if !req.packfile_uris_protocols.is_empty() {
        let joined = req.packfile_uris_protocols.join(" ");
        pkt::encode_data(&mut buf, format!("packfile-uris {joined}\n").as_bytes());
    }
    if req.done {
        pkt::encode_data(&mut buf, b"done\n");
    }
    pkt::encode_flush(&mut buf);
    buf
}

fn find_subsequence(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

fn locate_pack_offset(path: &Path) -> Option<u64> {
    use std::io::{Read, Seek, SeekFrom};
    let mut f = std::fs::File::open(path).ok()?;
    // The pack payload starts with the literal "PACK". Scan the file head.
    let mut buf = vec![0u8; 8 * 1024];
    let mut pos = 0u64;
    loop {
        let n = f.read(&mut buf).ok()?;
        if n == 0 {
            return None;
        }
        if let Some(i) = find_subsequence(&buf[..n], b"PACK") {
            return Some(pos + i as u64);
        }
        // Seek back a little to handle boundary splits.
        if n < buf.len() {
            return None;
        }
        pos += n as u64 - 3;
        f.seek(SeekFrom::Start(pos)).ok()?;
    }
}

// ---------------------------------------------------------------------------
// gix upload-pack helpers
// ---------------------------------------------------------------------------

/// Tree visitor for connectivity checking: records every referenced tree
/// and non-tree (blob/gitlink) oid in a seen-set and verifies existence.
/// Follow a tip through annotated tags, verifying each object exists, and return
/// the kind and id of the final non-tag object.
fn peel_tip(
    repo: &gix::Repository,
    mut id: gix_hash::ObjectId,
    seen: &mut HashSet<gix_hash::ObjectId>,
    buf: &mut Vec<u8>,
) -> Result<(gix_object::Kind, gix_hash::ObjectId), GitError> {
    use gix_object::Find;
    for _ in 0..64 {
        let data = repo
            .objects
            .try_find(&id, buf)
            .map_err(GitError::Gix)?
            .ok_or_else(|| GitError::MissingObject {
                oid: id.to_hex().to_string(),
            })?;
        if data.kind != gix_object::Kind::Tag {
            return Ok((data.kind, id));
        }
        seen.insert(id);
        let tag = gix_object::TagRefIter::from_bytes(data.data, repo.object_hash());
        let target = tag.target_id().map_err(|e| GitError::Gix(Box::new(e)))?;
        id = target;
    }
    Err(GitError::InvalidInput(format!(
        "tag chain too deep at {}",
        id.to_hex()
    )))
}

struct ConnectivityVisitor<'a> {
    seen: &'a mut HashSet<gix_hash::ObjectId>,
    repo: &'a gix::Repository,
    /// First object found missing (reported as `MissingObject`).
    missing: Option<gix_hash::ObjectId>,
}

impl<'a> TreeVisit for ConnectivityVisitor<'a> {
    fn pop_front_tracked_path_and_set_current(&mut self) {}
    fn pop_back_tracked_path_and_set_current(&mut self) {}
    fn push_back_tracked_path_component(&mut self, _c: &gix_object::bstr::BStr) {}
    fn push_path_component(&mut self, _c: &gix_object::bstr::BStr) {}
    fn pop_path_component(&mut self) {}

    fn visit_tree(
        &mut self,
        entry: &gix_object::tree::EntryRef<'_>,
    ) -> gix_traverse::tree::visit::Action {
        // Already verified (shared subtree): do not descend again. On a
        // monorepo this is the difference between "changed paths" and "4M files".
        if !self.seen.insert(entry.oid.to_owned()) {
            return std::ops::ControlFlow::Continue(false);
        }
        if !self.repo.has_object(entry.oid) {
            self.missing = Some(entry.oid.to_owned());
            return std::ops::ControlFlow::Break(());
        }
        std::ops::ControlFlow::Continue(true)
    }

    fn visit_nontree(
        &mut self,
        entry: &gix_object::tree::EntryRef<'_>,
    ) -> gix_traverse::tree::visit::Action {
        // Submodule entries (gitlinks) point at commits of *another* repository;
        // they are never expected to exist here (git/git: sha1collisiondetection).
        if entry.mode.is_commit() {
            return std::ops::ControlFlow::Continue(true);
        }
        if self.seen.insert(entry.oid.to_owned()) {
            if !self.repo.has_object(entry.oid) {
                self.missing = Some(entry.oid.to_owned());
                return std::ops::ControlFlow::Break(());
            }
        }
        std::ops::ControlFlow::Continue(true)
    }
}

/// Parse a filter spec string into a PackFilter.
#[derive(Debug, Clone)]
pub(crate) enum PackFilter {
    None,
    BlobNone,
    BlobLimit(u64),
    Tree(usize),
}

pub(crate) fn parse_filter(spec: &str) -> PackFilter {
    if spec == "blob:none" {
        return PackFilter::BlobNone;
    }
    if let Some(rest) = spec.strip_prefix("blob:limit=") {
        if let Ok(n) = rest.parse::<u64>() {
            return PackFilter::BlobLimit(n);
        }
    }
    if let Some(rest) = spec.strip_prefix("tree:") {
        if let Ok(n) = rest.parse::<usize>() {
            return PackFilter::Tree(n);
        }
    }
    PackFilter::None
}

/// Compute the object set for a pack: reachable(wants) - reachable(common
/// haves), honoring filters and include_tag.
///
/// Handles non-commit wants (blobs, trees, tags) for partial-clone lazy fetch
/// where the client sends `want <blob-oid>` directly.
pub(crate) fn compute_object_set(
    repo: &gix::Repository,
    wants: &[gix_hash::ObjectId],
    common_haves: &[gix_hash::ObjectId],
    filter: Option<&str>,
    include_tag: bool,
    deepen: Option<u32>,
) -> Result<HashSet<gix_hash::ObjectId>, GitError> {
    let pack_filter = filter.map(parse_filter).unwrap_or(PackFilter::None);
    let mut set: HashSet<gix_hash::ObjectId> = HashSet::new();
    let mut buf = Vec::new();
    let kind = repo.object_hash();

    // Separate wants into commits (for rev-walk) and non-commits (direct add).
    // Partial-clone lazy fetch sends `want <blob-oid>`; allow-any-sha1 lets
    // non-commit OIDs appear in wants.
    let mut commit_wants: Vec<gix_hash::ObjectId> = Vec::new();
    for w in wants {
        if let Ok(Some(hdr)) = repo.objects.try_header(w) {
            match hdr.kind {
                ObjKind::Commit => commit_wants.push(*w),
                ObjKind::Tag => {
                    set.insert(*w);
                    // Follow tag chain to final target.
                    let mut cur = *w;
                    loop {
                        let obj = repo.find_object(cur).map_err(|e| ge(e))?;
                        if obj.kind != ObjKind::Tag {
                            break;
                        }
                        let tag =
                            gix_object::TagRef::from_bytes(&obj.data, kind).map_err(|e| ge(e))?;
                        let target = tag.target();
                        set.insert(target);
                        cur = target;
                    }
                    // If the final target is a commit, rev-walk from it.
                    if let Ok(Some(h)) = repo.objects.try_header(&cur) {
                        if h.kind == ObjKind::Commit {
                            commit_wants.push(cur);
                        }
                    }
                }
                ObjKind::Tree => {
                    set.insert(*w);
                    if !matches!(pack_filter, PackFilter::Tree(0)) {
                        walk_tree_with_filter(repo, *w, &mut set, &pack_filter, 0, &mut buf)?;
                    }
                }
                ObjKind::Blob => {
                    set.insert(*w);
                }
            }
        } else {
            // Object not found — still add it (client asked for it).
            set.insert(*w);
        }
    }

    // Build the hidden set: common haves (always) + shallow boundaries (if
    // deepen is set). Hiding the shallow boundary commits stops the rev-walk
    // at the requested depth.
    let mut hidden: Vec<gix_hash::ObjectId> = common_haves.to_vec();
    if let Some(depth) = deepen {
        hidden.extend(compute_shallow(repo, wants, depth)?.exclude);
    }

    // Walk commits from commit_wants, stopping at hidden commits.
    if !commit_wants.is_empty() {
        let walk = repo
            .rev_walk(commit_wants.iter().copied())
            .with_hidden(hidden.iter().copied())
            .all()
            .map_err(|e| ge(e))?;

        for item in walk {
            let info = item.map_err(|e| ge(e))?;
            let cid = info.id;
            if !set.insert(cid) {
                continue;
            }
            if filter.is_none() && deepen.is_none() && common_haves.is_empty() {
                // The output counter's TreeContents expansion will resolve
                // each commit's tree and contents. Avoid decoding every
                // commit and walking its tree here as well.
                continue;
            }
            let mut commit = repo
                .objects
                .find_commit_iter(&cid, &mut buf)
                .map_err(|e| ge(e))?;
            let tree_id = commit.tree_id().map_err(|e| ge(e))?;

            // `tree:0` sends no trees at all, the root included.
            if matches!(pack_filter, PackFilter::Tree(0)) {
                continue;
            }
            if set.insert(tree_id) {
                walk_tree_with_filter(repo, tree_id, &mut set, &pack_filter, 0, &mut buf)?;
            }
        }
    }

    // If include_tag, find annotated tags whose target is in the set.
    if include_tag {
        let snap = read_refs(repo.path());
        if let Ok(snap) = snap {
            for r in &snap.refs {
                if let Ok(tag_oid) = gix_hash::ObjectId::from_hex(r.oid.as_bytes()) {
                    if set.contains(&tag_oid) {
                        continue;
                    }
                    if let Ok(obj) = repo.find_object(tag_oid) {
                        if obj.kind == ObjKind::Tag {
                            if let Ok(tag) = gix_object::TagRef::from_bytes(&obj.data, kind) {
                                let target = tag.target();
                                if set.contains(&target) {
                                    set.insert(tag_oid);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(set)
}

/// Recursively walk a tree, collecting oids with filter support.
/// The `tree_id` itself is assumed to already be in `set` (added by the
/// caller); only sub-trees and non-tree entries are inserted here.
pub(crate) fn walk_tree_with_filter(
    repo: &gix::Repository,
    tree_id: gix_hash::ObjectId,
    set: &mut HashSet<gix_hash::ObjectId>,
    filter: &PackFilter,
    depth: usize,
    buf: &mut Vec<u8>,
) -> Result<(), GitError> {
    // Collect entries first to end the mutable borrow of buf before recursing.
    let tree_iter = repo
        .objects
        .find_tree_iter(&tree_id, buf)
        .map_err(|e| ge(e))?;
    let entries: Vec<(gix_object::tree::EntryMode, gix_hash::ObjectId)> = tree_iter
        .map(|res| {
            let e = res.map_err(|e| ge(e))?;
            Ok((e.mode, e.oid.to_owned()))
        })
        .collect::<Result<_, GitError>>()?;
    for (mode, oid) in entries {
        if mode.is_tree() {
            if let PackFilter::Tree(max_depth) = filter {
                if depth + 1 > *max_depth {
                    set.insert(oid);
                    continue;
                }
            }
            if set.insert(oid) {
                walk_tree_with_filter(repo, oid, set, filter, depth + 1, buf)?;
            }
        } else {
            // Gitlinks point into another repository; never pack them.
            if mode.is_commit() {
                continue;
            }
            if matches!(filter, PackFilter::BlobNone) {
                continue;
            }
            if let PackFilter::BlobLimit(limit) = filter {
                if let Ok(Some(hdr)) = repo.objects.try_header(&oid) {
                    if hdr.size > *limit {
                        continue;
                    }
                }
            }
            set.insert(oid);
        }
    }
    Ok(())
}

/// Compute shallow boundary commits for a deepen-by-depth request.
/// Shallow plan for a `deepen <depth>` request: `shallow` = the commits at
/// depth `depth` from the wants that still have parents (git's `shallow`
/// lines: they are sent, their parents are not), `exclude` = those parents
/// (the walk's hidden boundary).
pub(crate) struct ShallowPlan {
    pub shallow: Vec<gix_hash::ObjectId>,
    pub exclude: Vec<gix_hash::ObjectId>,
}

pub(crate) fn compute_shallow(
    repo: &gix::Repository,
    wants: &[gix_hash::ObjectId],
    depth: u32,
) -> Result<ShallowPlan, GitError> {
    let mut seen: HashSet<gix_hash::ObjectId> = HashSet::new();
    let mut buf = Vec::new();
    let mut level: Vec<gix_hash::ObjectId> = wants.to_vec();
    let mut shallow = Vec::new();
    let mut exclude = Vec::new();
    for d in 1..=depth.max(1) {
        if level.is_empty() {
            break; // history exhausted (e.g. `--unshallow` sends a huge depth)
        }
        let mut next: Vec<gix_hash::ObjectId> = Vec::new();
        for cid in &level {
            if !seen.insert(*cid) {
                continue;
            }
            let commit = repo
                .objects
                .find_commit_iter(cid, &mut buf)
                .map_err(|e| ge(e))?;
            let parents: Vec<gix_hash::ObjectId> =
                commit.parent_ids().map(|p| p.to_owned()).collect();
            if d == depth.max(1) {
                if !parents.is_empty() {
                    shallow.push(*cid);
                }
                exclude.extend(parents);
            } else {
                next.extend(parents);
            }
        }
        level = next;
    }
    exclude.sort_unstable();
    exclude.dedup();
    // A commit reachable within the depth is never excluded.
    exclude.retain(|e| !seen.contains(e));
    Ok(ShallowPlan { shallow, exclude })
}

/// Compute the SHA checksum trailer for a pack header (used for empty packs).
pub(crate) fn compute_pack_trailer(data: &[u8], kind: gix_hash::Kind) -> gix_hash::ObjectId {
    use gix_hash::hasher;
    let mut h = hasher(kind);
    h.update(data);
    // try_finalize always succeeds when the hasher has been fed data.
    h.try_finalize().expect("hash finalization must succeed")
}

/// The hash git names a split commit-graph layer by: the file's trailing
/// checksum (hash size from the header's hash-version byte).
fn commit_graph_layer_hash(path: &Path) -> Result<String, GitError> {
    let data = std::fs::read(path).map_err(GitError::Io)?;
    // Header: "CGPH" version(1) hash-version(1) chunks(1) base-graphs(1)
    if data.len() < 8 || &data[..4] != b"CGPH" {
        return Err(GitError::InvalidInput(format!(
            "{} is not a commit-graph",
            path.display()
        )));
    }
    let len = if data[5] == 2 { 32 } else { 20 };
    if data.len() < 8 + len {
        return Err(GitError::InvalidInput(format!(
            "{} is truncated",
            path.display()
        )));
    }
    Ok(hex::encode(&data[data.len() - len..]))
}

/// Derive a pack's reverse index (`.rev`, RIDX v1) from its `.idx`: header
/// (`RIDX`, version 1, hash id), N × u32 BE index positions sorted by pack
/// offset, the pack checksum (from the idx trailer), and the checksum of the
/// file. Byte-identical to `git index-pack --rev-index` / `pack.writeReverseIndex`
/// (git's `write_rev_index_positions` sorts by offset with the index position
/// as the tiebreaker, which cannot occur: offsets are unique). Written to a
/// temp name and renamed, so a reader never sees a partial file.
pub fn write_rev_from_idx(
    idx_path: &Path,
    rev_path: &Path,
    kind: gix_hash::Kind,
) -> Result<(), GitError> {
    let index =
        gix_pack::index::File::at(idx_path, kind).map_err(|e| GitError::Gix(Box::new(e)))?;
    let n = index.num_objects();
    let mut by_offset: Vec<(u64, u32)> = index
        .iter()
        .enumerate()
        .map(|(i, e)| (e.pack_offset, i as u32))
        .collect();
    by_offset.sort_unstable();
    let mut out = Vec::with_capacity(12 + 4 * n as usize + 2 * kind.len_in_bytes());
    out.extend_from_slice(b"RIDX");
    out.extend_from_slice(&1u32.to_be_bytes());
    out.extend_from_slice(
        &(if kind == gix_hash::Kind::Sha1 {
            1u32
        } else {
            2u32
        })
        .to_be_bytes(),
    );
    for (_, pos) in &by_offset {
        out.extend_from_slice(&pos.to_be_bytes());
    }
    out.extend_from_slice(index.pack_checksum().as_bytes());
    let mut h = gix_hash::hasher(kind);
    h.update(&out);
    let trailer = h
        .try_finalize()
        .map_err(|e| GitError::InvalidInput(format!("rev checksum: {e}")))?;
    out.extend_from_slice(trailer.as_bytes());
    let tmp = rev_path.with_extension("rev.tmp");
    std::fs::write(&tmp, &out).map_err(GitError::Io)?;
    std::fs::rename(&tmp, rev_path).map_err(GitError::Io)?;
    Ok(())
}

#[cfg(test)]
mod index_pack_trace_tests {
    use super::*;

    #[test]
    fn parse_trace2_exit_and_region_leave() {
        let sample = r#"
{"event":"start","t_abs":0.0001}
{"event":"region_leave","category":"index-pack","label":"resolve_deltas","t_rel":1.5}
{"event":"region_leave","category":"index-pack","label":"fsck","t_rel":0.25}
{"event":"exit","t_abs":2.251,"code":0}
"#;
        let p = parse_index_pack_trace2(sample);
        assert_eq!(p.git_ms, 2251);
        assert_eq!(
            p.regions,
            vec![
                ("index-pack:resolve_deltas".into(), 1500),
                ("index-pack:fsck".into(), 250),
            ]
        );
        let phases = format_index_pack_phases(3, p.git_ms, &p.regions);
        assert_eq!(
            phases,
            "feed=3,git=2251,index-pack:resolve_deltas=1500,index-pack:fsck=250"
        );
    }

    #[test]
    fn upload_pack_does_not_advertise_unimplemented_deepen_modes() {
        let caps = capabilities_for(Service::UploadPack, ObjectFormat::Sha1);
        assert!(!caps.split_whitespace().any(|c| c == "deepen-since"));
        assert!(!caps.split_whitespace().any(|c| c == "deepen-not"));
        assert!(!caps.split_whitespace().any(|c| c == "packfile-uris"));
        assert!(caps.split_whitespace().any(|c| c == "shallow"));
        assert!(caps.split_whitespace().any(|c| c == "filter"));
    }

    #[test]
    fn successful_index_pack_records_non_zero_git_ms() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("src");
        std::fs::create_dir_all(&src).unwrap();
        let run = |args: &[&str]| {
            let o = std::process::Command::new("git")
                .current_dir(&src)
                .args(args)
                .output()
                .unwrap();
            assert!(
                o.status.success(),
                "{:?} {}",
                args,
                String::from_utf8_lossy(&o.stderr)
            );
            o
        };
        run(&["init", "-q"]);
        run(&["config", "user.email", "t@t"]);
        run(&["config", "user.name", "t"]);
        std::fs::write(src.join("f"), "hello\n").unwrap();
        run(&["add", "f"]);
        run(&["commit", "-qm", "c"]);
        let pack = {
            let mut child = std::process::Command::new("git")
                .current_dir(&src)
                .args(["pack-objects", "--stdout", "--revs"])
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap();
            {
                let mut stdin = child.stdin.take().unwrap();
                use std::io::Write;
                stdin.write_all(b"HEAD\n").unwrap();
            }
            let out = child.wait_with_output().unwrap();
            assert!(
                out.status.success(),
                "{}",
                String::from_utf8_lossy(&out.stderr)
            );
            out.stdout
        };
        let dest = dir.path().join("dest.git");
        let o = std::process::Command::new("git")
            .args(["init", "-q", "--bare", dest.to_str().unwrap()])
            .output()
            .unwrap();
        assert!(o.status.success());
        let input = dir.path().join("in.pack");
        std::fs::write(&input, &pack).unwrap();
        let outcome = git_index_pack(&input, &dest, false, true).expect("index-pack");
        assert!(outcome.object_count > 0);
        assert!(
            outcome.git_ms > 0 || outcome.feed_ms > 0,
            "expected a phase: {}",
            outcome.phases
        );
        assert!(
            outcome.phases.contains("git="),
            "phases must name git_ms: {}",
            outcome.phases
        );
        assert!(outcome.pack_path.exists());
        assert!(outcome.idx_path.exists());
    }
}
