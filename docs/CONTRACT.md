# walgit cross-crate contract

Context: **the original cross-crate interface contract (2026-08-18/19, written so eight owners could build the
crates in parallel)**, kept as the reference for names and shapes. Rule still in force: *extend, do not rename*
— a type or function listed here is relied on by another crate. **Where this file and the code disagree, the
code is right and this file is stale**; verify with `rg`/`cargo doc` before relying on a signature. Known
supersessions (2026-08-20 sweep): `RepoHandle::sync()` is now the *Serve* level of the sync-level family
(`sync_refs` / `sync` = Serve / `sync_full` / `sync_objects`, `AGENTS.md §2.3`); auth is Google identities only
(`AGENTS.md §1.3`, no "admin token"); the server router is `web/API.md` + `AGENTS.md D15/D20/D26/D27`; bundle
schedule/retention semantics are specified only by `docs/BUNDLE_URI_DESIGN.md §3–§4` (calendar slots,
slot-epoch tokens, contiguous-chain retention, main-only refs). Read when you touch a crate boundary; update
the relevant block when you extend one.

Shared interfaces between crates. Implement exactly these names/shapes; extend freely, do not rename.
Original owners (parallel batch): StoreS3, StoreGcs, StoreCoord, GitEngine, Wal, Server, Bundle, Cli.
Read `AGENTS.md` first (design §1–§2, decisions §3; the original layout/phases/config draft is the measurement log).

## Existing (do not rewrite; extend only)
- `walgit-proto`: prost types from `proto/walgit/v1/wal.proto` (Manifest, LogSegmentRef, LogEntry, PackRef,
  RefTransaction/RefUpdate, Checkpoint(+Ref), RefSnapshot/Ref, Lease, BundleList/BundleEntry); `keys::*`;
  `frame::{encode_entries,decode_entries}` (uvarint-framed log encoding); `time::*`; `keys::POLICY` / `policy_key` (`policy.json` rule language, `docs/POLICY.md`).
- `walgit-store`: `ObjectStore` trait (`Version` opaque CAS token, `GetOptions{if_none_match,if_match,range}`,
  `GetResult::{NotModified,Object}`, `PutMode::{Overwrite,Create,Update(Version)}`, `PutBody::{Bytes,Stream,File}`,
  `PutOptions`, `StoreError::{NotFound,PreconditionFailed{current},Retryable,InvalidArgument,Other}`,
  `ObjectStoreExt`, `Prefixed`, `memory::MemoryStore`, `util::{collect,once,file_stream,backoff,retry}`),
  placeholder modules `coord.rs`, `gcs.rs`, `s3.rs`.
- `walgit-config`: `Config` for walgit.toml (+ `WALGIT__` env overrides, `PORT`); `Config::with_settings` accepts
  only `[bundles]`, `[maintenance]`, `[compaction]`, `[upstream]`, and `[integrations]` in repo-scoped settings.

## walgit-git (owner: GitEngine)

```rust
pub struct RepoId { owner: String, name: String }
// FromStr("owner/name" | "owner/name.git"), Display "owner/name". Validation: each part ASCII [A-Za-z0-9._-],
// no leading '.', not "..", 1..=100 chars. fn owner(), name(), store_prefix() (walgit_proto::keys::repo_prefix),
// local_dir(root:&Path)->PathBuf (= root/owner/name.git).
pub enum ObjectFormat { Sha1, Sha256 } // From<walgit_config::ObjectFormat>, <-> gix_hash::Kind, as_str()

/// Bare git repo on local disk in standard layout (objects/pack/*.{pack,idx}, loose refs + packed-refs, HEAD,
/// config with repositoryformatversion / extensions.objectformat) readable by gix AND upstream git.
/// Clone-able handle (Arc inside), thread-safe.
pub struct LocalRepo;
impl LocalRepo {
  pub fn init(root: &Path, id: &RepoId, format: ObjectFormat) -> Result<Self, GitError>;
  pub fn open(root: &Path, id: &RepoId) -> Result<Option<Self>, GitError>;
  pub fn id(&self) -> &RepoId; pub fn path(&self) -> &Path; pub fn object_format(&self) -> ObjectFormat;
  pub fn gix(&self) -> gix::Repository;          // per-thread handle from shared ThreadSafeRepository
  pub fn refresh(&self) -> Result<(), GitError>;  // re-read odb/refs after pack/ref changes

  // ---- packs
  /// = git index-pack: stream in, write objects/pack/pack-<checksum>.{pack,idx}; thin packs resolved against
  /// the odb (--fix-thin); verify checksum; opts.fsck => object-level validation. Empty input => Ok(None).
  pub async fn ingest_pack<R: tokio::io::AsyncRead + Unpin + Send + 'static>(&self, pack: R, opts: IngestOptions)
      -> Result<Option<IngestedPack>, GitError>;
  pub struct IngestOptions { pub fsck: bool, pub max_bytes: Option<u64>, pub thin: bool }
  pub struct IngestedPack { pub checksum: gix_hash::ObjectId, pub pack_path: PathBuf, pub idx_path: PathBuf,
      pub pack_size: u64, pub idx_size: u64, pub object_count: u64 }
  /// Atomically move downloaded files into objects/pack/ (rename), then refresh.
  pub async fn install_pack(&self, pack: &Path, idx: &Path, extra: &[PathBuf]) -> Result<(), GitError>;
  /// Delete .pack/.idx/.rev/.bitmap. Caller guarantees no readers (wal holds a lock).
  pub fn remove_pack(&self, checksum: &gix_hash::oid) -> Result<(), GitError>;
  pub fn packs(&self) -> Result<Vec<PackInfo>, GitError>;
  pub struct PackInfo { pub checksum: gix_hash::ObjectId, pub pack_size: u64, pub idx_size: u64,
      pub object_count: u64, pub has_rev: bool, pub has_bitmap: bool }
  pub fn pack_path(&self, checksum: &gix_hash::oid) -> PathBuf; // objects/pack/pack-<hex>.pack (idx: set_extension)

  // ---- refs
  /// All refs sorted by name incl. peeled tags + HEAD symbolic target. `From` both ways with
  /// walgit_proto::v1::RefSnapshot.
  pub fn refs(&self) -> Result<RefSnapshotData, GitError>;
  /// Atomic all-or-nothing. check_old => verify old_oid (zero = must not exist). Supports HEAD symbolic update.
  /// Error GitError::RefConflict{name, expected, actual}.
  pub fn apply_ref_txn(&self, txn: &walgit_proto::v1::RefTransaction, check_old: bool) -> Result<(), GitError>;
  /// Replace ALL refs + HEAD (write packed-refs directly; must be fast for 500k refs).
  pub fn load_ref_snapshot(&self, snap: &walgit_proto::v1::RefSnapshot) -> Result<(), GitError>;
  pub fn pack_refs(&self) -> Result<(), GitError>;

  // ---- objects
  pub fn has_object(&self, oid: &gix_hash::oid) -> bool;
  /// Every object reachable from tips exists. `stop_at_existing_refs` => stop at objects reachable from
  /// current refs (rev-list --objects <tips> --not --all). Error GitError::MissingObject{oid}.
  pub fn check_connectivity(&self, tips: &[gix_hash::ObjectId], stop_at_existing_refs: bool) -> Result<(), GitError>;

  // ---- protocol, server side
  /// protocol v2 `fetch`: parsed args in, pkt-line response out (acknowledgments, shallow-info, wanted-refs,
  /// packfile-uris (empty), packfile with sideband) per git protocol-v2 doc. Pack via gix_pack::data::output
  /// (count + entries with delta reuse from on-disk packs). Engine selectable (UploadPackEngine::{Gix,Git}).
  pub async fn upload_pack<W: tokio::io::AsyncWrite + Unpin + Send>(&self, req: UploadPackRequest, out: W)
      -> Result<UploadPackStats, GitError>;
  /// Raw passthrough: spawns `git upload-pack --stateless-rpc` (GIT_PROTOCOL set) — used for v0 and for
  /// engine=Git.
  pub async fn upload_pack_raw<R, W>(&self, protocol: Protocol, body: R, out: W) -> Result<(), GitError>;
  /// v2 ls-refs from the ref snapshot; efficient prefix filtering.
  pub fn ls_refs(&self, args: &LsRefsArgs) -> Result<Vec<LsRefsLine>, GitError>;
  pub struct LsRefsArgs { pub ref_prefixes: Vec<String>, pub symrefs: bool, pub peel: bool, pub unborn: bool }
  /// v0 advertisement with capabilities.
  pub fn advertise_refs_v0(&self, service: Service, out: &mut Vec<u8>) -> Result<(), GitError>;
  pub enum Service { UploadPack, ReceivePack }  // FromStr("git-upload-pack"|"git-receive-pack")

  // ---- upstream git helpers
  pub async fn git(&self, args: &[&str]) -> Result<std::process::Output, GitError>; // cwd=repo, GIT_DIR set
  pub async fn repack(&self, opts: RepackOptions) -> Result<RepackResult, GitError>;
  pub struct RepackOptions { pub mode: RepackMode /* Geometric{factor} | Full */, pub write_bitmap: bool,
      pub write_midx: bool, pub keep: Vec<gix_hash::ObjectId> }
  pub struct RepackResult { pub new_packs: Vec<PackInfo>, pub removed: Vec<gix_hash::ObjectId> }
  /// `git bundle create`.
  pub async fn write_bundle(&self, out: &Path, refs: &[String], exclude: &[gix_hash::ObjectId])
      -> Result<BundleInfo, GitError>;
  pub struct BundleInfo { pub size: u64, pub pack_offset: u64 }
}
/// Bundle header ("# v2 git bundle\n" [+ "-<oid> prereq\n"]* + "<oid> <ref>\n"* + "\n") so a full bundle
/// can be rendered as header + existing pack bytes without git.
pub fn bundle_header(refs: &RefSnapshotData, prerequisites: &[gix_hash::ObjectId], format: ObjectFormat) -> Vec<u8>;

pub mod pkt;      // pkt-line read/write, flush/delim/response-end, sideband encode; Protocol::{V0,V2} from
                  // GIT_PROTOCOL header; command/arg parsing for v2 (ls-refs, fetch, object-info, bundle-uri)
pub mod receive;  // parse receive-pack request: caps + commands ("old new refname\0caps"), push-options,
                  // => (walgit_proto::v1::RefTransaction, ReceiveCaps{report_status_v2, side_band_64k,
                  // atomic, quiet, push_options, agent, object_format}); pack bytes follow in the same body.
                  // `report_status(caps, unpack: Result, per_ref: &[(name, Result<(),String>)], out)` writer
                  // producing report-status(-v2), sideband-framed when requested.
pub enum GitError { Io, Gix(Box<dyn Error+Send+Sync>), Pack, RefConflict{name,expected,actual}, MissingObject{oid},
                    Fsck(String), Subprocess{cmd,status,stderr}, InvalidInput(String), Protocol(String) }
```

## walgit-store::coord (owner: StoreCoord)

```rust
/// Generic read-modify-write CAS loop on a protobuf object. `f(None)` when absent. Returning `None` from `f`
/// aborts with Ok(None). Retries on PreconditionFailed (re-reading) up to `max_retries`, on Retryable with
/// backoff. Returns the written meta + value.
pub async fn cas_update<T: prost::Message + Default, F>(store: &dyn ObjectStore, key: &str, max_retries: u32, f: F)
    -> Result<Option<(ObjectMeta, T)>, CoordError>
  where F: FnMut(Option<&T>) -> Result<Option<T>, CoordError>;
/// Read a protobuf object with its version. Ok(None) if absent.
pub async fn get_message<T: prost::Message + Default>(store: &dyn ObjectStore, key: &str)
    -> Result<Option<(ObjectMeta, T)>, CoordError>;
pub async fn get_message_if_changed<T>(store, key, known: &Version) -> Result<Option<(ObjectMeta, T)>, CoordError>;

/// Lease = walgit_proto::v1::Lease at `key`, acquired by Create or by Update over an expired lease.
pub struct LeaseGuard; // holds store handle, key, holder id, current Version; Drop => best-effort release
impl LeaseGuard {
  pub async fn heartbeat(&mut self, ttl: Duration) -> Result<(), CoordError>;      // CAS-extend expires_at
  pub async fn release(self) -> Result<(), CoordError>;                            // CAS delete
  pub fn spawn_heartbeat(self: Arc<Mutex<Self>>, every: Duration, ttl: Duration) -> tokio::task::JoinHandle<()>;
  pub fn holder(&self) -> &str; pub fn expires_at(&self) -> SystemTime;
}
pub async fn try_acquire(store: DynStore, key: &str, holder: &str, purpose: &str, ttl: Duration)
    -> Result<Option<LeaseGuard>, CoordError>;   // None = held by someone else and not expired
pub async fn acquire(store, key, holder, purpose, ttl, wait_up_to: Duration) -> Result<Option<LeaseGuard>, CoordError>;
pub fn instance_id() -> &'static str; // explicit instance name/id, hostname+pid, or uuid; computed once
pub enum CoordError { Store(StoreError), Decode(prost::DecodeError), Aborted, RetriesExhausted{key, attempts}, Other }
```

## walgit-store backends (owners: StoreS3, StoreGcs)

```rust
// s3.rs
pub struct S3Store; impl S3Store { pub async fn new(cfg: &walgit_config::StoreConfig) -> anyhow::Result<Self>; }
// gcs.rs
pub struct GcsStore; impl GcsStore { pub async fn new(cfg: &walgit_config::StoreConfig) -> anyhow::Result<Self>; }
// lib.rs
pub async fn open_store(cfg: &walgit_config::Config) -> anyhow::Result<DynStore>; // by cfg.store.backend, applies Prefixed(cfg.store_prefix())
```
Contract tests: `crates/walgit-store/tests/contract.rs` with a `run_contract(store: DynStore)` suite executed for
memory always, for s3 when `WALGIT_TEST_S3_ENDPOINT` set (bucket `WALGIT_TEST_BUCKET`, default "walgit-test"),
for gcs when `WALGIT_TEST_GCS_BUCKET` set.

## walgit-wal (owner: Wal)

```rust
pub struct Registry;   // one per process: DynStore + Arc<Config> + cache_root; DashMap<RepoId, Arc<RepoHandle>>
impl Registry {
  pub fn new(store: DynStore, cfg: Arc<walgit_config::Config>) -> Arc<Self>;
  /// Open existing (materialize local copy lazily). Err(WalError::NotFound) if manifest.pb absent.
  pub async fn open(&self, id: &RepoId) -> Result<Arc<RepoHandle>, WalError>;
  /// CAS-create manifest.pb (PutMode::Create). Err(WalError::AlreadyExists).
  pub async fn create(&self, id: &RepoId, format: ObjectFormat) -> Result<Arc<RepoHandle>, WalError>;
  pub async fn open_or_create(&self, id: &RepoId, format: ObjectFormat) -> Result<Arc<RepoHandle>, WalError>;
  pub async fn list(&self) -> Result<Vec<RepoId>, WalError>;   // list "repos/" prefix (delimiter-less scan is ok v1)
  pub fn store(&self) -> &DynStore; pub fn config(&self) -> &Arc<Config>;
  /// Disk cache maintenance: evict idle repos beyond cache.max_bytes / evict_idle_after.
  pub async fn evict_idle(&self) -> Result<EvictReport, WalError>;
}
pub struct RepoHandle;
impl RepoHandle {
  pub fn id(&self) -> &RepoId;
  pub fn local(&self) -> &LocalRepo;
  pub fn store(&self) -> &Prefixed;                       // repo-scoped
  pub fn manifest(&self) -> Arc<walgit_proto::v1::Manifest>;   // last known
  pub fn manifest_version(&self) -> Option<Version>;
  /// Freshness check (conditional GET on manifest.pb; honors wal.freshness_ttl) + catch-up (download new
  /// packs, apply log entries after our seq, apply COMPACT: install new pack, remove superseded). Returns a
  /// read guard; while any guard is alive no pack is removed locally. Every request calls this first.
  pub async fn sync(&self) -> Result<ReadGuard<'_>, WalError>;
  /// Force full re-materialize from store (repair).
  pub async fn rematerialize(&self) -> Result<(), WalError>;
  /// Publish a push. `pack` was produced by LocalRepo::ingest_pack on this handle's local repo (already on
  /// disk). Steps: upload pack+idx to wal/<sha>.{pack,idx} (skip if exists) ‖ verify txn old values against
  /// synced refs; then CAS: append LogEntry to log (new segment object per batch on regional buckets),
  /// cas_update manifest (head_seq+1, packs+=, log_segments+=); on PreconditionFailed: re-sync, re-verify
  /// old values (RefConflict per ref → whole push rejected unless !atomic and per-ref reporting), retry.
  /// Then apply refs locally. Coalesces concurrent publishes on this handle (wal.batch_window/max_batch).
  pub async fn publish_push(&self, pack: Option<IngestedPack>, txn: RefTransaction, meta: HashMap<String,String>)
      -> Result<PublishResult, WalError>;
  pub struct PublishResult { pub seq: u64, pub per_ref: Vec<(String, Result<(), RefError>)> }
  pub async fn publish_ref_update(&self, txn: RefTransaction, meta) -> Result<PublishResult, WalError>;
  /// COMPACT entry: new pack (already local, e.g. from LocalRepo::repack) superseding `supersedes`.
  pub async fn publish_compact(&self, new_pack: PackInfo, supersedes: Vec<gix_hash::ObjectId>, tier: u32)
      -> Result<u64, WalError>;
  /// Write checkpoint at current head (refs snapshot + pack set), then CAS manifest (checkpoint=, min_seq=,
  /// log_segments trimmed). Idempotent.
  pub async fn write_checkpoint(&self) -> Result<CheckpointRef, WalError>;
  /// Read log entries [from_seq, to_seq] from the store (provenance/rewind tooling).
  pub async fn read_log(&self, from_seq: u64, to_seq: Option<u64>) -> Result<Vec<LogEntry>, WalError>;
  pub fn last_access(&self) -> Instant;  pub fn touch(&self);
}
// snapshot.rs — time travel, shared by `walgit wal materialize --at-seq` and the server's `snapshot` op.
// Non-mutating: nothing is published and the serving copy stays at the WAL head. Runs on the bulk runtime.
pub mod snapshot {
  pub type Narrator = Arc<dyn Fn(String) + Send + Sync>;   // narration sink: CLI prints, server notices
  pub const REFS_MAX: usize = 1000;                        // refs carried inline in a Snapshot
  pub struct Snapshot { pub repo: String, pub at_seq: u64, pub head_seq: u64, pub from_seq: u64,
                       pub entries: u64, pub git_dir: String, pub ref_count: usize,
                       pub refs: Vec<SnapshotRef>, pub head_target: String,
                       pub packs: Vec<SnapshotPack>, pub hostname: String, pub built_at: String }
  /// Rebuild `id` as it was at `at_seq` into `out` (which must not exist).
  pub async fn materialize_at(registry: Arc<Registry>, id: RepoId, at_seq: u64, out: PathBuf,
                              narrate: Narrator) -> Result<Snapshot, WalError>;
  /// Same replay into `<cache.dir>/snapshots/<owner>/<name>/<at_seq>/`, idempotently; the bool is
  /// "built now". Err(Invalid) for seq 0 or past the head, Err(TooLarge) beyond the cache budget.
  pub async fn snapshot_at(registry: Arc<Registry>, id: RepoId, at_seq: u64, narrate: Narrator)
      -> Result<(Snapshot, bool), WalError>;
  pub fn snapshot_dir(cache_dir: &Path, id: &RepoId, at_seq: u64) -> PathBuf;
  /// The completed snapshot in `dir` (its marker is written last), else None.
  pub fn read_snapshot(dir: &Path) -> Option<Snapshot>;
}
pub enum WalError { NotFound, AlreadyExists, RefConflict{name, expected, actual}, Store(StoreError),
                    Coord(CoordError), Git(GitError), Corrupt(String), Invalid(String),
                    Retry{attempts}, Io(std::io::Error), TooLarge{bytes, max} }
pub enum RefError { NonFastForward, Conflict{expected,actual}, Rejected(String), Missing }
```

## walgit-server (owner: Server)

```rust
pub struct AppState { pub cfg: Arc<Config>, pub store: DynStore, pub registry: Arc<walgit_wal::Registry>,
                      pub bundles: Arc<walgit_bundle::Bundler>, pub auth: Arc<auth::Authenticator> }
pub fn router(state: Arc<AppState>) -> axum::Router;
/// Bind, serve (HTTP/1.1 + h2c), graceful shutdown on SIGTERM/SIGINT/`shutdown` future.
pub async fn serve(state: Arc<AppState>, shutdown: impl Future<Output=()> + Send) -> anyhow::Result<()>;
// Routes (all under /{owner}/{repo}[.git]):
//   GET  /info/refs?service=git-upload-pack|git-receive-pack   (v0 advert or v2 capability advert per Git-Protocol)
//   POST /git-upload-pack   POST /git-receive-pack   (Content-Encoding: gzip supported; streaming both ways)
//   GET  /HEAD  GET /objects/info/packs (404 unless dumb enabled)
//   POST /info/lfs/objects/batch  PUT/GET /info/lfs/objects/{oid}  POST /info/lfs/verify
//   GET  /bundles/list  GET /bundles/{strategy}/{name}    (bundle-uri targets; ETag/Range/immutable caching)
//   POST /api/ops/snapshot?at_seq=N  GET /api/snapshot/{seq}   (WAL time travel; non-mutating, per instance)
//   PUT  /  (create repo, write permission)   DELETE / (write permission)
// Non-repo: GET /healthz /readyz /metrics ; GET / (list repos, text/plain)
// Auth: verified Google identity; writes require write permission. Sync level depends on the endpoint
// (Refs, Serve, Full, or Objects; AGENTS.md §2.3).
```

## walgit-bundle (owner: Bundle)

```rust
pub struct Bundler; impl Bundler {
  pub fn new(registry: Arc<Registry>, cfg: Arc<Config>) -> Arc<Self>;
  /// Evaluate all strategies for `id` at `now`; build those due (leased per repo+strategy), upload
  /// bundles/<strategy>/<ts>-<sha>.bundle, cas_update bundles/list.pb, prune keep=N. Returns built entries.
  pub async fn run_due(&self, id: &RepoId, now: SystemTime) -> Result<Vec<BundleEntry>, BundleError>;
  pub async fn build(&self, id: &RepoId, strategy: &str) -> Result<BundleEntry, BundleError>;
  pub async fn list(&self, id: &RepoId) -> Result<Option<BundleList>, BundleError>;
  /// git bundle-list text (bundle.version=1, bundle.mode, bundle.heuristic=creationToken, bundle.<id>.uri/
  /// creationToken); uri = `{base_url}/{owner}/{repo}/bundles/{strategy}/{name}` or signed URL per config.
  pub async fn render_list(&self, id: &RepoId, base_url: &str) -> Result<Option<String>, BundleError>;
  /// v2 `bundle-uri` command response lines (key=value pkt-lines).
  pub async fn protocol_v2_lines(&self, id, base_url) -> Result<Vec<String>, BundleError>;
  pub async fn run_all_due(&self, now) -> Result<(), BundleError>; // every repo in registry.list()
}

/// Abstraction over the registry; `walgit_wal::Registry` implements it and tests may use any impl.
#[async_trait]
pub trait BundleSource: Send + Sync + 'static {
  async fn open_repo(&self, id: &RepoId) -> Result<BundleRepoHandle, BundleError>;
  async fn list_repos(&self) -> Result<Vec<RepoId>, BundleError>;
}
pub struct BundleRepoHandle {
  pub local: walgit_git::LocalRepo,
  pub store: walgit_store::Prefixed,
  pub head_seq: u64,
}
/// `Bundler::new_with_source(source: Arc<dyn BundleSource>, cfg)` accepts custom sources;
/// `Bundler::new(registry: Arc<Registry>, cfg)` delegates to it.
pub use walgit_git::RepoId; // re-exported
pub enum BundleError { Store, Decode, Git, StrategyNotFound, RepoNotFound, InvalidRepoId,
  InvalidSchedule, Io, BundleNotFound, NoRefs, NoNewObjects, RetriesExhausted, Other }
```

### Proto addition (owner: Bundle)
`BundleEntry` gains `repeated Ref tips = 11;` — the ref tips (name+oid+peeled) a
bundle contains. For incremental bundles, the base bundle's tips are the
prerequisites. Backward compatible (field 11 was unused).

### Schedule / retention semantics
Normative rules live in `docs/BUNDLE_URI_DESIGN.md §3–§4`: six-field UTC calendar slots, WAL state as of each
slot, slot-epoch creation tokens, oldest-first backfill, contiguous-chain retention, and main-only selection
where configured. Do not derive scheduling behavior from this interface catalog.

## walgit-cli (owner: Cli)
`walgit --config walgit.toml <cmd>`: `serve` | `compact [owner/name|--all] [--once]` | `bundle run [--repo] [--strategy]` |
`repo create|list|info` | `wal ls|show|materialize --at-seq` | `synth --out DIR --size s|m|l [--commits N --files M]`
| `import --from GITDIR owner/name` | `config check|dump`. Also `Containerfile`, `compose.yaml` (rustfs +
walgit), `justfile`, `walgit.example.toml`, `tests/e2e.sh` (real git vs. server on memory store and on rustfs).
