use std::fs;
use std::path::Path;
use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::extract::{Path as AxumPath, State};
use axum::http::{HeaderMap, HeaderValue, Method, Request, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use prost::Message;
use rust_embed::RustEmbed;
use serde::Serialize;
use walgit_proto::v1::{Checkpoint, EntryKind};
use walgit_store::{GetOptions, GetResult, ObjectStore};

use crate::AppState;
use crate::error::ApiError;

#[derive(RustEmbed)]
#[folder = "../../web/dist"]
// Debug builds read the folder at runtime; `allow_missing` lets the crate
// compile where the web build is absent (distributed sccache workers).
// Release images assert the optimized artefacts exist (Containerfile).
#[allow_missing = true]
struct Assets;

/// Look up a UI asset. Release builds embed `web/dist`; debug builds read it
/// from disk. When the crate was compiled on a machine without `web/dist`
/// (distributed sccache), rust-embed's baked-in folder path does not
/// canonicalize, so fall back to reading relative to this crate's manifest.
fn embedded(path: &str) -> Option<rust_embed::EmbeddedFile> {
    if let Some(f) = Assets::get(path) {
        return Some(f);
    }
    if cfg!(debug_assertions) {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../web/dist")
            .canonicalize()
            .ok()?;
        let file = root.join(path).canonicalize().ok()?;
        if !file.starts_with(&root) {
            return None;
        }
        return rust_embed::utils::read_file_from_fs(&file).ok();
    }
    None
}

const INDEX: &str = "index.html";
const IMMUTABLE: &str = "public, max-age=31536000, immutable";

/// Routes for the embedded SPA and the optional WAL overview page.
pub fn router(state: Arc<AppState>) -> Router {
    let r = Router::new()
        .route("/", get(root))
        .route("/_ui/{*path}", get(asset))
        .route("/services/setup.json", get(setup_json))
        // "API" docs page (SPA route); `/api/v1` is the JSON discovery document (D20).
        .route("/api", get(index_route))
        .route("/{owner}", get(index_route))
        .route(
            "/{owner}/{repo}",
            get(index_route)
                .put(crate::dispatch)
                .delete(crate::dispatch),
        )
        .route("/{owner}/{repo}/tree/{*rest}", get(index_route))
        .route("/{owner}/{repo}/blob/{*rest}", get(index_route))
        .route("/{owner}/{repo}/commits", get(index_route))
        .route("/{owner}/{repo}/commits/{*rest}", get(index_route))
        .route("/{owner}/{repo}/commit/{*rest}", get(index_route))
        .route("/{owner}/{repo}/wal", get(index_route))
        .route("/{owner}/{repo}/settings", get(index_route));
    let mut r = r;
    for base in crate::web::api::REPO_API_BASES {
        r = r
            .route(&format!("{base}/overview"), get(overview))
            .route(&format!("{base}/ops"), get(ops_list))
            .route(
                &format!("{base}/ops/{{op}}"),
                axum::routing::post(ops_start),
            )
            .route(&format!("{base}/tasks"), get(tasks_list))
            .route(&format!("{base}/tasks/{{id}}"), get(task_stream))
            .route(&format!("{base}/snapshot/{{seq}}"), get(snapshot));
    }
    r.with_state(state)
}

/// The installer a not-yet-signed-in user needs — the only route on the open
/// `/services/public/*` prefix;
/// nothing under this router is gated, so nothing with data may ever be added to it.
pub fn public_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/services/public/install.sh", get(install_sh))
        // The certificate this process presents (self_signed/files, D39): public
        // material, what the installer pins for git. 404 behind an edge (h2c).
        .route("/services/public/ca.pem", get(ca_pem))
        // Nothing else lives on the public lane: explicit 404 so no gated route can ever be
        // reached through it by accident.
        .route(
            "/services/public/{*rest}",
            get(|| async { StatusCode::NOT_FOUND }),
        )
        .with_state(state)
}

async fn ca_pem(State(state): State<Arc<AppState>>) -> Response {
    match &state.tls {
        Some(t) => (
            [
                (header::CONTENT_TYPE, "application/x-pem-file"),
                (header::CACHE_CONTROL, "no-cache"),
                (header::CONTENT_DISPOSITION, "inline; filename=\"ca.pem\""),
            ],
            t.cert_pem.clone(),
        )
            .into_response(),
        None => (
            StatusCode::NOT_FOUND,
            "this host does not terminate TLS itself",
        )
            .into_response(),
    }
}

async fn root(State(state): State<Arc<AppState>>, req: Request<Body>) -> Response {
    let query_text = req
        .uri()
        .query()
        .is_some_and(|query| query.split('&').any(|part| part == "format=text"));
    let accept = req
        .headers()
        .get(header::ACCEPT)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    if query_text || (accept.contains("text/plain") && !accept.contains("text/html")) {
        return match crate::admin::list_repos(&state, req.headers()).await {
            Ok(response) => response,
            Err(error) => error.into_response(),
        };
    }
    index(req.method(), req.headers())
}

/// SPA entry point for every page route. `no-cache` (not `no-store`): the
/// browser keeps it and revalidates with `If-None-Match`; a deploy that only
/// changes the import map costs one 304-or-tiny-200 round trip.
async fn index_route(req: Request<Body>) -> Response {
    index(req.method(), req.headers())
}

fn index(method: &Method, headers: &HeaderMap) -> Response {
    match embedded(INDEX) {
        Some(file) => embedded_response(INDEX, file, method, headers, "no-cache"),
        None => (StatusCode::INTERNAL_SERVER_ERROR, "web assets are missing").into_response(),
    }
}

#[derive(serde::Deserialize, Default)]
pub struct InstallQuery {
    /// `owner/name` — the script ends by exec'ing `git clone` of that repository.
    repo: Option<String>,
    /// Alias of `repo`.
    tree: Option<String>,
}

/// `/services/public/install.sh[?repo=owner/name]`: the ONE idempotent client setup command — token, credential helper, bundle URIs, self-test, and with
/// `repo` the clone. Open at the app (no credential exists yet when it is fetched).
pub async fn install_sh(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    axum::extract::Query(q): axum::extract::Query<InstallQuery>,
) -> Response {
    let base_url = crate::smart::request_base_url(&state, &headers);
    let repo = q
        .repo
        .or(q.tree)
        .as_deref()
        .map(|r| r.trim_matches('/').trim_end_matches(".git").to_string())
        .filter(|r| {
            r.split_once('/')
                .is_some_and(|(o, n)| walgit_git::RepoId::new(o, n).is_ok())
        });
    (
        [
            (header::CONTENT_TYPE, "text/x-shellscript; charset=utf-8"),
            (header::CACHE_CONTROL, "public, max-age=300"),
            (
                header::CONTENT_DISPOSITION,
                "inline; filename=\"install.sh\"",
            ),
        ],
        crate::setup::install_script(&state.cfg, &base_url, repo.as_deref()),
    )
        .into_response()
}

/// `/services/setup.json[?repo=owner/name]`: the clone/setup recipes the Clone menu and
/// the API page render (`setup::Recipes`) — one source of truth for the one-liners, so the
/// UI never re-derives the token command or the OAuth client.
async fn setup_json(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    axum::extract::Query(q): axum::extract::Query<InstallQuery>,
) -> Response {
    let base_url = crate::smart::request_base_url(&state, &headers);
    let repo = q
        .repo
        .or(q.tree)
        .as_deref()
        .map(|r| r.trim_matches('/').trim_end_matches(".git").to_string())
        .filter(|r| {
            r.split_once('/')
                .is_some_and(|(o, n)| walgit_git::RepoId::new(o, n).is_ok())
        });
    (
        [(header::CACHE_CONTROL, "no-cache")],
        axum::Json(crate::setup::recipes(
            &state.cfg,
            &base_url,
            repo.as_deref(),
        )),
    )
        .into_response()
}

/// `GET|HEAD /repos.js` | `/repos.mjs` — the browser SDK (`web/sdk/`, built
/// into `web/dist/` by `pnpm run build`). Permanent URL, so `no-cache` +
/// strong ETag (revalidated per deploy), precompressed like every asset.
pub async fn sdk_asset(req: Request<Body>) -> Response {
    let name = req.uri().path().trim_start_matches('/');
    match embedded(name) {
        Some(file) => embedded_response(name, file, req.method(), req.headers(), "no-cache"),
        None => (
            StatusCode::NOT_FOUND,
            "sdk not built (web/dist/repos.js missing)",
        )
            .into_response(),
    }
}

/// `GET|HEAD /_ui/{path}` — embedded build output.
///
/// * `assets/*` carry a content hash in their name → `immutable` for a year.
///   Anything else (none today besides `index.html`) is `no-cache`.
/// * Strong `ETag` (build-time sha256 of the bytes) on everything,
///   `If-None-Match` → `304`.
/// * Brotli/gzip: the build emits `.br`/`.gz` siblings (max quality, once);
///   the best encoding the client accepts is served byte-for-byte with
///   `Content-Encoding` + `Vary: Accept-Encoding`. Nothing is compressed at
///   request time.
/// * `Content-Length` always; `HEAD` answered without a body.
async fn asset(AxumPath(path): AxumPath<String>, req: Request<Body>) -> Response {
    let path = path.trim_start_matches('/');
    // Never hand out the precompressed siblings directly: their identity is the
    // uncompressed asset (content negotiation picks the encoding).
    if path.ends_with(".br") || path.ends_with(".gz") {
        return (StatusCode::NOT_FOUND, "asset not found").into_response();
    }
    let Some(content) = embedded(path) else {
        return (StatusCode::NOT_FOUND, "asset not found").into_response();
    };
    let cache = if path.starts_with("assets/") {
        IMMUTABLE
    } else {
        "no-cache"
    };
    embedded_response(path, content, req.method(), req.headers(), cache)
}

fn embedded_response(
    path: &str,
    file: rust_embed::EmbeddedFile,
    method: &Method,
    headers: &HeaderMap,
    cache: &'static str,
) -> Response {
    let etag = format!("\"{}\"", hex::encode(&file.metadata.sha256_hash()[..16]));
    let etag_hit = headers
        .get_all(header::IF_NONE_MATCH)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .map(|t| t.trim().trim_start_matches("W/"))
        .any(|t| t == "*" || t == etag);
    let mut resp = Response::new(Body::empty());
    {
        let h = resp.headers_mut();
        h.insert(header::ETAG, HeaderValue::from_str(&etag).unwrap());
        h.insert(header::CACHE_CONTROL, HeaderValue::from_static(cache));
        h.insert(header::VARY, HeaderValue::from_static("Accept-Encoding"));
        h.insert(
            header::X_CONTENT_TYPE_OPTIONS,
            HeaderValue::from_static("nosniff"),
        );
    }
    if etag_hit {
        *resp.status_mut() = StatusCode::NOT_MODIFIED;
        return resp;
    }
    let (encoding, data) = negotiate_encoding(path, headers).unwrap_or((None, file.data));
    let h = resp.headers_mut();
    h.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static(content_type(path)),
    );
    h.insert(header::CONTENT_LENGTH, HeaderValue::from(data.len()));
    if let Some(enc) = encoding {
        h.insert(header::CONTENT_ENCODING, HeaderValue::from_static(enc));
    }
    if method != Method::HEAD {
        *resp.body_mut() = Body::from(data);
    }
    resp
}

/// Pick the best precompressed variant the client accepts (`br` > `gzip`).
/// Returns `None` when the client accepts neither or no sibling was built.
fn negotiate_encoding(
    path: &str,
    headers: &HeaderMap,
) -> Option<(Option<&'static str>, std::borrow::Cow<'static, [u8]>)> {
    let accept = headers
        .get_all(header::ACCEPT_ENCODING)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .map(|t| t.trim())
        .filter(|t| !t.is_empty())
        .collect::<Vec<_>>();
    let accepts = |name: &str| {
        accept.iter().any(|t| {
            let (coding, q) = t.split_once(';').map_or((*t, None), |(c, q)| (c, Some(q)));
            coding.trim().eq_ignore_ascii_case(name)
                && !q.is_some_and(|q| q.trim().trim_start_matches("q=").trim() == "0")
        })
    };
    for (name, ext) in [("br", ".br"), ("gzip", ".gz")] {
        if accepts(name) {
            if let Some(f) = embedded(&format!("{path}{ext}")) {
                return Some((Some(name), f.data));
            }
        }
    }
    None
}

fn content_type(path: &str) -> &'static str {
    match Path::new(path).extension().and_then(|e| e.to_str()) {
        Some("css") => "text/css; charset=utf-8",
        Some("js") | Some("mjs") => "text/javascript; charset=utf-8",
        Some("json") | Some("map") => "application/json; charset=utf-8",
        Some("html") => "text/html; charset=utf-8",
        Some("svg") => "image/svg+xml",
        Some("png") => "image/png",
        Some("jpg") | Some("jpeg") => "image/jpeg",
        Some("gif") => "image/gif",
        Some("webp") => "image/webp",
        Some("ico") => "image/x-icon",
        Some("woff") => "font/woff",
        Some("woff2") => "font/woff2",
        Some("txt") => "text/plain; charset=utf-8",
        _ => "application/octet-stream",
    }
}

#[derive(Serialize)]
struct Overview {
    repo: String,
    /// Which instance rendered this page (kind, name, shape, build).
    instance: crate::instance::InstanceInfo,
    clone_url: String,
    /// One-time git setup for this host (credential helper + bundle-uri), multi-line.
    setup: String,
    /// `curl -fsSL …/services/public/install.sh | sh` one-liner (the open lane; no token needed).
    install: String,
    /// Absolute URL of the installer (browser download is already signed in).
    install_url: String,
    hostname: String,
    health: Health,
    manifest: ManifestInfo,
    local: LocalInfo,
    packs: PacksInfo,
    bundles: Vec<BundleInfo>,
    /// Calendar slot table (built / missing / unavailable / wrong-host) and
    /// who maintains this repository (heartbeats). See `walgit bundle plan`.
    bundle_plan: BundlePlanInfo,
    compactions: Vec<CompactionInfo>,
    node: serde_json::Map<String, serde_json::Value>,
    ops: OpsInfo,
    /// Ready-to-paste git invocations for this repo.
    clone: CloneInfo,
}

#[derive(Serialize, Default)]
struct BundlePlanInfo {
    slots: Vec<SlotInfo>,
    /// The next slot of each strategy and the unit it will run (Sunday's base rebuild, visibly).
    upcoming: Vec<crate::maintain::Upcoming>,
    maintainers: Vec<MaintainerInfo>,
    /// True when no live heartbeat covers this repository.
    orphaned: bool,
}

#[derive(Serialize)]
struct SlotInfo {
    strategy: String,
    kind: String,
    /// Slot epoch seconds (0 = chain-level row).
    slot: u64,
    /// `built` | `missing` | `blocked` | `unavailable` | `wrong-host`
    status: String,
    detail: String,
    bundle_id: Option<String>,
}

#[derive(Serialize)]
struct MaintainerInfo {
    host: String,
    disk: String,
    max_pack_bytes: u64,
    last_pass_age_secs: Option<u64>,
    alive: bool,
    passes: u64,
    last_unit: String,
}

#[derive(Serialize)]
struct CloneInfo {
    /// `git -c http.extraHeader="Authorization: Bearer $WALGIT_TOKEN" … clone <url>`: per-command, no installer.
    manual: String,
    /// Plain clone (after the installer ran).
    plain: String,
}

#[derive(Serialize)]
struct Health {
    status: &'static str,
    issues: Vec<String>,
    /// The last connectivity audit as the maintainer recorded it in `fsck.pb` (any host), else
    /// "never audited".
    deep: String,
    /// Maintenance this repository is missing; each maps to an op. `auto` says when the
    /// maintainer loop does it by itself — then the button is a "do it now", not a chore.
    suggestions: Vec<Suggestion>,
}

#[derive(Serialize)]
struct Suggestion {
    op: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    params: Option<String>,
    reason: String,
    /// How/when the maintainer loop performs this without anyone asking (None: a human must).
    #[serde(skip_serializing_if = "Option::is_none")]
    auto: Option<String>,
}

#[derive(Serialize)]
struct OpsInfo {
    available: Vec<crate::ops::OpSpec>,
    /// Recent + running tasks on this instance (ops and automatic ones:
    /// materialize, remote-index).
    recent: Vec<walgit_wal::TaskRecord>,
    /// Configured bundle strategies (for the bundle op's `strategy` param).
    bundle_strategies: Vec<String>,
}

#[derive(Serialize)]
struct ManifestInfo {
    version: String,
    next_seq: u64,
    min_seq: u64,
    segments: Vec<SegmentInfo>,
    tail_entries: usize,
    entries: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    checkpoint: Option<BundleInfo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    packset: Option<PacksetInfo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    advertised_bundle_uri: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_push: Option<String>,
}

#[derive(Serialize)]
struct SegmentInfo {
    key: String,
    first_seq: u64,
    last_seq: u64,
    size: u64,
}

#[derive(Serialize)]
struct PacksetInfo {
    at_seq: u64,
    packs: usize,
    bytes: u64,
    created: String,
    creator: String,
}

#[derive(Serialize)]
struct BundleInfo {
    sha: String,
    size: u64,
    at_seq: u64,
    created: String,
    creator: String,
    uri: String,
    /// Chain facts (empty for the checkpoint bundle): strategy, full|incremental, the bundle whose
    /// tips are this one's prerequisites (empty for a full), creationToken, object filter, ref tips.
    #[serde(default)]
    strategy: String,
    #[serde(default)]
    kind: String,
    #[serde(default)]
    base_id: String,
    #[serde(default)]
    creation_token: u64,
    #[serde(default)]
    filter: String,
    #[serde(default)]
    tips: Vec<(String, String)>,
}

#[derive(Serialize)]
struct LocalInfo {
    version: String,
    next_seq: u64,
    bootstrap: u64,
    reconciled: bool,
    size_bytes: u64,
    /// How objects are served here: `local` (packs on disk), `remote` (pack set
    /// too large: indexes local, data by range read), `pending` (packs not yet
    /// downloaded).
    objects: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    remote: Option<RemoteInfo>,
}

#[derive(Serialize)]
struct RemoteInfo {
    packs: usize,
    objects: u64,
    decoded: u64,
    block_range_reads: u64,
    block_bytes_read: u64,
    block_cache_bytes: u64,
}

#[derive(Serialize)]
struct PacksInfo {
    live: usize,
    live_bytes: u64,
    pushes: usize,
}

#[derive(Serialize)]
struct CompactionInfo {
    seq: u64,
    level: u32,
    first_seq: u64,
    last_seq: u64,
    pack_size: u64,
    superseded_packs: usize,
    superseded_bytes: u64,
    at: String,
    primary: String,
}

async fn overview(
    State(state): State<Arc<AppState>>,
    AxumPath((owner, repo)): AxumPath<(String, String)>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    state.auth.require_read(&headers).await.map_err(auth_err)?;
    let id =
        walgit_git::RepoId::new(&owner, &repo).map_err(|e| ApiError::NotFound(e.to_string()))?;
    let handle = state.registry.open(&id).await.map_err(wal_err)?;
    // read_log performs its own freshness check; acquire the read guard only
    // after it has completed because read_log may need the write lock.
    let entries = handle.read_log(1, None).await.map_err(wal_err)?;
    // Refs-level sync only: the overview must render for repos whose packs do
    // not fit this instance (that is exactly when people look at it).
    let _guard = handle.sync_refs().await.map_err(wal_err)?;
    let manifest = handle.manifest();
    let version = handle
        .manifest_version()
        .map(|version| version.to_string())
        .unwrap_or_default();
    let base_url = crate::smart::request_base_url(&state, &headers);
    let clone_url = format!("{}/{}.git", base_url, id);
    let recipes = crate::setup::recipes(&state.cfg, &base_url, Some(&id.to_string()));
    let setup = recipes.setup_text.clone();

    let checkpoint = checkpoint_info(&handle, &manifest, &base_url).await?;
    let created = manifest
        .updated_at
        .as_ref()
        .map(timestamp)
        .unwrap_or_default();
    let packs_bytes = manifest.packs.iter().map(|pack| pack.pack_size).sum();
    let packset = if manifest.packs.is_empty() {
        None
    } else {
        Some(PacksetInfo {
            at_seq: manifest.head_seq,
            packs: manifest.packs.len(),
            bytes: packs_bytes,
            created: created.clone(),
            creator: manifest.writer.clone(),
        })
    };
    let last_push = entries
        .iter()
        .filter(|entry| entry.kind() == EntryKind::Push)
        .filter_map(|entry| entry.created_at.as_ref().map(timestamp))
        .last();
    let mut push_count = 0;
    let mut compactions = Vec::new();
    let mut pack_by_checksum = std::collections::HashMap::new();
    for entry in &entries {
        if let Some(pack) = &entry.pack {
            pack_by_checksum.insert(pack.checksum.as_str(), (pack.seq, pack.pack_size));
        }
        if entry.kind() == EntryKind::Push && entry.pack.is_some() {
            push_count += 1;
        }
        if entry.kind() == EntryKind::Compact {
            let mut first = u64::MAX;
            let mut last = 0;
            let mut bytes = 0;
            for checksum in &entry.supersedes {
                if let Some((seq, size)) = pack_by_checksum.get(checksum.as_str()) {
                    first = first.min(*seq);
                    last = last.max(*seq);
                    bytes += *size;
                }
            }
            compactions.push(CompactionInfo {
                seq: entry.seq,
                level: entry.pack.as_ref().map_or(0, |pack| pack.tier),
                first_seq: if first == u64::MAX { 0 } else { first },
                last_seq: last,
                pack_size: entry.pack.as_ref().map_or(0, |pack| pack.pack_size),
                superseded_packs: entry.supersedes.len(),
                superseded_bytes: bytes,
                at: entry.created_at.as_ref().map(timestamp).unwrap_or_default(),
                primary: entry.writer.clone(),
            });
        }
    }
    let size_bytes = repo_size(handle.local().path()).await;
    let local_version = handle.local_version().unwrap_or_default();
    let reconciled = local_version == version && handle.applied_seq() == manifest.head_seq;
    let remote_reader = handle.remote();
    let objects_mode = if handle.packs_ready() {
        "local"
    } else if remote_reader.is_some() || !handle.packs_fit() {
        "remote"
    } else {
        "pending"
    };
    let remote_info = remote_reader.map(|r| {
        let (reads, bytes, cached) = state.registry.blocks().stats();
        RemoteInfo {
            packs: r.pack_count(),
            objects: r.total_objects(),
            decoded: r.objects_decoded.load(std::sync::atomic::Ordering::Relaxed),
            block_range_reads: reads,
            block_bytes_read: bytes,
            block_cache_bytes: cached,
        }
    });
    let bundles = bundle_infos(&state, &id, &base_url).await?;

    // Health + suggestions.
    let mut issues = Vec::new();
    let mut suggestions = Vec::new();
    let disk_mode_note = state.cfg.cache_is_disk().then(|| {
        format!(
            "local packs · {} on {} (disk mode, no cache budget; eviction only above {:.0}% disk use)",
            walgit_wal::remote::human_bytes(manifest.packs.iter().map(|p| p.pack_size + p.idx_size).sum()),
            state.cfg.cache.dir.display(),
            state.cfg.cache.disk_high_watermark * 100.0
        )
    });
    if disk_mode_note.is_some() {
        // D25: no budget on the SSD host — never the too-large path.
    } else if !handle.packs_fit() {
        issues.push(format!(
            "pack set ({}) exceeds this instance's cache limit ({}); objects are read from the store by range, clones must use bundle-uri",
            walgit_wal::remote::human_bytes(manifest.packs.iter().map(|p| p.pack_size + p.idx_size).sum()),
            walgit_wal::remote::human_bytes(state.cfg.cache.max_bytes.as_u64())
        ));
    }
    if manifest.head_seq > 0 && !reconciled {
        issues.push(format!(
            "local copy on {} is at seq {} but the WAL head is {}",
            walgit_store::coord::instance_id(),
            handle.applied_seq(),
            manifest.head_seq
        ));
        suggestions.push(Suggestion {
            op: "sync",
            params: None,
            reason: "catch the local copy up to the WAL head".into(),
            auto: Some(
                "the next request to this instance revalidates (one conditional GET)".into(),
            ),
        });
    }
    let has_bitmap_base = manifest.packs.iter().any(|p| p.tier == 2 && p.has_bitmap);
    let fresh = manifest.packs.iter().filter(|p| p.tier == 0).count();
    let ecfg = handle.effective_config();
    let compaction_on = ecfg.compaction.enabled && state.cfg.has_role(walgit_config::Role::Compact);
    let live_bytes: u64 = manifest
        .packs
        .iter()
        .map(|p| p.pack_size + p.idx_size)
        .sum();
    let weekly = ecfg
        .bundles
        .strategy
        .iter()
        .find(|s| s.kind == walgit_config::BundleKind::Full)
        .map(|s| s.name.clone());
    if !manifest.packs.is_empty() && !has_bitmap_base {
        suggestions.push(Suggestion {
            op: "compact",
            params: Some("base=1".into()),
            reason: "no bitmap'd base pack: clones compute reachability on every instance".into(),
            auto: match (&weekly, compaction_on) {
                (Some(w), true) => Some(format!(
                    "at the next `{w}` slot, on a maintainer whose capacity holds the pack set ({})",
                    walgit_wal::remote::human_bytes(live_bytes)
                )),
                (Some(_), false) => None,
                (None, _) => None,
            },
        });
    } else if fresh >= ecfg.compaction.trigger_packs.max(2) {
        suggestions.push(Suggestion {
            op: "compact",
            params: None,
            reason: format!("{fresh} fresh push packs waiting to be folded"),
            auto: compaction_on.then(|| {
                format!(
                    "geometric fold on the maintainer's next pass (trigger: {} packs / {})",
                    ecfg.compaction.trigger_packs, ecfg.compaction.trigger_bytes
                )
            }),
        });
    }
    if manifest.head_seq > 0 {
        let cp_seq = manifest.checkpoint.as_ref().map(|c| c.seq).unwrap_or(0);
        let behind = manifest.head_seq.saturating_sub(cp_seq);
        if behind >= state.cfg.wal.snapshot_every_entries.max(1) || (cp_seq == 0 && behind > 0) {
            suggestions.push(Suggestion {
                op: "checkpoint",
                params: None,
                reason: if cp_seq == 0 {
                    "no checkpoint yet: cold materialize replays the whole log".into()
                } else {
                    format!("checkpoint is {behind} entries behind the head")
                },
                auto: Some(format!(
                    "first unit of the maintainer's next pass (every {} entries / {} / {} of tail)",
                    state.cfg.wal.snapshot_every_entries,
                    humantime::format_duration(state.cfg.wal.checkpoint_interval),
                    state.cfg.wal.checkpoint_tail_bytes
                )),
            });
        }
        if ecfg.bundles.enabled && bundles.is_empty() {
            suggestions.push(Suggestion {
                op: "bundle",
                params: None,
                reason: "no bundle-uri bundle published: initial clones go through upload-pack".into(),
                auto: weekly.as_ref().map(|w| format!("the first `{w}` slot at or after the repository's first WAL state, on the maintainer's next pass")),
            });
        }
    }
    // The audit verdict lives in the store (`fsck.pb`, written by whichever maintainer ran it),
    // not in this instance's task memory.
    let fsck_report = crate::ops::read_fsck(&handle).await.ok().flatten();
    let deep = match &fsck_report {
        Some(r) => {
            let when =
                r.at.as_ref()
                    .map(|t| t.seconds)
                    .map(|s| {
                        chrono::DateTime::from_timestamp(s, 0)
                            .map(|d| d.format("%Y-%m-%d %H:%MZ").to_string())
                            .unwrap_or_default()
                    })
                    .unwrap_or_default();
            let verdict = if r.missing_total == 0 && r.problems == 0 {
                "clean".to_string()
            } else {
                format!(
                    "{} missing object(s), {} other problem(s){}",
                    r.missing_total,
                    r.problems,
                    if r.repaired_seq > 0 {
                        format!("; repaired at seq {}", r.repaired_seq)
                    } else {
                        String::new()
                    }
                )
            };
            format!(
                "{verdict} at seq {} ({when}, {}, {:.1}s)",
                r.seq, r.host, r.elapsed_secs
            )
        }
        None => "never audited".into(),
    };
    let fsck_every = ecfg.maintenance.fsck_interval;
    if fsck_report.is_none() && manifest.head_seq > 0 {
        suggestions.push(Suggestion {
            op: "fsck",
            params: Some("connectivity=1".into()),
            reason: "connectivity never audited".into(),
            auto: (!fsck_every.is_zero()).then(|| {
                format!(
                    "lowest-priority unit: runs when nothing else is due, then every {}",
                    humantime::format_duration(fsck_every)
                )
            }),
        });
    } else if let Some(r) = &fsck_report
        && (r.missing_total > 0 || r.problems > 0)
        && r.repaired_seq == 0
    {
        issues.push(format!(
            "last fsck found {} missing object(s), {} other problem(s)",
            r.missing_total, r.problems
        ));
        suggestions.push(Suggestion {
            op: "repair",
            params: None,
            reason: "fetch the missing objects from upstream.git and publish them".into(),
            auto: ecfg.upstream.git.as_ref().map(|_| {
                "the repair unit, right after checkpoints in the maintainer's priority".to_string()
            }),
        });
    }
    let status = if issues.iter().any(|i| i.starts_with("last fsck found")) {
        "error"
    } else if !issues.is_empty() {
        "degraded"
    } else {
        "ok"
    };
    let ops = OpsInfo {
        available: crate::ops::OPS.to_vec(),
        recent: state.registry.tasks().recent(&id.to_string()),
        bundle_strategies: state
            .cfg
            .bundles
            .strategy
            .iter()
            .map(|s| s.name.clone())
            .collect(),
    };
    let clone = CloneInfo {
        manual: recipes.manual_clone.clone(),
        plain: recipes.plain_clone.clone(),
    };
    // Slot table + maintainers (best effort; never fails the overview).
    let bundle_plan = {
        let ctx = walgit_bundle::slots::PlanContext {
            first_state: handle.first_state_time(),
            can_full: true,
            can_incremental: true,
            wrong_host_reason: None,
        };
        let slots = match state
            .bundles
            .plan(&id, std::time::SystemTime::now(), ctx)
            .await
        {
            Ok(rows) => rows
                .into_iter()
                .map(|r| {
                    use walgit_bundle::slots::SlotStatus as S;
                    let (status, detail, bundle_id) = match r.status {
                        S::Built { id, size, seq } => {
                            ("built", format!("{size} bytes, seq {seq}"), Some(id))
                        }
                        S::Missing => ("missing", String::new(), None),
                        S::Pending => (
                            "pending",
                            "slot just fired; built or settled after the 2-minute close grace"
                                .into(),
                            None,
                        ),
                        S::Blocked(w) => ("blocked", w, None),
                        S::Unavailable => ("unavailable", "no WAL state at that time".into(), None),
                        S::TooSmall { commits, min } => (
                            "too-small",
                            format!(
                                "{commits} commits since base (min {min}); next slot catches up"
                            ),
                            None,
                        ),
                        S::Skipped { reason } => ("skipped", reason, None),
                        S::WrongHost(w) => ("wrong-host", w, None),
                    };
                    SlotInfo {
                        strategy: r.strategy,
                        kind: format!("{:?}", r.kind).to_lowercase(),
                        slot: r.slot,
                        status: status.into(),
                        detail,
                        bundle_id,
                    }
                })
                .collect(),
            Err(_) => Vec::new(),
        };
        let hbs_all = crate::maintain::heartbeats(&state)
            .await
            .unwrap_or_default();
        let upcoming = crate::maintain::upcoming(
            &handle,
            &handle.effective_config(),
            &hbs_all,
            std::time::SystemTime::now(),
        )
        .await;
        let maintainers: Vec<MaintainerInfo> = match Ok::<_, anyhow::Error>(hbs_all) {
            Ok(hbs) => hbs
                .into_iter()
                .filter(|h| {
                    walgit_config::repo_listed(&h.repos, id.owner(), id.name())
                        && !walgit_config::repo_listed(&h.exclude, id.owner(), id.name())
                })
                .map(|h| {
                    let age = h
                        .last_pass_at
                        .as_ref()
                        .map(walgit_proto::time::to_system)
                        .and_then(|t| std::time::SystemTime::now().duration_since(t).ok())
                        .map(|d| d.as_secs());
                    MaintainerInfo {
                        host: h.host,
                        disk: h.disk,
                        max_pack_bytes: h.max_pack_bytes,
                        last_pass_age_secs: age,
                        alive: age.map(|a| a < 600).unwrap_or(false),
                        passes: h.passes,
                        last_unit: h.last_unit,
                    }
                })
                .collect(),
            Err(_) => Vec::new(),
        };
        let orphaned = !maintainers.iter().any(|m| m.alive);
        BundlePlanInfo {
            slots,
            upcoming,
            maintainers,
            orphaned,
        }
    };
    let body = Overview {
        repo: id.to_string(),
        instance: crate::instance::info(&state.cfg),
        clone_url,
        setup,
        install: recipes.install.clone(),
        install_url: recipes.install_url.clone(),
        hostname: walgit_store::coord::instance_id().to_string(),
        health: Health {
            status,
            issues,
            deep,
            suggestions,
        },
        manifest: ManifestInfo {
            version: version.clone(),
            next_seq: manifest.head_seq.saturating_add(1),
            min_seq: manifest.min_seq,
            segments: manifest
                .log_segments
                .iter()
                .map(|segment| SegmentInfo {
                    key: segment.key.clone(),
                    first_seq: segment.first_seq,
                    last_seq: segment.last_seq,
                    size: segment.size,
                })
                .collect(),
            tail_entries: entries.len(),
            entries: entries.len(),
            checkpoint,
            packset,
            advertised_bundle_uri: None,
            last_push,
        },
        local: LocalInfo {
            version: local_version.clone(),
            next_seq: handle.applied_seq().saturating_add(1),
            bootstrap: handle.applied_seq(),
            reconciled,
            size_bytes,
            objects: objects_mode,
            remote: remote_info,
        },
        packs: PacksInfo {
            live: manifest.packs.len(),
            live_bytes: packs_bytes,
            pushes: push_count,
        },
        bundles,
        bundle_plan,
        compactions,
        node: {
            let mut m = serde_json::Map::new();
            if let Some(n) = disk_mode_note {
                m.insert("storage".into(), serde_json::Value::String(n));
            }
            m
        },
        ops,
        clone,
    };
    Ok((
        [
            (header::CONTENT_TYPE, "application/json"),
            (header::CACHE_CONTROL, "no-store"),
        ],
        serde_json::to_vec(&body).map_err(|e| ApiError::Internal(e.to_string()))?,
    )
        .into_response())
}

/// `GET …/ops` — available ops + recent outcomes on this instance.
async fn ops_list(
    State(state): State<Arc<AppState>>,
    AxumPath((owner, repo)): AxumPath<(String, String)>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    state.auth.require_read(&headers).await.map_err(auth_err)?;
    let id =
        walgit_git::RepoId::new(&owner, &repo).map_err(|e| ApiError::NotFound(e.to_string()))?;
    let body = OpsInfo {
        available: crate::ops::OPS.to_vec(),
        recent: state.registry.tasks().recent(&id.to_string()),
        bundle_strategies: state
            .cfg
            .bundles
            .strategy
            .iter()
            .map(|s| s.name.clone())
            .collect(),
    };
    Ok((
        [
            (header::CONTENT_TYPE, "application/json"),
            (header::CACHE_CONTROL, "no-store"),
        ],
        serde_json::to_vec(&body).map_err(|e| ApiError::Internal(e.to_string()))?,
    )
        .into_response())
}

/// `POST …/ops/{op}?<params>` — run a maintenance op on this instance as a
/// background task and stream it (SSE envelope: `task`, `notice`, `progress`,
/// then `result` `{"task","value"}` or `error`). Write permission required.
/// If the same op is already running here the response attaches to that task
/// instead (same stream shape; its `task.id` tells you which).
async fn ops_start(
    State(state): State<Arc<AppState>>,
    AxumPath((owner, repo, op)): AxumPath<(String, String, String)>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let principal = state.auth.require_write(&headers).await.map_err(auth_err)?;
    let id =
        walgit_git::RepoId::new(&owner, &repo).map_err(|e| ApiError::NotFound(e.to_string()))?;
    // Make sure the repo exists before spawning anything.
    state.registry.open(&id).await.map_err(wal_err)?;
    tracing::info!(repo = %id, op = %op, by = %principal.name, ?params, "ops.start");
    let task = match crate::ops::start(state.clone(), id, &op, params).await {
        Ok(t) => t,
        Err(crate::ops::StartError::UnknownOp) => {
            return Err(ApiError::NotFound(format!("unknown op {op}")));
        }
        Err(crate::ops::StartError::AlreadyRunning(existing)) => existing,
    };
    Ok(crate::sse::task_stream(task))
}

/// `GET …/tasks` — running + recent background tasks of this repo on this
/// instance (materialize, remote-index, fsck, compact, bundle, ...). The UI
/// polls this to show what is happening to a repo.
async fn tasks_list(
    State(state): State<Arc<AppState>>,
    AxumPath((owner, repo)): AxumPath<(String, String)>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    state.auth.require_read(&headers).await.map_err(auth_err)?;
    let id =
        walgit_git::RepoId::new(&owner, &repo).map_err(|e| ApiError::NotFound(e.to_string()))?;
    let tasks = state.registry.tasks();
    let body = serde_json::json!({
        "hostname": walgit_store::coord::instance_id(),
        "running": tasks.running(&id.to_string()),
        "recent": tasks.recent(&id.to_string()),
    });
    Ok((
        [
            (header::CONTENT_TYPE, "application/json"),
            (header::CACHE_CONTROL, "no-store"),
        ],
        serde_json::to_vec(&body).map_err(|e| ApiError::Internal(e.to_string()))?,
    )
        .into_response())
}

/// `GET …/tasks/{id}` — attach to a task: SSE replay of its packets so far,
/// then live, then the terminal `result`/`error`. JSON (no SSE accept) returns
/// the record.
async fn task_stream(
    State(state): State<Arc<AppState>>,
    AxumPath((owner, repo, task_id)): AxumPath<(String, String, String)>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    state.auth.require_read(&headers).await.map_err(auth_err)?;
    let id =
        walgit_git::RepoId::new(&owner, &repo).map_err(|e| ApiError::NotFound(e.to_string()))?;
    let task = state
        .registry
        .tasks()
        .get(&task_id)
        .filter(|t| t.record().repo == id.to_string())
        .ok_or_else(|| ApiError::NotFound(format!("task {task_id} (tasks are per instance; this one may have run elsewhere or aged out)")))?;
    if !crate::sse::wants_sse(&headers) {
        return Ok((
            [
                (header::CONTENT_TYPE, "application/json"),
                (header::CACHE_CONTROL, "no-store"),
            ],
            serde_json::to_vec(&task.record()).map_err(|e| ApiError::Internal(e.to_string()))?,
        )
            .into_response());
    }
    Ok(crate::sse::task_stream(task))
}

/// `GET …/snapshot/{seq}` — the WAL rewind of `seq` this instance has
/// materialized (`POST …/ops/snapshot?at_seq={seq}`): its refs, its pack set
/// and where the rebuilt copy lives. Same auth as the overview (read).
///
/// Snapshots are per instance and a cache like every other byte on disk, so
/// this is `no-store` and the 404 names the op that builds one. It never
/// touches live refs: `head_seq` in the answer is where the serving copy is.
async fn snapshot(
    State(state): State<Arc<AppState>>,
    AxumPath((owner, repo, seq)): AxumPath<(String, String, u64)>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    state.auth.require_read(&headers).await.map_err(auth_err)?;
    let id =
        walgit_git::RepoId::new(&owner, &repo).map_err(|e| ApiError::NotFound(e.to_string()))?;
    // Answer nothing about a repository that does not exist.
    state.registry.open(&id).await.map_err(wal_err)?;
    let dir = walgit_wal::snapshot::snapshot_dir(&state.cfg.cache.dir, &id, seq);
    let body = tokio::task::spawn_blocking(move || walgit_wal::snapshot::read_snapshot(&dir))
        .await
        .map_err(|e| ApiError::Internal(e.to_string()))?
        .ok_or_else(|| {
            ApiError::NotFound(format!(
                "no snapshot of {id} at seq {seq} on {}; build one with POST /{id}/api/ops/snapshot?at_seq={seq}",
                walgit_store::coord::instance_id()
            ))
        })?;
    Ok((
        [
            (header::CONTENT_TYPE, "application/json"),
            (header::CACHE_CONTROL, "no-store"),
        ],
        serde_json::to_vec(&body).map_err(|e| ApiError::Internal(e.to_string()))?,
    )
        .into_response())
}

async fn checkpoint_info(
    handle: &walgit_wal::RepoHandle,
    manifest: &walgit_proto::v1::Manifest,
    _base_url: &str,
) -> Result<Option<BundleInfo>, ApiError> {
    let Some(reference) = &manifest.checkpoint else {
        return Ok(None);
    };
    let (size, created, creator) = match handle
        .store()
        .get(&reference.key, GetOptions::default())
        .await
    {
        Ok(GetResult::Object { meta, body }) => {
            let bytes = walgit_store::util::collect(body, meta.size as usize)
                .await
                .map_err(|e| ApiError::Internal(e.to_string()))?;
            let checkpoint = Checkpoint::decode(bytes.as_ref())
                .map_err(|e| ApiError::Internal(e.to_string()))?;
            (
                meta.size,
                checkpoint
                    .created_at
                    .as_ref()
                    .map(timestamp)
                    .unwrap_or_default(),
                checkpoint.writer,
            )
        }
        Ok(GetResult::NotModified { .. }) => (0, String::new(), String::new()),
        Err(walgit_store::StoreError::NotFound { .. }) => (0, String::new(), String::new()),
        Err(error) => return Err(ApiError::Internal(error.to_string())),
    };
    Ok(Some(BundleInfo {
        sha: reference.key.clone(),
        size,
        at_seq: reference.seq,
        created,
        creator,
        // Checkpoints are store objects (not served over HTTP); show the key.
        uri: format!("gs://…/{}", reference.key),
        strategy: String::new(),
        kind: String::new(),
        base_id: String::new(),
        creation_token: 0,
        filter: String::new(),
        tips: Vec::new(),
    }))
}

async fn bundle_infos(
    state: &AppState,
    id: &walgit_git::RepoId,
    base_url: &str,
) -> Result<Vec<BundleInfo>, ApiError> {
    let list = state
        .bundles
        .list(id)
        .await
        .map_err(|e| ApiError::Internal(e.to_string()))?;
    let Some(list) = list else {
        return Ok(Vec::new());
    };
    // Exactly the URIs the bundle-uri advertisement hands to git (one code
    // path: walgit_bundle::render::bundle_uri), so the WAL page never shows a
    // link that differs from what clients download.
    let handle = state.registry.open(id).await.map_err(wal_err)?;
    let mut out = Vec::with_capacity(list.bundles.len());
    for bundle in list.bundles {
        let uri = walgit_bundle::render::bundle_uri(
            &bundle,
            id.owner(),
            id.name(),
            base_url,
            state.cfg.bundles.serve_via,
            handle.store(),
            state.cfg.bundles.signed_url_ttl,
        )
        .await
        .map_err(|e| ApiError::Internal(e.to_string()))?;
        out.push(BundleInfo {
            sha: bundle.id.clone(),
            size: bundle.size,
            at_seq: bundle.seq,
            created: bundle
                .created_at
                .as_ref()
                .map(timestamp)
                .unwrap_or_default(),
            creator: String::new(),
            uri,
            strategy: bundle.strategy.clone(),
            kind: bundle.kind.clone(),
            base_id: bundle.base_id.clone(),
            creation_token: bundle.creation_token,
            filter: bundle.filter.clone(),
            tips: bundle
                .tips
                .iter()
                .map(|t| (t.name.clone(), t.oid.clone()))
                .collect(),
        });
    }
    Ok(out)
}

fn timestamp(value: &prost_types::Timestamp) -> String {
    chrono::DateTime::<chrono::Utc>::from_timestamp(value.seconds, value.nanos as u32)
        .map(|date| date.to_rfc3339())
        .unwrap_or_default()
}

async fn repo_size(path: &Path) -> u64 {
    let path = path.to_owned();
    tokio::task::spawn_blocking(move || size_recursive(&path))
        .await
        .unwrap_or(0)
}

fn size_recursive(path: &Path) -> u64 {
    let Ok(metadata) = fs::symlink_metadata(path) else {
        return 0;
    };
    if metadata.is_file() {
        return metadata.len();
    }
    if !metadata.is_dir() {
        return 0;
    }
    fs::read_dir(path)
        .into_iter()
        .flatten()
        .flatten()
        .map(|entry| size_recursive(&entry.path()))
        .sum()
}

fn wal_err(error: walgit_wal::WalError) -> ApiError {
    match error {
        walgit_wal::WalError::NotFound => ApiError::NotFound("repository not found".into()),
        other => ApiError::Internal(format!("wal: {other}")),
    }
}

fn auth_err(error: crate::auth::AuthError) -> ApiError {
    match error {
        crate::auth::AuthError::Invalid | crate::auth::AuthError::Unauthorized => {
            ApiError::Unauthorized
        }
        crate::auth::AuthError::Forbidden => ApiError::Forbidden,
        crate::auth::AuthError::Unavailable => {
            ApiError::ServiceUnavailable("auth provider unavailable".into())
        }
    }
}
