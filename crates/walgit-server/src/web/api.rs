//! Read-only JSON API for the web UI (`web/API.md`, v2 contract).
//!
//! Two URL classes: ref-dependent (`refs`, `refs/{branches,tags}`,
//! `resolve`, name-addressed tree/blob/commits/commit) answered from a
//! per-manifest-version [`RefIndex`] with `stale-while-revalidate` + `ETag`,
//! and sha-addressed immutable ones (`tree/<sha>`, `blob/<sha>`,
//! `commits?ref=<sha>`, `commit/<sha>`) rendered once and cached in memory
//! (and, for remotely served repos, in the object store so every instance
//! shares one render cache).
//!
//! Object access (`Need::Objects`) goes through [`RepoHandle::sync_objects`]:
//! packs on disk when they fit this instance, otherwise the remote reader
//! (pack indexes local, data by range read; `web/objects.rs` faults what a
//! git command will touch into the loose store). Anything that cannot answer
//! immediately streams the SSE envelope (`crate::sse`) when the client
//! accepts it: notices + progress from the repo's task channel, then
//! `result`/`error`.

use std::future::Future;
use std::sync::Arc;

use axum::{
    Router,
    extract::{Path, Query, State},
    http::{HeaderMap, header},
    response::{IntoResponse, Response},
    routing::get,
};
use futures::StreamExt;
use serde::Serialize;
use walgit_store::{GetOptions, ObjectStore, Prefixed, PutBody, PutMode};
use walgit_wal::{ObjectAccess, RepoHandle, Reporter};

use crate::sse::Rendered;
use crate::web::objects::{CommitMeta, Remote};
use crate::{AppState, auth::AuthError, cache::RefIndex, error::ApiError};

const MAX_BLOB: usize = 2 * 1024 * 1024;
const IMMUTABLE: &str = "private, max-age=31536000, immutable";
const SWR: &str = "private, max-age=0, stale-while-revalidate=60";
const DEFAULT_PAGE: usize = 100;
const MAX_PAGE: usize = 1000;
/// Store key prefix (inside the repo prefix) of the shared render cache.
const SHARED_CACHE_PREFIX: &str = "cache/api/v1/";

#[derive(Serialize, Clone)]
pub(crate) struct RefInfo {
    pub(crate) name: String,
    pub(crate) sha: String,
}
#[derive(Serialize)]
struct Refs {
    head: Option<RefInfo>,
}
#[derive(Serialize)]
struct RefPage {
    refs: Vec<RefInfo>,
    more: bool,
}
#[derive(Serialize, Clone)]
struct Resolved {
    #[serde(rename = "ref")]
    ref_name: String,
    sha: String,
    path: String,
    kind: &'static str,
}
#[derive(Serialize, Clone)]
struct Commit {
    sha: String,
    parents: Vec<String>,
    author: String,
    author_email: String,
    author_date: String,
    committer: String,
    commit_date: String,
    subject: String,
    /// The message body WITHOUT the trailer block (see `trailers`).
    body: String,
    /// Git trailers of the message (`Key: value` lines of the last paragraph,
    /// `git interpret-trailers --parse` rules), in order.
    trailers: Vec<super::trailers::Trailer>,
}
impl From<CommitMeta> for Commit {
    fn from(m: CommitMeta) -> Self {
        let (body, trailers) = super::trailers::split_trailers(&m.body);
        Commit {
            sha: m.id.to_string(),
            parents: m
                .parents
                .iter()
                .map(std::string::ToString::to_string)
                .collect(),
            author: m.author,
            author_email: m.author_email,
            author_date: m.author_date,
            committer: m.committer,
            commit_date: m.commit_date,
            subject: m.subject,
            body,
            trailers,
        }
    }
}
impl Commit {
    fn with_body(mut self, raw: &str) -> Self {
        let (body, trailers) = super::trailers::split_trailers(raw.trim());
        self.body = body;
        self.trailers = trailers;
        self
    }
}
#[derive(Serialize)]
struct Tree {
    #[serde(rename = "ref")]
    ref_name: String,
    sha: String,
    path: String,
    entries: Vec<TreeEntry>,
    #[serde(skip_serializing_if = "Option::is_none")]
    commit: Option<Commit>,
    #[serde(skip_serializing_if = "Option::is_none")]
    readme: Option<Readme>,
}
#[derive(Serialize)]
struct TreeEntry {
    name: String,
    #[serde(rename = "type")]
    kind: String,
    mode: String,
    size: i64,
    sha: String,
}
#[derive(Serialize)]
struct Blob {
    #[serde(rename = "ref")]
    ref_name: String,
    sha: String,
    path: String,
    name: String,
    size: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    contents: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    binary: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    too_large: Option<bool>,
}
#[derive(Serialize)]
struct Readme {
    name: String,
    contents: String,
}
#[derive(Serialize)]
struct Commits {
    #[serde(rename = "ref")]
    ref_name: String,
    sha: String,
    commits: Vec<Commit>,
    more: bool,
}
#[derive(Serialize)]
struct Stat {
    path: String,
    additions: i64,
    deletions: i64,
}
#[derive(Serialize)]
struct CommitDetail {
    commit: Commit,
    stats: Vec<Stat>,
    patch: String,
}
#[derive(serde::Deserialize, Default)]
struct CommitQuery {
    #[serde(rename = "ref")]
    ref_: Option<String>,
    path: Option<String>,
    skip: Option<usize>,
    n: Option<usize>,
}
#[derive(serde::Deserialize, Default)]
struct BlobQuery {
    raw: Option<String>,
}
#[derive(serde::Deserialize, Default)]
struct RefListQuery {
    prefix: Option<String>,
    q: Option<String>,
    after: Option<String>,
    n: Option<usize>,
}

pub fn router(state: Arc<AppState>) -> Router {
    // D26/D27: repo-scoped endpoints live under the repository's own prefix,
    // `/{owner}/{repo}/api/…` (bearer/session lane) and `/{owner}/{repo}/api-browser/…`
    // (browser lane: another origin with `credentials: "include"`);
    // same handlers, the lane differs by credential handling only. Non-repo
    // endpoints keep `/services/api/owners*` and `/api/v1/*`.
    let mut r = Router::new()
        .route("/services/api/instance", get(instance_info))
        .route("/services/api/owners", get(owners))
        .route("/services/api/owners/{owner}", get(owner_repos));
    for base in REPO_API_BASES {
        r = r
            .route(&format!("{base}/refs"), get(refs))
            .route(&format!("{base}/refs/{{kind}}"), get(ref_list))
            .route(&format!("{base}/resolve"), get(resolve_root))
            .route(&format!("{base}/resolve/"), get(resolve_root))
            .route(&format!("{base}/resolve/{{*rest}}"), get(resolve))
            .route(&format!("{base}/tree/{{*rest}}"), get(tree))
            .route(&format!("{base}/blob/{{*rest}}"), get(blob))
            .route(&format!("{base}/commits"), get(commits))
            .route(&format!("{base}/commit/{{sha}}"), get(commit_detail));
    }
    r.with_state(state)
}

/// Route prefixes of the repo-scoped JSON API (D27): one per lane, both
/// *after* the repository prefix. No lane-first forms, no aliases (banner).
pub const REPO_API_BASES: [&str; 2] = ["/{owner}/{repo}/api", "/{owner}/{repo}/api-browser"];

pub(crate) fn auth_err(e: AuthError) -> ApiError {
    match e {
        AuthError::Invalid | AuthError::Unauthorized => ApiError::Unauthorized,
        AuthError::Forbidden => ApiError::Forbidden,
        AuthError::Unavailable => ApiError::ServiceUnavailable("auth provider unavailable".into()),
    }
}
fn not_found(msg: impl Into<String>) -> ApiError {
    ApiError::NotFound(msg.into())
}
fn internal<E: std::fmt::Display>(e: E) -> ApiError {
    ApiError::Internal(e.to_string())
}

/// One synced view of a repository for the duration of a request.
pub struct Repo {
    id: String,
    local: walgit_git::LocalRepo,
    #[allow(dead_code)]
    version: String,
    pub(crate) index: Arc<RefIndex>,
    handle: Arc<RepoHandle>,
    access: ObjectAccess,
    /// Whether objects are readable (`Need::Objects` satisfied).
    objects: bool,
    reporter: Reporter,
    /// Shared render cache (object store) — set for remotely served repos.
    shared: Option<Prefixed>,
}
impl Repo {
    fn remote(&self) -> Option<Remote> {
        match &self.access {
            ObjectAccess::Remote(r) if self.objects => Some(Remote::new(
                r.clone(),
                self.local.clone(),
                self.reporter.clone(),
            )),
            _ => None,
        }
    }
    /// Upgrade a refs-level view to objects (used by `resolve` for raw revisions).
    async fn need_objects(&mut self, st: &AppState) -> Result<(), ApiError> {
        if self.objects {
            return Ok(());
        }
        let (guard, access) = self
            .handle
            .sync_objects()
            .await
            .map_err(crate::smart::wal_err)?;
        drop(guard);
        self.objects = true;
        self.access = access;
        self.shared = shared_cache(st, &self.handle, &self.access);
        Ok(())
    }
}

/// What a request needs from the local copy: refs only (cheap, always
/// possible) or objects too (packs on disk or the remote reader).
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Need {
    Refs,
    Objects,
}

fn shared_cache(st: &AppState, handle: &RepoHandle, access: &ObjectAccess) -> Option<Prefixed> {
    (st.cfg.cache.shared_render_cache && access.is_remote()).then(|| handle.store().clone())
}

async fn open(
    st: &AppState,
    headers: &HeaderMap,
    owner: &str,
    name: &str,
) -> Result<Arc<RepoHandle>, ApiError> {
    st.auth.require_read(headers).await.map_err(auth_err)?;
    let id = walgit_git::RepoId::new(owner, name).map_err(|_| not_found("repository"))?;
    st.registry.open(&id).await.map_err(|e| match e {
        walgit_wal::WalError::NotFound => not_found("repository"),
        _ => internal(e),
    })
}

async fn view(
    st: &AppState,
    handle: Arc<RepoHandle>,
    need: Need,
    reporter: Reporter,
) -> Result<Repo, ApiError> {
    let (guard, access, objects) = match need {
        Need::Refs => (
            handle.sync_refs().await.map_err(crate::smart::wal_err)?,
            ObjectAccess::Local,
            false,
        ),
        Need::Objects => {
            let (g, a) = handle.sync_objects().await.map_err(crate::smart::wal_err)?;
            (g, a, true)
        }
    };
    // The guard is held until the local handle has been cloned and the ref
    // index for this manifest version exists. The local repository itself is
    // thread-safe and subsequent git commands read its synced state.
    let local = handle.local().clone();
    let version = handle
        .manifest_version()
        .map(|v| v.as_str().to_string())
        .unwrap_or_default();
    let id = handle.id().to_string();
    let index = st
        .caches
        .ref_index
        .get_or_build(&id, &version, || local.refs())
        .map_err(internal)?;
    drop(guard);
    let shared = shared_cache(st, &handle, &access);
    Ok(Repo {
        id,
        local,
        version,
        index,
        handle,
        access,
        objects,
        reporter,
        shared,
    })
}

fn shared_key(cache_key: &str) -> String {
    use sha1::Digest;
    let h = sha1::Sha1::digest(cache_key.as_bytes());
    format!("{SHARED_CACHE_PREFIX}{}.json", hex::encode(h))
}

/// Run one endpoint: auth + open, immutable caches, then either a plain
/// response or (when the answer needs long work and the client accepts it)
/// the SSE envelope streaming the repo's progress until the result.
pub(crate) async fn run<F, Fut>(
    st: &Arc<AppState>,
    headers: &HeaderMap,
    owner: &str,
    name: &str,
    need: Need,
    immutable_key: Option<String>,
    work: F,
) -> Result<Response, ApiError>
where
    F: FnOnce(Repo) -> Fut + Send + 'static,
    Fut: Future<Output = Result<Rendered, ApiError>> + Send + 'static,
{
    let handle = open(st, headers, owner, name).await?;
    let slow = need == Need::Objects && !handle.packs_ready();
    if let Some(key) = &immutable_key {
        if let Some(hit) = st.caches.api_immutable.get(key) {
            metrics::counter!("walgit_api_immutable_hit", "tier" => "memory").increment(1);
            return Ok(Rendered::json(hit, IMMUTABLE, None).into_response(headers));
        }
        if slow
            && st.cfg.cache.shared_render_cache
            && let Ok(walgit_store::GetResult::Object { body, meta }) = handle
                .store()
                .get(&shared_key(key), GetOptions::default())
                .await
            && let Ok(b) = walgit_store::util::collect(body, meta.size as usize).await
        {
            metrics::counter!("walgit_api_immutable_hit", "tier" => "store").increment(1);
            st.caches.api_immutable.insert(key.clone(), b.clone());
            return Ok(Rendered::json(b, IMMUTABLE, None).into_response(headers));
        }
    }
    if slow && crate::sse::wants_sse(headers) {
        let (tx, rx) = tokio::sync::broadcast::channel(256);
        let sources = vec![handle.subscribe_progress(), rx];
        let st2 = st.clone();
        let fut = async move {
            let repo = view(&st2, handle, need, Reporter::for_repo(tx)).await?;
            work(repo).await
        };
        return Ok(crate::sse::envelope(sources, fut));
    }
    let repo = view(st, handle, need, Reporter::none()).await?;
    Ok(work(repo).await?.into_response(headers))
}

// ---- response helpers --------------------------------------------------------

pub(crate) fn etag_for(sha: &str) -> String {
    format!("\"{sha}\"")
}
pub(crate) fn json_swr<T: Serialize>(value: &T, etag: Option<&str>) -> Rendered {
    Rendered::json(json_bytes(value), SWR, etag.map(str::to_string))
}
fn json_bytes<T: Serialize>(value: &T) -> bytes::Bytes {
    bytes::Bytes::from(serde_json::to_vec(value).unwrap_or_default())
}
fn is_full_sha(s: &str) -> bool {
    (s.len() == 40 || s.len() == 64) && s.bytes().all(|b| b.is_ascii_hexdigit())
}

/// Finish a ref-or-sha addressed request: immutable (+LRU +shared cache) or
/// SWR+ETag (304 handled by `Rendered::into_response`).
fn finish(
    st: &AppState,
    r: &Repo,
    immutable: bool,
    cache_key: &str,
    sha: &str,
    body: bytes::Bytes,
) -> Rendered {
    if immutable {
        st.caches
            .api_immutable
            .insert(cache_key.to_string(), body.clone());
        if let Some(store) = &r.shared {
            let store = store.clone();
            let key = shared_key(cache_key);
            let b = body.clone();
            tokio::spawn(async move {
                if let Err(e) = store
                    .put(&key, PutBody::Bytes(b), PutMode::Overwrite.into())
                    .await
                {
                    tracing::debug!(error = %e, key, "shared render cache put failed");
                }
            });
        }
        return Rendered::json(body, IMMUTABLE, None);
    }
    Rendered::json(body, SWR, Some(etag_for(sha)))
}

// ---- instance ----------------------------------------------------------------

/// Which instance answered (kind/name/revision/build) — footer of the UI. Not
/// cached: every response should reflect the machine that produced it.
async fn instance_info(
    State(st): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    st.auth.require_read(&headers).await.map_err(auth_err)?;
    let mut r = axum::Json(crate::instance::info(&st.cfg)).into_response();
    r.headers_mut()
        .insert(header::CACHE_CONTROL, "no-store".parse().unwrap());
    Ok(r)
}

// ---- owners ------------------------------------------------------------------

pub(crate) async fn owners(
    State(st): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    st.auth.require_read(&headers).await.map_err(auth_err)?;
    let repos = st.registry.list().await.map_err(internal)?;
    let mut out: Vec<String> = repos.into_iter().map(|r| r.owner().to_string()).collect();
    out.sort();
    out.dedup();
    Ok(json_swr(&out, None).into_response(&headers))
}
pub(crate) async fn owner_repos(
    State(st): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(owner): Path<String>,
) -> Result<Response, ApiError> {
    st.auth.require_read(&headers).await.map_err(auth_err)?;
    let repos = st.registry.list().await.map_err(internal)?;
    let mut out: Vec<String> = repos
        .into_iter()
        .filter(|r| r.owner() == owner)
        .map(|r| r.name().to_string())
        .collect();
    out.sort();
    out.dedup();
    Ok(json_swr(&out, None).into_response(&headers))
}

// ---- refs --------------------------------------------------------------------

async fn refs(
    State(st): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((owner, repo_name)): Path<(String, String)>,
) -> Result<Response, ApiError> {
    run(
        &st,
        &headers,
        &owner,
        &repo_name,
        Need::Refs,
        None,
        |r| async move {
            let head = r.index.head().map(|(name, sha)| RefInfo { name, sha });
            let etag = etag_for(head.as_ref().map_or("unborn", |h| h.sha.as_str()));
            Ok(json_swr(&Refs { head }, Some(&etag)))
        },
    )
    .await
}

async fn ref_list(
    State(st): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((owner, repo_name, kind)): Path<(String, String, String)>,
    Query(q): Query<RefListQuery>,
) -> Result<Response, ApiError> {
    let wants_sse = crate::sse::wants_sse(&headers);
    let handle = open(&st, &headers, &owner, &repo_name).await?;
    let r = view(&st, handle, Need::Refs, Reporter::none()).await?;
    let list = match kind.as_str() {
        "branches" => &r.index.branches,
        "tags" => &r.index.tags,
        _ => return Err(not_found("ref namespace")),
    };
    let n = q.n.unwrap_or(DEFAULT_PAGE).clamp(1, MAX_PAGE);
    let prefix = q
        .prefix
        .as_deref()
        .map(|p| p.trim_matches('/'))
        .filter(|p| !p.is_empty())
        .map(|p| format!("{p}/"));
    let needle =
        q.q.as_deref()
            .filter(|s| !s.is_empty())
            .map(str::to_ascii_lowercase);
    let after = q.after.as_deref().unwrap_or("");
    // Byte-sorted: skip straight to the first candidate (> after, >= prefix).
    let lower = match &prefix {
        Some(p) if p.as_str() > after => p.as_str(),
        _ => after,
    };
    let start = list
        .partition_point(|(name, _)| name.as_str() <= lower && name.as_str() != lower)
        .max(list.partition_point(|(name, _)| name.as_str() <= after));
    let mut refs = Vec::with_capacity(n.min(256));
    let mut more = false;
    for (name, sha) in &list[start..] {
        if let Some(p) = &prefix
            && !name.starts_with(p.as_str())
        {
            break; // sorted: no further names share the prefix
        }
        if let Some(nd) = &needle
            && !name.to_ascii_lowercase().contains(nd.as_str())
        {
            continue;
        }
        if refs.len() == n {
            more = true;
            break;
        }
        refs.push(RefInfo {
            name: name.clone(),
            sha: sha.clone(),
        });
    }
    if wants_sse {
        // Streamed form: one `ref` packet per ref, then `done` (web/API.md).
        let mut items: Vec<Result<bytes::Bytes, std::convert::Infallible>> =
            Vec::with_capacity(refs.len() + 1);
        for r in &refs {
            items.push(Ok(crate::sse::packet("ref", r)));
        }
        items.push(Ok(crate::sse::packet(
            "done",
            &serde_json::json!({ "more": more }),
        )));
        let mut resp = crate::sse::sse_response(futures::stream::iter(items));
        resp.headers_mut()
            .insert(header::CACHE_CONTROL, SWR.parse().unwrap());
        return Ok(resp);
    }
    Ok(json_swr(&RefPage { refs, more }, None).into_response(&headers))
}

// ---- resolve -----------------------------------------------------------------

/// §3: longest branch/tag prefix of `rest` wins (branch beats tag on ties);
/// else the first segment must be a commit-ish; empty -> default branch.
async fn resolve_rest(r: &Repo, rest: &str) -> Result<Resolved, ApiError> {
    let rest = rest.trim_matches('/');
    if rest.is_empty() {
        let (name, sha) = r.index.head().ok_or_else(|| not_found("unborn HEAD"))?;
        return Ok(Resolved {
            ref_name: name,
            sha,
            path: String::new(),
            kind: "branch",
        });
    }
    // Candidate prefixes, longest first.
    let mut cut_points: Vec<usize> = rest.match_indices('/').map(|(i, _)| i).collect();
    cut_points.push(rest.len());
    for &cut in cut_points.iter().rev() {
        let name = &rest[..cut];
        let path = rest[cut..].trim_start_matches('/').to_string();
        if let Some(sha) = r.index.branch(name) {
            return Ok(Resolved {
                ref_name: name.to_string(),
                sha: sha.to_string(),
                path,
                kind: "branch",
            });
        }
        if let Some(sha) = r.index.tag(name) {
            return Ok(Resolved {
                ref_name: name.to_string(),
                sha: sha.to_string(),
                path,
                kind: "tag",
            });
        }
    }
    let (first, path) = match rest.split_once('/') {
        Some((f, p)) => (f, p.to_string()),
        None => (rest, String::new()),
    };
    let sha = rev_parse_commit(r, first).await?;
    Ok(Resolved {
        ref_name: first.to_string(),
        sha,
        path,
        kind: "commit",
    })
}

/// Resolve a single revision name (no path): branch, tag, then git rev-parse.
async fn resolve_name(r: &Repo, name: &str) -> Result<Resolved, ApiError> {
    if (name.is_empty() || name == "HEAD")
        && let Some((n, sha)) = r.index.head()
    {
        return Ok(Resolved {
            ref_name: n,
            sha,
            path: String::new(),
            kind: "branch",
        });
    }
    if let Some(sha) = r.index.branch(name) {
        return Ok(Resolved {
            ref_name: name.into(),
            sha: sha.into(),
            path: String::new(),
            kind: "branch",
        });
    }
    if let Some(sha) = r.index.tag(name) {
        return Ok(Resolved {
            ref_name: name.into(),
            sha: sha.into(),
            path: String::new(),
            kind: "tag",
        });
    }
    let sha = rev_parse_commit(r, name).await?;
    Ok(Resolved {
        ref_name: name.into(),
        sha,
        path: String::new(),
        kind: "commit",
    })
}

/// `rev-parse --verify <rev>^{commit}`: local git when objects are on disk,
/// the pack indexes (unique prefix, tag peel) when served remotely.
async fn rev_parse_commit(r: &Repo, rev: &str) -> Result<String, ApiError> {
    if rev.is_empty() || rev.starts_with('-') {
        return Err(not_found("revision"));
    }
    if !r.objects {
        return Err(not_found(format!("unknown revision {rev}")));
    }
    if let Some(remote) = r.remote() {
        return Ok(remote.resolve_commitish(rev).await?.to_string());
    }
    let out = git(
        &r.local,
        vec![
            "rev-parse".into(),
            "--verify".into(),
            "--quiet".into(),
            "--end-of-options".into(),
            format!("{rev}^{{commit}}"),
        ],
    )
    .await
    .map_err(|_| not_found(format!("unknown revision {rev}")))?;
    let sha = String::from_utf8_lossy(&out).trim().to_string();
    if sha.is_empty() {
        return Err(not_found(format!("unknown revision {rev}")));
    }
    Ok(sha)
}

async fn resolve_root(
    State(st): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((owner, repo_name)): Path<(String, String)>,
) -> Result<Response, ApiError> {
    resolve_impl(&st, &headers, &owner, &repo_name, "").await
}
async fn resolve(
    State(st): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((owner, repo_name, rest)): Path<(String, String, String)>,
) -> Result<Response, ApiError> {
    resolve_impl(&st, &headers, &owner, &repo_name, &rest).await
}
async fn resolve_impl(
    st: &Arc<AppState>,
    headers: &HeaderMap,
    owner: &str,
    repo_name: &str,
    rest: &str,
) -> Result<Response, ApiError> {
    // Branch/tag names resolve from the index; a raw revision falls back to
    // object access. Refs-only first so huge repos still answer for named refs.
    let handle = open(st, headers, owner, repo_name).await?;
    let mut r = view(st, handle, Need::Refs, Reporter::none()).await?;
    let res = match resolve_rest(&r, rest).await {
        Ok(res) => res,
        Err(ApiError::NotFound(_)) if !r.objects => {
            r.need_objects(st).await?;
            resolve_rest(&r, rest).await?
        }
        Err(e) => return Err(e),
    };
    let etag = etag_for(&res.sha);
    Ok(json_swr(&res, Some(&etag)).into_response(headers))
}

/// Split `{ref}/{path}` for tree/blob: a leading full sha is taken verbatim
/// (immutable response); otherwise §3 resolution (SWR + `ETag`).
fn split_addr(rest: &str) -> Option<(Resolved, bool)> {
    let rest = rest.trim_matches('/');
    let (first, path) = match rest.split_once('/') {
        Some((f, p)) => (f, p.trim_matches('/').to_string()),
        None => (rest, String::new()),
    };
    is_full_sha(first).then(|| {
        (
            Resolved {
                ref_name: first.to_string(),
                sha: first.to_string(),
                path,
                kind: "commit",
            },
            true,
        )
    })
}
async fn resolve_addr(r: &Repo, rest: &str) -> Result<(Resolved, bool), ApiError> {
    if let Some(x) = split_addr(rest) {
        return Ok(x);
    }
    Ok((resolve_rest(r, rest).await?, false))
}

// ---- git plumbing ------------------------------------------------------------

fn output_bytes(out: std::process::Output) -> Result<Vec<u8>, ApiError> {
    if out.status.success() {
        Ok(out.stdout)
    } else {
        Err(not_found(
            String::from_utf8_lossy(&out.stderr).trim().to_string(),
        ))
    }
}
async fn git(local: &walgit_git::LocalRepo, args: Vec<String>) -> Result<Vec<u8>, ApiError> {
    let refs: Vec<&str> = args.iter().map(String::as_str).collect();
    let out = local.git(&refs).await.map_err(internal)?;
    output_bytes(out)
}

fn parse_commit_record(record: &str) -> Option<Commit> {
    let mut p = record.split('\0');
    let sha = p.next()?.trim().to_string();
    if sha.is_empty() {
        return None;
    }
    let parents = p.next()?.split_whitespace().map(str::to_string).collect();
    Some(
        Commit {
            sha,
            parents,
            author: p.next()?.to_string(),
            author_email: p.next()?.to_string(),
            author_date: p.next()?.to_string(),
            committer: p.next()?.to_string(),
            commit_date: p.next()?.to_string(),
            subject: p.next()?.to_string(),
            body: String::new(),
            trailers: Vec::new(),
        }
        .with_body(p.next().unwrap_or("")),
    )
}
fn parse_commits(bytes: &[u8]) -> Vec<Commit> {
    String::from_utf8_lossy(bytes)
        .split('\x1e')
        .filter_map(parse_commit_record)
        .collect()
}
fn log_format() -> String {
    "%x1e%H%x00%P%x00%an%x00%ae%x00%aI%x00%cn%x00%cI%x00%s%x00%b%x00".to_string()
}

// ---- tree ----------------------------------------------------------------------

fn tree_key(repo: &str, sha: &str, path: &str) -> String {
    format!("{repo}\0tree\0{sha}\0{path}")
}

async fn tree(
    State(st): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((owner, repo_name, rest)): Path<(String, String, String)>,
) -> Result<Response, ApiError> {
    let key = split_addr(&rest)
        .map(|(res, _)| tree_key(&format!("{owner}/{repo_name}"), &res.sha, &res.path));
    let st2 = st.clone();
    run(
        &st,
        &headers,
        &owner,
        &repo_name,
        Need::Objects,
        key,
        move |r| async move {
            let (res, immutable) = resolve_addr(&r, &rest).await?;
            let key = tree_key(&r.id, &res.sha, &res.path);
            if immutable && let Some(hit) = st2.caches.api_immutable.get(&key) {
                return Ok(Rendered::json(hit, IMMUTABLE, None));
            }
            let body = match r.remote() {
                Some(remote) => render_tree_remote(&remote, &res).await?,
                None => render_tree(&r.local, &res).await?,
            };
            Ok(finish(&st2, &r, immutable, &key, &res.sha, body))
        },
    )
    .await
}

async fn render_tree(
    local: &walgit_git::LocalRepo,
    res: &Resolved,
) -> Result<bytes::Bytes, ApiError> {
    let spec = if res.path.is_empty() {
        format!("{}^{{tree}}", res.sha)
    } else {
        format!("{}:{}", res.sha, res.path)
    };
    let bytes = git(
        local,
        vec!["ls-tree".into(), "-l".into(), "-z".into(), spec],
    )
    .await?;
    let mut entries = Vec::new();
    for item in bytes.split(|b| *b == 0).filter(|x| !x.is_empty()) {
        let Some(tab) = item.iter().position(|b| *b == b'\t') else {
            continue;
        };
        let (meta, name) = item.split_at(tab);
        let name = &name[1..];
        // `ls-tree -l` right-aligns the size with padding spaces.
        let fields: Vec<&[u8]> = meta
            .split(|b| *b == b' ')
            .filter(|f| !f.is_empty())
            .collect();
        if fields.len() < 4 {
            continue;
        }
        let kind = String::from_utf8_lossy(fields[1]).to_string();
        let size = if kind == "blob" {
            String::from_utf8_lossy(fields[3]).parse().unwrap_or(-1)
        } else {
            -1
        };
        entries.push(TreeEntry {
            name: String::from_utf8_lossy(name).to_string(),
            kind,
            mode: String::from_utf8_lossy(fields[0]).to_string(),
            size,
            sha: String::from_utf8_lossy(fields[2]).to_string(),
        });
    }
    sort_entries(&mut entries);
    let commit = newest_commit(local, &res.sha, &res.path)
        .await
        .ok()
        .and_then(|b| parse_commits(&b).into_iter().next());
    let mut readme = None;
    if let Some(e) = readme_entry(&entries)
        && let Ok(content) = git(local, vec!["cat-file".into(), "blob".into(), e.sha.clone()]).await
        && let Ok(s) = String::from_utf8(content)
    {
        readme = Some(Readme {
            name: e.name.clone(),
            contents: s,
        });
    }
    Ok(json_bytes(&Tree {
        ref_name: res.ref_name.clone(),
        sha: res.sha.clone(),
        path: res.path.clone(),
        entries,
        commit,
        readme,
    }))
}

fn sort_entries(entries: &mut [TreeEntry]) {
    entries.sort_by(|a, b| {
        let ad = a.kind == "tree";
        let bd = b.kind == "tree";
        bd.cmp(&ad)
            .then_with(|| a.name.as_bytes().cmp(b.name.as_bytes()))
    });
}
fn readme_entry(entries: &[TreeEntry]) -> Option<&TreeEntry> {
    entries.iter().find(|e| {
        e.kind == "blob"
            && [
                "readme",
                "readme.md",
                "readme.markdown",
                "readme.txt",
                "readme.rst",
            ]
            .contains(&e.name.to_ascii_lowercase().as_str())
    })
}

/// Tree listing straight from the remote pack set: entries from the parsed
/// tree, blob sizes from pack entry headers (concurrent), the newest commit
/// via a bounded walk, README read directly.
async fn render_tree_remote(remote: &Remote, res: &Resolved) -> Result<bytes::Bytes, ApiError> {
    let sha =
        gix_hash::ObjectId::from_hex(res.sha.as_bytes()).map_err(|_| not_found("revision"))?;
    remote.reporter.notice(format!(
        "Reading {} from the WAL pack set",
        if res.path.is_empty() {
            "the root tree".to_string()
        } else {
            format!("tree {}", res.path)
        }
    ));
    let (_c, target, mode) = remote.fault_path(&sha, &res.path).await?;
    if !mode.is_tree() {
        return Err(not_found(format!("'{}' is not a tree", res.path)));
    }
    let raw = remote.tree_entries(&target).await?;
    let total = raw.len();
    let entries: Vec<TreeEntry> = futures::stream::iter(raw)
        .map(|e| async move {
            let kind = match e.mode.kind() {
                gix_object::tree::EntryKind::Tree => "tree",
                gix_object::tree::EntryKind::Commit => "commit",
                _ => "blob",
            };
            let size = if kind == "blob" {
                remote
                    .kind_and_size(&e.oid)
                    .await
                    .ok()
                    .flatten()
                    .map_or(-1, |(_, s)| s as i64)
            } else {
                -1
            };
            TreeEntry {
                name: String::from_utf8_lossy(&e.name).to_string(),
                kind: kind.to_string(),
                mode: format!("{:06o}", e.mode.kind() as u16),
                size,
                sha: e.oid.to_string(),
            }
        })
        .buffer_unordered(32)
        .collect()
        .await;
    if total > 64 {
        remote.reporter.notice(format!("Sized {total} entries"));
    }
    let mut entries = entries;
    sort_entries(&mut entries);
    let commit = remote
        .newest_touching(sha, &res.path)
        .await?
        .map(Commit::from);
    let mut readme = None;
    if let Some(e) = readme_entry(&entries)
        && let Ok(oid) = gix_hash::ObjectId::from_hex(e.sha.as_bytes())
        && let Ok(o) = remote.get(&oid).await
        && let Ok(s) = String::from_utf8(o.data.to_vec())
    {
        readme = Some(Readme {
            name: e.name.clone(),
            contents: s,
        });
    }
    Ok(json_bytes(&Tree {
        ref_name: res.ref_name.clone(),
        sha: res.sha.clone(),
        path: res.path.clone(),
        entries,
        commit,
        readme,
    }))
}

async fn newest_commit(
    local: &walgit_git::LocalRepo,
    sha: &str,
    path: &str,
) -> Result<Vec<u8>, ApiError> {
    let mut a = vec![
        "log".into(),
        "-1".into(),
        format!("--format={}", log_format()),
        sha.into(),
    ];
    if !path.is_empty() {
        a.push("--".into());
        a.push(path.into());
    }
    git(local, a).await
}

// ---- blob ----------------------------------------------------------------------

fn blob_key(repo: &str, sha: &str, path: &str) -> String {
    format!("{repo}\0blob\0{sha}\0{path}")
}

async fn blob(
    State(st): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((owner, repo_name, rest)): Path<(String, String, String)>,
    Query(q): Query<BlobQuery>,
) -> Result<Response, ApiError> {
    let raw = q.raw.is_some();
    // `?raw` is a page navigation (the "Raw" link): never the SSE envelope.
    let mut plain_headers = headers.clone();
    if raw {
        plain_headers.remove(header::ACCEPT);
    }
    let key = if raw {
        None
    } else {
        split_addr(&rest)
            .map(|(res, _)| blob_key(&format!("{owner}/{repo_name}"), &res.sha, &res.path))
    };
    let st2 = st.clone();
    run(
        &st,
        &plain_headers,
        &owner,
        &repo_name,
        Need::Objects,
        key,
        move |r| async move {
            let (res, immutable) = resolve_addr(&r, &rest).await?;
            if res.path.is_empty() {
                return Err(not_found("blob path"));
            }
            let name = res.path.rsplit('/').next().unwrap_or(&res.path).to_string();
            let (size, bytes): (i64, Option<Vec<u8>>) = if let Some(remote) = r.remote() {
                let sha = gix_hash::ObjectId::from_hex(res.sha.as_bytes())
                    .map_err(|_| not_found("revision"))?;
                remote
                    .reporter
                    .notice(format!("Reading {} from the WAL pack set", res.path));
                let (_c, target, mode) = remote.fault_path(&sha, &res.path).await?;
                if !mode.is_blob_or_symlink() {
                    return Err(not_found(format!("'{}' is not a file", res.path)));
                }
                let (_, size) = remote
                    .kind_and_size(&target)
                    .await?
                    .ok_or_else(|| not_found("blob"))?;
                if size as usize > MAX_BLOB {
                    (size as i64, None)
                } else {
                    let o = remote.get(&target).await?;
                    (size as i64, Some(o.data.to_vec()))
                }
            } else {
                let bytes = git(
                    &r.local,
                    vec![
                        "cat-file".into(),
                        "blob".into(),
                        format!("{}:{}", res.sha, res.path),
                    ],
                )
                .await?;
                (bytes.len() as i64, Some(bytes))
            };
            let is_text = size <= MAX_BLOB as i64
                && bytes
                    .as_ref()
                    .is_some_and(|b| !b.contains(&0) && std::str::from_utf8(b).is_ok());
            if raw && is_text {
                let etag = etag_for(&res.sha);
                return Ok(Rendered {
                    body: bytes::Bytes::from(bytes.unwrap_or_default()),
                    content_type: "text/plain; charset=utf-8",
                    cache_control: if immutable { IMMUTABLE } else { SWR },
                    etag: (!immutable).then_some(etag),
                });
            }
            let b = if size > MAX_BLOB as i64 {
                Blob {
                    ref_name: res.ref_name.clone(),
                    sha: res.sha.clone(),
                    path: res.path.clone(),
                    name,
                    size,
                    contents: None,
                    binary: None,
                    too_large: Some(true),
                }
            } else if !is_text {
                Blob {
                    ref_name: res.ref_name.clone(),
                    sha: res.sha.clone(),
                    path: res.path.clone(),
                    name,
                    size,
                    contents: None,
                    binary: Some(true),
                    too_large: None,
                }
            } else {
                Blob {
                    ref_name: res.ref_name.clone(),
                    sha: res.sha.clone(),
                    path: res.path.clone(),
                    name,
                    size,
                    contents: Some(
                        String::from_utf8(bytes.unwrap_or_default()).unwrap_or_default(),
                    ),
                    binary: None,
                    too_large: None,
                }
            };
            let key = blob_key(&r.id, &res.sha, &res.path);
            Ok(finish(&st2, &r, immutable, &key, &res.sha, json_bytes(&b)))
        },
    )
    .await
}

// ---- commits -------------------------------------------------------------------

fn commits_key(repo: &str, sha: &str, path: &str, skip: usize, n: usize) -> String {
    format!("{repo}\0commits\0{sha}\0{path}\0{skip}\0{n}")
}

async fn commits(
    State(st): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((owner, repo_name)): Path<(String, String)>,
    Query(q): Query<CommitQuery>,
) -> Result<Response, ApiError> {
    let reference = q.ref_.clone().unwrap_or_else(|| "HEAD".into());
    let skip = q.skip.unwrap_or(0);
    let n = q.n.unwrap_or(35).clamp(1, 200);
    let path = q.path.clone().unwrap_or_default();
    let key = is_full_sha(&reference)
        .then(|| commits_key(&format!("{owner}/{repo_name}"), &reference, &path, skip, n));
    let st2 = st.clone();
    run(
        &st,
        &headers,
        &owner,
        &repo_name,
        Need::Objects,
        key,
        move |r| async move {
            let (res, immutable) = if is_full_sha(&reference) {
                (
                    Resolved {
                        ref_name: reference.clone(),
                        sha: reference.clone(),
                        path: String::new(),
                        kind: "commit",
                    },
                    true,
                )
            } else {
                (resolve_name(&r, &reference).await?, false)
            };
            let key = commits_key(&r.id, &res.sha, &path, skip, n);
            if immutable && let Some(hit) = st2.caches.api_immutable.get(&key) {
                return Ok(Rendered::json(hit, IMMUTABLE, None));
            }
            let mut cs: Vec<Commit> = if let Some(remote) = r.remote() {
                let start = gix_hash::ObjectId::from_hex(res.sha.as_bytes())
                    .map_err(|_| not_found("revision"))?;
                let label = if path.is_empty() {
                    "Walking history".to_string()
                } else {
                    format!("Walking history of {path}")
                };
                remote.reporter.notice(format!(
                    "{label} from {} (reading commits from the WAL pack set)",
                    &res.sha[..12]
                ));
                let all = remote
                    .walk(
                        start,
                        (!path.is_empty()).then_some(path.as_str()),
                        skip + n + 1,
                        &label,
                    )
                    .await?;
                all.into_iter().skip(skip).map(Commit::from).collect()
            } else {
                let mut a = vec![
                    "log".into(),
                    format!("--format={}", log_format()),
                    "--no-color".into(),
                    format!("--skip={skip}"),
                    format!("-{count}", count = n.saturating_add(1)),
                    res.sha.clone(),
                ];
                if !path.is_empty() {
                    a.extend(["--".into(), path.clone()]);
                }
                parse_commits(&git(&r.local, a).await?)
            };
            let more = cs.len() > n;
            if more {
                cs.truncate(n);
            }
            let body = json_bytes(&Commits {
                ref_name: res.ref_name.clone(),
                sha: res.sha.clone(),
                commits: cs,
                more,
            });
            Ok(finish(&st2, &r, immutable, &key, &res.sha, body))
        },
    )
    .await
}

// ---- commit detail -------------------------------------------------------------

fn commit_key(repo: &str, sha: &str) -> String {
    format!("{repo}\0commit\0{sha}")
}

async fn commit_detail(
    State(st): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((owner, repo_name, rev)): Path<(String, String, String)>,
) -> Result<Response, ApiError> {
    let key = is_full_sha(&rev).then(|| commit_key(&format!("{owner}/{repo_name}"), &rev));
    let st2 = st.clone();
    run(
        &st,
        &headers,
        &owner,
        &repo_name,
        Need::Objects,
        key,
        move |r| async move {
            let immutable = is_full_sha(&rev);
            let sha = if immutable {
                rev.clone()
            } else {
                resolve_name(&r, &rev).await?.sha
            };
            let key = commit_key(&r.id, &sha);
            if immutable && let Some(hit) = st2.caches.api_immutable.get(&key) {
                return Ok(Rendered::json(hit, IMMUTABLE, None));
            }
            if let Some(remote) = r.remote() {
                // Fault the commit, its first parent and every object the diff
                // touches into the loose store; `git show` below then runs as-is.
                let oid = gix_hash::ObjectId::from_hex(sha.as_bytes())
                    .map_err(|_| not_found("commit"))?;
                remote.reporter.notice(format!(
                    "Reading commit {} from the WAL pack set",
                    &sha[..12]
                ));
                remote.fault_commit_diff(&oid).await?;
            }
            let commit = parse_commits(
                // `--diff-merges=off`: plain `show -s` on a merge still sets up
                // the combined diff and reads the other parents' subtrees (on a
                // remotely served repo those are exactly the objects we did not
                // fault).
                &git(
                    &r.local,
                    vec![
                        "show".into(),
                        "-s".into(),
                        "--diff-merges=off".into(),
                        format!("--format={}", log_format()),
                        sha.clone(),
                    ],
                )
                .await?,
            )
            .into_iter()
            .next()
            .ok_or_else(|| not_found("commit"))?;
            let stat_out = git(
                &r.local,
                vec![
                    "show".into(),
                    "--format=".into(),
                    "--numstat".into(),
                    "-M".into(),
                    "--diff-merges=first-parent".into(),
                    "--root".into(),
                    sha.clone(),
                ],
            )
            .await?;
            let stats = parse_stats(&stat_out);
            let patch = String::from_utf8_lossy(
                &git(
                    &r.local,
                    vec![
                        "show".into(),
                        "--format=".into(),
                        "-p".into(),
                        "-M".into(),
                        "--no-color".into(),
                        "--no-ext-diff".into(),
                        "--diff-merges=first-parent".into(),
                        "--root".into(),
                        sha.clone(),
                    ],
                )
                .await?,
            )
            .into_owned();
            let body = json_bytes(&CommitDetail {
                commit,
                stats,
                patch,
            });
            Ok(finish(&st2, &r, immutable, &key, &sha, body))
        },
    )
    .await
}
fn parse_stats(bytes: &[u8]) -> Vec<Stat> {
    String::from_utf8_lossy(bytes)
        .lines()
        .filter_map(|line| {
            let f: Vec<&str> = line.split('\t').collect();
            if f.len() < 3 || (!f[0].chars().all(|c| c.is_ascii_digit()) && f[0] != "-") {
                return None;
            }
            let path = normalize_rename(f[2]);
            Some(Stat {
                path,
                additions: if f[0] == "-" {
                    -1
                } else {
                    f[0].parse().unwrap_or(-1)
                },
                deletions: if f[1] == "-" {
                    -1
                } else {
                    f[1].parse().unwrap_or(-1)
                },
            })
        })
        .collect()
}
/// `git --numstat -M` prints renames as `old => new` or `prefix/{old => new}/suffix`;
/// return the new path.
fn normalize_rename(s: &str) -> String {
    if let (Some(open), Some(close)) = (s.find('{'), s.rfind('}'))
        && open < close
    {
        let inner = &s[open + 1..close];
        if let Some((_, new)) = inner.split_once(" => ") {
            let mut out = String::with_capacity(s.len());
            out.push_str(&s[..open]);
            out.push_str(new);
            out.push_str(&s[close + 1..]);
            return out.replace("//", "/");
        }
    }
    if let Some((_, new)) = s.split_once(" => ") {
        return new.to_string();
    }
    s.to_string()
}

#[cfg(test)]
mod tests {
    #[test]
    fn rename_paths() {
        assert_eq!(
            super::normalize_rename("src/{main.rs => app.rs}"),
            "src/app.rs"
        );
        assert_eq!(super::normalize_rename("{a => b}/x.rs"), "b/x.rs");
        assert_eq!(super::normalize_rename("a/{ => sub}/x.rs"), "a/sub/x.rs");
        assert_eq!(super::normalize_rename("old.rs => new.rs"), "new.rs");
        assert_eq!(super::normalize_rename("plain.rs"), "plain.rs");
    }
}
