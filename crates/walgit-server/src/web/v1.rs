//! The non-repo programmatic surface (`/api/v1`, D20/D27, `web/API.md`) and
//! the repo admin routes registered under both lanes of
//! [`crate::web::api::REPO_API_BASES`].
//!
//! Lanes (D27) are a segment *after* the repository prefix:
//! * `/{o}/{r}/api/…` — a bearer token or the same-origin session cookie for the same-origin bundled UI;
//! * `/{o}/{r}/api-browser/…` — the browser lane for other origins (`credentials:
//!   "include"`), authenticated by the same session cookie (`SameSite=None`).
//! Same handlers; lanes differ by credential handling and CORS, never by a
//! rewrite. Non-repo: `/api/v1` (discovery), `/api/v1/me`, `/api/v1/authenticate`
//! (+ the `/api-browser/v1/me|authenticate` pair the SDK's popup uses),
//! `/api/v1/owners*`. The SDK (`repos.js`, `web/sdk/`) maps this one to one.

use std::sync::Arc;

use axum::{
    Router,
    body::Body,
    extract::{Path, Request, State},
    http::{HeaderMap, HeaderValue, Method, StatusCode, header},
    middleware::Next,
    response::{Html, IntoResponse, Response},
    routing::{get, post},
};
use serde::Serialize;

use crate::repo::RepoRoute;
use crate::web::api::{Need, RefInfo, etag_for, json_swr, run};
use crate::{AppState, error::ApiError};

/// Canonical prefix of the versioned API.
pub const API_V1: &str = "/api/v1";
/// Browser-lane segment (D27): `/{o}/{r}/api-browser/*` for repo routes and
/// `/api-browser/v1/me|authenticate` for the SDK's sign-in popup. Never stripped or
/// rewritten — the lane differs by credential handling and CORS only.
pub const API_BROWSER: &str = "/api-browser";

pub fn router(state: Arc<AppState>) -> Router {
    let mut r = Router::new()
        .route(API_V1, get(discovery))
        .route(&format!("{API_V1}/"), get(discovery))
        .route(&format!("{API_V1}/me"), get(me))
        .route(&format!("{API_V1}/authenticate"), get(authenticate))
        .route(&format!("{API_BROWSER}/v1/me"), get(me))
        .route(&format!("{API_BROWSER}/v1/authenticate"), get(authenticate))
        .route(&format!("{API_V1}/owners"), get(crate::web::api::owners))
        .route(
            &format!("{API_V1}/owners/{{owner}}/repos"),
            get(crate::web::api::owner_repos),
        );
    // Repo admin under both lanes: summary/create/delete, policy, settings.
    for base in crate::web::api::REPO_API_BASES {
        r = r
            .route(base, get(repo_summary).put(repo_admin).delete(repo_admin))
            .route(
                &format!("{base}/policy"),
                get(repo_admin).put(repo_admin).delete(repo_admin),
            )
            .route(&format!("{base}/policy/{{sub}}"), post(repo_admin_sub))
            .route(
                &format!("{base}/settings"),
                get(repo_admin).put(repo_admin).delete(repo_admin),
            )
            .route(
                &format!("{base}/settings/{{sub}}"),
                get(repo_admin_sub).post(repo_admin_sub),
            );
    }
    r.with_state(state)
}

// ---- CORS (browser lane from other origins) -----------------------------------

fn origin_allowed(cfg: &walgit_config::Config, origin: &str) -> bool {
    cfg.server.cors_origins.iter().any(|pat| {
        if let Some((scheme, host)) = pat.split_once("://*.") {
            origin
                .strip_prefix(scheme)
                .and_then(|o| o.strip_prefix("://"))
                .is_some_and(|o| {
                    o.ends_with(host)
                        && o.len() > host.len()
                        && o.as_bytes()[o.len() - host.len() - 1] == b'.'
                        && !o.contains('/')
                })
        } else {
            pat.eq_ignore_ascii_case(origin)
        }
    })
}

const CORS_HEADERS: &str = "Authorization, Content-Type, Accept, If-None-Match, X-Requested-With";
const CORS_METHODS: &str = "GET, HEAD, POST, PUT, DELETE, OPTIONS";
const CORS_EXPOSE: &str = "ETag, Cache-Control, Content-Type, Location";

/// CORS for `/api*`: only origins in `server.cors_origins` get credentials;
/// preflights are answered here (unauthenticated — a preflight carries no
/// credentials by definition); a state-changing request that names an
/// unapproved foreign origin is refused as a cross-origin request guard.
pub async fn cors(State(st): State<Arc<AppState>>, req: Request<Body>, next: Next) -> Response {
    let path = req.uri().path();
    let is_api = is_cors_api_path(path);
    let origin = req
        .headers()
        .get(header::ORIGIN)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let Some(origin) = origin.filter(|_| is_api) else {
        return next.run(req).await;
    };
    let allowed = origin_allowed(&st.cfg, &origin);
    if req.method() == Method::OPTIONS {
        let mut r = StatusCode::NO_CONTENT.into_response();
        if allowed {
            cors_headers(r.headers_mut(), &origin);
            r.headers_mut().insert(
                "access-control-allow-methods",
                HeaderValue::from_static(CORS_METHODS),
            );
            r.headers_mut().insert(
                "access-control-allow-headers",
                HeaderValue::from_static(CORS_HEADERS),
            );
            r.headers_mut()
                .insert("access-control-max-age", HeaderValue::from_static("600"));
        }
        return r;
    }
    let same_origin = req
        .headers()
        .get(header::HOST)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|h| {
            origin
                .strip_prefix("https://")
                .or_else(|| origin.strip_prefix("http://"))
                == Some(h)
        });
    if !allowed && !same_origin && !matches!(*req.method(), Method::GET | Method::HEAD) {
        return (
            StatusCode::FORBIDDEN,
            "walgit: cross-origin request from an origin that is not in server.cors_origins\n",
        )
            .into_response();
    }
    let mut resp = next.run(req).await;
    if allowed {
        cors_headers(resp.headers_mut(), &origin);
    }
    resp
}

/// Where CORS applies (D27): the non-repo `/api*` and `/api-browser*` roots and
/// the repo lanes `/{o}/{r}/api[-browser](/…)?`. Never the SPA pages, git, auth.
pub fn is_cors_api_path(path: &str) -> bool {
    path == "/api"
        || path.starts_with("/api/")
        || path == API_BROWSER
        || path.starts_with("/api-browser/")
        || is_repo_api_path(path)
}

/// `/{o}/{r}/api[-browser](/…)?` — the repo-scoped lanes (D27).
pub fn is_repo_api_path(path: &str) -> bool {
    let mut it = path.trim_start_matches('/').splitn(4, '/');
    let (Some(o), Some(r), Some(seg)) = (it.next(), it.next(), it.next()) else {
        return false;
    };
    !o.is_empty() && !r.is_empty() && (seg == "api" || seg == "api-browser")
}

fn cors_headers(h: &mut HeaderMap, origin: &str) {
    if let Ok(v) = HeaderValue::from_str(origin) {
        h.insert("access-control-allow-origin", v);
    }
    h.insert(
        "access-control-allow-credentials",
        HeaderValue::from_static("true"),
    );
    h.insert(
        "access-control-expose-headers",
        HeaderValue::from_static(CORS_EXPOSE),
    );
    h.append(header::VARY, HeaderValue::from_static("Origin"));
}

// ---- endpoints ---------------------------------------------------------------

#[derive(Serialize)]
struct Discovery<'a> {
    name: &'a str,
    version: u32,
    base: String,
    browser_base: String,
    sdk: String,
    docs: &'a str,
    auth: DiscoveryAuth<'a>,
    endpoints: Vec<&'a str>,
}
#[derive(Serialize)]
struct DiscoveryAuth<'a> {
    bearer: String,
    /// Where the human sign-in / install recipes live (`setup::Recipes`).
    setup: String,
    browser: &'a str,
    authenticate: String,
}

/// `GET /api/v1` — what this is and where the pieces are.
async fn discovery(State(st): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    let base_url = crate::smart::request_base_url(&st, &headers);
    let body = Discovery {
        name: "walgit",
        version: 1,
        base: format!("{base_url}{API_V1}"),
        // D27: non-repo browser lane (popup). Repo JSON is /{o}/{r}/api-browser/*.
        browser_base: format!("{base_url}{API_BROWSER}/v1"),
        sdk: format!("{base_url}/repos.js"),
        docs: "https://git.example.com/api",
        auth: DiscoveryAuth {
            bearer: "Authorization: Bearer <token>  (an access token from /_auth/tokens, a static token, or an ID token)".to_string(),
            setup: format!("{base_url}/services/setup.json"),
            browser: "fetch(`/{owner}/{repo}/api-browser/…`, {credentials: \"include\"}); on 401 open `authenticate` in a popup and retry",
            authenticate: format!("{base_url}{API_BROWSER}/v1/authenticate"),
        },
        endpoints: vec![
            "GET  /api/v1/me",
            "GET  /api/v1/owners",
            "GET  /api/v1/owners/{owner}/repos",
            "GET  /api/v1/authenticate   (also /api-browser/v1/me|authenticate for the browser lane)",
            "-- repository routes live under the repository (D27): /{owner}/{repo}/api/… (bearer/session) and /{owner}/{repo}/api-browser/… (browser lane) --",
            "GET|PUT|DELETE /{owner}/{repo}/api",
            "GET  /{owner}/{repo}/api/refs",
            "GET  /{owner}/{repo}/api/refs/{branches|tags}?prefix&q&after&n",
            "GET  /{owner}/{repo}/api/resolve/{rev}[/{path}]",
            "GET  /{owner}/{repo}/api/tree/{rev}[/{path}]",
            "GET  /{owner}/{repo}/api/blob/{rev}/{path}[?raw]",
            "GET  /{owner}/{repo}/api/commits?ref&path&skip&n",
            "GET  /{owner}/{repo}/api/commit/{sha}",
            "GET  /{owner}/{repo}/api/commit/{sha}/merge-queue",
            "GET  /{owner}/{repo}/api/overview",
            "GET  /{owner}/{repo}/api/tasks[/{id}]",
            "GET  /{owner}/{repo}/api/ops",
            "POST /{owner}/{repo}/api/ops/{op}",
            "GET  /{owner}/{repo}/api/snapshot/{seq}   (a WAL rewind materialized by ops/snapshot)",
            "GET|PUT|DELETE /{owner}/{repo}/api/policy",
            "POST /{owner}/{repo}/api/policy/validate | dry-run?last=N",
            "GET|PUT|DELETE /{owner}/{repo}/api/settings",
            "GET  /{owner}/{repo}/api/settings/effective | history | describe",
            "POST /{owner}/{repo}/api/settings/validate",
        ],
    };
    let mut r = axum::Json(body).into_response();
    r.headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    r
}

/// `GET /api/v1/me` — the caller, or 401. Never cached.
async fn me(State(st): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    match st.auth.authenticate(&headers).await {
        Ok(p) if !p.anonymous || st.auth.anonymous_read_allowed() => {
            let mut r = axum::Json(serde_json::json!({
                "principal": p.name,
                "write": p.write,
                "anonymous": p.anonymous,
            }))
            .into_response();
            r.headers_mut()
                .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
            r
        }
        Ok(_) => ApiError::Unauthorized.into_response(),
        Err(e) => crate::web::api::auth_err(e).into_response(),
    }
}

/// `GET /api/v1/authenticate`: the popup landing page of the browser lane.
/// `require_auth` sends an unauthenticated browser through sign-in first; this
/// authenticated page then tells its opener and closes. The SDK opens it when a
/// browser-lane call answers 401.
async fn authenticate(State(st): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    match st.auth.require_read(&headers).await {
        Ok(p) => {
            let page = AUTHENTICATE_HTML.replace("{{principal}}", &html_escape(&p.name));
            let mut r = Html(page).into_response();
            r.headers_mut()
                .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
            r
        }
        Err(e) => crate::web::api::auth_err(e).into_response(),
    }
}

const AUTHENTICATE_HTML: &str = r#"<!doctype html>
<meta charset="utf-8">
<title>walgit — signed in</title>
<style>body{font:15px system-ui,sans-serif;margin:3rem auto;max-width:28rem;text-align:center;color:#222}</style>
<p>Signed in to walgit as <b>{{principal}}</b>.</p>
<p id="m">You can close this window.</p>
<script>
(function () {
  var msg = { type: "repos:authenticated", principal: "{{principal}}" };
  try { if (window.opener) { window.opener.postMessage(msg, "*"); } } catch (e) {}
  try { if (window.parent && window.parent !== window) { window.parent.postMessage(msg, "*"); } } catch (e) {}
  // Only a window we opened ourselves (same site or cross-site via the SDK) is closed.
  if (window.opener) { setTimeout(function () { window.close(); }, 150); }
})();
</script>
"#;

fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

#[derive(Serialize)]
struct RepoSummary {
    owner: String,
    name: String,
    full_name: String,
    head: Option<RefInfo>,
    branches: usize,
    tags: usize,
    clone_url: String,
    html_url: String,
    api_url: String,
}

/// `GET /{owner}/{repo}/api[-browser]` — one cheap, ref-level summary (SWR +
/// ETag on the head sha). Counts are O(1) from the ref index.
async fn repo_summary(
    State(st): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((owner, name)): Path<(String, String)>,
) -> Result<Response, ApiError> {
    let base_url = crate::smart::request_base_url(&st, &headers);
    let (o, n) = (owner.clone(), name.clone());
    run(
        &st,
        &headers,
        &owner,
        &name,
        Need::Refs,
        None,
        move |r| async move {
            let head = r.index.head().map(|(name, sha)| RefInfo { name, sha });
            let etag = etag_for(head.as_ref().map(|h| h.sha.as_str()).unwrap_or("unborn"));
            let full = format!("{o}/{n}");
            Ok(json_swr(
                &RepoSummary {
                    owner: o,
                    name: n,
                    full_name: full.clone(),
                    head,
                    branches: r.index.branches.len(),
                    tags: r.index.tags.len(),
                    clone_url: format!("{base_url}/{full}.git"),
                    html_url: format!("{base_url}/{full}"),
                    api_url: format!("{base_url}/{full}/api"),
                },
                Some(&etag),
            ))
        },
    )
    .await
}

/// `GET|POST /{o}/{r}/api[-browser]/settings/{sub}` (effective | history | validate) and `POST …/policy/{sub}`.
async fn repo_admin_sub(
    State(st): State<Arc<AppState>>,
    Path((owner, name, _sub)): Path<(String, String, String)>,
    req: Request<Body>,
) -> Response {
    repo_admin(State(st), Path((owner, name)), req).await
}

/// `PUT|DELETE /{o}/{r}/api[-browser]` and `GET|PUT|DELETE …/policy|settings`: the
/// same handlers as the repo root (`crate::dispatch_route`).
async fn repo_admin(
    State(st): State<Arc<AppState>>,
    Path((owner, name)): Path<(String, String)>,
    req: Request<Body>,
) -> Response {
    let Ok(id) = walgit_git::RepoId::new(&owner, &name) else {
        return ApiError::NotFound("repository".into()).into_response();
    };
    let path = req.uri().path();
    // Everything after `/{o}/{r}/api` or `/{o}/{r}/api-browser`: "", "policy",
    // "settings", "settings/history", …
    let sub = {
        let mut sub = String::new();
        for lane in ["api-browser", "api"] {
            let marker = format!("/{owner}/{name}/{lane}");
            if let Some(i) = path.find(&marker) {
                sub = path[i + marker.len()..].trim_start_matches('/').to_string();
                break;
            }
        }
        sub
    };
    let route = RepoRoute {
        id,
        subpath: sub,
        had_git_suffix: false,
    };
    let query = req.uri().query().unwrap_or("").to_string();
    let method = req.method().clone();
    let headers = req.headers().clone();
    let peer = crate::request_peer(&req);
    crate::dispatch_route(&st, &route, method, headers, query, req.into_body(), peer).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cors_covers_prefix_form_and_v1() {
        assert!(is_cors_api_path("/api/v1/me"));
        assert!(is_cors_api_path("/api-browser/v1/me"));
        assert!(is_cors_api_path("/acme/monorepo/api"));
        assert!(is_cors_api_path("/acme/monorepo/api/refs"));
        assert!(is_cors_api_path("/acme/monorepo/api-browser/refs"));
        assert!(is_cors_api_path("/o/r/api/settings/effective"));
        assert!(!is_cors_api_path("/acme/monorepo/settings")); // SPA
        assert!(!is_cors_api_path("/acme/monorepo.git/info/refs"));
        assert!(!is_cors_api_path("/_auth/check"));
        assert!(!is_cors_api_path("/services/api/owners"));
    }
}
