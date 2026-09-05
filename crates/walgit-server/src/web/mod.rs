pub mod api;
pub mod login;
pub mod objects;
pub mod trailers;
pub mod ui;
pub mod v1;

use std::sync::Arc;

use axum::{
    body::Body,
    extract::{Request, State},
    http::{StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Redirect, Response},
};

use crate::AppState;

/// Send a browser on `localhost` / `127.0.0.1` to `walgit.localhost` (same port).
/// Keep `/_auth/*` on the literal loopback host so an issuer's registered callback remains exact.
/// Git, curl, and the installer are not browsers and are not redirected.
pub async fn canonical_browser_host(
    State(st): State<Arc<AppState>>,
    req: Request<Body>,
    next: Next,
) -> Response {
    let path = req.uri().path();
    let skip = path.starts_with("/_auth/")
        || path == "/healthz"
        || path == "/readyz"
        || path.starts_with("/services/public");
    let browser = is_browser(req.headers());
    let get = req.method() == axum::http::Method::GET || req.method() == axum::http::Method::HEAD;
    if get
        && browser
        && !skip
        && let Some(dest) = walgit_localhost_host(
            req.headers()
                .get(header::HOST)
                .and_then(|v| v.to_str().ok()),
        )
    {
        let scheme = if st.cfg.tls_enabled() {
            "https"
        } else {
            "http"
        };
        let pq = req
            .uri()
            .path_and_query()
            .map_or("/", http::uri::PathAndQuery::as_str);
        let loc = format!("{scheme}://{dest}{pq}");
        return (StatusCode::FOUND, [(header::LOCATION, loc)]).into_response();
    }
    next.run(req).await
}

fn is_browser(headers: &axum::http::HeaderMap) -> bool {
    let accept = headers
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if accept.contains("text/html") {
        return true;
    }
    if headers.get("sec-fetch-dest").and_then(|v| v.to_str().ok()) == Some("document") {
        return true;
    }
    headers
        .get(header::USER_AGENT)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|ua| ua.contains("Mozilla"))
}

fn walgit_localhost_host(host: Option<&str>) -> Option<String> {
    let host = host?.trim();
    let (name, port) = match host.rsplit_once(':') {
        Some((n, p)) if !n.is_empty() && p.bytes().all(|b| b.is_ascii_digit()) => (n, Some(p)),
        _ => (host, None),
    };
    let name = name.trim_matches(|c| c == '[' || c == ']');
    if !matches!(name, "localhost" | "127.0.0.1" | "::1") {
        return None;
    }
    Some(match port {
        Some(p) => format!("walgit.localhost:{p}"),
        None => "walgit.localhost".into(),
    })
}

/// Authentication gate for everything that is not a git endpoint or the login
/// flow itself: the SPA shell and assets, the installer and credential helper,
/// health and metrics. A browser without identity is sent through
/// `/_auth/login?next=…` when app-owned sign-in is configured; otherwise
/// 401 + `WWW-Authenticate: Bearer`.
pub async fn require_auth(
    State(st): State<Arc<AppState>>,
    req: Request<Body>,
    next: Next,
) -> Response {
    match st.auth.require_read(req.headers()).await {
        Ok(_) => next.run(req).await,
        Err(e) => {
            let accepts_html = req
                .headers()
                .get(header::ACCEPT)
                .and_then(|v| v.to_str().ok())
                .is_some_and(|a| a.contains("text/html"));
            let has_bearer = req.headers().contains_key(header::AUTHORIZATION);
            let is_get = req.method() == axum::http::Method::GET;
            if is_get && accepts_html && !has_bearer && st.auth.browser_login_enabled() {
                let next_url = req
                    .uri()
                    .path_and_query()
                    .map_or_else(|| "/".to_string(), |pq| pq.as_str().to_string());
                let q = url_encode(&next_url);
                return Redirect::temporary(&format!("/_auth/login?next={q}")).into_response();
            }
            let status = match e {
                crate::auth::AuthError::Forbidden => StatusCode::FORBIDDEN,
                crate::auth::AuthError::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
                _ => StatusCode::UNAUTHORIZED,
            };
            let why = match e {
                crate::auth::AuthError::Forbidden => {
                    "your account is not in an allowed domain".to_string()
                }
                crate::auth::AuthError::Unavailable => {
                    "the token verifier is temporarily unavailable; retry".to_string()
                }
                _ => match crate::setup::recipes(
                    &st.cfg,
                    &crate::smart::request_base_url(&st, req.headers()),
                    None,
                )
                .token_url
                {
                    Some(u) => format!(
                        "sign in, or send `Authorization: Bearer <token>` (create one at {u})"
                    ),
                    None => "send `Authorization: Bearer <token>`".to_string(),
                },
            };
            let mut resp =
                (status, format!("walgit: authentication required: {why}\n")).into_response();
            if status == StatusCode::UNAUTHORIZED {
                resp.headers_mut().insert(
                    header::WWW_AUTHENTICATE,
                    "Bearer realm=\"walgit\"".parse().unwrap(),
                );
            }
            resp
        }
    }
}

pub(crate) fn url_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char);
            }
            _ => {
                let _ = std::fmt::Write::write_fmt(&mut out, format_args!("%{b:02X}"));
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::walgit_localhost_host;

    #[test]
    fn localhost_becomes_walgit_localhost_same_port() {
        assert_eq!(
            walgit_localhost_host(Some("localhost:8080")).as_deref(),
            Some("walgit.localhost:8080"),
        );
        assert_eq!(
            walgit_localhost_host(Some("127.0.0.1:8080")).as_deref(),
            Some("walgit.localhost:8080"),
        );
        assert_eq!(walgit_localhost_host(Some("walgit.localhost:8080")), None);
        assert_eq!(walgit_localhost_host(Some("git.example.com")), None);
    }
}
