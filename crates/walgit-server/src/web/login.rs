//! Browser sign-in: the `OpenID` Connect authorization-code flow against
//! `server.auth.issuer`, done by walgit itself. `GET /_auth/login?next=/p`
//! redirects to the issuer's `authorization_endpoint` (from discovery),
//! `GET /_auth/callback` exchanges the code at the `token_endpoint`, verifies the
//! returned ID token (signature, issuer, audience = our client, domain policy)
//! and sets the HMAC-signed session cookie; `/_auth/logout` clears it;
//! `/_auth/me` reports the principal.
//!
//! `/_auth/tokens` is where git gets its credential: a signed-in browser `POST`s
//! there and receives a walgit access token (`wgt_…`, `server.auth.access_token_ttl`)
//! to paste into the credential helper; `GET` renders the small page that does it.
//! Tokens are stateless — rotating `session_secret` revokes them all.

use std::sync::Arc;

use axum::{
    Router,
    extract::{Query, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Redirect, Response},
    routing::get,
};

use crate::{AppState, auth::SESSION_COOKIE};

const STATE_TTL_SECS: u64 = 600;

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/_auth/login", get(login))
        .route("/_auth/callback", get(callback))
        .route("/_auth/claimed", get(claimed))
        .route("/_auth/logout", get(logout))
        .route("/_auth/me", get(me))
        .route("/_auth/check", get(check))
        .route("/_auth/tokens", get(tokens_page).post(mint_token))
        .with_state(state)
}

#[derive(serde::Deserialize, Default)]
struct LoginQuery {
    next: Option<String>,
}
#[derive(serde::Deserialize, Default)]
struct CallbackQuery {
    code: Option<String>,
    state: Option<String>,
    error: Option<String>,
}

fn safe_next(next: Option<String>) -> String {
    match next {
        Some(n) if n.starts_with('/') && !n.starts_with("//") => n,
        _ => "/".to_string(),
    }
}

fn request_port(st: &AppState, headers: &HeaderMap) -> u16 {
    headers
        .get(header::HOST)
        .and_then(|v| v.to_str().ok())
        .and_then(|h| h.rsplit_once(':'))
        .and_then(|(_, p)| p.parse().ok())
        .unwrap_or_else(|| st.cfg.server.listen.port())
}

/// The public origin (`server.public_url`, else forwarded host) is a loopback development
/// origin (`walgit.localhost`). Only there does the OAuth client allow a `localhost`
/// redirect and does the callback hop between the two hostnames.
fn loopback_origin(st: &AppState, headers: &HeaderMap) -> bool {
    let base = crate::smart::request_base_url(st, headers);
    let host = base.split("://").nth(1).unwrap_or(&base);
    let host = host.split('/').next().unwrap_or(host);
    let host = host.rsplit_once(':').map_or(host, |(h, _)| h);
    host == "walgit.localhost" || host == "localhost" || host == "127.0.0.1" || host == "[::1]"
}

/// Where the issuer sends the code. A public deployment (`public_url = https://git.example.com`,
/// an OAuth client with that exact redirect): `<origin>/_auth/callback`. Local development:
/// most issuers allow only literal `localhost` / `127.0.0.1` as a loopback redirect, the
/// browser origin is `walgit.localhost`, and `callback` hops back to it.
fn redirect_uri(st: &AppState, headers: &HeaderMap) -> String {
    if !loopback_origin(st, headers) {
        return format!(
            "{}/_auth/callback",
            crate::smart::request_base_url(st, headers)
        );
    }
    let scheme = if st.cfg.tls_enabled() {
        "https"
    } else {
        "http"
    };
    let port = request_port(st, headers);
    if port == 443 && scheme == "https" || port == 80 && scheme == "http" {
        format!("{scheme}://localhost/_auth/callback")
    } else {
        format!("{scheme}://localhost:{port}/_auth/callback")
    }
}

fn walgit_origin(st: &AppState, headers: &HeaderMap) -> String {
    let scheme = if st.cfg.tls_enabled() {
        "https"
    } else {
        "http"
    };
    let port = request_port(st, headers);
    if port == 443 && scheme == "https" || port == 80 && scheme == "http" {
        format!("{scheme}://walgit.localhost")
    } else {
        format!("{scheme}://walgit.localhost:{port}")
    }
}

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

fn urlencode(s: &str) -> String {
    let mut out = String::with_capacity(s.len() * 3);
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

async fn login(
    State(st): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(q): Query<LoginQuery>,
) -> Response {
    if !st.auth.browser_login_enabled() {
        return (
            StatusCode::UNAUTHORIZED,
            "not signed in: browser sign-in is not configured on this server (server.auth.mode = \"oidc\" \
             with oauth_client_id/oauth_client_secret/session_secret). API and git clients authenticate with \
             `Authorization: Bearer <token>`.",
        )
            .into_response();
    }
    let Ok(disco) = st.auth.discovery().await else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "identity provider unavailable (OIDC discovery failed)",
        )
            .into_response();
    };
    let (client_id, _) = st.auth.oauth_client().unwrap();
    let next = safe_next(q.next);
    let nonce: u64 = rand::random();
    let payload = format!("{}\n{nonce:x}\n{next}", now() + STATE_TTL_SECS);
    let Some(state) = st.auth.sign(payload.as_bytes()) else {
        return (StatusCode::NOT_IMPLEMENTED, "session secret missing").into_response();
    };
    let sep = if disco.authorization_endpoint.contains('?') {
        '&'
    } else {
        '?'
    };
    let mut url = format!(
        "{}{sep}client_id={}&redirect_uri={}&response_type=code&scope={}&state={}&prompt=select_account",
        disco.authorization_endpoint,
        urlencode(client_id),
        urlencode(&redirect_uri(&st, &headers)),
        urlencode("openid email"),
        urlencode(&state),
    );
    // Google honours `hd` as a domain hint on its account chooser; other issuers ignore it.
    if let Some(hd) = st.cfg.server.auth.allowed_domains.first() {
        {
            let _ = std::fmt::Write::write_fmt(&mut url, format_args!("&hd={}", urlencode(hd)));
        };
    }
    let mut r = Redirect::to(&url).into_response();
    r.headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    r
}

async fn exchange_code(
    token_endpoint: &str,
    form: &[(&str, &str); 5],
) -> Result<reqwest::Response, reqwest::Error> {
    let client = reqwest::Client::builder()
        .connect_timeout(std::time::Duration::from_secs(5))
        .timeout(std::time::Duration::from_secs(15))
        .build()
        .expect("reqwest client");
    let mut last = None;
    for attempt in 1u8..=2 {
        match client.post(token_endpoint).form(form).send().await {
            Ok(r) => return Ok(r),
            Err(e) if attempt < 2 && (e.is_connect() || e.is_timeout()) => {
                tracing::warn!(attempt, error = %e, "oauth token exchange retrying");
                last = Some(e);
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
            Err(e) => return Err(e),
        }
    }
    Err(last.expect("retry left an error"))
}

#[derive(serde::Deserialize)]
struct TokenResponse {
    id_token: Option<String>,
    error: Option<String>,
    error_description: Option<String>,
}

async fn callback(
    State(st): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(q): Query<CallbackQuery>,
) -> Response {
    if let Some(e) = q.error {
        return (StatusCode::UNAUTHORIZED, format!("sign-in failed: {e}")).into_response();
    }
    let (Some(code), Some(state)) = (q.code, q.state) else {
        return (StatusCode::BAD_REQUEST, "missing code/state").into_response();
    };
    let Some(payload) = st.auth.verify_signed(&state) else {
        return (StatusCode::BAD_REQUEST, "invalid state").into_response();
    };
    let payload = String::from_utf8_lossy(&payload).to_string();
    let mut parts = payload.splitn(3, '\n');
    let exp: u64 = parts.next().and_then(|e| e.parse().ok()).unwrap_or(0);
    let _nonce = parts.next();
    let next = safe_next(parts.next().map(str::to_string));
    if now() > exp {
        return (StatusCode::BAD_REQUEST, "sign-in expired; try again").into_response();
    }
    let Some((client_id, client_secret)) = st.auth.oauth_client() else {
        return (
            StatusCode::NOT_IMPLEMENTED,
            "browser sign-in not configured",
        )
            .into_response();
    };
    let form = [
        ("code", code.as_str()),
        ("client_id", client_id),
        ("client_secret", client_secret),
        ("redirect_uri", &redirect_uri(&st, &headers)),
        ("grant_type", "authorization_code"),
    ];
    let Ok(disco) = st.auth.discovery().await else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "identity provider unavailable",
        )
            .into_response();
    };
    // The code is single-use: only retry when we never heard back (connect/timeout).
    let resp = match exchange_code(&disco.token_endpoint, &form).await {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(error = %e, connect = e.is_connect(), timeout = e.is_timeout(), "oauth token exchange failed");
            return (StatusCode::BAD_GATEWAY, "token exchange failed").into_response();
        }
    };
    let tok: TokenResponse = match resp.json().await {
        Ok(t) => t,
        Err(e) => {
            tracing::warn!(error = %e, "oauth token response unreadable");
            return (StatusCode::BAD_GATEWAY, "token exchange failed").into_response();
        }
    };
    let Some(id_token) = tok.id_token else {
        tracing::warn!(error = ?tok.error, description = ?tok.error_description, "oauth token exchange returned no id_token");
        return (
            StatusCode::UNAUTHORIZED,
            format!("sign-in failed: {}", tok.error.unwrap_or_default()),
        )
            .into_response();
    };
    let principal = match st.auth.verify_login_id_token(&id_token).await {
        Ok(p) => p,
        Err(crate::auth::AuthError::EmailUnverified) => {
            let retry_url = format!(
                "{}/_auth/login?next={}",
                crate::smart::request_base_url(&st, &headers),
                urlencode(&next),
            );
            return email_verification_response(&retry_url);
        }
        Err(crate::auth::AuthError::Forbidden) => {
            return (
                StatusCode::FORBIDDEN,
                "your account is not in an allowed domain",
            )
                .into_response();
        }
        Err(e) => {
            tracing::warn!(error = ?e, "login id_token rejected");
            return (StatusCode::UNAUTHORIZED, "sign-in failed: token rejected").into_response();
        }
    };
    let Some(value) = st.auth.session_cookie_value(&principal.name) else {
        return (StatusCode::NOT_IMPLEMENTED, "session secret missing").into_response();
    };
    let cookie = session_set_cookie(&st, &headers, &value);
    tracing::info!(principal = %principal.name, "browser sign-in");
    // Public origin: the callback ran there; the cookie is already right — go to `next`.
    if !loopback_origin(&st, &headers) {
        let mut r = Redirect::to(&next).into_response();
        r.headers_mut()
            .insert(header::SET_COOKIE, HeaderValue::from_str(&cookie).unwrap());
        r.headers_mut()
            .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
        return r;
    }
    // Standalone: the cookie is host-scoped: set it on localhost (this callback) and hop to
    // walgit.localhost so the same session is issued there too.
    let hop = format!("{}\n{}\n{}", now() + 60, value, next);
    let dest = match st.auth.sign(hop.as_bytes()) {
        Some(ticket) => format!(
            "{}/_auth/claimed?ticket={}&next={}",
            walgit_origin(&st, &headers),
            urlencode(&ticket),
            urlencode(&next),
        ),
        None => next.clone(),
    };
    let mut r = Redirect::to(&dest).into_response();
    r.headers_mut()
        .insert(header::SET_COOKIE, HeaderValue::from_str(&cookie).unwrap());
    r.headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    r
}

#[derive(serde::Deserialize, Default)]
struct ClaimedQuery {
    ticket: Option<String>,
    next: Option<String>,
}

async fn claimed(
    State(st): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(q): Query<ClaimedQuery>,
) -> Response {
    let next = safe_next(q.next);
    let Some(ticket) = q.ticket else {
        return Redirect::to(&next).into_response();
    };
    let Some(payload) = st.auth.verify_signed(&ticket) else {
        return Redirect::to(&next).into_response();
    };
    let payload = String::from_utf8_lossy(&payload).to_string();
    let mut parts = payload.splitn(3, '\n');
    let exp: u64 = parts.next().and_then(|e| e.parse().ok()).unwrap_or(0);
    let value = parts.next().unwrap_or("");
    if now() > exp || value.is_empty() {
        return Redirect::to(&next).into_response();
    }
    let cookie = session_set_cookie(&st, &headers, value);
    let mut r = Redirect::to(&next).into_response();
    r.headers_mut()
        .insert(header::SET_COOKIE, HeaderValue::from_str(&cookie).unwrap());
    r.headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    r
}

async fn logout(State(st): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    let secure = crate::smart::request_base_url(&st, &headers).starts_with("https://");
    let cookie = format!(
        "{SESSION_COOKIE}=; Path=/; Max-Age=0; HttpOnly; {}",
        cookie_site(&st, secure)
    );
    let mut r = Redirect::to("/").into_response();
    r.headers_mut()
        .insert(header::SET_COOKIE, HeaderValue::from_str(&cookie).unwrap());
    r
}

/// `GET /_auth/check` — optional `auth_request` target for an nginx edge. Same verifier as
/// every other route (bearer or session cookie) and no body: `204` +
/// `X-Walgit-Principal` / `X-Walgit-Write`, or `401`/`403`/`503`.
async fn check(State(st): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    match st.auth.require_read(&headers).await {
        Ok(p) => {
            let mut r = StatusCode::NO_CONTENT.into_response();
            let h = r.headers_mut();
            if let Ok(v) = HeaderValue::from_str(&p.name) {
                h.insert("x-walgit-principal", v);
            }
            h.insert(
                "x-walgit-write",
                HeaderValue::from_static(if p.write { "1" } else { "0" }),
            );
            h.insert(
                header::CACHE_CONTROL,
                HeaderValue::from_static("private, max-age=300"),
            );
            r
        }
        Err(crate::auth::AuthError::Forbidden) => crate::error::ApiError::Forbidden.into_response(),
        Err(crate::auth::AuthError::Unavailable) => {
            crate::error::ApiError::ServiceUnavailable("auth provider unavailable".into())
                .into_response()
        }
        Err(_) => crate::error::ApiError::Unauthorized.into_response(),
    }
}

async fn me(State(st): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    match st.auth.authenticate(&headers).await {
        Ok(p) if !p.anonymous => {
            let mut r = axum::Json(serde_json::json!({ "principal": p.name, "write": p.write }))
                .into_response();
            r.headers_mut()
                .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
            r
        }
        _ => crate::error::ApiError::Unauthorized.into_response(),
    }
}

/// `SameSite`/`Secure` attributes of the session cookie. With a cross-origin
/// browser lane configured (`server.cors_origins`) the cookie must travel
/// on credentialed fetches from those origins: `SameSite=None; Secure`.
/// Otherwise `Lax`.
pub fn session_set_cookie(st: &AppState, headers: &HeaderMap, value: &str) -> String {
    let secure = crate::smart::request_base_url(st, headers).starts_with("https://");
    format!(
        "{SESSION_COOKIE}={value}; Path=/; Max-Age={}; HttpOnly; {}",
        st.auth.session_ttl().as_secs(),
        cookie_site(st, secure)
    )
}

/// Sliding sessions (middleware on every app response): a valid session cookie
/// older than `session_ttl / 4` whose principal still passes policy is
/// re-issued with a fresh `exp` via `Set-Cookie`.
pub async fn refresh_session(
    State(st): State<Arc<AppState>>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let path = req.uri().path();
    let fresh = if path.starts_with("/_auth/") {
        None
    } else {
        st.auth.session_refresh_value(req.headers())
    };
    let cookie = fresh.map(|v| session_set_cookie(&st, req.headers(), &v));
    let mut resp = next.run(req).await;
    if let Some(c) = cookie
        && (resp.status().is_success() || resp.status().is_redirection())
        && let Ok(hv) = HeaderValue::from_str(&c)
    {
        resp.headers_mut().append(header::SET_COOKIE, hv);
    }
    resp
}

/// `POST /_auth/tokens` — mint a walgit access token for the signed-in browser principal.
/// Same-origin only (a cookie-bearing cross-site POST is refused by `Sec-Fetch-Site`).
async fn mint_token(State(st): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    if !st.auth.issued_tokens_enabled() {
        return (
            StatusCode::NOT_IMPLEMENTED,
            "issued tokens are off (server.auth.session_secret unset)",
        )
            .into_response();
    }
    if headers
        .get("sec-fetch-site")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v != "same-origin" && v != "none")
    {
        return crate::error::ApiError::Forbidden.into_response();
    }
    let principal = match st.auth.authenticate(&headers).await {
        Ok(p) if !p.anonymous => p,
        _ => return crate::error::ApiError::Unauthorized.into_response(),
    };
    let Some(token) = st.auth.access_token(&principal.name) else {
        return (StatusCode::NOT_IMPLEMENTED, "session secret missing").into_response();
    };
    let (exp, _) = st
        .auth
        .access_token_claims(&token)
        .unwrap_or((0, String::new()));
    tracing::info!(principal = %principal.name, exp, "access token minted");
    let mut r = axum::Json(serde_json::json!({
        "token": token,
        "principal": principal.name,
        "write": principal.write,
        "expires_at": exp,
    }))
    .into_response();
    r.headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    r
}

/// `GET /_auth/tokens` — a small page: one button that POSTs and shows the token with the
/// git one-liner that stores it. Unauthenticated browsers are sent to sign in first.
async fn tokens_page(State(st): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    let signed_in = matches!(st.auth.authenticate(&headers).await, Ok(p) if !p.anonymous);
    if !signed_in {
        let mut r = Redirect::to("/_auth/login?next=%2F_auth%2Ftokens").into_response();
        r.headers_mut()
            .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
        return r;
    }
    let base = crate::smart::request_base_url(&st, &headers);
    let host = base
        .split("://")
        .nth(1)
        .unwrap_or(&base)
        .trim_end_matches('/')
        .to_string();
    let ttl_days = st.auth.access_token_ttl().as_secs() / 86400;
    let html = TOKENS_PAGE
        .replace("{{host}}", &html_escape(&host))
        .replace("{{ttl_days}}", &ttl_days.to_string());
    let mut r = axum::response::Html(html).into_response();
    r.headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    r
}

fn email_verification_response(retry_url: &str) -> Response {
    let html = EMAIL_VERIFICATION_PAGE.replace("{{retry_url}}", &html_escape(retry_url));
    let mut response = (StatusCode::UNAUTHORIZED, axum::response::Html(html)).into_response();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

const EMAIL_VERIFICATION_PAGE: &str = r#"<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Verify your email to sign in</title>
<style>
body{font:16px/1.5 system-ui,sans-serif;max-width:36rem;margin:4rem auto;padding:0 1.5rem;color:#1a1a1a}
a{display:inline-block;padding:.6rem 1.2rem;border:1px solid currentColor;border-radius:999px;color:inherit;text-decoration:none}
a:focus-visible{outline:2px solid currentColor;outline-offset:3px}
@media(prefers-color-scheme:dark){body{background:#1a1a1a;color:#f4f4f5}}
</style></head><body>
<h1>Verify your email to sign in</h1>
<p>Your sign-in provider has not verified your email address. Complete email verification with your provider, then sign in again.</p>
<a href="{{retry_url}}">Sign in again</a>
</body></html>"#;

fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

const TOKENS_PAGE: &str = r#"<!doctype html>
<html lang="en"><head><meta charset="utf-8"><title>walgit · access token</title>
<style>
body{font:15px/1.5 system-ui,sans-serif;max-width:44rem;margin:4rem auto;padding:0 1rem;color:#1a1a1a}
pre{background:#f4f4f5;padding:.8rem 1rem;border-radius:6px;overflow:auto;white-space:pre-wrap;word-break:break-all}
button{font:inherit;padding:.5rem 1rem;border-radius:6px;border:1px solid #999;background:#fff;cursor:pointer}
.muted{color:#666}
</style></head><body>
<h1>Access token for <code>{{host}}</code></h1>
<p>A token lets <code>git</code> and scripts act as you for {{ttl_days}} days. It is shown once; walgit does not store it.</p>
<p><button id="mint">Create a token</button></p>
<div id="out" hidden>
<p>Store it for git (uses <code>git credential-store</code>; the token is the password):</p>
<pre id="cmd"></pre>
<p class="muted">Or send it as <code>Authorization: Bearer &lt;token&gt;</code>. Tokens cannot be listed or revoked one by one; rotating the server's <code>session_secret</code> revokes them all.</p>
</div>
<script>
document.getElementById('mint').onclick = async () => {
  const r = await fetch('/_auth/tokens', {method:'POST', credentials:'same-origin'});
  if (!r.ok) { alert('could not mint a token: HTTP ' + r.status); return; }
  const j = await r.json();
  const host = location.host;
  document.getElementById('cmd').textContent =
    `git config --global credential.https://${host}.helper store
` +
    `printf 'protocol=https\nhost=${host}\nusername=${j.principal}\npassword=${j.token}\n' | git credential approve
`;
  document.getElementById('out').hidden = false;
  document.getElementById('mint').disabled = true;
};
</script></body></html>"#;

fn cookie_site(st: &AppState, secure: bool) -> &'static str {
    if !st.cfg.server.cors_origins.is_empty() && secure {
        "SameSite=None; Secure"
    } else if secure {
        "SameSite=Lax; Secure"
    } else {
        "SameSite=Lax"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Form, Json, body::Body, http::Request, routing::post};
    use base64::Engine;
    use jsonwebtoken::{Algorithm, EncodingKey, Header};
    use serde_json::json;
    use std::collections::HashMap;
    use tower::ServiceExt;

    #[tokio::test]
    async fn callback_explains_unverified_email_after_validating_the_token() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let issuer = format!("http://{}", listener.local_addr().unwrap());
        let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ED25519).unwrap();
        let other_key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ED25519).unwrap();
        let mut header = Header::new(Algorithm::EdDSA);
        header.kid = Some("test".into());
        let tokens: HashMap<_, _> = [
            ("unverified", false, "browser", &key),
            ("verified", true, "browser", &key),
            ("wrong-audience", false, "another-client", &key),
            ("forged", false, "browser", &other_key),
        ]
        .into_iter()
        .map(|(code, verified, audience, signing_key)| {
            let token = jsonwebtoken::encode(
                &header,
                &json!({
                    "iss": issuer, "aud": audience, "exp": now() + 3600,
                    "email": "member@example.com", "email_verified": verified,
                }),
                &EncodingKey::from_ed_der(&signing_key.serialize_der()),
            )
            .unwrap();
            (code, token)
        })
        .collect();
        let discovery = json!({
            "issuer": issuer,
            "authorization_endpoint": format!("{issuer}/authorize"),
            "token_endpoint": format!("{issuer}/token"),
            "jwks_uri": format!("{issuer}/keys"),
        });
        let jwks = json!({"keys": [{
            "kid": "test", "kty": "OKP", "crv": "Ed25519",
            "x": base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(key.public_key_raw()),
        }]});
        let provider = Router::new()
            .route(
                "/.well-known/openid-configuration",
                get(async move || Json(discovery)),
            )
            .route("/keys", get(async move || Json(jwks)))
            .route(
                "/token",
                post(async move |Form(form): Form<HashMap<String, String>>| {
                    Json(json!({"id_token": tokens[form["code"].as_str()]}))
                }),
            );
        let provider_task = tokio::spawn(async move {
            axum::serve(listener, provider).await.unwrap();
        });
        let cache = tempfile::tempdir().unwrap();
        let mut cfg = walgit_config::Config::default();
        cfg.cache.dir = cache.path().into();
        cfg.server.public_url = Some("https://git.example.com/git".into());
        cfg.server.auth.mode = walgit_config::AuthMode::Oidc;
        cfg.server.auth.issuer = issuer;
        cfg.server.auth.allowed_domains = vec!["example.com".into()];
        cfg.server.auth.oauth_client_id = Some("browser".into());
        cfg.server.auth.oauth_client_secret = Some("test-client-secret".into());
        cfg.server.auth.session_secret = Some("test-session-secret-at-least-32-bytes".into());
        // The local fake provider uses HTTP. Production config still requires HTTPS.
        let state = AppState::new(Arc::new(cfg), walgit_store::memory::MemoryStore::shared())
            .await
            .unwrap();
        let app = router(state);
        for code in ["unverified", "verified", "wrong-audience", "forged"] {
            let login = app
                .clone()
                .oneshot(
                    Request::builder()
                        .uri("/_auth/login?next=%2F_auth%2Ftokens")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            let location =
                reqwest::Url::parse(login.headers()[header::LOCATION].to_str().unwrap()).unwrap();
            let signed_state = location
                .query_pairs()
                .find(|(name, _)| name == "state")
                .unwrap()
                .1
                .into_owned();
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/_auth/callback?code={code}&state={}",
                            urlencode(&signed_state)
                        ))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            if code == "verified" {
                assert_eq!(response.status(), StatusCode::SEE_OTHER);
                assert!(response.headers().contains_key(header::SET_COOKIE));
                assert_eq!(response.headers()[header::LOCATION], "/_auth/tokens");
                continue;
            }
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
            assert!(!response.headers().contains_key(header::SET_COOKIE));
            if code == "unverified" {
                assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
                assert_eq!(
                    response.headers()[header::CONTENT_TYPE],
                    "text/html; charset=utf-8"
                );
            }
            let body = axum::body::to_bytes(response.into_body(), 8192)
                .await
                .unwrap();
            let text = std::str::from_utf8(&body).unwrap();
            if code == "unverified" {
                assert!(text.contains("<h1>Verify your email to sign in</h1>"));
                assert!(text.contains("Complete email verification with your provider"));
                assert!(text.contains(
                    "href=\"https://git.example.com/git/_auth/login?next=%2F_auth%2Ftokens\""
                ));
                assert!(text.contains(">Sign in again</a>"));
            } else {
                assert_eq!(text, "sign-in failed: token rejected");
            }
        }
        provider_task.abort();
    }

    #[tokio::test]
    async fn email_verification_page_escapes_the_retry_link() {
        let response = email_verification_response("/login?next=\"<script>&");
        let body = axum::body::to_bytes(response.into_body(), 8192)
            .await
            .unwrap();
        let text = std::str::from_utf8(&body).unwrap();
        assert!(text.contains("href=\"/login?next=&quot;&lt;script&gt;&amp;\""));
        assert!(!text.contains("<script>"));
    }
}
