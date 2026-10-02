//! Authentication: `none` / `token` / `oidc` / `proxy`. Resolves a request to a
//! [`Principal`] (name, write/admin bits, owner scope).
//!
//! * **`token`** — static tokens from the config, presented as `Authorization:
//!   Bearer <token>` or as the password of HTTP Basic (any user name).
//! * **`oidc`** — any `OpenID` Connect issuer. Three credentials are accepted:
//!   1. an **ID token** from the issuer in `Authorization: Bearer` (RS256/ES256,
//!      signature against the issuer's JWKS, `iss`, `exp`, `aud` ∈ `audiences` ∪
//!      {`oauth_client_id`}, `email_verified`), for CLIs that can mint one;
//!   2. a **walgit access token** (`wgt_…`, minted at `/_auth/tokens` by a
//!      signed-in browser): HMAC-signed, stateless, the shape git and scripts
//!      use; also accepted as a Basic password;
//!   3. the **session cookie** set by the browser sign-in (`web/login.rs`).
//!   Static `tokens` are honoured in this mode too (robots, CI).
//!   Every path ends in the same allowlist: `allowed_domains` / `allowed_emails`,
//!   `write_domains`.
//! * **`proxy`** — an identity-aware proxy in front has already authenticated and
//!   authorized the caller and says so in headers (D50): `X-Walgit-Principal` (who),
//!   `X-Walgit-Access` (`read` | `write` | `admin`), optionally `X-Walgit-Owners` (the
//!   owners that exist for this caller). The proxy proves itself on every request with
//!   `X-Walgit-Proxy-Secret`, loopback listen included. A request that fails that proof
//!   is a 403 naming the proxy, never a 401: the client's credential was not the one
//!   rejected, and a 401 makes git erase it. These headers are read in no other mode.
//!
//! An edge in front of walgit may take the client's `Authorization` for its own
//! hop credential; it then announces `client-authorization` in
//! `X-Walgit-Capabilities` and carries the client's header in
//! `X-Walgit-Authorization`. Nothing is inferred from configuration.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use async_trait::async_trait;
use axum::http::{HeaderMap, StatusCode};
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use serde::Deserialize;
use tokio::sync::Mutex;
use walgit_config::{ACCESS_TOKEN_PREFIX, AuthMode, StaticToken};

/// End-user identity: set by a trusted forwarder (push broker hop), by anyone in `none`
/// mode, and by the proxy in `proxy` mode.
pub const PRINCIPAL_HEADER: &str = "x-walgit-principal";
/// `proxy` mode: the caller's access level as the proxy decided it — `read`, `write`
/// (implies read) or `admin` (implies write).
pub const PROXY_ACCESS_HEADER: &str = "x-walgit-access";
/// `proxy` mode: `<owner>[,<owner>…]` or `*`. Absent = every owner.
pub const PROXY_OWNERS_HEADER: &str = "x-walgit-owners";
/// `proxy` mode: the shared secret named by `server.auth.proxy_secret_env`.
pub const PROXY_SECRET_HEADER: &str = "x-walgit-proxy-secret";
/// Shortest accepted proxy secret (the same floor as `session_secret`), counted after
/// surrounding whitespace is trimmed.
const MIN_PROXY_SECRET_BYTES: usize = 32;
/// Client `Authorization` as copied by an edge before it replaces that header with its own
/// hop credential. Read only when the edge announces `client-authorization`.
pub const FORWARDED_AUTHORIZATION_HEADER: &str = "x-walgit-authorization";
/// Name of the browser session cookie.
pub const SESSION_COOKIE: &str = "walgit_session";
/// Clock skew tolerated on ID tokens.
const ID_TOKEN_LEEWAY_SECS: u64 = 30;

fn unix_now() -> Option<u64> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .map(|d| d.as_secs())
}

/// A resolved principal. `write` is false for anonymous read.
#[derive(Debug, Clone)]
pub struct Principal {
    pub name: String,
    pub write: bool,
    /// Repository deletion and PUT/DELETE settings and `policy.json`.
    /// Independent of `write` (push and repository creation).
    pub admin: bool,
    pub anonymous: bool,
    /// Owners this principal may address at all ([`OwnerScope::All`] outside `proxy` mode).
    pub owners: OwnerScope,
}

impl Principal {
    pub fn anonymous() -> Self {
        Self {
            name: "anonymous".to_string(),
            write: false,
            admin: false,
            anonymous: true,
            owners: OwnerScope::All,
        }
    }

    /// Whether `owner` exists for this principal (listings, and every route under it).
    pub fn sees_owner(&self, owner: &str) -> bool {
        self.owners.contains(owner)
    }
}

/// The owners a principal may address. Only `proxy` mode narrows it (`X-Walgit-Owners`):
/// an owner outside the list does not exist for the caller — listings omit it and every
/// route under its prefix answers 404, exactly like an owner without repositories. Never
/// 403: the answer must not confirm that the owner exists. The proxy remains the
/// authority for per-repository decisions; this is the listing filter and a second wall.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum OwnerScope {
    #[default]
    All,
    /// Exact (case-sensitive, like the bucket prefix) owner names. Empty = none.
    Only(Vec<String>),
}

impl OwnerScope {
    pub fn contains(&self, owner: &str) -> bool {
        match self {
            OwnerScope::All => true,
            OwnerScope::Only(owners) => owners.iter().any(|o| o == owner),
        }
    }

    /// Parse an `X-Walgit-Owners` value: `*`, or comma-separated owner names (blank entries
    /// skipped, so an empty value is the empty scope). A malformed entry refuses the whole
    /// header rather than dropping it: a proxy that sends garbage is misconfigured, and
    /// guessing which part it meant is how a scope widens.
    fn parse(value: &str) -> Option<Self> {
        if value.trim() == "*" {
            return Some(OwnerScope::All);
        }
        let mut owners = Vec::new();
        for owner in value.split(',').map(str::trim).filter(|o| !o.is_empty()) {
            // Owner names follow the repository-id rules; the name half is a placeholder.
            walgit_git::RepoId::new(owner, "_").ok()?;
            owners.push(owner.to_string());
        }
        Some(OwnerScope::Only(owners))
    }
}

/// How a `proxy`-mode request proves it came from the proxy. There is no loopback
/// exemption: in a sidecar deployment every container of the pod shares the network
/// namespace, so "can reach 127.0.0.1" names the pod, not the proxy.
enum ProxyTrust {
    /// SHA-256 of the shared secret. The presented value is hashed too and the digests are
    /// compared in constant time, so neither length nor prefix leaks by timing.
    Secret([u8; 32]),
    /// The secret could not be resolved (none configured, or an unset, blank or short
    /// variable): every request is refused.
    Refuse,
}

/// `server.auth.proxy_secret_env` resolved through `env`: `Ok(None)` outside `proxy`
/// mode, `Ok(Some(secret))` in it, `Err` when none is configured or the named variable
/// is unset, blank or shorter than 32 bytes. The value is trimmed the way the header
/// value is (`single_header`): a secret file or Kubernetes `Secret` ending in a newline
/// must match the header the proxy sends, and surrounding whitespace can never travel
/// in a header value anyway. Startup calls this with the process environment so a
/// missing secret fails the boot instead of refusing every request behind a green
/// `/readyz`.
pub fn resolve_proxy_secret(
    auth: &walgit_config::AuthConfig,
    env: &dyn Fn(&str) -> Option<String>,
) -> Result<Option<String>, String> {
    if auth.mode != AuthMode::Proxy {
        return Ok(None);
    }
    let Some(var) = auth.proxy_secret_env.as_deref().filter(|v| !v.is_empty()) else {
        return Err("server.auth.proxy_secret_env is required in proxy mode".to_string());
    };
    let value = env(var).unwrap_or_default();
    let value = value.trim();
    if value.is_empty() {
        return Err(format!(
            "server.auth.proxy_secret_env: ${var} is unset or blank"
        ));
    }
    if value.len() < MIN_PROXY_SECRET_BYTES {
        return Err(format!(
            "server.auth.proxy_secret_env: ${var} must be at least {MIN_PROXY_SECRET_BYTES} bytes (after trimming whitespace)"
        ));
    }
    Ok(Some(value.to_string()))
}

fn process_env(name: &str) -> Option<String> {
    std::env::var(name).ok()
}

fn proxy_trust(cfg: &walgit_config::Config, env: &dyn Fn(&str) -> Option<String>) -> ProxyTrust {
    use sha2::Digest;
    if cfg.server.auth.mode != AuthMode::Proxy {
        return ProxyTrust::Refuse;
    }
    match resolve_proxy_secret(&cfg.server.auth, env) {
        Ok(Some(secret)) => ProxyTrust::Secret(sha2::Sha256::digest(secret.as_bytes()).into()),
        // Unreachable in proxy mode; an authenticator built from an unvalidated config must
        // not be the one place a missing secret is honoured.
        Ok(None) => ProxyTrust::Refuse,
        Err(e) => {
            tracing::error!(error = %e, "proxy secret unavailable; refusing every request");
            ProxyTrust::Refuse
        }
    }
}

/// Constant-time equality of two SHA-256 digests.
fn digests_equal(a: &[u8; 32], b: &[u8; 32]) -> bool {
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// The single value of `name`: `Ok(None)` when absent, `Err` when repeated or not visible
/// ASCII. A repeated identity header means something between the client and walgit
/// appended instead of replacing — the request is not trusted to mean either value.
fn single_header<'h>(headers: &'h HeaderMap, name: &str) -> Result<Option<&'h str>, ()> {
    let mut values = headers.get_all(name).iter();
    let Some(first) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(());
    }
    first.to_str().map(|v| Some(v.trim())).map_err(|_| ())
}

/// A JWKS public key: RSA or EC P-256.
#[derive(Debug, Clone)]
pub enum JwksKey {
    Rsa {
        kid: String,
        n: String,
        e: String,
    },
    Ec {
        kid: String,
        crv: String,
        x: String,
        y: String,
    },
}

impl JwksKey {
    pub fn kid(&self) -> &str {
        match self {
            JwksKey::Rsa { kid, .. } | JwksKey::Ec { kid, .. } => kid,
        }
    }

    fn decoding_key(&self) -> Result<(DecodingKey, Algorithm), String> {
        match self {
            JwksKey::Rsa { kid, n, e } => DecodingKey::from_rsa_components(n, e)
                .map(|k| (k, Algorithm::RS256))
                .map_err(|err| format!("invalid RSA JWKS key {kid}: {err}")),
            JwksKey::Ec { kid, crv, x, y } => {
                if crv != "P-256" {
                    return Err(format!("unsupported EC curve {crv} for JWKS key {kid}"));
                }
                DecodingKey::from_ec_components(x, y)
                    .map(|k| (k, Algorithm::ES256))
                    .map_err(|err| format!("invalid EC JWKS key {kid}: {err}"))
            }
        }
    }
}

/// A fetched key set and its HTTP cache lifetime.
#[derive(Debug, Clone)]
pub struct JwksResponse {
    pub keys: Vec<JwksKey>,
    pub max_age: Duration,
}

/// Injectable source for a JWKS document. Tests use this to avoid the network.
#[async_trait]
pub trait JwksSource: Send + Sync {
    async fn fetch(&self) -> Result<JwksResponse, String>;
}

/// The issuer's discovery document (`/.well-known/openid-configuration`), the parts we use.
#[derive(Debug, Clone, Deserialize)]
pub struct Discovery {
    pub issuer: String,
    pub authorization_endpoint: String,
    pub token_endpoint: String,
    pub jwks_uri: String,
}

/// Fetches the discovery document once and the JWKS from `jwks_uri`, honouring
/// `Cache-Control: max-age`.
struct HttpOidcSource {
    client: reqwest::Client,
    issuer: String,
    discovery: Mutex<Option<Arc<Discovery>>>,
}

impl HttpOidcSource {
    async fn discovery(&self) -> Result<Arc<Discovery>, String> {
        if let Some(d) = self.discovery.lock().await.as_ref() {
            return Ok(d.clone());
        }
        let url = format!("{}/.well-known/openid-configuration", self.issuer);
        let doc: Discovery = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| format!("OIDC discovery {url}: {e}"))?
            .error_for_status()
            .map_err(|e| format!("OIDC discovery {url}: {e}"))?
            .json()
            .await
            .map_err(|e| format!("OIDC discovery {url}: {e}"))?;
        let doc = Arc::new(doc);
        *self.discovery.lock().await = Some(doc.clone());
        Ok(doc)
    }
}

#[derive(Debug, Deserialize)]
struct JwksDocument {
    keys: Vec<Jwk>,
}

#[derive(Debug, Deserialize)]
struct Jwk {
    kid: Option<String>,
    kty: String,
    n: Option<String>,
    e: Option<String>,
    crv: Option<String>,
    x: Option<String>,
    y: Option<String>,
}

impl Jwk {
    fn into_key(self) -> Option<JwksKey> {
        let kid = self.kid?;
        match self.kty.as_str() {
            "RSA" => Some(JwksKey::Rsa {
                kid,
                n: self.n?,
                e: self.e?,
            }),
            "EC" => Some(JwksKey::Ec {
                kid,
                crv: self.crv?,
                x: self.x?,
                y: self.y?,
            }),
            _ => None,
        }
    }
}

#[async_trait]
impl JwksSource for HttpOidcSource {
    async fn fetch(&self) -> Result<JwksResponse, String> {
        let url = self.discovery().await?.jwks_uri.clone();
        let response = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| format!("JWKS request failed: {e}"))?;
        let max_age = response
            .headers()
            .get(reqwest::header::CACHE_CONTROL)
            .and_then(|v| v.to_str().ok())
            .and_then(parse_max_age)
            .unwrap_or(Duration::from_mins(5));
        let document: JwksDocument = response
            .error_for_status()
            .map_err(|e| format!("JWKS response failed: {e}"))?
            .json()
            .await
            .map_err(|e| format!("JWKS response decode failed: {e}"))?;
        let keys = document
            .keys
            .into_iter()
            .filter_map(Jwk::into_key)
            .collect();
        Ok(JwksResponse { keys, max_age })
    }
}

fn parse_max_age(value: &str) -> Option<Duration> {
    value.split(',').find_map(|part| {
        let (name, seconds) = part.trim().split_once('=')?;
        if name.trim().eq_ignore_ascii_case("max-age") {
            Some(Duration::from_secs(seconds.trim().parse().ok()?))
        } else {
            None
        }
    })
}

#[derive(Clone)]
struct CachedKey {
    kid: String,
    key: DecodingKey,
    alg: Algorithm,
}

struct CachedJwks {
    keys: Vec<CachedKey>,
    expires_at: Instant,
}

/// One JWKS endpoint with its cache: serve cached keys until `max_age` elapses,
/// then refresh in the background and keep serving stale keys; refresh inline
/// on a cold cache or an unknown `kid` (key rotation).
struct KeySet {
    source: Arc<dyn JwksSource>,
    cache: Arc<Mutex<Option<CachedJwks>>>,
    refreshing: Arc<AtomicBool>,
}

impl KeySet {
    fn new(source: Arc<dyn JwksSource>) -> Self {
        Self {
            source,
            cache: Arc::new(Mutex::new(None)),
            refreshing: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Find the key for `kid`, refreshing once when it is unknown.
    async fn find(&self, kid: Option<&str>) -> Result<(DecodingKey, Algorithm), AuthError> {
        let mut keys = self.keys().await?;
        let mut selected = keys.iter().find(|k| Some(k.kid.as_str()) == kid).cloned();
        if selected.is_none() {
            // A key rotation is the one case where stale keys cannot verify the token.
            if let Ok(fresh) = self.refresh().await {
                keys = fresh;
                selected = keys.iter().find(|k| Some(k.kid.as_str()) == kid).cloned();
            }
        }
        selected.map(|k| (k.key, k.alg)).ok_or_else(|| {
            tracing::debug!(?kid, available_keys = keys.len(), "JWKS key not found");
            AuthError::Invalid
        })
    }

    async fn keys(&self) -> Result<Vec<CachedKey>, AuthError> {
        let now = Instant::now();
        if let Some(cached) = self.cache.lock().await.as_ref() {
            if cached.expires_at > now {
                return Ok(cached.keys.clone());
            }
            let stale = cached.keys.clone();
            self.spawn_refresh();
            return Ok(stale);
        }
        self.refresh().await
    }

    async fn refresh(&self) -> Result<Vec<CachedKey>, AuthError> {
        refresh_cache(self.source.clone(), self.cache.clone())
            .await
            .map_err(|e| {
                tracing::warn!(error = %e, "JWKS refresh failed");
                AuthError::Unavailable
            })
    }

    fn spawn_refresh(&self) {
        if self
            .refreshing
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            return;
        }
        let source = self.source.clone();
        let cache = self.cache.clone();
        let refreshing = self.refreshing.clone();
        tokio::spawn(async move {
            if let Err(e) = refresh_cache(source, cache).await {
                tracing::warn!(error = %e, "background JWKS refresh failed");
            }
            refreshing.store(false, Ordering::Release);
        });
    }
}

async fn refresh_cache(
    source: Arc<dyn JwksSource>,
    cache: Arc<Mutex<Option<CachedJwks>>>,
) -> Result<Vec<CachedKey>, String> {
    let response = source.fetch().await?;
    let mut keys = Vec::with_capacity(response.keys.len());
    for jwk in response.keys {
        let (key, alg) = jwk.decoding_key()?;
        keys.push(CachedKey {
            kid: jwk.kid().to_string(),
            key,
            alg,
        });
    }
    if keys.is_empty() {
        return Err("JWKS contained no usable keys".into());
    }
    *cache.lock().await = Some(CachedJwks {
        keys: keys.clone(),
        expires_at: Instant::now() + response.max_age,
    });
    Ok(keys)
}

/// Pluggable authenticator backed by [`walgit_config::AuthConfig`].
pub struct Authenticator {
    mode: AuthMode,
    anonymous_read: bool,
    tokens: Vec<StaticToken>,
    issuer: String,
    allowed_domains: Vec<String>,
    allowed_emails: Vec<String>,
    trusted_forwarders: Vec<String>,
    admin_emails: Vec<String>,
    admin_domains: Vec<String>,
    /// Accepted `aud` of bearer ID tokens (`audiences` ∪ `oauth_client_id`).
    audiences: Vec<String>,
    write_domains: Option<Vec<String>>,
    keys: KeySet,
    /// The live discovery document source (None when a test injected the JWKS directly).
    discovery: Option<Arc<HttpOidcSource>>,
    /// Session-cookie / access-token signing key (None = both disabled).
    session_secret: Option<Vec<u8>>,
    session_ttl: Duration,
    access_token_ttl: Duration,
    oauth_client_id: Option<String>,
    oauth_client_secret: Option<String>,
    /// `proxy` mode's trust boundary (`Refuse` in every other mode, where it is never read).
    proxy_trust: ProxyTrust,
}

impl Authenticator {
    pub fn new(cfg: &walgit_config::Config) -> Arc<Self> {
        let source = Arc::new(HttpOidcSource {
            client: reqwest::Client::new(),
            issuer: cfg.server.auth.issuer.trim_end_matches('/').to_string(),
            discovery: Mutex::new(None),
        });
        Self::build(cfg, source.clone(), Some(source), &process_env)
    }

    /// Construct an authenticator with an injectable JWKS source (tests: no network; the
    /// browser sign-in endpoints are unavailable).
    pub fn with_key_source(cfg: &walgit_config::Config, keys: Arc<dyn JwksSource>) -> Arc<Self> {
        Self::build(cfg, keys, None, &process_env)
    }

    fn build(
        cfg: &walgit_config::Config,
        keys: Arc<dyn JwksSource>,
        discovery: Option<Arc<HttpOidcSource>>,
        env: &dyn Fn(&str) -> Option<String>,
    ) -> Arc<Self> {
        let auth = &cfg.server.auth;
        let oauth_client_id = auth.oauth_client_id.clone().filter(|s| !s.is_empty());
        let mut audiences = auth.audiences.clone();
        if let Some(id) = &oauth_client_id
            && !audiences.contains(id)
        {
            audiences.push(id.clone());
        }
        Arc::new(Self {
            mode: auth.mode,
            anonymous_read: auth.anonymous_read,
            tokens: resolve_tokens(&auth.tokens),
            issuer: auth.issuer.trim_end_matches('/').to_string(),
            allowed_domains: auth
                .allowed_domains
                .iter()
                .map(|v| v.to_ascii_lowercase())
                .collect(),
            allowed_emails: auth
                .allowed_emails
                .iter()
                .map(|v| v.to_ascii_lowercase())
                .collect(),
            trusted_forwarders: auth
                .trusted_forwarders
                .iter()
                .map(|v| v.to_ascii_lowercase())
                .collect(),
            admin_emails: auth
                .admin_emails
                .iter()
                .map(|v| v.to_ascii_lowercase())
                .collect(),
            admin_domains: auth
                .admin_domains
                .iter()
                .map(|v| v.to_ascii_lowercase())
                .collect(),
            audiences,
            write_domains: auth
                .write_domains
                .as_ref()
                .map(|v| v.iter().map(|d| d.to_ascii_lowercase()).collect()),
            keys: KeySet::new(keys),
            discovery,
            session_secret: auth
                .session_secret
                .as_ref()
                .filter(|s| !s.is_empty())
                .map(|s| s.as_bytes().to_vec()),
            session_ttl: auth.session_ttl,
            access_token_ttl: auth.access_token_ttl,
            oauth_client_id,
            oauth_client_secret: auth.oauth_client_secret.clone().filter(|s| !s.is_empty()),
            proxy_trust: proxy_trust(cfg, env),
        })
    }

    /// Whether anonymous callers may read.
    pub fn anonymous_read_allowed(&self) -> bool {
        self.anonymous_read
    }

    pub fn mode(&self) -> AuthMode {
        self.mode
    }

    /// Browser sign-in is available when an OAuth client and a session secret exist.
    pub fn browser_login_enabled(&self) -> bool {
        self.mode == AuthMode::Oidc
            && self.session_secret.is_some()
            && self.oauth_client_id.is_some()
            && self.oauth_client_secret.is_some()
    }
    /// walgit-issued access tokens can be minted (and verified) on this host.
    pub fn issued_tokens_enabled(&self) -> bool {
        self.mode == AuthMode::Oidc && self.session_secret.is_some()
    }
    pub fn oauth_client(&self) -> Option<(&str, &str)> {
        Some((
            self.oauth_client_id.as_deref()?,
            self.oauth_client_secret.as_deref()?,
        ))
    }
    pub fn session_ttl(&self) -> Duration {
        self.session_ttl
    }
    pub fn access_token_ttl(&self) -> Duration {
        self.access_token_ttl
    }
    pub fn issuer(&self) -> &str {
        &self.issuer
    }

    /// The issuer's discovery document (authorization/token endpoints for the browser flow).
    pub async fn discovery(&self) -> Result<Arc<Discovery>, AuthError> {
        let Some(src) = &self.discovery else {
            return Err(AuthError::Unavailable);
        };
        src.discovery().await.map_err(|e| {
            tracing::warn!(error = %e, "OIDC discovery failed");
            AuthError::Unavailable
        })
    }

    /// Sign `payload` (opaque) with the session secret: `base64url(payload).base64url(mac)`.
    pub fn sign(&self, payload: &[u8]) -> Option<String> {
        use base64::Engine;
        use hmac::{Hmac, Mac};
        let secret = self.session_secret.as_ref()?;
        let mut mac = Hmac::<sha2::Sha256>::new_from_slice(secret).ok()?;
        mac.update(payload);
        let tag = mac.finalize().into_bytes();
        let e = base64::engine::general_purpose::URL_SAFE_NO_PAD;
        Some(format!("{}.{}", e.encode(payload), e.encode(tag)))
    }
    /// Verify a value produced by [`Self::sign`], returning the payload.
    pub fn verify_signed(&self, value: &str) -> Option<Vec<u8>> {
        use base64::Engine;
        use hmac::{Hmac, Mac};
        let secret = self.session_secret.as_ref()?;
        let (p, t) = value.split_once('.')?;
        let e = base64::engine::general_purpose::URL_SAFE_NO_PAD;
        let payload = e.decode(p).ok()?;
        let tag = e.decode(t).ok()?;
        let mut mac = Hmac::<sha2::Sha256>::new_from_slice(secret).ok()?;
        mac.update(&payload);
        mac.verify_slice(&tag).ok()?;
        Some(payload)
    }

    /// `kind\nexp\niat\nemail`, signed. Shared shape of the cookie (`session`) and of issued
    /// tokens (`token`); the kind keeps them apart so a cookie value never works as a bearer.
    fn mint(&self, email: &str, ttl: Duration, kind: &str) -> Option<String> {
        let now = unix_now()?;
        let exp = now + ttl.as_secs();
        self.sign(format!("{kind}\n{exp}\n{now}\n{email}").as_bytes())
    }
    /// `(exp, iat, email)` of a valid, unexpired signed value of `kind`.
    fn claims_of(&self, value: &str, kind: &str) -> Option<(u64, u64, String)> {
        let payload = self.verify_signed(value)?;
        let payload = String::from_utf8(payload).ok()?;
        let mut parts = payload.splitn(4, '\n');
        if parts.next()? != kind {
            return None;
        }
        let exp: u64 = parts.next()?.parse().ok()?;
        let iat: u64 = parts.next()?.parse().ok()?;
        let email = parts.next()?.to_string();
        if unix_now()? >= exp {
            return None;
        }
        Some((exp, iat, email))
    }

    /// Mint a session cookie value for `email`.
    pub fn session_cookie_value(&self, email: &str) -> Option<String> {
        self.mint(email, self.session_ttl, "session")
    }

    /// Mint a walgit access token for `email` (`wgt_…`), valid `access_token_ttl`.
    pub fn access_token(&self, email: &str) -> Option<String> {
        if !self.issued_tokens_enabled() {
            return None;
        }
        Some(format!(
            "{ACCESS_TOKEN_PREFIX}{}",
            self.mint(email, self.access_token_ttl, "token")?
        ))
    }
    /// `(exp, email)` of a valid access token.
    pub fn access_token_claims(&self, token: &str) -> Option<(u64, String)> {
        let raw = token.strip_prefix(ACCESS_TOKEN_PREFIX)?;
        let (exp, _, email) = self.claims_of(raw, "token")?;
        Some((exp, email))
    }

    fn session_claims(&self, headers: &HeaderMap) -> Option<(u64, u64, String)> {
        let raw = cookie_value(headers, SESSION_COOKIE)?;
        self.claims_of(&raw, "session")
    }

    /// Principal from a valid, unexpired session cookie (policy re-applied).
    fn authenticate_cookie(&self, headers: &HeaderMap) -> Option<Principal> {
        let (_, _, email) = self.session_claims(headers)?;
        self.principal_for_email(email).ok()
    }

    /// Sliding sessions: a fresh cookie value when the request carries a valid
    /// session older than a quarter of `session_ttl` whose principal still
    /// passes policy — `None` otherwise.
    pub fn session_refresh_value(&self, headers: &HeaderMap) -> Option<String> {
        let (_, iat, email) = self.session_claims(headers)?;
        if unix_now()?.saturating_sub(iat) < self.session_ttl.as_secs() / 4 {
            return None;
        }
        let principal = self.principal_for_email(email).ok()?;
        self.session_cookie_value(&principal.name)
    }

    /// Verify an ID token obtained by the server itself (OAuth callback):
    /// signature, issuer, expiry, `aud == oauth_client_id`, then the domain policy.
    pub async fn verify_login_id_token(&self, token: &str) -> Result<Principal, AuthError> {
        let (client_id, _) = self.oauth_client().ok_or(AuthError::Unavailable)?;
        self.verify_id_token(token, &[client_id]).await
    }

    /// Resolve the principal from request headers, applying a forwarded end-user
    /// identity only after authenticating the forwarding caller.
    pub async fn authenticate(&self, headers: &HeaderMap) -> Result<Principal, AuthError> {
        let p = self.authenticate_with_forwarding(headers).await?;
        // Attach the user to the enclosing `http.request` span so every log
        // line of this request (store.get, wal.sync, git.*) carries it.
        tracing::Span::current().record("principal", p.name.as_str());
        Ok(p)
    }

    async fn authenticate_with_forwarding(
        &self,
        headers: &HeaderMap,
    ) -> Result<Principal, AuthError> {
        // The proxy's principal header is the identity itself, not a forwarding claim.
        if self.mode == AuthMode::Proxy {
            return self.authenticate_proxy(headers);
        }
        let caller = self.authenticate_inner(headers).await?;
        let Some(forwarded) = headers
            .get(PRINCIPAL_HEADER)
            .and_then(|v| v.to_str().ok())
            .map(str::trim)
            .filter(|v| !v.is_empty())
        else {
            return Ok(caller);
        };
        if self.mode == AuthMode::None
            || (caller.write
                && self
                    .trusted_forwarders
                    .iter()
                    .any(|f| f.eq_ignore_ascii_case(&caller.name)))
        {
            return Ok(Principal {
                name: forwarded.to_string(),
                write: caller.write,
                admin: self.is_admin(forwarded),
                anonymous: false,
                owners: OwnerScope::All,
            });
        }
        Ok(caller)
    }

    /// `proxy` mode: the proxy proves itself first (nothing else it says is read before
    /// that; failing is [`AuthError::UntrustedProxy`], a 403 — the client's credential is
    /// not what was rejected), then names the caller (401 without one: there is no
    /// anonymous access) and its access level (403 when missing or unknown — fail closed,
    /// never a default). Admin comes only from the proxy; nothing in the config grants it.
    fn authenticate_proxy(&self, headers: &HeaderMap) -> Result<Principal, AuthError> {
        let result = self.proxy_principal(headers);
        if matches!(result, Err(AuthError::UntrustedProxy)) {
            // Operator-facing: every request through a misconfigured proxy fails this way.
            tracing::warn!(
                secret_present = headers.contains_key(PROXY_SECRET_HEADER),
                "request did not prove it came through the proxy (X-Walgit-Proxy-Secret missing, wrong or repeated, or a repeated identity header): a misconfigured proxy, or a caller bypassing it"
            );
        }
        result
    }

    /// [`Self::authenticate_proxy`] without logging (also asked by [`Self::hides_owner`]).
    fn proxy_principal(&self, headers: &HeaderMap) -> Result<Principal, AuthError> {
        use sha2::Digest;
        match &self.proxy_trust {
            ProxyTrust::Secret(expected) => {
                let presented = single_header(headers, PROXY_SECRET_HEADER)
                    .map_err(|()| AuthError::UntrustedProxy)?;
                let digest: [u8; 32] = sha2::Sha256::digest(presented.unwrap_or("")).into();
                if presented.is_none() || !digests_equal(&digest, expected) {
                    return Err(AuthError::UntrustedProxy);
                }
            }
            ProxyTrust::Refuse => return Err(AuthError::UntrustedProxy),
        }
        // A repeated principal means the proxy appended instead of replacing a header the
        // client sent: the proxy's fault, so not a 401 either.
        let name = single_header(headers, PRINCIPAL_HEADER)
            .map_err(|()| AuthError::UntrustedProxy)?
            .filter(|v| !v.is_empty())
            .ok_or(AuthError::Unauthorized)?;
        let access =
            single_header(headers, PROXY_ACCESS_HEADER).map_err(|()| AuthError::Forbidden)?;
        let (write, admin) = match access.map(str::to_ascii_lowercase).as_deref() {
            Some("read") => (false, false),
            Some("write") => (true, false),
            Some("admin") => (true, true),
            other => {
                tracing::debug!(access = ?other, "proxy access level missing or unknown");
                return Err(AuthError::Forbidden);
            }
        };
        let owners = match single_header(headers, PROXY_OWNERS_HEADER) {
            Ok(None) => OwnerScope::All,
            Ok(Some(v)) => OwnerScope::parse(v).ok_or(AuthError::Forbidden)?,
            Err(()) => return Err(AuthError::Forbidden),
        };
        Ok(Principal {
            name: name.to_string(),
            write,
            admin,
            anonymous: false,
            owners,
        })
    }

    /// A static token or an issued access token, from a bearer or a Basic password.
    fn opaque_token_principal(&self, tok: &str) -> Option<Result<Principal, AuthError>> {
        if let Some(st) = self.tokens.iter().find(|t| t.token == tok) {
            return Some(Ok(Principal {
                name: st.principal.clone(),
                write: st.write,
                admin: st.admin,
                anonymous: false,
                owners: OwnerScope::All,
            }));
        }
        if tok.starts_with(ACCESS_TOKEN_PREFIX) {
            return Some(match self.access_token_claims(tok) {
                Some((_, email)) => self.principal_for_email(email),
                None => Err(AuthError::Invalid),
            });
        }
        None
    }

    async fn authenticate_inner(&self, headers: &HeaderMap) -> Result<Principal, AuthError> {
        match self.mode {
            AuthMode::None => Ok(Principal {
                name: "anon".to_string(),
                write: true,
                admin: true,
                anonymous: false,
                owners: OwnerScope::All,
            }),
            AuthMode::Token => {
                let presented =
                    bearer_token(headers).or_else(|| basic_credentials(headers).map(|(_, p)| p));
                match presented {
                    Some(tok) => self
                        .opaque_token_principal(&tok)
                        .unwrap_or(Err(AuthError::Invalid)),
                    None => Ok(Principal::anonymous()),
                }
            }
            AuthMode::Oidc => {
                if let Some(tok) = bearer_token(headers) {
                    if let Some(r) = self.opaque_token_principal(&tok) {
                        return r;
                    }
                    return self.verify_id_token(&tok, &[]).await;
                }
                if let Some((_, pass)) = basic_credentials(headers) {
                    return self
                        .opaque_token_principal(&pass)
                        .unwrap_or(Err(AuthError::Invalid));
                }
                // No credentials at all → a browser session cookie may carry identity.
                self.authenticate_cookie(headers)
                    .ok_or(AuthError::Unauthorized)
            }
            // `authenticate_with_forwarding` answers proxy mode before reaching here.
            AuthMode::Proxy => self.authenticate_proxy(headers),
        }
    }

    /// Whether `owner` is outside the caller's owner scope: `proxy` mode with an
    /// `X-Walgit-Owners` list that does not name it. A request that does not authenticate
    /// is not hidden here — its handler answers with its own 401/403, so the credential
    /// story (git's `erase` on a real 401, the in-band help) stays in one place.
    pub fn hides_owner(&self, headers: &HeaderMap, owner: &str) -> bool {
        self.mode == AuthMode::Proxy
            && self
                .proxy_principal(headers)
                .is_ok_and(|p| !p.sees_owner(owner))
    }

    /// Require a principal with `write` for git push / LFS upload / repo create.
    pub async fn require_write(&self, headers: &HeaderMap) -> Result<Principal, AuthError> {
        let p = self.authenticate(headers).await?;
        if p.write {
            Ok(p)
        } else {
            Err(AuthError::Forbidden)
        }
    }

    /// Require a principal that may delete repositories or mutate settings and `policy.json`.
    pub async fn require_admin(&self, headers: &HeaderMap) -> Result<Principal, AuthError> {
        let p = self.authenticate(headers).await?;
        if p.admin {
            Ok(p)
        } else {
            Err(AuthError::Forbidden)
        }
    }

    fn is_admin(&self, name: &str) -> bool {
        if self.mode == AuthMode::None {
            return true;
        }
        let lower = name.to_ascii_lowercase();
        if self.admin_emails.iter().any(|e| e == &lower) {
            return true;
        }
        if let Some((_, domain)) = lower.rsplit_once('@')
            && self.admin_domains.iter().any(|d| d == domain)
        {
            return true;
        }
        self.tokens
            .iter()
            .any(|t| t.admin && t.principal.eq_ignore_ascii_case(name))
    }

    /// Require read access: anonymous read only when `anonymous_read` allows it.
    pub async fn require_read(&self, headers: &HeaderMap) -> Result<Principal, AuthError> {
        let p = self.authenticate(headers).await?;
        if !p.anonymous || self.anonymous_read {
            Ok(p)
        } else {
            Err(AuthError::Unauthorized)
        }
    }

    /// Verify an ID token against the issuer's JWKS. `only_aud` empty = the configured
    /// `audiences`; otherwise exactly `only_aud` (the login callback's own client).
    async fn verify_id_token(
        &self,
        token: &str,
        only_aud: &[&str],
    ) -> Result<Principal, AuthError> {
        let header = decode_header(token).map_err(|e| {
            tracing::debug!(error = %e, token_len = token.len(), "ID token header decode failed");
            AuthError::Invalid
        })?;
        if !matches!(header.alg, Algorithm::RS256 | Algorithm::ES256) {
            tracing::debug!(alg = ?header.alg, "ID token algorithm rejected");
            return Err(AuthError::Invalid);
        }
        let (key, alg) = self.keys.find(header.kid.as_deref()).await?;
        if alg != header.alg {
            tracing::debug!(alg = ?header.alg, key_alg = ?alg, "ID token algorithm does not match its key");
            return Err(AuthError::Invalid);
        }
        let mut validation = Validation::new(alg);
        validation.leeway = ID_TOKEN_LEEWAY_SECS;
        validation.set_issuer(&issuer_forms(&self.issuer));
        let audiences: Vec<&str> = if only_aud.is_empty() {
            self.audiences.iter().map(String::as_str).collect()
        } else {
            only_aud.to_vec()
        };
        if audiences.is_empty() {
            tracing::debug!("ID token presented but no audience is configured");
            return Err(AuthError::Invalid);
        }
        validation.set_audience(&audiences);
        validation.required_spec_claims.insert("aud".to_string());
        let claims = decode::<IdClaims>(token, &key, &validation)
            .map_err(|e| {
                tracing::debug!(error = %e, configured_audiences = ?audiences, "ID token validation failed");
                AuthError::Invalid
            })?
            .claims;
        if !claims.email_verified {
            tracing::debug!(email = %claims.email, "ID token email is not verified");
            return Err(AuthError::Invalid);
        }
        tracing::debug!(iss = %claims.iss, aud = ?claims.aud, email = %claims.email, "ID token validated");
        self.principal_for_email(claims.email)
    }

    /// Apply the domain/email allowlist and `write_domains` policy to a verified email.
    fn principal_for_email(&self, email: String) -> Result<Principal, AuthError> {
        let Some((_, domain)) = email.rsplit_once('@') else {
            return Err(AuthError::Invalid);
        };
        let email_lower = email.to_ascii_lowercase();
        let domain_lower = domain.to_ascii_lowercase();
        let allowed = self.allowed_domains.iter().any(|d| d == &domain_lower)
            || self.allowed_emails.iter().any(|e| e == &email_lower);
        if !allowed {
            return Err(AuthError::Forbidden);
        }
        let write = match &self.write_domains {
            None => allowed,
            Some(domains) => domains.iter().any(|d| d == &domain_lower),
        };
        Ok(Principal {
            name: email.clone(),
            write,
            admin: self.is_admin(&email),
            anonymous: false,
            owners: OwnerScope::All,
        })
    }
}

/// Some issuers (Google) put `iss` both with and without the scheme; accept the bare host
/// form for any issuer.
fn issuer_forms(issuer: &str) -> Vec<String> {
    let mut v = vec![issuer.to_string()];
    if let Some(bare) = issuer.strip_prefix("https://") {
        v.push(bare.to_string());
    }
    v
}

#[derive(Debug, Deserialize)]
struct IdClaims {
    iss: String,
    #[serde(default)]
    aud: Option<serde_json::Value>,
    email: String,
    #[serde(default)]
    email_verified: bool,
}

#[derive(Debug)]
pub enum AuthError {
    Invalid,
    Unauthorized,
    Forbidden,
    Unavailable,
    /// `proxy` mode: the request did not prove it came through the proxy (secret missing,
    /// wrong or repeated; a repeated identity header). A 403 naming the proxy, never a
    /// 401: what failed is the proxy's configuration, not the client's credential, and a
    /// 401 is what makes git erase that credential (§1.3). Not a 5xx either: a proxy
    /// retries 5xx from its upstream, and a caller bypassing the proxy is simply refused.
    UntrustedProxy,
}

impl AuthError {
    pub fn status(&self) -> StatusCode {
        match self {
            AuthError::Invalid | AuthError::Unauthorized => StatusCode::UNAUTHORIZED,
            AuthError::Forbidden | AuthError::UntrustedProxy => StatusCode::FORBIDDEN,
            AuthError::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        }
    }
}

fn parse_bearer(value: &str) -> Option<String> {
    let (scheme, rest) = value.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("Bearer") {
        return None;
    }
    let tok = rest.trim();
    if tok.is_empty() {
        None
    } else {
        Some(tok.to_string())
    }
}

/// `X-Walgit-Capabilities` token an edge sends when it has taken over the client's
/// `Authorization`: the client's header is in `X-Walgit-Authorization` (absent = the client
/// sent none) and `Authorization` is the hop's own credential, never the client's.
/// Announced per request, never inferred from config.
pub const CLIENT_AUTHORIZATION_CAPABILITY: &str = "client-authorization";

fn edge_owns_authorization(headers: &HeaderMap) -> bool {
    headers
        .get_all(crate::static_object::CAPABILITIES_HEADER)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .any(|c| {
            c.trim()
                .eq_ignore_ascii_case(CLIENT_AUTHORIZATION_CAPABILITY)
        })
}

/// The client's `Authorization` header value: the header itself when walgit is hit
/// directly, the edge-forwarded copy when an edge announced `client-authorization`.
fn client_authorization(headers: &HeaderMap) -> Option<String> {
    // Nothing announced the capability, so `Authorization` is the client's own and a
    // forwarded copy nobody vouched for is not read at all (D39 (2), §1.3).
    if !edge_owns_authorization(headers) {
        return headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
    }
    // Behind the edge, a missing copy means the client sent no credential; the
    // Authorization that is there is the hop's own.
    headers
        .get(FORWARDED_AUTHORIZATION_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(str::to_string)
}

fn bearer_token(headers: &HeaderMap) -> Option<String> {
    client_authorization(headers).and_then(|v| parse_bearer(&v))
}

/// Value of cookie `name` from the `Cookie` header(s).
pub fn cookie_value(headers: &HeaderMap, name: &str) -> Option<String> {
    for h in &headers.get_all(axum::http::header::COOKIE) {
        let Ok(s) = h.to_str() else { continue };
        for part in s.split(';') {
            let part = part.trim();
            if let Some(v) = part.strip_prefix(name).and_then(|r| r.strip_prefix('=')) {
                return Some(v.to_string());
            }
        }
    }
    None
}

fn basic_credentials(headers: &HeaderMap) -> Option<(String, String)> {
    let v = client_authorization(headers)?;
    let (scheme, rest) = v.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("Basic") {
        return None;
    }
    let decoded = base64_decode(rest.trim())?;
    let s = String::from_utf8(decoded).ok()?;
    let (u, p) = s.split_once(':')?;
    Some((u.to_string(), p.to_string()))
}

fn base64_decode(s: &str) -> Option<Vec<u8>> {
    const TABLE: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len() * 3 / 4);
    let mut buf = 0u32;
    let mut bits = 0u32;
    for &b in bytes {
        if b == b'=' {
            break;
        }
        let val = TABLE.iter().position(|&t| t == b)? as u32;
        buf = (buf << 6) | val;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((buf >> bits) as u8);
            buf &= (1 << bits) - 1;
        }
    }
    Some(out)
}

fn resolve_tokens(tokens: &[StaticToken]) -> Vec<StaticToken> {
    tokens
        .iter()
        .map(|t| {
            let token = t
                .token_env
                .as_ref()
                .and_then(|v| std::env::var(v).ok())
                .unwrap_or_else(|| t.token.clone());
            StaticToken {
                principal: t.principal.clone(),
                token,
                token_env: None,
                write: t.write,
                admin: t.admin,
            }
        })
        .filter(|t| !t.token.is_empty())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::header::AUTHORIZATION;
    use jsonwebtoken::{EncodingKey, Header, encode};
    use serde::Serialize;
    use std::sync::atomic::AtomicUsize;

    const ISSUER: &str = "https://accounts.google.com";
    const AUD: &str = "https://example.test";
    const SECRET: &str = "0123456789abcdef0123456789abcdef-session-secret";

    /// Behind the edge (`client-authorization` capability) `Authorization` is the hop's own
    /// credential: with no `X-Walgit-Authorization` there is no client bearer (so the session
    /// cookie gets its turn). Without the capability, `Authorization` is the client's and the
    /// forwarded header is not read at all.
    #[test]
    fn edge_owned_authorization_is_not_the_client() {
        let mut h = HeaderMap::new();
        h.insert(AUTHORIZATION, "Bearer invoker".parse().unwrap());
        assert_eq!(bearer_token(&h).as_deref(), Some("invoker"));
        h.insert(
            crate::static_object::CAPABILITIES_HEADER,
            "accel-redirect, client-authorization".parse().unwrap(),
        );
        assert_eq!(bearer_token(&h), None);
        h.insert(
            FORWARDED_AUTHORIZATION_HEADER,
            "Bearer client".parse().unwrap(),
        );
        assert_eq!(bearer_token(&h).as_deref(), Some("client"));

        let mut direct = HeaderMap::new();
        direct.insert(AUTHORIZATION, "Bearer a".parse().unwrap());
        direct.insert(FORWARDED_AUTHORIZATION_HEADER, "Bearer b".parse().unwrap());
        assert_eq!(
            bearer_token(&direct).as_deref(),
            Some("a"),
            "hit directly, a forwarded copy no edge announced is ignored"
        );
        direct.insert(
            crate::static_object::CAPABILITIES_HEADER,
            "client-authorization".parse().unwrap(),
        );
        assert_eq!(
            bearer_token(&direct).as_deref(),
            Some("b"),
            "the announced capability makes the forwarded copy the client's"
        );
    }

    #[test]
    fn base64_roundtrip() {
        assert_eq!(base64_decode("dXNlcjpwYXNz"), Some(b"user:pass".to_vec()));
        assert_eq!(base64_decode("YWJjZGVmZ2g="), Some(b"abcdefgh".to_vec()));
    }

    struct MockSource {
        calls: AtomicUsize,
        response: Mutex<Result<JwksResponse, String>>,
    }

    #[async_trait]
    impl JwksSource for MockSource {
        async fn fetch(&self) -> Result<JwksResponse, String> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.response.lock().await.clone()
        }
    }

    #[derive(Serialize)]
    struct TestClaims<'a> {
        iss: &'a str,
        aud: &'a str,
        exp: usize,
        email: &'a str,
        email_verified: bool,
    }

    // gitleaks:allow — fixed test fixture; never loaded outside this module's OIDC verifier tests.
    const PRIVATE_KEY: &[u8] = br"-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDJETqse41HRBsc
7cfcq3ak4oZWFCoZlcic525A3FfO4qW9BMtRO/iXiyCCHn8JhiL9y8j5JdVP2Q9Z
IpfElcFd3/guS9w+5RqQGgCR+H56IVUyHZWtTJbKPcwWXQdNUX0rBFcsBzCRESJL
eelOEdHIjG7LRkx5l/FUvlqsyHDVJEQsHwegZ8b8C0fz0EgT2MMEdn10t6Ur1rXz
jMB/wvCg8vG8lvciXmedyo9xJ8oMOh0wUEgxziVDMMovmC+aJctcHUAYubwoGN8T
yzcvnGqL7JSh36Pwy28iPzXZ2RLhAyJFU39vLaHdljwthUaupldlNyCfa6Ofy4qN
ctlUPlN1AgMBAAECggEAdESTQjQ70O8QIp1ZSkCYXeZjuhj081CK7jhhp/4ChK7J
GlFQZMwiBze7d6K84TwAtfQGZhQ7km25E1kOm+3hIDCoKdVSKch/oL54f/BK6sKl
qlIzQEAenho4DuKCm3I4yAw9gEc0DV70DuMTR0LEpYyXcNJY3KNBOTjN5EYQAR9s
2MeurpgK2MdJlIuZaIbzSGd+diiz2E6vkmcufJLtmYUT/k/ddWvEtz+1DnO6bRHh
xuuDMeJA/lGB/EYloSLtdyCF6sII6C6slJJtgfb0bPy7l8VtL5iDyz46IKyzdyzW
tKAn394dm7MYR1RlUBEfqFUyNK7C+pVMVoTwCC2V4QKBgQD64syfiQ2oeUlLYDm4
CcKSP3RnES02bcTyEDFSuGyyS1jldI4A8GXHJ/lG5EYgiYa1RUivge4lJrlNfjyf
dV230xgKms7+JiXqag1FI+3mqjAgg4mYiNjaao8N8O3/PD59wMPeWYImsWXNyeHS
55rUKiHERtCcvdzKl4u35ZtTqQKBgQDNKnX2bVqOJ4WSqCgHRhOm386ugPHfy+8j
m6cicmUR46ND6ggBB03bCnEG9OtGisxTo/TuYVRu3WP4KjoJs2LD5fwdwJqpgtHl
yVsk45Y1Hfo+7M6lAuR8rzCi6kHHNb0HyBmZjysHWZsn79ZM+sQnLpgaYgQGRbKV
DZWlbw7g7QKBgQCl1u+98UGXAP1jFutwbPsx40IVszP4y5ypCe0gqgon3UiY/G+1
zTLp79GGe/SjI2VpQ7AlW7TI2A0bXXvDSDi3/5Dfya9ULnFXv9yfvH1QwWToySpW
Kvd1gYSoiX84/WCtjZOr0e0HmLIb0vw0hqZA4szJSqoxQgvF22EfIWaIaQKBgQCf
34+OmMYw8fEvSCPxDxVvOwW2i7pvV14hFEDYIeZKW2W1HWBhVMzBfFB5SE8yaCQy
pRfOzj9aKOCm2FjjiErVNpkQoi6jGtLvScnhZAt/lr2TXTrl8OwVkPrIaN0bG/AS
aUYxmBPCpXu3UjhfQiWqFq/mFyzlqlgvuCc9g95HPQKBgAscKP8mLxdKwOgX8yFW
GcZ0izY/30012ajdHY+/QK5lsMoxTnn0skdS+spLxaS5ZEO4qvPVb8RAoCkWMMal
2pOhmquJQVDPDLuZHdrIiKiDM20dy9sMfHygWcZjQ4WSxf/J7T9canLZIXFhHAZT
3wc9h4G8BBCtWN2TN/LsGZdB
-----END PRIVATE KEY-----
";
    const MODULUS: &str = "yRE6rHuNR0QbHO3H3Kt2pOKGVhQqGZXInOduQNxXzuKlvQTLUTv4l4sggh5_CYYi_cvI-SXVT9kPWSKXxJXBXd_4LkvcPuUakBoAkfh-eiFVMh2VrUyWyj3MFl0HTVF9KwRXLAcwkREiS3npThHRyIxuy0ZMeZfxVL5arMhw1SRELB8HoGfG_AtH89BIE9jDBHZ9dLelK9a184zAf8LwoPLxvJb3Il5nncqPcSfKDDodMFBIMc4lQzDKL5gvmiXLXB1AGLm8KBjfE8s3L5xqi-yUod-j8MtvIj812dkS4QMiRVN_by2h3ZY8LYVGrqZXZTcgn2ujn8uKjXLZVD5TdQ";
    const EXPONENT: &str = "AQAB";

    fn config() -> walgit_config::Config {
        let mut cfg = walgit_config::Config::default();
        cfg.server.auth.mode = AuthMode::Oidc;
        cfg.server.auth.issuer = ISSUER.into();
        cfg.server.auth.allowed_domains = vec!["Example.com".into()];
        cfg.server.auth.audiences = vec![AUD.into()];
        cfg.server.auth.anonymous_read = false;
        cfg
    }

    fn bearer(tok: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(AUTHORIZATION, format!("Bearer {tok}").parse().unwrap());
        h
    }

    fn basic(user: &str, pass: &str) -> HeaderMap {
        use base64::Engine;
        let mut h = HeaderMap::new();
        let v = base64::engine::general_purpose::STANDARD.encode(format!("{user}:{pass}"));
        h.insert(AUTHORIZATION, format!("Basic {v}").parse().unwrap());
        h
    }

    fn token(email: &str, iss: &str, aud: &str, exp: usize, verified: bool) -> String {
        let claims = TestClaims {
            iss,
            aud,
            exp,
            email,
            email_verified: verified,
        };
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some("test".into());
        encode(
            &header,
            &claims,
            &EncodingKey::from_rsa_pem(PRIVATE_KEY).unwrap(),
        )
        .unwrap()
    }

    fn source() -> Arc<MockSource> {
        Arc::new(MockSource {
            calls: AtomicUsize::new(0),
            response: Mutex::new(Ok(JwksResponse {
                keys: vec![JwksKey::Rsa {
                    kid: "test".into(),
                    n: MODULUS.into(),
                    e: EXPONENT.into(),
                }],
                max_age: Duration::from_hours(1),
            })),
        })
    }

    fn static_token(principal: &str, token: &str, write: bool) -> StaticToken {
        StaticToken {
            principal: principal.into(),
            token: token.into(),
            token_env: None,
            write,
            admin: false,
        }
    }

    #[tokio::test]
    async fn token_mode_accepts_bearer_and_basic_and_rejects_the_rest() {
        let mut cfg = walgit_config::Config::default();
        cfg.server.auth.mode = AuthMode::Token;
        cfg.server.auth.anonymous_read = false;
        cfg.server.auth.tokens = vec![
            static_token("alice", "s3cret", true),
            static_token("ci", "r0bot", false),
        ];
        let auth = Authenticator::new(&cfg);
        let p = auth.authenticate(&bearer("s3cret")).await.unwrap();
        assert_eq!((p.name.as_str(), p.write), ("alice", true));
        let p = auth
            .authenticate(&basic("anything", "r0bot"))
            .await
            .unwrap();
        assert_eq!((p.name.as_str(), p.write), ("ci", false));
        assert!(matches!(
            auth.authenticate(&bearer("nope")).await,
            Err(AuthError::Invalid)
        ));
        assert!(matches!(
            auth.require_read(&HeaderMap::new()).await,
            Err(AuthError::Unauthorized)
        ));
        assert!(matches!(
            auth.require_write(&basic("x", "r0bot")).await,
            Err(AuthError::Forbidden)
        ));
    }

    #[tokio::test]
    async fn forwarded_principal_requires_trusted_caller() {
        let mut cfg = walgit_config::Config::default();
        cfg.server.auth.mode = AuthMode::Token;
        cfg.server.auth.tokens = vec![static_token("front", "secret", true)];
        cfg.server.auth.trusted_forwarders = vec!["front".into()];
        let auth = Authenticator::new(&cfg);
        let mut headers = bearer("secret");
        headers.insert("x-walgit-principal", "user@example.com".parse().unwrap());
        assert_eq!(
            auth.authenticate(&headers).await.unwrap().name,
            "user@example.com"
        );

        cfg.server.auth.trusted_forwarders.clear();
        let auth = Authenticator::new(&cfg);
        assert_eq!(auth.authenticate(&headers).await.unwrap().name, "front");
    }

    #[tokio::test]
    async fn id_token_accept_and_reject_paths() {
        let source = source();
        let auth = Authenticator::with_key_source(&config(), source.clone());
        let h = bearer(&token("dev@example.com", ISSUER, AUD, 4_000_000_000, true));
        let principal = auth.authenticate(&h).await.unwrap();
        assert_eq!(principal.name, "dev@example.com");
        assert!(principal.write);
        auth.authenticate(&h).await.unwrap();
        assert_eq!(source.calls.load(Ordering::Relaxed), 1, "JWKS cached");

        for (iss, email, verified, aud) in [
            ("bad", "dev@example.com", true, AUD),
            (ISSUER, "dev@other.com", true, AUD),
            (ISSUER, "dev@example.com", false, AUD),
            (ISSUER, "dev@example.com", true, "wrong"),
        ] {
            let h = bearer(&token(email, iss, aud, 4_000_000_000, verified));
            assert!(matches!(
                auth.authenticate(&h).await,
                Err(AuthError::Invalid | AuthError::Forbidden)
            ));
        }
        let expired = bearer(&token("dev@example.com", ISSUER, AUD, 1, true));
        assert!(matches!(
            auth.authenticate(&expired).await,
            Err(AuthError::Invalid)
        ));
        // Google's bare-host issuer form is accepted.
        let bare = bearer(&token(
            "dev@example.com",
            "accounts.google.com",
            AUD,
            4_000_000_000,
            true,
        ));
        assert!(auth.authenticate(&bare).await.is_ok());
    }

    #[tokio::test]
    async fn id_token_accepts_any_configured_audience_and_the_web_client() {
        let mut cfg = config();
        cfg.server.auth.audiences = vec!["a".into(), "b".into()];
        cfg.server.auth.oauth_client_id = Some("web-client".into());
        cfg.server.auth.oauth_client_secret = Some("x".into());
        cfg.server.auth.session_secret = Some(SECRET.into());
        let auth = Authenticator::with_key_source(&cfg, source());
        for aud in ["a", "b", "web-client"] {
            let h = bearer(&token("dev@example.com", ISSUER, aud, 4_000_000_000, true));
            assert!(auth.authenticate(&h).await.is_ok(), "aud {aud}");
        }
        let h = bearer(&token("dev@example.com", ISSUER, "c", 4_000_000_000, true));
        assert!(matches!(
            auth.authenticate(&h).await,
            Err(AuthError::Invalid)
        ));
    }

    #[tokio::test]
    async fn missing_token_and_jwks_failure() {
        let auth = Authenticator::with_key_source(&config(), source());
        assert!(matches!(
            auth.authenticate(&HeaderMap::new()).await,
            Err(AuthError::Unauthorized)
        ));
        let failing = Arc::new(MockSource {
            calls: AtomicUsize::new(0),
            response: Mutex::new(Err("offline".into())),
        });
        let auth = Authenticator::with_key_source(&config(), failing);
        let h = bearer(&token("dev@example.com", ISSUER, AUD, 4_000_000_000, true));
        assert!(matches!(
            auth.authenticate(&h).await,
            Err(AuthError::Unavailable)
        ));
    }

    #[tokio::test]
    async fn email_allowlist_and_write_domains() {
        let mut cfg = config();
        cfg.server.auth.allowed_domains.clear();
        cfg.server.auth.allowed_emails = vec!["svc@other.com".into()];
        cfg.server.auth.write_domains = Some(vec!["other.com".into()]);
        let auth = Authenticator::with_key_source(&cfg, source());
        let h = bearer(&token("svc@other.com", ISSUER, AUD, 4_000_000_000, true));
        assert!(auth.authenticate(&h).await.unwrap().write);
        let h = bearer(&token("else@other.com", ISSUER, AUD, 4_000_000_000, true));
        assert!(matches!(
            auth.authenticate(&h).await,
            Err(AuthError::Forbidden)
        ));
    }

    #[tokio::test]
    async fn stale_jwks_cache_survives_refresh_failure() {
        let source = source();
        source.response.lock().await.as_mut().unwrap().max_age = Duration::ZERO;
        let auth = Authenticator::with_key_source(&config(), source.clone());
        let h = bearer(&token("dev@example.com", ISSUER, AUD, 4_000_000_000, true));
        assert!(auth.authenticate(&h).await.is_ok());
        *source.response.lock().await = Err("offline".into());
        assert!(auth.authenticate(&h).await.is_ok());
    }

    #[tokio::test]
    async fn static_tokens_work_in_oidc_mode_too() {
        let mut cfg = config();
        cfg.server.auth.tokens = vec![static_token("deploy-bot", "r0bot", true)];
        let auth = Authenticator::with_key_source(&cfg, source());
        assert_eq!(
            auth.authenticate(&bearer("r0bot")).await.unwrap().name,
            "deploy-bot"
        );
        assert_eq!(
            auth.authenticate(&basic("git", "r0bot"))
                .await
                .unwrap()
                .name,
            "deploy-bot"
        );
    }

    #[tokio::test]
    async fn issued_access_tokens_are_bearers_and_basic_passwords_and_never_cookies() {
        let mut cfg = config();
        cfg.server.auth.session_secret = Some(SECRET.into());
        cfg.server.auth.access_token_ttl = Duration::from_hours(1);
        let auth = Authenticator::with_key_source(&cfg, source());
        let tok = auth.access_token("dev@example.com").unwrap();
        assert!(tok.starts_with(ACCESS_TOKEN_PREFIX));
        let (exp, email) = auth.access_token_claims(&tok).unwrap();
        assert_eq!(email, "dev@example.com");
        assert!(exp.abs_diff(unix_now().unwrap() + 3600) <= 2);
        assert_eq!(
            auth.authenticate(&bearer(&tok)).await.unwrap().name,
            "dev@example.com"
        );
        assert_eq!(
            auth.authenticate(&basic("x-access-token", &tok))
                .await
                .unwrap()
                .name,
            "dev@example.com"
        );

        // Tampered / foreign-secret / wrong kind tokens are Invalid (a real 401: git erases them).
        let mut bad = tok.clone();
        bad.pop();
        assert!(matches!(
            auth.authenticate(&bearer(&bad)).await,
            Err(AuthError::Invalid)
        ));
        let cookie_as_bearer = format!(
            "{ACCESS_TOKEN_PREFIX}{}",
            auth.session_cookie_value("dev@example.com").unwrap()
        );
        assert!(matches!(
            auth.authenticate(&bearer(&cookie_as_bearer)).await,
            Err(AuthError::Invalid)
        ));
        let mut h = HeaderMap::new();
        h.insert(
            "cookie",
            format!(
                "{SESSION_COOKIE}={}",
                tok.trim_start_matches(ACCESS_TOKEN_PREFIX)
            )
            .parse()
            .unwrap(),
        );
        assert!(
            matches!(auth.authenticate(&h).await, Err(AuthError::Unauthorized)),
            "a token is not a session"
        );

        // Policy is re-applied at use: a principal that lost its domain is Forbidden.
        cfg.server.auth.allowed_domains = vec!["elsewhere.org".into()];
        let stricter = Authenticator::with_key_source(&cfg, source());
        assert!(matches!(
            stricter.authenticate(&bearer(&tok)).await,
            Err(AuthError::Forbidden)
        ));

        // Without a session secret nothing is minted and every wgt_ token is Invalid.
        cfg.server.auth.session_secret = None;
        let none = Authenticator::with_key_source(&cfg, source());
        assert!(none.access_token("dev@example.com").is_none());
        assert!(matches!(
            none.authenticate(&bearer(&tok)).await,
            Err(AuthError::Invalid)
        ));
    }
}

#[cfg(test)]
mod proxy_tests {
    use super::*;

    const SECRET: &str = "0123456789abcdef0123456789abcdef-proxy-secret";
    /// `SECRET` as a secret file usually holds it.
    const SECRET_NEWLINE: &str = "0123456789abcdef0123456789abcdef-proxy-secret\n";

    struct NoKeys;
    #[async_trait]
    impl JwksSource for NoKeys {
        async fn fetch(&self) -> Result<JwksResponse, String> {
            Err("no keys in proxy mode".into())
        }
    }

    fn proxy_config(listen: &str, secret_env: Option<&str>) -> walgit_config::Config {
        let mut cfg = walgit_config::Config::default();
        cfg.server.listen = listen.parse().unwrap();
        cfg.server.auth.mode = AuthMode::Proxy;
        cfg.server.auth.anonymous_read = false;
        cfg.server.auth.proxy_secret_env = secret_env.map(str::to_string);
        cfg
    }

    /// An authenticator whose environment holds `WALGIT_PROXY_SECRET = value`.
    fn proxy_auth(cfg: &walgit_config::Config, value: Option<&str>) -> Arc<Authenticator> {
        let value = value.map(str::to_string);
        let env = move |name: &str| (name == "WALGIT_PROXY_SECRET").then(|| value.clone())?;
        Authenticator::build(cfg, Arc::new(NoKeys), None, &env)
    }

    /// The sidecar shape: loopback listen, and the secret all the same.
    fn sidecar() -> Arc<Authenticator> {
        proxy_auth(
            &proxy_config("127.0.0.1:8080", Some("WALGIT_PROXY_SECRET")),
            Some(SECRET),
        )
    }

    /// Exactly these headers.
    fn bare(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for (k, v) in pairs {
            h.append(
                axum::http::HeaderName::from_bytes(k.as_bytes()).unwrap(),
                v.parse().unwrap(),
            );
        }
        h
    }

    /// These headers from the proxy: with its secret.
    fn asserted(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut h = bare(pairs);
        h.insert(PROXY_SECRET_HEADER, SECRET.parse().unwrap());
        h
    }

    #[tokio::test]
    async fn the_proxy_names_the_caller_and_its_access_level() {
        let auth = sidecar();
        for (access, write, admin) in [
            ("read", false, false),
            ("write", true, false),
            ("admin", true, true),
            (" Admin ", true, true),
        ] {
            let h = asserted(&[
                (PRINCIPAL_HEADER, "dev@example.com"),
                (PROXY_ACCESS_HEADER, access),
            ]);
            let p = auth.authenticate(&h).await.unwrap();
            assert_eq!(
                (p.name.as_str(), p.write, p.admin, p.anonymous),
                ("dev@example.com", write, admin, false),
                "{access}"
            );
            assert_eq!(p.owners, OwnerScope::All, "no header = every owner");
        }
        let read = asserted(&[(PRINCIPAL_HEADER, "r"), (PROXY_ACCESS_HEADER, "read")]);
        auth.require_read(&read).await.unwrap();
        assert!(matches!(
            auth.require_write(&read).await,
            Err(AuthError::Forbidden)
        ));
        let write = asserted(&[(PRINCIPAL_HEADER, "w"), (PROXY_ACCESS_HEADER, "write")]);
        assert!(matches!(
            auth.require_admin(&write).await,
            Err(AuthError::Forbidden)
        ));

        // No principal (or a blank one) is a 401: there is no anonymous access.
        for h in [
            asserted(&[(PROXY_ACCESS_HEADER, "admin")]),
            asserted(&[(PRINCIPAL_HEADER, "  "), (PROXY_ACCESS_HEADER, "admin")]),
            asserted(&[]),
        ] {
            assert!(matches!(
                auth.require_read(&h).await,
                Err(AuthError::Unauthorized)
            ));
        }
        // A missing or unknown access level is a 403, never a default.
        for access in [None, Some(""), Some("owner"), Some("read,write")] {
            let mut h = asserted(&[(PRINCIPAL_HEADER, "dev@example.com")]);
            if let Some(a) = access {
                h.insert(PROXY_ACCESS_HEADER, a.parse().unwrap());
            }
            assert!(
                matches!(auth.authenticate(&h).await, Err(AuthError::Forbidden)),
                "{access:?}"
            );
        }
        // Appended rather than replaced: neither value is trusted, and the fault is the
        // proxy's (a 403 naming it), not the client credential's (a 401 would erase it).
        let twice = asserted(&[
            (PRINCIPAL_HEADER, "a"),
            (PRINCIPAL_HEADER, "b"),
            (PROXY_ACCESS_HEADER, "read"),
        ]);
        assert!(matches!(
            auth.authenticate(&twice).await,
            Err(AuthError::UntrustedProxy)
        ));
        let twice = asserted(&[
            (PRINCIPAL_HEADER, "a"),
            (PROXY_ACCESS_HEADER, "read"),
            (PROXY_ACCESS_HEADER, "admin"),
        ]);
        assert!(matches!(
            auth.authenticate(&twice).await,
            Err(AuthError::Forbidden)
        ));
        // Config-granted admin does not exist in proxy mode, `none`'s implicit one included.
        assert!(!auth.is_admin("dev@example.com"));
    }

    #[tokio::test]
    async fn the_proxy_proves_itself_with_the_shared_secret() {
        let cfg = proxy_config("0.0.0.0:8080", Some("WALGIT_PROXY_SECRET"));
        let auth = proxy_auth(&cfg, Some(SECRET));
        let identity = [
            (PRINCIPAL_HEADER, "dev@example.com"),
            (PROXY_ACCESS_HEADER, "admin"),
        ];
        let with = |secret: &[&str]| {
            let mut h = bare(&identity);
            for s in secret {
                h.append(PROXY_SECRET_HEADER, s.parse().unwrap());
            }
            h
        };
        assert!(auth.authenticate(&with(&[SECRET])).await.unwrap().admin);
        for bad in [
            &[][..],
            &[""][..],
            &["wrong"][..],
            &[&SECRET[1..]][..],
            &[SECRET, SECRET][..],
        ] {
            let err = auth.authenticate(&with(bad)).await.unwrap_err();
            assert!(matches!(err, AuthError::UntrustedProxy), "{bad:?}: {err:?}");
            // Never a 401: git would erase the client's stored credential, which is not
            // what failed.
            assert_eq!(err.status(), StatusCode::FORBIDDEN, "{bad:?}");
        }
        // A bad secret is refused before any other header is read.
        let mut h = with(&["wrong"]);
        h.remove(PROXY_ACCESS_HEADER);
        h.remove(PRINCIPAL_HEADER);
        assert!(matches!(
            auth.authenticate(&h).await,
            Err(AuthError::UntrustedProxy)
        ));

        // Loopback (the sidecar shape) requires it too: the whole pod shares loopback.
        let auth = sidecar();
        assert!(auth.authenticate(&with(&[SECRET])).await.is_ok());
        assert!(matches!(
            auth.authenticate(&bare(&identity)).await,
            Err(AuthError::UntrustedProxy)
        ));

        // A secret file / Kubernetes Secret ending in a newline (or padded) still matches
        // the header, which arrives trimmed.
        for stored in [
            format!("{SECRET}\n"),
            format!("{SECRET}\r\n"),
            format!("  {SECRET} \n"),
        ] {
            let auth = proxy_auth(&cfg, Some(stored.as_str()));
            assert!(
                auth.authenticate(&with(&[SECRET])).await.is_ok(),
                "{stored:?}"
            );
            assert!(matches!(
                auth.authenticate(&with(&[&SECRET[1..]])).await,
                Err(AuthError::UntrustedProxy)
            ));
        }

        // Unresolvable secret, or none configured (loopback included): nothing is trusted.
        for (cfg, value) in [
            (cfg.clone(), None),
            (cfg.clone(), Some("")),
            (cfg.clone(), Some("\n")),
            (cfg.clone(), Some("short")),
            (proxy_config("0.0.0.0:8080", None), None),
            (proxy_config("127.0.0.1:8080", None), None),
        ] {
            let auth = proxy_auth(&cfg, value);
            assert!(
                matches!(
                    auth.authenticate(&with(&[SECRET])).await,
                    Err(AuthError::UntrustedProxy)
                ),
                "{value:?}"
            );
            assert!(matches!(
                auth.authenticate(&with(&[])).await,
                Err(AuthError::UntrustedProxy)
            ));
        }
    }

    #[test]
    fn the_secret_resolves_at_startup_or_fails_it() {
        let cfg = proxy_config("0.0.0.0:8080", Some("WALGIT_PROXY_SECRET"));
        let env = |v: Option<&'static str>| move |_: &str| v.map(str::to_string);
        assert_eq!(
            resolve_proxy_secret(&cfg.server.auth, &env(Some(SECRET)))
                .unwrap()
                .as_deref(),
            Some(SECRET)
        );
        // Trimmed like the header: a trailing newline is not part of the secret.
        assert_eq!(
            resolve_proxy_secret(&cfg.server.auth, &env(Some(SECRET_NEWLINE)))
                .unwrap()
                .as_deref(),
            Some(SECRET)
        );
        for (value, why) in [
            (None, "unset"),
            (Some(""), "unset"),
            (Some("\n"), "blank"),
            (Some(" \t\r\n"), "blank"),
            (Some("short"), "32 bytes"),
            // 31 bytes once the newline is trimmed.
            (Some("0123456789abcdef0123456789abcde\n"), "32 bytes"),
        ] {
            let err = resolve_proxy_secret(&cfg.server.auth, &env(value)).unwrap_err();
            assert!(err.contains(why), "{value:?}: {err}");
        }
        // Required in proxy mode, loopback included; not read in any other mode.
        let loopback = proxy_config("127.0.0.1:8080", None);
        let err = resolve_proxy_secret(&loopback.server.auth, &env(Some(SECRET))).unwrap_err();
        assert!(err.contains("required"), "{err}");
        let token = walgit_config::Config::default();
        assert_eq!(
            resolve_proxy_secret(&token.server.auth, &env(None)),
            Ok(None)
        );
    }

    #[tokio::test]
    async fn owner_scope_narrows_what_exists_for_the_caller() {
        assert_eq!(OwnerScope::parse("*"), Some(OwnerScope::All));
        assert_eq!(OwnerScope::parse(" * "), Some(OwnerScope::All));
        assert_eq!(
            OwnerScope::parse("acme, tools-2,,"),
            Some(OwnerScope::Only(vec!["acme".into(), "tools-2".into()]))
        );
        assert_eq!(OwnerScope::parse(""), Some(OwnerScope::Only(vec![])));
        for bad in ["*,acme", "acme/app", "../x", ".hidden", "a b"] {
            assert_eq!(OwnerScope::parse(bad), None, "{bad}");
        }
        let only = OwnerScope::Only(vec!["acme".into()]);
        assert!(only.contains("acme") && !only.contains("Acme") && !only.contains("other"));
        assert!(!OwnerScope::Only(vec![]).contains("acme"));

        let auth = sidecar();
        let scoped = |owners: &str| {
            asserted(&[
                (PRINCIPAL_HEADER, "dev@example.com"),
                (PROXY_ACCESS_HEADER, "read"),
                (PROXY_OWNERS_HEADER, owners),
            ])
        };
        let p = auth.authenticate(&scoped("acme,tools")).await.unwrap();
        assert!(p.sees_owner("acme") && p.sees_owner("tools") && !p.sees_owner("other"));
        assert!(auth.hides_owner(&scoped("acme"), "other"));
        assert!(!auth.hides_owner(&scoped("acme"), "acme"));
        assert!(!auth.hides_owner(&scoped("*"), "other"));
        assert!(auth.hides_owner(&scoped(""), "acme"), "empty = no owners");
        assert!(matches!(
            auth.authenticate(&scoped("acme/app")).await,
            Err(AuthError::Forbidden)
        ));
        // Unauthenticated requests are not hidden: their handler answers 401/403.
        assert!(!auth.hides_owner(&asserted(&[(PROXY_OWNERS_HEADER, "acme")]), "other"));
    }

    /// The proxy's headers mean nothing in any other mode: no access level, no scope, no
    /// secret is read, whoever sends them.
    #[tokio::test]
    async fn proxy_headers_are_ignored_outside_proxy_mode() {
        let forged = [
            (PROXY_ACCESS_HEADER, "admin"),
            (PROXY_OWNERS_HEADER, "nobody"),
            (PROXY_SECRET_HEADER, SECRET),
        ];

        let mut cfg = walgit_config::Config::default();
        cfg.server.auth.mode = AuthMode::Token;
        cfg.server.auth.tokens = vec![StaticToken {
            principal: "alice".into(),
            token: "s3cret".into(),
            token_env: None,
            write: false,
            admin: false,
        }];
        let auth = proxy_auth(&cfg, Some(SECRET));
        let mut h = bare(&forged);
        h.insert(
            axum::http::header::AUTHORIZATION,
            "Bearer s3cret".parse().unwrap(),
        );
        h.insert(PRINCIPAL_HEADER, "root@example.com".parse().unwrap());
        let p = auth.authenticate(&h).await.unwrap();
        assert_eq!(
            (p.name.as_str(), p.write, p.admin, &p.owners),
            ("alice", false, false, &OwnerScope::All),
            "no trusted forwarder, so not even the principal header is read"
        );
        assert!(!auth.hides_owner(&h, "acme"));
        let anon = auth.authenticate(&bare(&forged)).await.unwrap();
        assert!(anon.anonymous && !anon.write && !anon.admin);

        // `none` keeps its own (loopback-only) forwarding, but never the proxy's scope.
        let auth = proxy_auth(&walgit_config::Config::default(), Some(SECRET));
        let mut h = bare(&forged);
        h.insert(PROXY_ACCESS_HEADER, "read".parse().unwrap());
        let p = auth.authenticate(&h).await.unwrap();
        assert_eq!((p.write, &p.owners), (true, &OwnerScope::All));
        assert!(!auth.hides_owner(&h, "acme"));
    }
}

#[cfg(test)]
mod session_tests {
    use super::*;

    fn auth_with_ttl(ttl_secs: u64) -> Arc<Authenticator> {
        let mut cfg = walgit_config::Config::default();
        cfg.server.auth.mode = AuthMode::Oidc;
        cfg.server.auth.session_secret =
            Some("0123456789abcdef0123456789abcdef-session-secret".into());
        cfg.server.auth.session_ttl = Duration::from_secs(ttl_secs);
        cfg.server.auth.allowed_domains = vec!["example.com".into()];
        Authenticator::new(&cfg)
    }

    fn headers_with(cookie: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(
            "cookie",
            http::HeaderValue::from_str(&format!("{SESSION_COOKIE}={cookie}")).unwrap(),
        );
        h
    }

    fn session(auth: &Authenticator, exp: u64, iat: u64, email: &str) -> String {
        auth.sign(format!("session\n{exp}\n{iat}\n{email}").as_bytes())
            .unwrap()
    }

    #[test]
    fn cookie_is_minted_with_exp_and_iat() {
        let auth = auth_with_ttl(30 * 86400);
        let value = auth.session_cookie_value("u@example.com").unwrap();
        let payload = String::from_utf8(auth.verify_signed(&value).unwrap()).unwrap();
        let parts: Vec<&str> = payload.split('\n').collect();
        assert_eq!(parts[0], "session");
        let (exp, iat): (u64, u64) = (parts[1].parse().unwrap(), parts[2].parse().unwrap());
        assert_eq!(parts[3], "u@example.com");
        assert_eq!(exp - iat, 30 * 86400);
        assert!(unix_now().unwrap().abs_diff(iat) <= 2);
        assert_eq!(
            walgit_config::Config::default().server.auth.session_ttl,
            Duration::from_hours(720)
        );
    }

    #[test]
    fn sessions_slide_after_a_quarter_of_the_ttl_and_policy_still_rules() {
        let auth = auth_with_ttl(400);
        let now = unix_now().unwrap();
        let young = session(&auth, now + 400, now, "u@example.com");
        assert!(
            auth.session_refresh_value(&headers_with(&young)).is_none(),
            "younger than ttl/4: no refresh"
        );
        assert!(auth.authenticate_cookie(&headers_with(&young)).is_some());

        let old = session(&auth, now + 299, now - 101, "u@example.com");
        let fresh = auth
            .session_refresh_value(&headers_with(&old))
            .expect("older than ttl/4: refreshed");
        let payload = String::from_utf8(auth.verify_signed(&fresh).unwrap()).unwrap();
        let new_exp: u64 = payload.split('\n').nth(1).unwrap().parse().unwrap();
        assert!(
            new_exp >= now + 400 - 1,
            "fresh exp = now + ttl, later than the old {}",
            now + 299
        );

        let expired = session(&auth, now - 1, now - 401, "u@example.com");
        assert!(auth.authenticate_cookie(&headers_with(&expired)).is_none());
        assert!(
            auth.session_refresh_value(&headers_with(&expired))
                .is_none()
        );

        let revoked = session(&auth, now + 299, now - 101, "u@elsewhere.org");
        assert!(
            auth.authenticate_cookie(&headers_with(&revoked)).is_none(),
            "policy re-applied"
        );
        assert!(
            auth.session_refresh_value(&headers_with(&revoked))
                .is_none(),
            "no refresh for a revoked principal"
        );
    }

    #[tokio::test]
    async fn a_junk_bearer_is_invalid_and_only_no_credential_falls_back_to_the_cookie() {
        let auth = auth_with_ttl(3600);
        let cookie = auth.session_cookie_value("dev@example.com").unwrap();
        let mut h = HeaderMap::new();
        h.insert(
            "cookie",
            format!("{SESSION_COOKIE}={cookie}").parse().unwrap(),
        );
        h.insert("authorization", "Bearer not-a-jwt".parse().unwrap());
        assert!(matches!(
            auth.authenticate(&h).await.unwrap_err(),
            AuthError::Invalid
        ));
        h.insert(FORWARDED_AUTHORIZATION_HEADER, "".parse().unwrap());
        assert!(matches!(
            auth.authenticate(&h).await.unwrap_err(),
            AuthError::Invalid
        ));
        h.remove("authorization");
        h.remove(FORWARDED_AUTHORIZATION_HEADER);
        assert_eq!(auth.authenticate(&h).await.unwrap().name, "dev@example.com");
        assert!(auth.authenticate(&HeaderMap::new()).await.is_err());
    }
}
