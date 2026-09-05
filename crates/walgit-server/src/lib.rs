//! Git smart HTTP server (protocol v0/v2), LFS, bundle serving, admin, health, metrics.
//! See AGENTS.md Phase 3.
#![allow(
    clippy::case_sensitive_file_extension_comparisons,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::doc_lazy_continuation,
    clippy::expect_used,
    clippy::if_same_then_else,
    clippy::implicit_hasher,
    clippy::indexing_slicing,
    clippy::many_single_char_names,
    clippy::match_wildcard_for_single_variants,
    clippy::needless_continue,
    clippy::needless_pass_by_value,
    clippy::ref_option,
    clippy::string_slice,
    clippy::struct_field_names,
    clippy::too_many_arguments,
    clippy::trivially_copy_pass_by_ref,
    clippy::type_complexity,
    clippy::unnested_or_patterns,
    clippy::unnecessary_wraps,
    clippy::unused_async,
    clippy::unused_self,
    clippy::unwrap_used,
    clippy::unreadable_literal,
    clippy::used_underscore_binding
)]

pub mod admin;
pub mod auth;
pub mod bridge;
pub mod bundles;
pub mod cache;
pub mod error;
pub mod events;
pub mod follow;
pub mod forward;
pub mod health;
pub mod instance;
pub mod lfs;
pub mod lfs_upstream;
pub mod maintain;
pub mod metrics;
pub mod middleware;
pub mod ops;
pub mod pktline;
pub mod policy;
pub mod prewarm;
pub mod rebuild;
pub mod repo;
pub mod settings;
pub mod setup;
pub mod smart;
pub mod sse;
pub mod static_object;
pub mod stream;
pub mod telemetry;
pub mod tls;
pub mod web;

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::extract::ConnectInfo;
use axum::http::{Method, Request};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use bytes::Bytes;
use metrics_exporter_prometheus::PrometheusHandle;
use walgit_store::DynStore;

use crate::error::ApiError;
use crate::repo::{RepoRoute, parse_repo_route};

/// Shared server state.
pub struct AppState {
    pub cfg: Arc<walgit_config::Config>,
    pub store: DynStore,
    pub registry: Arc<walgit_wal::Registry>,
    pub bundles: Arc<walgit_bundle::Bundler>,
    pub auth: Arc<auth::Authenticator>,
    pub semaphores: middleware::RepoSemaphores,
    /// HTTP requests in flight (counted until the response body is done); on the watchdog line.
    pub inflight: Arc<middleware::Inflight>,
    pub caches: cache::ServerCaches,
    pub metrics_handle: Arc<PrometheusHandle>,
    /// Read-through LFS upstream client (`upstream.lfs`).
    pub lfs_upstream: lfs_upstream::Upstream,
    /// Startup prewarm state (gates /readyz when configured).
    pub readiness: Arc<prewarm::Readiness>,
    /// The events bridge (`events` role, docs/EVENTS.md): WAL → bus
    /// (`webhook`) from a per-repo cursor. None unless in role with a
    /// bus sink configured.
    pub bridge: Option<Arc<bridge::Bridge>>,
    /// Last upstream-follow round per repository on this instance (`[upstream] follow`).
    pub follow: follow::FollowStatuses,
    /// In-process TLS (standalone, D39); `None` behind an edge (h2c).
    pub tls: Option<Arc<tls::Tls>>,
}

impl AppState {
    /// Build a full `AppState` from a config + store (memory or opened backend).
    pub async fn new(
        cfg: Arc<walgit_config::Config>,
        store: DynStore,
    ) -> anyhow::Result<Arc<Self>> {
        let registry = walgit_wal::Registry::new(store.clone(), cfg.clone());
        let bridge = bridge::Bridge::new(&cfg, registry.clone());
        let bundle_source: Arc<dyn walgit_bundle::BundleSource> =
            Arc::new(RegistryBundleSource(registry.clone()));
        let bundles = walgit_bundle::Bundler::new_with_source(bundle_source, cfg.clone());
        let metrics_handle = metrics::install()?;
        let tls = tls::load(&cfg)?;
        if let Some(t) = &tls {
            tracing::info!(fingerprint = %t.fingerprint, mode = ?cfg.server.tls.mode, "TLS terminated in-process");
        }
        Ok(Arc::new(Self {
            cfg: cfg.clone(),
            store,
            registry,
            bundles,
            auth: auth::Authenticator::new(&cfg),
            semaphores: middleware::RepoSemaphores::new(cfg.server.max_concurrent_per_repo),
            inflight: Arc::new(middleware::Inflight::default()),
            caches: cache::ServerCaches::new(&cfg),
            metrics_handle,
            lfs_upstream: lfs_upstream::Upstream::new(),
            readiness: prewarm::Readiness::new(),
            bridge,
            follow: follow::FollowStatuses::default(),
            tls,
        }))
    }
}

/// Build a full axum router.
pub fn router(state: Arc<AppState>) -> Router {
    // Dynamic web responses (JSON API, SPA index/overview) are compressed on
    // the fly; git smart-HTTP, bundles and LFS bytes never are (packs are
    // already compressed, and `Content-Length`/`Range` must stay exact).
    // Embedded `/_ui/assets` arrive precompressed from the build and carry
    // their own `Content-Encoding`, which this layer leaves untouched; SSE is
    // excluded by the layer's default predicate.
    let web_compression = tower_http::compression::CompressionLayer::new()
        .br(true)
        .gzip(true)
        .quality(tower_http::CompressionLevel::Fastest);
    // Nothing with content is public: the SPA shell/assets, installer, credential
    // helper and metrics all sit behind `web::require_auth` (identity from a
    // bearer token or a session cookie). The JSON API and git
    // endpoints do their own `require_read`/`require_write`; `/_auth/*` is the
    // login flow itself; `/services/public/*` is the installer a not-yet-signed-in
    // user needs.
    // `/healthz` and `/readyz` stay open: a startup probe carries no
    // credentials (a 401 there means no revision can ever start — seen 2026-08-20),
    // and they expose only a status word and a pending-prewarm count.
    let gated = Router::new()
        .merge(
            web::ui::router(state.clone())
                .with_state(())
                .layer(web_compression.clone()),
        )
        .route("/metrics", get(metrics::metrics_route))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            web::require_auth,
        ));

    Router::new()
        .merge(
            web::api::router(state.clone())
                .with_state(())
                .layer(web_compression.clone()),
        )
        .merge(
            web::v1::router(state.clone())
                .with_state(())
                .layer(web_compression),
        )
        .merge(gated)
        .route("/healthz", get(health::healthz))
        .route("/readyz", get(health::readyz))
        // The SDK is a static artefact with no data in it; it must load from a
        // `<script>` tag on another site before any session exists (D20).
        .route("/repos.js", get(web::ui::sdk_asset))
        .route("/repos.mjs", get(web::ui::sdk_asset))
        // `/services/public/*` is the one open area: data-free routes only, never a
        // bearer, never repo data — today exactly the installer, everything else 404.
        .merge(web::ui::public_router(state.clone()).with_state(()))
        .merge(web::login::router(state.clone()).with_state(()))
        // Events bridge wake-up (docs/EVENTS.md): the Pub/Sub push envelope of
        // a GCS notification. Authenticated (the push SA's ID token); 404 when
        // this instance is not a bridge.
        .route(
            "/_events/notify",
            axum::routing::post(
                |axum::extract::State(st): axum::extract::State<Arc<AppState>>,
                 headers: axum::http::HeaderMap,
                 body: Body| async move {
                    bridge::http_notify(&st, &headers, body)
                        .await
                        .unwrap_or_else(axum::response::IntoResponse::into_response)
                },
            ),
        )
        .fallback(dispatch)
        // Sliding browser sessions: re-issue a session cookie older than ttl/4.
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            web::login::refresh_session,
        ))
        // CORS for `server.cors_origins` on `/api*` and `/{o}/{r}/api[-browser]/*`
        // (D27: what the SDK emits; nothing is stripped or rewritten).
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            web::v1::cors,
        ))
        // A panicking handler must only fail its own request (500), never the process.
        .layer(tower_http::catch_panic::CatchPanicLayer::custom(
            panic_response,
        ))
        // `Server: walgit/<ver> (<kind>; <who>)` on every response (incl. errors,
        // SSE, git pkt streams): which machine answered, without logs. The UI
        // footer shows the same facts; curl -I shows this.
        .layer(tower_http::set_header::SetResponseHeaderLayer::overriding(
            axum::http::header::SERVER,
            axum::http::HeaderValue::from_static(instance::server_header(&state.cfg)),
        ))
        // The same value under an application-specific name: intermediaries may
        // replace the standard `Server` header, but should preserve this one.
        .layer(tower_http::set_header::SetResponseHeaderLayer::overriding(
            axum::http::HeaderName::from_static("x-walgit-server"),
            axum::http::HeaderValue::from_static(instance::server_header(&state.cfg)),
        ))
        // HTTP/2 carries the host in `:authority`, not `Host`; normalize so every
        // handler can build public URLs from `Host`.
        .layer(axum::middleware::map_request(host_from_authority))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            web::canonical_browser_host,
        ))
        // Outermost: `http.request` span (request_id, trace, principal, repo,
        // status, elapsed) — every log line inside inherits its fields.
        .layer(axum::middleware::from_fn_with_state(
            state.inflight.clone(),
            middleware::request_id,
        ))
        .with_state(state)
}

async fn host_from_authority(mut req: Request<Body>) -> Request<Body> {
    if !req.headers().contains_key(axum::http::header::HOST)
        && let Some(auth) = req.uri().authority().map(std::string::ToString::to_string)
        && let Ok(v) = axum::http::HeaderValue::from_str(&auth)
    {
        req.headers_mut().insert(axum::http::header::HOST, v);
    }
    req
}

fn panic_response(err: Box<dyn std::any::Any + Send + 'static>) -> Response {
    let msg = err
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| {
            err.downcast_ref::<&str>()
                .map(std::string::ToString::to_string)
        })
        .unwrap_or_else(|| "unknown panic".to_string());
    tracing::error!(panic = %msg, "request handler panicked");
    (
        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        "internal error",
    )
        .into_response()
}

/// A 1 s ticker that reports when it was not scheduled on time: blocking
/// work on a tokio worker (prod 2026-08-20: minutes-long stalls that also
/// froze the store deadlines) shows up here as "async runtime stalled" with
/// the gap, instead of only as mysterious slow spans everywhere. A late tick
/// is not proof of a blocked worker: the whole process is paused just the
/// same when a serverless host throttles CPU between requests (a service without
/// `--no-cpu-throttling` doing background work) or the memory cgroup reclaims
/// under tmpfs pressure — so the line carries RSS and an explicit caveat
/// (2026-08-22: 11 stalls on a front during a 11.9 GB background prefetch,
/// every one in a gap between requests, none inside one).
fn spawn_runtime_watchdog(
    tasks: Arc<walgit_wal::tasks::Tasks>,
    inflight: Arc<middleware::Inflight>,
) {
    tokio::spawn(async move {
        let mut last = std::time::Instant::now();
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            let gap = last.elapsed();
            let inflight = inflight.get();
            let tasks_running = tasks.running_count();
            ::metrics::gauge!("walgit_tasks_running").set(tasks_running as f64);
            if gap > std::time::Duration::from_millis(2500) {
                ::metrics::counter!("walgit_runtime_stall_total").increment(1);
                ::metrics::histogram!("walgit_runtime_stall_seconds").record(gap.as_secs_f64());
                let rss_mb = std::fs::read_to_string("/proc/self/statm")
                    .ok()
                    .and_then(|s| {
                        s.split_whitespace()
                            .nth(1)
                            .and_then(|p| p.parse::<u64>().ok())
                    })
                    .map(|pages| pages * 4096 / (1024 * 1024));
                tracing::warn!(
                    gap_ms = u64::try_from(gap.as_millis()).unwrap_or(u64::MAX),
                    inflight,
                    tasks_running,
                    lock_wait_max_ms = walgit_wal::lockwait::max_wait_ms(),
                    rss_mb,
                    "async runtime stalled (inflight = 0: the process was paused — CPU throttling between requests or memory reclaim; inflight > 0: a worker was blocked or starved, trace it)"
                );
            }
            last = std::time::Instant::now();
        }
    });
}

/// Fallback dispatcher: parse `/{owner}/{repo}[.git]/<sub>` and route.
async fn dispatch(
    axum::extract::State(st): axum::extract::State<Arc<AppState>>,
    req: Request<Body>,
) -> Response {
    let path = req.uri().path().to_string();
    let query = req.uri().query().unwrap_or("").to_string();
    let method = req.method().clone();
    let headers = req.headers().clone();
    let peer = request_peer(&req);
    let body = req.into_body();

    let Some(route) = parse_repo_route(&path) else {
        return ApiError::NotFound("no such route".into()).into_response();
    };
    dispatch_route(&st, &route, method, headers, query, body, peer).await
}

pub(crate) fn request_peer(req: &Request<Body>) -> Option<SocketAddr> {
    req.extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|c| c.0)
}

/// Route a parsed repo request (`/{owner}/{repo}[.git]/<sub>` or the same sub
/// under `/{owner}/{repo}/api[-browser]`) to git smart-HTTP, LFS, bundles,
/// repo admin and policy handlers.
pub(crate) async fn dispatch_route(
    st: &Arc<AppState>,
    route: &RepoRoute,
    method: Method,
    headers: axum::http::HeaderMap,
    query: String,
    body: Body,
    peer: Option<SocketAddr>,
) -> Response {
    let mut body = Some(body);
    let sub = route.subpath.as_str();
    let result: Result<Response, ApiError> = async {
        match (&method, sub) {
            (&Method::GET, "info/refs") => {
                let _permit = acquire(st, route).await;
                smart::info_refs(st, route, &headers, &query).await
            }
            (&Method::POST, "git-upload-pack") => {
                let _permit = acquire(st, route).await;
                smart::upload_pack(st, route, &headers, body.take().unwrap()).await
            }
            (&Method::POST, "git-receive-pack") => {
                let _permit = acquire(st, route).await;
                smart::receive_pack(st, route, &headers, body.take().unwrap()).await
            }
            (&Method::POST, "info/lfs/objects/batch") => {
                let bytes = collect_body(body.take().unwrap()).await?;
                lfs::batch(st, route, &headers, bytes).await
            }
            (&Method::GET | &Method::HEAD, s)
                if s.starts_with("info/lfs/objects/") && !s.ends_with("/batch") =>
            {
                lfs::get_object(st, route, &method, &headers, &query, peer).await
            }
            (&Method::PUT, s) if s.starts_with("info/lfs/objects/") => {
                lfs::put_object(st, route, &headers, body.take().unwrap()).await
            }
            (&Method::POST, "info/lfs/verify") => {
                let bytes = collect_body(body.take().unwrap()).await?;
                lfs::verify(st, route, &headers, bytes).await
            }
            (&Method::GET, "bundles/list") => {
                bundles::list(st, route, &headers, &query, true).await
            }
            (&Method::GET, "bundles/catchup") => {
                bundles::list(st, route, &headers, &query, false).await
            }
            (&Method::GET | &Method::HEAD, s)
                if s.starts_with("bundles/") && s != "bundles/list" && s != "bundles/catchup" =>
            {
                bundles::object(st, route, &method, &headers, peer).await
            }
            (&Method::PUT, "") => admin::create(st, route, &headers, &query).await,
            (&Method::DELETE, "") => admin::delete(st, route, &headers).await,
            // Admin routes reach here only through `/{o}/{r}/api[-browser]/…` (web::v1).
            (&Method::GET, "policy") => policy::http_get(st, route, &headers).await,
            (&Method::PUT, "policy") => {
                policy::http_put(st, route, &headers, body.take().unwrap()).await
            }
            (&Method::DELETE, "policy") => policy::http_delete(st, route, &headers).await,
            (&Method::GET, "settings") => settings::http_get(st, route, &headers).await,
            (&Method::GET, "settings/effective") => {
                settings::http_effective(st, route, &headers).await
            }
            (&Method::GET, "settings/history") => settings::http_history(st, route, &headers).await,
            (&Method::GET, "settings/describe") => {
                settings::http_describe(st, route, &headers).await
            }
            (&Method::PUT, "settings") => {
                settings::http_put(st, route, &headers, &query, body.take().unwrap()).await
            }
            (&Method::DELETE, "settings") => settings::http_delete(st, route, &headers).await,
            (&Method::POST, "settings/validate") => {
                settings::http_validate(st, route, &headers, body.take().unwrap()).await
            }
            (&Method::POST, "policy/validate") => {
                settings::http_policy_validate(st, route, &headers, body.take().unwrap()).await
            }
            (&Method::POST, "policy/dry-run") => {
                settings::http_policy_dry_run(st, route, &headers, &query, body.take().unwrap())
                    .await
            }
            _ => Err(ApiError::NotFound(format!("no route for {method} {sub}"))),
        }
    }
    .await;

    match result {
        Ok(r) => r,
        Err(e) => e.into_response(),
    }
}

async fn acquire(st: &Arc<AppState>, route: &RepoRoute) -> tokio::sync::OwnedSemaphorePermit {
    st.semaphores.acquire(&route.id.to_string()).await
}

pub(crate) async fn collect_body(body: Body) -> Result<Bytes, ApiError> {
    to_bytes(body, 64 * 1024 * 1024)
        .await
        .map_err(|e| ApiError::BadRequest(format!("body read: {e}")))
}

/// One or two TCP listeners. A loopback bind also takes the other family on the
/// same port (`127.0.0.1` ⇔ `::1`) so `*.localhost` works in browsers, which
/// resolve the name to IPv6 first (a v4-only bind looks like connection refused).
pub(crate) struct TcpAccept {
    listeners: Vec<tokio::net::TcpListener>,
}

impl TcpAccept {
    pub async fn bind(addr: std::net::SocketAddr) -> anyhow::Result<Self> {
        let first = tokio::net::TcpListener::bind(addr).await?;
        let bound = first.local_addr()?;
        let mut listeners = vec![first];
        if let Some(twin) = loopback_twin(bound) {
            match tokio::net::TcpListener::bind(twin).await {
                Ok(l) => listeners.push(l),
                Err(e) => tracing::debug!(%twin, error = %e, "loopback twin not bound"),
            }
        }
        Ok(Self { listeners })
    }

    pub async fn accept(
        &mut self,
    ) -> std::io::Result<(tokio::net::TcpStream, std::net::SocketAddr)> {
        match self.listeners.as_mut_slice() {
            [a] => a.accept().await,
            [a, b] => tokio::select! {
                r = a.accept() => r,
                r = b.accept() => r,
            },
            _ => unreachable!("TcpAccept always has 1 or 2 sockets"),
        }
    }

    pub fn local_addr(&self) -> std::io::Result<std::net::SocketAddr> {
        self.listeners[0].local_addr()
    }

    pub fn addrs(&self) -> Vec<std::net::SocketAddr> {
        self.listeners
            .iter()
            .filter_map(|l| l.local_addr().ok())
            .collect()
    }
}

fn loopback_twin(addr: std::net::SocketAddr) -> Option<std::net::SocketAddr> {
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
    match addr.ip() {
        IpAddr::V4(v) if v.is_loopback() => Some((Ipv6Addr::LOCALHOST, addr.port()).into()),
        IpAddr::V6(v) if v.is_loopback() => Some((Ipv4Addr::LOCALHOST, addr.port()).into()),
        _ => None,
    }
}

/// axum `Listener` over [`TcpAccept`] (the dual-stack loopback binder). Nagle
/// is disabled per-connection via `.tap_io(set_nodelay)` at the serve site.
struct NodelayListener(TcpAccept);

impl axum::serve::Listener for NodelayListener {
    type Io = tokio::net::TcpStream;
    type Addr = std::net::SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        loop {
            match self.0.accept().await {
                Ok((stream, addr)) => return (stream, addr),
                Err(e) => {
                    tracing::warn!(error = ?e, "TCP accept failed");
                }
            }
        }
    }

    fn local_addr(&self) -> std::io::Result<Self::Addr> {
        self.0.local_addr()
    }
}

/// Enable `TCP_NODELAY` on an accepted stream. Applied via `Listener::tap_io` so
/// the connection stays a plain `TcpStream` and axum's blanket `Connected` impl
/// for `TapIo` supplies the peer `SocketAddr` to `ConnectInfo` (used by the
/// accel-redirect loopback check). Git's receive-pack status is many small
/// pkt-lines; leaving Nagle on turns those into delayed-ACK stalls.
fn set_nodelay(stream: &mut tokio::net::TcpStream) {
    if let Err(e) = stream.set_nodelay(true) {
        tracing::debug!(error = ?e, "failed to set TCP_NODELAY");
    }
}

/// Bind, serve (HTTP/1.1 + h2c, or TLS with ALPN h2/http1.1 when
/// `server.tls` is on), graceful shutdown on `shutdown`.
pub async fn serve(
    state: Arc<AppState>,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> anyhow::Result<()> {
    let addr = state.cfg.server.listen;
    let state_for_shutdown = state.clone();
    prewarm::spawn(state.clone());
    bridge::spawn_sweeper(state.clone());
    spawn_runtime_watchdog(state.registry.tasks().clone(), state.inflight.clone());
    let app = router(state);
    let listener = TcpAccept::bind(addr).await?;
    let tls = state_for_shutdown.tls.clone();
    tracing::info!(%addr, addrs = ?listener.addrs(), tls = tls.is_some(), url = %listen_url(&state_for_shutdown.cfg), "walgit-server listening");

    let st = state_for_shutdown.clone();
    let phase2 = Arc::new(tokio::sync::Notify::new());
    let phase2_tx = phase2.clone();
    let graceful = async move {
        shutdown.await;
        // D31 phase 1 — serving untouched: no new unit starts, the running
        // unit is interrupted at once (D22 redoes it; a unit too expensive to
        // redo is made resumable, not awaited), and we wait for it to be gone.
        walgit_wal::tasks::begin_drain();
        let interrupted = st.registry.tasks().interrupt_where(crate::ops::is_op);
        tracing::info!(units = ?interrupted, "shutdown signal received: units interrupted, serving continues");
        let deadline = std::time::Instant::now() + UNIT_STOP_MAX;
        loop {
            let running: Vec<_> = st
                .registry
                .tasks()
                .running_all()
                .into_iter()
                .filter(|t| crate::ops::is_op(&t.kind))
                .collect();
            if running.is_empty() {
                break;
            }
            if std::time::Instant::now() >= deadline {
                tracing::warn!(count = running.len(), kinds = ?running.iter().map(|t| format!("{}:{}", t.repo, t.kind)).collect::<Vec<_>>(), bound = ?UNIT_STOP_MAX, "shutdown: a unit did not stop within the bound; proceeding");
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        // D31 phase 2 — serving drain: readyz 503, new object work refused
        // with 503 + Retry-After; in-flight requests get `server.drain_timeout`.
        walgit_wal::tasks::begin_shutdown();
        phase2_tx.notify_one();
        tracing::info!(drain_timeout = ?st.cfg.server.drain_timeout, "shutdown: serving drain (readyz 503, new object work refused); in-flight requests finish");
        // A beat for the edge/LB to see the 503 before the listener closes.
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    };
    let serving = async move {
        if let Some(t) = tls {
            axum::serve(
                tls::TlsListener {
                    tcp: listener,
                    acceptor: t.acceptor.clone(),
                },
                app,
            )
            .with_graceful_shutdown(graceful)
            .await
        } else {
            use axum::serve::ListenerExt;
            axum::serve(
                NodelayListener(listener).tap_io(set_nodelay),
                app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
            )
            .with_graceful_shutdown(graceful)
            .await
        }
    };
    // In-flight requests get `server.drain_timeout` from phase 2 on (a stuck
    // sideband stream must not hold the restart): axum waits for open
    // connections, we cap that.
    let bound = state_for_shutdown.cfg.server.drain_timeout;
    tokio::select! {
        r = serving => r?,
        () = async { phase2.notified().await; tokio::time::sleep(bound).await } => {
            tracing::warn!(?bound, "shutdown: in-flight requests still open past server.drain_timeout; exiting");
        }
    }
    Ok(())
}

/// The origin this process answers at: `server.public_url`, else scheme from
/// `server.tls` + the listen address. Loopback is advertised as `walgit.localhost`
/// (browsers on `localhost` 302 here).
pub fn listen_url(cfg: &walgit_config::Config) -> String {
    if let Some(u) = &cfg.server.public_url {
        return u.trim_end_matches('/').to_string();
    }
    let scheme = if cfg.tls_enabled() { "https" } else { "http" };
    let ip = cfg.server.listen.ip();
    let host = if ip.is_loopback() || ip.is_unspecified() {
        "walgit.localhost".to_string()
    } else if ip.is_ipv6() {
        format!("[{ip}]")
    } else {
        ip.to_string()
    };
    format!("{scheme}://{host}:{}", cfg.server.listen.port())
}

/// Phase-1 bound: how long an interrupted unit may take to be gone (its future
/// is dropped on abort; a blocking git child is left to die with the container).
const UNIT_STOP_MAX: std::time::Duration = std::time::Duration::from_secs(30);

/// Server-side adapter implementing `walgit_bundle::BundleSource` over a
/// `walgit_wal::Registry`. Defined here (server owns the type) so the orphan
/// rule is satisfied; lets us use `Bundler::new_with_source` without waiting for
/// a Registry impl in the bundle crate.
struct RegistryBundleSource(Arc<walgit_wal::Registry>);

#[async_trait::async_trait]
impl walgit_bundle::BundleSource for RegistryBundleSource {
    async fn open_repo(
        &self,
        id: &walgit_git::RepoId,
    ) -> Result<walgit_bundle::BundleRepoHandle, walgit_bundle::BundleError> {
        let h = self.0.open(id).await.map_err(|e| match e {
            walgit_wal::WalError::NotFound => {
                walgit_bundle::BundleError::RepoNotFound(id.to_string())
            }
            other => walgit_bundle::BundleError::Other(other.to_string()),
        })?;
        Ok(walgit_bundle::BundleRepoHandle {
            local: h.local().clone(),
            store: h.store().clone(),
            head_seq: h.manifest().head_seq,
            engine: walgit_bundle::BundleEngine::Git,
            cfg: Some(h.effective_config()),
        })
    }

    async fn prepare_objects(
        &self,
        id: &walgit_git::RepoId,
    ) -> Result<(), walgit_bundle::BundleError> {
        // `git bundle create` streams from the local copy: bring the packs
        // here first (Serve level). Registry::open alone is refs-level — the
        // maintainer built from it and git said "bad object refs/heads/main".
        // Too-large repos fail with the "larger than this instance" message
        // that run_all_due treats as "skipped, the VM job builds those".
        // Never from open_repo: list/advert callers hold a read guard and a
        // sync here would deadlock on the repo's write lock.
        let h = self
            .0
            .open(id)
            .await
            .map_err(|e| walgit_bundle::BundleError::Other(e.to_string()))?;
        drop(
            h.sync()
                .await
                .map_err(|e| walgit_bundle::BundleError::Other(e.to_string()))?,
        );
        Ok(())
    }

    /// gix (+ remote faulter) when the base is remote-served or linked from
    /// the store mount (stock git would read every boundary tree through the
    /// mount or fail), git on a complete local copy.
    async fn engine(&self, id: &walgit_git::RepoId) -> walgit_bundle::BundleEngine {
        let Ok(h) = self.0.open(id).await else {
            return walgit_bundle::BundleEngine::Git;
        };
        if !h.remote_served().is_empty() {
            match h.remote_reader().await {
                Ok(reader) => {
                    return walgit_bundle::BundleEngine::Gix {
                        faulter: Some(Arc::new(walgit_wal::remote::Faulter::new(
                            reader,
                            h.local().clone(),
                        ))),
                    };
                }
                Err(e) => {
                    tracing::warn!(repo = %id, error = %e, "remote reader unavailable for bundle build; using git");
                }
            }
        }
        let linked = h.local().packs().is_ok_and(|ps| {
            ps.iter()
                .any(|p| h.local().pack_path(&p.checksum).is_symlink())
        });
        if linked {
            return walgit_bundle::BundleEngine::Gix { faulter: None };
        }
        walgit_bundle::BundleEngine::Git
    }

    async fn refs_as_of(
        &self,
        id: &walgit_git::RepoId,
        at: std::time::SystemTime,
    ) -> Result<Option<(walgit_git::RefSnapshotData, u64)>, walgit_bundle::BundleError> {
        let h = self
            .0
            .open(id)
            .await
            .map_err(|e| walgit_bundle::BundleError::Other(e.to_string()))?;
        let (snap, seq) = h
            .refs_as_of(at)
            .await
            .map_err(|e| walgit_bundle::BundleError::Other(e.to_string()))?;
        if seq == 0 {
            return Ok(None); // nothing at that time (or predates replayable history)
        }
        Ok(Some((snap.into(), seq)))
    }

    async fn list_repos(&self) -> Result<Vec<walgit_git::RepoId>, walgit_bundle::BundleError> {
        Ok(self
            .0
            .list()
            .await
            .map_err(|e| walgit_bundle::BundleError::Other(e.to_string()))?)
    }
}

#[cfg(test)]
mod listen_tests {
    #[tokio::test]
    async fn ipv4_loopback_also_accepts_ipv6() {
        let m = super::TcpAccept::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let port = m.local_addr().unwrap().port();
        if !m.addrs().iter().any(std::net::SocketAddr::is_ipv6) {
            return; // no IPv6 on this host
        }
        tokio::net::TcpStream::connect((std::net::Ipv6Addr::LOCALHOST, port))
            .await
            .expect("::1 twin");
    }
}
