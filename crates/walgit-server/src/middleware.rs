//! HTTP middleware: request id + tracing span. Request timeout, body limit and
//! tracing layers are applied in [`crate::router`] via `tower-http`. Per-repo
//! concurrency limiting lives in the handlers (they hold a repo-keyed semaphore
//! for the duration of the git operation).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use axum::http::{HeaderValue, Request};
use axum::middleware::Next;
use axum::response::Response;
use dashmap::DashMap;
use tokio::sync::Semaphore;
use tracing::Instrument;
use uuid::Uuid;

const REQUEST_ID_HEADER: &str = "x-request-id";

/// HTTP requests in flight on one server — counted from the moment the request enters the
/// middleware until its **response body is dropped** (a streamed fetch, an SSE envelope or a
/// sideband narration is in flight until the last byte, not until the handler returns). The
/// runtime watchdog prints it: a late tick with `inflight = 0` is the process being paused
/// (serverless CPU throttling between requests, memory reclaim), with `inflight > 0` a real stall.
/// Lives on `AppState` (not a static) so several servers in one test process count separately.
#[derive(Default)]
pub struct Inflight(AtomicU64);

impl Inflight {
    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
    fn enter(&self) {
        let n = self.0.fetch_add(1, Ordering::Relaxed) + 1;
        ::metrics::gauge!("walgit_http_inflight").set(n as f64);
    }
    fn leave(&self) {
        let n = self.0.fetch_sub(1, Ordering::Relaxed) - 1;
        ::metrics::gauge!("walgit_http_inflight").set(n as f64);
    }
}

/// Response body wrapper that keeps the request counted until the body is finished or
/// abandoned (dropped on client disconnect too).
struct CountedBody {
    inner: axum::body::Body,
    inflight: Arc<Inflight>,
}

impl Drop for CountedBody {
    fn drop(&mut self) {
        self.inflight.leave();
    }
}

impl http_body::Body for CountedBody {
    type Data = bytes::Bytes;
    type Error = axum::Error;
    fn poll_frame(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        // axum::body::Body is Unpin.
        std::pin::Pin::new(&mut self.get_mut().inner).poll_frame(cx)
    }
    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }
    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.size_hint()
    }
}

/// Generate a request id, attach it to the `http.request` span + response
/// header. The span captures method, path, and trace context from
/// `X-Cloud-Trace-Context` / `traceparent` headers for Cloud Logging
/// correlation. Duration and status are recorded when the response arrives.
pub async fn request_id(
    axum::extract::State(inflight): axum::extract::State<Arc<Inflight>>,
    mut req: Request<axum::body::Body>,
    next: Next,
) -> Response {
    // Honor a caller-supplied id (front → broker; tests) so events correlate
    // with the user-visible request, not an internal hop (docs/EVENTS.md).
    let id = req
        .headers()
        .get(REQUEST_ID_HEADER)
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty())
        .map_or_else(
            || Uuid::new_v4().to_string(),
            std::string::ToString::to_string,
        );
    if let Ok(hv) = HeaderValue::from_str(&id) {
        req.headers_mut().insert(REQUEST_ID_HEADER, hv);
    }
    let method = req.method().clone();
    let path = req.uri().path().to_string();
    inflight.enter();

    // Extract trace context for Cloud Logging correlation (a serverless host's LB
    // always sets X-Cloud-Trace-Context; the layer mints one otherwise).
    let trace_id = crate::telemetry::extract_trace_id(req.headers());
    let user_agent = req
        .headers()
        .get(axum::http::header::USER_AGENT)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.chars().take(200).collect::<String>())
        .unwrap_or_default();

    let span = tracing::info_span!(
        "http.request",
        request_id = %id,
        method = %method,
        path = %path,
        user_agent = %user_agent,
        status = 0u16,
        bytes_in = 0u64,
        bytes_out = 0u64,
        // Recorded later by the authenticator / repo resolution.
        principal = tracing::field::Empty,
        repo = tracing::field::Empty,
        trace_id = tracing::field::Empty,
    );

    if let Some(tid) = &trace_id {
        span.record("trace_id", tid.as_str());
    }

    let resp = next.run(req).instrument(span.clone()).await;
    let mut resp = resp;

    // Record status code on the span.
    span.record("status", resp.status().as_u16());

    if let Ok(hv) = HeaderValue::from_str(&id) {
        resp.headers_mut().insert(REQUEST_ID_HEADER, hv);
    }
    // Stay counted until the body is done (streamed fetches, SSE, sideband narration).
    resp.map(|body| {
        axum::body::Body::new(CountedBody {
            inner: body,
            inflight,
        })
    })
}

/// Per-repo concurrency limiter: `max_concurrent_per_repo` slots per repo id.
#[derive(Clone)]
pub struct RepoSemaphores {
    map: Arc<DashMap<String, Arc<Semaphore>>>,
    max: usize,
}

impl RepoSemaphores {
    pub fn new(max: usize) -> Self {
        Self {
            map: Arc::new(DashMap::new()),
            max,
        }
    }

    /// Acquire a permit for `repo_key`. The permit guards the git operation; drop
    /// it to release the slot.
    pub async fn acquire(&self, repo_key: &str) -> tokio::sync::OwnedSemaphorePermit {
        let sem = self
            .map
            .entry(repo_key.to_string())
            .or_insert_with(|| Arc::new(Semaphore::new(self.max)))
            .clone();
        sem.acquire_owned().await.expect("semaphore never closed")
    }
}
