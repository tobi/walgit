//! Receive-pack forwarding to a push broker (`wal.push_broker_url`): one warm
//! writer that batches the manifest CAS for many small repositories.
//!
//! The request body and broker response are deliberately kept as streams.  A
//! front falls back to its local receive-pack path only when no broker response
//! was obtained, or when the broker explicitly reports a gateway-unavailable
//! status before a response body is consumed.
//!
//! The hop authenticates with `wal.push_broker_token` (or `WALGIT_BROKER_TOKEN`):
//! a static token the broker lists under `server.auth.tokens` with `write = true`
//! and whose principal is in its `trusted_forwarders`, so the end user's identity
//! travels in `X-Walgit-Principal`. A loopback broker needs no token.

use std::time::Instant;

use axum::{
    body::Body,
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::Response,
};
use futures::StreamExt;

use crate::{auth::Principal, repo::RepoRoute};

/// The result of trying to forward one receive-pack body.
pub enum ForwardOutcome {
    /// A broker response was obtained and can be returned to the Git client.
    Response(Response),
    /// The broker was not reached or returned a gateway-unavailable response.
    /// The caller still owns the body only when the broker was not reached before
    /// any bytes were sent; smart.rs therefore buffers only the local fallback
    /// path and never retries after an acknowledged broker request.
    Fallback,
}

/// Stream one receive-pack request to the broker and stream its response back.
/// `X-Walgit-Forwarded: 1` is added by the caller and must not be forwarded again.
#[tracing::instrument(name = "push.forward", skip_all)]
pub async fn receive_pack(
    broker_url: &str,
    route: &RepoRoute,
    headers: &HeaderMap,
    body: Body,
    principal: &Principal,
    broker_token: Option<&str>,
) -> ForwardOutcome {
    let started = Instant::now();
    let endpoint = format!(
        "{}/{}/{}.git/git-receive-pack",
        broker_url.trim_end_matches('/'),
        route.id.owner(),
        route.id.name()
    );
    let client = reqwest::Client::new();
    let stream = body
        .into_data_stream()
        .map(|chunk| chunk.map_err(|e| std::io::Error::other(e.to_string())));
    let mut request = client
        .post(&endpoint)
        .body(reqwest::Body::wrap_stream(stream));

    for name in [
        header::CONTENT_TYPE,
        header::CONTENT_ENCODING,
        header::EXPECT,
        header::CONTENT_RANGE,
        header::HeaderName::from_static("git-protocol"),
        header::HeaderName::from_static("x-request-id"),
    ] {
        if let Some(value) = headers.get(&name) {
            request = request.header(name, value);
        }
    }
    request = request.header("X-Walgit-Forwarded", "1");
    if let Ok(value) = HeaderValue::try_from(principal.name.as_str()) {
        request = request.header("X-Walgit-Principal", value);
    }

    if !is_local_broker(broker_url) {
        let token = std::env::var("WALGIT_BROKER_TOKEN")
            .ok()
            .filter(|v| !v.is_empty())
            .or_else(|| broker_token.map(str::to_string).filter(|v| !v.is_empty()));
        if let Some(token) = token {
            request = request.bearer_auth(token);
        } else {
            tracing::warn!(
                elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
                "push broker token unset (wal.push_broker_token / WALGIT_BROKER_TOKEN); falling back"
            );
            return ForwardOutcome::Fallback;
        }
    }

    let response = match request.send().await {
        Ok(response) => response,
        Err(error) => {
            tracing::warn!(%error, elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX), "push broker unavailable; falling back");
            return ForwardOutcome::Fallback;
        }
    };
    if matches!(
        response.status(),
        StatusCode::BAD_GATEWAY | StatusCode::SERVICE_UNAVAILABLE | StatusCode::GATEWAY_TIMEOUT
    ) {
        tracing::warn!(
            status = response.status().as_u16(),
            elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
            "push broker gateway failure; falling back"
        );
        return ForwardOutcome::Fallback;
    }

    let status = response.status();
    let response_headers = response.headers().clone();
    let stream = response
        .bytes_stream()
        .map(|chunk| chunk.map_err(|e| std::io::Error::other(e.to_string())));
    let mut builder = Response::builder().status(status);
    for name in [
        header::CONTENT_TYPE,
        header::CONTENT_ENCODING,
        header::CACHE_CONTROL,
        header::ETAG,
    ] {
        if let Some(value) = response_headers.get(&name) {
            builder = builder.header(name, value);
        }
    }
    let output = builder
        .body(Body::from_stream(stream))
        .unwrap_or_else(|_| Response::new(Body::empty()));
    let outcome = if status.is_success() { "ok" } else { "error" };
    metrics::counter!("walgit_push_forwarded_total", "outcome" => outcome).increment(1);
    tracing::info!(
        status = status.as_u16(),
        elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        "push broker response streamed"
    );
    ForwardOutcome::Response(output)
}

fn is_local_broker(url: &str) -> bool {
    let Ok(url) = reqwest::Url::parse(url) else {
        return false;
    };
    matches!(url.host_str(), Some("127.0.0.1" | "localhost" | "::1"))
}
