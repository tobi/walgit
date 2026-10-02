//! HTTP error responses. Git-protocol user errors (non-fast-forward, bad ref)
//! are *not* mapped here: they are reported as `unpack`/`ng` pkt-lines inside a
//! 200 response per the smart HTTP contract. Only transport/auth/routing errors
//! become HTTP error statuses.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

/// Body of [`ApiError::UntrustedProxy`] (and of the in-band git `ERR` for it).
pub const UNTRUSTED_PROXY_MESSAGE: &str = "forbidden: the request did not come through walgit's identity-aware proxy \
     (X-Walgit-Proxy-Secret missing or wrong): the proxy in front of walgit is misconfigured, or the request \
     bypassed it. Your credential was not rejected; the operator must fix the proxy";

#[derive(Debug)]
pub enum ApiError {
    NotFound(String),
    BadRequest(String),
    Unauthorized,
    Forbidden,
    /// `proxy` mode: the request did not prove it came through the proxy
    /// (`AuthError::UntrustedProxy`). 403, never 401: git erases the client's stored
    /// credential on a 401, and the credential is not what failed.
    UntrustedProxy,
    Conflict(String),
    PayloadTooLarge,
    UnsupportedMediaType(String),
    ServiceUnavailable(String),
    Internal(String),
}

impl ApiError {
    pub fn status(&self) -> StatusCode {
        match self {
            ApiError::NotFound(_) => StatusCode::NOT_FOUND,
            ApiError::BadRequest(_) => StatusCode::BAD_REQUEST,
            ApiError::Unauthorized => StatusCode::UNAUTHORIZED,
            ApiError::Forbidden | ApiError::UntrustedProxy => StatusCode::FORBIDDEN,
            ApiError::Conflict(_) => StatusCode::CONFLICT,
            ApiError::PayloadTooLarge => StatusCode::PAYLOAD_TOO_LARGE,
            ApiError::UnsupportedMediaType(_) => StatusCode::UNSUPPORTED_MEDIA_TYPE,
            ApiError::ServiceUnavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
            ApiError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl ApiError {
    /// The plain-text body / SSE `error` packet message.
    pub fn message(&self) -> String {
        match self {
            ApiError::NotFound(m) => format!("not found: {m}"),
            ApiError::BadRequest(m) => format!("bad request: {m}"),
            ApiError::Unauthorized => "unauthorized".to_string(),
            ApiError::Forbidden => "forbidden".to_string(),
            ApiError::UntrustedProxy => UNTRUSTED_PROXY_MESSAGE.to_string(),
            ApiError::Conflict(m) => format!("conflict: {m}"),
            ApiError::PayloadTooLarge => "payload too large".to_string(),
            ApiError::UnsupportedMediaType(m) => format!("unsupported media type: {m}"),
            ApiError::ServiceUnavailable(m) => format!("service unavailable: {m}"),
            ApiError::Internal(m) => format!("internal error: {m}"),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let msg = self.message();
        let status = self.status();
        // Every 5xx is an operator-facing event: log it with its text (the
        // access log carries status only; a 500 whose reason lives solely in
        // the client's terminal is undebuggable).
        if status.is_server_error() {
            tracing::warn!(status = status.as_u16(), error = %msg, "request failed");
        }
        let mut resp = (status, msg).into_response();
        // RFC 6750: a 401 from a Bearer-protected resource MUST include
        // WWW-Authenticate. Auth failures (JWKS) surface as 401/503.
        // RFC 6750: Bearer only. Never offer Basic: browsers would show a
        // password dialog and there is no password-based way in.
        if self.status() == StatusCode::UNAUTHORIZED {
            resp.headers_mut().insert(
                axum::http::header::WWW_AUTHENTICATE,
                "Bearer realm=\"walgit\"".parse().unwrap(),
            );
        }
        // 503s are transient by contract (placement refusal during a fallback,
        // a store deadline, a warming copy): say when to come back.
        if status == StatusCode::SERVICE_UNAVAILABLE {
            resp.headers_mut().insert(
                axum::http::header::RETRY_AFTER,
                axum::http::HeaderValue::from_static("15"),
            );
        }
        resp
    }
}

impl From<walgit_store::StoreError> for ApiError {
    fn from(e: walgit_store::StoreError) -> Self {
        match e {
            walgit_store::StoreError::NotFound { key } => ApiError::NotFound(key),
            other => ApiError::Internal(other.to_string()),
        }
    }
}
