//! Prometheus metrics exporter. When enabled, installs a recorder once per
//! process and exposes the rendered scrape via `/metrics`.

use std::sync::{Arc, OnceLock};

use axum::Extension;
use axum::response::IntoResponse;
use metrics_exporter_prometheus::PrometheusHandle;

static HANDLE: OnceLock<Arc<PrometheusHandle>> = OnceLock::new();

/// Install the Prometheus recorder once per process and return a shared handle.
/// Safe to call repeatedly (subsequent calls return the same handle).
pub fn install() -> anyhow::Result<Arc<PrometheusHandle>> {
    use metrics_exporter_prometheus::PrometheusBuilder;

    if let Some(h) = HANDLE.get() {
        return Ok(h.clone());
    }

    let rec = PrometheusBuilder::new().build_recorder();
    let handle = Arc::new(rec.handle());
    // set_global_recorder fails if already set; ignore that race — the handle is
    // what we actually need.
    let _ = metrics::set_global_recorder(Box::new(rec));
    // Two callers may race here; OnceLock keeps the first, both handles are
    // identical (same recorder), so dropping the second is harmless.
    let _ = HANDLE.set(handle.clone());
    Ok(HANDLE.get().unwrap_or(&handle).clone())
}

/// `GET /metrics`
pub async fn metrics_route(
    Extension(handle): Extension<Arc<PrometheusHandle>>,
) -> impl IntoResponse {
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4",
        )],
        handle.render(),
    )
        .into_response()
}
