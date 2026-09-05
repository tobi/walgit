//! Prometheus metrics exporter. Installs a recorder up front (once per process)
//! and exposes the rendered scrape via `/metrics`.

use std::sync::{Arc, OnceLock};

use axum::extract::State;
use axum::response::IntoResponse;
use metrics_exporter_prometheus::PrometheusHandle;

use crate::AppState;

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
pub async fn metrics_route(State(st): State<Arc<AppState>>) -> impl IntoResponse {
    let body = st.metrics_handle.render();
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4",
        )],
        body,
    )
}
