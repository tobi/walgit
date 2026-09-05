//! Tracing initialisation for Cloud Logging structured logs.
//!
//! When `telemetry.log_format = json`, emits one JSON object per line to
//! stdout compatible with a serverless host / Cloud Logging:
//!
//! * `severity` — DEBUG / INFO / WARNING / ERROR
//! * `message` — event message or span name
//! * `time` — RFC3339 timestamp
//! * `logging.googleapis.com/trace` — `projects/<PROJECT>/traces/<trace-id>`
//!   (parsed from the incoming `X-Cloud-Trace-Context` or `traceparent`
//!   header and recorded as a span field)
//! * `elapsed_ms` — wall-clock duration (span-close lines only)
//! * `span: {name, kind}` — e.g. `http.request`/`http`, `wal.sync`/`wal`,
//!   `store.get`/`store`, `task.run`/`task`. Nested on purpose: Logs Explorer
//!   addresses it as `jsonPayload.span.name` (a literal dotted key would need
//!   `jsonPayload."span.name"` and is easy to get wrong).
//! * All span fields of the *whole ancestor chain* flattened to top-level keys
//!   (innermost wins): `repo`, `principal`, `request_id`, `method`, `path`,
//!   `status`, `task_id`, `task_kind`, `bytes_in`, `bytes_out`, …  A
//!   `store.get` inside a request therefore carries the request's `repo`,
//!   `principal` and trace.
//! * Every line carries `logging.googleapis.com/trace`: the incoming request
//!   trace when there is one, otherwise a trace id minted for the root span
//!   (background tasks, prewarm, compaction) so one job groups in the UI.
//!
//! Performance spans (INFO and above) emit a log line on span *close* so they
//! are queryable in Cloud Logging:
//!   `jsonPayload.span.name="git.upload_pack" AND jsonPayload.elapsed_ms > 1000`

use std::collections::HashMap;
use std::io::{self, Write};
use std::sync::OnceLock;
use std::time::Instant;

use parking_lot::Mutex;
use serde_json::{Map, Value, json};
use tracing::span::Id as SpanId;
use tracing::{
    Event, Level, Subscriber,
    field::{Field, Visit},
};
use tracing_subscriber::{
    EnvFilter,
    layer::{Context, Layer},
    registry::LookupSpan,
};

use walgit_config::{Config, LogFormat};

// ---------------------------------------------------------------------------
// Project ID resolution
// ---------------------------------------------------------------------------

/// Resolve the GCP project id for trace correlation.
/// Priority: config → `GOOGLE_CLOUD_PROJECT` env → metadata server → None.
fn resolve_project_id(cfg: &Config) -> Option<String> {
    if let Some(p) = &cfg.telemetry.trace_project {
        return Some(p.clone());
    }
    if let Ok(p) = std::env::var("GOOGLE_CLOUD_PROJECT")
        && !p.is_empty()
    {
        return Some(p);
    }
    // Probe metadata only when the documented GCE override is present. Off-GCP,
    // resolving metadata.google.internal can otherwise stall startup.
    #[cfg(not(test))]
    if std::env::var_os("GCE_METADATA_HOST").is_some_and(|v| !v.is_empty())
        && let Some(p) = fetch_project_from_metadata()
    {
        return Some(p);
    }
    None
}

#[cfg(not(test))]
fn fetch_project_from_metadata() -> Option<String> {
    let rt = tokio::runtime::Runtime::new().ok()?;
    rt.block_on(async {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(2))
            .build()
            .ok()?;
        let resp = client
            .get("http://metadata.google.internal/computeMetadata/v1/project/project-id")
            .header("Metadata-Flavor", "Google")
            .send()
            .await
            .ok()?;
        if resp.status().is_success() {
            let text = resp.text().await.ok()?;
            let trimmed = text.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }
        None
    })
}

// ---------------------------------------------------------------------------
// CloudLoggingLayer — custom tracing layer
// ---------------------------------------------------------------------------

/// Per-span data tracked by [`CloudLoggingLayer`].
struct SpanData {
    start: Instant,
    /// Last `on_exit`: when the span's future last ran. `elapsed_ms` is measured
    /// to this instant, never to `on_close`: tracing's registry defers a parent's
    /// close while any child span is alive, and the GCS SDK's internal spans
    /// under `store.put` linger until the stream idles (~3 min) — prod
    /// 2026-08-21: a 1.2 s bundle upload logged `elapsed_ms = 173700`, and
    /// `maintain.unit` with it.
    last_exit: Option<Instant>,
    name: &'static str,
    level: Level,
    /// Cloud Logging spanId (16-char hex).
    cloud_span_id: String,
    /// Trace id (32-char hex): inherited from the parent, taken from the
    /// span's own `trace_id` field, or minted for a root span.
    trace_id: String,
    /// Recorded fields (name → JSON value).
    fields: Map<String, Value>,
}

/// A custom `tracing` layer that emits Cloud Logging structured JSON.
///
/// * **Events** produce a JSON line immediately with `severity` = event level.
/// * **Spans** at INFO or above produce a JSON line on close with `elapsed_ms`,
///   `span.{name,kind}`, and the fields of the span and all its ancestors.
/// * Spans below INFO are tracked for context inheritance but do not emit
///   a close log line.
pub struct CloudLoggingLayer {
    project_id: Option<String>,
    spans: Mutex<HashMap<SpanId, SpanData>>,
    /// Test sink: when set, records go here instead of stdout.
    sink: Option<std::sync::Arc<Mutex<Vec<Map<String, Value>>>>>,
}

const HIDDEN_FIELDS: &[&str] = &["trace_id"];

impl CloudLoggingLayer {
    pub fn new(project_id: Option<String>) -> Self {
        Self {
            project_id,
            spans: Mutex::new(HashMap::new()),
            sink: None,
        }
    }

    /// A layer that collects records in memory (tests of the JSON shape).
    #[cfg(test)]
    pub fn with_sink(sink: std::sync::Arc<Mutex<Vec<Map<String, Value>>>>) -> Self {
        Self {
            project_id: None,
            spans: Mutex::new(HashMap::new()),
            sink: Some(sink),
        }
    }

    /// Derive `span.kind` from the span name prefix.
    fn span_kind(name: &str) -> &'static str {
        match name.split('.').next().unwrap_or("") {
            "http" => "http",
            "wal" => "wal",
            "store" => "store",
            "git" => "git",
            "task" => "task",
            "push" => "push",
            "maintain" => "maintain",
            "bundle" => "bundle",
            "remote" => "remote",
            _ => "other",
        }
    }

    /// Emit a JSON log line to stdout.
    fn emit(&self, record: &Map<String, Value>) {
        if let Some(sink) = &self.sink {
            sink.lock().push(record.clone());
            return;
        }
        let line = serde_json::to_string(record).unwrap_or_else(|_| "{}".into());
        let stdout = io::stdout();
        let mut guard = stdout.lock();
        let _ = guard.write_all(line.as_bytes());
        let _ = guard.write_all(b"\n");
    }

    /// Build the base JSON record with standard Cloud Logging fields.
    fn base_record(&self, severity: &str, message: &str) -> Map<String, Value> {
        let mut map = Map::new();
        map.insert("severity".into(), json!(severity));
        map.insert("message".into(), json!(message));
        map.insert(
            "time".into(),
            json!(
                chrono::Utc::now()
                    .format("%Y-%m-%dT%H:%M:%S%.9fZ")
                    .to_string()
            ),
        );
        if let Ok(name) = std::env::var("WALGIT_INSTANCE_NAME") {
            map.insert("service".into(), json!(name));
        }
        map
    }

    fn put_trace(&self, record: &mut Map<String, Value>, trace_id: &str, span_id: &str) {
        if let Some(project) = &self.project_id {
            record.insert(
                "logging.googleapis.com/trace".into(),
                json!(format!("projects/{}/traces/{}", project, trace_id)),
            );
        } else {
            record.insert("trace_id".into(), json!(trace_id));
        }
        record.insert("logging.googleapis.com/spanId".into(), json!(span_id));
    }

    /// Flatten the fields of `chain` (root → leaf; innermost wins) into `record`.
    fn merge_chain_fields<'a>(
        record: &mut Map<String, Value>,
        chain: impl Iterator<Item = &'a SpanData>,
    ) {
        for data in chain {
            for (k, v) in &data.fields {
                if !HIDDEN_FIELDS.contains(&k.as_str()) {
                    record.insert(k.clone(), v.clone());
                }
            }
        }
    }
}

impl<S> Layer<S> for CloudLoggingLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &tracing::span::Attributes<'_>, id: &SpanId, ctx: Context<'_, S>) {
        let metadata = attrs.metadata();
        let mut visitor = FieldCollector::default();
        attrs.record(&mut visitor);

        let own_trace = visitor
            .values
            .get("trace_id")
            .and_then(|v| v.as_str())
            .map(std::string::ToString::to_string);
        let parent_trace = ctx
            .span(id)
            .and_then(|s| s.parent())
            .and_then(|p| self.spans.lock().get(&p.id()).map(|d| d.trace_id.clone()));
        let trace_id = own_trace.or(parent_trace).unwrap_or_else(generate_trace_id);

        let data = SpanData {
            start: Instant::now(),
            last_exit: None,
            name: metadata.name(),
            level: *metadata.level(),
            cloud_span_id: generate_span_id(),
            trace_id,
            fields: visitor.values,
        };
        self.spans.lock().insert(id.clone(), data);
    }

    fn on_exit(&self, id: &SpanId, _ctx: Context<'_, S>) {
        if let Some(d) = self.spans.lock().get_mut(id) {
            d.last_exit = Some(Instant::now());
        }
    }

    fn on_record(&self, id: &SpanId, values: &tracing::span::Record<'_>, _ctx: Context<'_, S>) {
        let mut spans = self.spans.lock();
        if let Some(data) = spans.get_mut(id) {
            let mut visitor = FieldCollector {
                values: std::mem::take(&mut data.fields),
            };
            values.record(&mut visitor);
            data.fields = visitor.values;
            // A trace id recorded late (http.request records it after
            // parsing headers) must win over the minted one.
            if let Some(t) = data.fields.get("trace_id").and_then(|v| v.as_str()) {
                data.trace_id = t.to_string();
            }
        }
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        let metadata = event.metadata();
        let severity = level_to_severity(metadata.level());

        let mut visitor = FieldCollector::default();
        event.record(&mut visitor);

        let message = visitor
            .values
            .get("message")
            .and_then(|v| v.as_str())
            .map_or_else(
                || metadata.name().to_string(),
                std::string::ToString::to_string,
            );

        let mut record = self.base_record(severity, &message);
        record.insert("target".into(), json!(metadata.target()));

        // Ancestor span fields (root→leaf), then event fields (override).
        let mut trace: Option<(String, String)> = None;
        if let Some(scope) = ctx.event_scope(event) {
            let spans = self.spans.lock();
            let chain: Vec<&SpanData> = scope
                .from_root()
                .filter_map(|s| spans.get(&s.id()))
                .collect();
            Self::merge_chain_fields(&mut record, chain.iter().copied());
            if let Some(leaf) = chain.last() {
                trace = Some((leaf.trace_id.clone(), leaf.cloud_span_id.clone()));
            }
        }
        for (k, v) in &visitor.values {
            if k != "message" && !HIDDEN_FIELDS.contains(&k.as_str()) {
                record.insert(k.clone(), v.clone());
            }
        }
        if let Some(t) = visitor.values.get("trace_id").and_then(|v| v.as_str()) {
            let sid = trace
                .as_ref()
                .map_or_else(generate_span_id, |t| t.1.clone());
            trace = Some((t.to_string(), sid));
        }
        if let Some((tid, sid)) = trace {
            self.put_trace(&mut record, &tid, &sid);
        }

        self.emit(&record);
    }

    fn on_close(&self, id: SpanId, ctx: Context<'_, S>) {
        // Collect ancestors' fields before removing ourselves.
        let mut record;
        {
            let spans = self.spans.lock();
            let Some(data) = spans.get(&id) else { return };
            if data.level < Level::INFO {
                drop(spans);
                self.spans.lock().remove(&id);
                return;
            }
            let end = data.last_exit.unwrap_or_else(Instant::now);
            let elapsed_ms =
                u64::try_from(end.duration_since(data.start).as_millis()).unwrap_or(u64::MAX);
            record = self.base_record(level_to_severity(&data.level), data.name);
            record.insert("elapsed_ms".into(), json!(elapsed_ms));
            // Close deferred well past the last poll (a lingering child): say so
            // separately instead of inflating the work's duration.
            let idle_ms = u64::try_from(end.elapsed().as_millis()).unwrap_or(u64::MAX);
            if idle_ms >= 1000 {
                record.insert("close_deferred_ms".into(), json!(idle_ms));
            }
            record.insert(
                "span".into(),
                json!({ "name": data.name, "kind": Self::span_kind(data.name) }),
            );
            if let Some(scope) = ctx.span_scope(&id) {
                let chain: Vec<&SpanData> = scope
                    .from_root()
                    .filter_map(|s| spans.get(&s.id()))
                    .collect();
                Self::merge_chain_fields(&mut record, chain.iter().copied());
            } else {
                Self::merge_chain_fields(&mut record, std::iter::once(data));
            }
            self.put_trace(&mut record, &data.trace_id, &data.cloud_span_id);
        }
        self.spans.lock().remove(&id);
        self.emit(&record);
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn level_to_severity(level: &Level) -> &'static str {
    match *level {
        Level::ERROR => "ERROR",
        Level::WARN => "WARNING",
        Level::INFO => "INFO",
        Level::DEBUG | Level::TRACE => "DEBUG",
    }
}

/// Generate a random 16-character hex span id for Cloud Logging.
fn generate_span_id() -> String {
    format!("{:016x}", rand::random::<u64>())
}

/// Generate a random 32-character hex trace id (for root spans without an
/// incoming trace: background tasks, prewarm, compaction, publisher).
pub fn generate_trace_id() -> String {
    format!("{:032x}", rand::random::<u128>())
}

/// Tracing field visitor that collects all field values as JSON.
#[derive(Default)]
struct FieldCollector {
    values: Map<String, Value>,
}

impl Visit for FieldCollector {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        let s = format!("{value:?}");
        self.values.insert(field.name().to_string(), json!(s));
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.values.insert(field.name().to_string(), json!(value));
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.values.insert(field.name().to_string(), json!(value));
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.values.insert(field.name().to_string(), json!(value));
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.values.insert(field.name().to_string(), json!(value));
    }

    fn record_f64(&mut self, field: &Field, value: f64) {
        self.values.insert(field.name().to_string(), json!(value));
    }

    fn record_error(&mut self, field: &Field, value: &(dyn std::error::Error + 'static)) {
        self.values
            .insert(field.name().to_string(), json!(value.to_string()));
    }
}

// ---------------------------------------------------------------------------
// Trace context parsing
// ---------------------------------------------------------------------------

/// Parse the `X-Cloud-Trace-Context` header.
/// Format: `TRACE_ID/SPAN_ID;o=TRACE_TRUE`
/// Returns the `trace_id` (32-char hex).
pub fn parse_x_cloud_trace_context(header: &str) -> Option<String> {
    let trace_id = header.split('/').next()?;
    let trimmed = trace_id.trim();
    if !trimmed.is_empty() && trimmed.len() == 32 && trimmed.chars().all(|c| c.is_ascii_hexdigit())
    {
        Some(trimmed.to_lowercase())
    } else {
        None
    }
}

/// Parse the W3C `traceparent` header.
/// Format: `00-TRACE_ID-PARENT_ID-TRACE_FLAGS`
/// Returns the `trace_id` (32-char hex).
pub fn parse_traceparent(header: &str) -> Option<String> {
    let parts: Vec<&str> = header.split('-').collect();
    if parts.len() >= 4 {
        let trace_id = parts[1].trim();
        if trace_id.len() == 32 && trace_id.chars().all(|c| c.is_ascii_hexdigit()) {
            return Some(trace_id.to_lowercase());
        }
    }
    None
}

/// Extract a trace id from HTTP headers, trying `X-Cloud-Trace-Context` then
/// `traceparent`.
pub fn extract_trace_id(headers: &axum::http::HeaderMap) -> Option<String> {
    if let Some(v) = headers
        .get("x-cloud-trace-context")
        .and_then(|v| v.to_str().ok())
        && let Some(tid) = parse_x_cloud_trace_context(v)
    {
        return Some(tid);
    }
    if let Some(v) = headers.get("traceparent").and_then(|v| v.to_str().ok())
        && let Some(tid) = parse_traceparent(v)
    {
        return Some(tid);
    }
    None
}

// ---------------------------------------------------------------------------
// Init
// ---------------------------------------------------------------------------

static PROJECT_ID: OnceLock<Option<String>> = OnceLock::new();

/// Initialise `tracing-subscriber` from `[telemetry]`.
///
/// * `log_format` selects JSON (Cloud Logging) or pretty (human).
/// * `log_filter` is the default `EnvFilter`; `RUST_LOG` overrides it entirely.
/// * When JSON, installs a [`CloudLoggingLayer`] that emits structured JSON
///   with Cloud Logging trace correlation and span-close performance lines.
pub fn tracing_init(cfg: &Config) {
    use tracing_subscriber::prelude::*;
    // Idempotent: tests (and anything else) may call this more than once per process.
    static INIT: OnceLock<()> = OnceLock::new();
    if INIT.set(()).is_err() {
        return;
    }

    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(&cfg.telemetry.log_filter));

    let project_id = PROJECT_ID.get_or_init(|| resolve_project_id(cfg));

    let registry = tracing_subscriber::registry().with(filter);

    match cfg.telemetry.log_format {
        LogFormat::Json => {
            let cloud_layer = CloudLoggingLayer::new(project_id.clone());
            registry.with(cloud_layer).init();
        }
        LogFormat::Pretty => {
            registry
                .with(tracing_subscriber::fmt::layer().pretty())
                .init();
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_xctc_valid() {
        assert_eq!(
            parse_x_cloud_trace_context("abcdef0123456789abcdef0123456789/12345;o=1"),
            Some("abcdef0123456789abcdef0123456789".into())
        );
    }

    #[test]
    fn parse_xctc_too_short() {
        assert_eq!(parse_x_cloud_trace_context("abc/123"), None);
    }

    #[test]
    fn parse_traceparent_valid() {
        let tp = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        assert_eq!(
            parse_traceparent(tp),
            Some("4bf92f3577b34da6a3ce929d0e0e4736".into())
        );
    }

    #[test]
    fn parse_traceparent_invalid() {
        assert_eq!(parse_traceparent("garbage"), None);
    }

    /// Verify that span.kind is derived correctly from span name prefixes.
    #[test]
    fn span_kind_derivation() {
        assert_eq!(CloudLoggingLayer::span_kind("http.request"), "http");
        assert_eq!(CloudLoggingLayer::span_kind("wal.sync"), "wal");
        assert_eq!(CloudLoggingLayer::span_kind("store.get"), "store");
        assert_eq!(CloudLoggingLayer::span_kind("git.upload_pack"), "git");
        assert_eq!(CloudLoggingLayer::span_kind("other.thing"), "other");
        assert_eq!(CloudLoggingLayer::span_kind("noprefix"), "other");
    }

    /// Verify that `extract_trace_id` works with both header formats.
    #[test]
    fn extract_trace_id_from_headers() {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            "x-cloud-trace-context",
            "abcdef0123456789abcdef0123456789/12345;o=1"
                .parse()
                .unwrap(),
        );
        assert_eq!(
            extract_trace_id(&headers),
            Some("abcdef0123456789abcdef0123456789".into())
        );

        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            "traceparent",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
                .parse()
                .unwrap(),
        );
        assert_eq!(
            extract_trace_id(&headers),
            Some("4bf92f3577b34da6a3ce929d0e0e4736".into())
        );

        let headers = axum::http::HeaderMap::new();
        assert_eq!(extract_trace_id(&headers), None);
    }

    /// A child span's close line carries ITS OWN `elapsed_ms`, never the
    /// parent's: a 1 ms `store.get` inside a 60 ms `task.run` logs ≈ 1 ms
    /// (and inherits the parent's fields like `task_id`).
    #[test]
    fn child_span_elapsed_is_its_own() {
        use tracing_subscriber::prelude::*;
        let sink = std::sync::Arc::new(Mutex::new(Vec::new()));
        let layer = CloudLoggingLayer::with_sink(sink.clone());
        let sub = tracing_subscriber::registry().with(layer);
        tracing::subscriber::with_default(sub, || {
            let parent = tracing::info_span!("task.run", task_id = "t1", bytes = 5u64);
            let _p = parent.enter();
            std::thread::sleep(std::time::Duration::from_millis(60));
            {
                let child = tracing::info_span!(
                    "store.get",
                    key = "k",
                    bytes = 7u64,
                    queued_ms = tracing::field::Empty
                );
                let _c = child.enter();
                std::thread::sleep(std::time::Duration::from_millis(2));
            }
        });
        let recs = sink.lock();
        let child = recs
            .iter()
            .find(|r| r["message"] == "store.get")
            .expect("store.get close line");
        let parent = recs
            .iter()
            .find(|r| r["message"] == "task.run")
            .expect("task.run close line");
        let ce = child["elapsed_ms"].as_u64().unwrap();
        let pe = parent["elapsed_ms"].as_u64().unwrap();
        assert!(
            ce < 40,
            "child elapsed {ce} should be ~2 ms, not the parent's"
        );
        assert!(pe >= 60, "parent elapsed {pe}");
        assert_eq!(child["task_id"], "t1", "context fields inherited");
        assert_eq!(child["bytes"], 7, "innermost field wins");
        assert!(
            child.get("queued_ms").is_none(),
            "Empty fields are not emitted"
        );
    }

    /// A span whose close is deferred by a lingering child (the GCS SDK's
    /// stream spans under `store.put`) reports the time its own future ran,
    /// not the time until the child let go.
    #[test]
    fn elapsed_is_measured_to_the_last_exit_not_the_deferred_close() {
        use tracing_subscriber::prelude::*;
        let sink = std::sync::Arc::new(Mutex::new(Vec::new()));
        let layer = CloudLoggingLayer::with_sink(sink.clone());
        let sub = tracing_subscriber::registry().with(layer);
        tracing::subscriber::with_default(sub, || {
            let parent = tracing::info_span!("store.put", key = "k");
            let lingering = {
                let _p = parent.enter();
                std::thread::sleep(std::time::Duration::from_millis(5));
                // A child created inside the parent and kept alive after it.
                tracing::info_span!("sdk.stream")
            };
            drop(parent); // parent's future is done; its close waits for the child
            std::thread::sleep(std::time::Duration::from_millis(1100));
            drop(lingering);
        });
        let recs = sink.lock();
        let put = recs
            .iter()
            .find(|r| r["message"] == "store.put")
            .expect("store.put close line");
        let e = put["elapsed_ms"].as_u64().unwrap();
        assert!(
            e < 40,
            "elapsed {e} must be the ~5 ms of work, not the 1.1 s the child lingered"
        );
        assert!(
            put["close_deferred_ms"].as_u64().unwrap() >= 1000,
            "{put:?}"
        );
    }

    #[test]
    fn ids_have_expected_shape() {
        assert_eq!(generate_span_id().len(), 16);
        assert_eq!(generate_trace_id().len(), 32);
        assert_ne!(generate_trace_id(), generate_trace_id());
    }
}
