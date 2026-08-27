//! GCS backend over the google-cloud-storage 1.x client.
//!
//! Version tokens are GCS object generations rendered as decimal strings.
//! Conditional reads use `if_generation_not_match`; conditional writes use
//! `if_generation_match` (0 for create-if-absent). Range reads use
//! `read_offset` / `read_limit`. Streaming uploads use `send_buffered` for
//! non-seekable sources and `send_unbuffered` for seekable ones (Bytes).

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;

use google_cloud_auth::credentials::Builder as AuthBuilder;
use google_cloud_gax::error::rpc::Code;
use google_cloud_storage::builder::storage::SignedUrlBuilder;
use google_cloud_storage::client::{Storage, StorageControl};
use google_cloud_storage::model_ext::ReadRange;
use google_cloud_storage::signed_url::UrlStyle;
use google_cloud_storage::streaming_source::StreamingSource;

use crate::{
    ByteStream, GetOptions, GetResult, ObjectMeta, ObjectStore, PutBody, PutMode, PutOptions,
    Result, StoreError, Version, util,
};

const IMMUTABLE_CACHE_CONTROL: &str = "public, max-age=31536000, immutable";
/// Below this size, a single-shot upload avoids the resumable-session
/// round-trip used by `send_buffered`. This covers normal one-commit pushes.
const SINGLE_SHOT_PUT_LIMIT: u64 = 8 * 1024 * 1024;
const FILE_CHUNK_SIZE: usize = 256 * 1024;
const LIST_PAGE_SIZE: i32 = 1000;

/// Per-call deadlines. A GCS call that neither answers nor fails must not hang a
/// request forever (seen in prod: one metadata GET took 27 minutes and the
/// request with it). Every RPC below is bounded; a timed-out call surfaces as
/// [`StoreError::Retryable`] ("exceeded deadline") so the caller fails fast
/// (503) or retries. Metadata/control calls are quick (p99 ~80 ms); body reads
/// and uploads get time proportional to their size.
/// Mid-stream resumes per bulk read before the error is surfaced.
const BULK_RESUME_ATTEMPTS: u32 = 5;
const META_DEADLINE: std::time::Duration = std::time::Duration::from_secs(10);
const READ_OPEN_DEADLINE: std::time::Duration = std::time::Duration::from_secs(60);
/// Per chunk of a streaming body read (not the whole stream).
const READ_CHUNK_DEADLINE: std::time::Duration = std::time::Duration::from_secs(60);
const PUT_MIN_DEADLINE: std::time::Duration = std::time::Duration::from_secs(30);
/// Uploads get this many bytes per second on top of `PUT_MIN_DEADLINE` (1 MiB/s floor).
const PUT_BYTES_PER_SEC: u64 = 1024 * 1024;
/// Idempotent metadata reads that time out are retried once after a short jittered pause.
const READ_RETRIES: u32 = 1;

fn put_deadline(bytes: u64) -> std::time::Duration {
    PUT_MIN_DEADLINE + std::time::Duration::from_secs(bytes / PUT_BYTES_PER_SEC)
}

fn deadline_error(op: &str, key: &str, deadline: std::time::Duration) -> StoreError {
    tracing::warn!(
        op,
        key,
        deadline_ms = deadline.as_millis() as u64,
        "gcs call exceeded deadline"
    );
    StoreError::retryable(anyhow::anyhow!(
        "gcs {op} {key} exceeded deadline of {deadline:?}"
    ))
}

/// Run a GCS call under `deadline`; the client error keeps its meaning through
/// `map_error` (NotFound / PreconditionFailed / NotModified), a timeout becomes
/// [`deadline_error`]. `retries` extra attempts are made only when the deadline
/// fired (the call is idempotent for every caller that passes > 0).
async fn call<T, F, Fut>(
    op: &'static str,
    key: &str,
    deadline: std::time::Duration,
    retries: u32,
    mut make: F,
) -> std::result::Result<T, GcsCallError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = std::result::Result<T, google_cloud_storage::Error>>,
{
    let mut attempt = 0u32;
    loop {
        match tokio::time::timeout(deadline, make()).await {
            Ok(Ok(v)) => return Ok(v),
            Ok(Err(e)) => return Err(GcsCallError::Gcs(e)),
            Err(_) => {
                if attempt >= retries {
                    return Err(GcsCallError::Deadline(deadline));
                }
                tracing::warn!(
                    op,
                    key,
                    attempt,
                    deadline_ms = deadline.as_millis() as u64,
                    "gcs call exceeded deadline, retrying"
                );
                attempt += 1;
                let jitter = 100
                    + (std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.subsec_nanos())
                        .unwrap_or(0)
                        % 400) as u64;
                tokio::time::sleep(std::time::Duration::from_millis(jitter)).await;
            }
        }
    }
}

enum GcsCallError {
    Gcs(google_cloud_storage::Error),
    Deadline(std::time::Duration),
}

impl GcsCallError {
    fn into_store(self, op: &'static str, key: &str) -> StoreError {
        match self {
            GcsCallError::Gcs(e) => map_error(key, e),
            GcsCallError::Deadline(d) => deadline_error(op, key, d),
        }
    }
    fn is_not_modified(&self) -> bool {
        matches!(self, GcsCallError::Gcs(e) if is_not_modified(e))
    }
    fn is_not_found(&self) -> bool {
        matches!(self, GcsCallError::Gcs(e) if is_not_found(e))
    }
}

/// GCS-backed [`ObjectStore`].
pub struct GcsStore {
    /// Control-plane data client: manifests, logs, checkpoints, leases,
    /// bundle lists, policy — small objects that must stay fast while bulk
    /// traffic runs.
    storage: Storage,
    /// Bulk data clients (own channels/connection pools, round-robin): pack /
    /// idx / side-file / bundle / LFS bytes and every ranged read. Prod
    /// finding 2026-08-20: with one client, a large repository's 7.5 GB history-pack
    /// download (16 stripes) starved a 184-byte GET and the manifest's
    /// conditional GET for 4–11 minutes (head-of-line on the shared channel).
    bulk: Vec<Storage>,
    bulk_next: std::sync::atomic::AtomicUsize,
    /// Bulk **reads** go over plain HTTPS (JSON API `alt=media` + `Range`)
    /// through reqwest clients with their own connection pools: the gRPC
    /// clients above all share one transport, so even separate `Storage` /
    /// `StorageControl` instances queue a 200-byte GET behind a 7.5 GB
    /// download (measured: control calls 3–11 s while 32 stripes stream).
    bulk_http: Vec<reqwest::Client>,
    creds: Option<google_cloud_auth::credentials::Credentials>,
    bucket: String,
    /// Global cap on concurrent bulk requests per process (leaves headroom).
    bulk_permits: std::sync::Arc<tokio::sync::Semaphore>,
    bulk_permits_total: usize,
    /// `telemetry.lock_wait_warn`: a permit wait past this is a WARN `lock wait` line.
    permit_wait_warn: std::time::Duration,
    control: StorageControl,
    bucket_resource: String, // "projects/_/buckets/{bucket}"
    signing_signer: Option<google_cloud_auth::signer::Signer>,
    signing_service_account: Option<String>,
}

impl GcsStore {
    /// Construct from a parsed `StoreConfig`.
    ///
    /// Uses ADC (Application Default Credentials) for authentication — workload
    /// identity on a serverless host, `gcloud auth application-default login` locally.
    /// The endpoint override from `cfg.gcs.endpoint` is applied to both the
    /// data (`Storage`) and control (`StorageControl`) clients, allowing
    /// emulator use.
    /// Set `telemetry.lock_wait_warn` for the bulk-permit WARN line (default 1 s).
    pub fn with_permit_wait_warn(mut self, d: std::time::Duration) -> Self {
        self.permit_wait_warn = d;
        self
    }

    pub async fn new(cfg: &walgit_config::StoreConfig) -> anyhow::Result<Self> {
        if cfg.bucket.is_empty() {
            anyhow::bail!("gcs store requires a non-empty bucket name");
        }

        let endpoint = cfg.gcs.endpoint.clone();
        let bucket_resource = format!("projects/_/buckets/{}", cfg.bucket);

        let storage = Storage::builder()
            .with_endpoint(endpoint.clone())
            .build()
            .await?;
        let mut bulk = Vec::new();
        let mut bulk_http = Vec::new();
        for _ in 0..cfg.gcs.bulk_clients.max(1) {
            bulk.push(
                Storage::builder()
                    .with_endpoint(endpoint.clone())
                    .build()
                    .await?,
            );
            bulk_http.push(
                reqwest::Client::builder()
                    .http1_only()
                    .pool_max_idle_per_host(64)
                    .connect_timeout(std::time::Duration::from_secs(10))
                    .build()?,
            );
        }
        let creds = google_cloud_auth::credentials::Builder::default()
            .build()
            .ok();

        let control = StorageControl::builder()
            .with_endpoint(endpoint)
            .build()
            .await?;

        // Build a signer for V4 signed URLs. ADC-based signer works on a serverless host
        // (via IAM signBlob) and locally (via service account key if present).
        let signing_signer = AuthBuilder::default().build_signer().ok();

        Ok(Self {
            storage,
            bulk,
            bulk_next: std::sync::atomic::AtomicUsize::new(0),
            bulk_http,
            creds,
            bucket: cfg.bucket.clone(),
            bulk_permits: std::sync::Arc::new(tokio::sync::Semaphore::new(
                cfg.gcs.bulk_concurrency.max(1),
            )),
            bulk_permits_total: cfg.gcs.bulk_concurrency.max(1),
            permit_wait_warn: std::time::Duration::from_secs(1),
            control,
            bucket_resource,
            signing_signer,
            signing_service_account: cfg.gcs.signing_service_account.clone(),
        })
    }

    /// What a resume needs (pools/creds are Arc'd inside; cheap).
    fn clone_for_resume(&self) -> BulkHttp {
        BulkHttp {
            clients: self.bulk_http.clone(),
            next: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            creds: self.creds.clone(),
            bucket: self.bucket.clone(),
            permits: self.bulk_permits.clone(),
            endpoint: std::env::var("WALGIT_GCS_HTTP_ENDPOINT")
                .unwrap_or_else(|_| "https://storage.googleapis.com".into()),
        }
    }

    /// The data client for `key` (+ a bulk permit when it is bulk traffic).
    async fn data_client(
        &self,
        key: &str,
        ranged: bool,
    ) -> (&Storage, Option<tokio::sync::OwnedSemaphorePermit>) {
        if ranged || util::is_bulk_key(key) {
            let i = self
                .bulk_next
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                % self.bulk.len();
            // Permit wait is queueing, not transfer: record it on the store
            // span (`queued_ms`) so a long `elapsed_ms` is explainable (prod
            // 2026-08-21: a 184-byte pack GET "took 433 s" — it waited behind
            // 32 stripes of a large repository's idx/history pack on this semaphore).
            let t = std::time::Instant::now();
            let permit = self.bulk_permits.clone().acquire_owned().await.ok();
            let queued = t.elapsed();
            if queued.as_millis() > 0 {
                tracing::Span::current().record("queued_ms", queued.as_millis() as u64);
            }
            metrics::histogram!("walgit_store_bulk_queue_seconds").record(queued.as_secs_f64());
            if queued > std::time::Duration::ZERO {
                metrics::histogram!("walgit_lock_wait_seconds", "lock" => "gcs_bulk_permit")
                    .record(queued.as_secs_f64());
            }
            if queued >= self.permit_wait_warn {
                tracing::warn!(
                    lock = "gcs_bulk_permit",
                    key,
                    wait_ms = queued.as_millis() as u64,
                    "lock wait"
                );
            }
            metrics::gauge!("walgit_store_bulk_inflight")
                .set((self.bulk_permits_total - self.bulk_permits.available_permits()) as f64);
            (&self.bulk[i], permit)
        } else {
            (&self.storage, None)
        }
    }

    fn meta_from_object(obj: &google_cloud_storage::model::Object) -> ObjectMeta {
        ObjectMeta {
            key: obj.name.clone(),
            size: obj.size as u64,
            version: gen_version(obj.generation),
        }
    }

    /// Fetch the current generation for an object via a metadata GET.
    async fn current_generation(&self, key: &str) -> Option<Version> {
        let req = google_cloud_storage::model::GetObjectRequest::new()
            .set_bucket(self.bucket_resource.clone())
            .set_object(key.to_owned());
        match call("head", key, META_DEADLINE, READ_RETRIES, || {
            self.control.get_object().with_request(req.clone()).send()
        })
        .await
        {
            Ok(obj) => Some(gen_version(obj.generation)),
            Err(_) => None,
        }
    }

    /// Bulk read over HTTPS (own pool): `GET .../o/<key>?alt=media` with an
    /// optional `Range`. Returns (object size, generation, body stream).
    ///
    /// The body **resumes**: when the stream dies mid-way ("error decoding
    /// response body", a chunk deadline — prod 2026-08-21 ×6, aborting a
    /// history-pack install) the wrapper re-issues the request for the bytes
    /// not yet delivered (`Range: bytes=<pos>-<end>`, pinned to the same
    /// generation so a rewritten object can never be spliced), up to
    /// `BULK_RESUME_ATTEMPTS` times with jittered backoff.
    /// `walgit_remote_chunk_retries_total` counts the resumes.
    async fn bulk_http_read(
        &self,
        key: &str,
        range: Option<std::ops::Range<u64>>,
        if_generation_match: Option<i64>,
    ) -> Result<(u64, Option<i64>, ByteStream)> {
        self.clone_for_resume()
            .read(key, range, if_generation_match)
            .await
    }
}

impl BulkHttp {
    /// Resuming read: see [`GcsStore::bulk_http_read`].
    pub(crate) async fn read(
        &self,
        key: &str,
        range: Option<std::ops::Range<u64>>,
        if_generation_match: Option<i64>,
    ) -> Result<(u64, Option<i64>, ByteStream)> {
        let (size, generation, first) = self.open(key, range.clone(), if_generation_match).await?;
        let end = range.as_ref().map(|r| r.end).unwrap_or(size);
        let start = range.as_ref().map(|r| r.start).unwrap_or(0);
        let this = self.clone();
        let key_owned = key.to_owned();
        struct St {
            inner: ByteStream,
            pos: u64,
            attempts: u32,
        }
        let st = St {
            inner: first,
            pos: start,
            attempts: 0,
        };
        let body: ByteStream = Box::pin(futures::stream::unfold(
            (st, this, key_owned),
            move |(mut st, this, key)| async move {
                loop {
                    match st.inner.next().await {
                        Some(Ok(b)) => {
                            st.pos += b.len() as u64;
                            return Some((Ok(b), (st, this, key)));
                        }
                        None => {
                            if end > 0 && st.pos < end && st.attempts < BULK_RESUME_ATTEMPTS {
                                // Short body without an error: resume too.
                                tracing::warn!(key = %key, pos = st.pos, end, "gcs bulk read ended early; resuming");
                            } else {
                                return None;
                            }
                        }
                        Some(Err(e)) => {
                            if st.attempts >= BULK_RESUME_ATTEMPTS || st.pos >= end {
                                return Some((Err(e), (st, this, key)));
                            }
                            tracing::warn!(key = %key, pos = st.pos, end, attempt = st.attempts + 1, error = %e, "gcs bulk read failed mid-stream; resuming from the bytes received");
                        }
                    }
                    st.attempts += 1;
                    metrics::counter!("walgit_remote_chunk_retries_total").increment(1);
                    let backoff = std::time::Duration::from_millis(
                        200 * (1u64 << st.attempts.min(5)) + rand::random::<u64>() % 250,
                    );
                    tokio::time::sleep(backoff).await;
                    match this.open(&key, Some(st.pos..end), generation).await {
                        Ok((_, _, s)) => st.inner = s,
                        Err(e) => {
                            if st.attempts >= BULK_RESUME_ATTEMPTS {
                                return Some((Err(e), (st, this, key)));
                            }
                            // Try again after the next backoff (loop: inner is exhausted → None arm).
                            st.inner = Box::pin(futures::stream::empty());
                        }
                    }
                }
            },
        ));
        Ok((size, generation, body))
    }
}

/// The bulk HTTPS read path, detached from the store so a body stream can
/// re-open itself (resume) after its `GcsStore` borrow is gone.
#[derive(Clone)]
pub(crate) struct BulkHttp {
    clients: Vec<reqwest::Client>,
    next: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    creds: Option<google_cloud_auth::credentials::Credentials>,
    bucket: String,
    permits: std::sync::Arc<tokio::sync::Semaphore>,
    /// JSON API endpoint (`https://storage.googleapis.com`; tests point it at
    /// a local server that cuts bodies).
    endpoint: String,
}

impl BulkHttp {
    /// Test constructor: no credentials, any endpoint.
    #[cfg(test)]
    pub(crate) fn for_tests(endpoint: String, bucket: String) -> Self {
        BulkHttp {
            clients: vec![reqwest::Client::new()],
            next: Default::default(),
            creds: None,
            bucket,
            permits: std::sync::Arc::new(tokio::sync::Semaphore::new(8)),
            endpoint,
        }
    }

    async fn open(
        &self,
        key: &str,
        range: Option<std::ops::Range<u64>>,
        if_generation_match: Option<i64>,
    ) -> Result<(u64, Option<i64>, ByteStream)> {
        let headers = match &self.creds {
            Some(creds) => match creds
                .headers(http::Extensions::new())
                .await
                .map_err(StoreError::other)?
            {
                google_cloud_auth::credentials::CacheableResource::New { data, .. } => data,
                google_cloud_auth::credentials::CacheableResource::NotModified => {
                    http::HeaderMap::new()
                }
            },
            None if self.endpoint.starts_with("https://storage.googleapis.com") => {
                return Err(StoreError::other(anyhow::anyhow!(
                    "no credentials for bulk http"
                )));
            }
            None => http::HeaderMap::new(),
        };
        let i = self.next.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % self.clients.len();
        let permit = self.permits.clone().acquire_owned().await.ok();
        let url = format!(
            "{}/storage/v1/b/{}/o/{}?alt=media{}",
            self.endpoint,
            self.bucket,
            urlencode(key),
            if_generation_match
                .map(|g| format!("&ifGenerationMatch={g}"))
                .unwrap_or_default()
        );
        let mut req = self.clients[i].get(&url).headers(headers);
        if let Some(r) = &range {
            req = req.header(
                reqwest::header::RANGE,
                format!("bytes={}-{}", r.start, r.end.saturating_sub(1)),
            );
        }
        let resp = match tokio::time::timeout(READ_OPEN_DEADLINE, req.send()).await {
            Ok(r) => r.map_err(StoreError::other)?,
            Err(_) => return Err(deadline_error("read", key, READ_OPEN_DEADLINE)),
        };
        let status = resp.status();
        if status == reqwest::StatusCode::NOT_FOUND {
            return Err(StoreError::NotFound {
                key: key.to_owned(),
            });
        }
        if status == reqwest::StatusCode::PRECONDITION_FAILED
            || status == reqwest::StatusCode::RANGE_NOT_SATISFIABLE
        {
            return Err(StoreError::PreconditionFailed {
                key: key.to_owned(),
                current: None,
            });
        }
        if status.is_server_error() || status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            return Err(StoreError::retryable(anyhow::anyhow!(
                "gcs bulk read {key}: HTTP {status}"
            )));
        }
        if !status.is_success() {
            return Err(StoreError::other(anyhow::anyhow!(
                "gcs bulk read {key}: HTTP {status}"
            )));
        }
        // Total size: from Content-Range for ranges, Content-Length otherwise.
        let size = resp
            .headers()
            .get(reqwest::header::CONTENT_RANGE)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.rsplit('/').next().and_then(|t| t.parse::<u64>().ok()))
            .or_else(|| resp.content_length())
            .unwrap_or(0);
        let generation = resp
            .headers()
            .get("x-goog-generation")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<i64>().ok());
        let key_owned = key.to_owned();
        let stream = resp.bytes_stream();
        let body: ByteStream = Box::pin(futures::stream::unfold(
            (stream, key_owned, permit),
            |(mut stream, key, permit)| async move {
                match tokio::time::timeout(READ_CHUNK_DEADLINE, stream.next()).await {
                    Ok(Some(Ok(b))) => Some((Ok(b), (stream, key, permit))),
                    Ok(Some(Err(e))) => Some((
                        Err(StoreError::retryable(anyhow::anyhow!(
                            "gcs bulk read {key}: {e}"
                        ))),
                        (stream, key, permit),
                    )),
                    Ok(None) => None,
                    Err(_) => Some((
                        Err(deadline_error("read_chunk", &key, READ_CHUNK_DEADLINE)),
                        (stream, key, permit),
                    )),
                }
            },
        ));
        Ok((size, generation, body))
    }
}

impl GcsStore {
    /// Read the object body with an optional range.
    async fn read_object_body(
        &self,
        key: &str,
        range: Option<std::ops::Range<u64>>,
    ) -> Result<ByteStream> {
        if self.creds.is_some() && (range.is_some() || util::is_bulk_key(key)) {
            return Ok(self.bulk_http_read(key, range, None).await?.2);
        }
        let (client, permit) = self.data_client(key, range.is_some()).await;
        let mut builder = client.read_object(self.bucket_resource.clone(), key.to_owned());
        if let Some(ref r) = range {
            builder = builder.set_read_range(range_to_read_range(r));
        }
        let resp = match tokio::time::timeout(READ_OPEN_DEADLINE, builder.send()).await {
            Ok(r) => r.map_err(|e| map_error(key, e))?,
            Err(_) => return Err(deadline_error("read", key, READ_OPEN_DEADLINE)),
        };
        Ok(unfold_response(key, resp, permit))
    }

    async fn put_inner(&self, key: &str, body: PutBody, opts: PutOptions) -> Result<ObjectMeta> {
        let result = match body {
            PutBody::Bytes(b) => {
                let (client, _permit) = self.data_client(key, false).await;
                let mut builder =
                    client.write_object(self.bucket_resource.clone(), key.to_owned(), b);
                builder = apply_put_opts(builder, &opts);
                builder.send_unbuffered().await
            }
            PutBody::File(path) => {
                let small = tokio::fs::metadata(&path)
                    .await
                    .map(|m| m.len() <= SINGLE_SHOT_PUT_LIMIT)
                    .unwrap_or(false);
                if small {
                    let bytes = tokio::fs::read(&path).await.map_err(StoreError::other)?;
                    let (client, _permit) = self.data_client(key, false).await;
                    let mut builder = client.write_object(
                        self.bucket_resource.clone(),
                        key.to_owned(),
                        Bytes::from(bytes),
                    );
                    builder = apply_put_opts(builder, &opts);
                    builder.send_unbuffered().await
                } else {
                    let stream = crate::util::file_stream(path, None, FILE_CHUNK_SIZE);
                    let source = StoreStreamSource {
                        inner: tokio::sync::Mutex::new(stream),
                    };
                    let (client, _permit) = self.data_client(key, false).await;
                    let mut builder =
                        client.write_object(self.bucket_resource.clone(), key.to_owned(), source);
                    builder = apply_put_opts(builder, &opts);
                    builder.send_buffered().await
                }
            }
            PutBody::Stream { len, stream } if len <= SINGLE_SHOT_PUT_LIMIT => {
                let bytes = crate::util::collect(stream, len as usize).await?;
                let (client, _permit) = self.data_client(key, false).await;
                let mut builder =
                    client.write_object(self.bucket_resource.clone(), key.to_owned(), bytes);
                builder = apply_put_opts(builder, &opts);
                builder.send_unbuffered().await
            }
            PutBody::Stream { stream, .. } => {
                let source = StoreStreamSource {
                    inner: tokio::sync::Mutex::new(stream),
                };
                let (client, _permit) = self.data_client(key, false).await;
                let mut builder =
                    client.write_object(self.bucket_resource.clone(), key.to_owned(), source);
                builder = apply_put_opts(builder, &opts);
                builder.send_buffered().await
            }
        };

        let obj = match result {
            Ok(obj) => obj,
            Err(e) => return Err(map_error(key, e)),
        };
        Ok(Self::meta_from_object(&obj))
    }
}

#[async_trait]
impl ObjectStore for GcsStore {
    fn backend(&self) -> &'static str {
        "gcs"
    }

    async fn get(&self, key: &str, opts: GetOptions) -> Result<GetResult> {
        // if_none_match → metadata GET with if_generation_not_match.
        // 304 / FailedPrecondition = NotModified (cheap, no body download).
        if let Some(v) = &opts.if_none_match {
            // If the version can't be parsed as a GCS generation, it can never
            // match the current generation → the object is always "changed"
            // from the caller's perspective. Skip the precondition and return
            // the object directly.
            match parse_generation(v) {
                Some(generation) => {
                    let req = google_cloud_storage::model::GetObjectRequest::new()
                        .set_bucket(self.bucket_resource.clone())
                        .set_object(key.to_owned())
                        .set_if_generation_not_match(generation);

                    let result = match call("get", key, META_DEADLINE, READ_RETRIES, || {
                        self.control.get_object().with_request(req.clone()).send()
                    })
                    .await
                    {
                        Ok(obj) => {
                            let meta = Self::meta_from_object(&obj);
                            let body = self.read_object_body(key, opts.range.clone()).await?;
                            Ok(GetResult::Object { meta, body })
                        }
                        Err(e) => {
                            if e.is_not_modified() {
                                Ok(GetResult::NotModified {
                                    version: gen_version(generation),
                                })
                            } else {
                                Err(e.into_store("get", key))
                            }
                        }
                    };
                    return result;
                }
                None => {
                    // Non-numeric version: can never match a GCS generation,
                    // so the object is always "changed" → fall through to read.
                }
            }
        }

        // Direct read (no if_none_match, or if_none_match with non-numeric
        // version that can never match a GCS generation).
        if self.creds.is_some() && (opts.range.is_some() || util::is_bulk_key(key)) {
            let if_gen = match &opts.if_match {
                Some(v) => match parse_generation(v) {
                    Some(g) => Some(g),
                    None => {
                        return Err(StoreError::PreconditionFailed {
                            key: key.to_owned(),
                            current: None,
                        });
                    }
                },
                None => None,
            };
            let (size, generation, body) =
                self.bulk_http_read(key, opts.range.clone(), if_gen).await?;
            let meta = ObjectMeta {
                key: key.to_owned(),
                size,
                version: generation
                    .map(gen_version)
                    .unwrap_or_else(|| Version::new("")),
            };
            return Ok(GetResult::Object { meta, body });
        }
        let (client, permit) = self.data_client(key, opts.range.is_some()).await;
        let mut builder = client.read_object(self.bucket_resource.clone(), key.to_owned());

        if let Some(v) = &opts.if_match {
            if let Some(generation) = parse_generation(v) {
                builder = builder.set_if_generation_match(generation);
            } else {
                // Non-numeric version can never match a GCS generation.
                return Err(StoreError::PreconditionFailed {
                    key: key.to_owned(),
                    current: None,
                });
            }
        }

        if let Some(ref range) = opts.range {
            builder = builder.set_read_range(range_to_read_range(range));
        }

        let resp = match tokio::time::timeout(READ_OPEN_DEADLINE, builder.send()).await {
            Ok(r) => r.map_err(|e| map_error(key, e))?,
            Err(_) => return Err(deadline_error("read", key, READ_OPEN_DEADLINE)),
        };
        let obj = resp.object();
        let meta = ObjectMeta {
            key: key.to_owned(),
            size: obj.size as u64,
            version: gen_version(obj.generation),
        };

        Ok(GetResult::Object {
            meta,
            body: unfold_response(key, resp, permit),
        })
    }

    async fn head(&self, key: &str) -> Result<Option<ObjectMeta>> {
        let req = google_cloud_storage::model::GetObjectRequest::new()
            .set_bucket(self.bucket_resource.clone())
            .set_object(key.to_owned());
        match call("head", key, META_DEADLINE, READ_RETRIES, || {
            self.control.get_object().with_request(req.clone()).send()
        })
        .await
        {
            Ok(obj) => Ok(Some(Self::meta_from_object(&obj))),
            Err(e) if e.is_not_found() => Ok(None),
            Err(e) => Err(e.into_store("head", key)),
        }
    }

    async fn put(&self, key: &str, body: PutBody, opts: PutOptions) -> Result<ObjectMeta> {
        let size_hint = match &body {
            PutBody::Bytes(b) => b.len() as u64,
            PutBody::File(p) => tokio::fs::metadata(p).await.map(|m| m.len()).unwrap_or(0),
            PutBody::Stream { len, .. } => *len,
        };
        let deadline = put_deadline(size_hint);
        match tokio::time::timeout(deadline, self.put_inner(key, body, opts)).await {
            Ok(r) => r,
            Err(_) => Err(deadline_error("put", key, deadline)),
        }
    }

    fn supports_compose(&self) -> bool {
        true
    }
    fn compose_is_native(&self) -> bool {
        true
    }

    /// GCS `ComposeObject`: up to 32 sources per call, no data transfer.
    async fn compose(
        &self,
        dest: &str,
        sources: &[String],
        opts: PutOptions,
    ) -> Result<ObjectMeta> {
        use google_cloud_storage::model::compose_object_request::SourceObject;
        if sources.is_empty() || sources.len() > 32 {
            return Err(StoreError::InvalidArgument(format!(
                "compose needs 1..=32 sources, got {}",
                sources.len()
            )));
        }
        let mut destination = google_cloud_storage::model::Object::new()
            .set_bucket(self.bucket_resource.clone())
            .set_name(dest.to_owned());
        if let Some(ct) = opts.content_type {
            destination = destination.set_content_type(ct.to_owned());
        }
        if opts.immutable {
            destination = destination.set_cache_control("public, max-age=31536000, immutable");
        }
        let mut builder = self
            .control
            .compose_object()
            .set_destination(destination)
            .set_source_objects(
                sources
                    .iter()
                    .map(|s| SourceObject::new().set_name(s.clone()))
                    .collect::<Vec<_>>(),
            );
        match &opts.mode {
            PutMode::Overwrite => {}
            PutMode::Create => builder = builder.set_if_generation_match(0),
            PutMode::Update(v) => match parse_generation(v) {
                Some(g) => builder = builder.set_if_generation_match(g),
                None => {
                    return Err(StoreError::PreconditionFailed {
                        key: dest.to_owned(),
                        current: None,
                    });
                }
            },
        }
        let obj = match tokio::time::timeout(PUT_MIN_DEADLINE, builder.send()).await {
            Ok(r) => r.map_err(|e| map_error(dest, e))?,
            Err(_) => return Err(deadline_error("compose", dest, PUT_MIN_DEADLINE)),
        };
        Ok(Self::meta_from_object(&obj))
    }

    async fn delete(&self, key: &str, if_version: Option<Version>) -> Result<()> {
        let mut req = google_cloud_storage::model::DeleteObjectRequest::new()
            .set_bucket(self.bucket_resource.clone())
            .set_object(key.to_owned());

        if let Some(v) = &if_version {
            match parse_generation(v) {
                Some(generation) => {
                    req = req.set_if_generation_match(generation);
                }
                None => {
                    // Non-numeric version can never match a GCS generation.
                    // If the object exists → PreconditionFailed; else → NotFound.
                    if let Some(current) = self.current_generation(key).await {
                        return Err(StoreError::PreconditionFailed {
                            key: key.to_owned(),
                            current: Some(current),
                        });
                    } else {
                        return Err(StoreError::NotFound {
                            key: key.to_owned(),
                        });
                    }
                }
            }
        }

        match call("delete", key, META_DEADLINE, 0, || {
            self.control
                .delete_object()
                .with_request(req.clone())
                .send()
        })
        .await
        {
            Ok(()) => Ok(()),
            Err(e) if e.is_not_found() => {
                if if_version.is_some() {
                    Err(StoreError::NotFound {
                        key: key.to_owned(),
                    })
                } else {
                    Ok(()) // unconditional delete of absent object
                }
            }
            Err(e) => Err(e.into_store("delete", key)),
        }
    }

    fn list(
        &self,
        prefix: &str,
        start_after: Option<&str>,
    ) -> crate::BoxStream<'static, Result<ObjectMeta>> {
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<ObjectMeta>>(32);
        let control = self.control.clone();
        let bucket_resource = self.bucket_resource.clone();
        let prefix = prefix.to_owned();
        let start_after = start_after.map(|s| s.to_owned());

        tokio::spawn(async move {
            let mut page_token = String::new();
            let mut skip_key = start_after;

            loop {
                let mut req = google_cloud_storage::model::ListObjectsRequest::new()
                    .set_parent(bucket_resource.clone())
                    .set_prefix(prefix.clone())
                    .set_page_size(LIST_PAGE_SIZE);

                // GCS list is lexicographic. start_after means "skip this key".
                // lexicographic_start is inclusive, so we filter the exact match.
                if let Some(ref sa) = skip_key {
                    req = req.set_lexicographic_start(sa.clone());
                }

                if !page_token.is_empty() {
                    req = req.set_page_token(page_token.clone());
                }

                let list_key = format!("{prefix}/list");
                let resp = match call("list", &list_key, META_DEADLINE, READ_RETRIES, || {
                    control.list_objects().with_request(req.clone()).send()
                })
                .await
                {
                    Ok(r) => r,
                    Err(e) => {
                        let _ = tx.send(Err(e.into_store("list", &list_key))).await;
                        return;
                    }
                };

                for obj in &resp.objects {
                    if let Some(ref sa) = skip_key {
                        if obj.name == *sa {
                            continue;
                        }
                    }
                    if tx.send(Ok(Self::meta_from_object(obj))).await.is_err() {
                        return; // consumer dropped
                    }
                }

                if resp.next_page_token.is_empty() {
                    break;
                }
                page_token = resp.next_page_token;
                skip_key = None; // only skip on first page
            }
        });

        Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx))
    }

    async fn list_prefixes(&self, prefix: &str) -> Result<Vec<String>> {
        // Delimited listing: GCS returns the common prefixes and omits the objects below them,
        // so `repos/` costs one page per ~1000 owners instead of a walk over every object.
        let mut out = Vec::new();
        let mut page_token = String::new();
        let list_key = format!("{prefix}/list_prefixes");
        loop {
            let mut req = google_cloud_storage::model::ListObjectsRequest::new()
                .set_parent(self.bucket_resource.clone())
                .set_prefix(prefix.to_owned())
                .set_delimiter("/")
                .set_page_size(LIST_PAGE_SIZE);
            if !page_token.is_empty() {
                req = req.set_page_token(page_token.clone());
            }
            let resp = call(
                "list_prefixes",
                &list_key,
                META_DEADLINE,
                READ_RETRIES,
                || self.control.list_objects().with_request(req.clone()).send(),
            )
            .await
            .map_err(|e| e.into_store("list_prefixes", &list_key))?;
            out.extend(resp.prefixes.iter().cloned());
            if resp.next_page_token.is_empty() {
                break;
            }
            page_token = resp.next_page_token;
        }
        out.sort();
        out.dedup();
        Ok(out)
    }

    async fn accel_target(&self, key: &str) -> Option<crate::AccelTarget> {
        let creds = self.creds.as_ref()?;
        let headers = match creds.headers(http::Extensions::new()).await.ok()? {
            google_cloud_auth::credentials::CacheableResource::New { data, .. } => data,
            google_cloud_auth::credentials::CacheableResource::NotModified => return None,
        };
        let authorization = headers
            .get(http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        Some(crate::AccelTarget {
            url: format!(
                "https://storage.googleapis.com/{}/{}",
                self.bucket,
                crate::util::encode_path(key)
            ),
            authorization,
        })
    }

    async fn signed_get_url(&self, key: &str, ttl: std::time::Duration) -> Result<Option<String>> {
        let signer = match &self.signing_signer {
            Some(s) => s.clone(),
            None => return Ok(None),
        };

        let builder = SignedUrlBuilder::for_object(self.bucket_resource.clone(), key.to_owned())
            .with_method(http::Method::GET)
            .with_expiration(ttl)
            .with_url_style(UrlStyle::PathStyle);

        let builder = if let Some(ref email) = self.signing_service_account {
            builder.with_client_email(email.clone())
        } else {
            builder
        };

        match builder.sign_with(&signer).await {
            Ok(url) => Ok(Some(url)),
            Err(e) => Err(StoreError::other(e)),
        }
    }
}

// ---- helpers ----

/// Render a GCS generation (i64) as a decimal string Version.
fn gen_version(generation: i64) -> Version {
    Version::new(generation.to_string())
}

/// Parse a Version back into a generation (i64).
fn parse_generation(v: &Version) -> Option<i64> {
    v.as_str().parse::<i64>().ok()
}

/// Convert our half-open `Range<u64>` to GCS `ReadRange`.
/// `range.start..range.end` means bytes [start, end).
fn range_to_read_range(range: &std::ops::Range<u64>) -> ReadRange {
    let offset = range.start;
    let limit = range.end.saturating_sub(range.start);
    ReadRange::segment(offset, limit)
}

/// Convert the crate's `ReadObjectResponse` into our `ByteStream`.
/// Uses `Option<ReadObjectResponse>` as unfold state so the closure can be
/// `FnMut` (borrows the state rather than moving it).
fn unfold_response(
    key: &str,
    resp: google_cloud_storage::read_object::ReadObjectResponse,
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
) -> ByteStream {
    let key = key.to_owned();
    let stream = futures::stream::unfold(
        (Some(resp), key, permit),
        |(mut resp, key, permit)| async move {
            let r = match resp.as_mut() {
                Some(r) => r,
                None => return None,
            };
            match tokio::time::timeout(READ_CHUNK_DEADLINE, r.next()).await {
                Ok(Some(Ok(bytes))) => Some((Ok(bytes), (resp, key, permit))),
                Ok(Some(Err(e))) => Some((Err(StoreError::other(e)), (resp, key, permit))),
                Ok(None) => None,
                // A stalled body ends the stream with a retryable error; the
                // response is dropped so the next poll yields None.
                Err(_) => Some((
                    Err(deadline_error("read_chunk", &key, READ_CHUNK_DEADLINE)),
                    (None, key, permit),
                )),
            }
        },
    );
    Box::pin(stream)
}

/// A `StreamingSource` wrapping our `ByteStream` for `send_buffered` uploads.
/// `tokio::sync::Mutex` makes it `Sync` (required by the GCS client) while
/// keeping async access to the inner stream.
struct StoreStreamSource {
    inner: tokio::sync::Mutex<ByteStream>,
}

impl StreamingSource for StoreStreamSource {
    type Error = StoreError;
    async fn next(&mut self) -> Option<std::result::Result<Bytes, Self::Error>> {
        let mut guard = self.inner.lock().await;
        guard.next().await
    }
}

/// Apply put-mode preconditions and metadata to a `WriteObject` builder.
/// Builder methods consume `self` and return `Self`, so we chain them.
fn apply_put_opts<T, S>(
    mut builder: google_cloud_storage::builder::storage::WriteObject<T, S>,
    opts: &PutOptions,
) -> google_cloud_storage::builder::storage::WriteObject<T, S>
where
    S: google_cloud_storage::stub::Storage + 'static,
{
    match &opts.mode {
        PutMode::Overwrite => {}
        PutMode::Create => {
            builder = builder.set_if_generation_match(0_i64);
        }
        PutMode::Update(v) => {
            if let Some(generation) = parse_generation(v) {
                builder = builder.set_if_generation_match(generation);
            }
        }
    }

    if let Some(ct) = opts.content_type {
        builder = builder.set_content_type(ct);
    }

    if opts.immutable {
        builder = builder.set_cache_control(IMMUTABLE_CACHE_CONTROL);
    }

    builder
}

// ---- error mapping ----

fn is_not_found(e: &google_cloud_storage::Error) -> bool {
    if let Some(status) = e.status() {
        if status.code == Code::NotFound {
            return true;
        }
    }
    if let Some(code) = e.http_status_code() {
        return code == 404;
    }
    false
}

/// 304 Not Modified: returned when `if_generation_not_match` fails
/// (generation IS the same → object unchanged).
fn is_not_modified(e: &google_cloud_storage::Error) -> bool {
    if let Some(code) = e.http_status_code() {
        if code == 304 {
            return true;
        }
    }
    if let Some(status) = e.status() {
        // GCS returns 304 as FailedPrecondition for if_generation_not_match.
        return status.code == Code::FailedPrecondition;
    }
    false
}

fn is_precondition_failed(e: &google_cloud_storage::Error) -> bool {
    if let Some(status) = e.status() {
        if status.code == Code::FailedPrecondition {
            return true;
        }
    }
    if let Some(code) = e.http_status_code() {
        // GCS JSON API sometimes returns 412 with Code::Unknown.
        return code == 412;
    }
    false
}

fn is_retryable(e: &google_cloud_storage::Error) -> bool {
    if let Some(status) = e.status() {
        return matches!(
            status.code,
            Code::Unavailable
                | Code::DeadlineExceeded
                | Code::ResourceExhausted
                | Code::Internal
                | Code::Aborted
        );
    }
    if let Some(code) = e.http_status_code() {
        return matches!(code, 503 | 504 | 429 | 500);
    }
    e.is_connect() || e.is_io() || e.is_transient_and_before_rpc()
}

fn map_error(key: &str, e: google_cloud_storage::Error) -> StoreError {
    if is_not_found(&e) {
        StoreError::NotFound {
            key: key.to_owned(),
        }
    } else if is_precondition_failed(&e) {
        StoreError::PreconditionFailed {
            key: key.to_owned(),
            current: None,
        }
    } else if is_retryable(&e) {
        StoreError::retryable(e)
    } else {
        StoreError::other(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gen_version_formats_decimal() {
        assert_eq!(gen_version(1234567890).as_str(), "1234567890");
        assert_eq!(gen_version(0).as_str(), "0");
        assert_eq!(gen_version(-1).as_str(), "-1");
    }

    #[test]
    fn parse_generation_roundtrip() {
        let v = gen_version(42);
        assert_eq!(parse_generation(&v), Some(42));
    }

    #[test]
    fn parse_generation_invalid() {
        let v = Version::new("not-a-number");
        assert_eq!(parse_generation(&v), None);
    }

    #[test]
    fn range_to_read_range_basic() {
        let _r = range_to_read_range(&(10..30)); // [10, 30) → offset=10, limit=20
    }

    #[test]
    fn range_to_read_range_zero_start() {
        let _r = range_to_read_range(&(0..100));
    }

    #[test]
    fn range_to_read_range_empty() {
        let _r = range_to_read_range(&(50..50));
    }

    #[test]
    fn is_not_found_via_status() {
        let e = google_cloud_gax::error::Error::service(
            google_cloud_gax::error::rpc::Status::default()
                .set_code(Code::NotFound)
                .set_message("not found"),
        );
        assert!(is_not_found(&e));
        assert!(!is_precondition_failed(&e));
        assert!(!is_retryable(&e));
    }

    #[test]
    fn is_not_found_via_http() {
        let e = google_cloud_gax::error::Error::http(
            404,
            http::HeaderMap::new(),
            bytes::Bytes::from_static(b"not found"),
        );
        assert!(is_not_found(&e));
    }

    #[test]
    fn is_precondition_failed_via_status() {
        let e = google_cloud_gax::error::Error::service(
            google_cloud_gax::error::rpc::Status::default()
                .set_code(Code::FailedPrecondition)
                .set_message("412"),
        );
        assert!(is_precondition_failed(&e));
        assert!(!is_not_found(&e));
    }

    #[test]
    fn is_precondition_failed_via_http() {
        let e = google_cloud_gax::error::Error::http(
            412,
            http::HeaderMap::new(),
            bytes::Bytes::from_static(b"precondition"),
        );
        assert!(is_precondition_failed(&e));
    }

    #[test]
    fn is_not_modified_via_http_304() {
        let e = google_cloud_gax::error::Error::http(
            304,
            http::HeaderMap::new(),
            bytes::Bytes::from_static(b"not modified"),
        );
        assert!(is_not_modified(&e));
    }

    #[test]
    fn is_not_modified_via_failed_precondition() {
        let e = google_cloud_gax::error::Error::service(
            google_cloud_gax::error::rpc::Status::default()
                .set_code(Code::FailedPrecondition)
                .set_message("304 Not Modified"),
        );
        assert!(is_not_modified(&e));
    }

    #[test]
    fn is_retryable_unavailable() {
        let e = google_cloud_gax::error::Error::service(
            google_cloud_gax::error::rpc::Status::default()
                .set_code(Code::Unavailable)
                .set_message("503"),
        );
        assert!(is_retryable(&e));
    }

    #[test]
    fn is_retryable_deadline_exceeded() {
        let e = google_cloud_gax::error::Error::service(
            google_cloud_gax::error::rpc::Status::default()
                .set_code(Code::DeadlineExceeded)
                .set_message("504"),
        );
        assert!(is_retryable(&e));
    }

    #[test]
    fn is_retryable_resource_exhausted() {
        let e = google_cloud_gax::error::Error::service(
            google_cloud_gax::error::rpc::Status::default()
                .set_code(Code::ResourceExhausted)
                .set_message("429"),
        );
        assert!(is_retryable(&e));
    }

    #[test]
    fn is_retryable_internal() {
        let e = google_cloud_gax::error::Error::service(
            google_cloud_gax::error::rpc::Status::default()
                .set_code(Code::Internal)
                .set_message("500"),
        );
        assert!(is_retryable(&e));
    }

    #[test]
    fn is_retryable_aborted() {
        let e = google_cloud_gax::error::Error::service(
            google_cloud_gax::error::rpc::Status::default()
                .set_code(Code::Aborted)
                .set_message("aborted"),
        );
        assert!(is_retryable(&e));
    }

    #[test]
    fn is_retryable_http_503() {
        let e = google_cloud_gax::error::Error::http(
            503,
            http::HeaderMap::new(),
            bytes::Bytes::from_static(b"unavailable"),
        );
        assert!(is_retryable(&e));
    }

    #[test]
    fn is_not_retryable_not_found() {
        let e = google_cloud_gax::error::Error::service(
            google_cloud_gax::error::rpc::Status::default()
                .set_code(Code::NotFound)
                .set_message("404"),
        );
        assert!(!is_retryable(&e));
    }

    #[test]
    fn is_not_retryable_permission_denied() {
        let e = google_cloud_gax::error::Error::service(
            google_cloud_gax::error::rpc::Status::default()
                .set_code(Code::PermissionDenied)
                .set_message("403"),
        );
        assert!(!is_retryable(&e));
        assert!(!is_not_found(&e));
        assert!(!is_precondition_failed(&e));
    }

    #[test]
    fn map_error_not_found() {
        let e = google_cloud_gax::error::Error::service(
            google_cloud_gax::error::rpc::Status::default()
                .set_code(Code::NotFound)
                .set_message("not found"),
        );
        let se = map_error("test/key", e);
        assert!(se.is_not_found());
        assert_eq!(se.to_string(), "object not found: test/key");
    }

    #[test]
    fn map_error_precondition_failed() {
        let e = google_cloud_gax::error::Error::service(
            google_cloud_gax::error::rpc::Status::default()
                .set_code(Code::FailedPrecondition)
                .set_message("412"),
        );
        let se = map_error("test/key", e);
        assert!(se.is_precondition_failed());
        match &se {
            StoreError::PreconditionFailed { key, current } => {
                assert_eq!(key, "test/key");
                assert_eq!(*current, None);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn map_error_retryable() {
        let e = google_cloud_gax::error::Error::service(
            google_cloud_gax::error::rpc::Status::default()
                .set_code(Code::Unavailable)
                .set_message("503"),
        );
        let se = map_error("test/key", e);
        assert!(se.is_retryable());
    }

    #[test]
    fn map_error_other() {
        let e = google_cloud_gax::error::Error::service(
            google_cloud_gax::error::rpc::Status::default()
                .set_code(Code::PermissionDenied)
                .set_message("403"),
        );
        let se = map_error("test/key", e);
        assert!(!se.is_not_found());
        assert!(!se.is_precondition_failed());
        assert!(!se.is_retryable());
    }
}

fn urlencode(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 8);
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

#[cfg(test)]
mod bulk_key_tests {
    use crate::util;

    #[test]
    fn control_plane_keys_are_never_bulk() {
        for k in [
            "prefix/repos/o/r/manifest.pb",
            "prefix/repos/o/r/log/0000000000000001.pb",
            "prefix/repos/o/r/checkpoints/0000000000000001/refs.pb",
            "prefix/repos/o/r/leases/compact.pb",
            "prefix/repos/o/r/bundles/list.pb",
            "prefix/repos/o/r/policy.json",
            "prefix/repos/o/r/cache/api/v1/abc.json",
        ] {
            assert!(!util::is_bulk_key(k), "{k} must be control plane");
        }
        for k in [
            "prefix/repos/o/r/wal/abc.pack",
            "prefix/repos/o/r/wal/abc.idx",
            "prefix/repos/o/r/wal/abc.commit-graph",
            "prefix/repos/o/r/bundles/weekly/2026-abc.bundle",
            "prefix/repos/o/r/lfs/objects/ab/cd/abcd",
        ] {
            assert!(util::is_bulk_key(k), "{k} must be bulk");
        }
    }
}

#[cfg(test)]
mod resume_tests {
    use super::*;
    use futures::StreamExt;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// A "GCS" that serves a 3000-byte object but cuts the connection after
    /// 1000 bytes of the FIRST two responses; honours `Range` and
    /// `ifGenerationMatch`. The resuming reader must deliver all 3000 bytes
    /// in order with exactly two resumes.
    #[tokio::test]
    async fn bulk_read_resumes_after_mid_stream_cut() {
        use axum::{Router, extract::Query, http::HeaderMap, routing::get};
        let data: std::sync::Arc<Vec<u8>> =
            std::sync::Arc::new((0..3000u32).map(|i| (i % 251) as u8).collect());
        let hits = std::sync::Arc::new(AtomicUsize::new(0));
        let d = data.clone();
        let h = hits.clone();
        let app = Router::new().route(
            "/storage/v1/b/{bucket}/o/{key}",
            get(
                move |headers: HeaderMap,
                      Query(q): Query<std::collections::HashMap<String, String>>| {
                    let d = d.clone();
                    let h = h.clone();
                    async move {
                        let n = h.fetch_add(1, Ordering::SeqCst);
                        assert_eq!(q.get("alt").map(String::as_str), Some("media"));
                        // Resumes must pin the generation.
                        if n > 0 {
                            assert_eq!(q.get("ifGenerationMatch").map(String::as_str), Some("7"));
                        }
                        let (start, end) = headers
                            .get("range")
                            .and_then(|v| v.to_str().ok())
                            .and_then(|v| v.strip_prefix("bytes="))
                            .and_then(|v| v.split_once('-'))
                            .map(|(a, b)| {
                                (a.parse::<usize>().unwrap(), b.parse::<usize>().unwrap() + 1)
                            })
                            .unwrap_or((0, d.len()));
                        let body: Vec<u8> = d[start..end].to_vec();
                        let cut = n < 2;
                        let stream = futures::stream::iter(
                            body.chunks(100).map(|c| c.to_vec()).collect::<Vec<_>>(),
                        )
                        .enumerate()
                        .then(move |(i, c)| async move {
                            if cut && i >= 10 {
                                // Let hyper flush the first 1000 bytes before the cut.
                                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                                Err(std::io::Error::other("boom"))
                            } else {
                                Ok::<_, std::io::Error>(bytes::Bytes::from(c))
                            }
                        });
                        let mut resp =
                            axum::response::Response::new(axum::body::Body::from_stream(stream));
                        *resp.status_mut() = if headers.contains_key("range") {
                            axum::http::StatusCode::PARTIAL_CONTENT
                        } else {
                            axum::http::StatusCode::OK
                        };
                        resp.headers_mut().insert(
                            "content-range",
                            format!("bytes {}-{}/{}", start, end - 1, d.len())
                                .parse()
                                .unwrap(),
                        );
                        resp.headers_mut()
                            .insert("x-goog-generation", "7".parse().unwrap());
                        resp
                    }
                },
            ),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let bulk = BulkHttp::for_tests(format!("http://{addr}"), "b".into());
        let (size, generation, mut body) = bulk.read("k", None, None).await.unwrap();
        assert_eq!((size, generation), (3000, Some(7)));
        let mut got = Vec::new();
        while let Some(chunk) = body.next().await {
            got.extend_from_slice(&chunk.expect("resumed stream never surfaces the cut"));
        }
        assert_eq!(got, *data, "all bytes, in order, across two resumes");
        assert_eq!(
            hits.load(Ordering::SeqCst),
            3,
            "first request + two resumes"
        );
    }
}
