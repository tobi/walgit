//! Object store abstraction with compare-and-swap semantics.
//!
//! Every backend exposes exactly the primitives walgit needs to build
//! linearizable coordination on top of a bucket:
//!
//! * versioned reads (`If-None-Match` → `NotModified`, cheap freshness check),
//! * conditional writes (`Create` = if-absent, `Update(v)` = CAS on version),
//! * conditional deletes, range reads, streaming bodies, prefix listing.
//!
//! [`Version`] is opaque to callers: GCS generation, S3/rustfs ETag, or a
//! counter in [`memory::MemoryStore`]. Callers must never parse it.

use std::{fmt, ops::Range, pin::Pin, sync::Arc};

use bytes::Bytes;
use futures::Stream;
use tracing::Instrument;

#[cfg(feature = "azure")]
pub mod azure;
pub mod coord;
pub use coord::CoordError;
pub mod fault;
#[cfg(feature = "gcs")]
pub mod gcs;
pub mod memory;
#[cfg(feature = "s3")]
pub mod s3;
pub mod util;

pub type BoxStream<'a, T> = Pin<Box<dyn Stream<Item = T> + Send + 'a>>;
pub type ByteStream = BoxStream<'static, Result<Bytes, StoreError>>;

/// Opaque object version (GCS generation / ETag / counter). Compare only for equality.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Version(Arc<str>);

impl Version {
    pub fn new(s: impl Into<Arc<str>>) -> Self {
        Version(s.into())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl fmt::Debug for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "v{:?}", &*self.0)
    }
}
impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// What an edge needs to fetch one object itself (see [`ObjectStore::accel_target`]).
#[derive(Debug, Clone)]
pub struct AccelTarget {
    /// Absolute URL the edge proxies to (the object, not a listing).
    pub url: String,
    /// `Authorization` header value for that request, when the URL itself is not credentialed.
    pub authorization: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectMeta {
    pub key: String,
    /// Size of the whole object in bytes — also for range reads (HTTP
    /// `Content-Range: bytes a-b/total` needs it).
    pub size: u64,
    pub version: Version,
}

#[derive(Clone, Debug, Default)]
pub struct GetOptions {
    /// Return `GetResult::NotModified` if the current version equals this.
    pub if_none_match: Option<Version>,
    /// Fail with `PreconditionFailed` if the current version differs from this.
    pub if_match: Option<Version>,
    /// Byte range to read (half-open). `None` = whole object.
    pub range: Option<Range<u64>>,
}

pub enum GetResult {
    NotModified { version: Version },
    Object { meta: ObjectMeta, body: ByteStream },
}

impl GetResult {
    /// Collect the body into memory (small objects: indexes, leases, snapshots).
    pub async fn bytes(self) -> Result<Option<(ObjectMeta, Bytes)>, StoreError> {
        match self {
            GetResult::NotModified { .. } => Ok(None),
            GetResult::Object { meta, body } => {
                let b = util::collect(body, meta.size as usize).await?;
                Ok(Some((meta, b)))
            }
        }
    }
    pub fn version(&self) -> &Version {
        match self {
            GetResult::NotModified { version } => version,
            GetResult::Object { meta, .. } => &meta.version,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PutMode {
    /// Unconditional overwrite.
    Overwrite,
    /// Only if the object does not exist (if-generation-match: 0 / If-None-Match: *).
    Create,
    /// Only if the current version equals the given one (CAS).
    Update(Version),
}

/// Body for `put`. Streams must have a known length (object stores require it
/// for single-shot puts; backends may switch to multipart/resumable above a threshold).
pub enum PutBody {
    Bytes(Bytes),
    Stream {
        len: u64,
        stream: ByteStream,
    },
    /// Local file; backends may use sendfile/mmap or read in chunks.
    File(std::path::PathBuf),
}

impl From<Bytes> for PutBody {
    fn from(b: Bytes) -> Self {
        PutBody::Bytes(b)
    }
}
impl From<Vec<u8>> for PutBody {
    fn from(b: Vec<u8>) -> Self {
        PutBody::Bytes(Bytes::from(b))
    }
}

#[derive(Clone, Debug, Default)]
pub struct PutOptions {
    pub mode: PutMode,
    pub content_type: Option<&'static str>,
    /// Hint: content never changes under this key (all `wal/` objects). Backends may
    /// set long cache headers.
    pub immutable: bool,
}
impl Default for PutMode {
    fn default() -> Self {
        PutMode::Overwrite
    }
}
impl From<PutMode> for PutOptions {
    fn from(mode: PutMode) -> Self {
        PutOptions {
            mode,
            ..Default::default()
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("object not found: {key}")]
    NotFound { key: String },
    /// CAS/precondition failure. `current` is the observed version if the backend reports it.
    #[error("precondition failed on {key} (current version: {current:?})")]
    PreconditionFailed {
        key: String,
        current: Option<Version>,
    },
    /// Transient: caller may retry with backoff.
    #[error("retryable store error: {0}")]
    Retryable(#[source] anyhow::Error),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl StoreError {
    pub fn is_not_found(&self) -> bool {
        matches!(self, StoreError::NotFound { .. })
    }
    pub fn is_precondition_failed(&self) -> bool {
        matches!(self, StoreError::PreconditionFailed { .. })
    }
    pub fn is_retryable(&self) -> bool {
        matches!(self, StoreError::Retryable(_))
    }
    pub fn other(e: impl Into<anyhow::Error>) -> Self {
        StoreError::Other(e.into())
    }
    pub fn retryable(e: impl Into<anyhow::Error>) -> Self {
        StoreError::Retryable(e.into())
    }
}

pub type Result<T, E = StoreError> = std::result::Result<T, E>;

#[async_trait::async_trait]
pub trait ObjectStore: Send + Sync + 'static {
    /// Human-readable backend id for logs/metrics ("gcs", "s3", "memory").
    fn backend(&self) -> &'static str;
    /// Whether this store is a prefixing wrapper. Used to avoid duplicate
    /// operation spans when prefixes are nested.
    fn is_prefixed(&self) -> bool {
        false
    }

    async fn get(&self, key: &str, opts: GetOptions) -> Result<GetResult>;

    /// Metadata only. `Ok(None)` if absent.
    async fn head(&self, key: &str) -> Result<Option<ObjectMeta>>;

    async fn put(&self, key: &str, body: PutBody, opts: PutOptions) -> Result<ObjectMeta>;

    /// Delete. `if_version` = CAS delete. Deleting an absent object is `Ok(())`
    /// when unconditional and `NotFound` when conditional.
    async fn delete(&self, key: &str, if_version: Option<Version>) -> Result<()>;

    /// Lexicographically ordered listing of keys with `prefix`, starting after
    /// `start_after` if given. Backends page internally.
    fn list(
        &self,
        prefix: &str,
        start_after: Option<&str>,
    ) -> BoxStream<'static, Result<ObjectMeta>>;

    /// The "directories" directly below `prefix`: every distinct `prefix + <segment>/` among the
    /// keys under `prefix` (a delimited listing — GCS/S3 `delimiter = "/"` — so it walks the
    /// prefixes, never the objects under them). Sorted. `prefix` must end in `/` or be empty.
    async fn list_prefixes(&self, prefix: &str) -> Result<Vec<String>>;

    /// Presigned/authenticated URL for direct client download (bundles, LFS).
    /// Backends without support return `Ok(None)`.
    async fn signed_get_url(
        &self,
        _key: &str,
        _ttl: std::time::Duration,
    ) -> Result<Option<String>> {
        Ok(None)
    }

    /// How a trusted edge (nginx `X-Accel-Redirect`) fetches `key` on the client's behalf:
    /// a URL it can `proxy_pass`, and the `Authorization` value to send with it, if any.
    /// GCS: the path-style URL + this process's bearer token (no token on the edge, nothing to
    /// refresh). S3: a presigned GET URL (`Range` is not a signed header, so the edge may slice).
    /// Backends without one return `None` and the bytes stream through walgit.
    async fn accel_target(&self, _key: &str) -> Option<AccelTarget> {
        None
    }

    /// Whether [`ObjectStore::compose`] is available. GCS: native (<= 32 sources
    /// per call, no data movement). S3: multipart `UploadPartCopy` (server-side copy,
    /// no bytes through this process beyond one small part).
    fn supports_compose(&self) -> bool {
        false
    }

    /// Whether compose is a metadata operation (GCS). When false (S3) a compose still
    /// moves bytes inside the bucket, so callers that only want a parallel upload of one
    /// file use the backend's multipart PUT instead of part objects + compose.
    fn compose_is_native(&self) -> bool {
        false
    }

    /// Server-side concatenation of `sources` (in order) into `dest`, honouring
    /// `opts.mode` on the destination. Sources are left in place.
    async fn compose(
        &self,
        _dest: &str,
        _sources: &[String],
        _opts: PutOptions,
    ) -> Result<ObjectMeta> {
        Err(StoreError::InvalidArgument(
            "compose is not supported by this backend".into(),
        ))
    }
}

/// Convenience extension methods shared by all backends.
#[async_trait::async_trait]
pub trait ObjectStoreExt: ObjectStore {
    /// Full read into memory. `Ok(None)` if absent.
    async fn get_bytes(&self, key: &str) -> Result<Option<(ObjectMeta, Bytes)>> {
        match self.get(key, GetOptions::default()).await {
            Ok(r) => r.bytes().await,
            Err(StoreError::NotFound { .. }) => Ok(None),
            Err(e) => Err(e),
        }
    }
    /// Freshness check: `Ok(None)` if unchanged, `Ok(Some(..))` with the new body otherwise,
    /// `NotFound` if deleted.
    async fn get_if_changed(
        &self,
        key: &str,
        known: &Version,
    ) -> Result<Option<(ObjectMeta, Bytes)>> {
        let r = self
            .get(
                key,
                GetOptions {
                    if_none_match: Some(known.clone()),
                    ..Default::default()
                },
            )
            .await?;
        r.bytes().await
    }
    async fn put_bytes(
        &self,
        key: &str,
        body: impl Into<Bytes> + Send,
        mode: PutMode,
    ) -> Result<ObjectMeta> {
        self.put(key, PutBody::Bytes(body.into()), mode.into())
            .await
    }
    async fn exists(&self, key: &str) -> Result<bool> {
        Ok(self.head(key).await?.is_some())
    }
}
impl<T: ObjectStore + ?Sized> ObjectStoreExt for T {}

pub type DynStore = Arc<dyn ObjectStore>;

/// A store scoped to a key prefix (e.g. one repository). All keys are relative.
#[derive(Clone)]
pub struct Prefixed {
    inner: DynStore,
    prefix: Arc<str>,
}

impl Prefixed {
    pub fn new(inner: DynStore, prefix: impl Into<Arc<str>>) -> Self {
        let prefix: Arc<str> = prefix.into();
        debug_assert!(prefix.is_empty() || prefix.ends_with('/'));
        Prefixed { inner, prefix }
    }
    pub fn prefix(&self) -> &str {
        &self.prefix
    }
    pub fn inner(&self) -> &DynStore {
        &self.inner
    }
    fn full(&self, key: &str) -> String {
        let mut s = String::with_capacity(self.prefix.len() + key.len());
        s.push_str(&self.prefix);
        s.push_str(key);
        s
    }
    fn strip(&self, mut meta: ObjectMeta) -> ObjectMeta {
        if let Some(rest) = meta.key.strip_prefix(&*self.prefix) {
            meta.key = rest.to_owned();
        }
        meta
    }
}

#[async_trait::async_trait]
impl ObjectStore for Prefixed {
    fn backend(&self) -> &'static str {
        self.inner.backend()
    }
    fn is_prefixed(&self) -> bool {
        true
    }
    async fn get(&self, key: &str, opts: GetOptions) -> Result<GetResult> {
        let instrument = !self.inner.is_prefixed();
        let full_key = self.full(key);
        // No span at all for nested prefix layers (avoids duplicate lines).
        let span = if !instrument {
            tracing::Span::none()
        } else {
            tracing::info_span!(
                "store.get",
                backend = self.inner.backend(),
                key = %full_key,
                bytes = 0u64,
                queued_ms = tracing::field::Empty,
                outcome = tracing::field::Empty,
                error = tracing::field::Empty,
            )
        };
        let result = if instrument {
            self.inner
                .get(&full_key, opts)
                .instrument(span.clone())
                .await
        } else {
            self.inner.get(&full_key, opts).await
        };
        match &result {
            Ok(GetResult::Object { meta, .. }) => {
                span.record("bytes", meta.size);
                span.record("outcome", "ok");
            }
            Ok(GetResult::NotModified { .. }) => {
                span.record("outcome", "not_modified");
            }
            Err(e) if e.is_not_found() => {
                span.record("outcome", "not_found");
            }
            // A CAS loss (lease held elsewhere, concurrent manifest writer) is
            // the protocol working, not a failure: never `outcome=error`.
            Err(e) if e.is_precondition_failed() => {
                span.record("outcome", "precondition_failed");
            }
            Err(e) => {
                span.record("outcome", "error");
                span.record("error", tracing::field::display(e));
            }
        }
        match result {
            Ok(GetResult::Object { meta, body }) => Ok(GetResult::Object {
                meta: self.strip(meta),
                body,
            }),
            Ok(GetResult::NotModified { version }) => Ok(GetResult::NotModified { version }),
            Err(e) => Err(e),
        }
    }
    async fn head(&self, key: &str) -> Result<Option<ObjectMeta>> {
        let instrument = !self.inner.is_prefixed();
        let full_key = self.full(key);
        // No span at all for nested prefix layers (avoids duplicate lines).
        let span = if !instrument {
            tracing::Span::none()
        } else {
            tracing::info_span!(
                "store.head",
                backend = self.inner.backend(),
                key = %full_key,
                bytes = 0u64,
                queued_ms = tracing::field::Empty,
                outcome = tracing::field::Empty,
                error = tracing::field::Empty,
            )
        };
        let result = if instrument {
            self.inner.head(&full_key).instrument(span.clone()).await
        } else {
            self.inner.head(&full_key).await
        };
        match &result {
            Ok(Some(meta)) => {
                span.record("bytes", meta.size);
                span.record("outcome", "ok");
            }
            Ok(None) => {
                span.record("outcome", "not_found");
            }
            Err(e) if e.is_not_found() => {
                span.record("outcome", "not_found");
            }
            // A CAS loss (lease held elsewhere, concurrent manifest writer) is
            // the protocol working, not a failure: never `outcome=error`.
            Err(e) if e.is_precondition_failed() => {
                span.record("outcome", "precondition_failed");
            }
            Err(e) => {
                span.record("outcome", "error");
                span.record("error", tracing::field::display(e));
            }
        }
        Ok(result?.map(|m| self.strip(m)))
    }
    async fn put(&self, key: &str, body: PutBody, opts: PutOptions) -> Result<ObjectMeta> {
        let bytes = match &body {
            PutBody::Bytes(b) => b.len() as u64,
            PutBody::Stream { len, .. } => *len,
            PutBody::File(p) => std::fs::metadata(p).map(|m| m.len()).unwrap_or(0),
        };
        let instrument = !self.inner.is_prefixed();
        let full_key = self.full(key);
        // No span at all for nested prefix layers (avoids duplicate lines).
        let span = if !instrument {
            tracing::Span::none()
        } else {
            tracing::info_span!(
                "store.put",
                backend = self.inner.backend(),
                key = %full_key,
                bytes,
                outcome = tracing::field::Empty,
                error = tracing::field::Empty,
            )
        };
        let result = if instrument {
            self.inner
                .put(&full_key, body, opts)
                .instrument(span.clone())
                .await
        } else {
            self.inner.put(&full_key, body, opts).await
        };
        match &result {
            Ok(_) => {
                span.record("outcome", "ok");
            }
            Err(e) if e.is_not_found() => {
                span.record("outcome", "not_found");
            }
            // A CAS loss (lease held elsewhere, concurrent manifest writer) is
            // the protocol working, not a failure: never `outcome=error`.
            Err(e) if e.is_precondition_failed() => {
                span.record("outcome", "precondition_failed");
            }
            Err(e) => {
                span.record("outcome", "error");
                span.record("error", tracing::field::display(e));
            }
        }
        result.map(|m| self.strip(m))
    }
    async fn delete(&self, key: &str, if_version: Option<Version>) -> Result<()> {
        let instrument = !self.inner.is_prefixed();
        let full_key = self.full(key);
        // No span at all for nested prefix layers (avoids duplicate lines).
        let span = if !instrument {
            tracing::Span::none()
        } else {
            tracing::info_span!(
                "store.delete",
                backend = self.inner.backend(),
                key = %full_key,
                outcome = tracing::field::Empty,
                error = tracing::field::Empty,
            )
        };
        let result = if instrument {
            self.inner
                .delete(&full_key, if_version)
                .instrument(span.clone())
                .await
        } else {
            self.inner.delete(&full_key, if_version).await
        };
        match &result {
            Ok(()) => {
                span.record("outcome", "ok");
            }
            Err(e) if e.is_not_found() => {
                span.record("outcome", "not_found");
            }
            // A CAS loss (lease held elsewhere, concurrent manifest writer) is
            // the protocol working, not a failure: never `outcome=error`.
            Err(e) if e.is_precondition_failed() => {
                span.record("outcome", "precondition_failed");
            }
            Err(e) => {
                span.record("outcome", "error");
                span.record("error", tracing::field::display(e));
            }
        }
        result
    }
    fn list(
        &self,
        prefix: &str,
        start_after: Option<&str>,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        let full_prefix = self.full(prefix);
        let _span = (!self.inner.is_prefixed()).then(|| {
            tracing::debug_span!(
                "store.list",
                backend = self.inner.backend(),
                prefix = %full_prefix,
            )
            .entered()
        });
        use futures::StreamExt;
        let this = self.clone();
        let start_after = start_after.map(|s| self.full(s));
        Box::pin(
            self.inner
                .list(&full_prefix, start_after.as_deref())
                .map(move |r| r.map(|m| this.strip(m))),
        )
    }
    async fn list_prefixes(&self, prefix: &str) -> Result<Vec<String>> {
        let full_prefix = self.full(prefix);
        let span = (!self.inner.is_prefixed()).then(|| {
            tracing::debug_span!(
                "store.list_prefixes",
                backend = self.inner.backend(),
                prefix = %full_prefix,
            )
        });
        let fut = self.inner.list_prefixes(&full_prefix);
        let out = match span {
            Some(span) => fut.instrument(span).await?,
            None => fut.await?,
        };
        Ok(out
            .into_iter()
            .map(|p| {
                p.strip_prefix(&*self.prefix)
                    .map(str::to_owned)
                    .unwrap_or(p)
            })
            .collect())
    }
    async fn signed_get_url(&self, key: &str, ttl: std::time::Duration) -> Result<Option<String>> {
        self.inner.signed_get_url(&self.full(key), ttl).await
    }
    async fn accel_target(&self, key: &str) -> Option<AccelTarget> {
        self.inner.accel_target(&self.full(key)).await
    }
    fn supports_compose(&self) -> bool {
        self.inner.supports_compose()
    }
    fn compose_is_native(&self) -> bool {
        self.inner.compose_is_native()
    }
    async fn compose(
        &self,
        dest: &str,
        sources: &[String],
        opts: PutOptions,
    ) -> Result<ObjectMeta> {
        let full_sources: Vec<String> = sources.iter().map(|s| self.full(s)).collect();
        let meta = self
            .inner
            .compose(&self.full(dest), &full_sources, opts)
            .await?;
        Ok(self.strip(meta))
    }
}

/// Open a store backend by config, wrapped in the global key prefix.
pub async fn open_store(cfg: &walgit_config::Config) -> anyhow::Result<DynStore> {
    let prefix = cfg.store_prefix();
    let inner: DynStore = match cfg.store.backend {
        walgit_config::StoreBackend::Memory => Arc::new(memory::MemoryStore::new()),
        walgit_config::StoreBackend::S3 => {
            #[cfg(feature = "s3")]
            {
                Arc::new(s3::S3Store::new(&cfg.store).await?)
            }
            #[cfg(not(feature = "s3"))]
            {
                anyhow::bail!("s3 backend requires the `s3` feature")
            }
        }
        walgit_config::StoreBackend::Gcs => {
            #[cfg(feature = "gcs")]
            {
                Arc::new(
                    gcs::GcsStore::new(&cfg.store)
                        .await?
                        .with_permit_wait_warn(cfg.telemetry.lock_wait_warn),
                )
            }
            #[cfg(not(feature = "gcs"))]
            {
                anyhow::bail!("gcs backend requires the `gcs` feature")
            }
        }
        walgit_config::StoreBackend::Azure => {
            #[cfg(feature = "azure")]
            {
                Arc::new(azure::AzureStore::new(&cfg.store).await?)
            }
            #[cfg(not(feature = "azure"))]
            {
                anyhow::bail!("azure backend requires the `azure` feature")
            }
        }
    };
    if prefix.is_empty() {
        Ok(inner)
    } else {
        Ok(Arc::new(Prefixed::new(inner, prefix)))
    }
}
