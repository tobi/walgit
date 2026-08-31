//! Azure Blob Storage backend.
//!
//! Uses `azure_storage_blob` for every operation. Auth is always Entra ID
//! (`azure_identity`): the config file names only a credential *kind*, never a
//! secret, and the SDK's `BearerTokenAuthorizationPolicy` fetches and refreshes
//! tokens for the hardcoded `https://storage.azure.com/.default` scope.
//!
//! ## Naming
//!
//! `store.azure.account` is the storage account; `store.bucket` is the container
//! inside it. The two combine into the container URL the SDK client is built
//! from: `https://{account}.blob.core.windows.net/{container}`. Container names
//! are restricted to lowercase alphanumerics and hyphens, so no percent-encoding
//! is needed when splicing one into the URL.
//!
//! ## Version tokens
//!
//! Azure `ETag`s are used as opaque `Version` strings, with quotes stripped
//! consistently on read and never stored — the same contract as `s3.rs`. Azure
//! `ETag`s look like `"0x8D1..."`: the quotes are part of the wire format, not of
//! the value. [`strip_etag`] and [`to_wire_etag`] are the only two places that
//! know this, so the whole round trip is one edit away if the live service
//! disagrees. Callers never parse the token; equality comparison suffices.
//!
//! ## Conditional PUT
//!
//! `PutMode::Create`    → `If-None-Match: *`   (blob must not exist).
//! `PutMode::Update(v)` → `If-Match: "<etag>"` (CAS on the current `ETag`).
//! Azure answers a lost create race with **409 `BlobAlreadyExists`** and a failed
//! `If-Match` with **412 `ConditionNotMet`**; both are [`StoreError::PreconditionFailed`],
//! whose `current` we fill with a follow-up HEAD, exactly as `s3.rs` does.
//!
//! ## Conditional DELETE
//!
//! Unlike S3, Azure has a **native** conditional delete: `Delete Blob` takes
//! `If-Match`. The HEAD + compare + DELETE emulation `s3.rs` documents (and its
//! check-then-act race) therefore does not apply here — the service decides.
//! A HEAD is issued only *after* a lost conditional delete, to tell "gone"
//! (`NotFound`) from "changed" (`PreconditionFailed`) and to name the winner.
//!
//! ## Status
//!
//! `get`/`head`/`put`/`delete` are implemented for non-chunked bodies. Bodies at
//! or above `multipart_threshold` route to `chunked_put`, and listing is still a
//! stub; both land in the tasks that follow, which is also what the
//! currently-unread fields below are for.

use std::num::NonZero;
use std::ops::Range;
use std::sync::Arc;

use azure_core::credentials::TokenCredential;
use azure_core::error::ErrorKind;
use azure_core::http::headers::{ETAG, HeaderName, Headers};
use azure_core::http::{Etag, Url};
use azure_identity::{
    ClientSecretCredential, DeveloperToolsCredential, ManagedIdentityCredential,
    WorkloadIdentityCredential,
};
use azure_storage_blob::models::{
    BlobClientDeleteOptions, BlobClientDownloadOptions, BlobClientGetPropertiesResultHeaders,
    BlockBlobClientUploadOptions, HttpRange,
};
use azure_storage_blob::{BlobContainerClient, BlobServiceClient};
use bytes::Bytes;
use futures::StreamExt;
use walgit_config::AzureCredentialKind;

use crate::{
    BoxStream, GetOptions, GetResult, ObjectMeta, ObjectStore, PutBody, PutMode, PutOptions,
    Result, StoreError, Version, util,
};

/// `Cache-Control` written for objects the caller marked immutable (`wal/`).
const IMMUTABLE_CACHE_CONTROL: &str = "public, max-age=31536000, immutable";

/// `Content-Range` — read for the *total* object size on a range GET.
const CONTENT_RANGE: HeaderName = HeaderName::from_static("content-range");

/// One request at a time. walgit does its own chunking and wants the headers of
/// a single GET/PUT, not a stitched partitioned transfer; `NonZero::MIN` is 1.
const SEQUENTIAL: NonZero<usize> = NonZero::<usize>::MIN;

/// Azure Blob Storage object store.
///
/// The `allow` covers fields that are populated here and first read by the
/// operation tasks that follow (SAS signing, accelerated reads, chunked put).
#[allow(dead_code)]
pub struct AzureStore {
    /// Client scoped to the container named by `store.bucket`.
    container: BlobContainerClient,
    /// Account-scoped client — only `get_user_delegation_key` (SAS signing) needs it.
    service: BlobServiceClient,
    /// Kept for the accel/SAS paths, which need a bearer token of their own.
    credential: Arc<dyn TokenCredential>,
    /// reqwest client for streaming GETs via SAS URLs.
    http: reqwest::Client,
    account: String,
    bucket: String,
    /// Resolved blob endpoint for the account, no trailing slash.
    endpoint: String,
    multipart_threshold: u64,
    multipart_part_size: u64,
}

impl AzureStore {
    /// Build a store from `walgit-config::StoreConfig`.
    ///
    /// Fails closed when `store.azure.account` is unset: there is no sensible
    /// default account, and a wrong endpoint would surface as opaque 404s.
    ///
    /// Credentials come from the environment, never from config:
    /// `DeveloperTools` shells out to `az`/`azd`, `ClientSecret` reads
    /// `AZURE_TENANT_ID` / `AZURE_CLIENT_ID` / `AZURE_CLIENT_SECRET`, and the
    /// managed/workload identity kinds read their pod or instance metadata.
    // Nothing here awaits yet; the signature matches the other backends so
    // `open_store` can await every constructor the same way.
    #[allow(clippy::unused_async)]
    pub async fn new(cfg: &walgit_config::StoreConfig) -> anyhow::Result<Self> {
        let account = cfg.azure.account.trim();
        if account.is_empty() {
            anyhow::bail!(
                "azure: `store.azure.account` is required (the storage account name; \
                 `store.bucket` names the container inside it)"
            );
        }

        // Empty endpoint = the public cloud host for this account. An override
        // exists for Azurite and the sovereign clouds.
        let configured = cfg.azure.endpoint.trim();
        let endpoint = if configured.is_empty() {
            format!("https://{account}.blob.core.windows.net")
        } else {
            configured.trim_end_matches('/').to_owned()
        };
        let container_url = format!("{endpoint}/{}", cfg.bucket);

        // Only the credential *kind* is configurable. The client secret stays
        // inside `azure_core::credentials::Secret`, which redacts its own Debug,
        // and is never logged or echoed into an error.
        let credential: Arc<dyn TokenCredential> = match cfg.azure.credential {
            AzureCredentialKind::DeveloperTools => DeveloperToolsCredential::new(None)?,
            AzureCredentialKind::ClientSecret => {
                let tenant_id = require_env("AZURE_TENANT_ID")?;
                let client_id = require_env("AZURE_CLIENT_ID")?;
                let secret = require_env("AZURE_CLIENT_SECRET")?;
                ClientSecretCredential::new(&tenant_id, client_id, secret.into(), None)?
            }
            AzureCredentialKind::ManagedIdentity => ManagedIdentityCredential::new(None)?,
            AzureCredentialKind::WorkloadIdentity => WorkloadIdentityCredential::new(None)?,
        };

        // Both clients reject a non-https URL once a credential is attached, so a
        // misconfigured endpoint cannot leak a bearer token onto the wire.
        let service =
            BlobServiceClient::new(parse_url(&endpoint)?, Some(Arc::clone(&credential)), None)?;
        let container = BlobContainerClient::new(
            parse_url(&container_url)?,
            Some(Arc::clone(&credential)),
            None,
        )?;
        let http = reqwest::Client::builder().build()?;

        Ok(AzureStore {
            container,
            service,
            credential,
            http,
            account: account.to_owned(),
            bucket: cfg.bucket.clone(),
            endpoint,
            multipart_threshold: cfg.multipart_threshold.as_u64(),
            multipart_part_size: cfg.multipart_part_size.as_u64(),
        })
    }

    /// Staged-block upload for bodies at or above `multipart_threshold`.
    ///
    /// Stub until the chunked-put task lands; `put` routes to it by length so
    /// the size decision already lives in one place.
    // Nothing awaits yet — the signature is the one the chunked implementation needs.
    #[allow(clippy::unused_async)]
    async fn chunked_put(
        &self,
        key: &str,
        _body: PutBody,
        len: u64,
        _opts: &PutOptions,
    ) -> Result<ObjectMeta> {
        Err(StoreError::InvalidArgument(format!(
            "azure: chunked put not implemented yet ({key}, {len} bytes)"
        )))
    }
}

/// Reads a required environment variable, naming it — never its value — on failure.
fn require_env(name: &str) -> anyhow::Result<String> {
    std::env::var(name).map_err(|_| anyhow::anyhow!("azure: env var {name} not set"))
}

/// Parses a client URL, reporting the URL (which carries no credential) on failure.
fn parse_url(url: &str) -> anyhow::Result<Url> {
    Url::parse(url).map_err(|e| anyhow::anyhow!("azure: invalid url {url}: {e}"))
}

/// Placeholder for the operations the following tasks implement.
fn not_implemented() -> StoreError {
    StoreError::InvalidArgument("azure: not implemented yet".into())
}

// ---- ETag <-> Version --------------------------------------------------

/// Strips the quotes Azure wraps an `ETag` in on the wire (`"0x8D1"` → `0x8D1`).
fn strip_etag(tag: &Etag) -> String {
    tag.as_ref().trim_matches('"').to_owned()
}

/// The `Version` for a response `ETag`, or the empty token when the service sent
/// none — the same fallback `s3.rs` uses.
fn version_from_etag(tag: Option<&Etag>) -> Version {
    Version::new(tag.map(strip_etag).unwrap_or_default())
}

/// The wire form of a stored `Version` for `If-Match` / `If-None-Match`.
///
/// Idempotent: a token that already carries quotes is not quoted twice.
fn to_wire_etag(v: &Version) -> Etag {
    Etag::from(format!("\"{}\"", v.as_str().trim_matches('"')))
}

// ---- error classification ----------------------------------------------

/// HTTP status of an SDK error, when it carries one.
fn http_status(e: &azure_core::Error) -> Option<u16> {
    e.http_status().map(u16::from)
}

/// A conditional GET whose precondition held: the SDK surfaces the 304 as an
/// `Err`, so `get` has to look for it before classifying anything.
fn is_not_modified(e: &azure_core::Error) -> bool {
    http_status(e) == Some(304)
}

/// The `ETag` from an error's raw response, when the SDK kept one (it does for
/// every response `check_success` rejects, 304 included).
fn version_from_error(e: &azure_core::Error) -> Option<Version> {
    let ErrorKind::HttpResponse {
        raw_response: Some(raw),
        ..
    } = e.kind()
    else {
        return None;
    };
    raw.headers()
        .get_optional_str(&ETAG)
        .map(|s| Version::new(s.trim_matches('"')))
}

/// Maps an SDK error onto the store's error vocabulary.
///
/// 404 → `NotFound`, 409 (`BlobAlreadyExists`, a lost `If-None-Match: *` race)
/// and 412 (`ConditionNotMet`) → `PreconditionFailed`, 429 and 5xx → `Retryable`,
/// everything else → `Other`. Callers that can observe the current version
/// (`put`) fill `current` themselves.
fn classify(key: &str, e: azure_core::Error) -> StoreError {
    match http_status(&e) {
        Some(404) => StoreError::NotFound {
            key: key.to_owned(),
        },
        Some(409 | 412) => StoreError::PreconditionFailed {
            key: key.to_owned(),
            current: None,
        },
        Some(429 | 500..=599) => StoreError::retryable(context(key, e)),
        _ => StoreError::other(context(key, e)),
    }
}

/// Names the key an SDK error happened on.
///
/// The SDK's `Display` carries the status, the service error code and the
/// service message — never request headers — so no bearer token or signed URL
/// can travel out with it.
fn context(key: &str, e: azure_core::Error) -> anyhow::Error {
    anyhow::Error::new(e).context(format!("azure: {key}"))
}

// ---- request/response plumbing -----------------------------------------

/// Converts a half-open walgit range to the SDK's offset+length form.
///
/// Empty and inverted ranges are rejected here: `HttpRange::from(Range<u64>)`
/// computes `end - start` and would panic, and the service rejects a
/// zero-length range anyway.
fn http_range(r: &Range<u64>) -> Result<HttpRange> {
    if r.end <= r.start {
        return Err(StoreError::InvalidArgument(format!(
            "azure: invalid range {}..{} (must be non-empty and ascending)",
            r.start, r.end
        )));
    }
    Ok(HttpRange::new(r.start, r.end - r.start))
}

/// Total object size from `Content-Range: bytes a-b/total`, when present.
///
/// `ObjectMeta::size` is the size of the whole object (as on GCS/memory), also
/// for range reads — and the SDK issues a ranged request even for a whole-object
/// download, so this is the common path, not the exception.
fn total_from_content_range(headers: &Headers) -> Option<u64> {
    headers
        .get_optional_str(&CONTENT_RANGE)
        .and_then(|v| v.rsplit_once('/'))
        .and_then(|(_, total)| total.trim().parse::<u64>().ok())
}

/// Measures a body, handing it back with its length.
///
/// Takes ownership rather than borrowing: `PutBody` is `Send` but not `Sync`, so
/// a reference to one held across the `stat` await would make `put`'s future
/// non-`Send` and fail the `ObjectStore` bound.
async fn measure_body(body: PutBody) -> Result<(PutBody, u64)> {
    match body {
        PutBody::Bytes(b) => {
            let len = b.len() as u64;
            Ok((PutBody::Bytes(b), len))
        }
        PutBody::Stream { len, stream } => Ok((PutBody::Stream { len, stream }, len)),
        PutBody::File(path) => {
            let len = tokio::fs::metadata(&path)
                .await
                .map_err(|e| StoreError::other(anyhow::anyhow!("stat {}: {e}", path.display())))?
                .len();
            Ok((PutBody::File(path), len))
        }
    }
}

/// Materializes a below-threshold body. Stream bodies are walgit's small
/// objects (manifests, leases); large ones arrive as `File` and go to
/// `chunked_put` instead of here.
async fn collect_body(body: PutBody, len: u64) -> Result<Bytes> {
    Ok(match body {
        PutBody::Bytes(b) => b,
        // `len` is only a capacity hint; saturating is right on a 32-bit target.
        PutBody::Stream { stream, .. } => {
            util::collect(stream, usize::try_from(len).unwrap_or(usize::MAX)).await?
        }
        PutBody::File(path) => Bytes::from(
            tokio::fs::read(&path)
                .await
                .map_err(|e| StoreError::other(anyhow::anyhow!("read {}: {e}", path.display())))?,
        ),
    })
}

#[async_trait::async_trait]
impl ObjectStore for AzureStore {
    fn backend(&self) -> &'static str {
        "azure"
    }

    async fn get(&self, key: &str, opts: GetOptions) -> Result<GetResult> {
        let range = opts.range.as_ref().map(http_range).transpose()?;
        let download = BlobClientDownloadOptions {
            if_match: opts.if_match.as_ref().map(to_wire_etag),
            if_none_match: opts.if_none_match.as_ref().map(to_wire_etag),
            range,
            parallel: Some(SEQUENTIAL),
            ..Default::default()
        };

        let resp = match self
            .container
            .blob_client(key)
            .download(Some(download))
            .await
        {
            Ok(resp) => resp,
            // 304 before anything else: a satisfied `If-None-Match` is not an
            // error to us. The ETag rides along in the response headers the SDK
            // attached to the error; the HEAD is a fallback for the day it
            // stops attaching them (one extra round trip, not-modified only).
            Err(e) if is_not_modified(&e) => {
                let version = match version_from_error(&e) {
                    Some(v) => v,
                    None => self.head(key).await?.map(|m| m.version).ok_or_else(|| {
                        StoreError::NotFound {
                            key: key.to_owned(),
                        }
                    })?,
                };
                return Ok(GetResult::NotModified { version });
            }
            Err(e) => return Err(classify(key, e)),
        };

        let meta = ObjectMeta {
            key: key.to_owned(),
            size: total_from_content_range(&resp.headers)
                .or(resp.properties.content_length)
                .unwrap_or(0),
            version: version_from_etag(resp.properties.etag.as_ref()),
        };
        let body = resp
            .body
            .map(|r| r.map_err(|e| StoreError::retryable(anyhow::anyhow!("azure body: {e}"))))
            .boxed();
        Ok(GetResult::Object { meta, body })
    }

    async fn head(&self, key: &str) -> Result<Option<ObjectMeta>> {
        let resp = match self.container.blob_client(key).get_properties(None).await {
            Ok(resp) => resp,
            Err(e) => {
                let err = classify(key, e);
                return if err.is_not_found() {
                    Ok(None)
                } else {
                    Err(err)
                };
            }
        };

        // Both accessors only re-parse headers the service already sent; a
        // failure here is a malformed response, not a missing blob.
        let etag = resp.etag().map_err(|e| classify(key, e))?;
        let size = resp.content_length().map_err(|e| classify(key, e))?;
        Ok(Some(ObjectMeta {
            key: key.to_owned(),
            size: size.unwrap_or(0),
            version: version_from_etag(etag.as_ref()),
        }))
    }

    async fn put(&self, key: &str, body: PutBody, opts: PutOptions) -> Result<ObjectMeta> {
        let (body, len) = measure_body(body).await?;
        if len >= self.multipart_threshold {
            return self.chunked_put(key, body, len, &opts).await;
        }
        let bytes = collect_body(body, len).await?;

        // Partitioning at the body length keeps this a single `Put Blob`: the
        // SDK only stages blocks when the content exceeds the partition size,
        // and anything big enough to want that took `chunked_put` above.
        let partition_size = NonZero::new(len.max(1)).unwrap_or(NonZero::<u64>::MIN);
        let upload = BlockBlobClientUploadOptions {
            if_match: match &opts.mode {
                PutMode::Update(v) => Some(to_wire_etag(v)),
                PutMode::Create | PutMode::Overwrite => None,
            },
            // `*` is the wildcard, not an ETag: it is never quoted.
            if_none_match: matches!(opts.mode, PutMode::Create).then(|| Etag::from("*")),
            blob_content_type: opts.content_type.map(str::to_owned),
            blob_cache_control: opts.immutable.then(|| IMMUTABLE_CACHE_CONTROL.to_owned()),
            parallel: Some(SEQUENTIAL),
            partition_size: Some(partition_size),
            ..Default::default()
        };

        let result = self
            .container
            .blob_client(key)
            .block_blob_client()
            // `RequestContent::from` is an inherent `Vec<u8>` constructor; the
            // zero-copy `From<Bytes>` impl is reached through `into`.
            .upload(bytes.into(), Some(upload))
            .await;

        match result {
            Ok(resp) => Ok(ObjectMeta {
                key: key.to_owned(),
                size: len,
                version: version_from_etag(resp.etag.as_ref()),
            }),
            Err(e) => {
                let mut err = classify(key, e);
                // Fill `current` via HEAD on a CAS failure — the service reports
                // the conflict but not what won.
                if let StoreError::PreconditionFailed { current, .. } = &mut err
                    && current.is_none()
                {
                    *current = self.head(key).await.ok().flatten().map(|m| m.version);
                }
                Err(err)
            }
        }
    }

    async fn delete(&self, key: &str, if_version: Option<Version>) -> Result<()> {
        // Native conditional delete: no HEAD + compare + DELETE emulation (and
        // none of its check-then-act race) is needed on Azure.
        let options = BlobClientDeleteOptions {
            if_match: if_version.as_ref().map(to_wire_etag),
            ..Default::default()
        };

        match self.container.blob_client(key).delete(Some(options)).await {
            Ok(_) => Ok(()),
            Err(e) => {
                let err = classify(key, e);
                match (&err, &if_version) {
                    // Deleting an absent blob unconditionally is a no-op, the
                    // same leniency `s3.rs` applies.
                    (StoreError::NotFound { .. }, None) => Ok(()),
                    // A conditional delete that lost: the trait contract wants
                    // `NotFound` when the blob is gone and `PreconditionFailed`
                    // when it merely changed. Azure reports a missing blob as
                    // 404 `BlobNotFound` (already `NotFound` above), but one
                    // HEAD on this rare path also makes us right if it ever
                    // answers 412 instead — and it names the version that won.
                    (StoreError::PreconditionFailed { .. }, Some(_)) => {
                        match self.head(key).await {
                            Ok(None) => Err(StoreError::NotFound {
                                key: key.to_owned(),
                            }),
                            Ok(Some(meta)) => Err(StoreError::PreconditionFailed {
                                key: key.to_owned(),
                                current: Some(meta.version),
                            }),
                            // The HEAD itself failed: keep what the service said.
                            Err(_) => Err(err),
                        }
                    }
                    _ => Err(err),
                }
            }
        }
    }

    fn list(
        &self,
        _prefix: &str,
        _start_after: Option<&str>,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        Box::pin(futures::stream::once(async { Err(not_implemented()) }))
    }

    async fn list_prefixes(&self, _prefix: &str) -> Result<Vec<String>> {
        Err(not_implemented())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use azure_core::error::ErrorKind;
    use azure_core::http::headers::{HeaderName, Headers};
    use azure_core::http::{RawResponse, StatusCode};

    /// An error shaped exactly like the one `azure_core`'s `check_success`
    /// builds for a bodiless error response: status plus the service's
    /// `x-ms-error-code`. `ErrorKind::HttpResponse` has public fields, so this
    /// is the real thing, not a stand-in — `classify` is tested directly.
    fn http_err(status: u16) -> azure_core::Error {
        ErrorKind::HttpResponse {
            status: StatusCode::from(status),
            error_code: None,
            raw_response: None,
        }
        .into_error()
    }

    /// Same, but carrying a raw response with headers — what a 304 looks like.
    fn http_err_with_headers(status: u16, headers: Headers) -> azure_core::Error {
        ErrorKind::HttpResponse {
            status: StatusCode::from(status),
            error_code: None,
            raw_response: Some(Box::new(RawResponse::from_bytes(
                StatusCode::from(status),
                headers,
                bytes::Bytes::new(),
            ))),
        }
        .into_error()
    }

    fn headers_with(name: &'static str, value: &'static str) -> Headers {
        let mut h = Headers::new();
        h.insert(HeaderName::from_static(name), value);
        h
    }

    #[test]
    fn etag_quotes_stripped() {
        assert_eq!(strip_etag(&Etag::from("\"0x8D1\"".to_string())), "0x8D1");
        assert_eq!(strip_etag(&Etag::from("0x8D1".to_string())), "0x8D1");
    }

    #[test]
    fn wire_etag_is_quoted() {
        assert_eq!(to_wire_etag(&Version::new("0x8D1")).as_ref(), "\"0x8D1\"");
    }

    #[test]
    fn wire_etag_round_trips() {
        let v = Version::new("0x8DDEADBEEF");
        assert_eq!(strip_etag(&to_wire_etag(&v)), v.as_str());
    }

    #[test]
    fn wire_etag_does_not_double_quote() {
        // A Version that already carries quotes (a token minted elsewhere) must
        // not become `""x""` on the wire.
        assert_eq!(
            to_wire_etag(&Version::new("\"0x8D1\"")).as_ref(),
            "\"0x8D1\""
        );
    }

    #[test]
    fn version_from_etag_strips_and_defaults() {
        assert_eq!(
            version_from_etag(Some(&Etag::from("\"0x8D1\"".to_string()))).as_str(),
            "0x8D1"
        );
        assert_eq!(version_from_etag(None).as_str(), "");
    }

    #[test]
    fn error_classification() {
        assert!(classify("k", http_err(404)).is_not_found());
        assert!(classify("k", http_err(409)).is_precondition_failed());
        assert!(classify("k", http_err(412)).is_precondition_failed());
        assert!(classify("k", http_err(429)).is_retryable());
        assert!(classify("k", http_err(503)).is_retryable());
        assert!(classify("k", http_err(500)).is_retryable());
    }

    #[test]
    fn error_classification_other() {
        let e = classify("k", http_err(400));
        assert!(!e.is_not_found() && !e.is_precondition_failed() && !e.is_retryable());
    }

    #[test]
    fn error_classification_without_status_is_other() {
        let e = classify("k", ErrorKind::Io.into_error());
        assert!(!e.is_not_found() && !e.is_precondition_failed() && !e.is_retryable());
    }

    #[test]
    fn error_message_names_the_key() {
        // Errors may name the key; they must never carry header contents.
        let e = classify("refs/heads/main", http_err(400));
        assert!(e.to_string().contains("refs/heads/main"), "got {e}");
    }

    #[test]
    fn not_modified_is_detected() {
        assert!(is_not_modified(&http_err(304)));
        assert!(!is_not_modified(&http_err(404)));
        assert!(!is_not_modified(&ErrorKind::Io.into_error()));
    }

    #[test]
    fn version_from_error_reads_the_etag_header() {
        let e = http_err_with_headers(304, headers_with("etag", "\"0x8D1\""));
        assert_eq!(
            version_from_error(&e).map(|v| v.to_string()),
            Some("0x8D1".to_owned())
        );
    }

    #[test]
    fn version_from_error_is_none_without_a_raw_response() {
        assert!(version_from_error(&http_err(304)).is_none());
    }

    #[test]
    fn http_range_is_half_open() {
        // walgit ranges are half-open; the HTTP header is inclusive.
        assert_eq!(
            http_range(&(0..10)).expect("valid").to_string(),
            "bytes=0-9"
        );
        assert_eq!(
            http_range(&(200..255)).expect("valid").to_string(),
            "bytes=200-254"
        );
    }

    #[test]
    fn http_range_rejects_empty_and_inverted() {
        // `HttpRange::from(Range<u64>)` computes `end - start`: an inverted range
        // would panic, and an empty one is rejected by the SDK anyway.
        assert!(http_range(&(10..10)).is_err());
        // Built by hand: a literal `10..5` is a deny-by-default clippy error.
        assert!(http_range(&Range { start: 10, end: 5 }).is_err());
    }

    #[test]
    fn total_size_from_content_range() {
        let h = headers_with("content-range", "bytes 0-9/255");
        assert_eq!(total_from_content_range(&h), Some(255));
    }

    #[test]
    fn total_size_from_content_range_unknown_total() {
        assert_eq!(
            total_from_content_range(&headers_with("content-range", "bytes 0-9/*")),
            None
        );
        assert_eq!(total_from_content_range(&Headers::new()), None);
    }

    /// walgit keys are hierarchical (`refs/heads/main`, `wal/000123`). The SDK
    /// addresses a blob by percent-encoding the whole name into one path
    /// segment, slashes included — the service decodes it back, and the SDK's
    /// own recorded tests exercise `folder/subfolder/file.txt` this way. This
    /// pins the shape so a future SDK bump that changes it is caught here and
    /// not in production.
    #[test]
    fn hierarchical_keys_become_one_encoded_segment() {
        let container = BlobContainerClient::new(
            "https://acct.blob.core.windows.net/cont"
                .parse()
                .expect("static url"),
            None,
            None,
        )
        .expect("container client");
        assert_eq!(
            container.blob_client("refs/heads/main").url().as_str(),
            "https://acct.blob.core.windows.net/cont/refs%2Fheads%2Fmain"
        );
        assert_eq!(
            container.blob_client("obj").url().as_str(),
            "https://acct.blob.core.windows.net/cont/obj"
        );
    }
}
