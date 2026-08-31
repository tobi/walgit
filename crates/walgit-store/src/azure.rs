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
//! ## Listing
//!
//! [`ObjectStore::list`] rides the SDK's own `Pager`, which is already a
//! `Stream` of items across pages — no buffer of our own, unlike `s3.rs`, which
//! has to unfold one. Azure has no server-side `start_after`, so that filter is
//! applied client-side (strictly greater, as the contract defines it).
//!
//! [`ObjectStore::list_prefixes`] cannot use the SDK at all: `azure_storage_blob`
//! 1.1.0-beta.2 exposes no `delimiter` parameter anywhere, and a delimited
//! listing is the only way to walk "directories" without paging every blob
//! beneath them. It therefore issues the `List Blobs` REST call directly through
//! [`AzureStore::http`], with an Entra token from the same credential the SDK
//! uses. That is one request shape, not a second data plane: `x-ms-version`
//! ([`API_VERSION`]) is kept equal to the SDK's own, and the URL is built with
//! the SDK's `UrlExt::query_builder`, so both paths encode identically.
//!
//! ## Status
//!
//! Everything except chunked put is implemented. Bodies at or above
//! `multipart_threshold` route to `chunked_put`, which lands in the task that
//! follows — as do the SAS/accel paths the currently-unread fields below are for.

use std::num::NonZero;
use std::ops::Range;
use std::sync::Arc;

use azure_core::credentials::TokenCredential;
use azure_core::error::ErrorKind;
use azure_core::http::headers::{ETAG, HeaderName, Headers};
use azure_core::http::{Etag, Url, UrlExt};
use azure_identity::{
    ClientSecretCredential, DeveloperToolsCredential, ManagedIdentityCredential,
    WorkloadIdentityCredential,
};
use azure_storage_blob::models::{
    BlobClientDeleteOptions, BlobClientDownloadOptions, BlobClientGetPropertiesResultHeaders,
    BlobContainerClientListBlobsOptions, BlobItem, BlockBlobClientUploadOptions, HttpRange,
};
use azure_storage_blob::{BlobContainerClient, BlobServiceClient};
use bytes::Bytes;
use futures::StreamExt;
use serde::Deserialize;
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

/// Items per listing page, matching `s3.rs`'s `max_keys(1000)`. Azure's own
/// default is 5000; a smaller page bounds the memory one `Pager` step holds.
const LIST_PAGE_SIZE: i32 = 1000;

/// `x-ms-version` for the one call walgit makes without the SDK.
///
/// Copied verbatim from `azure_storage_blob`'s `DEFAULT_VERSION` (which is
/// `pub(crate)`, so it cannot be imported): every SDK request already sends
/// this, and a delimited listing must not silently negotiate a different
/// service contract. Re-check it on an SDK bump.
const API_VERSION: &str = "2026-04-06";

/// The Entra scope for storage data-plane tokens — the same one the SDK's own
/// `BearerTokenAuthorizationPolicy` hardcodes.
const STORAGE_SCOPE: &str = "https://storage.azure.com/.default";

/// The service's own error identifier, e.g. `ContainerNotFound`. Short, and
/// free of anything a caller sent — safe to put in an error message.
const MS_ERROR_CODE: &str = "x-ms-error-code";

/// Azure Blob Storage object store.
///
/// The `allow` covers `service`, `account` and `multipart_part_size`: populated
/// here, first read by the tasks that follow (SAS signing, chunked put).
#[allow(dead_code)]
pub struct AzureStore {
    /// Client scoped to the container named by `store.bucket`.
    container: BlobContainerClient,
    /// Account-scoped client — only `get_user_delegation_key` (SAS signing) needs it.
    service: BlobServiceClient,
    /// The token source for the requests the SDK pipeline does not make:
    /// the delimited listing, and the accel/SAS paths to come.
    credential: Arc<dyn TokenCredential>,
    /// reqwest client for the requests the SDK cannot make: the delimited
    /// listing, and streaming GETs via SAS URLs.
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

    /// An `Authorization` value for the supplemental REST path.
    ///
    /// Every SDK call gets its token from the pipeline's own auth policy; only
    /// the delimited listing, which the SDK cannot make, needs one by hand. The
    /// token lives inside `azure_core`'s `Secret` (whose `Debug` redacts) until
    /// this line, is handed straight to a request header, and never reaches a
    /// log line or an error string: a failed fetch has no token to leak, and
    /// `classify` only ever prints the SDK's status/code/message.
    async fn bearer(&self, ctx: &str) -> Result<String> {
        let token = self
            .credential
            .get_token(&[STORAGE_SCOPE], None)
            .await
            .map_err(|e| classify(ctx, e))?;
        Ok(format!("Bearer {}", token.token.secret()))
    }

    /// One page of a delimited (`delimiter=/`) `List Blobs`.
    ///
    /// Returns the page's `BlobPrefix` names and the marker to continue with,
    /// `None` on the last page.
    async fn hierarchy_page(
        &self,
        prefix: &str,
        marker: Option<&str>,
    ) -> Result<(Vec<String>, Option<String>)> {
        let url = hierarchy_url(&self.endpoint, &self.bucket, prefix, marker)?;

        // `set_sensitive` marks the value redacted in `Headers`' own `Debug`:
        // belt and braces, since nothing here logs a request in the first place.
        let mut auth = reqwest::header::HeaderValue::from_str(&self.bearer(prefix).await?)
            .map_err(|_| {
                StoreError::other(anyhow::anyhow!(
                    "azure list_prefixes {prefix}: token is not a valid header value"
                ))
            })?;
        auth.set_sensitive(true);

        let resp = self
            .http
            .get(url)
            .header(reqwest::header::AUTHORIZATION, auth)
            .header("x-ms-version", API_VERSION)
            .send()
            .await
            // `without_url` so no request URL can ever ride out in an error —
            // this one carries no signature, and none of them ever will.
            .map_err(|e| {
                StoreError::retryable(anyhow::anyhow!(
                    "azure list_prefixes {prefix}: {}",
                    e.without_url()
                ))
            })?;

        let status = resp.status();
        if !status.is_success() {
            let status = status.as_u16();
            let code = resp
                .headers()
                .get(MS_ERROR_CODE)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("unknown")
                .to_owned();
            return Err(from_status(prefix, status, || {
                anyhow::anyhow!("azure list_prefixes {prefix}: HTTP {status} ({code})")
            }));
        }

        let body = resp.bytes().await.map_err(|e| {
            StoreError::retryable(anyhow::anyhow!(
                "azure list_prefixes {prefix}: {}",
                e.without_url()
            ))
        })?;
        parse_hierarchy_page(&body)
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
    // Through `version_from_etag`, so quote handling stays in the one place the
    // module doc promises — a raw header is exactly what `Etag` wraps.
    raw.headers()
        .get_optional_str(&ETAG)
        .map(|s| version_from_etag(Some(&Etag::from(s))))
}

/// Maps an SDK error onto the store's error vocabulary.
///
/// 404 → `NotFound`, 409 (`BlobAlreadyExists`, a lost `If-None-Match: *` race)
/// and 412 (`ConditionNotMet`) → `PreconditionFailed`, 429 and 5xx → `Retryable`,
/// everything else → `Other`. Callers that can observe the current version
/// (`put`) fill `current` themselves.
///
/// An error with no status never reached (or never finished with) the service.
/// `Io` and `Connection` are the transport failures — reset, TLS, timeout, DNS,
/// refused connect — and are `Retryable`, matching what `s3.rs` does with the
/// equivalent `reqwest` failure and what the SDK's own retry policy retries on.
/// This matters beyond taste: [`coord::cas_update`](crate::coord) — the
/// read-modify-write loop behind every manifest and lease — retries only
/// `Retryable` and `PreconditionFailed` and returns anything else at once, and
/// the server turns a retryable store error into a 503 the client can retry
/// rather than a 500. A transport blip filed as `Other` would fail a push
/// outright. Remaining status-less kinds (`Credential`, `DataConversion`,
/// `Other`) are real faults → `Other`.
fn classify(key: &str, e: azure_core::Error) -> StoreError {
    if let Some(status) = http_status(&e) {
        return from_status(key, status, || context(key, e));
    }
    match e.kind() {
        ErrorKind::Io | ErrorKind::Connection => StoreError::retryable(context(key, e)),
        _ => StoreError::other(context(key, e)),
    }
}

/// The status → error mapping [`classify`] documents, in one place.
///
/// Shared with the supplemental REST path in `list_prefixes`, which has a
/// `reqwest` response rather than an SDK error and so cannot go through
/// [`classify`] — but must land on the same verdicts. `detail` is only built
/// for the variants that carry a message.
fn from_status(key: &str, status: u16, detail: impl FnOnce() -> anyhow::Error) -> StoreError {
    match status {
        404 => StoreError::NotFound {
            key: key.to_owned(),
        },
        409 | 412 => StoreError::PreconditionFailed {
            key: key.to_owned(),
            current: None,
        },
        429 | 500..=599 => StoreError::retryable(detail()),
        _ => StoreError::other(detail()),
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

// ---- listing -----------------------------------------------------------

/// The `ObjectMeta` for one listed blob.
///
/// Every field is optional in the generated model but present in a real
/// listing; the fallbacks match what `s3.rs` does with the same absences (empty
/// key, zero size, empty version).
fn object_meta(item: BlobItem) -> ObjectMeta {
    let (size, etag) = item
        .properties
        .map_or((None, None), |p| (p.content_length, p.etag));
    ObjectMeta {
        key: item.name.unwrap_or_default(),
        size: size.unwrap_or(0),
        version: version_from_etag(etag.as_ref()),
    }
}

/// Whether a listed key survives `start_after`.
///
/// `List Blobs` has no server-side equivalent — its `startFrom` is a
/// hierarchical-namespace parameter and *inclusive* — so the cut is made
/// client-side. Strictly greater, which is what the contract defines and what
/// S3's `start-after` does; the listing is lexicographic, so this only ever
/// drops a leading run.
fn past_start_after(key: &str, start_after: Option<&str>) -> bool {
    start_after.is_none_or(|after| key > after)
}

/// The URL of one delimited `List Blobs` page.
///
/// Built through the SDK's own `UrlExt::query_builder`, so the query is encoded
/// exactly as the SDK encodes its flat listing: `/` becomes `%2F` (in both the
/// delimiter and the prefix) and the parameters come out in sorted order. An
/// empty prefix means the whole container and is dropped rather than sent as an
/// empty value.
fn hierarchy_url(
    endpoint: &str,
    container: &str,
    prefix: &str,
    marker: Option<&str>,
) -> Result<Url> {
    // Through `parse_url`, so a bad endpoint reads the same here as it does
    // from the constructor. The URL it names carries no credential: this
    // request authenticates with a header, never a signature in the query.
    let mut url = parse_url(&format!("{endpoint}/{container}"))
        .map_err(|e| StoreError::InvalidArgument(e.to_string()))?;
    {
        let mut query = url.query_builder();
        query
            .set_pair("restype", "container")
            .set_pair("comp", "list")
            .set_pair("delimiter", "/")
            .set_pair("maxresults", LIST_PAGE_SIZE.to_string());
        if !prefix.is_empty() {
            query.set_pair("prefix", prefix);
        }
        if let Some(marker) = marker {
            query.set_pair("marker", marker);
        }
        query.build();
    }
    Ok(url)
}

/// The `BlobPrefix` names and continuation marker of one delimited listing page.
///
/// Deserialized with `azure_core`'s XML support — the SDK's own `quick-xml`,
/// reached through its public re-export, so there is one XML stack in the tree.
/// Unknown elements are ignored, which is what makes this the *delimited* read:
/// `<Blobs>` also carries the `<Blob>` entries directly under the prefix, and a
/// prefix walk must not see them.
fn parse_hierarchy_page(body: &[u8]) -> Result<(Vec<String>, Option<String>)> {
    let page: HierarchyPage = azure_core::xml::from_xml(body).map_err(|e| {
        // The SDK's message embeds the whole document. Keep it as the anyhow
        // *source* so a log line stays one line and `{:#}` still has it all.
        StoreError::other(anyhow::Error::new(e).context("azure: malformed delimited list response"))
    })?;
    let prefixes = page
        .blobs
        .prefixes
        .into_iter()
        .filter_map(|p| p.name.content)
        .collect();
    // Azure sends `<NextMarker />` on the last page as often as it omits it.
    Ok((prefixes, page.next_marker.filter(|m| !m.is_empty())))
}

/// The slice of an `EnumerationResults` document a prefix walk reads.
#[derive(Deserialize)]
struct HierarchyPage {
    #[serde(rename = "Blobs", default)]
    blobs: HierarchyBlobs,
    #[serde(rename = "NextMarker", default)]
    next_marker: Option<String>,
}

#[derive(Default, Deserialize)]
struct HierarchyBlobs {
    #[serde(rename = "BlobPrefix", default)]
    prefixes: Vec<BlobPrefixEntry>,
}

#[derive(Deserialize)]
struct BlobPrefixEntry {
    #[serde(rename = "Name")]
    name: PrefixName,
}

/// `<Name>` is text content, and carries an `Encoded="true"` attribute when the
/// name holds characters XML cannot represent — which is why this is a struct
/// and not a bare `String`: serde must be able to skip that attribute.
///
/// The percent-decoding the attribute would call for is deliberately not done.
/// walgit prefixes are `repos/<owner>/<name>/`, and `walgit-git` validates both
/// parts as ASCII `[A-Za-z0-9._-]`, so the service can never set it. If that
/// ever changed, the registry's `RepoId::from_str` would reject the
/// still-encoded name — a repository missing from a listing, never a corrupted
/// one.
#[derive(Deserialize)]
struct PrefixName {
    #[serde(rename = "$text")]
    content: Option<String>,
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
        prefix: &str,
        start_after: Option<&str>,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        let options = BlobContainerClientListBlobsOptions {
            prefix: (!prefix.is_empty()).then(|| prefix.to_owned()),
            maxresults: Some(LIST_PAGE_SIZE),
            ..Default::default()
        };

        // The SDK's `Pager` is already a `Stream` of items *across* pages: it
        // fetches the next page when the current one runs out and threads the
        // marker itself. `s3.rs` unfolds its own buffer for want of that.
        let pager = match self.container.list_blobs(Some(options)) {
            Ok(pager) => pager,
            // Building the pager only fails before any request goes out. The
            // trait hands back a stream, not a `Result`, so the failure travels
            // as a stream of exactly one `Err`.
            Err(e) => {
                let err = classify(prefix, e);
                return Box::pin(futures::stream::once(async move { Err(err) }));
            }
        };

        let ctx = prefix.to_owned();
        let start_after = start_after.map(str::to_owned);
        Box::pin(
            pager
                .map(move |item| item.map(object_meta).map_err(|e| classify(&ctx, e)))
                .filter(move |item| {
                    // Errors pass through untouched; only keys are filtered.
                    let keep = match item {
                        Ok(meta) => past_start_after(&meta.key, start_after.as_deref()),
                        Err(_) => true,
                    };
                    std::future::ready(keep)
                }),
        )
    }

    async fn list_prefixes(&self, prefix: &str) -> Result<Vec<String>> {
        let mut out = Vec::new();
        let mut marker: Option<String> = None;
        loop {
            let (prefixes, next) = self.hierarchy_page(prefix, marker.as_deref()).await?;
            out.extend(prefixes);
            marker = next;
            if marker.is_none() {
                break;
            }
        }
        // Azure lists prefixes in order and never repeats one within a page,
        // but the sort+dedup keeps the contract independent of that promise —
        // the same belt-and-braces `s3.rs` applies.
        out.sort();
        out.dedup();
        Ok(out)
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
    fn transport_errors_without_status_are_retryable() {
        // A reset/TLS/timeout (`Io`) or a refused connect / DNS failure
        // (`Connection`) must retry, as the same failure does on S3 — the WAL's
        // manifest CAS loop gives up on anything that is not retryable.
        assert!(classify("k", ErrorKind::Io.into_error()).is_retryable());
        assert!(classify("k", ErrorKind::Connection.into_error()).is_retryable());
    }

    #[test]
    fn error_classification_without_status_is_other() {
        // Non-transport, status-less kinds are real faults, not blips.
        for kind in [
            ErrorKind::Credential,
            ErrorKind::DataConversion,
            ErrorKind::Other,
        ] {
            let e = classify("k", kind.into_error());
            assert!(
                !e.is_not_found() && !e.is_precondition_failed() && !e.is_retryable(),
                "expected Other, got {e:?}"
            );
        }
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

    // ---- flat listing (list) --------------------------------------------

    /// `start_after` is exclusive: the contract's `list(base, Some("b"))` must
    /// yield `c, d, e` and never `b` itself.
    #[test]
    fn start_after_is_strictly_greater() {
        assert!(!past_start_after("p/b", Some("p/b")), "must skip itself");
        assert!(!past_start_after("p/a", Some("p/b")));
        assert!(past_start_after("p/c", Some("p/b")));
        // A prefix of the bound sorts before it; a longer key after it.
        assert!(!past_start_after("p/", Some("p/b")));
        assert!(past_start_after("p/b0", Some("p/b")));
        // No bound: everything survives.
        assert!(past_start_after("", None));
    }

    /// Pins the `BlobItem` fields a listing reads (name, size, `ETag`) against
    /// an SDK bump: they are all `Option` in the generated model, so a rename
    /// would silently degrade to empty/zero rather than fail to compile. The
    /// item is deserialized from the wire shape because the model is
    /// `#[non_exhaustive]` and cannot be built by hand.
    #[test]
    fn listed_item_carries_key_size_and_stripped_etag() {
        let xml = r#"<Blob><Name>repos/acme/x/manifest.pb</Name>
  <Properties><Content-Length>17</Content-Length><Etag>"0x8D1"</Etag></Properties>
</Blob>"#;
        let item: BlobItem = azure_core::xml::from_xml(xml.as_bytes()).expect("blob item");
        let meta = object_meta(item);
        assert_eq!(meta.key, "repos/acme/x/manifest.pb");
        assert_eq!(meta.size, 17);
        // Quoted on the wire, unquoted as a `Version` — as everywhere else.
        assert_eq!(meta.version.as_str(), "0x8D1");
    }

    /// A blob the service listed without properties still yields a usable
    /// entry, exactly as `s3.rs` does with the same absences.
    #[test]
    fn listed_item_defaults_when_the_service_omits_fields() {
        let item: BlobItem =
            azure_core::xml::from_xml(b"<Blob><Name>k</Name></Blob>".as_slice()).expect("item");
        let meta = object_meta(item);
        assert_eq!(meta.key, "k");
        assert_eq!(meta.size, 0);
        assert_eq!(meta.version.as_str(), "");
    }

    // ---- delimited listing (list_prefixes) ------------------------------

    #[test]
    fn parses_blob_prefixes_and_next_marker() {
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<EnumerationResults ServiceEndpoint="https://a.blob.core.windows.net/" ContainerName="c">
  <Prefix>repos/</Prefix><Delimiter>/</Delimiter>
  <Blobs>
    <BlobPrefix><Name>repos/alice/</Name></BlobPrefix>
    <BlobPrefix><Name>repos/bob/</Name></BlobPrefix>
  </Blobs>
  <NextMarker>tok123</NextMarker>
</EnumerationResults>"#;
        let (prefixes, marker) = parse_hierarchy_page(xml.as_bytes()).expect("parse");
        assert_eq!(prefixes, vec!["repos/alice/", "repos/bob/"]);
        assert_eq!(marker.as_deref(), Some("tok123"));
    }

    /// The last page: Azure sends `<NextMarker />` (or omits it). Either way
    /// the loop in `list_prefixes` must stop, so both become `None`.
    #[test]
    fn parses_last_page_without_next_marker() {
        let empty = r#"<?xml version="1.0" encoding="utf-8"?>
<EnumerationResults ContainerName="c">
  <Blobs><BlobPrefix><Name>repos/alice/</Name></BlobPrefix></Blobs>
  <NextMarker />
</EnumerationResults>"#;
        let (prefixes, marker) = parse_hierarchy_page(empty.as_bytes()).expect("parse");
        assert_eq!(prefixes, vec!["repos/alice/"]);
        assert_eq!(marker, None, "an empty NextMarker ends the listing");

        let absent = r#"<?xml version="1.0" encoding="utf-8"?>
<EnumerationResults ContainerName="c">
  <Blobs><BlobPrefix><Name>repos/alice/</Name></BlobPrefix></Blobs>
</EnumerationResults>"#;
        let (prefixes, marker) = parse_hierarchy_page(absent.as_bytes()).expect("parse");
        assert_eq!(prefixes, vec!["repos/alice/"]);
        assert_eq!(marker, None);
    }

    /// A leaf prefix (only blobs directly under it, no sub-"directories") and
    /// an empty container both list as no prefixes — what `test_list`'s
    /// `leaf.is_empty()` assertion demands.
    #[test]
    fn parses_page_without_prefixes() {
        let only_blobs = r#"<?xml version="1.0" encoding="utf-8"?>
<EnumerationResults ContainerName="c">
  <Blobs>
    <Blob><Name>repos/a/b/manifest.pb</Name><Properties><Content-Length>3</Content-Length><Etag>"0x8D1"</Etag></Properties></Blob>
  </Blobs>
  <NextMarker />
</EnumerationResults>"#;
        let (prefixes, marker) = parse_hierarchy_page(only_blobs.as_bytes()).expect("parse");
        assert!(prefixes.is_empty(), "a <Blob> is not a <BlobPrefix>");
        assert_eq!(marker, None);

        let empty = r#"<?xml version="1.0" encoding="utf-8"?>
<EnumerationResults ContainerName="c"><Blobs /><NextMarker /></EnumerationResults>"#;
        let (prefixes, marker) = parse_hierarchy_page(empty.as_bytes()).expect("parse");
        assert!(prefixes.is_empty());
        assert_eq!(marker, None);
    }

    #[test]
    fn rejects_malformed_xml() {
        let err = parse_hierarchy_page(b"not xml at all").expect_err("must fail");
        // Never a bare panic, and never retryable: a malformed body is a fault.
        assert!(!err.is_retryable() && !err.is_not_found());
    }

    #[test]
    fn hierarchy_url_encodes_the_prefix() {
        // The `/` separators inside a walgit prefix must travel percent-encoded
        // (`%2F`), as must the delimiter itself.
        let url = hierarchy_url(
            "https://acct.blob.core.windows.net",
            "cont",
            "repos/acme/",
            None,
        )
        .expect("url");
        assert_eq!(
            url.as_str(),
            "https://acct.blob.core.windows.net/cont\
             ?comp=list&delimiter=%2F&maxresults=1000&prefix=repos%2Facme%2F&restype=container"
        );
    }

    #[test]
    fn hierarchy_url_appends_the_marker() {
        let url = hierarchy_url(
            "https://acct.blob.core.windows.net",
            "cont",
            "repos/",
            Some("2!tok/123"),
        )
        .expect("url");
        assert_eq!(
            url.as_str(),
            "https://acct.blob.core.windows.net/cont\
             ?comp=list&delimiter=%2F&marker=2%21tok%2F123&maxresults=1000\
             &prefix=repos%2F&restype=container"
        );
    }

    /// An empty prefix means "the whole container": the parameter is dropped
    /// rather than sent empty.
    #[test]
    fn hierarchy_url_omits_an_empty_prefix() {
        let url =
            hierarchy_url("https://acct.blob.core.windows.net", "cont", "", None).expect("url");
        assert_eq!(
            url.as_str(),
            "https://acct.blob.core.windows.net/cont\
             ?comp=list&delimiter=%2F&maxresults=1000&restype=container"
        );
    }
}
