//! Azure Blob Storage with conditional publication and bounded streaming.
//!
//! The generated SDK lacks delimiter/BlobPrefix support, and its managed GET
//! partitions even a single requested range. Those two operations use the same
//! SDK HTTP pipeline directly; all authentication, retry and transport remain
//! SDK-owned. PUTs publish once, after uniquely named blocks have been staged.
//! Signed URLs are user-delegation SAS: HMAC over a cached key the account
//! issues to this identity, so walgit never holds a shared key.

use std::num::NonZero;
use std::sync::Arc;

use async_trait::async_trait;
use azure_core::credentials::{TokenCredential, TokenRequestOptions};
use azure_core::error::ErrorKind;
use azure_core::http::headers::{AUTHORIZATION, CONTENT_LENGTH, ETAG, HeaderName};
use azure_core::http::policies::auth::{Authorizer, BearerTokenAuthorizationPolicy, OnRequest};
use azure_core::http::{
    ClientMethodOptions, ClientOptions, Context, Etag, Method, Pipeline, Request, StatusCode,
};
use azure_core::time::{Duration, OffsetDateTime};
use azure_storage_blob::models::{
    BlobClientDeleteOptions, BlobClientGetPropertiesResultHeaders,
    BlobContainerClientListBlobsOptions, BlockBlobClientCommitBlockListOptions,
    BlockBlobClientCommitBlockListResultHeaders, BlockBlobClientStageBlockFromUrlOptions,
    BlockBlobClientUploadOptions, BlockLookupList, HttpRange,
};
use azure_storage_blob::{
    BlobClient, BlobContainerClient, BlobContainerClientOptions, BlockBlobClient,
};
use base64::Engine as _;
use bytes::{Bytes, BytesMut};
use futures::stream::{BoxStream, StreamExt, TryStreamExt};
use hmac::{Hmac, Mac};
use serde::Deserialize;
use sha2::Sha256;
use url::Url;
use uuid::Uuid;
use walgit_config::{AzureCredential, StoreConfig};

use crate::{
    ByteStream, GetOptions, GetResult, ObjectMeta, ObjectStore, PutBody, PutMode, PutOptions,
    Result, StoreError, Version, util,
};

const MAX_PAGE: i32 = 5000;
const MAX_BLOCKS: usize = 50_000;
const MAX_BLOCK_BYTES: u64 = 4000 * 1024 * 1024;
// A conservative source-copy bound also works with emulators and older accounts.
const MAX_COPY_BYTES: u64 = 100 * 1024 * 1024;
const AZURE_API_VERSION: &str = "2026-04-06";
const STORAGE_SCOPE: &str = "https://storage.azure.com/.default";
const CONTENT_RANGE: HeaderName = HeaderName::from_static("content-range");
const COPY_SOURCE: HeaderName = HeaderName::from_static("x-ms-copy-source");
// The SAS layout this module signs. Newer service versions add fields to the
// string-to-sign; the signed `sv` pins which layout the service verifies.
const SAS_VERSION: &str = "2020-12-06";
const SAS_CLOCK_SKEW: Duration = Duration::minutes(5);
const DELEGATION_KEY_LIFETIME: Duration = Duration::hours(24);
// The service refuses a user delegation key valid for longer than seven days.
const DELEGATION_KEY_MAX_LIFETIME: Duration = Duration::days(7);

pub struct AzureStore {
    container: Arc<BlobContainerClient>,
    pipeline: Pipeline,
    /// `None` under SAS authentication (a user delegation key needs an Entra
    /// identity, and the configured SAS may grant more than a read) or when
    /// no account name is known for the canonical resource.
    signing: Option<Signing>,
    delegation_key: parking_lot::Mutex<Option<Arc<CachedDelegationKey>>>,
    /// The container URL carries the configured SAS instead of a credential.
    sas_auth: bool,
    multipart_threshold: u64,
    multipart_part_size: usize,
    max_concurrent_blocks: usize,
}

struct Signing {
    account: String,
    container: String,
    /// `{endpoint}` without the container segment: where the delegation key is requested.
    service_url: Url,
}

/// A user delegation key as the service returned it, every field verbatim so
/// the string-to-sign carries exactly what the service will recompute.
#[derive(Deserialize)]
struct DelegationKey {
    #[serde(rename = "SignedOid")]
    oid: String,
    #[serde(rename = "SignedTid")]
    tid: String,
    #[serde(rename = "SignedStart")]
    start: String,
    #[serde(rename = "SignedExpiry")]
    expiry: String,
    #[serde(rename = "SignedService")]
    service: String,
    #[serde(rename = "SignedVersion")]
    version: String,
    /// Base64 key material. Never logged, never in an error.
    #[serde(rename = "Value")]
    value: String,
}

struct CachedDelegationKey {
    key: DelegationKey,
    expires_at: OffsetDateTime,
}

impl AzureStore {
    pub fn new(cfg: &StoreConfig) -> anyhow::Result<Self> {
        let sas = match std::env::var(&cfg.azure.sas_token_env) {
            Ok(token) if !token.is_empty() => Some(token),
            Err(std::env::VarError::NotPresent) => None,
            _ => anyhow::bail!(
                "azure: {} must be unset or contain a non-empty SAS token",
                cfg.azure.sas_token_env
            ),
        };
        let url = container_url(cfg, sas.as_deref())?;
        let credential = if sas.is_some() {
            None
        } else {
            Some(credential(cfg.azure.credential)?)
        };
        Self::with_client_options(cfg, url, credential, ClientOptions::default())
    }

    fn with_client_options(
        cfg: &StoreConfig,
        url: Url,
        credential: Option<Arc<dyn TokenCredential>>,
        mut options: ClientOptions,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            (1..=MAX_BLOCK_BYTES).contains(&cfg.multipart_part_size.as_u64()),
            "azure: multipart_part_size must be between 1 byte and 4000 MiB"
        );
        anyhow::ensure!(
            cfg.multipart_threshold.as_u64() <= MAX_BLOCK_BYTES,
            "azure: multipart_threshold must be at most 4000 MiB"
        );
        anyhow::ensure!(
            cfg.azure.max_concurrent_blocks > 0,
            "azure: max_concurrent_blocks must be positive"
        );
        anyhow::ensure!(
            credential.is_none() || url.scheme() == "https",
            "azure: identity authentication requires HTTPS"
        );
        // Disable transparent decompression: stored bytes and byte ranges are exact.
        if options.transport.is_none() {
            options.transport = Some(azure_core::http::Transport::new(
                azure_core::http::new_http_client(Some(azure_core::http::HttpClientOptions {
                    automatic_decompression: false,
                })),
            ));
        }
        let sas_auth = credential.is_none();
        let signing = if sas_auth {
            None
        } else {
            signing_target(cfg, &url)?
        };
        if let Some(credential) = credential {
            // One authorizer/cache supplies both headers on each retry of a
            // server-side copy. A private source does not inherit destination auth.
            options.per_try_policies.push(Arc::new(
                BearerTokenAuthorizationPolicy::new(credential, [STORAGE_SCOPE])
                    .with_on_request(Arc::new(BlobAuthorization)),
            ));
        }
        let container = BlobContainerClient::new(
            url,
            None,
            Some(BlobContainerClientOptions {
                client_options: options.clone(),
                version: AZURE_API_VERSION.into(),
            }),
        )?;
        let pipeline = Pipeline::new(
            option_env!("CARGO_PKG_NAME"),
            option_env!("CARGO_PKG_VERSION"),
            options,
            Vec::new(),
            Vec::new(),
            None,
        );
        Ok(Self {
            container: Arc::new(container),
            pipeline,
            signing,
            delegation_key: parking_lot::Mutex::new(None),
            sas_auth,
            multipart_threshold: cfg.multipart_threshold.as_u64(),
            multipart_part_size: usize::try_from(cfg.multipart_part_size.as_u64())?,
            max_concurrent_blocks: cfg.azure.max_concurrent_blocks,
        })
    }

    fn blob(&self, key: &str) -> BlobClient {
        self.container.blob_client(key)
    }
}

fn container_url(cfg: &StoreConfig, sas: Option<&str>) -> anyhow::Result<Url> {
    let endpoint = if cfg.azure.endpoint.is_empty() {
        anyhow::ensure!(
            !cfg.azure.account.is_empty()
                && cfg
                    .azure
                    .account
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit()),
            "azure: set a valid storage account or endpoint"
        );
        format!("https://{}.blob.core.windows.net", cfg.azure.account)
    } else {
        cfg.azure.endpoint.clone()
    };
    // Never include the raw endpoint/token in errors; they may contain credentials.
    let mut url =
        Url::parse(&endpoint).map_err(|_| anyhow::anyhow!("azure: invalid endpoint URL"))?;
    anyhow::ensure!(
        matches!(url.scheme(), "http" | "https")
            && url.host_str().is_some()
            && url.username().is_empty()
            && url.password().is_none()
            && url.query().is_none()
            && url.fragment().is_none(),
        "azure: endpoint must be an HTTP(S) URL without credentials, query or fragment"
    );
    anyhow::ensure!(
        !cfg.bucket.is_empty()
            && cfg
                .bucket
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-'),
        "azure: store.bucket must name a container"
    );
    url.path_segments_mut()
        .map_err(|()| anyhow::anyhow!("azure: invalid endpoint path"))?
        .pop_if_empty()
        .push(&cfg.bucket);
    if let Some(sas) = sas {
        url.set_query(Some(sas.trim_start_matches('?')));
    }
    Ok(url)
}

/// The account and service URL a signed URL is computed against. The canonical
/// resource names the account, which a custom `endpoint` does not reveal, so
/// signing is off until `store.azure.account` is set alongside one.
fn signing_target(cfg: &StoreConfig, container_url: &Url) -> anyhow::Result<Option<Signing>> {
    if cfg.azure.account.is_empty() {
        return Ok(None);
    }
    let mut service_url = container_url.clone();
    service_url
        .path_segments_mut()
        .map_err(|()| anyhow::anyhow!("azure: invalid endpoint path"))?
        .pop();
    Ok(Some(Signing {
        account: cfg.azure.account.clone(),
        container: cfg.bucket.clone(),
        service_url,
    }))
}

fn credential(kind: AzureCredential) -> anyhow::Result<Arc<dyn TokenCredential>> {
    match kind {
        AzureCredential::WorkloadIdentity => {
            Ok(azure_identity::WorkloadIdentityCredential::new(None)?)
        }
        AzureCredential::Auto if std::env::var_os("AZURE_FEDERATED_TOKEN_FILE").is_some() => {
            Ok(azure_identity::WorkloadIdentityCredential::new(None)?)
        }
        AzureCredential::ManagedIdentity | AzureCredential::Auto => {
            let id = std::env::var("AZURE_CLIENT_ID")
                .ok()
                .map(azure_identity::UserAssignedId::ClientId);
            Ok(azure_identity::ManagedIdentityCredential::new(Some(
                azure_identity::ManagedIdentityCredentialOptions {
                    user_assigned_id: id,
                    ..Default::default()
                },
            ))?)
        }
        AzureCredential::AzureCli => Ok(azure_identity::AzureCliCredential::new(None)?),
        AzureCredential::ClientSecret => {
            // Named, never echoed: a missing variable is reported by name only.
            let var = |name: &str| {
                std::env::var(name).map_err(|_| anyhow::anyhow!("azure: {name} is not set"))
            };
            Ok(azure_identity::ClientSecretCredential::new(
                &var("AZURE_TENANT_ID")?,
                var("AZURE_CLIENT_ID")?,
                var("AZURE_CLIENT_SECRET")?.into(),
                None,
            )?)
        }
    }
}

#[derive(Debug)]
struct BlobAuthorization;

#[async_trait]
impl OnRequest for BlobAuthorization {
    async fn on_request(
        &self,
        context: &mut Context,
        request: &mut Request,
        authorizer: &dyn Authorizer,
    ) -> azure_core::Result<()> {
        authorizer
            .authorize(
                request,
                &[STORAGE_SCOPE],
                TokenRequestOptions {
                    method_options: ClientMethodOptions {
                        context: context.clone(),
                    },
                },
            )
            .await?;
        if request.headers().get_optional_str(&COPY_SOURCE).is_some() {
            let value = request.headers().get_str(&AUTHORIZATION)?.to_owned();
            request.insert_header("x-ms-copy-source-authorization", value);
        }
        Ok(())
    }
}

fn status_of(error: &azure_core::Error) -> Option<StatusCode> {
    error.http_status()
}

fn map_error(key: &str, error: &azure_core::Error) -> StoreError {
    // Do not propagate SDK error bodies/URLs: SAS signatures and copy-source
    // credentials can occur there. Status and Azure error code suffice to diagnose.
    match error.kind() {
        ErrorKind::HttpResponse {
            status: StatusCode::NotFound,
            ..
        } => StoreError::NotFound { key: key.into() },
        ErrorKind::HttpResponse {
            status: StatusCode::PreconditionFailed,
            ..
        } => StoreError::PreconditionFailed {
            key: key.into(),
            current: None,
        },
        ErrorKind::HttpResponse {
            status: StatusCode::Conflict,
            error_code: Some(code),
            ..
        } if code == "BlobAlreadyExists" => StoreError::PreconditionFailed {
            key: key.into(),
            current: None,
        },
        // Transient container state; other 409 codes (lease, snapshot) are real faults.
        ErrorKind::HttpResponse {
            status: StatusCode::Conflict,
            error_code: Some(code),
            ..
        } if code == "ContainerBeingDeleted" => StoreError::retryable(anyhow::anyhow!(
            "azure: {key}: HTTP 409 (ContainerBeingDeleted)"
        )),
        ErrorKind::HttpResponse {
            status, error_code, ..
        } => {
            let message = anyhow::anyhow!(
                "azure: {key}: HTTP {status} ({})",
                error_code.as_deref().unwrap_or("unknown")
            );
            if status.is_server_error()
                || *status == StatusCode::TooManyRequests
                || *status == StatusCode::RequestTimeout
            {
                StoreError::retryable(message)
            } else {
                StoreError::other(message)
            }
        }
        ErrorKind::Connection | ErrorKind::Io => {
            StoreError::retryable(anyhow::anyhow!("azure: {key}: transport error"))
        }
        _ => StoreError::other(anyhow::anyhow!("azure: {key}: SDK {:?}", error.kind())),
    }
}

fn version_of(etag: Option<Etag>) -> Result<Version> {
    match etag {
        Some(etag) if !etag.to_string().is_empty() => Ok(Version::new(etag.to_string())),
        _ => Err(StoreError::other(anyhow::anyhow!(
            "azure: missing object ETag"
        ))),
    }
}

fn etag(v: &Version) -> Etag {
    Etag::from(v.as_str())
}
fn required_size(size: Option<u64>) -> Result<u64> {
    size.ok_or_else(|| StoreError::other(anyhow::anyhow!("azure: missing object length")))
}

#[async_trait]
impl ObjectStore for AzureStore {
    fn backend(&self) -> &'static str {
        "azure"
    }

    async fn get(&self, key: &str, opts: GetOptions) -> Result<GetResult> {
        let mut request = Request::new(self.blob(key).url().clone(), Method::Get);
        request.insert_header("x-ms-version", AZURE_API_VERSION);
        if let Some(v) = &opts.if_match {
            request.insert_header("if-match", v.as_str().to_owned());
        }
        if let Some(v) = &opts.if_none_match {
            request.insert_header("if-none-match", v.as_str().to_owned());
        }
        if let Some(r) = &opts.range {
            if r.end <= r.start {
                return Err(StoreError::InvalidArgument(
                    "azure: range must be non-empty and increasing".into(),
                ));
            }
            request.insert_header("range", format!("bytes={}-{}", r.start, r.end - 1));
        }
        let result = match self
            .pipeline
            .stream(&Context::new(), &mut request, None)
            .await
        {
            Ok(r) => r,
            Err(e) if status_of(&e) == Some(StatusCode::NotModified) => {
                return opts
                    .if_none_match
                    .map(|version| GetResult::NotModified { version })
                    .ok_or_else(|| StoreError::other(anyhow::anyhow!("azure: unexpected 304")));
            }
            Err(e) => return Err(map_error(key, &e)),
        };
        let headers = result.headers();
        let version = version_of(headers.get_optional_str(&ETAG).map(Etag::from))?;
        let size = if result.status() == StatusCode::PartialContent {
            headers
                .get_optional_str(&CONTENT_RANGE)
                .and_then(|v| v.rsplit_once('/'))
                .and_then(|(_, s)| s.parse().ok())
        } else {
            headers
                .get_optional_str(&CONTENT_LENGTH)
                .and_then(|s| s.parse().ok())
        };
        let size = required_size(size)?;
        let key_owned = key.to_owned();
        Ok(GetResult::Object {
            meta: ObjectMeta {
                key: key.into(),
                size,
                version,
            },
            body: result
                .into_body()
                .map(move |c| c.map_err(|e| map_error(&key_owned, &e)))
                .boxed(),
        })
    }

    async fn head(&self, key: &str) -> Result<Option<ObjectMeta>> {
        match self.blob(key).get_properties(None).await {
            Ok(r) => Ok(Some(ObjectMeta {
                key: key.into(),
                size: required_size(r.content_length().map_err(|e| map_error(key, &e))?)?,
                version: version_of(r.etag().map_err(|e| map_error(key, &e))?)?,
            })),
            Err(e) if status_of(&e) == Some(StatusCode::NotFound) => Ok(None),
            Err(e) => Err(map_error(key, &e)),
        }
    }

    async fn put(&self, key: &str, body: PutBody, opts: PutOptions) -> Result<ObjectMeta> {
        // Every body at or below the threshold is one request: a pack push is a
        // `File`, and staging it would pay a block plus a commit for a few KiB.
        let small = |len: u64| len <= self.multipart_threshold;
        match body {
            PutBody::Bytes(bytes) if small(bytes.len() as u64) => {
                self.put_single(key, bytes, &opts).await
            }
            PutBody::Stream { len, stream } if small(len) => {
                let bytes =
                    util::collect(stream, usize::try_from(len).map_err(StoreError::other)?).await?;
                if bytes.len() as u64 != len {
                    return Err(StoreError::InvalidArgument(
                        "azure: upload stream length differs from declared length".into(),
                    ));
                }
                self.put_single(key, bytes, &opts).await
            }
            PutBody::File(path) => {
                let len = tokio::fs::metadata(&path)
                    .await
                    .map_err(StoreError::other)?
                    .len();
                if small(len) {
                    let bytes = tokio::fs::read(&path).await.map_err(StoreError::other)?;
                    self.put_single(key, Bytes::from(bytes), &opts).await
                } else {
                    self.put_staged(key, PutBody::File(path), opts).await
                }
            }
            other => self.put_staged(key, other, opts).await,
        }
    }

    async fn delete(&self, key: &str, if_version: Option<Version>) -> Result<()> {
        let options = BlobClientDeleteOptions {
            if_match: if_version.as_ref().map(etag),
            ..Default::default()
        };
        match self.blob(key).delete(Some(options)).await {
            Ok(_) => Ok(()),
            Err(e) if status_of(&e) == Some(StatusCode::NotFound) && if_version.is_none() => Ok(()),
            Err(e)
                if status_of(&e) == Some(StatusCode::PreconditionFailed)
                    && if_version.is_some() =>
            {
                // Azure can return 412 for both a missing blob and a stale ETag.
                // Disambiguate only on failure; never race HEAD against DELETE.
                if self.head(key).await?.is_none() {
                    Err(StoreError::NotFound { key: key.into() })
                } else {
                    Err(map_error(key, &e))
                }
            }
            Err(e) => Err(map_error(key, &e)),
        }
    }

    fn list(
        &self,
        prefix: &str,
        start_after: Option<&str>,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        let prefix = prefix.to_owned();
        let start_after = start_after.map(str::to_owned);
        let options = BlobContainerClientListBlobsOptions {
            prefix: Some(prefix.clone()),
            maxresults: Some(MAX_PAGE),
            start_from: start_after.clone(),
            ..Default::default()
        };
        let pager = match self.container.list_blobs(Some(options)) {
            Ok(p) => p,
            Err(e) => {
                return futures::stream::once(async move { Err(map_error(&prefix, &e)) }).boxed();
            }
        };
        pager
            .into_pages()
            .map(move |page| {
                let prefix = prefix.clone();
                let start_after = start_after.clone();
                async move {
                    let page = page.map_err(|e| map_error(&prefix, &e))?;
                    let body: azure_storage_blob::models::ListBlobsResponse =
                        page.into_body().xml().map_err(|e| map_error(&prefix, &e))?;
                    let mut out = Vec::new();
                    for item in body.blob_items {
                        let name = item.name.ok_or_else(|| {
                            StoreError::other(anyhow::anyhow!("azure: listing missing name"))
                        })?;
                        // Also enforce the bound on emulators that ignore startFrom.
                        if start_after.as_ref().is_some_and(|start| &name <= start) {
                            continue;
                        }
                        let props = item.properties.ok_or_else(|| {
                            StoreError::other(anyhow::anyhow!("azure: listing missing properties"))
                        })?;
                        out.push(ObjectMeta {
                            key: name,
                            size: required_size(props.content_length)?,
                            version: version_of(props.etag)?,
                        });
                    }
                    Ok::<_, StoreError>(out)
                }
            })
            .buffered(1)
            .map_ok(|items| futures::stream::iter(items.into_iter().map(Ok)))
            .try_flatten()
            .boxed()
    }

    async fn list_prefixes(&self, prefix: &str) -> Result<Vec<String>> {
        if !prefix.is_empty() && !prefix.ends_with('/') {
            return Err(StoreError::InvalidArgument(
                "azure: prefix must end in /".into(),
            ));
        }
        let mut out = Vec::new();
        let mut marker: Option<String> = None;
        loop {
            let (mut prefixes, next) = self.list_delimited(prefix, marker.as_deref()).await?;
            out.append(&mut prefixes);
            match next.filter(|m| !m.is_empty()) {
                Some(next) if marker.as_ref() != Some(&next) => marker = Some(next),
                Some(_) => {
                    return Err(StoreError::other(anyhow::anyhow!(
                        "azure: listing repeated continuation marker"
                    )));
                }
                None => break,
            }
        }
        out.sort();
        out.dedup();
        Ok(out)
    }

    fn supports_compose(&self) -> bool {
        true
    }
    fn compose_is_native(&self) -> bool {
        false
    }

    async fn compose(
        &self,
        dest: &str,
        sources: &[String],
        opts: PutOptions,
    ) -> Result<ObjectMeta> {
        let block = self.blob(dest).block_blob_client();
        let upload = Uuid::new_v4();
        let mut blocks = Vec::new();
        let mut total = 0u64;
        let part_size = (self.multipart_part_size as u64).min(MAX_COPY_BYTES);
        for source in sources {
            let meta = self
                .head(source)
                .await?
                .ok_or_else(|| StoreError::NotFound { key: source.into() })?;
            let url = self.blob(source).url().to_string(); // preserves SAS and percent-encodes object keys
            let ranges =
                (0..meta.size).step_by(usize::try_from(part_size).map_err(StoreError::other)?);
            let start_index = blocks.len();
            let ids: Vec<_> = (0..meta.size.div_ceil(part_size))
                .map(|i| {
                    block_id(
                        upload,
                        start_index + usize::try_from(i).map_err(StoreError::other)?,
                    )
                })
                .collect::<Result<_>>()?;
            futures::stream::iter(ids.clone().into_iter().zip(ranges))
                .map(|(id, start)| {
                    let range = start..start.saturating_add(part_size).min(meta.size);
                    let options = BlockBlobClientStageBlockFromUrlOptions {
                        source_if_match: Some(etag(&meta.version)),
                        source_range: Some(HttpRange::from(range)),
                        ..Default::default()
                    };
                    let block = &block;
                    let url = url.clone();
                    async move {
                        // The HTTP request has no body: content-length is ZERO, not the copied range length.
                        block
                            .stage_block_from_url(&id, 0, url, Some(options))
                            .await
                            .map_err(|e| map_error(source, &e))?;
                        Ok::<_, StoreError>(())
                    }
                })
                .buffer_unordered(self.max_concurrent_blocks)
                .try_collect::<Vec<_>>()
                .await?;
            total = total.checked_add(meta.size).ok_or_else(|| {
                StoreError::InvalidArgument("azure: compose size overflow".into())
            })?;
            blocks.extend(ids);
        }
        if blocks.is_empty() {
            return self.put(dest, PutBody::Bytes(Bytes::new()), opts).await;
        }
        self.commit(dest, &block, blocks, total, &opts).await
    }

    /// A read-only user-delegation SAS URL: signed with a key the account
    /// issues to this identity, never with a shared key walgit does not hold.
    /// The returned string is a credential for that one blob until `ttl` passes.
    async fn signed_get_url(&self, key: &str, ttl: std::time::Duration) -> Result<Option<String>> {
        let Some(signing) = &self.signing else {
            return Ok(None);
        };
        let ttl = Duration::try_from(ttl).map_err(|_| {
            StoreError::InvalidArgument("azure: signed URL ttl out of range".into())
        })?;
        if ttl <= Duration::ZERO || ttl.saturating_add(SAS_CLOCK_SKEW) > DELEGATION_KEY_MAX_LIFETIME
        {
            return Err(StoreError::InvalidArgument(
                "azure: signed URL ttl must be positive and under seven days".into(),
            ));
        }
        let now = OffsetDateTime::now_utc();
        let expiry = now.saturating_add(ttl);
        let delegation = &self.delegation_key(key, now, expiry).await?.key;
        let sas = Sas {
            permissions: "r",
            start: sas_time(now.saturating_sub(SAS_CLOCK_SKEW)),
            expiry: sas_time(expiry),
            resource: format!("/blob/{}/{}/{key}", signing.account, signing.container),
            protocol: "https",
            resource_type: "b",
        };
        let signature = sas.sign(delegation)?;
        let mut url = self.blob(key).url().clone();
        {
            let mut q = url.query_pairs_mut();
            q.clear()
                .append_pair("sv", SAS_VERSION)
                .append_pair("spr", sas.protocol)
                .append_pair("st", &sas.start)
                .append_pair("se", &sas.expiry)
                .append_pair("sr", sas.resource_type)
                .append_pair("sp", sas.permissions)
                .append_pair("skoid", &delegation.oid)
                .append_pair("sktid", &delegation.tid)
                .append_pair("skt", &delegation.start)
                .append_pair("ske", &delegation.expiry)
                .append_pair("sks", &delegation.service)
                .append_pair("skv", &delegation.version)
                .append_pair("sig", &signature);
        }
        Ok(Some(url.into()))
    }

    /// Under identity auth a one-hour read SAS, as S3 hands the edge a presigned
    /// URL; under SAS auth the blob URL already carries the configured token,
    /// which a trusted edge may hold as GCS's edge holds this process's bearer.
    /// `Range` is not a signed header either way, so the edge may slice.
    async fn accel_target(&self, key: &str) -> Option<crate::AccelTarget> {
        let url = if self.sas_auth {
            self.blob(key).url().to_string()
        } else {
            self.signed_get_url(key, std::time::Duration::from_hours(1))
                .await
                .ok()
                .flatten()?
        };
        Some(crate::AccelTarget {
            url,
            authorization: None,
        })
    }
}

impl AzureStore {
    /// A delegation key covering a URL that expires at `until`: the cached one
    /// when it still reaches, else one request for a fresh key. Racing callers
    /// may both fetch; either key verifies, and the lock is never held across
    /// the round trip.
    async fn delegation_key(
        &self,
        key: &str,
        now: OffsetDateTime,
        until: OffsetDateTime,
    ) -> Result<Arc<CachedDelegationKey>> {
        if let Some(cached) = self.delegation_key.lock().clone()
            && cached.expires_at >= until
        {
            return Ok(cached);
        }
        let Some(signing) = &self.signing else {
            return Err(StoreError::other(anyhow::anyhow!(
                "azure: signing unavailable"
            )));
        };
        let expires_at = now.saturating_add(DELEGATION_KEY_LIFETIME).max(until);
        let mut url = signing.service_url.clone();
        url.query_pairs_mut()
            .append_pair("restype", "service")
            .append_pair("comp", "userdelegationkey");
        let mut request = Request::new(url, Method::Post);
        request.insert_header("x-ms-version", AZURE_API_VERSION);
        request.insert_header("content-type", "application/xml");
        request.set_body(Bytes::from(format!(
            "<?xml version=\"1.0\" encoding=\"utf-8\"?><KeyInfo><Start>{}</Start><Expiry>{}</Expiry></KeyInfo>",
            sas_time(now.saturating_sub(SAS_CLOCK_SKEW)),
            sas_time(expires_at)
        )));
        let response = self
            .pipeline
            .send(&Context::new(), &mut request, None)
            .await
            .map_err(|e| map_error(key, &e))?;
        let key: DelegationKey = quick_xml::de::from_reader(&*response.into_body())
            .map_err(|_| StoreError::other(anyhow::anyhow!("azure: malformed delegation key")))?;
        let expires_at = azure_core::time::parse_rfc3339(&key.expiry)
            .map_err(|_| StoreError::other(anyhow::anyhow!("azure: malformed delegation key")))?;
        let cached = Arc::new(CachedDelegationKey { key, expires_at });
        *self.delegation_key.lock() = Some(cached.clone());
        Ok(cached)
    }

    async fn list_delimited(
        &self,
        prefix: &str,
        marker: Option<&str>,
    ) -> Result<(Vec<String>, Option<String>)> {
        let mut url = self.container.url().clone();
        {
            let mut q = url.query_pairs_mut();
            q.append_pair("restype", "container")
                .append_pair("comp", "list")
                .append_pair("delimiter", "/")
                .append_pair("prefix", prefix)
                .append_pair("maxresults", &MAX_PAGE.to_string());
            if let Some(m) = marker {
                q.append_pair("marker", m);
            }
        }
        let mut request = Request::new(url, Method::Get);
        request.insert_header("x-ms-version", AZURE_API_VERSION);
        let response = self
            .pipeline
            .send(&Context::new(), &mut request, None)
            .await
            .map_err(|e| map_error(prefix, &e))?;
        parse_blob_prefixes(&response.into_body())
            .map_err(|_| StoreError::other(anyhow::anyhow!("azure: malformed delimited listing")))
    }

    async fn put_staged(&self, key: &str, body: PutBody, opts: PutOptions) -> Result<ObjectMeta> {
        let (len, stream) = match body {
            PutBody::Bytes(bytes) => (bytes.len() as u64, util::once(bytes)),
            PutBody::Stream { len, stream } => (len, stream),
            PutBody::File(path) => {
                let file = tokio::fs::File::open(path)
                    .await
                    .map_err(StoreError::other)?;
                let len = file.metadata().await.map_err(StoreError::other)?.len();
                (
                    len,
                    tokio_util::io::ReaderStream::with_capacity(file, self.multipart_part_size)
                        .map(|r| r.map_err(StoreError::other))
                        .boxed(),
                )
            }
        };
        if len.div_ceil(self.multipart_part_size as u64) > MAX_BLOCKS as u64 {
            return Err(StoreError::InvalidArgument(
                "azure: upload exceeds 50000 blocks; increase multipart_part_size".into(),
            ));
        }
        let input = UploadChunks {
            stream,
            remaining: len,
            pending: Bytes::new(),
            part: self.multipart_part_size,
        };
        let chunks = futures::stream::try_unfold(input, |mut state| async move {
            state
                .next()
                .await
                .map(|chunk| chunk.map(|chunk| (chunk, state)))
        });
        let block = self.blob(key).block_blob_client();
        let upload = Uuid::new_v4();
        // At most concurrency parts plus one input chunk are retained. No task
        // detaches: any stream/stage failure drops outstanding requests and never commits.
        let indexed: Vec<_> = chunks
            .enumerate()
            .map(|(index, chunk)| {
                let block = &block;
                async move {
                    let chunk = chunk?;
                    let id = block_id(upload, index)?;
                    block
                        .stage_block(&id, chunk.len() as u64, chunk.into(), None)
                        .await
                        .map_err(|e| map_error(key, &e))?;
                    Ok::<_, StoreError>((index, id))
                }
            })
            .buffer_unordered(self.max_concurrent_blocks)
            .try_collect()
            .await?;
        let mut indexed = indexed;
        indexed.sort_by_key(|(i, _)| *i);
        self.commit(
            key,
            &block,
            indexed.into_iter().map(|(_, id)| id).collect(),
            len,
            &opts,
        )
        .await
    }

    async fn put_single(&self, key: &str, bytes: Bytes, opts: &PutOptions) -> Result<ObjectMeta> {
        let len = bytes.len() as u64;
        let options = BlockBlobClientUploadOptions {
            if_match: match &opts.mode {
                PutMode::Update(v) => Some(etag(v)),
                _ => None,
            },
            if_none_match: matches!(opts.mode, PutMode::Create).then(|| Etag::from("*")),
            blob_content_type: opts.content_type.map(Into::into),
            blob_cache_control: opts
                .immutable
                .then(|| "public, max-age=31536000, immutable".into()),
            // Prevent the SDK from starting a second managed multipart upload.
            partition_size: NonZero::new(len.max(1)),
            ..Default::default()
        };
        let result = self
            .blob(key)
            .block_blob_client()
            .upload(bytes.into(), Some(options))
            .await
            .map_err(|e| map_error(key, &e))?;
        Ok(ObjectMeta {
            key: key.into(),
            size: len,
            version: version_of(result.etag)?,
        })
    }

    async fn commit(
        &self,
        key: &str,
        block: &BlockBlobClient,
        blocks: Vec<Vec<u8>>,
        total: u64,
        opts: &PutOptions,
    ) -> Result<ObjectMeta> {
        let lookup = BlockLookupList {
            latest: Some(blocks),
            ..Default::default()
        };
        let options = BlockBlobClientCommitBlockListOptions {
            if_match: match &opts.mode {
                PutMode::Update(v) => Some(etag(v)),
                _ => None,
            },
            if_none_match: matches!(opts.mode, PutMode::Create).then(|| Etag::from("*")),
            blob_content_type: opts.content_type.map(Into::into),
            blob_cache_control: opts
                .immutable
                .then(|| "public, max-age=31536000, immutable".into()),
            ..Default::default()
        };
        let result = block
            .commit_block_list(
                lookup
                    .try_into()
                    .map_err(|_| StoreError::other(anyhow::anyhow!("azure: invalid block list")))?,
                Some(options),
            )
            .await
            .map_err(|e| map_error(key, &e))?;
        Ok(ObjectMeta {
            key: key.into(),
            size: total,
            version: version_of(result.etag().map_err(|e| map_error(key, &e))?)?,
        })
    }
}

fn block_id(upload: Uuid, index: usize) -> Result<Vec<u8>> {
    if index >= MAX_BLOCKS {
        return Err(StoreError::InvalidArgument(
            "azure: upload exceeds 50000 blocks".into(),
        ));
    }
    // A fresh upload namespace prevents two writers staging over each other;
    // fixed-width IDs meet Azure's equal-length-per-blob requirement.
    Ok(format!("{}-{index:05}", upload.simple()).into_bytes())
}

#[derive(Deserialize)]
#[serde(rename = "EnumerationResults")]
struct DelimitedListing {
    #[serde(rename = "Blobs")]
    blobs: DelimitedBlobs,
    #[serde(rename = "NextMarker")]
    next_marker: Option<String>,
}
#[derive(Deserialize)]
struct DelimitedBlobs {
    #[serde(rename = "BlobPrefix", default)]
    prefixes: Vec<BlobPrefix>,
}
#[derive(Deserialize)]
struct BlobPrefix {
    #[serde(rename = "Name")]
    name: String,
}

/// The signed fields of one blob-scoped user delegation SAS.
struct Sas {
    permissions: &'static str,
    start: String,
    expiry: String,
    resource: String,
    protocol: &'static str,
    resource_type: &'static str,
}

impl Sas {
    /// The `sv = 2020-12-06` string-to-sign: unset optional fields stay as
    /// empty lines, and the key's own fields are the service's verbatim strings.
    fn string_to_sign(&self, key: &DelegationKey) -> String {
        [
            self.permissions,
            &self.start,
            &self.expiry,
            &self.resource,
            &key.oid,
            &key.tid,
            &key.start,
            &key.expiry,
            &key.service,
            &key.version,
            "", // signedAuthorizedUserObjectId
            "", // signedUnauthorizedUserObjectId
            "", // signedCorrelationId
            "", // signedIP
            self.protocol,
            SAS_VERSION,
            self.resource_type,
            "", // signedSnapshotTime
            "", // signedEncryptionScope
            "", // rscc
            "", // rscd
            "", // rsce
            "", // rscl
            "", // rsct
        ]
        .join("\n")
    }

    fn sign(&self, key: &DelegationKey) -> Result<String> {
        let engine = base64::engine::general_purpose::STANDARD;
        let secret = engine
            .decode(&key.value)
            .map_err(|_| StoreError::other(anyhow::anyhow!("azure: malformed delegation key")))?;
        let mut mac = Hmac::<Sha256>::new_from_slice(&secret)
            .map_err(|_| StoreError::other(anyhow::anyhow!("azure: malformed delegation key")))?;
        mac.update(self.string_to_sign(key).as_bytes());
        Ok(engine.encode(mac.finalize().into_bytes()))
    }
}

/// `YYYY-MM-DDThh:mm:ssZ`, the only form a SAS accepts; `t` is UTC.
fn sas_time(t: OffsetDateTime) -> String {
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        t.year(),
        u8::from(t.month()),
        t.day(),
        t.hour(),
        t.minute(),
        t.second()
    )
}

fn parse_blob_prefixes(body: &[u8]) -> anyhow::Result<(Vec<String>, Option<String>)> {
    let listing: DelimitedListing = quick_xml::de::from_reader(body)?;
    Ok((
        listing.blobs.prefixes.into_iter().map(|p| p.name).collect(),
        listing.next_marker,
    ))
}

struct UploadChunks {
    stream: ByteStream,
    remaining: u64,
    pending: Bytes,
    part: usize,
}
impl UploadChunks {
    async fn next(&mut self) -> Result<Option<Bytes>> {
        let want = self
            .part
            .min(usize::try_from(self.remaining).unwrap_or(usize::MAX));
        let mut out = BytesMut::with_capacity(want);
        loop {
            if self.pending.is_empty() {
                self.pending = match self.stream.next().await {
                    Some(chunk) => chunk?,
                    None if self.remaining == 0 => {
                        return Ok((!out.is_empty()).then(|| out.freeze()));
                    }
                    None => {
                        return Err(StoreError::InvalidArgument(
                            "azure: upload stream shorter than declared length".into(),
                        ));
                    }
                };
                if self.pending.len() as u64 > self.remaining {
                    return Err(StoreError::InvalidArgument(
                        "azure: upload stream longer than declared length".into(),
                    ));
                }
                if self.pending.is_empty() {
                    continue;
                }
            }
            let take = self.pending.len().min(want - out.len());
            out.extend_from_slice(&self.pending.split_to(take));
            self.remaining -= take as u64;
            // Check EOF before emitting the last part, so mismatched streams cannot commit.
            if out.len() == want && self.remaining > 0 {
                return Ok(Some(out.freeze()));
            }
        }
    }
}

#[cfg(test)]
mod tests;
