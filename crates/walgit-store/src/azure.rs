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
//! Both the single-shot `Put Blob` and the `Put Block List` that ends a chunked
//! upload take the mapping from [`put_conditions`], so every `PutMode` behaves
//! the same at either size. `s3.rs` cannot do that — S3's
//! `CreateMultipartUpload` carries no conditional header, so multipart there is
//! `Overwrite`-only — but on Azure staging publishes nothing and the condition
//! is evaluated atomically at the commit.
//!
//! ## Chunked PUT
//!
//! Bodies at or above `multipart_threshold` are staged as `multipart_part_size`
//! blocks and committed in one call. Block ids are zero-padded decimal so they
//! are all the same length (an Azure requirement) and sort in staging order;
//! the SDK base64-encodes them on the wire, so they are passed as raw ASCII.
//! Blocks staged for a commit that never happened belong to no blob and Azure
//! collects them after seven days, so a failed upload needs no abort call.
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
//! ## Signed URLs
//!
//! [`ObjectStore::signed_get_url`] mints a **user-delegation SAS**: a read-only,
//! time-boxed URL signed with a key the account issues to *this* identity
//! (`Get User Delegation Key`), never with the account's shared key — walgit
//! never holds one, and the config has nowhere to put one. The key is cached in
//! [`AzureStore::delegation_key`] and refreshed by [`needs_new_delegation_key`].
//!
//! The result is a credential. Anyone holding it can read that one blob until
//! `se`, so it is never logged, traced, or put into an error message, and no
//! test writes a whole signed URL down.
//!
//! ## Status
//!
//! The whole `ObjectStore` data plane is implemented, signed URLs included.

use std::num::NonZero;
use std::ops::Range;
use std::sync::Arc;

use azure_core::base64;
use azure_core::credentials::{Secret, TokenCredential};
use azure_core::error::ErrorKind;
use azure_core::hmac::hmac_sha256;
use azure_core::http::headers::{ETAG, HeaderName, Headers};
use azure_core::http::{Etag, RequestContent, Url, UrlExt};
use azure_core::time::{Duration, OffsetDateTime};
use azure_identity::{
    ClientSecretCredential, DeveloperToolsCredential, ManagedIdentityCredential,
    WorkloadIdentityCredential,
};
use azure_storage_blob::models::{
    BlobClientDeleteOptions, BlobClientDownloadOptions, BlobClientGetPropertiesResultHeaders,
    BlobContainerClientListBlobsOptions, BlobItem, BlockBlobClientCommitBlockListOptions,
    BlockBlobClientCommitBlockListResultHeaders, BlockBlobClientUploadOptions, BlockLookupList,
    HttpRange, KeyInfo,
};
use azure_storage_blob::{BlobContainerClient, BlobServiceClient};
use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use parking_lot::Mutex;
use serde::Deserialize;
use walgit_config::AzureCredentialKind;

use crate::{
    BoxStream, ByteStream, GetOptions, GetResult, ObjectMeta, ObjectStore, PutBody, PutMode,
    PutOptions, Result, StoreError, Version, util,
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

/// Blocks one `Put Block List` may name — a hard service limit.
///
/// Checked before staging so an oversized body fails naming its own numbers
/// rather than as a 400 after everything has been uploaded. At the default
/// 32 MiB part size this caps a chunked put at 1.5 TiB.
const MAX_BLOCKS: u64 = 50_000;

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

/// `sp=r` — the permission a walgit signed URL grants: read one blob, nothing
/// else. Never widened; a URL that could write would be a write credential
/// handed to a git client.
const SAS_PERMISSIONS: &str = "r";

/// `sr=b` — the signature covers exactly one blob, not a container and not a
/// "directory" of them.
const SAS_RESOURCE: &str = "b";

/// `spr=https` — the service refuses the SAS over plain HTTP, so a signature
/// cannot be observed in transit even if the URL is used carelessly.
const SAS_PROTOCOL: &str = "https";

/// `sv` — the service version whose string-to-sign layout [`SasFields::string_to_sign`]
/// builds, and which every minted SAS therefore declares.
///
/// **This is deliberately not the delegation key's `skv`.** The two are
/// independent: `skv` states which version *issued the key*, `sv` which
/// version's signing rules the URL was built under, and the live service
/// accepts a SAS whose `sv` is older than its `skv`.
///
/// It has to be pinned. `skv` comes back as whatever version the SDK
/// negotiated — currently `2026-04-06` — and at that version the service
/// builds a **28-field** string-to-sign, four more than the documented
/// 24-field layout below. The four are interleaved, not appended: padding the
/// layout out to 28 (or 29, or 30) trailing-empty fields still fails to match,
/// and this preview version's field order is undocumented, so it cannot be
/// guessed. `2020-12-06` is the version that introduced the 24-field layout
/// (it added `signedEncryptionScope`) and is the oldest one this code signs
/// correctly.
///
/// Verified against the live service (Task 8): at `sv = skv = 2026-04-06`
/// every field count from 24 to 30 returns `AuthenticationFailed`, while
/// `sv = 2020-12-06` with `skv` left as issued returns `200 OK`.
///
/// Raise this only alongside a layout change proven against the service.
const SAS_VERSION: &str = "2020-12-06";

/// Backdate applied to every `st`/`skt`, and the margin the cache keeps between
/// a URL's expiry and its key's.
///
/// Azure judges the window by *its* clock. A server running a few minutes fast
/// would otherwise mint URLs that are not valid yet, which reads to a client as
/// an unexplained 403.
const CLOCK_SKEW: Duration = Duration::minutes(5);

/// How long each user delegation key is requested for. The service's own
/// ceiling is seven days; a day is long enough that signing is almost always
/// zero round trips and short enough to bound a leaked key.
const KEY_LIFETIME: Duration = Duration::hours(24);

/// The longest `ttl` [`ObjectStore::signed_get_url`] will sign.
///
/// A URL cannot outlive the key that signed it, keys are asked for
/// [`KEY_LIFETIME`], and [`needs_new_delegation_key`] keeps a [`CLOCK_SKEW`]
/// margin below that. Anything longer is refused rather than silently
/// shortened — refusing also stops the refresh test from asking for a key no
/// fetch can satisfy, which would re-fetch on *every* call and still hand back
/// a URL the service rejects.
const MAX_SIGNED_TTL: Duration = Duration::hours(23);

/// Azure Blob Storage object store.
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
    /// The user delegation key SAS URLs are signed with, cached until it can no
    /// longer cover a URL being minted. `None` until the first signature.
    ///
    /// This is a *signing key* cache, not an access-token cache, and the
    /// distinction is what makes 24 hours safe: Azure enforces revocation
    /// server-side — revoking the account's user delegation keys invalidates
    /// every SAS already built from them — so holding one here cannot extend
    /// access past a revocation. Exposure per URL is bounded by the caller's
    /// `ttl` either way.
    delegation_key: Mutex<Option<Arc<CachedDelegationKey>>>,
}

/// A user delegation key, reduced to the strings one SAS needs.
///
/// The service returns every field as an `Option`; they are checked and
/// converted once, here, so signing itself cannot fail on a missing field and
/// the same bytes reach the string-to-sign and the query string.
struct CachedDelegationKey {
    /// The key value as base64 — what [`hmac_sha256`] takes (it decodes it
    /// again itself). The SDK already base64-*decoded* the wire value into
    /// bytes, so this is a re-encode, not a passthrough.
    ///
    /// `Secret`'s `Debug` prints `Secret`, so the key cannot reach a log line
    /// through a derived `Debug` somewhere upstream.
    value: Secret,
    /// `skoid` — the Entra object id the key was issued to.
    skoid: String,
    /// `sktid` — its tenant.
    sktid: String,
    /// `skt` / `ske` — the key's own validity window, verbatim in the wire
    /// form the service sent.
    skt: String,
    ske: String,
    /// `sks` — the service the key is good for (`b` for blob).
    sks: String,
    /// `skv` — the service version that minted it. Travels onto the URL as
    /// `skv` and into the string-to-sign, but is *not* the SAS's own `sv`:
    /// that is [`SAS_VERSION`], the version whose layout this code signs.
    skv: String,
    /// `ske` as an instant, for [`needs_new_delegation_key`].
    expiry: OffsetDateTime,
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
            delegation_key: Mutex::new(None),
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
    /// Stages the body as `multipart_part_size` blocks — one request at a time,
    /// no fan-out — then makes them the blob with a single `Put Block List`.
    /// The conditional headers ride on that commit ([`put_conditions`], the
    /// same mapping the single-shot path uses), so the whole upload publishes
    /// atomically: readers see the previous blob, or none, until the commit
    /// lands, and the CAS is evaluated against that instant.
    ///
    /// A failed or abandoned commit needs no cleanup call. Staged blocks belong
    /// to no blob until a commit names them — they are invisible to reads and
    /// listings — and Azure garbage-collects uncommitted blocks after seven
    /// days. So there is no `abort` here, unlike `s3.rs`'s `abort_multipart`.
    async fn chunked_put(
        &self,
        key: &str,
        body: PutBody,
        len: u64,
        opts: &PutOptions,
    ) -> Result<ObjectMeta> {
        let part = self.multipart_part_size;
        let blocks = len.div_ceil(effective_part(len, part));
        if blocks > MAX_BLOCKS {
            return Err(StoreError::InvalidArgument(format!(
                "azure put {key}: {len} bytes at a {part}-byte part size needs {blocks} blocks, \
                 over the service limit of {MAX_BLOCKS}"
            )));
        }

        let mut stream = body_stream(body, len, part);
        let mut carry = Bytes::new();

        let client = self.container.blob_client(key).block_blob_client();
        let mut ids: Vec<Vec<u8>> = Vec::with_capacity(usize::try_from(blocks).unwrap_or(0));
        for (i, want) in chunk_sizes(len, part).enumerate() {
            let want = usize::try_from(want).unwrap_or(usize::MAX);
            let chunk = next_chunk(key, &mut stream, &mut carry, want).await?;
            let id = block_id(i as u64);
            client
                // `&id` is raw ASCII: the SDK base64-encodes block ids itself,
                // for the `blockid` query parameter and for the commit body
                // alike. `RequestContent::from` is the `Vec<u8>` constructor;
                // the zero-copy `From<Bytes>` is reached through `into`.
                .stage_block(&id, chunk.len() as u64, chunk.into(), None)
                .await
                .map_err(|e| classify(key, e))?;
            ids.push(id);
        }

        // Order of the commit list is the order of the blob's bytes.
        let list = BlockLookupList {
            latest: Some(ids),
            ..Default::default()
        };
        let (if_match, if_none_match) = put_conditions(&opts.mode);
        let commit = BlockBlobClientCommitBlockListOptions {
            if_match,
            if_none_match,
            blob_content_type: opts.content_type.map(str::to_owned),
            blob_cache_control: opts.immutable.then(|| IMMUTABLE_CACHE_CONTROL.to_owned()),
            ..Default::default()
        };
        let body = RequestContent::try_from(list).map_err(|e| classify(key, e))?;

        match client.commit_block_list(body, Some(commit)).await {
            // The commit answers with the new blob's ETag in a header, not a
            // body; an unparsable one degrades to an empty version, exactly as
            // a missing ETag does on the single-shot path.
            Ok(resp) => Ok(ObjectMeta {
                key: key.to_owned(),
                size: len,
                version: version_from_etag(resp.etag().ok().flatten().as_ref()),
            }),
            Err(e) => Err(self.put_error(key, e).await),
        }
    }

    /// The `StoreError` for a failed write, with `current` filled in.
    ///
    /// On a CAS failure the service reports the conflict but not what won, so
    /// one follow-up HEAD names the winner — as `s3.rs` does. Shared by the
    /// single-shot upload and the chunked commit.
    async fn put_error(&self, key: &str, e: azure_core::Error) -> StoreError {
        let mut err = classify(key, e);
        if let StoreError::PreconditionFailed { current, .. } = &mut err
            && current.is_none()
        {
            *current = self.head(key).await.ok().flatten().map(|m| m.version);
        }
        err
    }

    /// A delegation key able to cover a URL signed at `now` for `ttl`: the
    /// cached one when it still reaches far enough, a fresh one otherwise.
    ///
    /// `ctx` only ever names the blob being signed, for the error message.
    async fn delegation_key(
        &self,
        ctx: &str,
        now: OffsetDateTime,
        ttl: Duration,
    ) -> Result<Arc<CachedDelegationKey>> {
        // The guard dies with the statement: the fetch below must not hold a
        // sync lock across an await, and the `Arc` is what makes that cheap.
        let cached = self.delegation_key.lock().clone();
        if let Some(key) = cached
            && !needs_new_delegation_key(now, ttl, Some(key.expiry))
        {
            return Ok(key);
        }

        // Two callers racing here both fetch. That is deliberate: the service
        // is happy to mint a key twice, both are valid, and the last writer
        // wins — whereas holding the lock across the round trip would put every
        // signature in the process behind one request.
        let fresh = Arc::new(self.fetch_delegation_key(ctx, now).await?);
        *self.delegation_key.lock() = Some(Arc::clone(&fresh));
        Ok(fresh)
    }

    /// One `Get User Delegation Key` round trip, validated into a
    /// [`CachedDelegationKey`].
    ///
    /// The key is requested backdated by [`CLOCK_SKEW`] and valid for
    /// [`KEY_LIFETIME`]; the SAS window it later signs is always a sub-range of
    /// that, which [`needs_new_delegation_key`] is what enforces.
    async fn fetch_delegation_key(
        &self,
        ctx: &str,
        now: OffsetDateTime,
    ) -> Result<CachedDelegationKey> {
        let info = KeyInfo {
            start: Some(now.saturating_sub(CLOCK_SKEW)),
            expiry: Some(now.saturating_add(KEY_LIFETIME)),
            ..Default::default()
        };
        let body = RequestContent::try_from(info).map_err(|e| classify(ctx, e))?;
        let key = self
            .service
            .get_user_delegation_key(body, None)
            .await
            .map_err(|e| classify(ctx, e))?
            .into_model()
            .map_err(|e| classify(ctx, e))?;

        let expiry = key
            .signed_expiry
            .ok_or_else(|| missing_key_field("SignedExpiry"))?;
        Ok(CachedDelegationKey {
            // The SDK deserializes `<Value>` *through* base64 into bytes;
            // `hmac_sha256` wants the base64 back, and decodes it itself.
            value: Secret::new(base64::encode(
                key.value.ok_or_else(|| missing_key_field("Value"))?,
            )),
            skoid: key
                .signed_oid
                .ok_or_else(|| missing_key_field("SignedOid"))?,
            sktid: key
                .signed_tid
                .ok_or_else(|| missing_key_field("SignedTid"))?,
            skt: sas_time(
                key.signed_start
                    .ok_or_else(|| missing_key_field("SignedStart"))?,
            ),
            ske: sas_time(expiry),
            sks: key
                .signed_service
                .ok_or_else(|| missing_key_field("SignedService"))?,
            skv: key
                .signed_version
                .ok_or_else(|| missing_key_field("SignedVersion"))?,
            expiry,
        })
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

/// The conditional headers for one [`PutMode`], as `(If-Match, If-None-Match)`.
///
/// One mapping, two call sites — the single-shot `Put Blob` and the
/// `Put Block List` that commits a chunked upload — so the modes cannot drift
/// apart between the two paths.
fn put_conditions(mode: &PutMode) -> (Option<Etag>, Option<Etag>) {
    match mode {
        PutMode::Overwrite => (None, None),
        // `*` is the wildcard, not an ETag: it is never quoted.
        PutMode::Create => (None, Some(Etag::from("*"))),
        PutMode::Update(v) => (Some(to_wire_etag(v)), None),
    }
}

// ---- chunked put -------------------------------------------------------

/// The id of the `i`-th staged block.
///
/// Every block id in one blob must be the *same* length — an Azure requirement
/// — and the commit list's order is the blob's byte order, so zero-padded
/// decimal keeps byte order and staging order in step. 16 digits covers
/// [`MAX_BLOCKS`] many times over, so the width is never exceeded.
///
/// Raw ASCII, never base64: the SDK encodes block ids itself, both into the
/// `blockid` query parameter (`generated/clients/block_blob_client.rs:344`,
/// `set_pair("blockid", base64::encode(block_id))`) and into the commit body
/// (`BlockLookupList::latest` serializes through
/// `models_serde::option_vec_encoded_bytes_std`). Encoding here would
/// double-encode.
fn block_id(i: u64) -> Vec<u8> {
    format!("{i:016}").into_bytes()
}

/// The chunk stream for one body, cut at the part size the split expects.
///
/// `file_stream` already cuts a file at the size asked for and a `Bytes` body
/// arrives whole, so both take [`next_chunk`]'s slicing path; only a caller's
/// stream is regrouped.
///
/// The file reader is cut at [`effective_part`], never at the raw configured
/// size: `file_stream` reads `min(chunk, remaining)` bytes per item, so a chunk
/// of zero would end the stream at once while [`chunk_sizes`] still expected
/// one whole-body chunk — and the mismatch would surface as a spurious
/// [`short_body`]. The block-count guard, the split and the reader all
/// normalize through the same function so they cannot disagree.
fn body_stream(body: PutBody, len: u64, part_size: u64) -> ByteStream {
    match body {
        PutBody::Bytes(b) => util::once(b),
        PutBody::Stream { stream, .. } => stream,
        // Saturating only bites on a 32-bit target, where a part that large
        // could not be buffered anyway.
        PutBody::File(path) => util::file_stream(
            path,
            None,
            usize::try_from(effective_part(len, part_size)).unwrap_or(usize::MAX),
        ),
    }
}

/// The part size actually used for a `len`-byte body.
///
/// A configured size of zero would never terminate the split, so the whole body
/// becomes one chunk instead. One definition, so the block-count guard and the
/// staging loop always agree.
fn effective_part(len: u64, part_size: u64) -> u64 {
    if part_size == 0 {
        len.max(1)
    } else {
        part_size
    }
}

/// The successive chunk sizes a `len`-byte body splits into at `part_size`.
///
/// Pure arithmetic, kept apart from the I/O so the boundaries (exact multiple,
/// shorter than a part, one byte over) are testable without a service. Total
/// for inputs `chunked_put` never passes it: an empty body yields nothing, and
/// a zero part size yields one chunk rather than dividing by zero.
fn chunk_sizes(len: u64, part_size: u64) -> impl Iterator<Item = u64> {
    let part = effective_part(len, part_size);
    let mut remaining = len;
    std::iter::from_fn(move || {
        if remaining == 0 {
            return None;
        }
        let n = part.min(remaining);
        remaining -= n;
        Some(n)
    })
}

/// A body that ran out before the length its caller declared.
///
/// Raised before anything is committed: the alternative is publishing a
/// truncated blob and reporting the length that was promised.
fn short_body(key: &str, missing: usize) -> StoreError {
    StoreError::InvalidArgument(format!(
        "azure put {key}: body ended {missing} bytes short of its declared length"
    ))
}

/// Pulls exactly `want` bytes off `stream`, holding any overshoot in `carry`.
///
/// A producer's chunk boundaries have nothing to do with the part size, so they
/// are regrouped here. Whole producer chunks are pulled until one of them alone
/// covers the request, and that one is sliced rather than copied — a `Bytes`
/// body and a file stream (which `file_stream` already cuts at the part size)
/// take that path every time; only a finer-grained producer is stitched.
///
/// A body *longer* than declared is truncated at `len`: the driver asks for
/// exactly the chunks that length splits into and never comes back for more.
async fn next_chunk(
    key: &str,
    stream: &mut ByteStream,
    carry: &mut Bytes,
    want: usize,
) -> Result<Bytes> {
    while carry.is_empty() {
        match stream.next().await {
            Some(next) => *carry = next?,
            None => return Err(short_body(key, want)),
        }
    }
    if carry.len() >= want {
        return Ok(carry.split_to(want));
    }

    let mut buf = BytesMut::with_capacity(want);
    buf.extend_from_slice(&std::mem::take(carry));
    while buf.len() < want {
        let Some(next) = stream.next().await else {
            return Err(short_body(key, want - buf.len()));
        };
        let mut next = next?;
        let take = want - buf.len();
        if next.len() > take {
            buf.extend_from_slice(&next.split_to(take));
            *carry = next;
        } else {
            buf.extend_from_slice(&next);
        }
    }
    Ok(buf.freeze())
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

// ---- user-delegation SAS -----------------------------------------------

/// The signed fields of one user-delegation SAS.
///
/// One value carries both halves of the token — what gets hashed
/// ([`SasFields::string_to_sign`]) and what goes on the URL ([`signed_url`]) —
/// so a parameter can never be signed but not sent, or sent but not signed.
/// Either mistake is an opaque `AuthenticationFailed` from the service.
struct SasFields<'a> {
    /// `sp` — the permissions granted.
    permissions: &'a str,
    /// `st` / `se` — the URL's own window, already in SAS wire form.
    start: String,
    expiry: String,
    /// The canonicalized resource: `/blob/{account}/{container}/{key}`, the key
    /// *undecoded*. See [`canonical_resource`].
    resource: String,
    /// The delegation key: `skoid`/`sktid`/`skt`/`ske`/`sks`/`skv` travel from
    /// here verbatim. `sv` does not come from here — it is [`SAS_VERSION`].
    key: &'a CachedDelegationKey,
    /// `sr` — the kind of resource the signature covers.
    signed_resource: &'a str,
    /// `spr` — the protocols the SAS may be used over.
    protocol: &'a str,
}

impl SasFields<'_> {
    /// The 24 newline-separated fields Azure hashes for a user delegation SAS
    /// at `sv` 2020-12-06, in the order the "Create a user delegation SAS"
    /// REST reference prescribes.
    ///
    /// The 23-field layout that predates `signedEncryptionScope` belongs to
    /// `sv` 2020-02-10 and earlier. The layout is chosen by `sv`, which is
    /// [`SAS_VERSION`] and not the key's `skv` — see that constant for why the
    /// two must not be tied together.
    ///
    /// Every field walgit does not use is present and **empty**. The separators
    /// are positional: dropping an unused field shifts every field after it and
    /// the service computes a different hash. The empty ones, in order:
    /// `saoid`, `suoid`, `scid`, `sip`, the snapshot time, `ses`, then the five
    /// response-header overrides `rscc`/`rscd`/`rsce`/`rscl`/`rsct`.
    fn string_to_sign(&self) -> String {
        let fields: [&str; 24] = [
            self.permissions,     // sp
            &self.start,          // st
            &self.expiry,         // se
            &self.resource,       // canonicalizedResource
            &self.key.skoid,      // skoid
            &self.key.sktid,      // sktid
            &self.key.skt,        // skt
            &self.key.ske,        // ske
            &self.key.sks,        // sks
            &self.key.skv,        // skv
            "",                   // saoid  — signedAuthorizedUserObjectId
            "",                   // suoid  — signedUnauthorizedUserObjectId
            "",                   // scid   — signedCorrelationId
            "",                   // sip    — signedIP
            self.protocol,        // spr
            SAS_VERSION,          // sv     — the layout above, not `skv`
            self.signed_resource, // sr
            "",                   // signedSnapshotTime
            "",                   // ses    — signedEncryptionScope
            "",                   // rscc   — Cache-Control override
            "",                   // rscd   — Content-Disposition override
            "",                   // rsce   — Content-Encoding override
            "",                   // rscl   — Content-Language override
            "",                   // rsct   — Content-Type override
        ];
        fields.join("\n")
    }
}

/// HMAC-SHA256 of the string-to-sign under the delegation key, base64.
///
/// The key stays inside `Secret` up to this call. `hmac_sha256`'s own errors
/// describe the *shape* of the failure ("failed to create hmac from key"), never
/// its material, so the error is safe to surface.
fn sign(fields: &SasFields<'_>) -> Result<String> {
    hmac_sha256(&fields.string_to_sign(), &fields.key.value)
        .map_err(|e| StoreError::other(anyhow::Error::new(e).context("azure: SAS signing")))
}

/// `url` plus every parameter the signature covers, and the signature.
///
/// **The result is a credential.** It ends in `sig=…`; anyone holding it can
/// read that blob until `se`. It must never be logged, traced, or interpolated
/// into an error — and nothing here does: the only failure path is [`sign`],
/// which never sees the URL.
///
/// Values go through the SDK's own query builder, so `:` in a timestamp and the
/// `+`, `/` and `=` a base64 signature can contain all travel percent-encoded
/// and none of them can read as a separator.
fn signed_url(fields: &SasFields<'_>, mut url: Url) -> Result<Url> {
    let sig = sign(fields)?;
    {
        let mut query = url.query_builder();
        query
            .set_pair("sv", SAS_VERSION)
            .set_pair("sr", fields.signed_resource)
            .set_pair("st", fields.start.as_str())
            .set_pair("se", fields.expiry.as_str())
            .set_pair("sp", fields.permissions)
            .set_pair("spr", fields.protocol)
            .set_pair("skoid", fields.key.skoid.as_str())
            .set_pair("sktid", fields.key.sktid.as_str())
            .set_pair("skt", fields.key.skt.as_str())
            .set_pair("ske", fields.key.ske.as_str())
            .set_pair("sks", fields.key.sks.as_str())
            .set_pair("skv", fields.key.skv.as_str())
            .set_pair("sig", sig);
        query.build();
    }
    Ok(url)
}

/// The blob's own URL, with `key` spliced in as a **path**.
///
/// `UrlExt::append_path` goes through `Url::set_path`, which keeps `/` as a
/// separator and percent-encodes everything else that is not path-safe — `?`
/// and `#` included. That matters beyond tidiness: repository and object names
/// reach this from the wire, and a key able to open a query string could
/// otherwise smuggle an unsigned parameter onto a signed URL.
fn blob_url(endpoint: &str, container: &str, key: &str) -> Result<Url> {
    let mut url = parse_url(&format!("{endpoint}/{container}"))
        .map_err(|e| StoreError::InvalidArgument(e.to_string()))?;
    url.append_path(key);
    Ok(url)
}

/// The canonicalized resource Azure signs: `/blob/{account}/{container}/{key}`.
///
/// The key appears **raw**. The service rebuilds this string from the request
/// it received, after decoding the path, so signing the percent-encoded form
/// would never match.
fn canonical_resource(account: &str, container: &str, key: &str) -> String {
    format!("/blob/{account}/{container}/{key}")
}

/// A SAS timestamp: RFC 3339 UTC at whole-second precision, `2026-08-31T13:00:00Z`.
///
/// `from_unix_timestamp` re-anchors at UTC *and* drops sub-second precision in
/// one step — the service rejects fractional seconds, and an instant carrying
/// an offset (RFC 3339 permits one, and the key's own timestamps are parsed as
/// RFC 3339) would otherwise print a window shifted by that offset. It can only
/// fail outside the representable range, which no value that was already an
/// `OffsetDateTime` can reach.
fn sas_time(t: OffsetDateTime) -> String {
    let utc = OffsetDateTime::from_unix_timestamp(t.unix_timestamp()).unwrap_or(t);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        utc.year(),
        u8::from(utc.month()),
        utc.day(),
        utc.hour(),
        utc.minute(),
        utc.second()
    )
}

/// Whether a URL signed at `now` for `ttl` needs a delegation key the cache
/// does not hold.
///
/// Pure, so the policy is testable without a service: refresh when nothing is
/// cached, or when the cached key's `ske` does not clear the URL's own expiry
/// by [`CLOCK_SKEW`]. A URL outliving its key is simply rejected, for its whole
/// life, by a service whose clock is not ours.
fn needs_new_delegation_key(
    now: OffsetDateTime,
    ttl: Duration,
    cached_ske: Option<OffsetDateTime>,
) -> bool {
    let needed_until = now.saturating_add(ttl).saturating_add(CLOCK_SKEW);
    cached_ske.is_none_or(|ske| needed_until > ske)
}

/// The signing window for a caller's `ttl`, or why it cannot be signed.
///
/// See [`MAX_SIGNED_TTL`] for why an over-long one is refused instead of
/// clamped. A zero window is refused too: it is not a URL, it is a 403.
fn signing_ttl(key: &str, ttl: std::time::Duration) -> Result<Duration> {
    Duration::try_from(ttl)
        .ok()
        .filter(|t| *t > Duration::ZERO && *t <= MAX_SIGNED_TTL)
        .ok_or_else(|| {
            StoreError::InvalidArgument(format!(
                "azure signed_get_url {key}: ttl must be above zero and at most {} hours",
                MAX_SIGNED_TTL.whole_hours()
            ))
        })
}

/// A delegation key the service returned without a field the signature needs.
/// Names the field, never the key material.
fn missing_key_field(field: &str) -> StoreError {
    StoreError::other(anyhow::anyhow!(
        "azure: the user delegation key response carries no {field}"
    ))
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

        // Every mode chunks. `s3.rs` restricts multipart to `PutMode::Overwrite`
        // because S3's `CreateMultipartUpload` cannot carry a conditional
        // header, and the condition would have to be evaluated there; Azure has
        // no such restriction, since staging publishes nothing and the
        // condition rides on the atomic `Put Block List` that commits.
        if len >= self.multipart_threshold {
            return self.chunked_put(key, body, len, &opts).await;
        }
        let bytes = collect_body(body, len).await?;

        // Partitioning at the body length keeps this a single `Put Blob`: the
        // SDK only stages blocks when the content exceeds the partition size,
        // and anything big enough to want that took `chunked_put` above.
        let partition_size = NonZero::new(len.max(1)).unwrap_or(NonZero::<u64>::MIN);
        let (if_match, if_none_match) = put_conditions(&opts.mode);
        let upload = BlockBlobClientUploadOptions {
            if_match,
            if_none_match,
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
            Err(e) => Err(self.put_error(key, e).await),
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

    /// A user-delegation SAS URL for `key`: readable for `ttl` by anyone, with
    /// no `Authorization` header.
    ///
    /// **The returned string is a credential.** It ends in `sig=…` and grants
    /// read access to that one blob until it expires, so it must never be
    /// logged, traced, or put into an error message. Hand it to the caller and
    /// nowhere else.
    async fn signed_get_url(&self, key: &str, ttl: std::time::Duration) -> Result<Option<String>> {
        let ttl = signing_ttl(key, ttl)?;
        // One `now` for the window, the cache decision and the key request, so
        // the three cannot disagree about which instant this is.
        let now = OffsetDateTime::now_utc();
        let delegation = self.delegation_key(key, now, ttl).await?;
        let fields = SasFields {
            permissions: SAS_PERMISSIONS,
            start: sas_time(now.saturating_sub(CLOCK_SKEW)),
            expiry: sas_time(now.saturating_add(ttl)),
            resource: canonical_resource(&self.account, &self.bucket, key),
            key: &delegation,
            signed_resource: SAS_RESOURCE,
            protocol: SAS_PROTOCOL,
        };
        let url = signed_url(&fields, blob_url(&self.endpoint, &self.bucket, key)?)?;
        Ok(Some(String::from(url)))
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

    // ---- chunked put ---------------------------------------------------

    /// Azure rejects a block list whose ids are not all the same length. Zero
    /// padding to a fixed width is what makes that true across magnitudes.
    #[test]
    fn block_ids_are_uniform_length() {
        for i in [0u64, 1, 9, 10, 99, 100, 999, 1_000, 49_999, MAX_BLOCKS - 1] {
            assert_eq!(
                block_id(i).len(),
                16,
                "block id for {i} is not 16 bytes: {:?}",
                String::from_utf8_lossy(&block_id(i))
            );
        }
    }

    /// The commit lists ids in the order the blocks were staged, so byte order
    /// and numeric order must agree — the property zero padding buys.
    #[test]
    fn block_ids_sort_like_their_numbers() {
        assert!(block_id(2) < block_id(10), "2 must sort before 10");
        let ids: Vec<Vec<u8>> = (0..2_000u64).map(block_id).collect();
        let mut sorted = ids.clone();
        sorted.sort();
        assert_eq!(ids, sorted, "byte order diverges from numeric order");
    }

    /// Block ids are plain ASCII digits: the SDK base64-encodes them for both
    /// the `blockid` query parameter and the commit body, so nothing here does.
    #[test]
    fn block_ids_are_ascii_digits() {
        assert_eq!(block_id(0), b"0000000000000000");
        assert_eq!(block_id(42), b"0000000000000042");
    }

    #[test]
    fn chunks_split_an_exact_multiple_evenly() {
        assert_eq!(chunk_sizes(300, 100).collect::<Vec<_>>(), [100, 100, 100]);
    }

    #[test]
    fn chunks_below_the_part_size_are_one_chunk() {
        assert_eq!(chunk_sizes(99, 100).collect::<Vec<_>>(), [99]);
        assert_eq!(chunk_sizes(100, 100).collect::<Vec<_>>(), [100]);
    }

    #[test]
    fn chunks_leave_a_remainder_last() {
        assert_eq!(chunk_sizes(101, 100).collect::<Vec<_>>(), [100, 1]);
        assert_eq!(chunk_sizes(250, 100).collect::<Vec<_>>(), [100, 100, 50]);
    }

    /// Total for the degenerate inputs `chunked_put` never passes it: an empty
    /// body yields no chunks, and a zero part size does not divide by zero.
    #[test]
    fn chunks_are_total_for_degenerate_input() {
        assert_eq!(chunk_sizes(0, 100).collect::<Vec<_>>(), Vec::<u64>::new());
        assert_eq!(chunk_sizes(0, 0).collect::<Vec<_>>(), Vec::<u64>::new());
        assert_eq!(chunk_sizes(7, 0).collect::<Vec<_>>(), [7]);
    }

    /// Chunk sizes always sum back to the body length, whatever the part size.
    #[test]
    fn chunks_sum_to_the_body_length() {
        for len in [1u64, 5, 64, 65, 4096, 1_048_577] {
            for part in [1u64, 7, 64, 4096] {
                assert_eq!(
                    chunk_sizes(len, part).sum::<u64>(),
                    len,
                    "len {len} part {part}"
                );
            }
        }
    }

    /// The one mode→condition mapping both the single-shot upload and the
    /// chunked commit use. `*` is the wildcard, never quoted; an `Update`
    /// version goes back on the wire quoted.
    #[test]
    fn put_conditions_map_every_mode() {
        let (m, n) = put_conditions(&PutMode::Overwrite);
        assert!(m.is_none() && n.is_none());

        let (m, n) = put_conditions(&PutMode::Create);
        assert!(m.is_none());
        assert_eq!(n.map(|e| e.to_string()).as_deref(), Some("*"));

        let (m, n) = put_conditions(&PutMode::Update(Version::new("0x8D1")));
        assert_eq!(m.map(|e| e.to_string()).as_deref(), Some("\"0x8D1\""));
        assert!(n.is_none());
    }

    /// A body that runs out early must fail *before* the commit, not commit a
    /// short blob and report the length the caller claimed.
    #[tokio::test]
    async fn a_short_stream_is_rejected() {
        let mut stream = util::once(Bytes::from_static(b"abc"));
        let mut carry = Bytes::new();
        let err = next_chunk("k", &mut stream, &mut carry, 8)
            .await
            .expect_err("short body must error");
        assert!(matches!(err, StoreError::InvalidArgument(_)), "{err:?}");
    }

    /// A producer chunk that already covers the request is sliced, not copied —
    /// the path a `Bytes` body and `util::file_stream` take for every chunk of
    /// a multi-gigabyte upload. Shared-allocation slices keep their addresses.
    #[tokio::test]
    async fn whole_chunks_are_sliced_not_copied() {
        let body = Bytes::from_static(b"0123456789");
        let base = body.as_ptr();
        let mut stream = util::once(body);
        let mut carry = Bytes::new();
        for (i, want) in chunk_sizes(10, 4).enumerate() {
            let chunk = next_chunk("k", &mut stream, &mut carry, usize::try_from(want).unwrap())
                .await
                .expect("chunk");
            assert_eq!(
                chunk.as_ptr(),
                // SAFETY-free pointer arithmetic: comparing addresses only.
                base.wrapping_add(i * 4),
                "chunk {i} was copied out of the original allocation"
            );
        }
    }

    /// Drives a body through exactly what `chunked_put` drives it through —
    /// the block-count guard, `body_stream`, `chunk_sizes` and `next_chunk` —
    /// and asserts the four agree: every requested chunk is delivered in full
    /// and the pieces reassemble the original bytes.
    ///
    /// Returns the chunk sizes so a caller can pin the split as well.
    async fn drive_chunking(body: PutBody, len: u64, part: u64, expect: &[u8]) -> Vec<usize> {
        // The same normalization `chunked_put` applies before staging.
        let blocks = len.div_ceil(effective_part(len, part));
        assert!(blocks <= MAX_BLOCKS, "guard would reject {len}/{part}");

        let mut stream = body_stream(body, len, part);
        let mut carry = Bytes::new();
        let mut sizes = Vec::new();
        let mut seen = BytesMut::new();
        for want in chunk_sizes(len, part) {
            let want = usize::try_from(want).expect("fits");
            let chunk = next_chunk("k", &mut stream, &mut carry, want)
                .await
                .unwrap_or_else(|e| panic!("chunk of {want} at part {part}: {e}"));
            assert_eq!(chunk.len(), want, "short chunk at part {part}");
            seen.extend_from_slice(&chunk);
            sizes.push(want);
        }
        assert_eq!(&seen[..], expect, "body did not reassemble at part {part}");
        assert_eq!(
            sizes.len() as u64,
            blocks,
            "the guard counted {blocks} blocks, the split produced {}",
            sizes.len()
        );
        sizes
    }

    /// A `File` body must reach the staging loop through the *same* part-size
    /// normalization as the guard and the split. It did not: the file reader was
    /// cut at the raw configured size, so `multipart_part_size == 0` — which
    /// nothing validates — ended the stream immediately and failed a put the
    /// design says should succeed as a single block.
    #[tokio::test]
    async fn file_bodies_chunk_consistently_at_every_part_size() {
        let data: Vec<u8> = (0..250u32).map(|i| (i % 251) as u8).collect();
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("body.bin");
        std::fs::write(&path, &data).expect("write");
        let len = data.len() as u64;

        // The regression: a zero part size is one whole-body block, not a
        // spurious short-body error.
        assert_eq!(
            drive_chunking(PutBody::File(path.clone()), len, 0, &data).await,
            [250]
        );

        // And the ordinary sizes still split where the arithmetic says.
        assert_eq!(
            drive_chunking(PutBody::File(path.clone()), len, 100, &data).await,
            [100, 100, 50]
        );
        assert_eq!(
            drive_chunking(PutBody::File(path.clone()), len, 250, &data).await,
            [250]
        );
        assert_eq!(
            drive_chunking(PutBody::File(path), len, 4096, &data).await,
            [250]
        );
    }

    /// The other two body shapes go through the same seam, so they are held to
    /// the same agreement — including at the part size that broke `File`.
    #[tokio::test]
    async fn bytes_and_stream_bodies_chunk_consistently_too() {
        let data: Vec<u8> = (0..250u32).map(|i| (i % 251) as u8).collect();
        let bytes = Bytes::from(data.clone());
        let len = data.len() as u64;

        for part in [0u64, 100, 250, 4096] {
            let sizes = drive_chunking(PutBody::Bytes(bytes.clone()), len, part, &data).await;
            let stream = PutBody::Stream {
                len,
                stream: util::once(bytes.clone()),
            };
            assert_eq!(
                drive_chunking(stream, len, part, &data).await,
                sizes,
                "Bytes and Stream disagree at part {part}"
            );
        }
    }

    /// Regrouping is independent of how the producer chunked the body.
    #[tokio::test]
    async fn chunks_regroup_across_stream_boundaries() {
        let parts = vec![
            Bytes::from_static(b"ab"),
            Bytes::from_static(b"cde"),
            Bytes::from_static(b"fghi"),
        ];
        let mut stream: crate::ByteStream =
            Box::pin(futures::stream::iter(parts.into_iter().map(Ok)));
        let mut carry = Bytes::new();
        let mut out: Vec<Bytes> = Vec::new();
        for want in chunk_sizes(9, 4) {
            out.push(
                next_chunk(
                    "k",
                    &mut stream,
                    &mut carry,
                    usize::try_from(want).expect("fits"),
                )
                .await
                .expect("chunk"),
            );
        }
        assert_eq!(out, [&b"abcd"[..], &b"efgh"[..], &b"i"[..]]);
    }

    // ---- user-delegation SAS -------------------------------------------

    /// A delegation key with recognisable, fixed field values. `value` is the
    /// base64 of `0123456789abcdef` — real key material never appears in a
    /// test, and this one is not a credential for anything.
    fn test_key() -> CachedDelegationKey {
        CachedDelegationKey {
            value: Secret::new("MDEyMzQ1Njc4OWFiY2RlZg=="),
            skoid: "oid-123".into(),
            sktid: "tid-456".into(),
            skt: "2026-08-31T12:00:00Z".into(),
            ske: "2026-09-01T12:00:00Z".into(),
            sks: "b".into(),
            skv: "2025-07-05".into(),
            expiry: at("2026-09-01T12:00:00Z"),
        }
    }

    /// The fixture the layout and signature tests both sign.
    fn test_fields(key: &CachedDelegationKey) -> SasFields<'_> {
        SasFields {
            permissions: SAS_PERMISSIONS,
            start: "2026-08-31T13:00:00Z".into(),
            expiry: "2026-08-31T14:00:00Z".into(),
            resource: "/blob/myacct/walgit/repos/o/r/manifest.pb".into(),
            key,
            signed_resource: SAS_RESOURCE,
            protocol: SAS_PROTOCOL,
        }
    }

    fn at(s: &str) -> OffsetDateTime {
        azure_core::time::parse_rfc3339(s).expect("timestamp")
    }

    /// The executable record of the string-to-sign layout.
    ///
    /// 24 fields for `sv >= 2020-12-06` (the 23-field layout is `sv` 2020-02-10
    /// and earlier, which predates `signedEncryptionScope`). The separators are
    /// positional: a dropped empty field shifts every field after it and the
    /// service computes a different hash, so the count is asserted first.
    #[test]
    fn sas_string_to_sign_layout() {
        let key = test_key();
        let s = test_fields(&key).string_to_sign();
        let lines: Vec<&str> = s.split('\n').collect();

        assert_eq!(lines.len(), 24, "user delegation SAS signs 24 fields");
        assert_eq!(lines[0], "r"); // sp
        assert_eq!(lines[1], "2026-08-31T13:00:00Z"); // st
        assert_eq!(lines[2], "2026-08-31T14:00:00Z"); // se
        assert_eq!(lines[3], "/blob/myacct/walgit/repos/o/r/manifest.pb");
        assert_eq!(lines[4], "oid-123"); // skoid
        assert_eq!(lines[5], "tid-456"); // sktid
        assert_eq!(lines[6], "2026-08-31T12:00:00Z"); // skt
        assert_eq!(lines[7], "2026-09-01T12:00:00Z"); // ske
        assert_eq!(lines[8], "b"); // sks
        assert_eq!(lines[9], "2025-07-05"); // skv
        assert!(lines[10].is_empty(), "saoid"); // signedAuthorizedUserObjectId
        assert!(lines[11].is_empty(), "suoid"); // signedUnauthorizedUserObjectId
        assert!(lines[12].is_empty(), "scid"); // signedCorrelationId
        assert!(lines[13].is_empty(), "sip"); // signedIP
        assert_eq!(lines[14], "https"); // spr
        assert_eq!(lines[15], SAS_VERSION); // sv — the pinned layout version
        assert_eq!(lines[16], "b"); // sr
        assert!(lines[17].is_empty(), "snapshot time");
        assert!(lines[18].is_empty(), "ses"); // signedEncryptionScope
        assert!(lines[19].is_empty(), "rscc");
        assert!(lines[20].is_empty(), "rscd");
        assert!(lines[21].is_empty(), "rsce");
        assert!(lines[22].is_empty(), "rscl");
        assert!(lines[23].is_empty(), "rsct");
    }

    /// `sv` and `skv` are independent, and the test key's values differ so a
    /// regression that re-ties them (the pre-Task-8 behaviour) shows up here
    /// rather than as an opaque `AuthenticationFailed` from the service.
    ///
    /// `sv` selects the string-to-sign layout and must stay at the version
    /// this code implements; `skv` is whatever version issued the key, which
    /// the SDK's negotiated `x-ms-version` puts far ahead of it.
    #[test]
    fn sas_sv_is_pinned_and_independent_of_skv() {
        let key = test_key();
        let fields = test_fields(&key);
        let s = fields.string_to_sign();
        let lines: Vec<&str> = s.split('\n').collect();
        assert_eq!(lines[9], "2025-07-05", "skv travels verbatim from the key");
        assert_eq!(lines[15], SAS_VERSION, "sv is the pinned layout version");
        assert_ne!(
            lines[15], lines[9],
            "sv must not be re-tied to skv: the newer version signs a 28-field layout"
        );
    }

    /// A fixed vector. The expectation was computed independently of this code
    /// — HMAC-SHA256 of the 24-field string above, keyed with the *decoded*
    /// `MDEyMzQ1Njc4OWFiY2RlZg==` (`0123456789abcdef`) — so it pins the whole
    /// chain: the layout, the base64 round trip the SDK's decoded `Value`
    /// forces, and the signature encoding. The live check is the contract test
    /// against Azure; this is the tripwire that catches a silent change first.
    ///
    /// The vector changed in Task 8 when `sv` was pinned to [`SAS_VERSION`]
    /// rather than echoing `skv`: field 15 of the signed string is a different
    /// value now, so every byte after it hashes differently.
    #[test]
    fn sas_signature_is_deterministic() {
        let key = test_key();
        let sig = sign(&test_fields(&key)).expect("sign");
        assert_eq!(sig, "eNoFLfmA+/Jiz/FtbseXOGvCWvF0Fbe5eFAaA74bN/k=");
    }

    /// A signature is HMAC-SHA256: 32 bytes, base64.
    #[test]
    fn sas_signature_is_32_bytes_of_base64() {
        let key = test_key();
        let sig = sign(&test_fields(&key)).expect("sign");
        let raw = azure_core::base64::decode(&sig).expect("base64");
        assert_eq!(raw.len(), 32);
    }

    /// Every parameter the signature covers must reach the URL, and the URL
    /// must carry nothing else: an unsigned extra would be ignored, a missing
    /// one is `AuthenticationFailed`. Asserted on the parsed pairs — the signed
    /// URL itself is a credential and is never written into a test file.
    #[test]
    fn sas_url_carries_exactly_the_signed_parameters() {
        let key = test_key();
        let url = signed_url(
            &test_fields(&key),
            blob_url(
                "https://myacct.blob.core.windows.net",
                "walgit",
                "repos/o/r/manifest.pb",
            )
            .expect("url"),
        )
        .expect("sign");

        let pairs: std::collections::BTreeMap<String, String> = url
            .query_pairs()
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect();
        let mut names: Vec<&str> = pairs.keys().map(String::as_str).collect();
        names.sort_unstable();
        assert_eq!(
            names,
            [
                "se", "sig", "ske", "skoid", "sks", "skt", "sktid", "skv", "sp", "spr", "sr", "st",
                "sv",
            ]
        );

        assert_eq!(pairs["sp"], "r");
        assert_eq!(pairs["sr"], "b");
        assert_eq!(pairs["spr"], "https");
        // `sv` is the pinned layout version, `skv` the key's own — the URL
        // carries both, and they differ.
        assert_eq!(pairs["sv"], SAS_VERSION);
        assert_eq!(pairs["skv"], "2025-07-05");
        assert_eq!(pairs["skoid"], "oid-123");
        assert_eq!(pairs["sktid"], "tid-456");
        assert_eq!(pairs["sks"], "b");
        assert_eq!(pairs["skt"], "2026-08-31T12:00:00Z");
        assert_eq!(pairs["ske"], "2026-09-01T12:00:00Z");
        assert_eq!(pairs["st"], "2026-08-31T13:00:00Z");
        assert_eq!(pairs["se"], "2026-08-31T14:00:00Z");
        // The signature itself: shape only, never its value.
        assert_eq!(
            azure_core::base64::decode(&pairs["sig"])
                .expect("base64")
                .len(),
            32
        );

        assert_eq!(url.scheme(), "https");
        assert_eq!(url.path(), "/walgit/repos/o/r/manifest.pb");
    }

    /// The URL's query is percent-encoded, so the `+`, `/` and `=` a base64
    /// signature can contain never read as separators or as another parameter.
    #[test]
    fn sas_url_percent_encodes_its_values() {
        let key = test_key();
        let url = signed_url(
            &test_fields(&key),
            blob_url("https://myacct.blob.core.windows.net", "walgit", "o/r").expect("url"),
        )
        .expect("sign");
        let query = url.query().expect("query");
        assert!(query.contains("st=2026-08-31T13%3A00%3A00Z"), "timestamps");
        // Whatever this signature's bytes happen to be, none of base64's three
        // non-alphanumeric characters may travel raw: `&`-splitting the query
        // finds the parameter, so a raw `=` or `&` would have split it wrong.
        let sig = query
            .split('&')
            .find_map(|p| p.strip_prefix("sig="))
            .expect("sig");
        assert!(!sig.contains('+') && !sig.contains('/') && !sig.contains('='));
    }

    /// A key is attacker-influenced (repository and object names travel in it).
    /// It is spliced in as a *path*, so a `?` or `#` inside one can never open
    /// a query string or a fragment and smuggle an unsigned parameter onto a
    /// signed URL.
    #[test]
    fn blob_url_encodes_a_key_that_looks_like_a_query() {
        let url = blob_url(
            "https://myacct.blob.core.windows.net",
            "walgit",
            "repos/o/r?sp=racwd#x y",
        )
        .expect("url");
        assert_eq!(url.query(), None);
        assert_eq!(url.fragment(), None);
        assert_eq!(url.path(), "/walgit/repos/o/r%3Fsp=racwd%23x%20y");
    }

    /// The canonicalized resource is the *decoded* path: the service rebuilds
    /// it from the request it received, so signing the encoded form would never
    /// match.
    #[test]
    fn canonical_resource_keeps_the_key_raw() {
        assert_eq!(
            canonical_resource("myacct", "walgit", "repos/o/r/manifest.pb"),
            "/blob/myacct/walgit/repos/o/r/manifest.pb"
        );
    }

    /// SAS timestamps are RFC 3339 UTC at whole-second precision — the service
    /// rejects fractional seconds, and an offset other than `Z` would shift the
    /// window.
    #[test]
    fn sas_time_is_utc_seconds() {
        assert_eq!(sas_time(at("2026-08-31T13:00:00Z")), "2026-08-31T13:00:00Z");
        // Sub-second precision is dropped, not rounded up.
        assert_eq!(
            sas_time(at("2026-08-31T13:00:00.987654321Z")),
            "2026-08-31T13:00:00Z"
        );
        // An instant carrying an offset is re-anchored at UTC, not reprinted.
        assert_eq!(
            sas_time(at("2026-08-31T15:30:00+02:30")),
            "2026-08-31T13:00:00Z"
        );
    }

    // ---- delegation key cache ------------------------------------------

    /// No key at all: fetch.
    #[test]
    fn refresh_when_no_key_is_cached() {
        assert!(needs_new_delegation_key(
            at("2026-08-31T13:00:00Z"),
            Duration::hours(1),
            None
        ));
    }

    /// A key that outlives the URL by more than the skew margin is reused —
    /// the whole point of the cache is that most calls make no round trip.
    #[test]
    fn reuse_a_key_that_covers_the_url() {
        assert!(!needs_new_delegation_key(
            at("2026-08-31T13:00:00Z"),
            Duration::hours(1),
            Some(at("2026-09-01T12:00:00Z"))
        ));
    }

    /// A key that expires before the URL does would mint a URL the service
    /// rejects for its whole life.
    #[test]
    fn refresh_when_the_key_expires_before_the_url() {
        assert!(needs_new_delegation_key(
            at("2026-08-31T13:00:00Z"),
            Duration::hours(4),
            Some(at("2026-08-31T15:00:00Z"))
        ));
    }

    /// The margin: a key expiring exactly at the URL's expiry is refused —
    /// the two clocks are not the same clock.
    #[test]
    fn refresh_inside_the_skew_margin() {
        let now = at("2026-08-31T13:00:00Z");
        let ttl = Duration::hours(1);
        // Expiry exactly at the URL's own — inside the margin.
        assert!(needs_new_delegation_key(
            now,
            ttl,
            Some(at("2026-08-31T14:00:00Z"))
        ));
        // One second short of the full margin — still inside it.
        assert!(needs_new_delegation_key(
            now,
            ttl,
            Some(at("2026-08-31T14:04:59Z"))
        ));
        // Exactly the margin — the first instant that is good enough.
        assert!(!needs_new_delegation_key(
            now,
            ttl,
            Some(at("2026-08-31T14:05:00Z"))
        ));
    }

    // ---- ttl bounds ------------------------------------------------------

    /// A URL cannot outlive the key that signs it, and the key is asked for 24
    /// hours. An over-long ttl is refused, not quietly shortened: shortening
    /// would hand back a URL that dies before the caller expects it to, and
    /// accepting it would re-fetch a key on every call and still sign a URL the
    /// service rejects.
    #[test]
    fn signing_ttl_rejects_what_no_key_can_cover() {
        assert!(signing_ttl("k", std::time::Duration::from_hours(1)).is_ok());
        assert!(matches!(
            signing_ttl("k", std::time::Duration::from_hours(24)),
            Err(StoreError::InvalidArgument(_))
        ));
    }

    /// A zero (or negative-after-skew) window is not a URL, it is a 403.
    #[test]
    fn signing_ttl_rejects_a_zero_window() {
        assert!(matches!(
            signing_ttl("k", std::time::Duration::ZERO),
            Err(StoreError::InvalidArgument(_))
        ));
    }
}
