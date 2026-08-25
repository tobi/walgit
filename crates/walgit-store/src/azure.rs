//! Azure Blob Storage backend.
//!
//! Speaks the Blob REST API over `reqwest` with Shared Key (HMAC-SHA256) or
//! AKS Workload Identity. The REST API keeps account-key/Azurite support and
//! permits service and user-delegation SAS signing without a second client.
//!
//! ## Version tokens
//!
//! Blob ETags are opaque `Version` strings. Quotes are stripped on read and
//! re-applied on `If-Match` / `If-None-Match`. Callers never parse them.
//!
//! ## Conditional PUT
//!
//! `PutMode::Create`    → `If-None-Match: *`
//! `PutMode::Update(v)` → `If-Match: "<etag>"`
//! Both 412 and 409 (BlobAlreadyExists) map to `PreconditionFailed`.
//!
//! ## Compose
//!
//! Put Block From URL (server-side copy of each source, authenticated with a
//! short SAS) + Put Block List. Azurite copies bounded ranges through this
//! process because it does not implement Put Block From URL. `compose_is_native`
//! is false: bytes still move inside the account.
//!
//! ## URL shape
//!
//! Production (`{account}.blob.*`): `https://{account}.blob.core.windows.net/{container}/{key}`.
//! Azurite / custom endpoint: `{endpoint}/{account}/{container}/{key}`.

use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use bytes::Bytes;
use chrono::{DateTime, SecondsFormat, Utc};
use futures::StreamExt;
use hmac::{Hmac, Mac};
use sha2::Sha256;

use crate::{
    BoxStream, GetOptions, GetResult, ObjectMeta, ObjectStore, PutBody, PutMode, PutOptions,
    Result, StoreError, Version, util,
};

const API_VERSION: &str = "2021-12-02";
const IMMUTABLE_CACHE_CONTROL: &str = "public, max-age=31536000, immutable";

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone)]
enum AzureAuth {
    SharedKey {
        key: Vec<u8>,
    },
    WorkloadIdentity {
        credential: Arc<azure_identity::WorkloadIdentityCredential>,
        delegation_key: Arc<tokio::sync::Mutex<Option<UserDelegation>>>,
    },
}

/// Azure Blob object store.
#[derive(Clone)]
pub struct AzureStore {
    /// Control-plane client for metadata objects and operations.
    http: reqwest::Client,
    /// Bulk-key and ranged GET/PUT client — its own pool keeps multi-GB
    /// transfers from stalling manifest traffic (D19).
    bulk: reqwest::Client,
    account: String,
    /// Origin + account-path prefix, no trailing slash.
    /// Production: `https://acct.blob.core.windows.net`
    /// Azurite: `http://127.0.0.1:10000/devstoreaccount1`
    service_url: String,
    container: String,
    auth: AzureAuth,
    path_style: bool,
    multipart_threshold: u64,
    multipart_part_size: u64,
}

impl AzureStore {
    /// Build a store from `walgit-config::StoreConfig`.
    ///
    /// The selected credential mode is exclusive. Missing configuration or
    /// credentials fail before the store can serve requests.
    pub async fn new(cfg: &walgit_config::StoreConfig) -> anyhow::Result<Self> {
        let parsed = resolve_azure(cfg)?;
        let http = reqwest::Client::builder()
            .pool_max_idle_per_host(8)
            .timeout(Duration::from_secs(60))
            .build()?;
        let bulk = reqwest::Client::builder()
            .pool_max_idle_per_host(32)
            .timeout(Duration::from_secs(3600))
            .build()?;
        Ok(AzureStore {
            http,
            bulk,
            account: parsed.account,
            service_url: parsed.service_url,
            container: cfg.bucket.clone(),
            path_style: parsed.path_style,
            auth: parsed.auth,
            multipart_threshold: cfg.multipart_threshold.as_u64(),
            multipart_part_size: cfg.multipart_part_size.as_u64().max(1),
        })
    }

    /// Create the container if it does not exist (409 = already there).
    pub async fn ensure_container(&self) -> Result<()> {
        let url = format!("{}?restype=container", self.container_url());
        let resp = self
            .execute(
                self.http
                    .put(&url)
                    .header("content-length", "0")
                    .body(Bytes::new()),
                false,
            )
            .await?;
        match resp.status().as_u16() {
            201 | 202 | 409 => Ok(()),
            s => {
                let body = resp.text().await.unwrap_or_default();
                Err(StoreError::other(anyhow::anyhow!(
                    "azure create container {s}: {body}"
                )))
            }
        }
    }

    fn container_url(&self) -> String {
        format!("{}/{}", self.service_url, self.container)
    }

    fn object_url(&self, key: &str) -> String {
        format!("{}/{}", self.container_url(), util::encode_path(key))
    }

    fn client(&self, bulk: bool) -> &reqwest::Client {
        if bulk { &self.bulk } else { &self.http }
    }

    async fn execute(
        &self,
        builder: reqwest::RequestBuilder,
        bulk: bool,
    ) -> Result<reqwest::Response> {
        let mut req = builder
            .build()
            .map_err(|e| StoreError::other(anyhow::anyhow!("azure build request: {e}")))?;
        req.headers_mut().insert(
            "x-ms-version",
            API_VERSION.parse().expect("api version is ascii"),
        );
        if !req.headers().contains_key("x-ms-date") {
            let date = Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string();
            req.headers_mut().insert(
                "x-ms-date",
                date.parse()
                    .map_err(|e| StoreError::other(anyhow::anyhow!("azure date header: {e}")))?,
            );
        }
        match &self.auth {
            AzureAuth::SharedKey { key } => {
                let auth =
                    shared_key_auth(&self.account, key, req.method(), req.url(), req.headers())?;
                req.headers_mut().insert(
                    reqwest::header::AUTHORIZATION,
                    auth.parse().map_err(|e| {
                        StoreError::other(anyhow::anyhow!("azure auth header: {e}"))
                    })?,
                );
            }
            AzureAuth::WorkloadIdentity { credential, .. } => {
                let token = Self::bearer_token(credential).await?;
                req.headers_mut().insert(
                    reqwest::header::AUTHORIZATION,
                    format!("Bearer {token}").parse().map_err(|e| {
                        StoreError::other(anyhow::anyhow!("azure bearer header: {e}"))
                    })?,
                );
            }
        }
        self.client(bulk)
            .execute(req)
            .await
            .map_err(|e| StoreError::retryable(anyhow::anyhow!("azure http: {e}")))
    }

    async fn bearer_token(
        credential: &azure_identity::WorkloadIdentityCredential,
    ) -> Result<String> {
        use azure_core::credentials::TokenCredential;
        let token = credential
            .get_token(&["https://storage.azure.com/.default"], None)
            .await
            .map_err(|e| {
                StoreError::other(anyhow::anyhow!("azure workload identity token: {e}"))
            })?;
        Ok(token.token.secret().to_owned())
    }

    fn range_header(range: &std::ops::Range<u64>) -> String {
        format!("bytes={}-{}", range.start, range.end.saturating_sub(1))
    }

    fn quote_etag(v: &str) -> String {
        if v == "*" || (v.starts_with('"') && v.ends_with('"')) {
            v.to_owned()
        } else {
            format!("\"{v}\"")
        }
    }

    fn apply_put_conditions(b: reqwest::RequestBuilder, mode: &PutMode) -> reqwest::RequestBuilder {
        match mode {
            PutMode::Overwrite => b,
            PutMode::Create => b.header("if-none-match", "*"),
            PutMode::Update(v) => b.header("if-match", Self::quote_etag(v.as_str())),
        }
    }

    fn get_result_from_response(key: &str, resp: reqwest::Response) -> Result<GetResult> {
        let status = resp.status().as_u16();
        let etag = header_str(resp.headers(), "etag").map(|s| strip_etag(&s));
        let content_length =
            header_str(resp.headers(), "content-length").and_then(|s| s.parse::<u64>().ok());
        let total = header_str(resp.headers(), "content-range").and_then(|v| {
            v.rsplit_once('/')
                .and_then(|(_, t)| t.trim().parse::<u64>().ok())
        });
        match status {
            200 | 206 => {
                let meta = ObjectMeta {
                    key: key.into(),
                    size: total.or(content_length).unwrap_or(0),
                    version: Version::new(etag.as_deref().unwrap_or("")),
                };
                let body = resp
                    .bytes_stream()
                    .map(|r| {
                        r.map_err(|e| StoreError::retryable(anyhow::anyhow!("azure body: {e}")))
                    })
                    .boxed();
                Ok(GetResult::Object { meta, body })
            }
            304 => Ok(GetResult::NotModified {
                version: Version::new(etag.as_deref().unwrap_or("")),
            }),
            404 => Err(StoreError::NotFound { key: key.into() }),
            412 | 409 => Err(StoreError::PreconditionFailed {
                key: key.into(),
                current: etag.map(Version::new),
            }),
            s if s >= 500 || s == 429 => Err(StoreError::Retryable(anyhow::anyhow!(
                "azure get status {s}"
            ))),
            s => Err(StoreError::Other(anyhow::anyhow!("azure get status {s}"))),
        }
    }

    fn classify_write(key: &str, status: u16, etag: Option<String>, body: &str) -> StoreError {
        match status {
            404 => StoreError::NotFound { key: key.into() },
            412 | 409 => StoreError::PreconditionFailed {
                key: key.into(),
                current: etag.map(Version::new),
            },
            s if s >= 500 || s == 429 => {
                StoreError::Retryable(anyhow::anyhow!("azure status {s}: {body}"))
            }
            s => StoreError::Other(anyhow::anyhow!("azure status {s}: {body}")),
        }
    }

    async fn put_blob(&self, key: &str, body: Bytes, opts: &PutOptions) -> Result<ObjectMeta> {
        let len = body.len() as u64;
        let url = self.object_url(key);
        let bulk = util::is_bulk_key(key);
        let mut builder = self
            .client(bulk)
            .put(&url)
            .header("x-ms-blob-type", "BlockBlob")
            .header("content-length", len.to_string())
            .body(body);
        builder = Self::apply_put_conditions(builder, &opts.mode);
        if let Some(content_type) = opts.content_type {
            builder = builder.header("x-ms-blob-content-type", content_type);
        }
        if opts.immutable {
            builder = builder.header("x-ms-blob-cache-control", IMMUTABLE_CACHE_CONTROL);
        }
        let resp = self.execute(builder, bulk).await?;
        let status = resp.status().as_u16();
        let etag = header_str(resp.headers(), "etag").map(|s| strip_etag(&s));
        if (200..300).contains(&status) {
            return Ok(ObjectMeta {
                key: key.into(),
                size: len,
                version: Version::new(etag.as_deref().unwrap_or("")),
            });
        }
        let body = resp.text().await.unwrap_or_default();
        let mut err = Self::classify_write(key, status, etag, &body);
        if let StoreError::PreconditionFailed { current: c, .. } = &mut err
            && c.is_none()
        {
            *c = self.head(key).await.ok().flatten().map(|m| m.version);
        }
        Err(err)
    }

    fn block_id(upload: &str, n: u32) -> String {
        base64::engine::general_purpose::STANDARD.encode(format!("{upload}:{n:016}"))
    }

    fn upload_id() -> String {
        uuid::Uuid::new_v4().simple().to_string()
    }

    async fn put_block(&self, key: &str, upload: &str, n: u32, data: Bytes) -> Result<()> {
        let id = Self::block_id(upload, n);
        let url = format!(
            "{}?comp=block&blockid={}",
            self.object_url(key),
            query_encode(&id)
        );
        let len = data.len();
        let builder = self
            .bulk
            .put(&url)
            .header("content-length", len.to_string())
            .header("x-ms-blob-type", "BlockBlob")
            .body(data);
        let resp = self.execute(builder, true).await?;
        let status = resp.status().as_u16();
        if (200..300).contains(&status) {
            return Ok(());
        }
        let body = resp.text().await.unwrap_or_default();
        Err(Self::classify_write(key, status, None, &body))
    }

    async fn put_block_from_url(
        &self,
        dest: &str,
        upload: &str,
        n: u32,
        source_url: &str,
        range: std::ops::Range<u64>,
    ) -> Result<()> {
        let id = Self::block_id(upload, n);
        let url = format!(
            "{}?comp=block&blockid={}",
            self.object_url(dest),
            query_encode(&id)
        );
        let src_range = format!("bytes={}-{}", range.start, range.end.saturating_sub(1));
        let b = self
            .bulk
            .put(&url)
            .header("content-length", "0")
            .header("x-ms-copy-source", source_url)
            .header("x-ms-source-range", src_range)
            .body(Bytes::new());
        let resp = self.execute(b, true).await?;
        let status = resp.status().as_u16();
        if (200..300).contains(&status) {
            return Ok(());
        }
        let body = resp.text().await.unwrap_or_default();
        Err(Self::classify_write(dest, status, None, &body))
    }

    async fn commit_block_list(
        &self,
        key: &str,
        upload: &str,
        n_blocks: u32,
        opts: &PutOptions,
        size: u64,
    ) -> Result<ObjectMeta> {
        let mut xml = String::from("<?xml version=\"1.0\" encoding=\"utf-8\"?><BlockList>");
        for i in 0..n_blocks {
            xml.push_str("<Latest>");
            xml.push_str(&Self::block_id(upload, i));
            xml.push_str("</Latest>");
        }
        xml.push_str("</BlockList>");
        let url = format!("{}?comp=blocklist", self.object_url(key));
        let bulk = util::is_bulk_key(key);
        let mut builder = self
            .client(bulk)
            .put(&url)
            .header("content-type", "application/xml")
            .header("content-length", xml.len().to_string())
            .body(xml);
        builder = Self::apply_put_conditions(builder, &opts.mode);
        if let Some(content_type) = opts.content_type {
            builder = builder.header("x-ms-blob-content-type", content_type);
        }
        if opts.immutable {
            builder = builder.header("x-ms-blob-cache-control", IMMUTABLE_CACHE_CONTROL);
        }
        let resp = self.execute(builder, bulk).await?;
        let status = resp.status().as_u16();
        let etag = header_str(resp.headers(), "etag").map(|s| strip_etag(&s));
        if (200..300).contains(&status) {
            return Ok(ObjectMeta {
                key: key.into(),
                size,
                version: Version::new(etag.as_deref().unwrap_or("")),
            });
        }
        let body = resp.text().await.unwrap_or_default();
        let mut err = Self::classify_write(key, status, etag, &body);
        if let StoreError::PreconditionFailed { current: c, .. } = &mut err
            && c.is_none()
        {
            *c = self.head(key).await.ok().flatten().map(|m| m.version);
        }
        Err(err)
    }

    async fn put_blocks_bytes(
        &self,
        key: &str,
        data: Bytes,
        opts: &PutOptions,
    ) -> Result<ObjectMeta> {
        let size = data.len() as u64;
        let part = self.multipart_part_size as usize;
        let upload = Self::upload_id();
        let mut n = 0u32;
        let mut offset = 0usize;
        while offset < data.len() {
            let end = (offset + part).min(data.len());
            self.put_block(key, &upload, n, data.slice(offset..end))
                .await?;
            n += 1;
            offset = end;
        }
        if n == 0 {
            self.put_block(key, &upload, 0, Bytes::new()).await?;
            n = 1;
        }
        self.commit_block_list(key, &upload, n, opts, size).await
    }

    async fn put_stream(
        &self,
        key: &str,
        mut stream: crate::ByteStream,
        len: u64,
        opts: &PutOptions,
    ) -> Result<ObjectMeta> {
        let part = self.multipart_part_size as usize;
        let upload = Self::upload_id();
        let mut buf = bytes::BytesMut::new();
        let mut n = 0u32;
        while let Some(chunk) = stream.next().await {
            buf.extend_from_slice(&chunk?);
            while buf.len() >= part {
                let block = buf.split_to(part).freeze();
                self.put_block(key, &upload, n, block).await?;
                n += 1;
            }
        }
        if !buf.is_empty() || n == 0 {
            self.put_block(key, &upload, n, buf.freeze()).await?;
            n += 1;
        }
        self.commit_block_list(key, &upload, n, opts, len).await
    }

    async fn list_page(
        &self,
        prefix: &str,
        marker: Option<&str>,
        delimiter: Option<&str>,
        max_results: u32,
    ) -> Result<ListPage> {
        let mut url = format!("{}?restype=container&comp=list", self.container_url());
        if !prefix.is_empty() {
            url.push_str("&prefix=");
            url.push_str(&query_encode(prefix));
        }
        if let Some(m) = marker.filter(|s| !s.is_empty()) {
            url.push_str("&marker=");
            url.push_str(&query_encode(m));
        }
        if let Some(d) = delimiter {
            url.push_str("&delimiter=");
            url.push_str(&query_encode(d));
        }
        url.push_str("&maxresults=");
        url.push_str(&max_results.to_string());
        let resp = self.execute(self.http.get(&url), false).await?;
        let status = resp.status().as_u16();
        if !(200..300).contains(&status) {
            let body = resp.text().await.unwrap_or_default();
            return Err(Self::classify_write(prefix, status, None, &body));
        }
        let body = resp
            .text()
            .await
            .map_err(|e| StoreError::other(anyhow::anyhow!("azure list body: {e}")))?;
        Ok(parse_list_xml(&body))
    }

    /// Service SAS (account key) or user-delegation SAS (Entra).
    async fn mint_sas(&self, key: &str, ttl: Duration) -> Result<String> {
        let start = Utc::now() - chrono::Duration::minutes(5);
        let expiry =
            Utc::now() + chrono::Duration::from_std(ttl).unwrap_or(chrono::Duration::hours(1));
        let st = start.to_rfc3339_opts(SecondsFormat::Secs, true);
        let se = expiry.to_rfc3339_opts(SecondsFormat::Secs, true);
        let resource = format!("/blob/{}/{}/{}", self.account, self.container, key);
        match &self.auth {
            AzureAuth::SharedKey { key: account_key } => {
                // sp + st + se + canonicalizedResource + ident + ip + proto + sv + sr + snapshot
                // + enc + rscc + rscd + rsce + rscl + rsct  (2020-12-06+)
                let string_to_sign =
                    format!("r\n{st}\n{se}\n{resource}\n\n\n\n{API_VERSION}\nb\n\n\n\n\n\n\n");
                let sig = hmac_b64(account_key, &string_to_sign);
                Ok(format!(
                    "{}?sp=r&st={}&se={}&sv={}&sr=b&sig={}",
                    self.object_url(key),
                    query_encode(&st),
                    query_encode(&se),
                    API_VERSION,
                    query_encode(&sig)
                ))
            }
            AzureAuth::WorkloadIdentity { .. } => {
                let udef = self.user_delegation_key(expiry).await?;
                let string_to_sign = format!(
                    "r\n{st}\n{se}\n{resource}\n{}\n{}\n{}\n{}\n{}\n{}\n\n\n\n\n{API_VERSION}\nb\n\n\n\n\n\n\n",
                    udef.oid, udef.tid, udef.start, udef.expiry, udef.service, udef.version
                );
                let sig = hmac_b64(&udef.key, &string_to_sign);
                Ok(format!(
                    "{}?sp=r&st={}&se={}&sv={}&sr=b&skoid={}&sktid={}&skt={}&ske={}&sks={}&skv={}&sig={}",
                    self.object_url(key),
                    query_encode(&st),
                    query_encode(&se),
                    API_VERSION,
                    query_encode(&udef.oid),
                    query_encode(&udef.tid),
                    query_encode(&udef.start),
                    query_encode(&udef.expiry),
                    query_encode(&udef.service),
                    query_encode(&udef.version),
                    query_encode(&sig),
                ))
            }
        }
    }

    async fn user_delegation_key(&self, required_expiry: DateTime<Utc>) -> Result<UserDelegation> {
        let AzureAuth::WorkloadIdentity { delegation_key, .. } = &self.auth else {
            return Err(StoreError::other(anyhow::anyhow!(
                "azure: user delegation key requires workload identity"
            )));
        };
        let mut cache = delegation_key.lock().await;
        let now = Utc::now();
        if let Some(key) = cached_delegation_key(cache.as_ref(), &required_expiry, &now) {
            return Ok(key);
        }

        let start = now - chrono::Duration::minutes(5);
        let expiry = now + chrono::Duration::hours(167);
        if required_expiry + chrono::Duration::minutes(5) >= expiry {
            return Err(StoreError::InvalidArgument(
                "azure user-delegation signed URL TTL must be less than 167 hours".into(),
            ));
        }
        let start_text = start.to_rfc3339_opts(SecondsFormat::Secs, true);
        let expiry_text = expiry.to_rfc3339_opts(SecondsFormat::Secs, true);
        let url = format!(
            "{}?restype=service&comp=userdelegationkey",
            self.service_url
        );
        let body = format!(
            "<?xml version=\"1.0\" encoding=\"utf-8\"?><KeyInfo><Start>{start_text}</Start><Expiry>{expiry_text}</Expiry></KeyInfo>"
        );
        let response = self
            .execute(
                self.http
                    .post(&url)
                    .header("content-type", "application/xml")
                    .header("content-length", body.len().to_string())
                    .body(body),
                false,
            )
            .await?;
        let status = response.status().as_u16();
        let text = response.text().await.unwrap_or_default();
        if !(200..300).contains(&status) {
            return Err(StoreError::other(anyhow::anyhow!(
                "azure user delegation key {status}: {text}"
            )));
        }
        let value = xml_text(&text, "Value").ok_or_else(|| {
            StoreError::other(anyhow::anyhow!("azure user delegation key: no Value"))
        })?;
        let key = base64::engine::general_purpose::STANDARD
            .decode(value.trim())
            .map_err(|e| StoreError::other(anyhow::anyhow!("azure delegation key: {e}")))?;
        let signed_expiry = xml_text(&text, "SignedExpiry").unwrap_or(&expiry_text);
        let expires_at = DateTime::parse_from_rfc3339(signed_expiry)
            .map(|value| value.with_timezone(&Utc))
            .map_err(|e| StoreError::other(anyhow::anyhow!("azure delegation expiry: {e}")))?;
        let delegation = UserDelegation {
            oid: xml_text(&text, "SignedOid").unwrap_or_default().to_owned(),
            tid: xml_text(&text, "SignedTid").unwrap_or_default().to_owned(),
            start: xml_text(&text, "SignedStart")
                .unwrap_or(&start_text)
                .to_owned(),
            expiry: signed_expiry.to_owned(),
            expires_at,
            service: xml_text(&text, "SignedService").unwrap_or("b").to_owned(),
            version: xml_text(&text, "SignedVersion")
                .unwrap_or(API_VERSION)
                .to_owned(),
            key,
        };
        *cache = Some(delegation.clone());
        Ok(delegation)
    }
}

fn cached_delegation_key(
    cached: Option<&UserDelegation>,
    required_expiry: &DateTime<Utc>,
    now: &DateTime<Utc>,
) -> Option<UserDelegation> {
    cached
        .filter(|key| delegation_key_is_fresh(&key.expires_at, required_expiry, now))
        .cloned()
}

fn delegation_key_is_fresh(
    expires_at: &DateTime<Utc>,
    required_expiry: &DateTime<Utc>,
    now: &DateTime<Utc>,
) -> bool {
    let margin = chrono::Duration::minutes(5);
    expires_at.signed_duration_since(required_expiry.to_owned()) > margin
        && expires_at.signed_duration_since(now.to_owned()) > margin
}

#[derive(Clone)]
struct UserDelegation {
    oid: String,
    tid: String,
    start: String,
    expiry: String,
    expires_at: DateTime<Utc>,
    service: String,
    version: String,
    key: Vec<u8>,
}

struct ParsedAzure {
    account: String,
    service_url: String,
    path_style: bool,
    auth: AzureAuth,
}

fn resolve_azure(cfg: &walgit_config::StoreConfig) -> anyhow::Result<ParsedAzure> {
    let azure = &cfg.azure;
    if azure.account.is_empty() {
        anyhow::bail!("azure: store.azure.account must be set");
    }
    let auth = match azure.credential {
        walgit_config::AzureCredential::AccountKey => {
            let encoded = std::env::var(&azure.account_key_env).map_err(|_| {
                anyhow::anyhow!(
                    "azure: account_key credential requires env {}",
                    azure.account_key_env
                )
            })?;
            let key = base64::engine::general_purpose::STANDARD
                .decode(encoded.trim())
                .map_err(|e| anyhow::anyhow!("azure: account key is not valid base64: {e}"))?;
            AzureAuth::SharedKey { key }
        }
        walgit_config::AzureCredential::WorkloadIdentity => {
            let credential = azure_identity::WorkloadIdentityCredential::new(None)
                .map_err(|e| anyhow::anyhow!("azure workload identity credential: {e}"))?;
            AzureAuth::WorkloadIdentity {
                credential,
                delegation_key: Arc::new(tokio::sync::Mutex::new(None)),
            }
        }
    };
    let (service_url, path_style) = service_url_for(&azure.account, &azure.endpoint);
    Ok(ParsedAzure {
        account: azure.account.clone(),
        service_url,
        path_style,
        auth,
    })
}

fn service_url_for(account: &str, endpoint: &str) -> (String, bool) {
    if endpoint.is_empty() {
        return (format!("https://{account}.blob.core.windows.net"), false);
    }
    let endpoint = endpoint.trim_end_matches('/');
    let Ok(u) = reqwest::Url::parse(endpoint) else {
        return (endpoint.to_owned(), true);
    };
    let host = u.host_str().unwrap_or("");
    let path_style = !host.eq_ignore_ascii_case(&format!("{account}.blob.core.windows.net"))
        && !host
            .to_ascii_lowercase()
            .starts_with(&format!("{}.blob.", account.to_ascii_lowercase()));
    if path_style {
        let origin = format!(
            "{}://{}{}",
            u.scheme(),
            host,
            u.port().map(|p| format!(":{p}")).unwrap_or_default()
        );
        (format!("{origin}/{account}"), true)
    } else {
        let origin = format!(
            "{}://{}{}",
            u.scheme(),
            host,
            u.port().map(|p| format!(":{p}")).unwrap_or_default()
        );
        (origin, false)
    }
}

fn shared_key_auth(
    account: &str,
    key: &[u8],
    method: &reqwest::Method,
    url: &reqwest::Url,
    headers: &reqwest::header::HeaderMap,
) -> Result<String> {
    let sts = string_to_sign(account, method, url, headers);
    let sig = hmac_b64(key, &sts);
    Ok(format!("SharedKey {account}:{sig}"))
}

fn string_to_sign(
    account: &str,
    method: &reqwest::Method,
    url: &reqwest::Url,
    headers: &reqwest::header::HeaderMap,
) -> String {
    let content_length = header_str(headers, "content-length")
        .filter(|v| v != "0")
        .unwrap_or_default();
    format!(
        "{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}{}",
        method.as_str(),
        header_str(headers, "content-encoding").unwrap_or_default(),
        header_str(headers, "content-language").unwrap_or_default(),
        content_length,
        header_str(headers, "content-md5").unwrap_or_default(),
        header_str(headers, "content-type").unwrap_or_default(),
        header_str(headers, "date").unwrap_or_default(),
        header_str(headers, "if-modified-since").unwrap_or_default(),
        header_str(headers, "if-match").unwrap_or_default(),
        header_str(headers, "if-none-match").unwrap_or_default(),
        header_str(headers, "if-unmodified-since").unwrap_or_default(),
        header_str(headers, "range").unwrap_or_default(),
        canonicalize_headers(headers),
        canonicalize_resource(account, url),
    )
}

fn canonicalize_headers(headers: &reqwest::header::HeaderMap) -> String {
    let mut ms: Vec<(String, String)> = headers
        .iter()
        .filter_map(|(k, v)| {
            let name = k.as_str().to_ascii_lowercase();
            if !name.starts_with("x-ms-") {
                return None;
            }
            let val = v.to_str().ok()?.trim().to_owned();
            Some((name, val))
        })
        .collect();
    ms.sort_by(|a, b| a.0.cmp(&b.0));
    let mut out = String::new();
    for (n, v) in ms {
        out.push_str(&n);
        out.push(':');
        out.push_str(&v);
        out.push('\n');
    }
    out
}

fn canonicalize_resource(account: &str, url: &reqwest::Url) -> String {
    let mut can = String::new();
    can.push('/');
    can.push_str(account);
    can.push_str(url.path());
    let mut params: Vec<(String, String)> = url
        .query_pairs()
        .map(|(k, v)| (k.to_ascii_lowercase(), v.into_owned()))
        .collect();
    params.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
    for (n, v) in params {
        can.push('\n');
        can.push_str(&n);
        can.push(':');
        can.push_str(&v);
    }
    can
}

fn hmac_b64(key: &[u8], data: &str) -> String {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC-SHA256 accepts any key length");
    mac.update(data.as_bytes());
    base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes())
}

fn header_str(h: &reqwest::header::HeaderMap, name: &str) -> Option<String> {
    h.get(name)?.to_str().ok().map(|s| s.to_owned())
}

fn strip_etag(v: &str) -> String {
    v.trim()
        .replace("&quot;", "\"")
        .trim_matches('"')
        .to_owned()
}

fn query_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
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

fn xml_text<'a>(s: &'a str, tag: &str) -> Option<&'a str> {
    let open = format!("<{tag}>");
    let open_alt = format!("<{tag} ");
    let close = format!("</{tag}>");
    let start = if let Some(i) = s.find(&open) {
        i + open.len()
    } else {
        let i = s.find(&open_alt)?;
        s[i..].find('>')? + i + 1
    };
    let end = s[start..].find(&close)? + start;
    Some(&s[start..end])
}

struct ListPage {
    blobs: Vec<ObjectMeta>,
    prefixes: Vec<String>,
    next_marker: Option<String>,
}

fn parse_list_xml(body: &str) -> ListPage {
    let mut blobs = Vec::new();
    let mut prefixes = Vec::new();
    // Walk <Blob>…</Blob> (not BlobPrefix).
    let mut rest = body;
    while let Some(start) = rest.find("<Blob>") {
        let after = &rest[start + 6..];
        let Some(end) = after.find("</Blob>") else {
            break;
        };
        let blob = &after[..end];
        rest = &after[end + 7..];
        // Skip BlobPrefix accidentally? <Blob> doesn't match BlobPrefix.
        let Some(name) = xml_text(blob, "Name") else {
            continue;
        };
        let name = xml_unescape(name);
        let size = xml_text(blob, "Content-Length")
            .and_then(|s| s.trim().parse::<u64>().ok())
            .unwrap_or(0);
        let etag = xml_text(blob, "Etag")
            .or_else(|| xml_text(blob, "ETag"))
            .map(strip_etag)
            .unwrap_or_default();
        blobs.push(ObjectMeta {
            key: name,
            size,
            version: Version::new(etag),
        });
    }
    rest = body;
    while let Some(start) = rest.find("<BlobPrefix>") {
        let after = &rest[start + 12..];
        let Some(end) = after.find("</BlobPrefix>") else {
            break;
        };
        let p = &after[..end];
        rest = &after[end + 13..];
        if let Some(name) = xml_text(p, "Name") {
            prefixes.push(xml_unescape(name));
        }
    }
    let next_marker = xml_text(body, "NextMarker")
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty());
    ListPage {
        blobs,
        prefixes,
        next_marker,
    }
}

fn xml_unescape(s: &str) -> String {
    s.replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
}

#[async_trait::async_trait]
impl ObjectStore for AzureStore {
    fn backend(&self) -> &'static str {
        "azure"
    }

    async fn get(&self, key: &str, opts: GetOptions) -> Result<GetResult> {
        let url = self.object_url(key);
        let bulk = opts.range.is_some() || util::is_bulk_key(key);
        let mut builder = self.client(bulk).get(&url);
        if let Some(version) = &opts.if_none_match {
            builder = builder.header("if-none-match", Self::quote_etag(version.as_str()));
        }
        if let Some(version) = &opts.if_match {
            builder = builder.header("if-match", Self::quote_etag(version.as_str()));
        }
        if let Some(range) = &opts.range {
            builder = builder.header("range", Self::range_header(range));
        }
        let response = self.execute(builder, bulk).await?;
        Self::get_result_from_response(key, response)
    }

    async fn head(&self, key: &str) -> Result<Option<ObjectMeta>> {
        let url = self.object_url(key);
        let resp = self.execute(self.http.head(&url), false).await?;
        let status = resp.status().as_u16();
        match status {
            200 => {
                let etag = header_str(resp.headers(), "etag").map(|s| strip_etag(&s));
                let size = header_str(resp.headers(), "content-length")
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or(0);
                Ok(Some(ObjectMeta {
                    key: key.into(),
                    size,
                    version: Version::new(etag.as_deref().unwrap_or("")),
                }))
            }
            404 => Ok(None),
            s => {
                let body = resp.text().await.unwrap_or_default();
                Err(Self::classify_write(key, s, None, &body))
            }
        }
    }

    async fn put(&self, key: &str, body: PutBody, opts: PutOptions) -> Result<ObjectMeta> {
        match body {
            PutBody::Bytes(b) => {
                if b.len() as u64 > self.multipart_threshold {
                    self.put_blocks_bytes(key, b, &opts).await
                } else {
                    self.put_blob(key, b, &opts).await
                }
            }
            PutBody::Stream { len, stream } => {
                if len > self.multipart_threshold {
                    self.put_stream(key, stream, len, &opts).await
                } else {
                    let collected = util::collect(stream, len as usize).await?;
                    self.put_blob(key, collected, &opts).await
                }
            }
            PutBody::File(path) => {
                let meta = tokio::fs::metadata(&path).await.map_err(|e| {
                    StoreError::other(anyhow::anyhow!("stat {}: {e}", path.display()))
                })?;
                let len = meta.len();
                if len > self.multipart_threshold {
                    let stream = util::file_stream(path, None, 1024 * 1024);
                    self.put_stream(key, stream, len, &opts).await
                } else {
                    let data = tokio::fs::read(&path).await.map_err(|e| {
                        StoreError::other(anyhow::anyhow!("read {}: {e}", path.display()))
                    })?;
                    self.put_blob(key, Bytes::from(data), &opts).await
                }
            }
        }
    }

    async fn delete(&self, key: &str, if_version: Option<Version>) -> Result<()> {
        let url = self.object_url(key);
        let mut builder = self.http.delete(&url);
        if let Some(version) = &if_version {
            builder = builder.header("if-match", Self::quote_etag(version.as_str()));
        }
        let response = self.execute(builder, false).await?;
        let status = response.status().as_u16();
        match status {
            200 | 202 => Ok(()),
            404 if if_version.is_none() => Ok(()),
            404 => Err(StoreError::NotFound { key: key.into() }),
            412 | 409 => match self.head(key).await? {
                None => Err(StoreError::NotFound { key: key.into() }),
                Some(meta) => Err(StoreError::PreconditionFailed {
                    key: key.into(),
                    current: Some(meta.version),
                }),
            },
            status => {
                let body = response.text().await.unwrap_or_default();
                Err(Self::classify_write(key, status, None, &body))
            }
        }
    }

    fn list(
        &self,
        prefix: &str,
        start_after: Option<&str>,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        let store = self.clone();
        let prefix = prefix.to_owned();
        let start_after = start_after.map(str::to_owned);
        Box::pin(futures::stream::unfold(
            ListState {
                store,
                prefix,
                start_after,
                marker: None,
                started: false,
                buffer: Vec::new().into_iter(),
            },
            |mut state| async move {
                loop {
                    if let Some(item) = state.buffer.next() {
                        return Some((item, state));
                    }
                    if state.started && state.marker.is_none() {
                        return None;
                    }
                    state.started = true;
                    match state
                        .store
                        .list_page(&state.prefix, state.marker.as_deref(), None, 1000)
                        .await
                    {
                        Ok(page) => {
                            state.marker = page.next_marker;
                            let skip = state.start_after.as_deref();
                            let items: Vec<Result<ObjectMeta>> = page
                                .blobs
                                .into_iter()
                                .filter(|m| skip.is_none_or(|s| m.key.as_str() > s))
                                .map(Ok)
                                .collect();
                            if items.is_empty() && state.marker.is_some() {
                                continue;
                            }
                            state.buffer = items.into_iter();
                            let item = state.buffer.next();
                            return item.map(|i| (i, state));
                        }
                        Err(e) => return Some((Err(e), state)),
                    }
                }
            },
        ))
    }

    async fn list_prefixes(&self, prefix: &str) -> Result<Vec<String>> {
        let mut out = Vec::new();
        let mut marker = None;
        loop {
            let page = self
                .list_page(prefix, marker.as_deref(), Some("/"), 1000)
                .await?;
            out.extend(page.prefixes);
            marker = page.next_marker;
            if marker.is_none() {
                break;
            }
        }
        out.sort();
        out.dedup();
        Ok(out)
    }

    async fn signed_get_url(&self, key: &str, ttl: Duration) -> Result<Option<String>> {
        Ok(Some(self.mint_sas(key, ttl).await?))
    }

    async fn accel_target(&self, key: &str) -> Option<crate::AccelTarget> {
        let url = self
            .signed_get_url(key, Duration::from_secs(3600))
            .await
            .ok()
            .flatten()?;
        Some(crate::AccelTarget {
            url,
            authorization: None,
        })
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
        if sources.is_empty() {
            return Err(StoreError::InvalidArgument(
                "compose needs at least one source".into(),
            ));
        }
        let mut sizes = Vec::with_capacity(sources.len());
        for src in sources {
            let m = self
                .head(src)
                .await?
                .ok_or_else(|| StoreError::NotFound { key: src.clone() })?;
            sizes.push(m.size);
        }
        let total: u64 = sizes.iter().sum();
        let upload = Self::upload_id();
        let mut n = 0u32;
        for (src, size) in sources.iter().zip(sizes.iter().copied()) {
            if size == 0 {
                self.put_block(dest, &upload, n, Bytes::new()).await?;
                n += 1;
                continue;
            }
            let source_url = if self.path_style {
                None
            } else {
                Some(self.mint_sas(src, Duration::from_secs(3600)).await?)
            };
            let mut off = 0u64;
            while off < size {
                let end = (off + self.multipart_part_size).min(size);
                if let Some(source_url) = &source_url {
                    self.put_block_from_url(dest, &upload, n, source_url, off..end)
                        .await?;
                } else {
                    let result = self
                        .get(
                            src,
                            GetOptions {
                                range: Some(off..end),
                                ..GetOptions::default()
                            },
                        )
                        .await?;
                    let GetResult::Object { body, .. } = result else {
                        return Err(StoreError::other(anyhow::anyhow!(
                            "azure compose source unexpectedly returned not modified: {src}"
                        )));
                    };
                    let bytes = util::collect(body, (end - off) as usize).await?;
                    self.put_block(dest, &upload, n, bytes).await?;
                }
                n += 1;
                off = end;
            }
        }
        self.commit_block_list(dest, &upload, n, &opts, total).await
    }
}

struct ListState {
    store: AzureStore,
    prefix: String,
    start_after: Option<String>,
    marker: Option<String>,
    started: bool,
    buffer: std::vec::IntoIter<Result<ObjectMeta>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn service_url_production_is_host_style() {
        let (u, path) = service_url_for("myacct", "");
        assert_eq!(u, "https://myacct.blob.core.windows.net");
        assert!(!path);
        let (u, path) = service_url_for("myacct", "https://myacct.blob.core.windows.net");
        assert_eq!(u, "https://myacct.blob.core.windows.net");
        assert!(!path);
    }

    #[test]
    fn service_url_azurite_is_path_style() {
        let (u, path) = service_url_for("devstoreaccount1", "http://127.0.0.1:10000");
        assert_eq!(u, "http://127.0.0.1:10000/devstoreaccount1");
        assert!(path);
    }

    #[test]
    fn etag_roundtrip_quotes() {
        assert_eq!(strip_etag("\"0xABC\""), "0xABC");
        assert_eq!(strip_etag("&quot;0xABC&quot;"), "0xABC");
        assert_eq!(AzureStore::quote_etag("0xABC"), "\"0xABC\"");
        assert_eq!(AzureStore::quote_etag("*"), "*");
    }

    #[test]
    fn block_ids_are_scoped_to_an_upload() {
        let first = AzureStore::block_id("first", 0);
        let second = AzureStore::block_id("second", 0);
        assert_ne!(first, second);
        assert_eq!(first, AzureStore::block_id("first", 0));
    }

    #[test]
    fn delegation_key_cache_keeps_a_safe_expiry_margin() {
        let now = Utc::now();
        let required = now + chrono::Duration::minutes(30);
        let valid_expiry = now + chrono::Duration::hours(1);
        let delegation = UserDelegation {
            oid: "oid".into(),
            tid: "tid".into(),
            start: now.to_rfc3339(),
            expiry: valid_expiry.to_rfc3339(),
            expires_at: valid_expiry.to_owned(),
            service: "b".into(),
            version: API_VERSION.into(),
            key: vec![1, 2, 3],
        };
        assert_eq!(
            cached_delegation_key(Some(&delegation), &required, &now)
                .expect("valid cached key")
                .key,
            vec![1, 2, 3]
        );
        assert!(delegation_key_is_fresh(&valid_expiry, &required, &now));
        let exact_margin = required + chrono::Duration::minutes(5);
        assert!(!delegation_key_is_fresh(&exact_margin, &required, &now));
        let expired = UserDelegation {
            expires_at: exact_margin,
            ..delegation
        };
        assert!(cached_delegation_key(Some(&expired), &required, &now).is_none());
        let near_expiry = now + chrono::Duration::minutes(5);
        assert!(!delegation_key_is_fresh(&near_expiry, &now, &now));
    }

    #[test]
    fn list_xml_blobs_and_prefixes() {
        let xml = r#"
<EnumerationResults>
  <Blobs>
    <Blob><Name>a/b</Name><Properties><Content-Length>3</Content-Length><Etag>"0x1"</Etag></Properties></Blob>
    <BlobPrefix><Name>a/c/</Name></BlobPrefix>
  </Blobs>
  <NextMarker>more</NextMarker>
</EnumerationResults>"#;
        let p = parse_list_xml(xml);
        assert_eq!(p.blobs.len(), 1);
        assert_eq!(p.blobs[0].key, "a/b");
        assert_eq!(p.blobs[0].size, 3);
        assert_eq!(p.blobs[0].version.as_str(), "0x1");
        assert_eq!(p.prefixes, vec!["a/c/".to_string()]);
        assert_eq!(p.next_marker.as_deref(), Some("more"));
    }

    #[test]
    fn canonicalized_resource_includes_account_and_sorted_query() {
        let url = reqwest::Url::parse(
            "http://127.0.0.1:10000/devstoreaccount1/c/k?comp=list&restype=container",
        )
        .unwrap();
        let r = canonicalize_resource("devstoreaccount1", &url);
        assert!(r.starts_with("/devstoreaccount1/devstoreaccount1/c/k\n"));
        assert!(r.contains("\ncomp:list"));
        assert!(r.contains("\nrestype:container"));
    }

    #[tokio::test]
    async fn missing_creds_fail_closed() {
        let mut cfg = walgit_config::Config::default();
        cfg.store.backend = walgit_config::StoreBackend::Azure;
        cfg.store.bucket = "walgit-test".into();
        cfg.store.azure.account = "devstoreaccount1".into();
        cfg.store.azure.credential = walgit_config::AzureCredential::AccountKey;
        cfg.store.azure.account_key_env = "WALGIT_TEST_AZURE_KEY_MUST_NOT_EXIST_9f3a".into();
        let err = match crate::open_store(&cfg).await {
            Ok(_) => panic!("missing creds must fail"),
            Err(e) => e.to_string(),
        };
        assert!(
            err.contains("WALGIT_TEST_AZURE_KEY_MUST_NOT_EXIST_9f3a"),
            "{err}"
        );
    }
}
