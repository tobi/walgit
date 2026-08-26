//! S3-compatible backend (rustfs for local dev / CI).
//!
//! Uses `aws-sdk-s3` for all operations. GET responses are streamed via
//! presigned URLs + `reqwest` because the SDK's `GetObjectOutput::body()`
//! returns `&ByteStream` with no owned-body extractor. All other operations
//! (PUT, HEAD, DELETE, LIST) use the SDK directly.
//!
//! ## Version tokens
//!
//! S3 ETags are used as opaque `Version` strings. Quotes are stripped
//! consistently on read and never stored. For non-multipart uploads the
//! ETag is the MD5 of the content; for multipart uploads it is a compound
//! hash. Callers never parse the token — equality comparison suffices.
//!
//! ## Conditional PUT
//!
//! `PutMode::Create`    → `If-None-Match: *`  (object must not exist).
//! `PutMode::Update(v)` → `If-Match: <etag>`  (CAS on current ETag).
//! On failure the SDK returns a `PreconditionFailed` service error; we fill
//! `current` via a follow-up HEAD when the SDK doesn't include it.
//!
//! ## Conditional DELETE
//!
//! Conditional deletes use S3's native `If-Match` ETag precondition. The
//! precondition is evaluated by S3 at the delete linearization point; callers
//! must not emulate it with `HEAD` followed by an unconditional delete.
//!
//! ## Multipart upload
//!
//! Objects above `cfg.multipart_threshold` use CreateMultipartUpload +
//! UploadPart + CompleteMultipartUpload. S3 applies `If-None-Match`/`If-Match`
//! at multipart completion, so immutable creates and CAS updates remain
//! conditional even when the object is larger than the single-PUT limit.
//! Completion conflicts are surfaced as precondition failures and incomplete
//! uploads are aborted on every failure path.
//!
//! ## rustfs compatibility (tested with rustfs/rustfs:latest)
//!
//! See the compatibility notes at the bottom of this file.

use std::ops::Range;
use std::time::Duration;

use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::config::Credentials;
use aws_sdk_s3::presigning::PresigningConfig;
use aws_sdk_s3::primitives::ByteStream as S3ByteStream;
use bytes::Bytes;
use futures::StreamExt;

use crate::{
    BoxStream, GetOptions, GetResult, ObjectMeta, ObjectStore, PutBody, PutMode, PutOptions,
    Result, StoreError, Version, util,
};

/// S3-compatible object store.
pub struct S3Store {
    client: S3Client,
    bucket: String,
    /// reqwest client for streaming GETs via presigned URLs.
    http: reqwest::Client,
    multipart_threshold: u64,
    multipart_part_size: u64,
}

impl S3Store {
    /// Build a store from `walgit-config::StoreConfig`.
    ///
    /// Credentials are read from the env vars named in
    /// `cfg.s3.access_key_env` / `cfg.s3.secret_key_env`
    /// (defaults `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`), plus
    /// `AWS_SESSION_TOKEN` when present.
    pub async fn new(cfg: &walgit_config::StoreConfig) -> anyhow::Result<Self> {
        let access_key = std::env::var(&cfg.s3.access_key_env).map_err(|_| {
            anyhow::anyhow!("s3: env var {} not set (access key)", cfg.s3.access_key_env)
        })?;
        let secret_key = std::env::var(&cfg.s3.secret_key_env).map_err(|_| {
            anyhow::anyhow!("s3: env var {} not set (secret key)", cfg.s3.secret_key_env)
        })?;

        let creds = static_credentials(
            &access_key,
            &secret_key,
            std::env::var("AWS_SESSION_TOKEN").ok(),
        );
        let region = aws_sdk_s3::config::Region::new(cfg.s3.region.clone());

        let mut s3_config = aws_sdk_s3::Config::builder()
            .region(region)
            .credentials_provider(creds)
            .force_path_style(cfg.s3.force_path_style)
            .behavior_version_latest();

        if !cfg.s3.endpoint.is_empty() {
            s3_config = s3_config.endpoint_url(&cfg.s3.endpoint);
        }

        let client = S3Client::from_conf(s3_config.build());
        let http = reqwest::Client::builder().build()?;

        Ok(S3Store {
            client,
            bucket: cfg.bucket.clone(),
            http,
            multipart_threshold: cfg.multipart_threshold.as_u64(),
            multipart_part_size: cfg.multipart_part_size.as_u64(),
        })
    }

    /// `bytes=start-(end-1)` for a half-open range (S3 Range is inclusive).
    fn range_header(range: &Range<u64>) -> String {
        format!("bytes={}-{}", range.start, range.end.saturating_sub(1))
    }

    // ---- GET via presigned URL + reqwest (true streaming) ---------------

    async fn presigned_get(&self, key: &str, opts: &GetOptions) -> Result<reqwest::Response> {
        let presigning = PresigningConfig::expires_in(Duration::from_secs(60))
            .map_err(|e| StoreError::other(anyhow::anyhow!("presigning config: {e}")))?;

        let mut builder = self.client.get_object().bucket(&self.bucket).key(key);

        if let Some(v) = &opts.if_none_match {
            builder = builder.if_none_match(v.as_str());
        }
        if let Some(v) = &opts.if_match {
            builder = builder.if_match(v.as_str());
        }
        if let Some(r) = &opts.range {
            builder = builder.range(Self::range_header(r));
        }

        let presigned = builder
            .presigned(presigning)
            .await
            .map_err(|e| StoreError::other(anyhow::anyhow!("presigning get: {e}")))?;

        let mut req = self.http.get(presigned.uri());
        for (name, value) in presigned.headers() {
            req = req.header(name, value);
        }

        req.send()
            .await
            .map_err(|e| StoreError::retryable(anyhow::anyhow!("s3 get http: {e}")))
    }

    fn get_result_from_response(key: &str, resp: reqwest::Response) -> Result<GetResult> {
        let status = resp.status();
        let etag = resp
            .headers()
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.trim_matches('"').to_owned());
        let content_length = resp
            .headers()
            .get("content-length")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok());

        // `ObjectMeta::size` is the size of the whole object (as on GCS/memory),
        // also for range reads: `Content-Range: bytes a-b/total` carries it.
        let total = resp
            .headers()
            .get("content-range")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.rsplit_once('/'))
            .and_then(|(_, t)| t.trim().parse::<u64>().ok());

        match status.as_u16() {
            200 | 206 => {
                let version = Version::new(etag.as_deref().unwrap_or(""));
                let meta = ObjectMeta {
                    key: key.into(),
                    size: total.or(content_length).unwrap_or(0),
                    version,
                };
                let body = resp
                    .bytes_stream()
                    .map(|r| r.map_err(|e| StoreError::retryable(anyhow::anyhow!("s3 body: {e}"))))
                    .boxed();
                Ok(GetResult::Object { meta, body })
            }
            304 => Ok(GetResult::NotModified {
                version: Version::new(etag.as_deref().unwrap_or("")),
            }),
            404 => Err(StoreError::NotFound { key: key.into() }),
            412 => Err(StoreError::PreconditionFailed {
                key: key.into(),
                current: etag.map(Version::new),
            }),
            s if s >= 500 || s == 429 => {
                Err(StoreError::Retryable(anyhow::anyhow!("s3 get status {s}")))
            }
            s => Err(StoreError::Other(anyhow::anyhow!("s3 get status {s}"))),
        }
    }
}

// ---- PutBody → SDK ByteStream ------------------------------------------

async fn body_to_s3(body: PutBody) -> Result<(S3ByteStream, u64)> {
    Ok(match body {
        PutBody::Bytes(b) => {
            let len = b.len() as u64;
            (S3ByteStream::from(b), len)
        }
        PutBody::Stream { len, stream } => {
            // Collect into Bytes: walgit's Stream bodies are small objects
            // (manifests, leases). Large packs use PutBody::File which
            // streams via ByteStream::read_from().
            let collected = util::collect(stream, len as usize).await?;
            (S3ByteStream::from(collected), len)
        }
        PutBody::File(path) => {
            let meta = tokio::fs::metadata(&path)
                .await
                .map_err(|e| StoreError::other(anyhow::anyhow!("stat {}: {e}", path.display())))?;
            let len = meta.len();
            let stream = S3ByteStream::read_from()
                .path(&path)
                .buffer_size(64 * 1024)
                .build()
                .await
                .map_err(|e| StoreError::other(anyhow::anyhow!("file stream: {e}")))?;
            (stream, len)
        }
    })
}

// ---- error classification ----------------------------------------------

/// Extract the error code string from an SdkError's service error metadata.
fn err_code<E>(err: &aws_sdk_s3::error::SdkError<E>) -> Option<&str>
where
    E: aws_sdk_s3::error::ProvideErrorMetadata,
{
    err.as_service_error().and_then(|e| e.meta().code())
}

fn classify_put_error(
    key: &str,
    err: aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::put_object::PutObjectError>,
) -> StoreError {
    let code = err_code(&err).unwrap_or("");
    match code {
        "PreconditionFailed" | "ConditionalRequestConflict" => StoreError::PreconditionFailed {
            key: key.into(),
            current: None,
        },
        _ => StoreError::Other(anyhow::anyhow!("s3 put error: {err}")),
    }
}

fn classify_list_error(
    err: aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Error>,
) -> StoreError {
    StoreError::Other(anyhow::anyhow!("s3 list error: {err}"))
}

#[async_trait::async_trait]
impl ObjectStore for S3Store {
    fn backend(&self) -> &'static str {
        "s3"
    }

    async fn get(&self, key: &str, opts: GetOptions) -> Result<GetResult> {
        let resp = self.presigned_get(key, &opts).await?;
        Self::get_result_from_response(key, resp)
    }

    async fn head(&self, key: &str) -> Result<Option<ObjectMeta>> {
        let resp = self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await;

        match resp {
            Ok(out) => {
                let etag = out.e_tag().map(|s| s.trim_matches('"').to_owned());
                let size = out.content_length().unwrap_or(0) as u64;
                Ok(Some(ObjectMeta {
                    key: key.into(),
                    size,
                    version: Version::new(etag.as_deref().unwrap_or("")),
                }))
            }
            Err(err) => {
                if let Some(aws_sdk_s3::operation::head_object::HeadObjectError::NotFound(_)) =
                    err.as_service_error()
                {
                    return Ok(None);
                }
                Err(StoreError::Other(anyhow::anyhow!("s3 head error: {err}")))
            }
        }
    }

    async fn put(&self, key: &str, body: PutBody, opts: PutOptions) -> Result<ObjectMeta> {
        let (s3_body, len) = body_to_s3(body).await?;

        // Apply conditional create/update at multipart completion. This keeps
        // the storage abstraction atomic for large immutable packs too.
        let use_multipart = len > self.multipart_threshold;

        if use_multipart {
            return self.multipart_put(key, s3_body, len, &opts).await;
        }

        let mut builder = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(s3_body)
            .content_length(len as i64);

        match &opts.mode {
            PutMode::Overwrite => {}
            PutMode::Create => {
                builder = builder.if_none_match("*");
            }
            PutMode::Update(v) => {
                builder = builder.if_match(v.as_str());
            }
        }

        if let Some(ct) = opts.content_type {
            builder = builder.content_type(ct);
        }

        let result = builder.send().await;
        match result {
            Ok(resp) => {
                let etag = resp.e_tag().map(|s| s.trim_matches('"').to_owned());
                Ok(ObjectMeta {
                    key: key.into(),
                    size: len,
                    version: Version::new(etag.as_deref().unwrap_or("")),
                })
            }
            Err(e) => {
                let mut err = classify_put_error(key, e);
                // Fill `current` via HEAD if we got a PreconditionFailed.
                if let StoreError::PreconditionFailed { current: c, .. } = &mut err
                    && c.is_none()
                {
                    *c = self.head(key).await.ok().flatten().map(|m| m.version);
                }
                Err(err)
            }
        }
    }

    async fn delete(&self, key: &str, if_version: Option<Version>) -> Result<()> {
        let mut request = self.client.delete_object().bucket(&self.bucket).key(key);
        if let Some(want) = &if_version {
            request = request.if_match(want.as_str());
        }
        let resp = request.send().await;

        match resp {
            Ok(_) => Ok(()),
            Err(err) => {
                let code = err_code(&err).unwrap_or("");
                if if_version.is_some()
                    && matches!(code, "PreconditionFailed" | "ConditionalRequestConflict")
                {
                    return Err(StoreError::PreconditionFailed {
                        key: key.into(),
                        current: self.head(key).await.ok().flatten().map(|m| m.version),
                    });
                }
                // S3 DeleteObject is idempotent: deleting a non-existent key
                // returns Ok, not an error. If we get here, it's a real error.
                // For unconditional deletes we treat any error as transient.
                if if_version.is_none() {
                    // Unconditional delete — be lenient (idempotent on S3/rustfs).
                    let err_str = err.to_string();
                    if err_str.contains("404")
                        || err_str.contains("NoSuchKey")
                        || err_str.contains("not found")
                    {
                        return Ok(());
                    }
                }
                Err(StoreError::Other(anyhow::anyhow!("s3 delete error: {err}")))
            }
        }
    }

    fn list(
        &self,
        prefix: &str,
        start_after: Option<&str>,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let prefix = prefix.to_owned();
        let start_after = start_after.map(|s| s.to_owned());

        Box::pin(futures::stream::unfold(
            ListState {
                client,
                bucket,
                prefix,
                start_after,
                continuation_token: None,
                started: false,
                buffer: Vec::new().into_iter(),
            },
            |mut state| async move {
                // Drain buffered items first.
                if let Some(item) = state.buffer.next() {
                    return Some((item, state));
                }

                if state.started && state.continuation_token.is_none() {
                    return None;
                }
                state.started = true;

                let mut builder = state
                    .client
                    .list_objects_v2()
                    .bucket(&state.bucket)
                    .prefix(&state.prefix)
                    .max_keys(1000);

                if let Some(sa) = &state.start_after {
                    builder = builder.start_after(sa);
                }
                if let Some(ct) = &state.continuation_token {
                    builder = builder.continuation_token(ct);
                }

                match builder.send().await {
                    Ok(resp) => {
                        let items: Vec<Result<ObjectMeta>> = resp
                            .contents()
                            .iter()
                            .map(|obj| {
                                let etag = obj.e_tag().map(|s| s.trim_matches('"').to_owned());
                                Ok(ObjectMeta {
                                    key: obj.key().unwrap_or("").to_owned(),
                                    size: obj.size().unwrap_or(0) as u64,
                                    version: Version::new(etag.as_deref().unwrap_or("")),
                                })
                            })
                            .collect();

                        state.continuation_token = resp
                            .is_truncated()
                            .unwrap_or(false)
                            .then(|| resp.next_continuation_token().map(|s| s.to_owned()))
                            .flatten();
                        state.buffer = items.into_iter();

                        let item = state.buffer.next();
                        item.map(|i| (i, state))
                    }
                    Err(err) => Some((Err(classify_list_error(err)), state)),
                }
            },
        ))
    }

    async fn list_prefixes(&self, prefix: &str) -> Result<Vec<String>> {
        let mut out = Vec::new();
        let mut continuation_token: Option<String> = None;
        loop {
            let mut builder = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(prefix)
                .delimiter("/")
                .max_keys(1000);
            if let Some(ct) = &continuation_token {
                builder = builder.continuation_token(ct);
            }
            let resp = builder.send().await.map_err(classify_list_error)?;
            out.extend(
                resp.common_prefixes()
                    .iter()
                    .filter_map(|p| p.prefix().map(str::to_owned)),
            );
            continuation_token = resp
                .is_truncated()
                .unwrap_or(false)
                .then(|| resp.next_continuation_token().map(|s| s.to_owned()))
                .flatten();
            if continuation_token.is_none() {
                break;
            }
        }
        out.sort();
        out.dedup();
        Ok(out)
    }

    /// A presigned GET (1 h): the edge needs no credentials and `Range` stays free (unsigned).
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

    /// Concatenate `sources` into `dest` with one multipart upload whose parts are
    /// `UploadPartCopy` byte ranges of the sources — nothing streams through this process
    /// except the parts S3 will not copy: every part but the last must be >= 5 MiB, so a
    /// small source (a bundle header in front of a 30 GB pack) is fetched and uploaded
    /// together with the beginning of the next source as one ordinary part.
    async fn compose(
        &self,
        dest: &str,
        sources: &[String],
        opts: PutOptions,
    ) -> Result<ObjectMeta> {
        const MIN_PART: u64 = 5 * 1024 * 1024;
        const COPY_PART: u64 = 1024 * 1024 * 1024; // <= 5 GiB per UploadPartCopy
        if sources.is_empty() {
            return Err(StoreError::InvalidArgument(
                "compose needs at least one source".into(),
            ));
        }
        // Sizes first: the layout of parts depends on them.
        let mut sizes = Vec::with_capacity(sources.len());
        for src in sources {
            let m = self
                .head(src)
                .await?
                .ok_or_else(|| StoreError::NotFound { key: src.clone() })?;
            sizes.push(m.size);
        }
        let total: u64 = sizes.iter().sum();
        // The virtual concatenation, cut into parts: a part is [start, end) of the whole.
        // Runs that lie inside one source and are >= MIN_PART become copies; everything else
        // (a small source, the tail that pads it to MIN_PART) is read and uploaded.
        let mut create = self
            .client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(dest);
        if let Some(ct) = opts.content_type {
            create = create.content_type(ct);
        }
        if opts.immutable {
            create = create.cache_control("public, max-age=31536000, immutable");
        }
        let upload = create
            .send()
            .await
            .map_err(|e| StoreError::Other(anyhow::anyhow!("s3 create multipart: {e}")))?;
        let upload_id = upload
            .upload_id()
            .ok_or_else(|| {
                StoreError::other(anyhow::anyhow!("no upload_id from CreateMultipartUpload"))
            })?
            .to_owned();
        let mut parts: Vec<aws_sdk_s3::types::CompletedPart> = Vec::new();
        let mut part_number = 1i32;
        let mut pos: u64 = 0; // absolute offset into the concatenation
        let offset_of = |i: usize| -> u64 { sizes[..i].iter().sum() };
        let result: Result<()> = async {
            while pos < total {
                // Which source does `pos` fall in, and how far does it run?
                let i = (0..sources.len())
                    .find(|&i| pos < offset_of(i) + sizes[i])
                    .unwrap();
                let src_end = offset_of(i) + sizes[i];
                let run = src_end - pos;
                let last_part = src_end == total;
                if run >= MIN_PART || last_part {
                    // Copy a range of this one source.
                    let len = run.min(COPY_PART);
                    let from = pos - offset_of(i);
                    let part = self
                        .client
                        .upload_part_copy()
                        .bucket(&self.bucket)
                        .key(dest)
                        .upload_id(&upload_id)
                        .part_number(part_number)
                        .copy_source(format!(
                            "{}/{}",
                            self.bucket,
                            crate::util::encode_path(&sources[i])
                        ))
                        .copy_source_range(format!("bytes={from}-{}", from + len - 1))
                        .send()
                        .await
                        .map_err(|e| {
                            StoreError::Other(anyhow::anyhow!("s3 upload part copy: {e}"))
                        })?;
                    let etag = part
                        .copy_part_result()
                        .and_then(|r| r.e_tag())
                        .unwrap_or("")
                        .to_owned();
                    parts.push(
                        aws_sdk_s3::types::CompletedPart::builder()
                            .e_tag(etag)
                            .part_number(part_number)
                            .build(),
                    );
                    pos += len;
                } else {
                    // Too small to copy on its own: read MIN_PART bytes across source boundaries.
                    let want = MIN_PART.min(total - pos);
                    let mut buf = Vec::with_capacity(want as usize);
                    let mut p = pos;
                    while (buf.len() as u64) < want {
                        let j = (0..sources.len())
                            .find(|&j| p < offset_of(j) + sizes[j])
                            .unwrap();
                        let from = p - offset_of(j);
                        let take = (sizes[j] - from).min(want - buf.len() as u64);
                        let (_, bytes) = self
                            .get(
                                &sources[j],
                                GetOptions {
                                    range: Some(from..from + take),
                                    ..GetOptions::default()
                                },
                            )
                            .await?
                            .bytes()
                            .await?
                            .ok_or_else(|| StoreError::NotFound {
                                key: sources[j].clone(),
                            })?;
                        buf.extend_from_slice(&bytes);
                        p += take;
                    }
                    let len = buf.len() as u64;
                    let part = self
                        .client
                        .upload_part()
                        .bucket(&self.bucket)
                        .key(dest)
                        .upload_id(&upload_id)
                        .part_number(part_number)
                        .body(S3ByteStream::from(Bytes::from(buf)))
                        .content_length(len as i64)
                        .send()
                        .await
                        .map_err(|e| StoreError::Other(anyhow::anyhow!("s3 upload part: {e}")))?;
                    parts.push(
                        aws_sdk_s3::types::CompletedPart::builder()
                            .e_tag(part.e_tag().unwrap_or("").to_owned())
                            .part_number(part_number)
                            .build(),
                    );
                    pos += len;
                }
                part_number += 1;
            }
            Ok(())
        }
        .await;
        if let Err(e) = result {
            let _ = self.abort_multipart(dest, &upload_id).await;
            return Err(e);
        }
        let completed = aws_sdk_s3::types::CompletedMultipartUpload::builder()
            .set_parts(Some(parts))
            .build();
        let mut complete = self
            .client
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(dest)
            .upload_id(&upload_id)
            .multipart_upload(completed);
        if let PutMode::Create = &opts.mode {
            complete = complete.if_none_match("*");
        }
        if let PutMode::Update(v) = &opts.mode {
            complete = complete.if_match(v.as_str());
        }
        let resp = match complete.send().await {
            Ok(r) => r,
            Err(e) => {
                let _ = self.abort_multipart(dest, &upload_id).await;
                let code = err_code(&e).unwrap_or("");
                if matches!(code, "PreconditionFailed" | "ConditionalRequestConflict") {
                    return Err(StoreError::PreconditionFailed {
                        key: dest.to_owned(),
                        current: self.head(dest).await.ok().flatten().map(|m| m.version),
                    });
                }
                return Err(StoreError::Other(anyhow::anyhow!(
                    "s3 complete multipart: {e}"
                )));
            }
        };
        let etag = resp.e_tag().map(|s| s.trim_matches('"').to_owned());
        Ok(ObjectMeta {
            key: dest.into(),
            size: total,
            version: Version::new(etag.as_deref().unwrap_or("")),
        })
    }

    async fn signed_get_url(&self, key: &str, ttl: Duration) -> Result<Option<String>> {
        let presigning = PresigningConfig::expires_in(ttl)
            .map_err(|e| StoreError::other(anyhow::anyhow!("presigning config: {e}")))?;
        let presigned = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .presigned(presigning)
            .await
            .map_err(|e| StoreError::other(anyhow::anyhow!("presigning: {e}")))?;
        Ok(Some(presigned.uri().to_owned()))
    }
}

/// State for the lazy list stream.
struct ListState {
    client: S3Client,
    bucket: String,
    prefix: String,
    start_after: Option<String>,
    continuation_token: Option<String>,
    started: bool,
    buffer: std::vec::IntoIter<Result<ObjectMeta>>,
}

// ---- multipart upload ----------------------------------------------------

impl S3Store {
    async fn multipart_put(
        &self,
        key: &str,
        body: S3ByteStream,
        len: u64,
        opts: &PutOptions,
    ) -> Result<ObjectMeta> {
        let mut create = self
            .client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(key);

        if let Some(ct) = opts.content_type {
            create = create.content_type(ct);
        }

        let upload = create
            .send()
            .await
            .map_err(|e| StoreError::Other(anyhow::anyhow!("s3 create multipart: {e}")))?;

        let upload_id = upload
            .upload_id()
            .ok_or_else(|| {
                StoreError::other(anyhow::anyhow!("no upload_id from CreateMultipartUpload"))
            })?
            .to_owned();

        let part_size = self.multipart_part_size;
        let mut part_number = 1i32;
        let mut uploaded_parts: Vec<aws_sdk_s3::types::CompletedPart> = Vec::new();
        let mut remaining = len;

        use tokio::io::AsyncReadExt;
        let mut reader = body.into_async_read();

        while remaining > 0 {
            let this_part = part_size.min(remaining);
            let to_read = this_part as usize;
            let mut buf = vec![0u8; to_read];
            let mut read_total = 0;

            while read_total < to_read {
                let n = match reader.read(&mut buf[read_total..]).await {
                    Ok(n) => n,
                    Err(e) => {
                        let _ = self.abort_multipart(key, &upload_id).await;
                        return Err(StoreError::other(anyhow::anyhow!("multipart read: {e}")));
                    }
                };
                if n == 0 {
                    break;
                }
                read_total += n;
            }

            if read_total == 0 {
                break;
            }
            buf.truncate(read_total);
            let actual = read_total as u64;

            let part = match self
                .client
                .upload_part()
                .bucket(&self.bucket)
                .key(key)
                .upload_id(&upload_id)
                .part_number(part_number)
                .body(S3ByteStream::from(Bytes::from(buf)))
                .content_length(actual as i64)
                .send()
                .await
            {
                Ok(p) => p,
                Err(e) => {
                    let _ = self.abort_multipart(key, &upload_id).await;
                    return Err(StoreError::Other(anyhow::anyhow!("s3 upload part: {e}")));
                }
            };

            let etag = part.e_tag().unwrap_or("").to_owned();
            uploaded_parts.push(
                aws_sdk_s3::types::CompletedPart::builder()
                    .e_tag(etag)
                    .part_number(part_number)
                    .build(),
            );

            remaining -= actual;
            part_number += 1;
        }

        let completed = aws_sdk_s3::types::CompletedMultipartUpload::builder()
            .set_parts(Some(uploaded_parts))
            .build();

        let resp = match self
            .client
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .upload_id(&upload_id)
            .multipart_upload(completed)
            .send()
            .await
        {
            Ok(r) => r,
            Err(e) => {
                let _ = self.abort_multipart(key, &upload_id).await;
                return Err(StoreError::Other(anyhow::anyhow!(
                    "s3 complete multipart: {e}"
                )));
            }
        };

        let etag = resp.e_tag().map(|s| s.trim_matches('"').to_owned());
        Ok(ObjectMeta {
            key: key.into(),
            size: len,
            version: Version::new(etag.as_deref().unwrap_or("")),
        })
    }

    async fn abort_multipart(&self, key: &str, upload_id: &str) -> Result<()> {
        self.client
            .abort_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .upload_id(upload_id)
            .send()
            .await
            .map_err(|e| StoreError::other(anyhow::anyhow!("abort multipart: {e}")))?;
        Ok(())
    }
}

fn static_credentials(
    access_key: &str,
    secret_key: &str,
    session_token: Option<String>,
) -> Credentials {
    Credentials::new(access_key, secret_key, session_token, None, "walgit-static")
}

// ---- rustfs compatibility notes (integration testing) -------------------
//
// 1. Presigned URLs: rustfs honors SigV4 presigned GET URLs with conditional
//    headers (If-None-Match, If-Match, Range) in SignedHeaders.
// 2. If-None-Match: * on PUT: 412 "PreconditionFailed" when object exists.
// 3. If-Match: <etag> on PUT: 412 when ETag mismatch.
// 4. 304 Not Modified: HTTP 304 with ETag header, empty body.
// 5. ListObjectsV2: StartAfter, ContinuationToken, IsTruncated/NextToken OK.
// 6. DeleteObject: idempotent for absent keys (204).
// 7. Multipart: CreateMultipartUpload + UploadPart + CompleteMultipartUpload
//    supported. Conditional Create/Update preconditions are applied at
//    CompleteMultipartUpload; providers that reject them must fail closed.
// 8. ETags: quoted, MD5 for single-PUT, compound for multipart. Quotes
//    stripped consistently in our Version.
// 9. force_path_style: required for rustfs local dev.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn static_credentials_include_session_token_when_present() {
        let creds = static_credentials("access", "secret", Some("session".into()));

        assert_eq!(creds.access_key_id(), "access");
        assert_eq!(creds.secret_access_key(), "secret");
        assert_eq!(creds.session_token(), Some("session"));
    }

    #[test]
    fn static_credentials_work_without_session_token() {
        let creds = static_credentials("access", "secret", None);

        assert_eq!(creds.access_key_id(), "access");
        assert_eq!(creds.secret_access_key(), "secret");
        assert_eq!(creds.session_token(), None);
    }
}
