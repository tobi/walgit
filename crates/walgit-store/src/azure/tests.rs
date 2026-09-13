#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use super::*;
use azure_core::credentials::AccessToken;
use azure_core::http::headers::Headers;
use azure_core::http::{AsyncRawResponse, HttpClient, RetryOptions, Transport};
use azure_core::time::{Duration, OffsetDateTime};
use futures::future::{BoxFuture, FutureExt};
use std::collections::HashMap;
use std::fmt;
use std::fmt::Write;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Barrier;

// The SDK owns auth, retries and serialization in these tests. Only the HTTP
// transport is substituted, so the actual requests can be asserted precisely.
struct FakeClient {
    calls: Arc<Mutex<Vec<Request>>>,
    reply: Box<
        dyn Fn(Request) -> BoxFuture<'static, azure_core::Result<AsyncRawResponse>> + Send + Sync,
    >,
}
impl fmt::Debug for FakeClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("FakeClient")
    }
}
#[async_trait]
impl HttpClient for FakeClient {
    async fn execute_request(&self, request: &Request) -> azure_core::Result<AsyncRawResponse> {
        self.calls.lock().unwrap().push(request.clone());
        (self.reply)(request.clone()).await
    }
}

fn fixture<F, Fut>(
    reply: F,
    credential: Option<Arc<dyn TokenCredential>>,
) -> (AzureStore, Arc<Mutex<Vec<Request>>>)
where
    F: Fn(Request) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = azure_core::Result<AsyncRawResponse>> + Send + 'static,
{
    let calls = Arc::new(Mutex::new(Vec::new()));
    let client = FakeClient {
        calls: calls.clone(),
        reply: Box::new(move |r| reply(r).boxed()),
    };
    let cfg = StoreConfig {
        bucket: "container".into(),
        multipart_threshold: bytesize::ByteSize::b(0),
        multipart_part_size: bytesize::ByteSize::b(4),
        azure: walgit_config::AzureConfig {
            account: "account".into(),
            max_concurrent_blocks: 2,
            ..Default::default()
        },
        ..Default::default()
    };
    let mut url = Url::parse("https://account.blob.core.windows.net/container").unwrap();
    if credential.is_none() {
        url.set_query(Some("sig=synthetic-sas"));
    }
    let store = AzureStore::with_client_options(
        &cfg,
        url,
        credential,
        ClientOptions {
            transport: Some(Transport::new(Arc::new(client))),
            retry: RetryOptions::none(),
            ..Default::default()
        },
    )
    .unwrap();
    (store, calls)
}

fn response(
    status: StatusCode,
    body: impl Into<Bytes>,
    headers: &[(&'static str, &str)],
) -> AsyncRawResponse {
    let mut h = Headers::new();
    for &(key, value) in headers {
        h.insert(HeaderName::from_static(key), value.to_owned());
    }
    AsyncRawResponse::from_bytes(status, h, body)
}
fn ok() -> AsyncRawResponse {
    response(
        StatusCode::Created,
        Bytes::new(),
        &[("etag", "\"new-version\"")],
    )
}
fn header<'a>(r: &'a Request, name: &'static str) -> Option<&'a str> {
    r.headers().get_optional_str(&HeaderName::from_static(name))
}
fn query(r: &Request, name: &str) -> Option<String> {
    r.url()
        .query_pairs()
        .find(|(k, _)| k == name)
        .map(|(_, v)| v.into_owned())
}

#[test]
fn endpoint_paths_and_sas_are_preserved_without_interpreting_object_keys_as_urls() {
    let mut cfg = StoreConfig {
        bucket: "container".into(),
        ..Default::default()
    };
    for endpoint in [
        "http://localhost:10000/account",
        "http://localhost:10000/account/",
    ] {
        cfg.azure.endpoint = endpoint.into();
        let url = container_url(&cfg, Some("?sig=a%2Bb&sp=rw")).unwrap();
        assert_eq!(url.path(), "/account/container");
        let client = BlobContainerClient::new(url, None, None).unwrap();
        let blob = client.blob_client("repo/space & ?#%.pack");
        assert_eq!(
            blob.url().path(),
            "/account/container/repo%2Fspace%20&%20%3F%23%25.pack"
        );
        assert_eq!(blob.url().query(), Some("sig=a%2Bb&sp=rw"));
    }
    cfg.azure.endpoint = "https://example.test/?sig=secret-value".into();
    assert!(
        !container_url(&cfg, None)
            .unwrap_err()
            .to_string()
            .contains("secret-value")
    );
}

#[tokio::test]
async fn one_range_get_reports_total_size_and_does_not_prebuffer_the_body() {
    let polls = Arc::new(AtomicUsize::new(0));
    let tracked = polls.clone();
    let (store, calls) = fixture(
        move |r| {
            assert_eq!(header(&r, "range"), Some("bytes=5-7"));
            assert_eq!(header(&r, "if-match"), Some("\"version\""));
            let polls = tracked.clone();
            async move {
                let mut headers = Headers::new();
                headers.insert("etag", "\"version\"");
                headers.insert("content-range", "bytes 5-7/100");
                headers.insert("content-length", "3");
                Ok(AsyncRawResponse::new(
                    StatusCode::PartialContent,
                    headers,
                    futures::stream::once(async move {
                        polls.fetch_add(1, Ordering::SeqCst);
                        Ok(Bytes::from_static(b"abc"))
                    })
                    .boxed(),
                ))
            }
        },
        None,
    );
    let result = store
        .get(
            "key",
            GetOptions {
                range: Some(5..8),
                if_match: Some(Version::new("\"version\"")),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    let (meta, bytes) = result.bytes().await.unwrap().unwrap();
    assert_eq!(meta.size, 100);
    assert_eq!(&bytes[..], b"abc");
    assert_eq!(calls.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn malformed_lengths_and_source_errors_never_commit() {
    let (store, calls) = fixture(|_| async { Ok(ok()) }, None);
    for (len, body) in [(9, b"12345678".as_slice()), (7, b"12345678"), (0, b"x")] {
        assert!(
            store
                .put(
                    "key",
                    PutBody::Stream {
                        len,
                        stream: util::once(Bytes::copy_from_slice(body))
                    },
                    PutMode::Create.into()
                )
                .await
                .is_err()
        );
    }
    let stream = futures::stream::iter([
        Ok(Bytes::from_static(b"1234")),
        Err(StoreError::InvalidArgument("source failed".into())),
    ])
    .boxed();
    assert!(
        store
            .put(
                "key",
                PutBody::Stream { len: 8, stream },
                PutMode::Create.into()
            )
            .await
            .is_err()
    );
    assert!(
        calls
            .lock()
            .unwrap()
            .iter()
            .all(|r| query(r, "comp").as_deref() == Some("block"))
    );
}

#[tokio::test]
async fn chunk_reader_applies_backpressure_and_obeys_the_declared_length() {
    let reads = Arc::new(AtomicUsize::new(0));
    let count = reads.clone();
    let input = futures::stream::iter(0..100)
        .map(move |_| {
            count.fetch_add(1, Ordering::SeqCst);
            Ok(Bytes::from_static(b"data"))
        })
        .boxed();
    let mut chunks = UploadChunks {
        stream: input,
        remaining: 400,
        pending: Bytes::new(),
        part: 8,
    };
    assert_eq!(chunks.next().await.unwrap().unwrap(), "datadata");
    assert_eq!(
        reads.load(Ordering::SeqCst),
        2,
        "must not drain input into a Vec"
    );
    let mut collected = 8;
    while let Some(chunk) = chunks.next().await.unwrap() {
        assert!(chunk.len() <= 8);
        collected += chunk.len();
    }
    assert_eq!(collected, 400);
}

#[derive(Default)]
struct Blocks {
    staged: HashMap<String, Bytes>,
    committed: Option<Vec<u8>>,
}
#[derive(Deserialize)]
struct CommitBody {
    #[serde(rename = "Latest")]
    ids: Vec<String>,
}

#[tokio::test]
async fn competing_staged_creates_cannot_mix_each_others_blocks() {
    let state = Arc::new(Mutex::new(Blocks::default()));
    let captured = state.clone();
    let staged = Arc::new(Barrier::new(4));
    let (store, calls) = fixture(
        move |r| {
            let state = captured.clone();
            let staged = staged.clone();
            async move {
                if query(&r, "comp").as_deref() == Some("block") {
                    state
                        .lock()
                        .unwrap()
                        .staged
                        .insert(query(&r, "blockid").unwrap(), Bytes::from(r.body()));
                    // Neither writer can commit until BOTH writers have staged all their parts.
                    staged.wait().await;
                    Ok(ok())
                } else {
                    assert_eq!(header(&r, "if-none-match"), Some("*"));
                    let mut state = state.lock().unwrap();
                    if state.committed.is_some() {
                        return Ok(response(StatusCode::PreconditionFailed, Bytes::new(), &[]));
                    }
                    let body = Bytes::from(r.body());
                    let commit: CommitBody = quick_xml::de::from_reader(body.as_ref()).unwrap();
                    state.committed = Some(
                        commit
                            .ids
                            .iter()
                            .flat_map(|id| state.staged.get(id).unwrap().iter().copied())
                            .collect(),
                    );
                    Ok(ok())
                }
            }
        },
        None,
    );
    let result = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        tokio::join!(
            store.put(
                "key",
                Bytes::from_static(b"AAAAAAAA").into(),
                PutMode::Create.into()
            ),
            store.put(
                "key",
                Bytes::from_static(b"BBBBBBBB").into(),
                PutMode::Create.into()
            )
        )
    })
    .await
    .unwrap();
    assert_ne!(result.0.is_ok(), result.1.is_ok());
    let winner = if result.0.is_ok() {
        b"AAAAAAAA"
    } else {
        b"BBBBBBBB"
    };
    assert_eq!(state.lock().unwrap().committed.as_deref().unwrap(), winner);
    assert_eq!(
        state.lock().unwrap().staged.len(),
        4,
        "each upload needs its own block namespace"
    );
    assert_eq!(
        calls.lock().unwrap().len(),
        6,
        "two blocks and one conditional commit per writer"
    );
}

#[derive(Debug)]
struct RotatingCredential(AtomicUsize);
#[async_trait]
impl TokenCredential for RotatingCredential {
    async fn get_token(
        &self,
        scopes: &[&str],
        _: Option<TokenRequestOptions<'_>>,
    ) -> azure_core::Result<AccessToken> {
        assert_eq!(scopes, &[STORAGE_SCOPE]);
        let n = self.0.fetch_add(1, Ordering::SeqCst) + 1;
        let expiry = if n == 1 {
            OffsetDateTime::now_utc() - Duration::seconds(1)
        } else {
            OffsetDateTime::now_utc() + Duration::hours(1)
        };
        Ok(AccessToken::new(format!("synthetic-token-{n}"), expiry))
    }
}

#[tokio::test]
async fn compose_pins_ranges_and_authenticates_private_sources_with_refreshed_credentials() {
    let credential = Arc::new(RotatingCredential(AtomicUsize::new(0)));
    let (store, calls) = fixture(
        |r| async move {
            if r.method() == Method::Head {
                Ok(response(
                    StatusCode::Ok,
                    Bytes::new(),
                    &[("content-length", "9"), ("etag", "\"source-version\"")],
                ))
            } else {
                Ok(ok())
            }
        },
        Some(credential.clone()),
    );
    let result = store
        .compose("dest", &["repo/source ?#".into()], PutMode::Create.into())
        .await
        .unwrap();
    assert_eq!(result.size, 9);
    let calls = calls.lock().unwrap();
    assert_eq!(calls.len(), 5, "one source HEAD, three copies, one commit");
    assert_eq!(
        header(&calls[0], "authorization"),
        Some("Bearer synthetic-token-1")
    );
    let mut ranges = Vec::new();
    for call in calls
        .iter()
        .filter(|r| header(r, "x-ms-copy-source").is_some())
    {
        assert_eq!(header(call, "content-length"), Some("0"));
        assert_eq!(
            header(call, "x-ms-source-if-match"),
            Some("\"source-version\"")
        );
        assert_eq!(
            header(call, "x-ms-copy-source-authorization"),
            Some("Bearer synthetic-token-2")
        );
        assert_eq!(
            header(call, "authorization"),
            Some("Bearer synthetic-token-2")
        );
        let source = Url::parse(header(call, "x-ms-copy-source").unwrap()).unwrap();
        assert_eq!(source.query(), None);
        assert!(source.path().ends_with("repo%2Fsource%20%3F%23"));
        ranges.push(header(call, "x-ms-source-range").unwrap().to_owned());
    }
    ranges.sort();
    assert_eq!(ranges, ["bytes=0-3", "bytes=4-7", "bytes=8-8"]);
    assert_eq!(credential.0.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn copy_failure_does_not_commit_a_partial_destination() {
    let (store, calls) = fixture(
        |r| async move {
            if r.method() == Method::Head {
                Ok(response(
                    StatusCode::Ok,
                    Bytes::new(),
                    &[("content-length", "9"), ("etag", "\"source-version\"")],
                ))
            } else {
                Ok(response(StatusCode::PreconditionFailed, Bytes::new(), &[]))
            }
        },
        None,
    );
    assert!(
        store
            .compose("dest", &["source".into()], PutMode::Overwrite.into())
            .await
            .unwrap_err()
            .is_precondition_failed()
    );
    assert!(
        calls
            .lock()
            .unwrap()
            .iter()
            .all(|r| query(r, "comp").as_deref() != Some("blocklist"))
    );
}

#[tokio::test]
async fn delimited_listing_paginates_and_decodes_escaped_names() {
    let (store, calls) = fixture(
        |r| async move {
            assert_eq!(query(&r, "delimiter").as_deref(), Some("/"));
            assert_eq!(query(&r, "prefix").as_deref(), Some("repos/"));
            let body = if query(&r, "marker").is_none() {
                "<EnumerationResults><Blobs><BlobPrefix><Name>repos/a&amp;b/</Name></BlobPrefix></Blobs><NextMarker>a&amp;b</NextMarker></EnumerationResults>"
            } else {
                assert_eq!(query(&r, "marker").as_deref(), Some("a&b"));
                "<EnumerationResults><Blobs><BlobPrefix><Name>repos/z/</Name></BlobPrefix></Blobs><NextMarker/></EnumerationResults>"
            };
            Ok(response(StatusCode::Ok, body, &[]))
        },
        None,
    );
    assert_eq!(
        store.list_prefixes("repos/").await.unwrap(),
        ["repos/a&b/", "repos/z/"]
    );
    assert_eq!(calls.lock().unwrap().len(), 2);
    assert!(parse_blob_prefixes(b"<Error><Code>Nope</Code></Error>").is_err());
}

#[tokio::test]
async fn listing_errors_and_non_cas_conflicts_are_not_hidden() {
    let (store, _) = fixture(
        |_| async {
            Ok(response(
                StatusCode::Conflict,
                "<Error><Code>ContainerBeingDeleted</Code></Error>",
                &[("x-ms-error-code", "ContainerBeingDeleted")],
            ))
        },
        None,
    );
    let error = store.list("", None).next().await.unwrap().unwrap_err();
    assert!(!error.is_precondition_failed());
    assert!(
        error.is_retryable(),
        "a container mid-delete resolves on its own"
    );
    assert!(store.list_prefixes("").await.is_err());
    let lease = azure_core::Error::new(
        ErrorKind::HttpResponse {
            status: StatusCode::Conflict,
            error_code: Some("LeaseIdMissing".into()),
            raw_response: None,
        },
        "lease",
    );
    let mapped = map_error("key", &lease);
    assert!(!mapped.is_retryable() && !mapped.is_precondition_failed());
    let error = azure_core::Error::with_message(ErrorKind::Io, "https://example/?sig=SECRET");
    let mapped = map_error("key", &error);
    assert!(mapped.is_retryable());
    assert!(!mapped.to_string().contains("SECRET"));
    assert!(version_of(None).is_err());
    assert!(required_size(None).is_err());
}

#[tokio::test]
async fn small_conditional_upload_is_one_request_with_immutable_metadata() {
    let (mut store, calls) = fixture(|_| async { Ok(ok()) }, None);
    store.multipart_threshold = 64;
    store
        .put(
            "key",
            Bytes::from_static(b"bytes").into(),
            PutOptions {
                mode: PutMode::Update(Version::new("\"old-version\"")),
                content_type: Some("application/octet-stream"),
                immutable: true,
            },
        )
        .await
        .unwrap();
    let calls = calls.lock().unwrap();
    assert_eq!(calls.len(), 1);
    assert!(query(&calls[0], "comp").is_none());
    assert_eq!(header(&calls[0], "if-match"), Some("\"old-version\""));
    assert_eq!(
        header(&calls[0], "x-ms-blob-cache-control"),
        Some("public, max-age=31536000, immutable")
    );
    assert_eq!(
        header(&calls[0], "x-ms-blob-content-type"),
        Some("application/octet-stream")
    );
}

#[tokio::test]
async fn flat_listing_follows_markers_and_excludes_every_key_at_or_before_start_after() {
    let (store, calls) = fixture(
        |r| async move {
            assert_eq!(query(&r, "startFrom").as_deref(), Some("b"));
            let (names, next) = if query(&r, "marker").is_none() {
                (vec!["a", "b"], "page2")
            } else {
                (vec!["c", "d"], "")
            };
            let mut blobs = String::new();
            for name in names {
                write!(blobs, "<Blob><Name>{name}</Name><Properties><Content-Length>1</Content-Length><Etag>version</Etag><BlobType>BlockBlob</BlobType></Properties></Blob>").unwrap();
            }
            Ok(response(
                StatusCode::Ok,
                format!(
                    "<EnumerationResults><Blobs>{blobs}</Blobs><NextMarker>{next}</NextMarker></EnumerationResults>"
                ),
                &[],
            ))
        },
        None,
    );
    let objects = store
        .list("", Some("b"))
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(
        objects.iter().map(|o| o.key.as_str()).collect::<Vec<_>>(),
        ["c", "d"]
    );
    assert_eq!(calls.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn conditional_delete_only_probes_absence_after_precondition_failure() {
    let (store, calls) = fixture(
        |r| async move {
            match r.method() {
                Method::Delete => Ok(response(StatusCode::PreconditionFailed, Bytes::new(), &[])),
                Method::Head => Ok(response(StatusCode::NotFound, Bytes::new(), &[])),
                _ => panic!("unexpected request"),
            }
        },
        None,
    );
    assert!(
        store
            .delete("key", Some(Version::new("old")))
            .await
            .unwrap_err()
            .is_not_found()
    );
    let calls = calls.lock().unwrap();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0].method(), Method::Delete);
    assert_eq!(calls[1].method(), Method::Head);
}

#[tokio::test]
async fn small_file_and_stream_bodies_are_one_request() {
    let (mut store, calls) = fixture(|_| async { Ok(ok()) }, None);
    store.multipart_threshold = 64;
    let file = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(file.path(), b"pack bytes").unwrap();
    store
        .put(
            "packs/small",
            PutBody::File(file.path().into()),
            PutMode::Create.into(),
        )
        .await
        .unwrap();
    store
        .put(
            "leases/small",
            PutBody::Stream {
                len: 5,
                stream: util::once(Bytes::from_static(b"lease")),
            },
            PutMode::Overwrite.into(),
        )
        .await
        .unwrap();
    {
        let calls = calls.lock().unwrap();
        assert_eq!(calls.len(), 2, "one Put Blob per small body, no staging");
        assert!(calls.iter().all(|r| query(r, "comp").is_none()));
        assert_eq!(header(&calls[0], "if-none-match"), Some("*"));
        assert_eq!(Bytes::from(calls[0].body()), "pack bytes");
        assert_eq!(header(&calls[0], "content-length"), Some("10"));
    }
    // A declared length the stream does not deliver never reaches the service.
    assert!(
        store
            .put(
                "leases/short",
                PutBody::Stream {
                    len: 9,
                    stream: util::once(Bytes::from_static(b"lease")),
                },
                PutMode::Overwrite.into(),
            )
            .await
            .is_err()
    );
    assert_eq!(calls.lock().unwrap().len(), 2);
    // Above the threshold a file is still staged and committed.
    std::fs::write(file.path(), vec![b'x'; 65]).unwrap();
    store
        .put(
            "packs/large",
            PutBody::File(file.path().into()),
            PutMode::Create.into(),
        )
        .await
        .unwrap();
    let calls = calls.lock().unwrap();
    assert_eq!(
        query(calls.last().unwrap(), "comp").as_deref(),
        Some("blocklist")
    );
}

#[derive(Debug)]
struct FixedCredential;
#[async_trait]
impl TokenCredential for FixedCredential {
    async fn get_token(
        &self,
        _: &[&str],
        _: Option<TokenRequestOptions<'_>>,
    ) -> azure_core::Result<AccessToken> {
        Ok(AccessToken::new(
            "synthetic-token",
            OffsetDateTime::now_utc() + Duration::hours(1),
        ))
    }
}

const KEY_VALUE: &str = "c3ludGhldGljLWRlbGVnYXRpb24ta2V5"; // "synthetic-delegation-key"

fn delegation_key_xml(expiry: &str) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"utf-8\"?><UserDelegationKey>\
         <SignedOid>oid-1</SignedOid><SignedTid>tid-1</SignedTid>\
         <SignedStart>2026-01-01T00:00:00Z</SignedStart><SignedExpiry>{expiry}</SignedExpiry>\
         <SignedService>b</SignedService><SignedVersion>2025-01-05</SignedVersion>\
         <Value>{KEY_VALUE}</Value></UserDelegationKey>"
    )
}

#[tokio::test]
async fn signed_urls_are_user_delegation_sas_from_one_cached_key() {
    let key_expiry = sas_time(OffsetDateTime::now_utc() + Duration::hours(1));
    let (store, calls) = fixture(
        move |r| {
            let body = delegation_key_xml(&key_expiry);
            async move {
                assert_eq!(r.method(), Method::Post);
                assert_eq!(query(&r, "comp").as_deref(), Some("userdelegationkey"));
                assert_eq!(r.url().path(), "/");
                assert_eq!(header(&r, "authorization"), Some("Bearer synthetic-token"));
                let key_info = String::from_utf8(Bytes::from(r.body()).to_vec()).unwrap();
                assert!(key_info.contains("<KeyInfo><Start>") && key_info.contains("Z</Expiry>"));
                Ok(response(StatusCode::Ok, body, &[]))
            }
        },
        Some(Arc::new(FixedCredential)),
    );
    let ttl = std::time::Duration::from_mins(30);
    let url = store
        .signed_get_url("repos/o/r ?#/bundle.pack", ttl)
        .await
        .unwrap()
        .unwrap();
    let url = Url::parse(&url).unwrap();
    assert_eq!(url.scheme(), "https");
    assert_eq!(
        url.path(),
        "/container/repos%2Fo%2Fr%20%3F%23%2Fbundle.pack"
    );
    let q: HashMap<String, String> = url.query_pairs().into_owned().collect();
    assert_eq!(q["sv"], "2020-12-06");
    assert_eq!(q["sr"], "b");
    assert_eq!(q["sp"], "r");
    assert_eq!(q["spr"], "https");
    assert_eq!(q["skoid"], "oid-1");
    assert_eq!(q["sktid"], "tid-1");
    assert_eq!(q["skt"], "2026-01-01T00:00:00Z");
    assert_eq!(q["sks"], "b");
    assert_eq!(q["skv"], "2025-01-05");
    assert!(q["st"].ends_with('Z') && q["se"].ends_with('Z') && q["st"] < q["se"]);
    // The 2020-12-06 string-to-sign, written out line by line so the layout is
    // pinned here rather than shared with the implementation.
    let string_to_sign = format!(
        "r\n{st}\n{se}\n/blob/account/container/repos/o/r ?#/bundle.pack\n\
         oid-1\ntid-1\n2026-01-01T00:00:00Z\n{ske}\nb\n2025-01-05\n\n\n\n\nhttps\n2020-12-06\nb\n\n\n\n\n\n\n",
        st = q["st"],
        se = q["se"],
        ske = q["ske"],
    );
    let mut mac = Hmac::<Sha256>::new_from_slice(b"synthetic-delegation-key").unwrap();
    mac.update(string_to_sign.as_bytes());
    let expected = base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes());
    assert_eq!(q["sig"], expected);
    assert!(!url.as_str().contains(KEY_VALUE));

    // The key is reused while it covers the URL, and replaced once it cannot.
    store.signed_get_url("other", ttl).await.unwrap().unwrap();
    assert_eq!(calls.lock().unwrap().len(), 1);
    store
        .signed_get_url("other", std::time::Duration::from_hours(2))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(calls.lock().unwrap().len(), 2);
    for ttl in [
        std::time::Duration::ZERO,
        std::time::Duration::from_hours(8 * 24),
    ] {
        assert!(matches!(
            store.signed_get_url("other", ttl).await.unwrap_err(),
            StoreError::InvalidArgument(_)
        ));
    }
    assert_eq!(calls.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn signing_is_off_under_sas_auth_or_without_an_account() {
    let (store, calls) = fixture(|_| async { panic!("no request expected") }, None);
    assert!(
        store
            .signed_get_url("key", std::time::Duration::from_mins(1))
            .await
            .unwrap()
            .is_none()
    );
    let cfg = StoreConfig {
        bucket: "container".into(),
        azure: walgit_config::AzureConfig {
            endpoint: "https://blobs.example.test".into(),
            ..Default::default()
        },
        ..Default::default()
    };
    let store = AzureStore::with_client_options(
        &cfg,
        container_url(&cfg, None).unwrap(),
        Some(Arc::new(FixedCredential)),
        ClientOptions::default(),
    )
    .unwrap();
    assert!(
        store.signing.is_none(),
        "a custom endpoint hides the account name"
    );
    assert!(calls.lock().unwrap().is_empty());
}

#[test]
fn a_service_principal_without_its_variables_is_named_not_guessed() {
    // The harness clears no environment, so only the unset case is portable;
    // a set variable would leak between parallel tests.
    if std::env::var_os("AZURE_TENANT_ID").is_some() {
        return;
    }
    let error = credential(AzureCredential::ClientSecret).unwrap_err();
    assert_eq!(error.to_string(), "azure: AZURE_TENANT_ID is not set");
}

#[tokio::test]
async fn the_edge_gets_a_credentialed_url_and_no_header_under_either_auth() {
    let key_expiry = sas_time(OffsetDateTime::now_utc() + Duration::hours(2));
    let (store, _) = fixture(
        move |_| {
            let body = delegation_key_xml(&key_expiry);
            async move { Ok(response(StatusCode::Ok, body, &[])) }
        },
        Some(Arc::new(FixedCredential)),
    );
    let target = store.accel_target("repos/o/r/bundle").await.unwrap();
    assert!(target.authorization.is_none());
    let url = Url::parse(&target.url).unwrap();
    assert_eq!(url.path(), "/container/repos%2Fo%2Fr%2Fbundle");
    let q: HashMap<String, String> = url.query_pairs().into_owned().collect();
    assert_eq!(q["sp"], "r");
    assert!(q.contains_key("sig"));

    let (store, calls) = fixture(|_| async { panic!("no request expected") }, None);
    let target = store.accel_target("repos/o/r/bundle").await.unwrap();
    assert!(target.authorization.is_none());
    assert_eq!(
        target.url,
        "https://account.blob.core.windows.net/container/repos%2Fo%2Fr%2Fbundle?sig=synthetic-sas"
    );
    assert!(calls.lock().unwrap().is_empty());
}
