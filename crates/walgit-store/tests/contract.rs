//! Backend-agnostic contract suite for `ObjectStore`.
//!
//! `run_contract(store, prefix)` exercises every observable guarantee of the
//! trait: CAS create/update semantics, conditional GET (304 / 412), range
//! reads, head, delete (conditional + idempotent), list ordering and prefix
//! isolation, large streamed put/get roundtrip with checksum, and the
//! multipart upload path.
//!
//! The suite is executed against `MemoryStore` always, and against `S3Store`
//! when `WALGIT_TEST_S3_ENDPOINT` is set. `GcsStore` is tested when
//! `WALGIT_TEST_GCS_BUCKET` is set (StoreGcs adds that wrapper), and
//! `AzureStore` when `WALGIT_TEST_AZURE_ACCOUNT` is set (`just test-azure`).

use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use futures::StreamExt;
use walgit_store::{
    DynStore, GetOptions, GetResult, PutBody, PutMode, PutOptions, StoreError, memory::MemoryStore,
};

/// Run the full contract suite against `store` under `prefix`.
///
/// `prefix` should be unique per run (e.g. a random string) so concurrent
/// test runs don't collide on the same backend.
pub async fn run_contract(store: DynStore, prefix: &str) {
    let p = |k: &str| -> String {
        if prefix.is_empty() {
            k.to_owned()
        } else {
            format!("{prefix}/{k}")
        }
    };

    test_put_create_wins_once(&store, &p("concurrent")).await;
    test_update_cas(&store, &p("cas")).await;
    test_get_if_none_match(&store, &p("inm")).await;
    test_get_if_match_mismatch(&store, &p("im")).await;
    test_range_reads(&store, &p("range")).await;
    test_head_and_absent(&store, &p("head")).await;
    test_delete(&store, &p("del")).await;
    test_list(&store, &p("list")).await;
    test_large_streamed_roundtrip(&store, &p("large")).await;
    test_multipart_path(&store, &p("multi")).await;
    test_compose(&store, &p("compose")).await;
}

/// `compose`: a small header object followed by a body larger than S3's 5 MiB minimum
/// part size (the weekly-bundle shape: bundle header ∘ base pack), byte-exact, and
/// `PutMode::Create` refuses to overwrite. Backends without compose are skipped.
async fn test_compose(store: &DynStore, key: &str) {
    if !store.supports_compose() {
        eprintln!("skipping compose: not supported by {}", store.backend());
        return;
    }
    let header = Bytes::from_static(b"# v3 git bundle\n@object-format=sha1\n\n");
    let mut body = vec![0u8; 6 * 1024 * 1024 + 12345];
    for (i, b) in body.iter_mut().enumerate() {
        *b = (i % 251) as u8;
    }
    let body = Bytes::from(body);
    let h = format!("{key}.hdr");
    let b = format!("{key}.body");
    store
        .put(
            &h,
            PutBody::Bytes(header.clone()),
            PutOptions::from(PutMode::Overwrite),
        )
        .await
        .expect("put header");
    store
        .put(
            &b,
            PutBody::Bytes(body.clone()),
            PutOptions::from(PutMode::Overwrite),
        )
        .await
        .expect("put body");
    let meta = store
        .compose(
            key,
            &[h.clone(), b.clone()],
            PutOptions {
                mode: PutMode::Create,
                immutable: true,
                content_type: Some("application/x-git-bundle"),
            },
        )
        .await
        .expect("compose");
    assert_eq!(meta.size, (header.len() + body.len()) as u64);
    let r = store
        .get(key, GetOptions::default())
        .await
        .expect("get composed");
    let (m, got) = collect_body(r).await;
    assert_eq!(m.size, meta.size);
    assert_eq!(&got[..header.len()], &header[..]);
    assert_eq!(&got[header.len()..], &body[..], "composed body differs");
    // Create on an existing object is a precondition failure.
    let again = store
        .compose(
            key,
            &[h.clone(), b.clone()],
            PutOptions::from(PutMode::Create),
        )
        .await;
    assert!(
        matches!(again, Err(StoreError::PreconditionFailed { .. })),
        "{again:?}"
    );
    for k in [key, h.as_str(), b.as_str()] {
        let _ = store.delete(k, None).await;
    }
}

// ---- helpers -----------------------------------------------------------

/// Collect a GetResult body into Bytes, asserting it's an Object.
async fn collect_body(r: GetResult) -> (walgit_store::ObjectMeta, Bytes) {
    match r {
        GetResult::Object { meta, body } => {
            let collected = walgit_store::util::collect(body, meta.size as usize)
                .await
                .expect("body collect");
            (meta, collected)
        }
        GetResult::NotModified { .. } => panic!("expected Object, got NotModified"),
    }
}

/// Put bytes with a given mode.
async fn put_bytes(
    store: &DynStore,
    key: &str,
    data: impl Into<Bytes> + Send,
    mode: PutMode,
) -> walgit_store::ObjectMeta {
    store
        .put(
            key,
            PutBody::Bytes(data.into()),
            PutOptions {
                mode,
                ..Default::default()
            },
        )
        .await
        .expect("put")
}

// ---- individual tests --------------------------------------------------

/// 32 concurrent `PutMode::Create` tasks: exactly one wins.
async fn test_put_create_wins_once(store: &DynStore, key: &str) {
    // Clean slate.
    let _ = store.delete(key, None).await;

    let n = 32;
    let mut handles = Vec::with_capacity(n);
    for i in 0..n {
        let s = store.clone();
        let k = key.to_owned();
        handles.push(tokio::spawn(async move {
            let result = s
                .put(
                    &k,
                    PutBody::Bytes(Bytes::from(format!(" contender {i}"))),
                    PutOptions {
                        mode: PutMode::Create,
                        ..Default::default()
                    },
                )
                .await;
            result.is_ok()
        }));
    }

    let mut wins = 0u32;
    for h in handles {
        if h.await.expect("task join") {
            wins += 1;
        }
    }
    assert_eq!(wins, 1, "Create: exactly one of {n} should win, got {wins}");

    // Cleanup.
    let _ = store.delete(key, None).await;
}

/// Update CAS: winner updates, loser gets PreconditionFailed.
async fn test_update_cas(store: &DynStore, key: &str) {
    let _ = store.delete(key, None).await;

    // Initial put.
    let meta = put_bytes(store, key, b"v1".as_slice(), PutMode::Create).await;
    let v1 = meta.version;

    // Two concurrent updates from the same version.
    let s1 = store.clone();
    let s2 = store.clone();
    let k1 = key.to_owned();
    let k2 = key.to_owned();
    let v1c = v1.clone();
    let v1c2 = v1.clone();

    let (r1, r2) = tokio::join!(
        async {
            s1.put(
                &k1,
                PutBody::Bytes(Bytes::from("winner")),
                PutOptions {
                    mode: PutMode::Update(v1c),
                    ..Default::default()
                },
            )
            .await
        },
        async {
            s2.put(
                &k2,
                PutBody::Bytes(Bytes::from("loser")),
                PutOptions {
                    mode: PutMode::Update(v1c2),
                    ..Default::default()
                },
            )
            .await
        },
    );

    let wins = [r1.is_ok(), r2.is_ok()].iter().filter(|&&w| w).count();
    assert_eq!(wins, 1, "Update CAS: exactly one should win");

    // The loser should have PreconditionFailed.
    let loser_err = if r1.is_err() { r1 } else { r2 };
    if let Err(e) = loser_err {
        assert!(
            e.is_precondition_failed(),
            "loser should be PreconditionFailed, got {e:?}"
        );
    }

    // The winner's version should differ from v1.
    let meta2 = store.head(key).await.expect("head").expect("exists");
    assert_ne!(meta2.version, v1, "version should change after update");

    // Update with stale version fails.
    let stale = store
        .put(
            key,
            PutBody::Bytes(Bytes::from("stale")),
            PutOptions {
                mode: PutMode::Update(v1),
                ..Default::default()
            },
        )
        .await;
    assert!(stale.is_err(), "stale update should fail");
    assert!(
        stale.unwrap_err().is_precondition_failed(),
        "stale update should be PreconditionFailed"
    );

    let _ = store.delete(key, None).await;
}

/// if_none_match: NotModified when unchanged, Object when changed.
async fn test_get_if_none_match(store: &DynStore, key: &str) {
    let _ = store.delete(key, None).await;

    let meta = put_bytes(store, key, b"hello".as_slice(), PutMode::Create).await;
    let v = meta.version;

    // Same version → NotModified.
    let r = store
        .get(
            key,
            GetOptions {
                if_none_match: Some(v.clone()),
                ..Default::default()
            },
        )
        .await
        .expect("get if_none_match same");
    assert!(
        matches!(r, GetResult::NotModified { .. }),
        "should be NotModified"
    );

    // Different version → Object.
    let r = store
        .get(
            key,
            GetOptions {
                if_none_match: Some(walgit_store::Version::new("different")),
                ..Default::default()
            },
        )
        .await
        .expect("get if_none_match different");
    let (_, body) = collect_body(r).await;
    assert_eq!(&body[..], b"hello");

    let _ = store.delete(key, None).await;
}

/// if_match mismatch → PreconditionFailed.
async fn test_get_if_match_mismatch(store: &DynStore, key: &str) {
    let _ = store.delete(key, None).await;

    put_bytes(store, key, b"data".as_slice(), PutMode::Create).await;

    // Wrong if_match → PreconditionFailed.
    match store
        .get(
            key,
            GetOptions {
                if_match: Some(walgit_store::Version::new("wrong-etag")),
                ..Default::default()
            },
        )
        .await
    {
        Err(e) => {
            assert!(
                e.is_precondition_failed(),
                "if_match mismatch should be PreconditionFailed, got {e:?}"
            );
        }
        Ok(_) => panic!("if_match mismatch should fail, but got Ok"),
    }

    let _ = store.delete(key, None).await;
}

/// Range reads: start, middle, tail — exact bytes.
async fn test_range_reads(store: &DynStore, key: &str) {
    let _ = store.delete(key, None).await;

    let data: Vec<u8> = (0..255u8).collect();
    put_bytes(store, key, Bytes::from(data.clone()), PutMode::Create).await;

    // Start: bytes 0..10
    let r = store
        .get(
            key,
            GetOptions {
                range: Some(Range { start: 0, end: 10 }),
                ..Default::default()
            },
        )
        .await
        .expect("range start");
    let (_, body) = collect_body(r).await;
    assert_eq!(&body[..], &data[0..10], "range start mismatch");

    // Middle: bytes 100..150
    let r = store
        .get(
            key,
            GetOptions {
                range: Some(Range {
                    start: 100,
                    end: 150,
                }),
                ..Default::default()
            },
        )
        .await
        .expect("range middle");
    let (_, body) = collect_body(r).await;
    assert_eq!(&body[..], &data[100..150], "range middle mismatch");

    // Tail: bytes 200..255
    let r = store
        .get(
            key,
            GetOptions {
                range: Some(Range {
                    start: 200,
                    end: 255,
                }),
                ..Default::default()
            },
        )
        .await
        .expect("range tail");
    let (_, body) = collect_body(r).await;
    assert_eq!(&body[..], &data[200..255], "range tail mismatch");

    let _ = store.delete(key, None).await;
}

/// head: present and absent.
async fn test_head_and_absent(store: &DynStore, key: &str) {
    let _ = store.delete(key, None).await;

    // Absent.
    assert!(
        store.head(key).await.expect("head absent").is_none(),
        "should be absent"
    );

    // Present.
    let meta = put_bytes(store, key, b"head-test".as_slice(), PutMode::Create).await;
    let h = store
        .head(key)
        .await
        .expect("head present")
        .expect("exists");
    assert_eq!(h.size, 9);
    assert_eq!(h.version, meta.version);

    let _ = store.delete(key, None).await;
}

/// delete: unconditional idempotent + conditional.
async fn test_delete(store: &DynStore, key: &str) {
    let _ = store.delete(key, None).await;

    // Put then conditional delete with wrong version → fail.
    let meta = put_bytes(store, key, b"delete-me".as_slice(), PutMode::Create).await;
    let wrong = walgit_store::Version::new("wrong");
    let err = store.delete(key, Some(wrong)).await;
    assert!(err.is_err(), "delete with wrong version should fail");
    assert!(
        err.unwrap_err().is_precondition_failed(),
        "delete wrong version should be PreconditionFailed"
    );

    // Object still exists.
    assert!(
        store.head(key).await.expect("head").is_some(),
        "object should still exist"
    );

    // Conditional delete with correct version → ok.
    store
        .delete(key, Some(meta.version))
        .await
        .expect("delete with correct version");
    assert!(
        store.head(key).await.expect("head").is_none(),
        "object should be gone"
    );

    // Unconditional delete of absent key → idempotent Ok.
    store
        .delete(key, None)
        .await
        .expect("delete absent is idempotent");

    // Conditional delete of absent key → NotFound.
    let err = store
        .delete(key, Some(walgit_store::Version::new("anything")))
        .await;
    assert!(err.is_err(), "conditional delete absent should fail");
    assert!(
        err.unwrap_err().is_not_found(),
        "conditional delete absent should be NotFound"
    );
}

/// list: ordering, start_after, prefix isolation.
async fn test_list(store: &DynStore, base: &str) {
    // Clean up any previous data under base.
    let existing: Vec<_> = store.list(base, None).collect::<Vec<_>>().await;
    for m in existing {
        let _ = store.delete(&m.expect("list item").key, None).await;
    }

    let keys = [
        format!("{base}/a"),
        format!("{base}/b"),
        format!("{base}/c"),
        format!("{base}/d"),
        format!("{base}/e"),
    ];
    for k in &keys {
        put_bytes(store, k, b"x".as_slice(), PutMode::Create).await;
    }

    // Full listing under base prefix, sorted.
    let listed: Vec<String> = store
        .list(base, None)
        .map(|r| r.expect("list item").key)
        .collect()
        .await;
    assert_eq!(listed, keys, "list should be sorted and match");

    // start_after: skip a and b.
    let listed: Vec<String> = store
        .list(base, Some(&format!("{base}/b")))
        .map(|r| r.expect("list item").key)
        .collect()
        .await;
    assert_eq!(listed, &keys[2..], "start_after should skip a and b");

    // Delimited listing: the "directories" directly below a prefix, never the objects beneath.
    let nested = [
        format!("{base}/dirs/acme/monorepo/manifest.pb"),
        format!("{base}/dirs/acme/monorepo/wal/deadbeef.pack"),
        format!("{base}/dirs/acme/large/manifest.pb"),
        format!("{base}/dirs/test/e2e/manifest.pb"),
    ];
    for k in &nested {
        put_bytes(store, k, b"x".as_slice(), PutMode::Create).await;
    }
    let owners = store
        .list_prefixes(&format!("{base}/dirs/"))
        .await
        .expect("list_prefixes");
    assert_eq!(
        owners,
        vec![format!("{base}/dirs/acme/"), format!("{base}/dirs/test/")]
    );
    let acme = store
        .list_prefixes(&format!("{base}/dirs/acme/"))
        .await
        .expect("list_prefixes");
    assert_eq!(
        acme,
        vec![
            format!("{base}/dirs/acme/large/"),
            format!("{base}/dirs/acme/monorepo/")
        ]
    );
    let leaf = store
        .list_prefixes(&format!("{base}/dirs/test/e2e/"))
        .await
        .expect("list_prefixes");
    assert!(
        leaf.is_empty(),
        "objects directly under the prefix are not prefixes"
    );
    for k in &nested {
        let _ = store.delete(k, None).await;
    }

    // Prefix isolation: a different prefix doesn't see our keys.
    let other: Vec<String> = store
        .list("zzz-isolated", None)
        .map(|r| r.expect("list item").key)
        .collect()
        .await;
    assert!(other.is_empty(), "prefix isolation should hold");

    // Cleanup.
    for k in &keys {
        let _ = store.delete(k, None).await;
    }
}

/// 8 MiB streamed put/get roundtrip with checksum.
async fn test_large_streamed_roundtrip(store: &DynStore, key: &str) {
    let _ = store.delete(key, None).await;

    // 8 MiB of pseudo-random but deterministic data.
    let size: usize = 8 * 1024 * 1024;
    let mut data = Vec::with_capacity(size);
    let mut state: u32 = 0x1234_5678;
    for _ in 0..size {
        // xorshift32
        state ^= state << 13;
        state ^= state >> 17;
        state ^= state << 5;
        data.push((state & 0xFF) as u8);
    }
    let data = Bytes::from(data);

    // Checksum (SHA-1).
    use sha1::{Digest, Sha1};
    let mut hasher = Sha1::new();
    hasher.update(&data);
    let expected_checksum = hasher.finalize();

    // Put as a stream.
    let len = data.len() as u64;
    let chunk = data.clone();
    let stream = walgit_store::util::once(chunk);
    let meta = store
        .put(
            key,
            PutBody::Stream { len, stream },
            PutOptions {
                mode: PutMode::Create,
                ..Default::default()
            },
        )
        .await
        .expect("large put");
    assert_eq!(meta.size, len);

    // Get back and verify.
    let r = store
        .get(key, GetOptions::default())
        .await
        .expect("large get");
    let (_, body) = collect_body(r).await;
    assert_eq!(body.len(), size, "size mismatch");

    let mut hasher = Sha1::new();
    hasher.update(&body);
    let actual_checksum = hasher.finalize();
    assert_eq!(
        actual_checksum.as_slice(),
        expected_checksum.as_slice(),
        "checksum mismatch on 8 MiB roundtrip"
    );

    let _ = store.delete(key, None).await;
}

/// Multipart path: put an object above the threshold, verify roundtrip.
/// For MemoryStore this exercises the same code path (no multipart).
/// For S3Store with a small threshold, this triggers multipart upload.
async fn test_multipart_path(store: &DynStore, key: &str) {
    let _ = store.delete(key, None).await;

    // 6 MiB — above the 5 MiB threshold; S3 requires parts >= 5 MiB
    // (except the last). With 5 MiB part size: 2 parts (5 MiB + 1 MiB).
    let size: usize = 6 * 1024 * 1024;
    let mut data = Vec::with_capacity(size);
    let mut state: u32 = 0xABCD_1234;
    for _ in 0..size {
        state ^= state << 13;
        state ^= state >> 17;
        state ^= state << 5;
        data.push((state & 0xFF) as u8);
    }
    let data = Bytes::from(data);

    let len = data.len() as u64;
    let stream = walgit_store::util::once(data.clone());
    store
        .put(
            key,
            PutBody::Stream { len, stream },
            PutOptions {
                mode: PutMode::Overwrite,
                ..Default::default()
            },
        )
        .await
        .expect("multipart put");

    let r = store
        .get(key, GetOptions::default())
        .await
        .expect("multipart get");
    let (_, body) = collect_body(r).await;
    assert_eq!(&body[..], &data[..], "multipart roundtrip mismatch");

    let _ = store.delete(key, None).await;
}

// ---- test wrappers -----------------------------------------------------

#[tokio::test]
async fn memory_contract() {
    let store: DynStore = Arc::new(MemoryStore::new());
    run_contract(store, "").await;
}

#[cfg(feature = "s3")]
#[tokio::test]
async fn s3_contract() {
    let endpoint = match std::env::var("WALGIT_TEST_S3_ENDPOINT") {
        Ok(v) => v,
        Err(_) => {
            eprintln!("skipping s3_contract: WALGIT_TEST_S3_ENDPOINT not set");
            return;
        }
    };
    let bucket = std::env::var("WALGIT_TEST_BUCKET").unwrap_or_else(|_| "walgit-test".into());
    let _access_key =
        std::env::var("AWS_ACCESS_KEY_ID").expect("AWS_ACCESS_KEY_ID required for S3 tests");
    let _secret_key = std::env::var("AWS_SECRET_ACCESS_KEY")
        .expect("AWS_SECRET_ACCESS_KEY required for S3 tests");

    // Unique prefix per run.
    let prefix = format!("contract-test-{}", uuid::Uuid::new_v4().simple());

    let cfg = walgit_config::StoreConfig {
        backend: walgit_config::StoreBackend::S3,
        bucket: bucket.clone(),
        prefix: prefix.clone(),
        s3: walgit_config::S3Config {
            endpoint: endpoint.clone(),
            region: "us-east-1".into(),
            access_key_env: "AWS_ACCESS_KEY_ID".into(),
            secret_key_env: "AWS_SECRET_ACCESS_KEY".into(),
            force_path_style: true,
        },
        multipart_threshold: bytesize::ByteSize::mib(5),
        multipart_part_size: bytesize::ByteSize::mib(5),
        ..Default::default()
    };

    let store = walgit_store::s3::S3Store::new(&cfg)
        .await
        .expect("S3Store::new");
    let store: DynStore = Arc::new(store);

    run_contract(store.clone(), &prefix).await;

    // Cleanup: delete all objects under the prefix.
    let to_delete: Vec<_> = futures::stream::iter(
        walgit_store::ObjectStore::list(store.as_ref(), &prefix, None)
            .collect::<Vec<_>>()
            .await,
    )
    .filter_map(|r| async move { r.ok() })
    .collect::<Vec<_>>()
    .await;
    for m in to_delete {
        let _ = store.delete(&m.key, None).await;
    }
}

#[cfg(feature = "gcs")]
#[tokio::test]
async fn gcs_contract() {
    let bucket = match std::env::var("WALGIT_TEST_GCS_BUCKET") {
        Ok(v) => v,
        Err(_) => {
            eprintln!("skipping gcs_contract: WALGIT_TEST_GCS_BUCKET not set");
            return;
        }
    };

    // Install the rustls crypto provider (required for TLS with google-cloud-storage).
    let _ = rustls::crypto::ring::default_provider().install_default();

    // Unique prefix per run.
    let prefix = format!("contract-test-{}", uuid::Uuid::new_v4().simple());
    eprintln!("[gcs_contract] bucket={bucket} prefix={prefix}");

    let cfg = walgit_config::StoreConfig {
        backend: walgit_config::StoreBackend::Gcs,
        bucket: bucket.clone(),
        prefix: prefix.clone(),
        ..Default::default()
    };

    let store = walgit_store::gcs::GcsStore::new(&cfg)
        .await
        .expect("GcsStore::new");
    let store: DynStore = Arc::new(store);

    run_contract(store.clone(), &prefix).await;

    // Cleanup: delete all objects under the prefix.
    let to_delete: Vec<_> = futures::stream::iter(
        walgit_store::ObjectStore::list(store.as_ref(), &prefix, None)
            .collect::<Vec<_>>()
            .await,
    )
    .filter_map(|r| async move { r.ok() })
    .collect::<Vec<_>>()
    .await;
    let count = to_delete.len();
    for m in &to_delete {
        let _ = store.delete(&m.key, None).await;
    }
    eprintln!("[gcs_contract] cleanup done ({count} objects deleted)");
}

#[cfg(feature = "azure")]
#[tokio::test]
async fn azure_contract() {
    let Ok(account) = std::env::var("WALGIT_TEST_AZURE_ACCOUNT") else {
        eprintln!("skipping azure_contract: WALGIT_TEST_AZURE_ACCOUNT not set");
        return;
    };
    let container =
        std::env::var("WALGIT_TEST_AZURE_CONTAINER").unwrap_or_else(|_| "walgit-test".into());

    // Unique prefix per run.
    let prefix = format!("contract-test-{}", uuid::Uuid::new_v4().simple());
    eprintln!("[azure_contract] account={account} container={container} prefix={prefix}");

    let cfg = walgit_config::StoreConfig {
        backend: walgit_config::StoreBackend::Azure,
        bucket: container.clone(),
        prefix: prefix.clone(),
        azure: walgit_config::AzureConfig {
            account,
            ..Default::default()
        },
        // The same 5 MiB pair `s3_contract` uses, and for the same reason: with
        // the default threshold the 6 MiB multipart case and the 8 MiB streamed
        // roundtrip both fit a single upload, and the staged-block path
        // (stage_block + commit_block_list) would never run against the service.
        multipart_threshold: bytesize::ByteSize::mib(5),
        multipart_part_size: bytesize::ByteSize::mib(5),
        ..Default::default()
    };

    let store = walgit_store::azure::AzureStore::new(&cfg)
        .await
        .expect("AzureStore::new");
    let store: DynStore = Arc::new(store);

    run_contract(store.clone(), &prefix).await;

    // The user-delegation SAS must be readable by a client holding no Azure
    // credentials at all: a plain GET with no Authorization header.
    //
    // The URL is itself a credential (it ends in `sig=…`), so nothing below
    // prints it — including on failure, where a `reqwest::Error`'s own Display
    // would embed the URL it was fetching. Failures name the key and the HTTP
    // status, never the URL or the error.
    let sas_key = format!("{prefix}/sas-probe");
    store
        .put(
            &sas_key,
            PutBody::Bytes(Bytes::from_static(b"sas")),
            PutOptions::from(PutMode::Create),
        )
        .await
        .expect("sas probe put");
    let url = store
        .signed_get_url(&sas_key, std::time::Duration::from_mins(5))
        .await
        .expect("signed_get_url")
        .expect("azure signs a URL for every key");
    let resp = match reqwest::Client::new().get(url).send().await {
        Ok(r) => r,
        Err(e) => panic!(
            "[azure_contract] sas probe {sas_key}: request failed (connect={}, timeout={})",
            e.is_connect(),
            e.is_timeout()
        ),
    };
    let status = resp.status();
    // The service's own error code (`AuthenticationFailed`, `AuthorizationFailure`,
    // …) is a header, not part of the credential — the one useful thing to print
    // when the probe fails.
    let code = resp
        .headers()
        .get("x-ms-error-code")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("-")
        .to_owned();
    let Ok(body) = resp.bytes().await else {
        // The error is dropped rather than printed: its Display carries the URL.
        panic!("[azure_contract] sas probe {sas_key}: HTTP {status} {code}, body unreadable")
    };
    assert!(
        status.is_success(),
        "[azure_contract] sas probe {sas_key}: HTTP {status} {code} \
         (a signed URL must need no credentials)"
    );
    assert_eq!(
        &body[..],
        b"sas",
        "[azure_contract] sas probe {sas_key}: unexpected body"
    );

    // Cleanup: delete all objects under the prefix (the SAS probe included).
    let to_delete: Vec<_> = futures::stream::iter(
        walgit_store::ObjectStore::list(store.as_ref(), &prefix, None)
            .collect::<Vec<_>>()
            .await,
    )
    .filter_map(|r| async move { r.ok() })
    .collect::<Vec<_>>()
    .await;
    let count = to_delete.len();
    for m in &to_delete {
        let _ = store.delete(&m.key, None).await;
    }
    eprintln!("[azure_contract] cleanup done ({count} objects deleted)");
}

/// Control plane must stay fast under bulk load (prod 2026-08-20: a 184-byte
/// GET and the manifest's conditional GET queued 4–11 min behind a 7.5 GB
/// striped download on the shared channel). Against the real bucket: read
/// 1 GiB of a big object as 32 parallel 32 MiB ranges (bulk clients + permits)
/// while timing small control-plane calls every 250 ms; every small call must
/// stay under 2 s. `WALGIT_TEST_GCS_BUCKET=walgit-store WALGIT_TEST_GCS_BIG_KEY=<key under prefix>`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gcs_control_plane_not_starved_by_bulk() {
    let (Ok(bucket), Ok(big_key)) = (
        std::env::var("WALGIT_TEST_GCS_BUCKET"),
        std::env::var("WALGIT_TEST_GCS_BIG_KEY"),
    ) else {
        eprintln!(
            "skipping: set WALGIT_TEST_GCS_BUCKET and WALGIT_TEST_GCS_BIG_KEY (e.g. prefix/repos/acme/monorepo/wal/<sha>.pack)"
        );
        return;
    };
    let _ = rustls::crypto::ring::default_provider().install_default();
    let mut cfg = walgit_config::StoreConfig {
        backend: walgit_config::StoreBackend::Gcs,
        bucket,
        prefix: String::new(),
        ..Default::default()
    };
    if let Ok(n) = std::env::var("WALGIT_TEST_BULK_CONCURRENCY") {
        cfg.gcs.bulk_concurrency = n.parse().unwrap();
    }
    let store: DynStore = Arc::new(
        walgit_store::gcs::GcsStore::new(&cfg)
            .await
            .expect("GcsStore::new"),
    );
    // Baseline: the probe pair with no load.
    let t = std::time::Instant::now();
    let size = store
        .head(&big_key)
        .await
        .unwrap()
        .expect("big object exists")
        .size;
    let _ = store
        .get(
            &big_key,
            GetOptions {
                if_none_match: Some(walgit_store::Version::new("1")),
                range: Some(0..16),
                ..Default::default()
            },
        )
        .await;
    eprintln!("baseline probe {:?}", t.elapsed());
    let total = (1u64 << 30).min(size);
    const CHUNK: u64 = 32 * 1024 * 1024;
    let bulk = {
        let store = store.clone();
        let big_key = big_key.clone();
        tokio::spawn(async move {
            let t = std::time::Instant::now();
            let starts: Vec<u64> = (0..total).step_by(CHUNK as usize).collect();
            let n = futures::stream::iter(starts)
                .map(|start| {
                    let store = store.clone();
                    let key = big_key.clone();
                    async move {
                        let r = store
                            .get(
                                &key,
                                GetOptions {
                                    range: Some(start..(start + CHUNK).min(total)),
                                    ..Default::default()
                                },
                            )
                            .await
                            .unwrap();
                        match r {
                            GetResult::Object { body, .. } => {
                                walgit_store::util::collect(body, CHUNK as usize)
                                    .await
                                    .unwrap()
                                    .len()
                            }
                            _ => 0,
                        }
                    }
                })
                .buffer_unordered(32)
                .fold(0usize, |a, b| async move { a + b })
                .await;
            (n, t.elapsed())
        })
    };
    // Control-plane probes while the bulk read runs: a small-object HEAD +
    // conditional GET of the big object's metadata.
    let mut worst = std::time::Duration::ZERO;
    let mut probes = 0;
    while !bulk.is_finished() {
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        let t = std::time::Instant::now();
        let _ = store.head(&big_key).await.unwrap();
        let t_head = t.elapsed();
        let t = std::time::Instant::now();
        let _ = store
            .get(
                &big_key,
                GetOptions {
                    if_none_match: Some(walgit_store::Version::new("1")),
                    range: Some(0..16),
                    ..Default::default()
                },
            )
            .await;
        let t_cond = t.elapsed();
        // Small control-plane objects next to the pack (the repo's manifest /
        // bundle list): a plain GET must not wait for a bulk permit.
        // The ranged cond-get on the big key is bulk by design (it takes a
        // permit behind the stripes); the control-plane bar is head + smalls.
        let mut d = t_head;
        let mut smalls = Vec::new();
        if let Some(repo_root) = big_key.rsplit_once("/wal/").map(|(a, _)| a) {
            for small in [
                format!("{repo_root}/manifest.pb"),
                format!("{repo_root}/bundles/list.pb"),
            ] {
                let t = std::time::Instant::now();
                let _ = store.get(&small, GetOptions::default()).await;
                smalls.push(t.elapsed());
                d = d.max(t.elapsed());
            }
        }
        eprintln!("probe: head {t_head:?} cond-get {t_cond:?} small {smalls:?}");
        worst = worst.max(d);
        probes += 1;
    }
    let (bytes, took) = bulk.await.unwrap();
    eprintln!(
        "bulk {} MiB in {:.1}s ({:.0} MB/s); {probes} probes, worst {:?}",
        bytes >> 20,
        took.as_secs_f64(),
        bytes as f64 / 1e6 / took.as_secs_f64(),
        worst
    );
    assert!(probes >= 4);
    // From a laptop the uplink itself saturates (100 MB/s) and inflates every
    // RTT (measured 70–190 ms per control call under load); a control object
    // wrongly classified bulk waits for the whole download here.
    assert!(
        worst < std::time::Duration::from_secs(2),
        "a control-plane call took {worst:?} under bulk load"
    );
}
