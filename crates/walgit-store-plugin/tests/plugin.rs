#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]

use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use std::sync::Arc;
use walgit_store::{
    DynStore, GetOptions, GetResult, ObjectStoreExt, Prefixed, PutBody, PutMode, PutOptions,
    StoreError, memory::MemoryStore,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires a built cdylib; run just test-plugin"]
async fn dynamic_passthrough_preserves_the_store_contract() {
    let path = std::env::var("WALGIT_TEST_PLUGIN").expect("just test-plugin sets the cdylib path");
    assert!(
        walgit_store_plugin::load(
            std::path::Path::new(&path),
            serde_json::json!({"unsupported": true}),
            Arc::new(MemoryStore::new()),
        )
        .await
        .is_err()
    );
    let raw: DynStore = Arc::new(MemoryStore::new());
    let inner: DynStore = Arc::new(Prefixed::new(raw.clone(), "global/"));
    let store =
        walgit_store_plugin::load(std::path::Path::new(&path), serde_json::json!({}), inner)
            .await
            .unwrap();
    let bytes = Bytes::from(vec![42; 2 * 1024 * 1024 + 17]);
    let meta = store
        .put(
            "repos/owner/repo/wal/pack",
            PutBody::Bytes(bytes.clone()),
            PutMode::Create.into(),
        )
        .await
        .unwrap();
    assert_eq!(meta.size, bytes.len() as u64);
    assert!(
        raw.head("global/repos/owner/repo/wal/pack")
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        raw.head("global/global/repos/owner/repo/wal/pack")
            .await
            .unwrap()
            .is_none()
    );
    let (_, read) = store.get_bytes(&meta.key).await.unwrap().unwrap();
    assert_eq!(read, bytes);
    let range = store
        .get(
            &meta.key,
            GetOptions {
                range: Some(1_048_570..1_048_590),
                if_match: Some(meta.version.clone()),
                ..GetOptions::default()
            },
        )
        .await
        .unwrap();
    let GetResult::Object {
        meta: ranged_meta,
        body,
    } = range
    else {
        panic!("expected range");
    };
    assert_eq!(ranged_meta.size, meta.size);
    assert_eq!(
        walgit_store::util::collect(body, 20).await.unwrap(),
        bytes.slice(1_048_570..1_048_590)
    );
    assert!(matches!(
        store
            .get(
                &meta.key,
                GetOptions {
                    if_none_match: Some(meta.version.clone()),
                    ..GetOptions::default()
                }
            )
            .await
            .unwrap(),
        GetResult::NotModified { .. }
    ));
    let conflicts = futures::future::join_all((0..16).map(|_| {
        store.put(
            "race",
            PutBody::Bytes(Bytes::from_static(b"one")),
            PutMode::Create.into(),
        )
    }))
    .await;
    assert_eq!(conflicts.iter().filter(|r| r.is_ok()).count(), 1);
    assert!(
        conflicts
            .iter()
            .filter(|r| r.is_err())
            .all(|r| matches!(r, Err(StoreError::PreconditionFailed { .. })))
    );
    let updated = store
        .put(
            &meta.key,
            PutBody::Bytes(Bytes::from_static(b"new")),
            PutMode::Update(meta.version.clone()).into(),
        )
        .await
        .unwrap();
    assert!(matches!(
        store.delete(&meta.key, Some(meta.version.clone())).await,
        Err(StoreError::PreconditionFailed { .. })
    ));
    assert!(matches!(
        store
            .get(
                &meta.key,
                GetOptions {
                    if_match: Some(meta.version),
                    ..GetOptions::default()
                }
            )
            .await,
        Err(StoreError::PreconditionFailed { .. })
    ));
    let listed: Vec<_> = store.list("repos/", None).try_collect().await.unwrap();
    assert_eq!(listed, vec![updated.clone()]);
    store
        .delete(&meta.key, Some(updated.version))
        .await
        .unwrap();
    assert!(store.head(&meta.key).await.unwrap().is_none());
    let failed = futures::stream::iter(vec![
        Ok(Bytes::from_static(b"partial")),
        Err(StoreError::retryable(anyhow::anyhow!("interrupted upload"))),
    ]);
    assert!(
        store
            .put(
                "interrupted",
                PutBody::Stream {
                    len: 100,
                    stream: Box::pin(failed)
                },
                PutOptions::default()
            )
            .await
            .is_err()
    );
    assert!(store.head("interrupted").await.unwrap().is_none());
    let file = tempfile::NamedTempFile::new().unwrap();
    tokio::fs::write(file.path(), b"file body").await.unwrap();
    store
        .put(
            "file",
            PutBody::File(file.path().into()),
            PutOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(
        store.get_bytes("file").await.unwrap().unwrap().1,
        "file body"
    );
    assert!(
        store
            .signed_get_url("file", std::time::Duration::from_mins(1))
            .await
            .unwrap()
            .is_none()
    );
    assert!(store.accel_target("file").await.is_none());
    let result = store.get("file", GetOptions::default()).await.unwrap();
    let GetResult::Object { mut body, .. } = result else {
        panic!("expected object");
    };
    drop(store); // response stream must keep the plugin runtime/store alive
    assert_eq!(body.next().await.unwrap().unwrap(), "file body");
}

#[tokio::test]
async fn missing_plugin_fails_closed() {
    let inner: DynStore = Arc::new(MemoryStore::new());
    assert!(
        walgit_store_plugin::load(
            std::path::Path::new("/no-such-walgit-plugin.so"),
            serde_json::json!({}),
            inner
        )
        .await
        .is_err()
    );
}

#[tokio::test]
#[ignore = "requires a built incompatible module; run just test-plugin"]
async fn incompatible_layout_is_rejected_before_initialization() {
    let path = std::env::var("WALGIT_TEST_INCOMPATIBLE").unwrap();
    let result = walgit_store_plugin::load(
        std::path::Path::new(&path),
        serde_json::json!({}),
        Arc::new(MemoryStore::new()),
    )
    .await;
    let error = result.err().expect("wrong module layout must be rejected");
    assert!(
        error.to_string().contains("checking storage plugin ABI"),
        "{error:#}"
    );
}

// A reproducible local overhead probe, not a timing assertion or a cloud benchmark.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual measurement; set WALGIT_TEST_PLUGIN and run with --nocapture"]
async fn passthrough_overhead() {
    let path = std::env::var("WALGIT_TEST_PLUGIN").unwrap();
    let raw: DynStore = Arc::new(MemoryStore::new());
    let wrapped = walgit_store_plugin::load(
        std::path::Path::new(&path),
        serde_json::json!({}),
        raw.clone(),
    )
    .await
    .unwrap();
    for size in [1024, 1024 * 1024, 8 * 1024 * 1024] {
        raw.put(
            "bench",
            PutBody::Bytes(Bytes::from(vec![42; size])),
            PutOptions::default(),
        )
        .await
        .unwrap();
        let count = if size == 1024 { 1000_u32 } else { 100_u32 };
        for (label, store) in [("raw", &raw), ("plugin", &wrapped)] {
            for _ in 0..10 {
                std::hint::black_box(store.get_bytes("bench").await.unwrap().unwrap());
            }
            let start = std::time::Instant::now();
            for _ in 0..count {
                std::hint::black_box(store.get_bytes("bench").await.unwrap().unwrap());
            }
            println!(
                "BENCH bytes={size} path={label} iterations={count} mean_us={:.2}",
                start.elapsed().as_secs_f64() * 1_000_000.0 / f64::from(count)
            );
        }
    }
}
