//! `upstream.lfs` read-through (per-repo D24 setting): a mock upstream LFS
//! server holds one object; walgit's store has none.
//! - batch `upload`: the object is reported present with **no actions** (git-lfs
//!   then skips the upload and the push proceeds); unknown objects still get an
//!   upload action.
//! - batch `download`: our own href; `GET objects/<oid>` streams the bytes from
//!   the upstream and persists them into the store (second GET served locally).
//! - upstream lacks it: 404 on download, upload action on upload.

mod harness;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use anyhow::Result;
use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    routing::{get, post},
};
use harness::Server;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use walgit_proto::keys;
use walgit_store::memory::MemoryStore;
use walgit_store::{ObjectStoreExt, PutMode};

struct Mock {
    oid: String,
    body: Vec<u8>,
    batches: AtomicUsize,
    downloads: AtomicUsize,
    base: std::sync::Mutex<String>,
}

async fn mock_batch(State(m): State<Arc<Mock>>, Json(req): Json<Value>) -> Json<Value> {
    m.batches.fetch_add(1, Ordering::SeqCst);
    assert_eq!(
        req["operation"], "download",
        "walgit always asks the upstream for downloads"
    );
    let base = m.base.lock().unwrap().clone();
    let objects: Vec<Value> = req["objects"]
        .as_array()
        .unwrap()
        .iter()
        .map(|o| {
            let oid = o["oid"].as_str().unwrap();
            // GitHub rejects a wrong size ("Object … is not 0 bytes"): so does the mock.
            if oid == m.oid && o["size"].as_u64() != Some(m.body.len() as u64) {
                return json!({"oid": oid, "size": o["size"], "error": {"code": 422, "message": format!("Object {oid} is not {} bytes", o["size"])}});
            }
            if oid == m.oid {
                json!({"oid": oid, "size": m.body.len(), "actions": {"download": {"href": format!("{base}/media/{oid}"), "header": {"X-Mock-Token": "t"}}}})
            } else {
                json!({"oid": oid, "size": o["size"], "error": {"code": 404, "message": "not found"}})
            }
        })
        .collect();
    Json(json!({"transfer": "basic", "objects": objects}))
}

async fn mock_media(
    State(m): State<Arc<Mock>>,
    headers: axum::http::HeaderMap,
) -> (StatusCode, Vec<u8>) {
    m.downloads.fetch_add(1, Ordering::SeqCst);
    assert_eq!(
        headers.get("x-mock-token").map(|v| v.to_str().unwrap()),
        Some("t"),
        "batch headers are forwarded"
    );
    (StatusCode::OK, m.body.clone())
}

async fn start_mock(body: Vec<u8>) -> Result<(Arc<Mock>, String)> {
    let oid = hex::encode(Sha256::digest(&body));
    let mock = Arc::new(Mock {
        oid,
        body,
        batches: AtomicUsize::new(0),
        downloads: AtomicUsize::new(0),
        base: Default::default(),
    });
    let app = Router::new()
        .route("/lfs/objects/batch", post(mock_batch))
        .route("/media/{oid}", get(mock_media))
        .with_state(mock.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let base = format!("http://{}", listener.local_addr()?);
    *mock.base.lock().unwrap() = base.clone();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    Ok((mock, base))
}

fn batch_req(op: &str, objects: &[(&str, usize)]) -> Value {
    json!({"operation": op, "transfers": ["basic"], "objects": objects.iter().map(|(o, s)| json!({"oid": o, "size": s})).collect::<Vec<_>>()})
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_presence_checks_are_parallel_and_preserve_batch_order() -> Result<()> {
    let mut raw_store = MemoryStore::new();
    let objects: Vec<String> = (0..100).map(|i| format!("{i:064x}")).collect();
    for oid in objects.iter().step_by(2) {
        raw_store
            .put_bytes(
                &format!("repos/o/r/{}", keys::lfs_key(oid)),
                "present",
                PutMode::Create,
            )
            .await?;
    }
    raw_store.latency = Some(Duration::from_millis(20));

    let server = Server::start_with_store_and_tweak(Arc::new(raw_store), |_| {}).await?;
    server.put_repo("o", "r").await?;
    let batch_url = format!("{}/o/r.git/info/lfs/objects/batch", server.base_url);
    let request = json!({
        "operation": "download",
        "transfers": ["basic"],
        "objects": objects
            .iter()
            .map(|oid| json!({"oid": oid, "size": 7}))
            .collect::<Vec<_>>(),
    });

    let started = Instant::now();
    let response: Value = reqwest::Client::new()
        .post(batch_url)
        .json(&request)
        .send()
        .await?
        .json()
        .await?;
    let elapsed = started.elapsed();

    assert!(
        elapsed < Duration::from_secs(1),
        "100 independent presence checks took {elapsed:?}"
    );
    let response_objects = response["objects"].as_array().unwrap();
    assert_eq!(response_objects.len(), objects.len());
    for (i, (actual, oid)) in response_objects.iter().zip(&objects).enumerate() {
        assert_eq!(actual["oid"], oid.as_str(), "response order changed at {i}");
        if i % 2 == 0 {
            assert!(actual["actions"]["download"].is_object());
        } else {
            assert_eq!(actual["error"]["code"], 404);
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upstream_objects_are_present_for_upload_and_streamed_then_persisted_for_download()
-> Result<()> {
    let body: Vec<u8> = (0..50_000u32).map(|i| (i % 241) as u8).collect();
    let (mock, mock_base) = start_mock(body.clone()).await?;
    let upstream = format!("{mock_base}/lfs");
    let server = Server::start_with_tweak(move |c| c.upstream.lfs = Some(upstream)).await?;
    server.put_repo("o", "r").await?;
    let client = reqwest::Client::new();
    let batch_url = format!("{}/o/r.git/info/lfs/objects/batch", server.base_url);
    let unknown = "0".repeat(64);

    // upload: the upstream's object has no actions; the unknown one gets an upload action.
    let r: Value = client
        .post(&batch_url)
        .json(&batch_req(
            "upload",
            &[(&mock.oid, body.len()), (&unknown, 7)],
        ))
        .send()
        .await?
        .json()
        .await?;
    let objs = r["objects"].as_array().unwrap();
    assert_eq!(objs[0]["oid"], mock.oid);
    assert!(
        objs[0].get("actions").is_none(),
        "upstream-held object must carry no actions: {objs:?}"
    );
    assert!(objs[0].get("error").is_none());
    assert!(
        objs[1]["actions"]["upload"]["href"]
            .as_str()
            .unwrap()
            .ends_with(&unknown)
    );
    assert_eq!(
        mock.batches.load(Ordering::SeqCst),
        1,
        "one upstream batch per request"
    );

    // download: our href; unknown → per-object 404.
    let r: Value = client
        .post(&batch_url)
        .json(&batch_req(
            "download",
            &[(&mock.oid, body.len()), (&unknown, 7)],
        ))
        .send()
        .await?
        .json()
        .await?;
    let objs = r["objects"].as_array().unwrap();
    let href = objs[0]["actions"]["download"]["href"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(
        href.starts_with(&server.base_url),
        "served through us: {href}"
    );
    assert!(
        href.ends_with(&format!("?size={}", body.len())),
        "href carries the size for the upstream batch: {href}"
    );
    assert_eq!(objs[1]["error"]["code"], 404);

    // GET streams the bytes through (Content-Length exact) …
    let r = client.get(&href).send().await?;
    assert_eq!(r.status(), StatusCode::OK);
    assert_eq!(
        r.headers()["content-length"],
        body.len().to_string().as_str()
    );
    assert_eq!(r.bytes().await?.as_ref(), &body[..]);
    assert_eq!(mock.downloads.load(Ordering::SeqCst), 1);

    // … and persists into the store (bounded wait: the put runs after the stream ends).
    let key = format!("repos/o/r/{}", keys::lfs_key(&mock.oid));
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while !server.store.exists(&key).await.unwrap_or(false) {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("object persisted into the store");
    let (_, stored) = server.store.get_bytes(&key).await?.expect("stored");
    assert_eq!(stored.as_ref(), &body[..]);

    // Second GET is local: full static contract (ETag), no upstream traffic.
    let r = client.get(&href).send().await?;
    assert_eq!(r.status(), StatusCode::OK);
    assert!(r.headers().contains_key("etag"));
    assert_eq!(r.bytes().await?.as_ref(), &body[..]);
    assert_eq!(
        mock.downloads.load(Ordering::SeqCst),
        1,
        "served from the store now"
    );
    // upload batch for it now needs no upstream either.
    let before = mock.batches.load(Ordering::SeqCst);
    let r: Value = client
        .post(&batch_url)
        .json(&batch_req("upload", &[(&mock.oid, body.len())]))
        .send()
        .await?
        .json()
        .await?;
    // The exact shape git-lfs's transfer queue accepts for "server has it": NO
    // `actions` key (a verify-only object means "upload, then verify" to it and
    // fails a push of a pointer with no local bytes — a large push, 2026-08-21).
    assert!(
        r["objects"][0].get("actions").is_none(),
        "present object must carry no actions at all: {r}"
    );
    assert_eq!(r["objects"][0]["oid"], mock.oid);
    assert_eq!(r["objects"][0]["authenticated"], true);
    assert_eq!(
        mock.batches.load(Ordering::SeqCst),
        before,
        "no upstream call for a local object"
    );

    // Unknown object on GET: 404 (upstream says no).
    let missing = format!("{}/o/r.git/info/lfs/objects/{unknown}", server.base_url);
    assert_eq!(
        client.get(&missing).send().await?.status(),
        StatusCode::NOT_FOUND
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upstream_is_a_per_repo_setting() -> Result<()> {
    let body = b"lfs via settings".to_vec();
    let (mock, mock_base) = start_mock(body.clone()).await?;
    let server = Server::start().await?;
    server.put_repo("o", "r").await?;
    let client = reqwest::Client::new();
    let batch_url = format!("{}/o/r.git/info/lfs/objects/batch", server.base_url);
    // Host config has no upstream: the object is unknown.
    let r: Value = client
        .post(&batch_url)
        .json(&batch_req("upload", &[(&mock.oid, body.len())]))
        .send()
        .await?
        .json()
        .await?;
    assert!(r["objects"][0]["actions"]["upload"].is_object());
    // D24 setting turns it on for this repo only.
    let settings_url = format!("{}/o/r/api/settings", server.base_url);
    let r = client
        .put(&settings_url)
        .header("Content-Type", "application/toml")
        .body(format!("[upstream]\nlfs = \"{mock_base}/lfs\"\n"))
        .send()
        .await?;
    assert!(
        r.status().is_success(),
        "settings put: {} {}",
        r.status(),
        r.text().await?
    );
    let r: Value = client
        .post(&batch_url)
        .json(&batch_req("upload", &[(&mock.oid, body.len())]))
        .send()
        .await?
        .json()
        .await?;
    assert!(r["objects"][0].get("actions").is_none(), "{r}");
    assert_eq!(mock.batches.load(Ordering::SeqCst), 1);
    Ok(())
}
