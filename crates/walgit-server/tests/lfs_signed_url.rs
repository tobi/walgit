//! `lfs.serve_via = "signed_url"` uploads: the batch's `upload` action becomes a
//! presigned, checksummed PUT straight to the store, `verify` stays on walgit.
//!
//! The in-memory store signs nothing (like every backend that cannot bind a
//! sha256 on a PUT), so it takes a `fake_signed_put_base`: point it anywhere and
//! it answers the way a signing backend does. Pointed at a URL nobody serves, the
//! batch response itself is under test; pointed at a mock bucket that checks the
//! checksum the way S3 does, real `git lfs push` is.

mod harness;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::Result;
use base64::Engine as _;
use harness::{Server, TestRepo, git_in};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use walgit_store::ObjectStoreExt;
use walgit_store::memory::{FAKE_SIGNED_PUT_CHECKSUM_HEADER, MemoryStore};

const TOKEN: &str = "writer-token";
/// Nothing listens here: the tests that use it read the batch answer, not bytes.
const FAKE_BUCKET: &str = "https://storage.example.test/test-bucket";

fn tokens(cfg: &mut walgit_config::Config) {
    cfg.server.auth.mode = walgit_config::AuthMode::Token;
    cfg.server.auth.anonymous_read = false;
    cfg.server.auth.tokens = vec![walgit_config::StaticToken {
        principal: "writer@example.com".into(),
        token: TOKEN.into(),
        token_env: None,
        write: true,
        admin: false,
    }];
}

/// `Server::put_repo` sends no credential, and these servers require one.
async fn create_repo(server: &Server) -> Result<()> {
    let resp = reqwest::Client::new()
        .put(format!("{}/o/r", server.base_url))
        .bearer_auth(TOKEN)
        .send()
        .await?;
    assert!(resp.status().is_success(), "create repo: {}", resp.status());
    Ok(())
}

fn batch_upload(oid: &str, size: usize) -> Value {
    json!({
        "operation": "upload",
        "transfers": ["basic"],
        "objects": [{"oid": oid, "size": size}],
    })
}

async fn upload_batch(server: &Server, oid: &str, size: usize) -> Result<Value> {
    let resp = reqwest::Client::new()
        .post(format!(
            "{}/o/r.git/info/lfs/objects/batch",
            server.base_url
        ))
        .bearer_auth(TOKEN)
        .json(&batch_upload(oid, size))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert_eq!(
        resp.headers()
            .get("cache-control")
            .and_then(|v| v.to_str().ok()),
        Some("no-store"),
        "a batch answer carries signed URLs and a credential"
    );
    Ok(resp.json().await?)
}

/// A store that signs checksummed PUTs: the client uploads to it directly, with
/// the store's headers, and comes back to walgit only for `verify`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn signed_url_uploads_go_to_the_store_with_the_oid_bound_to_the_put() -> Result<()> {
    let body = b"lfs bytes that never touch walgit".to_vec();
    let oid = hex::encode(Sha256::digest(&body));
    let store = MemoryStore::shared();
    *store.fake_signed_put_base.lock() = Some(FAKE_BUCKET.to_string());
    let server = Server::start_with_store_and_tweak(store, |c| {
        tokens(c);
        c.lfs.serve_via = walgit_config::BundleServe::SignedUrl;
        c.lfs.signed_url_ttl = std::time::Duration::from_secs(90);
    })
    .await?;
    create_repo(&server).await?;

    let r = upload_batch(&server, &oid, body.len()).await?;
    let obj = &r["objects"][0];
    let upload = &obj["actions"]["upload"];
    assert_eq!(
        upload["href"].as_str(),
        Some(
            format!(
                "{FAKE_BUCKET}/repos/o/r/lfs/objects/{}/{}/{oid}?X-Test-Signature=1",
                &oid[..2],
                &oid[2..4]
            )
            .as_str()
        ),
        "the upload href is the store's signed URL: {obj}"
    );
    // The checksum the store signed is the oid itself, so the PUT can only land
    // the bytes the client said it was uploading.
    assert_eq!(
        upload["header"][FAKE_SIGNED_PUT_CHECKSUM_HEADER].as_str(),
        Some(
            base64::engine::general_purpose::STANDARD
                .encode(Sha256::digest(&body))
                .as_str()
        ),
        "signed headers must reach the client verbatim: {obj}"
    );
    assert_eq!(upload["expires_in"].as_u64(), Some(90));

    // `verify` is walgit's, unchanged, and still authenticated: `authenticated`
    // stops git-lfs adding walgit's credential to the store's URL, so the one it
    // needs for `verify` travels on that action.
    assert_eq!(obj["authenticated"], true);
    assert_eq!(
        obj["actions"]["verify"]["href"].as_str(),
        Some(format!("{}/o/r/info/lfs/verify", server.base_url).as_str()),
        "{obj}"
    );
    assert_eq!(
        obj["actions"]["verify"]["header"]["authorization"].as_str(),
        Some(format!("Bearer {TOKEN}").as_str()),
        "{obj}"
    );

    // Nothing was written by handing out the URL: the object is still missing, so
    // a second batch offers the same upload rather than reporting it present.
    let again = upload_batch(&server, &oid, body.len()).await?;
    assert!(again["objects"][0]["actions"]["upload"].is_object());
    Ok(())
}

/// The default. `verify` gets no credential of its own because git-lfs
/// authenticates the walgit href itself, and the PUT's sha256 gate still runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn proxy_is_the_default_and_still_checks_the_sha256_itself() -> Result<()> {
    let body = b"lfs bytes through walgit".to_vec();
    let oid = hex::encode(Sha256::digest(&body));
    let server = Server::start_with_tweak(tokens).await?;
    create_repo(&server).await?;

    let r = upload_batch(&server, &oid, body.len()).await?;
    let obj = &r["objects"][0];
    let href = obj["actions"]["upload"]["href"]
        .as_str()
        .expect("upload href")
        .to_string();
    assert_eq!(
        href,
        format!("{}/o/r/info/lfs/objects/{oid}", server.base_url)
    );
    assert!(obj["actions"]["upload"]["header"].is_null(), "{obj}");
    assert!(obj["actions"]["verify"]["header"].is_null(), "{obj}");
    assert!(
        obj["authenticated"].is_null(),
        "git-lfs must authenticate our own href: {obj}"
    );

    let client = reqwest::Client::new();
    // Bytes that do not hash to the oid are refused before the store write.
    let bad = client
        .put(&href)
        .bearer_auth(TOKEN)
        .body(b"other bytes".to_vec())
        .send()
        .await?;
    assert_eq!(bad.status(), reqwest::StatusCode::BAD_REQUEST);
    // The real ones land, and `verify` confirms them.
    let ok = client
        .put(&href)
        .bearer_auth(TOKEN)
        .body(body.clone())
        .send()
        .await?;
    assert_eq!(ok.status(), reqwest::StatusCode::OK);
    let verified = client
        .post(format!("{}/o/r/info/lfs/verify", server.base_url))
        .bearer_auth(TOKEN)
        .json(&json!({"oid": oid, "size": body.len()}))
        .send()
        .await?;
    assert_eq!(verified.status(), reqwest::StatusCode::OK);
    Ok(())
}

/// Fail closed. A store that signs nothing, and a store whose signing is denied,
/// both leave the upload on walgit — never a signed PUT that is not bound to the
/// oid, and never a failed push.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_store_that_cannot_bind_the_checksum_keeps_the_upload_on_walgit() -> Result<()> {
    let oid = hex::encode(Sha256::digest(b"unsignable"));
    for signing_fails in [false, true] {
        let mut store = MemoryStore::new();
        store.signing_fails = signing_fails;
        if signing_fails {
            // Signing is configured, and denied: still the proxy href, not a 500.
            *store.fake_signed_put_base.lock() = Some(FAKE_BUCKET.to_string());
        }
        let server = Server::start_with_store_and_tweak(Arc::new(store), |c| {
            tokens(c);
            c.lfs.serve_via = walgit_config::BundleServe::SignedUrl;
        })
        .await?;
        create_repo(&server).await?;

        let r = upload_batch(&server, &oid, 10).await?;
        let obj = &r["objects"][0];
        assert_eq!(
            obj["actions"]["upload"]["href"].as_str(),
            Some(format!("{}/o/r/info/lfs/objects/{oid}", server.base_url).as_str()),
            "signing_fails={signing_fails}: {obj}"
        );
        assert!(obj["actions"]["upload"]["header"].is_null(), "{obj}");
        assert!(obj["authenticated"].is_null(), "{obj}");
    }
    Ok(())
}

/// `lfs.max_object_bytes` can only be enforced where the bytes pass through, so
/// an object over the cap is not signed: it goes to the proxy href, which rejects
/// it (413) instead of the store accepting it behind walgit's back.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_object_over_the_cap_is_never_signed() -> Result<()> {
    let body = vec![b'x'; 4096];
    let oid = hex::encode(Sha256::digest(&body));
    let store = MemoryStore::shared();
    *store.fake_signed_put_base.lock() = Some(FAKE_BUCKET.to_string());
    let server = Server::start_with_store_and_tweak(store, |c| {
        tokens(c);
        c.lfs.serve_via = walgit_config::BundleServe::SignedUrl;
        c.lfs.max_object_bytes = bytesize::ByteSize::b(1024);
    })
    .await?;
    create_repo(&server).await?;

    let r = upload_batch(&server, &oid, body.len()).await?;
    let obj = &r["objects"][0];
    let href = obj["actions"]["upload"]["href"]
        .as_str()
        .expect("upload href")
        .to_string();
    assert_eq!(
        href,
        format!("{}/o/r/info/lfs/objects/{oid}", server.base_url),
        "{obj}"
    );
    let too_big = reqwest::Client::new()
        .put(&href)
        .bearer_auth(TOKEN)
        .body(body)
        .send()
        .await?;
    assert_eq!(too_big.status(), reqwest::StatusCode::PAYLOAD_TOO_LARGE);

    // Under the cap the same repo signs again.
    let small = b"small".to_vec();
    let small_oid = hex::encode(Sha256::digest(&small));
    let r = upload_batch(&server, &small_oid, small.len()).await?;
    assert!(
        r["objects"][0]["actions"]["upload"]["href"]
            .as_str()
            .is_some_and(|h| h.starts_with(FAKE_BUCKET)),
        "{r}"
    );
    Ok(())
}

/// A mock bucket that behaves like S3 on a presigned, checksummed PUT: it refuses
/// a body whose sha256 is not the one in the signed header (S3's `BadDigest`), and
/// on success the object is in the store — which is where walgit's `verify` looks.
struct Bucket {
    store: Arc<MemoryStore>,
    puts: AtomicUsize,
    bad_digests: AtomicUsize,
    authorized_puts: AtomicUsize,
}

async fn bucket_put(
    axum::extract::State(b): axum::extract::State<Arc<Bucket>>,
    axum::extract::Path(key): axum::extract::Path<String>,
    headers: axum::http::HeaderMap,
    body: bytes::Bytes,
) -> axum::http::StatusCode {
    b.puts.fetch_add(1, Ordering::SeqCst);
    if headers.contains_key(axum::http::header::AUTHORIZATION) {
        // A presigned URL carries its own signature; a second credential is what
        // S3 rejects as "only one auth mechanism allowed".
        b.authorized_puts.fetch_add(1, Ordering::SeqCst);
        return axum::http::StatusCode::BAD_REQUEST;
    }
    let signed = headers
        .get(FAKE_SIGNED_PUT_CHECKSUM_HEADER)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_owned();
    if signed != base64::engine::general_purpose::STANDARD.encode(Sha256::digest(&body)) {
        b.bad_digests.fetch_add(1, Ordering::SeqCst);
        return axum::http::StatusCode::BAD_REQUEST;
    }
    match b
        .store
        .put_bytes(&key, body, walgit_store::PutMode::Overwrite)
        .await
    {
        Ok(_) => axum::http::StatusCode::OK,
        Err(_) => axum::http::StatusCode::INTERNAL_SERVER_ERROR,
    }
}

/// The whole flow with the real client, under token auth: `git lfs push` sends
/// the bytes to the bucket with the signed checksum header and none of walgit's
/// credential, then comes back to walgit for `verify` — which is authenticated
/// only by the header the batch put on that action, since `authenticated: true`
/// stops git-lfs consulting the credential helper for either request.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn git_lfs_pushes_straight_to_the_bucket_and_verifies_with_us() -> Result<()> {
    if !git_lfs_present() {
        eprintln!("git lfs not present; skipping");
        return Ok(());
    }
    let store = MemoryStore::shared();
    let bucket = Arc::new(Bucket {
        store: store.clone(),
        puts: AtomicUsize::new(0),
        bad_digests: AtomicUsize::new(0),
        authorized_puts: AtomicUsize::new(0),
    });
    let app = axum::Router::new()
        .route("/{*key}", axum::routing::put(bucket_put))
        .with_state(bucket.clone())
        .layer(axum::extract::DefaultBodyLimit::disable());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let bucket_base = format!("http://{}", listener.local_addr()?);
    tokio::spawn(async move { axum::serve(listener, app).await });

    *store.fake_signed_put_base.lock() = Some(bucket_base);
    let server = Server::start_with_store_and_tweak(store, |c| {
        tokens(c);
        c.lfs.serve_via = walgit_config::BundleServe::SignedUrl;
    })
    .await?;
    create_repo(&server).await?;

    let payload = b"bytes that only ever reach the bucket\n";
    let src = TestRepo::synthetic(1, 1)?;
    // A credential helper, not a blanket `http.extraHeader`: it is consulted only
    // for the requests git-lfs decides need our credential, which is the
    // distinction `authenticated` draws and this test rests on.
    let helper = bearer_helper(&src)?;
    git_in(&src, &["config", "credential.helper", &helper])?;
    git_in(&src, &["lfs", "install", "--local"])?;
    git_in(&src, &["lfs", "track", "*.bin"])?;
    std::fs::write(src.join("blob.bin"), payload)?;
    git_in(&src, &["add", ".gitattributes", "blob.bin"])?;
    git_in(&src, &["commit", "-m", "lfs"])?;
    git_in(
        &src,
        &["remote", "add", "origin", &server.repo_url("o", "r")],
    )?;
    // The LFS half on its own: batch, transfer, verify — no git objects involved.
    git_in(&src, &["lfs", "push", "origin", "main"])?;

    assert_eq!(
        bucket.puts.load(Ordering::SeqCst),
        1,
        "git-lfs uploaded to the bucket"
    );
    assert_eq!(bucket.bad_digests.load(Ordering::SeqCst), 0);
    assert_eq!(
        bucket.authorized_puts.load(Ordering::SeqCst),
        0,
        "walgit's credential must never travel to the bucket"
    );
    // The object is ours from here on: the next batch reports it present.
    let oid = hex::encode(Sha256::digest(payload));
    let r = upload_batch(&server, &oid, payload.len()).await?;
    assert!(
        r["objects"][0].get("actions").is_none(),
        "the bucket's object is ours now: {r}"
    );
    assert_eq!(
        server
            .store
            .get_bytes(&format!(
                "repos/o/r/lfs/objects/{}/{}/{oid}",
                &oid[..2],
                &oid[2..4]
            ))
            .await?
            .expect("stored")
            .1
            .as_ref(),
        payload
    );
    Ok(())
}

/// A `git credential` helper (git ≥ 2.46 authtype protocol) that hands out
/// `TOKEN`. Written next to the repo so nothing global is touched.
fn bearer_helper(dir: &std::path::Path) -> Result<String> {
    let path = dir.join("bearer-helper");
    std::fs::write(
        &path,
        format!(
            "#!/bin/sh\ncase \"$1\" in get) while IFS= read -r l; do [ -z \"$l\" ] && break; done; \
             printf 'capability[]=authtype\\nauthtype=Bearer\\ncredential={TOKEN}\\n\\n' ;; esac\n"
        ),
    )?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755))?;
    }
    Ok(path.display().to_string())
}

fn git_lfs_present() -> bool {
    std::process::Command::new("git")
        .args(["lfs", "version"])
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}
