//! Git LFS batch API + basic transfer (download/upload/verify). Objects live at
//! `lfs/objects/<oid[0:2]>/<oid[2:4]>/<oid>` in the repo-scoped store.
use std::collections::HashMap;

use axum::body::Body;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use walgit_proto::keys;

use crate::AppState;
use crate::error::ApiError;
use crate::repo::RepoRoute;
use crate::smart::open_repo;
use crate::stream::body_to_async_read;
use walgit_store::{ObjectStore, ObjectStoreExt, PutBody, PutMode};

#[derive(Debug, Deserialize)]
pub struct BatchRequest {
    pub operation: String, // "upload" | "download"
    pub transfers: Option<Vec<String>>,
    pub objects: Vec<BatchObject>,
}

#[derive(Debug, Deserialize)]
pub struct BatchObject {
    pub oid: String,
    pub size: u64,
}

#[derive(Debug, Serialize)]
struct BatchResponse<'a> {
    transfer: &'a str,
    objects: Vec<BatchRespObject>,
}

#[derive(Debug, Serialize)]
struct BatchRespObject {
    oid: String,
    size: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    authenticated: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    actions: Option<Actions>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<LfsError>,
}

#[derive(Debug, Serialize)]
struct Actions {
    #[serde(skip_serializing_if = "Option::is_none")]
    download: Option<Action>,
    #[serde(skip_serializing_if = "Option::is_none")]
    upload: Option<Action>,
    #[serde(skip_serializing_if = "Option::is_none")]
    verify: Option<Action>,
}

#[derive(Debug, Serialize)]
struct Action {
    href: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    header: Option<std::collections::HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    expires_in: Option<u64>,
}

#[derive(Debug, Serialize)]
struct LfsError {
    code: u16,
    message: String,
}

/// `POST /{repo}/info/lfs/objects/batch`
pub async fn batch(
    st: &AppState,
    route: &RepoRoute,
    headers: &HeaderMap,
    body_bytes: Bytes,
) -> Result<Response, ApiError> {
    let body: BatchRequest = serde_json::from_slice(&body_bytes)
        .map_err(|e| ApiError::BadRequest(format!("invalid lfs batch: {e}")))?;
    if !st.cfg.lfs.enabled {
        return Err(ApiError::NotFound("lfs disabled".into()));
    }
    let _ = st.auth.require_read(headers).await.map_err(auth_err)?;
    not_served_here(st, &route.id)?;
    let handle = open_repo(st, &route.id, false).await?;
    let store = handle.store().clone();
    let base = base_url(st, route, headers);
    let cfg = handle.effective_config();

    let is_upload = body.operation == "upload";
    for o in &body.objects {
        require_lfs_oid(&o.oid)?;
    }
    // Local presence first; then one bounded upstream batch for the misses.
    let mut local = Vec::with_capacity(body.objects.len());
    let mut missing = Vec::new();
    for o in &body.objects {
        let exists = store.exists(&keys::lfs_key(&o.oid)).await.unwrap_or(false);
        local.push(exists);
        if !exists {
            missing.push((o.oid.clone(), o.size));
        }
    }
    let upstream_has = match (&cfg.upstream.lfs, missing.is_empty()) {
        (Some(upstream), false) => {
            st.lfs_upstream
                .batch(upstream, cfg.upstream.token_env.as_deref(), &missing)
                .await
        }
        _ => HashMap::default(),
    };

    let mut objs = Vec::with_capacity(body.objects.len());
    for (o, exists) in body.objects.iter().zip(local) {
        let key = keys::lfs_key(&o.oid);
        let at_upstream = !exists && upstream_has.contains_key(&o.oid);
        let mut actions = Actions {
            download: None,
            upload: None,
            verify: None,
        };
        // git-lfs reads `authenticated` once per object and applies it to the
        // transfer *and* to `verify`: true = "the hrefs carry their own
        // authorization, add none". Only a store URL is in that position, so it
        // is set exactly where one is handed out.
        let mut authenticated = None;
        if is_upload && (exists || at_upstream) {
            // We (or the upstream) already hold it: NO `actions` key at all =
            // "server has it", the push proceeds without bytes. A verify-only
            // object is read by git-lfs's transfer queue as "upload needed,
            // then verify" and, with no local bytes (a pointer from history),
            // fails the whole push: "object … missing locally and on remote"
            //. `verify` only ever accompanies
            // an `upload` action.
            objs.push(BatchRespObject {
                oid: o.oid.clone(),
                size: o.size,
                authenticated: Some(true),
                actions: None,
                error: None,
            });
            continue;
        }
        if is_upload {
            // The bytes go straight to the bucket when the store can sign a PUT
            // that only accepts this oid's content; otherwise they come through us.
            match signed_upload(st, &cfg, &store, &key, &o.oid, o.size).await {
                Some(put) => {
                    actions.upload = Some(Action {
                        href: put.url,
                        header: Some(put.headers.into_iter().collect()),
                        expires_in: Some(put.expires_in.as_secs()),
                    });
                    authenticated = Some(true);
                    // `verify` stays walgit's either way: the store guarantees the
                    // content, we still confirm the object arrived at the promised
                    // size. It needs a credential, and `authenticated` has just
                    // told git-lfs to add none — so it carries the one the client
                    // used on this batch, the way `X-Amz-*` rides the upload href.
                    actions.verify = Some(Action {
                        href: format!("{base}/info/lfs/verify"),
                        header: crate::auth::client_authorization(headers).map(|value| {
                            std::collections::HashMap::from([(
                                axum::http::header::AUTHORIZATION.as_str().to_owned(),
                                value,
                            )])
                        }),
                        expires_in: None,
                    });
                }
                None => {
                    actions.upload = Some(Action {
                        href: format!("{base}/info/lfs/objects/{}", o.oid),
                        header: None,
                        expires_in: None,
                    });
                    actions.verify = Some(Action {
                        href: format!("{base}/info/lfs/verify"),
                        header: None,
                        expires_in: None,
                    });
                }
            }
        } else if at_upstream {
            // Streamed through us (and persisted) on GET. The upstream's batch
            // demands the exact size and a bare GET has none, so the href carries
            // it (stateless: any instance can serve the GET).
            actions.download = Some(Action {
                href: format!("{base}/info/lfs/objects/{}?size={}", o.oid, o.size),
                header: None,
                expires_in: None,
            });
        } else if exists {
            let signed = match cfg.lfs.serve_via {
                walgit_config::BundleServe::SignedUrl => store
                    .signed_get_url(&key, st.cfg.lfs.signed_url_ttl)
                    .await
                    .ok()
                    .flatten(),
                walgit_config::BundleServe::Proxy => None,
            };
            if signed.is_some() {
                authenticated = Some(true);
            }
            actions.download = Some(Action {
                href: signed.unwrap_or_else(|| format!("{base}/info/lfs/objects/{}", o.oid)),
                header: None,
                expires_in: None,
            });
        } else {
            // missing object on download: per-object 404 error
            objs.push(BatchRespObject {
                oid: o.oid.clone(),
                size: o.size,
                authenticated: None,
                actions: None,
                error: Some(LfsError {
                    code: 404,
                    message: "object not found".into(),
                }),
            });
            continue;
        }
        objs.push(BatchRespObject {
            oid: o.oid.clone(),
            size: o.size,
            authenticated,
            actions: Some(actions),
            error: None,
        });
    }
    let batch_resp = BatchResponse {
        transfer: "basic",
        objects: objs,
    };
    let json = serde_json::to_vec(&batch_resp).map_err(|e| ApiError::Internal(e.to_string()))?;
    let mut resp = (StatusCode::OK, json).into_response();
    resp.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        "application/vnd.git-lfs+json".parse().unwrap(),
    );
    // Per principal, and the answer can carry signed store URLs and the
    // credential `verify` needs: never a cache's business.
    resp.headers_mut().insert(
        axum::http::header::CACHE_CONTROL,
        axum::http::HeaderValue::from_static("no-store"),
    );
    Ok(resp)
}

/// A presigned PUT for `key`, or `None` to keep the proxying href.
///
/// Asked for only under `lfs.serve_via = "signed_url"`, and not for an object
/// bigger than `lfs.max_object_bytes` — a signed PUT bypasses walgit, so the
/// only place that cap can still be applied is the proxy path. The store answers
/// `None` unless it can sign a PUT that accepts nothing but this oid's content
/// (`ObjectStore::signed_put_url`), and a signing failure is a fallback, never a
/// failed push.
async fn signed_upload(
    st: &AppState,
    cfg: &walgit_config::Config,
    store: &walgit_store::Prefixed,
    key: &str,
    oid: &str,
    size: u64,
) -> Option<walgit_store::SignedPut> {
    if cfg.lfs.serve_via != walgit_config::BundleServe::SignedUrl
        || size > st.cfg.lfs.max_object_bytes.as_u64()
    {
        return None;
    }
    // The oid *is* the sha256 (`require_lfs_oid` checked the shape), so it is
    // also the checksum the PUT has to be bound to.
    let mut digest = [0u8; 32];
    if hex::decode_to_slice(oid, &mut digest).is_err() {
        return None;
    }
    match store
        .signed_put_url(key, st.cfg.lfs.signed_url_ttl, &digest)
        .await
    {
        Ok(put) => put,
        Err(error) => {
            tracing::warn!(oid, %error, "lfs: signing an upload failed; proxying it instead");
            None
        }
    }
}

/// `GET|HEAD /{repo}/info/lfs/objects/{oid}` — stream the object with the full
/// immutable-object contract (strong `ETag`, 304, Range/If-Range, HEAD,
/// Content-Length); see `static_object`. LFS objects are sha256-addressed.
pub async fn get_object(
    st: &AppState,
    route: &RepoRoute,
    method: &axum::http::Method,
    headers: &HeaderMap,
    query: &str,
    peer: Option<std::net::SocketAddr>,
) -> Result<Response, ApiError> {
    if !st.cfg.lfs.enabled {
        return Err(ApiError::NotFound("lfs disabled".into()));
    }
    let _ = st.auth.require_read(headers).await.map_err(auth_err)?;
    not_served_here(st, &route.id)?;
    let oid = route_sub_last(&route.subpath)?;
    require_lfs_oid(oid)?;
    let handle = open_repo(st, &route.id, false).await?;
    let store = handle.store().clone();
    let key = keys::lfs_key(oid);
    let cfg = handle.effective_config();
    if let Some(upstream) = &cfg.upstream.lfs
        && !store.exists(&key).await.unwrap_or(false)
    {
        let size = query
            .split('&')
            .find_map(|kv| kv.strip_prefix("size="))
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);
        return read_through(st, &cfg, upstream, store, oid, size, key, method).await;
    }
    match crate::static_object::serve(
        &store,
        &key,
        method,
        headers,
        crate::static_object::ServeOptions {
            accel: st.cfg.server.accel_redirect,
            peer,
            ..Default::default()
        },
    )
    .await
    {
        // The store key is ours, not the client's: name the object as git-lfs knows it.
        Err(ApiError::NotFound(_)) => Err(ApiError::NotFound(format!(
            "LFS object {oid} is not in {}",
            route.id
        ))),
        r => r,
    }
}

/// An object we lack but `lfs.upstream` has: stream it to the client while
/// tee-ing into a spool file; after a complete, sha256-verified read the spool
/// is `put` into the store (never on a short or mismatching read). No Range on
/// this path: the object is served whole once, then by `static_object`. `size`
/// comes from the href's `?size=` (GitHub's batch rejects a wrong size).
async fn read_through(
    st: &AppState,
    cfg: &walgit_config::Config,
    upstream: &str,
    store: walgit_store::Prefixed,
    oid: &str,
    size: u64,
    key: String,
    method: &axum::http::Method,
) -> Result<Response, ApiError> {
    let found = st
        .lfs_upstream
        .batch(
            upstream,
            cfg.upstream.token_env.as_deref(),
            &[(oid.to_string(), size)],
        )
        .await;
    let Some(obj) = found.get(oid).cloned() else {
        return Err(ApiError::NotFound("object not found".into()));
    };
    if *method == axum::http::Method::HEAD {
        return Ok(Response::builder()
            .status(StatusCode::OK)
            .header(axum::http::header::CONTENT_LENGTH, obj.size)
            .header(axum::http::header::CONTENT_TYPE, "application/octet-stream")
            .body(Body::empty())
            .unwrap());
    }
    let (len, mut upstream_body) = st
        .lfs_upstream
        .open(&obj)
        .await
        .map_err(|e| ApiError::ServiceUnavailable(format!("lfs upstream: {e}")))?;
    let spool_dir = st.cfg.cache.dir.join("lfs-spool");
    tokio::fs::create_dir_all(&spool_dir)
        .await
        .map_err(|e| ApiError::Internal(e.to_string()))?;
    let spool_path = spool_dir.join(format!("{oid}.{}", uuid::Uuid::new_v4()));
    let mut spool = tokio::fs::File::create(&spool_path)
        .await
        .map_err(|e| ApiError::Internal(e.to_string()))?;
    let expected_oid = oid.to_string();
    let expected_len = len;
    let repo_key = key.clone();
    let (tx, rx) = tokio::sync::mpsc::channel::<std::io::Result<Bytes>>(8);
    tokio::spawn(async move {
        use futures::StreamExt;
        use sha2::{Digest, Sha256};
        use tokio::io::AsyncWriteExt;
        let mut hasher = Sha256::new();
        let mut got: u64 = 0;
        let mut complete = true;
        while let Some(chunk) = upstream_body.next().await {
            match chunk {
                Ok(b) => {
                    hasher.update(&b);
                    got += b.len() as u64;
                    if spool.write_all(&b).await.is_err() {
                        complete = false;
                    }
                    if tx.send(Ok(b)).await.is_err() {
                        // Client went away: keep pulling so the object still lands in the store.
                        tracing::debug!(oid = %expected_oid, "lfs read-through: client gone, finishing for the store");
                    }
                }
                Err(e) => {
                    complete = false;
                    let _ = tx.send(Err(e)).await;
                    break;
                }
            }
        }
        drop(tx);
        let ok = complete
            && got == expected_len
            && hex::encode(hasher.finalize()) == expected_oid
            && spool.flush().await.is_ok();
        drop(spool);
        if ok {
            match store
                .put(
                    &repo_key,
                    PutBody::File(spool_path.clone()),
                    PutMode::Overwrite.into(),
                )
                .await
            {
                Ok(_) => {
                    metrics::counter!("walgit_lfs_upstream_total", "op" => "persist", "result" => "ok").increment(1);
                    tracing::info!(oid = %expected_oid, bytes = got, "lfs read-through: persisted from upstream");
                }
                Err(error) => {
                    metrics::counter!("walgit_lfs_upstream_total", "op" => "persist", "result" => "error").increment(1);
                    tracing::warn!(oid = %expected_oid, %error, "lfs read-through: store put failed");
                }
            }
        } else {
            metrics::counter!("walgit_lfs_upstream_total", "op" => "persist", "result" => "incomplete").increment(1);
            tracing::warn!(oid = %expected_oid, bytes = got, expected = expected_len, "lfs read-through: short or mismatching upstream read; not persisted");
        }
        let _ = tokio::fs::remove_file(&spool_path).await;
    });
    let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(axum::http::header::CONTENT_LENGTH, len)
        .header(axum::http::header::CONTENT_TYPE, "application/octet-stream")
        .header(axum::http::header::CACHE_CONTROL, "no-store")
        .body(Body::from_stream(stream))
        .unwrap())
}

/// `PUT /{repo}/info/lfs/objects/{oid}` — stream upload, verify size + sha256.
pub async fn put_object(
    st: &AppState,
    route: &RepoRoute,
    headers: &HeaderMap,
    body: Body,
) -> Result<Response, ApiError> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use sha2::{Digest, Sha256};

    if !st.cfg.lfs.enabled {
        return Err(ApiError::NotFound("lfs disabled".into()));
    }
    let _ = st.auth.require_write(headers).await.map_err(auth_err)?;
    not_served_here(st, &route.id)?;
    let oid = route_sub_last(&route.subpath)?;
    require_lfs_oid(oid)?;
    let handle = open_repo(st, &route.id, false).await?;
    let store = handle.store().clone();
    let key = keys::lfs_key(oid);
    let max = st.cfg.lfs.max_object_bytes.as_u64();

    let tmp = tempfile::NamedTempFile::new().map_err(|e| ApiError::Internal(e.to_string()))?;
    let mut file = tokio::fs::File::from_std(
        tmp.reopen()
            .map_err(|e| ApiError::Internal(e.to_string()))?,
    );
    let mut reader = body_to_async_read(body);
    let mut hasher = Sha256::new();
    let mut n = 0u64;
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let k = reader
            .read(&mut buf)
            .await
            .map_err(|e| ApiError::Internal(e.to_string()))?;
        if k == 0 {
            break;
        }
        n += k as u64;
        if n > max {
            return Err(ApiError::PayloadTooLarge);
        }
        hasher.update(&buf[..k]);
        file.write_all(&buf[..k])
            .await
            .map_err(|e| ApiError::Internal(e.to_string()))?;
    }
    file.flush()
        .await
        .map_err(|e| ApiError::Internal(e.to_string()))?;
    drop(file);
    if hex::encode(hasher.finalize()) != oid {
        return Err(ApiError::BadRequest("lfs object sha256 mismatch".into()));
    }
    store
        .put(
            &key,
            PutBody::File(tmp.path().to_path_buf()),
            PutMode::Overwrite.into(),
        )
        .await
        .map_err(store_err)?;
    Ok(StatusCode::OK.into_response())
}

/// `POST /{repo}/info/lfs/verify`
pub async fn verify(
    st: &AppState,
    route: &RepoRoute,
    headers: &HeaderMap,
    body_bytes: Bytes,
) -> Result<Response, ApiError> {
    if !st.cfg.lfs.enabled {
        return Err(ApiError::NotFound("lfs disabled".into()));
    }
    let body: BatchObject = serde_json::from_slice(&body_bytes)
        .map_err(|e| ApiError::BadRequest(format!("invalid lfs verify: {e}")))?;
    require_lfs_oid(&body.oid)?;
    let _ = st.auth.require_write(headers).await.map_err(auth_err)?;
    let handle = open_repo(st, &route.id, false).await?;
    let store = handle.store().clone();
    let key = keys::lfs_key(&body.oid);
    let meta = store.head(&key).await.map_err(store_err)?;
    match meta {
        Some(m) if m.size == body.size => Ok(StatusCode::OK.into_response()),
        Some(_) => Err(ApiError::BadRequest("lfs size mismatch".into())),
        None => Err(ApiError::NotFound(body.oid.clone())),
    }
}

/// Placement (D29/D30): LFS transfer is object work; a host that does not
/// serve the repository answers 503 (`ApiError::ServiceUnavailable` carries
/// `Retry-After`) before touching the store.
fn not_served_here(st: &AppState, id: &walgit_git::RepoId) -> Result<(), ApiError> {
    if st.cfg.placement.serves(id.owner(), id.name()) {
        Ok(())
    } else {
        metrics::counter!("walgit_not_served_here_total", "service" => "lfs").increment(1);
        Err(ApiError::ServiceUnavailable(format!(
            "{id} is not served by this host; retry shortly"
        )))
    }
}

fn require_lfs_oid(oid: &str) -> Result<(), ApiError> {
    if keys::lfs_oid_ok(oid) {
        Ok(())
    } else {
        Err(ApiError::BadRequest("invalid lfs oid".into()))
    }
}

fn route_sub_last(subpath: &str) -> Result<&str, ApiError> {
    subpath
        .rsplit('/')
        .next()
        .ok_or_else(|| ApiError::NotFound("object id".into()))
}

fn base_url(st: &AppState, route: &RepoRoute, headers: &HeaderMap) -> String {
    format!(
        "{}/{}",
        crate::smart::request_base_url(st, headers),
        route.id
    )
}

fn auth_err(e: crate::auth::AuthError) -> ApiError {
    match e {
        crate::auth::AuthError::Invalid | crate::auth::AuthError::Unauthorized => {
            ApiError::Unauthorized
        }
        crate::auth::AuthError::Forbidden => ApiError::Forbidden,
        crate::auth::AuthError::Unavailable => {
            ApiError::ServiceUnavailable("auth provider unavailable".into())
        }
    }
}
fn store_err(e: walgit_store::StoreError) -> ApiError {
    e.into()
}
