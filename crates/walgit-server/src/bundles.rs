//! Bundle serving: `GET /{repo}/bundles/list` (git bundle-list text, no-cache)
//! and `GET|HEAD /{repo}/bundles/{strategy}/{name}` (streamed bundle with
//! strong `ETag` = store version, immutable caching, Range/If-Range,
//! If-None-Match, HEAD — `static_object`).

use axum::http::{HeaderMap, Method, StatusCode};
use axum::response::{IntoResponse, Response};

use crate::AppState;
use crate::error::ApiError;
use crate::repo::RepoRoute;
use crate::smart::open_repo;
use crate::static_object;

/// `GET /{repo}/bundles/list[?filter=blob:none]` — the clone list: the unfiltered chain by
/// default (what the protocol advertises); `?filter=blob:none` is the blobless
/// family for `git clone --filter=blob:none --bundle-uri=<this URL>` (git does
/// not match `bundle.<id>.filter` itself, so the two never share a list).
/// `GET /{repo}/bundles/catchup[?filter=blob:none]` — the same list without the fulls: what the
/// recipes record in `fetch.bundleURI`, so a fetch only ever walks incremental links (a full newer
/// than the client's token would otherwise be downloaded whole).
pub async fn list(
    st: &AppState,
    route: &RepoRoute,
    headers: &HeaderMap,
    query: &str,
    fulls: bool,
) -> Result<Response, ApiError> {
    if !st.cfg.bundles.advertise {
        return Err(ApiError::NotFound("bundles disabled".into()));
    }
    let principal = st.auth.require_read(headers).await.map_err(auth_err)?;
    // This principal tried bundle-uri (see `smart::bundle_fallback_allowed`).
    st.caches.bundle_attempts.insert(
        format!("{}\0{}", route.id, principal.name),
        std::time::Instant::now(),
    );
    let handle = open_repo(st, &route.id, false).await?;
    let base = crate::smart::request_base_url(st, headers);
    let filter: Option<String> = query
        .split('&')
        .find_map(|kv| {
            kv.strip_prefix("filter=")
                .map(|v| v.replace("%3A", ":").replace("%3a", ":"))
        })
        .filter(|v| !v.is_empty());
    if let Some(f) = &filter
        && f != "blob:none"
    {
        return Err(ApiError::BadRequest(format!(
            "unsupported bundle filter {f:?} (blob:none)"
        )));
    }

    // Rendered-list cache keyed by the list object's OWN version (one metadata
    // probe, ~15 ms): a bundle published by any host changes `bundles/list.pb`,
    // never the manifest, so this — not a TTL — is the invariant that a fresh
    // clone never sees a stale list (2026-08-21: 20 minutes stale on the host
    // that had just published). The building host additionally invalidates.
    let repo_key = format!(
        "{}?{}{}",
        route.id,
        filter.as_deref().unwrap_or(""),
        if fulls { "" } else { "&catchup" }
    );
    let list_version =
        walgit_store::ObjectStore::head(handle.store(), walgit_proto::keys::BUNDLE_LIST)
            .await
            .map_err(ApiError::from)?
            .map(|m| m.version.to_string());
    let Some(list_version) = list_version else {
        return Err(ApiError::NotFound("no bundles".into()));
    };
    if let Some(cached) = st.caches.bundle_list.get(&repo_key, &list_version) {
        return Ok(render_bundle_list_response(cached));
    }

    let text = st
        .bundles
        .render_list(&route.id, &base, filter.as_deref(), fulls)
        .await
        .map_err(bundle_err)?;
    match text {
        Some(t) => {
            st.caches
                .bundle_list
                .insert(&repo_key, &list_version, t.clone());
            Ok(render_bundle_list_response(t))
        }
        None => Err(ApiError::NotFound("no bundles".into())),
    }
}

fn render_bundle_list_response(text: String) -> Response {
    let mut resp = (StatusCode::OK, text).into_response();
    let h = resp.headers_mut();
    h.insert(
        axum::http::header::CONTENT_TYPE,
        "text/plain; charset=utf-8".parse().unwrap(),
    );
    h.insert(
        axum::http::header::CACHE_CONTROL,
        "no-cache".parse().unwrap(),
    );
    resp
}

/// `GET|HEAD /{repo}/bundles/{strategy}/{name}` — streamed from the store
/// with the full immutable-object contract (strong `ETag`, 304, Range/If-Range,
/// HEAD, Content-Length); see `static_object`.
pub async fn object(
    st: &AppState,
    route: &RepoRoute,
    method: &Method,
    headers: &HeaderMap,
    peer: Option<std::net::SocketAddr>,
) -> Result<Response, ApiError> {
    let _ = st.auth.require_read(headers).await.map_err(auth_err)?;
    let handle = open_repo(st, &route.id, false).await?;
    let store = handle.store().clone();

    // subpath = "bundles/{strategy}/{name}"
    let key = format!(
        "bundles/{}",
        route
            .subpath
            .strip_prefix("bundles/")
            .unwrap_or(&route.subpath)
    );
    if key.split('/').any(|seg| seg.is_empty() || seg == "..") {
        return Err(ApiError::NotFound("bad bundle path".into()));
    }
    static_object::serve(
        &store,
        &key,
        method,
        headers,
        static_object::ServeOptions {
            content_type: "application/x-git-bundle",
            accel: st.cfg.server.accel_redirect,
            peer,
            ..Default::default()
        },
    )
    .await
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
fn bundle_err(e: walgit_bundle::BundleError) -> ApiError {
    ApiError::Internal(format!("bundle: {e}"))
}

/// Full bundle = header ∘ the single tier-2 base pack, refs from the checkpoint
/// at the base's seq (written now when the base is at head and none exists).
/// Full bundle = header (refs at the base's seq) ∘ tier-2 base pack via GCS
/// compose — zero bytes through this host, no index-pack, no disk. The weekly
/// for any repository that has a tier-2 base (a large repository); `slot` is the calendar
/// slot the bundle stands for (its `creationToken`; 0 = "now").
pub async fn compose_full_from_base(
    registry: &walgit_wal::Registry,
    id: &walgit_git::RepoId,
    strategy: &str,
    cfg: &walgit_config::Config,
    slot: u64,
) -> anyhow::Result<walgit_proto::v1::BundleEntry> {
    use tracing::info;
    use walgit_proto::prost::Message;
    use walgit_store::ObjectStoreExt;
    let handle = registry.open(id).await?;
    drop(handle.sync_refs().await?);
    let manifest = handle.manifest();
    // The base is the tier-2 pack that is not a derived history pack (D18:
    // `compact --base` publishes both at tier 2; the weekly composes the base).
    let bases = walgit_wal::base_packs(&manifest);
    anyhow::ensure!(
        bases.len() == 1,
        "compose needs exactly one tier-2 base pack (found {}; history packs excluded): an imported pack set — the base rebuild unit (`compact --base`) collapses it first",
        bases.len()
    );
    let base = bases[0].clone();
    let seq = base.seq;
    let store = handle.store();
    // Refs at the base's seq: the checkpoint there when one exists (the rebuild checkpoints right
    // after publishing), else replayed from the WAL — a checkpoint at or before the base's seq plus
    // the ref transactions through it. Pushes that landed since the rebuild no longer matter (the
    // rig's weekly compose failed every pass for as long as the churn kept refs moving, 2026-08-22);
    // only a log folded away below the base's seq with no checkpoint before it is unrecoverable.
    let refs_key = walgit_proto::keys::checkpoint_refs_key(seq);
    let snap = if let Some((_, bytes)) = store.get_bytes(&refs_key).await? {
        walgit_proto::v1::RefSnapshot::decode(bytes.as_ref())?
    } else {
        info!(
            base_seq = seq,
            head = manifest.head_seq,
            "no checkpoint at the base's seq: replaying the refs at that seq from the WAL for the compose"
        );
        handle.refs_at_seq(seq).await.map_err(|e| {
            anyhow::anyhow!("refs at the base's seq {seq} (head {}): {e} — run `walgit compact --base` again so a checkpoint exists at the base", manifest.head_seq)
        })?
    };
    let list = walgit_bundle::ops::read_list(store)
        .await?
        .unwrap_or_default();
    let prev_token = walgit_bundle::ops::max_creation_token(&list);
    let now = std::time::SystemTime::now();
    let format = handle.local().object_format();
    // A filtered (blobless) full strategy composes the D18 **history pack** of
    // the base (commits + trees = exactly `--filter=blob:none` of the refs at
    // the base's seq) under a `@filter=blob:none` header; the unfiltered one
    // composes the base itself.
    let filter = cfg
        .bundles
        .strategy
        .iter()
        .find(|s| s.name == strategy)
        .and_then(|s| s.filter.clone());
    let pack = match &filter {
        Some(_) => manifest
            .packs
            .iter()
            .filter(|p| p.kind == walgit_proto::v1::PackKind::History as i32 && p.derived_from == base.checksum)
            .max_by_key(|p| p.seq)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("strategy {strategy} is filtered but base {} has no history pack (D18) to compose; rebuild the base with git.history_pack on", &base.checksum[..12]))?,
        None => base.clone(),
    };
    let pack_path = handle
        .local()
        .pack_path(&walgit_git::gix_hash::ObjectId::from_hex(
            pack.checksum.as_bytes(),
        )?);
    let entry = walgit_bundle::ops::compose_full(
        store,
        &pack.checksum,
        pack.pack_size,
        pack_path.is_file().then_some(pack_path.as_path()),
        &snap,
        format,
        strategy,
        seq,
        prev_token,
        now,
        slot,
        filter.as_deref(),
    )
    .await?;
    let new_entry = entry.clone();
    let old_list = list;
    let cas = walgit_bundle::ops::cas_update_list(store, cfg.wal.cas_max_retries, |current| {
        let mut l = current.cloned().unwrap_or_default();
        l.mode = "all".into();
        l.heuristic = "creationToken".into();
        l.bundles.push(new_entry.clone());
        walgit_bundle::slots::retain(&cfg.bundles, &mut l);
        l.updated_at = Some(walgit_proto::time::now());
        Ok(Some(l))
    })
    .await?;
    if let Some((_, new_list)) = &cas {
        let pruned = walgit_bundle::ops::pruned_diff(&old_list, new_list);
        if !pruned.is_empty() {
            walgit_bundle::ops::delete_pruned(store, &pruned).await;
        }
    }
    Ok(entry)
}
