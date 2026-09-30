//! Admin endpoints: `PUT /{owner}/{repo}` (create), `DELETE /{owner}/{repo}`
//! (delete manifest + prefix objects), `PUT /{o}/{r}/api/head` (move HEAD),
//! `GET /` (list repos, text/plain).

use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use walgit_git::ObjectFormat;

use crate::AppState;
use crate::error::ApiError;
use crate::repo::RepoRoute;

/// `PUT /{owner}/{repo}[?object_format=sha1|sha256][&default_branch=<name>]` — create repo.
/// 201 on new, 409 if it exists, 400 for an unsupported format or an invalid branch name.
/// `default_branch` (a short name) overrides `git.default_branch` as HEAD's target (D50).
pub async fn create(
    st: &AppState,
    route: &RepoRoute,
    headers: &HeaderMap,
    query: &str,
) -> Result<Response, ApiError> {
    let _principal = st.auth.require_write(headers).await.map_err(auth_err)?;
    let param = |key: &str| {
        query
            .split('&')
            .filter_map(|kv| kv.split_once('='))
            .find(|(k, _)| *k == key)
            .map(|(_, v)| crate::settings::percent_decode(v))
    };
    let branch = param("default_branch").unwrap_or_else(|| st.cfg.git.default_branch.clone());
    walgit_config::refs::branch_ref(&branch)
        .map_err(|e| ApiError::BadRequest(format!("default_branch: {e}")))?;
    let format = match param("object_format").as_deref() {
        Some("sha256") => ObjectFormat::Sha256,
        Some("sha1") => ObjectFormat::Sha1,
        Some(other) => {
            return Err(ApiError::BadRequest(format!(
                "unsupported object format: {other}"
            )));
        }
        None => ObjectFormat::from(st.cfg.git.object_format),
    };
    match st
        .registry
        .create_with_default_branch(&route.id, format, &branch)
        .await
    {
        Ok(_h) => Ok((StatusCode::CREATED, "created").into_response()),
        Err(walgit_wal::WalError::AlreadyExists) => {
            Ok((StatusCode::CONFLICT, "already exists").into_response())
        }
        Err(e) => Err(wal_err(e)),
    }
}

/// `DELETE /{owner}/{repo}` — admin-only deletion of the manifest and every object under the repo prefix.
pub async fn delete(
    st: &AppState,
    route: &RepoRoute,
    headers: &HeaderMap,
) -> Result<Response, ApiError> {
    let _principal = st.auth.require_admin(headers).await.map_err(auth_err)?;
    st.registry.delete(&route.id).await.map_err(wal_err)?;
    Ok((StatusCode::NO_CONTENT, "").into_response())
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct HeadRequest {
    /// Short branch name (`main`), as `git.default_branch`.
    branch: String,
}

/// `PUT /{o}/{r}/api[-browser]/head` `{"branch": "<name>"}` — point HEAD at an existing
/// branch (admin). One WAL entry through the push publisher (`RepoHandle::publish_head`):
/// CAS-checked like any ref update, the branch's existence re-checked on every attempt.
/// 200 `{head: {name, sha}, seq}` (`seq` 0 = HEAD was already there, nothing written);
/// 400 invalid name/body, 404 unknown repository, 409 the branch does not exist.
pub async fn set_head(
    st: &AppState,
    route: &RepoRoute,
    headers: &HeaderMap,
    body: axum::body::Body,
) -> Result<Response, ApiError> {
    let principal = st.auth.require_admin(headers).await.map_err(auth_err)?;
    let bytes = crate::collect_body(body).await?;
    let req: HeadRequest = serde_json::from_slice(&bytes)
        .map_err(|e| ApiError::BadRequest(format!("expected {{\"branch\": \"<name>\"}}: {e}")))?;
    let target = walgit_config::refs::branch_ref(&req.branch)
        .map_err(|e| ApiError::BadRequest(format!("branch: {e}")))?;
    let handle = st.registry.open(&route.id).await.map_err(wal_err)?;
    let meta = std::collections::HashMap::from([("principal".to_string(), principal.name)]);
    let result = handle.publish_head(&target, meta).await.map_err(wal_err)?;
    if let Some((_, Err(e))) = result.per_ref.iter().find(|(name, _)| name == "HEAD") {
        return Err(ApiError::Conflict(e.to_string()));
    }
    let sha = handle
        .local()
        .ref_view()
        .map_err(|e| ApiError::Internal(format!("refs: {e}")))?
        .get(&target)
        .unwrap_or_default();
    Ok((
        StatusCode::OK,
        axum::Json(serde_json::json!({
            "head": {"name": req.branch, "sha": sha},
            "seq": result.seq,
        })),
    )
        .into_response())
}

/// `GET /` — list repos as text/plain, one `owner/name` per line.
pub async fn list_repos(st: &AppState, headers: &HeaderMap) -> Result<Response, ApiError> {
    let _ = st.auth.require_read(headers).await.map_err(auth_err)?;
    let repos = st.registry.list().await.map_err(wal_err)?;
    let body = repos
        .into_iter()
        .map(|r| r.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    Ok((
        StatusCode::OK,
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; charset=utf-8",
        )],
        body,
    )
        .into_response())
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
fn wal_err(e: walgit_wal::WalError) -> ApiError {
    match &e {
        walgit_wal::WalError::NotFound => ApiError::NotFound(e.to_string()),
        walgit_wal::WalError::Invalid(why) => ApiError::BadRequest(why.clone()),
        _ => ApiError::Internal(format!("wal: {e}")),
    }
}
