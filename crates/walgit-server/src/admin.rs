//! Admin endpoints: `PUT /{owner}/{repo}` (create), `DELETE /{owner}/{repo}`
//! (delete manifest + prefix objects), `GET /` (list repos, text/plain).

use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use walgit_git::ObjectFormat;

use crate::AppState;
use crate::error::ApiError;
use crate::repo::RepoRoute;

/// `PUT /{owner}/{repo}` — create repo. 201 on new, 409 if it exists.
pub async fn create(
    st: &AppState,
    route: &RepoRoute,
    headers: &HeaderMap,
    query: &str,
) -> Result<Response, ApiError> {
    let _principal = st.auth.require_write(headers).await.map_err(auth_err)?;
    let format = match query
        .split('&')
        .find_map(|part| part.strip_prefix("object_format="))
    {
        Some("sha256") => ObjectFormat::Sha256,
        Some("sha1") => ObjectFormat::Sha1,
        Some(other) => {
            return Err(ApiError::BadRequest(format!(
                "unsupported object format: {other}"
            )));
        }
        None => ObjectFormat::from(st.cfg.git.object_format),
    };
    match st.registry.create(&route.id, format).await {
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

/// `GET /` — list repos as text/plain, one `owner/name` per line.
pub async fn list_repos(st: &AppState, headers: &HeaderMap) -> Result<Response, ApiError> {
    let principal = st.auth.require_read(headers).await.map_err(auth_err)?;
    let repos = st.registry.list().await.map_err(wal_err)?;
    let body = repos
        .into_iter()
        .filter(|r| principal.sees_owner(r.owner()))
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
        crate::auth::AuthError::UntrustedProxy => ApiError::UntrustedProxy,
        crate::auth::AuthError::Unavailable => {
            ApiError::ServiceUnavailable("auth provider unavailable".into())
        }
    }
}
fn wal_err(e: walgit_wal::WalError) -> ApiError {
    match &e {
        walgit_wal::WalError::NotFound => ApiError::NotFound(e.to_string()),
        _ => ApiError::Internal(format!("wal: {e}")),
    }
}
