//! Admin endpoints: `PUT /{owner}/{repo}` (create), `DELETE /{owner}/{repo}`
//! (delete manifest + prefix objects), `GET /` (list repos, text/plain).

use std::collections::HashMap;

use axum::body::Body;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use serde::Deserialize;
use walgit_git::{ObjectFormat, gix_hash};
use walgit_proto::v1::{RefTransaction, RefUpdate};

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

/// `DELETE /{owner}/{repo}` — delete manifest + all objects under the repo prefix.
pub async fn delete(
    st: &AppState,
    route: &RepoRoute,
    headers: &HeaderMap,
) -> Result<Response, ApiError> {
    let _principal = st.auth.require_write(headers).await.map_err(auth_err)?;
    st.registry.delete(&route.id).await.map_err(wal_err)?;
    Ok((StatusCode::NO_CONTENT, "").into_response())
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProtectedRefRequest {
    name: String,
    oid: String,
}

/// Create a configured protected ref through the management API.
pub async fn create_protected_ref(
    st: &AppState,
    route: &RepoRoute,
    headers: &HeaderMap,
    body: Body,
) -> Result<Response, ApiError> {
    let principal = st.auth.require_admin(headers).await.map_err(auth_err)?;
    let bytes = crate::collect_body(body).await?;
    let request: ProtectedRefRequest = serde_json::from_slice(&bytes)
        .map_err(|error| ApiError::BadRequest(format!("invalid protected ref request: {error}")))?;
    walgit_git::validate_ref_name(&request.name)
        .map_err(|error| ApiError::BadRequest(error.to_string()))?;
    if !st
        .cfg
        .git
        .protected_ref_prefixes
        .iter()
        .any(|prefix| request.name.starts_with(prefix))
    {
        return Err(ApiError::BadRequest(
            "ref is not in a configured protected namespace".into(),
        ));
    }
    let oid = gix_hash::ObjectId::from_hex(request.oid.as_bytes())
        .map_err(|_| ApiError::BadRequest("invalid object id".into()))?;
    let oid_hex = oid.to_string();
    let handle = st.registry.open(&route.id).await.map_err(wal_err)?;
    let (guard, access) = handle.sync_objects().await.map_err(wal_err)?;
    let is_commit = match access {
        walgit_wal::ObjectAccess::Local => handle.local().is_commit(&oid).map_err(git_err)?,
        walgit_wal::ObjectAccess::Remote(remote) => remote
            .header(&oid)
            .await
            .map_err(wal_err)?
            .is_some_and(|(kind, _)| kind == gix_object::Kind::Commit),
    };
    if !is_commit {
        return Err(ApiError::NotFound(format!("commit {oid_hex}")));
    }
    let refs = handle.local().refs().map_err(git_err)?;
    if let Some(existing) = refs.refs.iter().find(|reference| reference.name == request.name) {
        if existing.oid == oid_hex {
            return Ok(StatusCode::NO_CONTENT.into_response());
        }
        return Err(ApiError::Conflict(format!(
            "{} already points to {}",
            request.name, existing.oid
        )));
    }
    let transaction = RefTransaction {
        updates: vec![RefUpdate {
            name: request.name,
            old_oid: "0".repeat(oid_hex.len()),
            new_oid: oid_hex.clone(),
            new_symbolic_target: String::new(),
            new_peeled: String::new(),
        }],
        push_options: Vec::new(),
        atomic: true,
    };
    let mut meta = HashMap::new();
    meta.insert("principal".to_string(), principal.name);
    drop(guard);
    let published = handle
        .publish_ref_update(transaction, meta)
        .await
        .map_err(wal_err)?;
    if let Some((_, Err(error))) = published.per_ref.into_iter().next() {
        if matches!(
            &error,
            walgit_wal::RefError::Conflict { actual, .. } if actual == &oid_hex
        ) {
            return Ok(StatusCode::NO_CONTENT.into_response());
        }
        return Err(ApiError::Conflict(error.to_string()));
    }
    Ok(StatusCode::NO_CONTENT.into_response())
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
fn git_err(e: walgit_git::GitError) -> ApiError {
    ApiError::Internal(format!("git: {e}"))
}
fn wal_err(e: walgit_wal::WalError) -> ApiError {
    match &e {
        walgit_wal::WalError::NotFound => ApiError::NotFound(e.to_string()),
        _ => ApiError::Internal(format!("wal: {e}")),
    }
}
