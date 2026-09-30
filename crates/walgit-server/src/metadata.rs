//! Human-readable metadata beside the ids: a repository's **description** and an
//! owner's **profile** (display name + description).
//!
//! Everything in walgit is keyed by `owner/repo`. A deployment whose owners and
//! repositories are opaque ids (UUIDs from another system) has nowhere else to keep the
//! name a person reads, so the owner list and repo lists are unreadable. These are two
//! small control objects in the bucket, shaped like `policy.json` (D16) and for the same
//! reasons *not* on the WAL: a label is not repository state, must not advance
//! `head_seq`, must not ride every refs-level sync, and must never be read by git paths.
//!
//! | Object | Key | Document |
//! |---|---|---|
//! | repository description | `repos/<o>/<r>/description.json` | `{"description": "…"}` |
//! | owner profile | `owners/<o>/profile.json` | `{"display_name": "…", "description": "…"}` (each optional, at least one) |
//!
//! The owner profile lives at the bucket root, not under `repos/<o>/`: `repos/` holds
//! repositories only and its delimited listing is how the registry finds them
//! (`Registry::list`), and an owner is not a routing unit (D26) — its surface is the
//! non-repository `/api/v1/owners/{owner}`. **Owners stay implicit**: a profile does not
//! create an owner, listings still derive owners from repository ids, and a profile whose
//! owner has no repository is simply not listed (it reappears with the first repository —
//! which is what lets an operator provision the name before the first push).
//!
//! Reads revalidate every time (principle IV): one GET per object, no in-process cache —
//! there is none for small control objects to reuse (`policy.json` is read the same way),
//! and a TTL here would be exactly the invented staleness the principle forbids. The one
//! place that multiplies GETs is `?detail=1` on the owner/repo listings: one GET per listed
//! item, `DETAIL_CONCURRENCY` in flight, only on that opt-in call and never on a git path
//! (`docs/ROUNDTRIPS.md` §2). Writes are last-writer-wins overwrites (the same as
//! `policy.json`): there is nothing to merge in a label.
//!
//! Validation is strict on write (unknown keys, non-strings, control characters, length)
//! and lenient on read: unknown keys are ignored so two binaries can share one bucket
//! during a roll, and an object that does not parse or is implausibly large is logged and
//! served as absent rather than failing a listing.

use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use futures::{StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use walgit_git::RepoId;
use walgit_proto::keys;
use walgit_store::{DynStore, GetOptions, GetResult, PutBody, PutMode, StoreError};

use crate::AppState;
use crate::error::ApiError;
use crate::repo::RepoRoute;
use crate::web::api::{auth_err, json_swr_digest};

/// A repository or owner description, in characters (GitHub's is 350; a line, not a README).
pub const DESCRIPTION_MAX_CHARS: usize = 512;
/// An owner's display name, in characters.
pub const DISPLAY_NAME_MAX_CHARS: usize = 100;
/// Upper bound on a request body or a stored object before it is parsed. Generous for the
/// limits above (4 bytes per char, JSON escaping), small enough that a hand-written or
/// hostile object cannot make a listing download megabytes per item.
pub const MAX_DOCUMENT_BYTES: usize = 8 * 1024;
/// GETs in flight for one `?detail=1` listing — the registry's manifest-HEAD fan-out width
/// (`Registry::list`), on the control-plane transport (D19), never with bulk bytes.
pub const DETAIL_CONCURRENCY: usize = 32;

// ---------------------------------------------------------------------------
// Documents
// ---------------------------------------------------------------------------

/// `repos/<o>/<r>/description.json`. Missing object = `{}`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepoDescription {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// `owners/<o>/profile.json`. Missing object = `{}`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OwnerProfile {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

impl OwnerProfile {
    pub fn is_empty(&self) -> bool {
        self.display_name.is_none() && self.description.is_none()
    }
}

/// One row of `GET /api/v1/owners?detail=1`, and the body of `GET /api/v1/owners/{owner}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct OwnerEntry {
    pub name: String,
    #[serde(flatten)]
    pub profile: OwnerProfile,
}

/// One row of `GET /api/v1/owners/{owner}/repos?detail=1`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RepoEntry {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// Plain text on one line: trimmed, non-empty, no control characters (a newline in a
/// list row or a `<title>` is a layout bug waiting to happen; markup is not interpreted —
/// the UI renders text), at most `max` characters.
pub fn clean_text(field: &str, value: &str, max: usize) -> Result<String, String> {
    let v = value.trim();
    if v.is_empty() {
        return Err(format!("{field} is empty (DELETE clears it)"));
    }
    if v.chars().any(char::is_control) {
        return Err(format!(
            "{field} must be plain text on one line (no control characters)"
        ));
    }
    let n = v.chars().count();
    if n > max {
        return Err(format!("{field} is {n} characters; the limit is {max}"));
    }
    Ok(v.to_string())
}

/// Strict write-side parse: a JSON object whose keys are all in `allowed` and whose values
/// are strings. A typo (`displayName`) must be a 400, not a silently empty field.
fn parse_object(bytes: &[u8], allowed: &[&str]) -> Result<Map<String, Value>, String> {
    if bytes.len() > MAX_DOCUMENT_BYTES {
        return Err(format!(
            "document is {} bytes; the limit is {MAX_DOCUMENT_BYTES}",
            bytes.len()
        ));
    }
    let Value::Object(map) =
        serde_json::from_slice::<Value>(bytes).map_err(|e| format!("invalid JSON: {e}"))?
    else {
        return Err("expected a JSON object".into());
    };
    for (k, v) in &map {
        if !allowed.contains(&k.as_str()) {
            return Err(format!(
                "unknown key {k:?} (expected {})",
                allowed.join(", ")
            ));
        }
        if !v.is_string() {
            return Err(format!("{k} must be a string"));
        }
    }
    Ok(map)
}

fn text_field(map: &Map<String, Value>, key: &str, max: usize) -> Result<Option<String>, String> {
    map.get(key)
        .and_then(Value::as_str)
        .map(|s| clean_text(key, s, max))
        .transpose()
}

/// `PUT …/description` body → a validated description.
pub fn parse_description(bytes: &[u8]) -> Result<RepoDescription, String> {
    let map = parse_object(bytes, &["description"])?;
    let description = text_field(&map, "description", DESCRIPTION_MAX_CHARS)?
        .ok_or("missing \"description\" (DELETE clears it)")?;
    Ok(RepoDescription {
        description: Some(description),
    })
}

/// `PUT /api/v1/owners/{owner}` body → a validated profile. `name` is accepted so a
/// client can PUT back what it GOT, but only when it is the owner in the path: the id is
/// the key, never a field to rename.
pub fn parse_profile(owner: &str, bytes: &[u8]) -> Result<OwnerProfile, String> {
    let map = parse_object(bytes, &["display_name", "description", "name"])?;
    if let Some(name) = map.get("name").and_then(Value::as_str)
        && name != owner
    {
        return Err(format!(
            "name {name:?} differs from the owner in the path ({owner:?}); the id cannot be changed"
        ));
    }
    let profile = OwnerProfile {
        display_name: text_field(&map, "display_name", DISPLAY_NAME_MAX_CHARS)?,
        description: text_field(&map, "description", DESCRIPTION_MAX_CHARS)?,
    };
    if profile.is_empty() {
        return Err("empty profile: set display_name and/or description (DELETE clears it)".into());
    }
    Ok(profile)
}

// ---------------------------------------------------------------------------
// Store
// ---------------------------------------------------------------------------

/// GET a small JSON control object; absent, oversized or unparsable = `T::default()`.
/// One request on every path: a 404 is the "none" answer, never a HEAD first.
async fn load_json<T: for<'de> Deserialize<'de> + Default>(
    store: &DynStore,
    key: &str,
) -> Result<T, StoreError> {
    let got = match store.get(key, GetOptions::default()).await {
        Ok(got) => got,
        Err(StoreError::NotFound { .. }) => return Ok(T::default()),
        Err(e) => return Err(e),
    };
    if let GetResult::Object { meta, .. } = &got
        && meta.size > MAX_DOCUMENT_BYTES as u64
    {
        tracing::warn!(key, size = meta.size, "metadata object too large; ignored");
        return Ok(T::default());
    }
    let Some((_, bytes)) = got.bytes().await? else {
        return Ok(T::default());
    };
    Ok(serde_json::from_slice(&bytes).unwrap_or_else(|e| {
        tracing::warn!(key, error = %e, "metadata object does not parse; ignored");
        T::default()
    }))
}

async fn save_json<T: Serialize>(store: &DynStore, key: &str, doc: &T) -> Result<(), StoreError> {
    let body = serde_json::to_vec(doc)
        .map_err(|e| StoreError::InvalidArgument(format!("encode {key}: {e}")))?;
    store
        .put(key, PutBody::from(body), PutMode::Overwrite.into())
        .await?;
    Ok(())
}

async fn clear_key(store: &DynStore, key: &str) -> Result<(), StoreError> {
    match store.delete(key, None).await {
        Ok(()) | Err(StoreError::NotFound { .. }) => Ok(()),
        Err(e) => Err(e),
    }
}

pub async fn load_description(
    store: &DynStore,
    id: &RepoId,
) -> Result<RepoDescription, StoreError> {
    load_json(store, &keys::description_key(id.owner(), id.name())).await
}
pub async fn save_description(
    store: &DynStore,
    id: &RepoId,
    doc: &RepoDescription,
) -> Result<(), StoreError> {
    save_json(store, &keys::description_key(id.owner(), id.name()), doc).await
}
pub async fn clear_description(store: &DynStore, id: &RepoId) -> Result<(), StoreError> {
    clear_key(store, &keys::description_key(id.owner(), id.name())).await
}

/// `owner` must already be valid (`walgit_git::validate_owner`): it becomes a key segment.
pub async fn load_profile(store: &DynStore, owner: &str) -> Result<OwnerProfile, StoreError> {
    load_json(store, &keys::owner_profile_key(owner)).await
}
pub async fn save_profile(
    store: &DynStore,
    owner: &str,
    doc: &OwnerProfile,
) -> Result<(), StoreError> {
    save_json(store, &keys::owner_profile_key(owner), doc).await
}
pub async fn clear_profile(store: &DynStore, owner: &str) -> Result<(), StoreError> {
    clear_key(store, &keys::owner_profile_key(owner)).await
}

/// Profiles for sorted owner names, same order: one GET each, `DETAIL_CONCURRENCY` in
/// flight (`buffered`, not `buffer_unordered`, so the listing's order survives).
pub async fn owner_details(
    store: &DynStore,
    owners: Vec<String>,
) -> Result<Vec<OwnerEntry>, StoreError> {
    futures::stream::iter(owners)
        .map(|name| {
            let store = store.clone();
            async move {
                let profile = load_profile(&store, &name).await?;
                Ok::<_, StoreError>(OwnerEntry { name, profile })
            }
        })
        .buffered(DETAIL_CONCURRENCY)
        .try_collect()
        .await
}

/// Descriptions for one owner's sorted repositories, same order and fan-out as above.
pub async fn repo_details(
    store: &DynStore,
    repos: Vec<RepoId>,
) -> Result<Vec<RepoEntry>, StoreError> {
    futures::stream::iter(repos)
        .map(|id| {
            let store = store.clone();
            async move {
                let doc = load_description(&store, &id).await?;
                Ok::<_, StoreError>(RepoEntry {
                    name: id.name().to_string(),
                    description: doc.description,
                })
            }
        })
        .buffered(DETAIL_CONCURRENCY)
        .try_collect()
        .await
}

// ---------------------------------------------------------------------------
// HTTP: /{o}/{r}/api[-browser]/description (via `crate::dispatch_route`)
// ---------------------------------------------------------------------------

/// `GET` → `{"description"?}`; 404 for an unknown repository. The repository check and
/// the object GET run concurrently: depth 1 GET on a warm handle.
pub async fn http_get_description(
    st: &AppState,
    route: &RepoRoute,
    headers: &HeaderMap,
) -> Result<Response, ApiError> {
    st.auth.require_read(headers).await.map_err(auth_err)?;
    let (exists, doc) = tokio::join!(
        crate::policy::ensure_repo(st, route),
        load_description(&st.store, &route.id)
    );
    exists?;
    let doc = doc.map_err(store_err)?;
    Ok(json_swr_digest(&doc).into_response(headers))
}

/// `PUT` (admin) body `{"description": "…"}` → 204.
pub async fn http_put_description(
    st: &AppState,
    route: &RepoRoute,
    headers: &HeaderMap,
    body: axum::body::Body,
) -> Result<Response, ApiError> {
    st.auth.require_admin(headers).await.map_err(auth_err)?;
    let bytes = read_body(body).await?;
    let doc = parse_description(&bytes).map_err(ApiError::BadRequest)?;
    crate::policy::ensure_repo(st, route).await?;
    save_description(&st.store, &route.id, &doc)
        .await
        .map_err(store_err)?;
    Ok((StatusCode::NO_CONTENT, "").into_response())
}

/// `DELETE` (admin) → 204, also when there was none.
pub async fn http_delete_description(
    st: &AppState,
    route: &RepoRoute,
    headers: &HeaderMap,
) -> Result<Response, ApiError> {
    st.auth.require_admin(headers).await.map_err(auth_err)?;
    crate::policy::ensure_repo(st, route).await?;
    clear_description(&st.store, &route.id)
        .await
        .map_err(store_err)?;
    Ok((StatusCode::NO_CONTENT, "").into_response())
}

// ---------------------------------------------------------------------------
// HTTP: /api/v1/owners/{owner} and /api-browser/v1/owners/{owner} (web::v1)
// ---------------------------------------------------------------------------

fn owner_arg(owner: &str) -> Result<(), ApiError> {
    walgit_git::validate_owner(owner).map_err(|e| ApiError::BadRequest(e.to_string()))
}

/// `GET` → `{name, display_name?, description?}`. 200 for any valid owner name, with or
/// without repositories or a profile — the same rule as `owners/{o}/repos` answering `[]`.
pub async fn http_get_profile(
    st: &AppState,
    owner: &str,
    headers: &HeaderMap,
) -> Result<Response, ApiError> {
    st.auth.require_read(headers).await.map_err(auth_err)?;
    owner_arg(owner)?;
    let profile = load_profile(&st.store, owner).await.map_err(store_err)?;
    let entry = OwnerEntry {
        name: owner.to_string(),
        profile,
    };
    Ok(json_swr_digest(&entry).into_response(headers))
}

/// `PUT` (admin) body `{display_name?, description?}` → 204. Allowed for an owner with no
/// repositories yet (it is not listed until one exists; nothing is created but the object).
pub async fn http_put_profile(
    st: &AppState,
    owner: &str,
    headers: &HeaderMap,
    body: axum::body::Body,
) -> Result<Response, ApiError> {
    st.auth.require_admin(headers).await.map_err(auth_err)?;
    owner_arg(owner)?;
    let bytes = read_body(body).await?;
    let profile = parse_profile(owner, &bytes).map_err(ApiError::BadRequest)?;
    save_profile(&st.store, owner, &profile)
        .await
        .map_err(store_err)?;
    Ok((StatusCode::NO_CONTENT, "").into_response())
}

/// `DELETE` (admin) → 204, also when there was none.
pub async fn http_delete_profile(
    st: &AppState,
    owner: &str,
    headers: &HeaderMap,
) -> Result<Response, ApiError> {
    st.auth.require_admin(headers).await.map_err(auth_err)?;
    owner_arg(owner)?;
    clear_profile(&st.store, owner).await.map_err(store_err)?;
    Ok((StatusCode::NO_CONTENT, "").into_response())
}

/// Bounded body read: 413 above `MAX_DOCUMENT_BYTES` without buffering more than that.
async fn read_body(body: axum::body::Body) -> Result<bytes::Bytes, ApiError> {
    axum::body::to_bytes(body, MAX_DOCUMENT_BYTES)
        .await
        .map_err(|_| ApiError::PayloadTooLarge)
}

fn store_err(e: StoreError) -> ApiError {
    match e {
        StoreError::InvalidArgument(msg) => ApiError::BadRequest(msg),
        e => ApiError::Internal(format!("store: {e}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn description_is_one_bounded_line() {
        let d = parse_description(br#"{"description":"  Payments ledger  "}"#).unwrap();
        assert_eq!(d.description.as_deref(), Some("Payments ledger"));
        for bad in [
            &br#"{"description":""}"#[..],
            br#"{"description":"   "}"#,
            br#"{"description":"a\nb"}"#,
            br#"{"description":7}"#,
            br#"{"descripton":"typo"}"#,
            br"{}",
            br#"["description"]"#,
            b"not json",
        ] {
            assert!(
                parse_description(bad).is_err(),
                "{}",
                String::from_utf8_lossy(bad)
            );
        }
        let at_limit = format!(
            r#"{{"description":"{}"}}"#,
            "é".repeat(DESCRIPTION_MAX_CHARS)
        );
        assert!(
            parse_description(at_limit.as_bytes()).is_ok(),
            "limit counts characters, not bytes"
        );
        let over = format!(
            r#"{{"description":"{}"}}"#,
            "x".repeat(DESCRIPTION_MAX_CHARS + 1)
        );
        assert!(parse_description(over.as_bytes()).is_err());
        let huge = vec![b' '; MAX_DOCUMENT_BYTES + 1];
        assert!(parse_description(&huge).unwrap_err().contains("limit"));
    }

    #[test]
    fn profile_fields_optional_but_not_all_absent() {
        let p = parse_profile("u-1", br#"{"display_name":"Platform team"}"#).unwrap();
        assert_eq!(p.display_name.as_deref(), Some("Platform team"));
        assert_eq!(p.description, None);
        let p = parse_profile(
            "u-1",
            br#"{"name":"u-1","display_name":"A","description":"B"}"#,
        )
        .unwrap();
        assert_eq!(
            (p.display_name.as_deref(), p.description.as_deref()),
            (Some("A"), Some("B"))
        );
        assert!(parse_profile("u-1", br"{}").is_err());
        assert!(parse_profile("u-1", br#"{"name":"u-2","display_name":"A"}"#).is_err());
        assert!(parse_profile("u-1", br#"{"displayName":"A"}"#).is_err());
        let long = format!(
            r#"{{"display_name":"{}"}}"#,
            "x".repeat(DISPLAY_NAME_MAX_CHARS + 1)
        );
        assert!(parse_profile("u-1", long.as_bytes()).is_err());
    }

    #[test]
    fn entries_omit_absent_fields() {
        let e = OwnerEntry {
            name: "u-1".into(),
            profile: OwnerProfile {
                display_name: Some("A".into()),
                description: None,
            },
        };
        assert_eq!(
            serde_json::to_value(&e).unwrap(),
            serde_json::json!({"name": "u-1", "display_name": "A"})
        );
        let r = RepoEntry {
            name: "r".into(),
            description: None,
        };
        assert_eq!(
            serde_json::to_value(&r).unwrap(),
            serde_json::json!({"name": "r"})
        );
    }

    #[tokio::test]
    async fn store_reads_are_lenient_and_absent_is_default() {
        let store: DynStore = std::sync::Arc::new(walgit_store::memory::MemoryStore::new());
        let id = RepoId::new("o", "r").unwrap();
        assert_eq!(
            load_description(&store, &id).await.unwrap(),
            RepoDescription::default()
        );
        // A future binary's extra key is ignored, not an error.
        store
            .put(
                &keys::description_key("o", "r"),
                PutBody::from(br#"{"description":"x","since":"v2"}"#.to_vec()),
                PutMode::Overwrite.into(),
            )
            .await
            .unwrap();
        assert_eq!(
            load_description(&store, &id)
                .await
                .unwrap()
                .description
                .as_deref(),
            Some("x")
        );
        // Garbage and oversized objects read as absent.
        store
            .put(
                &keys::owner_profile_key("o"),
                PutBody::from(b"{".to_vec()),
                PutMode::Overwrite.into(),
            )
            .await
            .unwrap();
        assert!(load_profile(&store, "o").await.unwrap().is_empty());
        store
            .put(
                &keys::owner_profile_key("p"),
                PutBody::from(vec![b' '; MAX_DOCUMENT_BYTES + 1]),
                PutMode::Overwrite.into(),
            )
            .await
            .unwrap();
        assert!(load_profile(&store, "p").await.unwrap().is_empty());
        clear_profile(&store, "nobody").await.unwrap();
        let rows = owner_details(&store, vec!["o".into(), "q".into()])
            .await
            .unwrap();
        assert_eq!(
            rows.iter().map(|r| r.name.as_str()).collect::<Vec<_>>(),
            ["o", "q"]
        );
    }
}
