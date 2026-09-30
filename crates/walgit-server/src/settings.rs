//! D24 per-repository settings over HTTP:
//! `GET|PUT|DELETE /{o}/{r}/api[-browser]/settings`, `GET …/settings/history`,
//! `GET …/settings/effective`, `GET …/settings/describe`, `POST …/settings/validate`.
//!
//! * `GET` → `{revision, author, updated_at, message, toml}` (or `revision: 0`).
//! * `PUT` body = the TOML document (`text/plain` or `application/toml`),
//!   optional `?message=`; validated against this host's config (the
//!   effective config must load and pass `Config::validate`); 400 with the
//!   reason on failure, nothing published. 200 `{revision}`.
//! * `DELETE` = publish an empty document.
//! * `…/effective` → the effective config as TOML (what the maintainer uses).
//! * `…/history` → SETTINGS entries in the live log.
use std::sync::Arc;

use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use serde_json::json;

use crate::error::ApiError;
use crate::{AppState, RepoRoute};

fn auth_err(e: crate::auth::AuthError) -> ApiError {
    match e {
        crate::auth::AuthError::Invalid | crate::auth::AuthError::Unauthorized => {
            ApiError::Unauthorized
        }
        _ => ApiError::Forbidden,
    }
}

async fn open(st: &AppState, route: &RepoRoute) -> Result<Arc<walgit_wal::RepoHandle>, ApiError> {
    st.registry.open(&route.id).await.map_err(|e| {
        if matches!(e, walgit_wal::WalError::NotFound) {
            ApiError::NotFound(format!("{}", route.id))
        } else {
            ApiError::Internal(format!("wal: {e}"))
        }
    })
}

fn ts(t: Option<&prost_types::Timestamp>) -> Option<String> {
    t.map(walgit_proto::time::to_system)
        .map(|s| chrono::DateTime::<chrono::Utc>::from(s).to_rfc3339())
}

pub async fn http_get(
    st: &AppState,
    route: &RepoRoute,
    headers: &HeaderMap,
) -> Result<Response, ApiError> {
    let _ = st.auth.require_read(headers).await.map_err(auth_err)?;
    let h = open(st, route).await?;
    h.sync_refs()
        .await
        .map_err(|e| ApiError::Internal(e.to_string()))?;
    let body = match h.settings() {
        None => json!({"revision": 0, "toml": "", "author": "", "updated_at": null, "message": ""}),
        Some(s) => {
            json!({"revision": s.revision, "toml": s.toml, "author": s.author, "updated_at": ts(s.updated_at.as_ref()), "message": s.message})
        }
    };
    Ok((
        StatusCode::OK,
        [(axum::http::header::CACHE_CONTROL, "no-store")],
        axum::Json(body),
    )
        .into_response())
}

pub async fn http_effective(
    st: &AppState,
    route: &RepoRoute,
    headers: &HeaderMap,
) -> Result<Response, ApiError> {
    let _ = st.auth.require_read(headers).await.map_err(auth_err)?;
    let h = open(st, route).await?;
    h.sync_refs()
        .await
        .map_err(|e| ApiError::Internal(e.to_string()))?;
    let text = h
        .validated_effective_config()
        .map_err(|e| ApiError::Internal(e.to_string()))?
        .public_settings_toml()
        .map_err(|e| ApiError::Internal(e.to_string()))?;
    Ok((
        StatusCode::OK,
        [
            (
                axum::http::header::CONTENT_TYPE,
                "application/toml; charset=utf-8",
            ),
            (axum::http::header::CACHE_CONTROL, "no-store"),
        ],
        text,
    )
        .into_response())
}

pub async fn http_history(
    st: &AppState,
    route: &RepoRoute,
    headers: &HeaderMap,
) -> Result<Response, ApiError> {
    let _ = st.auth.require_read(headers).await.map_err(auth_err)?;
    let h = open(st, route).await?;
    h.sync_refs()
        .await
        .map_err(|e| ApiError::Internal(e.to_string()))?;
    let m = h.manifest();
    let entries = h
        .read_log(m.min_seq.max(1), None)
        .await
        .map_err(|e| ApiError::Internal(e.to_string()))?;
    let items: Vec<serde_json::Value> = entries
        .iter()
        .filter(|e| e.kind() == walgit_proto::v1::EntryKind::Settings)
        .map(|e| {
            let s = e.settings.clone().unwrap_or_default();
            json!({"seq": e.seq, "revision": s.revision, "author": s.author, "message": s.message, "at": ts(e.created_at.as_ref()), "toml": s.toml})
        })
        .collect();
    Ok((
        StatusCode::OK,
        [(axum::http::header::CACHE_CONTROL, "no-store")],
        axum::Json(json!({"min_seq": m.min_seq, "entries": items})),
    )
        .into_response())
}

pub async fn http_put(
    st: &AppState,
    route: &RepoRoute,
    headers: &HeaderMap,
    query: &str,
    body: axum::body::Body,
) -> Result<Response, ApiError> {
    let principal = st.auth.require_admin(headers).await.map_err(auth_err)?;
    let h = open(st, route).await?;
    let bytes = crate::collect_body(body).await?;
    if bytes.len() > walgit_config::SETTINGS_MAX_BYTES {
        return Err(ApiError::PayloadTooLarge);
    }
    let text = std::str::from_utf8(&bytes)
        .map_err(|_| ApiError::BadRequest("settings must be UTF-8 TOML".into()))?;
    let message = query
        .split('&')
        .filter_map(|kv| kv.split_once('='))
        .find(|(k, _)| *k == "message")
        .map(|(_, v)| percent_decode(v))
        .unwrap_or_default();
    publish(&h, text, &principal.name, &message).await
}

pub async fn http_delete(
    st: &AppState,
    route: &RepoRoute,
    headers: &HeaderMap,
) -> Result<Response, ApiError> {
    let principal = st.auth.require_admin(headers).await.map_err(auth_err)?;
    let h = open(st, route).await?;
    publish(&h, "", &principal.name, "clear").await
}

async fn publish(
    h: &walgit_wal::RepoHandle,
    text: &str,
    author: &str,
    message: &str,
) -> Result<Response, ApiError> {
    match h.publish_settings(text, author, message).await {
        Ok(revision) => {
            Ok((StatusCode::OK, axum::Json(json!({"revision": revision}))).into_response())
        }
        Err(walgit_wal::WalError::Invalid(why)) => Err(ApiError::BadRequest(why)),
        Err(e) => Err(ApiError::Internal(e.to_string())),
    }
}

fn percent_decode(v: &str) -> String {
    let mut out = Vec::with_capacity(v.len());
    let b = v.as_bytes();
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'+' => out.push(b' '),
            b'%' if i + 2 < b.len() => {
                if let Ok(n) = u8::from_str_radix(&v[i + 1..i + 3], 16) {
                    out.push(n);
                    i += 3;
                    continue;
                }
                out.push(b'%');
            }
            c => out.push(c),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

// ---- Settings tab: describe / validate / policy validate + dry-run ---------

/// Flatten a TOML table to `a.b.c → value` (arrays as one value).
fn flatten(prefix: &str, t: &toml::Table, out: &mut Vec<(String, toml::Value)>) {
    for (k, v) in t {
        let key = if prefix.is_empty() {
            k.clone()
        } else {
            format!("{prefix}.{k}")
        };
        match v {
            toml::Value::Table(inner) => flatten(&key, inner, out),
            other => out.push((key, other.clone())),
        }
    }
}

fn toml_json(v: &toml::Value) -> serde_json::Value {
    serde_json::to_value(v).unwrap_or(serde_json::Value::Null)
}

/// Everything the Settings tab needs in one answer: placement (host-level, read-only),
/// the effective config as a flat `key → {value, source}` map restricted to
/// the settings sections, the current settings document and the history.
pub async fn http_describe(
    st: &AppState,
    route: &RepoRoute,
    headers: &HeaderMap,
) -> Result<Response, ApiError> {
    let _ = st.auth.require_read(headers).await.map_err(auth_err)?;
    let h = open(st, route).await?;
    h.sync_refs()
        .await
        .map_err(|e| ApiError::Internal(e.to_string()))?;
    let settings = h.settings();
    let effective = h
        .validated_effective_config()
        .map_err(|e| ApiError::Internal(e.to_string()))?;
    Ok((
        StatusCode::OK,
        [(axum::http::header::CACHE_CONTROL, "no-store")],
        axum::Json(describe_json(st, &h, &effective, settings.as_ref())?),
    )
        .into_response())
}

fn describe_json(
    st: &AppState,
    h: &walgit_wal::RepoHandle,
    effective: &walgit_config::Config,
    settings: Option<&walgit_proto::v1::RepoSettings>,
) -> Result<serde_json::Value, ApiError> {
    // Sources: every key of the settings sections; a key is "setting" when the
    // repo document sets it (rev/author), else "host" (walgit.toml ⊕ env).
    let host_doc: toml::Table =
        toml::Table::try_from(&*st.cfg).map_err(|e| ApiError::Internal(e.to_string()))?;
    let eff_doc: toml::Table =
        toml::Table::try_from(effective).map_err(|e| ApiError::Internal(e.to_string()))?;
    let set_doc: toml::Table = settings
        .map(|s| s.toml.parse::<toml::Table>().unwrap_or_default())
        .unwrap_or_default();
    let mut set_flat = Vec::new();
    flatten("", &set_doc, &mut set_flat);
    let set_keys: std::collections::HashSet<String> =
        set_flat.iter().map(|(k, _)| k.clone()).collect();
    let mut fields = Vec::new();
    for section in walgit_config::SETTINGS_SECTIONS {
        let mut eff_flat = Vec::new();
        if let Some(toml::Value::Table(t)) = eff_doc.get(*section) {
            flatten(section, t, &mut eff_flat);
        }
        let mut host_flat = Vec::new();
        if let Some(toml::Value::Table(t)) = host_doc.get(*section) {
            flatten(section, t, &mut host_flat);
        }
        let host_map: std::collections::HashMap<String, toml::Value> =
            host_flat.into_iter().collect();
        for (k, v) in eff_flat {
            if k.ends_with("token_env") {
                continue;
            }
            fields.push(json!({
                "key": k,
                "value": toml_json(&v),
                "host_value": host_map.get(&k).map(toml_json),
                "source": if set_keys.contains(&k) { "setting" } else { "host" },
            }));
        }
    }
    let m = h.manifest();
    Ok(json!({
        "repo": h.id().to_string(),
        "settings": settings.map(|s| json!({"revision": s.revision, "author": s.author, "message": s.message, "updated_at": ts(s.updated_at.as_ref()), "toml": s.toml})).unwrap_or(json!({"revision": 0, "toml": ""})),
        "sections": walgit_config::SETTINGS_SECTIONS,
        "maintenance": {
            "checkpoints": effective.maintenance.checkpoints,
            "interval_secs": effective.maintenance.interval.as_secs(),
            "this_host": {"name": crate::maintain::host_name(st), "maintains": st.cfg.placement.maintains(h.id().owner(), h.id().name()), "serves": st.cfg.placement.serves(h.id().owner(), h.id().name()), "disk": format!("{:?}", st.cfg.maintenance.disk).to_lowercase(), "max_pack_bytes": st.cfg.maintenance.max_pack_bytes.as_u64(), "cache_budget_bytes": st.cfg.cache_budget_bytes(), "roles": st.cfg.server.roles.iter().map(|r| format!("{r:?}").to_lowercase()).collect::<Vec<_>>()},
        },
        "packs": {"enabled": effective.packs.enabled, "fold_when_fresh_packs_reach": effective.packs.fold_when_fresh_packs_reach, "fold_when_max_age_secs": effective.packs.fold_when_max_age.as_secs(), "segment_max_bytes": effective.packs.segment_max_bytes.as_u64(), "freeze_when_settled_secs": effective.packs.freeze_when_settled.as_secs()},
        // D33: what this repository follows and what the last round on this instance did.
        "upstream": {
            "git": effective.upstream.git,
            "lfs": effective.upstream.lfs,
            "token_env": effective.upstream.token_env.is_some(),
            "follow": effective.upstream.follow,
            "follow_interval_secs": st.cfg.maintenance.follow_interval.as_secs(),
            "last_round": st.follow.get(&h.id().to_string()),
        },
        "fields": fields,
        "head_seq": m.head_seq,
    }))
}

/// `POST …/settings/validate` body = TOML: `{ok, errors[], fields[]}` —
/// the describe of the *would-be* effective config, without publishing.
pub async fn http_validate(
    st: &AppState,
    route: &RepoRoute,
    headers: &HeaderMap,
    body: axum::body::Body,
) -> Result<Response, ApiError> {
    let _ = st.auth.require_read(headers).await.map_err(auth_err)?;
    let h = open(st, route).await?;
    let bytes = crate::collect_body(body).await?;
    let text = std::str::from_utf8(&bytes)
        .map_err(|_| ApiError::BadRequest("settings must be UTF-8 TOML".into()))?;
    let out = match st.cfg.with_settings(text) {
        Ok(eff) => {
            let preview = walgit_proto::v1::RepoSettings {
                toml: text.to_string(),
                revision: h.settings().map_or(1, |s| s.revision + 1),
                author: "(preview)".into(),
                updated_at: None,
                message: String::new(),
            };
            let mut d = describe_json(st, &h, &eff, Some(&preview))?;
            d["ok"] = json!(true);
            d["errors"] = json!([]);
            d
        }
        Err(e) => json!({"ok": false, "errors": [format!("{e:#}")]}),
    };
    Ok((
        StatusCode::OK,
        [(axum::http::header::CACHE_CONTROL, "no-store")],
        axum::Json(out),
    )
        .into_response())
}

/// `POST …/policy/validate` body = policy JSON → `{ok, errors[], summary}`.
pub async fn http_policy_validate(
    st: &AppState,
    route: &RepoRoute,
    headers: &HeaderMap,
    body: axum::body::Body,
) -> Result<Response, ApiError> {
    let _ = st.auth.require_read(headers).await.map_err(auth_err)?;
    let _ = open(st, route).await?;
    let bytes = crate::collect_body(body).await?;
    let out = match crate::policy::parse_document(&bytes) {
        Ok(p) => {
            json!({"ok": true, "errors": [], "rules": p.rules.len(), "groups": p.groups.len(), "protect": p.has_protect()})
        }
        Err(e) => json!({"ok": false, "errors": [e.to_string()]}),
    };
    Ok((StatusCode::OK, axum::Json(out)).into_response())
}

/// `POST …/policy/dry-run?last=N` body = policy JSON (empty body = the saved
/// policy): evaluate it, after the host baseline exactly as receive-pack
/// would, against the last N PUSH entries in the live log
/// (principal + ref transaction as recorded) → per push, per ref: allowed or
/// the denying rule. Force detection uses the local copy when objects are
/// local, else "unknown" (treated as fast-forward).
pub async fn http_policy_dry_run(
    st: &AppState,
    route: &RepoRoute,
    headers: &HeaderMap,
    query: &str,
    body: axum::body::Body,
) -> Result<Response, ApiError> {
    let _ = st.auth.require_read(headers).await.map_err(auth_err)?;
    let h = open(st, route).await?;
    h.sync_refs()
        .await
        .map_err(|e| ApiError::Internal(e.to_string()))?;
    let last: usize = query
        .split('&')
        .filter_map(|kv| kv.split_once('='))
        .find(|(k, _)| *k == "last")
        .and_then(|(_, v)| v.parse().ok())
        .unwrap_or(20)
        .clamp(1, 200);
    let bytes = crate::collect_body(body).await?;
    let policy = if bytes.iter().all(u8::is_ascii_whitespace) {
        crate::policy::load(&st.store, &route.id)
            .await
            .map_err(|e| ApiError::Internal(e.to_string()))?
    } else {
        crate::policy::parse_document(&bytes).map_err(|e| ApiError::BadRequest(e.to_string()))?
    };
    let m = h.manifest();
    let entries = h
        .read_log(m.min_seq.max(1), None)
        .await
        .map_err(|e| ApiError::Internal(e.to_string()))?;
    let pushes: Vec<&walgit_proto::v1::LogEntry> = entries
        .iter()
        .rev()
        .filter(|e| e.kind() == walgit_proto::v1::EntryKind::Push && e.txn.is_some())
        .take(last)
        .collect();
    let local = h.local();
    let mut results = Vec::new();
    let (mut allowed_n, mut denied_n) = (0usize, 0usize);
    for e in pushes {
        let txn = e.txn.clone().unwrap();
        let principal = e
            .meta
            .get("principal")
            .cloned()
            .unwrap_or_else(|| e.writer.clone());
        let baseline = st.policy_baseline.as_ref();
        let mut forces = std::collections::HashSet::new();
        if crate::policy::needs_force_check(baseline, &policy) {
            for u in &txn.updates {
                if crate::policy::classify(&u.old_oid, &u.new_oid) == crate::policy::RefOp::Update
                    && matches!(local.is_ancestor(&u.old_oid, &u.new_oid).await, Ok(false))
                {
                    forces.insert(u.name.clone());
                }
            }
        }
        let ev = crate::policy::evaluate(baseline, &policy, &principal, &txn, |u| {
            forces.contains(&u.name)
        });
        let refs: Vec<serde_json::Value> = ev
            .per_ref
            .iter()
            .map(|(name, r)| {
                match r {
                    Ok(()) => allowed_n += 1,
                    Err(_) => denied_n += 1,
                }
                json!({"name": name, "ok": r.is_ok(), "reason": r.as_ref().err().cloned(), "force": forces.contains(name)})
            })
            .collect();
        results.push(json!({"seq": e.seq, "at": ts(e.created_at.as_ref()), "principal": principal, "atomic": txn.atomic, "refs": refs}));
    }
    Ok((StatusCode::OK, axum::Json(json!({"pushes": results.len(), "allowed": allowed_n, "denied": denied_n, "results": results}))).into_response())
}
