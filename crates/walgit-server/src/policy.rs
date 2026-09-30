//! Per-repo push policy. Language: `docs/POLICY.md`.
//!
//! Stored at `repos/<owner>/<repo>/policy.json` (not on the WAL). Missing file
//! = empty rules = allow-all. A host may add a **baseline** (`[policy] baseline`,
//! one more document of the same language, parsed at startup, held in memory):
//! every push is judged against the baseline and then the repository's own
//! document, each with its own roster, and must pass both. Receive-pack
//! evaluates after ingest so force-push can use `merge-base --is-ancestor`.

use std::collections::{HashMap, HashSet};

use serde::de::{self, Deserializer};
use serde::{Deserialize, Serialize};
use walgit_git::RepoId;
use walgit_proto::keys;
use walgit_proto::v1::{RefTransaction, RefUpdate};
use walgit_store::{DynStore, GetOptions, PutBody, PutMode, StoreError};

// ---------------------------------------------------------------------------
// Document
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepoPolicy {
    #[serde(default = "default_version")]
    pub version: u32,
    #[serde(default)]
    pub groups: Vec<Group>,
    #[serde(default)]
    pub rules: Vec<Rule>,
    /// Ignored. Operators write novels.
    #[serde(default, rename = "_comment")]
    pub comment: Option<String>,
}

fn default_version() -> u32 {
    1
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Group {
    pub name: String,
    #[serde(default)]
    pub members: Vec<String>,
    #[serde(default, rename = "_comment")]
    pub comment: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Rule {
    pub name: String,
    #[serde(default)]
    #[serde(rename = "match")]
    pub match_: Match,
    pub effect: Effect,
    #[serde(default)]
    pub mode: Option<String>,
    #[serde(default, rename = "_comment")]
    pub comment: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Match {
    #[serde(default)]
    pub refs: Vec<String>,
    #[serde(default)]
    pub principals: Vec<String>,
    #[serde(default)]
    pub paths: Vec<String>,
    #[serde(default, rename = "_comment")]
    pub comment: Option<String>,
}

/// Tagged union: exactly one key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Effect {
    #[serde(default)]
    pub protect: Option<ProtectEffect>,
    #[serde(default)]
    pub history: Option<HistoryEffect>,
    #[serde(default)]
    pub size: Option<SizeEffect>,
    #[serde(default, rename = "_comment")]
    pub comment: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProtectEffect {
    /// Absent = all four ops. `null` / `[]` = parse error.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_restricts"
    )]
    pub restricts: Option<Vec<Restrict>>,
    #[serde(default)]
    pub bypass: Vec<String>,
    #[serde(default, rename = "_comment")]
    pub comment: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Restrict {
    Create,
    Update,
    Delete,
    ForcePush,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryEffect {
    #[serde(default)]
    pub allowed_forwards: Option<u64>,
    #[serde(default)]
    pub allow_unrelated: Option<bool>,
    #[serde(default, rename = "_comment")]
    pub comment: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SizeEffect {
    #[serde(default)]
    pub blob_bytes: Option<u64>,
    #[serde(default)]
    pub push_bytes: Option<u64>,
    #[serde(default, rename = "_comment")]
    pub comment: Option<String>,
}

fn deserialize_restricts<'de, D>(d: D) -> Result<Option<Vec<Restrict>>, D::Error>
where
    D: Deserializer<'de>,
{
    let v: Option<Vec<Restrict>> = Option::deserialize(d)?;
    match v {
        None => Err(de::Error::custom(
            "restricts: null is a parse error (omit the key for all four ops)",
        )),
        Some(list) if list.is_empty() => Err(de::Error::custom(
            "restricts: [] is a parse error (omit the key for all four ops)",
        )),
        Some(list) => Ok(Some(list)),
    }
}

impl RepoPolicy {
    pub fn empty() -> Self {
        Self {
            version: 1,
            groups: Vec::new(),
            rules: Vec::new(),
            comment: None,
        }
    }

    pub fn has_protect(&self) -> bool {
        self.rules.iter().any(|r| r.effect.protect.is_some())
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.version != 1 {
            return Err(format!("unsupported policy version {}", self.version));
        }
        let mut group_names = HashSet::new();
        for g in &self.groups {
            if !valid_name(&g.name) {
                return Err(format!("groups: bad name {:?}", g.name));
            }
            if !group_names.insert(&g.name) {
                return Err(format!("groups: duplicate name {:?}", g.name));
            }
        }
        let mut rule_names = HashSet::new();
        for r in &self.rules {
            if !valid_name(&r.name) {
                return Err(format!("rules: bad name {:?}", r.name));
            }
            if !rule_names.insert(&r.name) {
                return Err(format!("rules: duplicate name {:?}", r.name));
            }
            let n = u8::from(r.effect.protect.is_some())
                + u8::from(r.effect.history.is_some())
                + u8::from(r.effect.size.is_some());
            if n != 1 {
                return Err(format!(
                    "rule {:?}: effect must have exactly one of protect, history, size",
                    r.name
                ));
            }
            if let Some(m) = &r.mode
                && m != "enforce"
                && m != "audit"
            {
                return Err(format!("rule {:?}: mode must be enforce|audit", r.name));
            }
            // ^ exclusions forbidden on first-match (union-like) families.
            if r.effect.history.is_some() || r.effect.size.is_some() {
                for pats in [&r.match_.refs, &r.match_.principals, &r.match_.paths] {
                    if pats.iter().any(|p| p.starts_with('^')) {
                        return Err(format!(
                            "rule {:?}: ^ exclusions are illegal on history/size (first-match)",
                            r.name
                        ));
                    }
                }
            }
        }
        check_overlap_bypass(self)?;
        Ok(())
    }
}

fn valid_name(s: &str) -> bool {
    let b = s.as_bytes();
    (1..=63).contains(&b.len())
        && b[0].is_ascii_lowercase()
        && b.iter()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == b'-')
}

/// Two overlapping protect rules with non-empty, disjoint bypass lists cannot
/// both be satisfied. AND would lock out the intended bot.
fn check_overlap_bypass(p: &RepoPolicy) -> Result<(), String> {
    let protect: Vec<&Rule> = p
        .rules
        .iter()
        .filter(|r| r.effect.protect.is_some())
        .collect();
    for (i, a) in protect.iter().enumerate() {
        for b in &protect[i + 1..] {
            if !ref_patterns_may_overlap(&a.match_.refs, &b.match_.refs) {
                continue;
            }
            let ra = restrict_set(a.effect.protect.as_ref().unwrap());
            let rb = restrict_set(b.effect.protect.as_ref().unwrap());
            if ra.is_disjoint(&rb) {
                continue;
            }
            let ba = &a.effect.protect.as_ref().unwrap().bypass;
            let bb = &b.effect.protect.as_ref().unwrap().bypass;
            if ba.is_empty() || bb.is_empty() {
                continue;
            }
            let set_a: HashSet<&str> = ba.iter().map(std::string::String::as_str).collect();
            let set_b: HashSet<&str> = bb.iter().map(std::string::String::as_str).collect();
            if set_a.is_disjoint(&set_b) {
                return Err(format!(
                    "protect rules {:?} and {:?} overlap with disjoint bypass lists",
                    a.name, b.name
                ));
            }
        }
    }
    Ok(())
}

fn restrict_set(p: &ProtectEffect) -> HashSet<Restrict> {
    match &p.restricts {
        None => HashSet::from([
            Restrict::Create,
            Restrict::Update,
            Restrict::Delete,
            Restrict::ForcePush,
        ]),
        Some(v) => v.iter().copied().collect(),
    }
}

/// Conservative: empty (match-all) overlaps everything; otherwise any pair of
/// non-exclusion patterns that share a prefix might overlap.
fn ref_patterns_may_overlap(a: &[String], b: &[String]) -> bool {
    if a.is_empty() || b.is_empty() {
        return true;
    }
    let inc = |pats: &[String]| {
        pats.iter()
            .filter(|p| !p.starts_with('^'))
            .cloned()
            .collect::<Vec<_>>()
    };
    let ia = inc(a);
    let ib = inc(b);
    if ia.is_empty() || ib.is_empty() {
        return true;
    }
    for x in &ia {
        for y in &ib {
            if glob_may_overlap(x, y) {
                return true;
            }
        }
    }
    false
}

fn glob_may_overlap(a: &str, b: &str) -> bool {
    if a == b {
        return true;
    }
    // Either pattern matches the other's literal stem, or either is a ** catch.
    glob_match(a, b.trim_end_matches('*').trim_end_matches('/'))
        || glob_match(b, a.trim_end_matches('*').trim_end_matches('/'))
        || a.contains("**")
        || b.contains("**")
}

// ---------------------------------------------------------------------------
// Globs
// ---------------------------------------------------------------------------

/// Doublestar: `*` / `?` stop at `/`; `**` crosses. `HEAD` is exact.
pub fn glob_match(pat: &str, text: &str) -> bool {
    if pat == "HEAD" {
        return text == "HEAD";
    }
    glob_bytes(pat.as_bytes(), text.as_bytes())
}

fn glob_bytes(pat: &[u8], text: &[u8]) -> bool {
    let mut pi = 0;
    let mut ti = 0;
    while pi < pat.len() {
        if pat[pi] == b'*' && pi + 1 < pat.len() && pat[pi + 1] == b'*' {
            let mut rest = &pat[pi + 2..];
            if rest.first() == Some(&b'/') {
                rest = &rest[1..];
            }
            if rest.is_empty() {
                return true;
            }
            let mut i = ti;
            loop {
                if glob_bytes(rest, &text[i..]) {
                    return true;
                }
                if i >= text.len() {
                    return false;
                }
                i += 1;
            }
        } else if pat[pi] == b'*' {
            let rest = &pat[pi + 1..];
            if glob_bytes(rest, &text[ti..]) {
                return true;
            }
            while ti < text.len() && text[ti] != b'/' {
                ti += 1;
                if glob_bytes(rest, &text[ti..]) {
                    return true;
                }
            }
            return false;
        } else if pat[pi] == b'?' {
            if ti >= text.len() || text[ti] == b'/' {
                return false;
            }
            ti += 1;
            pi += 1;
        } else {
            if ti >= text.len() || text[ti] != pat[pi] {
                return false;
            }
            ti += 1;
            pi += 1;
        }
    }
    ti == text.len()
}

/// Inclusion OR, then minus any `^` exclusion. Empty inclusion list = match all.
pub fn pattern_list_matches(patterns: &[String], text: &str) -> bool {
    let mut any_inc = false;
    let mut inc = false;
    let mut exc = false;
    for p in patterns {
        if let Some(rest) = p.strip_prefix('^') {
            if glob_match(rest, text) {
                exc = true;
            }
        } else {
            any_inc = true;
            if glob_match(p, text) {
                inc = true;
            }
        }
    }
    (inc || !any_inc) && !exc
}

// ---------------------------------------------------------------------------
// Actors / groups
// ---------------------------------------------------------------------------

fn principal_matches(
    spec: &str,
    principal: &str,
    groups: &HashMap<&str, &Group>,
    seen: &mut HashSet<String>,
) -> bool {
    if let Some(name) = spec.strip_prefix("group:") {
        if !seen.insert(name.to_string()) {
            return false; // cycle: do not admit
        }
        let Some(g) = groups.get(name) else {
            return false; // missing roster: include does not admit
        };
        return g
            .members
            .iter()
            .any(|m| principal_matches(m, principal, groups, seen));
    }
    if spec.starts_with('@') {
        // Tags are bound by the edge. We do not have a tag set yet.
        return false;
    }
    spec.eq_ignore_ascii_case(principal)
}

fn actor_list_matches(
    patterns: &[String],
    principal: &str,
    groups: &HashMap<&str, &Group>,
) -> bool {
    // Same inclusion/exclusion as globs, but each non-^ entry is an actor spec.
    let mut any_inc = false;
    let mut inc = false;
    let mut exc = false;
    for p in patterns {
        if let Some(rest) = p.strip_prefix('^') {
            let mut seen = HashSet::new();
            // Unresolvable exclude still excludes: treat missing group as hit.
            if rest.starts_with("group:") && !groups.contains_key(&rest[6..]) {
                exc = true;
            } else if principal_matches(rest, principal, groups, &mut seen) {
                exc = true;
            }
        } else {
            any_inc = true;
            let mut seen = HashSet::new();
            if principal_matches(p, principal, groups, &mut seen) {
                inc = true;
            }
        }
    }
    (inc || !any_inc) && !exc
}

// ---------------------------------------------------------------------------
// Eval
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefOp {
    NoOp,
    Create,
    Delete,
    Update,
}

pub fn classify(old: &str, new: &str) -> RefOp {
    match (old.is_empty(), new.is_empty()) {
        (true, true) => RefOp::NoOp,
        (true, false) => RefOp::Create,
        (false, true) => RefOp::Delete,
        (false, false) if old == new => RefOp::NoOp,
        (false, false) => RefOp::Update,
    }
}

#[derive(Debug, Clone)]
pub struct Eval {
    pub publish: RefTransaction,
    pub per_ref: Vec<(String, Result<(), String>)>,
}

impl Eval {
    pub fn any_denied(&self) -> bool {
        self.per_ref.iter().any(|(_, r)| r.is_err())
    }
    pub fn any_allowed(&self) -> bool {
        !self.publish.updates.is_empty()
    }
}

/// Whether a push needs force detection (`merge-base --is-ancestor` per update).
pub fn needs_force_check(baseline: Option<&RepoPolicy>, repo: &RepoPolicy) -> bool {
    baseline.is_some_and(RepoPolicy::has_protect) || repo.has_protect()
}

/// Judge `txn` against the host baseline (if any), then the repository's own
/// document. An update is allowed only if **both** allow it: `protect` is AND
/// across documents exactly as within one, so the order never changes a verdict,
/// only which rule is named (the first denying rule, baseline first). Each
/// document resolves `group:` against its own roster; a repository cannot join a
/// baseline group by defining one with the same name.
///
/// `is_force` is true when `new` is not a descendant of `old` (after ingest).
/// Tag retargets are treated as force regardless.
pub fn evaluate(
    baseline: Option<&RepoPolicy>,
    repo: &RepoPolicy,
    principal: &str,
    txn: &RefTransaction,
    is_force: impl Fn(&RefUpdate) -> bool,
) -> Eval {
    fn roster(p: &RepoPolicy) -> HashMap<&str, &Group> {
        p.groups.iter().map(|g| (g.name.as_str(), g)).collect()
    }
    let layers: Vec<(&RepoPolicy, HashMap<&str, &Group>, &str)> = baseline
        .map(|b| (b, roster(b), "baseline rule"))
        .into_iter()
        .chain(std::iter::once((repo, roster(repo), "rule")))
        .collect();
    let mut per_ref = Vec::with_capacity(txn.updates.len());
    let mut allowed = Vec::new();
    for u in &txn.updates {
        let denied = layers.iter().find_map(|(p, groups, what)| {
            deny_reason(p, groups, principal, u, &is_force)
                .map(|rule| format!("rejected by {what} '{rule}'"))
        });
        match denied {
            None => {
                per_ref.push((u.name.clone(), Ok(())));
                allowed.push(u.clone());
            }
            Some(msg) => per_ref.push((u.name.clone(), Err(msg))),
        }
    }
    if txn.atomic && per_ref.iter().any(|(_, r)| r.is_err()) {
        return Eval {
            publish: RefTransaction {
                updates: Vec::new(),
                push_options: txn.push_options.clone(),
                atomic: true,
            },
            per_ref,
        };
    }
    Eval {
        publish: RefTransaction {
            updates: allowed,
            push_options: txn.push_options.clone(),
            atomic: txn.atomic,
        },
        per_ref,
    }
}

/// The name of the first rule of `policy` that denies `u`.
fn deny_reason<'p>(
    policy: &'p RepoPolicy,
    groups: &HashMap<&str, &Group>,
    principal: &str,
    u: &RefUpdate,
    is_force: &impl Fn(&RefUpdate) -> bool,
) -> Option<&'p str> {
    let op = classify(&u.old_oid, &u.new_oid);
    if op == RefOp::NoOp {
        return None;
    }
    let force = is_force(u) || u.name.starts_with("refs/tags/");
    for rule in &policy.rules {
        let Some(protect) = &rule.effect.protect else {
            continue; // history/size: specified, not enforced
        };
        if !rule_matches(&rule.match_, &u.name, principal, groups) {
            continue;
        }
        if bypasses(protect, principal, groups) {
            continue;
        }
        let set = restrict_set(protect);
        let hit = match op {
            RefOp::Create => set.contains(&Restrict::Create),
            RefOp::Delete => set.contains(&Restrict::Delete),
            RefOp::Update if force => {
                set.contains(&Restrict::ForcePush) || set.contains(&Restrict::Update)
            }
            RefOp::Update => set.contains(&Restrict::Update),
            RefOp::NoOp => false,
        };
        if hit {
            return Some(&rule.name);
        }
    }
    None
}

fn rule_matches(
    m: &Match,
    ref_name: &str,
    principal: &str,
    groups: &HashMap<&str, &Group>,
) -> bool {
    if !m.refs.is_empty() && !pattern_list_matches(&m.refs, ref_name) {
        return false;
    }
    if !m.principals.is_empty() && !actor_list_matches(&m.principals, principal, groups) {
        return false;
    }
    // paths ignored on protect (see docs/POLICY.md)
    true
}

fn bypasses(p: &ProtectEffect, principal: &str, groups: &HashMap<&str, &Group>) -> bool {
    if p.bypass.is_empty() {
        return false;
    }
    actor_list_matches(&p.bypass, principal, groups)
}

// ---------------------------------------------------------------------------
// Store / HTTP
// ---------------------------------------------------------------------------

pub fn store_key(id: &RepoId) -> String {
    keys::policy_key(id.owner(), id.name())
}

pub async fn load(store: &DynStore, id: &RepoId) -> Result<RepoPolicy, StoreError> {
    let key = store_key(id);
    match store.get(&key, GetOptions::default()).await {
        Ok(got) => {
            let Some((_, bytes)) = got.bytes().await? else {
                return Ok(RepoPolicy::empty());
            };
            parse_bytes(&bytes)
        }
        Err(StoreError::NotFound { .. }) => Ok(RepoPolicy::empty()),
        Err(e) => Err(e),
    }
}

/// Parse + validate a policy document (Settings tab validate / dry-run).
pub fn parse_document(bytes: &[u8]) -> Result<RepoPolicy, StoreError> {
    parse_bytes(bytes)
}

fn parse_bytes(bytes: &[u8]) -> Result<RepoPolicy, StoreError> {
    parse(bytes).map_err(|e| StoreError::InvalidArgument(format!("policy.json: {e}")))
}

/// The one parser + validator, for `policy.json` and the baseline alike.
fn parse(bytes: &[u8]) -> Result<RepoPolicy, String> {
    let policy: RepoPolicy = serde_json::from_slice(bytes).map_err(|e| e.to_string())?;
    policy.validate()?;
    Ok(policy)
}

/// `[policy] baseline`, read and parsed once at startup (`AppState::new`,
/// `walgit config check`). Unreadable or invalid is an error, never "no
/// baseline": a host that meant to protect every repository must not come up
/// allowing everything.
pub fn load_baseline(cfg: &walgit_config::Config) -> anyhow::Result<Option<RepoPolicy>> {
    let Some(path) = &cfg.policy.baseline else {
        return Ok(None);
    };
    let bytes = std::fs::read(path)
        .map_err(|e| anyhow::anyhow!("reading policy.baseline {}: {e}", path.display()))?;
    let policy =
        parse(&bytes).map_err(|e| anyhow::anyhow!("policy.baseline {}: {e}", path.display()))?;
    Ok(Some(policy))
}

pub async fn save(store: &DynStore, id: &RepoId, policy: &RepoPolicy) -> Result<(), StoreError> {
    policy.validate().map_err(StoreError::InvalidArgument)?;
    let key = store_key(id);
    let body = serde_json::to_vec_pretty(policy)
        .map_err(|e| StoreError::InvalidArgument(format!("encode policy: {e}")))?;
    store
        .put(&key, PutBody::from(body), PutMode::Overwrite.into())
        .await?;
    Ok(())
}

pub async fn clear(store: &DynStore, id: &RepoId) -> Result<(), StoreError> {
    let key = store_key(id);
    match store.delete(&key, None).await {
        Ok(()) | Err(StoreError::NotFound { .. }) => Ok(()),
        Err(e) => Err(e),
    }
}

use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};

use crate::AppState;
use crate::error::ApiError;
use crate::repo::RepoRoute;

pub async fn http_get(
    st: &AppState,
    route: &RepoRoute,
    headers: &HeaderMap,
) -> Result<Response, ApiError> {
    let _ = st.auth.require_read(headers).await.map_err(auth_err)?;
    ensure_repo(st, route).await?;
    let policy = load(&st.store, &route.id).await.map_err(store_err)?;
    let body = serde_json::to_vec_pretty(&policy)
        .map_err(|e| ApiError::Internal(format!("encode policy: {e}")))?;
    Ok((
        StatusCode::OK,
        [(
            axum::http::header::CONTENT_TYPE,
            "application/json; charset=utf-8",
        )],
        body,
    )
        .into_response())
}

/// `GET …/policy/effective` → `{layers: [{source, policy}]}`: every document a
/// push is judged against, in evaluation order — `baseline` (only when the host
/// has one), then `repository` (the `GET …/policy` document; empty = none).
pub async fn http_get_effective(
    st: &AppState,
    route: &RepoRoute,
    headers: &HeaderMap,
) -> Result<Response, ApiError> {
    let _ = st.auth.require_read(headers).await.map_err(auth_err)?;
    ensure_repo(st, route).await?;
    let repo = load(&st.store, &route.id).await.map_err(store_err)?;
    let mut layers = Vec::new();
    if let Some(b) = &st.policy_baseline {
        layers.push(serde_json::json!({"source": "baseline", "policy": b}));
    }
    layers.push(serde_json::json!({"source": "repository", "policy": repo}));
    Ok((
        StatusCode::OK,
        [(axum::http::header::CACHE_CONTROL, "no-store")],
        axum::Json(serde_json::json!({ "layers": layers })),
    )
        .into_response())
}

pub async fn http_put(
    st: &AppState,
    route: &RepoRoute,
    headers: &HeaderMap,
    body: axum::body::Body,
) -> Result<Response, ApiError> {
    let _ = st.auth.require_admin(headers).await.map_err(auth_err)?;
    ensure_repo(st, route).await?;
    let bytes = crate::collect_body(body).await?;
    let policy = parse_bytes(&bytes).map_err(store_err)?;
    save(&st.store, &route.id, &policy)
        .await
        .map_err(store_err)?;
    Ok((StatusCode::NO_CONTENT, "").into_response())
}

pub async fn http_delete(
    st: &AppState,
    route: &RepoRoute,
    headers: &HeaderMap,
) -> Result<Response, ApiError> {
    let _ = st.auth.require_admin(headers).await.map_err(auth_err)?;
    ensure_repo(st, route).await?;
    clear(&st.store, &route.id).await.map_err(store_err)?;
    Ok((StatusCode::NO_CONTENT, "").into_response())
}

async fn ensure_repo(st: &AppState, route: &RepoRoute) -> Result<(), ApiError> {
    st.registry.open(&route.id).await.map(|_| ()).map_err(|e| {
        if matches!(e, walgit_wal::WalError::NotFound) {
            ApiError::NotFound(format!("{}", route.id))
        } else {
            ApiError::Internal(format!("wal: {e}"))
        }
    })
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

fn store_err(e: StoreError) -> ApiError {
    match e {
        StoreError::InvalidArgument(msg) => ApiError::BadRequest(msg),
        StoreError::NotFound { key } => ApiError::NotFound(key),
        e => ApiError::Internal(format!("store: {e}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn upd(name: &str, old: &str, new: &str) -> RefUpdate {
        RefUpdate {
            name: name.into(),
            old_oid: old.into(),
            new_oid: new.into(),
            new_symbolic_target: String::new(),
            new_peeled: String::new(),
        }
    }

    fn txn(updates: Vec<RefUpdate>, atomic: bool) -> RefTransaction {
        RefTransaction {
            updates,
            push_options: Vec::new(),
            atomic,
        }
    }

    fn lock_main_json() -> &'static str {
        r#"{
          "version": 1,
          "groups": [
            { "name": "admins", "members": ["alice@example.com"] }
          ],
          "rules": [
            {
              "name": "lock-main",
              "match": { "refs": ["refs/heads/main"] },
              "effect": {
                "protect": {
                  "restricts": ["delete", "force-push"],
                  "bypass": ["group:admins"]
                }
              }
            }
          ]
        }"#
    }

    fn lock_main() -> RepoPolicy {
        parse_bytes(lock_main_json().as_bytes()).unwrap()
    }

    #[test]
    fn classify_shapes() {
        assert_eq!(classify("", ""), RefOp::NoOp);
        assert_eq!(classify("", "abc"), RefOp::Create);
        assert_eq!(classify("abc", ""), RefOp::Delete);
        assert_eq!(classify("abc", "abc"), RefOp::NoOp);
        assert_eq!(classify("abc", "def"), RefOp::Update);
    }

    #[test]
    fn doublestar_glob() {
        assert!(glob_match("refs/heads/main", "refs/heads/main"));
        assert!(glob_match("refs/heads/*", "refs/heads/main"));
        assert!(!glob_match("refs/heads/*", "refs/heads/foo/bar"));
        assert!(glob_match("refs/heads/**", "refs/heads/foo/bar"));
        assert!(glob_match("refs/tags/**", "refs/tags/v1.0"));
        assert!(glob_match("HEAD", "HEAD"));
        assert!(!glob_match("HEAD", "refs/heads/HEAD"));
        assert!(pattern_list_matches(
            &["refs/tags/**".into(), "^refs/tags/tmp/**".into()],
            "refs/tags/v1"
        ));
        assert!(!pattern_list_matches(
            &["refs/tags/**".into(), "^refs/tags/tmp/**".into()],
            "refs/tags/tmp/x"
        ));
    }

    #[test]
    fn empty_policy_allows_everything() {
        let p = RepoPolicy::empty();
        let t = txn(
            vec![
                upd("refs/heads/main", "aaa", "bbb"),
                upd("refs/heads/main", "aaa", ""),
            ],
            false,
        );
        let ev = evaluate(None, &p, "bob@example.com", &t, |_| true);
        assert!(ev.per_ref.iter().all(|(_, r)| r.is_ok()));
        assert_eq!(ev.publish.updates.len(), 2);
    }

    #[test]
    fn force_and_delete_denied_on_main() {
        let p = lock_main();
        let t = txn(
            vec![
                upd("refs/heads/main", "aaa", "bbb"),
                upd("refs/heads/dev", "aaa", "bbb"),
            ],
            false,
        );
        let ev = evaluate(None, &p, "bob@example.com", &t, |_| true);
        assert!(
            ev.per_ref[0]
                .1
                .as_ref()
                .unwrap_err()
                .contains("rejected by rule 'lock-main'")
        );
        assert!(ev.per_ref[1].1.is_ok());
        assert_eq!(ev.publish.updates.len(), 1);

        let del = txn(vec![upd("refs/heads/main", "aaa", "")], false);
        let ev = evaluate(None, &p, "bob@example.com", &del, |_| false);
        assert!(ev.per_ref[0].1.as_ref().unwrap_err().contains("lock-main"));
        assert!(!ev.any_allowed());
    }

    #[test]
    fn ff_update_allowed() {
        let p = lock_main();
        let t = txn(vec![upd("refs/heads/main", "aaa", "bbb")], false);
        let ev = evaluate(None, &p, "bob@example.com", &t, |_| false);
        assert!(ev.per_ref[0].1.is_ok());
    }

    #[test]
    fn group_bypass() {
        let p = lock_main();
        let t = txn(vec![upd("refs/heads/main", "aaa", "bbb")], false);
        let ev = evaluate(None, &p, "Alice@example.com", &t, |_| true);
        assert!(ev.per_ref[0].1.is_ok());
    }

    #[test]
    fn create_allowed_when_not_restricted() {
        let p = lock_main();
        let t = txn(vec![upd("refs/heads/main", "", "aaa")], false);
        let ev = evaluate(None, &p, "bob@example.com", &t, |_| true);
        assert!(ev.per_ref[0].1.is_ok());
    }

    #[test]
    fn atomic_denies_all() {
        let p = lock_main();
        let t = txn(
            vec![
                upd("refs/heads/main", "aaa", "bbb"),
                upd("refs/heads/dev", "aaa", "bbb"),
            ],
            true,
        );
        let ev = evaluate(None, &p, "bob@example.com", &t, |_| true);
        assert!(ev.any_denied());
        assert!(!ev.any_allowed());
        assert_eq!(ev.per_ref.len(), 2);
    }

    #[test]
    fn tag_retarget_is_force() {
        let json = r#"{
          "version": 1,
          "rules": [{
            "name": "tags-immutable",
            "match": { "refs": ["refs/tags/**"] },
            "effect": { "protect": { "restricts": ["force-push"] } }
          }]
        }"#;
        let p = parse_bytes(json.as_bytes()).unwrap();
        let t = txn(vec![upd("refs/tags/v1", "aaa", "bbb")], false);
        // even if merge-base would say ff
        let ev = evaluate(None, &p, "bob@example.com", &t, |_| false);
        assert!(ev.per_ref[0].1.is_err());
    }

    #[test]
    fn unknown_rule_key_is_parse_error() {
        let json = r#"{
          "version": 1,
          "rules": [{
            "name": "x",
            "bypass_actrs": ["a"],
            "match": {},
            "effect": { "protect": {} }
          }]
        }"#;
        assert!(parse_bytes(json.as_bytes()).is_err());
    }

    #[test]
    fn empty_restricts_is_parse_error() {
        let json = r#"{
          "version": 1,
          "rules": [{
            "name": "x",
            "match": {},
            "effect": { "protect": { "restricts": [] } }
          }]
        }"#;
        assert!(parse_bytes(json.as_bytes()).is_err());
    }

    #[test]
    fn bad_name_rejected() {
        let json = r#"{
          "version": 1,
          "rules": [{
            "name": "Lock_Main",
            "match": {},
            "effect": { "protect": {} }
          }]
        }"#;
        assert!(parse_bytes(json.as_bytes()).is_err());
    }

    #[test]
    fn disjoint_bypass_overlap_rejected() {
        let json = r#"{
          "version": 1,
          "rules": [
            {
              "name": "a",
              "match": { "refs": ["refs/heads/main"] },
              "effect": { "protect": { "bypass": ["alice@example.com"] } }
            },
            {
              "name": "b",
              "match": { "refs": ["refs/heads/main"] },
              "effect": { "protect": { "bypass": ["bob@example.com"] } }
            }
          ]
        }"#;
        assert!(parse_bytes(json.as_bytes()).is_err());
    }

    #[test]
    fn history_caret_refused() {
        let json = r#"{
          "version": 1,
          "rules": [{
            "name": "h",
            "match": { "refs": ["refs/**", "^refs/notes/**"] },
            "effect": { "history": { "allow_unrelated": false } }
          }]
        }"#;
        assert!(parse_bytes(json.as_bytes()).is_err());
    }

    /// The baseline example in `docs/POLICY.md`, verbatim (asserted below).
    const BASELINE_EXAMPLE: &str = r#"{
  "version": 1,
  "_comment": "members work in draft/** and explore/**; production is the deploy robot's; the rest of refs/heads and refs/tags is automation's",
  "rules": [
    {
      "name": "reserve-heads-and-tags",
      "match": {
        "refs": ["refs/heads/**", "refs/tags/**", "^refs/heads/draft/**", "^refs/heads/explore/**"]
      },
      "effect": { "protect": { "bypass": ["svc:deploy", "svc:platform"] } }
    },
    {
      "name": "production-deploy-only",
      "match": { "refs": ["refs/heads/production", "refs/heads/production/**"] },
      "effect": { "protect": { "bypass": ["svc:deploy"] } }
    }
  ]
}"#;

    fn baseline_example() -> RepoPolicy {
        parse_bytes(BASELINE_EXAMPLE.as_bytes()).unwrap()
    }

    /// One update per op on `name`, judged alone (non-atomic): `Ok` or the reason.
    fn verdicts(
        baseline: Option<&RepoPolicy>,
        repo: &RepoPolicy,
        who: &str,
        name: &str,
    ) -> Vec<Result<(), String>> {
        // create, fast-forward update, force update, delete
        let cases = [
            ("", "bbb", false),
            ("aaa", "bbb", false),
            ("aaa", "ccc", true),
            ("aaa", "", false),
        ];
        cases
            .iter()
            .map(|(old, new, force)| {
                let t = txn(vec![upd(name, old, new)], false);
                let ev = evaluate(baseline, repo, who, &t, |_| *force);
                ev.per_ref[0].1.clone()
            })
            .collect()
    }

    fn all_ok(v: &[Result<(), String>]) -> bool {
        v.iter().all(Result::is_ok)
    }

    fn all_denied_by(v: &[Result<(), String>], reason: &str) -> bool {
        v.iter()
            .all(|r| r.as_ref().err().is_some_and(|e| e == reason))
    }

    #[test]
    fn baseline_example_is_the_documented_one() {
        let squash = |s: &str| s.split_whitespace().collect::<String>();
        let doc = include_str!("../../../docs/POLICY.md");
        assert!(
            squash(doc).contains(&squash(BASELINE_EXAMPLE)),
            "docs/POLICY.md must carry BASELINE_EXAMPLE verbatim"
        );
    }

    #[test]
    fn baseline_example_reserves_everything_but_sandboxes() {
        let b = baseline_example();
        let none = RepoPolicy::empty();
        let reserved = "rejected by baseline rule 'reserve-heads-and-tags'";
        let deploy_only = "rejected by baseline rule 'production-deploy-only'";
        let member = "bob@example.com";
        // Members: every op in the two sandboxes, nothing else under heads/tags.
        for r in ["refs/heads/draft/x", "refs/heads/explore/a/b"] {
            assert!(all_ok(&verdicts(Some(&b), &none, member, r)), "{r}");
        }
        for r in [
            "refs/heads/main",
            "refs/heads/draft",
            "refs/heads/drafts/x",
            "refs/tags/v1",
            "refs/heads/production",
            "refs/heads/production/eu",
        ] {
            // The first denying rule is named: the reservation, for production too.
            assert!(
                all_denied_by(&verdicts(Some(&b), &none, member, r), reserved),
                "{r}"
            );
        }
        // Outside refs/heads and refs/tags the baseline says nothing.
        assert!(all_ok(&verdicts(
            Some(&b),
            &none,
            member,
            "refs/notes/commits"
        )));
        // The deploy robot: everything, production included.
        for r in [
            "refs/heads/main",
            "refs/tags/v1",
            "refs/heads/production",
            "refs/heads/production/eu",
            "refs/heads/draft/x",
        ] {
            assert!(all_ok(&verdicts(Some(&b), &none, "svc:deploy", r)), "{r}");
        }
        // The platform robot: everything but production (AND: it bypasses the
        // reservation, not the production rule).
        for r in ["refs/heads/main", "refs/tags/v1", "refs/heads/release/2"] {
            assert!(all_ok(&verdicts(Some(&b), &none, "svc:platform", r)), "{r}");
        }
        for r in ["refs/heads/production", "refs/heads/production/eu"] {
            assert!(
                all_denied_by(&verdicts(Some(&b), &none, "svc:platform", r), deploy_only),
                "{r}"
            );
        }
    }

    #[test]
    fn repository_policy_adds_to_the_baseline_never_lifts_it() {
        let b = baseline_example();
        let reserved = "rejected by baseline rule 'reserve-heads-and-tags'";
        // An empty own document (what `PUT {}` or a fresh policy.json looks like)
        // does not drop the host's protection.
        let empty = parse_bytes(br#"{"version": 1, "rules": []}"#).unwrap();
        assert!(all_denied_by(
            &verdicts(Some(&b), &empty, "bob@example.com", "refs/heads/main"),
            reserved
        ));
        // A repository rule restricts further inside a sandbox; its denials name it
        // as a repository rule.
        let own = parse_bytes(
            br#"{
              "version": 1,
              "rules": [{
                "name": "freeze-draft-release",
                "match": { "refs": ["refs/heads/draft/release"] },
                "effect": { "protect": { "restricts": ["update", "delete", "force-push"], "bypass": ["alice@example.com"] } }
              }]
            }"#,
        )
        .unwrap();
        let v = verdicts(
            Some(&b),
            &own,
            "bob@example.com",
            "refs/heads/draft/release",
        );
        assert!(
            v[0].is_ok(),
            "create is not restricted by the repository rule"
        );
        assert!(
            v[1..]
                .iter()
                .all(|r| r.as_ref().unwrap_err() == "rejected by rule 'freeze-draft-release'")
        );
        assert!(all_ok(&verdicts(
            Some(&b),
            &own,
            "alice@example.com",
            "refs/heads/draft/release"
        )));
        assert!(all_ok(&verdicts(
            Some(&b),
            &own,
            "bob@example.com",
            "refs/heads/draft/other"
        )));
        // Both deny: the baseline rule is named (evaluation order), the verdict is the same.
        let wide = parse_bytes(
            br#"{"version": 1, "rules": [{"name": "lock-main", "match": {"refs": ["refs/heads/main"]}, "effect": {"protect": {}}}]}"#,
        )
        .unwrap();
        assert!(all_denied_by(
            &verdicts(Some(&b), &wide, "bob@example.com", "refs/heads/main"),
            reserved
        ));
        // … and a robot the baseline admits is still held by the repository's rule.
        assert!(all_denied_by(
            &verdicts(Some(&b), &wide, "svc:deploy", "refs/heads/main"),
            "rejected by rule 'lock-main'"
        ));
        // Without a baseline the repository document is the whole policy.
        assert!(all_ok(&verdicts(
            None,
            &empty,
            "bob@example.com",
            "refs/heads/main"
        )));
    }

    #[test]
    fn rosters_do_not_cross_documents() {
        let b = parse_bytes(
            br#"{
              "version": 1,
              "groups": [{ "name": "deployers", "members": ["svc:deploy"] }],
              "rules": [{
                "name": "production",
                "match": { "refs": ["refs/heads/production"] },
                "effect": { "protect": { "bypass": ["group:deployers"] } }
              }]
            }"#,
        )
        .unwrap();
        // A repository that defines a group of the same name does not join it.
        let own = parse_bytes(
            br#"{"version": 1, "groups": [{ "name": "deployers", "members": ["alice@example.com"] }], "rules": []}"#,
        )
        .unwrap();
        assert!(all_denied_by(
            &verdicts(Some(&b), &own, "alice@example.com", "refs/heads/production"),
            "rejected by baseline rule 'production'"
        ));
        assert!(all_ok(&verdicts(
            Some(&b),
            &own,
            "svc:deploy",
            "refs/heads/production"
        )));
    }

    #[test]
    fn baseline_uses_the_policy_json_validator() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = walgit_config::Config::default();
        assert!(load_baseline(&cfg).unwrap().is_none());
        let path = dir.path().join("baseline.json");
        cfg.policy.baseline = Some(path.clone());
        let e = load_baseline(&cfg).unwrap_err().to_string();
        assert!(e.contains("reading policy.baseline"), "{e}");
        std::fs::write(&path, BASELINE_EXAMPLE).unwrap();
        assert_eq!(load_baseline(&cfg).unwrap(), Some(baseline_example()));
        // Every refusal of the policy.json parser refuses the baseline too.
        for bad in [
            "not json",
            r#"{"version": 2, "rules": []}"#,
            r#"{"version": 1, "rules": [{"name": "x", "bypass_actrs": [], "match": {}, "effect": {"protect": {}}}]}"#,
            r#"{"version": 1, "rules": [{"name": "x", "match": {}, "effect": {"protect": {"restricts": []}}}]}"#,
            r#"{"version": 1, "rules": [
                {"name": "a", "match": {"refs": ["refs/heads/main"]}, "effect": {"protect": {"bypass": ["alice@example.com"]}}},
                {"name": "b", "match": {"refs": ["refs/heads/main"]}, "effect": {"protect": {"bypass": ["bob@example.com"]}}}
            ]}"#,
        ] {
            std::fs::write(&path, bad).unwrap();
            let e = load_baseline(&cfg).unwrap_err().to_string();
            assert!(e.starts_with("policy.baseline "), "{bad}: {e}");
        }
    }

    #[test]
    fn unknown_envelope_key_ignored() {
        let json = r#"{
          "version": 1,
          "future_knob": true,
          "rules": []
        }"#;
        assert!(parse_bytes(json.as_bytes()).is_ok());
    }
}
