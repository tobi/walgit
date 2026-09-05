#![allow(clippy::too_many_arguments)]
//! Render the bundle list in git's bundle-list config format and protocol v2
//! key=value lines.
//!
//! See: <https://git-scm.com/docs/bundle-uri> and
//!      <https://git-scm.com/docs/gitprotocol-v2> (bundle-uri command).

use std::time::Duration;

use walgit_config::{BundleServe, BundlesConfig};
use walgit_proto::v1::{BundleEntry, BundleList};
use walgit_store::{ObjectStore, Prefixed};

use crate::BundleError;

/// Extract the filename from a bundle key like
/// `bundles/weekly/20231114T221320Z-abc.bundle` → `20231114T221320Z-abc.bundle`.
fn filename_of(key: &str) -> &str {
    key.rsplit('/').next().unwrap_or(key)
}

/// Build the URI for a single bundle entry.
///
/// * **Proxy**: `{base_url}/{owner}/{repo}/bundles/{strategy}/{filename}`
/// * **`SignedUrl`**: `store.signed_get_url(key, ttl)`, falling back to Proxy
///   if the store doesn't support signed URLs.
pub async fn bundle_uri(
    entry: &BundleEntry,
    owner: &str,
    repo: &str,
    base_url: &str,
    serve_via: BundleServe,
    store: &Prefixed,
    signed_ttl: Duration,
) -> Result<String, BundleError> {
    match serve_via {
        BundleServe::SignedUrl => match store.signed_get_url(&entry.key, signed_ttl).await {
            Ok(Some(url)) => Ok(url),
            Ok(None) => Ok(proxy_uri(entry, owner, repo, base_url)),
            // Signing may be unavailable or denied by the store. A listing must
            // never fail on that: fall back to the authenticated proxy URI and
            // say so once per repository.
            Err(e) => {
                warn_signing_once(owner, repo, &e);
                Ok(proxy_uri(entry, owner, repo, base_url))
            }
        },
        BundleServe::Proxy => Ok(proxy_uri(entry, owner, repo, base_url)),
    }
}

static SIGNING_WARNED: std::sync::LazyLock<std::sync::Mutex<std::collections::HashSet<String>>> =
    std::sync::LazyLock::new(|| std::sync::Mutex::new(std::collections::HashSet::new()));

fn warn_signing_once(owner: &str, repo: &str, e: &dyn std::fmt::Display) {
    let key = format!("{owner}/{repo}");
    if SIGNING_WARNED
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert(key.clone())
    {
        tracing::warn!(repo = %key, error = %e, "signed bundle URL failed; serving proxy URIs instead (check the store signing permissions)");
    }
}

/// Relative path of a bundle on this host: `/{owner}/{repo}/bundles/{strategy}/{file}`.
pub fn bundle_path(entry: &BundleEntry, owner: &str, repo: &str) -> String {
    format!(
        "/{owner}/{repo}/bundles/{}/{}",
        entry.strategy,
        filename_of(&entry.key)
    )
}

/// Construct the proxy-style URI.
fn proxy_uri(entry: &BundleEntry, owner: &str, repo: &str, base_url: &str) -> String {
    let base = base_url.trim_end_matches('/');
    let filename = filename_of(&entry.key);
    format!(
        "{base}/{owner}/{repo}/bundles/{}/{}",
        entry.strategy, filename
    )
}

/// Render the bundle list as git config text (bundle-list format).
///
/// ```ini
/// [bundle]
///     version = 1
///     mode = all
///     heuristic = creationToken
///
/// [bundle "<id>"]
///     uri = <uri>
///     creationToken = <token>
/// ```
pub async fn render_list_text(
    list: &BundleList,
    owner: &str,
    repo: &str,
    base_url: &str,
    cfg: &BundlesConfig,
    store: &Prefixed,
    filter: Option<&str>,
    fulls: bool,
) -> Result<String, BundleError> {
    let mut out = String::new();
    out.push_str("[bundle]\n");
    out.push_str("    version = 1\n");
    out.push_str("    mode = all\n");
    out.push_str("    heuristic = creationToken\n");

    // One family per list: the entries whose filter equals the requested one
    // ("" = the unfiltered chain). Sorted by creation_token so git processes
    // them in order. `fulls = false` is the catch-up list (`bundles/catchup`, what the recipes put
    // in `fetch.bundleURI`): a client that fetches has history, it only ever needs the incremental
    // links above what it has — and git's creationToken walk would otherwise download every full
    // newer than its token (32 GB from a large repository on the first fetch after Sunday; rig, 2026-08-22).
    let want = filter.unwrap_or("");
    let mut bundles: Vec<&BundleEntry> = list
        .bundles
        .iter()
        .filter(|b| b.filter == want || (filter.is_none() && cfg.advertise_filtered))
        .filter(|b| fulls || !b.base_id.is_empty())
        .collect();
    bundles.sort_by_key(|b| b.creation_token);

    for entry in &bundles {
        let uri = bundle_uri(
            entry,
            owner,
            repo,
            base_url,
            serve_via_for(cfg, owner, repo),
            store,
            cfg.signed_url_ttl,
        )
        .await?;
        out.push('\n');
        {
            let _ =
                std::fmt::Write::write_fmt(&mut out, format_args!("[bundle \"{}\"]\n", entry.id));
        };
        {
            let _ = std::fmt::Write::write_fmt(&mut out, format_args!("    uri = {uri}\n"));
        };
        {
            let _ = std::fmt::Write::write_fmt(
                &mut out,
                format_args!("    creationToken = {}\n", entry.creation_token),
            );
        };
        if !entry.filter.is_empty() {
            {
                let _ = std::fmt::Write::write_fmt(
                    &mut out,
                    format_args!("    filter = {}\n", entry.filter),
                );
            };
        }
    }

    Ok(out)
}

/// Render the bundle list as protocol v2 key=value lines.
///
/// ```text
/// bundle.version=1
/// bundle.mode=all
/// bundle.heuristic=creationToken
/// bundle.<id>.uri=<uri>
/// bundle.<id>.creationtoken=<token>
/// ```
///
/// Subkeys are lowercase: git parses protocol lines with `bundle_list_update`,
/// which `strcmp`s the subkey against `"creationtoken"` (config files are
/// case-insensitive, the protocol is not; `git upload-pack` emits config keys
/// lowercased). A camelCase key is silently ignored -> creationToken 0 ->
/// "failed to fetch advertised bundles".
pub async fn protocol_v2_lines(
    list: &BundleList,
    owner: &str,
    repo: &str,
    base_url: &str,
    cfg: &BundlesConfig,
    store: &Prefixed,
) -> Result<Vec<String>, BundleError> {
    let mut lines = vec![
        "bundle.version=1".to_string(),
        "bundle.mode=all".to_string(),
        "bundle.heuristic=creationToken".to_string(),
    ];

    // The protocol-advertised list is the UNFILTERED chain only: git applies
    // no filter matching, and a full clone must never swallow a blobless bundle.
    let mut bundles: Vec<&BundleEntry> = list
        .bundles
        .iter()
        .filter(|b| b.filter.is_empty() || cfg.advertise_filtered)
        .collect();
    bundles.sort_by_key(|b| b.creation_token);

    for entry in &bundles {
        let uri = bundle_uri(
            entry,
            owner,
            repo,
            base_url,
            serve_via_for(cfg, owner, repo),
            store,
            cfg.signed_url_ttl,
        )
        .await?;
        lines.push(format!("bundle.{}.uri={}", entry.id, uri));
        lines.push(format!(
            "bundle.{}.creationtoken={}",
            entry.id, entry.creation_token
        ));
        if !entry.filter.is_empty() {
            lines.push(format!("bundle.{}.filter={}", entry.id, entry.filter));
        }
    }

    Ok(lines)
}

/// `serve_via` for one repository (`bundles.signed_url_for` overrides).
fn serve_via_for(cfg: &walgit_config::BundlesConfig, owner: &str, repo: &str) -> BundleServe {
    if walgit_config::repo_listed(&cfg.signed_url_for, owner, repo) {
        BundleServe::SignedUrl
    } else {
        cfg.serve_via
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use walgit_proto::v1::BundleEntry;

    fn make_entry(id: &str, strategy: &str, token: u64) -> BundleEntry {
        BundleEntry {
            id: id.into(),
            key: format!("bundles/{strategy}/{id}.bundle"),
            strategy: strategy.into(),
            kind: "full".into(),
            filter: String::new(),
            creation_token: token,
            slot: 0,
            seq: 1,
            size: 100,
            base_id: String::new(),
            created_at: None,
            version: "v1".into(),
            tips: vec![],
        }
    }

    fn make_cfg() -> BundlesConfig {
        BundlesConfig::default()
    }

    fn make_store() -> Prefixed {
        Prefixed::new(
            std::sync::Arc::new(walgit_store::memory::MemoryStore::new()) as walgit_store::DynStore,
            "repos/test/repo/",
        )
    }

    #[tokio::test]
    async fn render_list_config_format() {
        let list = BundleList {
            mode: "all".into(),
            heuristic: "creationToken".into(),
            bundles: vec![
                make_entry("weekly-100", "weekly", 100),
                make_entry("daily-200", "daily", 200),
            ],
            ..Default::default()
        };
        let cfg = make_cfg();
        let store = make_store();
        let text = render_list_text(
            &list,
            "test",
            "repo",
            "https://example.com",
            &cfg,
            &store,
            None,
            true,
        )
        .await
        .unwrap();
        assert!(text.contains("[bundle]"));
        assert!(text.contains("version = 1"));
        assert!(text.contains("mode = all"));
        assert!(text.contains("heuristic = creationToken"));
        assert!(text.contains("[bundle \"weekly-100\"]"));
        assert!(
            text.contains("uri = https://example.com/test/repo/bundles/weekly/weekly-100.bundle")
        );
        assert!(text.contains("creationToken = 100"));
        assert!(text.contains("[bundle \"daily-200\"]"));
        assert!(text.contains("creationToken = 200"));
    }

    #[tokio::test]
    async fn protocol_v2_format() {
        let list = BundleList {
            mode: "all".into(),
            heuristic: "creationToken".into(),
            bundles: vec![make_entry("weekly-100", "weekly", 100)],
            ..Default::default()
        };
        let cfg = make_cfg();
        let store = make_store();
        let lines = protocol_v2_lines(&list, "test", "repo", "https://example.com", &cfg, &store)
            .await
            .unwrap();
        assert!(lines.contains(&"bundle.version=1".to_string()));
        assert!(lines.contains(&"bundle.mode=all".to_string()));
        assert!(lines.contains(&"bundle.heuristic=creationToken".to_string()));
        assert!(lines.contains(&"bundle.weekly-100.uri=https://example.com/test/repo/bundles/weekly/weekly-100.bundle".to_string()));
        assert!(lines.contains(&"bundle.weekly-100.creationtoken=100".to_string()));
    }

    #[test]
    fn filename_extraction() {
        assert_eq!(filename_of("bundles/weekly/abc.bundle"), "abc.bundle");
        assert_eq!(filename_of("abc.bundle"), "abc.bundle");
    }
}
