#![allow(
    clippy::case_sensitive_file_extension_comparisons,
    clippy::unnecessary_wraps
)]
// Test fixtures use panics to fail the test, including shared helper functions.
#![allow(clippy::panic, clippy::string_slice)]

//! D26/D27 + no-compat banner: **repo prefix first, lane segment second**.
//! Source-level (grep), not HTTP.
//!
//! * Repo-scoped routes start with `/{owner}/{repo}` then `/api` or `/api-browser`.
//! * Lane-first repo forms are **gone**: `/api/v1/repos`, `/api-browser/v1/repos`,
//!   `/services/api/{owner}/{repo}` (nginx rewrite of those too).
//! * Non-repo survivors: `/api/v1` (discovery/me/owners), `/api-browser/v1/me`,
//!   `/api-browser/v1/authenticate`, `/services/api/owners|instance`.
//! * Clients must not emit the deleted lane-first repo forms.

use std::fs;
use std::path::{Path, PathBuf};

type TestResult = anyhow::Result<()>;

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn read(rel: &str) -> String {
    fs::read_to_string(root().join(rel)).unwrap_or_else(|e| panic!("read {rel}: {e}"))
}

/// Non-repo routes only (AGENTS.md D26). Repo aliases are **not** allowed.
fn allowed_route(path: &str) -> bool {
    let p = path.trim();
    let allow = [
        "/",
        "/metrics",
        "/healthz",
        "/readyz",
        "/repos.js",
        "/repos.mjs",
        "/_ui/{*path}",
        "/services/public/install.sh",
        "/services/public/ca.pem",
        "/services/public/{*rest}",
        "/services/setup.json",
        "/services/api/instance",
        "/services/api/owners",
        "/services/api/owners/{owner}",
        "/api",
        "/{owner}",
    ];
    if allow.contains(&p) {
        return true;
    }
    // Non-repo API (D27): /api/v1 discovery/me/owners — never /api/v1/repos.
    if p.starts_with("/api/v1") && !p.contains("{repo}") && !p.contains("/repos/") {
        return true;
    }
    // Non-repo browser lane: /api-browser/v1/me|authenticate — never /api-browser/v1/repos.
    if (p == "/api-browser" || p.starts_with("/api-browser/"))
        && !p.contains("/repos/")
        && !p.contains("{repo}")
    {
        return true;
    }
    // Repo prefix first, then optional lane: /{o}/{r}, /{o}/{r}/api, /{o}/{r}/api-browser, …
    p.starts_with("/{owner}/{repo}")
}

fn route_literals(src: &str) -> Vec<(usize, String)> {
    let mut out = Vec::new();
    for (i, line) in src.lines().enumerate() {
        let t = line.trim();
        if t.starts_with("//") {
            continue;
        }
        // `.route("/path"` or `.route("/path",`
        let Some(idx) = t.find(".route(\"") else {
            continue;
        };
        let rest = &t[idx + ".route(\"".len()..];
        let Some(end) = rest.find('"') else {
            continue;
        };
        out.push((i + 1, rest[..end].to_string()));
    }
    out
}

#[test]
fn repo_scoped_routes_start_with_owner_repo() -> TestResult {
    let files = [
        "crates/walgit-server/src/lib.rs",
        "crates/walgit-server/src/web/api.rs",
        "crates/walgit-server/src/web/v1.rs",
        "crates/walgit-server/src/web/ui.rs",
    ];
    let mut bad = Vec::new();
    for f in files {
        for (line, path) in route_literals(&read(f)) {
            if !allowed_route(&path) {
                bad.push(format!("{f}:{line}: {path}"));
            }
        }
    }
    assert!(
        bad.is_empty(),
        "repo-scoped routes must start with /{{owner}}/{{repo}} (or be on the D26 allow-list):\n{}",
        bad.join("\n")
    );
    Ok(())
}

fn forbidden_client_hits(src: &str, rel: &str) -> Vec<String> {
    let mut hits = Vec::new();
    for (i, line) in src.lines().enumerate() {
        let t = line.trim();
        if t.starts_with("//") || t.starts_with('*') || t.starts_with("/*") {
            continue;
        }
        // Documentation of the alias in comments is fine; code that builds a URL is not.
        if t.contains("/api/v1/repos") || t.contains("/api-browser/v1/repos") {
            hits.push(format!("{rel}:{}: lane-first repo URL", i + 1));
        }
        // `/services/api/{owner}/{repo}` — not owners/instance/install.
        if let Some(rest) = t.split("/services/api/").nth(1) {
            let next = rest
                .split(|c: char| !c.is_ascii_alphabetic() && c != '{' && c != '}')
                .next()
                .unwrap_or("");
            if !matches!(next, "owners" | "instance" | "") {
                hits.push(format!("{rel}:{}: /services/api/…", i + 1));
            }
        }
    }
    hits
}

#[test]
fn clients_emit_prefix_form() -> TestResult {
    let mut hits = Vec::new();
    hits.extend(forbidden_client_hits(
        &read("web/src/api.ts"),
        "web/src/api.ts",
    ));
    hits.extend(forbidden_client_hits(
        &read("web/sdk/repos.ts"),
        "web/sdk/repos.ts",
    ));
    hits.extend(forbidden_client_hits(
        &read("crates/walgit-server/src/setup.rs"),
        "crates/walgit-server/src/setup.rs",
    ));
    for (rel, src) in walk_ts("web/src") {
        if rel.ends_with("pages/ApiPage.tsx") {
            continue; // documents the alias
        }
        hits.extend(forbidden_client_hits(&src, &rel));
    }
    assert!(
        hits.is_empty(),
        "UI/SDK/setup must not emit lane-first repo URLs (/api/v1/repos, /api-browser/v1/repos, /services/api/{{o}}/{{r}}):\n{}",
        hits.join("\n")
    );
    Ok(())
}

fn walk_ts(dir: &str) -> Vec<(String, String)> {
    fn rec(dir: &Path, root: &Path, out: &mut Vec<(String, String)>) {
        let Ok(rd) = fs::read_dir(dir) else { return };
        for e in rd.flatten() {
            let p = e.path();
            if p.is_dir() {
                rec(&p, root, out);
                continue;
            }
            let Some(name) = p.file_name().and_then(|s| s.to_str()) else {
                continue;
            };
            if !(name.ends_with(".ts") || name.ends_with(".tsx")) {
                continue;
            }
            let rel = p.strip_prefix(root).unwrap_or(&p).display().to_string();
            if let Ok(s) = fs::read_to_string(&p) {
                out.push((rel, s));
            }
        }
    }

    let mut out = Vec::new();
    let base = root().join(dir);

    rec(&base, &root(), &mut out);
    out
}

/// Semantics of the edge large-repository location `~ ^/<o>/<r>(?:[./?]|$)`.
#[test]
fn repo_prefix_location_regex_semantics() {
    fn matches(path: &str, repo: &str) -> bool {
        let p = format!("/{repo}");
        path == p
            || path.starts_with(&format!("{p}/"))
            || path.starts_with(&format!("{p}."))
            || path.starts_with(&format!("{p}?"))
    }
    for ok in [
        "/acme/monorepo",
        "/acme/monorepo/",
        "/acme/monorepo.git/info/refs",
        "/acme/monorepo/api/refs",
        "/acme/monorepo/api-browser/refs",
        "/acme/monorepo/bundles/weekly/x",
        "/acme/monorepo/info/lfs/objects/aa",
        "/acme/monorepo/settings",
        "/acme/monorepo/tree/main",
    ] {
        assert!(matches(ok, "acme/monorepo"), "{ok}");
    }
    for no in [
        "/acme/monorepowide",
        "/acme/monorepo2",
        "/acme/monorepo-mirror",
        "/acme/monorep",
    ] {
        assert!(!matches(no, "acme/monorepo"), "{no}");
    }
}

/// D27: lane-first **repo** forms are gone. `/api-browser/v1/me|authenticate` and
/// `/{o}/{r}/api-browser` stay.
#[test]
fn deleted_aliases_are_gone() {
    let mut hits = Vec::new();
    for rel in [
        "crates/walgit-server/src/lib.rs",
        "crates/walgit-server/src/web/api.rs",
        "crates/walgit-server/src/web/v1.rs",
        "crates/walgit-server/src/web/ui.rs",
        "crates/walgit-server/src/settings.rs",
    ] {
        hits.extend(alias_hits(&read(rel), rel));
    }
    assert!(
        hits.is_empty(),
        "lane-first repo aliases still present (/api/v1/repos, /api-browser/v1/repos, /services/api/{{o}}/{{r}}):\n{}",
        hits.join("\n")
    );
}

fn alias_hits(src: &str, rel: &str) -> Vec<String> {
    let mut hits = Vec::new();
    for (i, line) in src.lines().enumerate() {
        let t = line.trim();
        if t.is_empty() {
            continue;
        }
        let services_repo_alias =
            t.contains("/services/api/{owner}/{repo}") || t.contains("/services/api/{o}/{r}");
        if t.contains("/api/v1/repos") {
            hits.push(format!("{rel}:{}: /api/v1/repos", i + 1));
        }
        if t.contains("/api-browser/v1/repos") {
            hits.push(format!("{rel}:{}: /api-browser/v1/repos", i + 1));
        }
        if services_repo_alias {
            hits.push(format!("{rel}:{}: /services/api/{{o}}/{{r}}", i + 1));
        }
    }
    hits
}
