#![allow(clippy::many_single_char_names)]
//! `/api/v1` (D20): the versioned programmatic surface, its browser-lane alias
//! (`/api-browser`), CORS for foreign origins, discovery, `me`, repo summary and
//! admin, and the SDK artefact route.

mod harness;

use harness::{Server, git_in};
use serde_json::Value;

type TestResult = anyhow::Result<()>;

async fn req(
    server: &Server,
    method: reqwest::Method,
    path: &str,
    extra: &[(&str, &str)],
) -> anyhow::Result<(reqwest::StatusCode, String, reqwest::header::HeaderMap)> {
    let mut r = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()?
        .request(method, format!("{}{path}", server.base_url))
        .header("Accept", "application/json");
    for (k, v) in extra {
        r = r.header(*k, *v);
    }
    let resp = r.send().await?;
    let status = resp.status();
    let headers = resp.headers().clone();
    Ok((status, resp.text().await?, headers))
}
async fn req_body(
    server: &Server,
    method: reqwest::Method,
    path: &str,
    extra: &[(&str, &str)],
    body: &'static str,
) -> anyhow::Result<(reqwest::StatusCode, String)> {
    let mut r = reqwest::Client::new()
        .request(method, format!("{}{path}", server.base_url))
        .header("Accept", "application/json")
        .body(body);
    for (k, v) in extra {
        r = r.header(*k, *v);
    }
    let resp = r.send().await?;
    let status = resp.status();
    Ok((status, resp.text().await?))
}
fn hdr(h: &reqwest::header::HeaderMap, k: &str) -> String {
    h.get(k)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string()
}
async fn json(server: &Server, path: &str) -> anyhow::Result<Value> {
    let (st, text, _) = req(server, reqwest::Method::GET, path, &[]).await?;
    anyhow::ensure!(st.is_success(), "GET {path} -> {st}: {text}");
    Ok(serde_json::from_str(&text)?)
}

fn fixture(server: &Server) -> anyhow::Result<String> {
    let dir = tempfile::tempdir()?.keep();
    git_in(&dir, &["init", "-q", "-b", "main"])?;
    git_in(&dir, &["config", "user.email", "t@t"])?;
    git_in(&dir, &["config", "user.name", "Tester"])?;
    std::fs::write(dir.join("README.md"), "# v1\n")?;
    git_in(&dir, &["add", "."])?;
    git_in(
        &dir,
        &[
            "commit",
            "-q",
            "-m",
            "initial\n\nSee https://github.com/o/r/pull/7 for context.\n\nMerge-Queue-Phase: target-publish\nMerge-Queue-Pull-Request: 7\nCo-authored-by: Jane <jane@example.com>",
        ],
    )?;
    git_in(
        &dir,
        &[
            "-c",
            "tag.forceSignAnnotated=false",
            "-c",
            "tag.gpgsign=false",
            "tag",
            "v1",
        ],
    )?;
    git_in(&dir, &["branch", "feature/x"])?;
    git_in(
        &dir,
        &["push", "-q", "--mirror", &server.repo_url("o", "r")],
    )?;
    Ok(git_in(&dir, &["rev-parse", "HEAD"])?.trim().to_string())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn v1_surface_and_browser_lane() -> TestResult {
    let server = Server::start_with_tweak(|c| {
        c.server.cors_origins = vec!["https://*.docs.example.com".into()];
    })
    .await?;
    server.put_repo("o", "r").await?;
    let head = fixture(&server)?;

    // discovery
    let d = json(&server, "/api/v1").await?;
    assert_eq!(d["version"], 1);
    assert!(d["sdk"].as_str().unwrap().ends_with("/repos.js"));
    assert!(
        d["browser_base"]
            .as_str()
            .unwrap()
            .ends_with("/api-browser/v1")
    );
    assert!(
        d["auth"]["authenticate"]
            .as_str()
            .unwrap()
            .ends_with("/api-browser/v1/authenticate")
    );
    // `docs` is this host's API page, derived from the same base as every
    // other URL in the document (AGENTS.md §5: no hardcoded hostnames).
    let base = d["base"].as_str().unwrap();
    assert_eq!(
        d["docs"],
        format!("{}/api", base.trim_end_matches("/api/v1")),
        "{d}"
    );

    // me (auth mode none in tests → anonymous principal)
    let (st, _, h) = req(&server, reqwest::Method::GET, "/api/v1/me", &[]).await?;
    assert_eq!(st, 200);
    assert_eq!(hdr(&h, "cache-control"), "no-store");

    // owners
    assert_eq!(
        json(&server, "/api/v1/owners").await?,
        serde_json::json!(["o"])
    );
    assert_eq!(
        json(&server, "/api/v1/owners/o/repos").await?,
        serde_json::json!(["r"])
    );
    assert_eq!(
        json(&server, "/api/v1/owners/nobody/repos").await?,
        serde_json::json!([])
    );

    // repo summary: SWR + ETag on head
    let (st, text, h) = req(&server, reqwest::Method::GET, "/o/r/api", &[]).await?;
    assert_eq!(st, 200, "{text}");
    let s: Value = serde_json::from_str(&text)?;
    assert_eq!(s["full_name"], "o/r");
    assert_eq!(s["head"]["name"], "main");
    assert_eq!(s["head"]["sha"], head);
    assert_eq!(s["branches"], 2);
    assert_eq!(s["tags"], 1);
    assert!(s["clone_url"].as_str().unwrap().ends_with("/o/r.git"));
    assert!(s["api_url"].as_str().unwrap().ends_with("/o/r/api"));
    assert_eq!(hdr(&h, "etag"), format!("\"{head}\""));
    assert!(hdr(&h, "cache-control").contains("stale-while-revalidate"));
    let (st, _, _) = req(
        &server,
        reqwest::Method::GET,
        "/o/r/api",
        &[("If-None-Match", &format!("\"{head}\""))],
    )
    .await?;
    assert_eq!(st, 304);
    assert_eq!(
        req(&server, reqwest::Method::GET, "/o/nope/api", &[])
            .await?
            .0,
        404
    );

    // the repo-scoped read endpoints are the same handlers as /{o}/{r}/api/…
    let refs = json(&server, "/o/r/api/refs").await?;
    assert_eq!(refs["head"]["sha"], head);
    let r = json(&server, "/o/r/api/resolve/feature/x").await?;
    assert_eq!(r["kind"], "branch");
    let t = json(&server, &format!("/o/r/api/tree/{head}")).await?;
    assert_eq!(t["entries"][0]["name"], "README.md");
    let b = json(&server, &format!("/o/r/api/blob/{head}/README.md")).await?;
    assert_eq!(b["contents"], "# v1\n");
    let c = json(&server, &format!("/o/r/api/commits?ref={head}")).await?;
    assert_eq!(c["commits"][0]["subject"], "initial");
    let c = json(&server, &format!("/o/r/api/commit/{head}")).await?;
    assert_eq!(c["commit"]["sha"], head);
    // Trailers split off the body (git interpret-trailers rules); body keeps the prose + URL.
    assert_eq!(
        c["commit"]["body"],
        "See https://github.com/o/r/pull/7 for context."
    );
    assert_eq!(
        c["commit"]["trailers"][1]["key"],
        "Merge-Queue-Pull-Request"
    );
    assert_eq!(c["commit"]["trailers"][1]["value"], "7");
    assert_eq!(c["commit"]["trailers"].as_array().unwrap().len(), 3);
    let tags = json(&server, "/o/r/api/refs/tags").await?;
    assert_eq!(tags["refs"][0]["name"], "v1");
    let tasks = json(&server, "/o/r/api/tasks").await?;
    assert!(tasks["running"].is_array());
    let (st, _, _) = req(&server, reqwest::Method::GET, "/o/r/api/overview", &[]).await?;
    assert_eq!(st, 200);

    // browser lane: /{o}/{r}/api-browser/… is the same surface (query preserved)
    let (st, text, _) = req(
        &server,
        reqwest::Method::GET,
        &format!("/o/r/api-browser/commits?ref={head}&n=1"),
        &[],
    )
    .await?;
    assert_eq!(st, 200, "{text}");
    let c: Value = serde_json::from_str(&text)?;
    assert_eq!(c["commits"].as_array().unwrap().len(), 1);

    // CORS: allowed wildcard origin gets credentials; foreign origin gets nothing; preflight is open.
    let (st, _, h) = req(
        &server,
        reqwest::Method::GET,
        "/o/r/api/refs",
        &[("Origin", "https://wiki.docs.example.com")],
    )
    .await?;
    assert_eq!(st, 200);
    assert_eq!(
        hdr(&h, "access-control-allow-origin"),
        "https://wiki.docs.example.com"
    );
    assert_eq!(hdr(&h, "access-control-allow-credentials"), "true");
    assert!(hdr(&h, "access-control-expose-headers").contains("ETag"));
    assert!(
        h.get_all("vary")
            .iter()
            .any(|v| v.to_str().unwrap().contains("Origin"))
    );
    let (st, _, h) = req(
        &server,
        reqwest::Method::GET,
        "/o/r/api/refs",
        &[("Origin", "https://evil.example")],
    )
    .await?;
    assert_eq!(st, 200);
    assert_eq!(hdr(&h, "access-control-allow-origin"), "");
    let (st, _, h) = req(
        &server,
        reqwest::Method::OPTIONS,
        "/o/r/api-browser/refs",
        &[
            ("Origin", "https://x.docs.example.com"),
            ("Access-Control-Request-Method", "GET"),
            ("Access-Control-Request-Headers", "authorization"),
        ],
    )
    .await?;
    assert_eq!(st, 204);
    assert!(hdr(&h, "access-control-allow-methods").contains("GET"));
    assert!(
        hdr(&h, "access-control-allow-headers")
            .to_ascii_lowercase()
            .contains("authorization")
    );
    // a state-changing call from a foreign origin is refused before it reaches a handler
    let (st, _, _) = req(
        &server,
        reqwest::Method::DELETE,
        "/o/r/api/policy",
        &[("Origin", "https://evil.example")],
    )
    .await?;
    assert_eq!(st, 403);
    // non-API paths never get CORS headers
    let (_, _, h) = req(
        &server,
        reqwest::Method::GET,
        "/o/r.git/info/refs?service=git-upload-pack",
        &[("Origin", "https://x.docs.example.com")],
    )
    .await?;
    assert_eq!(hdr(&h, "access-control-allow-origin"), "");
    // the browser lane is the same surface under /api-browser (D27)
    let (st, _, h) = req(
        &server,
        reqwest::Method::GET,
        "/o/r/api-browser/refs",
        &[("Origin", "https://x.docs.example.com")],
    )
    .await?;
    assert_eq!(st, 200);
    assert_eq!(
        hdr(&h, "access-control-allow-origin"),
        "https://x.docs.example.com"
    );
    // the lane-first forms are gone (banner: no aliases)
    for gone in [
        "/api/v1/repos/o/r",
        "/api/v1/repos/o/r/refs",
        "/api-browser/v1/repos/o/r/refs",
        "/services/api/o/r/refs",
    ] {
        assert_eq!(
            req(&server, reqwest::Method::GET, gone, &[]).await?.0,
            404,
            "{gone} must be gone"
        );
    }

    // policy + repo admin under the repo prefix
    let (st, _, _) = req(&server, reqwest::Method::GET, "/o/r/api/policy", &[]).await?;
    assert_eq!(st, 200);
    let (st, _, _) = req(&server, reqwest::Method::PUT, "/o/new/api", &[]).await?;
    assert!(st.is_success(), "{st}");
    assert_eq!(
        json(&server, "/api/v1/owners/o/repos").await?,
        serde_json::json!(["new", "r"])
    );
    let (st, _, _) = req(&server, reqwest::Method::DELETE, "/o/new/api", &[]).await?;
    assert!(st.is_success(), "{st}");
    assert_eq!(
        json(&server, "/api/v1/owners/o/repos").await?,
        serde_json::json!(["r"])
    );

    // authenticate: anonymous mode is "signed in" → the popup page
    let (st, text, h) = req(
        &server,
        reqwest::Method::GET,
        "/api-browser/v1/authenticate",
        &[],
    )
    .await?;
    assert_eq!(st, 200, "{text}");
    assert!(hdr(&h, "content-type").starts_with("text/html"));
    assert!(text.contains("repos:authenticated"));

    // the SDK artefacts (built into web/dist by `pnpm run build`) at their permanent URLs
    for name in ["/repos.js", "/repos.mjs"] {
        let (st, body, h) = req(&server, reqwest::Method::GET, name, &[]).await?;
        assert_eq!(st, 200, "{name}");
        assert!(
            hdr(&h, "content-type").starts_with("text/javascript"),
            "{name}"
        );
        assert_eq!(hdr(&h, "cache-control"), "no-cache");
        assert!(!hdr(&h, "etag").is_empty());
        // D27: the SDK puts the lane after the repository (`/o/r/api` | `/o/r/api-browser`) and
        // opens `/api-browser/v1/authenticate`; it never emits the deleted lane-first forms.
        assert!(
            body.contains("/api-browser/v1/authenticate") && body.contains("repos:authenticated"),
            "{name} is not the SDK"
        );
        assert!(
            !body.contains("/v1/repos") && !body.contains("services/api/"),
            "{name} emits a deleted lane-first form"
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_cors_without_config() -> TestResult {
    let server = Server::start().await?;
    let (st, _, h) = req(
        &server,
        reqwest::Method::GET,
        "/api/v1/owners",
        &[("Origin", "https://x.docs.example.com")],
    )
    .await?;
    assert_eq!(st, 200);
    assert_eq!(hdr(&h, "access-control-allow-origin"), "");
    let (st, _, h) = req(
        &server,
        reqwest::Method::OPTIONS,
        "/api/v1/owners",
        &[
            ("Origin", "https://x.docs.example.com"),
            ("Access-Control-Request-Method", "GET"),
        ],
    )
    .await?;
    assert_eq!(st, 204);
    assert_eq!(hdr(&h, "access-control-allow-origin"), "");
    Ok(())
}

/// D26/D27: everything of a repository under its own prefix — the
/// admin/settings surface at `/{o}/{r}/api[/policy|/settings…]`, and the
/// same under the browser lane `/{o}/{r}/api-browser/…`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn d26_prefix_form_matches_v1_alias() -> TestResult {
    let server = Server::start().await?;
    let c = reqwest::Client::new();
    // create via the prefix form
    assert_eq!(
        c.put(format!("{}/t/pfx/api", server.base_url))
            .send()
            .await?
            .status(),
        201
    );
    let a: serde_json::Value = c
        .get(format!("{}/t/pfx/api", server.base_url))
        .send()
        .await?
        .json()
        .await?;
    let b: serde_json::Value = c
        .get(format!("{}/t/pfx/api-browser", server.base_url))
        .send()
        .await?
        .json()
        .await?;
    assert_eq!(a["full_name"], "t/pfx");
    assert_eq!(a["full_name"], b["full_name"]);
    // settings + policy through the prefix form
    let r = c
        .put(format!(
            "{}/t/pfx/api/settings?message=via+prefix",
            server.base_url
        ))
        .body("[packs]\nfold_when_fresh_packs_reach = 7\n")
        .send()
        .await?;
    assert_eq!(r.status(), 200, "{}", r.text().await?);
    let s: serde_json::Value = c
        .get(format!("{}/t/pfx/api-browser/settings", server.base_url))
        .send()
        .await?
        .json()
        .await?;
    assert_eq!(s["revision"], 1);
    assert_eq!(s["message"], "via prefix");
    let d: serde_json::Value = c
        .get(format!("{}/t/pfx/api/settings/describe", server.base_url))
        .send()
        .await?
        .json()
        .await?;
    assert_eq!(d["packs"]["fold_when_fresh_packs_reach"], 7);
    let p: serde_json::Value = c
        .get(format!("{}/t/pfx/api/policy", server.base_url))
        .send()
        .await?
        .json()
        .await?;
    assert!(p.is_object());
    let v: serde_json::Value = c
        .post(format!("{}/t/pfx/api/policy/validate", server.base_url))
        .body("{}")
        .send()
        .await?
        .json()
        .await?;
    assert_eq!(v["ok"], true);
    // refs etc. (already D15)
    assert_eq!(
        c.get(format!("{}/t/pfx/api/refs", server.base_url))
            .send()
            .await?
            .status(),
        200
    );
    // delete via the prefix form
    assert_eq!(
        c.delete(format!("{}/t/pfx/api", server.base_url))
            .send()
            .await?
            .status(),
        204
    );
    assert_eq!(
        c.get(format!("{}/t/pfx/api", server.base_url))
            .send()
            .await?
            .status(),
        404
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repository_delete_requires_admin() -> TestResult {
    let server = Server::start_with_tweak(|c| {
        c.server.auth.mode = walgit_config::AuthMode::Token;
        c.server.auth.anonymous_read = false;
        c.server.auth.tokens = vec![
            walgit_config::StaticToken {
                principal: "writer".into(),
                token: "writer-token".into(),
                token_env: None,
                write: true,
                admin: false,
            },
            walgit_config::StaticToken {
                principal: "admin".into(),
                token: "admin-token".into(),
                token_env: None,
                write: true,
                admin: true,
            },
        ];
    })
    .await?;

    let writer = [("Authorization", "Bearer writer-token")];
    let admin = [("Authorization", "Bearer admin-token")];

    assert_eq!(
        req(&server, reqwest::Method::PUT, "/secure/delete/api", &writer,)
            .await?
            .0,
        201,
        "write permission still creates repositories"
    );

    for path in [
        "/secure/delete",
        "/secure/delete/api",
        "/secure/delete/api-browser",
    ] {
        assert_eq!(
            req(&server, reqwest::Method::DELETE, path, &writer)
                .await?
                .0,
            403,
            "non-admin deletion through {path}"
        );
    }
    assert_eq!(
        req(&server, reqwest::Method::GET, "/secure/delete/api", &writer,)
            .await?
            .0,
        200,
        "forbidden deletion must leave the repository intact"
    );

    assert_eq!(
        req(
            &server,
            reqwest::Method::DELETE,
            "/secure/delete/api",
            &admin,
        )
        .await?
        .0,
        204
    );
    assert_eq!(
        req(&server, reqwest::Method::GET, "/secure/delete/api", &admin,)
            .await?
            .0,
        404
    );
    Ok(())
}

/// D24 and API.md §5: on the JSON surface a write token creates repositories,
/// but the policy and settings documents move only with admin.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn policy_and_settings_writes_require_admin() -> TestResult {
    const POLICY: &str = r#"{"version":1,"groups":[],"rules":[]}"#;
    const SETTINGS: &str = "[packs]\nfold_when_fresh_packs_reach = 3\n";
    let server = Server::start_with_tweak(|c| {
        c.server.auth.mode = walgit_config::AuthMode::Token;
        c.server.auth.anonymous_read = false;
        c.server.auth.tokens = vec![
            walgit_config::StaticToken {
                principal: "writer".into(),
                token: "writer-token".into(),
                token_env: None,
                write: true,
                admin: false,
            },
            walgit_config::StaticToken {
                principal: "admin".into(),
                token: "admin-token".into(),
                token_env: None,
                write: true,
                admin: true,
            },
        ];
    })
    .await?;
    let writer = [("Authorization", "Bearer writer-token")];
    let admin = [("Authorization", "Bearer admin-token")];
    assert_eq!(
        req(&server, reqwest::Method::PUT, "/gates/repo/api", &writer)
            .await?
            .0,
        201,
        "write permission creates the repository"
    );

    for (path, body) in [
        ("/gates/repo/api/policy", POLICY),
        ("/gates/repo/api/settings", SETTINGS),
    ] {
        let (st, text) = req_body(&server, reqwest::Method::PUT, path, &writer, body).await?;
        assert_eq!(st, 403, "a write token must not PUT {path}: {text}");
        assert_eq!(
            req(&server, reqwest::Method::DELETE, path, &writer)
                .await?
                .0,
            403,
            "a write token must not DELETE {path}"
        );
    }
    let (st, text) = req_body(
        &server,
        reqwest::Method::PUT,
        "/gates/repo/api/policy",
        &admin,
        POLICY,
    )
    .await?;
    assert_eq!(st, 204, "{text}");
    let (st, text) = req_body(
        &server,
        reqwest::Method::PUT,
        "/gates/repo/api/settings",
        &admin,
        SETTINGS,
    )
    .await?;
    assert_eq!(st, 200, "{text}");
    Ok(())
}

/// Human-readable metadata (`crate::metadata`): a repository's description and an owner's
/// profile — read with read, written only with admin (also at create), strict on write,
/// absent fields omitted, and the owner listings' `?detail=1` beside unchanged plain shapes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn descriptions_and_owner_profiles() -> TestResult {
    use reqwest::Method;
    async fn send(
        server: &Server,
        method: Method,
        path: &str,
        auth: &[(&str, &str)],
        body: String,
    ) -> anyhow::Result<(reqwest::StatusCode, String)> {
        let mut r = reqwest::Client::new()
            .request(method, format!("{}{path}", server.base_url))
            .header("Accept", "application/json")
            .body(body);
        for (k, v) in auth {
            r = r.header(*k, *v);
        }
        let resp = r.send().await?;
        let status = resp.status();
        Ok((status, resp.text().await?))
    }
    async fn get_json(server: &Server, path: &str, auth: &[(&str, &str)]) -> anyhow::Result<Value> {
        let (st, text, _) = req(server, Method::GET, path, auth).await?;
        anyhow::ensure!(st == 200, "GET {path} -> {st}: {text}");
        Ok(serde_json::from_str(&text)?)
    }
    let token = |principal: &str, write: bool, admin: bool| walgit_config::StaticToken {
        principal: principal.into(),
        token: format!("{principal}-token"),
        token_env: None,
        write,
        admin,
    };
    let tokens = vec![
        token("reader", false, false),
        token("writer", true, false),
        token("admin", true, true),
    ];
    let server = Server::start_with_tweak(move |c| {
        c.server.auth.mode = walgit_config::AuthMode::Token;
        c.server.auth.anonymous_read = false;
        c.server.auth.tokens = tokens;
    })
    .await?;
    let reader = [("Authorization", "Bearer reader-token")];
    let writer = [("Authorization", "Bearer writer-token")];
    let admin = [("Authorization", "Bearer admin-token")];
    for repo in ["u1/alpha", "u1/beta", "u2/gamma"] {
        assert_eq!(
            req(&server, Method::PUT, &format!("/{repo}/api"), &writer)
                .await?
                .0,
            201
        );
    }

    // ---- repository description -------------------------------------------------------
    assert_eq!(
        req(&server, Method::GET, "/u1/alpha/api/description", &[])
            .await?
            .0,
        401,
        "reads need a credential"
    );
    assert_eq!(
        get_json(&server, "/u1/alpha/api/description", &reader).await?,
        serde_json::json!({})
    );
    let doc = r#"{"description":"  Alpha service  "}"#.to_string();
    for who in [&reader, &writer] {
        let (st, text) = send(
            &server,
            Method::PUT,
            "/u1/alpha/api/description",
            who,
            doc.clone(),
        )
        .await?;
        assert_eq!(st, 403, "{text}");
        assert_eq!(
            req(&server, Method::DELETE, "/u1/alpha/api/description", who)
                .await?
                .0,
            403
        );
    }
    let (st, text) = send(
        &server,
        Method::PUT,
        "/u1/alpha/api/description",
        &admin,
        doc.clone(),
    )
    .await?;
    assert_eq!(st, 204, "{text}");
    let (st, text, h) = req(
        &server,
        Method::GET,
        "/u1/alpha/api-browser/description",
        &reader,
    )
    .await?;
    assert_eq!(st, 200, "browser lane: {text}");
    assert_eq!(
        serde_json::from_str::<Value>(&text)?,
        serde_json::json!({"description": "Alpha service"})
    );
    let etag = hdr(&h, "etag");
    assert!(etag.starts_with('"'), "digest ETag: {etag:?}");
    assert!(hdr(&h, "cache-control").contains("stale-while-revalidate"));
    let revalidate = [reader[0], ("If-None-Match", etag.as_str())];
    assert_eq!(
        req(
            &server,
            Method::GET,
            "/u1/alpha/api/description",
            &revalidate
        )
        .await?
        .0,
        304
    );

    let too_long = format!(r#"{{"description":"{}"}}"#, "x".repeat(513));
    for bad in [
        too_long,
        r#"{"description":"two\nlines"}"#.to_string(),
        r#"{"description":""}"#.to_string(),
        r#"{"descripton":"typo"}"#.to_string(),
        r#"{"description":5}"#.to_string(),
    ] {
        let (st, text) = send(
            &server,
            Method::PUT,
            "/u1/alpha/api/description",
            &admin,
            bad.clone(),
        )
        .await?;
        assert_eq!(st, 400, "{bad}: {text}");
    }
    let (st, _) = send(
        &server,
        Method::PUT,
        "/u1/alpha/api/description",
        &admin,
        " ".repeat(9000),
    )
    .await?;
    assert_eq!(st, 413, "body above the document bound");
    let (st, _) = send(
        &server,
        Method::PUT,
        "/u1/nope/api/description",
        &admin,
        doc.clone(),
    )
    .await?;
    assert_eq!(
        st, 404,
        "no description for a repository that does not exist"
    );
    assert_eq!(
        req(&server, Method::GET, "/u1/nope/api/description", &reader)
            .await?
            .0,
        404
    );

    // ---- create with a description: admin only, validated before anything exists ---------
    assert_eq!(
        req(
            &server,
            Method::PUT,
            "/u2/delta/api?description=Delta%20svc",
            &writer
        )
        .await?
        .0,
        403,
        "write permission creates, but does not label"
    );
    assert_eq!(
        req(&server, Method::GET, "/u2/delta/api", &reader).await?.0,
        404,
        "refused create made nothing"
    );
    assert_eq!(
        req(
            &server,
            Method::PUT,
            "/u2/delta/api?object_format=sha1&description=Delta%20svc",
            &admin
        )
        .await?
        .0,
        201
    );
    assert_eq!(
        get_json(&server, "/u2/delta/api/description", &reader).await?,
        serde_json::json!({"description": "Delta svc"})
    );
    assert_eq!(
        req(
            &server,
            Method::PUT,
            "/u2/eps/api?description=a%0Ab",
            &admin
        )
        .await?
        .0,
        400
    );
    assert_eq!(
        req(&server, Method::GET, "/u2/eps/api", &reader).await?.0,
        404,
        "invalid label, no repo"
    );

    // ---- owner profile -------------------------------------------------------------------
    assert_eq!(
        get_json(&server, "/api/v1/owners/u1", &reader).await?,
        serde_json::json!({"name": "u1"})
    );
    let profile = r#"{"display_name":"Team One","description":"Owns alpha and beta"}"#.to_string();
    for who in [&reader, &writer] {
        let (st, _) = send(
            &server,
            Method::PUT,
            "/api/v1/owners/u1",
            who,
            profile.clone(),
        )
        .await?;
        assert_eq!(st, 403);
        assert_eq!(
            req(&server, Method::DELETE, "/api/v1/owners/u1", who)
                .await?
                .0,
            403
        );
    }
    let (st, text) = send(
        &server,
        Method::PUT,
        "/api/v1/owners/u1",
        &admin,
        profile.clone(),
    )
    .await?;
    assert_eq!(st, 204, "{text}");
    let want = serde_json::json!({"name": "u1", "display_name": "Team One", "description": "Owns alpha and beta"});
    assert_eq!(get_json(&server, "/api/v1/owners/u1", &reader).await?, want);
    assert_eq!(
        get_json(&server, "/api-browser/v1/owners/u1", &reader).await?,
        want,
        "browser lane"
    );
    for bad in [
        r"{}",
        r#"{"displayName":"x"}"#,
        r#"{"name":"u2","display_name":"x"}"#,
    ] {
        let (st, text) = send(
            &server,
            Method::PUT,
            "/api/v1/owners/u1",
            &admin,
            bad.to_string(),
        )
        .await?;
        assert_eq!(st, 400, "{bad}: {text}");
    }
    assert_eq!(
        req(&server, Method::GET, "/api/v1/owners/.hidden", &reader)
            .await?
            .0,
        400
    );
    // A profile does not create an owner: written, but not listed until a repository exists.
    let (st, _) = send(
        &server,
        Method::PUT,
        "/api/v1/owners/ghost",
        &admin,
        r#"{"display_name":"Ghost"}"#.into(),
    )
    .await?;
    assert_eq!(st, 204);

    // ---- listings: plain shapes unchanged, detail beside them ------------------------------
    assert_eq!(
        get_json(&server, "/api/v1/owners", &reader).await?,
        serde_json::json!(["u1", "u2"])
    );
    let (st, text, h) = req(&server, Method::GET, "/api/v1/owners?detail=1", &reader).await?;
    assert_eq!(st, 200, "{text}");
    assert_eq!(
        serde_json::from_str::<Value>(&text)?,
        serde_json::json!([
            {"name": "u1", "display_name": "Team One", "description": "Owns alpha and beta"},
            {"name": "u2"},
        ])
    );
    let etag = hdr(&h, "etag");
    let revalidate = [reader[0], ("If-None-Match", etag.as_str())];
    assert_eq!(
        req(&server, Method::GET, "/api/v1/owners?detail=1", &revalidate)
            .await?
            .0,
        304
    );
    assert_eq!(
        get_json(&server, "/api-browser/v1/owners?detail=1", &reader)
            .await?
            .as_array()
            .map(Vec::len),
        Some(2),
        "browser lane"
    );
    assert_eq!(
        get_json(&server, "/api/v1/owners/u1/repos", &reader).await?,
        serde_json::json!(["alpha", "beta"])
    );
    assert_eq!(
        get_json(&server, "/api/v1/owners/u1/repos?detail=1", &reader).await?,
        serde_json::json!([{"name": "alpha", "description": "Alpha service"}, {"name": "beta"}])
    );
    assert_eq!(
        get_json(&server, "/api/v1/owners/nobody/repos?detail=1", &reader).await?,
        serde_json::json!([])
    );
    assert_eq!(
        req(&server, Method::GET, "/api/v1/owners?detail=yes", &reader)
            .await?
            .0,
        400
    );

    // ---- clear, and a deleted repository takes its description with it ---------------------
    assert_eq!(
        req(&server, Method::DELETE, "/api/v1/owners/u1", &admin)
            .await?
            .0,
        204
    );
    assert_eq!(
        get_json(&server, "/api/v1/owners/u1", &reader).await?,
        serde_json::json!({"name": "u1"})
    );
    assert_eq!(
        req(&server, Method::DELETE, "/api/v1/owners/u1", &admin)
            .await?
            .0,
        204,
        "idempotent"
    );
    assert_eq!(
        req(&server, Method::DELETE, "/u2/delta/api/description", &admin)
            .await?
            .0,
        204
    );
    assert_eq!(
        get_json(&server, "/u2/delta/api/description", &reader).await?,
        serde_json::json!({})
    );
    assert_eq!(
        req(&server, Method::DELETE, "/u1/alpha/api", &admin)
            .await?
            .0,
        204
    );
    assert_eq!(
        req(&server, Method::PUT, "/u1/alpha/api", &writer).await?.0,
        201
    );
    assert_eq!(
        get_json(&server, "/u1/alpha/api/description", &reader).await?,
        serde_json::json!({}),
        "repository deletion removes every object under its prefix, the description too"
    );
    Ok(())
}
