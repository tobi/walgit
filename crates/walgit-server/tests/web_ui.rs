mod harness;

use anyhow::Result;
use harness::{Server, TestRepo, git_in};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn page_routes_serve_index_without_cache() -> Result<()> {
    let server = Server::start().await?;
    let client = reqwest::Client::new();
    let expected = client
        .get(format!("{}/", server.base_url))
        .send()
        .await?
        .text()
        .await?;
    for path in [
        "/",
        "/owner",
        "/owner/repo",
        "/owner/repo/tree/main/src/lib.rs",
        "/owner/repo/blob/main/src/lib.rs",
        "/owner/repo/commits",
        "/owner/repo/commits/main",
        "/owner/repo/commit/abc",
        "/owner/repo/wal",
        "/owner/repo/settings",
        "/api",
    ] {
        let response = client
            .get(format!("{}{}", server.base_url, path))
            .send()
            .await?;
        assert_eq!(response.status(), 200, "{path}");
        assert_eq!(response.headers()["cache-control"], "no-cache");
        assert!(
            response.headers()["content-type"]
                .to_str()?
                .starts_with("text/html")
        );
        assert_eq!(response.text().await?, expected, "{path}");
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn assets_have_content_type_and_immutable_cache() -> Result<()> {
    let server = Server::start().await?;
    let client = reqwest::Client::new();
    let index = client
        .get(format!("{}/", server.base_url))
        .send()
        .await?
        .text()
        .await?;
    let marker = "/_ui/assets/";
    let asset = index
        .split('"')
        .find(|part| {
            part.starts_with(marker)
                && std::path::Path::new(part)
                    .extension()
                    .is_some_and(|ext| ext.eq_ignore_ascii_case("js"))
        })
        .expect("built index references a JavaScript asset");
    let response = client
        .get(format!("{}{}", server.base_url, asset))
        .send()
        .await?;
    assert_eq!(response.status(), 200);
    assert_eq!(
        response.headers()["cache-control"],
        "public, max-age=31536000, immutable"
    );
    assert!(
        response.headers()["content-type"]
            .to_str()?
            .starts_with("text/javascript")
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overview_reports_push_and_unknown_repo_is_text_404() -> Result<()> {
    let server = Server::start().await?;
    server.put_repo("o", "r").await?;
    let src = TestRepo::synthetic(1, 1)?;
    git_in(&src, &["commit", "--allow-empty", "-m", "web overview"])?;
    git_in(
        &src,
        &["push", &server.repo_url("o", "r"), "HEAD:refs/heads/main"],
    )?;

    let client = reqwest::Client::new();
    let response = client
        .get(format!("{}/o/r/api/overview", server.base_url))
        .send()
        .await?;
    assert_eq!(response.status(), 200);
    let body: serde_json::Value = response.json().await?;
    assert_eq!(body["repo"], "o/r");
    assert!(body["manifest"]["next_seq"].as_u64().unwrap() >= 2);
    assert!(body["packs"]["pushes"].as_u64().unwrap() >= 1);
    assert!(body["bundles"].is_array());
    assert!(body["compactions"].is_array());

    let missing = client
        .get(format!("{}/no/such/api/overview", server.base_url))
        .send()
        .await?;
    assert_eq!(missing.status(), 404);
    assert!(
        missing.headers()["content-type"]
            .to_str()?
            .starts_with("text/plain")
    );
    Ok(())
}

/// Every response carries `Server: walgit/<ver> (<kind>; <who>)` — errors too.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_header_names_the_instance() -> anyhow::Result<()> {
    let server = harness::Server::start().await?;
    let c = reqwest::Client::new();
    for path in ["/readyz", "/services/api/owners", "/nope/nope/api/refs"] {
        let r = c.get(format!("{}{path}", server.base_url)).send().await?;
        let v = r
            .headers()
            .get("server")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        assert!(v.starts_with("walgit/"), "{path}: Server header = {v:?}");
        assert!(
            v.contains("(dev;") || v.contains("(serverless;") || v.contains("(ssd;"),
            "{path}: {v}"
        );
        // Same value under a name Google's frontend does not rewrite (the edge re-emits it as Server).
        let x = r
            .headers()
            .get("x-walgit-server")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        assert_eq!(x, v, "{path}: X-Walgit-Server must equal Server");
    }
    Ok(())
}
