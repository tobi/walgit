//! Per-repo push policy: HTTP get/put/delete and receive-pack enforcement.
mod harness;

type TestResult = anyhow::Result<()>;
use anyhow::Context;
use harness::{Server, TestRepo, git_in};
use std::process::Command;

const PROTECT_MAIN: &str = r#"{
  "version": 1,
  "rules": [
    {
      "name": "lock-main",
      "match": { "refs": ["refs/heads/main"] },
      "effect": {
        "protect": { "restricts": ["delete", "force-push"] }
      }
    }
  ]
}"#;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn policy_http_roundtrip() -> TestResult {
    let server = Server::start().await?;
    server.put_repo("t", "r").await?;

    let client = reqwest::Client::new();
    let url = format!("{}/t/r/policy", server.base_url);

    let empty = client.get(&url).send().await?;
    assert_eq!(empty.status(), 200);
    let body: serde_json::Value = empty.json().await?;
    assert_eq!(body["rules"].as_array().unwrap().len(), 0);

    let put = client
        .put(&url)
        .header("content-type", "application/json")
        .body(PROTECT_MAIN)
        .send()
        .await?;
    assert_eq!(
        put.status(),
        204,
        "{}",
        put.text().await.unwrap_or_default()
    );

    let got = client.get(&url).send().await?;
    let body: serde_json::Value = got.json().await?;
    assert_eq!(body["rules"][0]["name"], "lock-main");
    assert_eq!(body["rules"][0]["match"]["refs"][0], "refs/heads/main");

    let del = client.delete(&url).send().await?;
    assert_eq!(del.status(), 204);
    let empty = client.get(&url).send().await?;
    let body: serde_json::Value = empty.json().await?;
    assert_eq!(body["rules"].as_array().unwrap().len(), 0);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn policy_missing_repo_is_404() -> TestResult {
    let server = Server::start().await?;
    let status = reqwest::Client::new()
        .get(format!("{}/no/such/policy", server.base_url))
        .send()
        .await?
        .status();
    assert_eq!(status, 404);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn protected_main_rejects_force_and_delete() -> TestResult {
    let server = Server::start().await?;
    server.put_repo("t", "r").await?;
    let put = reqwest::Client::new()
        .put(format!("{}/t/r/policy", server.base_url))
        .header("content-type", "application/json")
        .body(PROTECT_MAIN)
        .send()
        .await?;
    assert_eq!(put.status(), 204);

    let src = TestRepo::synthetic(1, 1)?;
    git_in(&src, &["commit", "--allow-empty", "-m", "a"])?;
    git_in(&src, &["branch", "-M", "main"])?;
    git_in(
        &src,
        &["remote", "add", "origin", &server.repo_url("t", "r")],
    )?;
    git_in(&src, &["push", "origin", "main"])?;

    // Unrelated history + --force: policy, not CAS, must reject.
    git_in(&src, &["checkout", "--orphan", "other"])?;
    git_in(&src, &["commit", "--allow-empty", "-m", "other"])?;
    let force = Command::new("git")
        .current_dir(&*src)
        .args(["push", "--force", "origin", "other:main"])
        .output()?;
    let stderr = String::from_utf8_lossy(&force.stderr);
    assert!(
        !force.status.success(),
        "force-push of protected main succeeded: {stderr}"
    );
    assert!(
        stderr.contains("lock-main") || stderr.contains("rejected by rule"),
        "stderr should name the rule: {stderr}"
    );

    let del = Command::new("git")
        .current_dir(&*src)
        .args(["push", "origin", ":refs/heads/main"])
        .output()?;
    let stderr = String::from_utf8_lossy(&del.stderr);
    assert!(!del.status.success(), "delete of protected main succeeded");
    assert!(
        stderr.contains("lock-main") || stderr.contains("rejected by rule"),
        "stderr should name the rule: {stderr}"
    );

    // Unprotected branch may be force-pushed (orphan onto a new name).
    git_in(&src, &["checkout", "--orphan", "topic"])?;
    git_in(&src, &["commit", "--allow-empty", "-m", "topic"])?;
    git_in(&src, &["push", "origin", "topic"])?;
    git_in(&src, &["checkout", "--orphan", "topic2"])?;
    git_in(&src, &["commit", "--allow-empty", "-m", "topic-other"])?;
    git_in(&src, &["push", "--force", "origin", "topic2:topic"])
        .context("force-push of unprotected topic")?;

    // After clearing policy, force-push of main is allowed.
    let cleared = reqwest::Client::new()
        .delete(format!("{}/t/r/policy", server.base_url))
        .send()
        .await?;
    assert_eq!(cleared.status(), 204);
    git_in(&src, &["push", "--force", "origin", "other:main"])?;
    Ok(())
}

// ---- [policy] baseline ---------------------------------------------------------------

/// The `docs/POLICY.md` baseline example: members in draft/** and explore/**,
/// production for `svc:deploy`, the rest of heads/tags for the two robots.
const BASELINE: &str = r#"{
  "version": 1,
  "rules": [
    {
      "name": "reserve-heads-and-tags",
      "match": { "refs": ["refs/heads/**", "refs/tags/**", "^refs/heads/draft/**", "^refs/heads/explore/**"] },
      "effect": { "protect": { "bypass": ["svc:deploy", "svc:platform"] } }
    },
    {
      "name": "production-deploy-only",
      "match": { "refs": ["refs/heads/production", "refs/heads/production/**"] },
      "effect": { "protect": { "bypass": ["svc:deploy"] } }
    }
  ]
}"#;

/// A repository's own policy.json: nothing, then one restriction inside a sandbox.
const EMPTY: &str = r#"{"version": 1, "rules": []}"#;
const OWN: &str = r#"{"version": 1, "rules": [{"name": "freeze-drafts", "match": {"refs": ["refs/heads/draft/**"]}, "effect": {"protect": {"restricts": ["delete"]}}}]}"#;

/// Token mode: one member, two robots, one admin (who is also the only one to
/// write the repository's own policy.json).
fn principals(c: &mut walgit_config::Config) {
    c.server.auth.mode = walgit_config::AuthMode::Token;
    c.server.auth.anonymous_read = false;
    c.server.auth.tokens = [
        ("bob@example.com", "member", false),
        ("svc:deploy", "deploy", false),
        ("svc:platform", "platform", false),
        ("root", "admin", true),
    ]
    .into_iter()
    .map(|(principal, token, admin)| walgit_config::StaticToken {
        principal: principal.into(),
        token: token.into(),
        token_env: None,
        write: true,
        admin,
    })
    .collect();
}

/// `git push` as the principal behind `token`: `Ok(())` or the stderr of the refusal.
fn push_as(
    src: &std::path::Path,
    token: &str,
    args: &[&str],
) -> anyhow::Result<Result<(), String>> {
    let header = format!("http.extraHeader=Authorization: Bearer {token}");
    let out = Command::new("git")
        .current_dir(src)
        .args(["-c", &header, "push"])
        .args(args)
        .output()?;
    Ok(if out.status.success() {
        Ok(())
    } else {
        Err(String::from_utf8_lossy(&out.stderr).into_owned())
    })
}

async fn api(
    server: &Server,
    method: reqwest::Method,
    path: &str,
    body: Option<&str>,
) -> anyhow::Result<(u16, serde_json::Value)> {
    let mut r = reqwest::Client::new()
        .request(method, format!("{}{path}", server.base_url))
        .header("Authorization", "Bearer admin");
    if let Some(b) = body {
        r = r
            .header("content-type", "application/json")
            .body(b.to_string());
    }
    let resp = r.send().await?;
    let status = resp.status().as_u16();
    let text = resp.text().await?;
    Ok((
        status,
        serde_json::from_str(&text).unwrap_or(serde_json::Value::String(text)),
    ))
}

/// A repository without its own policy.json is judged by the host baseline on
/// receive-pack; one with its own is judged by both (it can add, never lift); the
/// dry-run judges exactly like receive-pack; `…/policy` stays the repository's
/// document and `…/policy/effective` shows both layers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn baseline_governs_receive_pack_settings_and_effective_view() -> TestResult {
    let dir = tempfile::tempdir()?;
    let baseline = dir.path().join("policy-baseline.json");
    std::fs::write(&baseline, BASELINE)?;

    // A host without a baseline records a member's push to main: the dry-run on
    // the guarded host below must deny it the way its receive-pack would.
    let plain = Server::start_with_tweak(principals).await?;
    let (st, body) = api(&plain, reqwest::Method::PUT, "/t/r/api", None).await?;
    assert!(st == 201 || st == 200, "{st} {body}");
    let src = TestRepo::synthetic(2, 1)?;
    git_in(
        &src,
        &["remote", "add", "origin", &plain.repo_url("t", "r")],
    )?;
    push_as(&src, "member", &["origin", "main"])?.map_err(anyhow::Error::msg)?;

    let guarded = plain
        .start_sibling_with(|c| {
            principals(c);
            c.policy.baseline = Some(baseline.clone());
        })
        .await?;
    git_in(
        &src,
        &["remote", "set-url", "origin", &guarded.repo_url("t", "r")],
    )?;
    git_in(&src, &["commit", "--allow-empty", "-m", "next"])?;

    // No policy.json: the baseline alone.
    let reserved = "rejected by baseline rule 'reserve-heads-and-tags'";
    let e = push_as(&src, "member", &["origin", "main"])?.unwrap_err();
    assert!(e.contains(reserved), "member moved main: {e}");
    push_as(&src, "member", &["origin", "main:refs/heads/draft/x"])?.map_err(anyhow::Error::msg)?;
    push_as(&src, "member", &["origin", ":refs/heads/draft/x"])?.map_err(anyhow::Error::msg)?;
    let e = push_as(&src, "member", &["origin", "main:refs/heads/production"])?.unwrap_err();
    assert!(e.contains(reserved), "{e}");
    let e = push_as(&src, "platform", &["origin", "main:refs/heads/production"])?.unwrap_err();
    assert!(
        e.contains("rejected by baseline rule 'production-deploy-only'"),
        "{e}"
    );
    push_as(&src, "deploy", &["origin", "main:refs/heads/production"])?
        .map_err(anyhow::Error::msg)?;
    push_as(&src, "platform", &["origin", "main"])?.map_err(anyhow::Error::msg)?;

    // An own policy.json adds to the baseline: an empty-rules document drops
    // nothing, a rule of its own restricts inside the sandbox.
    let (st, body) = api(
        &guarded,
        reqwest::Method::PUT,
        "/t/r/api/policy",
        Some(EMPTY),
    )
    .await?;
    assert_eq!(st, 204, "{body}");
    git_in(&src, &["commit", "--allow-empty", "-m", "again"])?;
    let e = push_as(&src, "member", &["origin", "main"])?.unwrap_err();
    assert!(
        e.contains(reserved),
        "an empty policy.json dropped the baseline: {e}"
    );
    let (st, body) = api(&guarded, reqwest::Method::PUT, "/t/r/api/policy", Some(OWN)).await?;
    assert_eq!(st, 204, "{body}");
    let e = push_as(&src, "member", &["origin", "main"])?.unwrap_err();
    assert!(e.contains(reserved), "the baseline still holds main: {e}");
    push_as(&src, "member", &["origin", "main:refs/heads/draft/y"])?.map_err(anyhow::Error::msg)?;
    let e = push_as(&src, "member", &["origin", ":refs/heads/draft/y"])?.unwrap_err();
    assert!(e.contains("rejected by rule 'freeze-drafts'"), "{e}");

    // `…/policy` is the repository's own document, unchanged in shape.
    let (st, own) = api(&guarded, reqwest::Method::GET, "/t/r/api/policy", None).await?;
    assert_eq!(st, 200);
    assert_eq!(own["rules"].as_array().unwrap().len(), 1);
    assert_eq!(own["rules"][0]["name"], "freeze-drafts");
    assert!(
        own.get("layers").is_none() && own.get("baseline").is_none(),
        "{own}"
    );
    // `…/policy/effective`: both layers, in evaluation order.
    let (st, eff) = api(
        &guarded,
        reqwest::Method::GET,
        "/t/r/api/policy/effective",
        None,
    )
    .await?;
    assert_eq!(st, 200, "{eff}");
    let layers = eff["layers"].as_array().unwrap();
    assert_eq!(layers.len(), 2, "{eff}");
    assert_eq!(layers[0]["source"], "baseline");
    assert_eq!(
        layers[0]["policy"]["rules"][0]["name"],
        "reserve-heads-and-tags"
    );
    assert_eq!(layers[1]["source"], "repository");
    assert_eq!(layers[1]["policy"], own);
    let (_, eff) = api(
        &plain,
        reqwest::Method::GET,
        "/t/r/api/policy/effective",
        None,
    )
    .await?;
    let layers = eff["layers"].as_array().unwrap();
    assert_eq!(layers.len(), 1, "no baseline on this host: {eff}");
    assert_eq!(layers[0]["source"], "repository");

    // Settings dry-run: the saved policy after the baseline, as receive-pack judges.
    // The member's first push to main (recorded by the plain host) is denied here …
    let (st, dr) = api(
        &guarded,
        reqwest::Method::POST,
        "/t/r/api/policy/dry-run?last=50",
        Some(""),
    )
    .await?;
    assert_eq!(st, 200, "{dr}");
    let first = dr["results"].as_array().unwrap().last().unwrap().clone();
    assert_eq!(first["principal"], "bob@example.com", "{dr}");
    assert_eq!(first["refs"][0]["name"], "refs/heads/main");
    assert_eq!(first["refs"][0]["ok"], false, "{first}");
    assert_eq!(first["refs"][0]["reason"], reserved);
    // … and allowed where there is none.
    let (_, dr) = api(
        &plain,
        reqwest::Method::POST,
        "/t/r/api/policy/dry-run?last=50",
        Some(""),
    )
    .await?;
    let first = dr["results"].as_array().unwrap().last().unwrap().clone();
    assert_eq!(first["refs"][0]["ok"], true, "{first}");
    Ok(())
}

/// Fail closed: a baseline that cannot be read or does not pass the policy.json
/// validator stops startup instead of serving allow-all.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalid_baseline_stops_startup() -> TestResult {
    let dir = tempfile::tempdir()?;
    let missing = dir.path().join("missing.json");
    let garbage = dir.path().join("garbage.json");
    std::fs::write(&garbage, "{ not json")?;
    let lockout = dir.path().join("lockout.json");
    std::fs::write(
        &lockout,
        r#"{"version": 1, "rules": [
          {"name": "a", "match": {"refs": ["refs/heads/main"]}, "effect": {"protect": {"bypass": ["alice@example.com"]}}},
          {"name": "b", "match": {"refs": ["refs/heads/main"]}, "effect": {"protect": {"bypass": ["bob@example.com"]}}}
        ]}"#,
    )?;
    for (path, want) in [
        (&missing, "reading policy.baseline"),
        (&garbage, "policy.baseline"),
        (&lockout, "disjoint bypass lists"),
    ] {
        let p = path.clone();
        let Err(e) = Server::start_with_tweak(move |c| c.policy.baseline = Some(p)).await else {
            panic!("started with baseline {}", path.display());
        };
        let e = format!("{e:#}");
        assert!(e.contains(want), "{}: {e}", path.display());
    }
    Ok(())
}
