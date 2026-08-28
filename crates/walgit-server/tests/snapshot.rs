//! WAL time travel: `POST …/ops/snapshot?at_seq=<n>` rebuilds the repository as
//! it was at a sequence, `GET …/api/snapshot/{n}` reads that rewind back, and
//! neither moves a live ref or the serving copy.

mod harness;

use harness::{Server, git, git_in};

/// Every await is bounded so a hang names the step instead of stalling CI.
macro_rules! step {
    ($name:literal, $e:expr) => {
        tokio::time::timeout(std::time::Duration::from_secs(60), $e)
            .await
            .unwrap_or_else(|_| panic!("step timed out: {}", $name))
    };
}

async fn post(server: &Server, path: &str) -> anyhow::Result<(reqwest::StatusCode, String)> {
    let resp = reqwest::Client::new()
        .post(format!("{}{path}", server.base_url))
        .header("Accept", "text/event-stream")
        .send()
        .await?;
    let status = resp.status();
    Ok((status, resp.text().await?))
}

async fn get(server: &Server, path: &str) -> anyhow::Result<(reqwest::StatusCode, String)> {
    let resp = reqwest::Client::new()
        .get(format!("{}{path}", server.base_url))
        .header("Accept", "application/json")
        .send()
        .await?;
    let status = resp.status();
    Ok((status, resp.text().await?))
}

/// The terminal packet of an SSE envelope (web/API.md §2b): `result` or `error`.
fn terminal(body: &str) -> (String, serde_json::Value) {
    let mut out = None;
    for chunk in body.split("\n\n") {
        let mut event = String::new();
        let mut data = String::new();
        for line in chunk.lines() {
            if let Some(rest) = line.strip_prefix("event:") {
                event = rest.trim().to_string();
            } else if let Some(rest) = line.strip_prefix("data:") {
                data.push_str(rest.strip_prefix(' ').unwrap_or(rest));
            }
        }
        if event == "result" || event == "error" {
            out = Some((event, serde_json::from_str(&data).unwrap_or_default()));
        }
    }
    out.unwrap_or_else(|| panic!("no terminal packet in:\n{body}"))
}

/// The `snapshot` op's result value, or the error message it failed with.
async fn run_snapshot(
    server: &Server,
    query: &str,
) -> anyhow::Result<Result<serde_json::Value, String>> {
    let (status, body) = post(server, &format!("/o/r/api/ops/snapshot{query}")).await?;
    assert_eq!(status, 200, "{body}");
    let (event, data) = terminal(&body);
    Ok(match event.as_str() {
        "result" => Ok(data["value"].clone()),
        _ => Err(data["message"].as_str().unwrap_or_default().to_string()),
    })
}

fn git_out(dir: &std::path::Path, args: &[&str]) -> String {
    git_in(dir, args).unwrap().trim().to_string()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn snapshot_rewinds_to_a_seq_without_moving_the_serving_copy() -> anyhow::Result<()> {
    let server = step!("start", Server::start())?;
    step!("put repo", server.put_repo("o", "r"))?;
    let src = tempfile::tempdir()?;
    git_in(src.path(), &["init", "-q", "-b", "main"])?;
    git_in(src.path(), &["config", "user.email", "t@t"])?;
    git_in(src.path(), &["config", "user.name", "Tester"])?;
    let mut tips = Vec::new();
    for i in 0..2 {
        std::fs::write(src.path().join(format!("f{i}.txt")), format!("{i}\n"))?;
        git_in(src.path(), &["add", "."])?;
        git_in(src.path(), &["commit", "-q", "-m", &format!("c{i}")])?;
        git(
            &["push", "-q", &server.repo_url("o", "r"), "main"],
            src.path(),
        )?;
        tips.push(git_out(src.path(), &["rev-parse", "HEAD"]));
    }
    let id = walgit_git::RepoId::new("o", "r")?;
    let handle = step!("open", server.state.registry.open(&id))?;
    assert_eq!(handle.manifest().head_seq, 2, "one WAL entry per push");

    // The rewind to seq 1: the first commit, nothing else.
    let value = step!("snapshot at_seq=1", run_snapshot(&server, "?at_seq=1"))?
        .unwrap_or_else(|e| panic!("snapshot at_seq=1 failed: {e}"));
    assert_eq!(value["built"], true);
    let snap = &value["snapshot"];
    assert_eq!(snap["at_seq"], 1);
    assert_eq!(snap["head_seq"], 2, "the WAL head is where it was");
    assert_eq!(
        snap["from_seq"], 0,
        "no checkpoint yet: replay from the start"
    );
    assert_eq!(snap["entries"], 1);
    assert_eq!(snap["ref_count"], 1);
    assert_eq!(snap["refs"][0]["name"], "refs/heads/main");
    assert_eq!(snap["refs"][0]["sha"].as_str(), Some(tips[0].as_str()));
    assert_eq!(snap["packs"].as_array().map(Vec::len), Some(1));

    // The rewound copy is a real repository, at the first commit, and it does
    // not know the second one.
    let git_dir = std::path::PathBuf::from(snap["git_dir"].as_str().unwrap());
    assert_eq!(
        git_dir,
        server.state.cfg.cache.dir.join("snapshots/o/r/1/o/r.git")
    );
    assert_eq!(
        git_out(&git_dir, &["rev-parse", "refs/heads/main"]),
        tips[0]
    );
    assert!(
        git_in(&git_dir, &["cat-file", "-e", &tips[1]]).is_err(),
        "the rewound copy must not contain the second commit"
    );

    // The serving copy did not move: refs, WAL head and local copy all at seq 2.
    let (status, body) = step!("refs", get(&server, "/o/r/api/refs"))?;
    assert_eq!(status, 200);
    let refs: serde_json::Value = serde_json::from_str(&body)?;
    assert_eq!(refs["head"]["sha"].as_str(), Some(tips[1].as_str()));
    assert_eq!(handle.manifest().head_seq, 2);
    assert_eq!(handle.applied_seq(), 2);
    assert_eq!(
        git_out(handle.local().path(), &["rev-parse", "refs/heads/main"]),
        tips[1]
    );

    // GET reads the rewind back; a sequence nobody materialized here is a 404
    // that names the op.
    let (status, body) = step!("get snapshot/1", get(&server, "/o/r/api/snapshot/1"))?;
    assert_eq!(status, 200, "{body}");
    let read: serde_json::Value = serde_json::from_str(&body)?;
    assert_eq!(read["at_seq"], 1);
    assert_eq!(read["refs"][0]["sha"].as_str(), Some(tips[0].as_str()));
    assert_eq!(read["git_dir"], snap["git_dir"]);
    let (status, body) = step!("get snapshot/2", get(&server, "/o/r/api/snapshot/2"))?;
    assert_eq!(status, 404);
    assert!(body.contains("ops/snapshot?at_seq=2"), "{body}");
    let (status, _) = step!("get snapshot/x", get(&server, "/o/r/api/snapshot/nope"))?;
    assert_eq!(status, 400, "an unparsable seq is not a repository path");

    // Idempotent: the second run returns the same tree, untouched.
    let value = step!("snapshot again", run_snapshot(&server, "?at_seq=1"))?
        .unwrap_or_else(|e| panic!("second snapshot at_seq=1 failed: {e}"));
    assert_eq!(value["built"], false);
    assert_eq!(value["snapshot"]["built_at"], read["built_at"]);

    // Rejections: missing, unparsable, zero, past the head.
    for (query, want) in [
        ("", "missing `at_seq`"),
        ("?at_seq=", "missing `at_seq`"),
        ("?at_seq=abc", "must be a WAL sequence number"),
        ("?at_seq=0", "at_seq must be 1 or greater"),
        ("?at_seq=99", "past the WAL head (2)"),
    ] {
        let err = step!("reject", run_snapshot(&server, query))?
            .err()
            .unwrap_or_else(|| panic!("at_seq {query:?} should have been refused"));
        assert!(err.contains(want), "at_seq {query:?}: {err}");
    }
    // Nothing was left behind for the refused sequences.
    assert!(!server.state.cfg.cache.dir.join("snapshots/o/r/0").exists());
    assert!(!server.state.cfg.cache.dir.join("snapshots/o/r/99").exists());

    // The op is in the catalogue as a read: the UI must not label it a write.
    let spec = walgit_server::ops::spec("snapshot").expect("snapshot op");
    assert!(!spec.mutating);
    assert_eq!(spec.params, ["at_seq"]);
    Ok(())
}
