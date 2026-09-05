//! Upstream follow (`[upstream] follow`, `walgit_server::follow`): the maintaining
//! host brings followed refs up to an upstream git host's through the WAL — the
//! same PUSH entry a push produces — fast-forward only. The upstream here is a
//! second walgit instance (smart HTTP v2 over 127.0.0.1, real `git fetch`).

mod harness;

use harness::{Server, git, git_in};

macro_rules! step {
    ($name:literal, $e:expr) => {
        tokio::time::timeout(std::time::Duration::from_mins(1), $e)
            .await
            .unwrap_or_else(|_| panic!("step timed out: {}", $name))
    };
}

fn commit(work: &std::path::Path, name: &str) -> anyhow::Result<String> {
    std::fs::write(work.join(name), name)?;
    git_in(work, &["add", "."])?;
    git_in(work, &["commit", "-q", "-m", name])?;
    Ok(git_in(work, &["rev-parse", "HEAD"])?.trim().to_string())
}

/// `git ls-remote <url>` → (ref → oid), peeled lines included as `<ref>^{}`.
fn ls_remote(url: &str) -> anyhow::Result<std::collections::HashMap<String, String>> {
    let out = std::process::Command::new("git")
        .args(["ls-remote", url])
        .output()?;
    anyhow::ensure!(
        out.status.success(),
        "ls-remote: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    Ok(String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter_map(|l| {
            l.split_once('\t')
                .map(|(o, n)| (n.to_string(), o.to_string()))
        })
        .collect())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn follows_upstream_refs_through_the_wal_fast_forward_only() -> anyhow::Result<()> {
    // Upstream: its own instance and store, repo u/src with 3 commits and an annotated tag.
    let up = step!("start upstream", Server::start())?;
    step!("put upstream repo", up.put_repo("u", "src"))?;
    let up_url = up.repo_url("u", "src");
    let work = tempfile::tempdir()?;
    git_in(work.path(), &["init", "-q", "-b", "main"])?;
    git_in(work.path(), &["config", "user.email", "t@t"])?;
    git_in(work.path(), &["config", "user.name", "Tester"])?;
    let c1 = commit(work.path(), "a")?;
    let c2 = commit(work.path(), "b")?;
    let c3 = commit(work.path(), "c")?;
    git_in(work.path(), &["tag", "-a", "v1", "-m", "v1", &c2])?;
    git(&["push", "-q", &up_url, "main", "v1"], work.path())?;

    // Follower: maintainer of everything, o/r follows u/src's main and v1.
    let fo = step!(
        "start follower",
        Server::start_with_tweak(|c| {
            c.server.roles = vec![walgit_config::Role::Serve, walgit_config::Role::Maintain];
            c.upstream.git = Some(up_url.clone());
            c.upstream.follow = vec!["refs/heads/main".into(), "refs/tags/v1".into()];
            c.compaction.enabled = false;
            c.bundles.enabled = false;
            c.wal.snapshot_every_entries = 0;
        })
    )?;
    step!("put follower repo", fo.put_repo("o", "r"))?;
    let fo_url = fo.repo_url("o", "r");
    let id = walgit_git::RepoId::new("o", "r")?;

    // Round 1: the whole history, both refs, one PUSH entry by `upstream`.
    let r = step!("round 1", walgit_server::follow::run_pass(&fo.state))?;
    assert_eq!(
        (r.repos, r.behind, r.published, r.failed),
        (1, 1, 1, 0),
        "{r:?}"
    );
    let refs = ls_remote(&fo_url)?;
    assert_eq!(refs.get("refs/heads/main"), Some(&c3));
    assert_eq!(
        refs.get("refs/tags/v1^{}"),
        Some(&c2),
        "peeled tag advertised: {refs:?}"
    );
    let h = step!("open", fo.state.registry.open(&id))?;
    assert_eq!(h.manifest().head_seq, 1);
    let entries = step!("log", h.read_log(1, None))?;
    assert_eq!(
        entries[0].meta.get("principal").map(String::as_str),
        Some("upstream")
    );
    assert_eq!(
        entries[0].meta.get("upstream").map(String::as_str),
        Some(up_url.as_str())
    );
    assert!(
        entries[0].pack.is_some(),
        "the delta travels as a pack in the entry"
    );
    let tasks = step!("tasks", fo.get_text("/o/r/api/tasks", &[]))?;
    assert!(tasks.contains("\"follow\""), "{tasks}");

    // Round 2: nothing moved — no entry, no task.
    let r = step!("round 2", walgit_server::follow::run_pass(&fo.state))?;
    assert_eq!((r.behind, r.published, r.failed), (0, 0, 0), "{r:?}");
    assert_eq!(h.manifest().head_seq, 1);
    assert_eq!(
        tasks.matches("\"follow\"").count(),
        step!("tasks again", fo.get_text("/o/r/api/tasks", &[]))?
            .matches("\"follow\"")
            .count()
    );

    // Upstream moves forward: the delta (one commit) is published.
    let c4 = commit(work.path(), "d")?;
    git(&["push", "-q", &up_url, "main"], work.path())?;
    let r = step!("round 3", walgit_server::follow::run_pass(&fo.state))?;
    assert_eq!((r.behind, r.published, r.failed), (1, 1, 0), "{r:?}");
    assert_eq!(ls_remote(&fo_url)?.get("refs/heads/main"), Some(&c4));
    assert_eq!(h.manifest().head_seq, 2);

    // Upstream rewinds main to c2: refused (not a fast-forward), nothing published, visible as a failed round.
    git(
        &[
            "push",
            "-q",
            "--force",
            &up_url,
            &format!("{c2}:refs/heads/main"),
        ],
        work.path(),
    )?;
    let r = step!("round 4", walgit_server::follow::run_pass(&fo.state))?;
    assert_eq!((r.behind, r.published, r.failed), (1, 0, 1), "{r:?}");
    assert_eq!(ls_remote(&fo_url)?.get("refs/heads/main"), Some(&c4));
    assert_eq!(h.manifest().head_seq, 2);

    // Upstream goes forward again past our tip: followed.
    git_in(work.path(), &["reset", "-q", "--hard", &c4])?;
    let c5 = commit(work.path(), "e")?;
    git(&["push", "-q", "--force", &up_url, "main"], work.path())?;
    let r = step!("round 5", walgit_server::follow::run_pass(&fo.state))?;
    assert_eq!((r.behind, r.published, r.failed), (1, 1, 0), "{r:?}");
    assert_eq!(ls_remote(&fo_url)?.get("refs/heads/main"), Some(&c5));

    // The Settings tab sees the configuration and this instance's last round.
    let d: serde_json::Value = serde_json::from_str(&step!(
        "describe",
        fo.get_text("/o/r/api/settings/describe", &[])
    )?)?;
    assert_eq!(d["upstream"]["git"], serde_json::json!(up_url));
    assert_eq!(
        d["upstream"]["follow"],
        serde_json::json!(["refs/heads/main", "refs/tags/v1"])
    );
    assert_eq!(
        d["upstream"]["last_round"]["outcome"], "published",
        "{}",
        d["upstream"]
    );
    assert_eq!(
        d["upstream"]["last_round"]["upstream"]["refs/heads/main"],
        serde_json::json!(c5)
    );

    // The manual op (UI/CLI) fetches itself; in sync now.
    let task = step!("op", crate::start_op(&fo, &id)).map_err(|e| anyhow::anyhow!(e))?;
    assert!(task.wait_done(std::time::Duration::from_secs(30)).await);
    let outcome = task.outcome().expect("finished");
    assert!(
        outcome
            .as_ref()
            .is_ok_and(|o| o.task.summary.contains("in sync")),
        "{outcome:?}"
    );

    // What the follower serves is complete: a fresh clone has the whole history.
    let clone = tempfile::tempdir()?;
    git(&["clone", "-q", &fo_url, "c"], clone.path())?;
    assert_eq!(
        git_in(&clone.path().join("c"), &["rev-parse", "HEAD"])?.trim(),
        c5
    );
    assert_eq!(
        git_in(&clone.path().join("c"), &["rev-list", "--count", "HEAD"])?.trim(),
        "5"
    );
    let _ = c1;
    Ok(())
}

async fn start_op(
    fo: &Server,
    id: &walgit_git::RepoId,
) -> Result<std::sync::Arc<walgit_wal::tasks::TaskState>, String> {
    match walgit_server::ops::start(
        fo.state.clone(),
        id.clone(),
        "follow",
        std::collections::HashMap::new(),
    )
    .await
    {
        Ok(t) | Err(walgit_server::ops::StartError::AlreadyRunning(t)) => Ok(t),
        Err(walgit_server::ops::StartError::UnknownOp) => Err("unknown op".into()),
    }
}
