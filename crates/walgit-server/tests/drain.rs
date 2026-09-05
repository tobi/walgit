//! Graceful drain after SIGTERM in two phases (D31; own binary: the flags are
//! process-global). Phase 1: no new unit starts, the running one is
//! interrupted at once (D22 redoes it) — and the instance SERVES NORMALLY
//! (readyz 200, fetches and pushes land). Phase 2, once the unit is gone:
//! `/readyz` is 503 + Retry-After (the edge stops routing here), new pushes
//! and fetches are refused with 503 + Retry-After before any work, in-flight
//! requests get `server.drain_timeout`.

mod harness;

use harness::{Server, git, git_in};
use std::collections::HashMap;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn after_sigterm_new_object_work_is_refused_and_no_unit_starts() -> anyhow::Result<()> {
    let server = Server::start_with_tweak(|c| {
        c.server.roles = vec![walgit_config::Role::Serve, walgit_config::Role::Maintain];
        c.wal.snapshot_every_entries = 1; // a checkpoint would be due after one push
    })
    .await?;
    server.put_repo("o", "r").await?;
    let src = tempfile::tempdir()?;
    git_in(src.path(), &["init", "-q", "-b", "main"])?;
    git_in(src.path(), &["config", "user.email", "t@t"])?;
    git_in(src.path(), &["config", "user.name", "Tester"])?;
    std::fs::write(src.path().join("f.txt"), "hi\n")?;
    git_in(src.path(), &["add", "."])?;
    git_in(src.path(), &["commit", "-q", "-m", "c"])?;
    git(
        &["push", "-q", &server.repo_url("o", "r"), "main"],
        src.path(),
    )?;
    let client = reqwest::Client::new();
    assert_eq!(
        client
            .get(format!("{}/readyz", server.base_url))
            .send()
            .await?
            .status(),
        200
    );

    // A long-running unit (stands in for a base rebuild): a task of an op kind
    // whose future never completes on its own.
    let h0 = server
        .state
        .registry
        .open(&walgit_git::RepoId::new("o", "r")?)
        .await?;
    let unit = match h0.begin_task("compact", HashMap::default()) {
        walgit_wal::Begin::Started(t) => t,
        walgit_wal::Begin::AlreadyRunning(_) => anyhow::bail!("compact already running"),
    };
    let unit_state = unit.state.clone();
    let join = tokio::spawn(async move {
        let _unit = unit; // dropped when aborted → recorded as interrupted
        std::future::pending::<()>().await;
    });
    unit_state.set_abort_handle(join.abort_handle());
    assert_eq!(server.state.registry.tasks().running_all().len(), 1);

    // D31 phase 1 — SIGTERM arrived: the running unit is INTERRUPTED AT ONCE
    // (D22 redoes it), the maintenance loop starts nothing new, and SERVING IS
    // UNTOUCHED: readyz 200, a fetch is answered, a push lands.
    walgit_wal::tasks::begin_drain();
    let interrupted = server
        .state
        .registry
        .tasks()
        .interrupt_where(walgit_server::ops::is_op);
    assert_eq!(
        interrupted,
        vec![("o/r".to_string(), "compact".to_string())]
    );
    let _ = join.await; // aborted
    assert!(
        server.state.registry.tasks().running_all().is_empty(),
        "the unit is gone within the bound"
    );
    let rec = server
        .state
        .registry
        .tasks()
        .last("o/r", "compact")
        .expect("recorded");
    assert_eq!(rec.ok, Some(false));
    assert!(rec.summary.starts_with("interrupted"), "{rec:?}");
    assert_eq!(
        client
            .get(format!("{}/readyz", server.base_url))
            .send()
            .await?
            .status(),
        200,
        "phase 1 keeps readyz 200"
    );
    let clone = tempfile::tempdir()?;
    git(
        &[
            "clone",
            "-q",
            &server.repo_url("o", "r"),
            clone.path().join("c").to_str().unwrap(),
        ],
        clone.path(),
    )?;
    assert!(
        clone.path().join("c/f.txt").exists(),
        "a fetch is served during the maintenance drain"
    );
    std::fs::write(src.path().join("p1.txt"), "phase one\n")?;
    git_in(src.path(), &["add", "."])?;
    git_in(src.path(), &["commit", "-q", "-m", "p1"])?;
    git(
        &["push", "-q", &server.repo_url("o", "r"), "main"],
        src.path(),
    )?;
    let h = server
        .state
        .registry
        .open(&walgit_git::RepoId::new("o", "r")?)
        .await?;
    h.sync().await?;
    assert_eq!(
        h.manifest().head_seq,
        2,
        "a push lands during the maintenance drain"
    );
    let report = walgit_server::maintain::run_pass(&server.state).await?;
    assert_eq!(report.units, 0, "no new unit starts in phase 1: {report:?}");

    // Phase 2 — the unit is done (or interrupted): serving drains.
    walgit_wal::tasks::begin_shutdown();

    let r = client
        .get(format!("{}/readyz", server.base_url))
        .send()
        .await?;
    assert_eq!(r.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        r.headers().get("retry-after").map(|v| v.to_str().unwrap()),
        Some("15")
    );
    assert!(r.text().await?.contains("draining"));

    // A new push: refused before any work, git sees the 503.
    std::fs::write(src.path().join("g.txt"), "more\n")?;
    git_in(src.path(), &["add", "."])?;
    git_in(src.path(), &["commit", "-q", "-m", "d"])?;
    let out = std::process::Command::new("git")
        .args(["push", &server.repo_url("o", "r"), "main"])
        .current_dir(src.path())
        .output()?;
    assert!(!out.status.success());
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("503"),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    h.sync().await?;
    assert_eq!(h.manifest().head_seq, 2, "nothing published while draining");

    // A new fetch: 503 + Retry-After + ERR line.
    let body =
        b"0011command=fetch0001000ewant 0000000000000000000000000000000000000000\n0009done\n0000"
            .to_vec();
    let r = client
        .post(format!("{}/o/r.git/git-upload-pack", server.base_url))
        .header("Git-Protocol", "version=2")
        .header("Content-Type", "application/x-git-upload-pack-request")
        .body(body)
        .send()
        .await?;
    assert_eq!(r.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        r.headers().get("retry-after").map(|v| v.to_str().unwrap()),
        Some("15")
    );
    assert!(r.text().await?.contains("restarting"));

    // Refs-level reads still answer (the edge's read-only fallback lands here until it sees readyz).
    let refs = server
        .get_text("/o/r.git/info/refs?service=git-upload-pack", &[])
        .await?;
    assert!(refs.contains("refs/heads/main"));

    // The maintenance pass starts no unit (a checkpoint was due).
    let report = walgit_server::maintain::run_pass(&server.state).await?;
    assert_eq!(report.units, 0, "{report:?}");
    assert_eq!(
        report.repos, 0,
        "no repo visited while draining: {report:?}"
    );
    Ok(())
}
