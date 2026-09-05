//! The `maintain` role's pass: checkpoint-if-due (refs-level, on an instance
//! that cannot hold the packs), bundles-if-due, compaction, all as tasks.

mod harness;

use harness::{Server, git, git_in};
use std::collections::HashMap;

/// Every await is bounded so a hang names the step instead of stalling CI.
macro_rules! step {
    ($name:literal, $e:expr) => {
        tokio::time::timeout(std::time::Duration::from_secs(30), $e)
            .await
            .unwrap_or_else(|_| panic!("step timed out: {}", $name))
    };
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pass_checkpoints_due_repos_refs_level_and_reports_tasks() -> anyhow::Result<()> {
    use walgit_server::maintain::{Unit, next_unit, run_pass};

    // Writer front: count trigger off, so nothing auto-checkpoints on push.
    let front = step!("start front", Server::start())?;
    step!("put repo", front.put_repo("o", "r"))?;
    let src = tempfile::tempdir()?;
    git_in(src.path(), &["init", "-q", "-b", "main"])?;
    git_in(src.path(), &["config", "user.email", "t@t"])?;
    git_in(src.path(), &["config", "user.name", "Tester"])?;
    for i in 0..3 {
        std::fs::write(src.path().join(format!("f{i}.txt")), format!("{i}\n"))?;
        git_in(src.path(), &["add", "."])?;
        git_in(src.path(), &["commit", "-q", "-m", &format!("c{i}")])?;
        git(
            &["push", "-q", &front.repo_url("o", "r"), "main"],
            src.path(),
        )?;
    }
    let m = step!(
        "open on front",
        front
            .state
            .registry
            .open(&walgit_git::RepoId::new("o", "r")?)
    )?
    .manifest();
    assert_eq!(m.head_seq, 3);
    assert!(
        m.checkpoint.is_none(),
        "no checkpoint yet: {:?}",
        m.checkpoint
    );

    // Maintainer: age trigger (1 ms) and a cache too small for any pack.
    let maint = step!(
        "start maintainer",
        front.start_sibling_with(|c| {
            c.server.roles = vec![walgit_config::Role::Maintain];
            c.cache.max_bytes = walgit_config::ByteSize::b(1);
            c.wal.snapshot_every_entries = 0;
            c.wal.checkpoint_interval = std::time::Duration::from_millis(1);
            c.compaction.enabled = false;
            c.bundles.enabled = false;
        })
    )?;
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    let report = step!(
        "maintain pass 1",
        walgit_server::maintain::run_pass(&maint.state)
    )?;
    assert_eq!(report.repos, 1);
    assert_eq!(report.checkpoints, 1, "{report:?}");

    // Manifest folded; the task is discoverable on the maintainer.
    let h = step!(
        "open on maintainer",
        maint
            .state
            .registry
            .open(&walgit_git::RepoId::new("o", "r")?)
    )?;
    let m = h.manifest();
    assert_eq!(m.checkpoint.as_ref().map(|c| c.seq), Some(3));
    assert!(m.log_segments.is_empty());
    assert!(
        h.local().packs()?.is_empty(),
        "refs-level: no pack downloaded"
    );
    let tasks = step!("tasks list", maint.get_text("/o/r/api/tasks", &[]))?;
    assert!(tasks.contains("\"checkpoint\""), "{tasks}");
    assert!(
        tasks.contains("\"trigger\":\"age\"") || tasks.contains("age"),
        "{tasks}"
    );

    // Second pass: nothing due.
    let report = step!(
        "maintain pass 2",
        walgit_server::maintain::run_pass(&maint.state)
    )?;
    assert_eq!(report.checkpoints, 0);

    // Bundles from a maintainer that never served this repo: the build must
    // materialize the packs itself (prod failed with "bad object refs/heads/main").
    let bundler = step!(
        "start bundler",
        front.start_sibling_with(|c| {
            c.server.roles = vec![walgit_config::Role::Maintain];
            c.wal.snapshot_every_entries = 0;
            c.compaction.enabled = false;
            c.bundles.enabled = true;
        })
    )?;
    // Priority loop: the first unit is the missing weekly slot (checkpoint is
    // not due), one unit per pass, next pass moves to the daily chain, and a
    // re-run after everything is built is idempotent (Idle).
    let id = walgit_git::RepoId::new("o", "r")?;
    assert!(
        matches!(step!("unit 1", next_unit(&bundler.state, &id))?, Unit::BundleSlot(ref s, _) if s == "weekly")
    );
    let report = step!("bundler pass", run_pass(&bundler.state))?;
    assert_eq!((report.units, report.bundles), (1, 1), "{report:?}");
    let list = step!(
        "bundle list",
        bundler.get_text("/o/r.git/bundles/list", &[])
    )?;
    assert!(list.contains("[bundle \"weekly-"), "{list}");
    // Weekly token = its slot (a Sunday 23:00 UTC epoch, divisible by 3600).
    let tok: u64 = list
        .lines()
        .find_map(|l| {
            l.trim()
                .strip_prefix("creationToken = ")
                .and_then(|v| v.parse().ok())
        })
        .unwrap();
    assert_eq!(tok % 3600, 0, "token is a slot epoch: {tok}");
    // Next units: dailies (oldest first) — but a daily slot with no new objects
    // over the weekly is skipped — then the lowest-priority audit (fsck, once;
    // clean), so the loop converges to Idle.
    let mut audits = 0;
    for _ in 0..40 {
        match step!("unit n", next_unit(&bundler.state, &id))? {
            Unit::Idle => break,
            Unit::BundleSlot(..) => {
                let _ = step!("pass n", run_pass(&bundler.state))?;
            }
            Unit::Fsck(_) => {
                audits += 1;
                let _ = step!("pass fsck", run_pass(&bundler.state))?;
            }
            other => panic!("unexpected unit {other:?}"),
        }
    }
    assert_eq!(
        audits, 1,
        "the audit runs once (never audited) and is then not due for fsck_interval"
    );
    assert_eq!(
        step!("unit idle", next_unit(&bundler.state, &id))?,
        Unit::Idle
    );
    let report = step!("idempotent pass", run_pass(&bundler.state))?;
    assert_eq!(report.units, 0, "{report:?}");
    // Placement by rule: a maintainer not assigned to the repo plans nothing.
    let elsewhere = step!(
        "start elsewhere",
        front.start_sibling_with(|c| {
            c.server.roles = vec![walgit_config::Role::Maintain];
            c.placement.maintain = vec!["acme/*".into()];
        })
    )?;
    assert_eq!(
        step!("not assigned", next_unit(&elsewhere.state, &id))?,
        Unit::NotAssigned
    );
    let report = step!("elsewhere pass", run_pass(&elsewhere.state))?;
    assert_eq!((report.repos, report.units), (0, 0));
    // Heartbeat: the maintainer writes maintain/<host>.pb.
    let excluded = step!(
        "start excluded",
        front.start_sibling_with(|c| {
            c.server.roles = vec![walgit_config::Role::Maintain];
            c.placement.maintain_exclude = vec!["o/r".into()];
        })
    )?;
    assert_eq!(
        step!("excluded", next_unit(&excluded.state, &id))?,
        Unit::NotAssigned
    );

    // The front sees the checkpoint and a fresh instance cold-starts from it.
    let cold = step!("start cold", front.start_sibling_with(|_| {}))?;
    let refs = step!("cold ls-remote", cold.ls_remote("o", "r"))?;
    let head = git_in(src.path(), &["rev-parse", "HEAD"])?;
    assert!(refs.contains(head.trim()), "{refs}");
    Ok(())
}

/// D28: a maintainer that excludes a repository is not its writer and refuses
/// the push up front (no sync, no pack read) naming the writer; the same host
/// accepts pushes for repositories it is assigned.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn maintainer_refuses_pushes_for_excluded_repos() -> anyhow::Result<()> {
    let server = step!(
        "start",
        Server::start_with_tweak(|c| {
            c.server.roles = vec![walgit_config::Role::Serve, walgit_config::Role::Maintain];
            c.placement.maintain_exclude = vec!["o/large".into()];
            c.maintenance.host = Some("broker".into());
        })
    )?;
    step!("put excluded", server.put_repo("o", "large"))?;
    step!("put assigned", server.put_repo("o", "small"))?;
    let src = tempfile::tempdir()?;
    git_in(src.path(), &["init", "-q", "-b", "main"])?;
    git_in(src.path(), &["config", "user.email", "t@t"])?;
    git_in(src.path(), &["config", "user.name", "Tester"])?;
    std::fs::write(src.path().join("f.txt"), "hi\n")?;
    git_in(src.path(), &["add", "."])?;
    git_in(src.path(), &["commit", "-q", "-m", "c"])?;

    let url = server.repo_url("o", "large");
    let out = std::process::Command::new("git")
        .args(["push", "--porcelain", &url, "main"])
        .current_dir(src.path())
        .env("GIT_TERMINAL_PROMPT", "0")
        .output()?;
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(!out.status.success(), "push must be refused: {text}");
    assert!(
        text.contains("o/large is written by"),
        "names the writer: {text}"
    );
    assert!(text.contains("refs/heads/main"), "ng per ref: {text}");
    let h = step!(
        "open",
        server
            .state
            .registry
            .open(&walgit_git::RepoId::new("o", "large")?)
    )?;
    assert_eq!(h.manifest().head_seq, 0, "nothing published");
    assert!(
        !server.registry_has_packs("o", "large").await,
        "no sync happened"
    );

    git(
        &["push", "-q", &server.repo_url("o", "small"), "main"],
        src.path(),
    )?;
    let h = step!(
        "open small",
        server
            .state
            .registry
            .open(&walgit_git::RepoId::new("o", "small")?)
    )?;
    assert_eq!(h.manifest().head_seq, 1);
    Ok(())
}

/// Integrity units (the original large-repository measurements, a large repository's 1,952 missing blobs): the
/// weekly `fsck` unit records missing objects at fsck.pb; the `repair` unit
/// fetches exactly those from `upstream.git` and publishes them as a pack; the
/// next `fsck` re-verifies clean. The upstream here is a second repository on
/// the same server (walgit serves wants by SHA when configured, like GitHub).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fsck_unit_records_missing_objects_and_repair_unit_fetches_them_from_upstream()
-> anyhow::Result<()> {
    let server = step!(
        "start",
        Server::start_with_tweak(|c| {
            c.git.allow_any_sha1_in_want = true;
            c.maintenance.checkpoints = false;
            c.compaction.enabled = false;
            c.bundles.enabled = false;
            c.maintenance.fsck_interval = std::time::Duration::from_hours(1);
        })
    )?;
    step!("put repo", server.put_repo("o", "r"))?;
    step!("put upstream", server.put_repo("o", "up"))?;
    let src = tempfile::tempdir()?;
    git_in(src.path(), &["init", "-q", "-b", "main"])?;
    git_in(src.path(), &["config", "user.email", "t@t"])?;
    git_in(src.path(), &["config", "user.name", "Tester"])?;
    std::fs::write(src.path().join("a.txt"), "one\n")?;
    git_in(src.path(), &["add", "."])?;
    git_in(src.path(), &["commit", "-q", "-m", "one"])?;
    let c1 = git_in(src.path(), &["rev-parse", "HEAD"])?
        .trim()
        .to_string();
    git(
        &["push", "-q", &server.repo_url("o", "r"), "main"],
        src.path(),
    )?;
    std::fs::write(src.path().join("b.txt"), "the blob the import dropped\n")?;
    git_in(src.path(), &["add", "."])?;
    git_in(src.path(), &["commit", "-q", "-m", "two"])?;
    let c2 = git_in(src.path(), &["rev-parse", "HEAD"])?
        .trim()
        .to_string();
    let tree2 = git_in(src.path(), &["rev-parse", "HEAD^{tree}"])?
        .trim()
        .to_string();
    let blob2 = git_in(src.path(), &["rev-parse", "HEAD:b.txt"])?
        .trim()
        .to_string();
    // The upstream has everything.
    git(
        &["push", "-q", &server.repo_url("o", "up"), "main"],
        src.path(),
    )?;

    // The hole: publish commit 2 + its tree WITHOUT the new blob (a pack that is
    // not the closure of the ref), then move main onto it — exactly the import's
    // mistake, which receive-pack's connectivity check would have refused.
    let holes = tempfile::tempdir()?;
    let out = std::process::Command::new("git")
        .current_dir(src.path())
        .args(["pack-objects", &format!("{}/pack", holes.path().display())])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .and_then(|mut c| {
            use std::io::Write;
            c.stdin
                .take()
                .unwrap()
                .write_all(format!("{c2}\n{tree2}\n").as_bytes())?;
            c.wait_with_output()
        })?;
    let sha = String::from_utf8_lossy(&out.stdout).trim().to_string();
    let id = walgit_git::RepoId::new("o", "r")?;
    let h = step!("open", server.state.registry.open(&id))?;
    step!("sync", h.sync())?;
    step!(
        "add hole pack",
        h.add_pack(
            &holes.path().join(format!("pack-{sha}.pack")),
            &holes.path().join(format!("pack-{sha}.idx")),
            0,
            None
        )
    )?;
    step!("sync2", h.sync())?;
    let txn = walgit_proto::v1::RefTransaction {
        updates: vec![walgit_proto::v1::RefUpdate {
            name: "refs/heads/main".into(),
            old_oid: c1.clone(),
            new_oid: c2.clone(),
            ..Default::default()
        }],
        ..Default::default()
    };
    step!(
        "move main",
        h.publish_push_synced(None, txn, HashMap::default())
    )?;

    // Pass 1: the audit (never audited) → fsck.pb lists the blob; the unit succeeds (a finding, not a failure).
    let unit = step!(
        "plan 1",
        walgit_server::maintain::next_unit(&server.state, &id)
    )?;
    assert!(
        matches!(unit, walgit_server::maintain::Unit::Fsck(_)),
        "{unit:?}"
    );
    let report = step!("pass 1", walgit_server::maintain::run_pass(&server.state))?;
    assert_eq!(report.units, 2, "both repositories audited: {report:?}");
    let f = walgit_server::ops::read_fsck(&h)
        .await
        .unwrap()
        .expect("fsck.pb written");
    assert_eq!(f.missing, vec![blob2.clone()], "{f:?}");
    assert_eq!(f.repaired_seq, 0);

    // No upstream → nothing can repair; the plan says so (Idle, the audit is fresh).
    let unit = step!(
        "plan no upstream",
        walgit_server::maintain::next_unit(&server.state, &id)
    )?;
    assert_eq!(unit, walgit_server::maintain::Unit::Idle, "{unit:?}");

    // With upstream.git (D24 setting) the repair unit is next.
    let client = reqwest::Client::new();
    let r = client
        .put(format!("{}/o/r/api/settings", server.base_url))
        .header("Content-Type", "application/toml")
        .body(format!(
            "[upstream]\ngit = \"{}\"\n",
            server.repo_url("o", "up")
        ))
        .send()
        .await?;
    assert!(r.status().is_success(), "{}", r.text().await?);
    let unit = step!(
        "plan 2",
        walgit_server::maintain::next_unit(&server.state, &id)
    )?;
    assert_eq!(unit, walgit_server::maintain::Unit::Repair(1), "{unit:?}");
    let head_before = h.manifest().head_seq;
    let report = step!("pass 2", walgit_server::maintain::run_pass(&server.state))?;
    assert_eq!(report.units, 1, "{report:?}");
    step!("sync3", h.sync())?;
    assert_eq!(
        h.manifest().head_seq,
        head_before + 1,
        "one COMPACT entry with the repaired objects"
    );
    let f = walgit_server::ops::read_fsck(&h).await.unwrap().unwrap();
    assert_eq!(f.repaired_seq, head_before + 1);
    let ok = std::process::Command::new("git")
        .current_dir(h.local().path())
        .args(["cat-file", "-e", &blob2])
        .status()?
        .success();
    assert!(ok, "the blob is back in the serving copy");

    // Pass 3: re-verify after the repair → clean, nothing due afterwards.
    let unit = step!(
        "plan 3",
        walgit_server::maintain::next_unit(&server.state, &id)
    )?;
    assert!(
        matches!(unit, walgit_server::maintain::Unit::Fsck(ref w) if w.contains("re-verify")),
        "{unit:?}"
    );
    let report = step!("pass 3", walgit_server::maintain::run_pass(&server.state))?;
    assert_eq!(report.units, 1, "{report:?}");
    let f = walgit_server::ops::read_fsck(&h).await.unwrap().unwrap();
    assert!(f.missing.is_empty() && f.problems == 0, "{f:?}");
    let unit = step!(
        "plan 4",
        walgit_server::maintain::next_unit(&server.state, &id)
    )?;
    assert_eq!(unit, walgit_server::maintain::Unit::Idle);
    Ok(())
}

/// A push whose pack references an object the server lacks (the client
/// believes the server has it) is refused with the reason ON EVERY REF —
/// `unpack ng` alone made git print "remote failed to report status" and the
/// server logged nothing (prod 2026-08-21 03:28Z, the 1,952-blob hole).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn connectivity_failure_is_reported_per_ref_not_as_remote_failure() -> anyhow::Result<()> {
    let server = step!("start", Server::start())?;
    step!("put repo", server.put_repo("o", "r"))?;
    let src = tempfile::tempdir()?;
    git_in(src.path(), &["init", "-q", "-b", "main"])?;
    git_in(src.path(), &["config", "user.email", "t@t"])?;
    git_in(src.path(), &["config", "user.name", "Tester"])?;
    std::fs::write(src.path().join("a.txt"), "one\n")?;
    git_in(src.path(), &["add", "."])?;
    git_in(src.path(), &["commit", "-q", "-m", "one"])?;
    git(
        &["push", "-q", &server.repo_url("o", "r"), "main"],
        src.path(),
    )?;
    std::fs::write(src.path().join("b.txt"), "two\n")?;
    git_in(src.path(), &["add", "."])?;
    git_in(src.path(), &["commit", "-q", "-m", "two"])?;
    let c2 = git_in(src.path(), &["rev-parse", "HEAD"])?
        .trim()
        .to_string();
    let blob2 = git_in(src.path(), &["rev-parse", "HEAD:b.txt"])?
        .trim()
        .to_string();
    // Make the client believe the server already has commit 2: a second remote ref
    // in the advertisement. We fake it by pushing only the commit + tree via a
    // ref the server accepts without connectivity (a tag on a pack that lacks the
    // blob is refused too) — so instead feed receive-pack a thin pack directly.
    // Simplest faithful reproduction: push main with `--no-thin` disabled and the
    // blob object deleted from the client's own odb *after* git decided it is
    // unchanged... Too brittle. Use the server API: publish the commit+tree pack
    // (no blob) and advertise `refs/heads/x` at c2; then `git push main` sends
    // zero objects (c2 is "already there") and the server's connectivity check
    // trips on the blob.
    let holes = tempfile::tempdir()?;
    let tree2 = git_in(src.path(), &["rev-parse", "HEAD^{tree}"])?
        .trim()
        .to_string();
    let out = std::process::Command::new("git")
        .current_dir(src.path())
        .args(["pack-objects", &format!("{}/pack", holes.path().display())])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .and_then(|mut c| {
            use std::io::Write;
            c.stdin
                .take()
                .unwrap()
                .write_all(format!("{c2}\n{tree2}\n").as_bytes())?;
            c.wait_with_output()
        })?;
    let sha = String::from_utf8_lossy(&out.stdout).trim().to_string();
    let id = walgit_git::RepoId::new("o", "r")?;
    let h = step!("open", server.state.registry.open(&id))?;
    step!("sync", h.sync())?;
    step!(
        "add hole pack",
        h.add_pack(
            &holes.path().join(format!("pack-{sha}.pack")),
            &holes.path().join(format!("pack-{sha}.idx")),
            0,
            None
        )
    )?;
    step!("sync2", h.sync())?;
    let txn = walgit_proto::v1::RefTransaction {
        updates: vec![walgit_proto::v1::RefUpdate {
            name: "refs/heads/x".into(),
            old_oid: String::new(),
            new_oid: c2.clone(),
            ..Default::default()
        }],
        ..Default::default()
    };
    step!(
        "advertise x",
        h.publish_push_synced(None, txn, HashMap::default())
    )?;
    // A new commit on top whose tree still references the missing blob (b.txt
    // unchanged): git sends commit 3 + its root tree, the server walks into b.txt.
    std::fs::write(src.path().join("a.txt"), "three\n")?;
    git_in(src.path(), &["add", "."])?;
    git_in(src.path(), &["commit", "-q", "-m", "three"])?;

    let out = std::process::Command::new("git")
        .args(["push", "--porcelain", &server.repo_url("o", "r"), "main"])
        .current_dir(src.path())
        .output()?;
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(!out.status.success(), "must be refused: {text}");
    assert!(
        !text.contains("remote failure") && !text.contains("failed to report status"),
        "git must see a proper report: {text}"
    );
    assert!(
        text.contains("refs/heads/main") && text.contains("connectivity") && text.contains(&blob2),
        "per-ref reason names the oid: {text}"
    );
    Ok(())
}

/// Placement (D29/D30, the operator: "the SSD host looks after acme/monorepo and a serverless host
/// doesn't"): a host whose `[placement] serve_exclude` names a repository answers
/// its object work — fetch, push, LFS — with 503 + Retry-After BEFORE any sync
/// (no task, no materialize), while refs-level reads (info/refs, ls-remote, the
/// API) keep working so the edge's read-only fallback is useful. Other repos are
/// served normally by the same host.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn host_excluded_from_serving_a_repo_refuses_object_work_with_503() -> anyhow::Result<()> {
    // The writer host holds both repos.
    let writer = step!("start writer", Server::start())?;
    step!("put big", writer.put_repo("acme", "big"))?;
    step!("put small", writer.put_repo("o", "small"))?;
    let src = tempfile::tempdir()?;
    git_in(src.path(), &["init", "-q", "-b", "main"])?;
    git_in(src.path(), &["config", "user.email", "t@t"])?;
    git_in(src.path(), &["config", "user.name", "Tester"])?;
    std::fs::write(src.path().join("f.txt"), "hi\n")?;
    git_in(src.path(), &["add", "."])?;
    git_in(src.path(), &["commit", "-q", "-m", "c"])?;
    git(
        &["push", "-q", &writer.repo_url("acme", "big"), "main"],
        src.path(),
    )?;
    git(
        &["push", "-q", &writer.repo_url("o", "small"), "main"],
        src.path(),
    )?;

    // A front that does not serve acme/*.
    let front = step!(
        "start front",
        writer.start_sibling_with(|c| {
            c.placement.serve_exclude = vec!["acme/*".into()];
        })
    )?;
    let client = reqwest::Client::new();

    // Refs-level still answers on the front.
    let refs = step!(
        "info/refs",
        front.get_text("/acme/big.git/info/refs?service=git-upload-pack", &[])
    )?;
    assert!(refs.contains("refs/heads/main"), "{refs}");
    let ls = step!("ls-remote", front.ls_remote("acme", "big"))?;
    assert!(ls.contains("refs/heads/main"));

    // Fetch (v2) → 503 + Retry-After + ERR naming the host; no task started.
    let body =
        b"0011command=fetch0001000ewant 0000000000000000000000000000000000000000\n0009done\n0000"
            .to_vec();
    let r = client
        .post(format!("{}/acme/big.git/git-upload-pack", front.base_url))
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
    let text = r.text().await?;
    assert!(text.contains("ERR walgit: acme/big is served by"), "{text}");
    let tasks = step!("tasks", front.get_text("/acme/big/api/tasks", &[]))?;
    assert!(
        !tasks.contains("materialize") && !tasks.contains("remote-index"),
        "no sync started: {tasks}"
    );

    // Push → 503 (git shows the RPC failure) and nothing published.
    std::fs::write(src.path().join("g.txt"), "more\n")?;
    git_in(src.path(), &["add", "."])?;
    git_in(src.path(), &["commit", "-q", "-m", "d"])?;
    let out = std::process::Command::new("git")
        .args(["push", &front.repo_url("acme", "big"), "main"])
        .current_dir(src.path())
        .output()?;
    assert!(!out.status.success());
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("503"), "{err}");
    let h = step!(
        "open",
        writer
            .state
            .registry
            .open(&walgit_git::RepoId::new("acme", "big")?)
    )?;
    step!("sync", h.sync_refs())?;
    assert_eq!(
        h.manifest().head_seq,
        1,
        "nothing published through the front"
    );

    // LFS batch → 503 too.
    let r = client
        .post(format!("{}/acme/big.git/info/lfs/objects/batch", front.base_url))
        .json(&serde_json::json!({"operation": "download", "objects": [{"oid": "0".repeat(64), "size": 1}]}))
        .send()
        .await?;
    assert_eq!(r.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);

    // The same front serves o/small: push + clone work.
    git(
        &["push", "-q", &front.repo_url("o", "small"), "main"],
        src.path(),
    )?;
    let clone = tempfile::tempdir()?;
    git(
        &[
            "clone",
            "-q",
            &front.repo_url("o", "small"),
            clone.path().to_str().unwrap(),
        ],
        clone.path().parent().unwrap(),
    )?;
    assert!(clone.path().join("g.txt").exists());
    Ok(())
}

/// The static bundle list must show a bundle this host just built — the list is
/// cached per repo (TTL) and the `bundle` op invalidates it. Prod 2026-08-21:
/// the SSD host advertised 4 hourlies for 20+ min after it had published the 5th
/// (the cache was keyed by manifest version, which a publish does not change).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bundle_list_shows_a_bundle_right_after_this_host_builds_it() -> anyhow::Result<()> {
    let server = step!(
        "start",
        Server::start_with_tweak(|c| {
            c.server.roles = vec![walgit_config::Role::Serve, walgit_config::Role::Maintain];
            c.bundles.enabled = true;
            c.bundles.min_commits = 1;
            c.compaction.enabled = false;
            c.maintenance.checkpoints = false;
        })
    )?;
    step!("put repo", server.put_repo("o", "r"))?;
    let src = tempfile::tempdir()?;
    git_in(src.path(), &["init", "-q", "-b", "main"])?;
    git_in(src.path(), &["config", "user.email", "t@t"])?;
    git_in(src.path(), &["config", "user.name", "Tester"])?;
    std::fs::write(src.path().join("f.txt"), "one\n")?;
    git_in(src.path(), &["add", "."])?;
    git_in(src.path(), &["commit", "-q", "-m", "one"])?;
    git(
        &["push", "-q", &server.repo_url("o", "r"), "main"],
        src.path(),
    )?;

    // Weekly via the op (what the maintainer runs), then read + cache the list.
    let id = walgit_git::RepoId::new("o", "r")?;
    let mut params = std::collections::HashMap::new();
    params.insert("strategy".to_string(), "weekly".to_string());
    let t = walgit_server::ops::start(server.state.clone(), id.clone(), "bundle", params)
        .await
        .map_err(|_| anyhow::anyhow!("op start failed"))?;
    assert!(t.wait_done(std::time::Duration::from_secs(30)).await);
    let list1 = step!("list 1", server.get_text("/o/r.git/bundles/list", &[]))?;
    assert!(list1.contains("[bundle \"weekly-"), "{list1}");
    assert!(!list1.contains("daily-"));
    let _again = step!(
        "list 1 again (cached)",
        server.get_text("/o/r.git/bundles/list", &[])
    )?;

    // New objects, a daily built by the op → the NEXT list shows it, no TTL wait.
    std::fs::write(src.path().join("g.txt"), "two\n")?;
    git_in(src.path(), &["add", "."])?;
    git_in(src.path(), &["commit", "-q", "-m", "two"])?;
    git(
        &["push", "-q", &server.repo_url("o", "r"), "main"],
        src.path(),
    )?;
    let mut params = std::collections::HashMap::new();
    params.insert("strategy".to_string(), "daily".to_string());
    let t = walgit_server::ops::start(server.state.clone(), id.clone(), "bundle", params)
        .await
        .map_err(|_| anyhow::anyhow!("op start failed"))?;
    assert!(t.wait_done(std::time::Duration::from_secs(30)).await);
    assert!(t.outcome().is_some_and(|o| o.is_ok()), "{:?}", t.outcome());
    let list2 = step!("list 2", server.get_text("/o/r.git/bundles/list", &[]))?;
    assert!(
        list2.contains("[bundle \"daily-"),
        "the list served right after the build must contain it:\n{list2}"
    );

    // Another host on the same bucket, which cached the list BEFORE the next build
    // and is never told about it: its next GET must still be fresh (the cache is
    // keyed by list.pb's own version, probed per request — not by a TTL).
    let other = step!(
        "start other",
        server.start_sibling_with(|c| {
            c.bundles.enabled = true;
        })
    )?;
    let seen = step!("other list", other.get_text("/o/r.git/bundles/list", &[]))?;
    assert!(seen.contains("daily-") && !seen.contains("hourly-"));
    std::fs::write(src.path().join("h.txt"), "three\n")?;
    git_in(src.path(), &["add", "."])?;
    git_in(src.path(), &["commit", "-q", "-m", "three"])?;
    git(
        &["push", "-q", &server.repo_url("o", "r"), "main"],
        src.path(),
    )?;
    let mut params = std::collections::HashMap::new();
    params.insert("strategy".to_string(), "hourly".to_string());
    let t = walgit_server::ops::start(server.state.clone(), id.clone(), "bundle", params)
        .await
        .map_err(|_| anyhow::anyhow!("op start failed"))?;
    assert!(t.wait_done(std::time::Duration::from_secs(30)).await);
    let list3 = step!(
        "other list after a build elsewhere",
        other.get_text("/o/r.git/bundles/list", &[])
    )?;
    assert!(
        list3.contains("[bundle \"hourly-"),
        "a host that did not build must still serve the new list at once:\n{list3}"
    );
    Ok(())
}

/// A hundred closed hourly slots with nothing to cut must not cost a hundred passes
/// (closed = the slot's as-of instant has passed, whatever the strategy's period —
/// a daily is final an hour after 23:00, not at the next 23:00):
/// `next_unit` settles them at plan time (refs-level; verdicts recorded in the
/// list), and the live slot with real objects is built in the SAME pass.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_pass_settles_all_closed_empty_slots() -> anyhow::Result<()> {
    let server = step!(
        "start",
        Server::start_with_tweak(|c| {
            c.server.roles = vec![walgit_config::Role::Serve, walgit_config::Role::Maintain];
            c.bundles.enabled = true;
            c.bundles.main_only = true;
            c.bundles.min_commits = 1;
            c.compaction.enabled = false;
            c.maintenance.checkpoints = false;
            c.maintenance.fsck_interval = std::time::Duration::ZERO;
            // weekly (full) + hourly on weekly: the closed hours since the weekly are empty.
            c.bundles.strategy.retain(|s| s.name != "daily");
            for s in &mut c.bundles.strategy {
                if s.name == "hourly" {
                    s.base = Some("weekly".into());
                    s.backfill_max = 0;
                }
            }
        })
    )?;
    step!("put repo", server.put_repo("o", "r"))?;
    let src = tempfile::tempdir()?;
    git_in(src.path(), &["init", "-q", "-b", "main"])?;
    git_in(src.path(), &["config", "user.email", "t@t"])?;
    git_in(src.path(), &["config", "user.name", "Tester"])?;
    std::fs::write(src.path().join("f.txt"), "one\n")?;
    git_in(src.path(), &["add", "."])?;
    git_in(src.path(), &["commit", "-q", "-m", "one"])?;
    // Publish the first state 31 hours in the past so that 30 closed hourly slots exist.
    let id = walgit_git::RepoId::new("o", "r")?;
    let h = step!("open", server.state.registry.open(&id))?;
    let now = std::time::SystemTime::now();
    let c1 = git_in(src.path(), &["rev-parse", "HEAD"])?
        .trim()
        .to_string();
    // Push normally first (objects), then time-shift the ref history with an explicit created_at.
    git(
        &["push", "-q", &server.repo_url("o", "r"), "main"],
        src.path(),
    )?;
    step!("sync", h.sync())?;
    // A weekly cut at the Sunday before yesterday (earliest state), via the op.
    let weekly = server
        .state
        .cfg
        .bundles
        .strategy
        .iter()
        .find(|s| s.name == "weekly")
        .unwrap()
        .clone();
    let sunday = walgit_bundle::slots::last_slot_at_or_before(
        &weekly,
        now - std::time::Duration::from_hours(36),
    )?
    .unwrap();
    let mut params = std::collections::HashMap::new();
    params.insert("strategy".to_string(), "weekly".to_string());
    params.insert("slot".to_string(), sunday.to_string());
    let t = walgit_server::ops::start(server.state.clone(), id.clone(), "bundle", params)
        .await
        .map_err(|_| anyhow::anyhow!("op start"))?;
    assert!(t.wait_done(std::time::Duration::from_secs(30)).await);
    let list = walgit_bundle::ops::read_list(h.store())
        .await?
        .expect("list");
    assert_eq!(list.bundles.len(), 1, "{list:?}");
    drop(c1);

    // New objects NOW (the live hour has real work).
    std::fs::write(src.path().join("g.txt"), "two\n")?;
    git_in(src.path(), &["add", "."])?;
    git_in(src.path(), &["commit", "-q", "-m", "two"])?;
    git(
        &["push", "-q", &server.repo_url("o", "r"), "main"],
        src.path(),
    )?;

    // Plan before: only the two newest hourly slots are wanted (D21, 2026-08-22) — the older ones
    // are not work even though they are missing; the closed one of the two is empty (the weekly
    // holds everything up to now-ish).
    let ctx = walgit_bundle::slots::PlanContext {
        first_state: h.first_state_time(),
        can_full: true,
        can_incremental: true,
        wrong_host_reason: None,
    };
    let rows = server.state.bundles.plan(&id, now, ctx).await?;
    let missing_before = rows
        .iter()
        .filter(|r| r.strategy == "hourly" && r.status == walgit_bundle::slots::SlotStatus::Missing)
        .count();
    assert_eq!(
        missing_before,
        walgit_bundle::slots::INCREMENTALS_KEPT,
        "only the newest slots are planned: {rows:?}"
    );

    // ONE pass.
    let report = step!("pass", walgit_server::maintain::run_pass(&server.state))?;
    let list = walgit_bundle::ops::read_list(h.store())
        .await?
        .expect("list");
    let hourlies: Vec<_> = list
        .bundles
        .iter()
        .filter(|b| b.strategy == "hourly")
        .collect();
    assert!(
        !list.skipped.is_empty(),
        "closed empty slots recorded in the list: {report:?}"
    );
    // Every CLOSED missing slot is settled in that one pass. The open (current)
    // slot may stay missing: the commit above was pushed after its fire time, so
    // as of the slot there is nothing new — it belongs to the next hour (D22).
    let hourly = server
        .state
        .cfg
        .bundles
        .strategy
        .iter()
        .find(|s| s.name == "hourly")
        .unwrap()
        .clone();
    let rows = server
        .state
        .bundles
        .plan(&id, std::time::SystemTime::now(), ctx)
        .await?;
    let still_missing_closed: Vec<u64> = rows
        .iter()
        .filter(|r| {
            r.strategy == "hourly"
                && r.status == walgit_bundle::slots::SlotStatus::Missing
                && walgit_bundle::slots::slot_closed(&hourly, r.slot, std::time::SystemTime::now())
        })
        .map(|r| r.slot)
        .collect();
    assert!(
        still_missing_closed.is_empty(),
        "after one pass no closed slot stays missing: {still_missing_closed:?}\nskipped={} built={}",
        list.skipped.len(),
        hourlies.len()
    );
    assert!(
        list.skipped.len() >= missing_before - 1,
        "settled at plan time, not one per pass: skipped={} missing_before={missing_before}",
        list.skipped.len()
    );
    Ok(())
}

/// Sunday's weekly on an ssd maintainer (the SSD host): the missing full slot of a
/// repository that has a tier-2 base and pushes since it first yields
/// `BaseRebuild` (compact --base: new base + history pack + checkpoint), then the
/// full slot itself, which COMPOSES header ∘ base (no pack-objects of the
/// history). Pushes landing after the rebuild do not re-trigger it this week.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn weekly_slot_rebuilds_the_base_then_composes_it_on_an_ssd_maintainer() -> anyhow::Result<()>
{
    use walgit_server::maintain::{Unit, next_unit, run_pass};
    let server = step!(
        "start",
        Server::start_with_tweak(|c| {
            c.server.roles = vec![
                walgit_config::Role::Serve,
                walgit_config::Role::Maintain,
                walgit_config::Role::Compact,
                walgit_config::Role::Bundle,
            ];
            c.maintenance.disk = walgit_config::MaintainerDisk::Ssd;
            c.cache.mode = walgit_config::CacheMode::Disk;
            c.bundles.enabled = true;
            c.bundles.strategy.truncate(1); // weekly only
            c.compaction.enabled = true;
            c.maintenance.checkpoints = false;
            c.maintenance.fsck_interval = std::time::Duration::ZERO;
        })
    )?;
    step!("put repo", server.put_repo("o", "r"))?;
    let src = tempfile::tempdir()?;
    git_in(src.path(), &["init", "-q", "-b", "main"])?;
    git_in(src.path(), &["config", "user.email", "t@t"])?;
    git_in(src.path(), &["config", "user.name", "Tester"])?;
    std::fs::write(src.path().join("f.txt"), "one\n")?;
    git_in(src.path(), &["add", "."])?;
    git_in(src.path(), &["commit", "-q", "-m", "one"])?;
    // A large repository's shape: the repository's FIRST entry is the base (import --direct
    // publishes a tier-2 pack + the ref snapshot), then pushes land on top.
    let id = walgit_git::RepoId::new("o", "r")?;
    let h = step!("open", server.state.registry.open(&id))?;
    let c1 = git_in(src.path(), &["rev-parse", "HEAD"])?
        .trim()
        .to_string();
    let packs = tempfile::tempdir()?;
    let out = std::process::Command::new("git")
        .current_dir(src.path())
        .args([
            "pack-objects",
            "--revs",
            &format!("{}/pack", packs.path().display()),
        ])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .and_then(|mut c| {
            use std::io::Write;
            c.stdin
                .take()
                .unwrap()
                .write_all(format!("{c1}\n").as_bytes())?;
            c.wait_with_output()
        })?;
    let sha = String::from_utf8_lossy(&out.stdout).trim().to_string();
    step!("sync0", h.sync())?;
    step!(
        "import base",
        h.add_pack(
            &packs.path().join(format!("pack-{sha}.pack")),
            &packs.path().join(format!("pack-{sha}.idx")),
            2,
            None
        )
    )?;
    let txn = walgit_proto::v1::RefTransaction {
        updates: vec![walgit_proto::v1::RefUpdate {
            name: "refs/heads/main".into(),
            old_oid: String::new(),
            new_oid: c1.clone(),
            ..Default::default()
        }],
        ..Default::default()
    };
    step!(
        "import refs",
        h.publish_push_synced(None, txn, HashMap::default())
    )?;
    step!("sync after base", h.sync())?;
    std::fs::write(src.path().join("g.txt"), "two\n")?;
    git_in(src.path(), &["add", "."])?;
    git_in(src.path(), &["commit", "-q", "-m", "two"])?;
    git(
        &["push", "-q", &server.repo_url("o", "r"), "main"],
        src.path(),
    )?;
    step!("sync after push", h.sync())?;

    // The missing weekly slot → rebuild the base first.
    let unit = step!("plan 1", next_unit(&server.state, &id))?;
    assert!(
        matches!(unit, Unit::BaseRebuild(ref s, _) if s == "weekly"),
        "{unit:?}"
    );
    let report = step!("pass 1 (rebuild)", run_pass(&server.state))?;
    assert_eq!(
        report.compactions,
        1,
        "rebuild unit: {report:?}\ntasks: {}",
        server.get_text("/o/r/api/tasks", &[]).await?
    );
    step!("sync after rebuild", h.sync())?;
    let base2 = h
        .manifest()
        .packs
        .iter()
        .filter(|p| p.tier == 2 && p.kind != walgit_proto::v1::PackKind::History as i32)
        .map(|p| p.checksum.clone())
        .next()
        .expect("new base");
    assert_ne!(sha, base2, "a new base was published");
    assert!(
        h.manifest().packs.iter().all(|p| p.tier == 2),
        "the rebuild superseded every smaller pack: {:?}",
        h.manifest()
            .packs
            .iter()
            .map(|p| p.tier)
            .collect::<Vec<_>>()
    );

    // A push lands between the rebuild and the compose (the rig's churn, 2026-08-22: the compose
    // refused for as long as refs kept moving — "no ref snapshot at the base's seq"). The header
    // must carry the refs AT THE BASE'S SEQ (replayed from the WAL), not the new tip.
    let base_tip = git_in(src.path(), &["rev-parse", "HEAD"])?
        .trim()
        .to_string();
    std::fs::write(src.path().join("between.txt"), "between\n")?;
    git_in(src.path(), &["add", "."])?;
    git_in(src.path(), &["commit", "-q", "-m", "between"])?;
    git(
        &["push", "-q", &server.repo_url("o", "r"), "main"],
        src.path(),
    )?;
    step!("sync after push between", h.sync())?;

    // Next: the weekly slot itself, composed from the new base.
    let unit = step!("plan 2", next_unit(&server.state, &id))?;
    assert!(
        matches!(unit, Unit::BundleSlot(ref s, _) if s == "weekly"),
        "{unit:?}"
    );
    let report = step!("pass 2 (compose)", run_pass(&server.state))?;
    assert_eq!(
        report.bundles,
        1,
        "compose unit: {report:?}\ntasks: {}",
        server.get_text("/o/r/api/tasks", &[]).await?
    );
    let list = walgit_bundle::ops::read_list(h.store())
        .await?
        .expect("list");
    let weekly = list
        .bundles
        .iter()
        .find(|b| b.strategy == "weekly")
        .expect("weekly entry");
    let m2 = h.manifest();
    let base_pack = m2.packs.iter().find(|p| p.checksum == base2).unwrap();
    assert!(
        weekly.size > base_pack.pack_size && weekly.size < base_pack.pack_size + 4096,
        "composed = header ∘ base pack: {} vs pack {}",
        weekly.size,
        base_pack.pack_size
    );
    assert_eq!(weekly.seq, base_pack.seq);
    let main_tip = weekly
        .tips
        .iter()
        .find(|t| t.name == "refs/heads/main")
        .expect("main tip");
    assert_eq!(
        main_tip.oid, base_tip,
        "the header carries main as of the base's seq, not the push that landed since"
    );

    // A push after the rebuild: no second rebuild this week; the plan is idle (slot built).
    std::fs::write(src.path().join("h.txt"), "three\n")?;
    git_in(src.path(), &["add", "."])?;
    git_in(src.path(), &["commit", "-q", "-m", "three"])?;
    git(
        &["push", "-q", &server.repo_url("o", "r"), "main"],
        src.path(),
    )?;
    step!("sync after push 2", h.sync())?;
    let unit = step!("plan 3", next_unit(&server.state, &id))?;
    assert!(!matches!(unit, Unit::BaseRebuild(..)), "{unit:?}");

    // An imported multi-pack set (large-repository measurement: 11 tier-2 packs, the 32 GB
    // base among 5 MB ones) is itself a reason to rebuild next week: the
    // compose needs exactly one base, and "the base" is the biggest one.
    let extra = tempfile::tempdir()?;
    let blob = git_in(src.path(), &["rev-parse", "HEAD:h.txt"])?
        .trim()
        .to_string();
    let out = std::process::Command::new("git")
        .current_dir(src.path())
        .args(["pack-objects", &format!("{}/pack", extra.path().display())])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .and_then(|mut c| {
            use std::io::Write;
            c.stdin
                .take()
                .unwrap()
                .write_all(format!("{blob}\n").as_bytes())?;
            c.wait_with_output()
        })?;
    let small = String::from_utf8_lossy(&out.stdout).trim().to_string();
    step!(
        "second tier-2 pack",
        h.add_pack(
            &extra.path().join(format!("pack-{small}.pack")),
            &extra.path().join(format!("pack-{small}.idx")),
            2,
            None
        )
    )?;
    step!("sync 3", h.sync())?;
    let m3 = h.manifest();
    assert_eq!(walgit_wal::base_packs(&m3).len(), 2);
    assert_eq!(
        walgit_wal::base_pack(&m3).unwrap().checksum,
        base2,
        "the base is the biggest tier-2 pack, not the newest"
    );
    let next_weekly =
        walgit_bundle::slots::from_epoch(weekly.slot) + std::time::Duration::from_hours(168);
    let up = walgit_server::maintain::upcoming(
        &h,
        &h.effective_config(),
        &walgit_server::maintain::heartbeats(&server.state).await?,
        next_weekly - std::time::Duration::from_mins(1),
    )
    .await;
    let w = up
        .iter()
        .find(|u| u.strategy == "weekly")
        .expect("weekly row");
    assert!(w.unit.starts_with("base rebuild"), "{w:?}");
    Ok(())
}

/// A pack published without its `.rev` (git < 2.41 wrote none; a large repository's whole
/// serving copy had none, 2.85 s per fetch — the original large-repository measurements) gets one
/// from the maintainer: built where the pack is local, uploaded as the
/// side-file, advertised in the manifest (`has_rev`) so every other host
/// downloads it on its next sync instead of rebuilding it per `pack-objects`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn maintainer_builds_and_publishes_missing_rev_indexes() -> anyhow::Result<()> {
    use walgit_server::maintain::{Unit, next_unit};
    let server = step!(
        "start",
        Server::start_with_tweak(|c| {
            c.maintenance.checkpoints = false;
            c.compaction.enabled = false;
            c.bundles.enabled = false;
            c.maintenance.fsck_interval = std::time::Duration::ZERO;
        })
    )?;
    step!("put repo", server.put_repo("o", "r"))?;
    let src = tempfile::tempdir()?;
    git_in(src.path(), &["init", "-q", "-b", "main"])?;
    git_in(src.path(), &["config", "user.email", "t@t"])?;
    git_in(src.path(), &["config", "user.name", "Tester"])?;
    std::fs::write(src.path().join("a.txt"), "one\n")?;
    git_in(src.path(), &["add", "."])?;
    git_in(src.path(), &["commit", "-q", "-m", "one"])?;
    git(
        &["push", "-q", &server.repo_url("o", "r"), "main"],
        src.path(),
    )?;
    let id = walgit_git::RepoId::new("o", "r")?;
    let h = step!("open", server.state.registry.open(&id))?;
    step!("sync", h.sync())?;
    // Push packs (gix ingest) carry no .rev and need none: below
    // REV_INDEX_MIN_OBJECTS the maintainer leaves them alone.
    assert!(
        h.manifest().packs.iter().all(|p| !p.has_rev),
        "{:?}",
        h.manifest().packs
    );
    assert_eq!(
        step!("idle (small packs)", next_unit(&server.state, &id))?,
        Unit::Idle
    );

    // A legacy-shaped pack: pack-objects to a file with reverse indexes off (no .rev).
    let legacy = tempfile::tempdir()?;
    let tree = git_in(src.path(), &["rev-parse", "HEAD^{tree}"])?
        .trim()
        .to_string();
    let out = std::process::Command::new("git")
        .current_dir(src.path())
        .args([
            "-c",
            "pack.writeReverseIndex=false",
            "pack-objects",
            &format!("{}/pack", legacy.path().display()),
        ])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .and_then(|mut c| {
            use std::io::Write;
            c.stdin
                .take()
                .unwrap()
                .write_all(format!("{tree}\n").as_bytes())?;
            c.wait_with_output()
        })?;
    let sha = String::from_utf8_lossy(&out.stdout).trim().to_string();
    assert!(!legacy.path().join(format!("pack-{sha}.rev")).exists());
    step!(
        "add legacy pack",
        h.add_pack(
            &legacy.path().join(format!("pack-{sha}.pack")),
            &legacy.path().join(format!("pack-{sha}.idx")),
            0,
            None
        )
    )?;
    step!("sync2", h.sync())?;
    assert!(
        h.manifest()
            .packs
            .iter()
            .any(|p| p.checksum == sha && !p.has_rev),
        "{:?}",
        h.manifest().packs
    );

    // Another host installs the pack as it is (no .rev) before the unit runs.
    let other = step!(
        "start other",
        server.start_sibling_with(|c| {
            c.server.roles = vec![walgit_config::Role::Serve];
        })
    )?;
    let h2 = step!("open other", other.state.registry.open(&id))?;
    step!("sync other", h2.sync())?;
    let rev2 = h2
        .local()
        .pack_path(&gix_hash::ObjectId::from_hex(sha.as_bytes())?)
        .with_extension("rev");
    assert!(!rev2.exists());

    // The unit (what the planner would emit for a ≥ REV_INDEX_MIN_OBJECTS pack):
    // build locally, upload the side-file, CAS the manifest.
    let mut params = std::collections::HashMap::new();
    params.insert("pack".to_string(), sha.clone());
    let task = walgit_server::ops::start(server.state.clone(), id.clone(), "rev-index", params)
        .await
        .map_err(|_| anyhow::anyhow!("rev-index op did not start"))?;
    assert!(task.wait_done(std::time::Duration::from_mins(1)).await);
    assert!(
        matches!(task.outcome(), Some(Ok(_))),
        "{:?}",
        task.outcome()
    );
    assert!(
        h.local()
            .pack_path(&gix_hash::ObjectId::from_hex(sha.as_bytes())?)
            .with_extension("rev")
            .exists()
    );
    step!("sync3", h.sync())?;
    let p = h
        .manifest()
        .packs
        .iter()
        .find(|p| p.checksum == sha)
        .cloned()
        .unwrap();
    assert!(p.has_rev, "advertised in the manifest: {p:?}");
    assert!(
        walgit_store::ObjectStore::head(h.store(), &walgit_proto::keys::rev_key(&sha))
            .await?
            .is_some(),
        "uploaded as the side-file"
    );
    assert_eq!(step!("idle", next_unit(&server.state, &id))?, Unit::Idle);

    // The other host, pack already installed, picks the side-file up on its
    // next sync (the manifest revision moved) — the fleet converges.
    step!("sync other 2", h2.sync())?;
    assert!(
        rev2.exists(),
        "installed pack gets the newly advertised side-file on sync"
    );
    assert_eq!(
        std::fs::read(&rev2)?,
        std::fs::read(
            h.local()
                .pack_path(&gix_hash::ObjectId::from_hex(sha.as_bytes())?)
                .with_extension("rev")
        )?
    );
    Ok(())
}

/// An incremental slot whose tip set equals the newest built incremental of
/// the strategy on the same base is `skipped (unchanged since <id>)` — recorded
/// like too-small, never cut. Without it an idle night cuts 23–48 identical
/// 315 MB hourlies on a large repository (2026-08-21 08:00/09:00/10:00, same tip, no push
/// since 06:43Z): `min_commits` counts since the BASE, not since the previous
/// incremental. Clients are unaffected (git stops at the first bundle whose
/// prerequisites it has).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn identical_incremental_slots_are_skipped_as_unchanged() -> anyhow::Result<()> {
    let server = step!(
        "start",
        Server::start_with_tweak(|c| {
            c.server.roles = vec![walgit_config::Role::Serve, walgit_config::Role::Maintain];
            c.bundles.enabled = true;
            c.bundles.main_only = true;
            c.bundles.min_commits = 1;
            c.compaction.enabled = false;
            c.maintenance.checkpoints = false;
            c.maintenance.fsck_interval = std::time::Duration::ZERO;
            c.bundles.strategy.retain(|s| s.name != "daily");
            for s in &mut c.bundles.strategy {
                if s.name == "hourly" {
                    s.base = Some("weekly".into());
                    s.backfill_max = 0;
                }
            }
        })
    )?;
    step!("put repo", server.put_repo("o", "r"))?;
    let src = tempfile::tempdir()?;
    git_in(src.path(), &["init", "-q", "-b", "main"])?;
    git_in(src.path(), &["config", "user.email", "t@t"])?;
    git_in(src.path(), &["config", "user.name", "Tester"])?;
    std::fs::write(src.path().join("f.txt"), "one\n")?;
    git_in(src.path(), &["add", "."])?;
    git_in(src.path(), &["commit", "-q", "-m", "one"])?;
    let c1 = git_in(src.path(), &["rev-parse", "HEAD"])?
        .trim()
        .to_string();
    std::fs::write(src.path().join("g.txt"), "two\n")?;
    git_in(src.path(), &["add", "."])?;
    git_in(src.path(), &["commit", "-q", "-m", "two"])?;
    let c2 = git_in(src.path(), &["rev-parse", "HEAD"])?
        .trim()
        .to_string();
    let id = walgit_git::RepoId::new("o", "r")?;
    let h = step!("open", server.state.registry.open(&id))?;
    let now = std::time::SystemTime::now();
    let hour = std::time::Duration::from_hours(1);
    // History with explicit times: c1 ten days ago (so a weekly slot with state
    // exists — a full with no state is cut from now), c2 six hours ago, nothing since.
    let pack_of = |revs: &str| -> anyhow::Result<Vec<u8>> {
        let out = std::process::Command::new("sh")
            .arg("-c")
            .arg(format!(
                "git rev-list --objects {revs} | git pack-objects --stdout"
            ))
            .current_dir(src.path())
            .output()?;
        Ok(out.stdout)
    };
    let txn = |name: &str, old: &str, new: &str| walgit_proto::v1::RefTransaction {
        updates: vec![walgit_proto::v1::RefUpdate {
            name: name.into(),
            old_oid: old.into(),
            new_oid: new.into(),
            ..Default::default()
        }],
        ..Default::default()
    };
    let ingest = |bytes: Vec<u8>| async {
        h.local()
            .ingest_pack(
                std::io::Cursor::new(bytes),
                walgit_git::IngestOptions {
                    fsck: false,
                    max_bytes: None,
                    thin: false,
                },
            )
            .await
            .unwrap()
            .unwrap()
    };
    let p1 = ingest(pack_of(&c1)?).await;
    step!(
        "c1 ten days ago",
        h.publish_push_at(
            Some(p1),
            txn("refs/heads/main", "", &c1),
            HashMap::default(),
            now - 240 * hour
        )
    )?;
    let p2 = ingest(pack_of(&format!("{c2} ^{c1}"))?).await;
    step!(
        "c2 six hours ago",
        h.publish_push_at(
            Some(p2),
            txn("refs/heads/main", &c1, &c2),
            HashMap::default(),
            now - 6 * hour
        )
    )?;
    step!("sync", h.sync())?;

    // Weekly at the last Sunday before c2 (state as of then: c1).
    let weekly = server
        .state
        .cfg
        .bundles
        .strategy
        .iter()
        .find(|s| s.name == "weekly")
        .unwrap()
        .clone();
    let sunday = walgit_bundle::slots::last_slot_at_or_before(&weekly, now - 48 * hour)?.unwrap();
    let mut params = std::collections::HashMap::new();
    params.insert("strategy".to_string(), "weekly".to_string());
    params.insert("slot".to_string(), sunday.to_string());
    let t = walgit_server::ops::start(server.state.clone(), id.clone(), "bundle", params)
        .await
        .map_err(|_| anyhow::anyhow!("op start"))?;
    assert!(t.wait_done(std::time::Duration::from_secs(30)).await);

    // Passes until idle: exactly ONE hourly (the slot that first sees c2); every
    // later closed slot is recorded `unchanged since <that hourly>`.
    for _ in 0..8 {
        let report = step!("pass", walgit_server::maintain::run_pass(&server.state))?;
        if report.units == 0 {
            break;
        }
    }
    let list = walgit_bundle::ops::read_list(h.store())
        .await?
        .expect("list");
    let hourlies: Vec<_> = list
        .bundles
        .iter()
        .filter(|b| b.strategy == "hourly")
        .collect();
    let hourly = server
        .state
        .cfg
        .bundles
        .strategy
        .iter()
        .find(|s| s.name == "hourly")
        .unwrap()
        .clone();
    let ctx = walgit_bundle::slots::PlanContext {
        first_state: h.first_state_time(),
        can_full: true,
        can_incremental: true,
        wrong_host_reason: None,
    };
    let rows = server
        .state
        .bundles
        .plan(&id, std::time::SystemTime::now(), ctx)
        .await?;
    let dbg: Vec<_> = rows
        .iter()
        .filter(|r| r.strategy == "hourly")
        .map(|r| (r.slot, format!("{:?}", r.status)))
        .collect();
    assert_eq!(
        hourlies.len(),
        1,
        "one hourly carries c2; the identical later slots are not cut: {:?}\nbundles={:?}\nskipped={:?}\nplan={dbg:#?}",
        hourlies.iter().map(|b| (&b.id, b.slot)).collect::<Vec<_>>(),
        list.bundles
            .iter()
            .map(|b| (&b.id, b.slot))
            .collect::<Vec<_>>(),
        list.skipped
            .iter()
            .map(|s| (s.slot, &s.reason))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        hourlies[0]
            .tips
            .iter()
            .map(|t| t.oid.as_str())
            .collect::<Vec<_>>(),
        vec![c2.as_str()]
    );
    let unchanged: Vec<_> = list
        .skipped
        .iter()
        .filter(|s| s.reason.starts_with("unchanged since "))
        .collect();
    // With only the two newest slots ever planned (D21, 2026-08-22) that is the one slot after it —
    // settled as unchanged once it is *closed* (120 s past its fire time); in the first two minutes of
    // an hour it is still open and legitimately not recorded yet.
    let other_slot = rows
        .iter()
        .filter(|r| r.strategy == "hourly" && r.slot != hourlies[0].slot)
        .map(|r| r.slot)
        .max();
    let other_closed = other_slot.is_some_and(|s| {
        walgit_bundle::slots::slot_closed(&hourly, s, std::time::SystemTime::now())
    });
    assert!(
        !other_closed || !unchanged.is_empty(),
        "the closed hour after it is skipped as unchanged: {:?}",
        list.skipped
            .iter()
            .map(|s| (s.slot, &s.reason))
            .collect::<Vec<_>>()
    );
    assert!(
        unchanged.iter().all(
            |s| s.reason == format!("unchanged since {}", hourlies[0].id)
                && s.slot > hourlies[0].slot
        ),
        "{unchanged:?}"
    );
    // And the plan shows them so — nothing is re-measured.
    let closed_missing = rows
        .iter()
        .filter(|r| {
            r.strategy == "hourly"
                && r.status == walgit_bundle::slots::SlotStatus::Missing
                && walgit_bundle::slots::slot_closed(&hourly, r.slot, std::time::SystemTime::now())
        })
        .count();
    assert_eq!(closed_missing, 0, "{rows:?}");
    Ok(())
}

/// The blobless bundle family (`filter = "blob:none"` strategies): the weekly
/// "history" bundle is the D18 history pack composed under a `@filter=blob:none`
/// header, incrementals pack with `--filter=blob:none`; they are advertised ONLY
/// at `bundles/list?filter=blob:none` (git does not match `bundle.<id>.filter`
/// against the clone's filter — a full clone would swallow them). A
/// `--filter=blob:none --bundle-uri=<that list>` clone seeds from them and
/// fetches blobs lazily; a full clone with the protocol list never sees them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn blobless_bundle_family_is_composed_from_the_history_pack_and_served_on_its_own_list()
-> anyhow::Result<()> {
    use walgit_server::maintain::{Unit, next_unit, run_pass};
    let server = step!(
        "start",
        Server::start_with_tweak(|c| {
            c.server.roles = vec![
                walgit_config::Role::Serve,
                walgit_config::Role::Maintain,
                walgit_config::Role::Compact,
                walgit_config::Role::Bundle,
            ];
            c.maintenance.disk = walgit_config::MaintainerDisk::Ssd;
            c.cache.mode = walgit_config::CacheMode::Disk;
            c.bundles.enabled = true;
            c.bundles.min_commits = 1;
            c.bundles.strategy.truncate(1); // weekly (full, unfiltered)
            let weekly = c.bundles.strategy[0].clone();
            c.bundles.strategy.push(walgit_config::BundleStrategy {
                name: "weekly-history".into(),
                filter: Some("blob:none".into()),
                ..weekly.clone()
            });
            c.bundles.strategy.push(walgit_config::BundleStrategy {
                name: "hourly-history".into(),
                kind: walgit_config::BundleKind::Incremental,
                base: Some("weekly-history".into()),
                schedule: "0 0 * * * *".into(),
                keep: 0,
                filter: Some("blob:none".into()),
                backfill_max: 0,
                ..weekly
            });
            c.compaction.enabled = true;
            c.maintenance.checkpoints = false;
            c.maintenance.fsck_interval = std::time::Duration::ZERO;
        })
    )?;
    step!("put repo", server.put_repo("o", "r"))?;
    let src = tempfile::tempdir()?;
    git_in(src.path(), &["init", "-q", "-b", "main"])?;
    git_in(src.path(), &["config", "user.email", "t@t"])?;
    git_in(src.path(), &["config", "user.name", "Tester"])?;
    std::fs::write(src.path().join("f.txt"), "one\n")?;
    git_in(src.path(), &["add", "."])?;
    git_in(src.path(), &["commit", "-q", "-m", "one"])?;
    // A large repository's shape: an imported tier-2 base + ref snapshot, then a push.
    let id = walgit_git::RepoId::new("o", "r")?;
    let h = step!("open", server.state.registry.open(&id))?;
    let c1 = git_in(src.path(), &["rev-parse", "HEAD"])?
        .trim()
        .to_string();
    let packs = tempfile::tempdir()?;
    let out = std::process::Command::new("git")
        .current_dir(src.path())
        .args([
            "pack-objects",
            "--revs",
            &format!("{}/pack", packs.path().display()),
        ])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .and_then(|mut c| {
            use std::io::Write;
            c.stdin
                .take()
                .unwrap()
                .write_all(format!("{c1}\n").as_bytes())?;
            c.wait_with_output()
        })?;
    let sha = String::from_utf8_lossy(&out.stdout).trim().to_string();
    step!("sync0", h.sync())?;
    step!(
        "import base",
        h.add_pack(
            &packs.path().join(format!("pack-{sha}.pack")),
            &packs.path().join(format!("pack-{sha}.idx")),
            2,
            None
        )
    )?;
    let txn = walgit_proto::v1::RefTransaction {
        updates: vec![walgit_proto::v1::RefUpdate {
            name: "refs/heads/main".into(),
            old_oid: String::new(),
            new_oid: c1.clone(),
            ..Default::default()
        }],
        ..Default::default()
    };
    step!(
        "import refs",
        h.publish_push_synced(None, txn, HashMap::default())
    )?;
    std::fs::write(src.path().join("f2.txt"), "one and a half\n")?;
    git_in(src.path(), &["add", "."])?;
    git_in(src.path(), &["commit", "-q", "-m", "one.5"])?;
    git(
        &["push", "-q", &server.repo_url("o", "r"), "main"],
        src.path(),
    )?;
    step!("sync", h.sync())?;

    // Passes until idle: base rebuild (base + D18 history pack), weekly compose,
    // weekly-history compose (of the history pack).
    for _ in 0..12 {
        let u = next_unit(&server.state, &id).await?;
        if u == Unit::Idle {
            break;
        }
        let _ = step!("pass", run_pass(&server.state))?;
    }
    step!("sync2", h.sync())?;
    let m = h.manifest();
    let base = walgit_wal::base_pack(&m).expect("base").clone();
    let hist = m
        .packs
        .iter()
        .find(|p| {
            p.kind == walgit_proto::v1::PackKind::History as i32 && p.derived_from == base.checksum
        })
        .expect("history pack")
        .clone();
    let list = walgit_bundle::ops::read_list(h.store())
        .await?
        .expect("list");
    let weekly = list
        .bundles
        .iter()
        .find(|b| b.strategy == "weekly")
        .expect("weekly");
    let weekly_h = list
        .bundles
        .iter()
        .find(|b| b.strategy == "weekly-history")
        .expect("weekly-history");
    assert_eq!(weekly.filter, "");
    assert_eq!(weekly_h.filter, "blob:none");
    assert!(
        weekly.size > base.pack_size && weekly.size < base.pack_size + 4096,
        "weekly = header ∘ base"
    );
    assert!(
        weekly_h.size > hist.pack_size && weekly_h.size < hist.pack_size + 4096,
        "weekly-history = header ∘ history pack: {} vs {}",
        weekly_h.size,
        hist.pack_size
    );
    // Header: v3 with the filter capability.
    let head = server
        .get_text(
            &format!("/o/r.git/{}", weekly_h.key),
            &[("Range", "bytes=0-63")],
        )
        .await?;
    assert!(
        head.starts_with("# v3 git bundle\n@filter=blob:none\n"),
        "{head:?}"
    );

    // A blobless incremental on it ("now" build): new commit with a new blob.
    std::fs::write(
        src.path().join("g.txt"),
        "two — a blob the incremental must NOT carry\n",
    )?;
    git_in(src.path(), &["add", "."])?;
    git_in(src.path(), &["commit", "-q", "-m", "two"])?;
    git(
        &["push", "-q", &server.repo_url("o", "r"), "main"],
        src.path(),
    )?;
    step!("sync3", h.sync())?;
    let inc = step!(
        "hourly-history build",
        server.state.bundles.build(&id, "hourly-history")
    )?;
    assert_eq!(
        (inc.kind.as_str(), inc.filter.as_str(), inc.base_id.as_str()),
        ("incremental", "blob:none", weekly_h.id.as_str())
    );
    let head = server
        .get_text(&format!("/o/r.git/{}", inc.key), &[("Range", "bytes=0-63")])
        .await?;
    assert!(
        head.starts_with("# v3 git bundle\n@filter=blob:none\n"),
        "{head:?}"
    );

    // Two lists, one family each.
    let plain = server.get_text("/o/r.git/bundles/list", &[]).await?;
    let blobless = server
        .get_text("/o/r.git/bundles/list?filter=blob:none", &[])
        .await?;
    assert!(
        plain.contains("[bundle \"weekly-")
            && !plain.contains("history")
            && !plain.contains("filter ="),
        "{plain}"
    );
    assert!(
        blobless.contains("[bundle \"weekly-history-")
            && blobless.contains("[bundle \"hourly-history-")
            && !blobless.contains("[bundle \"weekly-1"),
        "{blobless}"
    );
    assert_eq!(
        blobless.matches("    filter = blob:none\n").count(),
        2,
        "{blobless}"
    );
    let v2 = server
        .state
        .bundles
        .protocol_v2_lines(&id, &server.base_url)
        .await?;
    assert!(
        v2.iter().any(|l| l.starts_with("bundle.weekly-1")
            || l.starts_with("bundle.weekly-") && !l.contains("history"))
            && !v2.iter().any(|l| l.contains("history")),
        "{v2:?}"
    );

    // A blobless clone seeded from the blobless list: promisor packs from the
    // bundles, blobs missing until checkout fetches them lazily.
    let tmp = tempfile::tempdir()?;
    let c = tmp.path().join("blobless");
    git(
        &[
            "clone",
            "-q",
            "--filter=blob:none",
            "--no-checkout",
            &format!(
                "--bundle-uri={}/o/r.git/bundles/list?filter=blob:none",
                server.base_url
            ),
            &server.repo_url("o", "r"),
            c.to_str().unwrap(),
        ],
        tmp.path(),
    )?;
    let promisors = std::fs::read_dir(c.join(".git/objects/pack"))?
        .filter(|e| {
            e.as_ref()
                .unwrap()
                .path()
                .extension()
                .is_some_and(|x| x == "promisor")
        })
        .count();
    assert!(
        promisors >= 2,
        "bundle packs unbundled as promisor packs: {promisors}"
    );
    let missing = git_in(&c, &["rev-list", "--objects", "--all", "--missing=print"])?;
    assert!(
        missing.lines().any(|l| l.starts_with('?')),
        "blobs are NOT in the blobless bundles:\n{missing}"
    );
    git_in(&c, &["checkout", "-q", "main"])?; // lazy blob fetch from the server
    assert_eq!(
        std::fs::read_to_string(c.join("g.txt"))?,
        "two — a blob the incremental must NOT carry\n"
    );
    git_in(&c, &["fsck", "--connectivity-only"])?;

    // A full clone with bundle-uri via the protocol never sees the family.
    let f = tmp.path().join("full");
    git(
        &[
            "-c",
            "transfer.bundleURI=true",
            "clone",
            "-q",
            &server.repo_url("o", "r"),
            f.to_str().unwrap(),
        ],
        tmp.path(),
    )?;
    let promisors = std::fs::read_dir(f.join(".git/objects/pack"))?
        .filter(|e| {
            e.as_ref()
                .unwrap()
                .path()
                .extension()
                .is_some_and(|x| x == "promisor")
        })
        .count();
    assert_eq!(promisors, 0, "no promisor pack in a full clone");
    let missing = git_in(&f, &["rev-list", "--objects", "--all", "--missing=print"])?;
    assert!(!missing.lines().any(|l| l.starts_with('?')), "{missing}");
    git_in(&f, &["fsck"])?;
    Ok(())
}

/// D21 (2026-08-22): the list lists `keep` fulls and the two newest incrementals per
/// strategy — and the maintainer brings an existing list to that shape on its next pass,
/// deleting the pruned objects, even when the repository is idle and publishes nothing
/// (acme/walgit sat at 1 weekly + 3 dailies + 39 hourlies: 43 downloads
/// per fresh clone).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn maintainer_pass_brings_an_overgrown_bundle_list_to_retention() -> anyhow::Result<()> {
    use prost::Message;
    use walgit_store::{ObjectStore, ObjectStoreExt, PutMode};
    let server = step!(
        "start",
        Server::start_with_tweak(|c| {
            c.server.roles = vec![walgit_config::Role::Serve, walgit_config::Role::Maintain];
            c.bundles.enabled = true;
            // The D21 shape this test pins (the default chains the dailies since 2026-08-22).
            for s in &mut c.bundles.strategy {
                s.chain = false;
            }
        })
    )?;
    step!("put repo", server.put_repo("o", "r"))?;
    let id = walgit_git::RepoId::new("o", "r")?;
    let store =
        walgit_store::Prefixed::new(server.state.registry.store().clone(), id.store_prefix());
    // Seed: one weekly, three dailies on it, 13 hourlies on each daily (objects = dummy bytes).
    let mut list = walgit_proto::v1::BundleList {
        mode: "all".into(),
        heuristic: "creationToken".into(),
        ..Default::default()
    };
    let entry =
        |strategy: &str, kind: &str, slot: u64, base_id: &str| walgit_proto::v1::BundleEntry {
            id: format!("{strategy}-{slot}"),
            key: format!("bundles/{strategy}/{slot}.bundle"),
            strategy: strategy.into(),
            kind: kind.into(),
            creation_token: slot,
            slot,
            base_id: base_id.into(),
            ..Default::default()
        };
    let w0 = 1_787_000_400u64; // a Sunday 23:00-ish epoch; only ordering matters here
    list.bundles.push(entry("weekly", "full", w0, ""));
    for d in 1..=3u64 {
        let ds = w0 + d * 86_400;
        list.bundles
            .push(entry("daily", "incremental", ds, &format!("weekly-{w0}")));
        for h in 1..=13u64 {
            list.bundles.push(entry(
                "hourly",
                "incremental",
                ds + h * 3600,
                &format!("daily-{ds}"),
            ));
        }
    }
    assert_eq!(list.bundles.len(), 43);
    for b in &list.bundles {
        step!(
            "seed object",
            store.put_bytes(&b.key, b"bundle".as_slice(), PutMode::Create)
        )?;
    }
    step!(
        "seed list",
        store.put_bytes(
            walgit_proto::keys::BUNDLE_LIST,
            list.encode_to_vec(),
            PutMode::Create
        )
    )?;
    let text = step!("list before", server.get_text("/o/r.git/bundles/list", &[]))?;
    assert_eq!(text.matches("uri = ").count(), 43, "{text}");

    // One maintainer pass (whatever unit it picks) applies retention first.
    let _ = step!("pass", walgit_server::maintain::run_pass(&server.state))?;
    let after = walgit_bundle::ops::read_list(&store).await?.unwrap();
    let ids: Vec<&str> = after.bundles.iter().map(|b| b.id.as_str()).collect();
    assert_eq!(after.bundles.len(), 5, "{ids:?}");
    let d2 = w0 + 2 * 86_400;
    let d3 = w0 + 3 * 86_400;
    for want in [
        format!("weekly-{w0}"),
        format!("daily-{d2}"),
        format!("daily-{d3}"),
        format!("hourly-{}", d3 + 12 * 3600),
        format!("hourly-{}", d3 + 13 * 3600),
    ] {
        assert!(ids.contains(&want.as_str()), "{want} missing from {ids:?}");
    }
    // Pruned objects are gone, kept ones stay.
    assert!(
        step!(
            "pruned gone",
            store.head(&format!("bundles/hourly/{}.bundle", d2 + 3600))
        )?
        .is_none()
    );
    assert!(
        step!(
            "kept stays",
            store.head(&format!("bundles/hourly/{}.bundle", d3 + 13 * 3600))
        )?
        .is_some()
    );
    // Idempotent: a second pass changes nothing.
    let _ = step!("pass 2", walgit_server::maintain::run_pass(&server.state))?;
    let again = walgit_bundle::ops::read_list(&store).await?.unwrap();
    assert_eq!(again.bundles.len(), 5);
    Ok(())
}
