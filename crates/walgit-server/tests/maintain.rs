//! The `maintain` role's pass: checkpoint-if-due (refs-level, on an instance
//! that cannot hold the packs), compaction, all as tasks.

mod harness;

use harness::{Server, git, git_in};
use std::collections::HashMap;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn classification_preserves_push_bytes_and_shared_group_cuts_settle() -> anyhow::Result<()> {
    use walgit_server::ops::{CompactRequest, compact_repo};
    let server = Server::start_with_tweak(|c| {
        c.cache.mode = walgit_config::CacheMode::Disk;
        c.packs.frozen_coverage_target = 0.0;
    })
    .await?;
    server.put_repo("o", "shared").await?;
    let src = tempfile::tempdir()?;
    git_in(src.path(), &["init", "-q", "-b", "main"])?;
    git_in(
        src.path(),
        &["config", "user.email", "test@example.invalid"],
    )?;
    git_in(src.path(), &["config", "user.name", "Test"])?;
    std::fs::write(src.path().join("shared.txt"), "shared bytes\n")?;
    git_in(src.path(), &["add", "."])?;
    git_in(src.path(), &["commit", "-q", "-m", "code"])?;
    git(
        &["push", "-q", &server.repo_url("o", "shared"), "main"],
        src.path(),
    )?;
    let handle = server
        .state
        .registry
        .open(&walgit_git::RepoId::new("o", "shared")?)
        .await?;
    let before = handle.manifest().packs.clone();
    compact_repo(&handle, CompactRequest::default(), &|_| {}).await?;
    let after = handle.manifest();
    assert_eq!(before.len(), after.packs.len());
    for (old, new) in before.iter().zip(&after.packs) {
        assert_eq!(
            (&old.checksum, old.pack_size, old.tier),
            (&new.checksum, new.pack_size, new.tier)
        );
        assert_eq!(new.pack_groups, ["code"]);
    }
    // A distinct metadata commit shares the complete tree/blob with code.
    git_in(src.path(), &["checkout", "-q", "--orphan", "meta"])?;
    git_in(src.path(), &["commit", "-q", "-m", "metadata"])?;
    let meta = git_in(src.path(), &["rev-parse", "HEAD"])?;
    git(
        &[
            "push",
            "-q",
            &server.repo_url("o", "shared"),
            "HEAD:refs/meta/state",
        ],
        src.path(),
    )?;
    compact_repo(
        &handle,
        CompactRequest {
            force: true,
            rebuild_base: true,
        },
        &|_| {},
    )
    .await?;
    // Repair certificates after the conserving cut; it must not recut shared bytes.
    compact_repo(&handle, CompactRequest::default(), &|_| {}).await?;
    let stable = handle.manifest();
    assert!(
        stable
            .packs
            .iter()
            .any(|p| p.pack_groups.contains(&"code".into())
                && p.pack_groups.contains(&"meta".into()))
    );
    for _ in 0..3 {
        compact_repo(&handle, CompactRequest::default(), &|_| {}).await?;
        assert_eq!(handle.manifest().packs, stable.packs);
    }
    let cold = server.start_sibling_with(|_| {}).await?;
    let clone = tempfile::tempdir()?;
    git(
        &[
            "clone",
            "-q",
            "--mirror",
            "--server-option=ref-view=all",
            &cold.repo_url("o", "shared"),
            clone.path().to_str().unwrap(),
        ],
        src.path(),
    )?;
    assert_eq!(
        git_in(clone.path(), &["rev-parse", "refs/meta/state"])?.trim(),
        meta.trim()
    );
    git_in(clone.path(), &["fsck", "--strict"])?;
    Ok(())
}

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
            c.packs.enabled = false;
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

    // An object-capable maintainer audits the checkpointed repository once.
    let auditor = step!(
        "start auditor",
        front.start_sibling_with(|c| {
            c.server.roles = vec![walgit_config::Role::Maintain];
            c.wal.snapshot_every_entries = 0;
            c.packs.enabled = false;
        })
    )?;
    let id = walgit_git::RepoId::new("o", "r")?;
    assert!(matches!(
        step!("audit due", next_unit(&auditor.state, &id))?,
        Unit::Fsck(_)
    ));
    let report = step!("audit pass", run_pass(&auditor.state))?;
    assert_eq!(report.units, 1, "{report:?}");
    assert_eq!(
        step!("unit idle", next_unit(&auditor.state, &id))?,
        Unit::Idle
    );
    let report = step!("idempotent pass", run_pass(&auditor.state))?;
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
            c.packs.enabled = false;
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

    // Object-info is object work too and must refuse placement before sync.
    let mut body = Vec::new();
    walgit_server::pktline::encode_text(&mut body, "command=object-info\n");
    walgit_server::pktline::encode_delim(&mut body);
    walgit_server::pktline::encode_text(&mut body, "size\n");
    walgit_server::pktline::encode_text(
        &mut body,
        "oid 0000000000000000000000000000000000000000\n",
    );
    walgit_server::pktline::encode_flush(&mut body);
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

/// Manual base rebuilding conserves the imported base and subsequent pushes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn manual_base_rebuild_on_an_ssd_maintainer() -> anyhow::Result<()> {
    let server = step!(
        "start",
        Server::start_with_tweak(|c| {
            c.server.roles = vec![
                walgit_config::Role::Serve,
                walgit_config::Role::Maintain,
                walgit_config::Role::Compact,
            ];
            c.maintenance.disk = walgit_config::MaintainerDisk::Ssd;
            c.cache.mode = walgit_config::CacheMode::Disk;
            c.packs.enabled = true;
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

    let outcome = step!(
        "manual rebuild",
        walgit_server::ops::compact_repo(
            &h,
            walgit_server::ops::CompactRequest {
                force: true,
                rebuild_base: true
            },
            &|_| {},
        )
    )?;
    assert!(
        matches!(
            outcome,
            walgit_server::ops::CompactOutcome::Published {
                rebuild_base: true,
                ..
            }
        ),
        "{outcome:?}"
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

    // A push after the rebuild remains readable without another rebuild.
    std::fs::write(src.path().join("h.txt"), "three\n")?;
    git_in(src.path(), &["add", "."])?;
    git_in(src.path(), &["commit", "-q", "-m", "three"])?;
    git(
        &["push", "-q", &server.repo_url("o", "r"), "main"],
        src.path(),
    )?;
    step!("sync after push 2", h.sync())?;
    // Additional imported tier-2 packs remain represented in the inventory.
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
    let clone = tempfile::tempdir()?;
    git(
        &[
            "clone",
            "-q",
            &server.repo_url("o", "r"),
            clone.path().to_str().unwrap(),
        ],
        clone.path().parent().unwrap(),
    )?;
    git_in(clone.path(), &["fsck", "--strict"])?;
    assert_eq!(
        git_in(clone.path(), &["rev-parse", "HEAD"])?,
        git_in(src.path(), &["rev-parse", "HEAD"])?
    );
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
            c.packs.enabled = false;
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

/// Manual force and full-cut requests obey committed repository disablement.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn manual_pack_work_cannot_override_saved_disablement() -> anyhow::Result<()> {
    let server = Server::start_with_tweak(|cfg| cfg.packs.enabled = true).await?;
    server.put_repo("o", "disabled").await?;
    let id = walgit_git::RepoId::new("o", "disabled")?;
    let handle = server.state.registry.open(&id).await?;
    handle
        .publish_settings("[packs]\nenabled = false\n", "admin", "pause maintenance")
        .await?;
    let before = handle.manifest();
    for rebuild_base in [false, true] {
        let error = walgit_server::ops::compact_repo(
            &handle,
            walgit_server::ops::CompactRequest {
                force: true,
                rebuild_base,
            },
            &walgit_server::ops::noop_log,
        )
        .await
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("disabled by the effective repository settings"),
            "{error:#}"
        );
    }
    assert_eq!(
        handle.manifest(),
        before,
        "disabled requests changed committed state"
    );
    Ok(())
}
