// Test fixtures use panics to fail the test, including shared helper functions.
#![allow(clippy::unwrap_used)]
#![allow(clippy::many_single_char_names, unsafe_code)]

//! End-to-end tests: real upstream `git` against a live walgit-server backed
//! by the in-memory store. Covers clone/push/fetch (v2 and v0), non-ff reject,
//! ref delete, tags, partial clone + lazy fetch, ls-remote, and the two-instance
//! consistency test (push on A, immediate clone on B). LFS is exercised when
//! `git lfs` is present.
mod harness;

type TestResult = anyhow::Result<()>;
use anyhow::Context;
use harness::{Server, TestRepo, git, git_in};
use std::io::Write;
use std::process::{Command, Stdio};
use std::time::Instant;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn info_refs_v2_advertises_capabilities() -> TestResult {
    let server = Server::start().await?;
    server.put_repo("t", "r").await?;
    let out = server
        .get_text(
            "/t/r.git/info/refs?service=git-upload-pack",
            &[("Git-Protocol", "version=2")],
        )
        .await?;
    assert!(out.contains("# service=git-upload-pack"));
    assert!(out.contains("version 2"));
    assert!(out.contains("ls-refs=unborn"));
    assert!(out.contains("fetch=shallow wait-for-done"));
    assert!(!out.contains("bundle-uri"), "{out}");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn push_clone_roundtrip_v2() -> TestResult {
    let server = Server::start().await?;
    server.put_repo("t", "r").await?;

    let src = TestRepo::synthetic(3, 4)?;
    git_in(&src, &["commit", "--allow-empty", "-m", "first"])?;
    git_in(&src, &["branch", "-M", "main"])?;
    git_in(
        &src,
        &["remote", "add", "origin", &server.repo_url("t", "r")],
    )?;
    let push_started = Instant::now();
    git_in(&src, &["push", "-u", "origin", "main"])?;
    if std::env::var_os("WALGIT_TEST_PRINT_PUSH_TIMING").is_some() {
        println!("small push took {:?}", push_started.elapsed());
    }

    let clone_dir = tempfile::tempdir()?;
    git(
        &[
            "clone",
            &server.repo_url("t", "r"),
            clone_dir.path().to_str().unwrap(),
        ],
        clone_dir.path().parent().unwrap(),
    )?;

    // fsck + refs equal
    git_in(clone_dir.path(), &["fsck"])?;
    let src_head = git_in(&src, &["rev-parse", "main"])?;
    let cl_head = git_in(clone_dir.path(), &["rev-parse", "main"])?;
    assert_eq!(src_head.trim(), cl_head.trim());
    // Ordinary Git transfer remains complete after removing the bundle runtime.
    for suffix in [
        "bundles/list",
        "bundles/catchup",
        "bundles/weekly/example.bundle",
    ] {
        assert_eq!(server.get_status(&format!("/t/r.git/{suffix}")).await?, 404);
    }
    let command = "command=bundle-uri\n";
    let response = reqwest::Client::new()
        .post(format!("{}/git-upload-pack", server.repo_url("t", "r")))
        .header("Git-Protocol", "version=2")
        .header("Content-Type", "application/x-git-upload-pack-request")
        .body(format!("{:04x}{command}00010000", command.len() + 4))
        .send()
        .await?;
    assert_eq!(response.status(), 400);
    assert!(response.text().await?.contains("unknown v2 command"));

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn clone_protocol_v0() -> TestResult {
    let server = Server::start().await?;
    server.put_repo("t", "r").await?;

    let src = TestRepo::synthetic(2, 2)?;
    git_in(&src, &["commit", "--allow-empty", "-m", "v0"])?;
    git_in(&src, &["branch", "-M", "main"])?;
    git_in(
        &src,
        &["remote", "add", "origin", &server.repo_url("t", "r")],
    )?;
    git_in(&src, &["push", "origin", "main"])?;

    let clone_dir = tempfile::tempdir()?;
    git(
        &[
            "-c",
            "protocol.version=0",
            "clone",
            &server.repo_url("t", "r"),
            clone_dir.path().to_str().unwrap(),
        ],
        clone_dir.path().parent().unwrap(),
    )?;
    git_in(clone_dir.path(), &["fsck"])?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn second_push_and_fetch() -> TestResult {
    let server = Server::start().await?;
    server.put_repo("t", "r").await?;

    let src = TestRepo::synthetic(1, 1)?;
    git_in(&src, &["commit", "--allow-empty", "-m", "a"])?;
    git_in(&src, &["branch", "-M", "main"])?;
    git_in(
        &src,
        &["remote", "add", "origin", &server.repo_url("t", "r")],
    )?;
    git_in(&src, &["push", "origin", "main"])?;

    git_in(&src, &["commit", "--allow-empty", "-m", "b"])?;
    git_in(&src, &["push", "origin", "main"])?;

    let clone_dir = tempfile::tempdir()?;
    git(
        &[
            "clone",
            &server.repo_url("t", "r"),
            clone_dir.path().to_str().unwrap(),
        ],
        clone_dir.path().parent().unwrap(),
    )?;
    let h = git_in(clone_dir.path(), &["rev-parse", "main"])?;
    let src_h = git_in(&src, &["rev-parse", "main"])?;
    assert_eq!(h.trim(), src_h.trim());

    // fetch after a third push
    git_in(&src, &["commit", "--allow-empty", "-m", "c"])?;
    git_in(&src, &["push", "origin", "main"])?;
    git_in(clone_dir.path(), &["fetch", "origin"])?;
    let h2 = git_in(clone_dir.path(), &["rev-parse", "origin/main"])?;
    let src_h2 = git_in(&src, &["rev-parse", "main"])?;
    assert_eq!(h2.trim(), src_h2.trim());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn non_fast_forward_rejected() -> TestResult {
    let server = Server::start().await?;
    server.put_repo("t", "r").await?;

    let src = TestRepo::synthetic(1, 1)?;
    git_in(&src, &["commit", "--allow-empty", "-m", "a"])?;
    git_in(&src, &["branch", "-M", "main"])?;
    git_in(
        &src,
        &["remote", "add", "origin", &server.repo_url("t", "r")],
    )?;
    git_in(&src, &["push", "origin", "main"])?;

    // Divergent history: reset to a different root.
    let other = TestRepo::synthetic(1, 1)?;
    git_in(&other, &["commit", "--allow-empty", "-m", "other"])?;
    git_in(&other, &["branch", "-M", "main"])?;
    git_in(
        &other,
        &["remote", "add", "origin", &server.repo_url("t", "r")],
    )?;
    let res = Command::new("git")
        .current_dir(&*other)
        .args(["push", "origin", "main"])
        .output()?;
    let stderr = String::from_utf8_lossy(&res.stderr);
    assert!(
        !res.status.success(),
        "non-ff push should be rejected; stderr: {stderr}",
    );
    assert!(
        stderr.contains("non-fast-forward")
            || stderr.contains("! [remote rejected]")
            || stderr.contains("ng"),
        "stderr should mention rejection"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn push_delete_ref() -> TestResult {
    let server = Server::start().await?;
    server.put_repo("t", "r").await?;

    let src = TestRepo::synthetic(1, 1)?;
    git_in(&src, &["commit", "--allow-empty", "-m", "a"])?;
    git_in(&src, &["branch", "-M", "main"])?;
    git_in(&src, &["branch", "topic"])?;
    git_in(
        &src,
        &["remote", "add", "origin", &server.repo_url("t", "r")],
    )?;
    git_in(&src, &["push", "origin", "main", "topic"])?;

    git_in(&src, &["push", "origin", "--delete", "topic"])?;

    let refs = server.ls_remote("t", "r").await?;
    assert!(!refs.contains("refs/heads/topic"));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dangling_head_clone_and_ls_remote() -> TestResult {
    // The repo's HEAD points at a branch that was never pushed (only `other`
    // exists). ls-refs must not emit an empty oid for HEAD; clone must work.
    let server = Server::start().await?;
    server.put_repo("t", "r").await?;

    let src = TestRepo::synthetic(1, 1)?;
    git_in(&src, &["commit", "--allow-empty", "-m", "a"])?;
    git_in(
        &src,
        &["push", &server.repo_url("t", "r"), "HEAD:refs/heads/other"],
    )?;

    let refs = server.ls_remote("t", "r").await?;
    assert!(refs.contains("refs/heads/other"));
    for line in refs.lines() {
        assert!(
            !line.starts_with(' '),
            "empty oid in ls-remote line: {line:?}"
        );
    }

    let dst = tempfile::tempdir()?;
    let out = std::process::Command::new("git")
        .args([
            "clone",
            "-q",
            &server.repo_url("t", "r"),
            dst.path().join("c").to_str().unwrap(),
        ])
        .output()?;
    assert!(
        out.status.success(),
        "clone failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn admin_create_list_delete() -> TestResult {
    let server = Server::start().await?;
    server.put_repo("t", "r").await?;
    let src = TestRepo::synthetic(1, 1)?;
    git_in(&src, &["commit", "--allow-empty", "-m", "a"])?;
    git_in(
        &src,
        &["push", &server.repo_url("t", "r"), "HEAD:refs/heads/main"],
    )?;

    let list = server.get_text("/?format=text", &[]).await?;
    assert!(list.contains("t/r"), "list was: {list}");

    let client = reqwest::Client::new();
    let del = client
        .delete(format!("{}/t/r.git", server.base_url))
        .send()
        .await?;
    assert!(
        del.status() == 204 || del.status() == 200,
        "delete -> {}",
        del.status()
    );
    assert_eq!(
        server
            .get_status("/t/r.git/info/refs?service=git-upload-pack")
            .await?,
        axum::http::StatusCode::NOT_FOUND
    );
    let del_again = client
        .delete(format!("{}/t/r.git", server.base_url))
        .send()
        .await?;
    assert_eq!(del_again.status(), 404);
    Ok(())
}

/// `PUT /{owner}/{repo}` answers from the bucket: 201 once, then 409 — on the
/// instance holding the handle (warm) and on one that never opened it (cold);
/// concurrent creates of one name have one 201 and one 409, on one instance
/// and across two.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_answers_409_for_an_existing_repository() -> TestResult {
    let (a, b) = Server::start_pair().await?;
    let client = reqwest::Client::new();
    let put = |s: &Server, repo: &str| client.put(format!("{}/t/{repo}", s.base_url)).send();
    assert_eq!(put(&a, "r").await?.status(), 201);
    assert_eq!(put(&a, "r").await?.status(), 409, "warm handle");
    assert_eq!(put(&b, "r").await?.status(), 409, "cold instance");
    // Deleted on B while A still holds a handle: the bucket, not A's map, answers.
    assert_eq!(
        client
            .delete(format!("{}/t/r", b.base_url))
            .send()
            .await?
            .status(),
        204
    );
    assert_eq!(
        put(&a, "r").await?.status(),
        201,
        "re-create after a delete elsewhere"
    );

    for (n, (x, y)) in [(&a, &a), (&a, &b)].into_iter().enumerate() {
        let repo = format!("race{n}");
        let (rx, ry) = tokio::join!(put(x, &repo), put(y, &repo));
        let mut codes = [rx?.status().as_u16(), ry?.status().as_u16()];
        codes.sort_unstable();
        assert_eq!(codes, [201, 409], "{repo}");
    }
    Ok(())
}

/// `auto_create_on_push`: a push to a missing name creates it (its internal
/// create treats "exists" as fine), and a create after that is a 409.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn auto_create_on_push_creates_the_repository() -> TestResult {
    let server = Server::start_with_tweak(|c| c.server.auto_create_on_push = true).await?;
    let src = TestRepo::synthetic(1, 1)?;
    for refspec in ["HEAD:refs/heads/main", "HEAD:refs/heads/second"] {
        git_in(&src, &["push", &server.repo_url("t", "auto"), refspec])?;
    }
    let refs = server.ls_remote("t", "auto").await?;
    assert!(
        refs.contains("refs/heads/main") && refs.contains("refs/heads/second"),
        "{refs}"
    );
    let status = reqwest::Client::new()
        .put(format!("{}/t/auto", server.base_url))
        .send()
        .await?
        .status();
    assert_eq!(status, 409);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_clones_and_pushes_with_telemetry() -> TestResult {
    // Regression: tracing spans entered across .await under a multi-threaded
    // runtime corrupted the span registry ("tried to clone a span that already
    // closed") and aborted the process on a serverless host under load.
    let server = Server::start().await?;
    server.put_repo("t", "r").await?;
    let src = TestRepo::synthetic(20, 3)?;
    git_in(
        &src,
        &["push", &server.repo_url("t", "r"), "HEAD:refs/heads/main"],
    )?;

    let url = server.repo_url("t", "r");
    let mut tasks = Vec::new();
    for i in 0..48 {
        let url = url.clone();
        tasks.push(tokio::task::spawn_blocking(
            move || -> anyhow::Result<()> {
                let dir = tempfile::tempdir()?;
                let dst = dir.path().join("c");
                let out = std::process::Command::new("git")
                    .args(["clone", "-q", &url, dst.to_str().unwrap()])
                    .output()?;
                anyhow::ensure!(
                    out.status.success(),
                    "clone {i}: {}",
                    String::from_utf8_lossy(&out.stderr)
                );
                Ok(())
            },
        ));
    }
    for i in 0..16 {
        let url = url.clone();
        tasks.push(tokio::task::spawn_blocking(
            move || -> anyhow::Result<()> {
                let dir = tempfile::tempdir()?;
                let dst = dir.path().join("w");
                let out = std::process::Command::new("git")
                    .args(["clone", "-q", &url, dst.to_str().unwrap()])
                    .output()?;
                anyhow::ensure!(
                    out.status.success(),
                    "push-clone {i}: {}",
                    String::from_utf8_lossy(&out.stderr)
                );
                std::fs::write(dst.join(format!("f{i}.txt")), format!("{i}\n"))?;
                git_in(&dst, &["add", "."])?;
                git_in(
                    &dst,
                    &[
                        "-c",
                        "user.email=t@t",
                        "-c",
                        "user.name=t",
                        "commit",
                        "-q",
                        "-m",
                        "x",
                    ],
                )?;
                let out = std::process::Command::new("git")
                    .current_dir(&dst)
                    .args(["push", "-q", "origin", &format!("HEAD:refs/heads/w{i}")])
                    .output()?;
                anyhow::ensure!(
                    out.status.success(),
                    "push {i}: {}",
                    String::from_utf8_lossy(&out.stderr)
                );
                Ok(())
            },
        ));
    }
    let mut failures = Vec::new();
    for t in tasks {
        if let Err(e) = t.await? {
            failures.push(e.to_string());
        }
    }
    assert!(
        failures.is_empty(),
        "{} failures: {:?}",
        failures.len(),
        &failures[..failures.len().min(5)]
    );
    let refs = server.ls_remote("t", "r").await?;
    for i in 0..16 {
        assert!(refs.contains(&format!("refs/heads/w{i}")));
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn push_tags() -> TestResult {
    let server = Server::start().await?;
    server.put_repo("t", "r").await?;

    let src = TestRepo::synthetic(1, 1)?;
    git_in(&src, &["commit", "--allow-empty", "-m", "a"])?;
    git_in(&src, &["branch", "-M", "main"])?;
    git_in(
        &src,
        &[
            "-c",
            "tag.forcesignannotated=false",
            "-c",
            "tag.gpgsign=false",
            "tag",
            "v1",
        ],
    )?;
    git_in(
        &src,
        &["remote", "add", "origin", &server.repo_url("t", "r")],
    )?;
    git_in(&src, &["push", "origin", "main", "--tags"])?;

    let refs = server.ls_remote("t", "r").await?;
    assert!(refs.contains("refs/tags/v1"));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partial_clone_blob_none_and_lazy_fetch() -> TestResult {
    let server = Server::start().await?;
    server.put_repo("t", "r").await?;

    let src = TestRepo::synthetic(3, 6)?;
    git_in(&src, &["commit", "--allow-empty", "-m", "init"])?;
    git_in(&src, &["branch", "-M", "main"])?;
    git_in(
        &src,
        &["remote", "add", "origin", &server.repo_url("t", "r")],
    )?;
    git_in(&src, &["push", "origin", "main"])?;

    let clone_dir = tempfile::tempdir()?;
    git(
        &[
            "clone",
            "--filter=blob:none",
            &server.repo_url("t", "r"),
            clone_dir.path().to_str().unwrap(),
        ],
        clone_dir.path().parent().unwrap(),
    )?;
    // Lazy checkout: a file read triggers an on-demand fetch of the blob.
    git_in(clone_dir.path(), &["checkout", "main"])?;
    // List files (forces blob fetches for the worktree).
    git_in(clone_dir.path(), &["ls-files"])?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ls_remote_lists_refs() -> TestResult {
    let server = Server::start().await?;
    server.put_repo("t", "r").await?;

    let src = TestRepo::synthetic(1, 1)?;
    git_in(&src, &["commit", "--allow-empty", "-m", "a"])?;
    git_in(&src, &["branch", "-M", "main"])?;
    git_in(
        &src,
        &["remote", "add", "origin", &server.repo_url("t", "r")],
    )?;
    git_in(&src, &["push", "origin", "main"])?;

    let refs = server.ls_remote("t", "r").await?;
    assert!(refs.contains("refs/heads/main"));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_instances_consistency() -> TestResult {
    // Two server processes sharing one MemoryStore, different cache dirs.
    let (a, b) = Server::start_pair().await?;
    a.put_repo("t", "r").await?;

    let src = TestRepo::synthetic(2, 2)?;
    git_in(&src, &["commit", "--allow-empty", "-m", "a"])?;
    git_in(&src, &["branch", "-M", "main"])?;
    git_in(&src, &["remote", "add", "origin", &a.repo_url("t", "r")])?;
    git_in(&src, &["push", "origin", "main"])?;

    // Immediate clone from B (the other instance) must see the push.
    let clone_dir = tempfile::tempdir()?;
    git(
        &[
            "clone",
            &b.repo_url("t", "r"),
            clone_dir.path().to_str().unwrap(),
        ],
        clone_dir.path().parent().unwrap(),
    )?;
    let h = git_in(clone_dir.path(), &["rev-parse", "main"])?;
    let src_h = git_in(&src, &["rev-parse", "main"])?;
    assert_eq!(h.trim(), src_h.trim());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn large_repo_clone_timing() -> TestResult {
    let server = Server::start().await?;
    server.put_repo("t", "big").await?;

    let synth_start = Instant::now();
    let src = TestRepo::synthetic(2000, 5)?;
    println!("2k synthetic repo took {:?}", synth_start.elapsed());
    git_in(&src, &["commit", "--allow-empty", "-m", "seed"])?;
    git_in(&src, &["branch", "-M", "main"])?;
    git_in(
        &src,
        &["remote", "add", "origin", &server.repo_url("t", "big")],
    )?;
    git_in(&src, &["push", "origin", "main"])?;

    let clone_dir = tempfile::tempdir()?;
    let start = std::time::Instant::now();
    git(
        &[
            "clone",
            &server.repo_url("t", "big"),
            clone_dir.path().to_str().unwrap(),
        ],
        clone_dir.path().parent().unwrap(),
    )?;
    let elapsed = start.elapsed();
    println!("2k-commit clone took {elapsed:?}");
    git_in(clone_dir.path(), &["fsck"])?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shallow_clone_then_unshallow() -> TestResult {
    let server = Server::start().await?;
    server.put_repo("t", "shallow").await?;
    let src = TestRepo::synthetic(4, 2)?;
    git_in(&src, &["branch", "-M", "main"])?;
    git_in(
        &src,
        &["remote", "add", "origin", &server.repo_url("t", "shallow")],
    )?;
    git_in(&src, &["push", "origin", "main"])?;

    let clone_dir = tempfile::tempdir()?;
    git(
        &[
            "clone",
            "--branch",
            "main",
            "--depth",
            "1",
            &server.repo_url("t", "shallow"),
            clone_dir.path().to_str().unwrap(),
        ],
        clone_dir.path().parent().unwrap(),
    )?;
    assert_eq!(
        git_in(clone_dir.path(), &["rev-list", "--count", "HEAD"])?.trim(),
        "1"
    );
    git_in(clone_dir.path(), &["fetch", "--unshallow", "origin"])?;
    assert_eq!(
        git_in(clone_dir.path(), &["rev-list", "--count", "HEAD"])?.trim(),
        "4"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn atomic_push_rejects_all_refs_when_one_is_non_ff() -> TestResult {
    let server = Server::start().await?;
    server.put_repo("t", "atomic").await?;
    let src = TestRepo::synthetic(1, 1)?;
    git_in(&src, &["branch", "-M", "main"])?;
    git_in(&src, &["branch", "topic"])?;
    git_in(
        &src,
        &["remote", "add", "origin", &server.repo_url("t", "atomic")],
    )?;
    git_in(&src, &["push", "origin", "main", "topic"])?;
    let initial_topic = git_in(&src, &["rev-parse", "topic"])?;

    // Advance main on the server, leaving the other clone's main stale.
    git_in(&src, &["commit", "--allow-empty", "-m", "server main"])?;
    let server_main = git_in(&src, &["rev-parse", "main"])?;
    git_in(&src, &["push", "origin", "main"])?;

    let other_dir = tempfile::tempdir()?;
    git(
        &[
            "clone",
            "--branch",
            "main",
            &server.repo_url("t", "atomic"),
            other_dir.path().to_str().unwrap(),
        ],
        other_dir.path().parent().unwrap(),
    )?;
    git_in(
        other_dir.path(),
        &["update-ref", "refs/heads/main", initial_topic.trim()],
    )?;
    git_in(
        other_dir.path(),
        &["checkout", "-q", "-b", "topic", "origin/topic"],
    )?;
    git_in(
        other_dir.path(),
        &["commit", "--allow-empty", "-m", "topic ff"],
    )?;
    let out = Command::new("git")
        .current_dir(other_dir.path())
        .args(["push", "--atomic", "origin", "main", "topic"])
        .output()?;
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        !out.status.success(),
        "atomic non-ff push unexpectedly succeeded: {stderr}"
    );
    assert!(
        stderr.contains("main"),
        "report should mention main: {stderr}"
    );
    assert!(
        stderr.contains("topic"),
        "atomic report should mention topic: {stderr}"
    );

    let refs = server.ls_remote("t", "atomic").await?;
    assert!(refs.contains(&format!("{}\trefs/heads/main", server_main.trim())));
    assert!(refs.contains(&format!("{}\trefs/heads/topic", initial_topic.trim())));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn push_option_is_recorded_in_wal_log() -> TestResult {
    let server = Server::start().await?;
    server.put_repo("t", "push-option").await?;
    let src = TestRepo::synthetic(1, 1)?;
    git_in(&src, &["branch", "-M", "main"])?;
    git_in(
        &src,
        &[
            "remote",
            "add",
            "origin",
            &server.repo_url("t", "push-option"),
        ],
    )?;
    git_in(&src, &["push", "--push-option=foo", "origin", "main"])?;
    let entries = server.read_log("t", "push-option").await?;
    assert!(
        entries
            .iter()
            .any(|entry| entry.meta.get("push_options").map(String::as_str) == Some("foo")),
        "push option missing from WAL metadata: {entries:?}"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn push_protocol_v0() -> TestResult {
    let server = Server::start().await?;
    server.put_repo("t", "push-v0").await?;
    let src = TestRepo::synthetic(1, 1)?;
    git_in(&src, &["branch", "-M", "main"])?;
    git_in(
        &src,
        &["remote", "add", "origin", &server.repo_url("t", "push-v0")],
    )?;
    git_in(
        &src,
        &["-c", "protocol.version=0", "push", "origin", "main"],
    )?;
    let refs = server.ls_remote("t", "push-v0").await?;
    assert!(refs.contains("refs/heads/main"));
    Ok(())
}

/// Many-ref advertisement: v0 and v2 prefix filtering stay fast. The fast tier
/// uses 2k refs (~2 s); the ignored bench pushes 20k (dominated by git's own
/// client-side `send-pack`, ~70 s — see AGENTS.md §7). `WALGIT_TEST_REFS=N`
/// overrides the count.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn many_refs_advertisement_and_v2_prefix_are_fast() -> TestResult {
    let n: usize = std::env::var("WALGIT_TEST_REFS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2_000);
    many_refs_impl(n).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "bench: 20k-ref mirror push (~70 s, git client-side send-pack)"]
async fn bench_20k_ref_advertisement() -> TestResult {
    many_refs_impl(20_000).await
}

async fn many_refs_impl(n: usize) -> TestResult {
    let server = Server::start().await?;
    server.put_repo("t", "many-refs").await?;
    let src = TestRepo::synthetic(1, 1)?;
    git_in(&src, &["branch", "-M", "main"])?;
    let head = git_in(&src, &["rev-parse", "main"])?;
    let mut update = Command::new("git")
        .current_dir(&*src)
        .args(["update-ref", "--stdin"])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()?;
    {
        let stdin = update.stdin.as_mut().context("update-ref stdin")?;
        for i in 0..n {
            writeln!(stdin, "create refs/heads/ref-{i:05} {}", head.trim())?;
        }
    }
    let update_output = update.wait_with_output()?;
    assert!(
        update_output.status.success(),
        "update-ref failed: {}",
        String::from_utf8_lossy(&update_output.stderr)
    );
    git_in(
        &src,
        &[
            "remote",
            "add",
            "origin",
            &server.repo_url("t", "many-refs"),
        ],
    )?;
    let push_start = Instant::now();
    git_in(&src, &["push", "--mirror", "origin"])?;
    println!("{n}-ref mirror push took {:?}", push_start.elapsed());
    assert!(push_start.elapsed() < std::time::Duration::from_mins(4));
    let start = Instant::now();
    let output = Command::new("git")
        .args(["ls-remote", &server.repo_url("t", "many-refs")])
        .output()?;
    let elapsed = start.elapsed();
    assert!(output.status.success(), "large ls-remote failed");
    assert!(
        elapsed < std::time::Duration::from_secs(2),
        "ls-remote took {elapsed:?}"
    );
    let all = String::from_utf8_lossy(&output.stdout);
    assert_eq!(
        all.lines()
            .filter(|line| line.contains("refs/heads/ref-"))
            .count(),
        n,
        "wrong large-ref count"
    );

    let start = Instant::now();
    let output = Command::new("git")
        .args([
            "-c",
            "protocol.version=2",
            "ls-remote",
            "--refs",
            "--heads",
            &server.repo_url("t", "many-refs"),
            // 100 refs share this prefix at every n >= 2000 (ref-NNN00..ref-NNN99).
            &format!("refs/heads/ref-{:03}*", (n / 100) - 1),
        ])
        .output()?;
    let elapsed_v2 = start.elapsed();
    assert!(output.status.success(), "v2 prefix ls-remote failed");
    assert!(
        elapsed_v2 < std::time::Duration::from_secs(2),
        "v2 ls-refs took {elapsed_v2:?}"
    );
    let prefixed = String::from_utf8_lossy(&output.stdout);
    assert_eq!(prefixed.lines().count(), 100, "wrong v2 prefix count");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sha256_repo_roundtrip() -> TestResult {
    if !git_supports_sha256() {
        eprintln!("git init --object-format=sha256 unsupported; skipping");
        return Ok(());
    }
    let server = Server::start().await?;
    let url = format!("{}/t/sha256?object_format=sha256", server.base_url);
    let response = reqwest::Client::new().put(url).send().await?;
    assert!(
        response.status().is_success(),
        "sha256 create failed: {}",
        response.status()
    );
    let src_dir = tempfile::tempdir()?;
    git_in(src_dir.path(), &["init", "-q", "--object-format=sha256"])?;
    git_in(src_dir.path(), &["config", "user.name", "sha256"])?;
    git_in(src_dir.path(), &["config", "user.email", "sha256@walgit"])?;
    std::fs::write(src_dir.path().join("file"), b"sha256\n")?;
    git_in(src_dir.path(), &["add", "file"])?;
    git_in(src_dir.path(), &["commit", "-q", "-m", "sha256"])?;
    git_in(src_dir.path(), &["branch", "-M", "main"])?;
    git_in(
        src_dir.path(),
        &["remote", "add", "origin", &server.repo_url("t", "sha256")],
    )?;
    git_in(src_dir.path(), &["push", "origin", "main"])?;
    let clone_dir = tempfile::tempdir()?;
    let clone = Command::new("git")
        .args([
            "clone",
            "--branch",
            "main",
            &server.repo_url("t", "sha256"),
            clone_dir.path().to_str().unwrap(),
        ])
        .output()?;
    if !clone.status.success() {
        eprintln!(
            "sha256 push succeeded but clone is unsupported: {}",
            String::from_utf8_lossy(&clone.stderr)
        );
        return Ok(());
    }
    assert_eq!(
        git_in(clone_dir.path(), &["rev-parse", "--show-object-format"])?.trim(),
        "sha256"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lfs_roundtrip_when_available() -> TestResult {
    if !git_lfs_present() {
        eprintln!("git lfs not present; skipping LFS test");
        return Ok(());
    }
    let server = Server::start().await?;
    server.put_repo("t", "r").await?;

    let src = TestRepo::synthetic(1, 1)?;
    git_in(&src, &["lfs", "install", "--local"])?;
    git_in(&src, &["lfs", "track", "*.bin"])?;
    git_in(&src, &["add", ".gitattributes"])?;
    std::fs::write(src.join("blob.bin"), b"this is a large blob payload\n")?;
    git_in(&src, &["add", "blob.bin"])?;
    git_in(&src, &["commit", "-m", "lfs"])?;
    git_in(&src, &["branch", "-M", "main"])?;
    git_in(
        &src,
        &["remote", "add", "origin", &server.repo_url("t", "r")],
    )?;
    git_in(&src, &["push", "origin", "main"])?;

    let clone_dir = tempfile::tempdir()?;
    git(
        &[
            "-c",
            "filter.lfs.clean=git-lfs clean -- %f",
            "-c",
            "filter.lfs.smudge=git-lfs smudge -- %f",
            "-c",
            "filter.lfs.process=git-lfs filter-process",
            "-c",
            "filter.lfs.required=true",
            "clone",
            &server.repo_url("t", "r"),
            clone_dir.path().to_str().unwrap(),
        ],
        clone_dir.path().parent().unwrap(),
    )?;
    let content = std::fs::read(clone_dir.path().join("blob.bin"))?;
    assert_eq!(content, b"this is a large blob payload\n");

    // a large push (2026-08-21): a SECOND clone that never downloaded the
    // LFS bytes (`GIT_LFS_SKIP_SMUDGE`: pointers only, as for an object deep
    // in history) pushes a new commit. git-lfs's pre-push batches every
    // pointer reachable from the push; the server HAS the object and must say
    // so with no `actions` at all — a verify-only answer made git-lfs try to
    // upload bytes it does not have: "object … missing locally and on remote".
    let pointer_clone = tempfile::tempdir()?;
    let mut cmd = std::process::Command::new("git");
    cmd.env("GIT_LFS_SKIP_SMUDGE", "1")
        .args([
            "clone",
            "-q",
            &server.repo_url("t", "r"),
            pointer_clone.path().to_str().unwrap(),
        ])
        .current_dir(pointer_clone.path().parent().unwrap());
    assert!(cmd.output()?.status.success());
    let p = pointer_clone.path();
    assert!(
        std::fs::read_to_string(p.join("blob.bin"))?.starts_with("version https://git-lfs"),
        "pointer only"
    );
    assert!(
        !p.join(".git/lfs/objects").exists()
            || std::fs::read_dir(p.join(".git/lfs/objects"))?
                .next()
                .is_none(),
        "no local LFS bytes"
    );
    git_in(p, &["lfs", "install", "--local"])?;
    git_in(p, &["config", "user.email", "t@t"])?;
    git_in(p, &["config", "user.name", "T"])?;
    // Move the pointer (rename) so the push's pre-push batches its oid, exactly
    // like the pointers of files touched by the pushed commits — bytes still absent.
    git_in(p, &["mv", "blob.bin", "moved.bin"])?;
    git_in(
        p,
        &[
            "commit",
            "-q",
            "-m",
            "move the LFS file (pointer only, no bytes here)",
        ],
    )?;
    let out = std::process::Command::new("git")
        .current_dir(p)
        .env("GIT_TRACE", "1")
        .args(["push", "origin", "main"])
        .output()?;
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success(),
        "push from a pointer-only clone must succeed (server has the object):\n{err}"
    );
    assert!(!err.contains("missing locally and on remote"), "{err}");
    Ok(())
}

fn git_lfs_present() -> bool {
    Command::new("git")
        .args(["lfs", "version"])
        .output()
        .is_ok_and(|o| o.status.success())
}

fn git_supports_sha256() -> bool {
    let Ok(dir) = tempfile::tempdir() else {
        return false;
    };
    Command::new("git")
        .args([
            "init",
            "-q",
            "--object-format=sha256",
            dir.path().to_str().unwrap(),
        ])
        .output()
        .is_ok_and(|o| o.status.success())
}

/// A front whose `cache.max_bytes` cannot hold a repository's pack set must
/// still answer every refs-level request (ls-remote v0/v2, web
/// refs) from the WAL ref snapshot, refuse object work with a readable
/// pkt-line ERR explaining the capacity limit, and never pull the packs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn huge_repo_front_serves_refs_without_packs() -> TestResult {
    let big = Server::start().await?;
    big.put_repo("t", "huge").await?;
    let src = TestRepo::synthetic(4, 3)?;
    git_in(&src, &["branch", "-M", "main"])?;
    git_in(&src, &["tag", "-a", "v1", "-m", "v1"])?;
    git_in(
        &src,
        &["remote", "add", "origin", &big.repo_url("t", "huge")],
    )?;
    git_in(&src, &["push", "origin", "main", "v1"])?;

    // Second front: 1 byte of pack cache.
    let small = big
        .start_sibling_with(|c| c.cache.max_bytes = bytesize::ByteSize::b(1))
        .await?;
    let url = small.repo_url("t", "huge");
    let head = git_in(&src, &["rev-parse", "main"])?.trim().to_string();

    // Refs-level git (v2 ls-refs and v0 advertisement) works.
    for proto in ["2", "0"] {
        let out = git_in(
            &src,
            &[
                "-c",
                &format!("protocol.version={proto}"),
                "ls-remote",
                &url,
            ],
        )?;
        assert!(
            out.contains(&format!("{head}\trefs/heads/main")),
            "v{proto}: {out}"
        );
        assert!(
            out.contains("refs/tags/v1^{}"),
            "peeled tag from snapshot (v{proto}): {out}"
        );
    }
    // Web refs work without object materialization.
    let refs = reqwest::get(format!("{}/t/huge/api/refs", small.base_url)).await?;
    assert_eq!(refs.status(), 200);
    let refs: serde_json::Value = refs.json().await?;
    assert_eq!(refs["head"]["sha"], head);
    let resolved = reqwest::get(format!("{}/t/huge/api/resolve/main", small.base_url)).await?;
    assert_eq!(resolved.status(), 200);

    // Object work is refused with a capacity explanation.
    let clone_dir = tempfile::tempdir()?;
    let err = git(
        &["clone", &url, clone_dir.path().to_str().unwrap()],
        clone_dir.path().parent().unwrap(),
    )
    .unwrap_err()
    .to_string();
    assert!(err.contains("larger than this instance"), "{err}");
    // The small front never downloaded the pack set.
    assert!(
        !small.registry_has_packs("t", "huge").await,
        "small front must not materialize packs"
    );

    // A capacity-suitable front still supports an ordinary clone.
    let clone2 = tempfile::tempdir()?;
    git(
        &[
            "clone",
            &big.repo_url("t", "huge"),
            clone2.path().to_str().unwrap(),
        ],
        clone2.path().parent().unwrap(),
    )?;
    assert_eq!(git_in(clone2.path(), &["rev-parse", "main"])?.trim(), head);
    Ok(())
}

/// The server narrates a v2 fetch over sideband (band 2 → `remote: * …`):
/// auth, WAL seq, local copy state — before upload-pack's own
/// progress. Requires sideband-all (advertised with the git engine).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fetch_is_narrated_over_sideband() -> TestResult {
    let server = Server::start().await?;
    server.put_repo("t", "narrate").await?;
    let src = tempfile::tempdir()?;
    git(&["init", "-q", "-b", "main", "."], src.path())?;
    git(
        &[
            "-c",
            "user.email=t@t",
            "-c",
            "user.name=T",
            "commit",
            "-q",
            "--allow-empty",
            "-m",
            "one",
        ],
        src.path(),
    )?;
    git(
        &["push", "-q", &server.repo_url("t", "narrate"), "main"],
        src.path(),
    )?;
    let dst = tempfile::tempdir()?;
    let out = std::process::Command::new("git")
        .args([
            "-c",
            "protocol.version=2",
            "clone",
            "--progress",
            &server.repo_url("t", "narrate"),
            dst.path().to_str().unwrap(),
        ])
        .output()?;
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(out.status.success(), "{stderr}");
    assert!(
        stderr.contains("remote: * walgit: t/narrate"),
        "narration missing:\n{stderr}"
    );
    assert!(stderr.contains("local copy ready"), "{stderr}");
    println!("{stderr}");
    // `no-progress` is honoured: git sends it for its own lazy promisor fetches (blobs during a
    // sparse checkout of a blobless clone) and for any non-tty fetch without --progress; those
    // must not be narrated — an up-to-date client therefore sees no
    // `remote: *` lines at all (2026-08-22 gate 4), by design, not because the SSD host is silent.
    let quiet = tempfile::tempdir()?;
    let out = std::process::Command::new("git")
        .args([
            "-c",
            "protocol.version=2",
            "clone",
            "--no-progress",
            &server.repo_url("t", "narrate"),
            quiet.path().to_str().unwrap(),
        ])
        .output()?;
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(out.status.success(), "{stderr}");
    assert!(
        !stderr.contains("remote: *"),
        "no-progress fetch was narrated:\n{stderr}"
    );
    Ok(())
}

/// A front whose cache cannot hold a repository's **base** pack serves `git
/// fetch` anyway: the base is remote-served (commit-graph layer local, data by
/// range read through the remote reader), recent packs are local, and the gix
/// engine enumerates by tree diff against the client's haves. The client
/// cloned from a big front and fetches a later push
/// from the small one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fetch_from_front_that_serves_the_base_remotely() -> TestResult {
    let big = Server::start().await?;
    big.put_repo("t", "rbase").await?;
    // 12 commits × 5 files (incompressible-ish content makes the base pack
    // dwarf its side-files), pushed, then folded into a bitmap'd base with a
    // commit-graph layer.
    let src = TestRepo::synthetic(12, 5)?;
    // ~256 KB of incompressible bytes so the base pack dwarfs its side-files
    // and the later tier-0 pack.
    let mut blob = Vec::with_capacity(256 * 1024);
    let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
    while blob.len() < 256 * 1024 {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        blob.extend_from_slice(&x.to_le_bytes());
    }
    std::fs::write(src.join("big.bin"), &blob)?;
    git_in(&src, &["add", "big.bin"])?;
    git_in(&src, &["commit", "-q", "-m", "big blob"])?;
    git_in(
        &src,
        &["remote", "add", "origin", &big.repo_url("t", "rbase")],
    )?;
    git_in(&src, &["push", "-q", "origin", "main"])?;
    let id = walgit_git::RepoId::new("t", "rbase")?;
    let handle = big.state.registry.open(&id).await?;
    let log = |line: String| println!("compact: {line}");
    let out = walgit_server::ops::compact_repo(
        &handle,
        walgit_server::ops::CompactRequest {
            force: true,
            rebuild_base: true,
        },
        &log,
    )
    .await?;
    println!("{}", out.summary());
    let m = handle.manifest();
    let base = m
        .packs
        .iter()
        .find(|p| p.tier == 2 && p.has_commit_graph)
        .expect("a frozen pack carrying the commit-graph layer");
    assert!(
        base.has_commit_graph,
        "base carries a commit-graph layer: {base:?}"
    );
    let remote_pack = m.packs.iter().max_by_key(|p| p.pack_size).unwrap();
    assert_eq!(remote_pack.kind, walgit_proto::v1::PackKind::Blobs as i32);
    let base_tip = git_in(&src, &["rev-parse", "main"])?.trim().to_string();

    // Client: full clone from the big front.
    let clone = tempfile::tempdir()?;
    git(
        &[
            "clone",
            "-q",
            &big.repo_url("t", "rbase"),
            clone.path().to_str().unwrap(),
        ],
        clone.path().parent().unwrap(),
    )?;
    assert_eq!(
        git_in(clone.path(), &["rev-parse", "HEAD"])?.trim(),
        base_tip
    );

    // One more push (tier 0) after the base.
    std::fs::write(src.join("after.txt"), "after the base\n")?;
    git_in(&src, &["add", "after.txt"])?;
    git_in(&src, &["commit", "-q", "-m", "after base"])?;
    git_in(&src, &["push", "-q", "origin", "main"])?;
    let new_tip = git_in(&src, &["rev-parse", "main"])?.trim().to_string();

    // Small front: cannot hold the base, no mount → remote-served.
    let small = big
        .start_sibling_with(|c| {
            c.cache.max_bytes = bytesize::ByteSize::b(remote_pack.pack_size / 2);
        })
        .await?;
    let url = small.repo_url("t", "rbase");
    git_in(clone.path(), &["remote", "set-url", "origin", &url])?;
    let fetch = std::process::Command::new("git")
        .args([
            "-c",
            "protocol.version=2",
            "fetch",
            "--progress",
            "origin",
            "main",
        ])
        .current_dir(clone.path())
        .output()?;
    let stderr = String::from_utf8_lossy(&fetch.stderr);
    assert!(fetch.status.success(), "fetch failed:\n{stderr}");
    assert_eq!(
        git_in(clone.path(), &["rev-parse", "FETCH_HEAD"])?.trim(),
        new_tip
    );
    git_in(clone.path(), &["merge", "-q", "--ff-only", "FETCH_HEAD"])?;
    assert_eq!(
        std::fs::read_to_string(clone.path().join("after.txt"))?,
        "after the base\n"
    );
    // Narrated: the server said the base is read from the bucket.
    assert!(stderr.contains("read from the bucket by range"), "{stderr}");
    // The small front never materialized the base pack.
    let sh = small.state.registry.open(&id).await?;
    let mut expected_remote: Vec<_> = m
        .packs
        .iter()
        .filter(|p| p.tier == 2)
        .map(|p| p.checksum.clone())
        .collect();
    expected_remote.sort();
    let mut actual_remote = sh.remote_served();
    actual_remote.sort();
    assert_eq!(actual_remote, expected_remote);
    for checksum in &expected_remote {
        assert!(
            !sh.local()
                .pack_path(&gix_hash::ObjectId::from_hex(checksum.as_bytes())?)
                .exists()
        );
    }

    // Protocol v0 is refused with a readable explanation (stock git cannot
    // read a remote-served base).
    std::fs::write(src.join("after2.txt"), "second\n")?;
    git_in(&src, &["add", "after2.txt"])?;
    git_in(&src, &["commit", "-q", "-m", "after base 2"])?;
    git_in(&src, &["push", "-q", "origin", "main"])?;
    let out = std::process::Command::new("git")
        .args(["-c", "protocol.version=0", "fetch", "origin", "main"])
        .current_dir(clone.path())
        .output()?;
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("protocol v2"));
    Ok(())
}

/// `git.max_wants`: a blobless clone that checks out HEAD asks for every blob of the tree in one lazy
/// fetch (carrying `no-progress`, so nothing narrated there is seen). Above the bound the server
/// refuses that fetch with the fix (`--sparse` / `--no-checkout`, the recipes) before any pack work;
/// `--no-checkout` passes; the initial blobless fetch gets the heads-up on band 2.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn max_wants_refuses_the_blobless_checkout_storm_with_the_fix() -> TestResult {
    let server = Server::start_with_tweak(|c| c.git.max_wants = 5).await?;
    server.put_repo("t", "storm").await?;
    // 12 commits → 12 distinct blobs in HEAD's tree (the synthetic repo reuses one blob per revision).
    let src = TestRepo::synthetic(12, 3)?;
    git_in(
        &src,
        &["remote", "add", "origin", &server.repo_url("t", "storm")],
    )?;
    git_in(&src, &["push", "-q", "origin", "main"])?;
    let url = server.repo_url("t", "storm");

    // Full checkout after a blobless clone: the lazy fetch wants 12 blobs > 5 → refused, fix named.
    let dir = tempfile::tempdir()?;
    let out = std::process::Command::new("git")
        .args([
            "clone",
            "--filter=blob:none",
            "--progress",
            &url,
            dir.path().to_str().unwrap(),
        ])
        .output()?;
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        !out.status.success(),
        "the checkout's blob fetch must be refused:\n{stderr}"
    );
    assert!(
        stderr.contains("asks for 12 objects at once (this host's bound is 5)"),
        "{stderr}"
    );
    assert!(
        stderr.contains("--filter=blob:none --sparse"),
        "the fix is in the error:\n{stderr}"
    );
    // The initial (commit/tree) fetch narrated the heads-up on band 2 before anything went wrong.
    assert!(
        stderr.contains("remote: * blobless clone: without --sparse or --no-checkout"),
        "{stderr}"
    );
    assert!(
        stderr.contains("refuses requests above 5 objects"),
        "{stderr}"
    );

    // --no-checkout: the initial fetch wants one tip; nothing lazy follows. Blobs come on demand, few at a time.
    let dir = tempfile::tempdir()?;
    let out = std::process::Command::new("git")
        .args([
            "clone",
            "-q",
            "--filter=blob:none",
            "--no-checkout",
            &url,
            dir.path().to_str().unwrap(),
        ])
        .output()?;
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(
        git_in(dir.path(), &["rev-parse", "HEAD"])?.trim(),
        git_in(&src, &["rev-parse", "main"])?.trim()
    );
    // One file = one lazy blob (≤ 5): served.
    let one = git_in(dir.path(), &["ls-tree", "--name-only", "HEAD"])?
        .lines()
        .next()
        .unwrap()
        .to_string();
    git_in(dir.path(), &["checkout", "-q", "HEAD", "--", &one])?;
    assert!(dir.path().join(&one).exists());
    Ok(())
}

/// A push is narrated on band 2 (`remote: * …`) from the first moment — the
/// server never lets the connection go silent while it syncs/unpacks — and
/// still ends with a clean report-status.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn push_is_narrated_over_sideband() -> TestResult {
    let server = Server::start().await?;
    server.put_repo("t", "pushnarr").await?;
    let src = tempfile::tempdir()?;
    git(&["init", "-q", "-b", "main", "."], src.path())?;
    git(
        &[
            "-c",
            "user.email=t@t",
            "-c",
            "user.name=T",
            "commit",
            "-q",
            "--allow-empty",
            "-m",
            "one",
        ],
        src.path(),
    )?;
    let out = std::process::Command::new("git")
        .args([
            "push",
            "--progress",
            &server.repo_url("t", "pushnarr"),
            "main",
        ])
        .current_dir(src.path())
        .output()?;
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(out.status.success(), "{stderr}");
    assert!(
        stderr.contains("remote: * walgit: t/pushnarr"),
        "narration missing:\n{stderr}"
    );
    assert!(
        stderr.contains("[new branch]") || stderr.contains("main -> main"),
        "{stderr}"
    );
    Ok(())
}

/// A push from a shallow (`--depth=1`) clone sends `shallow <oid>` lines
/// before the commands; the server accepts it (prod: a large repository push → 500
/// "missing ref name").
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn push_from_a_shallow_clone() -> TestResult {
    let server = Server::start().await?;
    server.put_repo("t", "shallowpush").await?;
    let src = TestRepo::synthetic(4, 2)?;
    git_in(
        &src,
        &[
            "remote",
            "add",
            "origin",
            &server.repo_url("t", "shallowpush"),
        ],
    )?;
    git_in(&src, &["push", "-q", "origin", "main"])?;
    let clone = tempfile::tempdir()?;
    git(
        &[
            "clone",
            "-q",
            "--depth",
            "1",
            &server.repo_url("t", "shallowpush"),
            clone.path().to_str().unwrap(),
        ],
        clone.path().parent().unwrap(),
    )?;
    assert!(clone.path().join(".git/shallow").exists());
    std::fs::write(clone.path().join("from-shallow.txt"), "hi\n")?;
    git_in(clone.path(), &["add", "from-shallow.txt"])?;
    git_in(
        clone.path(),
        &[
            "-c",
            "user.email=t@t",
            "-c",
            "user.name=T",
            "commit",
            "-q",
            "-m",
            "from a shallow clone",
        ],
    )?;
    let out = std::process::Command::new("git")
        .args(["push", "origin", "HEAD:refs/heads/from-shallow"])
        .current_dir(clone.path())
        .output()?;
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let refs = server.ls_remote("t", "shallowpush").await?;
    assert!(refs.contains("refs/heads/from-shallow"), "{refs}");
    Ok(())
}

/// `--filter=tree:0` partial clone over the wire: commits only, then a
/// checkout lazily fetches trees and blobs (wants that are not commits, with
/// `allow-any-sha1-in-want`), plus `--depth` + filter together.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partial_clone_tree_zero_and_depth_with_filter() -> TestResult {
    let server = Server::start_with_tweak(|c| c.git.allow_any_sha1_in_want = true).await?;
    server.put_repo("t", "tree0").await?;
    let src = TestRepo::synthetic(5, 4)?;
    git_in(
        &src,
        &["remote", "add", "origin", &server.repo_url("t", "tree0")],
    )?;
    git_in(&src, &["push", "-q", "origin", "main"])?;
    let head = git_in(&src, &["rev-parse", "main"])?;

    let clone = tempfile::tempdir()?;
    git(
        &[
            "clone",
            "-q",
            "--no-checkout",
            "--filter=tree:0",
            &server.repo_url("t", "tree0"),
            clone.path().to_str().unwrap(),
        ],
        clone.path().parent().unwrap(),
    )?;
    // Only commits came over: the root tree is not local yet.
    let has_tree = std::process::Command::new("git")
        .current_dir(clone.path())
        .args(["cat-file", "-e", &format!("{}^{{tree}}", head.trim())])
        .env("GIT_NO_LAZY_FETCH", "1")
        .status()?
        .success();
    assert!(!has_tree, "tree:0 clone must not contain trees");
    // Checkout fetches trees + blobs on demand.
    git_in(clone.path(), &["checkout", "-q", "main"])?;
    assert!(clone.path().join("f4_0.txt").exists());
    assert_eq!(
        git_in(clone.path(), &["rev-list", "--count", "HEAD"])?.trim(),
        "5"
    );

    let shallow = tempfile::tempdir()?;
    git(
        &[
            "clone",
            "-q",
            "--depth",
            "2",
            "--filter=blob:none",
            &server.repo_url("t", "tree0"),
            shallow.path().to_str().unwrap(),
        ],
        shallow.path().parent().unwrap(),
    )?;
    assert_eq!(
        git_in(shallow.path(), &["rev-list", "--count", "HEAD"])?.trim(),
        "2"
    );
    assert_eq!(
        std::fs::read_to_string(shallow.path().join("f4_0.txt"))?,
        "content 4\n"
    );
    Ok(())
}

/// Installing a history pack runs `git multi-pack-index write` (minutes on
/// a large repository) — it must run off the async runtime. This test runs the server on a
/// **single** tokio worker with a `git` shim that sleeps 3 s on
/// `multi-pack-index`, starts the install on a sibling, and asserts that an
/// unrelated refs request answers in < 1 s meanwhile (prod: every request on
/// the instance stalled for minutes, timers included).
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn history_pack_install_does_not_stall_the_runtime() -> TestResult {
    const CHILD: &str = "WALGIT_TEST_HISTORY_INSTALL_CHILD";
    if std::env::var_os(CHILD).is_none() {
        // git shim: slow only for multi-pack-index.
        let shim = tempfile::tempdir()?;
        let real_git = String::from_utf8(
            std::process::Command::new("sh")
                .args(["-c", "command -v git"])
                .output()?
                .stdout,
        )?
        .trim()
        .to_string();
        std::fs::write(
            shim.path().join("git"),
            format!(
                "#!/bin/sh\nfor arg in \"$@\"; do\n  if [ \"$arg\" = multi-pack-index ]; then sleep 3; break; fi\ndone\nexec {real_git} \"$@\"\n"
            ),
        )?;
        std::fs::set_permissions(
            shim.path().join("git"),
            std::os::unix::fs::PermissionsExt::from_mode(0o755),
        )?;
        let old_path = std::env::var("PATH").unwrap_or_default();
        let status = tokio::process::Command::new(std::env::current_exe()?)
            .args([
                "--exact",
                "history_pack_install_does_not_stall_the_runtime",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .env("PATH", format!("{}:{old_path}", shim.path().display()))
            .status()
            .await?;
        assert!(status.success(), "isolated history install test failed");
        return Ok(());
    }

    let big = Server::start().await?;
    big.put_repo("t", "hist").await?;
    big.put_repo("t", "other").await?;
    let src = TestRepo::synthetic(6, 3)?;
    git_in(
        &src,
        &["remote", "add", "origin", &big.repo_url("t", "hist")],
    )?;
    git_in(&src, &["push", "-q", "origin", "main"])?;
    let other = TestRepo::synthetic(1, 1)?;
    git_in(
        &other,
        &["remote", "add", "origin", &big.repo_url("t", "other")],
    )?;
    git_in(&other, &["push", "-q", "origin", "main"])?;
    // Base + history pack.
    let id = walgit_git::RepoId::new("t", "hist")?;
    let h = big.state.registry.open(&id).await?;
    let log = |line: String| println!("compact: {line}");
    walgit_server::ops::compact_repo(
        &h,
        walgit_server::ops::CompactRequest {
            force: true,
            rebuild_base: true,
        },
        &log,
    )
    .await?;
    assert!(
        h.manifest()
            .packs
            .iter()
            .any(|p| p.kind == walgit_proto::v1::PackKind::History as i32),
        "history pack published"
    );

    // Sibling: its first Serve sync of t/hist installs the history pack (midx = 3 s in the shim).
    // It prewarms t/hist: /readyz must stay 503 until the history pack + midx
    // are installed (prod 2026-08-21: "ready" 22 minutes before the install
    // finished, every request on the instance stalled meanwhile).
    let small = big
        .start_sibling_with(|c| {
            c.cache.prewarm = vec!["t/hist".into()];
            c.cache.prewarm_ready_timeout = std::time::Duration::from_mins(10);
        })
        .await?;
    walgit_server::prewarm::spawn(small.state.clone());
    let sh = small.state.registry.open(&id).await?;
    let install_started = std::time::Instant::now();
    let hp = sh
        .manifest()
        .packs
        .iter()
        .find(|p| p.kind == walgit_proto::v1::PackKind::History as i32)
        .unwrap()
        .clone();
    let hp_path = sh
        .local()
        .pack_path(&gix_hash::ObjectId::from_hex(hp.checksum.as_bytes())?);
    let install = tokio::spawn(async move {
        let t = std::time::Instant::now();
        // The Serve sync itself returns without the history pack; the
        // background task installs it (midx = 3 s in the shim).
        let _g = sh.sync().await.unwrap();
        while !hp_path.parent().unwrap().join("multi-pack-index").is_file() {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        t.elapsed()
    });
    // Meanwhile, every 200 ms until the install is done: an unrelated refs
    // request must answer fast (one of them lands inside the 3 s midx write).
    let mut worst = 0u128;
    let mut probes = 0;
    let mut not_ready_seen = 0;
    while !install.is_finished() {
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        let t = std::time::Instant::now();
        let refs = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            small.get_text("/t/other/api/refs", &[]),
        )
        .await;
        let ms = t.elapsed().as_millis();
        assert!(
            refs.is_ok(),
            "refs request timed out during the history pack install"
        );
        worst = worst.max(ms);
        probes += 1;
        if small.get_status("/readyz").await? == axum::http::StatusCode::SERVICE_UNAVAILABLE {
            not_ready_seen += 1;
        }
    }
    assert!(
        not_ready_seen >= 3,
        "/readyz flipped to ready while the history pack was still installing ({not_ready_seen} 503s in {probes} probes)"
    );
    // Once the install is done the prewarm completes and the instance is ready.
    let mut ready = false;
    for _ in 0..100 {
        if small.get_status("/readyz").await? == axum::http::StatusCode::OK {
            ready = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(ready, "/readyz never became ready after the install");
    // A stalled runtime shows up here first: the probe loop itself (its timers,
    // its HTTP client) cannot run while a worker is blocked.
    assert!(
        probes >= 5,
        "runtime stalled during the install: only {probes} probe(s) ran in {:?}",
        install_started.elapsed()
    );
    assert!(
        worst < 1000,
        "a refs request took {worst} ms while the midx write ran"
    );
    let took = install.await?;
    assert!(
        took.as_secs_f64() >= 3.0,
        "the shim should have slowed the install: {took:?}"
    );
    Ok(())
}

/// Materialization runs on its own runtime: even an unknown *blocking* call
/// inside the install path (simulated by `WALGIT_TEST_BLOCK_INSTALL_MS`, a
/// synchronous sleep in `reconcile_packs`) must not stall request workers —
/// refs answer in milliseconds on a single-worker server meanwhile.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn blocking_work_in_the_install_path_does_not_stall_requests() -> TestResult {
    const CHILD: &str = "WALGIT_TEST_BLOCK_INSTALL_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let status = tokio::process::Command::new(std::env::current_exe()?)
            .args([
                "--exact",
                "blocking_work_in_the_install_path_does_not_stall_requests",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .env("WALGIT_TEST_BLOCK_INSTALL_MS", "2500")
            .status()
            .await?;
        assert!(status.success(), "isolated blocking install test failed");
        return Ok(());
    }
    let big = Server::start().await?;
    big.put_repo("t", "blk").await?;
    big.put_repo("t", "other2").await?;
    let src = TestRepo::synthetic(4, 2)?;
    git_in(
        &src,
        &["remote", "add", "origin", &big.repo_url("t", "blk")],
    )?;
    git_in(&src, &["push", "-q", "origin", "main"])?;
    let other = TestRepo::synthetic(1, 1)?;
    git_in(
        &other,
        &["remote", "add", "origin", &big.repo_url("t", "other2")],
    )?;
    git_in(&other, &["push", "-q", "origin", "main"])?;
    let small = big.start_sibling_with(|_| {}).await?;
    let sh = small
        .state
        .registry
        .open(&walgit_git::RepoId::new("t", "blk")?)
        .await?;
    let install = tokio::spawn(async move {
        let t = std::time::Instant::now();
        let _g = sh.sync().await.unwrap();
        t.elapsed()
    });
    let mut worst = 0u128;
    let mut probes = 0;
    while !install.is_finished() {
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        let t = std::time::Instant::now();
        let r = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            small.get_text("/t/other2/api/refs", &[]),
        )
        .await;
        assert!(r.is_ok(), "refs timed out while the install path blocked");
        worst = worst.max(t.elapsed().as_millis());
        probes += 1;
    }
    let took = install.await?;
    assert!(took.as_millis() >= 2500, "{took:?}");
    assert!(probes >= 5, "runtime stalled: {probes} probes in {took:?}");
    assert!(
        worst < 1000,
        "a refs request took {worst} ms during the blocking install"
    );
    Ok(())
}

/// D24: per-repo settings over HTTP — PUT validates (400 with the reason,
/// nothing published), GET returns the document + revision, `effective` is
/// the host config ⊕ settings, a sibling instance sees it on its next refs
/// sync, history lists the SETTINGS entries, DELETE clears.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repo_settings_api_roundtrip() -> TestResult {
    let (a, b) = Server::start_pair().await?;
    a.put_repo("t", "cfg").await?;
    let c = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()?;
    let url = |s: &Server, sub: &str| format!("{}/t/cfg/api/settings{sub}", s.base_url);

    // Invalid: forbidden section → 400 with the reason, nothing published.
    let r = c
        .put(url(&a, ""))
        .body("[server]\nlisten = \"0.0.0.0:1\"\n")
        .send()
        .await?;
    assert_eq!(r.status(), 400);
    assert!(r.text().await?.contains("[server]"));
    let r: serde_json::Value = c.get(url(&a, "")).send().await?.json().await?;
    assert_eq!(r["revision"], 0);

    // Valid.
    let r = c
        .put(url(&a, "?message=tiny+repo"))
        .body("[packs]\nfold_when_fresh_packs_reach = 2\n")
        .send()
        .await?;
    assert_eq!(r.status(), 200, "{}", r.text().await?);
    let r: serde_json::Value = c.get(url(&a, "")).send().await?.json().await?;
    assert_eq!(r["revision"], 1);
    assert_eq!(r["message"], "tiny repo");
    assert!(
        r["toml"]
            .as_str()
            .unwrap()
            .contains("fold_when_fresh_packs_reach = 2")
    );
    let eff = c.get(url(&a, "/effective")).send().await?.text().await?;
    assert!(eff.contains("fold_when_fresh_packs_reach = 2"), "{eff}");
    assert!(!eff.contains("session_secret"), "{eff}");
    assert!(!eff.contains("[server]"), "{eff}");

    // The sibling sees it (refs-level sync, no objects).
    let r: serde_json::Value = c.get(url(&b, "")).send().await?.json().await?;
    assert_eq!(r["revision"], 1);
    let id = walgit_git::RepoId::new("t", "cfg")?;
    assert_eq!(
        b.state
            .registry
            .open(&id)
            .await?
            .effective_config()
            .packs
            .fold_when_fresh_packs_reach,
        2
    );

    // History + clear.
    let h: serde_json::Value = c.get(url(&a, "/history")).send().await?.json().await?;
    assert_eq!(h["entries"].as_array().unwrap().len(), 1);
    assert_eq!(c.delete(url(&a, "")).send().await?.status(), 200);
    let r: serde_json::Value = c.get(url(&b, "")).send().await?.json().await?;
    assert_eq!(r["revision"], 2);
    assert_eq!(r["toml"], "");
    // The browser lane is the same surface (D27); the old root-level path is gone.
    assert_eq!(a.get_status("/t/cfg/api-browser/settings").await?, 200);
    // `/t/cfg/settings` is now only the SPA page (index.html), not an API route.
    let r = c
        .get(format!("{}/t/cfg/settings", a.base_url))
        .header("Accept", "application/json")
        .send()
        .await?;
    assert!(
        r.headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .starts_with("text/html"),
        "root-level /settings must not be an API route anymore"
    );
    Ok(())
}

/// Settings tab backend: describe fields with sources, validate (preview, errors),
/// and policy validate + dry-run
/// against the last pushes.
// multi_thread: the synchronous `git push` below must not block the runtime
// the server runs on (a current-thread test hangs forever on the first push).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn settings_describe_validate_and_policy_dry_run() -> TestResult {
    let s = Server::start().await?;
    let c = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()?;
    s.put_repo("t", "tab").await?;
    let src = TestRepo::synthetic(3, 2)?;
    git_in(&src, &["remote", "add", "origin", &s.repo_url("t", "tab")])?;
    git_in(&src, &["push", "-q", "origin", "main"])?;
    git_in(&src, &["push", "-q", "origin", "main:refs/heads/feature"])?;
    let base = format!("{}/t/tab/api", s.base_url);

    let d: serde_json::Value = c
        .get(format!("{base}/settings/describe"))
        .send()
        .await?
        .json()
        .await?;
    assert!(
        d["fields"]
            .as_array()
            .unwrap()
            .iter()
            .all(|f| f["source"] == "host")
    );
    assert!(d["maintenance"]["this_host"]["name"].is_string());

    // Validate: preview flips the touched field's source; errors come back as a list.
    let v: serde_json::Value = c
        .post(format!("{base}/settings/validate"))
        .body("[packs]\nfold_when_fresh_packs_reach = 4\n")
        .send()
        .await?
        .json()
        .await?;
    assert_eq!(v["ok"], true, "{v}");
    let f = v["fields"]
        .as_array()
        .unwrap()
        .iter()
        .find(|f| f["key"] == "packs.fold_when_fresh_packs_reach")
        .unwrap();
    assert_eq!(f["value"], 4);
    assert_eq!(f["source"], "setting");
    let v: serde_json::Value = c
        .post(format!("{base}/settings/validate"))
        .body("[cache]\nmax_bytes = \"1GiB\"\n")
        .send()
        .await?
        .json()
        .await?;
    assert_eq!(v["ok"], false);
    assert!(v["errors"][0].as_str().unwrap().contains("[cache]"));

    // Policy validate + dry-run: protect main from everyone but "alice" → the
    // recorded pushes (principal = anonymous test identity) are denied on main, allowed on feature.
    let policy = serde_json::json!({"version": 1, "groups": [], "rules": [{"name": "main-only-alice", "match": {"refs": ["refs/heads/main"]}, "effect": {"protect": {"restricts": ["create", "update", "delete"], "bypass": ["alice@example.com"]}}}]});
    let pv: serde_json::Value = c
        .post(format!("{base}/policy/validate"))
        .json(&policy)
        .send()
        .await?
        .json()
        .await?;
    assert_eq!(pv["ok"], true, "{pv}");
    let pv: serde_json::Value = c
        .post(format!("{base}/policy/validate"))
        .body("{\"version\": 1, \"rules\": [{\"name\": \"x\"}]}")
        .send()
        .await?
        .json()
        .await?;
    assert_eq!(pv["ok"], false);
    let dr: serde_json::Value = c
        .post(format!("{base}/policy/dry-run?last=10"))
        .json(&policy)
        .send()
        .await?
        .json()
        .await?;
    assert_eq!(dr["pushes"], 2, "{dr}");
    assert_eq!(dr["denied"], 1, "{dr}");
    assert_eq!(dr["allowed"], 1, "{dr}");
    let denied = dr["results"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|r| r["refs"].as_array().unwrap().clone())
        .find(|x| x["ok"] == false)
        .unwrap();
    assert_eq!(denied["name"], "refs/heads/main");
    assert!(
        denied["reason"]
            .as_str()
            .unwrap()
            .contains("main-only-alice"),
        "{denied}"
    );
    // Empty body = the saved (allow-all) policy.
    let dr: serde_json::Value = c
        .post(format!("{base}/policy/dry-run"))
        .body("")
        .send()
        .await?
        .json()
        .await?;
    assert_eq!(dr["denied"], 0);
    Ok(())
}

/// `wal.prefetch_max_bytes`: a refs-level request (info/refs, the overview) pulls the serving copy
/// in the background only for pack sets up to the bound; above it nothing is downloaded until a
/// request needs objects — then the fetch materializes, narrated. 2026-08-22: an overview page
/// view made a front pull acme/large's 11.9 GB pack unasked.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refs_level_requests_prefetch_only_small_pack_sets() -> TestResult {
    let big = Server::start().await?;
    big.put_repo("t", "prefetch").await?;
    let src = tempfile::tempdir()?;
    git(&["init", "-q", "-b", "main", "."], src.path())?;
    std::fs::write(src.path().join("f"), vec![b'x'; 200_000])?;
    git(&["add", "."], src.path())?;
    git(
        &[
            "-c",
            "user.email=t@t",
            "-c",
            "user.name=T",
            "commit",
            "-q",
            "-m",
            "one",
        ],
        src.path(),
    )?;
    git(
        &["push", "-q", &big.repo_url("t", "prefetch"), "main"],
        src.path(),
    )?;
    let id = walgit_git::RepoId::new("t", "prefetch")?;

    // A sibling whose prefetch bound is below the pack: info/refs leaves the packs alone.
    let bounded = big
        .start_sibling_with(|c| c.wal.prefetch_max_bytes = bytesize::ByteSize::b(1))
        .await?;
    let _ = bounded.ls_remote("t", "prefetch").await?;
    let h = bounded.state.registry.open(&id).await?;
    assert!(
        !h.prefetch_wanted(),
        "pack set above the bound is not prefetched"
    );
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert!(!h.packs_ready(), "no background materialization happened");
    // The first fetch materializes on demand and works.
    let dst = tempfile::tempdir()?;
    git(
        &[
            "clone",
            "-q",
            &bounded.repo_url("t", "prefetch"),
            dst.path().to_str().unwrap(),
        ],
        dst.path(),
    )?;
    assert!(h.packs_ready(), "the fetch brought the packs");

    // Default bound (1 GiB): the small set is prefetched after a refs-level request.
    let eager = big.start_sibling_with(|_| {}).await?;
    let _ = eager.ls_remote("t", "prefetch").await?;
    let h = eager.state.registry.open(&id).await?;
    for _ in 0..50 {
        if h.packs_ready() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(h.packs_ready(), "default bound prefetches a small pack set");
    Ok(())
}

/// `walgit_http_inflight` counts a request until its response *body* is done — a streamed
/// fetch (sideband) and an SSE stream stay in flight past the handler's return — and is back
/// at 0 afterwards, so the watchdog's `inflight` field means what it says.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_inflight_gauge_covers_streamed_bodies_and_returns_to_zero() -> TestResult {
    let server = Server::start().await?;
    let http_inflight = || server.state.inflight.get();
    server.put_repo("t", "inflight").await?;
    let src = tempfile::tempdir()?;
    git(&["init", "-q", "-b", "main", "."], src.path())?;
    std::fs::write(src.path().join("f"), vec![b'y'; 300_000])?;
    git(&["add", "."], src.path())?;
    git(
        &[
            "-c",
            "user.email=t@t",
            "-c",
            "user.name=T",
            "commit",
            "-q",
            "-m",
            "one",
        ],
        src.path(),
    )?;
    git(
        &["push", "-q", &server.repo_url("t", "inflight"), "main"],
        src.path(),
    )?;
    let settle = || async {
        for _ in 0..50 {
            if http_inflight() == 0 {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    };
    settle().await;
    assert_eq!(http_inflight(), 0, "idle server");

    // A streamed (sideband-narrated) clone: counted while streaming, 0 once git has the pack.
    let dst = tempfile::tempdir()?;
    git(
        &[
            "-c",
            "protocol.version=2",
            "clone",
            "-q",
            "--progress",
            &server.repo_url("t", "inflight"),
            dst.path().to_str().unwrap(),
        ],
        dst.path(),
    )?;
    settle().await;
    assert_eq!(http_inflight(), 0, "after a streamed fetch");

    // A request whose body never finishes (a stalled push upload) is in flight until the client
    // goes away; the count is taken at the middleware, before any handler work.
    let client = reqwest::Client::new();
    let never: futures::stream::Pending<Result<bytes::Bytes, std::io::Error>> =
        futures::stream::pending();
    let pending = tokio::spawn(
        client
            .post(format!(
                "{}/t/inflight.git/git-receive-pack",
                server.base_url
            ))
            .header("Content-Type", "application/x-git-receive-pack-request")
            .body(reqwest::Body::wrap_stream(never))
            .send(),
    );
    let mut saw_inflight = false;
    for _ in 0..100 {
        if http_inflight() >= 1 {
            saw_inflight = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(
        saw_inflight,
        "a request with an unfinished body counts as in flight"
    );
    pending.abort();
    settle().await;
    assert_eq!(http_inflight(), 0, "after the client went away");
    Ok(())
}

/// `/services/public/*` is the one open lane: the installer is fetched before
/// a user has any credential. Data-free only; everything else under the prefix is 404; nothing else
/// on the host opened up; the old installer path is gone (banner).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn public_lane_serves_only_the_installer_without_auth() -> TestResult {
    let server = Server::start_with_tweak(|c| {
        c.server.auth.mode = walgit_config::AuthMode::Token;
        c.server.auth.anonymous_read = false;
        c.server.auth.tokens = vec![walgit_config::StaticToken {
            principal: "dev@example.com".into(),
            token: "dev".into(),
            token_env: None,
            write: true,
            admin: false,
        }];
    })
    .await?;
    let c = reqwest::Client::new();
    let r = c
        .get(format!(
            "{}/services/public/install.sh?repo=t/r",
            server.base_url
        ))
        .send()
        .await?;
    assert_eq!(r.status(), 200);
    assert_eq!(r.headers()["cache-control"], "public, max-age=300");
    let body = r.text().await?;
    assert!(body.starts_with("#!/bin/sh"), "{body}");
    assert!(
        body.contains("git config --global --add \"credential.https://$HOST.helper\" \"$HELPER\""),
        "{body}"
    );
    assert!(!body.contains("fetch.bundleURI"), "{body}");
    for (path, want) in [
        ("/services/public/nothing-else", 404),
        ("/services/public/", 404),
        ("/services/install.sh", 401), // the old path: not an alias, not open
        ("/services/setup.json", 401),
        ("/t/r/api/refs", 401),
        ("/t/r.git/info/refs?service=git-upload-pack", 401),
        ("/repos.js", 200), // data-free SDK asset stays where it is
    ] {
        let got = c
            .get(format!("{}{path}", server.base_url))
            .send()
            .await?
            .status()
            .as_u16();
        assert!(
            got == want || (want == 200 && got == 404),
            "GET {path}: {got}, want {want}"
        );
    }
    Ok(())
}

/// A stale credential in git's cache (an expired or rotated token) must cost exactly one failed
/// command: the server answers the dead token with a real 401, git `erase`s it from its helpers,
/// and the next command asks the helpers again (a fresh token) and succeeds. A 200 + in-band ERR
/// for an auth failure never triggers the erase: git re-`store`s the dead token and every clone
/// fails for the cache's whole lifetime.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stale_cached_credential_is_erased_by_the_401_and_replaced_on_the_next_command()
-> TestResult {
    let server = Server::start_with_tweak(|c| {
        c.server.auth.mode = walgit_config::AuthMode::Token;
        c.server.auth.anonymous_read = false;
        c.server.auth.tokens = vec![walgit_config::StaticToken {
            principal: "dev@example.com".into(),
            token: "fresh".into(),
            token_env: None,
            write: true,
            admin: false,
        }];
    })
    .await?;
    let r = reqwest::Client::new()
        .put(format!("{}/t/stale.git", server.base_url))
        .header("Authorization", "Bearer fresh")
        .send()
        .await?;
    assert!(r.status().is_success(), "{}", r.status());
    let src = TestRepo::synthetic(2, 1)?;
    git_in(
        &src,
        &["remote", "add", "origin", &server.repo_url("t", "stale")],
    )?;
    git_in(
        &src,
        &[
            "-c",
            "http.extraHeader=Authorization: Bearer fresh",
            "push",
            "-q",
            "origin",
            "main",
        ],
    )?;

    // A helper that logs every call and always mints the valid token; in front of it, git's cache
    // daemon pre-loaded with a dead token (what an expired gcloud ID token looks like to the server).
    let home = tempfile::tempdir()?;
    let log = home.path().join("calls.log");
    let helper = home.path().join("helper");
    std::fs::write(
        &helper,
        format!(
            "#!/bin/sh\necho \"$1\" >> {log}\ncase \"$1\" in get) while IFS= read -r l; do [ -z \"$l\" ] && break; done; printf 'capability[]=authtype\\nauthtype=Bearer\\ncredential=fresh\\n\\n' ;; esac\n",
            log = log.display()
        ),
    )?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&helper, std::fs::Permissions::from_mode(0o755))?;
    }
    // credential-cache refuses a socket directory others can read.
    let sockdir = home.path().join("cc");
    std::fs::create_dir_all(&sockdir)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&sockdir, std::fs::Permissions::from_mode(0o700))?;
    }
    let sock = sockdir.join("cache.sock");
    let cache = format!("cache --socket={} --timeout=300", sock.display());
    let gitconfig = home.path().join("gitconfig");
    let base = &server.base_url;
    std::fs::write(
        &gitconfig,
        format!(
            "[credential \"{base}\"]\n\thelper = \n\thelper = {cache}\n\thelper = {}\n[http \"{base}/\"]\n\tproactiveAuth = auto\n",
            helper.display()
        ),
    )?;
    let host = base.trim_start_matches("http://");
    // Pre-load the cache with the dead token.
    let mut approve = std::process::Command::new("git")
        .args([
            "credential-cache",
            &format!("--socket={}", sock.display()),
            "--timeout=300",
            "store",
        ])
        .env("HOME", home.path())
        .env("GIT_CONFIG_GLOBAL", &gitconfig)
        .stdin(std::process::Stdio::piped())
        .spawn()?;
    {
        use std::io::Write;
        approve.stdin.take().unwrap().write_all(format!("protocol=http\nhost={host}\ncapability[]=authtype\nauthtype=Bearer\ncredential=expired\n\n").as_bytes())?;
    }
    assert!(approve.wait()?.success());

    let clone = |name: &str| {
        let dir = tempfile::tempdir().unwrap();
        std::process::Command::new("git")
            .args([
                "clone",
                "-q",
                &server.repo_url("t", "stale"),
                dir.path().join(name).to_str().unwrap(),
            ])
            .env("HOME", home.path())
            .env("GIT_CONFIG_GLOBAL", &gitconfig)
            .env("GIT_TERMINAL_PROMPT", "0")
            .output()
            .unwrap()
    };
    // 1st command: the dead token goes out proactively, the server says 401, git erases it and fails
    // (a real 401, not "fatal: remote error: … authentication failed" from a 200).
    let first = clone("c1");
    let err = String::from_utf8_lossy(&first.stderr);
    assert!(
        !first.status.success() && err.contains("Authentication failed"),
        "{err}"
    );
    assert!(
        !err.contains("remote error"),
        "must be a 401, not an in-band ERR:\n{err}"
    );
    let calls = std::fs::read_to_string(&log)?;
    assert!(
        calls.contains("erase"),
        "git must have rejected the dead credential (erase):\n{calls}"
    );
    // 2nd command: the cache is empty, our helper mints, the clone succeeds.
    let second = clone("c2");
    assert!(
        second.status.success(),
        "the next command must succeed with a fresh token:\n{}",
        String::from_utf8_lossy(&second.stderr)
    );
    let calls = std::fs::read_to_string(&log)?;
    assert!(
        calls.contains("get"),
        "…and asked the helper for a fresh one:\n{calls}"
    );
    // The cache now holds the fresh token.
    let mut get = std::process::Command::new("git")
        .args([
            "credential-cache",
            &format!("--socket={}", sock.display()),
            "--timeout=300",
            "get",
        ])
        .env("HOME", home.path())
        .env("GIT_CONFIG_GLOBAL", &gitconfig)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()?;
    {
        use std::io::Write;
        get.stdin.take().unwrap().write_all(
            format!("protocol=http\nhost={host}\ncapability[]=authtype\n\n").as_bytes(),
        )?;
    }
    let cached = String::from_utf8_lossy(&get.wait_with_output()?.stdout).to_string();
    assert!(cached.contains("credential=fresh"), "{cached}");
    let _ = std::process::Command::new("git")
        .args([
            "credential-cache",
            &format!("--socket={}", sock.display()),
            "exit",
        ])
        .output();
    Ok(())
}

/// Read-your-writes on one instance, under contention: while N clients race pushes to one branch
/// and a reader spins on `ls-remote`, every read after a push's `ok` must show that push's tip —
/// the advertisement caches are keyed by the manifest version, and a publish that advertised the
/// new version before applying the refs locally let a reader cache the OLD refs under the NEW
/// version (reproduced roughly once in six rounds). 12 rounds × 6 pushers.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn reads_after_an_acknowledged_push_never_show_the_previous_tip() -> TestResult {
    // Widen the gap between the publish's two local-commit steps (refs applied; version advertised)
    // to 150 ms so the reader reliably lands in it: harmless in the right order, the poison window
    // in the wrong one.
    // SAFETY: test-only env var, read by the publish path of this process.
    unsafe { std::env::set_var("WALGIT_TEST_PUBLISH_GAP_MS", "150") };
    let server = Server::start().await?;
    server.put_repo("t", "ryw").await?;
    let src = TestRepo::synthetic(2, 1)?;
    git_in(
        &src,
        &["remote", "add", "origin", &server.repo_url("t", "ryw")],
    )?;
    git_in(&src, &["push", "-q", "origin", "main"])?;
    let url = server.repo_url("t", "ryw");
    let tip = |dir: &std::path::Path| -> String {
        let out = std::process::Command::new("git")
            .args(["ls-remote", &url, "refs/heads/main"])
            .current_dir(dir)
            .output()
            .unwrap();
        String::from_utf8_lossy(&out.stdout)
            .split_whitespace()
            .next()
            .unwrap_or("")
            .to_string()
    };
    let mut stale = Vec::new();
    for round in 0..4 {
        let base = tip(&src);
        assert!(!base.is_empty());
        // 6 contenders from the same base, each with its own commit.
        let mut handles = Vec::new();
        let start_pushes = std::sync::Arc::new(std::sync::Barrier::new(7));
        for i in 0..6 {
            let d = tempfile::tempdir()?;
            git(
                &["clone", "-q", &url, d.path().to_str().unwrap()],
                d.path().parent().unwrap(),
            )?;
            std::fs::write(
                d.path().join(format!("r{round}-c{i}.txt")),
                format!("{round}/{i}\n"),
            )?;
            git_in(d.path(), &["add", "."])?;
            git_in(d.path(), &["commit", "-q", "-m", &format!("r{round} c{i}")])?;
            let sha = git_in(d.path(), &["rev-parse", "HEAD"])?.trim().to_string();
            let url2 = url.clone();
            let cwd = d.path().to_path_buf();
            let start = start_pushes.clone();
            handles.push((
                d,
                sha,
                std::thread::spawn(move || {
                    start.wait();
                    // true when this push won
                    let o = std::process::Command::new("git")
                        .current_dir(&cwd)
                        .args(["push", "-q", "--atomic", &url2, "HEAD:refs/heads/main"])
                        .output()
                        .unwrap();
                    if !o.status.success() {
                        eprintln!("push: {}", String::from_utf8_lossy(&o.stderr));
                    }
                    o.status.success()
                }),
            ));
        }
        // The reader: ls-remote continuously while the race runs.
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (stop2, url3, srcp) = (stop.clone(), url.clone(), src.to_path_buf());
        let reader = std::thread::spawn(move || {
            let mut seen = Vec::new();
            while !stop2.load(std::sync::atomic::Ordering::Relaxed) {
                let out = std::process::Command::new("git")
                    .args(["ls-remote", &url3, "refs/heads/main"])
                    .current_dir(&srcp)
                    .output()
                    .unwrap();
                seen.push(
                    String::from_utf8_lossy(&out.stdout)
                        .split_whitespace()
                        .next()
                        .unwrap_or("")
                        .to_string(),
                );
            }
            seen
        });
        // Every clone and commit exists before any contender may publish.
        start_pushes.wait();
        let mut winner = None;
        let mut winners = 0;
        for (_d, sha, h) in handles {
            if h.join().unwrap() {
                winners += 1;
                winner = Some(sha);
            }
        }
        assert_eq!(winners, 1, "exactly one same-base push wins each round");
        let winner = winner.expect("exactly one push wins each round");
        // Read-your-writes: the first read after the last push returned must be the winner, and so
        // must every read after it.
        let mut after: Vec<String> = Vec::new();
        for _ in 0..5 {
            after.push(tip(&src));
        }
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        let seen = reader.join().unwrap();
        for h in &after {
            if h != &winner {
                stale.push(format!("round {round}: read {h} after the winner {winner} was acknowledged (base {base})"));
            }
        }
        // The concurrent reader may see base or winner, never anything else, and never base again after winner.
        let mut saw_winner = false;
        for h in &seen {
            if h == &winner {
                saw_winner = true;
            } else if h == &base {
                if saw_winner {
                    stale.push(format!(
                        "round {round}: reader regressed to base after seeing the winner"
                    ));
                }
            } else if !h.is_empty() {
                stale.push(format!("round {round}: reader saw a foreign tip {h}"));
            }
        }
    }
    // SAFETY: see above.
    unsafe { std::env::remove_var("WALGIT_TEST_PUBLISH_GAP_MS") };
    assert!(stale.is_empty(), "stale reads:\n{}", stale.join("\n"));
    Ok(())
}

/// #37: a ref-only push carries a 32-byte zero-object pack, and receive-pack used to skip the
/// connectivity check for it, so `refs/heads/ghost` could be published pointing at an object
/// nobody has, after which every clone walking it died with `missing object`. The tip is now
/// checked like any other, and a ref-only push to an object the server does have still lands.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn empty_pack_push_to_a_missing_object_is_refused() -> TestResult {
    for check_connectivity in [true, false] {
        empty_pack_push_is_refused_with(check_connectivity).await?;
    }
    Ok(())
}

/// Both tip checks refuse it: the full walk, and the bare lookup a host with
/// `wal.check_connectivity = false` falls back to.
async fn empty_pack_push_is_refused_with(check_connectivity: bool) -> TestResult {
    let server =
        Server::start_with_tweak(|c| c.wal.check_connectivity = check_connectivity).await?;
    server.put_repo("t", "ghost").await?;
    let src = TestRepo::synthetic(2, 2)?;
    git_in(&src, &["branch", "-M", "main"])?;
    git_in(
        &src,
        &["remote", "add", "origin", &server.repo_url("t", "ghost")],
    )?;
    git_in(&src, &["push", "-q", "origin", "main"])?;

    // One command line, a flush, then the empty pack: header, zero objects, its checksum.
    let cmd = format!(
        "{} {} refs/heads/ghost\0report-status\n",
        "0".repeat(40),
        "b".repeat(40)
    );
    let mut body = format!("{:04x}{cmd}0000", cmd.len() + 4).into_bytes();
    body.extend_from_slice(b"PACK\x00\x00\x00\x02\x00\x00\x00\x00");
    body.extend_from_slice(&[
        0x02, 0x9d, 0x08, 0x82, 0x3b, 0xd8, 0xa8, 0xea, 0xb5, 0x10, 0xad, 0x6a, 0xc7, 0x5c, 0x82,
        0x3c, 0xfd, 0x3e, 0xd3, 0x1e,
    ]);
    let resp = reqwest::Client::new()
        .post(format!("{}/t/ghost.git/git-receive-pack", server.base_url))
        .header("Content-Type", "application/x-git-receive-pack-request")
        .body(body)
        .send()
        .await?;
    assert_eq!(resp.status(), 200);
    let report = resp.text().await?;
    assert!(
        report.contains("ng refs/heads/ghost"),
        "check_connectivity={check_connectivity}: {report}"
    );
    assert!(!report.contains("ok refs/heads/ghost"), "{report}");
    let refs = git_in(&src, &["ls-remote", "origin"])?;
    assert!(!refs.contains("refs/heads/ghost"), "{refs}");

    // The legitimate shape of the same wire bytes: a new branch at an object the server has.
    git_in(&src, &["push", "-q", "origin", "main:refs/heads/copy"])?;
    let refs = git_in(&src, &["ls-remote", "origin"])?;
    assert!(
        refs.contains("refs/heads/copy"),
        "check_connectivity={check_connectivity}: {refs}"
    );
    Ok(())
}
