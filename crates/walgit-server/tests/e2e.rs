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
async fn bundle_uri_clone_fetches_server_bundle() -> TestResult {
    let server = Server::start().await?;
    server.put_repo("t", "bundled").await?;
    let src = TestRepo::synthetic(3, 2)?;
    git_in(&src, &["branch", "-M", "main"])?;
    git_in(
        &src,
        &["remote", "add", "origin", &server.repo_url("t", "bundled")],
    )?;
    git_in(&src, &["push", "origin", "main"])?;
    server.build_bundle("t", "bundled", "weekly").await?;

    // The WAL page shows exactly the URI the bundle list advertises, and it downloads.
    let bundle_list = format!("{}/t/bundled/bundles/list", server.base_url);
    let list_text = reqwest::get(&bundle_list).await?.text().await?;
    let overview: serde_json::Value =
        reqwest::get(format!("{}/t/bundled/api/overview", server.base_url))
            .await?
            .json()
            .await?;
    let ui_uri = overview["bundles"][0]["uri"]
        .as_str()
        .expect("overview bundle uri")
        .to_string();
    assert!(
        list_text.contains(&format!("uri = {ui_uri}")),
        "overview uri {ui_uri} not in list:\n{list_text}"
    );
    let head = reqwest::Client::new().head(&ui_uri).send().await?;
    assert_eq!(
        head.status(),
        200,
        "overview bundle uri must be downloadable"
    );

    let clone_dir = tempfile::tempdir()?;
    git(
        &[
            "-c",
            "transfer.bundleURI=true",
            "clone",
            "--branch",
            "main",
            "--bundle-uri",
            &bundle_list,
            &server.repo_url("t", "bundled"),
            clone_dir.path().to_str().unwrap(),
        ],
        clone_dir.path().parent().unwrap(),
    )?;
    assert_eq!(
        git_in(clone_dir.path(), &["rev-parse", "main"])?.trim(),
        git_in(&src, &["rev-parse", "main"])?.trim()
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bundle_endpoints_when_landed() -> TestResult {
    let server = Server::start().await?;
    server.put_repo("t", "r").await?;
    // The bundle list endpoint should respond (404 no-bundles is acceptable
    // until the bundler has produced entries; never a 5xx).
    let status = server.get_status("/t/r.git/bundles/list").await?;
    assert!(status.as_u16() < 500, "bundle list returned {status}");
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
/// still answer every refs-level request (ls-remote v0/v2, bundle list, web
/// refs) from the WAL ref snapshot, refuse object work with a readable
/// pkt-line ERR pointing at bundle-uri, and never pull the packs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn huge_repo_front_serves_refs_and_bundles_without_packs() -> TestResult {
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
    big.build_bundle("t", "huge", "weekly").await?;

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
    // Bundle list + web refs work.
    let list = reqwest::get(format!("{}/t/huge/bundles/list", small.base_url)).await?;
    assert_eq!(list.status(), 200);
    assert!(list.text().await?.contains("version = 1"));
    let refs = reqwest::get(format!("{}/t/huge/api/refs", small.base_url)).await?;
    assert_eq!(refs.status(), 200);
    let refs: serde_json::Value = refs.json().await?;
    assert_eq!(refs["head"]["sha"], head);
    let resolved = reqwest::get(format!("{}/t/huge/api/resolve/main", small.base_url)).await?;
    assert_eq!(resolved.status(), 200);

    // Object work is refused with the bundle-uri hint, not a crash/OOM.
    let clone_dir = tempfile::tempdir()?;
    let err = git(
        &[
            "-c",
            "transfer.bundleURI=false",
            "clone",
            &url,
            clone_dir.path().to_str().unwrap(),
        ],
        clone_dir.path().parent().unwrap(),
    )
    .unwrap_err()
    .to_string();
    assert!(
        err.contains("transfer.bundleURI"),
        "clone error should explain bundle-uri: {err}"
    );
    assert!(err.contains("larger than this instance"), "{err}");
    // The small front never downloaded the pack set.
    assert!(
        !small.registry_has_packs("t", "huge").await,
        "small front must not materialize packs"
    );

    // And with bundle-uri the clone gets its objects from the bundle; the final
    // fetch against the small front is the part that still needs objects (today it
    // fails the same way), so clone via the big front for the bytes and check the
    // small front only rendered refs.
    let clone2 = tempfile::tempdir()?;
    git(
        &[
            "-c",
            "transfer.bundleURI=true",
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
/// auth, WAL seq, bundle facts, local copy state — before upload-pack's own
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
    // must not be narrated — a large repository clone whose bundles cover the tips therefore shows no
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
/// cloned from a big front (stand-in for bundle-uri) and fetches a later push
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
        &big.state.cfg,
        walgit_server::ops::CompactRequest {
            force: true,
            rebuild_base: true,
        },
        &log,
    )
    .await?;
    println!("{}", out.summary());
    let m = handle.manifest();
    let base = m.packs.iter().find(|p| p.tier == 2).expect("a tier-2 base");
    assert!(
        base.has_commit_graph,
        "base carries a commit-graph layer: {base:?}"
    );
    let base_tip = git_in(&src, &["rev-parse", "main"])?.trim().to_string();

    // Weekly full bundle at the base (what the VM job's compose would give).
    big.build_bundle("t", "rbase", "weekly").await?;
    // Client: full clone from the big front (what bundle-uri would give it).
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
        .start_sibling_with(|c| c.cache.max_bytes = bytesize::ByteSize::b(base.pack_size / 2))
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
    assert_eq!(sh.remote_served(), vec![base.checksum.clone()]);
    assert!(
        !sh.local()
            .pack_path(&gix_hash::ObjectId::from_hex(base.checksum.as_bytes())?)
            .exists()
    );

    // Incremental bundle built on the small front with the gix engine (the
    // weekly full one exists from the big front): verifies and applies on a
    // clone that has the base.
    let entry = small.state.bundles.build(&id, "daily").await?;
    assert_eq!(entry.kind, "incremental");
    let bundle_bytes = reqwest::get(format!(
        "{}/t/rbase.git/bundles/{}",
        small.base_url,
        entry.key.trim_start_matches("bundles/")
    ))
    .await?;
    assert_eq!(bundle_bytes.status(), 200, "{}", entry.key);
    let bundle_path = clone.path().join("daily.bundle");
    std::fs::write(&bundle_path, bundle_bytes.bytes().await?)?;
    let verify = std::process::Command::new("git")
        .current_dir(clone.path())
        .args(["bundle", "verify", bundle_path.to_str().unwrap()])
        .output()?;
    assert!(
        verify.status.success(),
        "{}",
        String::from_utf8_lossy(&verify.stderr)
    );
    let out = std::process::Command::new("git")
        .current_dir(clone.path())
        .args(["bundle", "list-heads", bundle_path.to_str().unwrap()])
        .output()?;
    assert!(
        String::from_utf8_lossy(&out.stdout).contains(&new_tip),
        "{}",
        String::from_utf8_lossy(&out.stdout)
    );

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

/// `bundles.require`: on a listed repository a fetch with zero haves (a clone
/// that skipped bundle-uri) is refused with the exact fix — v2 (band 3 when the
/// client accepts sideband, pkt ERR otherwise) and v0 — while clones through
/// bundle-uri and ordinary fetches with haves proceed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bundles_require_refuses_zero_have_fetches_with_the_fix() -> TestResult {
    let server = Server::start_with_tweak(|c| c.bundles.require = vec!["t/*".into()]).await?;
    server.put_repo("t", "req").await?;
    let src = TestRepo::synthetic(3, 2)?;
    git_in(
        &src,
        &["remote", "add", "origin", &server.repo_url("t", "req")],
    )?;
    git_in(&src, &["push", "-q", "origin", "main"])?;
    server.build_bundle("t", "req", "weekly").await?;
    let url = server.repo_url("t", "req");

    for proto in ["2", "0"] {
        let dir = tempfile::tempdir()?;
        let out = std::process::Command::new("git")
            .args([
                "-c",
                &format!("protocol.version={proto}"),
                "-c",
                "transfer.bundleURI=false",
                "clone",
                &url,
                dir.path().to_str().unwrap(),
            ])
            .output()?;
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            !out.status.success(),
            "v{proto} clone without bundle-uri must be refused:\n{stderr}"
        );
        assert!(
            stderr.contains("transfer.bundleURI true"),
            "v{proto} fix missing:\n{stderr}"
        );
        assert!(stderr.contains("served from static bundles"), "{stderr}");
    }

    // D16: bounded zero-have fetches (CI's shallow / filtered clones) are served.
    for args in [
        vec!["--depth", "1"],
        vec!["--filter=blob:none"],
        vec!["--depth", "1", "--filter=blob:none", "--single-branch"],
    ] {
        let d = tempfile::tempdir()?;
        let mut cmd = vec!["-c", "transfer.bundleURI=false", "clone", "-q"];
        cmd.extend(args.iter().copied());
        cmd.push(&url);
        cmd.push(d.path().to_str().unwrap());
        let out = std::process::Command::new("git").args(&cmd).output()?;
        assert!(
            out.status.success(),
            "{args:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert_eq!(
            git_in(d.path(), &["rev-parse", "HEAD"])?.trim(),
            git_in(&src, &["rev-parse", "main"])?.trim()
        );
    }

    // Through bundle-uri: history from the bundle, the server fills the gap.
    let dir = tempfile::tempdir()?;
    let out = std::process::Command::new("git")
        .args([
            "-c",
            "transfer.bundleURI=true",
            "clone",
            &url,
            dir.path().to_str().unwrap(),
        ])
        .output()?;
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let head = git_in(&src, &["rev-parse", "main"])?;
    assert_eq!(git_in(dir.path(), &["rev-parse", "HEAD"])?, head);

    // A later fetch (haves present) works.
    std::fs::write(src.join("more.txt"), "more\n")?;
    git_in(&src, &["add", "more.txt"])?;
    git_in(&src, &["commit", "-q", "-m", "more"])?;
    git_in(&src, &["push", "-q", "origin", "main"])?;
    git_in(dir.path(), &["fetch", "-q", "origin"])?;
    assert_eq!(
        git_in(dir.path(), &["rev-parse", "origin/main"])?,
        git_in(&src, &["rev-parse", "main"])?
    );
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
            "-c",
            "transfer.bundleURI=false",
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
        stderr.contains("--filter=blob:none --sparse --bundle-uri="),
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
            "-c",
            "transfer.bundleURI=false",
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

/// D17 amendment: a principal that fetched the bundle list within the hour
/// TRIED bundle-uri (its zero-have fetch is a bundle download that failed —
/// git never retries one) and gets ONE upload-pack full clone per 6 h, with a
/// loud band-2 warning; the next one is refused with the truthful message; a
/// principal that never fetched the list is refused as before.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bundles_require_allows_one_upload_pack_fallback_after_a_failed_bundle_attempt()
-> TestResult {
    let server = Server::start_with_tweak(|c| {
        c.bundles.require = vec!["t/*".into()];
        c.server.auth.mode = walgit_config::AuthMode::Token;
        c.server.auth.anonymous_read = false;
        c.server.auth.tokens = vec![
            walgit_config::StaticToken {
                principal: "dev@example.com".into(),
                token: "dev".into(),
                token_env: None,
                write: true,
                admin: false,
            },
            walgit_config::StaticToken {
                principal: "other@example.com".into(),
                token: "other".into(),
                token_env: None,
                write: true,
                admin: false,
            },
        ];
    })
    .await?;
    let r = reqwest::Client::new()
        .put(format!("{}/t/fb.git", server.base_url))
        .header("Authorization", "Bearer dev")
        .send()
        .await?;
    assert!(r.status().is_success(), "{}", r.status());
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
    let url = server.repo_url("t", "fb");
    git(
        &[
            "-c",
            "http.extraHeader=Authorization: Bearer dev",
            "push",
            "-q",
            &url,
            "main",
        ],
        src.path(),
    )?;
    let clone = |token: &str| {
        let d = tempfile::tempdir().unwrap();
        let out = std::process::Command::new("git")
            .args([
                "-c",
                &format!("http.extraHeader=Authorization: Bearer {token}"),
                "-c",
                "transfer.bundleURI=false",
                "clone",
                "--progress",
                &url,
                d.path().to_str().unwrap(),
            ])
            .output()
            .unwrap();
        (
            out.status.success(),
            String::from_utf8_lossy(&out.stderr).to_string(),
        )
    };
    // Never fetched the list → refused.
    let (ok, err) = clone("dev");
    assert!(!ok && err.contains("served from static bundles"), "{err}");
    // "Tried" bundle-uri: fetched the list.
    let list = reqwest::Client::new()
        .get(format!("{}/t/fb.git/bundles/list", server.base_url))
        .header("Authorization", "Bearer dev")
        .send()
        .await?;
    assert!(
        list.status() == 200 || list.status() == 404,
        "{}",
        list.status()
    );
    // One fallback, loudly.
    let (ok, err) = clone("dev");
    assert!(ok, "fallback clone must succeed:\n{err}");
    assert!(
        err.contains("WARNING") && err.contains("upload-pack ONCE"),
        "{err}"
    );
    // The second within 6 h: refused, and the message says so.
    let (ok, err) = clone("dev");
    assert!(!ok && err.contains("you may have used it"), "{err}");
    // Another principal that never tried: refused.
    let (ok, err) = clone("other");
    assert!(!ok && err.contains("served from static bundles"), "{err}");
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
                "#!/bin/sh\nif [ \"$1\" = multi-pack-index ]; then sleep 3; fi\nexec {real_git} \"$@\"\n"
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
        &big.state.cfg,
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

/// Signed bundle URLs that cannot be produced because store signing is
/// unavailable or denied must fall back to proxy URIs — the
/// static list, the v2 `bundle-uri` command and a clone through bundle-uri all
/// work — and the fetch narrates each advertised bundle by relative path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn signed_url_failure_falls_back_to_proxy_uris_and_bundles_are_narrated() -> TestResult {
    let server = Server::start_with_store_and_tweak(
        {
            let mut s = walgit_store::memory::MemoryStore::new();
            s.signing_fails = true;
            std::sync::Arc::new(s)
        },
        |c| c.bundles.signed_url_for = vec!["t/*".into()],
    )
    .await?;
    server.put_repo("t", "sig").await?;
    let src = TestRepo::synthetic(3, 2)?;
    git_in(
        &src,
        &["remote", "add", "origin", &server.repo_url("t", "sig")],
    )?;
    git_in(&src, &["push", "-q", "origin", "main"])?;
    server.build_bundle("t", "sig", "weekly").await?;

    // Static list: 200 with proxy URIs, never a 500.
    let list = server.get_text("/t/sig.git/bundles/list", &[]).await?;
    assert!(
        list.contains(&format!("uri = {}/t/sig/bundles/weekly/", server.base_url)),
        "{list}"
    );

    // Clone through bundle-uri (v2 command + fetch of the bundle), narrated.
    let dir = tempfile::tempdir()?;
    let out = std::process::Command::new("git")
        .args([
            "-c",
            "protocol.version=2",
            "-c",
            "transfer.bundleURI=true",
            "clone",
            "--progress",
            &server.repo_url("t", "sig"),
            dir.path().to_str().unwrap(),
        ])
        .output()?;
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(out.status.success(), "{stderr}");
    assert_eq!(
        git_in(dir.path(), &["rev-parse", "HEAD"])?,
        git_in(&src, &["rev-parse", "main"])?
    );
    // A fetch with work narrates the bundles from the client's point of view: the list's shape per
    // strategy, and which bundles THIS git applied (its haves are their tips) with their bytes.
    std::fs::write(src.join("n.txt"), "n\n")?;
    git_in(&src, &["add", "n.txt"])?;
    git_in(&src, &["commit", "-q", "-m", "n"])?;
    git_in(&src, &["push", "-q", "origin", "main"])?;
    let out = std::process::Command::new("git")
        .args(["-c", "protocol.version=2", "fetch", "--progress", "origin"])
        .current_dir(dir.path())
        .output()?;
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(out.status.success(), "{stderr}");
    assert!(
        stderr.contains("* bundle-uri: 1 listed — 1 weekly ("),
        "list shape missing:\n{stderr}"
    );
    assert!(
        stderr.contains("* bundle-uri: your git applied 1 bundle(s) = ")
            && stderr.contains("(weekly) — history as of "),
        "applied-bundles line missing:\n{stderr}"
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
        .body("[bundles]\nmin_commits = 2\n")
        .send()
        .await?;
    assert_eq!(r.status(), 200, "{}", r.text().await?);
    let r: serde_json::Value = c.get(url(&a, "")).send().await?.json().await?;
    assert_eq!(r["revision"], 1);
    assert_eq!(r["message"], "tiny repo");
    assert!(r["toml"].as_str().unwrap().contains("min_commits = 2"));
    let eff = c.get(url(&a, "/effective")).send().await?.text().await?;
    assert!(eff.contains("min_commits = 2"), "{eff}");
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
            .bundles
            .min_commits,
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

/// Settings tab backend: describe (strategies with next fire + human schedule,
/// fields with sources), validate (preview, errors), policy validate + dry-run
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
    let strategies = d["strategies"].as_array().unwrap();
    assert!(!strategies.is_empty());
    let weekly = strategies.iter().find(|x| x["name"] == "weekly").unwrap();
    assert_eq!(weekly["kind"], "full");
    assert!(
        weekly["schedule_human"]
            .as_str()
            .unwrap()
            .contains("Sunday"),
        "{weekly}"
    );
    assert!(weekly["next"].is_string());
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
        .body("[bundles]\nmin_commits = 4\n")
        .send()
        .await?
        .json()
        .await?;
    assert_eq!(v["ok"], true, "{v}");
    let f = v["fields"]
        .as_array()
        .unwrap()
        .iter()
        .find(|f| f["key"] == "bundles.min_commits")
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

/// The north star's second half — fast catch-up through bundles — needs `fetch.bundleURI` on the
/// clone: git 2.51 records only `fetch.bundleCreationToken` for an advertised bundle-uri clone, so a
/// later `git fetch` skips bundles entirely. Every recipe we emit passes `-c fetch.bundleURI=<list>`
/// (`setup::Recipes`). This clones the way the recipe does, pushes, cuts an incremental bundle, and
/// asserts the fetch took it: trace2 shows the list + bundle downloads, the upload-pack remainder is
/// tiny, and `fetch.bundleCreationToken` advanced.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fetch_after_the_recipe_clone_uses_the_bundles() -> TestResult {
    let server = Server::start().await?;
    server.put_repo("t", "catchup").await?;
    let src = TestRepo::synthetic(4, 3)?;
    git_in(&src, &["branch", "-M", "main"])?;
    git_in(
        &src,
        &["remote", "add", "origin", &server.repo_url("t", "catchup")],
    )?;
    git_in(&src, &["push", "-q", "origin", "main"])?;
    server.build_bundle("t", "catchup", "weekly").await?;

    // The recipe, verbatim from /services/setup.json (the UI's single source of truth).
    let setup: serde_json::Value = reqwest::Client::new()
        .get(format!(
            "{}/services/setup.json?repo=t/catchup",
            server.base_url
        ))
        .header("Authorization", "Bearer dev")
        .send()
        .await?
        .json()
        .await?;
    let plain = setup["plain_clone"].as_str().expect("plain_clone");
    // Fetches record the catch-up list (no fulls): `bundles/catchup`.
    let list = format!("{}/bundles/catchup", server.repo_url("t", "catchup"));
    assert_eq!(
        plain,
        format!(
            "git clone -c fetch.bundleURI={list} {}",
            server.repo_url("t", "catchup")
        )
    );
    let catchup_text = server
        .get_text("/t/catchup.git/bundles/catchup", &[])
        .await?;
    assert!(
        !catchup_text.contains("weekly-"),
        "the catch-up list carries no fulls:\n{catchup_text}"
    );
    let clone_text = server.get_text("/t/catchup.git/bundles/list", &[]).await?;
    assert!(
        clone_text.contains("weekly-"),
        "the clone list does:\n{clone_text}"
    );
    let clone_dir = tempfile::tempdir()?;
    let mut args: Vec<&str> = plain.split(' ').skip(1).collect(); // drop the leading "git"
    args.insert(0, "transfer.bundleURI=true");
    args.insert(0, "-c");
    let dir_str = clone_dir.path().to_str().unwrap();
    args.push(dir_str);
    git(&args, clone_dir.path().parent().unwrap())?;
    assert_eq!(
        git_in(clone_dir.path(), &["config", "fetch.bundleURI"])?.trim(),
        list,
        "the clone recorded the list"
    );
    let token_before: u64 = git_in(clone_dir.path(), &["config", "fetch.bundleCreationToken"])?
        .trim()
        .parse()?;

    // New history on the server, folded into a new incremental bundle (the maintainer's daily slot).
    for i in 0..3 {
        std::fs::write(src.join(format!("catchup-{i}.txt")), format!("{i}\n"))?;
        git_in(&src, &["add", "."])?;
        git_in(&src, &["commit", "-q", "-m", &format!("catchup {i}")])?;
    }
    git_in(&src, &["push", "-q", "origin", "main"])?;
    server.build_bundle("t", "catchup", "daily").await?;

    // The fetch, traced (GIT_TRACE2_EVENT needs a path git can open; a file it appends to):
    // the bundle list + the new bundle are downloaded (the bundle is unbundled through
    // `index-pack <file>`, never `--stdin`) and the creationToken moves to the new slot.
    let trace_dir = tempfile::tempdir()?;
    let trace_path = trace_dir.path().join("trace.jsonl");
    let out = std::process::Command::new("git")
        .current_dir(clone_dir.path())
        .env("GIT_TRACE2_EVENT", &trace_path)
        .args(["fetch", "origin"])
        .output()?;
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(
        git_in(clone_dir.path(), &["rev-parse", "origin/main"])?,
        git_in(&src, &["rev-parse", "main"])?
    );
    let events = std::fs::read_to_string(&trace_path).unwrap_or_default();
    // git downloads the list and each newer bundle with `git-remote-https <url>` children (the bundle is
    // unbundled in-process), then negotiates the remainder with upload-pack.
    let downloads: Vec<&str> = events
        .lines()
        .filter(|l| l.contains("\"child_start\"") && l.contains("git-remote-https\",\"http"))
        .collect();
    let list_downloads = downloads
        .iter()
        .filter(|l| l.contains("/bundles/catchup\""))
        .count();
    let bundle_downloads = downloads
        .iter()
        .filter(|l| l.contains("/bundles/daily/"))
        .count();
    assert_eq!(
        list_downloads,
        1,
        "the fetch must read the catch-up list once:\n{}",
        downloads.join("\n")
    );
    assert_eq!(
        bundle_downloads,
        1,
        "the fetch must download the one newer (daily) bundle, never the weekly:\n{}",
        downloads.join("\n")
    );
    assert!(
        !downloads.iter().any(|l| l.contains("/bundles/weekly/")),
        "{}",
        downloads.join("\n")
    );
    let token_after: u64 = git_in(clone_dir.path(), &["config", "fetch.bundleCreationToken"])?
        .trim()
        .parse()?;
    assert!(
        token_after > token_before,
        "creationToken must advance ({token_before} → {token_after})"
    );
    eprintln!(
        "fetch: creationToken {token_before} → {token_after}; list downloads {list_downloads}, bundle downloads {bundle_downloads}"
    );

    // The weekly rollover: more history, a NEW full, then a daily cut after it. A stale client's fetch
    // must walk daily → daily and never download the new full (on a large repository: 32 GB for every developer's
    // first fetch after Sunday, measured on the rig 2026-08-22). The catch-up list has no fulls, and
    // the first daily after the weekly chains on the previous daily (same tips as the weekly).
    for i in 3..6 {
        std::fs::write(src.join(format!("catchup-{i}.txt")), format!("{i}\n"))?;
        git_in(&src, &["add", "."])?;
        git_in(&src, &["commit", "-q", "-m", &format!("catchup {i}")])?;
    }
    git_in(&src, &["push", "-q", "origin", "main"])?;
    // A daily at the weekly's instant and the weekly itself (Sunday 23:00 on the real calendar: the
    // test cuts both "now" with slot 0, tokens increase with the clock).
    server.build_bundle("t", "catchup", "daily").await?;
    server.build_bundle("t", "catchup", "weekly").await?;
    std::fs::write(src.join("monday.txt"), "m\n")?;
    git_in(&src, &["add", "."])?;
    git_in(&src, &["commit", "-q", "-m", "monday"])?;
    git_in(&src, &["push", "-q", "origin", "main"])?;
    server.build_bundle("t", "catchup", "daily").await?;
    let trace_path = trace_dir.path().join("trace2.jsonl");
    let out = std::process::Command::new("git")
        .current_dir(clone_dir.path())
        .env("GIT_TRACE2_EVENT", &trace_path)
        .args(["fetch", "origin"])
        .output()?;
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(
        git_in(clone_dir.path(), &["rev-parse", "origin/main"])?,
        git_in(&src, &["rev-parse", "main"])?
    );
    let events = std::fs::read_to_string(&trace_path).unwrap_or_default();
    let downloads: Vec<&str> = events
        .lines()
        .filter(|l| l.contains("\"child_start\"") && l.contains("git-remote-https\",\"http"))
        .collect();
    assert!(
        !downloads.iter().any(|l| l.contains("/bundles/weekly/")),
        "a fetch across the weekly rollover must not download the new full:\n{}",
        downloads.join("\n")
    );
    let dailies = downloads
        .iter()
        .filter(|l| l.contains("/bundles/daily/"))
        .count();
    assert_eq!(
        dailies,
        2,
        "exactly the two dailies since the last fetch:\n{}",
        downloads.join("\n")
    );
    assert!(git_in(clone_dir.path(), &["fsck", "--no-dangling"]).is_ok());
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
    assert!(body.contains("-c fetch.bundleURI="), "{body}");
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
                "-c",
                "transfer.bundleURI=false",
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
            handles.push((
                d,
                sha,
                std::thread::spawn(move || {
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
        let mut winner = None;
        for (_d, sha, h) in handles {
            if h.join().unwrap() {
                winner = Some(sha);
            }
        }
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
