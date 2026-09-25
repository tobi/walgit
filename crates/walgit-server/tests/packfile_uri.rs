//! Real pack bytes exercise auth, durable admission, retirement and HTTP ranges.
mod harness;
use harness::{Server, TestRepo, git_in};
use reqwest::StatusCode;
use walgit_store::{ObjectStoreExt, PutMode};

#[tokio::test]
async fn missing_coverage_snapshots_share_one_three_read_budget() -> anyhow::Result<()> {
    use prost::Message;
    use std::sync::{Arc, atomic::Ordering};
    use walgit_proto::v1::{PackGroupCoverage, PackRef};
    let truth = walgit_store::memory::MemoryStore::shared();
    let link = walgit_store::fault::FaultStore::new(truth.clone(), "selector", 1);
    let cache = tempfile::tempdir()?;
    let mut cfg = walgit_config::Config::default();
    cfg.cache.dir = cache.path().to_owned();
    cfg.wal.prefetch_packs = false;
    cfg.wal.freshness_ttl = std::time::Duration::ZERO;
    let policy = cfg.refs.policy_identity();
    let registry = walgit_wal::Registry::new(link.clone(), Arc::new(cfg));
    let id = walgit_git::RepoId::new("o", "budget")?;
    let handle = registry.create(&id, walgit_git::ObjectFormat::Sha1).await?;
    let mut manifest = (*handle.manifest()).clone();
    for n in 1..=5 {
        let checksum = format!("{n:040x}");
        manifest.packs.push(PackRef {
            checksum: checksum.clone(),
            pack_groups: vec!["code".into()],
            group_coverages: vec![PackGroupCoverage {
                group: "code".into(),
                ref_policy: policy.clone(),
                refs_key: format!("checkpoints/refs/{n:064x}.pb"),
                packs: vec![checksum],
                ..Default::default()
            }],
            ..Default::default()
        });
    }
    manifest.revision += 1;
    truth
        .put_bytes(
            &format!("{}manifest.pb", id.store_prefix()),
            manifest.encode_to_vec(),
            PutMode::Overwrite,
        )
        .await?;
    drop(handle.sync_refs().await?);
    let request = walgit_git::UploadPackRequest {
        done: true,
        wants: vec![gix_hash::ObjectId::from_hex(
            b"1111111111111111111111111111111111111111",
        )?],
        packfile_uris_protocols: vec!["https".into()],
        ..Default::default()
    };
    let before = link.stats().ops.load(Ordering::Relaxed);
    assert!(
        walgit_server::packfile_uri::select(&handle, &request, "https://example.invalid/o/budget")
            .await
            .is_none()
    );
    assert_eq!(link.stats().ops.load(Ordering::Relaxed) - before, 3);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn blobless_uri_clone_and_dynamic_checkout_work_in_both_object_formats() -> anyhow::Result<()>
{
    for (format, name) in [
        (walgit_config::ObjectFormat::Sha1, "sha1"),
        (walgit_config::ObjectFormat::Sha256, "sha256"),
    ] {
        let server = Server::start_with_tweak(|cfg| {
            cfg.git.object_format = format;
            cfg.packfile_uri.uri_min_bytes = bytesize::ByteSize::b(1);
        })
        .await?;
        server.put_repo("o", name).await?;
        let source = tempfile::tempdir()?;
        git_in(
            source.path(),
            &[
                "init",
                "-q",
                "-b",
                "main",
                &format!("--object-format={name}"),
            ],
        )?;
        git_in(source.path(), &["config", "user.name", "Test"])?;
        git_in(
            source.path(),
            &["config", "user.email", "test@example.invalid"],
        )?;
        std::fs::write(source.path().join("file.txt"), "lazy blob contents\n")?;
        git_in(source.path(), &["add", "."])?;
        git_in(source.path(), &["commit", "-q", "-m", "initial"])?;
        git_in(
            source.path(),
            &["push", "-q", &server.repo_url("o", name), "main"],
        )?;
        let handle = server
            .state
            .registry
            .open(&walgit_git::RepoId::new("o", name)?)
            .await?;
        walgit_server::ops::compact_repo(
            &handle,
            walgit_server::ops::CompactRequest {
                force: true,
                rebuild_base: true,
            },
            &|_| {},
        )
        .await?;
        walgit_server::ops::compact_repo(
            &handle,
            walgit_server::ops::CompactRequest::default(),
            &|_| {},
        )
        .await?;
        let clone = tempfile::tempdir()?;
        let output = std::process::Command::new("git")
            .env("GIT_TRACE_PACKET", "1")
            .args([
                "-c",
                "protocol.version=2",
                "-c",
                "fetch.uriProtocols=http,https",
                "clone",
                "--progress",
                "--filter=blob:none",
                "--no-checkout",
                &server.repo_url("o", name),
                clone.path().to_str().unwrap(),
            ])
            .output()?;
        let trace = String::from_utf8_lossy(&output.stderr);
        assert!(output.status.success(), "{name}: {trace}");
        assert!(trace.contains("< \\1packfile-uris"), "{name}: {trace}");
        let missing = git_in(
            clone.path(),
            &["rev-list", "--objects", "--all", "--missing=print"],
        )?;
        assert!(
            missing.lines().any(|l| l.starts_with('?')),
            "static blobless clone must omit blobs: {missing}"
        );
        git_in(clone.path(), &["checkout", "-q", "-f", "main"])?;
        assert_eq!(
            std::fs::read_to_string(clone.path().join("file.txt"))?,
            "lazy blob contents\n"
        );
        git_in(clone.path(), &["fsck", "--strict"])?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn discovery_is_independent_of_pack_groups_and_full_view_preserves_mirrors()
-> anyhow::Result<()> {
    let server = Server::start_with_tweak(|cfg| cfg.refs.packfiles.clear()).await?;
    server.put_repo("o", "refs").await?;
    let source = TestRepo::synthetic(2, 1)?;
    let url = server.repo_url("o", "refs");
    git_in(
        &source,
        &[
            "push",
            "-q",
            &url,
            "main",
            "HEAD:refs/meta/state",
            "HEAD:refs/notes/tool",
        ],
    )?;
    for version in ["0", "2"] {
        let refs = git_in(
            &source,
            &[
                "-c",
                &format!("protocol.version={version}"),
                "ls-remote",
                &url,
            ],
        )?;
        assert!(refs.contains("refs/heads/main"));
        assert!(!refs.contains("refs/meta/"));
        assert!(!refs.contains("refs/notes/"));
    }
    let refs = git_in(
        &source,
        &[
            "-c",
            "protocol.version=2",
            "ls-remote",
            "--server-option=ref-view=all",
            "--server-option=extra",
            &url,
        ],
    )?;
    assert!(refs.contains("refs/meta/state"));
    assert!(refs.contains("refs/notes/tool"));
    let clone = tempfile::tempdir()?;
    git_in(
        &source,
        &[
            "clone",
            "-q",
            "--mirror",
            "--server-option=ref-view=all",
            &url,
            clone.path().to_str().unwrap(),
        ],
    )?;
    assert_eq!(
        git_in(clone.path(), &["rev-parse", "refs/meta/state"])?,
        git_in(&source, &["rev-parse", "HEAD"])?
    );
    git_in(clone.path(), &["fsck", "--strict"])?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stock_git_downloads_static_baseline_and_native_residual_while_gix_stays_dynamic()
-> anyhow::Result<()> {
    let server = Server::start_with_tweak(|cfg| {
        cfg.packfile_uri.uri_min_bytes = bytesize::ByteSize::b(1);
    })
    .await?;
    server.put_repo("o", "delivery").await?;
    let source = TestRepo::synthetic(4, 3)?;
    git_in(
        &source,
        &["push", "-q", &server.repo_url("o", "delivery"), "main"],
    )?;
    git_in(&source, &["checkout", "-q", "--orphan", "metadata"])?;
    git_in(&source, &["rm", "-q", "-rf", "."])?;
    std::fs::write(source.join("metadata.txt"), "metadata-only payload\n")?;
    git_in(&source, &["add", "."])?;
    git_in(&source, &["commit", "-q", "-m", "metadata"])?;
    let metadata_tip = git_in(&source, &["rev-parse", "HEAD"])?;
    git_in(
        &source,
        &[
            "push",
            "-q",
            &server.repo_url("o", "delivery"),
            "HEAD:refs/meta/state",
        ],
    )?;
    git_in(&source, &["checkout", "-q", "main"])?;
    let id = walgit_git::RepoId::new("o", "delivery")?;
    let handle = server.state.registry.open(&id).await?;
    walgit_server::ops::compact_repo(
        &handle,
        walgit_server::ops::CompactRequest {
            force: true,
            rebuild_base: true,
        },
        &|_| {},
    )
    .await?;
    walgit_server::ops::compact_repo(
        &handle,
        walgit_server::ops::CompactRequest::default(),
        &|_| {},
    )
    .await?;
    assert!(
        handle
            .manifest()
            .packs
            .iter()
            .any(|p| !p.group_coverages.is_empty())
    );
    // The selected snapshot precedes this push: the wire pack must carry its graph.
    std::fs::write(source.join("after-cut.txt"), "dynamic remainder\n")?;
    git_in(&source, &["add", "."])?;
    git_in(&source, &["commit", "-q", "-m", "after cut"])?;
    git_in(
        &source,
        &["push", "-q", &server.repo_url("o", "delivery"), "main"],
    )?;
    let tip = git_in(&source, &["rev-parse", "HEAD"])?;
    for (engine, protected, proactive) in [
        (walgit_config::UploadPackEngine::Git, false, false),
        (walgit_config::UploadPackEngine::Gix, false, false),
        (walgit_config::UploadPackEngine::Git, true, false),
        (walgit_config::UploadPackEngine::Git, true, true),
    ] {
        let front = server
            .start_sibling_with(|cfg| {
                cfg.git.upload_pack_engine = engine;
                cfg.packfile_uri.uri_min_bytes = bytesize::ByteSize::b(1);
                if protected {
                    cfg.server.auth.mode = walgit_config::AuthMode::Token;
                    cfg.server.auth.anonymous_read = false;
                    cfg.server.auth.tokens = vec![walgit_config::StaticToken {
                        principal: "reader".into(),
                        token: "test-reader".into(),
                        token_env: None,
                        write: false,
                        admin: false,
                    }];
                }
            })
            .await?;
        let clone_with = |git_args: &[&str], clone_args: &[&str]| {
            let clone = tempfile::tempdir()?;
            let output = std::process::Command::new("git")
                .env("GIT_TRACE_PACKET", "1")
                .env("GIT_TERMINAL_PROMPT", "0")
                .args(git_args)
                .args([
                    "-c",
                    "protocol.version=2",
                    "-c",
                    "fetch.uriProtocols=http,https",
                    "clone",
                    "--progress",
                ])
                .args(clone_args)
                .args([
                    front.repo_url("o", "delivery").as_str(),
                    clone.path().to_str().unwrap(),
                ])
                .output()?;
            anyhow::Ok((clone, output))
        };
        // The installer's shape for a protected host: its helper answers `get` with a bearer
        // and a username/password pair, `proactiveAuth` sends the pair with every request
        // (URI downloads included), and the clone recipe asks for URIs.
        let helper = "credential.helper=!f() { test \"$1\" = get || return 0; printf 'capability[]=authtype\\nauthtype=Bearer\\ncredential=test-reader\\nusername=token\\npassword=test-reader\\n'; }; f";
        let proactive_auth = format!("http.{}/.proactiveAuth=auto", front.base_url);
        let opt_in = ["-c", "remote.origin.serverOption=uri-auth=proactive"];
        if proactive {
            // Asking for URIs without proactive auth: git downloads them bare, gets the 401
            // and has no fallback, which is why the server never infers the option.
            let (_, bare) = clone_with(&["-c", "credential.helper=", "-c", helper], &opt_in)?;
            let stderr = String::from_utf8_lossy(&bare.stderr);
            assert!(!bare.status.success(), "{stderr}");
            assert!(stderr.contains("returned error: 401"), "{stderr}");
        }
        let (clone, result) = match (protected, proactive) {
            (false, _) => clone_with(&[], &[])?,
            (true, false) => clone_with(
                &["-c", "http.extraHeader=Authorization: Bearer test-reader"],
                &[],
            )?,
            (true, true) => clone_with(
                &[
                    "-c",
                    "credential.helper=",
                    "-c",
                    helper,
                    "-c",
                    &proactive_auth,
                ],
                &opt_in,
            )?,
        };
        let trace = String::from_utf8_lossy(&result.stderr);
        assert!(result.status.success(), "{engine:?}: {trace}");
        assert_eq!(
            trace.contains("< \\1packfile-uris"),
            engine == walgit_config::UploadPackEngine::Git && (!protected || proactive),
            "{trace}"
        );
        if protected && !proactive {
            assert!(trace.contains("WARNING: reusable pack delivery is unavailable"));
        }
        assert_eq!(git_in(clone.path(), &["rev-parse", "HEAD"])?, tip);
        assert_eq!(
            std::fs::read_to_string(clone.path().join("after-cut.txt"))?,
            "dynamic remainder\n"
        );
        git_in(clone.path(), &["fsck", "--strict"])?;
        let metadata = std::process::Command::new("git")
            .current_dir(clone.path())
            .args(["cat-file", "-e", metadata_tip.trim()])
            .output()?;
        assert!(
            !metadata.status.success(),
            "metadata-only commit leaked into ordinary code delivery"
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn protected_pack_downloads_require_membership_and_survive_retirement() -> anyhow::Result<()>
{
    let writer = Server::start().await?;
    writer.put_repo("o", "packs").await?;
    let source = TestRepo::synthetic(3, 2)?;
    git_in(
        &source,
        &["push", "-q", &writer.repo_url("o", "packs"), "main"],
    )?;
    let id = walgit_git::RepoId::new("o", "packs")?;
    let handle = writer.state.registry.open(&id).await?;
    let checksum = handle.manifest().packs[0].checksum.clone();
    let protected = writer
        .start_sibling_with(|cfg| {
            cfg.server.auth.mode = walgit_config::AuthMode::Token;
            cfg.server.auth.anonymous_read = false;
            cfg.server.auth.tokens = vec![walgit_config::StaticToken {
                principal: "reader".into(),
                token: "test-reader".into(),
                token_env: None,
                write: false,
                admin: false,
            }];
            // Static transfer must not materialize the repository into this cache.
            cfg.cache.max_bytes = bytesize::ByteSize::b(1);
        })
        .await?;
    let client = reqwest::Client::new();
    for extension in ["pack", "idx"] {
        let url = format!(
            "{}/o/packs.git/packfiles/{checksum}.{extension}",
            protected.base_url
        );
        let full = client.get(&url).bearer_auth("test-reader").send().await?;
        assert_eq!(full.status(), StatusCode::OK);
        assert!(
            full.headers()["cache-control"]
                .to_str()?
                .starts_with("private,")
        );
        let etag = full.headers()["etag"].to_str()?.to_owned();
        let bytes = full.bytes().await?;
        assert!(!bytes.is_empty());
        let head = client.head(&url).bearer_auth("test-reader").send().await?;
        assert_eq!(head.status(), StatusCode::OK);
        assert_eq!(
            head.headers()["content-length"]
                .to_str()?
                .parse::<usize>()?,
            bytes.len()
        );
        assert!(head.bytes().await?.is_empty());
        for token in [None, Some("invalid")] {
            let mut req = client.get(&url).header("If-None-Match", &etag);
            if let Some(token) = token {
                req = req.bearer_auth(token);
            }
            assert_eq!(req.send().await?.status(), StatusCode::UNAUTHORIZED);
        }
        assert_eq!(
            client
                .get(&url)
                .bearer_auth("test-reader")
                .header("If-None-Match", &etag)
                .send()
                .await?
                .status(),
            StatusCode::NOT_MODIFIED
        );
        let partial = client
            .get(&url)
            .bearer_auth("test-reader")
            .header("Range", "bytes=2-8")
            .header("If-Range", &etag)
            .send()
            .await?;
        assert_eq!(partial.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(partial.bytes().await?, bytes.slice(2..9));
        let weak = client
            .get(&url)
            .bearer_auth("test-reader")
            .header("Range", "bytes=2-8")
            .header("If-Range", format!("W/{etag}"))
            .send()
            .await?;
        assert_eq!(weak.status(), StatusCode::OK);
        assert_eq!(weak.bytes().await?, bytes);
        assert_eq!(
            client
                .get(&url)
                .bearer_auth("test-reader")
                .header("Range", "bytes=999999-")
                .send()
                .await?
                .status(),
            StatusCode::RANGE_NOT_SATISFIABLE
        );
        // New outputs commit before the original input leaves the live inventory.
        if extension == "pack" {
            walgit_server::ops::compact_repo(
                &handle,
                walgit_server::ops::CompactRequest {
                    force: true,
                    rebuild_base: true,
                },
                &|_| {},
            )
            .await?;
            assert!(
                handle
                    .manifest()
                    .retired_packs
                    .iter()
                    .any(|p| p.checksum == checksum)
            );
        }
        let retired = client.get(&url).bearer_auth("test-reader").send().await?;
        assert_eq!(retired.status(), StatusCode::OK);
        assert_eq!(retired.bytes().await?, bytes);
    }
    let orphan = "f".repeat(40);
    handle
        .store()
        .put_bytes(
            &format!("wal/{orphan}.pack"),
            b"uncommitted".to_vec(),
            PutMode::Create,
        )
        .await?;
    for name in [
        format!("{orphan}.pack"),
        format!("{checksum}.rev"),
        format!("{checksum}.pack/extra"),
    ] {
        let response = client
            .get(format!("{}/o/packs/packfiles/{name}", protected.base_url))
            .bearer_auth("test-reader")
            .send()
            .await?;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
    let cold = protected.state.registry.open(&id).await?;
    assert!(cold.local().packs()?.is_empty());
    Ok(())
}
