// Test fixtures use panics to fail the test, including shared helper functions.
#![allow(clippy::unwrap_used, clippy::case_sensitive_file_extension_comparisons)]

mod common;

use walgit_git::pkt::Protocol;
use walgit_git::{LocalRepo, ObjectFormat, RepoId, UploadPackRequest, gix_hash};

mod cm {
    pub use super::common::*;
}

#[derive(Debug, Clone, Copy)]
enum Engine {
    Gix,
    Git,
}

async fn setup() -> (tempfile::TempDir, LocalRepo, String, String) {
    let root = tempfile::TempDir::new().unwrap();
    let id = RepoId::new("acme", "up").unwrap();
    let repo = LocalRepo::init(root.path(), &id, ObjectFormat::Sha1).unwrap();
    let src = cm::SourceRepo::new();
    let a = src.head();
    let b = src.commit_file("file2.txt", "world\n", "second");
    let pack = src.pack(&["HEAD"], &[], false);
    repo.ingest_pack(
        cm::cursor(pack),
        walgit_git::IngestOptions {
            fsck: true,
            max_bytes: None,
            thin: false,
        },
    )
    .await
    .unwrap()
    .unwrap();
    // Set refs/heads/main = B so upload-pack advertises something.
    let zero = "0".repeat(40);
    let txn = walgit_proto::v1::RefTransaction {
        updates: vec![walgit_proto::v1::RefUpdate {
            name: "refs/heads/main".into(),
            old_oid: zero,
            new_oid: b.clone(),
            new_symbolic_target: String::new(),
            new_peeled: String::new(),
        }],
        push_options: vec![],
        atomic: true,
    };
    repo.apply_ref_txn(&txn, true).unwrap();
    (root, repo, a, b)
}

fn oid(s: &str) -> gix_hash::ObjectId {
    gix_hash::ObjectId::from_hex(s.as_bytes()).unwrap()
}

/// Build a v2 fetch command pkt-line body from a typed request (for the
/// engine=Git raw passthrough path). Uses the public builder to ensure all
/// fields are serialized.
fn build_fetch_body(req: &UploadPackRequest) -> Vec<u8> {
    walgit_git::build_v2_fetch_request(req)
}
/// Create a pack with no deltas (pack.window=0) to avoid gix Verify-mode
/// ODB lookup issues during first pack ingestion.
fn pack_no_delta(src: &cm::SourceRepo, includes: &[&str]) -> Vec<u8> {
    use std::io::Write;
    use std::process::Stdio;
    let mut input = String::new();
    for inc in includes {
        input.push_str(inc);
        input.push('\n');
    }
    let mut cmd = std::process::Command::new("git");
    cmd.current_dir(&*src.dir)
        .args(["-c", "pack.window=0", "pack-objects", "--stdout", "--revs"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = cmd.spawn().unwrap();
    child
        .stdin
        .as_mut()
        .unwrap()
        .write_all(input.as_bytes())
        .unwrap();
    let out = child.wait_with_output().unwrap();
    assert!(
        out.status.success(),
        "pack-objects --window=0 failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    out.stdout
}
async fn run_fetch(repo: &LocalRepo, engine: Engine, req: &UploadPackRequest) -> Vec<u8> {
    let mut out: Vec<u8> = Vec::new();
    match engine {
        Engine::Gix => {
            repo.upload_pack(req.clone(), &mut out).await.unwrap();
        }
        Engine::Git => {
            let body = build_fetch_body(req);
            repo.upload_pack_raw(Protocol::V2, &body[..], &mut out)
                .await
                .unwrap();
        }
    }
    out
}

#[tokio::test]
async fn fetch_no_haves_produces_valid_pack() {
    for engine in [Engine::Gix, Engine::Git] {
        let (_r, repo, _a, b) = setup().await;
        let req = UploadPackRequest {
            wants: vec![oid(&b)],
            haves: vec![],
            done: true,
            thin_pack: true,
            no_progress: true,
            include_tag: true,
            ofs_delta: true,
            sideband_all: false,
            wait_for_done: false,
            filter: None,
            deepen: None,
            deepen_since: None,
            deepen_not: vec![],
            shallow: vec![],
            want_refs: vec![],
            packfile_uris_protocols: vec![],
        };
        let resp = run_fetch(&repo, engine, &req).await;
        assert!(!resp.is_empty(), "empty response for {engine:?}");
        // With `done` and no haves, v2 omits the acknowledgments section and
        // streams the packfile directly (no NAK).
        let pack = cm::extract_packfile(&resp);
        assert!(pack.starts_with(b"PACK"), "packfile missing for {engine:?}");
        let tmp = cm::index_and_fsck(&pack);
        // B should be present in the indexed repo.
        let out = std::process::Command::new("git")
            .current_dir(tmp.path())
            .args(["cat-file", "-e", &b])
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "B missing from fetched pack {engine:?}"
        );
    }
}

#[tokio::test]
async fn fetch_with_haves_smaller_pack_and_ack() {
    for engine in [Engine::Gix, Engine::Git] {
        let (_r, repo, a, b) = setup().await;
        let req_full = UploadPackRequest {
            wants: vec![oid(&b)],
            haves: vec![],
            done: true,
            thin_pack: true,
            no_progress: true,
            include_tag: false,
            ofs_delta: true,
            sideband_all: false,
            wait_for_done: false,
            filter: None,
            deepen: None,
            deepen_since: None,
            deepen_not: vec![],
            shallow: vec![],
            want_refs: vec![],
            packfile_uris_protocols: vec![],
        };
        let req_ack = UploadPackRequest {
            wants: vec![oid(&b)],
            haves: vec![oid(&a)],
            done: false, // multi-round: get acknowledgments with ACK
            ..req_full.clone()
        };
        let req_inc = UploadPackRequest {
            wants: vec![oid(&b)],
            haves: vec![oid(&a)],
            done: true,
            ..req_full.clone()
        };
        let ack = run_fetch(&repo, engine, &req_ack).await;
        assert!(cm::has_ack(&ack, &a), "expected ACK {a} for {engine:?}");
        let full = run_fetch(&repo, engine, &req_full).await;
        let inc = run_fetch(&repo, engine, &req_inc).await;
        let pack_full = cm::extract_packfile(&full);
        let pack_inc = cm::extract_packfile(&inc);
        assert!(
            pack_inc.len() < pack_full.len(),
            "incremental not smaller {engine:?}: {} vs {}",
            pack_inc.len(),
            pack_full.len()
        );
        // Incremental pack still valid.
        let _tmp = cm::index_and_fsck(&pack_inc);
    }
}

#[tokio::test]
async fn fetch_filter_blob_none_no_blobs() {
    for engine in [Engine::Gix, Engine::Git] {
        let (_r, repo, _a, b) = setup().await;
        let req = UploadPackRequest {
            wants: vec![oid(&b)],
            haves: vec![],
            done: true,
            thin_pack: true,
            no_progress: true,
            include_tag: false,
            ofs_delta: true,
            sideband_all: false,
            wait_for_done: false,
            filter: Some("blob:none".into()),
            deepen: None,
            deepen_since: None,
            deepen_not: vec![],
            shallow: vec![],
            want_refs: vec![],
            packfile_uris_protocols: vec![],
        };
        let resp = run_fetch(&repo, engine, &req).await;
        let pack = cm::extract_packfile(&resp);
        assert!(pack.starts_with(b"PACK"), "packfile missing for {engine:?}");
        let (blobs, commits, _trees, _tags) = cm::pack_object_types(&pack);
        assert_eq!(blobs, 0, "filter blob:none produced blobs {engine:?}");
        assert!(commits >= 1, "no commits in filtered pack {engine:?}");
    }
}

#[tokio::test]
async fn fetch_filter_blob_limit_excludes_large_blobs() {
    for engine in [Engine::Gix, Engine::Git] {
        let root = tempfile::TempDir::new().unwrap();
        let id = RepoId::new("acme", "up").unwrap();
        let repo = LocalRepo::init(root.path(), &id, ObjectFormat::Sha1).unwrap();
        let src = cm::SourceRepo::new();
        // Create a small file and a large file.
        src.commit_file("small.txt", "tiny\n", "small");
        src.commit_file("big.txt", &"x".repeat(5000), "big");
        let head = src.head();
        let pack = src.pack(&["HEAD"], &[], false);
        repo.ingest_pack(
            cm::cursor(pack),
            walgit_git::IngestOptions {
                fsck: true,
                max_bytes: None,
                thin: false,
            },
        )
        .await
        .unwrap()
        .unwrap();
        // Set ref.
        let zero = "0".repeat(40);
        let txn = walgit_proto::v1::RefTransaction {
            updates: vec![walgit_proto::v1::RefUpdate {
                name: "refs/heads/main".into(),
                old_oid: zero,
                new_oid: head.clone(),
                new_symbolic_target: String::new(),
                new_peeled: String::new(),
            }],
            push_options: vec![],
            atomic: true,
        };
        repo.apply_ref_txn(&txn, true).unwrap();

        let req = UploadPackRequest {
            wants: vec![oid(&head)],
            haves: vec![],
            done: true,
            thin_pack: true,
            no_progress: true,
            include_tag: false,
            ofs_delta: true,
            sideband_all: false,
            wait_for_done: false,
            filter: Some("blob:limit=100".into()),
            deepen: None,
            deepen_since: None,
            deepen_not: vec![],
            shallow: vec![],
            want_refs: vec![],
            packfile_uris_protocols: vec![],
        };
        let resp = run_fetch(&repo, engine, &req).await;
        let pack = cm::extract_packfile(&resp);
        assert!(pack.starts_with(b"PACK"), "packfile missing for {engine:?}");
        let (blobs, commits, _trees, _tags) = cm::pack_object_types(&pack);
        // Blobs <=100 bytes should be present (file1.txt=6, small.txt=5);
        // big.txt=5000 should be excluded.
        assert_eq!(
            blobs, 2,
            "blob:limit=100 should exclude 5000-byte blob {engine:?}: got {blobs}"
        );
        assert!(commits >= 1, "no commits {engine:?}");
    }
}

#[tokio::test]
async fn fetch_filter_tree_0_only_root_tree() {
    for engine in [Engine::Gix, Engine::Git] {
        let (_r, repo, _a, b) = setup().await;
        let req = UploadPackRequest {
            wants: vec![oid(&b)],
            haves: vec![],
            done: true,
            thin_pack: true,
            no_progress: true,
            include_tag: false,
            ofs_delta: true,
            sideband_all: false,
            wait_for_done: false,
            filter: Some("tree:0".into()),
            deepen: None,
            deepen_since: None,
            deepen_not: vec![],
            shallow: vec![],
            want_refs: vec![],
            packfile_uris_protocols: vec![],
        };
        let resp = run_fetch(&repo, engine, &req).await;
        let pack = cm::extract_packfile(&resp);
        assert!(pack.starts_with(b"PACK"), "packfile missing for {engine:?}");
        let (blobs, commits, trees, _tags) = cm::pack_object_types(&pack);
        // tree:0 omits every tree and blob (git: "tree:<depth> omits all blobs
        // and trees whose depth from the root tree is >= depth"): commits only.
        assert_eq!(blobs, 0, "tree:0 should exclude blobs {engine:?}");
        assert!(commits >= 1, "no commits {engine:?}");
        assert_eq!(trees, 0, "tree:0 must not send trees ({engine:?})");
    }
}

#[tokio::test]
async fn fetch_include_tag_sends_annotated_tag() {
    for engine in [Engine::Gix, Engine::Git] {
        let root = tempfile::TempDir::new().unwrap();
        let id = RepoId::new("acme", "up").unwrap();
        let repo = LocalRepo::init(root.path(), &id, ObjectFormat::Sha1).unwrap();
        let src = cm::SourceRepo::new();
        let head = src.head();
        // Create an annotated tag.
        let tag_oid = src.annotated_tag("v1", &head);
        // Pack with the tag ref so the tag object is included.
        let pack = src.pack(&["HEAD", "refs/tags/v1"], &[], false);
        repo.ingest_pack(
            cm::cursor(pack),
            walgit_git::IngestOptions {
                fsck: true,
                max_bytes: None,
                thin: false,
            },
        )
        .await
        .unwrap()
        .unwrap();
        let zero = "0".repeat(40);
        let txn = walgit_proto::v1::RefTransaction {
            updates: vec![
                walgit_proto::v1::RefUpdate {
                    name: "refs/heads/main".into(),
                    old_oid: zero.clone(),
                    new_oid: head.clone(),
                    new_symbolic_target: String::new(),
                    new_peeled: String::new(),
                },
                walgit_proto::v1::RefUpdate {
                    name: "refs/tags/v1".into(),
                    old_oid: zero,
                    new_oid: tag_oid.clone(),
                    new_symbolic_target: String::new(),
                    new_peeled: String::new(),
                },
            ],
            push_options: vec![],
            atomic: true,
        };
        repo.apply_ref_txn(&txn, true).unwrap();

        // Fetch with include-tag.
        let req = UploadPackRequest {
            wants: vec![oid(&head)],
            haves: vec![],
            done: true,
            thin_pack: true,
            no_progress: true,
            include_tag: true,
            ofs_delta: true,
            sideband_all: false,
            wait_for_done: false,
            filter: None,
            deepen: None,
            deepen_since: None,
            deepen_not: vec![],
            shallow: vec![],
            want_refs: vec![],
            packfile_uris_protocols: vec![],
        };
        let resp = run_fetch(&repo, engine, &req).await;
        let pack = cm::extract_packfile(&resp);
        assert!(pack.starts_with(b"PACK"), "packfile missing for {engine:?}");
        let (_blobs, _commits, _trees, tags) = cm::pack_object_types(&pack);
        assert!(
            tags >= 1,
            "include-tag should include annotated tag object {engine:?}: got {tags} tags"
        );
    }
}

#[tokio::test]
async fn fetch_deepen_shallow_info() {
    for engine in [Engine::Gix, Engine::Git] {
        let root = tempfile::TempDir::new().unwrap();
        let id = RepoId::new("acme", "up").unwrap();
        let repo = LocalRepo::init(root.path(), &id, ObjectFormat::Sha1).unwrap();
        // Create a repo with 5 commits, using no-delta packs to avoid the
        // gix Verify-mode issue where an empty ODB can't resolve deltas
        // during first pack index writing.
        let src = cm::SourceRepo::new();
        let mut commits = Vec::new();
        commits.push(src.head());
        for i in 1..5 {
            commits.push(src.commit_file(&format!("f{i}.txt"), "x\n", &format!("c{i}")));
        }
        let head = commits.last().unwrap().clone();
        let pack = pack_no_delta(&src, &["HEAD"]);
        repo.ingest_pack(
            cm::cursor(pack),
            walgit_git::IngestOptions {
                fsck: true,
                max_bytes: None,
                thin: false,
            },
        )
        .await
        .unwrap()
        .unwrap();
        let zero = "0".repeat(40);
        let txn = walgit_proto::v1::RefTransaction {
            updates: vec![walgit_proto::v1::RefUpdate {
                name: "refs/heads/main".into(),
                old_oid: zero,
                new_oid: head.clone(),
                new_symbolic_target: String::new(),
                new_peeled: String::new(),
            }],
            push_options: vec![],
            atomic: true,
        };
        repo.apply_ref_txn(&txn, true).unwrap();

        // Fetch with deepen=2 (shallow clone, depth 2).
        let req = UploadPackRequest {
            wants: vec![oid(&head)],
            haves: vec![],
            done: true,
            thin_pack: true,
            no_progress: true,
            include_tag: false,
            ofs_delta: true,
            sideband_all: false,
            wait_for_done: false,
            filter: None,
            deepen: Some(2),
            deepen_since: None,
            deepen_not: vec![],
            shallow: vec![],
            want_refs: vec![],
            packfile_uris_protocols: vec![],
        };
        let resp = run_fetch(&repo, engine, &req).await;
        // For gix engine: check shallow-info section is present.
        // For git engine: the response should contain shallow-info too.
        let resp_str = String::from_utf8_lossy(&resp);
        assert!(
            resp_str.contains("shallow-info") || resp_str.contains("shallow "),
            "shallow-info section missing for {engine:?}"
        );
        // The pack should be valid and contain fewer commits than full.
        let pack = cm::extract_packfile(&resp);
        assert!(pack.starts_with(b"PACK"), "packfile missing for {engine:?}");
        let (_blobs, shallow_commits, _trees, _tags) = cm::pack_object_types(&pack);
        assert!(
            shallow_commits <= 2,
            "deepen=2 should produce at most 2 commits {engine:?}: got {shallow_commits}"
        );
    }
}

#[tokio::test]
async fn fetch_haves_shrink_pack() {
    for engine in [Engine::Gix, Engine::Git] {
        let (_r, repo, a, b) = setup().await;
        // Full fetch (no haves).
        let req_full = UploadPackRequest {
            wants: vec![oid(&b)],
            haves: vec![],
            done: true,
            thin_pack: true,
            no_progress: true,
            include_tag: false,
            ofs_delta: true,
            sideband_all: false,
            wait_for_done: false,
            filter: None,
            deepen: None,
            deepen_since: None,
            deepen_not: vec![],
            shallow: vec![],
            want_refs: vec![],
            packfile_uris_protocols: vec![],
        };
        // Incremental fetch (have A, want B).
        let req_inc = UploadPackRequest {
            wants: vec![oid(&b)],
            haves: vec![oid(&a)],
            done: true,
            ..req_full.clone()
        };
        let full = run_fetch(&repo, engine, &req_full).await;
        let inc = run_fetch(&repo, engine, &req_inc).await;
        let pack_full = cm::extract_packfile(&full);
        let pack_inc = cm::extract_packfile(&inc);
        assert!(
            pack_inc.len() < pack_full.len(),
            "incremental pack should be smaller than full pack {engine:?}: {} vs {}",
            pack_inc.len(),
            pack_full.len()
        );
        // Incremental pack must be valid.
        let _tmp = cm::index_and_fsck(&pack_inc);
    }
}

#[tokio::test]
async fn fetch_fsck_rejects_corrupt_pack() {
    // Verify that git fsck catches a corrupt pack (sanity check for the
    // test infrastructure, not the gix engine itself).
    let (_r, repo, _a, b) = setup().await;
    let req = UploadPackRequest {
        wants: vec![oid(&b)],
        haves: vec![],
        done: true,
        thin_pack: true,
        no_progress: true,
        include_tag: false,
        ofs_delta: true,
        sideband_all: false,
        wait_for_done: false,
        filter: None,
        deepen: None,
        deepen_since: None,
        deepen_not: vec![],
        shallow: vec![],
        want_refs: vec![],
        packfile_uris_protocols: vec![],
    };
    let resp = run_fetch(&repo, Engine::Gix, &req).await;
    let pack = cm::extract_packfile(&resp);
    assert!(pack.starts_with(b"PACK"));
    // Corrupt the pack by flipping a byte in the middle.
    let mut corrupt = pack.clone();
    let mid = corrupt.len() / 2;
    if mid > 0 {
        corrupt[mid] ^= 0xff;
    }
    // Index the corrupt pack — git index-pack should fail.
    let tmp = cm::fresh_bare();
    let pack_path = tmp.path().join("corrupt.pack");
    std::fs::write(&pack_path, &corrupt).unwrap();
    let out = std::process::Command::new("git")
        .current_dir(tmp.path())
        .args(["index-pack", pack_path.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(
        !out.status.success(),
        "index-pack should fail on corrupt pack, but succeeded"
    );
}

/// A tree entry of mode 160000 (submodule gitlink) names a commit in another
/// repository. The gix engine must not try to pack it (it would fail the
/// object lookup) under any filter, and the resulting pack must still be
/// complete and fsck-clean. git/git carries `sha1collisiondetection`.
#[tokio::test]
async fn fetch_skips_gitlink_entries() {
    let root = tempfile::TempDir::new().unwrap();
    let id = RepoId::new("acme", "gitlink").unwrap();
    let repo = LocalRepo::init(root.path(), &id, ObjectFormat::Sha1).unwrap();
    let src = cm::SourceRepo::new();
    cm::run_git(
        &src.dir,
        &[
            "update-index",
            "--add",
            "--cacheinfo",
            "160000,deadbeefdeadbeefdeadbeefdeadbeefdeadbeef,sub",
        ],
    );
    cm::run_git(&src.dir, &["commit", "-q", "-m", "add gitlink"]);
    let head = src.head();
    repo.ingest_pack(
        cm::cursor(pack_no_delta(&src, &["HEAD"])),
        walgit_git::IngestOptions {
            fsck: true,
            max_bytes: None,
            thin: false,
        },
    )
    .await
    .unwrap()
    .unwrap();
    repo.apply_ref_txn(
        &walgit_proto::v1::RefTransaction {
            updates: vec![walgit_proto::v1::RefUpdate {
                name: "refs/heads/main".into(),
                old_oid: "0".repeat(40),
                new_oid: head.clone(),
                new_symbolic_target: String::new(),
                new_peeled: String::new(),
            }],
            push_options: vec![],
            atomic: true,
        },
        true,
    )
    .unwrap();

    for filter in [
        None,
        Some("blob:none"),
        Some("blob:limit=100"),
        Some("tree:0"),
    ] {
        let req = UploadPackRequest {
            wants: vec![oid(&head)],
            haves: vec![],
            done: true,
            thin_pack: false,
            no_progress: true,
            include_tag: false,
            ofs_delta: true,
            sideband_all: false,
            wait_for_done: false,
            filter: filter.map(str::to_owned),
            deepen: None,
            deepen_since: None,
            deepen_not: vec![],
            shallow: vec![],
            want_refs: vec![],
            packfile_uris_protocols: vec![],
        };
        let resp = run_fetch(&repo, Engine::Gix, &req).await;
        let pack = cm::extract_packfile(&resp);
        assert!(pack.starts_with(b"PACK"), "no pack for filter {filter:?}");
        // Unfiltered pack must be complete; filtered packs are partial by design.
        if filter.is_none() {
            let tmp = cm::index_and_fsck(&pack);
            let out = std::process::Command::new("git")
                .current_dir(tmp.path())
                .args(["cat-file", "-e", &head])
                .output()
                .unwrap();
            assert!(out.status.success(), "head missing ({filter:?})");
        }
        // Exactly the source's commits (init + gitlink commit): the gitlink
        // target is foreign and must never be packed; the root tree is always
        // present.
        let (_blobs, commits, trees, _tags) = cm::pack_object_types(&pack);
        assert_eq!(commits, 2, "unexpected commit count ({filter:?})");
        if filter == Some("tree:0") {
            assert_eq!(trees, 0, "tree:0 sends no trees");
        } else {
            assert!(trees >= 1, "tree missing ({filter:?})");
        }
        let tmp = cm::fresh_bare();
        let pack_path = tmp.path().join("objects/pack/pack-test.pack");
        std::fs::create_dir_all(pack_path.parent().unwrap()).unwrap();
        std::fs::write(&pack_path, &pack).unwrap();
        cm::run_git(tmp.path(), &["index-pack", pack_path.to_str().unwrap()]);
        let out = std::process::Command::new("git")
            .current_dir(tmp.path())
            .args(["cat-file", "-e", "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef"])
            .output()
            .unwrap();
        assert!(
            !out.status.success(),
            "gitlink oid must not be packed ({filter:?})"
        );
    }
}

/// Engine comparison on a real repository (`WALGIT_BENCH_REPO=<path to .git
/// or worktree>`; e.g. `walgit synth --size l`). Prints wall times for a
/// diff-sized fetch (want HEAD, have HEAD~50) and a full clone, both engines.
/// `cargo test -p walgit-git --test upload_pack bench_fetch_engines -- --ignored --nocapture`
#[tokio::test]
#[ignore = "benchmark requires WALGIT_BENCH_REPO"]
async fn bench_fetch_engines() {
    let Ok(src_path) = std::env::var("WALGIT_BENCH_REPO") else {
        eprintln!("WALGIT_BENCH_REPO not set; skipping");
        return;
    };
    let src_dir = std::path::PathBuf::from(&src_path);
    let git_dir = {
        let out = std::process::Command::new("git")
            .current_dir(&src_dir)
            .args(["rev-parse", "--git-dir"])
            .output()
            .unwrap();
        let p = String::from_utf8_lossy(&out.stdout).trim().to_string();
        if std::path::Path::new(&p).is_absolute() {
            std::path::PathBuf::from(p)
        } else {
            src_dir.join(p)
        }
    };
    let rev = |r: &str| -> String {
        let out = std::process::Command::new("git")
            .current_dir(&src_dir)
            .args(["rev-parse", r])
            .output()
            .unwrap();
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    };
    let head = rev("HEAD");
    let old = rev("HEAD~50");
    let root = tempfile::TempDir::new().unwrap();
    let id = RepoId::new("bench", "repo").unwrap();
    let repo = LocalRepo::init(root.path(), &id, ObjectFormat::Sha1).unwrap();
    // Install the source's packs directly (copy pack+idx).
    let pack_dir = git_dir.join("objects/pack");
    for ent in std::fs::read_dir(&pack_dir).unwrap() {
        let p = ent.unwrap().path();
        let name = p.file_name().unwrap().to_string_lossy().to_string();
        if name.ends_with(".pack") {
            let idx = p.with_extension("idx");
            let tmp = root.path().join("tmp");
            std::fs::create_dir_all(&tmp).unwrap();
            let tp = tmp.join(&name);
            let ti = tmp.join(idx.file_name().unwrap());
            std::fs::copy(&p, &tp).unwrap();
            std::fs::copy(&idx, &ti).unwrap();
            repo.install_pack(&tp, &ti, &[]).await.unwrap();
        }
    }
    repo.apply_ref_txn(
        &walgit_proto::v1::RefTransaction {
            updates: vec![walgit_proto::v1::RefUpdate {
                name: "refs/heads/main".into(),
                old_oid: "0".repeat(40),
                new_oid: head.clone(),
                new_symbolic_target: String::new(),
                new_peeled: String::new(),
            }],
            push_options: vec![],
            atomic: true,
        },
        true,
    )
    .unwrap();
    let packs = repo.packs().unwrap();
    if let Some(p) = packs.iter().max_by_key(|p| p.pack_size) {
        repo.write_pack_commit_graph(&p.checksum, true)
            .await
            .unwrap();
    }
    eprintln!(
        "repo ready: {} pack(s), commit-graph chain {:?}",
        packs.len(),
        repo.commit_graph_chain().unwrap().len()
    );

    let mk = |wants: Vec<gix_hash::ObjectId>, haves: Vec<gix_hash::ObjectId>| UploadPackRequest {
        wants,
        haves,
        done: true,
        thin_pack: true,
        no_progress: true,
        include_tag: false,
        ofs_delta: true,
        sideband_all: false,
        wait_for_done: false,
        filter: None,
        deepen: None,
        deepen_since: None,
        deepen_not: vec![],
        shallow: vec![],
        want_refs: vec![],
        packfile_uris_protocols: vec![],
    };
    for (label, req) in [
        (
            "diff fetch (want HEAD, have HEAD~50)",
            mk(vec![oid(&head)], vec![oid(&old)]),
        ),
        ("full clone (want HEAD)", mk(vec![oid(&head)], vec![])),
    ] {
        for engine in [Engine::Git, Engine::Gix] {
            let t = std::time::Instant::now();
            let resp = run_fetch(&repo, engine, &req).await;
            let ms = t.elapsed().as_millis();
            let pack = cm::extract_packfile(&resp);
            let n = u32::from_be_bytes([pack[8], pack[9], pack[10], pack[11]]);
            eprintln!(
                "{label:40} {engine:?}: {ms:6} ms, {n} objects, {} bytes",
                pack.len()
            );
        }
    }
}
