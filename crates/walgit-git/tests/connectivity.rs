// Test fixtures use panics to fail the test, including shared helper functions.
#![allow(clippy::unwrap_used)]

mod common;

use walgit_git::{GitError, IngestOptions, LocalRepo, ObjectFormat, RepoId, gix_hash};

mod cm {
    pub use super::common::*;
}

async fn setup() -> (tempfile::TempDir, LocalRepo, String, String) {
    let root = tempfile::TempDir::new().unwrap();
    let id = RepoId::new("acme", "conn").unwrap();
    let repo = LocalRepo::init(root.path(), &id, ObjectFormat::Sha1).unwrap();
    let src = cm::SourceRepo::new();
    let a = src.head();
    let b = src.commit_file("file2.txt", "world\n", "second");
    let pack = src.pack(&["HEAD"], &[], false);
    repo.ingest_pack(
        cm::cursor(pack),
        IngestOptions {
            fsck: true,
            max_bytes: None,
            thin: false,
        },
    )
    .await
    .unwrap()
    .unwrap();
    (root, repo, a, b)
}

#[tokio::test]
async fn connectivity_pass() {
    let (_r, repo, _a, b) = setup().await;
    let b_oid = gix_hash::ObjectId::from_hex(b.as_bytes()).unwrap();
    repo.check_connectivity(&[b_oid], false).unwrap();
}

#[tokio::test]
async fn connectivity_missing_tip() {
    let (_r, repo, _a, _b) = setup().await;
    // A random oid that does not exist.
    let fake = gix_hash::ObjectId::from_hex(b"deadbeefdeadbeefdeadbeefdeadbeefdeadbeef").unwrap();
    let err = repo.check_connectivity(&[fake], false).unwrap_err();
    assert!(matches!(err, GitError::MissingObject { .. }));
}

#[tokio::test]
async fn connectivity_stop_at_existing_refs() {
    let (_r, repo, a, b) = setup().await;
    // Set a ref to A.
    let zero = "0".repeat(40);
    let txn = walgit_proto::v1::RefTransaction {
        updates: vec![walgit_proto::v1::RefUpdate {
            name: "refs/heads/base".into(),
            old_oid: zero,
            new_oid: a.clone(),
            new_symbolic_target: String::new(),
            new_peeled: String::new(),
        }],
        push_options: vec![],
        atomic: true,
    };
    repo.apply_ref_txn(&txn, true).unwrap();
    let b_oid = gix_hash::ObjectId::from_hex(b.as_bytes()).unwrap();
    // B reachable minus A's reachable: only the new commit/tree/blob; all present.
    repo.check_connectivity(&[b_oid], true).unwrap();
}

#[tokio::test]
async fn connectivity_empty_tips_ok() {
    let (_r, repo, _a, _b) = setup().await;
    repo.check_connectivity(&[], false).unwrap();
}

#[tokio::test]
async fn connectivity_annotated_tag_tip_after_commits_exist() {
    // Push 1: commits. Push 2: an annotated tag whose pack contains only the
    // tag object (the commit already exists and is an existing ref tip).
    let root = tempfile::TempDir::new().unwrap();
    let id = RepoId::new("acme", "tagtip").unwrap();
    let repo = LocalRepo::init(root.path(), &id, ObjectFormat::Sha1).unwrap();
    let src = cm::SourceRepo::new();
    let head = src.head();
    let pack = src.pack(&["HEAD"], &[], false);
    repo.ingest_pack(
        cm::cursor(pack),
        IngestOptions {
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
        false,
    )
    .unwrap();

    let tag = src.annotated_tag("v1", &head);
    let pack = src.pack(&["refs/tags/v1"], &["HEAD"], false);
    repo.ingest_pack(
        cm::cursor(pack),
        IngestOptions {
            fsck: true,
            max_bytes: None,
            thin: false,
        },
    )
    .await
    .unwrap()
    .unwrap();
    let tag_oid = gix_hash::ObjectId::from_hex(tag.as_bytes()).unwrap();
    repo.check_connectivity(&[tag_oid], true).unwrap();
    repo.check_connectivity(&[tag_oid], false).unwrap();

    // A tag object pointing at a missing commit must be rejected.
    let dangling = src.commit_file("later.txt", "x\n", "later");
    let tag2 = src.annotated_tag("v2", &dangling);
    let pack = src.pack(&["refs/tags/v2"], &[&dangling], false); // only the tag object
    repo.ingest_pack(
        cm::cursor(pack),
        IngestOptions {
            fsck: false,
            max_bytes: None,
            thin: false,
        },
    )
    .await
    .unwrap();
    let tag2_oid = gix_hash::ObjectId::from_hex(tag2.as_bytes()).unwrap();
    assert!(matches!(
        repo.check_connectivity(&[tag2_oid], true),
        Err(GitError::MissingObject { .. })
    ));
}

/// A pushed tree containing a submodule (gitlink) must pass: the gitlink's
/// commit lives in another repository and is never expected here
/// (regression: git/git's sha1collisiondetection made every push fail with
/// "delegate cancelled").
#[tokio::test]
async fn connectivity_tolerates_gitlinks() {
    let root = tempfile::TempDir::new().unwrap();
    let id = RepoId::new("acme", "sub").unwrap();
    let repo = LocalRepo::init(root.path(), &id, ObjectFormat::Sha1).unwrap();
    let src = cm::SourceRepo::new();
    // Add a gitlink entry pointing at a commit that exists nowhere.
    cm::run_git(
        &src.dir,
        &[
            "update-index",
            "--add",
            "--cacheinfo",
            "160000,deadbeefdeadbeefdeadbeefdeadbeefdeadbeef,vendor/sub",
        ],
    );
    cm::run_git(
        &src.dir,
        &[
            "-c",
            "user.email=t@t",
            "-c",
            "user.name=T",
            "commit",
            "-q",
            "-m",
            "add submodule",
        ],
    );
    let tip = src.head();
    let pack = src.pack(&["HEAD"], &[], false);
    repo.ingest_pack(
        cm::cursor(pack),
        IngestOptions {
            fsck: false,
            max_bytes: None,
            thin: false,
        },
    )
    .await
    .unwrap()
    .unwrap();
    let tip_oid = gix_hash::ObjectId::from_hex(tip.as_bytes()).unwrap();
    repo.check_connectivity(&[tip_oid], false).unwrap();
}
