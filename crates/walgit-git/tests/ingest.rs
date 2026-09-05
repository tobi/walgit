#![allow(clippy::format_collect)]

mod common;

use std::path::Path;

use std::io::Write;
use std::process::{Command, Stdio};
use walgit_git::{IngestOptions, LocalRepo, ObjectFormat, RepoId, gix_hash};

mod cm {
    pub use super::common::*;
}

#[tokio::test]
async fn ingest_pack_objects_present_fsck_ok() {
    let root = tempfile::TempDir::new().unwrap();
    let id = RepoId::new("acme", "repo").unwrap();
    let repo = LocalRepo::init(root.path(), &id, ObjectFormat::Sha1).unwrap();

    let src = cm::SourceRepo::new(); // commit A: file1
    let b = src.commit_file("file2.txt", "world\n", "second"); // commit B
    let pack = src.pack(&["HEAD"], &[], false);
    assert!(!pack.is_empty());

    // Pack checksum = trailing 20 bytes.
    let checksum = gix_hash::ObjectId::try_from(&pack[pack.len() - 20..]).unwrap();
    let ingested = repo
        .ingest_pack(
            cm::cursor(pack.clone()),
            IngestOptions {
                fsck: true,
                max_bytes: None,
                thin: false,
            },
        )
        .await
        .expect("ingest")
        .expect("some pack");
    assert_eq!(ingested.checksum, checksum);
    assert!(ingested.pack_path.exists());
    assert!(ingested.idx_path.exists());
    assert!(ingested.object_count > 0);
    assert_eq!(
        ingested.pack_path.file_name().unwrap().to_string_lossy(),
        format!("pack-{checksum}.pack")
    );

    // Objects present.
    let b_oid = gix_hash::ObjectId::from_hex(b.as_bytes()).unwrap();
    assert!(repo.has_object(&b_oid));
    // The root tree of B exists.
    let head_tree = cm::run_git(src.dir.as_path(), &["rev-parse", "HEAD^{tree}"]);
    let tree_oid = gix_hash::ObjectId::from_hex(head_tree.trim().as_bytes()).unwrap();
    assert!(repo.has_object(&tree_oid));

    // packs() listing.
    let packs = repo.packs().unwrap();
    assert_eq!(packs.len(), 1);
    assert_eq!(packs[0].checksum, checksum);

    // git fsck on the bare repo.
    let fsck = repo.git(&["fsck", "--full", "--strict"]).await.unwrap();
    assert!(
        fsck.status.success(),
        "fsck: {}",
        String::from_utf8_lossy(&fsck.stderr)
    );

    // max_bytes enforcement.
    let too_small = repo
        .ingest_pack(
            cm::cursor(pack.clone()),
            IngestOptions {
                fsck: false,
                max_bytes: Some(8),
                thin: false,
            },
        )
        .await;
    assert!(matches!(
        too_small,
        Err(walgit_git::GitError::InvalidInput(_))
    ));

    // empty body => Ok(None).
    let empty = repo
        .ingest_pack(
            cm::cursor(Vec::new()),
            IngestOptions {
                fsck: false,
                max_bytes: None,
                thin: false,
            },
        )
        .await
        .unwrap();
    assert!(empty.is_none());
}

#[tokio::test]
async fn ingest_thin_pack_resolves_against_odb() {
    let root = tempfile::TempDir::new().unwrap();
    let id = RepoId::new("acme", "thin").unwrap();
    let repo = LocalRepo::init(root.path(), &id, ObjectFormat::Sha1).unwrap();

    let src = cm::SourceRepo::new(); // commit A
    let a = src.head();
    src.commit_file("file2.txt", "world\n", "second"); // commit B
    let b = src.head();

    // Base pack with only A.
    let base = src.pack(&[a.as_str()], &[], false);
    repo.ingest_pack(
        cm::cursor(base),
        IngestOptions {
            fsck: true,
            max_bytes: None,
            thin: false,
        },
    )
    .await
    .unwrap()
    .unwrap();

    // Thin pack: B minus A.
    let thin = src.pack(&[b.as_str()], &[a.as_str()], true);
    assert!(!thin.is_empty());
    let ingested = repo
        .ingest_pack(
            cm::cursor(thin),
            IngestOptions {
                fsck: true,
                max_bytes: None,
                thin: true,
            },
        )
        .await
        .expect("ingest thin")
        .expect("some pack");
    // After --fix-thin, the pack is self-contained; B is present.
    let b_oid = gix_hash::ObjectId::from_hex(b.as_bytes()).unwrap();
    assert!(repo.has_object(&b_oid));
    // index-pack --rev-index writes the side-file so the next pack-objects
    // does not rebuild a reverse index in RAM.
    assert!(
        ingested.pack_path.with_extension("rev").exists(),
        "thin ingest must write pack-<sha>.rev"
    );
    // fsck clean (thin bases resolved).
    let fsck = repo.git(&["fsck", "--full"]).await.unwrap();
    assert!(
        fsck.status.success(),
        "fsck: {}",
        String::from_utf8_lossy(&fsck.stderr)
    );
    let _ = ingested;
}

#[tokio::test]
async fn install_and_remove_pack() {
    let root = tempfile::TempDir::new().unwrap();
    let id = RepoId::new("acme", "ir").unwrap();
    let repo = LocalRepo::init(root.path(), &id, ObjectFormat::Sha1).unwrap();
    let src = cm::SourceRepo::new();
    let pack = src.pack(&["HEAD"], &[], false);
    let ingested = repo
        .ingest_pack(
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
    let checksum = ingested.checksum;
    assert_eq!(repo.packs().unwrap().len(), 1);
    repo.remove_pack(&checksum).unwrap();
    assert!(repo.packs().unwrap().is_empty());
    // pack_path returns the conventional path even when absent.
    let p = repo.pack_path(&checksum);
    assert!(Path::new(&p).ends_with(format!("pack-{checksum}.pack")));
}

#[tokio::test]
async fn ingest_distinct_packs_concurrently() {
    let root = tempfile::TempDir::new().unwrap();
    let id = RepoId::new("acme", "concurrent").unwrap();
    let repo = LocalRepo::init(root.path(), &id, ObjectFormat::Sha1).unwrap();
    let src = cm::SourceRepo::new();

    let mut packs = Vec::new();
    let mut parent = src.head();
    for n in 0..16 {
        let head = src.commit_file(
            &format!("file-{n}.txt"),
            &format!("contents-{n}\n"),
            &format!("commit-{n}"),
        );
        packs.push(src.pack(&[head.as_str()], &[parent.as_str()], false));
        parent = head;
    }

    let results = futures::future::join_all(packs.into_iter().map(|pack| {
        let repo = repo.clone();
        async move {
            repo.ingest_pack(
                cm::cursor(pack),
                IngestOptions {
                    fsck: false,
                    max_bytes: None,
                    thin: false,
                },
            )
            .await
        }
    }))
    .await;
    for result in results {
        result.unwrap().expect("each distinct pack is ingested");
    }
    assert_eq!(repo.packs().unwrap().len(), 16);
}

#[tokio::test]
async fn ingest_large_delta_pack() {
    let source = tempfile::TempDir::new().unwrap();
    cm::run_git(
        source.path(),
        &["init", "-q", "--bare", source.path().to_str().unwrap()],
    );
    let mut stream = String::new();
    for i in 1..=2000 {
        {
            let _ = std::fmt::Write::write_fmt(
                &mut stream,
                format_args!(
                    "commit refs/heads/main\nmark :{i}\nauthor bench <bench@example.com> {i} +0000\ncommitter bench <bench@example.com> {i} +0000\n"
                ),
            );
        };
        let message = format!("commit {i}\n");
        {
            let _ = std::fmt::Write::write_fmt(
                &mut stream,
                format_args!("data {}\n{}\n", message.len(), message),
            );
        };
        if i > 1 {
            {
                let _ = std::fmt::Write::write_fmt(&mut stream, format_args!("from :{}\n", i - 1));
            };
        }
        stream.push_str("M 100644 inline file.txt\n");
        let content = format!("content {i} {}\n", "x".repeat(256));
        {
            let _ = std::fmt::Write::write_fmt(
                &mut stream,
                format_args!("data {}\n{}\n", content.len(), content),
            );
        };
    }
    let mut fast_import = Command::new("git")
        .current_dir(source.path())
        .args(["fast-import"])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let _ = fast_import
        .stdin
        .take()
        .unwrap()
        .write_all(stream.as_bytes());
    let imported = fast_import.wait_with_output().unwrap();
    assert!(
        imported.status.success(),
        "fast-import: {}",
        String::from_utf8_lossy(&imported.stderr)
    );

    let head = cm::run_git(source.path(), &["rev-parse", "refs/heads/main"]);
    let base = cm::run_git(source.path(), &["rev-parse", "refs/heads/main~1"]);
    let full = {
        let out = Command::new("git")
            .current_dir(source.path())
            .args(["pack-objects", "--all", "--stdout"])
            .output()
            .unwrap();
        assert!(out.status.success());
        out.stdout
    };
    assert!(full.starts_with(b"PACK"));
    let expected_count = u64::from(u32::from_be_bytes(full[8..12].try_into().unwrap()));

    let full_root = tempfile::TempDir::new().unwrap();
    let full_repo = LocalRepo::init(
        full_root.path(),
        &RepoId::new("acme", "large-full").unwrap(),
        ObjectFormat::Sha1,
    )
    .unwrap();
    let full_ingested = full_repo
        .ingest_pack(
            cm::cursor(full),
            IngestOptions {
                fsck: true,
                max_bytes: None,
                thin: false,
            },
        )
        .await
        .expect("large full pack")
        .expect("full pack is non-empty");
    assert_eq!(full_ingested.object_count, expected_count);
    let fsck = full_repo
        .git(&["fsck", "--full", "--strict"])
        .await
        .unwrap();
    assert!(
        fsck.status.success(),
        "full fsck: {}",
        String::from_utf8_lossy(&fsck.stderr)
    );

    let base_pack = {
        let mut child = Command::new("git")
            .current_dir(source.path())
            .args(["pack-objects", "--stdout", "--revs"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        child
            .stdin
            .take()
            .unwrap()
            .write_all(format!("{base}\n").as_bytes())
            .unwrap();
        let out = child.wait_with_output().unwrap();
        assert!(out.status.success());
        out.stdout
    };
    let thin_pack = {
        let mut child = Command::new("git")
            .current_dir(source.path())
            .args(["pack-objects", "--stdout", "--revs", "--thin"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        child
            .stdin
            .take()
            .unwrap()
            .write_all(format!("{head}\n^{base}\n").as_bytes())
            .unwrap();
        let out = child.wait_with_output().unwrap();
        assert!(out.status.success());
        out.stdout
    };
    let thin_root = tempfile::TempDir::new().unwrap();
    let thin_repo = LocalRepo::init(
        thin_root.path(),
        &RepoId::new("acme", "large-thin").unwrap(),
        ObjectFormat::Sha1,
    )
    .unwrap();
    thin_repo
        .ingest_pack(
            cm::cursor(base_pack),
            IngestOptions {
                fsck: false,
                max_bytes: None,
                thin: false,
            },
        )
        .await
        .unwrap();
    let thin_ingested = thin_repo
        .ingest_pack(
            cm::cursor(thin_pack),
            IngestOptions {
                fsck: true,
                max_bytes: None,
                thin: true,
            },
        )
        .await
        .expect("large thin pack")
        .expect("thin pack is non-empty");
    assert!(thin_ingested.object_count > 0);
    let head_oid = gix_hash::ObjectId::from_hex(head.trim().as_bytes()).unwrap();
    assert!(thin_repo.has_object(&head_oid));
    let fsck = thin_repo
        .git(&["fsck", "--full", "--strict"])
        .await
        .unwrap();
    assert!(
        fsck.status.success(),
        "thin fsck: {}",
        String::from_utf8_lossy(&fsck.stderr)
    );
}

/// The failure branches of the one ingest path (`git index-pack`; the gix engine is gone since
/// 2026-08-21): each refuses with the cause in the error, leaves no pack behind, and the repo keeps
/// serving. Oversize is caught while streaming (before index-pack), everything else by index-pack.
#[tokio::test]
async fn ingest_failures_name_the_cause_and_leave_nothing_behind() {
    let root = tempfile::TempDir::new().unwrap();
    let id = RepoId::new("acme", "bad").unwrap();
    let repo = LocalRepo::init(root.path(), &id, ObjectFormat::Sha1).unwrap();
    let src = cm::SourceRepo::new();
    // A big blob, then a one-line edit: `pack-objects --thin ^a b` deltas the new blob against the
    // excluded one, so the thin pack really has an external base (tiny files produce no delta).
    let big: String = (0..4000).map(|i| format!("line {i}\n")).collect();
    let a = src.commit_file("big.txt", &big, "big");
    let b = src.commit_file("big.txt", &format!("{big}tail\n"), "edit");
    let opts = |thin: bool, max_bytes: Option<u64>| IngestOptions {
        fsck: true,
        max_bytes,
        thin,
    };
    let pack_count = || repo.packs().map_or(0, |p| p.len());

    // 1. Oversize: refused while streaming, before index-pack ever runs.
    let full = src.pack(&[b.as_str()], &[], false);
    let err = repo
        .ingest_pack(cm::cursor(full.clone()), opts(false, Some(64)))
        .await
        .expect_err("too big");
    assert!(err.to_string().contains("max_bytes 64"), "{err}");
    assert_eq!(pack_count(), 0);

    // 2. Corrupt bytes: index-pack fails; its stderr is the error.
    let mut corrupt = full.clone();
    let mid = corrupt.len() / 2;
    corrupt[mid] ^= 0xff;
    let err = repo
        .ingest_pack(cm::cursor(corrupt), opts(false, None))
        .await
        .expect_err("corrupt");
    let s = err.to_string();
    assert!(
        s.contains("index-pack")
            && (s.contains("inflate")
                || s.contains("corrupt")
                || s.contains("bad")
                || s.contains("mismatch")),
        "{s}"
    );
    assert_eq!(pack_count(), 0);

    // 3. A thin pack whose base is not in the ODB: --fix-thin cannot complete it.
    let thin = src.pack(&[b.as_str()], &[a.as_str()], true);
    assert!(
        thin.len() < full.len() / 2,
        "test setup: the pack must be thin ({} vs {} bytes)",
        thin.len(),
        full.len()
    );
    let err = repo
        .ingest_pack(cm::cursor(thin.clone()), opts(true, None))
        .await
        .expect_err("no base");
    assert!(err.to_string().contains("index-pack"), "{err}");
    assert_eq!(pack_count(), 0);

    // 4. A thin pack ingested without `thin` (no --fix-thin): refused too, not silently incomplete.
    let err = repo
        .ingest_pack(cm::cursor(thin), opts(false, None))
        .await
        .expect_err("thin without fix-thin");
    assert!(err.to_string().contains("index-pack"), "{err}");
    assert_eq!(pack_count(), 0);

    // 5. fsck: a commit with a malformed header is rejected when fsck is on…
    let bad_commit = src.bad_object();
    let pack = src.pack(&[bad_commit.as_str()], &[], false);
    let err = repo
        .ingest_pack(cm::cursor(pack.clone()), opts(false, None))
        .await
        .expect_err("fsck");
    assert!(err.to_string().contains("index-pack"), "{err}");
    assert_eq!(pack_count(), 0);
    // …and accepted with fsck off (the knob is `wal.fsck_objects`).
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
    assert_eq!(pack_count(), 1);

    // 6. A ref-only push sends a pack with zero objects: Ok(None), nothing installed.
    let empty = src.pack(&[], &[], false);
    assert!(
        repo.ingest_pack(cm::cursor(empty), opts(false, None))
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(pack_count(), 1);
    // No temp files left under objects/pack.
    let leftovers: Vec<_> = std::fs::read_dir(repo.path().join("objects/pack"))
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|n| !n.starts_with("pack-"))
        .collect();
    assert!(leftovers.is_empty(), "{leftovers:?}");
}
