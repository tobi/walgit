mod common;

use std::time::Instant;

use walgit_git::{LocalRepo, ObjectFormat, RefSnapshotData, RepoId, gix_hash};
use walgit_proto::v1::{Ref, RefSnapshot, RefTransaction, RefUpdate};

mod cm {
    pub use super::common::*;
}

fn tx(updates: Vec<RefUpdate>) -> RefTransaction {
    RefTransaction {
        updates,
        push_options: vec![],
        atomic: true,
    }
}

fn update(name: &str, old: &str, new: &str) -> RefUpdate {
    RefUpdate {
        name: name.to_string(),
        old_oid: old.to_string(),
        new_oid: new.to_string(),
        new_symbolic_target: String::new(),
        new_peeled: String::new(),
    }
}

#[tokio::test]
async fn ref_txn_atomicity_and_conflicts() {
    let root = tempfile::TempDir::new().unwrap();
    let id = RepoId::new("acme", "refs").unwrap();
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

    let zero = "0".repeat(40);

    // Create refs/heads/main = B.
    repo.apply_ref_txn(&tx(vec![update("refs/heads/main", &zero, &b)]), true)
        .unwrap();
    let snap = repo.refs().unwrap();
    assert_eq!(
        snap.refs
            .iter()
            .find(|r| r.name == "refs/heads/main")
            .map(|r| r.oid.clone())
            .unwrap(),
        b
    );

    // Update to A with correct old.
    repo.apply_ref_txn(&tx(vec![update("refs/heads/main", &b, &a)]), true)
        .unwrap();
    assert_eq!(
        repo.refs()
            .unwrap()
            .refs
            .iter()
            .find(|r| r.name == "refs/heads/main")
            .unwrap()
            .oid,
        a
    );

    // Conflict: wrong old value.
    let err = repo
        .apply_ref_txn(&tx(vec![update("refs/heads/main", &b, &a)]), true)
        .unwrap_err();
    assert!(
        matches!(err, walgit_git::GitError::RefConflict { ref name, .. } if name == "refs/heads/main")
    );

    // Conflict: create when exists (old zero).
    let err = repo
        .apply_ref_txn(&tx(vec![update("refs/heads/main", &zero, &b)]), true)
        .unwrap_err();
    assert!(matches!(err, walgit_git::GitError::RefConflict { .. }));

    // Atomic multi-ref: both succeed or neither.
    let err = repo
        .apply_ref_txn(
            &tx(vec![
                update("refs/heads/dev", &zero, &b),
                update("refs/heads/main", &b, &a), // wrong old -> whole txn aborts
            ]),
            true,
        )
        .unwrap_err();
    assert!(matches!(err, walgit_git::GitError::RefConflict { .. }));
    // dev must NOT exist (atomic abort).
    assert!(
        repo.refs()
            .unwrap()
            .refs
            .iter()
            .find(|r| r.name == "refs/heads/dev")
            .is_none()
    );

    // HEAD symbolic update.
    repo.apply_ref_txn(
        &tx(vec![RefUpdate {
            name: "HEAD".to_string(),
            old_oid: String::new(),
            new_oid: String::new(),
            new_symbolic_target: "refs/heads/main".to_string(),
            new_peeled: String::new(),
        }]),
        true,
    )
    .unwrap();
    assert_eq!(repo.refs().unwrap().head_target, "refs/heads/main");

    // Delete ref.
    repo.apply_ref_txn(&tx(vec![update("refs/heads/main", &a, &zero)]), true)
        .unwrap();
    assert!(
        repo.refs()
            .unwrap()
            .refs
            .iter()
            .find(|r| r.name == "refs/heads/main")
            .is_none()
    );
}

#[tokio::test]
async fn load_ref_snapshot_50k_refs_fast_and_readable() {
    let root = tempfile::TempDir::new().unwrap();
    let id = RepoId::new("acme", "big").unwrap();
    let repo = LocalRepo::init(root.path(), &id, ObjectFormat::Sha1).unwrap();
    let src = cm::SourceRepo::new();
    let head = src.head();
    let pack = src.pack(&["HEAD"], &[], false);
    repo.ingest_pack(
        cm::cursor(pack),
        walgit_git::IngestOptions {
            fsck: false,
            max_bytes: None,
            thin: false,
        },
    )
    .await
    .unwrap()
    .unwrap();

    let n = 50_000usize;
    let refs: Vec<Ref> = (0..n)
        .map(|i| Ref {
            name: format!("refs/heads/b{i:05}"),
            oid: head.clone(),
            peeled: String::new(),
        })
        .collect();
    let snap = RefSnapshot {
        seq: 0,
        object_format: "sha1".into(),
        refs,
        head_target: "refs/heads/b00000".to_string(),
        created_at: None,
    };

    let t = Instant::now();
    repo.load_ref_snapshot(&snap).unwrap();
    let elapsed = t.elapsed();
    assert!(
        elapsed.as_secs_f64() < 1.0,
        "load_ref_snapshot took {elapsed:?}"
    );

    // Readable by upstream git for-each-ref.
    let out = repo
        .git(&["for-each-ref", "--format=%(refname)"])
        .await
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let count = String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter(|l| !l.is_empty())
        .count();
    assert_eq!(count, n);

    // refs() matches.
    let data: RefSnapshotData = repo.refs().unwrap();
    assert_eq!(data.refs.len(), n);
    assert_eq!(data.head_target, "refs/heads/b00000");

    // pack_refs collapses loose refs (already packed) — still readable.
    repo.pack_refs().unwrap();
    let out = repo.git(&["for-each-ref", "--count=999999"]).await.unwrap();
    let count = String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter(|l| !l.is_empty())
        .count();
    assert_eq!(count, n);
}

#[test]
fn ref_snapshot_roundtrip() {
    let data = RefSnapshotData {
        refs: vec![walgit_git::Ref {
            name: "refs/heads/main".into(),
            oid: "abc".into(),
            peeled: "def".into(),
        }],
        head_target: "refs/heads/main".into(),
    };
    let snap: RefSnapshot = data.clone().into();
    let back: RefSnapshotData = snap.into();
    assert_eq!(back, data);
}

fn _unused(_: gix_hash::ObjectId) {}

/// `RefView`: point lookups over the sorted snapshot with an overlay — HEAD
/// resolves through its target, the overlay wins, removals read as absent,
/// and a 466 k-ref verify costs microseconds, not an O(n) map.
#[test]
fn ref_view_lookups_are_logarithmic_and_overlay_aware() {
    use walgit_git::{Ref, RefSnapshotData, RefView};
    let n = 466_395u32;
    let mut refs: Vec<Ref> = (0..n)
        .map(|i| Ref {
            name: format!("refs/heads/ref-{i:06}"),
            oid: format!("{i:040x}"),
            peeled: String::new(),
        })
        .collect();
    refs.sort_by(|a, b| a.name.cmp(&b.name));
    let snap = std::sync::Arc::new(RefSnapshotData {
        refs,
        head_target: "refs/heads/ref-000007".into(),
    });
    let t = std::time::Instant::now();
    let mut view = RefView::new(snap.clone());
    assert_eq!(
        view.get("refs/heads/ref-123456").as_deref(),
        Some(format!("{:040x}", 123_456).as_str())
    );
    assert_eq!(
        view.get("HEAD").as_deref(),
        Some(format!("{:040x}", 7).as_str()),
        "HEAD through its target"
    );
    assert!(view.get("refs/heads/nope").is_none());
    view.set("refs/heads/ref-123456", "a".repeat(40));
    assert_eq!(
        view.get("refs/heads/ref-123456").as_deref(),
        Some("a".repeat(40).as_str()),
        "overlay wins"
    );
    view.remove("refs/heads/ref-000007");
    assert!(view.get("refs/heads/ref-000007").is_none());
    assert!(
        view.get("HEAD").is_none(),
        "HEAD follows the (now removed) target"
    );
    view.set("HEAD", "b".repeat(40));
    assert_eq!(view.get("HEAD").as_deref(), Some("b".repeat(40).as_str()));
    for i in 0..1000u32 {
        let _ = view.get(&format!("refs/heads/ref-{:06}", i * 400));
    }
    let el = t.elapsed();
    assert!(
        el.as_millis() < 200,
        "1000 lookups over 466 k refs took {el:?}"
    );
}
