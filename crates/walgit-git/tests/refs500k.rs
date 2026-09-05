// Test fixtures use panics to fail the test, including shared helper functions.
#![allow(
    clippy::unwrap_used,
    clippy::ignore_without_reason,
    clippy::used_underscore_binding
)]

//! `cargo test -p walgit-git --test refs500k -- --ignored --nocapture`: the per-push ref
//! bookkeeping at 500 k refs (AGENTS §1.4: cost must not scale with ref count on a hot path).
use std::io::Write;
use std::time::Instant;
use walgit_git::{LocalRepo, ObjectFormat, RepoId};

fn commit(dir: &std::path::Path, msg: &str) -> String {
    let out = std::process::Command::new("git")
        .args([
            "-C",
            dir.to_str().unwrap(),
            "-c",
            "user.email=t@t",
            "-c",
            "user.name=T",
            "commit-tree",
            "4b825dc642cb6eb9a060e54bf8d69288fbee4904",
            "-m",
            msg,
        ])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap().trim().to_string()
}

fn fixture(n_heads: usize, n_tags: usize) -> (tempfile::TempDir, LocalRepo) {
    let root = tempfile::tempdir().unwrap();
    let id = RepoId::new("t", "refs500k").unwrap();
    let repo = LocalRepo::init(root.path(), &id, ObjectFormat::Sha1).unwrap();
    let dir = repo.path().to_path_buf();
    // An empty tree commit and an annotated tag object to point at.
    let _ = std::process::Command::new("git")
        .args([
            "-C",
            dir.to_str().unwrap(),
            "hash-object",
            "-w",
            "-t",
            "tree",
            "--stdin",
        ])
        .stdin(std::process::Stdio::piped())
        .output();
    let c = commit(&dir, "one");
    let tag = {
        let out = std::process::Command::new("git")
            .args([
                "-C",
                dir.to_str().unwrap(),
                "-c",
                "user.email=t@t",
                "-c",
                "user.name=T",
                "mktag",
            ])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .and_then(|mut ch| {
                ch.stdin.as_mut().unwrap().write_all(
                    format!(
                        "object {c}\ntype commit\ntag v\ntagger T <t@t> 1700000000 +0000\n\nv\n"
                    )
                    .as_bytes(),
                )?;
                ch.wait_with_output()
            })
            .unwrap();
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8(out.stdout).unwrap().trim().to_string()
    };
    let mut packed = String::from("# pack-refs with: peeled fully-peeled sorted \n");
    let mut names: Vec<String> = (0..n_heads)
        .map(|i| format!("refs/heads/team{}/branch-{i:06}", i % 97))
        .collect();
    names.extend((0..n_tags).map(|i| format!("refs/tags/v{i:06}")));
    names.sort();
    for n in &names {
        if n.starts_with("refs/tags/") {
            {
                let _ = std::fmt::Write::write_fmt(&mut packed, format_args!("{tag} {n}\n^{c}\n"));
            };
        } else {
            {
                let _ = std::fmt::Write::write_fmt(&mut packed, format_args!("{c} {n}\n"));
            };
        }
    }
    std::fs::write(dir.join("packed-refs"), packed).unwrap();
    std::fs::write(dir.join("HEAD"), "ref: refs/heads/team0/branch-000000\n").unwrap();
    repo.refresh().unwrap();
    (root, repo)
}

fn txn(name: &str, old: &str, new: &str) -> walgit_proto::v1::RefTransaction {
    walgit_proto::v1::RefTransaction {
        updates: vec![walgit_proto::v1::RefUpdate {
            name: name.into(),
            old_oid: old.into(),
            new_oid: new.into(),
            ..Default::default()
        }],
        ..Default::default()
    }
}

#[test]
#[ignore = "500k ref benchmark; run in test-slow tier"]
fn push_bookkeeping_at_500k_refs() {
    let (_root, repo) = fixture(400_000, 100_000);
    let c2 = commit(repo.path(), "two");
    let zero = "0".repeat(40);
    let t = Instant::now();
    let snap = repo.refs_arc().unwrap();
    println!(
        "cold refs parse: {} ms ({} refs)",
        t.elapsed().as_millis(),
        snap.refs.len()
    );
    let t = Instant::now();
    let _ = repo.refs_arc().unwrap();
    println!("warm refs_arc: {} µs", t.elapsed().as_micros());
    for i in 0..3 {
        let name = format!("refs/heads/new-{i}");
        let t = Instant::now();
        repo.apply_ref_txn(&txn(&name, &zero, &c2), true).unwrap();
        let applied = t.elapsed();
        let t = Instant::now();
        let snap = repo.refs_arc().unwrap();
        let after = t.elapsed();
        println!(
            "push {i}: apply_ref_txn {} ms, refs_arc after {} ms ({} refs)",
            applied.as_millis(),
            after.as_millis(),
            snap.refs.len()
        );
    }
    // Update an existing packed ref, delete one.
    let t = Instant::now();
    repo.apply_ref_txn(
        &txn(
            "refs/heads/team3/branch-000003",
            &snap_oid(&repo, "refs/heads/team3/branch-000003"),
            &c2,
        ),
        true,
    )
    .unwrap();
    println!("update packed ref: {} ms", t.elapsed().as_millis());
    let t = Instant::now();
    let _ = repo.refs_arc().unwrap();
    println!("  refs_arc after: {} ms", t.elapsed().as_millis());
    let t = Instant::now();
    repo.apply_ref_txn(
        &txn(
            "refs/heads/team5/branch-000005",
            &snap_oid(&repo, "refs/heads/team5/branch-000005"),
            &zero,
        ),
        true,
    )
    .unwrap();
    println!("delete packed ref: {} ms", t.elapsed().as_millis());
    let t = Instant::now();
    let _ = repo.refs_arc().unwrap();
    println!("  refs_arc after: {} ms", t.elapsed().as_millis());
}

fn snap_oid(repo: &LocalRepo, name: &str) -> String {
    repo.refs_arc()
        .unwrap()
        .refs
        .iter()
        .find(|r| r.name == name)
        .unwrap()
        .oid
        .clone()
}

/// Not ignored: a push touching k refs must not re-parse the ref set (AGENTS §1.4). The
/// snapshot is patched from the transactions; `ref_view` sees them without materializing;
/// what a fresh parse would produce is exactly what the patched snapshot says (create,
/// update, delete of a packed ref, a new annotated tag with its peel, a HEAD symref move).
#[test]
fn pushes_patch_the_refs_cache_instead_of_reparsing() {
    let (_root, repo) = fixture(2_000, 500);
    let c2 = commit(repo.path(), "two");
    let zero = "0".repeat(40);
    let base = repo.refs_arc().unwrap();
    assert_eq!(repo.refs_parses(), 1);
    let existing = base
        .refs
        .iter()
        .find(|r| r.name.starts_with("refs/heads/team3/"))
        .unwrap()
        .clone();
    let victim = base
        .refs
        .iter()
        .find(|r| r.name.starts_with("refs/heads/team5/"))
        .unwrap()
        .clone();
    let tag_oid = base
        .refs
        .iter()
        .find(|r| r.name.starts_with("refs/tags/"))
        .unwrap()
        .oid
        .clone();

    repo.apply_ref_txn(&txn("refs/heads/new-branch", &zero, &c2), true)
        .unwrap();
    repo.apply_ref_txn(&txn(&existing.name, &existing.oid, &c2), true)
        .unwrap();
    repo.apply_ref_txn(&txn(&victim.name, &victim.oid, &zero), true)
        .unwrap();
    // New annotated tag (peel must come from the object, the update carries none).
    repo.apply_ref_txn(&txn("refs/tags/zz-new", &zero, &tag_oid), true)
        .unwrap();
    // HEAD → another branch.
    repo.apply_ref_txn(
        &walgit_proto::v1::RefTransaction {
            updates: vec![walgit_proto::v1::RefUpdate {
                name: "HEAD".into(),
                new_symbolic_target: "refs/heads/new-branch".into(),
                ..Default::default()
            }],
            ..Default::default()
        },
        true,
    )
    .unwrap();
    // Lookups between pushes did not re-parse, and see every change.
    let view = repo.ref_view().unwrap();
    assert_eq!(repo.refs_parses(), 1, "ref_view never parses");
    assert_eq!(
        view.get("refs/heads/new-branch").as_deref(),
        Some(c2.as_str())
    );
    assert_eq!(view.get(&existing.name).as_deref(), Some(c2.as_str()));
    assert_eq!(view.get(&victim.name), None);
    assert_eq!(view.head_target(), "refs/heads/new-branch");
    assert_eq!(view.head_oid().as_deref(), Some(c2.as_str()));

    // The materialized snapshot equals a fresh parse by an independent handle.
    let patched = repo.refs_arc().unwrap();
    assert_eq!(
        repo.refs_parses(),
        1,
        "pushes never re-parse; one copy folds them"
    );
    let fresh_handle = LocalRepo::open(_root.path(), &RepoId::new("t", "refs500k").unwrap())
        .unwrap()
        .unwrap();
    let fresh = fresh_handle.refs_arc().unwrap();
    assert_eq!(patched.head_target, fresh.head_target);
    assert_eq!(patched.refs.len(), fresh.refs.len());
    assert_eq!(
        patched.refs, fresh.refs,
        "patched snapshot == fresh parse (incl. peels, order)"
    );
    let zz = patched
        .refs
        .iter()
        .find(|r| r.name == "refs/tags/zz-new")
        .unwrap();
    assert!(!zz.peeled.is_empty(), "new annotated tag was peeled");
}
