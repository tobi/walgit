// Test fixtures use panics to fail the test, including shared helper functions.
#![allow(clippy::unwrap_used)]

mod common;

use walgit_git::{LocalRepo, LsRefsArgs, ObjectFormat, RepoId, gix_hash};
use walgit_proto::v1::{Ref, RefSnapshot};

mod cm {
    pub use super::common::*;
}

async fn setup() -> (tempfile::TempDir, LocalRepo, String, String, String) {
    let root = tempfile::TempDir::new().unwrap();
    let id = RepoId::new("acme", "ls").unwrap();
    let repo = LocalRepo::init(root.path(), &id, ObjectFormat::Sha1).unwrap();
    let src = cm::SourceRepo::new();
    let a = src.head();
    // annotated tag v1 -> commit A
    let tag_oid = src.annotated_tag("v1", &a);
    let b = src.commit_file("file2.txt", "world\n", "second");
    // pack everything (both commits + tag)
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
    // load refs
    let snap = RefSnapshot {
        seq: 0,
        object_format: "sha1".into(),
        refs: vec![
            Ref {
                name: "refs/heads/main".into(),
                oid: b.clone(),
                peeled: String::new(),
            },
            Ref {
                name: "refs/heads/dev".into(),
                oid: a.clone(),
                peeled: String::new(),
            },
            Ref {
                name: "refs/tags/v1".into(),
                oid: tag_oid.clone(),
                peeled: a.clone(),
            },
        ],
        head_target: "refs/heads/main".to_string(),
        created_at: None,
    };
    repo.load_ref_snapshot(&snap).unwrap();
    (root, repo, a, b, tag_oid)
}

#[tokio::test]
async fn ls_refs_prefix_heads() {
    let (_r, repo, _a, _b, _t) = setup().await;
    let lines = repo
        .ls_refs(&LsRefsArgs {
            ref_prefixes: vec!["refs/heads/".into()],
            symrefs: false,
            peel: false,
            unborn: false,
        })
        .unwrap();
    let names: Vec<_> = lines.iter().map(|l| l.name.clone()).collect();
    assert_eq!(names, vec!["refs/heads/dev", "refs/heads/main"]);
}

#[tokio::test]
async fn ls_refs_multiple_prefixes_are_or_and_empty_matches_all() {
    let (_r, repo, _a, _b, _t) = setup().await;

    let lines = repo
        .ls_refs(&LsRefsArgs {
            ref_prefixes: vec!["refs/heads/dev".into(), "refs/tags/".into()],
            symrefs: false,
            peel: false,
            unborn: false,
        })
        .unwrap();
    let names: Vec<_> = lines.iter().map(|l| l.name.as_str()).collect();
    assert_eq!(names, vec!["refs/heads/dev", "refs/tags/v1"]);

    let all = repo
        .ls_refs(&LsRefsArgs {
            ref_prefixes: Vec::new(),
            symrefs: false,
            peel: false,
            unborn: false,
        })
        .unwrap();
    // HEAD is advertised too when no prefix is given (as `git ls-remote` shows).
    assert_eq!(all.len(), 4);
    assert!(all.iter().any(|l| l.name == "HEAD"));
}

#[tokio::test]
async fn ls_refs_peel_tag() {
    let (_r, repo, a, _b, _t) = setup().await;
    let lines = repo
        .ls_refs(&LsRefsArgs {
            ref_prefixes: vec!["refs/tags/".into()],
            symrefs: false,
            peel: true,
            unborn: false,
        })
        .unwrap();
    assert_eq!(lines.len(), 1);
    assert_eq!(lines[0].name, "refs/tags/v1");
    assert_eq!(lines[0].peeled, a);
}

#[tokio::test]
async fn ls_refs_symrefs_head() {
    let (_r, repo, _a, b, _t) = setup().await;
    let lines = repo
        .ls_refs(&LsRefsArgs {
            ref_prefixes: vec![], // all
            symrefs: true,
            peel: false,

            unborn: false,
        })
        .unwrap();
    let head = lines.iter().find(|l| l.name == "HEAD").unwrap();
    assert_eq!(head.symref_target.as_deref(), Some("refs/heads/main"));
    assert_eq!(head.oid, b);
}

#[tokio::test]
async fn ls_refs_unborn() {
    let root = tempfile::TempDir::new().unwrap();
    let id = RepoId::new("acme", "unborn").unwrap();
    let repo = LocalRepo::init(root.path(), &id, ObjectFormat::Sha1).unwrap();
    // No refs; HEAD targets an unborn branch.
    let lines = repo
        .ls_refs(&LsRefsArgs {
            ref_prefixes: vec![],
            symrefs: true,
            peel: false,
            unborn: true,
        })
        .unwrap();
    let head = lines.iter().find(|l| l.name == "HEAD").unwrap();
    assert_eq!(head.oid, "unborn");
    assert_eq!(head.symref_target.as_deref(), Some("refs/heads/main"));
    assert_eq!(
        head.render(&LsRefsArgs {
            ref_prefixes: vec![],
            symrefs: false,
            peel: false,
            unborn: true
        }),
        "unborn HEAD symref-target:refs/heads/main\n"
    );
    // Without `unborn`, a dangling HEAD is simply omitted (never an empty oid).
    let lines = repo
        .ls_refs(&LsRefsArgs {
            ref_prefixes: vec![],
            symrefs: true,
            peel: false,
            unborn: false,
        })
        .unwrap();
    assert!(lines.iter().all(|l| l.name != "HEAD" && !l.oid.is_empty()));
}
#[test]
#[ignore = "large synthetic benchmark"]
fn bench_ls_refs_466k() {
    use std::time::Instant;

    let root = tempfile::TempDir::new().unwrap();
    let id = RepoId::new("acme", "scale").unwrap();
    let repo = LocalRepo::init(root.path(), &id, ObjectFormat::Sha1).unwrap();
    let oid = "1111111111111111111111111111111111111111";
    let mut refs = Vec::with_capacity(466_395);
    for i in 0..466_395u32 {
        refs.push(Ref {
            name: format!("refs/heads/ref-{i:06}"),
            oid: oid.into(),
            peeled: String::new(),
        });
    }
    let snap = RefSnapshot {
        seq: 0,
        object_format: "sha1".into(),
        refs,
        head_target: "refs/heads/ref-000000".into(),
        created_at: None,
    };
    repo.load_ref_snapshot(&snap).unwrap();

    let start = Instant::now();
    let lines = repo
        .ls_refs(&LsRefsArgs::default())
        .expect("render synthetic refs");
    let ls_elapsed = start.elapsed();
    let mut advert = Vec::new();
    let start = Instant::now();
    repo.advertise_refs_v0(walgit_git::Service::UploadPack, &mut advert)
        .unwrap();
    let advert_elapsed = start.elapsed();
    let rss_kib = std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("VmRSS:"))
                .and_then(|l| l.split_whitespace().nth(1))
                .map(str::to_owned)
        })
        .unwrap_or_else(|| "unknown".to_string());
    println!(
        "466395 refs: ls_refs={} lines in {:?}; v0={} bytes in {:?}; VmRSS={} KiB",
        lines.len(),
        ls_elapsed,
        advert.len(),
        advert_elapsed,
        rss_kib
    );
    assert_eq!(lines.len(), 466_395 + 1, "every ref + HEAD");
    // Second call: the parse is cached; a prefix is a range.
    let start = Instant::now();
    let lines2 = repo.ls_refs(&LsRefsArgs::default()).unwrap();
    let cached_elapsed = start.elapsed();
    let start = Instant::now();
    let prefix = repo
        .ls_refs(&LsRefsArgs {
            ref_prefixes: vec!["refs/heads/ref-1234".into()],
            ..Default::default()
        })
        .unwrap();
    let prefix_elapsed = start.elapsed();
    println!(
        "cached full ls_refs={} lines in {:?}; prefix (ref-1234*) {} lines in {:?}",
        lines2.len(),
        cached_elapsed,
        prefix.len(),
        prefix_elapsed
    );
    assert_eq!(prefix.len(), 100);
    assert!(
        prefix_elapsed.as_millis() < 20,
        "prefix must be a range, not a scan: {prefix_elapsed:?}"
    );
}

fn _unused(_: gix_hash::ObjectId) {}

/// Many refs: prefixes select by binary search over the sorted list (one range
/// per prefix, overlapping prefixes merged), the parsed refs are cached across
/// calls, and every ref writer invalidates the cache. 2026-08-21: a 500 k-ref
/// repo paid a 34 MB packed-refs parse (+ tag peeling) on every ls-refs.
#[tokio::test]
async fn ls_refs_prefixes_over_many_refs_are_ranges_and_the_cache_tracks_writes() {
    let (_root, repo, a, b, _tag) = setup().await;
    // 20 k refs pointing at the two commits, plus the originals.
    let mut refs: Vec<Ref> = (0..20_000)
        .map(|i| Ref {
            name: format!("refs/heads/team{}/f-{i:05}", i % 10),
            oid: if i % 2 == 0 { a.clone() } else { b.clone() },
            peeled: String::new(),
        })
        .collect();
    refs.push(Ref {
        name: "refs/heads/main".into(),
        oid: b.clone(),
        peeled: String::new(),
    });
    repo.load_ref_snapshot(&RefSnapshot {
        seq: 1,
        object_format: String::new(),
        refs,
        head_target: "refs/heads/main".into(),
        created_at: None,
    })
    .unwrap();

    let all = repo
        .ls_refs(&LsRefsArgs {
            ref_prefixes: vec![],
            symrefs: false,
            peel: false,
            unborn: false,
        })
        .unwrap();
    assert_eq!(all.len(), 20_001 + 1, "20 k branches + main + HEAD");
    let team3 = repo
        .ls_refs(&LsRefsArgs {
            ref_prefixes: vec!["refs/heads/team3/".into()],
            symrefs: false,
            peel: false,
            unborn: false,
        })
        .unwrap();
    assert_eq!(team3.len(), 2_000);
    assert!(
        team3
            .iter()
            .all(|l| l.name.starts_with("refs/heads/team3/"))
    );
    assert!(team3.windows(2).all(|w| w[0].name < w[1].name), "sorted");
    // Overlapping prefixes emit each ref once; HEAD comes along when a prefix matches it.
    let overlap = repo
        .ls_refs(&LsRefsArgs {
            ref_prefixes: vec![
                "refs/heads/team3/".into(),
                "refs/heads/team3/f-003".into(),
                "HEAD".into(),
            ],
            symrefs: true,
            peel: false,
            unborn: false,
        })
        .unwrap();
    assert_eq!(overlap.len(), 2_000 + 1, "{}", overlap.len());
    assert_eq!(overlap.iter().filter(|l| l.name == "HEAD").count(), 1);
    // A prefix matching nothing.
    assert!(
        repo.ls_refs(&LsRefsArgs {
            ref_prefixes: vec!["refs/heads/zzz".into()],
            symrefs: false,
            peel: false,
            unborn: false
        })
        .unwrap()
        .is_empty()
    );

    // Cache: same Arc until a write.
    let s1 = repo.refs_arc().unwrap();
    let s2 = repo.refs_arc().unwrap();
    assert!(std::sync::Arc::ptr_eq(&s1, &s2), "parsed once");
    let txn = walgit_proto::v1::RefTransaction {
        updates: vec![walgit_proto::v1::RefUpdate {
            name: "refs/heads/new".into(),
            old_oid: String::new(),
            new_oid: a.clone(),
            ..Default::default()
        }],
        ..Default::default()
    };
    repo.apply_ref_txn(&txn, false).unwrap();
    let s3 = repo.refs_arc().unwrap();
    assert!(!std::sync::Arc::ptr_eq(&s1, &s3), "a ref write invalidates");
    assert!(s3.refs.iter().any(|r| r.name == "refs/heads/new"));
    // An external writer (git itself) changing packed-refs is seen too.
    std::thread::sleep(std::time::Duration::from_millis(20));
    std::process::Command::new("git")
        .arg("-C")
        .arg(repo.path())
        .args(["pack-refs", "--all"])
        .status()
        .unwrap();
    let s4 = repo.refs_arc().unwrap();
    assert!(
        !std::sync::Arc::ptr_eq(&s3, &s4),
        "packed-refs changed on disk"
    );
    assert_eq!(s4.refs.len(), s3.refs.len());
}
