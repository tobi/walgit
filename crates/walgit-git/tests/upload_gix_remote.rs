// Test fixtures use panics to fail the test, including shared helper functions.
#![allow(clippy::unwrap_used)]

//! The gix upload-pack engine over a repository whose base pack is *not*
//! local: history from the commit-graph chain, `have`s from the faulter's
//! index, object enumeration by tree diff against parents, base objects
//! faulted in per tree level. Mirrors a serverless instance serving acme/monorepo
//! with the remote reader and no store mount.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use futures::future::BoxFuture;
use walgit_git::{
    IngestOptions, LocalRepo, ObjectFaulter, ObjectFormat, RepoId, UploadPackRequest, gix_hash,
};

mod cm {
    pub use super::common::*;
}

/// Serves objects from a complete sibling copy, counting what was asked for.
struct SiblingFaulter {
    full: LocalRepo,
    into: LocalRepo,
    faulted: AtomicUsize,
    rounds: AtomicUsize,
}

impl ObjectFaulter for SiblingFaulter {
    fn contains(&self, oid: &gix_hash::oid) -> bool {
        self.full.has_object(oid)
    }
    fn fault<'a>(
        &'a self,
        oids: &'a [gix_hash::ObjectId],
    ) -> BoxFuture<'a, Result<usize, walgit_git::GitError>> {
        Box::pin(async move {
            self.rounds.fetch_add(1, Ordering::Relaxed);
            let repo = self.full.gix();
            let mut n = 0;
            for oid in oids {
                if let Ok(obj) = repo.find_object(*oid) {
                    self.into.write_loose_object(obj.kind, oid, &obj.data)?;
                    n += 1;
                }
            }
            self.faulted.fetch_add(n, Ordering::Relaxed);
            Ok(n)
        })
    }
}

async fn ingest(repo: &LocalRepo, pack: Vec<u8>) -> gix_hash::ObjectId {
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
    .unwrap()
    .checksum
}

fn set_main(repo: &LocalRepo, old: &str, new: &str) {
    repo.apply_ref_txn(
        &walgit_proto::v1::RefTransaction {
            updates: vec![walgit_proto::v1::RefUpdate {
                name: "refs/heads/main".into(),
                old_oid: old.into(),
                new_oid: new.into(),
                new_symbolic_target: String::new(),
                new_peeled: String::new(),
            }],
            push_options: vec![],
            atomic: true,
        },
        true,
    )
    .unwrap();
}

fn req(
    wants: Vec<gix_hash::ObjectId>,
    haves: Vec<gix_hash::ObjectId>,
    sideband_all: bool,
) -> UploadPackRequest {
    UploadPackRequest {
        wants,
        haves,
        done: true,
        thin_pack: false,
        no_progress: false,
        include_tag: false,
        ofs_delta: true,
        sideband_all,
        wait_for_done: false,
        filter: None,
        deepen: None,
        deepen_since: None,
        deepen_not: vec![],
        shallow: vec![],
        want_refs: vec![],
        packfile_uris_protocols: vec![],
    }
}

#[tokio::test]
async fn diff_sized_fetch_without_base_pack_data() {
    // Source: a "monorepo" with 40 dirs × 10 files in the base, then 3
    // commits touching one nested file each (+ one new dir).
    let src = cm::SourceRepo::new();
    for d in 0..40 {
        for f in 0..10 {
            std::fs::create_dir_all(src.dir.join(format!("dir{d}/sub"))).unwrap();
            std::fs::write(
                src.dir.join(format!("dir{d}/sub/f{f}.txt")),
                format!("{d}-{f}\n"),
            )
            .unwrap();
        }
    }
    cm::run_git(&src.dir, &["add", "."]);
    cm::run_git(&src.dir, &["commit", "-q", "-m", "base"]);
    let base_tip = src.head();
    let base_pack = src.pack(&["HEAD"], &[], false);
    let c1 = src.commit_file("dir7/sub/f3.txt", "changed\n", "c1");
    let c2 = src.commit_file("dir7/sub/f4.txt", "changed too\n", "c2");
    let c3 = src.commit_file("newdir/x.txt", "x\n", "c3");
    let inc_pack = src.pack(&["HEAD"], &[&base_tip], false);

    // `full`: the whole repo (stands in for the bucket / remote reader).
    let root_full = tempfile::TempDir::new().unwrap();
    let full = LocalRepo::init(
        root_full.path(),
        &RepoId::new("acme", "mono").unwrap(),
        ObjectFormat::Sha1,
    )
    .unwrap();
    let base_cs = ingest(&full, base_pack.clone()).await;
    ingest(&full, inc_pack.clone()).await;
    set_main(&full, "", &c3);

    // `served`: base pack + commit-graph layer, then the base pack's data
    // and index removed (only the graph stays); the increment is local.
    let root = tempfile::TempDir::new().unwrap();
    let served = LocalRepo::init(
        root.path(),
        &RepoId::new("acme", "mono").unwrap(),
        ObjectFormat::Sha1,
    )
    .unwrap();
    assert_eq!(ingest(&served, base_pack).await, base_cs);
    set_main(&served, "", &base_tip);
    served
        .write_pack_commit_graph(&base_cs, false)
        .await
        .unwrap();
    let inc_cs = ingest(&served, inc_pack).await;
    set_main(&served, &base_tip, &c3);
    served.update_commit_graph(&[inc_cs], false).await.unwrap();
    assert!(!served.commit_graph_chain().unwrap().is_empty());
    for ext in ["pack", "idx"] {
        std::fs::remove_file(served.pack_path(&base_cs).with_extension(ext)).unwrap();
    }
    served.refresh().unwrap();
    let base_tip_oid = gix_hash::ObjectId::from_hex(base_tip.as_bytes()).unwrap();
    assert!(
        !served.has_object(&base_tip_oid),
        "base data is gone locally"
    );

    let faulter = Arc::new(SiblingFaulter {
        full: full.clone(),
        into: served.clone(),
        faulted: AtomicUsize::new(0),
        rounds: AtomicUsize::new(0),
    });

    // Fetch: want c3, have base tip.
    let want = gix_hash::ObjectId::from_hex(c3.as_bytes()).unwrap();
    let mut out = Vec::new();
    let stats = served
        .upload_pack_gix_with(
            req(vec![want], vec![base_tip_oid], true),
            &mut out,
            Some(&*faulter),
        )
        .await
        .unwrap();
    let pack = cm::extract_packfile(&out);
    assert!(pack.starts_with(b"PACK"));
    // Exactly the new objects: 3 commits, trees along dir7/sub + root ×
    // changes + newdir, 3 blobs. Never the other 39 dirs.
    let (blobs, commits, trees, _tags) = cm::pack_object_types(&pack);
    assert_eq!(commits, 3);
    assert_eq!(blobs, 3);
    assert!(
        trees <= 3 * 3 + 1,
        "tree diff must not walk untouched subtrees: {trees} trees"
    );
    assert_eq!(stats.objects as u64, blobs + commits + trees);

    // The client (who has the base) can index it and sees c3's tree complete.
    let client = cm::fresh_bare();
    let base_again = src.pack(&[&base_tip], &[], false);
    let bp = client.path().join("objects/pack/pack-base.pack");
    std::fs::create_dir_all(bp.parent().unwrap()).unwrap();
    std::fs::write(&bp, &base_again).unwrap();
    cm::run_git(client.path(), &["index-pack", bp.to_str().unwrap()]);
    let ip = client.path().join("objects/pack/pack-inc.pack");
    std::fs::write(&ip, &pack).unwrap();
    cm::run_git(client.path(), &["index-pack", ip.to_str().unwrap()]);
    cm::run_git(client.path(), &["update-ref", "refs/heads/main", &c3]);
    let fsck = std::process::Command::new("git")
        .current_dir(client.path())
        .args(["fsck", "--connectivity-only"])
        .output()
        .unwrap();
    assert!(
        fsck.status.success(),
        "{}",
        String::from_utf8_lossy(&fsck.stderr)
    );
    let ls = std::process::Command::new("git")
        .current_dir(client.path())
        .args(["ls-tree", "-r", "--name-only", &c3])
        .output()
        .unwrap();
    assert!(String::from_utf8_lossy(&ls.stdout).contains("newdir/x.txt"));

    // Base objects read: the base tip commit + root tree + dir7 + dir7/sub
    // (one level per round), never the other dirs.
    let faulted = faulter.faulted.load(Ordering::Relaxed);
    let rounds = faulter.rounds.load(Ordering::Relaxed);
    eprintln!(
        "faulted {faulted} base objects in {rounds} rounds; pack: {commits} commits {trees} trees {blobs} blobs"
    );
    assert!(faulted <= 8, "faulted {faulted} base objects");
    assert!(rounds <= 5, "{rounds} fault rounds");
    assert!(
        faulted >= 3,
        "diffing must have read the parent trees ({faulted})"
    );

    // sideband-all: every section line is a band-1 frame; progress on band 2.
    let lines = cm::parse_pkt_lines(&out);
    let mut saw_packfile_framed = false;
    let mut saw_progress = false;
    for l in &lines {
        if let cm::PktLine::Data(b) = l {
            if b.first() == Some(&1) && b[1..].starts_with(b"packfile\n") {
                saw_packfile_framed = true;
            }
            if b.first() == Some(&2) {
                saw_progress = true;
            }
            assert!(
                !b.starts_with(b"packfile\n"),
                "unframed section line with sideband-all"
            );
        }
    }
    assert!(saw_packfile_framed && saw_progress);

    // CI's clone: zero haves, depth 1, blob:none — the whole tree of c3 is
    // read from the base one level per round, no blobs, one commit.
    let f2 = Arc::new(SiblingFaulter {
        full: full.clone(),
        into: served.clone(),
        faulted: AtomicUsize::new(0),
        rounds: AtomicUsize::new(0),
    });
    let mut r = req(vec![want], vec![], false);
    r.deepen = Some(1);
    r.filter = Some("blob:none".into());
    let mut out3 = Vec::new();
    let stats3 = served
        .upload_pack_gix_with(r, &mut out3, Some(&*f2))
        .await
        .unwrap();
    let pack3 = cm::extract_packfile(&out3);
    let (b3, c3n, t3, _) = cm::pack_object_types(&pack3);
    assert_eq!(
        (b3, c3n),
        (0, 1),
        "blob:none depth 1: {b3} blobs {c3n} commits"
    );
    // root + 41 dirs (dir0..39 + newdir) + 40 sub/ trees
    assert_eq!(t3, 1 + 41 + 40, "trees: {t3}");
    assert_eq!(stats3.objects, 1 + t3);
    let (f3, r3) = (
        f2.faulted.load(Ordering::Relaxed),
        f2.rounds.load(Ordering::Relaxed),
    );
    eprintln!("depth-1 blob:none: faulted {f3} in {r3} rounds");
    assert!(r3 <= 5, "one round per tree level, got {r3}");
    assert!(f3 >= 40, "most of the tree lives in the base ({f3})");
    assert!(
        cm::parse_pkt_lines(&out3)
            .iter()
            .any(|l| matches!(l, cm::PktLine::Data(b) if b.starts_with(b"shallow-info\n")))
    );

    // Lazy-checkout blob want: the blob lives only in the base (not local);
    // it is faulted first and packed (prod: "fatal: expected 'packfile'").
    let blob = {
        let out = std::process::Command::new("git")
            .current_dir(&src.dir)
            .args(["rev-parse", &format!("{base_tip}:dir3/sub/f1.txt")])
            .output()
            .unwrap();
        gix_hash::ObjectId::from_hex(String::from_utf8_lossy(&out.stdout).trim().as_bytes())
            .unwrap()
    };
    assert!(!served.has_object(&blob));
    let f3 = Arc::new(SiblingFaulter {
        full: full.clone(),
        into: served.clone(),
        faulted: AtomicUsize::new(0),
        rounds: AtomicUsize::new(0),
    });
    let mut out4 = Vec::new();
    let stats4 = served
        .upload_pack_gix_with(req(vec![blob], vec![], true), &mut out4, Some(&*f3))
        .await
        .unwrap();
    assert_eq!(stats4.objects, 1);
    let pack4 = cm::extract_packfile(&out4);
    assert_eq!(cm::pack_object_types(&pack4).0, 1, "one blob");

    // Same fetch without sideband-all is framed classically.
    let mut out2 = Vec::new();
    served
        .upload_pack_gix_with(
            req(vec![want], vec![base_tip_oid], false),
            &mut out2,
            Some(&*faulter),
        )
        .await
        .unwrap();
    assert!(
        cm::parse_pkt_lines(&out2)
            .iter()
            .any(|l| matches!(l, cm::PktLine::Data(b) if b.as_slice() == b"packfile\n"))
    );
    let _ = (c1, c2);
}

/// No faulter and no base data: a clear error, not a hang or a corrupt pack.
#[tokio::test]
async fn missing_base_without_faulter_is_an_error() {
    let src = cm::SourceRepo::new();
    let base_tip = src.head();
    let base_pack = src.pack(&["HEAD"], &[], false);
    let c1 = src.commit_file("a.txt", "a\n", "c1");
    let inc_pack = src.pack(&["HEAD"], &[&base_tip], false);
    let root = tempfile::TempDir::new().unwrap();
    let served = LocalRepo::init(
        root.path(),
        &RepoId::new("acme", "m2").unwrap(),
        ObjectFormat::Sha1,
    )
    .unwrap();
    let base_cs = ingest(&served, base_pack).await;
    set_main(&served, "", &base_tip);
    served
        .write_pack_commit_graph(&base_cs, false)
        .await
        .unwrap();
    let inc_cs = ingest(&served, inc_pack).await;
    set_main(&served, &base_tip, &c1);
    served.update_commit_graph(&[inc_cs], false).await.unwrap();
    for ext in ["pack", "idx"] {
        std::fs::remove_file(served.pack_path(&base_cs).with_extension(ext)).unwrap();
    }
    served.refresh().unwrap();
    let want = gix_hash::ObjectId::from_hex(c1.as_bytes()).unwrap();
    let have = gix_hash::ObjectId::from_hex(base_tip.as_bytes()).unwrap();
    let mut out = Vec::new();
    // The have is unknown locally → treated as not common → a full clone is
    // attempted → the base objects are missing → error.
    let r = served
        .upload_pack_gix_with(req(vec![want], vec![have], false), &mut out, None)
        .await;
    assert!(r.is_err(), "must fail without the base: {r:?}");
}
