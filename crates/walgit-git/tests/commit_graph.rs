// Test fixtures use panics to fail the test, including shared helper functions.
#![allow(clippy::unwrap_used, clippy::many_single_char_names)]

mod common;

use std::process::Command;
use walgit_git::{IngestOptions, LocalRepo, ObjectFormat, RepoId, gix_hash};

mod cm {
    pub use super::common::*;
}

fn git(dir: &std::path::Path, args: &[&str]) -> std::process::Output {
    Command::new("git")
        .current_dir(dir)
        .env("GIT_DIR", dir)
        .args(args)
        .output()
        .unwrap()
}

async fn ingest(repo: &LocalRepo, pack: Vec<u8>) -> gix_hash::ObjectId {
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
    .unwrap()
    .checksum
}

fn set_main(repo: &LocalRepo, old: &str, oid: &str) {
    repo.apply_ref_txn(
        &walgit_proto::v1::RefTransaction {
            updates: vec![walgit_proto::v1::RefUpdate {
                name: "refs/heads/main".into(),
                old_oid: old.to_string(),
                new_oid: oid.to_string(),
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

/// Base layer side-file written next to the pack, installed as the chain base
/// on a sibling, extended incrementally by a second pack, verified by git and
/// used by rev-list without the base pack's data.
#[tokio::test]
async fn base_layer_roundtrip_and_incremental_update() {
    let root = tempfile::TempDir::new().unwrap();
    let id = RepoId::new("acme", "cg").unwrap();
    let writer = LocalRepo::init(root.path(), &id, ObjectFormat::Sha1).unwrap();
    let src = cm::SourceRepo::new();
    for i in 0..8 {
        src.commit_file(&format!("f{i}.txt"), "x\n", "fill");
    }
    let a = src.commit_file("a.txt", "a\n", "a");
    let base_pack = src.pack(&["HEAD"], &[], false);
    let base = ingest(&writer, base_pack).await;
    set_main(&writer, "", &a);

    let size = writer.write_pack_commit_graph(&base, true).await.unwrap();
    assert!(size > 0);
    let side = writer.pack_path(&base).with_extension("commit-graph");
    assert!(side.exists());
    let info = writer.packs().unwrap();
    assert!(
        info.iter()
            .any(|p| p.checksum == base && p.has_commit_graph)
    );
    assert_eq!(writer.commit_graph_chain().unwrap().len(), 1);

    // A sibling instance receives pack + side-file and installs the base.
    let root2 = tempfile::TempDir::new().unwrap();
    let reader = LocalRepo::init(root2.path(), &id, ObjectFormat::Sha1).unwrap();
    let tmp = tempfile::TempDir::new().unwrap();
    let p = tmp.path().join(format!("pack-{base}.pack"));
    let i = tmp.path().join(format!("pack-{base}.idx"));
    let g = tmp.path().join(format!("pack-{base}.commit-graph"));
    std::fs::copy(writer.pack_path(&base), &p).unwrap();
    std::fs::copy(writer.pack_path(&base).with_extension("idx"), &i).unwrap();
    std::fs::copy(&side, &g).unwrap();
    reader.install_pack(&p, &i, &[g]).await.unwrap();
    assert!(reader.install_commit_graph_base(&base).unwrap());
    assert!(
        reader.install_commit_graph_base(&base).unwrap(),
        "idempotent"
    );
    set_main(&reader, "", &a);
    let chain = reader.commit_graph_chain().unwrap();
    assert_eq!(chain, writer.commit_graph_chain().unwrap());
    let out = git(reader.path(), &["commit-graph", "verify"]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );

    // New commits arrive in a second pack; the chain grows by one layer.
    let c = src.commit_file("c.txt", "c\n", "c");
    let inc = ingest(&reader, src.pack(&["HEAD"], &[&a], false)).await;
    reader.update_commit_graph(&[inc], true).await.unwrap();
    let chain2 = reader.commit_graph_chain().unwrap();
    assert_eq!(chain2.len(), 2, "{chain2:?}");
    assert_eq!(chain2[0], chain[0], "base layer untouched");
    let out = git(reader.path(), &["commit-graph", "verify"]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );

    // History walks from the graph: hide the base pack's data and count.
    set_main(&reader, &a, &c);
    let base_pack_path = reader.pack_path(&base);
    let hidden = base_pack_path.with_extension("pack.hidden");
    std::fs::rename(&base_pack_path, &hidden).unwrap();
    let out = git(
        reader.path(),
        &["-c", "core.commitGraph=true", "rev-list", "--count", &c],
    );
    std::fs::rename(&hidden, &base_pack_path).unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(String::from_utf8_lossy(&out.stdout).trim(), "11");
}
