#![allow(clippy::unwrap_used)]
#[path = "common/mod.rs"]
mod common;

use walgit_git::{IngestOptions, LocalRepo, ObjectFormat, RepoId};

#[tokio::test]
async fn verifies_real_multi_pack_bitmap_and_rejects_missing_or_corrupt_bytes() {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let source = common::SourceRepo::with_object_format(match format {
            ObjectFormat::Sha1 => "sha1",
            ObjectFormat::Sha256 => "sha256",
        });
        let first = source.head();
        let root = tempfile::tempdir().unwrap();
        let repo =
            LocalRepo::init(root.path(), &RepoId::new("test", "bitmap").unwrap(), format).unwrap();
        let mut packs = Vec::new();
        for step in 0..2 {
            if step == 1 {
                source.commit_file("nested/next", "next", "next");
            }
            let excludes = if step == 0 {
                vec![]
            } else {
                vec![first.as_str()]
            };
            let pack = repo
                .ingest_pack(
                    common::cursor(source.pack(&["HEAD"], &excludes, false)),
                    IngestOptions {
                        fsck: true,
                        thin: false,
                        max_bytes: None,
                    },
                )
                .await
                .unwrap()
                .unwrap();
            packs.push(pack.checksum);
        }
        let commit = source.head();
        common::run_git(repo.path(), &["update-ref", "refs/heads/main", &commit]);
        common::run_git(repo.path(), &["symbolic-ref", "HEAD", "refs/heads/main"]);
        let verified = repo
            .write_verified_midx(&packs, commit.parse().unwrap(), ())
            .await
            .unwrap();
        assert!(verified.bitmap_path.is_file());
        // `multi` lets pack-objects reuse deltas across packs; a clone then
        // gets a thin pack whose bases are absent. Keep reuse to one pack.
        assert_eq!(
            common::run_git(repo.path(), &["config", "pack.allowPackReuse"]).trim(),
            "single"
        );
        common::run_git(repo.path(), &["fsck", "--strict"]);
        let bytes = std::fs::read(&verified.bitmap_path).unwrap();
        std::fs::remove_file(&verified.bitmap_path).unwrap();
        assert!(walgit_git::midx::verify_files(&repo).is_err());
        let mut corrupt = bytes.clone();
        *corrupt.last_mut().unwrap() ^= 1;
        std::fs::write(&verified.bitmap_path, corrupt).unwrap();
        assert!(walgit_git::midx::verify_files(&repo).is_err());
        std::fs::write(&verified.bitmap_path, bytes).unwrap();
        assert_eq!(
            walgit_git::midx::verify_files(&repo).unwrap().checksum,
            verified.checksum
        );
        repo.set_segmented_midx_mode(true);
        repo.write_history_midx().await.unwrap();
        assert!(
            walgit_git::midx::verify_files(&repo).is_ok(),
            "old history helper must not erase the full bitmap"
        );
        repo.remove_pack(packs.first().unwrap()).unwrap();
        assert!(!repo.path().join("objects/pack/multi-pack-index").exists());
        assert!(!verified.bitmap_path.exists());
    }
}
