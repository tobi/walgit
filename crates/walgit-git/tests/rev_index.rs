// Test fixtures use panics to fail the test, including shared helper functions.
#![allow(clippy::expect_used)]

//! `.rev` derived from the `.idx` alone must be byte-identical to git's
//! (`index-pack --rev-index`), so a pack can get its reverse index in seconds
//! (a large repository's 32 GB base: `index-pack --rev-index` re-reads the whole pack —
//! 4 GB in 52 min, 2026-08-21) and git accepts the file.

fn git(dir: &std::path::Path, args: &[&str]) -> String {
    let out = std::process::Command::new("git")
        .current_dir(dir)
        .args(args)
        .output()
        .expect("git");
    assert!(
        out.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

#[test]
fn rev_from_idx_is_byte_identical_to_gits_and_accepted_by_it() {
    let tmp = tempfile::tempdir().unwrap();
    let work = tmp.path().join("w");
    std::fs::create_dir_all(&work).unwrap();
    git(&work, &["init", "-q", "-b", "main"]);
    git(&work, &["config", "user.email", "t@t"]);
    git(&work, &["config", "user.name", "T"]);
    // Enough objects (and deltas) that offset order ≠ idx (sha) order.
    for i in 0..60 {
        std::fs::write(
            work.join(format!("f{}.txt", i % 7)),
            format!("{i}\n{}", "x".repeat(i * 40)),
        )
        .unwrap();
        git(&work, &["add", "."]);
        git(&work, &["commit", "-q", "-m", &format!("c{i}")]);
    }
    let packs = tmp.path().join("p");
    std::fs::create_dir_all(&packs).unwrap();
    // git's own .rev alongside the pack.
    let sha = {
        let out = std::process::Command::new("sh")
            .arg("-c")
            .arg(format!("git rev-list --objects --all | git -c pack.writeReverseIndex=true pack-objects {}/pack", packs.display()))
            .current_dir(&work)
            .output()
            .unwrap();
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    };
    let idx = packs.join(format!("pack-{sha}.idx"));
    let theirs = packs.join(format!("pack-{sha}.rev"));
    assert!(theirs.exists(), "git wrote a .rev");
    let ours = packs.join(format!("pack-{sha}.ours.rev"));
    walgit_git::write_rev_from_idx(&idx, &ours, gix_hash::Kind::Sha1).unwrap();
    assert_eq!(
        std::fs::read(&ours).unwrap(),
        std::fs::read(&theirs).unwrap(),
        "byte-identical to git's"
    );

    // And git uses ours: replace theirs, then a bitmap-free offset lookup
    // (`verify-pack` and `cat-file --batch-check` on a bare repo with the pack).
    std::fs::remove_file(&theirs).unwrap();
    std::fs::rename(&ours, &theirs).unwrap();
    let bare = tmp.path().join("b.git");
    git(tmp.path(), &["init", "-q", "--bare", "b.git"]);
    for ext in ["pack", "idx", "rev"] {
        std::fs::copy(
            packs.join(format!("pack-{sha}.{ext}")),
            bare.join("objects/pack").join(format!("pack-{sha}.{ext}")),
        )
        .unwrap();
    }
    git(
        &bare,
        &["verify-pack", "-v", &format!("objects/pack/pack-{sha}.idx")],
    );
    let head = git(&work, &["rev-parse", "HEAD"]);
    let out = std::process::Command::new("git")
        .current_dir(&bare)
        .args([
            "cat-file",
            "--batch-check=%(objectname) %(objecttype) %(objectsize:disk)",
        ])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .and_then(|mut c| {
            use std::io::Write;
            c.stdin
                .take()
                .unwrap()
                .write_all(format!("{head}\n").as_bytes())?;
            c.wait_with_output()
        })
        .unwrap();
    assert!(
        String::from_utf8_lossy(&out.stdout).starts_with(&format!("{head} commit ")),
        "{}",
        String::from_utf8_lossy(&out.stdout)
    );
    // A corrupt .rev would be reported by `git fsck`/`verify-pack`; also check
    // git's own reader loads it without the "ignoring" warning.
    let out = std::process::Command::new("git")
        .current_dir(&bare)
        .args([
            "-c",
            "core.multiPackIndex=false",
            "rev-list",
            "--objects",
            "--use-bitmap-index",
            "--all",
        ])
        .output()
        .unwrap();
    assert!(
        !String::from_utf8_lossy(&out.stderr).contains("reverse-index"),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
}
