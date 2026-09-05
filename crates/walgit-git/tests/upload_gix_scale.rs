// Test fixtures use panics to fail the test, including shared helper functions.
#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::unwrap_used,
    clippy::cast_possible_truncation,
    clippy::format_push_string
)]

//! Reproducer for AGENTS §6 "gix large-fetch object-id corruption and 178 GB OOM" (2026-08-21
//! 05:4xZ: a remainder pack carried an entry under another object's id; 07:0xZ: the same shape
//! replayed over a large repository was OOM-killed at 178 GB anon RSS after `Enumerating objects: 113683`).
//!
//! Synthetic repository with deep delta chains across TWO packs (a base pack and an incremental
//! pack, both `pack-objects --delta-base-offset`), served entirely locally through the gix engine
//! (frozen pack source, `Mode::PackCopyAndBaseObjects`), three fetch shapes:
//!   A. remainder: want tip, have base tip, `thin_pack = true` (prod's failing shape),
//!   B. remainder, `thin_pack = false` (every delta whose base is outside the set re-encoded),
//!   C. bounded zero-have: `--depth=1 --filter=blob:none` (CI's shape),
//!   D. full zero-have (`TreeContents` expansion).
//! Every output is indexed by stock git with `--strict` (ids recomputed from content), its object
//! set compared to `git rev-list --objects` of the source, and the process's max RSS delta is
//! bounded by a small multiple of the pack bytes.
//!
//! `cargo test -p walgit-git --test upload_gix_scale` runs the ~30 k-object variant (< 60 s);
//! `-- --ignored` runs the ~300 k-object one (`just test-slow`).

mod common;

use std::collections::BTreeSet;
use std::io::Write;
use std::process::{Command, Stdio};

use walgit_git::{IngestOptions, LocalRepo, ObjectFormat, RepoId, UploadPackRequest, gix_hash};

mod cm {
    pub use super::common::*;
}

fn max_rss_kb() -> u64 {
    // SAFETY: rusage contains C numeric fields whose all-zero values are valid.
    #[allow(unsafe_code)]
    let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
    // SAFETY: ru is aligned writable storage; RUSAGE_SELF is a supported selector.
    #[allow(unsafe_code)]
    let result = unsafe { libc::getrusage(libc::RUSAGE_SELF, &raw mut ru) };
    assert_eq!(
        result,
        0,
        "getrusage failed: {}",
        std::io::Error::last_os_error()
    );
    // getrusage reports ru_maxrss in KB on Linux but in BYTES on macOS/BSD.
    // Without this, the memory-bound assertion reads 1024x high on macOS and
    // fails a passing result (a 16 MB delta shown as "16832 MB").
    #[cfg(any(target_os = "macos", target_os = "ios"))]
    let kb = (u64::try_from(ru.ru_maxrss).expect("nonnegative peak RSS")) / 1024;
    #[cfg(not(any(target_os = "macos", target_os = "ios")))]
    let kb = u64::try_from(ru.ru_maxrss).expect("nonnegative peak RSS");
    kb
}

/// Deterministic source via `git fast-import`: `commits` commits, each rewriting `files_per_commit`
/// of `files` files with a line appended (long delta chains), in `dirs` directories (tree churn).
/// Returns the bare dir; `phase` splits the history into a base and an increment by commit number.
fn synth(commits: usize, files: usize, files_per_commit: usize, dirs: usize) -> tempfile::TempDir {
    let tmp = tempfile::TempDir::new().unwrap();
    let dir = tmp.path();
    cm::run_git(dir, &["init", "-q", "--bare", dir.to_str().unwrap()]);
    let mut child = Command::new("git")
        .current_dir(dir)
        .args(["fast-import", "--quiet", "--date-format=raw"])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    {
        let stdin = child.stdin.as_mut().unwrap();
        let mut w = std::io::BufWriter::with_capacity(1 << 20, stdin);
        let mut seed = 0x9E37_79B9_7F4A_7C15_u64;
        let mut next = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        // 1–32 KB of pseudo-random words per file to start (blobs of realistic size → deltas of
        // realistic size; the 2026-08-21 failure was over blobs, not trees).
        let mut contents: Vec<String> = (0..files)
            .map(|f| {
                let mut c = format!("file {f}\n");
                let words = 200 + (next() % 6000) as usize;
                for _ in 0..words {
                    {
                        let _ = std::fmt::Write::write_fmt(
                            &mut c,
                            format_args!("{:06x} ", next() & 0x00ff_ffff),
                        );
                    };
                }
                c.push('\n');
                c
            })
            .collect();
        for c in 1..=commits {
            writeln!(w, "commit refs/heads/main").unwrap();
            writeln!(w, "mark :{c}").unwrap();
            writeln!(
                w,
                "committer T <t@t> {} +0000",
                1_700_000_000 + c as u64 * 60
            )
            .unwrap();
            let msg = format!("commit {c}");
            writeln!(w, "data {}\n{msg}", msg.len()).unwrap();
            if c > 1 {
                writeln!(w, "from :{}", c - 1).unwrap();
            }
            for _ in 0..files_per_commit {
                let f = (next() as usize) % files;
                // Mostly appends (small deltas), sometimes a rewrite (a new base in the chain).
                if next() % 17 == 0 {
                    contents[f] = format!("file {f} rewritten at {c} {:016x}\n", next());
                } else {
                    contents[f].push_str(&format!("line {c} {:016x}\n", next()));
                }
                let path = format!("d{}/s{}/f{f}.txt", f % dirs, (f / dirs) % 7);
                writeln!(w, "M 100644 inline {path}").unwrap();
                writeln!(w, "data {}", contents[f].len()).unwrap();
                w.write_all(contents[f].as_bytes()).unwrap();
                writeln!(w).unwrap();
            }
            writeln!(w).unwrap();
            if c % 500 == 0 {
                writeln!(w, "checkpoint").unwrap();
            }
        }
        w.flush().unwrap();
    }
    let out = child.wait_with_output().unwrap();
    assert!(
        out.status.success(),
        "fast-import: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    tmp
}

fn rev_list_objects(
    dir: &std::path::Path,
    include: &str,
    exclude: Option<&str>,
    filter: Option<&str>,
) -> BTreeSet<String> {
    let mut args = vec!["rev-list", "--objects"];
    if let Some(f) = filter {
        args.push(f);
    }
    args.push(include);
    let ex;
    if let Some(e) = exclude {
        ex = format!("^{e}");
        args.push(&ex);
    }
    cm::run_git(dir, &args)
        .lines()
        .filter_map(|l| l.split_whitespace().next().map(str::to_owned))
        .collect()
}

fn pack_stdout(dir: &std::path::Path, includes: &[&str], excludes: &[&str]) -> Vec<u8> {
    let mut input = String::new();
    for i in includes {
        input.push_str(i);
        input.push('\n');
    }
    for e in excludes {
        input.push('^');
        input.push_str(e);
        input.push('\n');
    }
    let mut child = Command::new("git")
        .current_dir(dir)
        .args([
            "pack-objects",
            "--stdout",
            "--revs",
            "--delta-base-offset",
            "--depth=50",
            "--window=50",
            "-q",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .as_mut()
        .unwrap()
        .write_all(input.as_bytes())
        .unwrap();
    let out = child.wait_with_output().unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    out.stdout
}

fn req(
    wants: Vec<gix_hash::ObjectId>,
    haves: Vec<gix_hash::ObjectId>,
    thin: bool,
    deepen: Option<u32>,
    filter: Option<&str>,
) -> UploadPackRequest {
    UploadPackRequest {
        wants,
        haves,
        done: true,
        thin_pack: thin,
        no_progress: true,
        include_tag: false,
        ofs_delta: true,
        sideband_all: true,
        wait_for_done: false,
        filter: filter.map(str::to_owned),
        deepen,
        deepen_since: None,
        deepen_not: vec![],
        shallow: vec![],
        want_refs: vec![],
        packfile_uris_protocols: vec![],
    }
}

/// Entries of type `REF_DELTA` (7) in a v2 pack: walk the headers, skipping compressed data with a
/// throwaway inflater. A self-contained pack written by pack-copy has none.
fn count_ref_deltas(pack: &[u8]) -> usize {
    use std::io::Read;
    let n = u32::from_be_bytes(pack[8..12].try_into().unwrap()) as usize;
    let mut pos = 12usize;
    let mut refs = 0usize;
    for _ in 0..n {
        let mut c = pack[pos];
        pos += 1;
        let kind = (c >> 4) & 7;
        while c & 0x80 != 0 {
            c = pack[pos];
            pos += 1;
        }
        match kind {
            6 => {
                // OFS_DELTA: varint offset
                let mut c = pack[pos];
                pos += 1;
                while c & 0x80 != 0 {
                    c = pack[pos];
                    pos += 1;
                }
            }
            7 => {
                refs += 1;
                pos += 20;
            }
            _ => {}
        }
        // Compressed data: inflate to find its length.
        let mut d = flate2::read::ZlibDecoder::new(&pack[pos..]);
        let mut sink = Vec::new();
        d.read_to_end(&mut sink).unwrap();
        pos += d.total_in() as usize;
    }
    refs
}

/// Index `pack` with stock git (`--strict`: every id recomputed and checked) into a client repo
/// that already holds `base_pack` (for thin packs), then return the ids the pack delivered.
fn index_strict(pack: &[u8], pre: &[&[u8]], thin: bool) -> (tempfile::TempDir, BTreeSet<String>) {
    let client = cm::fresh_bare();
    let pd = client.path().join("objects/pack");
    std::fs::create_dir_all(&pd).unwrap();
    for (i, b) in pre.iter().enumerate() {
        let bp = pd.join(format!("pack-pre{i}.pack"));
        std::fs::write(&bp, b).unwrap();
        cm::run_git(client.path(), &["index-pack", bp.to_str().unwrap()]);
    }
    let p = pd.join("pack-out.pack");
    let out = if thin {
        // `--fix-thin` needs `--stdin`: git appends the missing bases and writes the pack under its name.
        let mut child = Command::new("git")
            .current_dir(client.path())
            .args([
                "index-pack",
                "--strict",
                "--stdin",
                "--fix-thin",
                p.to_str().unwrap(),
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        child.stdin.as_mut().unwrap().write_all(pack).unwrap();
        child.wait_with_output().unwrap()
    } else {
        std::fs::write(&p, pack).unwrap();
        Command::new("git")
            .current_dir(client.path())
            .args(["index-pack", "--strict", p.to_str().unwrap()])
            .output()
            .unwrap()
    };
    assert!(
        out.status.success(),
        "index-pack --strict rejected the gix pack: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    // Which ids came from the out pack (verify-pack lists every entry with its recomputed id).
    let idx = p.with_extension("idx");
    let vp = Command::new("git")
        .current_dir(client.path())
        .args(["verify-pack", "-v", idx.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(
        vp.status.success(),
        "{}",
        String::from_utf8_lossy(&vp.stderr)
    );
    let mut ids = BTreeSet::new();
    let mut n = 0usize;
    for l in String::from_utf8_lossy(&vp.stdout).lines() {
        let mut it = l.split_whitespace();
        let (Some(id), Some(kind)) = (it.next(), it.next()) else {
            continue;
        };
        if id.len() == 40 && matches!(kind, "commit" | "tree" | "blob" | "tag") {
            n += 1;
            assert!(
                ids.insert(id.to_string()),
                "duplicate entry id {id} in the gix pack"
            );
        }
    }
    assert_eq!(n, ids.len());
    (client, ids)
}

async fn run_shapes(commits: usize, files: usize, per_commit: usize, dirs: usize, split_at: usize) {
    let t0 = std::time::Instant::now();
    let src = synth(commits, files, per_commit, dirs);
    let base_tip = cm::run_git(
        src.path(),
        &["rev-parse", &format!("main~{}", commits - split_at)],
    )
    .trim()
    .to_string();
    let tip = cm::run_git(src.path(), &["rev-parse", "main"])
        .trim()
        .to_string();
    let base_pack = pack_stdout(src.path(), &[&base_tip], &[]);
    let inc_pack = pack_stdout(src.path(), &[&tip], &[&base_tip]);
    let total_objects = rev_list_objects(src.path(), "main", None, None).len();
    eprintln!(
        "synth: {commits} commits, {total_objects} objects, base pack {} MB, incremental pack {} MB, built in {:.1}s",
        base_pack.len() / 1_000_000,
        inc_pack.len() / 1_000_000,
        t0.elapsed().as_secs_f64()
    );

    // Two served layouts. (1) base pack + incremental pack (a push pack: deltas resolve inside it).
    // (2) ONE pack holding everything (`repack -ad` shape): the remainder's deltas have their bases
    // outside the wanted set — exactly what made gix emit REF_DELTAs through the per-chunk
    // offset→oid table (the 178 GB path) before the engine stopped producing thin packs.
    let one_pack = pack_stdout(src.path(), &[&tip], &[]);
    for (layout, packs) in [
        ("two packs", vec![&base_pack, &inc_pack]),
        ("one pack", vec![&one_pack]),
    ] {
        eprintln!("--- served layout: {layout}");
        let root = tempfile::TempDir::new().unwrap();
        let served = LocalRepo::init(
            root.path(),
            &RepoId::new("t", "scale").unwrap(),
            ObjectFormat::Sha1,
        )
        .unwrap();
        for p in packs {
            served
                .ingest_pack(
                    cm::cursor(p.clone()),
                    IngestOptions {
                        fsck: false,
                        max_bytes: None,
                        thin: false,
                    },
                )
                .await
                .unwrap()
                .unwrap();
        }
        served
            .apply_ref_txn(
                &walgit_proto::v1::RefTransaction {
                    updates: vec![walgit_proto::v1::RefUpdate {
                        name: "refs/heads/main".into(),
                        old_oid: String::new(),
                        new_oid: tip.clone(),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
                false,
            )
            .unwrap();
        let want = gix_hash::ObjectId::from_hex(tip.as_bytes()).unwrap();
        let have = gix_hash::ObjectId::from_hex(base_tip.as_bytes()).unwrap();
        let pack_bytes = (base_pack.len() + inc_pack.len()) as u64;

        let expected_remainder = rev_list_objects(src.path(), &tip, Some(&base_tip), None);
        // The client repo gets the base pack in every shape but D: `index-pack --strict` also checks that
        // every object the pack's commits/trees *reference* exists (a remainder references the base, a
        // depth-1 blobless pack its parent and blobs). The gix pack's own ids are recomputed regardless.
        let shapes: Vec<(&str, UploadPackRequest, Vec<&[u8]>, bool)> = vec![
            // The client offers `thin-pack`; the engine must still answer self-contained (no REF_DELTA
            // to a base outside the pack): indexed here WITHOUT --fix-thin, base present only so
            // --strict's reference check passes.
            (
                "A remainder thin",
                req(vec![want], vec![have], true, None, None),
                vec![&base_pack],
                false,
            ),
            (
                "B remainder self-contained",
                req(vec![want], vec![have], false, None, None),
                vec![&base_pack],
                false,
            ),
            (
                "C depth=1 blob:none",
                req(vec![want], vec![], false, Some(1), Some("blob:none")),
                vec![&base_pack, &inc_pack],
                false,
            ),
            (
                "D full clone",
                req(vec![want], vec![], false, None, None),
                vec![],
                false,
            ),
        ];
        for (name, r, pre, thin) in shapes {
            let rss0 = max_rss_kb();
            let t = std::time::Instant::now();
            let mut out = Vec::new();
            let stats = served
                .upload_pack_gix_with(r, &mut out, None)
                .await
                .unwrap();
            let took = t.elapsed();
            let rss_delta_mb = max_rss_kb().saturating_sub(rss0) / 1024;
            let pack = cm::extract_packfile(&out);
            let (_client, ids) = index_strict(&pack, &pre, thin);
            assert_eq!(
                count_ref_deltas(&pack),
                0,
                "{name}: the gix engine wrote a REF_DELTA (thin) entry"
            );
            eprintln!(
                "{name}: {} objects, {} MB pack, {:.1}s, max RSS grew {rss_delta_mb} MB",
                stats.objects,
                pack.len() / 1_000_000,
                took.as_secs_f64()
            );
            assert_eq!(
                stats.objects as usize,
                ids.len(),
                "{name}: stats vs indexed entries"
            );
            match name {
                "A remainder thin" | "B remainder self-contained" => {
                    assert_eq!(
                        ids, expected_remainder,
                        "{name}: object set differs from git rev-list"
                    );
                }
                "D full clone" => {
                    assert_eq!(
                        ids.len(),
                        total_objects,
                        "{name}: a full clone carries every object"
                    );
                }
                _ => {
                    // depth=1 + blob:none: the tip commit and its trees, no blobs, no parents.
                    let tip_trees =
                        rev_list_objects(src.path(), &tip, None, Some("--filter=blob:none"));
                    assert!(ids.contains(&tip), "{name}: tip missing");
                    assert!(
                        ids.iter().all(|i| tip_trees.contains(i)),
                        "{name}: an object outside the filtered set"
                    );
                    assert!(
                        !ids.contains(&base_tip),
                        "{name}: depth=1 must not carry the parent"
                    );
                }
            }
            // Memory: O(window), not O(objects) — a few times the pack bytes at most (the 178 GB failure
            // was ~1,500× the 113 MB the same fetch produces with stock git).
            let bound_mb = 256 + 4 * pack_bytes / 1_000_000;
            assert!(
                rss_delta_mb <= bound_mb,
                "{name}: max RSS grew {rss_delta_mb} MB (bound {bound_mb} MB)"
            );
        }
    }
}

/// ~30 k objects: runs in the normal tier.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gix_engine_packs_are_strict_valid_and_bounded_in_memory_30k() {
    run_shapes(1_500, 400, 8, 12, 1_200).await;
}

/// ~300 k objects with long delta chains across two packs: `just test-slow`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "large stress test; run in test-slow tier"]
async fn gix_engine_packs_are_strict_valid_and_bounded_in_memory_300k() {
    run_shapes(12_000, 1_500, 10, 40, 10_000).await;
}
