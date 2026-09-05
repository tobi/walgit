// Test fixtures use panics to fail the test, including shared helper functions.
#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::unwrap_used
)]

//! Shared helpers for walgit-git integration tests: build synthetic repos with
//! upstream `git` and produce packs via `git pack-objects`. Each test binary
//! uses a subset, so unused-item warnings are expected here.
#![allow(dead_code)]

use std::path::PathBuf;
use std::process::{Command, Stdio};

use tempfile::TempDir;

/// A non-bare source worktree created with upstream git.
pub struct SourceRepo {
    pub dir: PathBuf,
    _tmp: TempDir,
}

/// Owned cursor satisfying `AsyncRead + Unpin + Send + 'static` (`ingest_pack`
/// requires `'static`, so a borrowed `&[u8]` won't do).
pub fn cursor(b: Vec<u8>) -> std::io::Cursor<Vec<u8>> {
    std::io::Cursor::new(b)
}

impl SourceRepo {
    /// Create a source repo with one initial commit (`file1`).
    pub fn new() -> Self {
        let tmp = TempDir::new().expect("tmpdir");
        let dir = tmp.path().to_path_buf();
        run_git(&dir, &["init", "-q", dir.to_str().unwrap()]);
        run_git(&dir, &["config", "user.email", "t@t"]);
        run_git(&dir, &["config", "user.name", "t"]);
        run_git(&dir, &["config", "commit.gpgsign", "false"]);
        let s = SourceRepo { dir, _tmp: tmp };
        s.commit_file("file1.txt", "hello\n", "init");
        s
    }

    pub fn commit_file(&self, path: &str, content: &str, msg: &str) -> String {
        let p = self.dir.join(path);
        if let Some(parent) = p.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(&p, content).unwrap();
        run_git(&self.dir, &["add", path]);
        run_git(&self.dir, &["commit", "-q", "-m", msg]);
        self.head()
    }

    /// Branch the current HEAD under `name`.
    pub fn branch(&self, name: &str) {
        run_git(&self.dir, &["branch", name]);
    }

    /// Create an annotated tag pointing at `sha`, return the tag object id.
    pub fn annotated_tag(&self, name: &str, sha: &str) -> String {
        run_git(
            &self.dir,
            &["tag", "-a", name, sha, "-m", format!("tag {name}").as_str()],
        );
        // The tag object id (not the peeled commit).
        let out = Command::new("git")
            .current_dir(&self.dir)
            .args(["rev-parse", &format!("refs/tags/{name}")])
            .output()
            .unwrap();
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    }

    /// A commit object `git fsck` rejects (author line without the `<email>` and date): written
    /// with `hash-object --literally` so the source repo accepts it; returns its id. Only
    /// `pack-objects` with an explicit id reaches it (no ref points at it).
    pub fn bad_object(&self) -> String {
        let tree = run_git(&self.dir, &["rev-parse", "HEAD^{tree}"])
            .trim()
            .to_string();
        let body = format!("tree {tree}\nauthor broken\ncommitter broken\n\nbad\n");
        let mut child = Command::new("git")
            .current_dir(&self.dir)
            .args([
                "hash-object",
                "-t",
                "commit",
                "-w",
                "--literally",
                "--stdin",
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        {
            use std::io::Write;
            child
                .stdin
                .as_mut()
                .unwrap()
                .write_all(body.as_bytes())
                .unwrap();
        }
        let out = child.wait_with_output().unwrap();
        assert!(out.status.success());
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    }

    pub fn head(&self) -> String {
        let out = Command::new("git")
            .current_dir(&self.dir)
            .args(["rev-parse", "HEAD"])
            .output()
            .unwrap();
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    }

    pub fn rev(&self, rev: &str) -> String {
        let out = Command::new("git")
            .current_dir(&self.dir)
            .args(["rev-parse", rev])
            .output()
            .unwrap();
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    }

    /// Produce a pack covering objects reachable from `includes` but not from
    /// `excludes` (each a rev spec). When `thin`, emit a thin pack.
    pub fn pack(&self, includes: &[&str], excludes: &[&str], thin: bool) -> Vec<u8> {
        let mut input = String::new();
        for inc in includes {
            input.push_str(inc);
            input.push('\n');
        }
        for exc in excludes {
            input.push('^');
            input.push_str(exc);
            input.push('\n');
        }
        let mut cmd = Command::new("git");
        cmd.current_dir(&self.dir)
            .args(["pack-objects", "--stdout", "--revs"]);
        if thin {
            cmd.arg("--thin");
        }
        cmd.stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = cmd.spawn().unwrap();
        {
            use std::io::Write;
            let stdin = child.stdin.as_mut().unwrap();
            stdin.write_all(input.as_bytes()).unwrap();
        }
        let out = child.wait_with_output().unwrap();
        assert!(
            out.status.success(),
            "pack-objects failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        out.stdout
    }
}

pub fn run_git(dir: &std::path::Path, args: &[&str]) -> String {
    let out = Command::new("git")
        .current_dir(dir)
        .args(args)
        .output()
        .unwrap_or_else(|e| panic!("git {args:?}: {e}"));
    assert!(
        out.status.success(),
        "git {:?} failed: {}",
        args,
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).into_owned()
}

/// Make a fresh bare repo dir for index-pack verification.
pub fn fresh_bare() -> TempDir {
    let tmp = TempDir::new().unwrap();
    run_git(
        tmp.path(),
        &["init", "-q", "--bare", tmp.path().to_str().unwrap()],
    );
    tmp
}

/// Sync pkt-line parser for test assertions.
pub fn parse_pkt_lines(buf: &[u8]) -> Vec<PktLine> {
    let mut out = Vec::new();
    let mut pos = 0;
    while pos + 4 <= buf.len() {
        let len = match std::str::from_utf8(&buf[pos..pos + 4]) {
            Ok(s) => match u16::from_str_radix(s, 16) {
                Ok(n) => n as usize,
                Err(_) => break,
            },
            Err(_) => break,
        };
        match len {
            0 => {
                out.push(PktLine::Flush);
                pos += 4;
            }
            1 => {
                out.push(PktLine::Delim);
                pos += 4;
            }
            2 => {
                out.push(PktLine::ResponseEnd);
                pos += 4;
            }
            _ => {
                let end = pos + len;
                if end > buf.len() {
                    break;
                }
                out.push(PktLine::Data(buf[pos + 4..end].to_vec()));
                pos = end;
            }
        }
    }
    out
}

#[derive(Debug, Clone)]
pub enum PktLine {
    Data(Vec<u8>),
    Flush,
    Delim,
    ResponseEnd,
}

/// Demultiplex the v2 `packfile` section: given the full response bytes, find
/// the `packfile\n` section header and concatenate channel-1 data pkt-lines
/// into the raw pack bytes.
pub fn extract_packfile(response: &[u8]) -> Vec<u8> {
    let lines = parse_pkt_lines(response);
    let mut pack = Vec::new();
    let mut in_packfile = false;
    for line in lines {
        match line {
            PktLine::Flush | PktLine::Delim | PktLine::ResponseEnd => {
                in_packfile = false;
            }
            PktLine::Data(b) => {
                if !in_packfile {
                    // Plain section line, or (sideband-all) band-1 framed.
                    let line = if b.first() == Some(&1) {
                        &b[1..]
                    } else {
                        &b[..]
                    };
                    if line.strip_suffix(b"\n").is_some_and(|s| s == b"packfile") {
                        in_packfile = true;
                    }
                    continue;
                }
                // Sideband frame: first byte is the channel.
                if b.is_empty() {
                    continue;
                }
                let channel = b[0];
                if channel == 1 {
                    pack.extend_from_slice(&b[1..]);
                }
                // channel 2 = progress, 3 = error; ignore for pack extraction.
            }
        }
    }
    pack
}

/// True if the response contains an `ACK <oid>` line in the acknowledgments
/// section.
pub fn has_ack(response: &[u8], oid: &str) -> bool {
    let needle = format!("ACK {oid}");
    response
        .windows(needle.len())
        .any(|w| w == needle.as_bytes())
}

/// True if the response contains a `NAK` line.
pub fn has_nak(response: &[u8]) -> bool {
    response.windows(3).any(|w| w == b"NAK")
}

/// Count object types in a pack file via `git verify-pack -v` in a fresh bare
/// repo. Returns (`num_blobs`, `num_commits`, `num_trees`, `num_tags`).
pub fn pack_object_types(pack: &[u8]) -> (u64, u64, u64, u64) {
    let tmp = fresh_bare();
    // Write the pack and index it.
    let pack_path = tmp
        .path()
        .join("objects")
        .join("pack")
        .join("pack-test.pack");
    std::fs::create_dir_all(pack_path.parent().unwrap()).unwrap();
    std::fs::write(&pack_path, pack).unwrap();
    run_git(tmp.path(), &["index-pack", pack_path.to_str().unwrap()]);
    let idx = pack_path.with_extension("idx");
    let out = Command::new("git")
        .current_dir(tmp.path())
        .args(["verify-pack", "-v", idx.to_str().unwrap()])
        .output()
        .unwrap();
    let s = String::from_utf8_lossy(&out.stdout);
    let mut blobs = 0u64;
    let mut commits = 0u64;
    let mut trees = 0u64;
    let mut tags = 0u64;
    for line in s.lines() {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() >= 2 {
            match parts[1] {
                "blob" => blobs += 1,
                "commit" => commits += 1,
                "tree" => trees += 1,
                "tag" => tags += 1,
                _ => {}
            }
        }
    }
    (blobs, commits, trees, tags)
}

/// Index a pack into a fresh bare repo and run `git fsck`. Returns the repo
/// tempdir (kept alive for the caller's further use).
pub fn index_and_fsck(pack: &[u8]) -> TempDir {
    let tmp = fresh_bare();
    let pack_path = tmp
        .path()
        .join("objects")
        .join("pack")
        .join("pack-test.pack");
    std::fs::create_dir_all(pack_path.parent().unwrap()).unwrap();
    std::fs::write(&pack_path, pack).unwrap();
    run_git(tmp.path(), &["index-pack", pack_path.to_str().unwrap()]);
    // fsck
    let out = Command::new("git")
        .current_dir(tmp.path())
        .args(["fsck", "--full", "--strict"])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "fsck failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    tmp
}
