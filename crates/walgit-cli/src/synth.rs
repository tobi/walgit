//! `walgit synth` — generate a deterministic synthetic git repository.
//!
//! Produces a real git repo on disk by streaming a `git fast-import` stream
//! that we generate in Rust.  Same seed → byte-identical repo (same HEAD,
//! same objects, same refs).  The repo is a normal working tree, not bare, so
//! `git fsck` / `git log` work immediately.
//!
//! Size presets:
//!   **s** — 50 commits, 200 files
//!   **m** — 2 000 commits, 5 000 files, binary blobs, 20 branches, 50 tags
//!   **l** — 50 000 commits, 50 000 files

use std::io::Write;
use std::path::PathBuf;
use std::process::{Command, Stdio};

use anyhow::{Context, Result, bail};

use crate::SynthSize;

/// (commits, files, branches, tags, `binary_blobs`)
fn size_params(
    size: SynthSize,
    commits: Option<u64>,
    files: Option<u64>,
) -> (u64, u64, u64, u64, bool) {
    let (c, f, br, tg, bin) = match size {
        SynthSize::S => (50, 200, 1, 0, false),
        SynthSize::M => (2_000, 5_000, 20, 50, true),
        SynthSize::L => (50_000, 50_000, 4, 10, false),
    };
    (commits.unwrap_or(c), files.unwrap_or(f), br, tg, bin)
}

pub async fn run(
    out: PathBuf,
    size: SynthSize,
    commits: Option<u64>,
    files: Option<u64>,
    seed: Option<u64>,
) -> Result<()> {
    let (n_commits, n_files, n_branches, n_tags, binary) = size_params(size, commits, files);
    let seed = seed.unwrap_or(42);

    if out.exists() && std::fs::read_dir(&out)?.next().is_some() {
        bail!("output directory {} is not empty", out.display());
    }
    std::fs::create_dir_all(&out)?;

    // git init
    let _git_dir = out.join(".git");
    let status = Command::new("git")
        .args(["init", "--quiet"])
        .current_dir(&out)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .status()
        .context("running git init")?;
    if !status.success() {
        bail!("git init failed");
    }

    // Set a deterministic author/commiter identity so objects are reproducible.
    for (key, val) in [
        ("user.name", "walgit-synth"),
        ("user.email", "synth@walgit.local"),
        ("committer.date", "2020-01-01T00:00:00Z"),
        ("author.date", "2020-01-01T00:00:00Z"),
    ] {
        Command::new("git")
            .args(["config", key, val])
            .current_dir(&out)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()?;
    }

    tracing::info!(
        commits = n_commits,
        files = n_files,
        branches = n_branches,
        tags = n_tags,
        seed,
        "generating synthetic repo"
    );

    let stream = generate_stream(seed, n_commits, n_files, n_branches, n_tags, binary);

    // Pipe the stream to `git fast-import`.
    let mut child = Command::new("git")
        .args(["fast-import", "--quiet", "--done"])
        .current_dir(&out)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
        .context("spawning git fast-import")?;

    {
        let mut stdin = child.stdin.take().context("no stdin")?;
        stdin.write_all(&stream)?;
        stdin.flush()?;
    }

    let status = child.wait().context("waiting for git fast-import")?;
    if !status.success() {
        bail!("git fast-import failed (exit {status})");
    }

    // Checkout the main branch so it's a working tree.
    Command::new("git")
        .args(["checkout", "-f", "main"])
        .current_dir(&out)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()?;

    // Verify with git fsck.
    let fsck = Command::new("git")
        .args(["fsck", "--full", "--strict"])
        .current_dir(&out)
        .output()
        .context("running git fsck")?;
    if !fsck.status.success() {
        bail!(
            "git fsck failed:\n{}",
            String::from_utf8_lossy(&fsck.stderr)
        );
    }

    // Print the HEAD commit for verification.
    let head = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(&out)
        .output()?;
    let head = String::from_utf8_lossy(&head.stdout).trim().to_string();
    println!("synth OK: {n_commits} commits, {n_files} files, HEAD={head}");

    Ok(())
}

// ---------------------------------------------------------------------------
// fast-import stream generation
// ---------------------------------------------------------------------------

/// Deterministic LCG — fast, good enough for content generation.
struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed.max(1))
    }
    fn next_u64(&mut self) -> u64 {
        // xorshift64*
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545F4914F6CDD1D)
    }
    /// [0, n)
    fn below(&mut self, n: u64) -> u64 {
        if n == 0 { 0 } else { self.next_u64() % n }
    }
    /// Fills `buf` with deterministic pseudo-random bytes.
    fn fill_bytes(&mut self, buf: &mut [u8]) {
        let mut i = 0;
        while i + 8 <= buf.len() {
            let v = self.next_u64().to_le_bytes();
            buf[i..i + 8].copy_from_slice(&v);
            i += 8;
        }
        if i < buf.len() {
            let v = self.next_u64().to_le_bytes();
            let rem = buf.len() - i;
            buf[i..i + rem].copy_from_slice(&v[..rem]);
        }
    }
}

/// Generate the complete fast-import stream as a `Vec<u8>`.
fn generate_stream(
    seed: u64,
    n_commits: u64,
    n_files: u64,
    n_branches: u64,
    n_tags: u64,
    binary: bool,
) -> Vec<u8> {
    let mut w = StreamWriter::new();
    let mut rng = Rng::new(seed);

    // We maintain a simple model: each file has an evolving content blob.
    // File names are `dir/i/file_N.txt` (or `.bin` for binary files) to
    // exercise directory structure.
    //
    // For each commit we modify a subset of files (create/update), which
    // produces a deterministic DAG.
    //
    // Branches: we create `n_branches` branches at evenly-spaced commits.
    // Tags: we create `n_tags` annotated tags at evenly-spaced commits.

    let main = "refs/heads/main";
    let mut prev_mark: u32 = 0; // 0 = no parent (root commit)

    // Pre-compute branch creation points.
    let branch_points: Vec<(u64, String)> = if n_branches > 1 {
        (0..n_branches)
            .map(|i| {
                let bp = (i + 1) * n_commits / (n_branches + 1);
                let name = format!("refs/heads/branch-{i}");
                (bp.max(1), name)
            })
            .collect()
    } else {
        Vec::new()
    };

    // Pre-compute tag points.
    let tag_points: Vec<(u64, String)> = if n_tags > 0 {
        (0..n_tags)
            .map(|i| {
                let tp = (i + 1) * n_commits / (n_tags + 1);
                let name = format!("refs/tags/tag-{i}");
                (tp.max(1), name)
            })
            .collect()
    } else {
        Vec::new()
    };

    // mark counter: blobs and commits share the same mark space.
    let mut next_mark: u32 = 1;

    for commit_idx in 0..n_commits {
        let commit_num = commit_idx + 1; // 1-based

        // How many files to touch in this commit (1..=8, but capped by n_files).
        let touch = 1 + rng.below(8).min(n_files.max(1));
        let mut file_changes: Vec<(String, Vec<u8>)> = Vec::with_capacity(touch as usize);

        for _ in 0..touch {
            let file_idx = rng.below(n_files);
            let (path, content) = generate_file(&mut rng, file_idx, binary, commit_num);
            file_changes.push((path, content));
        }

        // Emit blobs and collect their marks.
        let mut blob_marks: Vec<(String, u32)> = Vec::with_capacity(file_changes.len());
        for (path, content) in &file_changes {
            let mark = next_mark;
            next_mark += 1;
            w.emit_blob(mark, content);
            blob_marks.push((path.clone(), mark));
        }

        // Emit commit.
        let commit_mark = next_mark;
        next_mark += 1;

        let ts = 1262304000 + commit_num * 60; // 2020-01-01 + 1min per commit
        let ts_str = format!("{ts} +0000");

        w.write_str(&format!("commit {main}\n"));
        w.write_str(&format!("mark :{commit_mark}\n"));
        w.write_str(&format!(
            "author walgit-synth <synth@walgit.local> {ts_str}\n"
        ));
        w.write_str(&format!(
            "committer walgit-synth <synth@walgit.local> {ts_str}\n"
        ));
        w.write_str(&format!("data {}\n", message_for(commit_num).len() + 1));
        w.write_str(&format!("{}\n", message_for(commit_num)));

        if prev_mark != 0 {
            w.write_str(&format!("from :{prev_mark}\n"));
        }

        for (path, mark) in &blob_marks {
            w.write_str(&format!("M 100644 :{mark} {path}\n"));
        }

        w.write('\n'); // end of commit
        prev_mark = commit_mark;

        // Create branches at the right points.
        for (bp, name) in &branch_points {
            if *bp == commit_num {
                w.write_str(&format!("reset {name}\n"));
                w.write_str(&format!("from :{commit_mark}\n\n"));
            }
        }

        // Create annotated tags at the right points.
        for (tp, name) in &tag_points {
            if *tp == commit_num {
                let tag_msg = format!("synth tag at commit {commit_num}");
                w.write_str(&format!("tag {name}\n"));
                w.write_str(&format!("from :{commit_mark}\n"));
                w.write_str(&format!(
                    "tagger walgit-synth <synth@walgit.local> {ts_str}\n"
                ));
                w.write_str(&format!("data {}\n", tag_msg.len() + 1));
                w.write_str(&format!("{tag_msg}\n\n"));
            }
        }
    }

    // If we never created a `main` branch (n_commits == 0), create an empty
    // root so the repo is valid.  In practice n_commits >= 50 so this is dead
    // code, but it keeps the function total.
    if n_commits == 0 {
        w.write_str(&format!("reset {main}\n\n"));
    }

    w.write_str("done\n");
    w.into_bytes()
}

fn message_for(n: u64) -> String {
    format!("synth commit {n}")
}

/// Generate a deterministic file path and content for file index `file_idx`
/// at `commit_num`.
fn generate_file(rng: &mut Rng, file_idx: u64, binary: bool, commit_num: u64) -> (String, Vec<u8>) {
    // Distribute files across directories: dir_0/, dir_1/, ...
    let dir = file_idx / 100;
    let is_binary = binary && file_idx.is_multiple_of(13);
    let ext = if is_binary { "bin" } else { "txt" };
    let path = format!("src/dir_{dir}/file_{file_idx:05}.{ext}");

    let content = if is_binary {
        // Binary blob: 256..4096 random bytes.
        let len = 256 + rng.below(3840) as usize;
        let mut buf = vec![0u8; len];
        rng.fill_bytes(&mut buf);
        buf
    } else {
        // Text: a few lines, deterministic.
        let lines = 3 + (rng.next_u64() % 20) as usize;
        let mut s = String::with_capacity(lines * 40);
        for i in 0..lines {
            {
                let _ = std::fmt::Write::write_fmt(
                    &mut s,
                    format_args!(
                        "line {i} of file {file_idx} at commit {commit_num}: {:016x}\n",
                        rng.next_u64()
                    ),
                );
            };
        }
        s.into_bytes()
    };

    (path, content)
}

// ---------------------------------------------------------------------------
// A small buffered writer for the fast-import stream.
// ---------------------------------------------------------------------------

struct StreamWriter {
    buf: Vec<u8>,
}

impl StreamWriter {
    fn new() -> Self {
        StreamWriter {
            buf: Vec::with_capacity(64 * 1024),
        }
    }

    fn write_str(&mut self, s: &str) {
        self.buf.extend_from_slice(s.as_bytes());
    }

    fn write(&mut self, b: char) {
        self.buf.push(b as u8);
    }

    /// Emit a blob with a mark: `blob\nmark :N\ndata <len>\n<bytes>\n`
    fn emit_blob(&mut self, mark: u32, content: &[u8]) {
        self.write_str(&format!("blob\nmark :{mark}\n"));
        self.write_str(&format!("data {}\n", content.len()));
        self.buf.extend_from_slice(content);
        self.write('\n');
    }

    fn into_bytes(self) -> Vec<u8> {
        self.buf
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn rng_determinism() {
        let mut a = Rng::new(42);
        let mut b = Rng::new(42);
        for _ in 0..100 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
    }

    #[test]
    fn stream_has_done() {
        let s = generate_stream(42, 5, 10, 1, 0, false);
        let text = String::from_utf8_lossy(&s);
        assert!(text.contains("done\n"));
        assert!(text.contains("commit refs/heads/main"));
        assert!(text.contains("blob\nmark :"));
    }

    #[tokio::test]
    async fn synth_s_produces_valid_repo() {
        let tmp = tempfile::tempdir().unwrap();
        let out = tmp.path().join("repo");
        run(out.clone(), SynthSize::S, None, None, Some(999))
            .await
            .unwrap();

        // Same seed → same HEAD.
        let tmp2 = tempfile::tempdir().unwrap();
        let out2 = tmp2.path().join("repo");
        run(out2, SynthSize::S, None, None, Some(999))
            .await
            .unwrap();

        let head1 = git_head(&out).unwrap();
        let head2 = git_head2(&tmp2).unwrap();
        assert_eq!(head1, head2, "same seed must produce same HEAD");
    }

    fn git_head(out: &Path) -> Result<String> {
        let o = Command::new("git")
            .args(["rev-parse", "HEAD"])
            .current_dir(out)
            .output()?;
        Ok(String::from_utf8_lossy(&o.stdout).trim().to_string())
    }

    fn git_head2(tmp: &tempfile::TempDir) -> Result<String> {
        let out = tmp.path().join("repo");
        git_head(&out)
    }
}
