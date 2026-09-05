//! Fetch exactly the given objects from an upstream git remote and pack them.
//!
//! The maintainer's `repair` unit (desired state: every object reachable from
//! refs is in a live pack) turns an fsck missing list into one pack that the
//! WAL publishes as a COMPACT entry. Scratch repository per call (`dir`), never
//! the serving copy; the remote must serve wants by SHA (GitHub does, for
//! commits, trees and blobs reachable from any ref — verified 2026-08-21).
//! Token (optional) goes through a one-shot credential helper, never argv.

use std::path::{Path, PathBuf};
use std::process::Stdio;

use crate::GitError;

pub struct RepairPack {
    /// Scratch directory holding the repo and the pack; the caller removes it after publishing.
    pub dir: PathBuf,
    pub pack: PathBuf,
    pub idx: PathBuf,
    pub objects: u64,
    pub bytes: u64,
}

/// Fetch every oid in `oids` from `upstream` into a scratch repo under `dir`,
/// then `pack-objects` exactly those oids into `pack-<sha>.pack` + `.idx`.
/// Wants are sent in batches (argv length, server limits).
pub async fn fetch_objects_as_pack(
    upstream: &str,
    token: Option<&str>,
    oids: &[String],
    dir: &Path,
) -> Result<RepairPack, GitError> {
    let scratch = dir.join(format!("repair-{}", uuid::Uuid::new_v4()));
    tokio::fs::create_dir_all(&scratch)
        .await
        .map_err(GitError::Io)?;
    let git = |args: &[&str]| {
        let mut c = tokio::process::Command::new("git");
        c.arg("--git-dir")
            .arg(&scratch)
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        c
    };
    let ok = |out: std::process::Output, what: &str| -> Result<std::process::Output, GitError> {
        if out.status.success() {
            Ok(out)
        } else {
            Err(GitError::Subprocess {
                cmd: what.to_string(),
                status: out.status.code(),
                stderr: String::from_utf8_lossy(&out.stderr).trim().to_string(),
            })
        }
    };
    ok(
        git(&["init", "-q", "--bare"])
            .output()
            .await
            .map_err(GitError::Io)?,
        "git init",
    )?;
    let helper = token
        .map(|t| {
            format!(
                "!f(){{ echo username=x-access-token; echo 'password={}'; }}; f",
                t.replace('\'', "")
            )
        })
        .unwrap_or_default();

    for chunk in oids.chunks(FETCH_BATCH) {
        let mut args: Vec<&str> = vec![
            "-c",
            "fetch.negotiationAlgorithm=noop",
            "-c",
            "protocol.version=2",
        ];
        let helper_arg = format!("credential.helper={helper}");
        if token.is_some() {
            args.extend(["-c", helper_arg.as_str()]);
        }
        args.extend([
            "fetch",
            "--no-tags",
            "--no-write-fetch-head",
            "--quiet",
            "--depth=1",
            upstream,
        ]);
        args.extend(chunk.iter().map(String::as_str));
        ok(
            git(&args).output().await.map_err(GitError::Io)?,
            "git fetch <upstream> <oids>",
        )?;
    }

    // Pack exactly the requested objects (no --revs: no closure, what was asked).
    let pack_base = scratch.join("pack");
    let mut child = git(&[
        "pack-objects",
        "--no-reuse-delta",
        "--compression=6",
        pack_base.to_str().unwrap_or("pack"),
    ])
    .stdin(Stdio::piped())
    .spawn()
    .map_err(GitError::Io)?;
    {
        use tokio::io::AsyncWriteExt;
        let mut stdin = child.stdin.take().ok_or_else(|| {
            GitError::InvalidInput("git pack-objects stdin unavailable".to_owned())
        })?;
        let mut input = oids.join("\n");
        input.push('\n');
        stdin
            .write_all(input.as_bytes())
            .await
            .map_err(GitError::Io)?;
    }
    let out = ok(
        child.wait_with_output().await.map_err(GitError::Io)?,
        "git pack-objects",
    )?;
    let sha = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if sha.len() < 40 {
        return Err(GitError::Protocol(format!(
            "pack-objects printed no checksum: {sha:?}"
        )));
    }
    let pack = scratch.join(format!("pack-{sha}.pack"));
    let idx = scratch.join(format!("pack-{sha}.idx"));
    let bytes = tokio::fs::metadata(&pack)
        .await
        .map_err(GitError::Io)?
        .len();
    // Every requested object must be in the pack (a want the server refused is a hole left open).
    let index = gix_pack::index::File::at(&idx, gix_hash::Kind::Sha1)
        .map_err(|e| GitError::Gix(Box::new(e)))?;
    let mut objects = 0u64;
    let mut first_missing = None;
    for o in oids {
        match gix_hash::ObjectId::from_hex(o.as_bytes()) {
            Ok(id) if index.lookup(id).is_some() => objects += 1,
            _ => {
                first_missing.get_or_insert(o.as_str());
            }
        }
    }
    if let Some(m) = first_missing {
        return Err(GitError::Protocol(format!(
            "upstream served {objects} of {} requested objects (first missing: {m})",
            oids.len()
        )));
    }
    Ok(RepairPack {
        dir: scratch,
        pack,
        idx,
        objects,
        bytes,
    })
}

const FETCH_BATCH: usize = 500;
