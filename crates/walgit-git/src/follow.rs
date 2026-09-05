//! Fetch the delta that brings refs of a serving copy up to the same refs on an
//! upstream git host, as one self-contained pack ready for `LocalRepo::ingest_pack`
//! — the maintainer's `follow` unit (`[upstream] follow`).
//!
//! A **scratch bare repository per followed repository** (`<dir>/<owner>/<name>.git`,
//! kept between rounds) whose `objects/info/alternates` is the serving copy's object
//! directory. Before each round its `refs/follow/<ref>` are set to the values the WAL
//! has, so `git fetch` negotiates from exactly our tips (one `ls-refs` round trip when
//! nothing moved), `index-pack --fix-thin` completes the thin pack from our own objects,
//! and the pack on disk is self-contained. Nothing is written into the serving copy
//! here: the caller streams the pack through `ingest_pack` like any push and then
//! calls [`FetchedDelta::discard_pack`]. Token (optional) goes through a one-shot
//! credential helper that reads it from the environment — never argv.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;

use crate::GitError;

pub struct FetchedDelta {
    /// The scratch repository (persistent; see module docs).
    pub dir: PathBuf,
    /// `ref → oid` as fetched (upstream's current tips for the asked refs).
    pub tips: HashMap<String, String>,
    /// The pack `git fetch` wrote, when anything was fetched (self-contained).
    pub pack: Option<PathBuf>,
}

impl FetchedDelta {
    /// Remove the fetched pack (+ index) from the scratch once its objects are in
    /// the serving copy; the scratch's refs stay as negotiation tips.
    pub async fn discard_pack(&self) {
        if let Some(p) = &self.pack {
            let _ = tokio::fs::remove_file(p).await;
            for ext in ["idx", "rev", "keep", "promisor", "mtimes"] {
                let _ = tokio::fs::remove_file(p.with_extension(ext)).await;
            }
        }
    }
}

/// Fetch `refs` from `upstream` into the scratch for `(owner, name)` under `dir`,
/// negotiating from `have` (`ref → oid` we hold; missing = fetch its history).
pub async fn fetch_refs<S: std::hash::BuildHasher + Sync>(
    upstream: &str,
    token: Option<&str>,
    serving_objects: &Path,
    have: &HashMap<String, String, S>,
    refs: &[String],
    scratch: &Path,
) -> Result<FetchedDelta, GitError> {
    let git = |args: &[&str]| {
        let mut c = tokio::process::Command::new("git");
        c.arg("--git-dir")
            .arg(scratch)
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        c.env("GIT_TERMINAL_PROMPT", "0");
        if let Some(t) = token {
            // The helper reads the token from the environment: it appears neither on a
            // command line nor in a config file.
            c.env("WALGIT_UPSTREAM_TOKEN", t)
                .env("GIT_CONFIG_COUNT", "1")
                .env("GIT_CONFIG_KEY_0", "credential.helper")
                .env(
                    "GIT_CONFIG_VALUE_0",
                    "!f() { echo username=x-access-token; echo \"password=$WALGIT_UPSTREAM_TOKEN\"; }; f",
                );
        }
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

    // Scratch: create once; its alternates are the serving copy's objects.
    if !scratch.join("HEAD").exists() {
        tokio::fs::create_dir_all(scratch)
            .await
            .map_err(GitError::Io)?;
        ok(
            git(&["init", "-q", "--bare"])
                .output()
                .await
                .map_err(GitError::Io)?,
            "git init",
        )?;
    }
    // Absolute: a relative alternates line is resolved against the scratch's objects dir.
    let serving_objects = std::path::absolute(serving_objects).map_err(GitError::Io)?;
    tokio::fs::create_dir_all(scratch.join("objects/info"))
        .await
        .map_err(GitError::Io)?;
    tokio::fs::write(
        scratch.join("objects/info/alternates"),
        format!("{}\n", serving_objects.display()),
    )
    .await
    .map_err(GitError::Io)?;
    // Leftover packs from a round whose ingest/publish failed: the objects are
    // fetched again (the WAL never saw them), so they are garbage here.
    let pack_dir = scratch.join("objects/pack");
    if let Ok(mut rd) = tokio::fs::read_dir(&pack_dir).await {
        while let Ok(Some(e)) = rd.next_entry().await {
            let _ = tokio::fs::remove_file(e.path()).await;
        }
    }
    // Negotiation tips = exactly the WAL's current values of the followed refs.
    {
        let mut input = String::new();
        for r in refs {
            match have.get(r) {
                Some(oid) => {
                    let _ = std::fmt::Write::write_fmt(
                        &mut input,
                        format_args!("update {} {oid}\n", follow_ref(r)),
                    );
                }
                None => {
                    let _ = std::fmt::Write::write_fmt(
                        &mut input,
                        format_args!("delete {}\n", follow_ref(r)),
                    );
                }
            }
        }
        let mut child = git(&["update-ref", "--stdin"])
            .stdin(Stdio::piped())
            .spawn()
            .map_err(GitError::Io)?;
        {
            use tokio::io::AsyncWriteExt;
            let mut stdin = child
                .stdin
                .take()
                .ok_or_else(|| std::io::Error::other("git stdin unavailable"))?;
            stdin
                .write_all(input.as_bytes())
                .await
                .map_err(GitError::Io)?;
        }
        ok(
            child.wait_with_output().await.map_err(GitError::Io)?,
            "git update-ref --stdin",
        )?;
    }

    let refspecs: Vec<String> = refs
        .iter()
        .map(|r| format!("+{r}:{}", follow_ref(r)))
        .collect();
    let mut args: Vec<&str> = vec![
        // Always a pack, never loose objects (ingest_pack takes a pack).
        "-c",
        "fetch.unpackLimit=1",
        "-c",
        "transfer.unpackLimit=1",
        "-c",
        "fetch.writeCommitGraph=false",
        "-c",
        "gc.auto=0",
        "-c",
        "protocol.version=2",
        "fetch",
        "--no-tags",
        "--no-write-fetch-head",
        "--no-auto-gc",
        "--quiet",
        upstream,
    ];
    args.extend(refspecs.iter().map(String::as_str));
    ok(
        git(&args).output().await.map_err(GitError::Io)?,
        "git fetch <upstream> <refs>",
    )?;
    read_scratch(scratch).await
}

/// What a previous [`fetch_refs`] left in the scratch: upstream's tips
/// (`refs/follow/*`) and the pack it wrote, if any.
pub async fn read_scratch(scratch: &Path) -> Result<FetchedDelta, GitError> {
    let out = tokio::process::Command::new("git")
        .arg("--git-dir")
        .arg(scratch)
        .args([
            "for-each-ref",
            "--format=%(objectname) %(refname)",
            "refs/follow/",
        ])
        .stdin(Stdio::null())
        .output()
        .await
        .map_err(GitError::Io)?;
    if !out.status.success() {
        return Err(GitError::Subprocess {
            cmd: "git for-each-ref".into(),
            status: out.status.code(),
            stderr: String::from_utf8_lossy(&out.stderr).trim().to_string(),
        });
    }
    let mut tips = HashMap::new();
    for line in String::from_utf8_lossy(&out.stdout).lines() {
        if let Some((oid, name)) = line.split_once(' ')
            && let Some(r) = name.strip_prefix("refs/follow/")
        {
            tips.insert(format!("refs/{r}"), oid.to_string());
        }
    }
    // The pack git wrote (one per fetch; none when nothing moved).
    let mut pack = None;
    if let Ok(mut rd) = tokio::fs::read_dir(scratch.join("objects/pack")).await {
        while let Ok(Some(e)) = rd.next_entry().await {
            let p = e.path();
            if p.extension().is_some_and(|x| x == "pack") {
                pack = Some(p);
            }
        }
    }
    Ok(FetchedDelta {
        dir: scratch.to_path_buf(),
        tips,
        pack,
    })
}

/// `refs/heads/main` → `refs/follow/heads/main` (the scratch's copy of upstream's ref).
fn follow_ref(r: &str) -> String {
    format!("refs/follow/{}", r.strip_prefix("refs/").unwrap_or(r))
}
