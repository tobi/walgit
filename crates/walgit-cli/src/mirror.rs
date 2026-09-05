//! `walgit mirror --from URL --to URL --dir PATH [--ref NAME]… [--interval 30s] [--once] [--force]`
//!
//! Keep refs of a repository on a walgit host equal to the same refs on another git host —
//! an inbound bridge while a repository's writes still land elsewhere:
//!
//! ```text
//! walgit mirror --from https://github.com/acme/monorepo.git \
//!               --to   https://git.example.com/acme/monorepo.git \
//!               --dir  /ssd/monorepo-mirror.git
//! ```
//!
//! A local bare repository (`--dir`, created on first run) is the buffer. Every tick is one
//! `git fetch` of the refs from `--from` (negotiation tips = those refs, no tags), then — only
//! for refs that moved since the last confirmed push — one `git ls-remote` on `--to` and one
//! `git push` of whatever differs (fast-forward only unless `--force`). Nothing is stored
//! besides the bare repo; a restart re-derives the state from one ls-remote. The pack count
//! stays bounded with `git repack --geometric` (the same incremental fold walgit's own
//! compaction uses) every `--repack-every`.
//!
//! Auth: `--to` gets a bearer token handed to git through `GIT_CONFIG_*` env (never argv), for
//! http(s) destinations only: `--identity token` (default) uses `$WALGIT_TOKEN` (an access
//! token from the destination's `/_auth/tokens`, or a static token); `--identity gcloud` runs
//! `gcloud auth print-identity-token` (a Google ID token, for a walgit whose OIDC issuer is
//! Google); `--identity gce` asks the GCE metadata server for an ID token with the destination
//! origin as audience. `--from` uses whatever the machine's git config does for that URL. The
//! two sides are separate git processes: neither token ever reaches the other host.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail, ensure};
use tokio::process::Command;
use tracing::{debug, error, info, warn};

pub struct MirrorArgs {
    pub from: String,
    pub to: String,
    pub dir: PathBuf,
    pub refs: Vec<String>,
    pub interval: Duration,
    pub once: bool,
    pub force: bool,
    pub repack_every: Duration,
    pub identity: Identity,
}

/// How the bearer token for `--to` is obtained.
#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum Identity {
    /// `$WALGIT_TOKEN`: a walgit access token or a static token.
    #[default]
    Token,
    /// `gcloud auth print-identity-token` — a Google ID token for the logged-in human.
    Gcloud,
    /// The GCE metadata server: this VM's service account, audience = the destination origin.
    Gce,
}

pub async fn run(args: MirrorArgs) -> Result<()> {
    let refs: Vec<String> = args.refs.iter().map(|r| full_ref_name(r)).collect();
    ensure!(!refs.is_empty(), "at least one --ref is required");
    ensure_bare_repo(&args.dir).await?;

    let token = Token::new(args.identity, &args.to);
    let mut m = Mirror {
        from: args.from,
        to: args.to,
        dir: args.dir,
        refs,
        force: args.force,
        token,
        pushed: HashMap::new(),
    };
    info!(from = %m.from, to = %m.to, dir = %m.dir.display(), refs = ?m.refs, interval = ?args.interval, force = m.force, identity = ?args.identity, "mirror: start");

    let mut last_repack = Instant::now();
    let mut fetched_since_repack = false;
    loop {
        let t0 = Instant::now();
        match m.tick().await {
            Ok(outcome) => {
                if outcome.fetched_anything {
                    fetched_since_repack = true;
                }
                match outcome.pushed.len() {
                    0 => debug!(
                        elapsed_ms = u64::try_from(t0.elapsed().as_millis()).unwrap_or(u64::MAX),
                        "mirror: nothing to do"
                    ),
                    n => info!(
                        elapsed_ms = u64::try_from(t0.elapsed().as_millis()).unwrap_or(u64::MAX),
                        refs = n,
                        "mirror: tick done"
                    ),
                }
            }
            Err(e) => {
                error!(
                    error = format!("{e:#}"),
                    elapsed_ms = u64::try_from(t0.elapsed().as_millis()).unwrap_or(u64::MAX),
                    "mirror: tick failed"
                );
                if args.once {
                    return Err(e);
                }
            }
        }
        if args.once {
            return Ok(());
        }
        if fetched_since_repack && last_repack.elapsed() >= args.repack_every {
            match m.repack().await {
                Ok(()) => {
                    fetched_since_repack = false;
                    last_repack = Instant::now();
                }
                // Not fatal: the next tick works on more packs; try again next period.
                Err(e) => error!(error = format!("{e:#}"), "mirror: repack failed"),
            }
        }
        tokio::time::sleep(args.interval).await;
    }
}

struct Mirror {
    from: String,
    to: String,
    dir: PathBuf,
    refs: Vec<String>,
    force: bool,
    token: Token,
    /// ref → sha last confirmed equal on `to` (by ls-remote or a successful push).
    /// Saves the ls-remote while the source does not move; a restart re-derives it.
    pushed: HashMap<String, String>,
}

#[derive(Debug, Default)]
struct TickOutcome {
    fetched_anything: bool,
    /// Refs pushed this tick.
    pushed: Vec<String>,
}

impl Mirror {
    async fn tick(&mut self) -> Result<TickOutcome> {
        let mut out = TickOutcome::default();

        let before = self.local_tips().await?;
        self.fetch(&before).await?;
        let after = self.local_tips().await?;

        // Refs whose local tip is not yet known to be on the destination.
        let mut candidates: Vec<(String, String)> = Vec::new();
        for name in &self.refs {
            let Some(sha) = after.get(name) else {
                warn!(r#ref = %name, from = %self.from, "mirror: ref does not exist on the source; skipping");
                continue;
            };
            if before.get(name) != Some(sha) {
                out.fetched_anything = true;
                info!(r#ref = %name, old = before.get(name).map_or("-", String::as_str), new = %sha, "mirror: source moved");
            }
            if self.pushed.get(name) != Some(sha) {
                candidates.push((name.clone(), sha.clone()));
            }
        }
        if candidates.is_empty() {
            return Ok(out);
        }

        let remote = self
            .ls_remote(
                &candidates
                    .iter()
                    .map(|(n, _)| n.as_str())
                    .collect::<Vec<_>>(),
            )
            .await?;
        let mut to_push: Vec<(String, String, Option<String>)> = Vec::new();
        for (name, sha) in candidates {
            match remote.get(&name) {
                Some(r) if *r == sha => {
                    info!(r#ref = %name, sha = %sha, "mirror: destination already in sync");
                    self.pushed.insert(name, sha);
                }
                other => to_push.push((name, sha, other.cloned())),
            }
        }
        if to_push.is_empty() {
            return Ok(out);
        }

        for (name, sha, old) in &to_push {
            let commits = match old {
                Some(old) => self
                    .rev_list_count(old, sha)
                    .await
                    .map_or_else(|_| "?".into(), |n| n.to_string()),
                None => "all".into(),
            };
            info!(r#ref = %name, old = old.as_deref().unwrap_or("-"), new = %sha, commits = %commits, to = %self.to, "mirror: pushing");
        }
        let t0 = Instant::now();
        let results = self.push(&to_push).await?;
        let mut rejected = Vec::new();
        for (name, sha, _) in to_push {
            match results.get(&name) {
                Some(Ok(())) => {
                    info!(r#ref = %name, sha = %sha, elapsed_ms = u64::try_from(t0.elapsed().as_millis()).unwrap_or(u64::MAX), "mirror: pushed");
                    self.pushed.insert(name.clone(), sha);
                    out.pushed.push(name);
                }
                Some(Err(reason)) => rejected.push(format!("{name}: {reason}")),
                None => rejected.push(format!("{name}: no status from git push")),
            }
        }
        if !rejected.is_empty() {
            let hint = if self.force {
                ""
            } else {
                " (destination diverged from the source? rerun with --force to make it follow)"
            };
            bail!("push rejected: {}{hint}", rejected.join("; "));
        }
        Ok(out)
    }

    /// `refs/heads/main` → sha for every mirrored ref that exists locally.
    async fn local_tips(&self) -> Result<HashMap<String, String>> {
        let mut cmd = git(Some(&self.dir));
        cmd.arg("show-ref").arg("--").args(&self.refs);
        let out = cmd.output().await.context("running git show-ref")?;
        // Exit 1 = none of the refs exist (fresh buffer); anything else is an error.
        if !out.status.success() && out.status.code() != Some(1) {
            bail!(
                "git show-ref failed: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            );
        }
        let mut tips = HashMap::new();
        for line in String::from_utf8_lossy(&out.stdout).lines() {
            if let Some((sha, name)) = line.split_once(' ') {
                tips.insert(name.to_string(), sha.to_string());
            }
        }
        Ok(tips)
    }

    /// One fetch of every mirrored ref; negotiation starts from the refs we already have
    /// (a missing one is fatal to `--negotiation-tip`, so only existing tips are passed).
    async fn fetch(&self, local: &HashMap<String, String>) -> Result<()> {
        let mut cmd = git(Some(&self.dir));
        cmd.args([
            "fetch",
            "--no-tags",
            "--no-write-fetch-head",
            "--no-auto-gc",
        ]);
        for r in self.refs.iter().filter(|r| local.contains_key(*r)) {
            cmd.arg(format!("--negotiation-tip={r}"));
        }
        cmd.arg(&self.from);
        for r in &self.refs {
            cmd.arg(format!("+{r}:{r}"));
        }
        // stderr inherited: git's progress/messages stay visible live (the first fetch of a
        // big repository can take hours); stdout has nothing we need.
        let status = cmd
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .status()
            .await
            .context("running git fetch")?;
        ensure!(
            status.success(),
            "git fetch from {} exited with {status} (see git's message above)",
            self.from
        );
        Ok(())
    }

    async fn ls_remote(&mut self, refs: &[&str]) -> Result<HashMap<String, String>> {
        let mut cmd = git(None);
        self.auth(&mut cmd).await?;
        cmd.args(["ls-remote", "--", &self.to]).args(refs);
        let out = cmd.output().await.context("running git ls-remote")?;
        if !out.status.success() {
            self.token.invalidate();
            bail!(
                "git ls-remote {} failed: {}",
                self.to,
                String::from_utf8_lossy(&out.stderr).trim()
            );
        }
        let mut found = HashMap::new();
        for line in String::from_utf8_lossy(&out.stdout).lines() {
            if let Some((sha, name)) = line.split_once('\t') {
                found.insert(name.to_string(), sha.to_string());
            }
        }
        Ok(found)
    }

    /// Push `(ref, sha, _)` and report per-ref outcome from `--porcelain` lines.
    async fn push(
        &mut self,
        updates: &[(String, String, Option<String>)],
    ) -> Result<HashMap<String, Result<(), String>>> {
        let mut cmd = git(Some(&self.dir));
        self.auth(&mut cmd).await?;
        cmd.args(["push", "--porcelain", "--no-verify"]);
        if self.force {
            cmd.arg("--force");
        }
        cmd.arg(&self.to);
        for (name, sha, _) in updates {
            cmd.arg(format!("{sha}:{name}"));
        }
        let out = cmd
            .stderr(Stdio::inherit())
            .output()
            .await
            .context("running git push")?;
        let mut results = HashMap::new();
        // Porcelain: `<flag>\t<from>:<to>\t<summary>` where flag ' ' ok, '+' forced, '-' deleted,
        // '*' new, '!' rejected, '=' up to date.
        for line in String::from_utf8_lossy(&out.stdout).lines() {
            let mut parts = line.splitn(3, '\t');
            let (Some(flag), Some(spec), summary) =
                (parts.next(), parts.next(), parts.next().unwrap_or(""))
            else {
                continue;
            };
            let Some((_, name)) = spec.split_once(':') else {
                continue;
            };
            let outcome = if flag == "!" {
                Err(summary.to_string())
            } else {
                Ok(())
            };
            results.insert(name.to_string(), outcome);
        }
        if !out.status.success() && results.values().all(std::result::Result::is_ok) {
            // Failed before any ref status (auth, connection, pack-objects): git said why on stderr.
            self.token.invalidate();
            bail!(
                "git push to {} exited with {} (see git's message above)",
                self.to,
                out.status
            );
        }
        Ok(results)
    }

    async fn rev_list_count(&self, old: &str, new: &str) -> Result<u64> {
        let out = git(Some(&self.dir))
            .args(["rev-list", "--count", &format!("{old}..{new}")])
            .output()
            .await?;
        ensure!(out.status.success(), "rev-list --count failed");
        Ok(String::from_utf8_lossy(&out.stdout).trim().parse()?)
    }

    /// Fold the small packs the fetches keep adding (geometric, like walgit's own compaction);
    /// the big base pack is left alone as long as the progression holds.
    async fn repack(&self) -> Result<()> {
        let t0 = Instant::now();
        let out = git(Some(&self.dir))
            .args(["repack", "-q", "-d", "-l", "--geometric=2", "--write-midx"])
            .output()
            .await
            .context("running git repack")?;
        ensure!(
            out.status.success(),
            "git repack failed: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        );
        info!(
            elapsed_ms = u64::try_from(t0.elapsed().as_millis()).unwrap_or(u64::MAX),
            "mirror: repacked"
        );
        Ok(())
    }

    /// Destination auth: a bearer token for HTTP(S) destinations, through environment
    /// config (not argv); credential helpers off so a bad token fails instead of prompting.
    async fn auth(&mut self, cmd: &mut Command) -> Result<()> {
        if !(self.to.starts_with("https://") || self.to.starts_with("http://")) {
            return Ok(());
        }
        let token = self.token.bearer().await?;
        git_config(
            cmd,
            &[
                (
                    "http.extraHeader",
                    &format!("Authorization: Bearer {token}"),
                ),
                ("credential.helper", ""),
            ],
        );
        Ok(())
    }
}

/// The bearer token for the destination, re-minted when older than `TOKEN_MAX_AGE`
/// (ID tokens live 1 h) or after any failed request to the destination.
struct Token {
    identity: Identity,
    /// Audience for the metadata-server token: the destination origin (`https://host`).
    audience: String,
    value: Option<(String, Instant)>,
}

const TOKEN_MAX_AGE: Duration = Duration::from_mins(50);
const METADATA_IDENTITY_URL: &str =
    "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/identity";

impl Token {
    fn new(identity: Identity, to: &str) -> Self {
        Token {
            identity,
            audience: origin_of(to),
            value: None,
        }
    }

    async fn bearer(&mut self) -> Result<String> {
        if let Some((v, minted)) = &self.value
            && minted.elapsed() < TOKEN_MAX_AGE
        {
            return Ok(v.clone());
        }
        let token = match self.identity {
            Identity::Token => std::env::var("WALGIT_TOKEN")
                .ok()
                .filter(|v| !v.trim().is_empty())
                .map(|v| v.trim().to_string())
                .context("WALGIT_TOKEN is unset: export an access token for the destination (its /_auth/tokens page), or use --identity gcloud|gce")?,
            Identity::Gcloud => gcloud_identity_token().await?,
            Identity::Gce => gce_identity_token(&self.audience).await?,
        };
        self.value = Some((token.clone(), Instant::now()));
        Ok(token)
    }

    fn invalidate(&mut self) {
        self.value = None;
    }
}

/// The logged-in human's Google ID token (`aud` = gcloud's own client; the destination must
/// list it in `server.auth.audiences`).
async fn gcloud_identity_token() -> Result<String> {
    let out = Command::new("gcloud")
        .args(["auth", "print-identity-token"])
        .stdin(Stdio::null())
        .output()
        .await
        .context("running `gcloud auth print-identity-token` (is gcloud installed?)")?;
    ensure!(
        out.status.success(),
        "`gcloud auth print-identity-token` failed: {} — run `gcloud auth login`",
        String::from_utf8_lossy(&out.stderr).trim()
    );
    let token = String::from_utf8_lossy(&out.stdout).trim().to_string();
    ensure!(
        !token.is_empty(),
        "`gcloud auth print-identity-token` printed nothing"
    );
    Ok(token)
}

/// The VM's service-account identity token from the GCE metadata server (`format=full` so the
/// email claims the destination checks are present) — what `gcloud auth print-identity-token`
/// does underneath on a VM, without gcloud in the image.
async fn gce_identity_token(audience: &str) -> Result<String> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;
    let resp = client
        .get(METADATA_IDENTITY_URL)
        .query(&[("audience", audience), ("format", "full")])
        .header("Metadata-Flavor", "Google")
        .send()
        .await
        .context(
            "GCE metadata server unreachable (is this a GCE VM? otherwise use --identity gcloud)",
        )?;
    ensure!(
        resp.status().is_success(),
        "GCE metadata identity token: HTTP {}",
        resp.status()
    );
    let token = resp.text().await?.trim().to_string();
    ensure!(
        !token.is_empty(),
        "GCE metadata identity token: empty response"
    );
    Ok(token)
}

/// `https://git.example.com/acme/monorepo.git` → `https://git.example.com` (the token audience).
fn origin_of(url: &str) -> String {
    match url.find("://") {
        Some(i) => {
            let rest = &url[i + 3..];
            let end = rest.find('/').unwrap_or(rest.len());
            url[..i + 3 + end].to_string()
        }
        None => url.to_string(),
    }
}

fn git(dir: Option<&Path>) -> Command {
    let mut cmd = Command::new("git");
    if let Some(d) = dir {
        cmd.arg("-C").arg(d);
    }
    cmd.env("GIT_TERMINAL_PROMPT", "0")
        .stdin(Stdio::null())
        .kill_on_drop(true);
    cmd
}

/// Pass config to git (and the helpers it spawns) through `GIT_CONFIG_{COUNT,KEY_n,VALUE_n}`.
fn git_config(cmd: &mut Command, pairs: &[(&str, &str)]) {
    cmd.env("GIT_CONFIG_COUNT", pairs.len().to_string());
    for (i, (k, v)) in pairs.iter().enumerate() {
        cmd.env(format!("GIT_CONFIG_KEY_{i}"), k)
            .env(format!("GIT_CONFIG_VALUE_{i}"), v);
    }
}

/// `main` → `refs/heads/main`; anything under `refs/` is taken as is.
fn full_ref_name(r: &str) -> String {
    if r.starts_with("refs/") {
        r.to_string()
    } else {
        format!("refs/heads/{r}")
    }
}

/// `--dir` is a bare repository: create it when missing, refuse anything else.
async fn ensure_bare_repo(dir: &Path) -> Result<()> {
    if dir.join("HEAD").exists() {
        let out = git(Some(dir))
            .args(["rev-parse", "--is-bare-repository"])
            .output()
            .await?;
        ensure!(
            out.status.success() && String::from_utf8_lossy(&out.stdout).trim() == "true",
            "{} exists but is not a bare git repository",
            dir.display()
        );
        return Ok(());
    }
    let out = Command::new("git")
        .args(["init", "-q", "--bare"])
        .arg(dir)
        .output()
        .await
        .context("running git init")?;
    ensure!(
        out.status.success(),
        "git init --bare {} failed: {}",
        dir.display(),
        String::from_utf8_lossy(&out.stderr).trim()
    );
    // Never let a stray git command in this directory start a full gc of a big repository;
    // `repack()` keeps the pack count down instead.
    let out = git(Some(dir))
        .args(["config", "gc.auto", "0"])
        .output()
        .await?;
    ensure!(out.status.success(), "git config gc.auto failed");
    info!(dir = %dir.display(), "mirror: created bare buffer repository");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sh_git(dir: &Path, args: &[&str]) -> String {
        let out = std::process::Command::new("git")
            .current_dir(dir)
            .args(args)
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    }

    fn commit(work: &Path, name: &str) -> String {
        std::fs::write(work.join(name), name).unwrap();
        sh_git(work, &["add", "."]);
        sh_git(work, &["commit", "-q", "-m", name]);
        sh_git(work, &["rev-parse", "HEAD"])
    }

    fn mirror(from: &Path, to: &Path, dir: &Path, force: bool) -> Mirror {
        Mirror {
            from: from.display().to_string(),
            to: to.display().to_string(),
            dir: dir.to_path_buf(),
            refs: vec!["refs/heads/main".into()],
            force,
            token: Token::new(Identity::Token, &to.display().to_string()),
            pushed: HashMap::new(),
        }
    }

    /// Source → buffer → destination over <file://>: first tick publishes everything, a moved
    /// source is pushed on the next tick, an unchanged source is a no-op (no push), a rewound
    /// source is rejected without `--force` and followed with it.
    #[tokio::test]
    async fn mirrors_main_fast_forward_and_force() {
        let tmp = tempfile::tempdir().unwrap();
        let work = tmp.path().join("work");
        std::fs::create_dir(&work).unwrap();
        sh_git(&work, &["init", "-q", "-b", "main"]);
        sh_git(&work, &["config", "user.email", "t@t"]);
        sh_git(&work, &["config", "user.name", "t"]);
        let c1 = commit(&work, "a");
        let src = tmp.path().join("src.git");
        sh_git(
            tmp.path(),
            &["init", "-q", "--bare", "-b", "main", "src.git"],
        );
        sh_git(&work, &["push", "-q", src.to_str().unwrap(), "main:main"]);
        let dst = tmp.path().join("dst.git");
        sh_git(
            tmp.path(),
            &["init", "-q", "--bare", "-b", "main", "dst.git"],
        );

        let dir = tmp.path().join("buffer.git");
        ensure_bare_repo(&dir).await.unwrap();
        assert_eq!(sh_git(&dir, &["config", "gc.auto"]), "0");
        ensure_bare_repo(&dir).await.unwrap(); // idempotent

        let mut m = mirror(&src, &dst, &dir, false);
        let out = m.tick().await.unwrap();
        assert!(out.fetched_anything);
        assert_eq!(out.pushed, vec!["refs/heads/main".to_string()]);
        assert_eq!(sh_git(&dst, &["rev-parse", "refs/heads/main"]), c1);

        // Nothing moved: no push, no ls-remote needed.
        let out = m.tick().await.unwrap();
        assert!(!out.fetched_anything);
        assert!(out.pushed.is_empty());

        // Fast-forward on the source → destination follows.
        let c2 = commit(&work, "b");
        sh_git(&work, &["push", "-q", src.to_str().unwrap(), "main:main"]);
        let out = m.tick().await.unwrap();
        assert_eq!(out.pushed.len(), 1);
        assert_eq!(sh_git(&dst, &["rev-parse", "refs/heads/main"]), c2);

        // Source rewinds to c1: rejected as non-fast-forward without --force …
        sh_git(
            &work,
            &[
                "push",
                "-q",
                "--force",
                src.to_str().unwrap(),
                &format!("{c1}:refs/heads/main"),
            ],
        );
        let err = m.tick().await.unwrap_err().to_string();
        assert!(
            err.contains("push rejected") && err.contains("--force"),
            "{err}"
        );
        assert_eq!(sh_git(&dst, &["rev-parse", "refs/heads/main"]), c2);
        // … and followed with it.
        m.force = true;
        let out = m.tick().await.unwrap();
        assert_eq!(out.pushed.len(), 1);
        assert_eq!(sh_git(&dst, &["rev-parse", "refs/heads/main"]), c1);

        // A destination someone else moved to what the source has is "already in sync": no push.
        let c3 = commit(&work, "c");
        sh_git(
            &work,
            &["push", "-q", "--force", src.to_str().unwrap(), "main:main"],
        );
        sh_git(
            &work,
            &["push", "-q", "--force", dst.to_str().unwrap(), "main:main"],
        );
        let out = m.tick().await.unwrap();
        assert!(out.pushed.is_empty());
        assert_eq!(m.pushed.get("refs/heads/main").unwrap(), &c3);

        m.repack().await.unwrap();
        assert_eq!(sh_git(&dir, &["rev-parse", "refs/heads/main"]), c3);
    }

    #[test]
    fn ref_names() {
        assert_eq!(full_ref_name("main"), "refs/heads/main");
        assert_eq!(full_ref_name("refs/tags/v1"), "refs/tags/v1");
    }

    #[test]
    fn origin_of_url() {
        assert_eq!(
            origin_of("https://git.example.com/acme/monorepo.git"),
            "https://git.example.com"
        );
        assert_eq!(
            origin_of("http://localhost:8080/o/r"),
            "http://localhost:8080"
        );
        assert_eq!(
            origin_of("https://git.example.com"),
            "https://git.example.com"
        );
        assert_eq!(origin_of("/tmp/dst.git"), "/tmp/dst.git");
    }
}
