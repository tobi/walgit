//! `walgit` (full CLI: serve | compact | bundle | repo | wal | synth | import | mirror | config)
//! and `walgit-server` (`walgit serve` under the name a standalone deployment expects, D39),
//! both thin bins over this library.
//!
//! The only flag is the global `--config PATH` (D8); no subcommand = `serve`. Every command loads
//! `walgit.toml`, applies `WALGIT__` env overrides, and initialises tracing
//! from `[telemetry]` before dispatching.
#![allow(
    clippy::case_sensitive_file_extension_comparisons,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::cloned_ref_to_slice_refs,
    clippy::doc_lazy_continuation,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::many_single_char_names,
    clippy::needless_continue,
    clippy::needless_pass_by_value,
    clippy::redundant_locals,
    clippy::string_slice,
    clippy::unnecessary_sort_by,
    clippy::unused_async,
    clippy::unwrap_used,
    clippy::unreadable_literal
)]

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

mod cli;
mod config_cmd;
mod settings_cmd;
mod synth;

mod bundle_cmd;
mod compact;
mod import;
mod import_direct;
mod mirror;
mod repo;
mod serve;
mod wal_cmd;

use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};

use walgit_config::Config;
use walgit_server::telemetry::tracing_init;

#[derive(Parser)]
#[command(
    name = "walgit",
    version = walgit_server::health::BUILD_SHA,
    about = "Git at any scale, on object storage, in Rust"
)]
struct Cli {
    /// Path to the configuration file.
    #[arg(
        long,
        global = true,
        env = "WALGIT_CONFIG",
        default_value = "walgit.toml"
    )]
    config: PathBuf,

    /// No subcommand = `serve`.
    #[command(subcommand)]
    command: Option<Command>,
}

/// `walgit-server`: the server and nothing else.
#[derive(Parser)]
#[command(name = "walgit-server", version = walgit_server::health::BUILD_SHA, about = "walgit, standalone: git at any scale on an object-storage bucket")]
struct ServerCli {
    /// Path to the configuration file.
    #[arg(
        long,
        global = true,
        env = "WALGIT_CONFIG",
        default_value = "walgit.toml"
    )]
    config: PathBuf,
}

#[derive(Subcommand)]
enum Command {
    /// Run the HTTP server (smart HTTP v0/v2, LFS, bundles, optional compaction/bundle loops).
    Serve,
    /// Trigger compaction (geometric repack) for one repo or all.
    Compact {
        /// `owner/name` — omit with `--all` for every repo.
        repo: Option<String>,
        #[arg(long)]
        all: bool,
        /// Run once and exit (no loop).
        #[arg(long)]
        once: bool,
        /// Rebuild the tier-2 base: full `git repack -adb` + bitmap + commit-graph
        /// layer, published as a COMPACT entry, then a checkpoint at that seq.
        /// Needs the whole pack set on local disk (the weekly VM job), never
        /// a serverless host. Follow with `walgit bundle compose`.
        #[arg(long)]
        base: bool,
    },
    /// Build and publish bundles.
    Bundle {
        #[command(subcommand)]
        action: BundleAction,
    },
    /// Repository management.
    Repo {
        #[command(subcommand)]
        action: RepoAction,
    },
    /// WAL inspection and rewind.
    Wal {
        #[command(subcommand)]
        action: WalAction,
    },
    /// Generate a deterministic synthetic repository via `git fast-import`.
    Synth {
        /// Output directory (must not exist or be empty).
        #[arg(long)]
        out: PathBuf,
        /// Repo size preset: s, m, l.
        #[arg(long)]
        size: SynthSize,
        /// Override commit count.
        #[arg(long)]
        commits: Option<u64>,
        /// Override file count.
        #[arg(long)]
        files: Option<u64>,
        /// PRNG seed for deterministic output.
        #[arg(long)]
        seed: Option<u64>,
    },
    /// Import an existing git repository into walgit.
    Import {
        /// Path to the source `.git` directory or working tree.
        #[arg(long)]
        from: PathBuf,
        /// Target repo id `owner/name`.
        repo: String,
        /// Copy the source's existing packfiles as-is instead of re-packing with
        /// `git pack-objects` (fast for large, already well-packed repos; no
        /// bitmap base is built — compaction will do that later).
        #[arg(long)]
        reuse_packs: bool,
        /// Publish straight into the bucket (no local walgit copy): upload the
        /// pack set (striped parallel uploads + compose), write a checkpoint and
        /// CAS the manifest. Best with ONE bitmap'd pack from
        /// `git pack-objects --all --write-bitmap-index <dir>/pack` (see --packs).
        #[arg(long)]
        direct: bool,
        /// Directory with the pack set to publish (--direct). Default: the source's objects/pack.
        #[arg(long)]
        packs: Option<PathBuf>,
        /// (--direct) Also publish a bundle-uri full bundle = header ∘ pack (zero extra upload on GCS).
        #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
        bundle: bool,
        /// (--direct) Bundle strategy name (default: first full strategy in config, else "import").
        #[arg(long)]
        bundle_strategy: Option<String>,
        /// (--direct) Supersede an existing non-empty repository.
        #[arg(long)]
        replace: bool,
        /// (--direct) An interrupted import resumes from its marker only while the target's manifest is
        /// unchanged; with --force a moved target starts the import over (uploads whose checksums
        /// match are still reused).
        #[arg(long)]
        force: bool,
        /// (--direct) Publish a commit-graph layer with the base pack (built from the source if absent).
        #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
        commit_graph: bool,
        /// Ref globs to publish (repeatable; `*` matches anything incl. `/`), e.g.
        /// `--refs refs/heads/main --refs 'refs/tags/v*'`. Default: refs/heads/* and
        /// refs/tags/* (never refs/remotes/*, refs/pull/*, notes); HEAD's target is always kept.
        #[arg(long = "refs")]
        refs: Vec<String>,
        /// (--direct) Also publish a history pack (commits + trees) derived from the base (D18).
        #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
        history_pack: bool,
        /// (--direct) Concurrent part uploads.
        #[arg(long, default_value_t = 8)]
        parallelism: usize,
        /// (--direct) Walk the full closure of the published refs against the pack set before
        /// uploading (ref tips are always checked). `--verify-closure=false` to skip the walk.
        #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
        verify_closure: bool,
    },
    /// Keep refs of a repository on a walgit host equal to the same refs on another git host
    /// (e.g. a GitHub repository's main → this walgit), through a local bare buffer repo.
    Mirror {
        /// Source repository URL (auth: whatever your git config does for it).
        #[arg(long)]
        from: String,
        /// Destination repository URL on a walgit host (auth: a bearer token, see --identity).
        #[arg(long)]
        to: String,
        /// Local bare repository used as the buffer (created if missing; needs disk for the repo).
        #[arg(long)]
        dir: PathBuf,
        /// Ref to mirror (repeatable). `main` means `refs/heads/main`.
        #[arg(long = "ref", default_value = "refs/heads/main")]
        refs: Vec<String>,
        /// Pause between ticks (fetch, then push what moved).
        #[arg(long, default_value = "30s", value_parser = humantime::parse_duration)]
        interval: std::time::Duration,
        /// One tick, then exit (non-zero when the push failed).
        #[arg(long)]
        once: bool,
        /// Make the destination follow the source even when that is not a fast-forward.
        #[arg(long)]
        force: bool,
        /// How often to fold the buffer's small packs (`git repack --geometric=2 --write-midx`).
        #[arg(long, default_value = "1h", value_parser = humantime::parse_duration)]
        repack_every: std::time::Duration,
        /// Where the destination's bearer token comes from: `token` ($`WALGIT_TOKEN`), `gcloud` (a Google
        /// ID token for you) or `gce` (this VM's service account via the metadata server).
        #[arg(long, value_enum, default_value_t = mirror::Identity::Token)]
        identity: mirror::Identity,
    },
    /// Validate or dump the configuration.
    Config {
        #[command(subcommand)]
        action: ConfigAction,
    },
}

#[derive(Subcommand)]
enum BundleAction {
    /// Build due bundles now.
    Run {
        /// Restrict to one repo.
        #[arg(long)]
        repo: Option<String>,
        /// Restrict to one strategy name.
        #[arg(long)]
        strategy: Option<String>,
    },
    /// Print the slot table of a repository: built / missing / unavailable / wrong-host
    /// per strategy and slot, which host maintains it and whether that host is alive.
    Plan {
        /// `owner/name`.
        repo: String,
    },
    /// Publish a full bundle = header ∘ tier-2 base pack via server-side compose
    /// (no disk, no bytes through this machine). The header carries the refs at
    /// the base's WAL seq (the checkpoint there), so the bundle is exact. Run
    /// right after `walgit compact --base` (the weekly VM job).
    Compose {
        /// `owner/name`.
        repo: String,
        /// Strategy name (default: the first `kind = "full"` strategy).
        #[arg(long)]
        strategy: Option<String>,
    },
    /// Remove bundles from the list (CAS) and delete their objects: for entries
    /// whose content is wrong (2026-08-21: slots cut from "now" under old tokens).
    /// The plan then shows the slots as missing/unavailable again and the
    /// maintainer rebuilds what is buildable (D22).
    Rm {
        /// `owner/name`.
        repo: String,
        /// Bundle ids (`<strategy>-<token>`), as shown by `bundle plan`.
        #[arg(required = true)]
        ids: Vec<String>,
    },
}

#[derive(Subcommand)]
enum RepoAction {
    /// Create a new repository.
    Create {
        /// `owner/name`.
        repo: String,
        /// `sha1` or `sha256`.
        #[arg(long, default_value = "sha1")]
        object_format: String,
    },
    /// List all repositories.
    List,
    /// Show details for one repository.
    Info {
        /// `owner/name`.
        repo: String,
    },
    /// Per-repo push policy (`policy.json` in the bucket).
    Policy {
        #[command(subcommand)]
        action: PolicyAction,
    },
    /// Per-repo settings in the WAL (D24): TOML overrides of [bundles],
    /// [maintenance], [compaction] on top of the host config.
    Settings {
        #[command(subcommand)]
        action: SettingsAction,
    },
}

#[derive(Subcommand)]
enum SettingsAction {
    /// Print the settings document (and revision/author), or "(none)".
    Show {
        /// `owner/name`.
        repo: String,
        /// Print the effective config (host ⊕ settings) as TOML instead.
        #[arg(long)]
        effective: bool,
    },
    /// Replace the settings document (validated; invalid = nothing published).
    Set {
        /// `owner/name`.
        repo: String,
        /// TOML file with the overrides (`-` = stdin). Empty file clears.
        #[arg(long)]
        file: std::path::PathBuf,
        /// Reason, shown in the history.
        #[arg(short, long, default_value = "")]
        message: String,
    },
    /// Clear the settings (back to the host config).
    Clear {
        /// `owner/name`.
        repo: String,
    },
    /// Settings history from the WAL (SETTINGS entries).
    History {
        /// `owner/name`.
        repo: String,
    },
}

#[derive(Subcommand)]
enum PolicyAction {
    /// Print the policy (empty document if none is set).
    Get {
        /// `owner/name`.
        repo: String,
    },
    /// Replace the policy from a JSON file.
    Set {
        /// `owner/name`.
        repo: String,
        /// Path to a policy.json document.
        #[arg(long)]
        file: PathBuf,
    },
    /// Delete the policy (back to allow-all).
    Clear {
        /// `owner/name`.
        repo: String,
    },
}

#[derive(Subcommand)]
enum WalAction {
    /// List WAL entries for a repo.
    Ls {
        /// `owner/name`.
        repo: String,
        #[arg(long)]
        from: Option<u64>,
        #[arg(long)]
        to: Option<u64>,
    },
    /// Show one WAL entry.
    Show {
        /// `owner/name`.
        repo: String,
        /// Sequence number.
        seq: u64,
    },
    /// Materialize the repo at a historical sequence into a fresh directory.
    Materialize {
        /// `owner/name`.
        repo: String,
        #[arg(long)]
        at_seq: u64,
        #[arg(long)]
        out: PathBuf,
    },
    /// Publish an already built pack (`pack-<checksum>.pack` + `.idx`) as a tier-2 COMPACT
    /// entry superseding nothing — e.g. a history pack (`--history-of <base checksum>`, D18)
    /// built from a mirror for a base imported before history packs existed.
    AddPack {
        /// `owner/name`.
        repo: String,
        /// Path to `pack-<checksum>.pack` (the `.idx` must sit next to it).
        pack: PathBuf,
        /// Mark as the history pack (commits + trees) of this base pack checksum.
        #[arg(long)]
        history_of: Option<String>,
        #[arg(long, default_value_t = 2)]
        tier: u32,
    },
    /// Attach side-files to a published pack (uploaded immutable, manifest CAS'd to
    /// advertise them): retrofit a commit-graph layer / rev / bitmap onto a base that
    /// was imported before they existed.
    AnnotatePack {
        /// `owner/name`.
        repo: String,
        /// Pack checksum (hex).
        checksum: String,
        /// A split commit-graph layer file (`graph-<hash>.graph`) covering the pack's commits.
        #[arg(long)]
        commit_graph: Option<PathBuf>,
        #[arg(long)]
        rev: Option<PathBuf>,
        #[arg(long)]
        bitmap: Option<PathBuf>,
    },
    /// Derive a pack's reverse index (`.rev`) from its `.idx` alone — seconds for
    /// a 32 GB pack (`git index-pack --rev-index` re-reads the whole pack: hours).
    /// Byte-identical to git's. Feed the result to `annotate-pack --rev`.
    RevIndex {
        /// `pack-<sha>.idx` (the `.rev` is written next to it unless --out).
        idx: PathBuf,
        #[arg(long)]
        out: Option<PathBuf>,
    },
}

#[derive(Subcommand)]
enum ConfigAction {
    /// Parse and validate the config file; print OK or the error.
    Check {
        /// Also apply `WALGIT__*` overrides from these env files (KEY=VALUE
        /// lines; `#` comments) on top of the process env, report every key
        /// this build ignores, and exit 3 when `--strict` and any was ignored.
        /// Useful in a process supervisor's pre-start check to validate an env
        /// file against the binary that will actually run.
        #[arg(long = "env-file")]
        env_files: Vec<std::path::PathBuf>,
        /// Exit 3 if any override was ignored (unknown in this build).
        #[arg(long)]
        strict: bool,
    },
    /// Print the effective config as TOML.
    Dump,
}

#[derive(clap::ValueEnum, Clone, Copy, Debug)]
enum SynthSize {
    /// 50 commits, 200 files.
    S,
    /// 2k commits, 5k files, binary blobs, 20 branches, 50 tags.
    M,
    /// 50k commits, 50k files.
    L,
}

fn load_config(path: &std::path::Path) -> Config {
    if path.exists() {
        match Config::load(path) {
            Ok(c) => c,
            Err(e) => {
                eprintln!(
                    "walgit: error loading config from {}: {e:#}",
                    path.display()
                );
                std::process::exit(1);
            }
        }
    } else {
        // Never run against defaults by accident: a typo'd --config would open the
        // default bucket with whatever credentials are around. Defaults + WALGIT__
        // env on purpose: `--config /dev/null`.
        eprintln!(
            "walgit: config file {} not found (pass --config PATH or WALGIT_CONFIG; `--config /dev/null` for defaults + WALGIT__ env)",
            path.display()
        );
        std::process::exit(2);
    }
}

pub fn main() -> Result<()> {
    let cli = Cli::parse();
    run(&cli.config, cli.command.unwrap_or(Command::Serve))
}

pub fn main_server() -> Result<()> {
    let cli = ServerCli::parse();
    run(&cli.config, Command::Serve)
}

fn run(config: &std::path::Path, command: Command) -> Result<()> {
    // Install the rustls crypto provider before any TLS code runs (GCS gRPC, reqwest).
    // Required for rustls 0.23+ — multiple providers in the dep tree; select one.
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("install rustls aws_lc_rs provider");

    let cfg = load_config(config);
    tracing_init(&cfg);

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    rt.block_on(async move { dispatch(command, cfg).await })
}

async fn dispatch(command: Command, cfg: Config) -> Result<()> {
    let cfg = std::sync::Arc::new(cfg);
    match command {
        Command::Config { action } => config_cmd::run(action, &cfg).await,
        Command::Synth {
            out,
            size,
            commits,
            files,
            seed,
        } => synth::run(out, size, commits, files, seed).await,
        Command::Serve => serve::run(&cfg).await,
        Command::Compact {
            repo,
            all,
            once,
            base,
        } => compact::run(repo, all, once, base, &cfg).await,
        Command::Bundle { action } => bundle_cmd::run(action, &cfg).await,
        Command::Repo { action } => repo::run(action, &cfg).await,
        Command::Wal { action } => wal_cmd::run(action, &cfg).await,
        Command::Mirror {
            from,
            to,
            dir,
            refs,
            interval,
            once,
            force,
            repack_every,
            identity,
        } => {
            mirror::run(mirror::MirrorArgs {
                from,
                to,
                dir,
                refs,
                interval,
                once,
                force,
                repack_every,
                identity,
            })
            .await
        }
        Command::Import {
            from,
            repo,
            reuse_packs,
            direct,
            packs,
            bundle,
            bundle_strategy,
            replace,
            force,
            parallelism,
            commit_graph,
            refs,
            history_pack,
            verify_closure,
        } => {
            if direct {
                import_direct::run(
                    import_direct::DirectOptions {
                        from,
                        repo,
                        packs,
                        bundle,
                        bundle_strategy,
                        replace,
                        parallelism,
                        commit_graph,
                        refs,
                        history_pack,
                        verify_closure,
                    },
                    &cfg,
                    force,
                )
                .await
            } else {
                import::run(from, repo, reuse_packs, refs, &cfg).await
            }
        }
    }
}
