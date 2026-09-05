//! `walgit compact [REPO|--all] [--once]` — trigger compaction manually.
//! Shares the decision/lease/repack/publish logic with the serve loop and the
//! web UI (`walgit_server::ops::compact_repo`).

use std::sync::Arc;

use anyhow::{Result, bail};
use tracing::{info, warn};

use walgit_config::Config;
use walgit_server::ops::{CompactRequest, compact_repo};
use walgit_store::open_store;
use walgit_wal::Registry;

use crate::cli::parse_repo_id;

pub async fn run(
    repo: Option<String>,
    all: bool,
    once: bool,
    base: bool,
    cfg: &Arc<Config>,
) -> Result<()> {
    if !cfg.compaction.enabled {
        bail!("compaction is disabled in config");
    }

    let store = open_store(cfg).await?;
    std::fs::create_dir_all(&cfg.cache.dir).ok();
    let registry = Registry::new(store, cfg.clone());

    let target_repos: Vec<walgit_git::RepoId> = if all {
        registry.list().await?
    } else if let Some(r) = repo {
        let (owner, name) = parse_repo_id(&r)?;
        vec![walgit_git::RepoId::new(owner, name)?]
    } else {
        bail!("specify a repo or --all");
    };

    if base {
        anyhow::ensure!(once || !all, "--base runs once per repo: pass --once");
    }
    loop {
        for id in &target_repos {
            match compact_one(&registry, id, cfg, base).await {
                Ok(summary) => println!("{id}: {summary}"),
                Err(e) => warn!(repo = %id, error = %e, "compaction failed"),
            }
        }
        if once {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_mins(1)).await;
    }
    Ok(())
}

async fn compact_one(
    registry: &Registry,
    id: &walgit_git::RepoId,
    cfg: &Config,
    base: bool,
) -> Result<String> {
    let handle = registry.open(id).await?;
    let log = |line: String| {
        info!(repo = %id, "{line}");
        println!("{id}: {line}");
    };
    let outcome = compact_repo(
        &handle,
        cfg,
        CompactRequest {
            force: base,
            rebuild_base: base,
        },
        &log,
    )
    .await?;
    let mut summary = outcome.summary();
    if base {
        // The weekly bundle is composed from this base with the refs at its
        // seq: write the checkpoint now so `walgit bundle compose` finds them.
        let cp = handle.write_checkpoint().await?;
        {
            let _ = std::fmt::Write::write_fmt(
                &mut summary,
                format_args!("; checkpoint at seq {}", cp.seq),
            );
        };
    }
    Ok(summary)
}
