//! `walgit serve` — run the HTTP server with optional compaction/bundle loops.
//!
//! Opens the object store from config, builds `AppState` (which constructs the
//! WAL registry, bundler, authenticator, semaphores, and metrics), then calls
//! `walgit_server::serve`. When the instance's roles include `compact` and/or
//! `bundle`, background loops are spawned:
//!
//!   * **compact loop** — every 60s, for every repo in `registry.list()`, if
//!     the compaction trigger is met (tier-0 packs ≥ `trigger_packs` or bytes
//!     ≥ `trigger_bytes`) and the compaction lease can be acquired, run
//!     `LocalRepo::repack(geometric)` → `RepoHandle::publish_compact`.
//!   * **bundle loop** — every 60s, `Bundler::run_all_due`.
//!   * **maintain** role — `walgit_server::maintain::run_loop`: checkpoint-if-due
//!     (refs-level), bundles-if-due and geometric compaction for every repo, each
//!     as a task. It subsumes the two loops above (they are skipped when the
//!     instance is a maintainer so work is not done twice).

use std::sync::Arc;

use anyhow::Result;
use tokio::signal;
use tracing::{info, warn};

use walgit_config::{Config, Role};
use walgit_server::{AppState, serve};
use walgit_store::open_store;

pub async fn run(cfg: &Arc<Config>) -> Result<()> {
    info!(backend = ?cfg.store.backend, "opening store");
    let store = open_store(cfg).await?;
    info!(backend = store.backend(), "store ready");

    // Ensure the cache directory exists.
    std::fs::create_dir_all(&cfg.cache.dir).ok();

    // AppState::new constructs the registry, bundler, auth, semaphores, metrics.
    let state = AppState::new(cfg.clone(), store).await?;

    // Spawn background loops for non-serving roles.
    let mut bg_handles = Vec::new();

    let maintainer = cfg.has_role(Role::Maintain);
    if maintainer {
        let st = state.clone();
        bg_handles.push(tokio::spawn(async move {
            walgit_server::maintain::run_loop(st).await;
        }));
        // Ingress from upstream hosts (`[upstream] follow`): its own loop, so a long
        // maintenance unit never delays it (D28: the maintaining host is the writer).
        let st = state.clone();
        bg_handles.push(tokio::spawn(async move {
            walgit_server::follow::run_loop(st).await;
        }));
    }

    if !maintainer && cfg.has_role(Role::Compact) {
        let reg = state.registry.clone();
        let c = cfg.clone();
        bg_handles.push(tokio::spawn(async move {
            compact_loop(reg, c).await;
        }));
    }

    if !maintainer && cfg.has_role(Role::Bundle) {
        let b = state.bundles.clone();
        let c = cfg.clone();
        bg_handles.push(tokio::spawn(async move {
            bundle_loop(b, c).await;
        }));
    }

    // Graceful shutdown on SIGTERM / SIGINT.
    let shutdown = async {
        #[cfg(unix)]
        {
            let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())
                .expect("install SIGTERM handler");
            let mut sigint = signal::unix::signal(signal::unix::SignalKind::interrupt())
                .expect("install SIGINT handler");
            tokio::select! {
                _ = sigterm.recv() => info!("received SIGTERM, shutting down"),
                _ = sigint.recv() => info!("received SIGINT, shutting down"),
            }
        }
        #[cfg(not(unix))]
        {
            signal::ctrl_c().await.expect("ctrl_c");
            info!("received Ctrl-C, shutting down");
        }
    };

    info!(listen = %cfg.server.listen, "starting server");
    serve(state, shutdown).await?;

    // Cancel background loops.
    for h in bg_handles {
        h.abort();
    }

    Ok(())
}

/// Compaction loop: every 60s, check each repo for compaction triggers.
async fn compact_loop(registry: Arc<walgit_wal::Registry>, cfg: Arc<Config>) {
    if !cfg.compaction.enabled {
        info!("compaction disabled by config, loop exiting");
        return;
    }
    let interval = std::time::Duration::from_mins(1);
    loop {
        tokio::time::sleep(interval).await;
        if let Err(e) = run_compaction_pass(&registry, &cfg).await {
            warn!(error = %e, "compaction pass failed");
        }
    }
}

async fn run_compaction_pass(registry: &walgit_wal::Registry, cfg: &Config) -> anyhow::Result<()> {
    let repos = registry.list().await?;
    for id in repos {
        let handle = match registry.open(&id).await {
            Ok(h) => h,
            Err(e) => {
                warn!(repo = %id, error = %e, "failed to open repo for compaction");
                continue;
            }
        };
        // Repositories whose pack set cannot live on this instance are compacted
        // on a larger or SSD-backed maintainer; skip quietly every pass.
        if !handle.packs_fit() {
            continue;
        }
        let log = |line: String| info!(repo = %id, "{line}");
        match walgit_server::ops::compact_repo(
            &handle,
            cfg,
            walgit_server::ops::CompactRequest::default(),
            &log,
        )
        .await
        {
            Ok(outcome) => info!(repo = %id, "{}", outcome.summary()),
            Err(e) => warn!(repo = %id, error = %e, "compaction failed"),
        }
    }
    Ok(())
}

/// Bundle loop: every 60s, run all due bundle strategies.
async fn bundle_loop(bundler: Arc<walgit_bundle::Bundler>, cfg: Arc<Config>) {
    if !cfg.bundles.enabled {
        info!("bundles disabled by config, loop exiting");
        return;
    }
    let interval = std::time::Duration::from_mins(1);
    loop {
        tokio::time::sleep(interval).await;
        let now = std::time::SystemTime::now();
        if let Err(e) = bundler.run_all_due(now).await {
            warn!(error = %e, "bundle pass failed");
        }
    }
}
