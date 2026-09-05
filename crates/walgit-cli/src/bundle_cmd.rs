//! `walgit bundle run [--repo] [--strategy]` — build due bundles.
//! `walgit bundle compose <repo> [--strategy]` — full bundle = header ∘ base pack (compose).

use std::sync::Arc;

use anyhow::{Result, bail};
use tracing::info;

use walgit_bundle::Bundler;
use walgit_config::Config;
use walgit_store::open_store;
use walgit_wal::Registry;

use crate::BundleAction;
use crate::cli::parse_repo_id;

pub async fn run(action: BundleAction, cfg: &Arc<Config>) -> Result<()> {
    if !cfg.bundles.enabled {
        bail!("bundles are disabled in config");
    }

    let store = open_store(cfg).await?;
    let store_root = store.clone();
    std::fs::create_dir_all(&cfg.cache.dir).ok();
    let registry = Registry::new(store, cfg.clone());
    let bundler = Bundler::new(registry.clone(), cfg.clone());

    match action {
        BundleAction::Run { repo, strategy } => {
            if let Some(r) = repo {
                let (owner, name) = parse_repo_id(&r)?;
                let id = walgit_git::RepoId::new(owner, name)?;

                if let Some(s) = strategy {
                    let entry = bundler.build(&id, &s).await?;
                    info!(repo = %id, strategy = %s, bundle = ?entry, "bundle built");
                    println!("bundle built: {} strategy={} size={}", id, s, entry.size);
                } else {
                    let now = std::time::SystemTime::now();
                    let entries = bundler.run_due(&id, now).await?;
                    println!("built {} bundle(s) for {}", entries.len(), id);
                    for e in &entries {
                        println!("  strategy={} size={}", e.strategy, e.size);
                    }
                }
            } else {
                let now = std::time::SystemTime::now();
                bundler.run_all_due(now).await?;
                println!("bundle pass complete");
            }
        }
        BundleAction::Plan { repo } => {
            let (owner, name) = parse_repo_id(&repo)?;
            let id = walgit_git::RepoId::new(owner, name)?;
            let handle = registry.open(&id).await?;
            drop(handle.sync_refs().await?);
            // This process is not a maintainer: plan with "unknown host" capacity
            // (can do everything) so the table shows built/missing/unavailable.
            let ctx = walgit_bundle::slots::PlanContext {
                first_state: handle.first_state_time(),
                can_full: true,
                can_incremental: true,
                wrong_host_reason: None,
            };
            let rows = bundler.plan(&id, std::time::SystemTime::now(), ctx).await?;
            {
                let m = handle.manifest();
                let fmt = |t: Option<std::time::SystemTime>| {
                    t.map_or_else(
                        || "-".into(),
                        |t| humantime::format_rfc3339_seconds(t).to_string(),
                    )
                };
                let cp = m.checkpoint.as_ref();
                println!(
                    "first state {}  (checkpoint seq {} created {} first_state_at {} as_of {}; head seq {})",
                    fmt(handle.first_state_time()),
                    cp.map_or(0, |c| c.seq),
                    fmt(cp
                        .and_then(|c| c.created_at.as_ref())
                        .map(walgit_proto::time::to_system)),
                    fmt(cp
                        .and_then(|c| c.first_state_at.as_ref())
                        .map(walgit_proto::time::to_system)),
                    fmt(cp
                        .and_then(|c| c.as_of.as_ref())
                        .map(walgit_proto::time::to_system)),
                    m.head_seq
                );
            }
            println!(
                "{:<8} {:<12} {:<20} status",
                "strategy", "kind", "slot (UTC)"
            );
            for r in &rows {
                let when = if r.slot == 0 {
                    "-".to_string()
                } else {
                    chrono::DateTime::<chrono::Utc>::from(walgit_bundle::slots::from_epoch(r.slot))
                        .format("%Y-%m-%d %H:%M")
                        .to_string()
                };
                let status = match &r.status {
                    walgit_bundle::slots::SlotStatus::Built { id, size, seq } => {
                        format!("built {id} ({size} bytes, seq {seq})")
                    }
                    walgit_bundle::slots::SlotStatus::Missing => "MISSING".into(),
                    walgit_bundle::slots::SlotStatus::Pending => {
                        "pending (inside the close grace)".into()
                    }
                    walgit_bundle::slots::SlotStatus::Blocked(why) => format!("blocked: {why}"),
                    walgit_bundle::slots::SlotStatus::Unavailable => {
                        "unavailable (no WAL state at that time)".into()
                    }
                    walgit_bundle::slots::SlotStatus::TooSmall { commits, min } => format!(
                        "too-small ({commits} commits since base, min {min}; next slot catches up)"
                    ),
                    walgit_bundle::slots::SlotStatus::Skipped { reason } => {
                        format!("skipped ({reason}; recorded in the list)")
                    }
                    walgit_bundle::slots::SlotStatus::WrongHost(why) => {
                        format!("wrong-host: {why}")
                    }
                };
                println!(
                    "{:<8} {:<12} {:<20} {}",
                    r.strategy,
                    format!("{:?}", r.kind).to_lowercase(),
                    when,
                    status
                );
            }
            // Who maintains it, and what the next slot of each strategy will run.
            let hbs = maintainers(&store_root).await?;
            let up = walgit_server::maintain::upcoming(
                &handle,
                &handle.effective_config(),
                &hbs,
                std::time::SystemTime::now(),
            )
            .await;
            if !up.is_empty() {
                println!("\nnext:");
                for u in &up {
                    let when = chrono::DateTime::<chrono::Utc>::from(
                        walgit_bundle::slots::from_epoch(u.slot),
                    )
                    .format("%Y-%m-%d %H:%M")
                    .to_string();
                    println!(
                        "  {:<8} {:<17} → {}{}",
                        u.strategy,
                        when,
                        u.unit,
                        u.host.as_deref().map_or_else(
                            || "  [no live maintainer]".into(),
                            |h| if u.unit.contains(h) {
                                String::new()
                            } else {
                                format!("  [{h}]")
                            }
                        )
                    );
                }
            }
            let mine: Vec<_> = hbs
                .iter()
                .filter(|h| {
                    walgit_config::repo_listed(&h.repos, id.owner(), id.name())
                        && !walgit_config::repo_listed(&h.exclude, id.owner(), id.name())
                })
                .collect();
            if mine.is_empty() {
                println!(
                    "\nmaintainers: NOBODY maintains {id} (no heartbeat with a matching assignment)"
                );
            } else {
                println!("\nmaintainers:");
                for h in mine {
                    let last = h.last_pass_at.as_ref().map(walgit_proto::time::to_system);
                    let age = last
                        .and_then(|t| std::time::SystemTime::now().duration_since(t).ok())
                        .map_or(u64::MAX, |d| d.as_secs());
                    println!(
                        "  {} ({}, {} cap) — last pass {}s ago ({}), {} passes, last unit: {}",
                        h.host,
                        h.disk,
                        h.max_pack_bytes,
                        if age == u64::MAX {
                            "?".into()
                        } else {
                            age.to_string()
                        },
                        if age < 600 { "alive" } else { "STALE" },
                        h.passes,
                        if h.last_unit.is_empty() {
                            "-"
                        } else {
                            &h.last_unit
                        }
                    );
                }
            }
        }
        BundleAction::Rm { repo, ids } => {
            let (owner, name) = parse_repo_id(&repo)?;
            let id = walgit_git::RepoId::new(owner, name)?;
            let handle = registry.open(&id).await?;
            let store = handle.store().clone();
            let wanted: std::collections::HashSet<&str> = ids.iter().map(String::as_str).collect();
            let mut removed: Vec<walgit_proto::v1::BundleEntry> = Vec::new();
            let res = walgit_bundle::ops::cas_update_list(&store, 8, |cur| {
                let Some(cur) = cur else { return Ok(None) };
                let mut next = cur.clone();
                removed = cur
                    .bundles
                    .iter()
                    .filter(|b| wanted.contains(b.id.as_str()))
                    .cloned()
                    .collect();
                if removed.is_empty() {
                    return Ok(None);
                }
                next.bundles.retain(|b| !wanted.contains(b.id.as_str()));
                next.updated_at = Some(walgit_proto::time::now());
                Ok(Some(next))
            })
            .await?;
            anyhow::ensure!(res.is_some(), "none of {ids:?} is in the list");
            for b in &removed {
                match walgit_store::ObjectStore::delete(&store, &b.key, None).await {
                    Ok(()) => println!(
                        "removed {} ({} bytes, token {}, slot {}) and deleted {}",
                        b.id, b.size, b.creation_token, b.slot, b.key
                    ),
                    Err(e) => println!(
                        "removed {} from the list; deleting {} failed: {e}",
                        b.id, b.key
                    ),
                }
            }
            let missing: Vec<&str> = ids
                .iter()
                .map(String::as_str)
                .filter(|i| !removed.iter().any(|b| b.id == *i))
                .collect();
            if !missing.is_empty() {
                println!("not in the list: {}", missing.join(", "));
            }
        }
        BundleAction::Compose { repo, strategy } => {
            let (owner, name) = parse_repo_id(&repo)?;
            let id = walgit_git::RepoId::new(owner, name)?;
            let strategy = match strategy {
                Some(s) => s,
                None => cfg
                    .bundles
                    .strategy
                    .iter()
                    .find(|s| s.kind == walgit_config::BundleKind::Full)
                    .map(|s| s.name.clone())
                    .ok_or_else(|| anyhow::anyhow!("no full bundle strategy configured"))?,
            };
            let entry = compose_full_from_base(&registry, &id, &strategy, cfg).await?;
            println!(
                "composed full bundle {} for {id}: {} bytes at seq {}, creationToken {}, {} tips",
                entry.key,
                entry.size,
                entry.seq,
                entry.creation_token,
                entry.tips.len()
            );
        }
    }
    Ok(())
}

/// Every maintainer heartbeat in the bucket (`maintain/<host>.pb`).
pub async fn maintainers(
    store: &walgit_store::DynStore,
) -> Result<Vec<walgit_proto::v1::MaintainerHeartbeat>> {
    use futures::StreamExt;
    use walgit_proto::prost::Message;
    use walgit_store::ObjectStoreExt;
    let mut out = Vec::new();
    let mut keys = store.list(walgit_proto::keys::MAINTAIN_DIR, None);
    while let Some(m) = keys.next().await {
        let m = m?;
        if let Some((_, bytes)) = store.get_bytes(&m.key).await?
            && let Ok(hb) = walgit_proto::v1::MaintainerHeartbeat::decode(bytes.as_ref())
        {
            out.push(hb);
        }
    }
    Ok(out)
}

/// Moved to `walgit_server::bundles::compose_full_from_base` (the maintainer's
/// weekly unit uses it too); the CLI delegates.
pub async fn compose_full_from_base(
    registry: &Registry,
    id: &walgit_git::RepoId,
    strategy: &str,
    cfg: &Config,
) -> Result<walgit_proto::v1::BundleEntry> {
    let now = std::time::SystemTime::now();
    let slot = cfg
        .bundles
        .strategy
        .iter()
        .find(|s| s.name == strategy)
        .and_then(|s| {
            walgit_bundle::slots::last_slot_at_or_before(s, now)
                .ok()
                .flatten()
        })
        .unwrap_or(0);
    walgit_server::bundles::compose_full_from_base(registry, id, strategy, cfg, slot).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use walgit_store::ObjectStoreExt;

    fn run_git(dir: &std::path::Path, args: &[&str]) -> String {
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

    /// Full bundle composed from the base pack with the refs at the base's
    /// seq: a later push does not leak into the header; the object is a valid
    /// bundle (`git bundle verify`) listing the base tip.
    #[tokio::test]
    async fn compose_full_uses_refs_at_base_seq() {
        let cache = tempfile::tempdir().unwrap();
        let store = walgit_store::memory::MemoryStore::shared();
        let mut cfg = Config::default();
        cfg.cache.dir = cache.path().to_path_buf();
        cfg.store.bucket = "test".into();
        cfg.wal.fsck_objects = false;
        cfg.wal.check_connectivity = false;
        cfg.wal.freshness_ttl = std::time::Duration::ZERO;
        cfg.wal.snapshot_every_entries = 0;
        cfg.wal.checkpoint_interval = std::time::Duration::ZERO;
        cfg.wal.checkpoint_tail_bytes = walgit_config::ByteSize::b(0);
        let cfg = Arc::new(cfg);
        let registry = Registry::new(store.clone(), cfg.clone());
        let id = walgit_git::RepoId::new("t", "compose").unwrap();
        let handle = registry
            .create(&id, walgit_git::ObjectFormat::Sha1)
            .await
            .unwrap();

        let work = tempfile::tempdir().unwrap();
        run_git(work.path(), &["init", "-q", "-b", "main"]);
        run_git(work.path(), &["config", "user.email", "t@t"]);
        run_git(work.path(), &["config", "user.name", "t"]);
        let mut prev = String::new();
        for i in 0..3 {
            std::fs::write(work.path().join(format!("f{i}")), format!("{i}\n")).unwrap();
            run_git(work.path(), &["add", "."]);
            run_git(work.path(), &["commit", "-q", "-m", &format!("c{i}")]);
            let c = run_git(work.path(), &["rev-parse", "HEAD"]);
            let out = std::process::Command::new("sh")
                .arg("-c")
                .arg(format!(
                    "git rev-list --objects {c} {} | git pack-objects --stdout",
                    if prev.is_empty() {
                        String::new()
                    } else {
                        format!("^{prev}")
                    }
                ))
                .current_dir(work.path())
                .output()
                .unwrap();
            let ingested = handle
                .local()
                .ingest_pack(
                    std::io::Cursor::new(out.stdout),
                    walgit_git::IngestOptions {
                        fsck: false,
                        max_bytes: None,
                        thin: false,
                    },
                )
                .await
                .unwrap()
                .unwrap();
            handle
                .publish_push(
                    Some(ingested),
                    walgit_proto::v1::RefTransaction {
                        updates: vec![walgit_proto::v1::RefUpdate {
                            name: "refs/heads/main".into(),
                            old_oid: prev.clone(),
                            new_oid: c.clone(),
                            new_symbolic_target: String::new(),
                            new_peeled: String::new(),
                        }],
                        push_options: vec![],
                        atomic: true,
                    },
                    HashMap::new(),
                )
                .await
                .unwrap();
            prev = c;
        }
        let base_tip = prev.clone();
        let repack = handle
            .local()
            .repack(walgit_git::RepackOptions {
                mode: walgit_git::RepackMode::Full,
                write_bitmap: true,
                write_midx: false,
                keep: vec![],
            })
            .await
            .unwrap();
        let base = repack.new_packs[0].clone();
        let base_seq = handle
            .publish_compact(base.clone(), repack.removed.clone(), 2)
            .await
            .unwrap();
        // Checkpoint at the base seq (what the VM job does right after compact --base).
        let cp = handle.write_checkpoint().await.unwrap();
        assert_eq!(cp.seq, base_seq);
        // A later push moves main past the base.
        std::fs::write(work.path().join("later"), "later\n").unwrap();
        run_git(work.path(), &["add", "."]);
        run_git(work.path(), &["commit", "-q", "-m", "later"]);
        let later = run_git(work.path(), &["rev-parse", "HEAD"]);
        let out = std::process::Command::new("sh")
            .arg("-c")
            .arg(format!(
                "git rev-list --objects {later} ^{base_tip} | git pack-objects --stdout"
            ))
            .current_dir(work.path())
            .output()
            .unwrap();
        let ingested = handle
            .local()
            .ingest_pack(
                std::io::Cursor::new(out.stdout),
                walgit_git::IngestOptions {
                    fsck: false,
                    max_bytes: None,
                    thin: false,
                },
            )
            .await
            .unwrap()
            .unwrap();
        handle
            .publish_push(
                Some(ingested),
                walgit_proto::v1::RefTransaction {
                    updates: vec![walgit_proto::v1::RefUpdate {
                        name: "refs/heads/main".into(),
                        old_oid: base_tip.clone(),
                        new_oid: later.clone(),
                        new_symbolic_target: String::new(),
                        new_peeled: String::new(),
                    }],
                    push_options: vec![],
                    atomic: true,
                },
                HashMap::new(),
            )
            .await
            .unwrap();

        let entry = compose_full_from_base(&registry, &id, "weekly", &cfg)
            .await
            .unwrap();
        assert_eq!(entry.kind, "full");
        assert_eq!(entry.seq, base_seq);
        assert!(
            entry
                .tips
                .iter()
                .any(|t| t.name == "refs/heads/main" && t.oid == base_tip),
            "{:?}",
            entry.tips
        );
        assert!(
            !entry.tips.iter().any(|t| t.oid == later),
            "later push must not be in the header"
        );
        let list = walgit_bundle::ops::read_list(handle.store())
            .await
            .unwrap()
            .unwrap();
        assert!(list.bundles.iter().any(|b| b.key == entry.key));
        // The composed object is a valid bundle.
        let (_, bytes) = handle.store().get_bytes(&entry.key).await.unwrap().unwrap();
        let f = cache.path().join("full.bundle");
        std::fs::write(&f, &bytes).unwrap();
        let clone = tempfile::tempdir().unwrap();
        run_git(clone.path(), &["init", "-q", "--bare"]);
        let v = std::process::Command::new("git")
            .current_dir(clone.path())
            .args(["bundle", "verify", f.to_str().unwrap()])
            .output()
            .unwrap();
        assert!(v.status.success(), "{}", String::from_utf8_lossy(&v.stderr));
        let heads = run_git(clone.path(), &["bundle", "list-heads", f.to_str().unwrap()]);
        assert!(
            heads.contains(&base_tip) && !heads.contains(&later),
            "{heads}"
        );
    }
}
