//! `walgit wal ls|show|materialize` — WAL inspection and rewind.

use std::sync::Arc;

use anyhow::{Result, bail};

use walgit_config::Config;
use walgit_store::open_store;
use walgit_wal::Registry;

use crate::WalAction;
use crate::cli::{parse_repo_id, println_kv};

pub async fn run(action: WalAction, cfg: &Arc<Config>) -> Result<()> {
    let store = open_store(cfg).await?;
    std::fs::create_dir_all(&cfg.cache.dir).ok();
    let registry = Registry::new(store, cfg.clone());

    match action {
        WalAction::Ls { repo, from, to } => {
            let (owner, name) = parse_repo_id(&repo)?;
            let id = walgit_git::RepoId::new(owner, name)?;
            let handle = registry.open(&id).await?;
            let entries = handle.read_log(from.unwrap_or(0), to).await?;

            if entries.is_empty() {
                println!("(no WAL entries)");
                return Ok(());
            }

            println!(
                "{:<6} {:<10} {:<12} {:<10} {}",
                "seq", "kind", "pack", "supersedes", "refs"
            );
            for e in &entries {
                let kind = format!("{:?}", e.kind);
                let pack = e
                    .pack
                    .as_ref()
                    .map(|p| p.checksum[..12].to_string())
                    .unwrap_or_default();
                let supersedes = e.supersedes.len();
                let ref_count = e.txn.as_ref().map(|t| t.updates.len()).unwrap_or(0);
                println!(
                    "{:<6} {:<10} {:<12} {:<10} {}",
                    e.seq, kind, pack, supersedes, ref_count
                );
            }
        }
        WalAction::AddPack {
            repo,
            pack,
            history_of,
            tier,
        } => {
            let (owner, name) = parse_repo_id(&repo)?;
            let id = walgit_git::RepoId::new(owner, name)?;
            let idx = pack.with_extension("idx");
            anyhow::ensure!(
                pack.is_file() && idx.is_file(),
                "need {} and {}",
                pack.display(),
                idx.display()
            );
            let handle = registry.open(&id).await?;
            if let Some(b) = &history_of {
                anyhow::ensure!(
                    handle
                        .manifest()
                        .packs
                        .iter()
                        .any(|p| &p.checksum == b && p.tier == 2),
                    "--history-of {b} is not a live tier-2 pack of {id}"
                );
            }
            let t = std::time::Instant::now();
            let seq = handle
                .add_pack(&pack, &idx, tier, history_of.clone())
                .await?;
            println!(
                "published {} as tier {tier}{} at seq {seq} in {:.1}s",
                pack.display(),
                history_of
                    .map(|b| format!(" (history pack of {b})"))
                    .unwrap_or_default(),
                t.elapsed().as_secs_f64()
            );
        }
        WalAction::RevIndex { idx, out } => {
            let out = out.unwrap_or_else(|| idx.with_extension("rev"));
            let t0 = std::time::Instant::now();
            walgit_git::write_rev_from_idx(&idx, &out, gix_hash::Kind::Sha1)?;
            println!(
                "{} ({} bytes) in {:.1}s",
                out.display(),
                std::fs::metadata(&out).map(|m| m.len()).unwrap_or(0),
                t0.elapsed().as_secs_f64()
            );
        }
        WalAction::AnnotatePack {
            repo,
            checksum,
            commit_graph,
            rev,
            bitmap,
        } => {
            let (owner, name) = parse_repo_id(&repo)?;
            let id = walgit_git::RepoId::new(owner, name)?;
            if commit_graph.is_none() && rev.is_none() && bitmap.is_none() {
                bail!("nothing to attach: pass --commit-graph / --rev / --bitmap");
            }
            if let Some(g) = &commit_graph {
                let head = std::fs::read(g)?;
                anyhow::ensure!(
                    head.len() > 8 && &head[..4] == b"CGPH",
                    "{} is not a commit-graph file",
                    g.display()
                );
            }
            let handle = registry.open(&id).await?;
            let p = handle
                .annotate_pack(&checksum, rev, bitmap, commit_graph)
                .await?;
            println!(
                "pack {} now advertises rev={} bitmap={} commit_graph={} (manifest revision {})",
                p.checksum,
                p.has_rev,
                p.has_bitmap,
                p.has_commit_graph,
                handle.manifest().revision
            );
        }
        WalAction::Show { repo, seq } => {
            let (owner, name) = parse_repo_id(&repo)?;
            let id = walgit_git::RepoId::new(owner, name)?;
            let handle = registry.open(&id).await?;
            let entries = handle.read_log(seq, Some(seq)).await?;

            let entry = entries
                .into_iter()
                .find(|e| e.seq == seq)
                .ok_or_else(|| anyhow::anyhow!("no WAL entry at seq {seq}"))?;

            println_kv("seq", entry.seq);
            println_kv("kind", format!("{:?}", entry.kind));
            println_kv("writer", &entry.writer);
            println_kv(
                "created_at",
                &entry
                    .created_at
                    .as_ref()
                    .map(|t| {
                        humantime::format_rfc3339_seconds(walgit_proto::time::to_system(t))
                            .to_string()
                    })
                    .unwrap_or_else(|| "(none — predates the field)".into()),
            );

            if let Some(pack) = &entry.pack {
                println_kv("pack_checksum", &pack.checksum);
                println_kv("pack_size", pack.pack_size);
                println_kv("pack_objects", pack.object_count);
                println_kv("pack_tier", pack.tier);
            }

            if !entry.supersedes.is_empty() {
                println!("supersedes:");
                for s in &entry.supersedes {
                    println!("  {s}");
                }
            }

            if let Some(txn) = &entry.txn {
                println!("ref_updates:");
                for u in &txn.updates {
                    println!("  {} {} -> {}", u.name, u.old_oid, u.new_oid);
                }
                if !txn.push_options.is_empty() {
                    println!("push_options: {:?}", txn.push_options);
                }
                println_kv("atomic", txn.atomic);
            }

            if let Some(cp) = &entry.checkpoint {
                println_kv("checkpoint_seq", cp.seq);
                println_kv("checkpoint_key", &cp.key);
            }

            if !entry.meta.is_empty() {
                println!("meta:");
                for (k, v) in &entry.meta {
                    println!("  {k} = {v}");
                }
            }
        }
        WalAction::Materialize { repo, at_seq, out } => {
            let (owner, name) = parse_repo_id(&repo)?;
            let id = walgit_git::RepoId::new(owner, name)?;
            let snap = walgit_wal::materialize_at(
                registry,
                id,
                at_seq,
                out,
                Arc::new(|line| println!("{line}")),
            )
            .await?;
            println_kv("git_dir", &snap.git_dir);
            println_kv("refs", snap.ref_count);
            println_kv("packs", snap.packs.len());
            println_kv("replayed_entries", snap.entries);
            println_kv("wal_head_seq", snap.head_seq);
        }
    }
    Ok(())
}
