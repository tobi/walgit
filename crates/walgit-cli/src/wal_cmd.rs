//! `walgit wal ls|show|materialize` — WAL inspection and rewind.

use std::sync::Arc;

use anyhow::{Result, bail};
use tracing::info;

use walgit_config::Config;
use walgit_git::ObjectFormat;
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
                "{:<6} {:<10} {:<12} {:<10} refs",
                "seq", "kind", "pack", "supersedes"
            );
            for e in &entries {
                let kind = format!("{:?}", e.kind);
                let pack = e
                    .pack
                    .as_ref()
                    .map(|p| p.checksum[..12].to_string())
                    .unwrap_or_default();
                let supersedes = e.supersedes.len();
                let ref_count = e.txn.as_ref().map_or(0, |t| t.updates.len());
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
                std::fs::metadata(&out).map_or(0, |m| m.len()),
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
                entry.created_at.as_ref().map_or_else(
                    || "(none — predates the field)".into(),
                    |t| {
                        humantime::format_rfc3339_seconds(walgit_proto::time::to_system(t))
                            .to_string()
                    },
                ),
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
            materialize_at(&registry, &id, at_seq, &out).await?;
        }
    }
    Ok(())
}

/// Rebuild `id` as it was at `at_seq` into `out`: refs from the newest
/// checkpoint ≤ `at_seq` (or from seq 0) + replayed log entries, packs from the
/// local serving copy when present (copied, never moved) or fetched from the
/// store. Works on any machine with bucket access (cold rewind).
pub async fn materialize_at(
    registry: &Registry,
    id: &walgit_git::RepoId,
    at_seq: u64,
    out: &std::path::Path,
) -> Result<()> {
    use walgit_store::ObjectStoreExt;

    use walgit_proto::prost::Message;

    let handle = registry.open(id).await?;

    // Read log entries up to at_seq and replay into a fresh LocalRepo.
    if out.exists() {
        bail!("output directory {} already exists", out.display());
    }
    std::fs::create_dir_all(out)?;

    let manifest = handle.manifest();
    let format = match manifest.object_format.as_str() {
        "sha1" => ObjectFormat::Sha1,
        "sha256" => ObjectFormat::Sha256,
        other => bail!("unknown object format in manifest: {other}"),
    };

    let local = walgit_git::LocalRepo::init(out, id, format)?;

    // Start from the newest checkpoint at or before `at_seq` when the
    // log before it has been folded (min_seq), else from seq 0.
    let mut start_seq = 0u64;
    let mut pack_set: Vec<walgit_proto::v1::PackRef> = Vec::new();
    if let Some(cp) = manifest.checkpoint.as_ref().filter(|c| c.seq <= at_seq) {
        let (_, bytes) = handle
            .store()
            .get_bytes(&cp.key)
            .await?
            .ok_or_else(|| anyhow::anyhow!("checkpoint object {} missing", cp.key))?;
        let cpo = walgit_proto::v1::Checkpoint::decode(bytes.as_ref())?;
        let (_, rb) = handle
            .store()
            .get_bytes(&cpo.refs_key)
            .await?
            .ok_or_else(|| anyhow::anyhow!("checkpoint refs {} missing", cpo.refs_key))?;
        let snap = walgit_proto::v1::RefSnapshot::decode(rb.as_ref())?;
        local.load_ref_snapshot(&snap)?;
        pack_set = cpo.packs.clone();
        start_seq = cp.seq;
        info!(
            seq = cp.seq,
            packs = pack_set.len(),
            refs = snap.refs.len(),
            "starting from checkpoint"
        );
    }

    // Replay (start_seq, at_seq]: pack set + ref transactions. Entries
    // older than the manifest's log window (folded into a checkpoint) are
    // probed directly as `log/<seq>.pb` objects — they stay in the store
    // until GC'd, so a cold rewind usually still works.
    let entries = if manifest.min_seq > 0 && start_seq + 1 < manifest.min_seq {
        let mut found = Vec::new();
        let mut seq = start_seq + 1;
        while seq <= at_seq.min(manifest.min_seq.saturating_sub(1)) {
            let key = walgit_proto::keys::log_segment_key(seq);
            match handle.store().get_bytes(&key).await? {
                Some((_, bytes)) => {
                    let (es, _) = walgit_proto::frame::decode_entries(&bytes)?;
                    let last = es.last().map_or(seq, |e| e.seq);
                    found.extend(es);
                    seq = last + 1;
                }
                None => bail!(
                    "history before seq {} is folded into a checkpoint and log/{seq:016x}.pb is gone; the oldest rewindable state is seq {}",
                    manifest.min_seq,
                    manifest
                        .checkpoint
                        .as_ref()
                        .map_or(manifest.min_seq, |c| c.seq)
                ),
            }
        }
        if at_seq >= manifest.min_seq {
            found.extend(handle.read_log(manifest.min_seq, Some(at_seq)).await?);
        }
        found
    } else {
        handle.read_log(start_seq + 1, Some(at_seq)).await?
    };
    info!(entries = entries.len(), "replaying log entries");
    for entry in &entries {
        if entry.seq <= start_seq || entry.seq > at_seq {
            continue;
        }
        if let Some(pack) = &entry.pack {
            pack_set.push(pack.clone());
        }
        pack_set.retain(|p| !entry.supersedes.contains(&p.checksum));
    }

    // Packs live at `at_seq`: copy from the local serving copy when it
    // has them, else fetch from the store (never move the live copy).
    let tmp = out.join(".walgit-tmp");
    std::fs::create_dir_all(&tmp)?;
    for p in &pack_set {
        let checksum = gix_hash::ObjectId::from_hex(p.checksum.as_bytes())?;
        let src = handle.local().pack_path(&checksum);
        if src.is_file() && !src.is_symlink() {
            for ext in ["pack", "idx", "rev", "bitmap", "commit-graph"] {
                let f = src.with_extension(ext);
                if f.is_file() {
                    std::fs::copy(&f, tmp.join(f.file_name().unwrap()))?;
                }
            }
            println!("pack {}: copied from the local copy", p.checksum);
        } else {
            println!(
                "pack {}: fetching {} from the store",
                p.checksum, p.pack_size
            );
            handle.fetch_pack_into(p, &tmp).await.map_err(|e| {
                anyhow::anyhow!(
                    "pack {} is not in the store any more (superseded and past retention?): {e}",
                    p.checksum
                )
            })?;
        }
        let pack_path = tmp.join(format!("pack-{}.pack", p.checksum));
        let idx_path = tmp.join(format!("pack-{}.idx", p.checksum));
        let extra: Vec<std::path::PathBuf> = ["rev", "bitmap", "commit-graph"]
            .iter()
            .map(|e| tmp.join(format!("pack-{}.{e}", p.checksum)))
            .filter(|f| f.is_file())
            .collect();
        local.install_pack(&pack_path, &idx_path, &extra).await?;
    }
    let _ = std::fs::remove_dir_all(&tmp);

    // Refs last (git checks the objects exist).
    for entry in &entries {
        if entry.seq <= start_seq || entry.seq > at_seq {
            continue;
        }
        if let Some(txn) = &entry.txn {
            local.apply_ref_txn(txn, false)?;
        }
    }

    local.refresh()?;
    info!(out = %out.display(), "materialized at seq {at_seq}");
    println!(
        "materialized {} at seq {} into {}",
        id,
        at_seq,
        out.display()
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

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

    /// Cold rewind: a registry with an empty cache materializes seq 2 (before
    /// the compaction: packs fetched from the store) and head (after it) into
    /// fresh directories; refs and objects match, the live copy is untouched.
    #[tokio::test]
    async fn materialize_at_seq_fetches_packs_from_the_store() {
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
        let id = walgit_git::RepoId::new("t", "rewind").unwrap();
        let handle = registry.create(&id, ObjectFormat::Sha1).await.unwrap();
        let work = tempfile::tempdir().unwrap();
        run_git(work.path(), &["init", "-q", "-b", "main"]);
        run_git(work.path(), &["config", "user.email", "t@t"]);
        run_git(work.path(), &["config", "user.name", "t"]);
        let mut prev = String::new();
        let mut tips = Vec::new();
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
            prev = c.clone();
            tips.push(c);
        }
        // Compact into a base (seq 4) and checkpoint there.
        let repack = handle
            .local()
            .repack(walgit_git::RepackOptions {
                mode: walgit_git::RepackMode::Full,
                write_bitmap: false,
                write_midx: false,
                keep: vec![],
            })
            .await
            .unwrap();
        let base = repack.new_packs[0].clone();
        let base_seq = handle
            .publish_compact(base, repack.removed.clone(), 2)
            .await
            .unwrap();
        assert_eq!(base_seq, 4);
        handle.write_checkpoint().await.unwrap();

        // Cold registry (no local packs at all).
        let cache2 = tempfile::tempdir().unwrap();
        let mut cfg2 = (*cfg).clone();
        cfg2.cache.dir = cache2.path().to_path_buf();
        let registry2 = Registry::new(store.clone(), Arc::new(cfg2));
        let outs = tempfile::tempdir().unwrap();

        // Head: from the checkpoint (base pack fetched from the store).
        let out_head = outs.path().join("head");
        materialize_at(&registry2, &id, base_seq, &out_head)
            .await
            .unwrap();
        let g = out_head.join("t").join("rewind.git");
        assert_eq!(run_git(&g, &["rev-parse", "refs/heads/main"]), tips[2]);
        assert!(
            std::process::Command::new("git")
                .current_dir(&g)
                .args(["fsck", "--connectivity-only"])
                .status()
                .unwrap()
                .success()
        );
        assert_eq!(
            std::fs::read_dir(g.join("objects/pack"))
                .unwrap()
                .filter(|e| e
                    .as_ref()
                    .unwrap()
                    .path()
                    .extension()
                    .is_some_and(|x| x == "pack"))
                .count(),
            1
        );

        // Seq 2: before the compaction; the two push packs still exist in the
        // store (retention) and refs replay to the second commit.
        let out_2 = outs.path().join("two");
        materialize_at(&registry2, &id, 2, &out_2).await.unwrap();
        let g2 = out_2.join("t").join("rewind.git");
        assert_eq!(run_git(&g2, &["rev-parse", "refs/heads/main"]), tips[1]);
        assert!(
            std::process::Command::new("git")
                .current_dir(&g2)
                .args(["fsck", "--connectivity-only"])
                .status()
                .unwrap()
                .success()
        );
        // The writer's live copy kept its packs.
        assert!(!handle.local().packs().unwrap().is_empty());
    }
}
