//! `walgit repo create|list|info` — repository management.

use std::sync::Arc;

use anyhow::{Result, bail};
use tracing::info;

use walgit_config::Config;
use walgit_git::ObjectFormat;
use walgit_store::open_store;
use walgit_wal::Registry;

use crate::cli::{parse_repo_id, println_kv};
use crate::{PolicyAction, RepoAction};

pub async fn run(action: RepoAction, cfg: &Arc<Config>) -> Result<()> {
    let store = open_store(cfg).await?;
    std::fs::create_dir_all(&cfg.cache.dir).ok();
    let registry = Registry::new(store.clone(), cfg.clone());

    if let RepoAction::Settings { action } = action {
        return crate::settings_cmd::run(action, cfg).await;
    }
    match action {
        RepoAction::Create {
            repo,
            object_format,
        } => {
            let (owner, name) = parse_repo_id(&repo)?;
            let id = walgit_git::RepoId::new(owner, name)?;
            let format = match object_format.as_str() {
                "sha1" => ObjectFormat::Sha1,
                "sha256" => ObjectFormat::Sha256,
                other => bail!("unknown object format `{other}` (expected sha1 or sha256)"),
            };
            let handle = registry.create(&id, format).await?;
            let manifest = handle.manifest();
            println_kv("repo", &id);
            println_kv("object_format", &manifest.object_format);
            println_kv("head_seq", manifest.head_seq);
            info!(repo = %id, "repo created");
        }
        RepoAction::List => {
            let repos = registry.list().await?;
            if repos.is_empty() {
                println!("(no repositories)");
            } else {
                for id in repos {
                    println!("{id}");
                }
            }
        }
        RepoAction::Info { repo } => {
            let (owner, name) = parse_repo_id(&repo)?;
            let id = walgit_git::RepoId::new(owner, name)?;
            let handle = registry.open(&id).await?;
            handle.sync().await?;
            let manifest = handle.manifest();
            let version = handle
                .manifest_version()
                .map_or_else(|| "(none)".into(), |v| v.to_string());

            println_kv("repo", &id);
            println_kv("object_format", &manifest.object_format);
            println_kv("head_seq", manifest.head_seq);
            println_kv("min_seq", manifest.min_seq);
            println_kv("revision", manifest.revision);
            println_kv("manifest_version", &version);

            let packs = &manifest.packs;
            println_kv("packs", packs.len());
            let total_pack_bytes: u64 = packs.iter().map(|p| p.pack_size).sum();
            println_kv("pack_bytes", total_pack_bytes);

            if let Some(cp) = &manifest.checkpoint {
                println_kv("checkpoint_seq", cp.seq);
                println_kv("checkpoint_key", &cp.key);
            }

            let segments = &manifest.log_segments;
            println_kv("log_segments", segments.len());
            for seg in segments {
                println!(
                    "  {} [{},{}] {} bytes{}",
                    seg.key,
                    seg.first_seq,
                    seg.last_seq,
                    seg.size,
                    if seg.sealed { " (sealed)" } else { "" }
                );
            }
        }
        RepoAction::Policy { action } => policy(action, &store).await?,
        RepoAction::Settings { .. } => unreachable!(),
    }
    Ok(())
}

async fn policy(action: PolicyAction, store: &walgit_store::DynStore) -> Result<()> {
    use walgit_server::policy::{self, RepoPolicy};
    match action {
        PolicyAction::Get { repo } => {
            let id = repo_id(&repo)?;
            let policy = policy::load(store, &id).await?;
            println!("{}", serde_json::to_string_pretty(&policy)?);
        }
        PolicyAction::Set { repo, file } => {
            let id = repo_id(&repo)?;
            let bytes = std::fs::read(&file)?;
            let doc: RepoPolicy = serde_json::from_slice(&bytes)?;
            policy::save(store, &id, &doc).await?;
            info!(repo = %id, "policy saved");
        }
        PolicyAction::Clear { repo } => {
            let id = repo_id(&repo)?;
            policy::clear(store, &id).await?;
            info!(repo = %id, "policy cleared");
        }
    }
    Ok(())
}

fn repo_id(repo: &str) -> Result<walgit_git::RepoId> {
    let (owner, name) = parse_repo_id(repo)?;
    Ok(walgit_git::RepoId::new(owner, name)?)
}
