//! `walgit settings show|set|clear|history <repo>` — D24 per-repo settings.
use std::sync::Arc;

use crate::open_store;
use anyhow::{Context, Result};
use walgit_config::Config;
use walgit_wal::Registry;

use crate::SettingsAction;
use crate::cli::parse_repo_id;

pub async fn run(action: SettingsAction, cfg: &Arc<Config>) -> Result<()> {
    let store = open_store(cfg).await?;
    std::fs::create_dir_all(&cfg.cache.dir).ok();
    let registry = Registry::new(store, cfg.clone());
    let author = std::env::var("USER").unwrap_or_else(|_| "cli".into());
    match action {
        SettingsAction::Show { repo, effective } => {
            let (owner, name) = parse_repo_id(&repo)?;
            let id = walgit_git::RepoId::new(owner, name)?;
            let h = registry.open(&id).await?;
            h.sync_refs().await?;
            if effective {
                println!("{}", toml::to_string_pretty(&*h.effective_config())?);
                return Ok(());
            }
            match h.settings() {
                None => println!("(none) — {repo} uses the host config"),
                Some(s) => {
                    println!(
                        "# revision {} by {} at {}{}",
                        s.revision,
                        s.author,
                        s.updated_at
                            .as_ref()
                            .map(|t| chrono::DateTime::<chrono::Utc>::from(
                                walgit_proto::time::to_system(t)
                            )
                            .to_rfc3339())
                            .unwrap_or_default(),
                        if s.message.is_empty() {
                            String::new()
                        } else {
                            format!(" — {}", s.message)
                        }
                    );
                    print!("{}", s.toml);
                    if !s.toml.ends_with('\n') {
                        println!();
                    }
                }
            }
            Ok(())
        }
        SettingsAction::Set {
            repo,
            file,
            message,
        } => {
            let (owner, name) = parse_repo_id(&repo)?;
            let id = walgit_git::RepoId::new(owner, name)?;
            let text = if file.as_os_str() == "-" {
                let mut s = String::new();
                std::io::Read::read_to_string(&mut std::io::stdin(), &mut s)?;
                s
            } else {
                std::fs::read_to_string(&file)
                    .with_context(|| format!("reading {}", file.display()))?
            };
            let h = registry.open(&id).await?;
            let rev = h.publish_settings(&text, &author, &message).await?;
            println!("settings published: {repo} revision {rev}");
            Ok(())
        }
        SettingsAction::Clear { repo } => {
            let (owner, name) = parse_repo_id(&repo)?;
            let id = walgit_git::RepoId::new(owner, name)?;
            let h = registry.open(&id).await?;
            let rev = h.publish_settings("", &author, "clear").await?;
            println!("settings cleared: {repo} revision {rev}");
            Ok(())
        }
        SettingsAction::History { repo } => {
            let (owner, name) = parse_repo_id(&repo)?;
            let id = walgit_git::RepoId::new(owner, name)?;
            let h = registry.open(&id).await?;
            h.sync_refs().await?;
            let m = h.manifest();
            let from = m.min_seq.max(1);
            let entries = h.read_log(from, None).await?;
            let mut n = 0;
            for e in entries
                .iter()
                .filter(|e| e.kind() == walgit_proto::v1::EntryKind::Settings)
            {
                let s = e.settings.clone().unwrap_or_default();
                let when = e
                    .created_at
                    .as_ref()
                    .map(|t| {
                        chrono::DateTime::<chrono::Utc>::from(walgit_proto::time::to_system(t))
                            .to_rfc3339()
                    })
                    .unwrap_or_default();
                println!(
                    "seq {} · revision {} · {} · {}{}",
                    e.seq,
                    s.revision,
                    when,
                    s.author,
                    if s.message.is_empty() {
                        String::new()
                    } else {
                        format!(" — {}", s.message)
                    }
                );
                for line in s.toml.lines() {
                    println!("    {line}");
                }
                n += 1;
            }
            if n == 0 {
                println!(
                    "no settings changes in the live log (min_seq {from}; older history is folded into checkpoints)"
                );
            }
            Ok(())
        }
    }
}
