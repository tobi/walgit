//! `walgit config check|dump` — validate or print the effective configuration.

use std::sync::Arc;

use anyhow::Result;

use crate::ConfigAction;
use walgit_config::Config;

pub async fn run(action: ConfigAction, cfg: &Arc<Config>) -> Result<()> {
    match action {
        ConfigAction::Check { env_files } => {
            let mut cfg: Config = (**cfg).clone();
            let mut vars: Vec<(String, String)> = Vec::new();
            for f in &env_files {
                let text = std::fs::read_to_string(f)
                    .map_err(|e| anyhow::anyhow!("reading {}: {e}", f.display()))?;
                for line in text.lines() {
                    let line = line.trim();
                    if line.is_empty() || line.starts_with('#') {
                        continue;
                    }
                    if let Some((k, v)) = line.split_once('=') {
                        vars.push((k.trim().to_string(), v.trim().trim_matches('"').to_string()));
                    }
                }
            }
            cfg.apply_env(vars.into_iter())?;
            cfg.validate()?;
            println!("config OK");
            Ok(())
        }
        ConfigAction::Dump => {
            let toml = toml::to_string_pretty(&**cfg)
                .map_err(|e| anyhow::anyhow!("serializing config: {e}"))?;
            println!("{toml}");
            Ok(())
        }
    }
}
