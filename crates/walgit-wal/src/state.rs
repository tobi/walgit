//! Persistent local state for a `RepoHandle`, stored in the repo dir so restarts
//! skip already-applied log entries.

use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::error::WalError;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RepoState {
    /// Opaque version string of the last manifest we applied.
    pub manifest_version: Option<String>,
    /// Last applied log entry sequence.
    pub applied_seq: u64,
    /// Manifest revision (diagnostics).
    pub revision: u64,
    /// Manifest revision whose live pack set (`Manifest.packs`) is fully
    /// installed locally. `!= revision` means refs were applied (refs-first
    /// sync) but packs still need reconciling before serving objects.
    #[serde(default)]
    pub packs_revision: u64,
    /// Pack checksums superseded by COMPACT entries that were applied at the
    /// refs level only; removed locally on the next full (packs) sync.
    #[serde(default)]
    pub pending_pack_removals: Vec<String>,
    /// Tier-2 packs served **remotely** on this instance (Serve level without a
    /// store mount on a pack set that does not fit): only their commit-graph
    /// layer is local; data comes through the remote reader (gix engine with
    /// an object faulter). Counted as "installed" by the pack reconciler.
    #[serde(default)]
    pub remote_served: Vec<String>,
}

impl RepoState {
    /// True when the local pack set matches the applied manifest.
    pub fn packs_ready(&self) -> bool {
        self.packs_revision == self.revision && self.pending_pack_removals.is_empty()
    }
}

impl RepoState {}

const STATE_FILE: &str = "walgit-state.json";

pub fn state_path(repo_dir: &Path) -> std::path::PathBuf {
    repo_dir.join(STATE_FILE)
}

pub fn load_state(repo_dir: &Path) -> RepoState {
    let path = state_path(repo_dir);
    match std::fs::read_to_string(&path) {
        Ok(text) => serde_json::from_str(&text).unwrap_or_default(),
        Err(_) => RepoState::default(),
    }
}

pub fn save_state(repo_dir: &Path, state: &RepoState) -> Result<(), WalError> {
    let path = state_path(repo_dir);
    let text = serde_json::to_string_pretty(state).map_err(|e| WalError::Corrupt(e.to_string()))?;
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, text)?;
    std::fs::rename(&tmp, &path)?;
    Ok(())
}
