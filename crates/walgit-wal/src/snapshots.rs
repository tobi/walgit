//! Exact committed ref snapshots and coherent producer inputs.
use std::sync::Arc;

use prost::Message;
use walgit_proto::v1::{Checkpoint, CheckpointRef, Manifest, RefSnapshot};
use walgit_store::{ObjectStoreExt, Prefixed, PutMode, StoreError, Version};

use crate::{RepoHandle, WalError};

/// One committed generation, including the refs and policy used by a producer.
#[derive(Clone)]
pub struct PublicationView {
    pub manifest: Arc<Manifest>,
    pub version: Option<Version>,
    pub refs: RefSnapshot,
    pub config: Arc<walgit_config::Config>,
}

/// A request's already-synced manifest and name-indexed refs, captured together.
/// Unlike producer snapshots, this does not materialize the full current ref list.
pub struct FetchView {
    pub manifest: Arc<Manifest>,
    pub refs: walgit_git::RefView,
    pub config: Arc<walgit_config::Config>,
}

/// Uploaded candidate. It acquires authority only when a manifest CAS names it.
#[derive(Clone, Debug)]
pub struct CoverageSnapshot {
    pub(crate) key: String,
    pub(crate) snapshot: RefSnapshot,
    pub(crate) repo: String,
}

impl CoverageSnapshot {
    pub fn key(&self) -> &str {
        &self.key
    }
    pub fn snapshot(&self) -> &RefSnapshot {
        &self.snapshot
    }
}

/// Reject unsupported durable formats instead of guessing their meaning. This
/// cannot fence an older binary that never enforced `format_version`.
pub fn validate_manifest(manifest: &Manifest) -> Result<(), WalError> {
    if manifest.format_version != walgit_proto::WAL_FORMAT_VERSION {
        return Err(WalError::Corrupt(format!(
            "unsupported manifest format {}",
            manifest.format_version
        )));
    }
    if !matches!(manifest.object_format.as_str(), "sha1" | "sha256") {
        return Err(WalError::Corrupt(
            "unsupported manifest object format".into(),
        ));
    }
    if let Some(cp) = &manifest.checkpoint
        && (cp.seq > manifest.head_seq || cp.key.is_empty())
    {
        return Err(WalError::Corrupt(
            "invalid committed checkpoint descriptor".into(),
        ));
    }
    Ok(())
}

pub(crate) async fn put_snapshot(
    store: &Prefixed,
    key: &str,
    bytes: Vec<u8>,
) -> Result<(), WalError> {
    match store.put_bytes(key, bytes.clone(), PutMode::Create).await {
        Ok(_) => Ok(()),
        Err(StoreError::PreconditionFailed { .. }) => {
            let (_, existing) = store
                .get_bytes(key)
                .await?
                .ok_or_else(|| WalError::Corrupt(format!("snapshot disappeared: {key}")))?;
            if existing.as_ref() != bytes {
                return Err(WalError::Corrupt(format!(
                    "snapshot content mismatch: {key}"
                )));
            }
            Ok(())
        }
        Err(e) => Err(e.into()),
    }
}

pub(crate) async fn read_snapshot(
    store: &Prefixed,
    key: &str,
    seq: u64,
    format: &str,
) -> Result<RefSnapshot, WalError> {
    let (_, bytes) = store
        .get_bytes(key)
        .await?
        .ok_or_else(|| WalError::Corrupt(format!("missing committed refs snapshot {key}")))?;
    let mut snapshot = if key.starts_with("checkpoints/refs/") {
        walgit_proto::snapshot::decode_verified(key, &bytes)
            .map_err(|e| WalError::Corrupt(format!("refs snapshot: {e}")))?
    } else {
        // Old snapshots did not consistently carry seq/object_format. The
        // committed checkpoint supplies those facts; never infer its refs key.
        let mut s = RefSnapshot::decode(bytes.as_ref())
            .map_err(|e| WalError::Corrupt(format!("refs snapshot decode: {e}")))?;
        if s.seq == 0 {
            s.seq = seq;
            s.object_format = format.to_string();
        }
        if s.object_format.is_empty() {
            s.object_format = format.to_string();
        }
        walgit_proto::snapshot::validate(&s)
            .map_err(|e| WalError::Corrupt(format!("refs snapshot: {e}")))?;
        s
    };
    if snapshot.seq != seq || snapshot.object_format != format {
        return Err(WalError::Corrupt(format!(
            "refs snapshot generation/format mismatch: {key}"
        )));
    }
    snapshot.created_at = None;
    Ok(snapshot)
}

pub(crate) async fn checkpoint_snapshot(
    store: &Prefixed,
    cp: &CheckpointRef,
    format: &str,
) -> Result<RefSnapshot, WalError> {
    let key = if cp.refs_key.is_empty() {
        let (_, bytes) = store
            .get_bytes(&cp.key)
            .await?
            .ok_or_else(|| WalError::Corrupt(format!("missing committed checkpoint {}", cp.key)))?;
        let checkpoint = Checkpoint::decode(bytes.as_ref())
            .map_err(|e| WalError::Corrupt(format!("checkpoint decode: {e}")))?;
        if checkpoint.seq != cp.seq
            || checkpoint.object_format != format
            || checkpoint.refs_key.is_empty()
        {
            return Err(WalError::Corrupt("checkpoint descriptor mismatch".into()));
        }
        checkpoint.refs_key
    } else {
        cp.refs_key.clone()
    };
    read_snapshot(store, &key, cp.seq, format).await
}

impl RepoHandle {
    pub async fn fetch_view(&self) -> Result<FetchView, WalError> {
        let _sync = self.sync_mutex.lock().await;
        let manifest = self.manifest();
        let local = self.local.clone();
        let refs = tokio::task::spawn_blocking(move || local.ref_view())
            .await
            .map_err(|e| WalError::Corrupt(e.to_string()))??;
        let config = self.validated_config_for_manifest(&manifest)?;
        Ok(FetchView {
            manifest,
            refs,
            config,
        })
    }

    /// Read exactly the descriptor in the request's captured manifest. Never
    /// search a newer manifest by sequence or substitute another candidate key.
    pub async fn read_coverage_snapshot(
        &self,
        view: &FetchView,
        key: &str,
        seq: u64,
    ) -> Result<RefSnapshot, WalError> {
        if view.manifest.repo != self.id.to_string()
            || !view
                .manifest
                .packs
                .iter()
                .flat_map(|p| &p.group_coverages)
                .any(|p| p.refs_key == key && p.covers_seq == seq)
        {
            return Err(WalError::Invalid(
                "snapshot is outside captured coverage authority".into(),
            ));
        }
        read_snapshot(&self.store, key, seq, &view.manifest.object_format).await
    }

    /// Revalidate and capture one committed manifest/token/refs/policy view.
    /// No pack materialization, and no refs lock survives the returned value.
    pub async fn publication_view(&self) -> Result<PublicationView, WalError> {
        let _sync = self.sync_mutex.lock().await;
        self.sync_locked_inner(&tracing::Span::current()).await?;
        let (manifest, version) = self.manifest_pair();
        let local = self.local.clone();
        let refs = tokio::task::spawn_blocking(move || local.refs())
            .await
            .map_err(|e| WalError::Corrupt(e.to_string()))??;
        let mut snapshot: RefSnapshot = refs.into();
        snapshot.seq = manifest.head_seq;
        snapshot.object_format.clone_from(&manifest.object_format);
        snapshot.created_at = None;
        let refs = walgit_proto::snapshot::canonicalize(&snapshot)
            .map_err(|e| WalError::Corrupt(e.to_string()))?;
        let config = self.validated_config_for_manifest(&manifest)?;
        Ok(PublicationView {
            manifest,
            version,
            refs,
            config,
        })
    }

    /// Upload immutable refs prepared from a coherent committed producer view.
    pub async fn prepare_coverage_snapshot(
        &self,
        view: &PublicationView,
    ) -> Result<CoverageSnapshot, WalError> {
        if view.manifest.repo != self.id.to_string()
            || view.refs.seq != view.manifest.head_seq
            || view.refs.object_format != view.manifest.object_format
        {
            return Err(WalError::Invalid(
                "coverage view does not match repository/generation".into(),
            ));
        }
        let bytes = walgit_proto::snapshot::encode(&view.refs)
            .map_err(|e| WalError::Invalid(e.to_string()))?;
        let key = walgit_proto::snapshot::key(&bytes);
        put_snapshot(&self.store, &key, bytes).await?;
        Ok(CoverageSnapshot {
            key,
            snapshot: view.refs.clone(),
            repo: view.manifest.repo.clone(),
        })
    }

    /// Resolve only an exact descriptor already named by the live manifest.
    /// An uncommitted candidate at the same sequence is never a substitute.
    pub async fn pin_coverage_refs(&self, seq: u64) -> Result<RefSnapshot, WalError> {
        let manifest = self.manifest();
        if let Some(key) = manifest
            .packs
            .iter()
            .flat_map(|p| &p.group_coverages)
            .find(|c| c.covers_seq == seq)
            .map(|c| c.refs_key.as_str())
        {
            return read_snapshot(&self.store, key, seq, &manifest.object_format).await;
        }
        if let Some(cp) = &manifest.checkpoint
            && cp.seq == seq
        {
            return checkpoint_snapshot(&self.store, cp, &manifest.object_format).await;
        }
        crate::log_reader::refs_at_seq(self, &manifest, seq).await
    }
}
