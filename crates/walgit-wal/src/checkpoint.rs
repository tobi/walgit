//! Checkpoint writing and store GC.
#![allow(clippy::needless_continue)]

use std::sync::Arc;

use prost::Message;
use walgit_proto::keys;
use walgit_proto::time;
use walgit_proto::v1::{Checkpoint, CheckpointRef, Manifest, RefSnapshot};
use walgit_store::{ObjectStore, PutBody, PutMode, PutOptions, StoreError};

use crate::error::WalError;
use crate::handle::RepoHandle;
use tracing::Instrument;

/// Why a checkpoint is due (see [`checkpoint_due`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckpointTrigger {
    Entries,
    Age,
    TailBytes,
}

impl std::fmt::Display for CheckpointTrigger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            CheckpointTrigger::Entries => "entries",
            CheckpointTrigger::Age => "age",
            CheckpointTrigger::TailBytes => "tail-bytes",
        })
    }
}

/// Whether `manifest` wants a checkpoint under `cfg`: entries since the last
/// one ≥ `wal.snapshot_every_entries`, or the last one older than
/// `wal.checkpoint_interval`, or the log tail after it larger than
/// `wal.checkpoint_tail_bytes` (each 0 = disabled). Nothing is due at
/// `head_seq == 0` or when the checkpoint is already at head.
pub fn checkpoint_due(
    manifest: &Manifest,
    cfg: &walgit_config::WalConfig,
) -> Option<CheckpointTrigger> {
    let head = manifest.head_seq;
    if head == 0 {
        return None;
    }
    let cp_seq = manifest.checkpoint.as_ref().map_or(0, |c| c.seq);
    if cp_seq >= head {
        return None;
    }
    let entries = head - cp_seq;
    if cfg.snapshot_every_entries > 0 && entries >= cfg.snapshot_every_entries {
        return Some(CheckpointTrigger::Entries);
    }
    let tail_max = cfg.checkpoint_tail_bytes.as_u64();
    if tail_max > 0 {
        let tail: u64 = manifest
            .log_segments
            .iter()
            .filter(|s| s.last_seq > cp_seq)
            .map(|s| s.size)
            .sum();
        if tail > tail_max {
            return Some(CheckpointTrigger::TailBytes);
        }
    }
    if !cfg.checkpoint_interval.is_zero() {
        // Age of the state the next checkpoint would fold: the last checkpoint
        // when there is one (its `created_at`, else unknown → not due by age),
        // otherwise the repo's first write (the oldest live log segment has no
        // timestamp; `updated_at` is the best manifest-only proxy and a
        // checkpoint-less repo with writes older than the interval is due).
        let since = match manifest.checkpoint.as_ref() {
            Some(c) => c.created_at.as_ref().map(time::to_system),
            None => manifest.updated_at.as_ref().map(time::to_system),
        };
        if let Some(t) = since
            && std::time::SystemTime::now()
                .duration_since(t)
                .unwrap_or_default()
                >= cfg.checkpoint_interval
        {
            return Some(CheckpointTrigger::Age);
        }
    }
    None
}

/// Write a checkpoint at the current head: refs snapshot + pack set, then
/// CAS manifest (checkpoint=, `min_seq`=, `log_segments` trimmed). Idempotent.
/// Needs only a **refs-level** sync (manifest + ref state): it works on an
/// instance that could never hold the repo's packs.
pub(crate) async fn write_checkpoint_impl(handle: &RepoHandle) -> Result<CheckpointRef, WalError> {
    let trigger = checkpoint_due(&handle.manifest(), &handle.cfg.wal)
        .map_or_else(|| "manual".into(), |t| t.to_string());
    let span = tracing::info_span!("wal.checkpoint", repo = %handle.id, trigger = %trigger, seq = tracing::field::Empty, refs = tracing::field::Empty, folded = tracing::field::Empty, outcome = tracing::field::Empty);
    let t0 = std::time::Instant::now();
    let r = write_checkpoint_inner(handle)
        .instrument(span.clone())
        .await;
    if let Ok(cp) = &r {
        span.record("seq", cp.seq);
        span.record("outcome", "ok");
        metrics::histogram!("walgit_checkpoint_seconds").record(t0.elapsed().as_secs_f64());
        metrics::counter!("walgit_checkpoints_total", "outcome" => "ok").increment(1);
    } else {
        span.record("outcome", "error");
        metrics::counter!("walgit_checkpoints_total", "outcome" => "error").increment(1);
    }
    r
}

async fn write_checkpoint_inner(handle: &RepoHandle) -> Result<CheckpointRef, WalError> {
    let writer = crate::handle::instance_id();
    let max_retries = handle.cfg.wal.cas_max_retries;

    // Sync to get current state (refs only; packs are taken from the manifest).
    handle.sync_impl_level(crate::sync::SyncLevel::Refs).await?;
    let manifest = handle.manifest.read().clone();

    // If checkpoint already at head, return it (idempotent)
    if let Some(ref cp) = manifest.checkpoint
        && cp.seq == manifest.head_seq
    {
        tracing::Span::current().record("folded", 0u64);
        return Ok(cp.clone());
    }

    let seq = manifest.head_seq;
    tracing::Span::current().record(
        "folded",
        seq - manifest.checkpoint.as_ref().map_or(0, |c| c.seq),
    );
    if seq == 0 {
        return Err(WalError::Corrupt(
            "cannot checkpoint empty repo (head_seq=0)".into(),
        ));
    }

    // Build refs snapshot from local repo
    let refs_data = handle.local.refs()?;
    let snap: RefSnapshot = refs_data.into();
    let refs_key = keys::checkpoint_refs_key(seq);
    let snap_bytes = snap.encode_to_vec();

    // Provenance for the slot planner (D22): the earliest state ever (carried forward) and the
    // time of the newest folded entry — from what this writer already applied (`first_entry_time`
    // / `last_entry_time` are maintained by every refs sync and every local publish), never a
    // log GET: after the freshness GET above the checkpoint is two rounds (both PUTs ∥, then
    // the CAS — ROUNDTRIPS §2).
    let prev = manifest.checkpoint.as_ref();
    let created_at = time::now();
    let first_state_at = prev
        .and_then(|c| c.first_state_at)
        .or_else(|| handle.first_entry_time.lock().map(time::from_system))
        .or_else(|| handle.first_seq_published_at.lock().map(time::from_system))
        .or_else(|| prev.and_then(|c| c.created_at));
    let as_of = handle
        .last_entry_time
        .lock()
        .map(time::from_system)
        .or_else(|| prev.and_then(|c| c.as_of))
        .or(Some(created_at));

    // The checkpoint: the pack set with its side-file inventory (idx/rev/bitmap/commit-graph
    // flags travel in PackRef) + the ref snapshot. `bundle_key` is left empty: nothing reads it
    // (`import --direct` still fills it), and looking the list up cost a round trip.
    let checkpoint = Checkpoint {
        seq,
        object_format: manifest.object_format.clone(),
        packs: manifest.packs.clone(),
        refs_key: refs_key.clone(),
        ref_count: {
            tracing::Span::current().record("refs", snap.refs.len() as u64);
            snap.refs.len() as u64
        },
        bundle_key: String::new(),
        created_at: Some(created_at),
        writer: writer.clone(),
    };
    let cp_key = keys::checkpoint_key(seq);
    let cp_bytes = checkpoint.encode_to_vec();

    // Round 1: both immutable objects in parallel. Keyed by seq and deterministic for a given
    // state, so a writer that dies here leaves garbage, never a hazard (sim
    // `sim_checkpoint_writer_crash_is_invisible_and_repaired`).
    let immutable = PutOptions {
        immutable: true,
        ..Default::default()
    };
    let (r_refs, r_cp) = tokio::join!(
        handle.store.put(
            &refs_key,
            PutBody::Bytes(bytes::Bytes::from(snap_bytes)),
            immutable.clone()
        ),
        handle.store.put(
            &cp_key,
            PutBody::Bytes(bytes::Bytes::from(cp_bytes)),
            immutable
        ),
    );
    r_refs?;
    r_cp?;

    let cp_ref = CheckpointRef {
        seq,
        key: cp_key.clone(),
        created_at: Some(created_at),
        first_state_at,
        as_of,
    };

    // Round 2: CAS manifest: set checkpoint, min_seq = seq+1, trim log_segments
    let mut attempts = 0u32;
    loop {
        let current_manifest = handle.manifest.read().clone();
        let known_version = handle.manifest_version.lock().clone();

        // If checkpoint already at or past head, done
        if let Some(ref cp) = current_manifest.checkpoint
            && cp.seq >= current_manifest.head_seq
        {
            return Ok(cp.clone());
        }

        let mut updated: Manifest = (*current_manifest).clone();
        updated.checkpoint = Some(cp_ref.clone());
        updated.min_seq = seq + 1;
        // Trim log_segments: keep only those with last_seq > seq
        updated.log_segments.retain(|s| s.last_seq > seq);
        updated.updated_at = Some(time::now());
        updated.writer = writer.clone();
        updated.revision += 1;

        let buf = updated.encode_to_vec();
        let mode = match &known_version {
            Some(v) => PutMode::Update(v.clone()),
            None => PutMode::Create,
        };

        match handle
            .store
            .put(
                keys::MANIFEST,
                PutBody::Bytes(bytes::Bytes::from(buf)),
                mode.into(),
            )
            .await
        {
            Ok(meta) => {
                *handle.manifest.write() = Arc::new(updated.clone());
                *handle.manifest_version.lock() = Some(meta.version.clone());
                {
                    let mut state = handle.state.lock();
                    state.manifest_version = Some(meta.version.as_str().to_string());
                    state.applied_seq = updated.head_seq;
                    let ready = state.packs_ready();
                    state.revision = updated.revision;
                    if ready {
                        state.packs_revision = updated.revision;
                    }
                }
                crate::state::save_state(handle.local.path(), &handle.state.lock().clone())?;
                return Ok(cp_ref);
            }
            Err(StoreError::PreconditionFailed { .. }) => {
                attempts += 1;
                if attempts >= max_retries {
                    return Err(WalError::Retry { attempts });
                }
                // Re-sync (refs) and retry
                handle.sync_impl_level(crate::sync::SyncLevel::Refs).await?;
                continue;
            }
            Err(e) => return Err(WalError::Store(e)),
        }
    }
}
