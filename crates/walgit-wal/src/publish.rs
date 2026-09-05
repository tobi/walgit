//! Publish path: linearizable CAS with batching.
#![allow(clippy::needless_continue)]
//!
//! Design:
//!   Each `RepoHandle` has a single-flight publisher task. `publish_push` and
//!   `publish_ref_update` enqueue a [`PublishRequest`] onto an mpsc channel
//!   and await a oneshot response. The publisher collects requests within
//!   `cfg.wal.batch_window` (up to `max_batch`), then processes them as one
//!   batch:
//!
//!   1. Sync to get the current manifest + local refs.
//!   2. Build a working ref map from the local refs. Verify each txn's old
//!      values against this map. A rejected txn gets a `RefConflict` response
//!      but does not sink the batch.
//!   3. Build `LogEntry`s for valid txns with contiguous seqs starting at
//!      `head_seq + 1`.
//!   4. Write one log segment object (`log/<first_seq>.pb`, framed) with
//!      `PutMode::Create` ([`claim_log_slot`]). On 412 there are two cases:
//!      a committed entry claimed the seq (re-sync, re-verify, re-seq, retry)
//!      or an **orphan** — a writer crashed (or is mid-flight) between its log
//!      PUT and its manifest CAS. Orphans are never overwritten: after a short
//!      grace the seq is *burned* (the segment goes to the next free seq; the
//!      log is strictly increasing, not dense — readers follow
//!      `manifest.log_segments`) and the orphan is CAS-deleted after commit.
//!   5. CAS the manifest: `head_seq = last_seq`, `packs += new PackRefs`,
//!      `log_segments += new segment`. On 412: re-sync (applies others'
//!      entries), re-verify, CAS-delete *our* segment, retry with new seq. On
//!      any other error the outcome is ambiguous (the bucket may have applied
//!      the write and lost the response): re-read the manifest; if it lists
//!      our segment the publish succeeded, else leave the segment in place
//!      (an orphan is harmless, a deleted committed segment is not).
//!   6. On success: apply all valid txns locally, update handle state, send
//!      success responses.
//!
//!   Liveness of this protocol against crashed/partitioned writers is covered
//!   by the simulation suite (`crates/walgit-server/tests/sim.rs`).

use crate::error::{RefError, WalError};
use crate::handle::RepoHandle;
use prost::Message;
use std::collections::HashMap;
use std::sync::{Arc, atomic::Ordering};
use tokio::sync::{mpsc, oneshot};
use tracing::Instrument;
use walgit_git::{IngestedPack, PackInfo};
use walgit_proto::keys;
use walgit_proto::v1::PackKind;
use walgit_proto::v1::{EntryKind, LogEntry, LogSegmentRef, Manifest, PackRef, RefTransaction};
use walgit_proto::{frame, time};
use walgit_store::{ObjectStore, Prefixed, PutBody, PutMode, PutOptions, StoreError};

/// Per-ref result within a publish.
pub struct PublishResult {
    pub seq: u64,
    pub per_ref: Vec<(String, Result<(), RefError>)>,
}

/// Request sent to the single-flight publisher.
pub(crate) struct PublishRequest {
    pub(crate) pack: Option<IngestedPack>,
    pub(crate) txn: RefTransaction,
    pub(crate) meta: HashMap<String, String>,
    /// True when receive-pack already performed the request freshness check.
    pub(crate) synced: bool,
    /// Explicit entry time (history replay); None = now. Validated monotonic
    /// (>= the head entry's `created_at`) before the batch is written.
    pub(crate) created_at: Option<prost_types::Timestamp>,
    pub(crate) response: oneshot::Sender<Result<PublishResult, WalError>>,
}

// ---- helpers ----

pub(crate) fn pack_ref_from_ingested(p: &IngestedPack, seq: u64) -> PackRef {
    PackRef {
        checksum: p.checksum.to_string(),
        pack_size: p.pack_size,
        idx_size: p.idx_size,
        has_rev: false,
        has_bitmap: false,
        has_commit_graph: false,
        object_count: p.object_count,
        seq,
        tier: 0,
        kind: PackKind::Objects as i32,
        derived_from: String::new(),
    }
}

pub(crate) fn pack_ref_from_info(p: &PackInfo, seq: u64, tier: u32) -> PackRef {
    PackRef {
        checksum: p.checksum.to_string(),
        pack_size: p.pack_size,
        idx_size: p.idx_size,
        has_rev: p.has_rev,
        has_bitmap: p.has_bitmap,
        has_commit_graph: p.has_commit_graph,
        object_count: p.object_count,
        seq,
        tier,
        kind: if p.history_of.is_some() {
            PackKind::History as i32
        } else {
            PackKind::Objects as i32
        },
        derived_from: p.history_of.clone().unwrap_or_default(),
    }
}

/// Upload a pack + idx to the store (immutable, skip if already present).
///
/// Both objects use create-if-absent directly. A precondition failure means
/// another publisher already uploaded the content-addressed object, so it is
/// success rather than a reason to spend another metadata round trip.
pub(crate) async fn upload_pack(store: &Prefixed, pack: &IngestedPack) -> Result<(), WalError> {
    let checksum = pack.checksum.to_string();
    let pack_key = keys::pack_key(&checksum);
    let idx_key = keys::idx_key(&checksum);

    let pack_put = put_immutable_create(store, pack_key, pack.pack_path.clone());
    let idx_put = put_immutable_create(store, idx_key, pack.idx_path.clone());
    tokio::try_join!(pack_put, idx_put)?;
    Ok(())
}

/// Above this size a pack upload is striped (`put_file_parallel`).
const PARALLEL_PUT_MIN_BYTES: u64 = 256 * 1024 * 1024;
const PARALLEL_PUT_STRIPES: usize = 8;

pub(crate) async fn put_immutable_create(
    store: &Prefixed,
    key: String,
    path: std::path::PathBuf,
) -> Result<(), WalError> {
    let opts = || PutOptions {
        mode: PutMode::Create,
        immutable: true,
        ..Default::default()
    };
    // Big packs go up striped (parts + server-side compose, ~8 × 100 MB/s):
    // a large repository's rebuilt base (32.4 GB) took 431 s single-stream at 75 MB/s in the
    // weekly dry run of 2026-08-21. Small packs (every push) stay one PUT.
    let size = tokio::fs::metadata(&path).await.map_or(0, |m| m.len());
    let put = if size >= PARALLEL_PUT_MIN_BYTES && store.supports_compose() {
        walgit_store::util::put_file_parallel(store, &key, &path, opts(), PARALLEL_PUT_STRIPES)
            .await
            .map(|_| ())
    } else {
        store
            .put(&key, PutBody::File(path.clone()), opts())
            .await
            .map(|_| ())
    };
    match put {
        Ok(()) => Ok(()),
        Err(StoreError::PreconditionFailed { .. }) => {
            // "Already exists" is the normal reading, but verify before the
            // manifest points at it: a 412 racing a GC delete (or a backend
            // hiccup) must not leave a referenced object missing. Rare path,
            // one HEAD; on a miss, write it unconditionally (content-addressed:
            // whoever wins wrote the same bytes).
            if store.head(&key).await?.is_some() {
                Ok(())
            } else {
                tracing::warn!(
                    key,
                    "create-if-absent reported the object present but HEAD finds nothing; writing it"
                );
                store
                    .put(
                        &key,
                        PutBody::File(path),
                        PutOptions {
                            mode: PutMode::Overwrite,
                            immutable: true,
                            ..Default::default()
                        },
                    )
                    .await
                    .map(|_| ())
                    .map_err(WalError::Store)
            }
        }
        Err(e) => Err(WalError::Store(e)),
    }
}

// ---- log slot claim (shared by push and compact publishers) ----

/// A log segment written and owned by this writer, awaiting the manifest CAS.
pub(crate) struct LogSlot {
    pub(crate) key: String,
    pub(crate) version: walgit_store::Version,
    pub(crate) first_seq: u64,
    /// Orphan segments (key, version as observed) whose seqs were burned to
    /// get here; CAS-deleted after our commit.
    pub(crate) burned: Vec<(String, walgit_store::Version)>,
}

pub(crate) enum ClaimOutcome {
    /// Segment written; commit it with the manifest CAS.
    Claimed(LogSlot),
    /// The seq was claimed by a committed entry: re-sync and retry.
    Contended,
}

/// How long a foreign `log/<seq>.pb` may sit uncommitted at `head_seq+1`
/// before we treat it as an orphan and burn the seq. A healthy writer CASes
/// the manifest milliseconds after its log PUT.
const ORPHAN_GRACE_PROBES: u32 = 3;
const ORPHAN_GRACE_STEP: std::time::Duration = std::time::Duration::from_millis(100);
/// Never burn more than this many seqs in one claim (a pile of orphans means
/// something else is wrong).
const MAX_BURN: usize = 8;

/// Unconditional fresh read of the manifest (not the handle's cached view).
pub(crate) async fn read_manifest_fresh(store: &Prefixed) -> Result<Option<Manifest>, WalError> {
    use walgit_store::ObjectStoreExt;
    match store.get_bytes(keys::MANIFEST).await? {
        Some((_, b)) => {
            Ok(Some(Manifest::decode(b.as_ref()).map_err(|e| {
                WalError::Corrupt(format!("manifest decode: {e}"))
            })?))
        }
        None => Ok(None),
    }
}

/// Claim `log/<seq>.pb` for the entries that `encode(first_seq)` produces,
/// starting at `head_seq + 1` (the head this writer synced to).
pub(crate) async fn claim_log_slot(
    store: &Prefixed,
    head_seq: u64,
    mut encode: impl FnMut(u64) -> bytes::Bytes,
) -> Result<ClaimOutcome, WalError> {
    let mut seq = head_seq + 1;
    let mut burned: Vec<(String, walgit_store::Version)> = Vec::new();
    loop {
        let key = keys::log_segment_key(seq);
        let bytes = encode(seq);
        match store
            .put(&key, PutBody::Bytes(bytes), PutMode::Create.into())
            .await
        {
            Ok(meta) => {
                return Ok(ClaimOutcome::Claimed(LogSlot {
                    key,
                    version: meta.version,
                    first_seq: seq,
                    burned,
                }));
            }
            Err(StoreError::PreconditionFailed { .. }) => {}
            Err(e) => return Err(WalError::Store(e)),
        }
        // Somebody wrote log/<seq>.pb. Committed (head moved) or orphan?
        let mut probes = 0u32;
        let orphan_version = loop {
            let fresh = read_manifest_fresh(store).await?;
            let fresh_head = fresh.as_ref().map_or(0, |m| m.head_seq);
            if fresh_head >= seq {
                return Ok(ClaimOutcome::Contended);
            }
            match store.head(&key).await? {
                // The other writer cleaned up after its own CAS failure: the
                // slot is free again.
                None => break None,
                Some(m) => {
                    probes += 1;
                    if probes >= ORPHAN_GRACE_PROBES {
                        break Some(m.version);
                    }
                    tokio::time::sleep(ORPHAN_GRACE_STEP).await;
                }
            }
        };
        match orphan_version {
            None => continue, // retry the Create at the same seq
            Some(v) => {
                tracing::warn!(
                    key,
                    seq,
                    "orphaned log segment at the head (writer crashed between log PUT and manifest CAS); burning the seq"
                );
                burned.push((key, v));
                if burned.len() >= MAX_BURN {
                    return Err(WalError::Corrupt(format!(
                        "{MAX_BURN} consecutive orphaned log segments from seq {}",
                        head_seq + 1
                    )));
                }
                seq += 1;
            }
        }
    }
}

/// After a successful manifest CAS: CAS-delete the burned orphans (best effort).
pub(crate) async fn sweep_burned(store: &Prefixed, slot: &LogSlot) {
    for (key, version) in &slot.burned {
        let _ = store.delete(key, Some(version.clone())).await;
    }
}

/// After a manifest CAS `PreconditionFailed`: CAS-delete *our* segment (only
/// the version we wrote; never anything a later writer put there).
pub(crate) async fn drop_own_slot(store: &Prefixed, slot: &LogSlot) {
    let _ = store.delete(&slot.key, Some(slot.version.clone())).await;
}

/// After a manifest CAS failed with a non-412 error: did the write land?
/// `Ok(Some(manifest))` when the fresh manifest lists our segment (committed),
/// `Ok(None)` when it does not (not committed; leave the orphan alone).
pub(crate) async fn cas_landed(
    store: &Prefixed,
    slot: &LogSlot,
) -> Result<Option<Manifest>, WalError> {
    let fresh = read_manifest_fresh(store).await?;
    Ok(fresh.filter(|m| {
        m.log_segments
            .iter()
            .any(|s| s.key == slot.key && s.first_seq == slot.first_seq)
    }))
}

/// Verify a ref transaction against a working ref map. Returns per-ref results.
pub(crate) fn verify_txn(
    txn: &RefTransaction,
    refs: &walgit_git::RefView,
) -> Vec<(String, Result<(), RefError>)> {
    txn.updates
        .iter()
        .map(|u| {
            // Symbolic ref update (HEAD -> target): oids empty, new_symbolic_target set
            if !u.new_symbolic_target.is_empty() {
                return (u.name.clone(), Ok(()));
            }
            let actual = refs.get(&u.name).unwrap_or_default();
            let expected = u.old_oid.clone();
            // old_oid empty or all-zero => ref must not exist (proto: "all-zero id
            // means does not exist"; the server normalizes to empty, the CLI sends zeros).
            if is_null_oid(&expected) {
                if actual.is_empty() {
                    (u.name.clone(), Ok(()))
                } else {
                    (
                        u.name.clone(),
                        Err(RefError::Conflict {
                            expected: "(new)".into(),
                            actual,
                        }),
                    )
                }
            } else if actual == expected {
                (u.name.clone(), Ok(()))
            } else {
                (u.name.clone(), Err(RefError::Conflict { expected, actual }))
            }
        })
        .collect()
}

/// Empty or all-zero hex = "no object" (create / delete / must-not-exist).
pub(crate) fn is_null_oid(hex: &str) -> bool {
    hex.is_empty() || hex.bytes().all(|b| b == b'0')
}

/// Update the working ref map with a txn's new values.
pub(crate) fn apply_txn_to_map(txn: &RefTransaction, refs: &mut walgit_git::RefView) {
    for u in &txn.updates {
        if !u.new_symbolic_target.is_empty() {
            refs.set(&u.name, u.new_symbolic_target.clone());
        } else if is_null_oid(&u.new_oid) {
            refs.remove(&u.name);
        } else {
            refs.set(&u.name, u.new_oid.clone());
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn build_log_entry_at(
    seq: u64,
    kind: EntryKind,
    pack: Option<PackRef>,
    txn: Option<&RefTransaction>,
    supersedes: Vec<String>,
    meta: &HashMap<String, String>,
    writer: &str,
    created_at: Option<prost_types::Timestamp>,
) -> LogEntry {
    LogEntry {
        seq,
        kind: kind as i32,
        pack,
        txn: txn.cloned(),
        supersedes,
        checkpoint: None,
        created_at: Some(created_at.unwrap_or_else(time::now)),
        writer: writer.to_string(),
        meta: meta.clone(),
        settings: None,
    }
}

// ---- publisher task ----

/// The single-flight publisher task. Runs until all senders are dropped.
pub(crate) async fn publisher_task(
    handle: Arc<RepoHandle>,
    mut rx: mpsc::UnboundedReceiver<PublishRequest>,
) {
    let batch_window = handle.cfg.wal.batch_window;
    let max_batch = handle.cfg.wal.max_batch;

    loop {
        let Some(first) = rx.recv().await else { break };

        let mut batch = Vec::with_capacity(max_batch.min(64));
        batch.push(first);

        // Do not unconditionally sleep for the batching window: a lone push
        // should enter the CAS immediately. If another request is queued or
        // still waiting to enqueue, wait briefly for concurrent pushes.
        if batch.len() < max_batch {
            let first_queued = rx.try_recv().ok();
            let concurrent = handle.publish_waiters.load(Ordering::Relaxed) > 1;
            if first_queued.is_some() || concurrent {
                if let Some(r) = first_queued {
                    batch.push(r);
                }
                if batch.len() < max_batch && batch_window > std::time::Duration::ZERO {
                    let deadline = tokio::time::sleep(batch_window);
                    tokio::pin!(deadline);
                    loop {
                        tokio::select! {
                            () = &mut deadline => break,
                            maybe_req = rx.recv() => {
                                match maybe_req {
                                    Some(r) => {
                                        batch.push(r);
                                        if batch.len() >= max_batch {
                                            break;
                                        }
                                    }
                                    None => break,
                                }
                            }
                        }
                    }
                }
            }
        }

        if let Err(e) = process_batch(&handle, batch).await {
            tracing::error!("publisher batch failed: {e}");
        }
    }

    *handle.publish_tx.lock() = None;
}

/// Per-request verification result.
struct Verified {
    per_ref: Vec<(String, Result<(), RefError>)>,
    valid: bool,
}

/// Process a batch of publish requests through the full CAS loop.
async fn process_batch(handle: &RepoHandle, batch: Vec<PublishRequest>) -> Result<(), WalError> {
    let batch_size = batch.len() as u64;
    let span = tracing::info_span!(
        "wal.publish",
        repo = %handle.id,
        batch_size,
        seq = 0u64,
        cas_retries = 0u32,
    );
    // Instrument awaited futures instead of carrying a thread-local span guard
    // across await points.

    let max_retries = handle.cfg.wal.cas_max_retries;
    let writer = crate::handle::instance_id();
    // Receive-pack has already checked freshness while it held its read guard.
    // Direct WAL callers still request the initial sync here.
    let needs_initial_sync = batch.iter().any(|req| !req.synced);

    let mut attempts = 0u32;

    loop {
        // 1. Sync to get current manifest + refs. CAS retries must re-sync;
        // the first attempt may reuse receive-pack's request sync.
        if (attempts > 0 || needs_initial_sync)
            && let Err(e) = handle.sync_impl().instrument(span.clone()).await
        {
            return finish_all_errors(batch, e);
        }
        let manifest = handle.manifest.read().clone();
        let head_seq = manifest.head_seq;
        let known_version = handle.manifest_version.lock().clone();

        // O(log refs) lookups over the cached snapshot + an overlay of what this
        // batch applied; never an O(refs) map per push.
        let mut working_refs = match handle.local.ref_view() {
            Ok(v) => v,
            Err(e) => return finish_all_errors(batch, WalError::from(e)),
        };

        // 2. Verify each txn against working ref map (+ explicit created_at
        //    must be monotonic: >= the head entry's time, >= earlier explicit
        //    times in this batch — the WAL's created_at order is history).
        let mut verified: Vec<Verified> = Vec::with_capacity(batch.len());
        let mut floor: Option<std::time::SystemTime> = *handle.last_entry_time.lock();
        for req in &batch {
            let mut per_ref = verify_txn(&req.txn, &working_refs);
            if let Some(ts) = &req.created_at {
                let t = time::to_system(ts);
                if let Some(f) = floor
                    && t < f
                {
                    let msg = format!(
                        "created_at {} is before the WAL head's {} (entries must be monotonic)",
                        chrono::DateTime::<chrono::Utc>::from(t).to_rfc3339(),
                        chrono::DateTime::<chrono::Utc>::from(f).to_rfc3339()
                    );
                    for (_, r) in &mut per_ref {
                        *r = Err(RefError::Rejected(msg.clone()));
                    }
                }
                if per_ref.iter().all(|(_, r)| r.is_ok()) {
                    floor = Some(t);
                }
            }
            let all_ok = per_ref.iter().all(|(_, r)| r.is_ok());
            if all_ok {
                apply_txn_to_map(&req.txn, &mut working_refs);
            }
            verified.push(Verified {
                per_ref,
                valid: all_ok,
            });
        }

        // 3. Collect valid indices
        let valid_indices: Vec<usize> = verified
            .iter()
            .enumerate()
            .filter(|(_, v)| v.valid)
            .map(|(i, _)| i)
            .collect();

        if valid_indices.is_empty() {
            // All rejected — send responses and return
            let responses: Vec<PublishResult> = verified
                .iter()
                .map(|v| PublishResult {
                    seq: 0,
                    per_ref: v.per_ref.clone(),
                })
                .collect();
            for (req, resp) in batch.into_iter().zip(responses) {
                let _ = req.response.send(Ok(resp));
            }
            return Ok(());
        }

        // 4. Build log entries for valid requests. Seqs are assigned by the
        // slot claim (an orphan at head+1 may push us forward), so building
        // is a function of `first_seq`.
        let build = |first_seq: u64| -> (Vec<LogEntry>, Vec<PackRef>) {
            let mut entries = Vec::with_capacity(valid_indices.len());
            let mut new_packs = Vec::new();
            for (offset, (req, _)) in batch
                .iter()
                .zip(&verified)
                .filter(|(_, v)| v.valid)
                .enumerate()
            {
                let seq = first_seq + offset as u64;
                let pack_ref = req.pack.as_ref().map(|p| pack_ref_from_ingested(p, seq));
                if let Some(pr) = &pack_ref {
                    new_packs.push(pr.clone());
                }
                entries.push(build_log_entry_at(
                    seq,
                    EntryKind::Push,
                    pack_ref,
                    Some(&req.txn),
                    Vec::new(),
                    &req.meta,
                    &writer,
                    req.created_at,
                ));
            }
            (entries, new_packs)
        };

        // Pack uploads (idempotent, content-addressed) run alongside the slot claim.
        let upload_futures = batch.iter().filter_map(|req| {
            req.pack
                .as_ref()
                .map(|pack| upload_pack(&handle.store, pack))
        });
        let pack_uploads = async move {
            for result in futures::future::join_all(upload_futures).await {
                result?;
            }
            Ok::<(), WalError>(())
        }
        .instrument(span.clone());
        let mut frame_len = 0usize;
        let claim = claim_log_slot(&handle.store, head_seq, |first_seq| {
            let (entries, _) = build(first_seq);
            let b = frame::encode_entries(entries.iter());
            frame_len = b.len();
            b
        })
        .instrument(span.clone());
        let (pack_result, claim_result) = tokio::join!(pack_uploads, claim);
        if let Err(e) = pack_result {
            return finish_all_errors(batch, e);
        }

        // 5. The log slot: claimed (ours to commit) or contended (someone
        // committed at that seq; re-sync and retry).
        let slot = match claim_result {
            Ok(ClaimOutcome::Claimed(slot)) => slot,
            Ok(ClaimOutcome::Contended) => {
                attempts += 1;
                if attempts >= max_retries {
                    span.record("cas_retries", attempts);
                    return finish_with_error(batch, &valid_indices, WalError::Retry { attempts });
                }
                continue;
            }
            Err(e) => {
                let msg = e.to_string();
                return finish_with_error_msg(batch, &valid_indices, &msg, e);
            }
        };
        let first_seq = slot.first_seq;
        let (entries, new_packs) = build(first_seq);
        let Some(last_seq) = entries.last().map(|e| e.seq) else {
            return finish_with_error(
                batch,
                &valid_indices,
                WalError::Corrupt("empty publish log batch".into()),
            );
        };

        // 6. Build updated manifest
        let mut updated: Manifest = (*manifest).clone();
        updated.head_seq = last_seq;
        updated.packs.extend(new_packs.iter().cloned());
        updated.packs.sort_by_key(|p| p.seq);

        let seg_ref = LogSegmentRef {
            key: slot.key.clone(),
            first_seq,
            last_seq,
            size: frame_len as u64,
            sealed: true,
        };
        updated.log_segments.push(seg_ref);
        updated.log_segments.sort_by_key(|s| s.first_seq);
        updated.updated_at = Some(time::now());
        updated.writer = writer.clone();
        updated.revision += 1;

        // CAS manifest
        let buf = updated.encode_to_vec();
        let mode = match &known_version {
            Some(v) => PutMode::Update(v.clone()),
            None => PutMode::Create,
        };

        let cas = handle
            .store
            .put(
                keys::MANIFEST,
                PutBody::Bytes(bytes::Bytes::from(buf)),
                mode.into(),
            )
            .instrument(span.clone())
            .await;
        // A non-412 error is ambiguous: the bucket may have applied the CAS and
        // lost the response. Look before deciding.
        let committed: Option<(Manifest, Option<walgit_store::Version>)> = match cas {
            Ok(meta) => Some((updated, Some(meta.version))),
            Err(StoreError::PreconditionFailed { .. }) => None,
            Err(e) => match cas_landed(&handle.store, &slot)
                .instrument(span.clone())
                .await
            {
                Ok(Some(fresh)) => {
                    tracing::warn!(repo = %handle.id, seq = last_seq, "manifest CAS errored but landed: {e}");
                    Some((fresh, None))
                }
                Ok(None) => {
                    // Not committed. Leave the segment: a later writer burns past
                    // it and sweeps it; deleting here could race a lost-response
                    // commit that `cas_landed` itself failed to observe.
                    let msg = e.to_string();
                    return finish_with_error_msg(batch, &valid_indices, &msg, WalError::Store(e));
                }
                Err(e2) => {
                    let msg = format!("{e} (and re-reading the manifest failed: {e2})");
                    return finish_with_error_msg(batch, &valid_indices, &msg, WalError::Store(e));
                }
            },
        };

        if let Some((committed, version)) = committed {
            // Success! Update handle state. A landed-but-errored CAS leaves
            // us without the new version: drop our cached one so the next
            // sync refetches unconditionally.
            let version = match version {
                Some(v) => v,
                None => match handle.store.head(keys::MANIFEST).await? {
                    Some(m) => m.version,
                    None => {
                        return finish_with_error(
                            batch,
                            &valid_indices,
                            WalError::Corrupt("manifest vanished after commit".into()),
                        );
                    }
                },
            };
            // The local commit — ref txns applied, then the new manifest version advertised — happens
            // under `sync_mutex`, the lock the refs phase of every sync holds: a sync that already read
            // the committed manifest would otherwise replay the same entry concurrently (two
            // `git update-ref` on one ref → a lock collision: rig round 2447 of 2450, 2026-08-23) and a
            // reader between the two steps would see one without the other. Refs first: the
            // advertisement/ls-refs caches are keyed by the manifest version, and the reverse order let
            // a reader cache the OLD refs under the NEW version (1 round in 6 on the rig).
            //
            // The WAL commit already happened (CAS ok) and is the truth: whatever the local apply does,
            // every waiter is answered `ok`. A failed apply leaves the version unadvertised, so the next
            // sync sees a change and replays the entry — the copy repairs itself. (Answering an error
            // here produced a durable push that git reported as failed — "0 winners", commit fetchable.)
            let mut local_ok = true;
            {
                let _sync_guard = crate::lockwait::timed(
                    "sync_mutex",
                    &handle.id,
                    handle.cfg.telemetry.lock_wait_warn,
                    || handle.sync_mutex.try_lock().ok(),
                    handle.sync_mutex.lock(),
                )
                .await;
                for (req, _) in batch.iter().zip(&verified).filter(|(_, v)| v.valid) {
                    if let Err(e) = handle.local.apply_ref_txn(&req.txn, false) {
                        tracing::warn!(repo = %handle.id, seq = last_seq, error = %e, "published (CAS ok), but applying the ref txn to the local copy failed; the next sync replays it");
                        metrics::counter!("walgit_publish_local_apply_failed_total").increment(1);
                        local_ok = false;
                        break;
                    }
                }
                if local_ok && let Err(e) = handle.local.refresh_async().await {
                    tracing::warn!(repo = %handle.id, seq = last_seq, error = %e, "published (CAS ok), but refreshing the local copy failed; the next sync repairs it");
                    local_ok = false;
                }
                // Test hook: widen the gap between the two local-commit steps (harmless in this order
                // and under this lock; the poison window with the steps reversed and no lock).
                if let Some(ms) = std::env::var("WALGIT_TEST_PUBLISH_GAP_MS")
                    .ok()
                    .and_then(|v| v.parse::<u64>().ok())
                {
                    tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
                }
                if local_ok {
                    *handle.manifest.write() = Arc::new(committed.clone());
                    *handle.manifest_version.lock() = Some(version.clone());
                    {
                        let mut state = handle.state.lock();
                        state.manifest_version = Some(version.as_str().to_string());
                        state.applied_seq = last_seq;
                        let ready = state.packs_ready();
                        state.revision = committed.revision;
                        if ready {
                            state.packs_revision = committed.revision;
                        }
                    }
                    if let Err(e) =
                        crate::state::save_state(handle.local.path(), &handle.state.lock().clone())
                    {
                        tracing::warn!(repo = %handle.id, error = %e, "published (CAS ok), but saving local state failed; the next sync repairs it");
                    }
                } else {
                    // Forget the known version so the next sync performs an unconditional GET and
                    // replays from the last applied seq.
                    handle.manifest_version.lock().take();
                }
            }
            sweep_burned(&handle.store, &slot)
                .instrument(span.clone())
                .await;

            for e in &entries {
                if let Some(t) = e.created_at.as_ref() {
                    note_entry_time(handle, e.seq, t);
                }
            }
            // Fold the pushed packs' commits into the local commit-graph
            // chain (cheap, incremental; off the client's critical path).
            if !new_packs.is_empty()
                && let Some(arc) = handle.self_arc.get().cloned()
            {
                let packs = new_packs.clone();
                tokio::spawn(async move {
                    let manifest = arc.manifest();
                    crate::sync::maintain_commit_graph(&arc, &manifest, &packs).await;
                });
            }

            // Build all responses (success for valid, rejection for invalid)
            let mut responses: Vec<PublishResult> = Vec::with_capacity(batch.len());
            let mut valid_offset = 0u64;
            for v in &verified {
                if v.valid {
                    let seq = first_seq + valid_offset;
                    valid_offset += 1;
                    responses.push(PublishResult {
                        seq,
                        per_ref: v.per_ref.clone(),
                    });
                } else {
                    responses.push(PublishResult {
                        seq: 0,
                        per_ref: v.per_ref.clone(),
                    });
                }
            }

            // Consume batch and send responses
            for (req, resp) in batch.into_iter().zip(responses) {
                let _ = req.response.send(Ok(resp));
            }

            // Maybe trigger checkpoint
            maybe_trigger_checkpoint(handle, last_seq);

            span.record("seq", last_seq);
            span.record("cas_retries", attempts);
            return Ok(());
        }
        // Lost the CAS: drop exactly the segment we wrote, re-sync, retry.
        drop_own_slot(&handle.store, &slot)
            .instrument(span.clone())
            .await;
        attempts += 1;
        if attempts >= max_retries {
            span.record("cas_retries", attempts);
            return finish_with_error(batch, &valid_indices, WalError::Retry { attempts });
        }
        continue;
    }
}

/// Send error responses to ALL request senders, then return the error.
/// Used when the batch fails before the CAS loop (upload, sync, refs).
fn finish_all_errors(batch: Vec<PublishRequest>, err: WalError) -> Result<(), WalError> {
    let msg = err.to_string();
    for req in batch {
        let _ = req.response.send(Err(WalError::Corrupt(msg.clone())));
    }
    Err(err)
}
/// Send error responses to valid request senders, then return the error.
/// Converts the error to a string for each sender since `WalError` is not Clone.
fn finish_with_error(
    batch: Vec<PublishRequest>,
    valid_indices: &[usize],
    err: WalError,
) -> Result<(), WalError> {
    let msg = err.to_string();
    // Every waiter gets an answer: the valid ones the batch error, the
    // rejected ones their per-ref rejection would be lost here — report the
    // batch error too rather than dropping the channel ("publisher dropped
    // response" told the caller nothing).
    let _ = valid_indices;
    for req in batch {
        let _ = req.response.send(Err(WalError::Corrupt(msg.clone())));
    }
    Err(err)
}

fn finish_with_error_msg(
    batch: Vec<PublishRequest>,
    valid_indices: &[usize],
    msg: &str,
    err: WalError,
) -> Result<(), WalError> {
    let _ = valid_indices;
    for req in batch {
        let _ = req.response.send(Err(WalError::Corrupt(msg.to_owned())));
    }
    Err(err)
}

fn maybe_trigger_checkpoint(handle: &RepoHandle, _head_seq: u64) {
    // Opportunistic: the writer that crossed a trigger folds the log. The
    // `maintain` role covers repos nobody pushes to (age trigger).
    let due = crate::checkpoint::checkpoint_due(&handle.manifest.read(), &handle.cfg.wal);
    if let Some(trigger) = due
        && let Some(arc) = handle.self_arc.get().cloned()
    {
        tokio::spawn(async move {
            match crate::checkpoint::write_checkpoint_impl(&arc).await {
                Ok(cp) => {
                    tracing::info!(repo = %arc.id, seq = cp.seq, %trigger, "auto checkpoint written");
                }
                Err(e) => {
                    tracing::warn!(repo = %arc.id, %trigger, "auto checkpoint failed: {e}");
                }
            }
        });
    }
}

// ---- publish_compact ----

/// Every entry this process commits moves `last_entry_time`: the checkpoint's `as_of` (the time
/// of the newest folded entry, D22) is then known locally for COMPACT/SETTINGS entries too, not
/// only for pushes and replayed segments — no log read at checkpoint time.
fn note_entry_time(handle: &RepoHandle, seq: u64, at: &prost_types::Timestamp) {
    let t = time::to_system(at);
    let mut slot = handle.last_entry_time.lock();
    if slot.map(|p| t > p).unwrap_or(true) {
        *slot = Some(t);
    }
    drop(slot);
    // Seq 1 is the repository's first state: the checkpoint's `first_state_at` without a log read.
    if seq == 1 {
        *handle.first_seq_published_at.lock() = Some(t);
    }
}

pub(crate) async fn publish_compact_impl(
    handle: &RepoHandle,
    new_pack: PackInfo,
    supersedes: Vec<gix_hash::ObjectId>,
    tier: u32,
) -> Result<u64, WalError> {
    let writer = crate::handle::instance_id();
    let max_retries = handle.cfg.wal.cas_max_retries;

    // Upload pack + idx + extras without metadata existence probes. All
    // immutable objects use create-if-absent and duplicate creates are
    // harmless; run every upload concurrently.
    let checksum = new_pack.checksum.to_string();
    let pack_path = handle.local.pack_path(&new_pack.checksum);
    let idx_path = {
        let mut p = pack_path.clone();
        p.set_extension("idx");
        p
    };
    let pack_key = keys::pack_key(&checksum);
    let idx_key = keys::idx_key(&checksum);
    let mut upload_futures = vec![
        put_immutable_create(&handle.store, pack_key, pack_path.clone()),
        put_immutable_create(&handle.store, idx_key, idx_path),
    ];
    for (flag, ext, key) in [
        (new_pack.has_rev, "rev", keys::rev_key(&checksum)),
        (new_pack.has_bitmap, "bitmap", keys::bitmap_key(&checksum)),
        (
            new_pack.has_commit_graph,
            "commit-graph",
            keys::commit_graph_key(&checksum),
        ),
    ] {
        if !flag {
            continue;
        }
        let path = pack_path.with_extension(ext);
        if path.exists() {
            upload_futures.push(put_immutable_create(&handle.store, key, path));
        }
    }
    for result in futures::future::join_all(upload_futures).await {
        result?;
    }

    let pack_ref = pack_ref_from_info(&new_pack, 0, tier); // seq set below
    let supersedes_hex: Vec<String> = supersedes
        .iter()
        .map(std::string::ToString::to_string)
        .collect();

    let mut attempts = 0u32;

    loop {
        if attempts > 0 {
            handle.sync_impl().await?;
        }

        let manifest = handle.manifest.read().clone();
        let known_version = handle.manifest_version.lock().clone();

        let entry_time = time::now();
        let make_entry = |seq: u64| LogEntry {
            seq,
            kind: EntryKind::Compact as i32,
            pack: Some(PackRef {
                seq,
                ..pack_ref.clone()
            }),
            txn: None,
            supersedes: supersedes_hex.clone(),
            checkpoint: None,
            created_at: Some(entry_time),
            writer: writer.clone(),
            meta: HashMap::new(),
            settings: None,
        };
        let mut frame_len = 0usize;
        let slot = match claim_log_slot(&handle.store, manifest.head_seq, |seq| {
            let b = frame::encode_entries(std::iter::once(&make_entry(seq)));
            frame_len = b.len();
            b
        })
        .await?
        {
            ClaimOutcome::Claimed(slot) => slot,
            ClaimOutcome::Contended => {
                attempts += 1;
                if attempts >= max_retries {
                    return Err(WalError::Retry { attempts });
                }
                continue;
            }
        };
        let seq = slot.first_seq;

        // Build updated manifest
        let mut updated: Manifest = (*manifest).clone();
        updated.head_seq = seq;
        let sup_set: std::collections::HashSet<&str> = supersedes_hex
            .iter()
            .map(std::string::String::as_str)
            .collect();
        updated
            .packs
            .retain(|p| !sup_set.contains(p.checksum.as_str()) && p.checksum != pack_ref.checksum);
        updated.packs.push(PackRef {
            seq,
            ..pack_ref.clone()
        });
        updated.packs.sort_by_key(|p| p.seq);

        let seg_ref = LogSegmentRef {
            key: slot.key.clone(),
            first_seq: seq,
            last_seq: seq,
            size: frame_len as u64,
            sealed: true,
        };
        updated.log_segments.push(seg_ref);
        updated.log_segments.sort_by_key(|s| s.first_seq);
        updated.updated_at = Some(time::now());
        updated.writer = writer.clone();
        updated.revision += 1;

        let buf = updated.encode_to_vec();
        let mode = match &known_version {
            Some(v) => PutMode::Update(v.clone()),
            None => PutMode::Create,
        };

        let cas = handle
            .store
            .put(
                keys::MANIFEST,
                PutBody::Bytes(bytes::Bytes::from(buf)),
                mode.into(),
            )
            .await;
        let committed = match cas {
            Ok(meta) => Some((updated, meta.version)),
            Err(StoreError::PreconditionFailed { .. }) => None,
            Err(e) => match cas_landed(&handle.store, &slot).await {
                Ok(Some(fresh)) => {
                    tracing::warn!(repo = %handle.id, seq, "compact manifest CAS errored but landed: {e}");
                    let v = handle
                        .store
                        .head(keys::MANIFEST)
                        .await?
                        .map(|m| m.version)
                        .ok_or_else(|| {
                            WalError::Corrupt("manifest vanished after commit".into())
                        })?;
                    Some((fresh, v))
                }
                Ok(None) | Err(_) => return Err(WalError::Store(e)),
            },
        };
        if let Some((committed, version)) = committed {
            *handle.manifest.write() = Arc::new(committed.clone());
            *handle.manifest_version.lock() = Some(version.clone());
            note_entry_time(handle, seq, &entry_time);
            {
                let mut state = handle.state.lock();
                state.manifest_version = Some(version.as_str().to_string());
                state.applied_seq = seq;
                // The publisher's own superseded packs are removed by the next pack sync like
                // everyone else's (a scratch-copy base rebuild leaves them in the serving copy;
                // a geometric fold already deleted them — the removal is then a no-op).
                for s in &supersedes_hex {
                    if !state.pending_pack_removals.contains(s) {
                        state.pending_pack_removals.push(s.clone());
                    }
                }
                let ready = state.packs_ready();
                state.revision = committed.revision;
                if ready {
                    state.packs_revision = committed.revision;
                }
            }
            crate::state::save_state(handle.local.path(), &handle.state.lock().clone())?;
            sweep_burned(&handle.store, &slot).await;
            return Ok(seq);
        }
        drop_own_slot(&handle.store, &slot).await;
        attempts += 1;
        if attempts >= max_retries {
            return Err(WalError::Retry { attempts });
        }
        continue;
    }
}

/// Attach side-files to an already published pack: upload each (immutable,
/// create-if-absent) and CAS the manifest so `PackRef` advertises them
/// (`has_rev` / `has_bitmap` / `has_commit_graph`). Used to retrofit a
/// commit-graph layer onto a base that was imported before layers existed.
pub(crate) async fn annotate_pack_impl(
    handle: &RepoHandle,
    checksum: &str,
    rev: Option<std::path::PathBuf>,
    bitmap: Option<std::path::PathBuf>,
    commit_graph: Option<std::path::PathBuf>,
) -> Result<PackRef, WalError> {
    let writer = crate::handle::instance_id();
    let max_retries = handle.cfg.wal.cas_max_retries;
    handle.sync_impl_level(crate::sync::SyncLevel::Refs).await?;
    if !handle
        .manifest
        .read()
        .packs
        .iter()
        .any(|p| p.checksum == checksum)
    {
        return Err(WalError::Corrupt(format!(
            "pack {checksum} is not in the live set"
        )));
    }
    let mut uploads = Vec::new();
    if let Some(p) = &rev {
        uploads.push(put_immutable_create(
            &handle.store,
            keys::rev_key(checksum),
            p.clone(),
        ));
    }
    if let Some(p) = &bitmap {
        uploads.push(put_immutable_create(
            &handle.store,
            keys::bitmap_key(checksum),
            p.clone(),
        ));
    }
    if let Some(p) = &commit_graph {
        uploads.push(put_immutable_create(
            &handle.store,
            keys::commit_graph_key(checksum),
            p.clone(),
        ));
    }
    for r in futures::future::join_all(uploads).await {
        r?;
    }
    let mut attempts = 0u32;
    loop {
        let current = handle.manifest.read().clone();
        let known_version = handle.manifest_version.lock().clone();
        let mut updated: Manifest = (*current).clone();
        let Some(p) = updated.packs.iter_mut().find(|p| p.checksum == checksum) else {
            return Err(WalError::Corrupt(format!(
                "pack {checksum} left the live set"
            )));
        };
        if rev.is_some() {
            p.has_rev = true;
        }
        if bitmap.is_some() {
            p.has_bitmap = true;
        }
        if commit_graph.is_some() {
            p.has_commit_graph = true;
        }
        let pack_ref = p.clone();
        updated.updated_at = Some(time::now());
        updated.writer = writer.clone();
        updated.revision += 1;
        let mode = match &known_version {
            Some(v) => PutMode::Update(v.clone()),
            None => PutMode::Create,
        };
        match handle
            .store
            .put(
                keys::MANIFEST,
                PutBody::Bytes(bytes::Bytes::from(updated.encode_to_vec())),
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
                    state.revision = updated.revision;
                }
                crate::state::save_state(handle.local.path(), &handle.state.lock().clone())?;
                return Ok(pack_ref);
            }
            Err(StoreError::PreconditionFailed { .. }) => {
                attempts += 1;
                if attempts >= max_retries {
                    return Err(WalError::Retry { attempts });
                }
                handle.sync_impl_level(crate::sync::SyncLevel::Refs).await?;
            }
            Err(e) => return Err(WalError::Store(e)),
        }
    }
}

/// Publish an already built pack (+ idx) as a COMPACT entry of `tier`
/// (supersedes nothing): e.g. a history pack derived from a base that was
/// imported before D18. The files are installed into this handle's local copy
/// (hard link or copy), then `publish_compact` uploads and CASes.
pub(crate) async fn add_pack_impl(
    handle: &RepoHandle,
    pack: &std::path::Path,
    idx: &std::path::Path,
    tier: u32,
    history_of: Option<String>,
) -> Result<u64, WalError> {
    let name = pack.file_name().and_then(|n| n.to_str()).unwrap_or("");
    let hex = name
        .strip_prefix("pack-")
        .and_then(|n| n.strip_suffix(".pack"))
        .ok_or_else(|| {
            WalError::Corrupt(format!(
                "pack file must be named pack-<checksum>.pack: {name}"
            ))
        })?;
    let checksum = gix_hash::ObjectId::from_hex(hex.as_bytes())
        .map_err(|e| WalError::Corrupt(format!("bad pack name {name}: {e}")))?;
    let dest = handle.local.pack_path(&checksum);
    std::fs::create_dir_all(
        dest.parent()
            .ok_or_else(|| WalError::Corrupt("destination has no parent".into()))?,
    )?;
    for (src, dst) in [(pack, dest.clone()), (idx, dest.with_extension("idx"))] {
        if !dst.exists() && std::fs::hard_link(src, &dst).is_err() {
            std::fs::copy(src, &dst)?;
        }
    }
    if let Some(base) = &history_of {
        handle.local.mark_history_pack(&checksum, base).await?;
    }
    handle.local.refresh_async().await?;
    let info = handle
        .local
        .packs()?
        .into_iter()
        .find(|p| p.checksum == checksum)
        .ok_or_else(|| WalError::Corrupt(format!("pack {hex} not visible after install")))?;
    publish_compact_impl(handle, info, Vec::new(), tier).await
}

/// D24: publish a settings document — one SETTINGS log entry (history) and the
/// manifest's inline `settings` (what readers use), committed by the manifest
/// CAS like every other write. Round trips on the happy path: log PUT + manifest
/// CAS (2); the slot protocol handles contention like compaction does.
pub(crate) async fn publish_settings_impl(
    handle: &RepoHandle,
    toml_text: &str,
    author: &str,
    message: &str,
) -> Result<u64, WalError> {
    let writer = crate::handle::instance_id();
    let max_retries = handle.cfg.wal.cas_max_retries;
    let mut attempts = 0u32;
    loop {
        handle.sync_impl_level(crate::sync::SyncLevel::Refs).await?;
        let manifest = handle.manifest.read().clone();
        let known_version = handle.manifest_version.lock().clone();
        let revision = manifest.settings.as_ref().map_or(0, |s| s.revision) + 1;
        let settings = walgit_proto::v1::RepoSettings {
            toml: toml_text.to_string(),
            revision,
            author: author.to_string(),
            updated_at: Some(time::now()),
            message: message.to_string(),
        };
        let entry_time = time::now();
        let make_entry = |seq: u64| LogEntry {
            seq,
            kind: EntryKind::Settings as i32,
            pack: None,
            txn: None,
            supersedes: Vec::new(),
            checkpoint: None,
            created_at: Some(entry_time),
            writer: writer.clone(),
            meta: HashMap::from([
                ("author".to_string(), author.to_string()),
                ("message".to_string(), message.to_string()),
            ]),
            settings: Some(settings.clone()),
        };
        let mut frame_len = 0usize;
        let slot = match claim_log_slot(&handle.store, manifest.head_seq, |seq| {
            let b = frame::encode_entries(std::iter::once(&make_entry(seq)));
            frame_len = b.len();
            b
        })
        .await?
        {
            ClaimOutcome::Claimed(slot) => slot,
            ClaimOutcome::Contended => {
                attempts += 1;
                if attempts >= max_retries {
                    return Err(WalError::Retry { attempts });
                }
                continue;
            }
        };
        let seq = slot.first_seq;
        let mut updated: Manifest = (*manifest).clone();
        updated.head_seq = seq;
        updated.settings = Some(settings);
        updated.log_segments.push(LogSegmentRef {
            key: slot.key.clone(),
            first_seq: seq,
            last_seq: seq,
            size: frame_len as u64,
            sealed: true,
        });
        updated.log_segments.sort_by_key(|s| s.first_seq);
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
                note_entry_time(handle, seq, &entry_time);
                {
                    let mut state = handle.state.lock();
                    state.manifest_version = Some(meta.version.as_str().to_string());
                    state.applied_seq = seq;
                    state.revision = updated.revision;
                }
                crate::state::save_state(handle.local.path(), &handle.state.lock().clone())?;
                *handle.effective.lock() = None;
                sweep_burned(&handle.store, &slot).await;
                tracing::info!(repo = %handle.id, seq, revision, author, "settings published");
                return Ok(revision);
            }
            Err(StoreError::PreconditionFailed { .. }) => {
                drop_own_slot(&handle.store, &slot).await;
                attempts += 1;
                if attempts >= max_retries {
                    return Err(WalError::Retry { attempts });
                }
                continue;
            }
            Err(e) => return Err(WalError::Store(e)),
        }
    }
}
