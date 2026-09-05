//! Read log entries from the store (provenance/rewind tooling).

use walgit_proto::v1::LogEntry;
use walgit_store::{GetOptions, GetResult, ObjectStore};

use crate::error::WalError;
use crate::handle::RepoHandle;

/// Read log entries in [`from_seq`, `to_seq`]. If `to_seq` is None, read up to
/// `manifest.head_seq`.
pub(crate) async fn read_log_impl(
    handle: &RepoHandle,
    from_seq: u64,
    to_seq: Option<u64>,
) -> Result<Vec<LogEntry>, WalError> {
    // Reading the log needs a *fresh manifest*, not a synced local copy: do a
    // lock-free conditional GET and use whichever manifest is newer. Taking
    // the repo's write lock here would deadlock callers that hold a read
    // guard (overview, tests), and freshness_ttl=0 makes that the common case.
    let known = handle.manifest_version.lock().clone();
    let manifest = match crate::sync::freshness_check(&handle.store, known.as_ref()).await? {
        crate::sync::SyncOutcome::Unchanged => handle.manifest.read().clone(),
        crate::sync::SyncOutcome::Changed { manifest, .. } => manifest,
    };
    let head_seq = manifest.head_seq;
    let to = to_seq.unwrap_or(head_seq).min(head_seq);

    if from_seq > to {
        return Ok(Vec::new());
    }

    // Find relevant segments
    let segments: Vec<&walgit_proto::v1::LogSegmentRef> = manifest
        .log_segments
        .iter()
        .filter(|s| s.last_seq >= from_seq && s.first_seq <= to)
        .collect();

    let mut entries = Vec::new();
    for seg in &segments {
        let res = handle.store.get(&seg.key, GetOptions::default()).await?;
        let bytes = match res {
            GetResult::Object { meta, body } => {
                walgit_store::util::collect(
                    body,
                    usize::try_from(meta.size).map_err(|e| WalError::Corrupt(e.to_string()))?,
                )
                .await?
            }
            GetResult::NotModified { .. } => continue,
        };

        let (seg_entries, _) = walgit_proto::frame::decode_entries(&bytes)
            .map_err(|e| WalError::Corrupt(format!("log segment decode: {e}")))?;

        for entry in seg_entries {
            if entry.seq >= from_seq && entry.seq <= to {
                entries.push(entry);
            }
        }
    }

    entries.sort_by_key(|e| e.seq);
    Ok(entries)
}

/// Ref state **as of** `at`: the newest checkpoint snapshot whose seq is at
/// or before the cut, plus every log entry's ref transaction with
/// `created_at <= at`, applied in memory (pure: no local copy touched). Used
/// to cut bundles for calendar slots. Returns `(snapshot, seq)` where `seq` is
/// the last entry applied (0 = none).
pub async fn refs_as_of(
    handle: &super::handle::RepoHandle,
    at: std::time::SystemTime,
) -> Result<(walgit_proto::v1::RefSnapshot, u64), WalError> {
    replay_refs(handle, Cut::Time(at)).await
}

/// Ref state **at** WAL `seq` exactly (the newest checkpoint at or before it + every ref
/// transaction through `seq`, in memory). `Err(Corrupt)` when the log before the only checkpoint
/// is folded away (`min_seq > seq`): that state is no longer replayable. Used by the weekly
/// compose, whose header must carry the refs at the base pack's seq whatever moved since.
pub async fn refs_at_seq(
    handle: &super::handle::RepoHandle,
    seq: u64,
) -> Result<walgit_proto::v1::RefSnapshot, WalError> {
    let manifest = handle.manifest();
    let cp_ok = manifest.checkpoint.as_ref().is_some_and(|cp| cp.seq <= seq);
    if !cp_ok && manifest.min_seq > 1 && manifest.min_seq > seq {
        return Err(WalError::Corrupt(format!(
            "refs at seq {seq} are not replayable: log folded up to {} and no checkpoint at or before",
            manifest.min_seq
        )));
    }
    Ok(replay_refs(handle, Cut::Seq(seq)).await?.0)
}

enum Cut {
    Time(std::time::SystemTime),
    Seq(u64),
}

async fn replay_refs(
    handle: &super::handle::RepoHandle,
    cut: Cut,
) -> Result<(walgit_proto::v1::RefSnapshot, u64), WalError> {
    use prost::Message;
    use walgit_store::ObjectStoreExt;
    let manifest = handle.manifest();
    // Start point: checkpoint ≤ cut (checkpoint created_at on the ref; when
    // the ref has no timestamp, fall back to replaying from seq 0).
    let mut snap = walgit_proto::v1::RefSnapshot::default();
    let mut from_seq = 0u64;
    if let Some(cp) = manifest.checkpoint.as_ref() {
        // A checkpoint holds the state as of its newest folded entry (`as_of`),
        // not its write time: usable for any cut at or after that instant. The
        // times come from the manifest ref, else from the checkpoint object
        // (refs written before they carried times — a large repository's import).
        let usable = match cut {
            Cut::Seq(seq) => cp.seq <= seq,
            Cut::Time(at) => {
                handle.learn_checkpoint_times().await?;
                let times = handle.checkpoint_times();
                let cp_time = times.and_then(|t| t.as_of.or(t.created_at));
                cp_time.is_some_and(|t| t <= at)
            }
        };
        if usable {
            if let Some((_, bytes)) = handle.store().get_bytes(&cp.key).await? {
                let cpo = walgit_proto::v1::Checkpoint::decode(bytes.as_ref())
                    .map_err(|e| WalError::Corrupt(format!("checkpoint decode: {e}")))?;
                if let Some((_, rb)) = handle.store().get_bytes(&cpo.refs_key).await? {
                    snap = walgit_proto::v1::RefSnapshot::decode(rb.as_ref())
                        .map_err(|e| WalError::Corrupt(format!("refs decode: {e}")))?;
                    from_seq = cp.seq;
                }
            }
        } else if manifest.min_seq > 1 {
            // History before the checkpoint is folded and the checkpoint is
            // newer than the cut: the best we can do is the checkpoint state
            // (the cut predates what is replayable). Callers treat seq 0 as
            // "nothing at that time".
        }
    }
    let to_seq = match cut {
        Cut::Seq(seq) => Some(seq),
        Cut::Time(_) => None,
    };
    let entries = handle.read_log(from_seq + 1, to_seq).await?;
    let mut map: std::collections::BTreeMap<String, walgit_proto::v1::Ref> =
        snap.refs.into_iter().map(|r| (r.name.clone(), r)).collect();
    let mut head_target = snap.head_target;
    let mut last_seq = from_seq;
    for e in &entries {
        match cut {
            Cut::Time(at) => {
                let t = e.created_at.as_ref().map(walgit_proto::time::to_system);
                if t.is_some_and(|t| t > at) {
                    break;
                }
            }
            Cut::Seq(seq) => {
                if e.seq > seq {
                    break;
                }
            }
        }
        if let Some(txn) = &e.txn {
            for u in &txn.updates {
                if !u.new_symbolic_target.is_empty() {
                    if u.name == "HEAD" {
                        head_target.clone_from(&u.new_symbolic_target);
                    }
                    continue;
                }
                let zero = u.new_oid.is_empty() || u.new_oid.chars().all(|c| c == '0');
                if zero {
                    map.remove(&u.name);
                } else {
                    map.insert(
                        u.name.clone(),
                        walgit_proto::v1::Ref {
                            name: u.name.clone(),
                            oid: u.new_oid.clone(),
                            peeled: u.new_peeled.clone(),
                        },
                    );
                }
            }
        }
        last_seq = e.seq;
    }
    Ok((
        walgit_proto::v1::RefSnapshot {
            seq: last_seq,
            object_format: manifest.object_format.clone(),
            refs: map.into_values().collect(),
            head_target,
            created_at: None,
        },
        last_seq,
    ))
}
