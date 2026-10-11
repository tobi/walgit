//! Metadata publication for already committed packs. Graph closure/conservation
//! starts with the producer; final retirement also verifies indexed conservation
//! and current-tip membership against each captured CAS basis.
use crate::{CoverageSnapshot, RepoHandle, WalError};
use prost::Message;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use walgit_proto::v1::{Manifest, PackAudience, PackGroupCoverage, PackKind, PackRef};
use walgit_store::{ObjectStore, PutBody, PutMode, StoreError};

#[derive(Clone, Debug, Default, PartialEq)]
pub struct PackClassification {
    pub checksum: String,
    pub kind: i32,
    pub audience: i32,
    pub ref_policy: String,
    pub pack_groups: Vec<String>,
    pub covers_seq: u64,
    pub coverage_refs_key: String,
    pub group_coverages: Vec<PackGroupCoverage>,
}

impl PackClassification {
    pub fn from_pack(pack: &PackRef) -> Self {
        Self {
            checksum: pack.checksum.clone(),
            kind: pack.kind,
            audience: pack.audience,
            ref_policy: pack.ref_policy.clone(),
            pack_groups: pack.pack_groups.clone(),
            covers_seq: pack.covers_seq,
            coverage_refs_key: pack.coverage_refs_key.clone(),
            group_coverages: pack.group_coverages.clone(),
        }
    }

    pub(crate) fn apply(&self, pack: &mut PackRef) {
        pack.kind = self.kind;
        pack.audience = self.audience;
        pack.ref_policy.clone_from(&self.ref_policy);
        pack.pack_groups.clone_from(&self.pack_groups);
        pack.covers_seq = self.covers_seq;
        pack.coverage_refs_key.clone_from(&self.coverage_refs_key);
        pack.group_coverages.clone_from(&self.group_coverages);
    }
}

fn invalid(message: impl Into<String>) -> WalError {
    WalError::Invalid(message.into())
}

fn audience_matches(
    pack: &PackRef,
    kind: walgit_config::PackGroupKind,
    cfg: &walgit_config::Config,
) -> bool {
    let code = pack.pack_groups.iter().any(|name| {
        cfg.refs
            .packfiles
            .get(name)
            .is_some_and(|g| g.kind == walgit_config::PackGroupKind::Code)
    });
    match kind {
        walgit_config::PackGroupKind::Code => pack.audience == PackAudience::Code as i32,
        walgit_config::PackGroupKind::Meta => {
            pack.audience
                == if code {
                    PackAudience::Code as i32
                } else {
                    PackAudience::Meta as i32
                }
        }
    }
}

/// Removing/reclassifying inputs revokes surviving certificates that no longer
/// describe a complete set. It does not delete their packs or rewrite bytes.
pub(crate) fn prune_invalid_coverages(
    manifest: &mut Manifest,
    cfg: &walgit_config::Config,
    changed: &[String],
) {
    let policy = cfg.refs.policy_identity();
    loop {
        let before = manifest.clone();
        let mut removed = false;
        for carrier in &mut manifest.packs {
            if changed.contains(&carrier.checksum) {
                continue;
            }
            carrier.group_coverages.retain(|proof| {
                let valid = proof.ref_policy == policy
                    && cfg.refs.packfiles.get(&proof.group).is_some_and(|group| {
                        !proof.packs.is_empty()
                            && proof.packs.iter().all(|id| {
                                before.packs.iter().any(|p| {
                                    p.checksum == *id
                                        && p.pack_groups.contains(&proof.group)
                                        && p.ref_policy == policy
                                        && p.covers_seq == proof.covers_seq
                                        && p.coverage_refs_key == proof.refs_key
                                })
                            })
                            && group.subtract.iter().all(|dependency| {
                                before
                                    .packs
                                    .iter()
                                    .flat_map(|p| &p.group_coverages)
                                    .any(|c| {
                                        c.group == *dependency
                                            && c.ref_policy == policy
                                            && c.covers_seq == proof.covers_seq
                                            && c.refs_key == proof.refs_key
                                    })
                            })
                    });
                removed |= !valid;
                valid
            });
        }
        if !removed {
            break;
        }
    }
}

/// One validator for COMPACT and metadata CAS attempts. Candidates have no
/// authority before this CAS; existing snapshot keys must already be committed.
pub(crate) async fn validate_certificates(
    handle: &RepoHandle,
    before: &Manifest,
    after: &Manifest,
    changed: &[String],
    candidates: &[CoverageSnapshot],
) -> Result<(), WalError> {
    let cfg = handle.validated_config_for_manifest(before)?;
    let policy = cfg.refs.policy_identity();
    let live: BTreeMap<&str, &PackRef> = after
        .packs
        .iter()
        .map(|p| (p.checksum.as_str(), p))
        .collect();
    let proofs: Vec<&PackGroupCoverage> = after
        .packs
        .iter()
        .flat_map(|p| &p.group_coverages)
        .collect();
    let mut validated = BTreeSet::new();
    let mut snapshots: BTreeMap<String, walgit_proto::v1::RefSnapshot> = BTreeMap::new();
    for checksum in changed {
        let pack = live
            .get(checksum.as_str())
            .ok_or_else(|| invalid("classified pack is no longer live"))?;
        if PackKind::try_from(pack.kind).is_err() || PackAudience::try_from(pack.audience).is_err()
        {
            return Err(invalid("unknown pack classification"));
        }
        let unique: BTreeSet<_> = pack.pack_groups.iter().collect();
        if unique.len() != pack.pack_groups.len() {
            return Err(invalid("duplicate pack group"));
        }
        for group in &pack.pack_groups {
            if group == "_retained" {
                if pack.audience != PackAudience::Retained as i32
                    || !pack.group_coverages.is_empty()
                {
                    return Err(invalid("retained packs cannot carry group coverage"));
                }
                continue;
            }
            let definition = cfg
                .refs
                .packfiles
                .get(group)
                .ok_or_else(|| invalid(format!("unknown pack group {group}")))?;
            if pack.ref_policy != policy || !audience_matches(pack, definition.kind, &cfg) {
                return Err(invalid(
                    "pack classification has stale policy or wrong audience",
                ));
            }
        }
        for coverage in &pack.group_coverages {
            if !pack.pack_groups.contains(&coverage.group)
                || pack.coverage_refs_key != coverage.refs_key
                || !coverage.packs.contains(&pack.checksum)
            {
                return Err(invalid("coverage carrier is outside its certificate"));
            }
            let mut pending = vec![coverage];
            while let Some(proof) = pending.pop() {
                if !validated.insert((
                    proof.group.clone(),
                    proof.covers_seq,
                    proof.refs_key.clone(),
                    proof.packs.clone(),
                )) {
                    continue;
                }
                if proof.ref_policy != policy
                    || proof.covers_seq > before.head_seq
                    || proof.refs_key.is_empty()
                {
                    return Err(invalid("coverage policy or captured generation is invalid"));
                }
                let definition = cfg
                    .refs
                    .packfiles
                    .get(&proof.group)
                    .ok_or_else(|| invalid("coverage names an unknown group"))?;
                let members: BTreeSet<_> = proof.packs.iter().collect();
                if members.is_empty() || members.len() != proof.packs.len() {
                    return Err(invalid("coverage members are empty or duplicated"));
                }
                for member in &proof.packs {
                    let p = live
                        .get(member.as_str())
                        .ok_or_else(|| invalid(format!("coverage member {member} is not live")))?;
                    if !p.pack_groups.contains(&proof.group)
                        || p.ref_policy != policy
                        || p.covers_seq != proof.covers_seq
                        || p.coverage_refs_key != proof.refs_key
                    {
                        return Err(invalid("coverage member scope changed"));
                    }
                    if !audience_matches(p, definition.kind, &cfg) {
                        return Err(invalid("coverage member audience changed"));
                    }
                }
                let snapshot = if let Some(cached) = snapshots.get(&proof.refs_key) {
                    cached.clone()
                } else if let Some(candidate) = candidates.iter().find(|s| s.key == proof.refs_key)
                {
                    if candidate.repo != before.repo {
                        return Err(invalid("coverage candidate belongs to another repository"));
                    }
                    candidate.snapshot.clone()
                } else {
                    let committed = before
                        .packs
                        .iter()
                        .flat_map(|p| &p.group_coverages)
                        .any(|p| p.refs_key == proof.refs_key && p.covers_seq == proof.covers_seq)
                        || before.checkpoint.as_ref().is_some_and(|cp| {
                            cp.refs_key == proof.refs_key && cp.seq == proof.covers_seq
                        });
                    if !committed {
                        return Err(invalid(
                            "coverage snapshot is not committed or supplied by its producer",
                        ));
                    }
                    crate::snapshots::read_snapshot(
                        &handle.store,
                        &proof.refs_key,
                        proof.covers_seq,
                        &before.object_format,
                    )
                    .await?
                };
                snapshots.insert(proof.refs_key.clone(), snapshot.clone());
                if snapshot.seq != proof.covers_seq
                    || snapshot.object_format != before.object_format
                {
                    return Err(invalid("coverage snapshot generation mismatch"));
                }
                for dependency in &definition.subtract {
                    let dependency_proof = proofs
                        .iter()
                        .find(|p| {
                            p.group == *dependency
                                && p.covers_seq == proof.covers_seq
                                && p.refs_key == proof.refs_key
                                && p.ref_policy == policy
                        })
                        .ok_or_else(|| {
                            invalid(format!(
                                "missing same-generation coverage for dependency {dependency}"
                            ))
                        })?;
                    if proofs.iter().any(|p| {
                        p.group == *dependency
                            && p.covers_seq == proof.covers_seq
                            && p.refs_key == proof.refs_key
                            && p.ref_policy == policy
                            && p.packs != dependency_proof.packs
                    }) {
                        return Err(invalid("conflicting dependency coverage"));
                    }
                    pending.push(*dependency_proof);
                }
            }
        }
    }
    Ok(())
}

impl RepoHandle {
    /// Reclassify a bounded batch without changing WAL sequence or pack bytes.
    /// Every retry proves membership and policy against its fresh CAS basis.
    pub async fn reclassify_packs(
        &self,
        changes: &[PackClassification],
        candidates: &[CoverageSnapshot],
    ) -> Result<(), WalError> {
        if changes.is_empty() {
            return Ok(());
        }
        if changes.len() > 128 || candidates.len() > 128 {
            return Err(invalid("classification batch exceeds 128"));
        }
        let ids: Vec<_> = changes.iter().map(|c| c.checksum.clone()).collect();
        if ids.iter().collect::<BTreeSet<_>>().len() != ids.len() {
            return Err(invalid("duplicate classification checksum"));
        }
        for attempt in 0..self.cfg.wal.cas_max_retries {
            self.sync_impl_level(crate::SyncLevel::Refs).await?;
            let (current, version) = self.manifest_pair();
            let mut updated = (*current).clone();
            for change in changes {
                let pack = updated
                    .packs
                    .iter_mut()
                    .find(|p| p.checksum == change.checksum)
                    .ok_or_else(|| invalid("classification input was retired"))?;
                change.apply(pack);
            }
            let cfg = self.validated_config_for_manifest(&current)?;
            prune_invalid_coverages(&mut updated, &cfg, &ids);
            validate_certificates(self, &current, &updated, &ids, candidates).await?;
            updated.revision += 1;
            updated.updated_at = Some(walgit_proto::time::now());
            updated.writer = crate::handle::instance_id();
            let mode = version.map_or(PutMode::Create, PutMode::Update);
            match self
                .store
                .put(
                    walgit_proto::keys::MANIFEST,
                    PutBody::Bytes(updated.encode_to_vec().into()),
                    mode.into(),
                )
                .await
            {
                Ok(meta) => {
                    let _sync = self.sync_mutex.lock().await;
                    if self.adopt_manifest(Arc::new(updated.clone()), meta.version.clone()) {
                        let mut state = self.state.lock();
                        state.manifest_version = Some(meta.version.as_str().to_string());
                        state.revision = updated.revision;
                    }
                    return Ok(());
                }
                Err(StoreError::PreconditionFailed { .. }) => {
                    tokio::time::sleep(std::time::Duration::from_millis(
                        5 + u64::from(attempt) * 7,
                    ))
                    .await;
                }
                Err(e) => return Err(e.into()),
            }
        }
        Err(WalError::Retry {
            attempts: self.cfg.wal.cas_max_retries,
        })
    }
}

impl RepoHandle {
    /// Complete a conserving multi-output replacement. Outputs must already be
    /// committed; until this CAS, all original inputs remain live. The producer
    /// validates raw links; the seal independently checks indexed conservation
    /// and all captured current tips before each CAS attempt.
    pub async fn seal_pack_replacement(
        &self,
        captured: &crate::PublicationView,
        inputs: &[String],
        outputs: &[PackRef],
        snapshots: &[CoverageSnapshot],
        lease: &tokio::sync::Mutex<walgit_store::coord::LeaseGuard>,
    ) -> Result<u64, WalError> {
        use crate::publish::{
            ClaimOutcome, SlotEvidence, claim_log_slot, drop_own_slot, slot_evidence, sweep_burned,
        };
        use walgit_proto::v1::{EntryKind, LogEntry, LogSegmentRef};
        use walgit_proto::{frame, keys, time};
        if inputs.is_empty() || outputs.is_empty() || inputs.len() > 4096 || outputs.len() > 4096 {
            return Err(invalid(
                "replacement needs bounded, nonempty input/output sets",
            ));
        }
        if captured.manifest.repo != self.id.to_string() {
            return Err(invalid("replacement input belongs to another repository"));
        }
        let output_ids: Vec<_> = outputs.iter().map(|p| p.checksum.clone()).collect();
        if inputs.iter().collect::<BTreeSet<_>>().len() != inputs.len()
            || output_ids.iter().collect::<BTreeSet<_>>().len() != output_ids.len()
        {
            return Err(invalid("duplicate replacement input/output"));
        }
        let retire: Vec<_> = inputs
            .iter()
            .filter(|id| !output_ids.contains(id))
            .cloned()
            .collect();
        for attempt in 0..self.cfg.wal.cas_max_retries {
            lease
                .lock()
                .await
                .heartbeat(captured.config.packs.lease_ttl)
                .await
                .map_err(|e| invalid(format!("replacement lease: {e}")))?;
            let view = self.publication_view().await?;
            let current = view.manifest;
            let version = view.version;
            let cfg = self.validated_config_for_manifest(&current)?;
            if cfg.refs.policy_identity() != captured.config.refs.policy_identity() {
                return Err(invalid("packing policy changed during replacement"));
            }
            // Recovery after a lost successful final response. Exact outputs
            // and retirement, not a local ledger, witness the already-sealed job.
            if !retire.is_empty()
                && retire.iter().all(|id| {
                    current.retired_packs.iter().any(|p| p.checksum == *id)
                        && !current.packs.iter().any(|p| p.checksum == *id)
                })
                && outputs.iter().all(|out| {
                    current.packs.iter().any(|p| {
                        PackClassification::from_pack(p) == PackClassification::from_pack(out)
                            && p.tier == out.tier
                            && p.derived_from == out.derived_from
                            && same_pack_bytes(p, out)
                    })
                })
            {
                return Ok(current.head_seq);
            }
            for id in inputs {
                let original = captured
                    .manifest
                    .packs
                    .iter()
                    .find(|p| p.checksum == *id)
                    .ok_or_else(|| invalid("replacement input was never captured"))?;
                let live = current
                    .packs
                    .iter()
                    .find(|p| p.checksum == *id)
                    .ok_or_else(|| invalid("replacement input retired during work"))?;
                if PackClassification::from_pack(original) != PackClassification::from_pack(live)
                    || original.derived_from != live.derived_from
                    || original.tier != live.tier
                    || !same_pack_bytes(original, live)
                {
                    return Err(invalid("replacement input scope changed during work"));
                }
            }
            let mut updated = (*current).clone();
            for out in outputs {
                let pack = updated
                    .packs
                    .iter_mut()
                    .find(|p| p.checksum == out.checksum)
                    .ok_or_else(|| invalid("replacement output is not committed"))?;
                if !same_pack_bytes(pack, out) {
                    return Err(invalid("replacement output descriptor changed"));
                }
                PackClassification::from_pack(out).apply(pack);
                pack.tier = out.tier;
                pack.derived_from.clone_from(&out.derived_from);
                if pack.published_at.is_none() {
                    pack.published_at = Some(time::now());
                }
            }
            // Validate before claiming a log slot: incomplete outputs never
            // create even an orphan retirement record.
            let prospective_seq = current.head_seq.saturating_add(1);
            updated.retire_packs(&retire, prospective_seq);
            updated.packs.retain(|p| !retire.contains(&p.checksum));
            prune_invalid_coverages(&mut updated, &cfg, &output_ids);
            validate_certificates(self, &current, &updated, &output_ids, snapshots).await?;
            crate::closure::verify_replacement_inventory(
                self, &current, &updated, &view.refs, inputs, outputs,
            )
            .await?;
            let at = time::now();
            let slot = match claim_log_slot(&self.store, current.head_seq, |seq| {
                let entry = LogEntry {
                    seq,
                    kind: EntryKind::Compact as i32,
                    supersedes: retire.clone(),
                    created_at: Some(at),
                    writer: crate::handle::instance_id(),
                    ..Default::default()
                };
                frame::encode_entries(std::iter::once(&entry))
            })
            .await?
            {
                ClaimOutcome::Claimed(slot) => slot,
                ClaimOutcome::Contended => continue,
            };
            let seq = slot.first_seq;
            for retired in &mut updated.retired_packs {
                if retire.contains(&retired.checksum)
                    && !current
                        .retired_packs
                        .iter()
                        .any(|p| p.checksum == retired.checksum)
                {
                    retired.retired_seq = seq;
                }
            }
            updated.head_seq = seq;
            updated.revision += 1;
            updated.updated_at = Some(at);
            updated.writer = crate::handle::instance_id();
            updated.log_segments.push(LogSegmentRef {
                key: slot.key.clone(),
                first_seq: seq,
                last_seq: seq,
                size: slot.bytes.len() as u64,
                sealed: true,
            });
            let mode = version.map_or(PutMode::Create, PutMode::Update);
            let result = self
                .store
                .put(
                    keys::MANIFEST,
                    PutBody::Bytes(updated.encode_to_vec().into()),
                    mode.into(),
                )
                .await;
            let committed = match result {
                Ok(meta) => Some((updated, meta.version)),
                // A 412 may answer a transport retry of our own landed CAS: look before
                // dropping the segment (see `publish::process_batch`).
                Err(StoreError::PreconditionFailed { .. }) => {
                    match slot_evidence(&self.store, &slot).await? {
                        SlotEvidence::Committed(fresh, version) => {
                            tracing::warn!(repo = %self.id, seq, "seal manifest CAS answered 412 but landed");
                            Some((*fresh, version))
                        }
                        SlotEvidence::Unlisted => None,
                        SlotEvidence::Unknown => {
                            return Err(WalError::CommitUnknown(format!(
                                "seal manifest CAS answered 412 and the manifest no longer shows whether seq {seq} landed"
                            )));
                        }
                    }
                }
                Err(error) => match crate::publish::cas_landed(&self.store, &slot).await? {
                    Some(pair) => Some(pair),
                    None => return Err(error.into()),
                },
            };
            if let Some((committed, version)) = committed {
                let _sync = self.sync_mutex.lock().await;
                let applied = self.state.lock().applied_seq;
                if committed.head_seq > seq && applied < committed.head_seq {
                    crate::sync::apply_delta(self, &committed, &version).await?;
                }
                if self.adopt_manifest(Arc::new(committed.clone()), version.clone()) {
                    let mut state = self.state.lock();
                    state.manifest_version = Some(version.as_str().to_string());
                    state.applied_seq = committed.head_seq;
                    state.revision = committed.revision;
                    for id in &retire {
                        if !state.pending_pack_removals.contains(id) {
                            state.pending_pack_removals.push(id.clone());
                        }
                    }
                }
                sweep_burned(&self.store, &slot).await;
                return Ok(seq);
            }
            drop_own_slot(&self.store, &slot).await;
            tokio::time::sleep(std::time::Duration::from_millis(5 + u64::from(attempt) * 7)).await;
        }
        Err(WalError::Retry {
            attempts: self.cfg.wal.cas_max_retries,
        })
    }
}

fn same_pack_bytes(left: &PackRef, right: &PackRef) -> bool {
    left.checksum == right.checksum
        && left.pack_size == right.pack_size
        && left.idx_size == right.idx_size
        && left.object_count == right.object_count
        && left.has_rev == right.has_rev
        && left.has_bitmap == right.has_bitmap
        && left.has_commit_graph == right.has_commit_graph
}
