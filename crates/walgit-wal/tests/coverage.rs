#![allow(clippy::unwrap_used, clippy::field_reassign_with_default)]
//! Authority tests use small manifest fixtures. Retirement tests also use real
//! Git indexes to exercise conservation independently of producer assertions.
use prost::Message;
use std::sync::Arc;
use walgit_config::{Config, PackGroupConfig, PackGroupKind};
use walgit_git::{ObjectFormat, RepoId};
use walgit_proto::v1::{
    Checkpoint, CheckpointRef, Manifest, PackAudience, PackGroupCoverage, PackKind, PackRef, Ref,
    RefSnapshot,
};
use walgit_store::{ObjectStoreExt, PutMode, memory::MemoryStore};
use walgit_wal::{CoverageSnapshot, PackClassification, Registry, RepoHandle};

fn config(dir: &std::path::Path) -> Config {
    let mut cfg = Config::default();
    cfg.cache.dir = dir.to_path_buf();
    cfg.wal.freshness_ttl = std::time::Duration::ZERO;
    cfg
}
fn id() -> RepoId {
    RepoId::new("test", "coverage").unwrap()
}
fn pack(n: u8) -> PackRef {
    PackRef {
        checksum: format!("{n:040x}"),
        seq: 1,
        kind: PackKind::Objects as i32,
        ..Default::default()
    }
}
async fn seed(store: &Arc<MemoryStore>, manifest: &Manifest) {
    store
        .put_bytes(
            &format!("{}manifest.pb", id().store_prefix()),
            manifest.encode_to_vec(),
            PutMode::Overwrite,
        )
        .await
        .unwrap();
}
async fn fresh(store: &Arc<MemoryStore>, cfg: Config) -> Arc<RepoHandle> {
    Registry::new(store.clone(), Arc::new(cfg))
        .open(&id())
        .await
        .unwrap()
}
fn classify(
    pack: &PackRef,
    group: &str,
    policy: &str,
    snapshot: &CoverageSnapshot,
    members: &[PackRef],
) -> PackClassification {
    PackClassification {
        checksum: pack.checksum.clone(),
        kind: PackKind::Objects as i32,
        audience: PackAudience::Code as i32,
        ref_policy: policy.to_string(),
        pack_groups: vec![group.to_string()],
        covers_seq: snapshot.snapshot().seq,
        coverage_refs_key: snapshot.key().to_string(),
        group_coverages: vec![PackGroupCoverage {
            group: group.to_string(),
            ref_policy: policy.to_string(),
            covers_seq: snapshot.snapshot().seq,
            packs: members.iter().map(|p| p.checksum.clone()).collect(),
            refs_key: snapshot.key().to_string(),
        }],
    }
}

async fn real_pack(store: &Arc<MemoryStore>, blobs: &[&str]) -> PackRef {
    use std::io::Write;
    use std::process::{Command, Stdio};
    let dir = tempfile::tempdir().unwrap();
    let run = |args: &[&str], input: &str| {
        let mut child = Command::new("git")
            .arg("-C")
            .arg(dir.path())
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        child
            .stdin
            .take()
            .unwrap()
            .write_all(input.as_bytes())
            .unwrap();
        let output = child.wait_with_output().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap().trim().to_owned()
    };
    run(&["init", "--bare"], "");
    let objects = blobs
        .iter()
        .map(|blob| run(&["hash-object", "-w", "--stdin"], blob))
        .collect::<Vec<_>>()
        .join("\n");
    let checksum = run(
        &["pack-objects", "objects/pack/pack"],
        &format!("{objects}\n"),
    );
    let pack = std::fs::read(
        dir.path()
            .join(format!("objects/pack/pack-{checksum}.pack")),
    )
    .unwrap();
    let index =
        std::fs::read(dir.path().join(format!("objects/pack/pack-{checksum}.idx"))).unwrap();
    let descriptor = PackRef {
        checksum: checksum.clone(),
        pack_size: pack.len() as u64,
        idx_size: index.len() as u64,
        object_count: blobs.len() as u64,
        ..Default::default()
    };
    for (ext, bytes) in [("pack", pack), ("idx", index)] {
        store
            .put_bytes(
                &format!("{}wal/{checksum}.{ext}", id().store_prefix()),
                bytes,
                PutMode::Overwrite,
            )
            .await
            .unwrap();
    }
    descriptor
}

// Seed an exact committed checkpoint, including intentionally damaged ref
// provenance, without using the publication API whose admission is under test.
async fn seed_next_refs(store: &Arc<MemoryStore>, h: &RepoHandle, refs: Vec<Ref>) {
    let mut manifest = (*h.manifest()).clone();
    let seq = manifest.head_seq + 1;
    let snapshot = RefSnapshot {
        seq,
        object_format: "sha1".into(),
        refs,
        ..Default::default()
    };
    let refs_key = format!("checkpoints/{seq}/refs.pb");
    store
        .put_bytes(
            &format!("{}{refs_key}", id().store_prefix()),
            snapshot.encode_to_vec(),
            PutMode::Create,
        )
        .await
        .unwrap();
    manifest.head_seq = seq;
    manifest.min_seq = seq + 1;
    manifest.log_segments.clear();
    manifest.revision += 1;
    manifest.checkpoint = Some(CheckpointRef {
        seq,
        key: format!("checkpoints/{seq}/checkpoint.pb"),
        refs_key,
        ..Default::default()
    });
    seed(store, &manifest).await;
}

#[tokio::test]
async fn replacement_seal_requires_exact_outputs_and_keeps_inputs_until_success() {
    let store = MemoryStore::shared();
    let dir = tempfile::tempdir().unwrap();
    let registry = Registry::new(store.clone(), Arc::new(config(dir.path())));
    let h = registry.create(&id(), ObjectFormat::Sha1).await.unwrap();
    h.publish_settings("", "test", "generation").await.unwrap();
    let mut manifest = (*h.manifest()).clone();
    let input = real_pack(&store, &["conserved"]).await;
    let mut output = real_pack(&store, &["conserved", "extra"]).await;
    output.published_at = Some(walgit_proto::time::now());
    output.pack_groups = vec!["_retained".into()];
    output.audience = PackAudience::Retained as i32;
    output.ref_policy = h.effective_config().refs.policy_identity();
    manifest.packs = vec![input.clone()];
    manifest.revision += 1;
    seed(&store, &manifest).await;
    let captured = h.publication_view().await.unwrap();
    // The producer commits output bytes additively before asking to retire inputs.
    manifest.packs.push(output.clone());
    manifest.revision += 1;
    seed(&store, &manifest).await;
    let lease = tokio::sync::Mutex::new(
        walgit_store::coord::try_acquire(
            store.clone(),
            "test-seal-lease",
            "test",
            "seal",
            std::time::Duration::from_mins(1),
        )
        .await
        .unwrap()
        .unwrap(),
    );
    let inputs = vec![input.checksum.clone()];
    let mut wrong = output.clone();
    wrong.idx_size += 1;
    let error = h
        .seal_pack_replacement(&captured, &inputs, &[wrong], &[], &lease)
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("output descriptor changed"),
        "{error}"
    );
    assert!(
        h.manifest()
            .packs
            .iter()
            .any(|p| p.checksum == input.checksum)
    );
    assert!(h.manifest().retired_packs.is_empty());
    // Even plausible descriptors and already-committed bytes cannot justify
    // dropping an unreachable indexed object from the captured input.
    let lossy = real_pack(&store, &["unrelated"]).await;
    manifest.packs.push(lossy.clone());
    manifest.revision += 1;
    seed(&store, &manifest).await;
    let error = h
        .seal_pack_replacement(
            &captured,
            &inputs,
            std::slice::from_ref(&lossy),
            &[],
            &lease,
        )
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("loses indexed object"),
        "{error}"
    );
    assert!(h.manifest().retired_packs.is_empty());
    seed_next_refs(
        &store,
        &h,
        vec![Ref {
            name: "refs/heads/new-after-plan".into(),
            oid: "a".repeat(40),
            ..Default::default()
        }],
    )
    .await;
    let error = h
        .seal_pack_replacement(&captured, &inputs, &[output.clone()], &[], &lease)
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("does not cover current tip"),
        "{error}"
    );
    assert!(h.manifest().retired_packs.is_empty());
    seed_next_refs(&store, &h, vec![]).await;
    let seq = h
        .seal_pack_replacement(&captured, &inputs, &[output.clone()], &[], &lease)
        .await
        .unwrap();
    assert_eq!(h.manifest().packs, vec![output.clone(), lossy]);
    assert!(
        h.manifest()
            .retired_packs
            .iter()
            .any(|p| p.checksum == input.checksum)
    );
    assert_eq!(
        h.seal_pack_replacement(&captured, &inputs, &[output], &[], &lease)
            .await
            .unwrap(),
        seq
    );
}

/// A client library retrying the seal's manifest CAS after its first reply was lost gets 412 from
/// the seal's own landed write. Like every other publisher, the seal must find its segment listed
/// and keep it: deleting it leaves the committed manifest pointing at a missing log segment.
#[tokio::test]
async fn a_landed_seal_answered_412_keeps_the_commit() {
    use walgit_store::fault::{FaultPlan, FaultStore};
    let store = MemoryStore::shared();
    let dir = tempfile::tempdir().unwrap();
    let link = FaultStore::new(store.clone(), "seal-landed-412", 1);
    let registry = Registry::new(link.clone(), Arc::new(config(dir.path())));
    let h = registry.create(&id(), ObjectFormat::Sha1).await.unwrap();
    h.publish_settings("", "test", "generation").await.unwrap();
    let mut manifest = (*h.manifest()).clone();
    let input = real_pack(&store, &["conserved"]).await;
    let mut output = real_pack(&store, &["conserved", "extra"]).await;
    output.published_at = Some(walgit_proto::time::now());
    output.pack_groups = vec!["_retained".into()];
    output.audience = PackAudience::Retained as i32;
    output.ref_policy = h.effective_config().refs.policy_identity();
    manifest.packs = vec![input.clone()];
    manifest.revision += 1;
    seed(&store, &manifest).await;
    let captured = h.publication_view().await.unwrap();
    manifest.packs.push(output.clone());
    manifest.revision += 1;
    seed(&store, &manifest).await;
    let lease = tokio::sync::Mutex::new(
        walgit_store::coord::try_acquire(
            store.clone(),
            "test-seal-lease",
            "test",
            "seal",
            std::time::Duration::from_mins(1),
        )
        .await
        .unwrap()
        .unwrap(),
    );
    link.set(FaultPlan {
        p_cas_fail_after: 1.0,
        only_keys: Some(vec!["manifest.pb".into()]),
        ..Default::default()
    });
    let sealed = h
        .seal_pack_replacement(
            &captured,
            std::slice::from_ref(&input.checksum),
            std::slice::from_ref(&output),
            &[],
            &lease,
        )
        .await;
    link.heal();
    let (_, bytes) = store
        .get_bytes(&format!("{}manifest.pb", id().store_prefix()))
        .await
        .unwrap()
        .unwrap();
    let committed = Manifest::decode(bytes.as_ref()).unwrap();
    for segment in &committed.log_segments {
        assert!(
            store
                .get_bytes(&format!("{}{}", id().store_prefix(), segment.key))
                .await
                .unwrap()
                .is_some(),
            "manifest lists {} but it is gone",
            segment.key
        );
    }
    let seq = sealed.expect("a durable seal must be acknowledged");
    assert_eq!(committed.head_seq, seq);
    assert!(
        committed
            .retired_packs
            .iter()
            .any(|p| p.checksum == input.checksum)
    );
    let reader_dir = tempfile::tempdir().unwrap();
    let reader = fresh(&store, config(reader_dir.path())).await;
    drop(reader.sync_refs().await.unwrap());
    assert_eq!(reader.manifest().head_seq, seq);
}

#[tokio::test]
async fn exact_snapshot_is_not_authority_until_cas_and_corruption_is_rejected() {
    let store = MemoryStore::shared();
    let d = tempfile::tempdir().unwrap();
    let registry = Registry::new(store.clone(), Arc::new(config(d.path())));
    let h = registry.create(&id(), ObjectFormat::Sha1).await.unwrap();
    h.publish_settings("", "test", "generation one")
        .await
        .unwrap();
    let mut manifest = (*h.manifest()).clone();
    manifest.packs = vec![pack(1)];
    manifest.revision += 1;
    seed(&store, &manifest).await;
    let view = h.publication_view().await.unwrap();
    let snapshot = h.prepare_coverage_snapshot(&view).await.unwrap();
    assert!(h.manifest().packs[0].group_coverages.is_empty());
    let c = classify(
        &pack(1),
        "code",
        &view.config.refs.policy_identity(),
        &snapshot,
        &[pack(1)],
    );
    assert!(
        h.reclassify_packs(std::slice::from_ref(&c), &[])
            .await
            .is_err()
    );
    let d2 = tempfile::tempdir().unwrap();
    let second = fresh(&store, config(d2.path())).await;
    assert!(second.manifest().packs[0].group_coverages.is_empty());
    h.reclassify_packs(&[c], std::slice::from_ref(&snapshot))
        .await
        .unwrap();
    assert_eq!(h.manifest().head_seq, view.manifest.head_seq);
    assert!(h.manifest().revision > view.manifest.revision);
    assert_eq!(h.pin_coverage_refs(1).await.unwrap(), *snapshot.snapshot());
    // A second reader must adopt metadata even with the same WAL head.
    second.sync_refs().await.unwrap();
    assert_eq!(second.manifest().packs[0].group_coverages.len(), 1);
    store
        .put_bytes(
            &format!("{}{}", id().store_prefix(), snapshot.key()),
            b"corrupt".to_vec(),
            PutMode::Overwrite,
        )
        .await
        .unwrap();
    assert!(second.pin_coverage_refs(1).await.is_err());
    // A duplicate candidate upload validates existing immutable bytes, too.
    assert!(h.prepare_coverage_snapshot(&view).await.is_err());
}

#[tokio::test]
async fn complete_same_generation_dependencies_and_live_scope_are_required() {
    let store = MemoryStore::shared();
    let d = tempfile::tempdir().unwrap();
    let mut cfg = config(d.path());
    cfg.refs.packfiles.insert(
        "extra".into(),
        PackGroupConfig {
            kind: PackGroupKind::Code,
            include: vec!["refs/heads/*".into()],
            subtract: vec!["code".into()],
        },
    );
    let registry = Registry::new(store.clone(), Arc::new(cfg.clone()));
    let h = registry.create(&id(), ObjectFormat::Sha1).await.unwrap();
    h.publish_settings("", "test", "generation one")
        .await
        .unwrap();
    let mut manifest = (*h.manifest()).clone();
    manifest.packs = vec![pack(1), pack(2)];
    manifest.revision += 1;
    seed(&store, &manifest).await;
    let view = h.publication_view().await.unwrap();
    let snapshot = h.prepare_coverage_snapshot(&view).await.unwrap();
    let policy = cfg.refs.policy_identity();
    let base = classify(&pack(1), "code", &policy, &snapshot, &[pack(1)]);
    let extra = classify(&pack(2), "extra", &policy, &snapshot, &[pack(2)]);
    assert!(
        h.reclassify_packs(
            std::slice::from_ref(&extra),
            std::slice::from_ref(&snapshot)
        )
        .await
        .is_err()
    );
    h.reclassify_packs(
        &[base.clone(), extra.clone()],
        std::slice::from_ref(&snapshot),
    )
    .await
    .unwrap();
    let mut wrong = base.clone();
    wrong.covers_seq += 1;
    assert!(
        h.reclassify_packs(&[wrong], std::slice::from_ref(&snapshot))
            .await
            .is_err()
    );
    let mut missing = base.clone();
    missing.group_coverages[0].packs.push(pack(99).checksum);
    assert!(
        h.reclassify_packs(&[missing], std::slice::from_ref(&snapshot))
            .await
            .is_err()
    );
    let mut stale = base.clone();
    stale.ref_policy = "old-policy".into();
    assert!(
        h.reclassify_packs(&[stale], std::slice::from_ref(&snapshot))
            .await
            .is_err()
    );
    // Reclassification revokes dependent proofs rather than leaving them usable.
    h.reclassify_packs(
        &[PackClassification {
            checksum: pack(1).checksum,
            kind: PackKind::Objects as i32,
            audience: PackAudience::Retained as i32,
            pack_groups: vec!["_retained".into()],
            ..Default::default()
        }],
        &[],
    )
    .await
    .unwrap();
    assert!(
        h.manifest()
            .packs
            .iter()
            .all(|p| p.group_coverages.is_empty())
    );
    assert!(h.reclassify_packs(&[extra], &[snapshot]).await.is_err());
}

#[tokio::test]
async fn old_checkpoint_uses_its_committed_custom_key_including_sha256_placeholders() {
    let store = MemoryStore::shared();
    let d = tempfile::tempdir().unwrap();
    let registry = Registry::new(store.clone(), Arc::new(config(d.path())));
    let h = registry.create(&id(), ObjectFormat::Sha256).await.unwrap();
    let snapshot = RefSnapshot {
        seq: 0,
        object_format: "sha1".into(),
        refs: vec![Ref {
            name: "refs/heads/main".into(),
            oid: "a".repeat(64),
            peeled: String::new(),
        }],
        head_target: "refs/heads/main".into(),
        created_at: None,
    };
    let refs_key = "old/custom/refs.pb";
    let cp_key = "old/custom/checkpoint.pb";
    let cp = Checkpoint {
        seq: 1,
        object_format: "sha256".into(),
        refs_key: refs_key.into(),
        ref_count: 1,
        ..Default::default()
    };
    for (key, bytes) in [
        (refs_key, snapshot.encode_to_vec()),
        (cp_key, cp.encode_to_vec()),
    ] {
        store
            .put_bytes(
                &format!("{}{key}", id().store_prefix()),
                bytes,
                PutMode::Create,
            )
            .await
            .unwrap();
    }
    let mut manifest = (*h.manifest()).clone();
    manifest.head_seq = 1;
    manifest.min_seq = 2;
    manifest.revision += 1;
    manifest.checkpoint = Some(CheckpointRef {
        seq: 1,
        key: cp_key.into(),
        ..Default::default()
    });
    seed(&store, &manifest).await;
    let d2 = tempfile::tempdir().unwrap();
    let reader = fresh(&store, config(d2.path())).await;
    let view = reader.publication_view().await.unwrap();
    assert_eq!(view.refs.object_format, "sha256");
    assert_eq!(view.refs.refs[0].oid, "a".repeat(64));
    assert_eq!(view.refs.seq, 1);
}

#[tokio::test]
async fn canonical_checkpoint_and_strict_format_validation() {
    let store = MemoryStore::shared();
    let d = tempfile::tempdir().unwrap();
    let registry = Registry::new(store.clone(), Arc::new(config(d.path())));
    let h = registry.create(&id(), ObjectFormat::Sha1).await.unwrap();
    h.publish_settings("", "test", "one").await.unwrap();
    let cp = h.write_checkpoint().await.unwrap();
    assert!(cp.refs_key.starts_with("checkpoints/refs/"));
    assert!(cp.key.contains("attempts/"));
    assert_eq!(h.pin_coverage_refs(cp.seq).await.unwrap().seq, cp.seq);
    let mut manifest = (*h.manifest()).clone();
    manifest.format_version = 999;
    seed(&store, &manifest).await;
    let d2 = tempfile::tempdir().unwrap();
    let reader = Registry::new(store.clone(), Arc::new(config(d2.path())));
    assert!(reader.open(&id()).await.is_err());
    assert!(h.sync_refs().await.is_err());
}
