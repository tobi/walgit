use super::*;
use crate::{LocalRepo, ObjectFormat, RepoId};
use std::process::Stdio;
use walgit_config::ByteSize;
use walgit_config::refs::PackGroupConfig;
use walgit_proto::v1::{Ref, RefSnapshot};

fn command(cwd: &Path, args: &[&str], input: &[u8]) -> Vec<u8> {
    let mut child = Command::new("git")
        .arg("-C")
        .arg(cwd)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child.stdin.take().unwrap().write_all(input).unwrap();
    let out = child.wait_with_output().unwrap();
    assert!(
        out.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    out.stdout
}

fn text(cwd: &Path, args: &[&str]) -> String {
    String::from_utf8(command(cwd, args, b""))
        .unwrap()
        .trim()
        .into()
}

async fn fixture(
    format: ObjectFormat,
) -> (tempfile::TempDir, MaintenanceInput, String, Vec<String>) {
    let root = tempfile::tempdir().unwrap();
    let work = root.path().join("source");
    std::fs::create_dir(&work).unwrap();
    text(
        &work,
        &[
            "init",
            "-b",
            "main",
            &format!("--object-format={}", format.as_str()),
        ],
    );
    text(&work, &["config", "user.name", "Fixture"]);
    text(&work, &["config", "user.email", "fixture@example.invalid"]);
    let names = vec![
        "space name.txt".into(),
        "tab\tname.txt".into(),
        "line\nname.txt".into(),
        "nested/é.txt".into(),
    ];
    std::fs::create_dir(work.join("nested")).unwrap();
    for (index, name) in names.iter().enumerate() {
        std::fs::write(work.join(name), format!("fixture {index}\n")).unwrap();
    }
    text(&work, &["add", "."]);
    text(&work, &["commit", "-qm", "main"]);
    text(&work, &["tag", "-a", "v1", "-m", "annotated"]);
    text(&work, &["checkout", "-qb", "topic"]);
    std::fs::write(work.join("topic.txt"), "topic bytes").unwrap();
    text(&work, &["add", "."]);
    text(&work, &["commit", "-qm", "topic"]);
    text(&work, &["checkout", "-q", "main"]);
    let meta_blob = String::from_utf8(command(
        &work,
        &["hash-object", "-w", "--stdin"],
        b"metadata-only payload",
    ))
    .unwrap();
    let mut entries = command(&work, &["ls-tree", "-z", "HEAD"], b"");
    entries.extend_from_slice(
        format!("100644 blob {}\tmetadata-only.txt\0", meta_blob.trim()).as_bytes(),
    );
    let tree = String::from_utf8(command(&work, &["mktree", "-z"], &entries))
        .unwrap()
        .trim()
        .to_owned();
    let meta = String::from_utf8(command(
        &work,
        &["commit-tree", &tree, "-m", "metadata"],
        b"",
    ))
    .unwrap();
    text(&work, &["update-ref", "refs/meta/state", meta.trim()]);
    text(&work, &["repack", "-ad"]);
    let orphan = String::from_utf8(command(
        &work,
        &["hash-object", "-w", "--stdin"],
        b"unreachable retained payload",
    ))
    .unwrap()
    .trim()
    .to_owned();
    let prefix = work.join(".git/objects/pack/pack");
    command(
        &work,
        &["pack-objects", prefix.to_str().unwrap()],
        format!("{orphan}\n").as_bytes(),
    );
    let mut packs = Vec::new();
    for entry in std::fs::read_dir(work.join(".git/objects/pack")).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().is_some_and(|e| e == "idx") {
            let index = gix_pack::index::File::at(&path, format.kind()).unwrap();
            packs.push(PackRef {
                checksum: index.pack_checksum().to_string(),
                object_count: u64::from(index.num_objects()),
                idx_size: std::fs::metadata(&path).unwrap().len(),
                pack_size: std::fs::metadata(path.with_extension("pack"))
                    .unwrap()
                    .len(),
                ..Default::default()
            });
        }
    }
    let refs = text(
        &work,
        &["for-each-ref", "--format=%(refname) %(objectname)"],
    )
    .lines()
    .map(|line| {
        let (name, oid) = line.split_once(' ').unwrap();
        Ref {
            name: name.into(),
            oid: oid.into(),
            peeled: String::new(),
        }
    })
    .collect();
    let snapshot = RefSnapshot {
        object_format: format.as_str().into(),
        refs,
        head_target: "refs/heads/main".into(),
        ..Default::default()
    };
    let input = crate::maintenance_input::prepare(
        work.join(".git"),
        root.path().join("attempts"),
        packs,
        snapshot,
        (),
    )
    .await
    .unwrap();
    (root, input, orphan, names)
}

fn resources() -> Resources {
    Resources {
        sort_memory_bytes: 1024 * 1024,
        delta_memory_bytes: 1024 * 1024,
        threads: 1,
    }
}

#[tokio::test]
async fn conserving_groups_reconstruct_tags_dependencies_and_unusual_tree_names() {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let (root, input, orphan, names) = fixture(format).await;
        let mut refs = RefsConfig::default();
        refs.packfiles.get_mut("code").unwrap().include =
            vec!["refs/heads/main".into(), "refs/tags/*".into()];
        refs.packfiles.insert(
            "topic".into(),
            PackGroupConfig {
                kind: PackGroupKind::Code,
                include: vec!["refs/heads/topic".into()],
                subtract: vec!["code".into()],
            },
        );
        let result = segment(input, refs, PacksConfig::default(), resources())
            .await
            .unwrap();
        assert!(
            result
                .outputs
                .iter()
                .any(|o| o.group == "topic" && o.dependencies == ["code"])
        );
        assert!(
            result
                .outputs
                .iter()
                .any(|o| o.group == "meta" && o.audience == Some(PackGroupKind::Meta))
        );
        assert!(
            result
                .outputs
                .iter()
                .any(|o| o.group == "_retained" && o.audience.is_none())
        );
        let rebuilt = LocalRepo::init(
            &root.path().join("rebuilt"),
            &RepoId::new("fixture", "repo").unwrap(),
            format,
        )
        .unwrap();
        let meta_only = text(
            result.input.repo.path(),
            &["rev-parse", "refs/meta/state:metadata-only.txt"],
        );
        for output in &result.outputs {
            assert!(!output.exceeds_target);
            let index = gix_pack::index::File::at(output.path.with_extension("idx"), format.kind())
                .unwrap();
            for entry in index.iter() {
                if output.audience == Some(PackGroupKind::Code) {
                    assert_ne!(entry.oid.to_string(), meta_only);
                }
                let kind = text(
                    result.input.repo.path(),
                    &["cat-file", "-t", &entry.oid.to_string()],
                );
                assert_eq!(
                    kind == "blob",
                    output.pack.kind == i32::from(PackKind::Blobs)
                );
            }
            for ext in ["pack", "idx", "rev"] {
                let path = output.path.with_extension(ext);
                if path.exists() {
                    let target = rebuilt
                        .path()
                        .join("objects/pack")
                        .join(path.file_name().unwrap());
                    if !target.exists() {
                        std::fs::copy(&path, target).unwrap();
                    }
                }
            }
        }
        rebuilt.load_ref_snapshot(&result.input.refs).unwrap();
        text(rebuilt.path(), &["fsck", "--full", "--strict"]);
        assert_eq!(
            text(rebuilt.path(), &["cat-file", "blob", &orphan]),
            "unreachable retained payload"
        );
        assert_eq!(
            text(rebuilt.path(), &["cat-file", "-t", "refs/tags/v1"]),
            "tag"
        );
        for (index, name) in names.iter().enumerate() {
            assert_eq!(
                command(
                    rebuilt.path(),
                    &["show", &format!("refs/heads/main:{name}")],
                    b""
                ),
                format!("fixture {index}\n").as_bytes()
            );
        }
        // Paths with spaces and tabs retain their complete Git hints. Git's
        // newline-oriented rev-list truncates the newline hint; the test above
        // proves the actual newline-containing tree name and blob survive.
        let hints = std::fs::read(result.inventory.parent().unwrap().join("code.blobs")).unwrap();
        assert!(
            hints
                .windows(b"space name.txt".len())
                .any(|w| w == b"space name.txt")
        );
        assert!(
            hints
                .windows(b"tab\tname.txt".len())
                .any(|w| w == b"tab\tname.txt")
        );
    }
}

#[tokio::test]
async fn empty_groups_conserve_everything_and_invalid_budgets_fail() {
    let (_root, input, _, _) = fixture(ObjectFormat::Sha1).await;
    let mut refs = RefsConfig::default();
    refs.packfiles.clear();
    let result = segment(input, refs, PacksConfig::default(), resources())
        .await
        .unwrap();
    assert!(result.outputs.iter().all(|o| o.group == "_retained"));
    assert!(
        result
            .outputs
            .iter()
            .any(|o| o.pack.kind == i32::from(PackKind::History))
    );
    assert!(
        result
            .outputs
            .iter()
            .any(|o| o.pack.kind == i32::from(PackKind::Blobs))
    );
    let (_root, input, _, _) = fixture(ObjectFormat::Sha1).await;
    let bad = Resources {
        sort_memory_bytes: 0,
        ..resources()
    };
    assert!(
        segment(input, RefsConfig::default(), PacksConfig::default(), bad)
            .await
            .is_err()
    );
}

#[test]
fn sort_budget_is_passed_in_bytes() {
    let command = sort_command(Path::new("work"), resources());
    assert!(
        command.get_args().any(|a| a == "--buffer-size=1048576b"),
        "{command:?}"
    );
}

#[test]
fn type_split_keeps_hint_bytes_and_rejects_missing_objects() {
    let root = tempfile::tempdir().unwrap();
    let typed = root.path().join("typed");
    let history = root.path().join("history");
    let blobs = root.path().join("blobs");
    let oid = "12".repeat(20);
    std::fs::write(
        &typed,
        format!("{oid} blob  spaced\tpath\n{oid} tag tag name\n"),
    )
    .unwrap();
    split_types(&typed, &history, &blobs).unwrap();
    assert_eq!(
        std::fs::read(blobs.clone()).unwrap(),
        format!("{oid}  spaced\tpath\n").as_bytes()
    );
    std::fs::write(&typed, format!("{oid} missing\n")).unwrap();
    assert!(split_types(&typed, &history, &blobs).is_err());
}

#[tokio::test]
async fn selected_subset_is_additive_until_all_planned_steps_finish() {
    let (_root, input, orphan, _) = fixture(ObjectFormat::Sha1).await;
    let selected = input
        .packs
        .iter()
        .find(|p| p.object_count == 1)
        .unwrap()
        .checksum
        .clone();
    let mut plan = plan(
        input,
        vec![selected.clone()],
        RefsConfig::default(),
        resources(),
    )
    .await
    .unwrap();
    assert_eq!(plan.selected.len(), 1);
    assert_eq!(plan.selected[0].checksum, selected);
    assert_eq!(plan.families.len(), 1);
    assert_eq!(plan.families[0].group, "_retained");
    assert!(plan.outputs.is_empty());
    plan = step(plan, 0, PacksConfig::default()).await.unwrap();
    let count = plan.outputs.len();
    plan = step(plan, 0, PacksConfig::default()).await.unwrap();
    assert_eq!(plan.outputs.len(), count);
    let result = finish(plan).await.unwrap();
    assert_eq!(
        std::fs::read_to_string(result.inventory).unwrap().trim(),
        orphan
    );
    assert_eq!(
        result
            .outputs
            .iter()
            .map(|p| p.pack.object_count)
            .sum::<u64>(),
        1
    );
    let (_root, input, _, _) = fixture(ObjectFormat::Sha1).await;
    let checksums = input.packs.iter().map(|p| p.checksum.clone()).collect();
    let incomplete = super::plan(input, checksums, RefsConfig::default(), resources())
        .await
        .unwrap();
    assert!(finish(incomplete).await.is_err());
}

#[tokio::test]
async fn target_splits_packs_and_reports_indivisible_oversized_object() {
    let root = tempfile::tempdir().unwrap();
    let source = LocalRepo::init(
        root.path(),
        &RepoId::new("fixture", "source").unwrap(),
        ObjectFormat::Sha1,
    )
    .unwrap();
    let mut state = 17_u64;
    let mut ids = String::new();
    for size in [2 * 1024 * 1024, 600 * 1024, 600 * 1024, 600 * 1024] {
        let bytes: Vec<u8> = (0..size)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state.to_le_bytes()[0]
            })
            .collect();
        ids.push_str(
            &String::from_utf8(command(
                source.path(),
                &["hash-object", "-w", "--stdin"],
                &bytes,
            ))
            .unwrap(),
        );
    }
    let prefix = source.path().join("objects/pack/pack");
    let checksum = String::from_utf8(command(
        source.path(),
        &["pack-objects", prefix.to_str().unwrap()],
        ids.as_bytes(),
    ))
    .unwrap()
    .trim()
    .to_owned();
    let path = source
        .path()
        .join("objects/pack")
        .join(format!("pack-{checksum}.pack"));
    let pack = PackRef {
        checksum,
        object_count: 4,
        pack_size: std::fs::metadata(&path).unwrap().len(),
        idx_size: std::fs::metadata(path.with_extension("idx")).unwrap().len(),
        ..Default::default()
    };
    let input = crate::maintenance_input::prepare(
        source.path().to_path_buf(),
        root.path().join("attempt"),
        vec![pack],
        RefSnapshot {
            object_format: "sha1".into(),
            ..Default::default()
        },
        (),
    )
    .await
    .unwrap();
    let tuning = PacksConfig {
        segment_max_bytes: ByteSize::mib(1),
        ..Default::default()
    };
    let result = segment(input, RefsConfig::default(), tuning, resources())
        .await
        .unwrap();
    assert!(result.outputs.len() >= 3);
    let oversized: Vec<_> = result.outputs.iter().filter(|p| p.exceeds_target).collect();
    assert_eq!(oversized.len(), 1);
    assert_eq!(oversized[0].pack.object_count, 1);
    assert_eq!(
        result
            .outputs
            .iter()
            .map(|p| p.pack.object_count)
            .sum::<u64>(),
        4
    );
}

#[tokio::test]
async fn resume_reclassifies_and_verifies_saved_family_bytes() {
    let (_root, input, _, _) = fixture(ObjectFormat::Sha1).await;
    let selected: Vec<_> = input.packs.iter().map(|p| p.checksum.clone()).collect();
    let descriptors = input.packs.clone();
    let snapshot = input.refs.clone();
    let mut original = plan(input, selected.clone(), RefsConfig::default(), resources())
        .await
        .unwrap();
    original = step(original, 0, PacksConfig::default()).await.unwrap();
    let receipt = StepReceipt {
        group: original.families[0].group.clone(),
        kind: original.families[0].kind,
        outputs: original.outputs.clone(),
    };
    let corrupt = receipt.outputs[0].path.clone();
    let saved_outputs = receipt.outputs.clone();
    let saved_group = receipt.group.clone();
    let saved_kind = receipt.kind;
    let scratch = original.input.persist();
    drop(original);
    let input =
        crate::maintenance_input::reopen(scratch.clone(), descriptors.clone(), snapshot.clone())
            .await
            .unwrap();
    let mut resumed = resume(
        input,
        selected.clone(),
        RefsConfig::default(),
        resources(),
        vec![receipt],
    )
    .await
    .unwrap();
    assert!(resumed.families[0].completed);
    assert_eq!(resumed.outputs.len(), saved_outputs.len());
    for index in 0..resumed.families.len() {
        resumed = step(resumed, index, PacksConfig::default()).await.unwrap();
    }
    finish(resumed).await.unwrap();
    // A saved receipt with a valid-looking checksum cannot skip corrupted bytes.
    let mut bytes = std::fs::read(&corrupt).unwrap();
    bytes[12] ^= 1;
    std::fs::remove_file(&corrupt).unwrap();
    std::fs::write(&corrupt, bytes).unwrap();
    let input = crate::maintenance_input::reopen(scratch.clone(), descriptors, snapshot)
        .await
        .unwrap();
    let receipt = StepReceipt {
        group: saved_group,
        kind: saved_kind,
        outputs: saved_outputs,
    };
    assert!(
        resume(
            input,
            selected,
            RefsConfig::default(),
            resources(),
            vec![receipt]
        )
        .await
        .is_err()
    );
    std::fs::remove_dir_all(scratch).unwrap();
}

#[tokio::test]
async fn reopened_input_rejects_uncommitted_loose_objects() {
    let (_root, mut input, _, _) = fixture(ObjectFormat::Sha1).await;
    let descriptors = input.packs.clone();
    let snapshot = input.refs.clone();
    let scratch = input.persist();
    command(
        input.repo.path(),
        &["hash-object", "-w", "--stdin"],
        b"uncommitted scratch object",
    );
    drop(input);
    assert!(
        crate::maintenance_input::reopen(scratch.clone(), descriptors, snapshot)
            .await
            .is_err()
    );
    std::fs::remove_dir_all(scratch).unwrap();
}

#[tokio::test]
async fn persisted_attempt_cannot_reopen_until_its_owner_exits() {
    let (_root, mut input, _, _) = fixture(ObjectFormat::Sha1).await;
    let scratch = input.persist();
    let packs = input.packs.clone();
    let refs = input.refs.clone();
    assert!(
        crate::maintenance_input::reopen(scratch.clone(), packs.clone(), refs.clone())
            .await
            .is_err()
    );
    drop(input);
    let reopened = crate::maintenance_input::reopen(scratch.clone(), packs, refs)
        .await
        .unwrap();
    drop(reopened);
    std::fs::remove_dir_all(scratch).unwrap();
}

#[tokio::test]
async fn classification_uses_common_groups_and_cannot_certify_mixed_pack() {
    let (_root, input, orphan, _) = fixture(ObjectFormat::Sha1).await;
    let selected = input.packs.iter().map(|p| p.checksum.clone()).collect();
    let initial = plan(input, selected, RefsConfig::default(), resources())
        .await
        .unwrap();
    let (initial, classification) = classify(initial).await.unwrap();
    assert!(
        classification
            .packs
            .iter()
            .any(|p| p.mixed && p.kind == PackKind::Objects)
    );
    let retained = classification
        .packs
        .iter()
        .find(|p| p.groups == ["_retained"])
        .unwrap();
    assert_eq!(retained.kind, PackKind::Blobs);
    assert!(!retained.mixed);
    assert!(
        classification
            .group_complete
            .values()
            .all(|complete| !complete)
    );
    assert!(initial.outputs.is_empty());
    assert!(
        std::fs::read_to_string(initial.inventory)
            .unwrap()
            .contains(&orphan)
    );
}

#[tokio::test]
async fn cancelled_waiter_keeps_attempt_locked_until_blocking_worker_finishes() {
    let (_root, mut input, _, _) = fixture(ObjectFormat::Sha1).await;
    let scratch = input.persist();
    let packs = input.packs.clone();
    let refs = input.refs.clone();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (done_tx, done_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let waiter = tokio::spawn(async move {
        tokio::task::spawn_blocking(move || {
            started_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            drop(input);
            done_tx.send(()).unwrap();
        })
        .await
        .unwrap();
    });
    started_rx.await.unwrap();
    waiter.abort();
    assert!(
        crate::maintenance_input::reopen(scratch.clone(), packs.clone(), refs.clone())
            .await
            .is_err()
    );
    release_tx.send(()).unwrap();
    done_rx.await.unwrap();
    let reopened = crate::maintenance_input::reopen(scratch.clone(), packs, refs)
        .await
        .unwrap();
    drop(reopened);
    std::fs::remove_dir_all(scratch).unwrap();
}

#[tokio::test]
async fn ordinary_mixed_type_code_pack_is_classified_without_a_single_pack_rewrite() {
    let (_root, mut input, _, _) = fixture(ObjectFormat::Sha1).await;
    for reference in &mut input.refs.refs {
        if reference.name.starts_with("refs/meta/") {
            reference.name = reference.name.replacen("refs/meta/", "refs/heads/", 1);
        }
    }
    let selected = input.packs.iter().map(|p| p.checksum.clone()).collect();
    let mut refs = RefsConfig::default();
    refs.packfiles.remove("meta");
    refs.packfiles.get_mut("code").unwrap().include = vec![
        "refs/heads/*".into(),
        "refs/tags/*".into(),
        "refs/meta/*".into(),
    ];
    let planned = plan(input, selected, refs, resources()).await.unwrap();
    let (planned, classification) = classify(planned).await.unwrap();
    let code = classification
        .packs
        .iter()
        .find(|p| p.groups == ["code"])
        .unwrap();
    assert_eq!(code.kind, PackKind::Objects);
    assert!(!code.mixed);
    assert_eq!(code.audience, Some(PackGroupKind::Code));
    assert_eq!(classification.group_complete.get("code"), Some(&true));
    assert!(planned.outputs.is_empty());
    assert!(planned.families.iter().all(|f| !f.completed));
}

#[tokio::test]
async fn identical_code_meta_families_share_membership_and_settle() {
    let (root, mut input, _, _) = fixture(ObjectFormat::Sha1).await;
    let main = input
        .refs
        .refs
        .iter()
        .find(|r| r.name == "refs/heads/main")
        .unwrap()
        .oid
        .clone();
    input.refs.refs.push(Ref {
        name: "refs/meta/shared".into(),
        oid: main,
        peeled: String::new(),
    });
    let mut refs = RefsConfig::default();
    refs.packfiles.get_mut("code").unwrap().include = vec!["refs/heads/main".into()];
    refs.packfiles.get_mut("meta").unwrap().include = vec!["refs/meta/shared".into()];
    let segmented = segment(input, refs.clone(), PacksConfig::default(), resources())
        .await
        .unwrap();
    let code_hashes: Vec<_> = segmented
        .outputs
        .iter()
        .filter(|o| o.group == "code")
        .map(|o| &o.pack.checksum)
        .collect();
    assert!(
        segmented
            .outputs
            .iter()
            .any(|o| o.group == "meta" && code_hashes.contains(&&o.pack.checksum))
    );
    let source = LocalRepo::init(
        &root.path().join("dedup"),
        &RepoId::new("fixture", "source").unwrap(),
        ObjectFormat::Sha1,
    )
    .unwrap();
    let mut unique = std::collections::BTreeMap::new();
    for output in &segmented.outputs {
        if unique
            .insert(output.pack.checksum.clone(), output.pack.clone())
            .is_some()
        {
            continue;
        }
        for ext in ["pack", "idx", "rev"] {
            let path = output.path.with_extension(ext);
            if path.exists() {
                std::fs::copy(
                    &path,
                    source
                        .path()
                        .join("objects/pack")
                        .join(path.file_name().unwrap()),
                )
                .unwrap();
            }
        }
    }
    let input = crate::maintenance_input::prepare(
        source.path().to_path_buf(),
        root.path().join("reclassify"),
        unique.into_values().collect(),
        segmented.input.refs.clone(),
        (),
    )
    .await
    .unwrap();
    let checksums = input.packs.iter().map(|p| p.checksum.clone()).collect();
    let planned = plan(input, checksums, refs, resources()).await.unwrap();
    let (planned, classification) = classify(planned).await.unwrap();
    assert!(classification.packs.iter().all(|p| !p.mixed));
    let shared: Vec<_> = classification
        .packs
        .iter()
        .filter(|p| p.groups.contains(&"code".into()) && p.groups.contains(&"meta".into()))
        .collect();
    assert!(!shared.is_empty());
    assert!(
        shared
            .iter()
            .all(|p| p.audience == Some(PackGroupKind::Code))
    );
    assert_eq!(classification.group_complete.get("code"), Some(&true));
    assert_eq!(classification.group_complete.get("meta"), Some(&true));
    assert!(planned.outputs.is_empty());
}
