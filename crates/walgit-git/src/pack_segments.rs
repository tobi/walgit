//! Conserving physical cuts from an isolated, committed object inventory.
//!
//! Inventories are files, sorted with an explicit memory limit. Git supplies path
//! hints for delta search (its line protocol truncates hints at newlines); actual
//! tree names and object bytes are never modified. No inventory is a certificate.
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::process::Command;

use walgit_config::packs::PacksConfig;
use walgit_config::refs::{PackGroupKind, RefsConfig};
use walgit_proto::v1::{PackKind, PackRef};

use crate::maintenance_input::MaintenanceInput;
use crate::pack_groups::resolve_groups;
use crate::{GitError, RefSnapshotData, ge};

/// Explicit worker resource headroom. Delta memory bounds the search window,
/// not Git's total RSS (Git also maintains object metadata and delta buffers).
#[derive(Debug, Clone, Copy)]
pub struct Resources {
    pub sort_memory_bytes: u64,
    pub delta_memory_bytes: u64,
    pub threads: usize,
}

#[derive(Debug, Clone)]
pub struct SegmentOutput {
    pub group: String,
    /// None is the retained family, whose objects matched no physical group.
    pub audience: Option<PackGroupKind>,
    pub dependencies: Vec<String>,
    pub pack: PackRef,
    pub path: PathBuf,
    /// Git's size target permits an indivisible oversized object.
    pub exceeds_target: bool,
}

pub struct SegmentedInput {
    pub input: MaintenanceInput,
    pub outputs: Vec<SegmentOutput>,
    /// Disk-backed exact input object inventory, useful for final seal checks.
    pub inventory: PathBuf,
}

#[derive(Debug)]
pub struct Family {
    pub group: String,
    pub audience: Option<PackGroupKind>,
    pub dependencies: Vec<String>,
    pub kind: PackKind,
    /// Sorted OID + Git path-hint list, readable without producing a pack.
    pub objects: PathBuf,
    pub completed: bool,
}

/// Disposable progress: only a caller's final guarded manifest CAS is durable.
pub struct SegmentPlan {
    pub input: MaintenanceInput,
    pub selected: Vec<PackRef>,
    pub families: Vec<Family>,
    pub outputs: Vec<SegmentOutput>,
    pub inventory: PathBuf,
    resources: Resources,
    work: PathBuf,
}

/// Plan physical families against all committed inputs, rewriting only selected
/// pack checksums. Frozen inputs can remain readable without being rewritten.
pub async fn plan(
    input: MaintenanceInput,
    selected: Vec<String>,
    refs: RefsConfig,
    resources: Resources,
) -> Result<SegmentPlan, GitError> {
    tokio::task::spawn_blocking(move || plan_blocking(input, selected, &refs, resources))
        .await
        .map_err(ge)?
}

fn plan_blocking(
    input: MaintenanceInput,
    selected: Vec<String>,
    refs: &RefsConfig,
    resources: Resources,
) -> Result<SegmentPlan, GitError> {
    if resources.sort_memory_bytes == 0 {
        return Err(invalid("positive sort budget required"));
    }
    crate::object_links::verify_indexed_links(
        &input.repo,
        &input.packs,
        &input.refs,
        resources.sort_memory_bytes,
    )?;
    let groups = resolve_groups(refs, &RefSnapshotData::from(input.refs.clone()))?;
    let work = tempfile::Builder::new()
        .prefix("segment-work-")
        .tempdir_in(input.repo.path())?
        .keep();
    let raw = work.join("indexed.raw");
    let mut indexed = BufWriter::new(File::create(&raw)?);
    let selected: Vec<PackRef> = selected
        .into_iter()
        .map(|checksum| {
            input
                .packs
                .iter()
                .find(|p| p.checksum == checksum)
                .cloned()
                .ok_or_else(|| invalid("selected input is not committed"))
        })
        .collect::<Result<_, _>>()?;
    if selected.is_empty() {
        return Err(invalid("no selected inputs"));
    }
    let mut unique = std::collections::BTreeSet::new();
    if selected.iter().any(|p| !unique.insert(&p.checksum)) {
        return Err(invalid("duplicate selected input"));
    }
    for descriptor in &selected {
        let path = input
            .repo
            .path()
            .join("objects/pack")
            .join(format!("pack-{}.idx", descriptor.checksum));
        append_index(&path, &input, &mut indexed)?;
    }
    indexed.flush()?;
    let inventory = work.join("indexed.sorted");
    sort(&raw, &inventory, &work, resources)?;
    let all_raw = work.join("all-indexed.raw");
    let mut all = BufWriter::new(File::create(&all_raw)?);
    for descriptor in &input.packs {
        append_index(
            &input
                .repo
                .path()
                .join("objects/pack")
                .join(format!("pack-{}.idx", descriptor.checksum)),
            &input,
            &mut all,
        )?;
    }
    all.flush()?;
    let all_inventory = work.join("all-indexed.sorted");
    sort(&all_raw, &all_inventory, &work, resources)?;
    let full_union_raw = work.join("all-groups.raw");
    let mut full_union = BufWriter::new(File::create(&full_union_raw)?);
    let union_raw = work.join("groups.raw");
    let mut union = BufWriter::new(File::create(&union_raw)?);
    let mut families = Vec::new();
    for (name, roots) in groups {
        let revisions = work.join(format!("{name}.revisions"));
        let mut writer = BufWriter::new(File::create(&revisions)?);
        for oid in &roots.include {
            writeln!(writer, "{oid}")?;
        }
        for oid in &roots.exclude {
            writeln!(writer, "^{oid}")?;
        }
        writer.flush()?;
        let raw = work.join(format!("{name}.raw"));
        if roots.include.is_empty() {
            File::create(&raw)?;
        } else {
            run(
                git(&input).args(["rev-list", "--objects", "--stdin"]),
                &revisions,
                &raw,
                &work,
            )?;
        }
        let full = work.join(format!("{name}.full"));
        sort(&raw, &full, &work, resources)?;
        for line in BufReader::new(File::open(&full)?).split(b'\n') {
            let line = line?;
            if line.is_empty() {
                continue;
            }
            full_union.write_all(line.split(|b| *b == b' ').next().unwrap_or_default())?;
            full_union.write_all(b"\n")?;
        }
        let list = work.join(format!("{name}.sorted"));
        intersect(&full, &inventory, &list)?;
        for line in BufReader::new(File::open(&list)?).split(b'\n') {
            let line = line?;
            if line.is_empty() {
                continue;
            }
            let oid = line.split(|b| *b == b' ').next().unwrap_or_default();
            union.write_all(oid)?;
            union.write_all(b"\n")?;
        }
        families.push((name, Some(roots.kind), roots.dependencies, list));
    }
    union.flush()?;
    let union_sorted = work.join("groups.sorted");
    sort(&union_raw, &union_sorted, &work, resources)?;
    let retained = work.join("_retained.sorted");
    difference(&inventory, &union_sorted, &retained, &work)?;
    // Verify the complete walks before selected-input intersection: otherwise
    // loose objects could masquerade as covered roots outside committed packs.
    full_union.flush()?;
    let full_union_sorted = work.join("all-groups.sorted");
    sort(&full_union_raw, &full_union_sorted, &work, resources)?;
    let extra = work.join("group-extra");
    difference(&full_union_sorted, &all_inventory, &extra, &work)?;
    if std::fs::metadata(extra)?.len() != 0 {
        return Err(invalid("group graph exceeds committed inventory"));
    }
    families.push(("_retained".into(), None, Vec::new(), retained));

    let mut planned = Vec::new();
    for (group, audience, dependencies, list) in families {
        let typed = work.join(format!("{group}.typed"));
        run(
            git(&input).args([
                "cat-file",
                "--batch-check=%(objectname) %(objecttype) %(rest)",
            ]),
            &list,
            &typed,
            &work,
        )?;
        let history = work.join(format!("{group}.history"));
        let blobs = work.join(format!("{group}.blobs"));
        split_types(&typed, &history, &blobs)?;
        for (kind, objects) in [(PackKind::History, history), (PackKind::Blobs, blobs)] {
            if std::fs::metadata(&objects)?.len() == 0 {
                continue;
            }
            planned.push(Family {
                group: group.clone(),
                audience,
                dependencies: dependencies.clone(),
                kind,
                objects,
                completed: false,
            });
        }
    }
    Ok(SegmentPlan {
        input,
        selected,
        families: planned,
        outputs: Vec::new(),
        inventory,
        resources,
        work,
    })
}

/// One bounded family unit. All packs from this unit remain additive until the
/// complete selected input set passes `finish()` and the caller seals its plan.
pub async fn step(
    plan: SegmentPlan,
    family: usize,
    tuning: PacksConfig,
) -> Result<SegmentPlan, GitError> {
    tokio::task::spawn_blocking(move || step_blocking(plan, family, &tuning))
        .await
        .map_err(ge)?
}

fn step_blocking(
    mut plan: SegmentPlan,
    family: usize,
    tuning: &PacksConfig,
) -> Result<SegmentPlan, GitError> {
    tuning.validate().map_err(invalid)?;
    if tuning.target_bytes() < 1024 * 1024 {
        return Err(invalid("at least 1 MiB pack target required"));
    }
    let budget = tuning
        .delta_budget(plan.resources.threads, plan.resources.delta_memory_bytes)
        .map_err(invalid)?;
    let item = plan
        .families
        .get(family)
        .ok_or_else(|| invalid("unknown family"))?;
    if item.completed {
        return Ok(plan);
    }
    let group = item.group.clone();
    let audience = item.audience;
    let dependencies = item.dependencies.clone();
    let kind = item.kind;
    let objects = item.objects.clone();
    let input = &plan.input;
    let work = plan.work.clone();
    let dir = work.join(format!("{group}-{}", kind.as_str_name()));
    std::fs::create_dir(&dir)?;
    let prefix = dir.join("pack");
    let hashes = dir.join("hashes");
    let mut command = git(input);
    command
        .args([
            "-c",
            "pack.writeReverseIndex=true",
            "pack-objects",
            "--non-empty",
            "--delta-base-offset",
            "--no-reuse-delta",
        ])
        .arg(format!("--max-pack-size={}", tuning.target_bytes()))
        .arg(format!("--window={}", tuning.segment_delta_window))
        .arg(format!("--depth={}", tuning.segment_delta_depth))
        .arg(format!("--threads={}", budget.threads))
        .arg(format!(
            "--window-memory={}",
            budget.window_memory_per_thread
        ));
    if !tuning.segment_reuse_objects {
        command.arg("--no-reuse-object");
    }
    command.arg(&prefix);
    run(&mut command, &objects, &hashes, &work)?;
    for hash in BufReader::new(File::open(hashes)?).lines() {
        let hash = hash?;
        let oid = gix_hash::ObjectId::from_hex(hash.as_bytes()).map_err(ge)?;
        if oid.kind() != input.repo.object_format().kind() {
            return Err(invalid("output checksum format mismatch"));
        }
        let path = dir.join(format!("pack-{hash}.pack"));
        // index-pack refuses thin/external delta bases. It verifies the
        // complete new pack/index pair before any output may be published.
        run(
            git(input).args(["index-pack", "--verify"]).arg(&path),
            &work.join("empty"),
            &dir.join("verified"),
            &work,
        )?;
        let idx = path.with_extension("idx");
        let index = gix_pack::index::File::at(&idx, oid.kind()).map_err(ge)?;
        if index.pack_checksum() != oid {
            return Err(invalid("output index checksum mismatch"));
        }
        let size = std::fs::metadata(&path)?.len();
        plan.outputs.push(SegmentOutput {
            group: group.clone(),
            audience,
            dependencies: dependencies.clone(),
            pack: PackRef {
                checksum: hash,
                pack_size: size,
                idx_size: std::fs::metadata(idx)?.len(),
                object_count: u64::from(index.num_objects()),
                has_rev: path.with_extension("rev").is_file(),
                kind: kind.into(),
                ..Default::default()
            },
            path,
            exceeds_target: size > tuning.target_bytes(),
        });
    }

    plan.families
        .get_mut(family)
        .ok_or_else(|| invalid("unknown family"))?
        .completed = true;
    Ok(plan)
}

/// Require every unit and exact conservation before final input retirement.
pub async fn finish(plan: SegmentPlan) -> Result<SegmentedInput, GitError> {
    tokio::task::spawn_blocking(move || {
        if plan.families.iter().any(|f| !f.completed) {
            return Err(invalid("cannot finish an incomplete cut"));
        }
        let SegmentPlan {
            input,
            outputs,
            inventory,
            resources,
            work,
            ..
        } = plan;
        // Exact union equality is mandatory, including unreachable retained objects.
        let produced_raw = work.join("produced.raw");
        let mut produced = BufWriter::new(File::create(&produced_raw)?);
        for output in &outputs {
            append_index(&output.path.with_extension("idx"), &input, &mut produced)?;
        }
        produced.flush()?;
        let produced_sorted = work.join("produced.sorted");
        sort(&produced_raw, &produced_sorted, &work, resources)?;
        let mismatch = work.join("mismatch");
        run(
            Command::new("comm")
                .args(["-3"])
                .arg(&inventory)
                .arg(&produced_sorted),
            &work.join("empty"),
            &mismatch,
            &work,
        )?;
        if std::fs::metadata(mismatch)?.len() != 0 {
            return Err(invalid("segmentation did not conserve indexed objects"));
        }
        Ok(SegmentedInput {
            input,
            outputs,
            inventory,
        })
    })
    .await
    .map_err(ge)?
}

/// One completed family's disposable output receipt. The server persists its
/// own policy/input/tuning identity; these fields alone authorize nothing.
pub struct StepReceipt {
    pub group: String,
    pub kind: PackKind,
    pub outputs: Vec<SegmentOutput>,
}

/// Rebuild classification from the captured committed view, then verify each
/// saved output and exact family inventory before skipping any completed unit.
pub async fn resume(
    input: MaintenanceInput,
    selected: Vec<String>,
    refs: RefsConfig,
    resources: Resources,
    receipts: Vec<StepReceipt>,
) -> Result<SegmentPlan, GitError> {
    tokio::task::spawn_blocking(move || {
        let mut plan = plan_blocking(input, selected, &refs, resources)?;
        let root = plan.input.scratch_root().canonicalize()?;
        for receipt in receipts {
            let index = plan
                .families
                .iter()
                .position(|f| f.group == receipt.group && f.kind == receipt.kind)
                .ok_or_else(|| invalid("receipt family absent from current plan"))?;
            let family = plan
                .families
                .get(index)
                .ok_or_else(|| invalid("unknown family"))?;
            if family.completed || receipt.outputs.is_empty() {
                return Err(invalid("duplicate or empty family receipt"));
            }
            let verify_repo = crate::LocalRepo::init(
                &plan.work.join(format!("verify-{index}")),
                &crate::RepoId::new("maintenance", "receipt")?,
                plan.input.repo.object_format(),
            )?;
            let raw = plan.work.join(format!("receipt-{index}.raw"));
            let mut actual = BufWriter::new(File::create(&raw)?);
            let mut checksums = std::collections::BTreeSet::new();
            for output in &receipt.outputs {
                if output.group != family.group
                    || output.audience != family.audience
                    || output.dependencies != family.dependencies
                    || output.pack.kind != i32::from(family.kind)
                    || !checksums.insert(&output.pack.checksum)
                {
                    return Err(invalid("receipt scope mismatch"));
                }
                let oid =
                    gix_hash::ObjectId::from_hex(output.pack.checksum.as_bytes()).map_err(ge)?;
                if oid.kind() != plan.input.repo.object_format().kind()
                    || oid.to_string() != output.pack.checksum
                {
                    return Err(invalid("receipt checksum mismatch"));
                }
                for (ext, required) in [("pack", true), ("idx", true), ("rev", output.pack.has_rev)]
                {
                    if !required {
                        continue;
                    }
                    let path = output.path.with_extension(ext).canonicalize()?;
                    if !path.starts_with(&root)
                        || path.file_name().is_none_or(|n| {
                            n != std::ffi::OsStr::new(&format!(
                                "pack-{}.{ext}",
                                output.pack.checksum
                            ))
                        })
                    {
                        return Err(invalid("receipt path escaped its attempt"));
                    }
                    let target = verify_repo.path().join("objects/pack").join(
                        path.file_name()
                            .ok_or_else(|| invalid("receipt path has no filename"))?,
                    );
                    if std::fs::hard_link(&path, &target).is_err() {
                        std::fs::copy(&path, target)?;
                    }
                }
                crate::maintenance_input::verify(
                    &verify_repo,
                    &output.pack,
                    plan.input.repo.object_format(),
                )?;
                append_index(&output.path.with_extension("idx"), &plan.input, &mut actual)?;
            }
            actual.flush()?;
            let actual_sorted = plan.work.join(format!("receipt-{index}.sorted"));
            sort(&raw, &actual_sorted, &plan.work, resources)?;
            let expected = plan.work.join(format!("receipt-{index}.expected"));
            let mut expected_writer = BufWriter::new(File::create(&expected)?);
            for line in BufReader::new(File::open(&family.objects)?).split(b'\n') {
                let line = line?;
                if line.is_empty() {
                    continue;
                }
                expected_writer.write_all(line.split(|b| *b == b' ').next().unwrap_or_default())?;
                expected_writer.write_all(b"\n")?;
            }
            expected_writer.flush()?;
            let mismatch = plan.work.join(format!("receipt-{index}.mismatch"));
            run(
                Command::new("comm")
                    .arg("-3")
                    .arg(&expected)
                    .arg(&actual_sorted),
                &plan.work.join("empty"),
                &mismatch,
                &plan.work,
            )?;
            if std::fs::metadata(mismatch)?.len() != 0 {
                return Err(invalid("receipt does not conserve its planned family"));
            }
            plan.families
                .get_mut(index)
                .ok_or_else(|| invalid("unknown family"))?
                .completed = true;
            plan.outputs.extend(receipt.outputs);
        }
        clean_old_work(&plan)?;
        Ok(plan)
    })
    .await
    .map_err(ge)?
}

// The input holds an exclusive OS lock. Keep only receipt output directories
// from old work, after their bytes and family inventories have been verified.
fn clean_old_work(plan: &SegmentPlan) -> Result<(), GitError> {
    for entry in std::fs::read_dir(plan.input.repo.path())? {
        let entry = entry?;
        let path = entry.path();
        if !entry.file_type()?.is_dir()
            || !entry
                .file_name()
                .to_string_lossy()
                .starts_with("segment-work-")
            || path == plan.work
        {
            continue;
        }
        if !plan.outputs.iter().any(|o| o.path.starts_with(&path)) {
            std::fs::remove_dir_all(path)?;
            continue;
        }
        for child in std::fs::read_dir(&path)? {
            let child = child?;
            let child_path = child.path();
            if plan.outputs.iter().any(|o| o.path.starts_with(&child_path)) {
                continue;
            }
            if child.file_type()?.is_dir() {
                std::fs::remove_dir_all(child_path)?;
            } else {
                std::fs::remove_file(child_path)?;
            }
        }
    }
    Ok(())
}

/// Convenience full-cut runner. Steps always disable reuse of existing deltas.
pub async fn segment(
    input: MaintenanceInput,
    refs: RefsConfig,
    tuning: PacksConfig,
    resources: Resources,
) -> Result<SegmentedInput, GitError> {
    let selected = input.packs.iter().map(|p| p.checksum.clone()).collect();
    let mut plan = plan(input, selected, refs, resources).await?;
    for index in 0..plan.families.len() {
        plan = step(plan, index, tuning.clone()).await?;
    }
    finish(plan).await
}

/// Merge a sorted path-hint inventory with sorted selected OIDs, keeping one
/// line from each file in memory. OIDs are fixed-width lowercase hex.
fn intersect(list: &Path, selected: &Path, output: &Path) -> Result<(), GitError> {
    let mut ids = BufReader::new(File::open(selected)?).split(b'\n');
    let mut current = ids.next().transpose()?;
    let mut out = BufWriter::new(File::create(output)?);
    for line in BufReader::new(File::open(list)?).split(b'\n') {
        let line = line?;
        if line.is_empty() {
            continue;
        }
        let oid = line.split(|b| *b == b' ').next().unwrap_or_default();
        while current.as_deref().is_some_and(|id| id < oid) {
            current = ids.next().transpose()?;
        }
        if current.as_deref() == Some(oid) {
            out.write_all(&line)?;
            out.write_all(b"\n")?;
        }
        if current.is_none() {
            break;
        }
    }
    out.flush()?;
    Ok(())
}

fn invalid(e: impl std::fmt::Display) -> GitError {
    GitError::InvalidInput(e.to_string())
}

fn git(input: &MaintenanceInput) -> Command {
    let mut command = Command::new("git");
    command.arg("-C").arg(input.repo.path());
    command
}

fn append_index(
    path: &Path,
    input: &MaintenanceInput,
    out: &mut impl Write,
) -> Result<(), GitError> {
    let index = gix_pack::index::File::at(path, input.repo.object_format().kind()).map_err(ge)?;
    for entry in index.iter() {
        writeln!(out, "{}", entry.oid)?;
    }
    Ok(())
}

fn sort(input: &Path, output: &Path, work: &Path, resources: Resources) -> Result<(), GitError> {
    run(&mut sort_command(work, resources), input, output, work)
}

/// GNU and BSD `sort` both read a bare `--buffer-size` number as KiB; the `b`
/// suffix keeps the budget in bytes instead of 1024 times larger.
fn sort_command(work: &Path, resources: Resources) -> Command {
    let mut command = Command::new("sort");
    command
        .args(["--stable", "--unique", "--key=1,1", "--parallel=1"])
        .arg(format!("--buffer-size={}b", resources.sort_memory_bytes))
        .arg("--temporary-directory")
        .arg(work);
    command
}

fn difference(a: &Path, b: &Path, output: &Path, work: &Path) -> Result<(), GitError> {
    run(
        Command::new("comm").arg("-23").arg(a).arg(b),
        &work.join("empty"),
        output,
        work,
    )
}

fn run(command: &mut Command, input: &Path, output: &Path, work: &Path) -> Result<(), GitError> {
    let empty = work.join("empty");
    if !empty.exists() {
        File::create(&empty)?;
    }
    let error = work.join("command.stderr");
    let status = command
        .env("LC_ALL", "C")
        .stdin(File::open(input)?)
        .stdout(File::create(output)?)
        .stderr(File::create(&error)?)
        .status()?;
    if !status.success() {
        let mut stderr = String::new();
        File::open(error)?
            .take(64 * 1024)
            .read_to_string(&mut stderr)?;
        return Err(GitError::Subprocess {
            cmd: format!("{command:?}"),
            status: status.code(),
            stderr,
        });
    }
    Ok(())
}

fn split_types(typed: &Path, history: &Path, blobs: &Path) -> Result<(), GitError> {
    let mut history = BufWriter::new(File::create(history)?);
    let mut blobs = BufWriter::new(File::create(blobs)?);
    for line in BufReader::new(File::open(typed)?).split(b'\n') {
        let line = line?;
        if line.is_empty() {
            continue;
        }
        // Split exactly twice. Everything after the type is Git's path hint,
        // including spaces, tabs and non-UTF8 bytes, passed back unchanged.
        let mut fields = line.splitn(3, |b| *b == b' ');
        let oid = fields.next().unwrap_or_default();
        let kind = fields.next().unwrap_or_default();
        let path = fields.next().unwrap_or_default();
        gix_hash::ObjectId::from_hex(oid).map_err(ge)?;
        let out = match kind {
            b"blob" => &mut blobs,
            b"commit" | b"tree" | b"tag" => &mut history,
            _ => {
                return Err(invalid(
                    "missing or unsupported object while classifying segment",
                ));
            }
        };
        out.write_all(oid)?;
        if !path.is_empty() {
            out.write_all(b" ")?;
            out.write_all(path)?;
        }
        out.write_all(b"\n")?;
    }
    history.flush()?;
    blobs.flush()?;
    Ok(())
}

#[derive(Debug)]
pub struct PackMembership {
    pub checksum: String,
    /// Groups containing every indexed object in this pack, never their union.
    pub groups: Vec<String>,
    pub kind: PackKind,
    pub audience: Option<PackGroupKind>,
    pub mixed: bool,
}

#[derive(Debug)]
pub struct Classification {
    pub packs: Vec<PackMembership>,
    /// Exact family coverage from fully scoped member packs. Subset plans never
    /// certify a complete captured group graph.
    pub group_complete: std::collections::BTreeMap<String, bool>,
}

/// Inspect disk-backed family inventories without rewriting any pack bytes.
pub async fn classify(plan: SegmentPlan) -> Result<(SegmentPlan, Classification), GitError> {
    tokio::task::spawn_blocking(move || {
        let mut memberships = Vec::new();
        for descriptor in &plan.selected {
            let path = plan
                .input
                .repo
                .path()
                .join("objects/pack")
                .join(format!("pack-{}.idx", descriptor.checksum));
            let index = gix_pack::index::File::at(&path, plan.input.repo.object_format().kind())
                .map_err(ge)?;
            let mut counts =
                std::collections::BTreeMap::<String, (u64, Option<PackGroupKind>)>::new();
            let mut history = false;
            let mut blobs = false;
            for family in &plan.families {
                let count = intersection_count(&index, &family.objects)?;
                let entry = counts
                    .entry(family.group.clone())
                    .or_insert((0, family.audience));
                entry.0 += count;
                if count > 0 {
                    history |= family.kind == PackKind::History;
                    blobs |= family.kind == PackKind::Blobs;
                }
            }
            let common: Vec<_> = counts
                .into_iter()
                .filter(|(_, (count, _))| *count == descriptor.object_count && *count > 0)
                .collect();
            let audience = if common
                .iter()
                .any(|(_, (_, kind))| *kind == Some(PackGroupKind::Code))
            {
                Some(PackGroupKind::Code)
            } else if common
                .iter()
                .any(|(_, (_, kind))| *kind == Some(PackGroupKind::Meta))
            {
                Some(PackGroupKind::Meta)
            } else {
                None
            };
            let groups = common.into_iter().map(|(name, _)| name).collect::<Vec<_>>();
            let kind = match (history, blobs) {
                (true, false) => PackKind::History,
                (false, true) => PackKind::Blobs,
                _ => PackKind::Objects,
            };
            let mixed = groups.is_empty();
            memberships.push(PackMembership {
                checksum: descriptor.checksum.clone(),
                groups,
                kind,
                audience,
                mixed,
            });
        }
        let all_selected = plan.selected.len() == plan.input.packs.len();
        let mut complete = std::collections::BTreeMap::new();
        for family in &plan.families {
            complete.entry(family.group.clone()).or_insert(all_selected);
        }
        for (group, complete) in &mut complete {
            if !*complete || group == "_retained" {
                *complete = false;
                continue;
            }
            let expected_raw = plan
                .work
                .join(format!("classification-{group}.expected-raw"));
            let mut expected = BufWriter::new(File::create(&expected_raw)?);
            for family in plan.families.iter().filter(|f| &f.group == group) {
                for line in BufReader::new(File::open(&family.objects)?).split(b'\n') {
                    let line = line?;
                    if line.is_empty() {
                        continue;
                    }
                    expected.write_all(line.split(|b| *b == b' ').next().unwrap_or_default())?;
                    expected.write_all(b"\n")?;
                }
            }
            expected.flush()?;
            let expected_sorted = plan.work.join(format!("classification-{group}.expected"));
            sort(&expected_raw, &expected_sorted, &plan.work, plan.resources)?;
            let actual_raw = plan.work.join(format!("classification-{group}.actual-raw"));
            let mut actual = BufWriter::new(File::create(&actual_raw)?);
            for member in memberships
                .iter()
                .filter(|m| !m.mixed && m.groups.contains(group))
            {
                let idx = plan
                    .input
                    .repo
                    .path()
                    .join("objects/pack")
                    .join(format!("pack-{}.idx", member.checksum));
                append_index(&idx, &plan.input, &mut actual)?;
            }
            actual.flush()?;
            let actual_sorted = plan.work.join(format!("classification-{group}.actual"));
            sort(&actual_raw, &actual_sorted, &plan.work, plan.resources)?;
            let mismatch = plan.work.join(format!("classification-{group}.mismatch"));
            run(
                Command::new("comm")
                    .arg("-3")
                    .arg(expected_sorted)
                    .arg(actual_sorted),
                &plan.work.join("empty"),
                &mismatch,
                &plan.work,
            )?;
            *complete = std::fs::metadata(mismatch)?.len() == 0;
        }
        Ok((
            plan,
            Classification {
                packs: memberships,
                group_complete: complete,
            },
        ))
    })
    .await
    .map_err(ge)?
}

fn intersection_count(index: &gix_pack::index::File, list: &Path) -> Result<u64, GitError> {
    let mut objects = index.iter().peekable();
    let mut count = 0;
    for line in BufReader::new(File::open(list)?).split(b'\n') {
        let line = line?;
        if line.is_empty() {
            continue;
        }
        let oid =
            gix_hash::ObjectId::from_hex(line.split(|b| *b == b' ').next().unwrap_or_default())
                .map_err(ge)?;
        while objects.peek().is_some_and(|entry| entry.oid < oid) {
            objects.next();
        }
        if objects.peek().is_some_and(|entry| entry.oid == oid) {
            count += 1;
        }
        if objects.peek().is_none() {
            break;
        }
    }
    Ok(count)
}

#[cfg(test)]
#[path = "pack_segments_tests.rs"]
mod tests;
