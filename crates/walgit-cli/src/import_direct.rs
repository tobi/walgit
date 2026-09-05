#![allow(
    clippy::struct_excessive_bools,
    clippy::format_collect,
    clippy::unused_self
)]
//! `walgit import --direct` — publish a repository straight into the bucket.
//!
//! No local walgit cache copy, no index-pack, no replay: the importer takes
//! ready-made packfiles (ideally ONE pack with `.idx/.rev/.bitmap`, e.g. from
//! `git pack-objects --all --write-bitmap-index`), uploads them with striped
//! parallel part uploads + server-side compose, writes a checkpoint (ref
//! snapshot + pack set) and CAS-publishes the manifest. Replicas then
//! materialize by downloading the pack set and loading the ref snapshot.
//!
//! With `--bundle` it also publishes a bundle-uri full bundle *without
//! re-uploading the pack*: bundle = header object ∘ pack object via compose
//! (GCS), so a fresh `git clone` gets its bytes straight from the bucket/CDN.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::{Instant, SystemTime};

use anyhow::{Context, Result, bail};
use tracing::info;
use walgit_proto::prost::Message;

use walgit_config::Config;
use walgit_proto::keys;
use walgit_proto::v1::{
    BundleEntry, BundleList, Checkpoint, CheckpointRef, Manifest, PackRef, Ref, RefSnapshot,
};
use walgit_proto::{WAL_FORMAT_VERSION, time};
use walgit_store::{
    ObjectStore, ObjectStoreExt, Prefixed, PutBody, PutMode, PutOptions, StoreError, open_store,
};

use crate::cli::parse_repo_id;

pub struct DirectOptions {
    pub from: PathBuf,
    pub repo: String,
    /// Directory holding the pack set to publish (pack-*.pack + .idx [+ .rev, .bitmap]).
    /// Defaults to the source repo's objects/pack.
    pub packs: Option<PathBuf>,
    /// Also publish a full bundle for bundle-uri (header ∘ pack).
    pub bundle: bool,
    /// Bundle strategy name (defaults to the first `kind = "full"` strategy, else "import").
    pub bundle_strategy: Option<String>,
    /// Replace an existing non-empty repository (new checkpoint supersedes everything).
    pub replace: bool,
    /// Concurrent part uploads.
    pub parallelism: usize,
    /// Publish a commit-graph layer with the base pack (built from the source
    /// with `git commit-graph write --reachable --split=replace --changed-paths`
    /// unless `pack-<checksum>.commit-graph` already sits next to it).
    pub commit_graph: bool,
    /// Ref globs to publish (see `import::RefFilter`); empty = heads + tags.
    pub refs: Vec<String>,
    /// Also publish a history pack (commits + trees, `pack-objects
    /// --filter=blob:none`) derived from the base, built from the source
    /// into `<pack dir>/../walgit-history/` (D18).
    pub history_pack: bool,
    /// Verify, before anything is uploaded, that every published ref tip and
    /// its whole closure exist in the pack set being published (a scratch
    /// repository whose only object source is `--packs`). A large repository's import
    /// advertised `refs/remotes/origin/main` whose 1,952 blobs were in no pack
    /// (the original large-repository measurements): pushes then failed connectivity for days of
    /// history. Tips are always checked; the closure walk is skipped with
    /// `--no-verify-closure` (minutes on a 60 M-object set).
    pub verify_closure: bool,
}

struct LocalPack {
    checksum: String,
    pack: PathBuf,
    idx: PathBuf,
    rev: Option<PathBuf>,
    bitmap: Option<PathBuf>,
    commit_graph: Option<PathBuf>,
    pack_size: u64,
    idx_size: u64,
    object_count: u64,
    /// History pack of this base checksum (commits + trees, D18).
    history_of: Option<String>,
}

/// Start over even when the target's manifest moved since an interrupted import began, or
/// re-publish a completed import (a new seq superseding the previous one).
impl DirectOptions {
    fn marker_path(&self, pack_dir: &Path, id: &walgit_git::RepoId) -> PathBuf {
        pack_dir
            .parent()
            .unwrap_or(pack_dir)
            .join("walgit-import")
            .join(format!("{}-{}.json", id.owner(), id.name()))
    }
}

/// What a run did — the resumability contract in numbers (`tests/import_resume.rs`).
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ImportReport {
    /// Objects (packs + side-files) uploaded by this run.
    pub uploaded: usize,
    /// Objects skipped because the marker or a HEAD said they were already in the bucket.
    pub skipped: usize,
    /// Manifest CAS writes (0 or 1).
    pub cas: usize,
    /// Local phases executed this run.
    pub verified: bool,
    pub built_commit_graph: bool,
    pub built_history_pack: bool,
    /// The target already held exactly this import: nothing was done.
    pub noop: bool,
    pub resumed: bool,
    pub seq: u64,
}

/// Phases of a direct import, in order; the marker names the last one completed. Uploads are
/// tracked per object (`ImportMarker::uploaded`), the bundle by its entry.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "kebab-case")]
pub enum ImportPhase {
    Started,
    Verified,
    SideFiles,
    HistoryPack,
    Uploaded,
    Bundled,
}

/// `walgit-import/<owner>-<repo>.json` next to the pack dir: what an interrupted import had
/// done. Keyed by the *intent* (repository + the exact ref set being published): a different
/// ref filter or a moved source is a different import and discards the marker.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ImportMarker {
    pub repo: String,
    /// SHA-1 over the sorted `oid name` lines of the ref snapshot being published.
    pub tips_hash: String,
    /// Target manifest version when the import started (`None` = the repository did not exist).
    pub base_manifest_version: Option<String>,
    pub base_head_seq: u64,
    /// The seq this import publishes at.
    pub seq: u64,
    pub phase: ImportPhase,
    /// Store keys already uploaded (pack + side-files), so a resumed run skips them without a HEAD.
    #[serde(default)]
    pub uploaded: Vec<String>,
    /// `pack-<hash>.pack` path of the history pack built from the source (reused on resume).
    #[serde(default)]
    pub history_pack: Option<PathBuf>,
    /// The composed full bundle, once published (its list entry is added after the manifest
    /// CAS): the protobuf `BundleEntry`, hex-encoded.
    #[serde(default)]
    pub bundle: Option<String>,
}

fn entry_to_hex(e: &BundleEntry) -> String {
    e.encode_to_vec()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}
fn entry_from_hex(s: &str) -> Option<BundleEntry> {
    let bytes: Option<Vec<u8>> = (0..s.len() / 2)
        .map(|i| u8::from_str_radix(&s[2 * i..2 * i + 2], 16).ok())
        .collect();
    BundleEntry::decode(bytes?.as_slice()).ok()
}

fn tips_hash(snap: &RefSnapshot) -> String {
    use sha1::Digest;
    let mut lines: Vec<String> = snap
        .refs
        .iter()
        .map(|r| format!("{} {}", r.oid, r.name))
        .collect();
    lines.sort();
    let mut h = sha1::Sha1::new();
    for l in &lines {
        h.update(l.as_bytes());
        h.update(b"\n");
    }
    h.update(snap.head_target.as_bytes());
    format!("{:x}", h.finalize())
}

fn read_import_marker(path: &Path) -> Option<ImportMarker> {
    serde_json::from_slice(&std::fs::read(path).ok()?).ok()
}

fn write_import_marker(path: &Path, m: &ImportMarker) -> Result<()> {
    std::fs::create_dir_all(path.parent().unwrap())?;
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, serde_json::to_vec_pretty(m)?)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

/// Test hook: abort the import of `repo` right after `phase`'s marker was written (a SIGTERM /
/// lost network between phases). `Uploaded` aborts after the first object of the upload phase.
pub static TEST_ABORT_AFTER: parking_lot::Mutex<
    Option<std::collections::HashMap<String, ImportPhase>>,
> = parking_lot::Mutex::new(None);

fn abort_after(repo: &str, phase: ImportPhase) -> Result<()> {
    let hook = TEST_ABORT_AFTER
        .lock()
        .as_ref()
        .and_then(|m| m.get(repo).copied());
    if hook == Some(phase) {
        bail!("import aborted by test hook after phase {phase:?}");
    }
    Ok(())
}

#[cfg(test)]
fn set_abort(repo: &str, phase: Option<ImportPhase>) {
    let mut g = TEST_ABORT_AFTER.lock();
    let m = g.get_or_insert_with(Default::default);
    match phase {
        Some(p) => {
            m.insert(repo.to_string(), p);
        }
        None => {
            m.remove(repo);
        }
    }
}

/// Decide what to do with a marker found on disk for this intent. Pure, unit-tested.
#[derive(Debug, PartialEq, Eq)]
pub enum ResumeDecision {
    /// No marker (or a marker for another intent): start over.
    Fresh,
    /// Continue from the marker's phase.
    Resume,
    /// The target moved since the import started and `--force` was not given.
    Refuse {
        started_at: Option<String>,
        now: Option<String>,
    },
}

pub fn decide_resume(
    marker: Option<&ImportMarker>,
    repo: &str,
    tips: &str,
    current_version: Option<&str>,
    force: bool,
) -> ResumeDecision {
    let Some(m) = marker else {
        return ResumeDecision::Fresh;
    };
    if m.repo != repo || m.tips_hash != tips {
        return ResumeDecision::Fresh;
    }
    if m.base_manifest_version.as_deref() != current_version {
        return if force {
            ResumeDecision::Fresh
        } else {
            ResumeDecision::Refuse {
                started_at: m.base_manifest_version.clone(),
                now: current_version.map(str::to_owned),
            }
        };
    }
    ResumeDecision::Resume
}

pub async fn run(opts: DirectOptions, cfg: &Arc<Config>, force: bool) -> Result<()> {
    let store = open_store(cfg).await?;
    let report = run_with_store(opts, cfg, store, force).await?;
    println!(
        "import: seq {}, {} object(s) uploaded, {} skipped, {} manifest write(s){}{}",
        report.seq,
        report.uploaded,
        report.skipped,
        report.cas,
        if report.resumed { ", resumed" } else { "" },
        if report.noop { ", nothing to do" } else { "" }
    );
    Ok(())
}

/// The import proper, against `store` (tests pass a shared in-memory store). `force`: see
/// [`ResumeDecision`]. Re-running a completed import is a no-op; an interrupted one resumes.
pub async fn run_with_store(
    opts: DirectOptions,
    cfg: &Arc<Config>,
    store: walgit_store::DynStore,
    force: bool,
) -> Result<ImportReport> {
    let started = Instant::now();
    let mut report = ImportReport::default();
    let (owner, name) = parse_repo_id(&opts.repo)?;
    let id = walgit_git::RepoId::new(owner, name)?;
    let repo_key = id.to_string();
    let git_dir = crate::import::resolve_git_dir(&opts.from)?;
    let format = crate::import::detect_object_format(&git_dir)?;
    info!(git_dir = %git_dir.display(), format = ?format, "direct import");

    // ---- refs -----------------------------------------------------------------
    let mut snap = read_ref_snapshot(&git_dir)?;
    {
        let filter = crate::import::RefFilter::new(opts.refs.clone());
        let before = snap.refs.len();
        let head = snap.head_target.clone();
        snap.refs.retain(|r| filter.keep(&r.name, &head));
        println!(
            "refs: publishing {} of {} ({} dropped by the ref filter)",
            snap.refs.len(),
            before,
            before - snap.refs.len()
        );
    }
    println!(
        "refs: {} ({} heads, {} tags), HEAD -> {}",
        snap.refs.len(),
        snap.refs
            .iter()
            .filter(|r| r.name.starts_with("refs/heads/"))
            .count(),
        snap.refs
            .iter()
            .filter(|r| r.name.starts_with("refs/tags/"))
            .count(),
        if snap.head_target.is_empty() {
            "(detached)"
        } else {
            &snap.head_target
        }
    );
    let tips = tips_hash(&snap);

    // ---- packs ----------------------------------------------------------------
    let pack_dir = opts
        .packs
        .clone()
        .unwrap_or_else(|| git_dir.join("objects").join("pack"));
    let mut packs = scan_packs(&pack_dir)?;
    anyhow::ensure!(
        !packs.is_empty(),
        "no pack-*.pack in {}",
        pack_dir.display()
    );
    if opts.packs.is_none() {
        let loose: u64 = count_loose(&git_dir);
        anyhow::ensure!(
            loose == 0,
            "{loose} loose objects in source; run `git repack -d` (or pass --packs)"
        );
    }
    let object_checksums: Vec<String> = packs
        .iter()
        .filter(|p| p.history_of.is_none())
        .map(|p| p.checksum.clone())
        .collect();

    // ---- store: is this import already there? resume or fresh? ----------------------
    let repo_store = Prefixed::new(store, id.store_prefix());
    let existing = repo_store.get_bytes(keys::MANIFEST).await?;
    let (base_manifest, base_version) = match existing {
        Some((meta, bytes)) => {
            let m = Manifest::decode(bytes.as_ref()).context("decoding existing manifest")?;
            if m.object_format != format.as_str() {
                bail!(
                    "object format mismatch: repo is {}, source is {}",
                    m.object_format,
                    format.as_str()
                );
            }
            (Some(m), Some(meta.version))
        }
        None => (None, None),
    };
    // Idempotent: the manifest already holds exactly this pack set (and a history pack when asked
    // for) → nothing to do, 0 uploads, 0 CAS. Checked before `--replace` is required.
    if let Some(m) = &base_manifest {
        let live: std::collections::HashSet<&str> =
            m.packs.iter().map(|p| p.checksum.as_str()).collect();
        let all_object_packs_live = object_checksums.iter().all(|c| live.contains(c.as_str()));
        let history_ok = !opts.history_pack
            || m.packs.iter().any(|p| {
                p.kind == walgit_proto::v1::PackKind::History as i32
                    && object_checksums.contains(&p.derived_from)
            });
        if all_object_packs_live && history_ok && m.head_seq > 0 {
            println!(
                "{id} already holds this import (seq {}, {} pack(s)); nothing to do",
                m.head_seq,
                m.packs.len()
            );
            let marker_path = opts.marker_path(&pack_dir, &id);
            let _ = std::fs::remove_file(&marker_path);
            report.noop = true;
            report.seq = m.head_seq;
            return Ok(report);
        }
        if m.head_seq > 0 && !opts.replace {
            bail!(
                "{} already has {} entries; pass --replace to supersede its content",
                id,
                m.head_seq
            );
        }
    }
    let marker_path = opts.marker_path(&pack_dir, &id);
    let current_version = base_version.as_ref().map(|v| v.as_str().to_string());
    let mut marker = match decide_resume(
        read_import_marker(&marker_path).as_ref(),
        &repo_key,
        &tips,
        current_version.as_deref(),
        force,
    ) {
        ResumeDecision::Resume => {
            let m = read_import_marker(&marker_path).unwrap();
            println!(
                "resuming import started at manifest {:?} (phase {:?} done, {} object(s) uploaded)",
                m.base_manifest_version,
                m.phase,
                m.uploaded.len()
            );
            report.resumed = true;
            m
        }
        ResumeDecision::Refuse { started_at, now } => bail!(
            "an interrupted import of {id} started when the manifest was {started_at:?}; it is {now:?} now (someone pushed or imported). \
             Re-run with --force to start over from the current state (the partial uploads are reused where their checksums match)"
        ),
        ResumeDecision::Fresh => {
            if marker_path.exists() {
                println!(
                    "discarding an interrupted import of a different intent or base (marker {})",
                    marker_path.display()
                );
            }
            let m = ImportMarker {
                repo: repo_key.clone(),
                tips_hash: tips.clone(),
                base_manifest_version: current_version.clone(),
                base_head_seq: base_manifest.as_ref().map_or(0, |m| m.head_seq),
                seq: base_manifest.as_ref().map_or(0, |m| m.head_seq) + 1,
                phase: ImportPhase::Started,
                uploaded: Vec::new(),
                history_pack: None,
                bundle: None,
            };
            write_import_marker(&marker_path, &m)?;
            m
        }
    };
    let seq = marker.seq;
    report.seq = seq;

    // ---- the invariant: every published tip's closure is in the pack set -----------
    if marker.phase < ImportPhase::Verified {
        let t = Instant::now();
        let tip_oids: Vec<String> = snap.refs.iter().map(|r| r.oid.clone()).collect();
        let missing = verify_refs_in_packs(&pack_dir, &tip_oids, opts.verify_closure)?;
        anyhow::ensure!(
            missing.is_empty(),
            "{} object(s) reachable from the refs being published are in no pack under {} \
             (first: {}); the pack set and the ref snapshot disagree — rebuild the pack from exactly \
             these refs (`git pack-objects --revs` with them) or narrow `--refs`",
            missing.len(),
            pack_dir.display(),
            missing[0]
        );
        println!(
            "verified: {} ref tip(s){} present in the pack set ({:.1}s)",
            tip_oids.len(),
            if opts.verify_closure {
                " and their full closure"
            } else {
                ""
            },
            t.elapsed().as_secs_f64()
        );
        report.verified = true;
        marker.phase = ImportPhase::Verified;
        write_import_marker(&marker_path, &marker)?;
        abort_after(&repo_key, ImportPhase::Verified)?;
    } else {
        println!("verified earlier (marker); skipping the closure walk");
    }

    // ---- side-files: one commit-graph layer next to the base (file presence = done) ----------
    if marker.phase < ImportPhase::SideFiles {
        if opts.commit_graph && packs[0].commit_graph.is_none() {
            let t = Instant::now();
            let side = packs[0].pack.with_extension("commit-graph");
            build_commit_graph_layer(&git_dir, &side)?;
            println!(
                "commit-graph: {} bytes in {:.1}s -> {}",
                std::fs::metadata(&side)?.len(),
                t.elapsed().as_secs_f64(),
                side.display()
            );
            packs[0].commit_graph = Some(side);
            report.built_commit_graph = true;
        }
        marker.phase = ImportPhase::SideFiles;
        write_import_marker(&marker_path, &marker)?;
        abort_after(&repo_key, ImportPhase::SideFiles)?;
    }

    // ---- history pack (D18), reused from the marker / the walgit-history dir -------------
    if opts.history_pack && !packs.iter().any(|p| p.history_of.is_some()) {
        let dir = pack_dir
            .parent()
            .unwrap_or(&pack_dir)
            .join("walgit-history");
        let reuse = marker
            .history_pack
            .as_ref()
            .filter(|p| p.exists() && p.with_extension("idx").exists())
            .cloned()
            .or_else(|| {
                // A history pack of this base left by an earlier run whose marker is gone.
                scan_packs(&dir).ok().and_then(|v| {
                    v.into_iter()
                        .find(|p| p.history_of.as_deref() == Some(packs[0].checksum.as_str()))
                        .map(|p| p.pack)
                })
            });
        let hp = if let Some(pack) = reuse {
            let v = scan_packs(&dir)?;
            let hp = v
                .into_iter()
                .find(|p| p.pack == pack)
                .context("history pack vanished")?;
            println!(
                "history pack {} reused from {}",
                hp.checksum,
                hp.pack.display()
            );
            hp
        } else {
            let t = Instant::now();
            std::fs::create_dir_all(&dir)?;
            let hp = build_history_pack(&git_dir, &dir, &packs[0].checksum)?;
            println!(
                "history pack {}: {} bytes, {} objects (commits + trees) in {:.1}s -> {}",
                hp.checksum,
                hp.pack_size,
                hp.object_count,
                t.elapsed().as_secs_f64(),
                hp.pack.display()
            );
            report.built_history_pack = true;
            hp
        };
        marker.history_pack = Some(hp.pack.clone());
        packs.push(hp);
    }
    if marker.phase < ImportPhase::HistoryPack {
        marker.phase = ImportPhase::HistoryPack;
        write_import_marker(&marker_path, &marker)?;
        abort_after(&repo_key, ImportPhase::HistoryPack)?;
    }
    let object_packs = packs.iter().filter(|p| p.history_of.is_none()).count();
    let total_bytes: u64 = packs.iter().map(|p| p.pack_size + p.idx_size).sum();
    for p in &packs {
        println!(
            "pack {}: {} bytes, {} objects{}{}{}",
            p.checksum,
            p.pack_size,
            p.object_count,
            if p.rev.is_some() { ", rev" } else { "" },
            if p.bitmap.is_some() { ", bitmap" } else { "" },
            if p.commit_graph.is_some() {
                ", commit-graph"
            } else {
                ""
            }
        );
    }
    if object_packs > 1 || packs[0].bitmap.is_none() {
        eprintln!(
            "note: {} pack(s), bitmap={} — for fastest serving import ONE pack built with \
             `git pack-objects --all --write-bitmap-index <dir>/pack`",
            packs.len(),
            packs[0].bitmap.is_some()
        );
    }

    // ---- upload packs + side-files (striped, parallel; per-object done set) ----------------
    // Each object: skip if the marker says it landed; else one HEAD (size must match); else upload.
    // The HEADs of one pack's side-files run in parallel (one round), never one after the other.
    let up_started = Instant::now();
    let mut pack_refs = Vec::new();
    let mut first_upload_done = false;
    for p in &packs {
        let mut objects: Vec<(String, PathBuf, u64, Option<&'static str>, usize)> = vec![(
            keys::pack_key(&p.checksum),
            p.pack.clone(),
            p.pack_size,
            Some("application/x-git-pack"),
            opts.parallelism,
        )];
        objects.push((
            keys::idx_key(&p.checksum),
            p.idx.clone(),
            p.idx_size,
            None,
            4,
        ));
        if let Some(r) = &p.rev {
            objects.push((
                keys::rev_key(&p.checksum),
                r.clone(),
                std::fs::metadata(r)?.len(),
                None,
                4,
            ));
        }
        if let Some(b) = &p.bitmap {
            objects.push((
                keys::bitmap_key(&p.checksum),
                b.clone(),
                std::fs::metadata(b)?.len(),
                None,
                4,
            ));
        }
        if let Some(g) = &p.commit_graph {
            objects.push((
                keys::commit_graph_key(&p.checksum),
                g.clone(),
                std::fs::metadata(g)?.len(),
                None,
                4,
            ));
        }
        // Which of them are already there: the marker first (no request), then HEADs in parallel.
        let unknown: Vec<&(String, PathBuf, u64, Option<&'static str>, usize)> = objects
            .iter()
            .filter(|o| !marker.uploaded.contains(&o.0))
            .collect();
        let heads = futures::future::join_all(unknown.iter().map(|o| {
            let st = repo_store.clone();
            let key = o.0.clone();
            let size = o.2;
            async move {
                st.head(&key)
                    .await
                    .map(|m| m.is_some_and(|m| m.size == size))
            }
        }))
        .await;
        let mut present: std::collections::HashSet<String> =
            marker.uploaded.iter().cloned().collect();
        for (o, h) in unknown.iter().zip(heads) {
            if h? {
                present.insert(o.0.clone());
            }
        }
        for (key, path, size, ct, par) in &objects {
            if present.contains(key) {
                println!("{key} already in store, skipping upload");
                report.skipped += 1;
                if !marker.uploaded.contains(key) {
                    marker.uploaded.push(key.clone());
                }
                continue;
            }
            let t = Instant::now();
            walgit_store::util::put_file_parallel(
                &repo_store,
                key,
                path,
                PutOptions {
                    mode: PutMode::Overwrite,
                    immutable: true,
                    content_type: *ct,
                },
                *par,
            )
            .await
            .with_context(|| format!("uploading {}", path.display()))?;
            println!(
                "uploaded {key} ({:.1} MB/s)",
                *size as f64 / 1e6 / t.elapsed().as_secs_f64().max(0.001)
            );
            report.uploaded += 1;
            marker.uploaded.push(key.clone());
            write_import_marker(&marker_path, &marker)?;
            if !first_upload_done {
                first_upload_done = true;
                abort_after(&repo_key, ImportPhase::Uploaded)?;
            }
        }
        pack_refs.push(PackRef {
            checksum: p.checksum.clone(),
            pack_size: p.pack_size,
            idx_size: p.idx_size,
            has_rev: p.rev.is_some(),
            has_bitmap: p.bitmap.is_some(),
            has_commit_graph: p.commit_graph.is_some(),
            object_count: p.object_count,
            seq,
            tier: 2,
            kind: if p.history_of.is_some() {
                walgit_proto::v1::PackKind::History as i32
            } else {
                walgit_proto::v1::PackKind::Objects as i32
            },
            derived_from: p.history_of.clone().unwrap_or_default(),
        });
    }
    if marker.phase < ImportPhase::Uploaded {
        marker.phase = ImportPhase::Uploaded;
        write_import_marker(&marker_path, &marker)?;
    }
    println!(
        "upload: {} bytes, {} object(s) uploaded, {} skipped, in {:.1}s",
        total_bytes,
        report.uploaded,
        report.skipped,
        up_started.elapsed().as_secs_f64()
    );

    // ---- checkpoint refs (small, idempotent re-put) -----------------------------------------
    let refs_key = keys::checkpoint_refs_key(seq);
    let mut snap = snap;
    snap.seq = seq;
    snap.object_format = format.as_str().to_string();
    snap.created_at = Some(time::now());
    repo_store
        .put(
            &refs_key,
            PutBody::Bytes(snap.encode_to_vec().into()),
            PutOptions {
                immutable: true,
                ..Default::default()
            },
        )
        .await?;

    // ---- bundle (header ∘ pack), once --------------------------------------------------------
    let mut bundle_key = String::new();
    let mut bundle_entry: Option<BundleEntry> = marker.bundle.as_deref().and_then(entry_from_hex);
    if opts.bundle && bundle_entry.is_none() {
        if object_packs == 1 {
            let strategy = opts.bundle_strategy.clone().unwrap_or_else(|| {
                cfg.bundles
                    .strategy
                    .iter()
                    .find(|s| s.kind == walgit_config::BundleKind::Full)
                    .map_or_else(|| "import".to_string(), |s| s.name.clone())
            });
            let p0 = &packs[0];
            match walgit_bundle::ops::compose_full(
                &repo_store,
                &p0.checksum,
                p0.pack_size,
                Some(p0.pack.as_path()),
                &snap,
                format,
                &strategy,
                seq,
                0,
                SystemTime::now(),
                0,
                None,
            )
            .await
            {
                Ok(entry) => {
                    println!(
                        "bundle: {} ({} bytes, {} tips)",
                        entry.key,
                        entry.size,
                        entry.tips.len()
                    );
                    marker.bundle = Some(entry_to_hex(&entry));
                    bundle_entry = Some(entry);
                }
                Err(e) => eprintln!("bundle publish failed (import continues): {e:#}"),
            }
        } else {
            eprintln!(
                "--bundle needs exactly one object pack (got {object_packs}); skipping bundle"
            );
        }
    }
    if let Some(e) = &bundle_entry {
        bundle_key = e.key.clone();
    }
    if marker.phase < ImportPhase::Bundled {
        marker.phase = ImportPhase::Bundled;
        write_import_marker(&marker_path, &marker)?;
        abort_after(&repo_key, ImportPhase::Bundled)?;
    }

    let checkpoint = Checkpoint {
        seq,
        object_format: format.as_str().to_string(),
        packs: pack_refs.clone(),
        refs_key: refs_key.clone(),
        ref_count: snap.refs.len() as u64,
        bundle_key,
        created_at: Some(time::now()),
        writer: format!("walgit-import@{}", hostname()),
    };
    let cp_key = keys::checkpoint_key(seq);
    repo_store
        .put(
            &cp_key,
            PutBody::Bytes(checkpoint.encode_to_vec().into()),
            PutOptions {
                immutable: true,
                ..Default::default()
            },
        )
        .await?;

    // ---- manifest CAS (the linearization point) --------------------------------------
    let manifest = Manifest {
        format_version: WAL_FORMAT_VERSION,
        repo: id.to_string(),
        object_format: format.as_str().to_string(),
        head_seq: seq,
        min_seq: seq + 1,
        // An import's first state is the import itself (history before it is
        // not in the WAL): first_state_at = as_of = now.
        checkpoint: Some(CheckpointRef {
            seq,
            key: cp_key,
            created_at: Some(walgit_proto::time::now()),
            first_state_at: Some(walgit_proto::time::now()),
            as_of: Some(walgit_proto::time::now()),
        }),
        log_segments: vec![],
        packs: pack_refs,
        updated_at: Some(time::now()),
        writer: format!("walgit-import@{}", hostname()),
        revision: base_manifest.as_ref().map_or(0, |m| m.revision) + 1,
        settings: None,
    };
    let mode = match base_version {
        Some(v) => PutMode::Update(v),
        None => PutMode::Create,
    };
    match repo_store
        .put(
            keys::MANIFEST,
            PutBody::Bytes(manifest.encode_to_vec().into()),
            mode.into(),
        )
        .await
    {
        Ok(meta) => {
            report.cas = 1;
            println!(
                "published {} at seq {} (manifest {})",
                id, seq, meta.version
            );
        }
        Err(StoreError::PreconditionFailed { .. }) => {
            bail!(
                "manifest changed underneath the import (concurrent writer); re-run (the marker keeps the uploads; --force to accept the new base)"
            )
        }
        Err(e) => return Err(e.into()),
    }

    // ---- bundle list (after the manifest: nothing advertises objects before they exist)
    if let Some(entry) = bundle_entry {
        let keep_strategy = entry.strategy.clone();
        let (_, list) = cas_update_bundle_list(&repo_store, |cur| {
            let mut list = cur.cloned().unwrap_or(BundleList {
                mode: "all".into(),
                heuristic: "creationToken".into(),
                bundles: vec![],
                updated_at: None,
                skipped: vec![],
            });
            // A new full import supersedes older bundles of the same strategy and
            // every incremental built on them.
            let old_ids: Vec<String> = list
                .bundles
                .iter()
                .filter(|b| b.strategy == keep_strategy)
                .map(|b| b.id.clone())
                .collect();
            list.bundles
                .retain(|b| b.strategy != keep_strategy && !old_ids.contains(&b.base_id));
            list.bundles.push(entry.clone());
            list.updated_at = Some(time::now());
            list
        })
        .await?;
        println!("bundle list: {} bundle(s)", list.bundles.len());
    }

    // Done: the marker goes (a re-run is answered by the manifest itself).
    let _ = std::fs::remove_file(&marker_path);
    println!("done in {:.1}s", started.elapsed().as_secs_f64());
    Ok(report)
}

// ---- helpers -------------------------------------------------------------------

fn hostname() -> String {
    std::fs::read_to_string("/etc/hostname")
        .map_or_else(|_| "local".into(), |s| s.trim().to_string())
}

fn count_loose(git_dir: &Path) -> u64 {
    Command::new("git")
        .args(["count-objects"])
        .current_dir(git_dir)
        .output()
        .ok()
        .and_then(|o| {
            String::from_utf8_lossy(&o.stdout)
                .split_whitespace()
                .next()
                .and_then(|n| n.parse().ok())
        })
        .unwrap_or(0)
}

/// `for-each-ref` with peeled tags + HEAD target.
fn read_ref_snapshot(git_dir: &Path) -> Result<RefSnapshot> {
    let out = Command::new("git")
        .args([
            "for-each-ref",
            "--format=%(objectname)%00%(*objectname)%00%(refname)",
        ])
        .current_dir(git_dir)
        .output()
        .context("git for-each-ref")?;
    anyhow::ensure!(
        out.status.success(),
        "git for-each-ref failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let mut refs = Vec::new();
    for line in String::from_utf8_lossy(&out.stdout).lines() {
        let mut p = line.split('\0');
        let (Some(oid), Some(peeled), Some(name)) = (p.next(), p.next(), p.next()) else {
            continue;
        };
        if name.is_empty() || oid.is_empty() {
            continue;
        }
        refs.push(Ref {
            name: name.to_string(),
            oid: oid.to_string(),
            peeled: peeled.to_string(),
        });
    }
    refs.sort_by(|a, b| a.name.cmp(&b.name));
    refs.dedup_by(|a, b| a.name == b.name);
    let head = Command::new("git")
        .args(["symbolic-ref", "-q", "HEAD"])
        .current_dir(git_dir)
        .output()?;
    let head_target = if head.status.success() {
        String::from_utf8_lossy(&head.stdout).trim().to_string()
    } else {
        String::new()
    };
    Ok(RefSnapshot {
        seq: 0,
        object_format: String::new(),
        refs,
        head_target,
        created_at: None,
    })
}

/// Objects reachable from `tips` that are in no pack under `pack_dir`: a scratch
/// bare repository whose `objects/pack` is `pack_dir` (symlink), `git rev-list
/// --objects --missing=print`. With `closure = false` only the tips themselves
/// are looked up (`cat-file --batch-check`).
pub fn verify_refs_in_packs(
    pack_dir: &Path,
    tips: &[String],
    closure: bool,
) -> Result<Vec<String>> {
    if tips.is_empty() {
        return Ok(Vec::new());
    }
    let scratch = tempfile::tempdir().context("scratch repo for verification")?;
    let objects = scratch.path().join("objects");
    std::fs::create_dir_all(objects.join("info"))?;
    std::fs::create_dir_all(scratch.path().join("refs"))?;
    std::os::unix::fs::symlink(std::fs::canonicalize(pack_dir)?, objects.join("pack"))?;
    std::fs::write(scratch.path().join("HEAD"), "ref: refs/heads/main\n")?;
    std::fs::write(
        scratch.path().join("config"),
        "[core]\n\tbare = true\n\trepositoryformatversion = 0\n",
    )?;
    let mut uniq: Vec<&str> = tips.iter().map(String::as_str).collect();
    uniq.sort_unstable();
    uniq.dedup();
    let mut input = uniq.join("\n");
    input.push('\n');
    let run = |args: &[&str], stdin: &str| -> Result<std::process::Output> {
        use std::io::Write;
        let mut child = Command::new("git")
            .arg("--git-dir")
            .arg(scratch.path())
            .args(args)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .with_context(|| format!("git {}", args.join(" ")))?;
        child.stdin.take().unwrap().write_all(stdin.as_bytes())?;
        Ok(child.wait_with_output()?)
    };
    // Tips first (cheap, names the exact ref problem).
    let out = run(&["cat-file", "--batch-check"], &input)?;
    let mut missing: Vec<String> = String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter(|l| l.ends_with(" missing"))
        .map(|l| l.split(' ').next().unwrap_or("").to_string())
        .collect();
    if !missing.is_empty() || !closure {
        return Ok(missing);
    }
    let out = run(
        &["rev-list", "--objects", "--missing=print", "--stdin"],
        &input,
    )?;
    anyhow::ensure!(
        out.status.success(),
        "git rev-list --objects --missing=print: {}",
        String::from_utf8_lossy(&out.stderr).trim()
    );
    missing.extend(
        String::from_utf8_lossy(&out.stdout)
            .lines()
            .filter_map(|l| l.strip_prefix('?'))
            .map(|l| l.split(' ').next().unwrap_or("").to_string()),
    );
    Ok(missing)
}

fn scan_packs(dir: &Path) -> Result<Vec<LocalPack>> {
    let mut out = Vec::new();
    for e in std::fs::read_dir(dir).with_context(|| format!("reading {}", dir.display()))? {
        let p = e?.path();
        if p.extension().is_none_or(|x| x != "pack") {
            continue;
        }
        let stem = p.file_stem().and_then(|s| s.to_str()).unwrap_or("");
        let hex = stem.strip_prefix("pack-").unwrap_or(stem);
        anyhow::ensure!(
            !hex.is_empty() && hex.bytes().all(|b| b.is_ascii_hexdigit()),
            "pack name is not a checksum: {stem}"
        );
        let idx = p.with_extension("idx");
        anyhow::ensure!(idx.exists(), "missing {}", idx.display());
        let opt = |ext: &str| {
            let q = p.with_extension(ext);
            q.exists().then_some(q)
        };
        out.push(LocalPack {
            checksum: hex.to_string(),
            pack_size: std::fs::metadata(&p)?.len(),
            idx_size: std::fs::metadata(&idx)?.len(),
            object_count: idx_object_count(&idx)?,
            rev: opt("rev"),
            bitmap: opt("bitmap"),
            commit_graph: opt("commit-graph"),
            history_of: std::fs::read_to_string(p.with_extension("history"))
                .ok()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty()),
            pack: p,
            idx,
        });
    }
    out.sort_by(|a, b| b.pack_size.cmp(&a.pack_size));
    Ok(out)
}

/// History pack of `base` from the source: `git pack-objects --filter=blob:none
/// --revs --all` into `dir/pack-<hash>.pack` + `.history` marker.
fn build_history_pack(git_dir: &Path, dir: &Path, base: &str) -> Result<LocalPack> {
    use std::io::Write;
    let tips = Command::new("git")
        .arg("--git-dir")
        .arg(git_dir)
        .args(["for-each-ref", "--format=%(objectname)"])
        .output()
        .context("git for-each-ref")?;
    anyhow::ensure!(tips.status.success(), "git for-each-ref failed");
    let mut child = Command::new("git")
        .arg("--git-dir")
        .arg(git_dir)
        .args([
            "pack-objects",
            "--filter=blob:none",
            "--revs",
            "--delta-base-offset",
        ])
        .arg(dir.join("pack"))
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .context("git pack-objects --filter=blob:none")?;
    child.stdin.take().unwrap().write_all(&tips.stdout)?;
    let out = child.wait_with_output()?;
    anyhow::ensure!(
        out.status.success(),
        "git pack-objects --filter=blob:none failed"
    );
    let hash = String::from_utf8_lossy(&out.stdout).trim().to_string();
    anyhow::ensure!(hash.len() >= 40, "pack-objects printed no hash: {hash:?}");
    let pack = dir.join(format!("pack-{hash}.pack"));
    let idx = dir.join(format!("pack-{hash}.idx"));
    std::fs::write(
        dir.join(format!("pack-{hash}.history")),
        format!("{base}\n"),
    )?;
    Ok(LocalPack {
        checksum: hash,
        pack_size: std::fs::metadata(&pack)?.len(),
        idx_size: std::fs::metadata(&idx)?.len(),
        object_count: idx_object_count(&idx)?,
        rev: None,
        bitmap: None,
        commit_graph: None,
        history_of: Some(base.to_string()),
        pack,
        idx,
    })
}

/// `git commit-graph write --reachable --split=replace --changed-paths` in the
/// source, then copy the single chain layer to `dest`.
fn build_commit_graph_layer(git_dir: &Path, dest: &Path) -> Result<()> {
    let st = std::process::Command::new("git")
        .arg("--git-dir")
        .arg(git_dir)
        .args([
            "commit-graph",
            "write",
            "--reachable",
            "--split=replace",
            "--changed-paths",
        ])
        .status()
        .context("git commit-graph write")?;
    anyhow::ensure!(st.success(), "git commit-graph write failed in source");
    let dir = git_dir.join("objects").join("info").join("commit-graphs");
    let chain = std::fs::read_to_string(dir.join("commit-graph-chain"))
        .context("commit-graph-chain missing after --split=replace")?;
    let hash = chain
        .lines()
        .last()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .ok_or_else(|| anyhow::anyhow!("empty commit-graph-chain"))?;
    std::fs::copy(dir.join(format!("graph-{hash}.graph")), dest)?;
    Ok(())
}

fn idx_object_count(idx: &Path) -> Result<u64> {
    use std::io::{Read, Seek, SeekFrom};
    let mut f = std::fs::File::open(idx)?;
    f.seek(SeekFrom::Start(8 + 255 * 4))?;
    let mut b = [0u8; 4];
    f.read_exact(&mut b)?;
    Ok(u64::from(u32::from_be_bytes(b)))
}

/// Git bundle header for `snap` (HEAD + refs/heads/* + refs/tags/*), no prerequisites.
async fn cas_update_bundle_list<F>(
    store: &Prefixed,
    mut f: F,
) -> Result<(walgit_store::Version, BundleList)>
where
    F: FnMut(Option<&BundleList>) -> BundleList,
{
    for _ in 0..16 {
        let cur = store.get_bytes(keys::BUNDLE_LIST).await?;
        let (mode, cur_list) = match &cur {
            None => (PutMode::Create, None),
            Some((meta, bytes)) => (
                PutMode::Update(meta.version.clone()),
                Some(BundleList::decode(bytes.as_ref())?),
            ),
        };
        let new_list = f(cur_list.as_ref());
        match store
            .put(
                keys::BUNDLE_LIST,
                PutBody::Bytes(new_list.encode_to_vec().into()),
                PutOptions::from(mode),
            )
            .await
        {
            Ok(meta) => return Ok((meta.version, new_list)),
            Err(StoreError::PreconditionFailed { .. }) => continue,
            Err(e) => return Err(e.into()),
        }
    }
    bail!("bundle list CAS retries exhausted")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn git(dir: &Path, args: &[&str]) -> String {
        let out = Command::new("git")
            .arg("-C")
            .arg(dir)
            .args(args)
            .output()
            .expect("git");
        assert!(
            out.status.success(),
            "git {:?}: {}",
            args,
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    }

    /// A large repository's shape (the original large-repository measurements): the pack holds main's closure, the
    /// ref list also names a tip 1 commit ahead (`refs/remotes/origin/main` then);
    /// that tip's commit + blob are in no pack → the import must refuse.
    #[test]
    fn refs_ahead_of_the_pack_set_are_refused() {
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("src");
        std::fs::create_dir_all(&src).unwrap();
        git(&src, &["init", "-q", "-b", "main"]);
        git(&src, &["config", "user.email", "t@t"]);
        git(&src, &["config", "user.name", "t"]);
        std::fs::write(src.join("a"), "a\n").unwrap();
        git(&src, &["add", "."]);
        git(&src, &["commit", "-q", "-m", "one"]);
        let main_tip = git(&src, &["rev-parse", "HEAD"]);
        // Pack exactly main's closure into its own dir.
        let packs = tmp.path().join("packs");
        std::fs::create_dir_all(&packs).unwrap();
        let out = Command::new("git")
            .arg("-C")
            .arg(&src)
            .args([
                "pack-objects",
                "--revs",
                &format!("{}/pack", packs.display()),
            ])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn()
            .and_then(|mut c| {
                use std::io::Write;
                c.stdin
                    .take()
                    .unwrap()
                    .write_all(format!("{main_tip}\n").as_bytes())?;
                c.wait_with_output()
            })
            .unwrap();
        assert!(out.status.success());
        // One more commit that is NOT in the pack.
        std::fs::write(src.join("b"), "b\n").unwrap();
        git(&src, &["add", "."]);
        git(&src, &["commit", "-q", "-m", "two"]);
        let ahead = git(&src, &["rev-parse", "HEAD"]);

        assert!(
            verify_refs_in_packs(&packs, &[main_tip.clone()], true)
                .unwrap()
                .is_empty()
        );
        let missing =
            verify_refs_in_packs(&packs, &[main_tip.clone(), ahead.clone()], false).unwrap();
        assert_eq!(missing, vec![ahead.clone()], "the tip itself is missing");
        let missing = verify_refs_in_packs(&packs, &[main_tip, ahead], true).unwrap();
        assert!(!missing.is_empty());
    }

    /// Tips present but a blob of their closure is not (exactly a large repository's 1,952 blobs).
    #[test]
    fn a_missing_blob_in_the_closure_is_found() {
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("src");
        std::fs::create_dir_all(&src).unwrap();
        git(&src, &["init", "-q", "-b", "main"]);
        git(&src, &["config", "user.email", "t@t"]);
        git(&src, &["config", "user.name", "t"]);
        std::fs::write(src.join("a"), "a\n").unwrap();
        git(&src, &["add", "."]);
        git(&src, &["commit", "-q", "-m", "one"]);
        let tip = git(&src, &["rev-parse", "HEAD"]);
        let blob = git(&src, &["rev-parse", "HEAD:a"]);
        let tree = git(&src, &["rev-parse", "HEAD^{tree}"]);
        let packs = tmp.path().join("packs");
        std::fs::create_dir_all(&packs).unwrap();
        // Pack commit + tree only (a blob:none history pack), no blob.
        let out = Command::new("git")
            .arg("-C")
            .arg(&src)
            .args(["pack-objects", &format!("{}/pack", packs.display())])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn()
            .and_then(|mut c| {
                use std::io::Write;
                c.stdin
                    .take()
                    .unwrap()
                    .write_all(format!("{tip}\n{tree}\n").as_bytes())?;
                c.wait_with_output()
            })
            .unwrap();
        assert!(out.status.success());
        assert!(
            verify_refs_in_packs(&packs, &[tip.clone()], false)
                .unwrap()
                .is_empty(),
            "tip is there"
        );
        assert_eq!(
            verify_refs_in_packs(&packs, &[tip], true).unwrap(),
            vec![blob]
        );
    }
}

#[cfg(test)]
mod resume_tests {
    use super::*;
    use std::process::Command;

    fn sh(dir: &Path, args: &[&str]) -> String {
        let o = Command::new("git")
            .args(args)
            .current_dir(dir)
            .output()
            .unwrap();
        assert!(
            o.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&o.stderr)
        );
        String::from_utf8_lossy(&o.stdout).trim().to_string()
    }

    /// A bare source with 3 commits, fully packed with a bitmap (one pack, the import's happy shape).
    fn source() -> tempfile::TempDir {
        let d = tempfile::tempdir().unwrap();
        sh(d.path(), &["init", "-q", "-b", "main", "."]);
        sh(d.path(), &["config", "user.email", "t@t"]);
        sh(d.path(), &["config", "user.name", "T"]);
        for i in 0..3 {
            std::fs::write(d.path().join(format!("f{i}")), vec![b'a' + i as u8; 20_000]).unwrap();
            sh(d.path(), &["add", "."]);
            sh(d.path(), &["commit", "-q", "-m", &format!("c{i}")]);
        }
        sh(d.path(), &["tag", "-a", "-m", "v1", "v1"]);
        sh(d.path(), &["repack", "-adb", "-q"]);
        sh(d.path(), &["prune-packed"]);
        d
    }

    fn opts(src: &Path, repo: &str) -> DirectOptions {
        DirectOptions {
            from: src.to_path_buf(),
            repo: repo.into(),
            packs: None,
            bundle: true,
            bundle_strategy: Some("weekly".into()),
            replace: false,
            parallelism: 2,
            commit_graph: true,
            refs: vec![],
            history_pack: true,
            verify_closure: true,
        }
    }

    fn cfg() -> Arc<Config> {
        let mut c = Config::default();
        c.store.backend = walgit_config::StoreBackend::Memory;
        Arc::new(c)
    }

    #[test]
    fn resume_decision_matches_intent_and_base() {
        let m = ImportMarker {
            repo: "o/r".into(),
            tips_hash: "t1".into(),
            base_manifest_version: Some("v1".into()),
            base_head_seq: 3,
            seq: 4,
            phase: ImportPhase::Uploaded,
            uploaded: vec![],
            history_pack: None,
            bundle: None,
        };
        assert_eq!(
            decide_resume(None, "o/r", "t1", Some("v1"), false),
            ResumeDecision::Fresh
        );
        assert_eq!(
            decide_resume(Some(&m), "o/r", "t1", Some("v1"), false),
            ResumeDecision::Resume
        );
        assert_eq!(
            decide_resume(Some(&m), "o/r", "t2", Some("v1"), false),
            ResumeDecision::Fresh,
            "other ref set = other import"
        );
        assert_eq!(
            decide_resume(Some(&m), "o/x", "t1", Some("v1"), false),
            ResumeDecision::Fresh,
            "other repo"
        );
        assert_eq!(
            decide_resume(Some(&m), "o/r", "t1", Some("v2"), false),
            ResumeDecision::Refuse {
                started_at: Some("v1".into()),
                now: Some("v2".into())
            },
            "the target moved: refuse without --force"
        );
        assert_eq!(
            decide_resume(Some(&m), "o/r", "t1", Some("v2"), true),
            ResumeDecision::Fresh,
            "--force starts over"
        );
        assert_eq!(
            decide_resume(Some(&m), "o/r", "t1", None, false),
            ResumeDecision::Refuse {
                started_at: Some("v1".into()),
                now: None
            },
            "repo deleted meanwhile"
        );
        assert!(
            ImportPhase::Started < ImportPhase::Verified
                && ImportPhase::Verified < ImportPhase::Uploaded
                && ImportPhase::Uploaded < ImportPhase::Bundled
        );
    }

    /// Kill after each phase in turn, then resume: every object is uploaded exactly once in total,
    /// the closure walk and the history pack happen once, there is one manifest CAS, and a second
    /// run on the completed import is a no-op (0 uploads, 0 CAS).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn interrupted_import_resumes_and_a_completed_one_is_a_noop() {
        let src = source();
        let cfg = cfg();
        let store = walgit_store::memory::MemoryStore::shared();
        let repo = "t/resume";
        let id = walgit_git::RepoId::new("t", "resume").unwrap();
        let pack_dir = crate::import::resolve_git_dir(src.path())
            .unwrap()
            .join("objects")
            .join("pack");
        let marker_path = opts(src.path(), repo).marker_path(&pack_dir, &id);
        // Uploads are counted from the marker's done set (the report is lost with a killed run).
        let mut prev_uploaded = 0usize;
        let mut total_uploaded = 0usize;
        for phase in [
            ImportPhase::Verified,
            ImportPhase::SideFiles,
            ImportPhase::HistoryPack,
            ImportPhase::Uploaded,
            ImportPhase::Bundled,
        ] {
            set_abort(repo, Some(phase));
            let r = run_with_store(opts(src.path(), repo), &cfg, store.clone(), false).await;
            assert!(r.is_err(), "killed after {phase:?} must fail: {r:?}");
            let marker = read_import_marker(&marker_path).expect("marker survives the kill");
            if phase == ImportPhase::Uploaded {
                // Killed after the first object of the upload phase: the phase is still the previous
                // one, the done set has exactly that object.
                assert_eq!(marker.phase, ImportPhase::HistoryPack, "{marker:?}");
                assert_eq!(
                    marker.uploaded.len(),
                    1,
                    "killed after the first object: {:?}",
                    marker.uploaded
                );
            } else {
                assert!(
                    marker.phase >= phase,
                    "marker at {:?} after killing at {phase:?}",
                    marker.phase
                );
            }
            assert!(
                marker.uploaded.len() >= prev_uploaded,
                "done set never shrinks"
            );
            total_uploaded += marker.uploaded.len() - prev_uploaded;
            prev_uploaded = marker.uploaded.len();
        }
        set_abort(repo, None);
        // The resumed run finishes without redoing any local phase or upload.
        let r = run_with_store(opts(src.path(), repo), &cfg, store.clone(), false)
            .await
            .unwrap();
        assert!(r.resumed && !r.noop && r.cas == 1, "{r:?}");
        assert!(
            !r.verified && !r.built_commit_graph && !r.built_history_pack,
            "local phases were not redone: {r:?}"
        );
        total_uploaded += r.uploaded;
        let repo_store = Prefixed::new(store.clone(), id.store_prefix());
        let m = Manifest::decode(
            repo_store
                .get_bytes(keys::MANIFEST)
                .await
                .unwrap()
                .unwrap()
                .1
                .as_ref(),
        )
        .unwrap();
        let mut expected = 0usize;
        for p in &m.packs {
            expected += 2
                + usize::from(p.has_rev)
                + usize::from(p.has_bitmap)
                + usize::from(p.has_commit_graph);
        }
        assert_eq!(
            total_uploaded, expected,
            "every object uploaded exactly once across all runs: {r:?} manifest {m:?}"
        );
        assert_eq!(
            r.uploaded + r.skipped,
            expected,
            "the final run accounted for every object: {r:?}"
        );
        assert_eq!(m.packs.len(), 2, "base + history pack: {m:?}");
        assert!(
            m.packs
                .iter()
                .any(|p| p.kind == walgit_proto::v1::PackKind::History as i32)
        );
        assert_eq!(m.head_seq, 1);
        assert!(!marker_path.exists(), "marker removed after success");
        let cas = r.cas;
        // Bundle list has exactly one weekly.
        let list = BundleList::decode(
            repo_store
                .get_bytes(keys::BUNDLE_LIST)
                .await
                .unwrap()
                .unwrap()
                .1
                .as_ref(),
        )
        .unwrap();
        assert_eq!(list.bundles.len(), 1, "{list:?}");

        // Completed import, same command again: no-op.
        let r2 = run_with_store(opts(src.path(), repo), &cfg, store.clone(), false)
            .await
            .unwrap();
        assert!(r2.noop, "{r2:?}");
        assert_eq!((r2.uploaded, r2.cas), (0, 0));
        assert_eq!(cas, 1);
        let m2 = Manifest::decode(
            repo_store
                .get_bytes(keys::MANIFEST)
                .await
                .unwrap()
                .unwrap()
                .1
                .as_ref(),
        )
        .unwrap();
        assert_eq!(
            m2.revision, m.revision,
            "a no-op run does not touch the manifest"
        );
    }

    /// The target moved between an interrupted import and the resume: refused without --force;
    /// with --force the import starts over on the new base and reuses the uploaded objects.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn resume_refuses_a_moved_target_unless_forced() {
        let src = source();
        let cfg = cfg();
        let store = walgit_store::memory::MemoryStore::shared();
        let repo = "t/moved";
        set_abort(repo, Some(ImportPhase::Uploaded));
        assert!(
            run_with_store(opts(src.path(), repo), &cfg, store.clone(), false)
                .await
                .is_err()
        );
        set_abort(repo, None);
        // Someone creates the repository (manifest appears) meanwhile.
        let id = walgit_git::RepoId::new("t", "moved").unwrap();
        let repo_store = Prefixed::new(store.clone(), id.store_prefix());
        let m = Manifest {
            format_version: WAL_FORMAT_VERSION,
            repo: id.to_string(),
            object_format: "sha1".into(),
            ..Default::default()
        };
        repo_store
            .put(
                keys::MANIFEST,
                PutBody::Bytes(m.encode_to_vec().into()),
                PutOptions::from(PutMode::Create),
            )
            .await
            .unwrap();
        let r = run_with_store(opts(src.path(), repo), &cfg, store.clone(), false).await;
        let err = r.err().map(|e| e.to_string()).unwrap_or_default();
        assert!(err.contains("--force"), "refused with the fix: {err}");
        // --force: fresh start on the current base; the object uploaded before the kill is skipped (HEAD).
        let r = run_with_store(opts(src.path(), repo), &cfg, store.clone(), true)
            .await
            .unwrap();
        assert!(
            !r.resumed && !r.noop && r.cas == 1 && r.skipped >= 1,
            "{r:?}"
        );
    }
}
