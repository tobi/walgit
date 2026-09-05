//! Resumable base rebuild (`docs/BUNDLE_URI_DESIGN.md` §5a): `git repack -adb` of a big
//! repository is 16–30 min of one core plus a 10-min history pack; a deploy mid-way (D31
//! interrupts units at once) used to throw that away and — worse — the repack rewrote the
//! *serving copy's* `objects/pack` in place.
//!
//! Now the rebuild works in a **scratch copy** under `<cache.dir>/_rebuild/<owner>/<repo>.git`
//! (on the SSD host `/data` is a bind mount that outlives the container; the copy is
//! `copy_file_range`, which XFS turns into a reflink — seconds, no bytes duplicated until
//! written), records a **phase marker** next to it after each completed phase, and the next
//! unit **resumes iff the manifest's `head_seq` is unchanged** since the rebuild started
//! (otherwise the scratch is discarded: a push landed, the pack would miss its objects).
//! Finished packs are hard-linked into the serving copy only at publish time — the serving
//! copy is never rewritten — and publish is idempotent (an already-live checksum is not
//! re-published; the supersede set is the packs that existed when the rebuild started).
//! D31 is unchanged: SIGTERM kills `git repack`; git writes packs by temp name + rename, so a
//! half-written pack never looks final; the marker's phase is whatever completed.

use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{Context, bail};
use walgit_config::Config;
use walgit_git::{LocalRepo, RepackMode, RepackOptions};
use walgit_wal::RepoHandle;

use crate::ops::Log;

/// Phases in order; the marker names the last one that completed.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "kebab-case")]
pub enum Phase {
    Copied,
    Repacked,
    HistoryPack,
    CommitGraph,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Marker {
    /// `manifest.head_seq` when the scratch copy was taken: resume only while it is unchanged.
    pub started_head_seq: u64,
    pub phase: Phase,
    /// The base pack(s) the repack produced (hex; one unless git split the pack).
    #[serde(default)]
    pub new_packs: Vec<String>,
    /// The D18 history pack derived from the first base (hex).
    #[serde(default)]
    pub history: Option<String>,
    /// The pack carrying the commit-graph layer (hex).
    #[serde(default)]
    pub commit_graph: Option<String>,
}

/// Test hook (sim): abort the rebuild of `repo` right after `phase`'s marker was written — a
/// SIGTERM between phases. Per repository so parallel tests do not see each other's hook.
pub static TEST_ABORT_AFTER: parking_lot::Mutex<Option<(String, Phase)>> =
    parking_lot::Mutex::new(None);

fn abort_after(repo: &walgit_git::RepoId, phase: Phase) -> anyhow::Result<()> {
    let hook = TEST_ABORT_AFTER.lock().clone();
    if let Some((r, p)) = hook
        && r == repo.to_string()
        && p == phase
    {
        bail!("rebuild aborted by test hook after phase {phase:?}");
    }
    Ok(())
}

pub struct RebuildOutcome {
    /// Checksums published (base(s), then the history pack).
    pub packs: Vec<String>,
    pub superseded: usize,
    /// True when this unit continued an earlier, interrupted rebuild.
    pub resumed: bool,
}

fn scratch_root(cfg: &Config) -> PathBuf {
    cfg.cache.dir.join("_rebuild")
}

fn marker_path(cfg: &Config, id: &walgit_git::RepoId) -> PathBuf {
    scratch_root(cfg)
        .join(id.owner())
        .join(format!("{}.json", id.name()))
}

fn read_marker(path: &Path) -> Option<Marker> {
    let bytes = std::fs::read(path).ok()?;
    serde_json::from_slice(&bytes).ok()
}

fn write_marker(path: &Path, m: &Marker) -> anyhow::Result<()> {
    std::fs::create_dir_all(path.parent().unwrap())?;
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, serde_json::to_vec_pretty(m)?)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

/// Recursive copy. `std::fs::copy` uses `copy_file_range` on Linux, which XFS/btrfs satisfy
/// with a reflink when source and destination share a filesystem (seconds for 40 GB, no
/// bytes duplicated until written) and which degrades to a plain copy elsewhere.
fn copy_tree(src: &Path, dst: &Path) -> std::io::Result<u64> {
    std::fs::create_dir_all(dst)?;
    let mut bytes = 0u64;
    for ent in std::fs::read_dir(src)? {
        let ent = ent?;
        let from = ent.path();
        let to = dst.join(ent.file_name());
        let ft = ent.file_type()?;
        if ft.is_dir() {
            bytes += copy_tree(&from, &to)?;
        } else if ft.is_file() {
            bytes += std::fs::copy(&from, &to)?;
        } else if ft.is_symlink() {
            // A mount-linked base (`pack-<sha>.pack` → store mount) is never rebuilt here:
            // the rebuild needs real files (compact_repo syncs Full first).
            let target = std::fs::read_link(&from)?;
            std::os::unix::fs::symlink(target, &to)?;
        }
    }
    Ok(bytes)
}

fn disk_avail(path: &Path) -> Option<u64> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    let c = CString::new(path.as_os_str().as_bytes()).ok()?;
    // SAFETY: statvfs is a C integer struct; all-zero is a valid initialized value.
    #[allow(unsafe_code)]
    let mut st: libc::statvfs = unsafe { std::mem::zeroed() };
    // SAFETY: c is NUL-terminated and live; st is aligned writable storage for statvfs.
    #[allow(unsafe_code)]
    let result = unsafe { libc::statvfs(c.as_ptr(), &raw mut st) };
    if result != 0 {
        return None;
    }
    Some(st.f_bavail as u64 * st.f_frsize as u64)
}

/// Hard-link (or copy) every side-file of `pack` from `from` into `into`'s pack dir; existing
/// files are left alone (a resumed install).
fn install_pack(
    from: &LocalRepo,
    into: &LocalRepo,
    pack: &gix_hash::ObjectId,
) -> anyhow::Result<()> {
    let src = from.pack_path(pack);
    let dst = into.pack_path(pack);
    std::fs::create_dir_all(dst.parent().unwrap())?;
    for ext in ["pack", "idx", "rev", "bitmap", "commit-graph", "history"] {
        let s = src.with_extension(ext);
        if !s.exists() {
            continue;
        }
        let d = dst.with_extension(ext);
        if d.exists() {
            continue;
        }
        if std::fs::hard_link(&s, &d).is_err() {
            std::fs::copy(&s, &d).with_context(|| format!("installing {}", d.display()))?;
        }
    }
    Ok(())
}

/// Rebuild the tier-2 base of `handle`'s repository in a scratch copy, resuming an interrupted
/// rebuild when the WAL head has not moved, then install + publish: the new base (superseding
/// every live pack that existed when the rebuild started), the D18 history pack, the
/// commit-graph layer. The caller holds the compaction lease and has synced Full.
pub async fn rebuild_base(
    handle: &RepoHandle,
    cfg: &Config,
    log: Log<'_>,
) -> anyhow::Result<RebuildOutcome> {
    let id = handle.id().clone();
    let root = scratch_root(cfg);
    let scratch_dir = id.local_dir(&root);
    let marker_path = marker_path(cfg, &id);
    let manifest = handle.manifest();
    let head = manifest.head_seq;

    // 1. Resume or start over.
    let mut marker = match read_marker(&marker_path) {
        Some(m) if m.started_head_seq == head && scratch_dir.join("objects").is_dir() => {
            log(format!(
                "resuming base rebuild started at head_seq {head}: phase {:?} done",
                m.phase
            ));
            m
        }
        Some(m) => {
            log(format!(
                "discarding interrupted base rebuild (started at head_seq {}, head is now {head}, phase {:?}): starting over",
                m.started_head_seq, m.phase
            ));
            let _ = std::fs::remove_dir_all(&scratch_dir);
            let _ = std::fs::remove_file(&marker_path);
            start_scratch(handle, cfg, &manifest, &scratch_dir, &marker_path, log)?
        }
        None => {
            let _ = std::fs::remove_dir_all(&scratch_dir);
            start_scratch(handle, cfg, &manifest, &scratch_dir, &marker_path, log)?
        }
    };
    let resumed = marker.phase > Phase::Copied;
    let scratch =
        LocalRepo::open(&root, &id)?.context("scratch copy did not open as a repository")?;

    // 2. Repack (full, bitmap) in the scratch copy.
    if marker.phase < Phase::Repacked {
        let t = Instant::now();
        let result = scratch
            .repack(RepackOptions {
                mode: RepackMode::Full,
                write_bitmap: true,
                write_midx: false,
                keep: Vec::new(),
            })
            .await?;
        let mut new: Vec<String> = result
            .new_packs
            .iter()
            .map(|p| p.checksum.to_hex().to_string())
            .collect();
        if new.is_empty() {
            // Already a single pack: `repack -adb` only added the bitmap. The base is that pack.
            let mut packs = scratch.packs()?;
            packs.sort_by_key(|p| std::cmp::Reverse(p.pack_size));
            if let Some(p) = packs
                .iter()
                .find(|p| p.has_bitmap && p.history_of.is_none())
            {
                new.push(p.checksum.to_hex().to_string());
            }
        }
        log(format!(
            "repack done in {:.1}s: {} new pack(s), {} removed in the scratch copy",
            t.elapsed().as_secs_f64(),
            result.new_packs.len(),
            result.removed.len()
        ));
        if new.is_empty() {
            bail!("full repack produced no base pack");
        }
        marker.new_packs = new;
        marker.phase = Phase::Repacked;
        write_marker(&marker_path, &marker)?;
        abort_after(&id, Phase::Repacked)?;
    }
    let bases: Vec<gix_hash::ObjectId> = marker
        .new_packs
        .iter()
        .filter_map(|h| gix_hash::ObjectId::from_hex(h.as_bytes()).ok())
        .collect();

    // 3. History pack (D18) of the first base.
    if marker.phase < Phase::HistoryPack {
        if cfg.git.history_pack
            && let Some(base) = bases.first()
        {
            let t = Instant::now();
            match scratch.write_history_pack(base).await {
                Ok(hp) => {
                    log(format!(
                        "history pack {} for base {base}: {} bytes, {} objects in {:.1}s",
                        hp.checksum,
                        hp.pack_size,
                        hp.object_count,
                        t.elapsed().as_secs_f64()
                    ));
                    marker.history = Some(hp.checksum.to_hex().to_string());
                }
                Err(e) => log(format!("history pack failed (continuing without): {e}")),
            }
        }
        marker.phase = Phase::HistoryPack;
        write_marker(&marker_path, &marker)?;
        abort_after(&id, Phase::HistoryPack)?;
    }

    // 4. One commit-graph layer on the biggest base.
    if marker.phase < Phase::CommitGraph {
        if cfg.git.commit_graph {
            let packs = scratch.packs()?;
            let biggest = bases
                .iter()
                .filter_map(|b| packs.iter().find(|p| &p.checksum == b))
                .max_by_key(|p| p.pack_size)
                .map(|p| p.checksum);
            if let Some(b) = biggest {
                let t = Instant::now();
                match scratch
                    .write_pack_commit_graph(&b, cfg.git.commit_graph_changed_paths)
                    .await
                {
                    Ok(bytes) => {
                        log(format!(
                            "commit-graph layer on pack {b}: {bytes} bytes in {:.1}s",
                            t.elapsed().as_secs_f64()
                        ));
                        marker.commit_graph = Some(b.to_hex().to_string());
                    }
                    Err(e) => log(format!(
                        "commit-graph write failed (continuing without): {e}"
                    )),
                }
            }
        }
        marker.phase = Phase::CommitGraph;
        write_marker(&marker_path, &marker)?;
        abort_after(&id, Phase::CommitGraph)?;
    }

    // 5. Install into the serving copy (links, never a rewrite) and publish. The supersede set is
    //    every pack that was live when the rebuild started (seq ≤ started_head_seq) and is not one
    //    of the new ones; pushes that landed since keep their packs.
    let mut to_install = bases.clone();
    if let Some(h) = marker
        .history
        .as_deref()
        .and_then(|h| gix_hash::ObjectId::from_hex(h.as_bytes()).ok())
    {
        to_install.push(h);
    }
    for p in &to_install {
        install_pack(&scratch, handle.local(), p)?;
    }
    handle.local().refresh_async().await?;
    let local_packs = handle.local().packs()?;
    let manifest = handle.manifest();
    let new_set: std::collections::HashSet<String> =
        to_install.iter().map(|c| c.to_hex().to_string()).collect();
    let supersedes: Vec<gix_hash::ObjectId> = manifest
        .packs
        .iter()
        .filter(|p| p.seq <= marker.started_head_seq && !new_set.contains(&p.checksum))
        .filter_map(|p| gix_hash::ObjectId::from_hex(p.checksum.as_bytes()).ok())
        .collect();
    let superseded = supersedes.len();
    let mut supersedes_left = Some(supersedes);
    let mut published = Vec::new();
    for c in &to_install {
        let hex = c.to_hex().to_string();
        let Some(info) = local_packs.iter().find(|p| &p.checksum == c).cloned() else {
            bail!("pack {hex} not visible in the serving copy after install");
        };
        let already = manifest.packs.iter().find(|p| p.checksum == hex);
        // Already live at the right tier and not the carrier of the supersede set: nothing to publish.
        if let Some(p) = already
            && p.tier == 2
            && (p.has_bitmap || info.history_of.is_some())
            && supersedes_left.as_ref().is_none_or(std::vec::Vec::is_empty)
        {
            log(format!(
                "pack {hex} is already live as tier 2: not re-published"
            ));
            published.push(hex);
            continue;
        }
        let sup = supersedes_left.take().unwrap_or_default();
        let seq = handle.publish_compact(info, sup, 2).await?;
        log(format!(
            "published pack {hex} as seq {seq}{}",
            if already.is_some() {
                " (promotion)"
            } else {
                ""
            }
        ));
        published.push(hex);
    }

    // 6. Done: the scratch and its marker go.
    let _ = std::fs::remove_dir_all(&scratch_dir);
    let _ = std::fs::remove_file(&marker_path);
    Ok(RebuildOutcome {
        packs: published,
        superseded,
        resumed,
    })
}

fn start_scratch(
    handle: &RepoHandle,
    cfg: &Config,
    manifest: &walgit_proto::v1::Manifest,
    scratch_dir: &Path,
    marker_path: &Path,
    log: Log<'_>,
) -> anyhow::Result<Marker> {
    // Headroom: the repack writes a new pack about the size of the live set (a reflink copy
    // costs nothing until then). Fail loudly rather than fill the disk the serving copy lives on.
    let need: u64 = manifest
        .packs
        .iter()
        .map(|p| p.pack_size + p.idx_size)
        .sum();
    if let Some(avail) = disk_avail(&cfg.cache.dir)
        && avail < need
    {
        bail!(
            "not enough disk for a base rebuild under {}: {} available, the pack set is {} (the repack writes a pack that size)",
            cfg.cache.dir.display(),
            walgit_wal::remote::human_bytes(avail),
            walgit_wal::remote::human_bytes(need)
        );
    }
    let t = Instant::now();
    let bytes = copy_tree(handle.local().path(), scratch_dir)
        .context("copying the serving copy to the scratch dir")?;
    log(format!(
        "scratch copy of the serving copy at {} ({} in {:.1}s; reflinked where the filesystem allows)",
        scratch_dir.display(),
        walgit_wal::remote::human_bytes(bytes),
        t.elapsed().as_secs_f64()
    ));
    let m = Marker {
        started_head_seq: manifest.head_seq,
        phase: Phase::Copied,
        new_packs: Vec::new(),
        history: None,
        commit_graph: None,
    };
    write_marker(marker_path, &m)?;
    abort_after(handle.id(), Phase::Copied)?;
    Ok(m)
}
