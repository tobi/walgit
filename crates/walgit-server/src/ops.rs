//! Repository maintenance operations ("make the repo great"): fsck, compaction,
//! bundle builds, checkpoints, re-materialize. Shared by the background loops
//! (`walgit serve` roles), the CLI, and the web UI's `POST …/ops/{op}` route,
//! which streams the op's log as SSE and records the outcome per instance.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tracing::Instrument;

use prost::Message;
use serde::Serialize;
use walgit_config::Config;
use walgit_git::{RepackMode, RepackOptions, RepoId};
use walgit_store::ObjectStoreExt;
use walgit_wal::RepoHandle;

use crate::AppState;

/// Callback that receives human-readable progress lines. Borrowed form of
/// [`walgit_wal::snapshot::Narrator`], so an op can hand the same sink to work
/// in the WAL crate that outlives the call.
pub type Log<'a> = &'a (dyn Fn(String) + Send + Sync);

pub fn noop_log(_: String) {}

// ---------------------------------------------------------------------------
// Catalogue
// ---------------------------------------------------------------------------

/// Ops the UI can trigger. `id` is the URL segment.
#[derive(Serialize, Clone)]
pub struct OpSpec {
    pub id: &'static str,
    pub label: &'static str,
    pub description: &'static str,
    /// Query parameters the op accepts (documentation for the UI).
    pub params: &'static [&'static str],
    /// Whether this op changes the WAL (everything but fsck/sync is a write).
    pub mutating: bool,
}

/// How many missing oids one `fsck.pb` carries (the repair unit works through them;
/// the next fsck finds whatever is left).
pub const FSCK_MISSING_LIST_MAX: usize = 100_000;

/// Whether a task kind is a maintenance op (the units the D31 maintenance drain
/// waits for; request-driven tasks — sync, prewarm, history-pack install — are not).
pub fn is_op(kind: &str) -> bool {
    OPS.iter().any(|o| o.id == kind)
}

pub const OPS: &[OpSpec] = &[
    OpSpec {
        id: "fsck",
        label: "fsck",
        description: "git fsck --full --strict on this instance's copy (deep object/connectivity check). \
                      connectivity=1 skips object content checks. Records the verdict at fsck.pb \
                      (missing objects feed the repair unit).",
        params: &["connectivity"],
        mutating: false,
    },
    OpSpec {
        id: "repair",
        label: "Repair",
        description: "Fetch the objects the last fsck found missing from upstream.git and publish them \
                      as a pack (COMPACT entry, no ref change).",
        params: &[],
        mutating: true,
    },
    OpSpec {
        id: "follow",
        label: "Follow upstream",
        description: "Bring the refs in upstream.follow up to upstream.git's now: fetch the delta over this copy's \
                      objects, ingest it like a push, fast-forward only, one PUSH entry (principal=upstream). The \
                      maintaining host runs this every maintenance.follow_interval when a ref moved.",
        params: &[],
        mutating: true,
    },
    OpSpec {
        id: "rev-index",
        label: "Reverse index",
        description: "Build pack-<sha>.rev for a published pack that has none (git < 2.41 wrote none), upload it \
                      as the side-file and advertise it in the manifest (has_rev). Without it git rebuilds the \
                      reverse index in memory on every pack-objects (a large repository's base: 2.85 s per fetch).",
        params: &["pack"],
        mutating: true,
    },
    OpSpec {
        id: "compact",
        label: "Compact",
        description: "Geometric repack (or full base rebuild with bitmaps) under the per-repo compaction lease, \
                      published as a COMPACT WAL entry. force=1 ignores the trigger thresholds; base=1 forces a bitmap'd base rebuild.",
        params: &["force", "base"],
        mutating: true,
    },
    OpSpec {
        id: "bundle",
        label: "Bundle",
        description: "Build and publish a bundle-uri bundle now (strategy=<name>, default: the first full strategy; \
                      strategy=due builds whatever the schedule says is due).",
        params: &["strategy"],
        mutating: true,
    },
    OpSpec {
        id: "checkpoint",
        label: "Checkpoint",
        description: "Write a checkpoint (pack set + ref snapshot) at the current head so cold materialize and bundles start from here.",
        params: &[],
        mutating: true,
    },
    OpSpec {
        id: "sync",
        label: "Sync",
        description: "Revalidate the manifest and catch this instance's local copy up to the WAL head.",
        params: &[],
        mutating: false,
    },
    OpSpec {
        id: "rematerialize",
        label: "Re-materialize",
        description: "Throw away this instance's local copy and rebuild it from the store (repair).",
        params: &[],
        mutating: false,
    },
    OpSpec {
        id: "snapshot",
        label: "Snapshot",
        description: "Time travel: rebuild the repository as it was at WAL sequence at_seq=<n> into \
                      <cache.dir>/snapshots/<owner>/<name>/<n>/ on this instance and report its refs. \
                      Nothing is published and no live ref moves — the serving copy stays at the head. \
                      Idempotent: a sequence already materialized here is returned as it is. Read it \
                      back at GET …/api/snapshot/{n}.",
        params: &["at_seq"],
        mutating: false,
    },
];

pub fn spec(id: &str) -> Option<&'static OpSpec> {
    OPS.iter().find(|o| o.id == id)
}

// ---------------------------------------------------------------------------
// Running an op = a walgit_wal task (unique id, (repo, kind) lock, log,
// attachable stream at GET …/tasks/{id})
// ---------------------------------------------------------------------------

pub enum StartError {
    UnknownOp,
    /// The same op is already running here; attach to this task instead.
    AlreadyRunning(Arc<walgit_wal::tasks::TaskState>),
}

/// Start `op` for `id` on this instance as a background task and return its
/// state (stream it with [`crate::sse::task_stream`]). The op keeps running if
/// every client goes away.
pub async fn start(
    state: Arc<AppState>,
    id: RepoId,
    op: &str,
    params: HashMap<String, String>,
) -> Result<Arc<walgit_wal::tasks::TaskState>, StartError> {
    let spec = spec(op).ok_or(StartError::UnknownOp)?;
    let handle = state
        .registry
        .open(&id)
        .await
        .map_err(|_| StartError::UnknownOp)?;
    let task = match handle.begin_task(spec.id, params.clone()) {
        walgit_wal::Begin::Started(t) => t,
        walgit_wal::Begin::AlreadyRunning(s) => return Err(StartError::AlreadyRunning(s)),
    };
    let task_state = task.state.clone();
    let op_id = spec.id;
    let span = task.span();
    let join = tokio::spawn(
        async move {
            let reporter = task.reporter();
            let repo = id.to_string();
            // An `Arc` sink, not a borrowed closure: work that runs on another
            // runtime (the WAL crate's bulk runtime) needs an owned one.
            let narrate: walgit_wal::snapshot::Narrator = Arc::new(move |line: String| {
                tracing::info!(repo = %repo, op = op_id, "{line}");
                reporter.notice(line);
            });
            let res = run(&state, &id, op_id, &params, narrate).await;
            match res {
                Ok((summary, value)) => {
                    task.finish_ok(summary, Some(value));
                }
                Err(e) => {
                    task.finish_err(500, e);
                }
            }
        }
        .instrument(span),
    );
    task_state.set_abort_handle(join.abort_handle());
    Ok(task_state)
}

/// The last connectivity audit of `handle`'s repository, if any.
pub async fn read_fsck(
    handle: &RepoHandle,
) -> Result<Option<walgit_proto::v1::FsckReport>, String> {
    use walgit_store::ObjectStoreExt;
    match handle.store().get_bytes(walgit_proto::keys::FSCK).await {
        Ok(Some((_, bytes))) => walgit_proto::v1::FsckReport::decode(bytes.as_ref())
            .map(Some)
            .map_err(|e| e.to_string()),
        Ok(None) => Ok(None),
        Err(e) => Err(e.to_string()),
    }
}

fn flag(params: &HashMap<String, String>, key: &str) -> bool {
    params
        .get(key)
        .map(|v| matches!(v.as_str(), "1" | "true" | "yes" | "on"))
        .unwrap_or(false)
}

async fn run(
    state: &Arc<AppState>,
    id: &RepoId,
    op: &str,
    params: &HashMap<String, String>,
    narrate: walgit_wal::snapshot::Narrator,
) -> Result<(String, serde_json::Value), String> {
    let log: Log<'_> = narrate.as_ref();
    let handle = state.registry.open(id).await.map_err(|e| e.to_string())?;
    match op {
        "fsck" => {
            let connectivity = flag(params, "connectivity");
            let guard = handle.sync().await.map_err(|e| e.to_string())?;
            let seq = handle.applied_seq();
            log(format!(
                "local copy at seq {} (manifest head {}), running git fsck{}",
                seq,
                handle.manifest().head_seq,
                if connectivity {
                    " --connectivity-only"
                } else {
                    " --full --strict"
                }
            ));
            let t0 = Instant::now();
            let mut lines = 0u64;
            let mut missing: Vec<String> = Vec::new();
            let report = handle
                .local()
                .fsck_streaming(connectivity, |l| {
                    lines += 1;
                    // `missing blob <oid>` / `missing tree <oid>` …
                    if let Some(rest) = l.strip_prefix("missing ")
                        && let Some(oid) = rest.split_whitespace().nth(1)
                        && oid.len() >= 40
                    {
                        missing.push(oid.to_string());
                    }
                    log(l);
                })
                .await
                .map_err(|e| e.to_string())?;
            drop(guard);
            missing.sort_unstable();
            missing.dedup();
            // The audit result lives in the bucket (not the WAL): the repair unit and
            // the gauge read it; every host sees the same verdict.
            let fsck = walgit_proto::v1::FsckReport {
                seq,
                at: Some(walgit_proto::time::now()),
                host: crate::maintain::host_name(state),
                missing_total: missing.len() as u64,
                missing: missing
                    .iter()
                    .take(FSCK_MISSING_LIST_MAX)
                    .cloned()
                    .collect(),
                problems: report.problems,
                elapsed_secs: t0.elapsed().as_secs_f64(),
                repaired_seq: 0,
            };
            handle
                .store()
                .put_bytes(
                    walgit_proto::keys::FSCK,
                    fsck.encode_to_vec(),
                    walgit_store::PutMode::Overwrite,
                )
                .await
                .map_err(|e| format!("writing fsck.pb: {e}"))?;
            metrics::gauge!("walgit_repo_missing_objects", "repo" => id.to_string())
                .set(missing.len() as f64);
            tracing::info!(repo = %id, seq, missing = missing.len(), problems = report.problems, elapsed_ms = t0.elapsed().as_millis() as u64, "fsck recorded");
            let summary = if report.ok {
                format!(
                    "fsck clean ({lines} lines, {:.0}s)",
                    t0.elapsed().as_secs_f64()
                )
            } else {
                format!(
                    "fsck found {} problem(s) ({} missing object(s)), exit {:?}",
                    report.problems,
                    missing.len(),
                    report.exit_code
                )
            };
            let value = serde_json::json!({"ok": report.ok, "problems": report.problems, "missing": missing.len(), "seq": seq});
            // Missing objects are a *finding*, not a failure of the unit: the repair
            // unit is the response (plan shows it). Corrupt objects stay a failure.
            if report.ok || !missing.is_empty() {
                Ok((summary, value))
            } else {
                Err(summary)
            }
        }
        "repair" => {
            // Desired state: every object reachable from refs is in a live pack.
            // Input: fsck.pb's missing list (the audit); source: upstream.git
            // (GitHub serves blob/tree wants by SHA); output: one pack published as
            // a COMPACT entry superseding nothing (exactly what `wal add-pack --tier 0`
            // did by hand for a large repository's 1,952 blobs, the original large-repository measurements).
            let cfg = handle.effective_config();
            let upstream = cfg
                .upstream
                .git
                .clone()
                .ok_or("repair: no upstream.git for this repository")?;
            let fsck = read_fsck(&handle)
                .await?
                .ok_or("repair: no fsck.pb (run fsck first)")?;
            if fsck.missing.is_empty() {
                return Ok((
                    "nothing to repair".into(),
                    serde_json::json!({"missing": 0}),
                ));
            }
            if fsck.missing_total as usize > fsck.missing.len() {
                log(format!(
                    "fsck listed {} of {} missing objects; repairing those, the next fsck finds the rest",
                    fsck.missing.len(),
                    fsck.missing_total
                ));
            }
            let token = match cfg.upstream.token_env.as_deref() {
                Some(name) => Some(
                    state
                        .lfs_upstream
                        .secret(name)
                        .await
                        .map_err(|e| format!("upstream token: {e}"))?,
                ),
                None => None,
            };
            let t0 = Instant::now();
            log(format!(
                "fetching {} object(s) from {upstream}",
                fsck.missing.len()
            ));
            let pack = walgit_git::repair::fetch_objects_as_pack(
                &upstream,
                token.as_deref(),
                &fsck.missing,
                &state.cfg.cache.dir.join("repair"),
            )
            .await
            .map_err(|e| format!("repair fetch: {e}"))?;
            log(format!(
                "packed {} object(s), {} bytes in {:.1}s; publishing",
                pack.objects,
                pack.bytes,
                t0.elapsed().as_secs_f64()
            ));
            let seq = handle
                .add_pack(&pack.pack, &pack.idx, 0, None)
                .await
                .map_err(|e| format!("publish: {e}"))?;
            let _ = tokio::fs::remove_dir_all(&pack.dir).await;
            // Record the repair on the audit so the unit is not due again until the
            // next fsck re-verifies (it will: the plan compares seqs).
            let done = walgit_proto::v1::FsckReport {
                repaired_seq: seq,
                ..fsck
            };
            handle
                .store()
                .put_bytes(
                    walgit_proto::keys::FSCK,
                    done.encode_to_vec(),
                    walgit_store::PutMode::Overwrite,
                )
                .await
                .map_err(|e| format!("writing fsck.pb: {e}"))?;
            metrics::counter!("walgit_repair_objects_total", "repo" => id.to_string())
                .increment(pack.objects);
            tracing::info!(repo = %id, seq, objects = pack.objects, bytes = pack.bytes, %upstream, elapsed_ms = t0.elapsed().as_millis() as u64, "repair published");
            Ok((
                format!(
                    "repaired {} object(s) ({} bytes) from upstream at seq {seq}",
                    pack.objects, pack.bytes
                ),
                serde_json::json!({"seq": seq, "objects": pack.objects, "bytes": pack.bytes}),
            ))
        }
        "follow" => crate::follow::op(state, &handle, id, params, log).await,
        "rev-index" => {
            // Desired state: every pack in the manifest advertises a `.rev`.
            let checksum = params
                .get("pack")
                .cloned()
                .ok_or("rev-index: missing `pack` (checksum)")?;
            let oid = gix_hash::ObjectId::from_hex(checksum.as_bytes())
                .map_err(|e| format!("rev-index: bad checksum {checksum}: {e}"))?;
            let t0 = Instant::now();
            let rev = handle
                .local()
                .write_rev_index(&oid)
                .await
                .map_err(|e| format!("rev-index: {e}"))?;
            let bytes = std::fs::metadata(&rev).map(|m| m.len()).unwrap_or(0);
            log(format!(
                "pack-{checksum}.rev: {bytes} bytes in {:.1}s; publishing",
                t0.elapsed().as_secs_f64()
            ));
            handle
                .annotate_pack(&checksum, Some(rev), None, None)
                .await
                .map_err(|e| format!("rev-index publish: {e}"))?;
            tracing::info!(repo = %id, pack = %checksum, bytes, elapsed_ms = t0.elapsed().as_millis() as u64, "rev index published");
            Ok((
                format!("pack-{checksum}.rev ({bytes} bytes) published"),
                serde_json::json!({"pack": checksum, "bytes": bytes}),
            ))
        }
        "compact" => {
            let force = flag(params, "force");
            let base = flag(params, "base");
            let out = compact_repo(
                &handle,
                &state.cfg,
                CompactRequest {
                    force,
                    rebuild_base: base,
                },
                log,
            )
            .await
            .map_err(|e| e.to_string())?;
            let summary = out.summary();
            Ok((summary, serde_json::to_value(&out).unwrap_or_default()))
        }
        "bundle" => {
            if !state.cfg.bundles.enabled {
                return Err("bundles are disabled in config".into());
            }
            let strategy = params.get("strategy").cloned().unwrap_or_default();
            if let Some(slot) = params.get("slot").and_then(|v| v.parse::<u64>().ok()) {
                // One calendar slot (the maintenance loop's unit): content as of the slot.
                log(format!(
                    "building {strategy} slot {slot} ({})",
                    walgit_bundle::slots::from_epoch(slot)
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_secs())
                        .unwrap_or(0)
                ));
                // A FULL slot of a repository that has a tier-2 base is a compose
                // of that base (header = refs at the base's seq) — never a
                // `pack-objects` of the whole history into a file (a large repository: 32 GB
                // through this host). The maintainer rebuilds the base first on
                // an ssd host when pushes landed since (`Unit::BaseRebuild`).
                let cfg_eff = handle.effective_config();
                let is_full = cfg_eff
                    .bundles
                    .strategy
                    .iter()
                    .any(|s| s.name == strategy && s.kind == walgit_config::BundleKind::Full);
                if is_full && walgit_wal::base_pack(&handle.manifest()).is_some() {
                    log(format!(
                        "{strategy} slot {slot}: composing header ∘ tier-2 base (no bytes through this host)"
                    ));
                    let e = crate::bundles::compose_full_from_base(
                        &state.registry,
                        id,
                        &strategy,
                        &cfg_eff,
                        slot,
                    )
                    .await
                    .map_err(|e| e.to_string())?;
                    state.caches.bundle_list.invalidate(&id.to_string());
                    return Ok((
                        format!(
                            "slot {} {} composed: {} bytes at seq {}, token {}",
                            e.strategy, slot, e.size, e.seq, e.creation_token
                        ),
                        serde_json::json!({ "id": e.id, "strategy": e.strategy, "slot": slot, "size": e.size, "seq": e.seq, "key": e.key, "built": true, "composed": true }),
                    ));
                }
                let entry = state
                    .bundles
                    .build_slot_unit(id, &strategy, slot)
                    .await
                    .map_err(|e| e.to_string())?;
                // The list changed (or a skip was recorded): this host's next
                // `GET bundles/list` must show it, not a cached render.
                state.caches.bundle_list.invalidate(&id.to_string());
                return Ok(match entry {
                    Some(e) => (
                        format!(
                            "slot {} {} built: {} bytes at seq {}, token {}",
                            e.strategy, slot, e.size, e.seq, e.creation_token
                        ),
                        serde_json::json!({ "id": e.id, "strategy": e.strategy, "slot": slot, "size": e.size, "seq": e.seq, "key": e.key, "built": true }),
                    ),
                    None => (
                        format!(
                            "slot {strategy} {slot}: nothing to build (built elsewhere, no new objects, or no refs at that time)"
                        ),
                        serde_json::json!({ "slot": slot, "built": false }),
                    ),
                });
            }
            if strategy == "due" {
                log("building all due bundle strategies".into());
                let entries = state
                    .bundles
                    .run_due(id, std::time::SystemTime::now())
                    .await
                    .map_err(|e| e.to_string())?;
                for e in &entries {
                    log(format!(
                        "built {} ({} bytes, token {})",
                        e.strategy, e.size, e.creation_token
                    ));
                }
                state.caches.bundle_list.invalidate(&id.to_string());
                let names: Vec<String> = entries.iter().map(|e| e.strategy.clone()).collect();
                return Ok((
                    format!("built {} due bundle(s)", entries.len()),
                    serde_json::json!({ "built": names }),
                ));
            }
            let strategy = if strategy.is_empty() {
                state
                    .cfg
                    .bundles
                    .strategy
                    .iter()
                    .find(|s| s.kind == walgit_config::BundleKind::Full)
                    .or(state.cfg.bundles.strategy.first())
                    .map(|s| s.name.clone())
                    .ok_or_else(|| "no bundle strategies configured".to_string())?
            } else {
                strategy
            };
            log(format!(
                "building bundle strategy {strategy} (git bundle create on the local copy, upload, CAS list)"
            ));
            let entry = state
                .bundles
                .build(id, &strategy)
                .await
                .map_err(|e| e.to_string())?;
            state.caches.bundle_list.invalidate(&id.to_string());
            let summary = format!(
                "bundle {} built: {} bytes at seq {}, creationToken {}",
                entry.strategy, entry.size, entry.seq, entry.creation_token
            );
            Ok((
                summary,
                serde_json::json!({
                    "id": entry.id, "strategy": entry.strategy, "kind": entry.kind,
                    "size": entry.size, "seq": entry.seq, "creation_token": entry.creation_token,
                    "key": entry.key,
                }),
            ))
        }
        "checkpoint" => {
            // Refs-level: a checkpoint is manifest + ref snapshot, it never
            // needs the packs on this instance (works for a large repository on a front).
            let guard = handle.sync_refs().await.map_err(|e| e.to_string())?;
            drop(guard);
            log(format!(
                "writing checkpoint at seq {}",
                handle.manifest().head_seq
            ));
            let cp = handle.write_checkpoint().await.map_err(|e| e.to_string())?;
            Ok((
                format!("checkpoint written at seq {}", cp.seq),
                serde_json::json!({ "at_seq": cp.seq }),
            ))
        }
        "sync" => {
            let before = handle.applied_seq();
            let guard = handle.sync().await.map_err(|e| e.to_string())?;
            drop(guard);
            let after = handle.applied_seq();
            let summary = format!(
                "synced: local seq {before} → {after}, manifest {}",
                handle
                    .manifest_version()
                    .map(|v| v.to_string())
                    .unwrap_or_default()
            );
            Ok((
                summary,
                serde_json::json!({ "before": before, "after": after }),
            ))
        }
        "rematerialize" => {
            log("discarding local copy and rebuilding from the store".into());
            handle.rematerialize().await.map_err(|e| e.to_string())?;
            Ok((
                format!("re-materialized at seq {}", handle.applied_seq()),
                serde_json::json!({ "seq": handle.applied_seq() }),
            ))
        }
        "snapshot" => {
            // Time travel, not a write: the WAL is replayed into a directory of
            // its own under `cache.dir` and the serving copy stays at the head
            // (AGENTS §2.4 — every state the repository was ever in is in the log).
            let raw = params.get("at_seq").map_or("", |v| v.trim());
            if raw.is_empty() {
                return Err("snapshot: missing `at_seq` (the WAL sequence to rewind to)".into());
            }
            let at_seq: u64 = raw.parse().map_err(|_| {
                format!("snapshot: `at_seq` must be a WAL sequence number, got {raw:?}")
            })?;
            let (snap, built) = walgit_wal::snapshot_at(
                state.registry.clone(),
                id.clone(),
                at_seq,
                narrate.clone(),
            )
            .await
            .map_err(|e| format!("snapshot: {e}"))?;
            tracing::info!(repo = %id, at_seq, built, refs = snap.ref_count, packs = snap.packs.len(), git_dir = %snap.git_dir, "snapshot materialized");
            let summary = format!(
                "{} at seq {} ({} ref(s), {} pack(s)) in {}; the WAL head is {} and the serving copy did not move",
                if built {
                    "materialized"
                } else {
                    "already materialized"
                },
                snap.at_seq,
                snap.ref_count,
                snap.packs.len(),
                snap.git_dir,
                snap.head_seq,
            );
            Ok((
                summary,
                serde_json::json!({ "built": built, "snapshot": snap }),
            ))
        }
        _ => Err("unknown op".into()),
    }
}

// ---------------------------------------------------------------------------
// Compaction (shared with the serve loop and the CLI)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, Default)]
pub struct CompactRequest {
    /// Ignore the trigger thresholds.
    pub force: bool,
    /// Force a full base rebuild (one pack + bitmap).
    pub rebuild_base: bool,
}

#[derive(Debug, Serialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum CompactOutcome {
    NotTriggered {
        tier0_packs: usize,
        tier0_bytes: u64,
    },
    LeaseHeld,
    Published {
        rebuild_base: bool,
        tier: u32,
        packs: Vec<String>,
        superseded: usize,
    },
}

impl CompactOutcome {
    pub fn summary(&self) -> String {
        match self {
            CompactOutcome::NotTriggered {
                tier0_packs,
                tier0_bytes,
            } => format!(
                "compaction not triggered ({tier0_packs} fresh packs, {tier0_bytes} bytes); use force=1"
            ),
            CompactOutcome::LeaseHeld => "compaction lease held by another instance".into(),
            CompactOutcome::Published {
                rebuild_base,
                tier,
                packs,
                superseded,
            } => format!(
                "{} published: {} pack(s) at tier {tier}, superseding {superseded}",
                if *rebuild_base {
                    "base rebuild"
                } else {
                    "geometric compaction"
                },
                packs.len()
            ),
        }
    }
}

/// Decide whether `handle` needs compaction, take the per-repo lease, repack
/// Whether the compaction trigger fires for `handle` (same rule as
/// [`compact_repo`] without `force`; base rebuilds are the VM job's on tmpfs hosts).
pub fn compaction_triggered(handle: &RepoHandle, cfg: &Config) -> bool {
    let manifest = handle.manifest();
    let tier0: Vec<_> = manifest.packs.iter().filter(|p| p.tier == 0).collect();
    let tier0_bytes: u64 = tier0.iter().map(|p| p.pack_size).sum();
    fold_due(tier0.len(), tier0_bytes, cfg)
}

/// Geometric folding is due when the fresh tier is over its count or byte trigger **and there is
/// something to fold**: one pack folds into itself (`git repack --geometric` writes nothing), so a
/// single big tier-0 pack — an import that never became a base — must not make every maintainer
/// pass run a 5 s no-op compaction (acme/large, 11.9 GB, 2026-08-22).
pub fn fold_due(tier0_count: usize, tier0_bytes: u64, cfg: &Config) -> bool {
    tier0_count >= 2
        && (tier0_count >= cfg.compaction.trigger_packs
            || tier0_bytes >= cfg.compaction.trigger_bytes.as_u64())
}

/// the local copy and publish the result as a COMPACT entry.
pub async fn compact_repo(
    handle: &RepoHandle,
    cfg: &Config,
    req: CompactRequest,
    log: Log<'_>,
) -> anyhow::Result<CompactOutcome> {
    // Sync to get the latest manifest, then release the read guard: the
    // publisher needs the repo lock and repack runs on the local copy anyway.
    // A base rebuild rewrites every byte, so it needs real local copies (never
    // a mount-linked base); geometric folding only touches tiers < 2.
    if req.rebuild_base {
        drop(handle.sync_full().await?);
    } else {
        drop(handle.sync().await?);
    }

    let manifest = handle.manifest();
    let tier0_packs: Vec<_> = manifest.packs.iter().filter(|p| p.tier == 0).collect();
    let tier0_count = tier0_packs.len();
    let tier0_bytes: u64 = tier0_packs.iter().map(|p| p.pack_size).sum();
    let base_bytes: u64 = manifest
        .packs
        .iter()
        .filter(|p| p.tier == 2)
        .map(|p| p.pack_size)
        .sum();
    let non_base_bytes: u64 = manifest
        .packs
        .iter()
        .filter(|p| p.tier < 2)
        .map(|p| p.pack_size)
        .sum();
    // The base (tier 2, one pack + bitmap) is rebuilt only when asked — the weekly slot's
    // `BaseRebuild` unit on the ssd host or `walgit compact --base` (AGENTS §2.5) — never by a
    // ratio inside the fold unit: on 2026-08-22 a redundant second full pack in a large repository's manifest
    // made "non-base ≥ 0.5 × base" true forever and every Compact unit ran a 30-min `repack -adb`
    // (7 × 32 GB packs in the bucket). Otherwise fold fresh packs geometrically into the medium tier.
    let rebuild_base = req.rebuild_base;
    let should_compact = req.force || fold_due(tier0_count, tier0_bytes, cfg) || rebuild_base;
    log(format!(
        "{} live packs: {tier0_count} fresh ({tier0_bytes} bytes), base {base_bytes} bytes, non-base {non_base_bytes} bytes; rebuild_base={rebuild_base}",
        manifest.packs.len()
    ));
    if !should_compact {
        return Ok(CompactOutcome::NotTriggered {
            tier0_packs: tier0_count,
            tier0_bytes,
        });
    }

    // Per-repo lease (the store handle is prefixed with the repo key).
    let lease_key = walgit_proto::keys::lease_key("compact");
    let holder = walgit_store::coord::instance_id();
    let lease_store: walgit_store::DynStore = Arc::new(handle.store().clone());
    let lease = walgit_store::coord::try_acquire(
        lease_store,
        &lease_key,
        holder,
        "compact",
        cfg.compaction.lease_ttl,
    )
    .await?;
    let Some(lease) = lease else {
        return Ok(CompactOutcome::LeaseHeld);
    };

    // A geometric fold never touches the base or a history pack (D18): both are `--keep-pack`'d.
    // On 2026-08-22 a fold on the SSD host (every pack a real local file) rolled a large repository's 32 GB base
    // and its 6 GB history pack into a tier-1 pack (seq 101) — no history pack for a day, and
    // the chain of consequences above.
    let protected: Vec<gix_hash::ObjectId> = manifest
        .packs
        .iter()
        .filter(|p| p.tier == 2 || p.kind == walgit_proto::v1::PackKind::History as i32)
        .filter_map(|p| gix_hash::ObjectId::from_hex(p.checksum.as_bytes()).ok())
        .collect();
    // Base rebuild: resumable, in a scratch copy, serving copy never rewritten (`rebuild.rs`,
    // BUNDLE_URI_DESIGN §5a). It installs + publishes itself; the lease is ours until it returns.
    if rebuild_base {
        log("lease acquired; base rebuild in a scratch copy (resumable)".to_string());
        let out = crate::rebuild::rebuild_base(handle, cfg, log).await;
        if let Err(e) = lease.release().await {
            log(format!("lease release failed: {e}"));
        }
        let out = out?;
        if out.resumed {
            log("rebuild resumed an earlier interrupted run".to_string());
        }
        return Ok(CompactOutcome::Published {
            rebuild_base: true,
            tier: 2,
            packs: out.packs,
            superseded: out.superseded,
        });
    }

    let repack_opts = RepackOptions {
        mode: RepackMode::Geometric {
            factor: cfg.compaction.factor,
        },
        write_bitmap: false,
        write_midx: true,
        keep: protected,
    };
    let tier = 1u32;
    log("lease acquired; running git repack -d --geometric --write-midx".to_string());
    let t = Instant::now();
    let result = match handle.local().repack(repack_opts).await {
        Ok(r) => r,
        Err(e) => {
            let _ = lease.release().await;
            return Err(e.into());
        }
    };
    log(format!(
        "repack done in {:.1}s: {} new pack(s), {} removed",
        t.elapsed().as_secs_f64(),
        result.new_packs.len(),
        result.removed.len()
    ));

    // Geometric: the new pack(s) supersede exactly what git removed — of the packs the manifest
    // lists (a stale local file nobody advertises is not a supersede).
    let live: std::collections::HashSet<String> =
        manifest.packs.iter().map(|p| p.checksum.clone()).collect();
    let supersedes: Vec<gix_hash::ObjectId> = result
        .removed
        .iter()
        .copied()
        .filter(|c| live.contains(&c.to_hex().to_string()))
        .collect();
    let superseded = supersedes.len();
    let mut supersedes_left = Some(supersedes);
    let mut packs = Vec::new();
    let mut first_err = None;
    for p in &result.new_packs {
        let hex = p.checksum.to_hex().to_string();
        let size = p.pack_size;
        match handle
            .publish_compact(p.clone(), supersedes_left.take().unwrap_or_default(), tier)
            .await
        {
            Ok(seq) => {
                log(format!("published pack {hex} ({size} bytes) as seq {seq}"));
                packs.push(hex);
            }
            Err(e) => {
                log(format!("publish_compact failed for {hex}: {e}"));
                first_err.get_or_insert(e);
            }
        }
    }
    if let Err(e) = lease.release().await {
        log(format!("lease release failed: {e}"));
    }
    if let Some(e) = first_err {
        return Err(e.into());
    }
    Ok(CompactOutcome::Published {
        rebuild_base: false,
        tier,
        packs,
        superseded,
    })
}

#[cfg(test)]
mod fold_tests {
    use super::fold_due;

    #[test]
    fn one_fresh_pack_never_triggers_folding_however_large() {
        let cfg = walgit_config::Config::default(); // trigger_packs 16, trigger_bytes 1 GiB
        assert!(
            !fold_due(1, 11_891_739_367, &cfg),
            "a single 11.9 GB import pack folds into itself"
        );
        assert!(!fold_due(0, 0, &cfg));
        assert!(
            fold_due(2, 2 << 30, &cfg),
            "two packs over the byte trigger"
        );
        assert!(fold_due(16, 1024, &cfg), "count trigger");
        assert!(!fold_due(15, 1024, &cfg));
    }
}
