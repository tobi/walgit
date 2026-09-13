//! Repository maintenance operations ("make the repo great"): fsck, compaction,
//! checkpoints, re-materialize. Shared by the background loops
//! (`walgit serve` roles), the CLI, and the web UI's `POST …/ops/{op}` route,
//! which streams the op's log as SSE and records the outcome per instance.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tracing::Instrument;

use prost::Message;
use serde::Serialize;

use walgit_git::RepoId;
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
        description: "Run one conserving pack lifecycle unit under the per-repository lease. \
                      force=1 ignores fold thresholds; base=1 requests a whole-cut re-segmentation.",
        params: &["force", "base"],
        mutating: true,
    },
    OpSpec {
        id: "checkpoint",
        label: "Checkpoint",
        description: "Write a checkpoint (pack set + ref snapshot) at the current head so cold materialize starts from here.",
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
        .is_some_and(|v| matches!(v.as_str(), "1" | "true" | "yes" | "on"))
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
            tracing::info!(repo = %id, seq, missing = missing.len(), problems = report.problems, elapsed_ms = u64::try_from(t0.elapsed().as_millis()).unwrap_or(u64::MAX), "fsck recorded");
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
            tracing::info!(repo = %id, seq, objects = pack.objects, bytes = pack.bytes, %upstream, elapsed_ms = u64::try_from(t0.elapsed().as_millis()).unwrap_or(u64::MAX), "repair published");
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
            let bytes = std::fs::metadata(&rev).map_or(0, |m| m.len());
            log(format!(
                "pack-{checksum}.rev: {bytes} bytes in {:.1}s; publishing",
                t0.elapsed().as_secs_f64()
            ));
            handle
                .annotate_pack(&checksum, Some(rev), None, None)
                .await
                .map_err(|e| format!("rev-index publish: {e}"))?;
            tracing::info!(repo = %id, pack = %checksum, bytes, elapsed_ms = u64::try_from(t0.elapsed().as_millis()).unwrap_or(u64::MAX), "rev index published");
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
    /// Force a conserving re-segmentation of the committed object set.
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
                    "whole-cut re-segmentation"
                } else {
                    "pack lifecycle unit"
                },
                packs.len()
            ),
        }
    }
}

/// Run one desired-state pack unit under the repository lease. Planning reads
/// validated committed settings before any bulk materialization.
pub async fn compact_repo(
    handle: &RepoHandle,
    req: CompactRequest,
    log: Log<'_>,
) -> anyhow::Result<CompactOutcome> {
    // A cancelled operation must also stop its heartbeat so the lease can expire.
    struct Heartbeat(tokio::task::JoinHandle<()>);
    impl Drop for Heartbeat {
        fn drop(&mut self) {
            self.0.abort();
        }
    }
    drop(handle.sync_refs().await?);
    let cfg = handle.validated_effective_config()?;
    anyhow::ensure!(
        cfg.packs.enabled,
        "pack maintenance is disabled by the effective repository settings"
    );
    let manifest = handle.manifest();
    let fresh: Vec<_> = manifest.packs.iter().filter(|p| p.tier == 0).collect();
    let not_triggered = || CompactOutcome::NotTriggered {
        tier0_packs: fresh.len(),
        tier0_bytes: fresh.iter().map(|p| p.pack_size).sum(),
    };
    if !req.force
        && !req.rebuild_base
        && crate::pack_lifecycle::plan(&manifest, &cfg, std::time::SystemTime::now()).is_none()
    {
        return Ok(not_triggered());
    }
    let store: walgit_store::DynStore = Arc::new(handle.store().clone());
    let Some(lease) = walgit_store::coord::try_acquire(
        store,
        &walgit_proto::keys::lease_key("compact"),
        walgit_store::coord::instance_id(),
        "pack lifecycle",
        cfg.packs.lease_ttl,
    )
    .await?
    else {
        return Ok(CompactOutcome::LeaseHeld);
    };
    let lease = Arc::new(tokio::sync::Mutex::new(lease));
    let mut heartbeat = Heartbeat(walgit_store::coord::LeaseGuard::spawn_heartbeat(
        lease.clone(),
        (cfg.packs.lease_ttl / 3).max(std::time::Duration::from_millis(1)),
        cfg.packs.lease_ttl,
    ));
    log("lease acquired; running one isolated pack lifecycle unit".into());
    let result =
        crate::pack_lifecycle::run(handle, &cfg, req.force, req.rebuild_base, log, &lease).await;
    heartbeat.0.abort();
    let _ = (&mut heartbeat.0).await;
    if let Ok(lease) = Arc::try_unwrap(lease)
        && let Err(error) = lease.into_inner().release().await
    {
        log(format!("lease release failed: {error}"));
    }
    match result? {
        None => Ok(not_triggered()),
        Some(out) => {
            if out.resumed {
                log("resumed validated pack lifecycle progress".into());
            }
            Ok(CompactOutcome::Published {
                rebuild_base: req.rebuild_base,
                tier: out.tier,
                packs: out.packs,
                superseded: out.superseded,
            })
        }
    }
}
