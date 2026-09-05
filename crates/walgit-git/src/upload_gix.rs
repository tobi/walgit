//! In-process upload-pack engine (D2) built for repositories that are bigger
//! than the instance serving them.
//!
//! What it does differently from a stock `git upload-pack`:
//!
//! * **History walks never need pack data**: commits come from the commit-graph
//!   chain (`objects/info/commit-graphs`, the tier-2 layer travels with the
//!   base pack in the WAL) and `have`s are validated against pack *indexes*
//!   (local or the remote reader's), so the 32 GB base pack is never opened
//!   for negotiation.
//! * **Object enumeration by tree diff**: with `have`s, each new commit is
//!   diffed against its parents and only the objects that are new relative to
//!   *every* parent enter the pack (unchanged subtrees are skipped by oid).
//!   A diff-sized fetch touches a few thousand objects instead of walking the
//!   monorepo's whole tree per commit. Objects that are not local (parent
//!   subtrees in the base) are reported in one batch per tree level and
//!   faulted in by an [`ObjectFaulter`] (the remote reader, or nothing when a
//!   store mount makes the base readable anyway), then the enumeration
//!   retries. Rounds ≈ path depth of the changes, each round one parallel
//!   batch of range reads.
//! * **Streaming output**: the pack is written as it is produced (bounded
//!   channel between the blocking generator and the sideband writer); nothing
//!   buffers a whole pack in memory.
//! * **`sideband-all`** framing (every section line on band 1) and band-2
//!   progress, so the server can narrate before and while the pack streams.
//!
//! Delta reuse: entries that already live in local packs are copied as-is
//! (`PackCopyAndBaseObjects`); loose (faulted) objects are compressed fresh.

use std::collections::{HashMap, HashSet};

use futures::future::BoxFuture;
use gix_object::{Find, FindHeader, Kind as ObjKind};
use gix_pack::data::Version as PackVersion;
use gix_pack::data::output::bytes::FromEntriesIter;
use gix_pack::data::output::count;
use gix_pack::data::output::entry;
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::{
    GitError, LocalRepo, PackFilter, UploadPackRequest, UploadPackStats, ge, parse_filter, pkt,
};

/// Access to objects that are not in the local object store: the remote
/// reader (pack indexes local, data by range read) or any other source that
/// can answer "is this object in the pack set" without I/O and write objects
/// into the local loose store on demand.
pub trait ObjectFaulter: Send + Sync {
    /// Whether the object exists in the (remote) pack set. Index lookup only.
    fn contains(&self, oid: &gix_hash::oid) -> bool;
    /// Fetch `oids` and write each into the local loose store. Returns how
    /// many were found (missing ones are simply not written).
    fn fault<'a>(
        &'a self,
        oids: &'a [gix_hash::ObjectId],
    ) -> BoxFuture<'a, Result<usize, GitError>>;
}

/// Maximum fault rounds (each round faults one tree level of the diff).
const MAX_FAULT_ROUNDS: usize = 64;

/// Run a synchronous, potentially long section without stalling the async
/// runtime: `block_in_place` on a multi-thread runtime (the worker yields its
/// core), inline on a current-thread runtime (tests).
fn blocking_section<T>(f: impl FnOnce() -> T) -> T {
    match tokio::runtime::Handle::try_current() {
        Ok(h) if h.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(f)
        }
        _ => f(),
    }
}

/// Write one section line, wrapped in a band-1 frame when the client asked
/// for `sideband-all` (flush/delim stay raw, as in git's `packet_writer`).
fn line(buf: &mut Vec<u8>, data: &[u8], sideband_all: bool) {
    if sideband_all {
        let mut framed = Vec::with_capacity(data.len() + 1);
        framed.push(1u8);
        framed.extend_from_slice(data);
        pkt::encode_data(buf, &framed);
    } else {
        pkt::encode_data(buf, data);
    }
}

/// Outcome of one (sync) enumeration attempt.
enum Enumerated {
    Done {
        set: HashSet<gix_hash::ObjectId>,
        commits: usize,
        diffed: bool,
    },
    /// Objects needed for the diff that are not local; fault and retry.
    Need(Vec<gix_hash::ObjectId>),
}

impl LocalRepo {
    /// The gix engine: see the module docs. `faulter` supplies objects that
    /// are not local (None = everything needed is local or readable through
    /// the pack directory, e.g. a store-mounted base).
    pub async fn upload_pack_gix_with<W: AsyncWrite + Unpin + Send>(
        &self,
        req: UploadPackRequest,
        mut out: W,
        faulter: Option<&dyn ObjectFaulter>,
    ) -> Result<UploadPackStats, GitError> {
        let sb_all = req.sideband_all;
        let progress = !req.no_progress;

        // ---- negotiation: which haves do we know? (indexes only) ----
        let common_haves: Vec<gix_hash::ObjectId> = {
            let repo = self.gix();
            req.haves
                .iter()
                .filter(|h| repo.has_object(h) || faulter.is_some_and(|f| f.contains(h)))
                .copied()
                .collect()
        };

        let mut hdr = Vec::with_capacity(1024);
        if !req.done {
            line(&mut hdr, b"acknowledgments\n", sb_all);
            if common_haves.is_empty() {
                line(&mut hdr, b"NAK\n", sb_all);
                pkt::encode_flush(&mut hdr);
                out.write_all(&hdr).await.map_err(GitError::Io)?;
                return Ok(UploadPackStats::default());
            }
            for h in &common_haves {
                line(&mut hdr, format!("ACK {}\n", h.to_hex()).as_bytes(), sb_all);
            }
            line(&mut hdr, b"ready\n", sb_all);
            pkt::encode_delim(&mut hdr);
        }

        // ---- shallow-info ----
        if let Some(depth) = req.deepen {
            let repo = self.gix();
            let plan = crate::compute_shallow(&repo, &req.wants, depth)?;
            line(&mut hdr, b"shallow-info\n", sb_all);
            for s in &plan.shallow {
                line(
                    &mut hdr,
                    format!("shallow {}\n", s.to_hex()).as_bytes(),
                    sb_all,
                );
            }
            // Client-side shallow commits that this fetch deepens past.
            for s in &req.shallow {
                if !plan.shallow.contains(s) {
                    line(
                        &mut hdr,
                        format!("unshallow {}\n", s.to_hex()).as_bytes(),
                        sb_all,
                    );
                }
            }
            pkt::encode_delim(&mut hdr);
        }

        // ---- wanted-refs ----
        if !req.want_refs.is_empty() {
            let snap = self.refs()?;
            let ref_map: HashMap<&str, &str> = snap
                .refs
                .iter()
                .map(|r| (r.name.as_str(), r.oid.as_str()))
                .collect();
            line(&mut hdr, b"wanted-refs\n", sb_all);
            for name in &req.want_refs {
                let oid = ref_map.get(name.as_str()).copied().unwrap_or_default();
                line(&mut hdr, format!("{oid} {name}\n").as_bytes(), sb_all);
            }
            pkt::encode_delim(&mut hdr);
        }

        // Sections so far go out now: the client sees acknowledgments while
        // we enumerate (and, with sideband-all, our progress lines).
        out.write_all(&hdr).await.map_err(GitError::Io)?;
        let mut sink = PackOut::Sideband {
            sb: pkt::Sideband::new(out),
            sb_all,
            progress,
        };
        let stats = self
            .produce_pack(&req, &common_haves, faulter, &mut sink)
            .await?;
        sink.finish().await?;
        Ok(stats)
    }

    /// A git bundle (`# v2 git bundle` header with `refs` and `-<prereq>`
    /// lines, then the pack) written with this engine: the pack is exactly
    /// "objects reachable from `refs` minus `prerequisites`", enumerated by
    /// tree diff, so incremental bundles of a repository whose base is
    /// linked/remote-served never walk the base's trees (stock `git bundle
    /// create` marks every tree of the boundary commits uninteresting = reads
    /// them all). `refs` are `(name, oid)`; prerequisites are commit ids the
    /// consumer must already have. Returns the pack's object count and bytes.
    pub async fn write_bundle_gix<W: AsyncWrite + Unpin + Send>(
        &self,
        mut out: W,
        refs: &[(String, gix_hash::ObjectId)],
        prerequisites: &[gix_hash::ObjectId],
        faulter: Option<&dyn ObjectFaulter>,
    ) -> Result<UploadPackStats, GitError> {
        let mut header = String::from("# v2 git bundle\n");
        for p in prerequisites {
            {
                let _ = std::fmt::Write::write_fmt(&mut header, format_args!("-{} \n", p.to_hex()));
            };
        }
        for (name, oid) in refs {
            {
                let _ = std::fmt::Write::write_fmt(
                    &mut header,
                    format_args!("{} {name}\n", oid.to_hex()),
                );
            };
        }
        header.push('\n');
        out.write_all(header.as_bytes())
            .await
            .map_err(GitError::Io)?;
        let req = UploadPackRequest {
            wants: refs.iter().map(|(_, o)| *o).collect(),
            haves: prerequisites.to_vec(),
            done: true,
            thin_pack: false,
            no_progress: true,
            include_tag: false,
            ofs_delta: true,
            sideband_all: false,
            wait_for_done: false,
            filter: None,
            deepen: None,
            deepen_since: None,
            deepen_not: vec![],
            shallow: vec![],
            want_refs: vec![],
            packfile_uris_protocols: vec![],
        };
        // Prerequisites are known by definition (the bundle's consumer has them).
        let common: Vec<gix_hash::ObjectId> = prerequisites.to_vec();
        let mut sink = PackOut::Raw(out);
        let stats = self.produce_pack(&req, &common, faulter, &mut sink).await?;
        sink.finish().await?;
        Ok(stats)
    }

    /// Enumerate (retrying after faults) and stream the pack into `sink`.
    async fn produce_pack<W: AsyncWrite + Unpin + Send>(
        &self,
        req: &UploadPackRequest,
        common_haves: &[gix_hash::ObjectId],
        faulter: Option<&dyn ObjectFaulter>,
        sink: &mut PackOut<W>,
    ) -> Result<UploadPackStats, GitError> {
        let t_start = std::time::Instant::now();
        // Wants that are not local (a blob of a lazy checkout, a tag in the
        // base) are faulted first so the enumeration knows their type.
        if let Some(f) = faulter {
            let missing: Vec<gix_hash::ObjectId> = {
                let repo = self.gix();
                req.wants
                    .iter()
                    .filter(|w| !repo.has_object(*w) && f.contains(w))
                    .copied()
                    .collect()
            };
            if !missing.is_empty() {
                sink.progress(&format!(
                    "reading {} wanted object(s) from the bucket\n",
                    missing.len()
                ))
                .await;
                let found = f.fault(&missing).await?;
                if found < missing.len() {
                    return Err(GitError::MissingObject {
                        oid: missing
                            .first()
                            .ok_or_else(|| {
                                GitError::InvalidInput("empty missing-object set".into())
                            })?
                            .to_hex()
                            .to_string(),
                    });
                }
                self.refresh_async().await?;
            }
        }

        // ---- enumerate (sync, retried after faulting missing objects) ----
        let filter = req.filter.as_deref().map_or(PackFilter::None, parse_filter);
        let has_filter = req.filter.is_some();
        let mut rounds = 0usize;
        let (set, commits, diffed) = loop {
            // The walk (commit-graph + tree diffs) is CPU/IO-bound for
            // seconds on big ranges: never on an async worker (D19).
            let attempt = blocking_section(|| {
                let repo = self.gix();
                enumerate(&repo, req, common_haves, &filter, faulter)
            })?;
            match attempt {
                Enumerated::Done {
                    set,
                    commits,
                    diffed,
                } => break (set, commits, diffed),
                Enumerated::Need(missing) => {
                    rounds += 1;
                    let Some(f) = faulter else {
                        return Err(GitError::MissingObject {
                            oid: missing
                                .first()
                                .ok_or_else(|| {
                                    GitError::InvalidInput("empty missing-object set".into())
                                })?
                                .to_hex()
                                .to_string(),
                        });
                    };
                    if rounds > MAX_FAULT_ROUNDS {
                        return Err(GitError::Protocol(format!(
                            "object enumeration did not converge after {MAX_FAULT_ROUNDS} fault rounds ({} still missing)",
                            missing.len()
                        )));
                    }
                    sink.progress(&format!(
                        "reading {} base object(s) from the bucket (round {rounds})\n",
                        missing.len()
                    ))
                    .await;
                    let found = f.fault(&missing).await?;
                    if found == 0 {
                        return Err(GitError::MissingObject {
                            oid: missing
                                .first()
                                .ok_or_else(|| {
                                    GitError::InvalidInput("empty missing-object set".into())
                                })?
                                .to_hex()
                                .to_string(),
                        });
                    }
                    self.refresh_async().await?;
                }
            }
        };
        let t_enum = t_start.elapsed();
        sink.progress(&format!(
            "Enumerating objects: {} ({} commit(s){}{})\n",
            set.len(),
            commits,
            if diffed { ", tree diff" } else { "" },
            if rounds > 0 {
                format!(", {rounds} fault round(s)")
            } else {
                String::new()
            }
        ))
        .await;

        // Anything in the set that is not local (a want pointing into the
        // base, a tag target, …) is faulted in one batch before packing.
        {
            let missing: Vec<gix_hash::ObjectId> = blocking_section(|| {
                let repo = self.gix();
                set.iter()
                    .filter(|o| !repo.has_object(*o))
                    .copied()
                    .collect()
            });
            if !missing.is_empty() {
                let Some(f) = faulter else {
                    return Err(GitError::MissingObject {
                        oid: missing
                            .first()
                            .ok_or_else(|| {
                                GitError::InvalidInput("empty missing-object set".into())
                            })?
                            .to_hex()
                            .to_string(),
                    });
                };
                sink.progress(&format!(
                    "reading {} wanted object(s) from the base pack\n",
                    missing.len()
                ))
                .await;
                let found = f.fault(&missing).await?;
                if found < missing.len() {
                    return Err(GitError::MissingObject {
                        oid: missing
                            .first()
                            .ok_or_else(|| {
                                GitError::InvalidInput("empty missing-object set".into())
                            })?
                            .to_hex()
                            .to_string(),
                    });
                }
                self.refresh_async().await?;
            }
        }

        // ---- packfile section, streamed ----
        sink.begin_pack().await?;

        let object_hash = self.object_format().kind();
        let find = {
            let tsr = self.inner.tsr.lock();
            frozen_pack_source(&tsr.objects)
        };
        let expansion = if !has_filter && req.deepen.is_none() && common_haves.is_empty() {
            count::objects::ObjectExpansion::TreeContents
        } else {
            count::objects::ObjectExpansion::AsIs
        };
        // Never a thin pack out of the gix engine, whatever the client offered (`thin-pack`): with
        // `allow_thin_pack`, gix's entry writer turns every OFS_DELTA whose base is outside the set
        // into a REF_DELTA by loading the **whole base pack's (offset → oid) table**
        // (`pack_offsets_and_oid`) — and it does so per 256-object chunk, per thread
        // (`iter_from_counts`: the table is a chunk-local cache). On a large repository that is 60 M entries ≈
        // 1.7 GB, rebuilt and sorted by 44 threads at once: the 178 GB anon RSS OOM of 2026-08-21
        // 07:0xZ (113,683 objects, 1,990 commits). It is also the only place gix *writes an object
        // id* into the pack; before `frozen_pack_source` a mid-pack refresh could pair an offset
        // with another pack's table — the entry-under-another-id pack of 05:4xZ. Self-contained:
        // deltas whose base the client has are re-encoded as full objects (+39 % bytes on a large repository's
        // hourly remainder, −35 % client index-pack; `docs/BUNDLE_URI_DESIGN.md` §5 made the same
        // call for bundles), memory stays O(threads × one object), no id is ever written.
        let thin = false;
        let ids: Vec<gix_hash::ObjectId> = set.into_iter().collect();

        let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<u8>>(16);
        let generator = tokio::task::spawn_blocking(move || {
            let mut w = ChanWriter {
                tx,
                buf: Vec::with_capacity(CHUNK),
            };
            let n = generate_pack_streaming(find, ids, object_hash, thin, expansion, &mut w)?;
            w.flush_all()?;
            Ok::<u32, GitError>(n)
        });
        let mut bytes = 0u64;
        while let Some(chunk) = rx.recv().await {
            bytes += chunk.len() as u64;
            sink.data(&chunk).await?;
        }
        let num_objects = generator
            .await
            .map_err(|e| GitError::Protocol(format!("pack generator panicked: {e}")))??;
        tracing::debug!(
            enumerate_ms = u64::try_from(t_enum.as_millis()).unwrap_or(u64::MAX),
            total_ms = u64::try_from(t_start.elapsed().as_millis()).unwrap_or(u64::MAX),
            objects = num_objects,
            bytes,
            rounds,
            "gix upload-pack timing"
        );
        sink.progress(&format!("Total {num_objects} objects, {bytes} bytes\n"))
            .await;
        Ok(UploadPackStats {
            objects: u64::from(num_objects),
            bytes,
        })
    }
}

/// Where the pack goes: the v2 packfile section (sideband, progress on band
/// 2) or a raw byte sink (bundle file).
enum PackOut<W: AsyncWrite + Unpin + Send> {
    Sideband {
        sb: pkt::Sideband<W>,
        sb_all: bool,
        progress: bool,
    },
    Raw(W),
}

impl<W: AsyncWrite + Unpin + Send> PackOut<W> {
    async fn progress(&mut self, text: &str) {
        match self {
            PackOut::Sideband {
                sb, progress: true, ..
            } => {
                let _ = sb.write_progress(text.as_bytes()).await;
            }
            PackOut::Sideband { .. } => {}
            PackOut::Raw(_) => {
                tracing::debug!(target: "walgit_git::upload_gix", "{}", text.trim_end());
            }
        }
    }
    async fn begin_pack(&mut self) -> Result<(), GitError> {
        if let PackOut::Sideband { sb, sb_all, .. } = self {
            let mut pf = Vec::with_capacity(16);
            line(&mut pf, b"packfile\n", *sb_all);
            sb.inner_mut().write_all(&pf).await.map_err(GitError::Io)?;
        }
        Ok(())
    }
    async fn data(&mut self, chunk: &[u8]) -> Result<(), GitError> {
        match self {
            PackOut::Sideband { sb, .. } => sb.write_data(chunk).await,
            PackOut::Raw(w) => w.write_all(chunk).await.map_err(GitError::Io),
        }
    }
    async fn finish(&mut self) -> Result<(), GitError> {
        match self {
            PackOut::Sideband { sb, .. } => sb.flush().await,
            PackOut::Raw(w) => w.flush().await.map_err(GitError::Io),
        }
    }
}

const CHUNK: usize = 256 * 1024;

/// `std::io::Write` that ships chunks over a bounded tokio channel
/// (backpressure from the client connection reaches the generator).
struct ChanWriter {
    tx: tokio::sync::mpsc::Sender<Vec<u8>>,
    buf: Vec<u8>,
}

impl ChanWriter {
    fn flush_all(&mut self) -> Result<(), GitError> {
        if !self.buf.is_empty() {
            let chunk = std::mem::take(&mut self.buf);
            self.tx.blocking_send(chunk).map_err(|_| {
                GitError::Io(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "client went away",
                ))
            })?;
        }
        Ok(())
    }
}

impl std::io::Write for ChanWriter {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        self.buf.extend_from_slice(data);
        if self.buf.len() >= CHUNK {
            let chunk = std::mem::replace(&mut self.buf, Vec::with_capacity(CHUNK));
            self.tx.blocking_send(chunk).map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::BrokenPipe, "client went away")
            })?;
        }
        Ok(data.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// A pack-copy source whose view of the object store cannot change for the
/// duration of one pack: every index is loaded into the handle's snapshot NOW,
/// packs are pinned (`prevent_pack_unload`), and the handle never refreshes
/// again (`refresh_never`).
///
/// Why: `count::objects` records each object as `(pack id, offset)` and
/// `iter_from_counts` copies the raw entry from that location later. A pack id
/// is a slot + an index inside the multi-pack-index — not a pack checksum. With
/// the default refresh mode a lookup that misses (an object from a pack
/// installed after the midx was written) lazily loads the next index *and*
/// consolidates on-disk state: a rewritten midx comes back as a new generation
/// with a different pack order, and `entry_by_location` then resolves the old
/// id in the new snapshot — another pack's bytes under the counted oid. Prod
/// 2026-08-21 05:4xZ (the SSD host, a churn pack installed every 3 min): the
/// remainder of a bundle-fed clone carried one such entry and the client died
/// with "The same object … appears twice in the pack". Pinning alone does not
/// help (it keeps packs mapped, not the id → pack mapping). With the frozen
/// snapshot an object that only exists in a newer pack is *not found* (the
/// fetch fails, the client retries) — never copied wrong.
pub(crate) fn frozen_pack_source(objects: &std::sync::Arc<gix_odb::Store>) -> gix_odb::HandleArc {
    let mut find = objects.to_cache_arc();
    find.prevent_pack_unload();
    // One lookup of an oid that cannot exist walks `load_one_index` until no
    // index remains: the handle's snapshot now holds every index on disk.
    let _ = gix_object::FindHeader::try_header(
        &find,
        &gix_hash::ObjectId::null(find.store_ref().object_hash()),
    );
    find.refresh_never();
    find
}

fn generate_pack_streaming(
    find: gix_odb::HandleArc,
    object_ids: Vec<gix_hash::ObjectId>,
    object_hash: gix_hash::Kind,
    allow_thin_pack: bool,
    input_object_expansion: count::objects::ObjectExpansion,
    out: &mut impl std::io::Write,
) -> Result<u32, GitError> {
    // Small sets (diff-sized fetches) are dominated by thread/chunk setup:
    // run them single-threaded; big sets use every core.
    let small = object_ids.len() < 4096;
    let thread_limit = if small {
        Some(1)
    } else {
        std::thread::available_parallelism()
            .map(std::num::NonZero::get)
            .ok()
    };
    let chunk_size = if small { 64 } else { 256 };
    let interrupt = std::sync::atomic::AtomicBool::new(false);
    let (counts, _) = count::objects(
        find.clone(),
        Box::new(object_ids.into_iter().map(Ok)),
        &gix_features::progress::Discard,
        &interrupt,
        count::objects::Options {
            thread_limit,
            chunk_size,
            input_object_expansion,
        },
    )
    .map_err(ge)?;
    if counts.is_empty() {
        let header = gix_pack::data::header::encode(PackVersion::V2, 0);
        let mut buf = header.to_vec();
        let trailer = crate::compute_pack_trailer(&buf, object_hash)?;
        buf.extend_from_slice(trailer.as_slice());
        out.write_all(&buf).map_err(GitError::Io)?;
        return Ok(0);
    }
    let num_entries = u32::try_from(counts.len())
        .map_err(|_| GitError::InvalidInput("pack exceeds u32 object count".into()))?;
    let progress: Box<dyn gix_features::progress::DynNestedProgress + 'static> =
        Box::new(gix_features::progress::Discard);
    let entries = entry::iter_from_counts(
        counts,
        find,
        progress,
        entry::iter_from_counts::Options {
            version: PackVersion::V2,
            mode: entry::iter_from_counts::Mode::PackCopyAndBaseObjects,
            allow_thin_pack,
            thread_limit,
            chunk_size,
            compression: gix::zlib::Compression::DEFAULT,
        },
    );
    let entries_in_order = gix_features::parallel::InOrderIter::from(entries);
    let pack_iter = FromEntriesIter::new(
        entries_in_order,
        out,
        num_entries,
        PackVersion::V2,
        object_hash,
    );
    for result in pack_iter {
        result.map_err(ge)?;
    }
    Ok(num_entries)
}

// ---------------------------------------------------------------------------
// Enumeration
// ---------------------------------------------------------------------------

fn enumerate(
    repo: &gix::Repository,
    req: &UploadPackRequest,
    common_haves: &[gix_hash::ObjectId],
    filter: &PackFilter,
    faulter: Option<&dyn ObjectFaulter>,
) -> Result<Enumerated, GitError> {
    // No haves and everything local (no faulter): the classic path (full tree
    // walk per commit with subtree dedupe; the count phase expands tree
    // contents for an unfiltered clone). With a faulter, the level-batched
    // walk below handles the no-have case too (each commit's tree diffed
    // against nothing = a full walk whose non-local trees are faulted one
    // level per round) — that is what a zero-have `--depth=1 --filter=blob:none`
    // clone of a remote-served monorepo needs.
    let diff_mode = !common_haves.is_empty();
    if !diff_mode && faulter.is_none() {
        let set = crate::compute_object_set(
            repo,
            &req.wants,
            common_haves,
            req.filter.as_deref(),
            req.include_tag,
            req.deepen,
        )?;
        return Ok(Enumerated::Done {
            set,
            commits: 0,
            diffed: false,
        });
    }

    let kind = repo.object_hash();
    let mut set: HashSet<gix_hash::ObjectId> = HashSet::new();
    let mut missing: Vec<gix_hash::ObjectId> = Vec::new();
    let mut buf = Vec::new();

    // Wants: commits walk, tags peel, trees/blobs direct.
    let mut commit_wants = Vec::new();
    for w in &req.wants {
        match repo.objects.try_header(w).map_err(GitError::Gix)? {
            Some(h) if h.kind == ObjKind::Commit => commit_wants.push(*w),
            Some(h) if h.kind == ObjKind::Tag => {
                set.insert(*w);
                let mut cur = *w;
                loop {
                    let Some(obj) = repo
                        .objects
                        .try_find(&cur, &mut buf)
                        .map_err(GitError::Gix)?
                    else {
                        missing.push(cur);
                        break;
                    };
                    if obj.kind != ObjKind::Tag {
                        if obj.kind == ObjKind::Commit {
                            commit_wants.push(cur);
                        } else if obj.kind == ObjKind::Tree {
                            set.insert(cur);
                            crate::walk_tree_with_filter(repo, cur, &mut set, filter, 0, &mut buf)?;
                        } else {
                            set.insert(cur);
                        }
                        break;
                    }
                    let tag = gix_object::TagRef::from_bytes(obj.data, kind).map_err(ge)?;
                    let target = tag.target();
                    set.insert(target);
                    cur = target;
                }
            }
            Some(h) if h.kind == ObjKind::Tree => {
                set.insert(*w);
                if !matches!(filter, PackFilter::Tree(0)) {
                    crate::walk_tree_with_filter(repo, *w, &mut set, filter, 0, &mut buf)?;
                }
            }
            Some(_) => {
                set.insert(*w);
            }
            None => {
                // Not local: maybe in the remote set (faulted before packing);
                // a commit there would need the graph, which we have.
                if faulter.is_some_and(|f| f.contains(w)) {
                    commit_wants.push(*w);
                } else {
                    set.insert(*w);
                }
            }
        }
    }

    // A have that is shallow on the client does not imply its parents: when
    // the client deepens, those parents must be sent, so such haves do not
    // hide anything.
    let mut hidden: Vec<gix_hash::ObjectId> = common_haves
        .iter()
        .filter(|h| !req.shallow.contains(h))
        .copied()
        .collect();
    if let Some(depth) = req.deepen {
        hidden.extend(crate::compute_shallow(repo, &req.wants, depth)?.exclude);
    }
    let hidden_set: HashSet<gix_hash::ObjectId> = hidden.iter().copied().collect();

    let mut commits = 0usize;
    if !commit_wants.is_empty() {
        let walk = repo
            .rev_walk(commit_wants.iter().copied())
            .with_hidden(hidden.iter().copied())
            .all()
            .map_err(ge)?;
        for item in walk {
            let info = item.map_err(ge)?;
            let cid = info.id;
            if hidden_set.contains(&cid) || !set.insert(cid) {
                continue;
            }
            commits += 1;
            // Tree of the commit and of each parent (parents that are hidden
            // or already sent both count as "the client has it").
            let (tree_id, parent_ids) = {
                let Some(obj) = repo
                    .objects
                    .try_find(&cid, &mut buf)
                    .map_err(GitError::Gix)?
                else {
                    missing.push(cid);
                    continue;
                };
                let c = gix_object::CommitRefIter::from_bytes(obj.data, kind);
                let mut tree = None;
                let mut parents = Vec::new();
                for tok in c {
                    match tok.map_err(ge)? {
                        gix_object::commit::ref_iter::Token::Tree { id } => tree = Some(id),
                        gix_object::commit::ref_iter::Token::Parent { id } => parents.push(id),
                        _ => break,
                    }
                }
                (
                    tree.ok_or_else(|| GitError::Protocol(format!("commit {cid} has no tree")))?,
                    parents,
                )
            };
            if matches!(filter, PackFilter::Tree(0)) {
                continue; // no trees at all, the root included
            }
            if parent_ids.is_empty() || !diff_mode {
                // Nothing the client has: full walk, non-local trees faulted
                // one level per round (olds = []).
                if set.insert(tree_id) {
                    diff_tree_new_objects(
                        repo,
                        tree_id,
                        &[],
                        filter,
                        0,
                        &mut set,
                        &mut missing,
                        &mut buf,
                    )?;
                }
                continue;
            }
            let mut parent_trees: Vec<Old> = Vec::with_capacity(parent_ids.len());
            let mut deferred = false;
            for p in &parent_ids {
                if let Some(obj) = repo.objects.try_find(p, &mut buf).map_err(GitError::Gix)? {
                    match gix_object::CommitRefIter::from_bytes(obj.data, kind).tree_id() {
                        Ok(t) => parent_trees.push(Old::Tree(t)),
                        Err(e) => return Err(ge(e)),
                    }
                } else {
                    // Parent commit not local (base): fault it, diff this
                    // commit on the retry.
                    missing.push(*p);
                    deferred = true;
                }
            }
            if deferred {
                set.remove(&cid);
                commits -= 1;
                continue;
            }
            if set.insert(tree_id) {
                diff_tree_new_objects(
                    repo,
                    tree_id,
                    &parent_trees,
                    filter,
                    0,
                    &mut set,
                    &mut missing,
                    &mut buf,
                )?;
            }
        }
    }

    if !missing.is_empty() {
        missing.sort_unstable();
        missing.dedup();
        return Ok(Enumerated::Need(missing));
    }

    // include-tag: annotated tags whose target is in the set.
    if req.include_tag
        && let Ok(snap) = crate::read_refs(repo.path())
    {
        for r in &snap.refs {
            let Ok(tag_oid) = gix_hash::ObjectId::from_hex(r.oid.as_bytes()) else {
                continue;
            };
            if set.contains(&tag_oid) {
                continue;
            }
            if let Ok(Some(obj)) = repo.objects.try_find(&tag_oid, &mut buf)
                && obj.kind == ObjKind::Tag
                && let Ok(tag) = gix_object::TagRef::from_bytes(obj.data, kind)
                && set.contains(&tag.target())
            {
                set.insert(tag_oid);
            }
        }
    }
    Ok(Enumerated::Done {
        set,
        commits,
        diffed: diff_mode,
    })
}

type Entry = (gix_object::tree::EntryMode, Vec<u8>, gix_hash::ObjectId);

/// A parent's subtree at the path being diffed.
#[derive(Clone, Copy)]
enum Old {
    /// The parent has no tree at this path: everything here is new.
    Absent,
    Tree(gix_hash::ObjectId),
}

/// Entries of a tree, or `None` when the tree is not local (recorded in
/// `missing` by the caller).
fn tree_entries(
    repo: &gix::Repository,
    oid: &gix_hash::oid,
    buf: &mut Vec<u8>,
) -> Result<Option<Vec<Entry>>, GitError> {
    let kind = repo.object_hash();
    let Some(obj) = repo.objects.try_find(oid, buf).map_err(GitError::Gix)? else {
        return Ok(None);
    };
    if obj.kind != ObjKind::Tree {
        return Ok(Some(Vec::new()));
    }
    let mut out = Vec::new();
    for e in gix_object::TreeRefIter::from_bytes(obj.data, kind) {
        let e = e.map_err(ge)?;
        out.push((e.mode, e.filename.to_vec(), e.oid.to_owned()));
    }
    Ok(Some(out))
}

/// Add to `set` every object under `new_tree` that is not present at the same
/// path with the same oid in *all* `olds` (parent subtrees). Subtrees equal to
/// a parent's are skipped whole. When a parent subtree is not local it is
/// pushed to `missing` and this subtree is deferred to the next round (the
/// retry re-walks from the root, which is cheap: everything above is local by
/// then), so each round faults exactly one tree level along the changed paths.
#[allow(clippy::too_many_arguments)]
fn diff_tree_new_objects(
    repo: &gix::Repository,
    new_tree: gix_hash::ObjectId,
    olds: &[Old],
    filter: &PackFilter,
    depth: usize,
    set: &mut HashSet<gix_hash::ObjectId>,
    missing: &mut Vec<gix_hash::ObjectId>,
    buf: &mut Vec<u8>,
) -> Result<(), GitError> {
    let Some(new_entries) = tree_entries(repo, &new_tree, buf)? else {
        // The new tree itself is not local (a want into the base).
        missing.push(new_tree);
        return Ok(());
    };
    let mut old_maps: Vec<HashMap<Vec<u8>, (gix_object::tree::EntryMode, gix_hash::ObjectId)>> =
        Vec::with_capacity(olds.len());
    let mut deferred = false;
    for o in olds {
        match o {
            Old::Absent => old_maps.push(HashMap::new()),
            Old::Tree(oid) => {
                if let Some(entries) = tree_entries(repo, oid, buf)? {
                    old_maps.push(entries.into_iter().map(|(m, n, o)| (n, (m, o))).collect());
                } else {
                    missing.push(*oid);
                    deferred = true;
                }
            }
        }
    }
    if deferred {
        // Come back once the parent subtree(s) are local; undo the insert so
        // the retry descends again.
        set.remove(&new_tree);
        return Ok(());
    }
    for (mode, name, oid) in new_entries {
        if mode.is_commit() {
            continue; // gitlink
        }
        // Unchanged relative to every parent → the client has it.
        if !old_maps.is_empty()
            && old_maps
                .iter()
                .all(|m| m.get(&name).is_some_and(|(_, o)| *o == oid))
        {
            continue;
        }
        if mode.is_tree() {
            if let PackFilter::Tree(max) = filter
                && depth + 1 > *max
            {
                set.insert(oid);
                continue;
            }
            if set.insert(oid) {
                let sub_olds: Vec<Old> = old_maps
                    .iter()
                    .map(|m| match m.get(&name) {
                        Some((om, o)) if om.is_tree() => Old::Tree(*o),
                        _ => Old::Absent,
                    })
                    .collect();
                diff_tree_new_objects(repo, oid, &sub_olds, filter, depth + 1, set, missing, buf)?;
            }
        } else {
            match filter {
                PackFilter::BlobNone => continue,
                PackFilter::BlobLimit(limit) => {
                    if let Ok(Some(h)) = repo.objects.try_header(&oid)
                        && h.size > *limit
                    {
                        continue;
                    }
                }
                _ => {}
            }
            set.insert(oid);
        }
    }
    Ok(())
}

#[cfg(test)]
mod frozen_source_tests {
    use super::*;
    use gix_pack::Find as _;

    fn git(dir: &std::path::Path, args: &[&str]) -> String {
        let out = std::process::Command::new("git")
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

    /// The mechanism behind prod's "same object appears twice" (2026-08-21): a
    /// `(pack id, offset)` taken while one pack set is on disk, resolved after a
    /// repack/midx rewrite reassigned the ids. The frozen source returns the
    /// original object's bytes (its pack stays pinned and its snapshot does not
    /// move); a plain handle — after the lazy load that a miss triggers — hands
    /// out whatever now lives at that id, or nothing.
    #[test]
    fn locations_survive_a_midx_rewrite_under_the_frozen_source() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        git(dir, &["init", "-q", "--bare"]);
        // Two packs with distinct content, newest first in gix's load order.
        let mut blobs = Vec::new();
        for (i, words) in ["one pack", "two pack"].iter().enumerate() {
            use std::io::Write;

            let content = format!("{words} {}\n", "x".repeat(300 + i * 50));
            let oid = {
                use std::io::Write;

                let mut c = std::process::Command::new("git")
                    .arg("-C")
                    .arg(dir)
                    .args(["hash-object", "-w", "--stdin"])
                    .stdin(std::process::Stdio::piped())
                    .stdout(std::process::Stdio::piped())
                    .spawn()
                    .unwrap();

                c.stdin
                    .take()
                    .unwrap()
                    .write_all(content.as_bytes())
                    .unwrap();
                String::from_utf8(c.wait_with_output().unwrap().stdout)
                    .unwrap()
                    .trim()
                    .to_string()
            };
            // Pack exactly this blob and drop the loose copy.
            let mut c = std::process::Command::new("git")
                .arg("-C")
                .arg(dir)
                .args(["pack-objects", "-q", "objects/pack/pack"])
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::piped())
                .spawn()
                .unwrap();

            c.stdin
                .take()
                .unwrap()
                .write_all(format!("{oid}\n").as_bytes())
                .unwrap();
            assert!(c.wait_with_output().unwrap().status.success());
            git(dir, &["prune-packed"]);
            std::thread::sleep(std::time::Duration::from_millis(20)); // distinct mtimes = deterministic load order
            blobs.push((
                gix_hash::ObjectId::from_hex(oid.as_bytes()).unwrap(),
                content,
            ));
        }
        // Serving copies carry a multi-pack-index (maintain_midx): ids are positions in it.
        git(dir, &["multi-pack-index", "write"]);
        let store = std::sync::Arc::new(
            gix_odb::Store::at_opts(
                dir.join("objects"),
                &mut std::iter::empty(),
                gix_odb::store::init::Options::default(),
            )
            .unwrap(),
        );

        // Take locations of both objects through a frozen source and a plain handle,
        // and remember the raw entry bytes each location yields today.
        let frozen = frozen_pack_source(&store);
        let mut plain = store.to_handle_arc();
        plain.prevent_pack_unload();
        let mut buf = Vec::new();
        let before: Vec<_> = blobs
            .iter()
            .map(|(oid, _)| {
                let loc = frozen.location_by_oid(oid, &mut buf).expect("in a pack");
                let data = frozen.entry_by_location(&loc).expect("entry").data;
                (*oid, loc, data)
            })
            .collect();
        let plain_before: Vec<_> = blobs
            .iter()
            .map(|(oid, _)| {
                let loc = plain.location_by_oid(oid, &mut buf).expect("in a pack");
                let data = plain.entry_by_location(&loc).expect("entry").data;
                (*oid, loc, data)
            })
            .collect();
        assert_ne!(
            before[0].2, before[1].2,
            "two different objects, two different entries"
        );

        // The large repository changes as the SSD host does: a NEW pack lands and the
        // multi-pack-index is rewritten in place. Inside a midx a pack id is its
        // position in the midx's (name-sorted) pack list, so a new pack whose name
        // sorts before the old ones shifts every id by one while the midx keeps its
        // slot (same path). Keep adding packs until that happens.
        let mut shifted = false;
        for i in 0..24 {
            use std::io::Write;

            let content = format!("later pack {i} {}\n", "y".repeat(200 + i));
            let oid = {
                use std::io::Write;

                let mut c = std::process::Command::new("git")
                    .arg("-C")
                    .arg(dir)
                    .args(["hash-object", "-w", "--stdin"])
                    .stdin(std::process::Stdio::piped())
                    .stdout(std::process::Stdio::piped())
                    .spawn()
                    .unwrap();

                c.stdin
                    .take()
                    .unwrap()
                    .write_all(content.as_bytes())
                    .unwrap();
                String::from_utf8(c.wait_with_output().unwrap().stdout)
                    .unwrap()
                    .trim()
                    .to_string()
            };
            let mut c = std::process::Command::new("git")
                .arg("-C")
                .arg(dir)
                .args(["pack-objects", "-q", "objects/pack/pack"])
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::piped())
                .spawn()
                .unwrap();

            c.stdin
                .take()
                .unwrap()
                .write_all(format!("{oid}\n").as_bytes())
                .unwrap();
            let name = String::from_utf8(c.wait_with_output().unwrap().stdout)
                .unwrap()
                .trim()
                .to_string();
            git(dir, &["prune-packed"]);
            std::thread::sleep(std::time::Duration::from_millis(20));
            git(dir, &["multi-pack-index", "write"]);
            let first_old = {
                let mut names: Vec<String> = blobs.iter().map(|_| String::new()).collect();
                names.clear();
                for e in std::fs::read_dir(dir.join("objects/pack")).unwrap() {
                    let p = e.unwrap().path();
                    if p.extension().is_some_and(|x| x == "idx") {
                        names.push(p.file_name().unwrap().to_string_lossy().to_string());
                    }
                }
                names.sort();
                names[0].clone()
            };
            if first_old.contains(&name) {
                shifted = true;
                break;
            }
        }
        assert!(
            shifted,
            "could not produce a pack that sorts first (24 tries)"
        );
        // A lookup miss is what lets a handle notice the disk (lazy index load).
        let ghost =
            gix_hash::ObjectId::from_hex(b"0123456789abcdef0123456789abcdef01234567").unwrap();
        let _ = gix_object::FindHeader::try_header(&frozen, &ghost);
        let _ = gix_object::FindHeader::try_header(&plain, &ghost);

        // Frozen: every old location still yields exactly the bytes it did.
        for (oid, loc, data) in &before {
            let now = frozen
                .entry_by_location(loc)
                .expect("pinned pack, frozen snapshot")
                .data;
            assert_eq!(
                &now, data,
                "frozen source must return the counted object's entry for {oid}"
            );
        }
        // (A plain handle happens to stay correct in this scenario too — gix moves a
        // changed midx to a new slot and keeps stable indices for pinned handles —
        // so the production failure involves more than this rewrite; the frozen
        // source removes the whole class regardless: nothing observed after the
        // snapshot can change what an id means.)
        let _ = plain_before;
    }
}
