//! Object access for repositories whose pack set does not fit the local cache
//! (tmpfs vs. a 32 GB monorepo pack): the pack **indexes** are
//! downloaded locally (`<repo>/remote-idx/<checksum>.idx`, ~6% of the pack),
//! the pack **data** stays in the object store and is read with range GETs
//! through a process-wide block cache (1 MiB blocks, LRU by bytes). Decoded
//! objects (after delta resolution) sit in a per-pack-set LRU keyed by pack
//! offset so delta bases are reused across chains.
//!
//! This is AGENTS.md §2.4 / AGENTS.md §2.4: "a pack storage layer able to serve directly
//! from GCS by range reads with a local block cache". It is used by the web
//! API (`Need::Objects` on a too-large repo) which faults the objects a git
//! command will touch into the local loose store and then runs the command
//! unchanged; upload-pack does not use it (clones go through bundle-uri).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use gix_pack::data::entry::Header;
use tracing::Instrument;
use walgit_proto::v1::{Manifest, PackRef};
use walgit_store::{GetOptions, GetResult, ObjectStore, Prefixed};

use crate::error::WalError;
use crate::progress::Reporter;

pub const BLOCK_SIZE: u64 = 1024 * 1024;

/// Process-wide cache of pack data blocks (all repos, all packs). Misses are
/// single-flight per block (`moka::future::Cache::try_get_with`).
pub struct BlockCache {
    cache: moka::future::Cache<(Arc<str>, u64), Bytes>,
    pub range_reads: AtomicU64,
    pub bytes_read: AtomicU64,
}

impl BlockCache {
    pub fn new(max_bytes: u64) -> Arc<Self> {
        Arc::new(BlockCache {
            cache: moka::future::Cache::builder()
                .max_capacity(max_bytes.max(BLOCK_SIZE * 4))
                .weigher(|_k: &(Arc<str>, u64), v: &Bytes| {
                    u32::try_from(v.len().max(1)).unwrap_or(u32::MAX)
                })
                .build(),
            range_reads: AtomicU64::new(0),
            bytes_read: AtomicU64::new(0),
        })
    }

    /// Fetch one block (`[n*BLOCK_SIZE, min((n+1)*BLOCK_SIZE, size))`) of the
    /// object `key` in `store`. `cache_key` must be globally unique (full
    /// store key including the repo prefix).
    async fn block(
        &self,
        store: &Prefixed,
        repo: &str,
        cache_key: &Arc<str>,
        key: &str,
        n: u64,
        size: u64,
    ) -> Result<Bytes, WalError> {
        let start = n * BLOCK_SIZE;
        let end = (start + BLOCK_SIZE).min(size);
        if start >= end {
            return Ok(Bytes::new());
        }
        let store = store.clone();
        let key = key.to_string();
        let reads = &self.range_reads;
        let bytes_read = &self.bytes_read;
        let hit = self.cache.contains_key(&(cache_key.clone(), n));
        let repo = repo.to_string();
        let repo_for_span = repo.clone();
        let key_for_span = key.clone();
        if hit {
            metrics::counter!("walgit_remote_block_cache_hits_total").increment(1);
        } else {
            metrics::counter!("walgit_remote_block_cache_misses_total").increment(1);
        }
        self.cache
            .try_get_with((cache_key.clone(), n), async move {
                reads.fetch_add(1, Ordering::Relaxed);
                let res = store
                    .get(&key, GetOptions { range: Some(start..end), ..Default::default() })
                    .await?;
                let body = match res {
                    GetResult::Object { body, .. } => body,
                    GetResult::NotModified { .. } => {
                        return Err(WalError::Corrupt(format!("unexpected 304 for {key}")));
                    }
                };
                let b = walgit_store::util::collect(body, usize::try_from(end - start).map_err(|e| WalError::Corrupt(e.to_string()))?).await?;
                if b.len() as u64 != end - start {
                    return Err(WalError::Corrupt(format!(
                        "short range read for {key}: {start}..{end} got {}",
                        b.len()
                    )));
                }
                bytes_read.fetch_add(b.len() as u64, Ordering::Relaxed);
                metrics::counter!("walgit_remote_range_reads_total", "repo" => repo.clone()).increment(1);
                metrics::counter!("walgit_remote_bytes_total", "repo" => repo.clone()).increment(b.len() as u64);
                Ok::<Bytes, WalError>(b)
            }.instrument(tracing::debug_span!("remote.read", repo = %repo_for_span, key = %key_for_span, block = n, bytes = end - start, cache_hit = hit)))
            .await
            .map_err(|e: Arc<WalError>| WalError::Corrupt(format!("remote pack block: {e}")))
    }

    pub fn stats(&self) -> (u64, u64, u64) {
        (
            self.range_reads.load(Ordering::Relaxed),
            self.bytes_read.load(Ordering::Relaxed),
            self.cache.weighted_size(),
        )
    }
}

/// A decoded object.
pub struct Obj {
    pub kind: gix_object::Kind,
    pub data: Bytes,
}

struct RemotePack {
    checksum: String,
    key: String,
    cache_key: Arc<str>,
    size: u64,
    idx: gix_pack::index::File,
}

/// Object reader over the live pack set of one manifest revision.
pub struct RemotePacks {
    store: Prefixed,
    packs: Vec<RemotePack>,
    blocks: Arc<BlockCache>,
    objects: moka::sync::Cache<(usize, u64), Arc<Obj>>,
    hash: gix_hash::Kind,
    pub revision: u64,
    pub objects_decoded: AtomicU64,
    /// `<owner>/<repo>` for spans/metrics (from the store prefix `repos/<o>/<r>/`).
    repo: String,
}

/// Where `<checksum>.idx` files for remote access live inside the local repo.
pub fn idx_dir(repo_dir: &std::path::Path) -> std::path::PathBuf {
    repo_dir.join("remote-idx")
}

impl RemotePacks {
    /// Download (once) and open the index of every live pack. Emits progress.
    pub async fn open(
        store: Prefixed,
        manifest: &Manifest,
        repo_dir: &std::path::Path,
        hash: gix_hash::Kind,
        blocks: Arc<BlockCache>,
        object_cache_bytes: u64,
        reporter: &Reporter,
    ) -> Result<Self, WalError> {
        let dir = idx_dir(repo_dir);
        tokio::fs::create_dir_all(&dir).await?;
        // History packs are always local copies: no remote index for them.
        let manifest = {
            let mut m = manifest.clone();
            m.packs
                .retain(|p| p.kind != walgit_proto::v1::PackKind::History as i32);
            m
        };
        let manifest = &manifest;
        let total_idx: u64 = manifest.packs.iter().map(|p| p.idx_size).sum();
        let live: std::collections::HashSet<&str> =
            manifest.packs.iter().map(|p| p.checksum.as_str()).collect();
        // Drop indexes of packs no longer live (compaction superseded them).
        if let Ok(mut rd) = tokio::fs::read_dir(&dir).await {
            while let Ok(Some(e)) = rd.next_entry().await {
                let name = e.file_name().to_string_lossy().to_string();
                if let Some(stem) = name.strip_suffix(".idx")
                    && !live.contains(stem)
                {
                    let _ = tokio::fs::remove_file(e.path()).await;
                }
            }
        }
        // An index the Serve level already installed (linked/local base) is
        // the same bytes: hard-link it instead of downloading 2 GB twice onto
        // the same tmpfs (and vice versa, see sync::link_and_install_pack).
        for p in &manifest.packs {
            let dest = dir.join(format!("{}.idx", p.checksum));
            if dest.exists() {
                continue;
            }
            let installed = repo_dir
                .join("objects")
                .join("pack")
                .join(format!("pack-{}.idx", p.checksum));
            if installed.is_file() && std::fs::hard_link(&installed, &dest).is_err() {
                let _ = std::fs::copy(&installed, &dest);
            }
        }
        let done = Arc::new(AtomicU64::new(0));
        let missing: Vec<&PackRef> = manifest
            .packs
            .iter()
            .filter(|p| !dir.join(format!("{}.idx", p.checksum)).exists())
            .collect();
        if !missing.is_empty() {
            reporter.notice(format!(
                "Pack set is {} across {} pack(s): too large for this instance's disk, reading objects straight from the WAL. Downloading {} pack index(es) ({}).",
                human_bytes(manifest.packs.iter().map(|p| p.pack_size).sum()),
                manifest.packs.len(),
                missing.len(),
                human_bytes(missing.iter().map(|p| p.idx_size).sum()),
            ));
            let throttle = Arc::new(crate::progress::Throttle::new(
                std::time::Duration::from_millis(250),
            ));
            let sem = Arc::new(tokio::sync::Semaphore::new(4));
            let mut tasks = Vec::new();
            for p in missing {
                let sem = sem.clone();
                let store = store.clone();
                let dir = dir.clone();
                let p = p.clone();
                let done = done.clone();
                let reporter = reporter.clone();
                let throttle = throttle.clone();
                tasks.push(tokio::spawn(async move {
                    let _permit = sem
                        .acquire()
                        .await
                        .map_err(|e| WalError::Corrupt(e.to_string()))?;
                    let tmp = dir.join(format!("{}.idx.tmp", p.checksum));
                    let dest = dir.join(format!("{}.idx", p.checksum));
                    let cb = |delta: u64, _t: u64| {
                        let all = done.fetch_add(delta, Ordering::Relaxed) + delta;
                        if throttle.tick(false) {
                            reporter.bar("Downloading pack indexes", all, Some(total_idx), "bytes");
                        }
                    };
                    crate::sync::download_object(
                        &store,
                        &walgit_proto::keys::idx_key(&p.checksum),
                        &tmp,
                        (p.idx_size > 0).then_some(p.idx_size),
                        Some(&cb),
                    )
                    .await?;
                    tokio::fs::rename(&tmp, &dest).await?;
                    Ok::<(), WalError>(())
                }));
            }
            for t in tasks {
                t.await.map_err(|e| WalError::Corrupt(e.to_string()))??;
            }
            reporter.bar(
                "Downloading pack indexes",
                total_idx,
                Some(total_idx),
                "bytes",
            );
        }
        let mut packs = Vec::with_capacity(manifest.packs.len());
        for p in &manifest.packs {
            let path = dir.join(format!("{}.idx", p.checksum));
            let idx = tokio::task::spawn_blocking({
                let path = path.clone();
                move || gix_pack::index::File::at(&path, hash)
            })
            .await
            .map_err(|e| WalError::Corrupt(e.to_string()))?
            .map_err(|e| WalError::Corrupt(format!("open pack index {}: {e}", path.display())))?;
            let key = walgit_proto::keys::pack_key(&p.checksum);
            let size = if p.pack_size > 0 {
                p.pack_size
            } else {
                store.head(&key).await?.map_or(0, |m| m.size)
            };
            packs.push(RemotePack {
                checksum: p.checksum.clone(),
                cache_key: Arc::from(format!("{}{}", store.prefix(), key)),
                key,
                size,
                idx,
            });
        }
        let repo = store
            .prefix()
            .trim_end_matches('/')
            .trim_start_matches("repos/")
            .to_string();
        Ok(RemotePacks {
            repo,
            store,
            packs,
            blocks,
            objects: moka::sync::Cache::builder()
                .max_capacity(object_cache_bytes.max(8 * 1024 * 1024))
                .weigher(|_k: &(usize, u64), v: &Arc<Obj>| {
                    u32::try_from(v.data.len().saturating_add(64)).unwrap_or(u32::MAX)
                })
                .build(),
            hash,
            revision: manifest.revision,
            objects_decoded: AtomicU64::new(0),
        })
    }

    pub fn hash(&self) -> gix_hash::Kind {
        self.hash
    }
    pub fn pack_count(&self) -> usize {
        self.packs.len()
    }
    pub fn pack_checksums(&self) -> Vec<&str> {
        self.packs.iter().map(|p| p.checksum.as_str()).collect()
    }
    pub fn total_objects(&self) -> u64 {
        self.packs
            .iter()
            .map(|p| u64::from(p.idx.num_objects()))
            .sum()
    }

    /// Locate an object: (pack index, pack offset).
    pub fn locate(&self, oid: &gix_hash::oid) -> Option<(usize, u64)> {
        for (i, p) in self.packs.iter().enumerate() {
            if let Some(ix) = p.idx.lookup(oid) {
                return Some((i, p.idx.pack_offset_at_index(ix)));
            }
        }
        None
    }
    pub fn contains(&self, oid: &gix_hash::oid) -> bool {
        self.locate(oid).is_some()
    }

    /// Unique-prefix lookup across all packs. `Ok(None)` = no match,
    /// `Err(Ambiguous)` = several.
    pub fn lookup_prefix(
        &self,
        prefix: gix_hash::Prefix,
    ) -> Result<Option<gix_hash::ObjectId>, Ambiguous> {
        let mut found: Option<gix_hash::ObjectId> = None;
        for p in &self.packs {
            match p.idx.lookup_prefix(prefix, None) {
                None => {}
                Some(Ok(ix)) => {
                    let oid = p.idx.oid_at_index(ix).to_owned();
                    if let Some(f) = &found {
                        if *f != oid {
                            return Err(Ambiguous);
                        }
                    } else {
                        found = Some(oid);
                    }
                }
                Some(Err(())) => return Err(Ambiguous),
            }
        }
        Ok(found)
    }

    /// Kind + decompressed size without materializing the object (deltas are
    /// inflated, bases are not).
    pub async fn header(
        &self,
        oid: &gix_hash::oid,
    ) -> Result<Option<(gix_object::Kind, u64)>, WalError> {
        let Some((pi, off)) = self.locate(oid) else {
            return Ok(None);
        };
        let mut cur = (pi, off);
        let mut size: Option<u64> = None;
        for _ in 0..256 {
            if let Some(o) = self.objects.get(&cur) {
                return Ok(Some((o.kind, size.unwrap_or(o.data.len() as u64))));
            }
            let (entry, _) = self.read_entry_header(cur.0, cur.1).await?;
            match entry.header {
                Header::Blob | Header::Tree | Header::Commit | Header::Tag => {
                    let kind = entry
                        .header
                        .as_kind()
                        .ok_or_else(|| WalError::Corrupt("expected base object kind".into()))?;
                    return Ok(Some((kind, size.unwrap_or(entry.decompressed_size))));
                }
                Header::OfsDelta { base_distance } => {
                    if size.is_none() {
                        let delta = self.inflate(cur.0, &entry).await?;
                        size = Some(delta_result_size(&delta)?);
                    }
                    cur = (cur.0, entry.pack_offset() - base_distance);
                }
                Header::RefDelta { base_id } => {
                    if size.is_none() {
                        let delta = self.inflate(cur.0, &entry).await?;
                        size = Some(delta_result_size(&delta)?);
                    }
                    cur = self.locate(&base_id).ok_or_else(|| {
                        WalError::Corrupt(format!("ref-delta base {base_id} missing from pack set"))
                    })?;
                }
            }
        }
        Err(WalError::Corrupt("delta chain too deep".into()))
    }

    /// Read and decode one object (delta chains resolved, cached).
    pub async fn find(&self, oid: &gix_hash::oid) -> Result<Option<Arc<Obj>>, WalError> {
        let Some((pi, off)) = self.locate(oid) else {
            return Ok(None);
        };
        Ok(Some(self.decode(pi, off).await?))
    }

    async fn decode(&self, pi: usize, off: u64) -> Result<Arc<Obj>, WalError> {
        if let Some(o) = self.objects.get(&(pi, off)) {
            return Ok(o);
        }
        let span = tracing::debug_span!("remote.decode", repo = %self.repo, pack = %self.packs.get(pi).ok_or_else(|| WalError::Corrupt("pack index out of bounds".into()))?.checksum, offset = off, oid_kind = tracing::field::Empty, chain = tracing::field::Empty);
        let r = self.decode_inner(pi, off).instrument(span.clone()).await;
        if let Ok((o, chain)) = &r {
            span.record("oid_kind", format!("{:?}", o.kind).to_lowercase());
            span.record("chain", *chain);
            metrics::histogram!("walgit_remote_delta_chain")
                .record(f64::from(u32::try_from(*chain).unwrap_or(u32::MAX)));
        }
        r.map(|(o, _)| o)
    }

    async fn decode_inner(&self, pi: usize, off: u64) -> Result<(Arc<Obj>, usize), WalError> {
        let mut chain: Vec<((usize, u64), Vec<u8>)> = Vec::new();
        let mut cur = (pi, off);
        let base: Arc<Obj> = loop {
            if let Some(o) = self.objects.get(&cur) {
                break o;
            }
            if chain.len() > 4096 {
                return Err(WalError::Corrupt("delta chain too deep".into()));
            }
            let (entry, head) = self.read_entry_header(cur.0, cur.1).await?;
            let data = self.inflate_with_head(cur.0, &entry, head).await?;
            match entry.header {
                Header::Blob | Header::Tree | Header::Commit | Header::Tag => {
                    let o = Arc::new(Obj {
                        kind: entry
                            .header
                            .as_kind()
                            .ok_or_else(|| WalError::Corrupt("expected base object kind".into()))?,
                        data: Bytes::from(data),
                    });
                    self.objects.insert(cur, o.clone());
                    self.objects_decoded.fetch_add(1, Ordering::Relaxed);
                    break o;
                }
                Header::OfsDelta { base_distance } => {
                    chain.push((cur, data));
                    cur = (cur.0, entry.pack_offset() - base_distance);
                }
                Header::RefDelta { base_id } => {
                    chain.push((cur, data));
                    cur = self.locate(&base_id).ok_or_else(|| {
                        WalError::Corrupt(format!("ref-delta base {base_id} missing from pack set"))
                    })?;
                }
            }
        };
        let mut base = base;
        let depth = chain.len();
        for (at, delta) in chain.into_iter().rev() {
            let out = apply_delta(&base.data, &delta)
                .map_err(|m| WalError::Corrupt(format!("delta at {}:{}: {m}", at.0, at.1)))?;
            let o = Arc::new(Obj {
                kind: base.kind,
                data: Bytes::from(out),
            });
            self.objects.insert(at, o.clone());
            self.objects_decoded.fetch_add(1, Ordering::Relaxed);
            base = o;
        }
        Ok((base, depth))
    }

    /// Bytes `[off, off+len)` of pack `pi`, assembled from cached blocks
    /// (missing blocks fetched concurrently).
    async fn read_at(&self, pi: usize, off: u64, len: u64) -> Result<Bytes, WalError> {
        let p = &self
            .packs
            .get(pi)
            .ok_or_else(|| WalError::Corrupt("pack index out of bounds".into()))?;
        let end = off.saturating_add(len).min(p.size);
        if off >= end {
            return Ok(Bytes::new());
        }
        let first = off / BLOCK_SIZE;
        let last = (end - 1) / BLOCK_SIZE;
        let futs = (first..=last).map(|n| {
            self.blocks
                .block(&self.store, &self.repo, &p.cache_key, &p.key, n, p.size)
        });
        let blocks = futures::future::try_join_all(futs).await?;
        if blocks.len() == 1 {
            let b = blocks
                .first()
                .ok_or_else(|| WalError::Corrupt("missing range block".into()))?;
            let s = usize::try_from(off - first * BLOCK_SIZE)
                .map_err(|e| WalError::Corrupt(e.to_string()))?;
            let e = usize::try_from(end - first * BLOCK_SIZE)
                .map_err(|e| WalError::Corrupt(e.to_string()))?;
            return Ok(b.slice_ref(
                b.get(s..e)
                    .ok_or_else(|| WalError::Corrupt("short range block".into()))?,
            ));
        }
        let mut out = Vec::with_capacity(
            usize::try_from(end - off).map_err(|e| WalError::Corrupt(e.to_string()))?,
        );
        for (i, b) in blocks.iter().enumerate() {
            let bstart = (first + i as u64) * BLOCK_SIZE;
            let s = usize::try_from(off.saturating_sub(bstart))
                .map_err(|e| WalError::Corrupt(e.to_string()))?;
            let e = usize::try_from((end - bstart).min(b.len() as u64))
                .map_err(|e| WalError::Corrupt(e.to_string()))?;
            out.extend_from_slice(
                b.get(s..e)
                    .ok_or_else(|| WalError::Corrupt("short range block".into()))?,
            );
        }
        Ok(Bytes::from(out))
    }

    /// Parse the entry header at `off` (reads up to 64 bytes; returns the
    /// bytes so the caller can start inflating without a second read).
    async fn read_entry_header(
        &self,
        pi: usize,
        off: u64,
    ) -> Result<(gix_pack::data::Entry, Bytes), WalError> {
        let head = self.read_at(pi, off, 64).await?;
        let entry = gix_pack::data::Entry::from_bytes(&head, off, self.hash)
            .map_err(|e| WalError::Corrupt(format!("pack entry at {off}: {e}")))?;
        Ok((entry, head))
    }

    async fn inflate(&self, pi: usize, entry: &gix_pack::data::Entry) -> Result<Vec<u8>, WalError> {
        let head = self.read_at(pi, entry.pack_offset(), 64).await?;
        self.inflate_with_head(pi, entry, head).await
    }

    /// Inflate the entry's compressed payload, pulling blocks as zlib asks for
    /// more input. The compressed length is unknown (no `.rev`), so blocks that
    /// plausibly cover it (decompressed size is an upper bound for nearly all
    /// git content) are requested concurrently up front.
    async fn inflate_with_head(
        &self,
        pi: usize,
        entry: &gix_pack::data::Entry,
        head: Bytes,
    ) -> Result<Vec<u8>, WalError> {
        use flate2::{Decompress, FlushDecompress, Status};
        let p = &self
            .packs
            .get(pi)
            .ok_or_else(|| WalError::Corrupt("pack index out of bounds".into()))?;
        let size = usize::try_from(entry.decompressed_size)
            .map_err(|e| WalError::Corrupt(e.to_string()))?;
        let data_off = entry.data_offset;
        let header_len = usize::try_from(data_off - entry.pack_offset())
            .map_err(|e| WalError::Corrupt(e.to_string()))?;
        // Prefetch: blocks from data_off through data_off + size (+ slack), bounded.
        {
            let guess_end =
                (data_off + entry.decompressed_size.min(64 * BLOCK_SIZE) + 64).min(p.size);
            if guess_end > data_off {
                let first = data_off / BLOCK_SIZE;
                let last = (guess_end - 1) / BLOCK_SIZE;
                if last > first {
                    let futs = (first..=last).map(|n| {
                        self.blocks
                            .block(&self.store, &self.repo, &p.cache_key, &p.key, n, p.size)
                    });
                    futures::future::try_join_all(futs).await?;
                }
            }
        }
        let mut out: Vec<u8> = Vec::with_capacity(size);
        let mut z = Decompress::new(true);
        // First feed: the rest of the 64-byte head after the header.
        let mut pos = data_off;
        let mut chunk: Bytes = if head.len() > header_len {
            head.slice(header_len..)
        } else {
            Bytes::new()
        };
        loop {
            if chunk.is_empty() {
                if pos >= p.size {
                    return Err(WalError::Corrupt(format!(
                        "pack entry at {} truncated",
                        entry.pack_offset()
                    )));
                }
                let block_end = ((pos / BLOCK_SIZE) + 1) * BLOCK_SIZE;
                chunk = self.read_at(pi, pos, block_end.min(p.size) - pos).await?;
            }
            let before_in = z.total_in();
            out.reserve(size.saturating_sub(out.len()).max(1));
            let status = z
                .decompress_vec(&chunk, &mut out, FlushDecompress::None)
                .map_err(|e| {
                    WalError::Corrupt(format!(
                        "inflate pack entry at {}: {e}",
                        entry.pack_offset()
                    ))
                })?;
            let consumed = usize::try_from(z.total_in() - before_in)
                .map_err(|e| WalError::Corrupt(e.to_string()))?;
            pos += consumed as u64;
            chunk = chunk.slice(consumed..);
            if out.len() >= size || status == Status::StreamEnd {
                break;
            }
            if consumed == 0 && !chunk.is_empty() && status == Status::Ok {
                // Output buffer full but size not reached: should not happen; guard.
                if out.len() >= out.capacity() {
                    out.reserve(1024);
                }
            }
        }
        if out.len() != size {
            return Err(WalError::Corrupt(format!(
                "pack entry at {}: inflated {} bytes, expected {size}",
                entry.pack_offset(),
                out.len()
            )));
        }
        Ok(out)
    }
}

#[derive(Debug)]
pub struct Ambiguous;

fn varint(d: &[u8], mut i: usize) -> Result<(u64, usize), &'static str> {
    let mut shift = 0u32;
    let mut v = 0u64;
    loop {
        let b = *d.get(i).ok_or("delta header truncated")?;
        i += 1;
        v |= u64::from(b & 0x7f) << shift;
        shift += 7;
        if b & 0x80 == 0 {
            return Ok((v, i));
        }
        if shift > 63 {
            return Err("delta header overflow");
        }
    }
}

fn delta_result_size(delta: &[u8]) -> Result<u64, WalError> {
    let (_, i) = varint(delta, 0).map_err(|m| WalError::Corrupt(m.into()))?;
    let (res, _) = varint(delta, i).map_err(|m| WalError::Corrupt(m.into()))?;
    Ok(res)
}

/// Apply a git delta (`base` + `delta` instructions → result).
pub fn apply_delta(base: &[u8], delta: &[u8]) -> Result<Vec<u8>, &'static str> {
    let (base_size, i) = varint(delta, 0)?;
    if base_size != base.len() as u64 {
        return Err("delta base size mismatch");
    }
    let (res_size, mut i) = varint(delta, i)?;
    let mut out =
        Vec::with_capacity(usize::try_from(res_size).map_err(|_| "delta result too large")?);
    while let Some(&cmd) = delta.get(i) {
        i += 1;
        if cmd & 0x80 != 0 {
            let mut ofs: u64 = 0;
            let mut size: u64 = 0;
            let mut nb = |shift: u32| -> Result<u64, &'static str> {
                let b = *delta.get(i).ok_or("delta copy truncated")?;
                i += 1;
                Ok(u64::from(b) << shift)
            };
            if cmd & 0x01 != 0 {
                ofs |= nb(0)?;
            }
            if cmd & 0x02 != 0 {
                ofs |= nb(8)?;
            }
            if cmd & 0x04 != 0 {
                ofs |= nb(16)?;
            }
            if cmd & 0x08 != 0 {
                ofs |= nb(24)?;
            }
            if cmd & 0x10 != 0 {
                size |= nb(0)?;
            }
            if cmd & 0x20 != 0 {
                size |= nb(8)?;
            }
            if cmd & 0x40 != 0 {
                size |= nb(16)?;
            }
            if size == 0 {
                size = 0x10000;
            }
            let end = ofs.checked_add(size).ok_or("delta copy overflow")?;
            if end > base.len() as u64 {
                return Err("delta copy out of base bounds");
            }
            let start = usize::try_from(ofs).map_err(|_| "delta copy offset too large")?;
            let end = usize::try_from(end).map_err(|_| "delta copy end too large")?;
            out.extend_from_slice(
                base.get(start..end)
                    .ok_or("delta copy out of base bounds")?,
            );
        } else if cmd != 0 {
            let n = cmd as usize;
            let src = delta.get(i..i + n).ok_or("delta insert truncated")?;
            out.extend_from_slice(src);
            i += n;
        } else {
            return Err("delta command 0 is reserved");
        }
    }
    if out.len() as u64 != res_size {
        return Err("delta result size mismatch");
    }
    Ok(out)
}

#[expect(
    clippy::cast_precision_loss,
    reason = "Human-readable byte counts intentionally round to one decimal place"
)]
pub fn human_bytes(n: u64) -> String {
    const U: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut v = n as f64;
    let mut unit = "B";
    for next in U.iter().skip(1) {
        if v < 1024.0 {
            break;
        }
        v /= 1024.0;
        unit = next;
    }
    if unit == "B" {
        format!("{n} B")
    } else {
        format!("{v:.1} {unit}")
    }
}

/// [`walgit_git::ObjectFaulter`] over the remote reader: the gix upload-pack
/// engine asks for the base objects a tree diff needs (one tree level per
/// round); they are read by range in parallel and written into the local
/// loose store so the rest of the engine (and stock git tooling) sees them.
pub struct Faulter {
    packs: Arc<RemotePacks>,
    local: walgit_git::LocalRepo,
    faulted: AtomicU64,
    rounds: AtomicU64,
}

impl Faulter {
    pub fn new(packs: Arc<RemotePacks>, local: walgit_git::LocalRepo) -> Self {
        Faulter {
            packs,
            local,
            faulted: AtomicU64::new(0),
            rounds: AtomicU64::new(0),
        }
    }

    /// `(objects faulted, fault rounds)` so far.
    pub fn stats(&self) -> (u64, u64) {
        (
            self.faulted.load(Ordering::Relaxed),
            self.rounds.load(Ordering::Relaxed),
        )
    }
}

impl walgit_git::ObjectFaulter for Faulter {
    fn contains(&self, oid: &gix_hash::oid) -> bool {
        self.packs.contains(oid)
    }

    fn fault<'a>(
        &'a self,
        oids: &'a [gix_hash::ObjectId],
    ) -> futures::future::BoxFuture<'a, Result<usize, walgit_git::GitError>> {
        let span = tracing::info_span!(
            "remote.fault",
            oids = oids.len(),
            found = tracing::field::Empty
        );
        Box::pin(
            async move {
                const PAR: usize = 32;

                self.rounds.fetch_add(1, Ordering::Relaxed);

                let mut n = 0usize;
                for chunk in oids.chunks(PAR) {
                    let results =
                        futures::future::join_all(chunk.iter().map(|oid| self.packs.find(oid)))
                            .await;
                    for (oid, r) in chunk.iter().zip(results) {
                        match r {
                            Ok(Some(obj)) => {
                                self.local.write_loose_object(obj.kind, oid, &obj.data)?;
                                n += 1;
                            }
                            Ok(None) => {}
                            Err(e) => {
                                return Err(walgit_git::GitError::Protocol(format!(
                                    "remote read of {oid}: {e}"
                                )));
                            }
                        }
                    }
                }
                self.faulted.fetch_add(n as u64, Ordering::Relaxed);
                tracing::Span::current().record("found", n as u64);
                metrics::counter!("walgit_remote_faulted_objects_total").increment(n as u64);
                Ok(n)
            }
            .instrument(span),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delta_roundtrip_insert_and_copy() {
        let base = b"hello world, this is the base object";
        // header: base size, result size; then copy 0..5 from base, insert "!!", copy 5..12
        let mut d = vec![
            u8::try_from(base.len()).expect("test base is shorter than 256 bytes"),
            5 + 2 + 7,
        ];
        d.extend([0x90, 5]); // copy ofs=0 (no ofs bytes), size=5 (0x10 flag)
        d.extend([2, b'!', b'!']);
        d.extend([0x91, 5, 7]); // copy ofs=5 size=7
        let out = apply_delta(base, &d).unwrap();
        assert_eq!(out, b"hello!! world,");
    }
}
