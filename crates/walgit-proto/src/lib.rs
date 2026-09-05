//! Generated protobuf types for walgit's on-store formats.
//!
//! Schema lives in `proto/walgit/v1/wal.proto`; it is the contract between
//! every walgit instance and must only evolve backward-compatibly.

// Documentation in this module is emitted by prost, including enum helper prose.
#[allow(clippy::doc_markdown)]
pub mod v1 {
    include!(concat!(env!("OUT_DIR"), "/walgit.v1.rs"));
}

pub use prost;
pub use prost::Message;
pub use prost_types;

/// Current `Manifest.format_version`.
pub const WAL_FORMAT_VERSION: u32 = 1;

/// Repo-relative object keys. Everything is under `repos/<owner>/<repo>/`.
pub mod keys {
    /// Prefix for a repository, always with trailing slash.
    pub fn repo_prefix(owner: &str, name: &str) -> String {
        format!("repos/{owner}/{name}/")
    }
    pub const MANIFEST: &str = "manifest.pb";
    pub const LOG_DIR: &str = "log/";
    pub const WAL_DIR: &str = "wal/";
    pub const CHECKPOINTS_DIR: &str = "checkpoints/";
    pub const LEASES_DIR: &str = "leases/";
    pub const BUNDLES_DIR: &str = "bundles/";
    pub const BUNDLE_LIST: &str = "bundles/list.pb";
    /// Bucket-root prefix of maintainer heartbeats (not under a repo).
    pub const MAINTAIN_DIR: &str = "maintain/";
    pub fn maintainer_key(host: &str) -> String {
        format!("{MAINTAIN_DIR}{host}.pb")
    }
    pub const LFS_DIR: &str = "lfs/objects/";
    /// Per-repo connectivity audit result (`FsckReport`). Overwritten, not WAL.
    pub const FSCK: &str = "fsck.pb";
    pub const CATALOG: &str = "meta/repos.pb";
    /// Per-repo push policy (JSON). Not on the WAL; CAS'd independently.
    pub const POLICY: &str = "policy.json";

    pub fn policy_key(owner: &str, name: &str) -> String {
        format!("{}{POLICY}", repo_prefix(owner, name))
    }

    /// `log/<first_seq:016x>.pb`
    pub fn log_segment_key(first_seq: u64) -> String {
        format!("{LOG_DIR}{first_seq:016x}.pb")
    }
    pub fn pack_key(checksum_hex: &str) -> String {
        format!("{WAL_DIR}{checksum_hex}.pack")
    }
    pub fn idx_key(checksum_hex: &str) -> String {
        format!("{WAL_DIR}{checksum_hex}.idx")
    }
    pub fn rev_key(checksum_hex: &str) -> String {
        format!("{WAL_DIR}{checksum_hex}.rev")
    }
    pub fn bitmap_key(checksum_hex: &str) -> String {
        format!("{WAL_DIR}{checksum_hex}.bitmap")
    }
    pub fn commit_graph_key(checksum_hex: &str) -> String {
        format!("{WAL_DIR}{checksum_hex}.commit-graph")
    }
    pub fn checkpoint_dir(seq: u64) -> String {
        format!("{CHECKPOINTS_DIR}{seq:016x}/")
    }
    pub fn checkpoint_key(seq: u64) -> String {
        format!("{CHECKPOINTS_DIR}{seq:016x}/checkpoint.pb")
    }
    pub fn checkpoint_refs_key(seq: u64) -> String {
        format!("{CHECKPOINTS_DIR}{seq:016x}/refs.pb")
    }
    pub fn checkpoint_bundle_key(seq: u64, checksum_hex: &str) -> String {
        format!("{CHECKPOINTS_DIR}{seq:016x}/{checksum_hex}.bundle")
    }
    pub fn lease_key(name: &str) -> String {
        format!("{LEASES_DIR}{name}.pb")
    }
    /// Git LFS oid: 64 hex characters (sha256).
    pub fn lfs_oid_ok(oid: &str) -> bool {
        oid.len() == 64 && oid.bytes().all(|b| b.is_ascii_hexdigit())
    }

    pub fn lfs_key(oid: &str) -> String {
        let (aa, bb) = match (oid.get(..2), oid.get(2..4)) {
            (Some(a), Some(b)) => (a, b),
            _ => ("", ""),
        };
        format!("{LFS_DIR}{aa}/{bb}/{oid}")
    }
}

/// Length-prefixed framing for log objects: `uvarint(len) || LogEntry`.
/// Appendable objects grow by appending frames; readers stop at the first
/// incomplete trailing frame.
pub mod frame {
    use bytes::{Bytes, BytesMut};
    use prost::Message;

    use crate::v1::LogEntry;

    pub fn encode_entry(e: &LogEntry, out: &mut BytesMut) {
        let len = e.encoded_len();
        prost::encoding::encode_varint(len as u64, out);
        out.reserve(len);
        // BytesMut grows as needed; encode_raw has no fallible capacity check.
        e.encode_raw(out);
    }

    pub fn encode_entries<'a>(entries: impl IntoIterator<Item = &'a LogEntry>) -> Bytes {
        let mut b = BytesMut::new();
        for e in entries {
            encode_entry(e, &mut b);
        }
        b.freeze()
    }

    /// Decode all complete frames. Returns entries and the number of bytes
    /// consumed (a trailing partial frame is left unconsumed, not an error).
    pub fn decode_entries(buf: &[u8]) -> Result<(Vec<LogEntry>, usize), prost::DecodeError> {
        let mut out = Vec::new();
        let mut pos = 0usize;
        while let Some(mut probe) = buf.get(pos..) {
            let Ok(len) = prost::encoding::decode_varint(&mut probe) else {
                break;
            };
            let Ok(len) = usize::try_from(len) else {
                break;
            };
            let Some(frame) = probe.get(..len) else {
                break;
            };
            out.push(LogEntry::decode(frame)?);
            pos = buf.len() - probe.len() + len;
        }
        Ok((out, pos))
    }
}

/// Convert to/from prost timestamps.
pub mod time {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    pub fn now() -> prost_types::Timestamp {
        from_system(SystemTime::now())
    }
    pub fn from_system(t: SystemTime) -> prost_types::Timestamp {
        let d = t.duration_since(UNIX_EPOCH).unwrap_or_default();
        prost_types::Timestamp {
            seconds: i64::try_from(d.as_secs()).unwrap_or(i64::MAX),
            nanos: i32::try_from(d.subsec_nanos()).unwrap_or(999_999_999),
        }
    }
    pub fn to_system(t: &prost_types::Timestamp) -> SystemTime {
        UNIX_EPOCH
            + Duration::new(
                t.seconds.max(0).cast_unsigned(),
                t.nanos.max(0).cast_unsigned(),
            )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn roundtrip_manifest() {
        let m = v1::Manifest {
            format_version: WAL_FORMAT_VERSION,
            repo: "acme/monorepo".into(),
            object_format: "sha1".into(),
            head_seq: 3,
            ..Default::default()
        };
        let bytes = m.encode_to_vec();
        let back = v1::Manifest::decode(bytes.as_slice()).unwrap();
        assert_eq!(back, m);
    }
    #[test]
    fn keys() {
        assert_eq!(keys::log_segment_key(66), "log/0000000000000042.pb");
        assert_eq!(keys::pack_key("x"), "wal/x.pack");
        assert_eq!(
            keys::checkpoint_key(1),
            "checkpoints/0000000000000001/checkpoint.pb"
        );
        assert_eq!(keys::lfs_key("abcdef"), "lfs/objects/ab/cd/abcdef");
        assert!(!keys::lfs_oid_ok("ab"));
        assert!(!keys::lfs_oid_ok("abcdef"));
        assert!(keys::lfs_oid_ok(&"a".repeat(64)));
        assert_eq!(keys::lfs_key("ab"), "lfs/objects///ab");
    }
    #[test]
    fn frames_roundtrip_and_partial() {
        let e1 = v1::LogEntry {
            seq: 1,
            kind: v1::EntryKind::Push as i32,
            ..Default::default()
        };
        let e2 = v1::LogEntry {
            seq: 2,
            kind: v1::EntryKind::Compact as i32,
            supersedes: vec!["a".into(); 40],
            ..Default::default()
        };
        let all = frame::encode_entries([&e1, &e2]);
        let (got, used) = frame::decode_entries(&all).unwrap();
        assert_eq!(got, vec![e1.clone(), e2.clone()]);
        assert_eq!(used, all.len());
        // Truncated tail: only e1 decodes, consumed = e1 frame length.
        let cut = &all[..all.len() - 5];
        let (got, used) = frame::decode_entries(cut).unwrap();
        assert_eq!(got, vec![e1.clone()]);
        assert_eq!(used, frame::encode_entries([&e1]).len());
    }
}
