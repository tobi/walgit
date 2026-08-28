//! WAL: index (`manifest.pb`), immutable entries, linearizable publish via CAS,
//! catch-up/materialize of local repos, ref snapshots. See AGENTS.md §2 and AGENTS.md §2.

mod checkpoint;
mod error;
mod handle;
pub mod lockwait;
mod log_reader;
pub mod progress;
mod publish;
mod registry;
pub mod remote;
pub mod snapshot;
mod state;
mod store_proto;
mod sync;
pub mod tasks;

pub use checkpoint::{CheckpointTrigger, checkpoint_due};
pub use error::{CoordError, RefError, WalError};
pub use handle::{ObjectAccess, RepoHandle};
pub use progress::{Progress, Reporter};
pub use publish::PublishResult;
pub use registry::{EvictReport, Registry};
pub use remote::{BlockCache, RemotePacks};
pub use snapshot::{Snapshot, materialize_at, snapshot_at};
pub use sync::{PackPlan, ReadGuard, SyncLevel};
pub use tasks::{Begin, TaskHandle, TaskRecord, Tasks};

/// The tier-2 **base packs** of a manifest: tier 2, not a derived history pack
/// (D18). Exactly one after `compact --base`; several after `import --direct`
/// of a multi-pack set (large-repository measurement: 11, the 32 GB base among 5 MB
/// packs — picking "max seq" there chose a 5 MB pack as the base).
pub fn base_packs(m: &walgit_proto::v1::Manifest) -> Vec<&walgit_proto::v1::PackRef> {
    m.packs
        .iter()
        .filter(|p| p.tier == 2 && p.kind != walgit_proto::v1::PackKind::History as i32)
        .collect()
}

/// The base pack proper: the biggest tier-2 non-history pack.
pub fn base_pack(m: &walgit_proto::v1::Manifest) -> Option<&walgit_proto::v1::PackRef> {
    base_packs(m).into_iter().max_by_key(|p| p.pack_size)
}
