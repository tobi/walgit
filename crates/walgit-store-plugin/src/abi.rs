//! Checked Rust plugin boundary. Ownership and layouts are provided by `abi_stable`.
// These allowances apply only to generated ABI glue, not hand-written unsafe code.
#![allow(
    unsafe_code,
    non_local_definitions,
    clippy::used_underscore_binding,
    clippy::expl_impl_clone_on_copy
)]

use abi_stable::std_types::{RBox, ROption, RResult, RString, RVec};
use abi_stable::{
    StableAbi, library::RootModule, package_version_strings, sabi_trait, sabi_types::VersionStrings,
};

pub const MAX_FRAME: usize = 1024 * 1024;
pub const MAX_METADATA: usize = 16 * 1024 * 1024;

/// One owned, serially polled byte stream. Dropping it cancels without draining.
/// The SDK invokes poll off async workers. Errors contain serialized `WireError`.
#[sabi_trait]
pub trait StreamApi: Send {
    #[sabi(last_prefix_field)]
    fn poll(&mut self) -> RResult<ROption<RVec<u8>>, RString>;
}
pub type Stream = ROption<StreamApi_TO<'static, RBox<()>>>;

#[repr(C)]
#[derive(StableAbi, Default)]
pub struct Reply {
    pub metadata: RVec<u8>,
    pub body: Stream,
}

/// A concurrent endpoint, kept alive until its calls and response streams end.
/// Methods are synchronous; the SDK handles blocking dispatch and panic errors.
#[sabi_trait]
pub trait StoreApi: Send + Sync {
    fn request(&self, metadata: RVec<u8>, body: Stream) -> RResult<Reply, RString>;
    fn supports_compose(&self) -> bool;
    #[sabi(last_prefix_field)]
    fn compose_is_native(&self) -> bool;
}
pub type Store = StoreApi_TO<'static, RBox<()>>;

#[repr(C)]
#[derive(StableAbi)]
#[sabi(kind(Prefix(prefix_ref = PluginRef)))]
pub struct Plugin {
    /// Consumes the inner endpoint on success and failure. Options are JSON.
    #[sabi(last_prefix_field)]
    pub create: extern "C" fn(Store, RVec<u8>) -> RResult<Store, RString>,
}

impl RootModule for PluginRef {
    abi_stable::declare_root_module_statics! {PluginRef}
    const BASE_NAME: &'static str = "walgit_store_plugin";
    const NAME: &'static str = "walgit_store_plugin";
    const VERSION_STRINGS: VersionStrings = package_version_strings!();
}
