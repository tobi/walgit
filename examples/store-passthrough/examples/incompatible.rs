//! Deliberately incompatible module for the loader contract test, not a plugin.
#![allow(unsafe_code, clippy::expl_impl_clone_on_copy)]
use abi_stable::{StableAbi, prefix_type::PrefixTypeTrait};

#[repr(C)]
#[derive(StableAbi)]
#[sabi(kind(Prefix(prefix_ref = WrongPluginRef)))]
pub struct WrongPlugin {
    #[sabi(last_prefix_field)]
    pub create: extern "C" fn() -> u64,
}
impl abi_stable::library::RootModule for WrongPluginRef {
    abi_stable::declare_root_module_statics! {WrongPluginRef}
    const BASE_NAME: &'static str = "walgit_store_plugin";
    const NAME: &'static str = "walgit_store_plugin";
    const VERSION_STRINGS: abi_stable::sabi_types::VersionStrings =
        abi_stable::package_version_strings!();
}
#[abi_stable::export_root_module]
pub fn get_library() -> WrongPluginRef {
    extern "C" fn incompatible_create() -> u64 {
        0
    }
    WrongPlugin {
        create: incompatible_create,
    }
    .leak_into_prefix()
}
