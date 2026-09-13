//! Load a checked Rust storage plugin or export an external `ObjectStore` decorator.
pub mod abi;
mod bridge;
mod wire;

use abi_stable::library::lib_header_from_path;
use anyhow::{Context, Result};
use std::{path::Path, sync::Arc};
use walgit_store::DynStore;

pub use abi_stable;
pub use bridge::export;

/// Load administrator-selected native code with checked module/type layouts.
/// Like Rotel, load each path separately; the root-module singleton convenience
/// loader would incorrectly reuse the first module for every plugin path.
/// `abi_stable` retains mappings until process exit. Hot unloading is unsupported.
pub async fn load(path: &Path, config: serde_json::Value, inner: DynStore) -> Result<DynStore> {
    let path = path.to_owned();
    let runtime = tokio::runtime::Handle::current();
    let config = serde_json::to_vec(&config)?;
    anyhow::ensure!(
        config.len() <= abi::MAX_METADATA,
        "plugin options exceed limit"
    );
    tokio::task::spawn_blocking(move || {
        let header = lib_header_from_path(&path).context("loading storage plugin")?;
        let module: abi::PluginRef = header
            .init_root_module()
            .context("checking storage plugin ABI")?;
        let host = bridge::expose(inner, runtime, None);
        let endpoint = (module.create())(host, config.into())
            .into_result()
            .map_err(|error| anyhow::anyhow!("storage plugin initialization failed: {error}"))?;
        Ok::<DynStore, anyhow::Error>(Arc::new(bridge::RemoteStore::new(endpoint)))
    })
    .await
    .context("storage plugin loader task failed")?
}

/// Export an async factory `(DynStore, serde_json::Value) -> Result<DynStore>`.
/// Decorators see logical object keys, with the global prefix applied underneath.
/// The implementation crate must depend on `abi_stable` 0.11, like this SDK.
#[macro_export]
macro_rules! export_plugin {
    ($factory:path) => {
        #[allow(unsafe_code)]
        #[abi_stable::export_root_module]
        pub fn get_library() -> $crate::abi::PluginRef {
            use $crate::abi_stable::prefix_type::PrefixTypeTrait;
            struct Factory;
            impl Factory {
                extern "C" fn create(
                    inner: $crate::abi::Store,
                    config: $crate::abi_stable::std_types::RVec<u8>,
                ) -> $crate::abi_stable::std_types::RResult<
                    $crate::abi::Store,
                    $crate::abi_stable::std_types::RString,
                > {
                    $crate::export(inner, config, $factory)
                }
            }
            $crate::abi::Plugin {
                create: Factory::create,
            }
            .leak_into_prefix()
        }
    };
}
