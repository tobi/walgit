//! Minimal external-store example: no encryption, identity or cloud dependency.
use walgit_store::DynStore;

async fn create(inner: DynStore, config: serde_json::Value) -> anyhow::Result<DynStore> {
    anyhow::ensure!(
        config.as_object().is_some_and(serde_json::Map::is_empty),
        "passthrough takes no options"
    );
    Ok(inner)
}

walgit_store_plugin::export_plugin!(create);
