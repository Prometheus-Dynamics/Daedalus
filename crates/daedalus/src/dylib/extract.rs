//! The plugin's private registry and the schema exported from it (plugin side).

use super::__support::LinkedDep;
use super::PluginSchema;
use crate::runtime::plugins::{Plugin, PluginRegistry, RegistryPluginExt};

/// The registry the descriptor's entry points read and the stable `invoke` runs nodes from: the
/// linked dependencies, then `P`. Unkeyed foreign port types (keys a dependency that is not
/// linked maps) are recorded instead of failing (`PluginRegistry::record_external_types`);
/// `codecs` records the node port codecs the stable path needs.
pub(super) fn registry<P: Plugin + Default>(
    deps: &[LinkedDep],
    codecs: bool,
) -> Result<(PluginRegistry, P), String> {
    let mut registry = PluginRegistry::new();
    registry.record_external_types();
    if codecs {
        registry.record_stable_codecs();
    }
    for dep in deps {
        let dep = dep();
        registry
            .install_plugin(&*dep)
            .map_err(|err| format!("linked dependency plugin `{}`: {err}", dep.id()))?;
    }
    let plugin = P::default();
    registry
        .install_plugin(&plugin)
        .map_err(|err| err.to_string())?;
    Ok((registry, plugin))
}

/// `plugin`'s [`PluginSchema`]: its manifest (with the linked dependencies added to
/// `dependencies`) and nodes, and the recorded external port types as
/// `plugin.metadata.external_types`.
pub(super) fn schema(
    registry: &PluginRegistry,
    plugin: &dyn Plugin,
    deps: &[LinkedDep],
) -> Result<PluginSchema, String> {
    // Not `combined_transport_capabilities`: its validation fails on the plugin's `deps` the
    // registry lacks, which only the host provides (and checks).
    let mut capabilities = registry.transport_capabilities.clone();
    for manifest in registry.plugin_manifests.values() {
        if capabilities.plugin_manifest(&manifest.id).is_none() {
            capabilities
                .register_plugin(manifest.clone())
                .map_err(|err| err.to_string())?;
        }
    }
    let mut schema = daedalus_ffi_host::export_registry_plugin_schema(&capabilities, plugin.id())
        .map_err(|err| err.to_string())?;
    schema
        .dependencies
        .extend(deps.iter().map(|dep| dep().id().to_string()));
    schema.dependencies.sort();
    schema.dependencies.dedup();
    let external: Vec<_> = registry
        .external_types()
        .iter()
        .map(|external| {
            serde_json::json!({
                "owner": external.owner,
                "port": external.port,
                "rust_type": external.rust_type,
            })
        })
        .collect();
    if !external.is_empty() {
        schema
            .plugin
            .metadata
            .insert("external_types".into(), external.into());
    }
    Ok(schema)
}
