use daedalus_ffi::load_manifest_plugin;
use daedalus_runtime::plugins::{Plugin, PluginRegistry, RegistryPluginExt};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let plugin = load_manifest_plugin("crates/ffi/lang/python/examples/plugin_demo.manifest.json")?;

    let mut registry = PluginRegistry::new();
    registry.install_plugin(&plugin)?;

    println!(
        "Registered {} nodes from manifest plugin '{}'",
        registry.registry.view().nodes.len(),
        plugin.id()
    );
    Ok(())
}
