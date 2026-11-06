//! An image build of the shared-library surface: a `cdylib` exporting the full
//! `daedalus-surface` C ABI, whose hosts start with the `stable_abi` plugin's nodes installed
//! through the Rust path (`install_plugin`, Rust handlers, no stable ABI). Separately built
//! plugins still install on top, by path. `daedalus-host`'s `shared_in_image` test loads it.

use daedalus::runtime::plugins::{PluginRegistry, PluginResult, RegistryPluginExt};
use daedalus_plugins_stable_abi::StableAbiPlugin;

/// The image's nodes, installed into every host the surface creates.
fn install(registry: &mut PluginRegistry) -> PluginResult<()> {
    registry.install_plugin(&StableAbiPlugin::new())
}

daedalus_surface::export_surface!(install);
