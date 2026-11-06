//! The `stable_bench` plugin: the nodes the `dylib_stable_bench` and `dylib_stable_bench_allocations`
//! tests time on the stable handler path against the same plugin installed statically (see
//! `docs/dynamic-plugins.md`, "The stable handler path").
//!
//! - [`config`]: nodes with N constant inputs (N = 0, 8, 16, 49), scalars or with a quarter
//!   strings, for the cost per constant.
//! - [`detections`]: `Detections` (mirroring Eidos's) through `Value`, a source and a sink.
//! - [`handle`]: the same `Detections` as a plugin-local foreign handle, a source and a sink.
//! - [`staged`]: a stage with one handle in, one out, a frame input and 0 or 10 constants.
//!
//! The `dylib` feature exports the plugin from a `cdylib` whose descriptor claims another rustc,
//! so a host refuses its Rust ABI and installs it through the stable path.

pub mod config;
pub mod detections;
pub mod handle;
pub mod staged;

use daedalus::macros::plugin;

#[allow(clippy::wildcard_imports)] // The node macros' generated items.
use config::*;
#[allow(clippy::wildcard_imports)]
use detections::*;
#[allow(clippy::wildcard_imports)]
use handle::*;
#[allow(clippy::wildcard_imports)]
use staged::*;

pub use detections::Detections;
pub use handle::DetectionsInterface;

#[plugin(
    id = "stable_bench",
    types(Detections),
    foreign_providers(Detections => DetectionsInterface),
    nodes(
        value_source,
        value_sink,
        handle_source,
        handle_sink,
        stage0,
        stage10,
        config_s0,
        config_s8,
        config_s16,
        config_s49,
        config_m8,
        config_m16,
        config_m49
    )
)]
pub struct StableBenchPlugin;

/// The rustc the exported descriptor claims.
pub const SIMULATED_RUSTC: &str = "rustc 1.85.0 (4d91de4e4 2025-02-17; simulated)";

#[cfg(feature = "dylib")]
mod export {
    use daedalus::dylib::{PLUGIN_ABI_VERSION, PluginDescriptor, StableExtension, StrView};

    /// Returns the Daedalus dynamic plugin ABI version.
    #[unsafe(no_mangle)]
    pub extern "C" fn daedalus_plugin_abi_version() -> u32 {
        PLUGIN_ABI_VERSION
    }

    /// The plugin's descriptor, claiming another toolchain.
    #[unsafe(no_mangle)]
    pub extern "C" fn daedalus_plugin_descriptor() -> PluginDescriptor {
        let mut descriptor = daedalus::plugin_descriptor!(super::StableBenchPlugin);
        descriptor.info.rustc_version = StrView::from_static(super::SIMULATED_RUSTC);
        descriptor
    }

    /// The stable extension (const inputs sent once, see `StableExtension`).
    #[unsafe(no_mangle)]
    pub extern "C" fn daedalus_plugin_stable_extension() -> StableExtension {
        daedalus::dylib::stable_extension()
    }
}
