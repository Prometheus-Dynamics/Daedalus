//! Facade for the Daedalus pipeline. Re-exports the stable surfaces from the layered crates
//! (registry, planner, runtime, engine) and provides end-to-end examples.
//! Per-crate plans live alongside each crate's `PLAN.md`.
//!
//! # Exported modules
//! - `registry`: registry types and builders.
//! - `planner`: graph + planner inputs/outputs.
//! - `runtime`: runtime plan and executor types.
//! - `engine` (feature `engine`): library-first host wiring everything together.

pub use daedalus_core as core;
pub use daedalus_core::compute::ComputeAffinity;
pub use daedalus_core::policy::BackpressureStrategy;
pub use daedalus_core::sync::{SyncGroup, SyncPolicy};
pub use daedalus_data as data;
#[cfg(feature = "engine")]
pub use daedalus_engine as engine;
#[cfg(feature = "ffi")]
pub use daedalus_ffi as ffi;
#[cfg(feature = "ffi")]
pub use daedalus_ffi::{FfiPluginError, PluginLibrary, export_plugin};
#[cfg(feature = "gpu-types")]
pub use daedalus_gpu as gpu;
#[cfg(feature = "gpu-types")]
pub use daedalus_gpu::{
    Backing, Compute, DeviceBridge, GpuBufferHandle, GpuBufferId, GpuImageHandle, GpuImageId,
};
pub use daedalus_macros as macros;
#[cfg(feature = "plugins")]
pub use daedalus_macros::plugin;
pub use daedalus_macros::{BranchPayload, Outputs, adapt, device, type_key};
#[cfg(feature = "plugins")]
pub use daedalus_nodes::declare_plugin;
pub use daedalus_planner as planner;
pub use daedalus_registry as registry;
pub use daedalus_runtime as runtime;
pub use daedalus_runtime::FanIn;
pub use daedalus_runtime::graph_builder;
pub use daedalus_runtime::handles::{NodeHandle, NodeHandleLike, PortHandle};
#[cfg(feature = "plugins")]
pub use daedalus_runtime::plugins::{
    NodeInstall, Plugin, PluginGroup, PluginInstallContext, PluginInstallable, PluginPart,
    PluginRegistry, TransportAdapterOptions,
};
#[cfg(feature = "plugins")]
pub use daedalus_runtime::{
    register_daedalus_types, register_daedalus_values, register_to_value_serializers,
};
pub use daedalus_transport as transport;
/// Host-bridge helpers for wiring host-side inputs/outputs.
///
/// ```no_run
/// use daedalus::host_bridge::{install_host_bridge, host_port};
/// use daedalus::runtime::host_bridge::HostBridgeManager;
/// use daedalus::runtime::plugins::PluginRegistry;
///
/// let mut registry = PluginRegistry::default();
/// let manager = HostBridgeManager::new();
/// let bridge = install_host_bridge(&mut registry, manager).expect("bridge");
/// let input = host_port(bridge.alias_name(), "in");
/// let output = host_port(bridge.alias_name(), "out");
/// let _ = (input, output);
/// ```
#[cfg(feature = "plugins")]
pub mod host_bridge;
#[cfg(feature = "plugins")]
pub use host_bridge::{
    HostBridgeInstallError, host_port, install_default_host_bridge, install_host_bridge,
};

// Optional plugin crates are re-exported via features; no in-crate plugins live here.

/// Return the crate version string.
///
/// ```
/// let ver = daedalus::version();
/// assert!(!ver.is_empty());
/// ```
pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}
