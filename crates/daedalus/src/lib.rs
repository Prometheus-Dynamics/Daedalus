//! Facade for the Daedalus pipeline. Re-exports the stable surfaces from the layered crates
//! (registry, planner, runtime, engine) and provides end-to-end examples.
//! Per-crate plans live alongside each crate's `PLAN.md`.
//!
//! # Quick start: plan + run
//! ```ignore
//! use daedalus::{
//!     Engine, EngineConfig, GpuBackend, RuntimeMode, EngineError,
//!     planner::{Graph, NodeInstance, Edge, PortRef, ComputeAffinity, NodeRef},
//!     registry::{Registry, NodeDescriptor, Port},
//!     data::model::{TypeExpr, ValueType},
//! };
//!
//! fn main() -> Result<(), EngineError> {
//!     // Build a minimal registry with one producer/consumer.
//!     let mut reg = Registry::new();
//!     let ty = TypeExpr::Scalar(ValueType::Int);
//!     reg.register_node(NodeDescriptor {
//!         id: "producer".into(),
//!         feature_flags: vec![],
//!         label: None,
//!         inputs: vec![],
//!         outputs: vec![Port { name: "out".into(), ty: ty.clone(), access: Default::default(), source: None, const_value: None }],
//!         default_compute: ComputeAffinity::CpuOnly,
//!         sync_groups: Vec::new(),
//!         metadata: Default::default(),
//!     })?;
//!     reg.register_node(NodeDescriptor {
//!         id: "consumer".into(),
//!         feature_flags: vec![],
//!         label: None,
//!         inputs: vec![Port { name: "in".into(), ty, access: Default::default(), source: None, const_value: None }],
//!         outputs: vec![],
//!         default_compute: ComputeAffinity::CpuOnly,
//!         sync_groups: Vec::new(),
//!         metadata: Default::default(),
//!     })?;
//!
//!     let graph = Graph {
//!         nodes: vec![
//!             NodeInstance {
//!                 id: "producer".into(),
//!                 bundle: None,
//!                 label: None,
//!                 inputs: vec![],
//!                 outputs: vec!["out".into()],
//!                 compute: ComputeAffinity::CpuOnly,
//!                 const_inputs: vec![],
//!                 sync_groups: vec![],
//!                 metadata: Default::default(),
//!             },
//!             NodeInstance {
//!                 id: "consumer".into(),
//!                 bundle: None,
//!                 label: None,
//!                 inputs: vec!["in".into()],
//!                 outputs: vec![],
//!                 compute: ComputeAffinity::CpuOnly,
//!                 const_inputs: vec![],
//!                 sync_groups: vec![],
//!                 metadata: Default::default(),
//!             },
//!         ],
//!         edges: vec![Edge {
//!             from: PortRef { node: NodeRef(0), port: "out".into() },
//!             to: PortRef { node: NodeRef(1), port: "in".into() },
//!             metadata: Default::default(),
//!         }],
//!         metadata: Default::default(),
//!     };
//!
//!     let mut cfg = EngineConfig::default();
//!     cfg.runtime.mode = RuntimeMode::Parallel;
//!     cfg.gpu = GpuBackend::Cpu;
//!     let engine = Engine::new(cfg)?;
//!
//!     // Provide a handler that moves data across edges.
//!     let hits = std::sync::Arc::new(std::sync::Mutex::new(0usize));
//!     let handler = {
//!         let hits = std::sync::Arc::clone(&hits);
//!         move |node: &daedalus::runtime::RuntimeNode,
//!               _ctx: &daedalus::runtime::ExecutionContext,
//!               io: &mut daedalus::runtime::NodeIo| {
//!             if node.id == "producer" {
//!                 io.push_output(Some("out"), daedalus::runtime::EdgePayload::Unit);
//!             } else if node.id == "consumer" {
//!                 for _ in io.inputs_for("in") {
//!                     *hits.lock().unwrap() += 1;
//!                 }
//!             }
//!             Ok(())
//!         }
//!     };
//!     let result = engine.run(&reg, graph, handler)?;
//!     assert_eq!(*hits.lock().unwrap(), 1);
//!     println!("telemetry: {:?}", result.telemetry);
//!     Ok(())
//! }
//! ```
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
#[cfg(feature = "gpu")]
pub use daedalus_gpu as gpu;
#[cfg(feature = "gpu")]
pub use daedalus_gpu::{
    ErasedPayload, GpuBufferHandle, GpuBufferId, GpuImageHandle, GpuImageId, GpuSendable, Payload,
};
pub use daedalus_macros as macros;
pub use daedalus_nodes::declare_plugin;
pub use daedalus_planner as planner;
pub use daedalus_registry as registry;
pub use daedalus_runtime as runtime;
pub use daedalus_runtime::FanIn;
pub use daedalus_runtime::handles::{NodeHandle, NodeHandleLike, PortHandle};
pub use daedalus_runtime::plugins::{NodeInstall, Plugin, PluginRegistry};
pub use daedalus_runtime::graph_builder;
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
/// let input = host_port(bridge.alias.clone(), "in");
/// let output = host_port(bridge.alias.clone(), "out");
/// let _ = (input, output);
/// ```
pub mod host_bridge;
pub use host_bridge::{host_port, install_host_bridge};

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
