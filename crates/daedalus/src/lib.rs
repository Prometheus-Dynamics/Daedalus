//! Facade for the Daedalus pipeline.
//!
//! This crate re-exports the stable public surfaces from the layered workspace:
//! core, transport, data, registry, planner, runtime, macros, optional engine,
//! optional GPU, and plugin helpers.
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
#[cfg(feature = "gpu-types")]
pub use daedalus_gpu as gpu;
#[cfg(feature = "gpu-types")]
pub use daedalus_gpu::{
    Backing, Compute, DeviceBridge, GpuBufferHandle, GpuBufferId, GpuImageHandle, GpuImageId,
};
pub use daedalus_macros as macros;
#[cfg(feature = "plugins")]
pub use daedalus_macros::plugin;
pub use daedalus_macros::{
    BranchPayload, DaedalusToValue, DaedalusTypeExpr, GpuBindings, GpuStateful, NodeConfig,
    Outputs, adapt, device, node, node_handler, type_key,
};
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
#[cfg(feature = "plugins")]
pub mod host_bridge;
#[cfg(feature = "plugins")]
pub use host_bridge::{
    HostBridgeInstallError, host_port, install_default_host_bridge, install_host_bridge,
};

// Optional plugin crates are re-exported via features; no in-crate plugins live here.

/// Common imports for application and example code.
///
/// The prelude keeps the facade ergonomic while leaving the layered crate modules
/// (`data`, `runtime`, `engine`, `transport`, and others) available for explicit
/// imports when callers need a narrower surface.
pub mod prelude {
    pub use crate::data::prelude::*;
    #[cfg(feature = "engine")]
    pub use crate::engine::{
        CacheSection, CacheStatus, CompiledRun, Engine, EngineCacheMetrics, EngineConfig,
        EngineConfigError, EngineError, GpuBackend, HostGraph, HostGraphInput, HostGraphLane,
        HostGraphOutput, HostGraphPayloadInput, HostGraphPayloadOutput, PlannerSection,
        PreparedPlan, PreparedRuntimePlan, RunResult, RuntimeMode, RuntimeSection,
    };
    pub use crate::registry::prelude::*;
    pub use crate::runtime::{
        DEFAULT_OUTPUT_PORT, ExecutionContext, ExecutionTelemetry, Executor, FanIn, MetricsLevel,
        NodeError, NodeIo, OwnedExecutor, RuntimePlan, RuntimeTransport, SchedulerConfig,
        StreamGraph, StreamGraphWorker, TypedInputResolution, TypedInputResolutionKind,
        build_runtime, graph_builder,
    };
    pub use crate::transport::{
        AccessMode, AdaptKind, AdapterId, AdapterKind, BoundaryPayloadError, Cpu, Device,
        DeviceClass, Gpu, Layout, LayoutHash, Payload, Residency, SourceId, TransportError,
        TypeKey,
    };
    #[cfg(feature = "gpu-types")]
    pub use crate::{Backing, Compute, DeviceBridge, GpuBufferHandle, GpuImageHandle};
    pub use crate::{
        BackpressureStrategy, BranchPayload, ComputeAffinity, DaedalusToValue, DaedalusTypeExpr,
        GpuBindings, GpuStateful, NodeConfig, NodeHandle, NodeHandleLike, Outputs, PortHandle,
        SyncGroup, SyncPolicy, adapt, device, node, node_handler, type_key,
    };
    #[cfg(feature = "plugins")]
    pub use crate::{
        HostBridgeInstallError, NodeInstall, Plugin, PluginGroup, PluginInstallContext,
        PluginInstallable, PluginPart, PluginRegistry, TransportAdapterOptions, declare_plugin,
        host_port, install_default_host_bridge, install_host_bridge, plugin,
        register_daedalus_types, register_daedalus_values, register_to_value_serializers,
    };
}

/// Return the crate version string.
///
pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}
