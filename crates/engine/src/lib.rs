//! Engine: library-first host that wires registry -> planner -> runtime.
//! No CLI surface; configuration comes from code or environment helpers.
//!
//! `no_std` + `alloc` without the `std` feature (implied by the default `threads`): plan, run
//! and drive graphs serially (see "Portability" in docs/development.md).
#![cfg_attr(not(any(feature = "std", test)), no_std)]

#[cfg_attr(not(feature = "std"), macro_use)]
extern crate alloc;
#[cfg(all(test, not(feature = "std")))]
extern crate std;

mod portable;
mod prelude;
mod trace;

mod cache;
mod compiled_run;
mod config;
#[cfg(feature = "config-env")]
pub mod diagnostics;
#[cfg(feature = "plugins")]
mod document;
mod domain;
mod engine;
mod engine_execution;
mod error;
mod host_graph;
mod prepared_plan;

pub use cache::{CacheStatus, EngineCacheMetrics};
pub use compiled_run::{CompiledRun, RunResult};
pub use config::{
    CacheSection, EngineConfig, EngineConfigError, GpuBackend, PlannerSection, RuntimeMode,
    RuntimeSection,
};
pub use daedalus_core::platform::Clock;
pub use daedalus_runtime::MetricsLevel;
#[cfg(all(feature = "std", target_os = "linux"))]
pub use daedalus_runtime::host_bridge::InboundFd;
pub use daedalus_runtime::host_bridge::{
    HostBatchOutcomes, HostBatchRejected, HostInputBatch, InboundWait, InboundWaiter,
    PayloadInspection, PayloadSummary,
};
pub use daedalus_runtime::{FrameOverheadReport, FrameTickSample};
pub use daedalus_runtime::{HostPortConnection, HostPortDescriptor, HostPortDirection};
pub use domain::{
    DomainExplanation, DomainGraphExplanation, DomainGraphStats, DomainInputExplanation,
    DomainLinkExplanation, DomainOverhead, DomainSharedNode, DomainStats, DomainTick,
    ExecutionDomain, LinkMode,
};
#[cfg(feature = "plugins")]
pub use domain::{SHARED_UPSTREAM_GRAPH, is_shareable};
pub use engine::Engine;
pub use error::EngineError;
pub use host_graph::{
    DEFAULT_FRAME_OVERHEAD_WINDOW, HostGraph, HostGraphDriveExit, HostGraphInput, HostGraphLane,
    HostGraphOutput, HostGraphPayloadInput, HostGraphPayloadOutput, HostGraphStopHandle,
    HostGraphTurn,
};
pub use prepared_plan::{PreparedPlan, PreparedRuntimePlan};
