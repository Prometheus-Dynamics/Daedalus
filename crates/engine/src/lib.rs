//! Engine: library-first host that wires registry -> planner -> runtime.
//! No CLI surface; configuration comes from code or environment helpers.

mod cache;
mod compiled_run;
mod config;
#[cfg(feature = "config-env")]
pub mod diagnostics;
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
pub use daedalus_runtime::MetricsLevel;
pub use engine::Engine;
pub use error::EngineError;
pub use host_graph::{
    HostGraph, HostGraphInput, HostGraphLane, HostGraphOutput, HostGraphPayloadInput,
    HostGraphPayloadOutput,
};
pub use prepared_plan::{PreparedPlan, PreparedRuntimePlan};
