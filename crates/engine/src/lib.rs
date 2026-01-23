//! Engine: library-first host that wires registry -> planner -> runtime.
//! No CLI surface; configuration comes from code or environment helpers.

mod config;
#[cfg(feature = "config-env")]
pub mod diagnostics;
mod engine;
mod error;

pub use config::{EngineConfig, GpuBackend, RuntimeMode};
pub use daedalus_runtime::MetricsLevel;
pub use engine::{Engine, RunResult};
pub use error::EngineError;
