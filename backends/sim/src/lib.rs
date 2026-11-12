//! A backend with no device behind it. It has the shape of a real one: two memory spaces,
//! transfers, merging, refusal and all three compiled forms. Tests use it to see what the planner
//! decided and which steps ran.

#![forbid(unsafe_code)]

mod backend;
mod counters;
mod memory_only;
mod steps;

pub use backend::{SimBackend, SimConfig, SimForm};
pub use counters::ran;
pub use memory_only::SimMemory;

pub const BACKEND_ID: &str = "sim";
pub const HOST: &str = "sim.host";
pub const DEVICE: &str = "sim.device";
pub const POOL: &str = "sim.pool";
pub const TO_DEVICE: &str = "sim.to_device";
pub const TO_HOST: &str = "sim.to_host";
/// Can be absorbed into the compute step that reads it.
pub const IMPORT: &str = "sim.import";
pub const POOL_TO_HOST: &str = "sim.pool_to_host";
/// A body listing this need is not taken.
pub const UNAVAILABLE: &str = "sim.unavailable";
