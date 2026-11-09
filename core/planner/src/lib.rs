//! The planner: solve the graph, ask the backends, assign, compile, stitch. It runs on a host or
//! in a build script, and it may allocate.

#![forbid(unsafe_code)]

pub mod assign;
pub mod pipeline;
pub mod regions;
pub mod solve;
pub mod stitch;

pub use pipeline::{Config, SolveError, solve};
