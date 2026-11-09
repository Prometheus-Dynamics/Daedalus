//! The planner: solve the graph, ask the backends, assign, compile, stitch. It runs on a host or
//! in a build script, and it may allocate.

#![forbid(unsafe_code)]

pub mod api;
pub mod assign;
pub mod config;
pub mod edit;
pub mod form;
pub mod pipeline;
pub mod regions;
pub mod solve;
pub mod stitch;

pub use api::{PlanError, Planned, Planner, plan};
pub use config::Config;
pub use form::Form;
pub use pipeline::{SolveError, solve};
