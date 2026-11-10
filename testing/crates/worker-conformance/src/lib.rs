//! The canonical worker fixtures, and a runner that drives any worker spec through the process
//! backend: each fixture is compiled as a one-node region, then run twice.

#![forbid(unsafe_code)]

pub mod fixtures;
pub mod runner;

pub use fixtures::{
    CUSTOM_KEY, Expect, Fixture, Port, Round, declarations, fixtures, hello, slot_bytes,
};
pub use runner::{Failure, run_all, run_fixtures};
