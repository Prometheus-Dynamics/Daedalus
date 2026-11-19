//! This crate's integration tests that need no process of their own, as one test binary:
//! one link instead of one per file. A file that needs a process to itself is a
//! `[[test]]` of its own in Cargo.toml, with the reason beside it. A new file goes in one
//! or the other; the boundary check refuses a test file in neither.

#[path = "behaviour.rs"]
mod behaviour;
#[path = "conformance.rs"]
mod conformance;
#[path = "drive.rs"]
mod drive;
mod support;
#[path = "units.rs"]
mod units;
