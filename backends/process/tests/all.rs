//! This crate's integration tests that need no process of their own, as one test binary:
//! one link instead of one per file. A file that needs a process to itself is a
//! `[[test]]` of its own in Cargo.toml, with the reason beside it. A new file goes in one
//! or the other; the boundary check refuses a test file in neither.

#[cfg(target_os = "linux")]
#[path = "any_type.rs"]
mod any_type;
#[path = "conformance.rs"]
mod conformance;
#[path = "cpu_wire.rs"]
mod cpu_wire;
#[path = "plugin_worker.rs"]
mod plugin_worker;
#[cfg(target_os = "linux")]
#[path = "shared_memory.rs"]
mod shared_memory;
#[path = "supervision.rs"]
mod supervision;
mod support;
