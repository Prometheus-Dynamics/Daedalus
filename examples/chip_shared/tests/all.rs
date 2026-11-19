//! This crate's integration tests that need no process of their own, as one test binary:
//! one link instead of one per file. A file that needs a process to itself is a
//! `[[test]]` of its own in Cargo.toml, with the reason beside it. A new file goes in one
//! or the other; the boundary check refuses a test file in neither.

#[path = "agreement.rs"]
mod agreement;
mod common;
#[path = "compiled.rs"]
mod compiled;
#[path = "generated.rs"]
mod generated;
#[path = "loaded.rs"]
mod loaded;
#[cfg(feature = "host")]
#[path = "runtime.rs"]
mod runtime;
