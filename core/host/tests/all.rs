//! This crate's integration tests that need no process of their own, as one test binary:
//! one link instead of one per file. A file that needs a process to itself is a
//! `[[test]]` of its own in Cargo.toml, with the reason beside it. A new file goes in one
//! or the other; the boundary check refuses a test file in neither.

#[cfg(unix)]
#[path = "drive.rs"]
mod drive;
#[path = "edge_policy.rs"]
mod edge_policy;
#[path = "inspect.rs"]
mod inspect;
#[path = "state.rs"]
mod state;
#[path = "stream.rs"]
mod stream;
#[path = "sync_group.rs"]
mod sync_group;
