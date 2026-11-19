//! This crate's integration tests that need no process of their own, as one test binary:
//! one link instead of one per file. A file that needs a process to itself is a
//! `[[test]]` of its own in Cargo.toml, with the reason beside it. A new file goes in one
//! or the other; the boundary check refuses a test file in neither.

#[path = "digest.rs"]
mod digest;
#[cfg(target_os = "linux")]
#[path = "dlopen_name.rs"]
mod dlopen_name;
#[path = "lock.rs"]
mod lock;
#[cfg(unix)]
#[path = "merge.rs"]
mod merge;
#[path = "program.rs"]
mod program;
mod support;
