//! This crate's integration tests that need no process of their own, as one test binary:
//! one link instead of one per file. A file that needs a process to itself is a
//! `[[test]]` of its own in Cargo.toml, with the reason beside it. A new file goes in one
//! or the other; the boundary check refuses a test file in neither.

#[cfg(unix)]
#[path = "c_host.rs"]
mod c_host;
#[cfg(unix)]
#[path = "cxx_client.rs"]
mod cxx_client;
#[path = "header.rs"]
mod header;
#[path = "reports.rs"]
mod reports;
mod support;
#[path = "surface.rs"]
mod surface;
