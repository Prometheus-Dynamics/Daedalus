//! This crate's integration tests that need no process of their own, as one test binary:
//! one link instead of one per file. A file that needs a process to itself is a
//! `[[test]]` of its own in Cargo.toml, with the reason beside it. A new file goes in one
//! or the other; the boundary check refuses a test file in neither.

#[cfg(unix)]
#[path = "drive.rs"]
mod drive;
#[path = "edge_policy.rs"]
mod edge_policy;
#[path = "host_io.rs"]
mod host_io;
#[path = "inspect.rs"]
mod inspect;
#[path = "probe.rs"]
mod probe;
#[path = "state.rs"]
mod state;
#[path = "stream.rs"]
mod stream;
#[path = "sync_group.rs"]
mod sync_group;
#[path = "telemetry.rs"]
mod telemetry;

// Every file here counts allocations per thread (daedalus_abi::counting), so they share one
// counting allocator and one process.
#[global_allocator]
static ALLOC: daedalus_abi::counting::CountingAlloc = daedalus_abi::counting::CountingAlloc;
