//! This crate's integration tests that need no process of their own, as one test binary:
//! one link instead of one per file. A file that needs a process to itself is a
//! `[[test]]` of its own in Cargo.toml, with the reason beside it. A new file goes in one
//! or the other; the boundary check refuses a test file in neither.

#[path = "adapter.rs"]
mod adapter;
mod common;
#[path = "devices.rs"]
mod devices;
#[path = "fragment.rs"]
mod fragment;
#[path = "host_bytes.rs"]
mod host_bytes;
#[cfg(target_os = "linux")]
#[path = "import_alignment.rs"]
mod import_alignment;
#[cfg(target_os = "linux")]
#[path = "import_offset.rs"]
mod import_offset;
#[cfg(target_os = "linux")]
#[path = "import_unknown.rs"]
mod import_unknown;
#[path = "lease.rs"]
mod lease;
#[cfg(target_os = "linux")]
#[path = "raw_gate.rs"]
mod raw_gate;
#[path = "region.rs"]
mod region;
#[path = "storage.rs"]
mod storage;
#[path = "transfer.rs"]
mod transfer;

// Every file here counts allocations per thread (daedalus_abi::counting), so they share one
// counting allocator and one process.
#[global_allocator]
static ALLOC: daedalus_abi::counting::CountingAlloc = daedalus_abi::counting::CountingAlloc;
