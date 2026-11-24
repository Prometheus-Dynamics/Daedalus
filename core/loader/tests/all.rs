//! This crate's integration tests that need no process of their own, as one test binary:
//! one link instead of one per file. A file that needs a process to itself is a
//! `[[test]]` of its own in Cargo.toml, with the reason beside it. A new file goes in one
//! or the other; the boundary check refuses a test file in neither.

#[path = "alloc.rs"]
mod alloc;
#[path = "codec.rs"]
mod codec;
#[path = "forward.rs"]
mod forward;
#[path = "loader.rs"]
mod loader;
#[path = "off.rs"]
mod off;
#[path = "owned.rs"]
mod owned;
#[path = "retry.rs"]
mod retry;
mod support;

// Every file here counts allocations per thread (daedalus_abi::counting), so they share one
// counting allocator and one process.
#[global_allocator]
static ALLOC: daedalus_abi::counting::CountingAlloc = daedalus_abi::counting::CountingAlloc;
