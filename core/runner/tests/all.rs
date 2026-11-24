//! This crate's integration tests that need no process of their own, as one test binary:
//! one link instead of one per file. A file that needs a process to itself is a
//! `[[test]]` of its own in Cargo.toml, with the reason beside it. A new file goes in one
//! or the other; the boundary check refuses a test file in neither.

#[path = "activity.rs"]
mod activity;
#[path = "async_step.rs"]
mod async_step;
#[path = "backpressure.rs"]
mod backpressure;
#[path = "cancel.rs"]
mod cancel;
#[path = "fail_fast.rs"]
mod fail_fast;
#[path = "failure.rs"]
mod failure;
#[path = "fault_log.rs"]
mod fault_log;
#[path = "fence.rs"]
mod fence;
#[path = "join.rs"]
mod join;
#[cfg(feature = "threads")]
#[path = "lanes.rs"]
mod lanes;
#[path = "no_alloc.rs"]
mod no_alloc;
#[path = "owned.rs"]
mod owned;
#[path = "per_tick.rs"]
mod per_tick;
#[path = "positions.rs"]
mod positions;
#[path = "progress_handle.rs"]
mod progress_handle;
#[path = "reuse.rs"]
mod reuse;
mod support;
#[path = "telemetry.rs"]
mod telemetry;
#[path = "tick.rs"]
mod tick;

// Every file here counts allocations per thread (daedalus_abi::counting), so they share one
// counting allocator and one process.
#[global_allocator]
static ALLOC: daedalus_abi::counting::CountingAlloc = daedalus_abi::counting::CountingAlloc;
