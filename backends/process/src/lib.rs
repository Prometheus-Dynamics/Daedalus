//! The process backend. A node whose body is served by a worker process runs there: the worker
//! speaks the wire protocol on its stdin and stdout, and this backend supervises it. A worker
//! that dies, hangs or faults makes its region report a fault. It respawns after a backoff capped
//! at 8 s, and never gives up on its own. An operator disables a backend by hand.
//!
//! The crate has no `unsafe`. Values cross as the bytes of their slots, read through
//! `daedalus_abi::Slot::bytes`.

#![forbid(unsafe_code)]

mod backoff;
mod child;
mod error;
mod reader;
mod region;
mod spec;
mod supervise;
mod telemetry;

pub use backoff::{BASE_DELAY, MAX_DELAY, delay_after};
pub use error::ProcessError;
pub use region::{BACKEND_ID, HOST, ProcessBackend};
pub use spec::WorkerSpec;
pub use telemetry::Snapshot;
