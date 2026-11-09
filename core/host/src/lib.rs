//! The host around one loaded graph. A host feeds values into input ports, runs ticks, and takes
//! values out of output ports. It holds no `unsafe`: the runner's safe value API does the work.

#![forbid(unsafe_code)]
// Errors carry their port and type names inline, so a refused call allocates nothing. That makes
// the error type large on purpose.
#![allow(clippy::result_large_err)]

mod io;
mod label;
mod ports;
mod take;

pub use daedalus_runner::TickReport;

pub use io::{FeedOutcome, Host, HostError};
pub use label::{LABEL_BYTES, Label};
pub use ports::{Dir, PortSpec, Ports, PortsError};
