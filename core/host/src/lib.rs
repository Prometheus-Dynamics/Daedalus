//! The host around one loaded graph. A host feeds values into input ports, runs ticks, and takes
//! values out of output ports. It holds no `unsafe`: the runner's safe value API does the work.

#![forbid(unsafe_code)]
// Errors carry their port and type names inline, so a refused call allocates nothing. That makes
// the error type large on purpose.
#![allow(clippy::result_large_err)]

#[cfg(feature = "alloc-probe")]
pub mod alloc_probe;
mod config_cache;
#[cfg(feature = "std")]
mod diagnostics;
#[cfg(feature = "std")]
mod drive;
mod freshness;
mod held;
mod inbound;
#[cfg(all(feature = "std", target_os = "linux"))]
pub mod inbound_fd;
mod inspect;
mod io;
mod label;
mod ports;
mod serializers;
#[cfg(feature = "std")]
mod snapshot;
mod state;
#[cfg(feature = "std")]
pub mod stream;
mod take;
mod telemetry;
mod tick_overhead;
mod value;
#[cfg(feature = "std")]
mod wait;

pub use daedalus_runner::TickReport;

pub use config_cache::ConfigCache;
#[cfg(feature = "std")]
pub use diagnostics::{PlanReport, StepReport, render_error};
#[cfg(feature = "std")]
pub use drive::StopHandle;
pub use inbound::{Drops, PortPolicy};
pub use inspect::{PayloadInspection, PayloadSummary};
pub use io::{FeedOutcome, Host, HostError};
pub use label::{LABEL_BYTES, Label};
pub use ports::{Dir, PortSpec, Ports, PortsError};
pub use serializers::{SerializerEntry, ValueDeserializer, ValueSerializer, ValueSerializers};
pub use state::{Codec, StateCodec, StateStore};
#[cfg(feature = "std")]
pub use state::{SnapshotError, restore_json, snapshot_json};
pub use telemetry::{Event, EventLog, ExecutionReport, Level, NodeRow, Telemetry};
pub use tick_overhead::{TickOverheadReport, TickOverheadWindow};
pub use value::HostValue;
#[cfg(feature = "std")]
pub use wait::{InboundSignal, InboundWait, InboundWaiter};
