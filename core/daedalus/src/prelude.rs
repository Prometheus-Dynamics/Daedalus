//! `use daedalus::prelude::*;` for the names most applications use.

pub use crate::host::{FeedOutcome, Host, HostError, TickReport};
pub use daedalus_value::Value;

#[cfg(feature = "std")]
pub use crate::host::{FaultReport, LoadOptions, LoadedHost, ReloadReport, StreamGraph};
#[cfg(feature = "std")]
pub use daedalus_planner::{Config, Planned, plan};

/// Writing nodes: the one macro, and the derives for config structs and value types. The code
/// they generate names `daedalus_registry` and the backend's authoring crate, so a crate that
/// writes nodes depends on those two as well as on this one.
pub use daedalus_derive::{NodeConfig, Type};
pub use daedalus_macros::node;

/// Reading a graph document, registering nodes, and reading what a run recorded.
#[cfg(feature = "std")]
pub use crate::host::{ExecutionReport, Level, Telemetry};
#[cfg(feature = "std")]
pub use daedalus_graph::{GraphBody, GraphDoc};
#[cfg(feature = "std")]
pub use daedalus_registry::{NodeRecord, Registry};
