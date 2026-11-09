//! The host around one loaded graph.

pub use daedalus_host::{
    Dir, FeedOutcome, Host, HostError, HostValue, PortInfo, PortSpec, Ports, PortsError,
    StateStore, TickOverheadReport, TickReport, ValueSerializers,
};

#[cfg(feature = "std")]
pub use daedalus_host::stream::{StreamGraph, StreamHost};
#[cfg(feature = "std")]
pub use daedalus_host::{
    ConstTable, ConstValue, ControlError, FaultReport, LoadError, LoadOptions, LoadedHost,
    PatchError, PatchOutcome, ReloadError, ReloadReport, StopHandle, UnmergedError,
};

/// Several graphs ticked on one thread.
#[cfg(feature = "std")]
pub use daedalus_host::domain;

/// Fusing several timestamped inputs.
#[cfg(feature = "std")]
pub use daedalus_host::sync_group;
