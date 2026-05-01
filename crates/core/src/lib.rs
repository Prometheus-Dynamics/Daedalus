//! Shared low-level types for the Daedalus workspace.
//!
//! This crate owns deterministic ids, logical clocks, message envelopes, channel
//! traits, backpressure policy names, sync metadata, and the optional metrics
//! facade. It intentionally stays dependency-light so registry, planner,
//! runtime, engine, FFI, and transport crates can depend on it without pulling
//! in higher-level execution concerns.

pub mod channels;
pub mod clock;
pub mod compute;
pub mod errors;
pub mod ids;
pub mod messages;
pub mod metadata;
pub mod policy;
pub mod stable_id;
pub mod sync;

#[cfg(feature = "metrics")]
pub mod metrics;

/// Commonly used types re-exported for convenience.
pub mod prelude {
    pub use crate::channels::{Backpressure, ChannelRecv, ChannelSend, RecvOutcome};
    pub use crate::clock::{Tick, TickClock};
    pub use crate::errors::{CoreError, CoreErrorCode};
    pub use crate::ids::{ChannelId, EdgeId, NodeId, PortId, RunId, TickId};
    pub use crate::messages::{Message, MessageMeta, Sequence, Token, Watermark};

    #[cfg(feature = "metrics")]
    pub use crate::metrics::{MetricsSink, NoopMetrics};
}
