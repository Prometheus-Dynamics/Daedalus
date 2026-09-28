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

/// Locks `mutex`, recovering the guard if a panicking holder poisoned it.
///
/// Every critical section in this crate leaves the guarded state structurally valid, so a
/// poisoned lock carries no information worth propagating.
pub(crate) fn lock_recover<T: ?Sized>(mutex: &std::sync::Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(feature = "metrics")]
pub mod metrics;

/// Cargo features of `daedalus-core` enabled in this build (comma-separated).
///
/// Together with [`CARGO_MANIFEST`], whose `[package.metadata.daedalus]` table classifies
/// each feature, this feeds the dynamic plugin build fingerprint.
pub const ENABLED_FEATURES: &str = env!("DAEDALUS_ENABLED_FEATURES");
/// This crate's `Cargo.toml`.
pub const CARGO_MANIFEST: &str = include_str!("../Cargo.toml");

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
