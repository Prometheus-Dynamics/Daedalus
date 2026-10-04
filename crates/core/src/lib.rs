//! Shared low-level types for the Daedalus workspace.
//!
//! This crate owns deterministic ids, logical clocks, message envelopes, channel
//! traits, backpressure policy names, sync metadata, and the optional metrics
//! facade. It intentionally stays dependency-light so registry, planner,
//! runtime, engine, FFI, and transport crates can depend on it without pulling
//! in higher-level execution concerns.
//!
//! `no_std` + `alloc` without the default `std` feature (see "Portability" in
//! docs/development.md).
#![cfg_attr(not(feature = "std"), no_std)]

#[cfg_attr(not(feature = "std"), macro_use)]
extern crate alloc;

mod portable;
pub(crate) use portable::lock_recover;

pub mod channels;
pub mod clock;
pub mod compute;
pub mod errors;
pub mod ids;
pub mod messages;
pub mod metadata;
pub mod platform;
pub mod policy;
pub mod stable_id;
pub mod sync;

#[cfg(feature = "metrics")]
pub mod metrics;

/// Defines `ENABLED_FEATURES` and `CARGO_MANIFEST` for the invoking crate.
///
/// Together they feed the dynamic plugin build fingerprint: `ENABLED_FEATURES` lists the enabled
/// Cargo features (set by the shared `crates/build_features.rs` build script) and the
/// `[package.metadata.daedalus]` table of `CARGO_MANIFEST` classifies each feature.
#[doc(hidden)]
#[macro_export]
macro_rules! build_facts {
    () => {
        #[doc = concat!(
            "Cargo features of `",
            env!("CARGO_PKG_NAME"),
            "` enabled in this build (comma-separated).\n\nTogether with [`CARGO_MANIFEST`], \
             whose `[package.metadata.daedalus]` table classifies each feature, this feeds the \
             dynamic plugin build fingerprint."
        )]
        pub const ENABLED_FEATURES: &str = env!("DAEDALUS_ENABLED_FEATURES");
        /// This crate's `Cargo.toml`.
        pub const CARGO_MANIFEST: &str =
            include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml"));
    };
}

build_facts!();

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
