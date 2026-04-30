//! Base plumbing for the Daedalus pipeline stack.
//!
//! This crate is intentionally small and feature-light: it owns IDs, errors,
//! clocks/ticks, message envelopes, channel primitives, and (optionally) a tiny
//! metrics facade. Higher layers (`daedalus-data`, `daedalus-registry`,
//! planner/runtime crates) depend on these building blocks and must not inject
//! new global state or cfg mazes here.
//!
//! IDs serialize deterministically as `"prefix:n"` strings (e.g., `"node:1"`,
//! `"edge:7"`) to keep planner/runtime diagnostics and golden outputs stable.
//! Message payloads are generic; callers are expected to uphold any `Send`/`Sync`
//! guarantees required by their runtime context.
//!
//! # Examples
//! Send/recv on a bounded channel:
//! ```
//! use daedalus_core::channels::{bounded, ChannelRecv, ChannelSend, RecvOutcome};
//!
//! let (tx, rx) = bounded(2);
//! assert_eq!(tx.send(1), daedalus_core::channels::Backpressure::Ok);
//! assert_eq!(tx.send(2), daedalus_core::channels::Backpressure::Ok);
//! assert_eq!(rx.try_recv(), RecvOutcome::Data(1));
//! assert_eq!(rx.try_recv(), RecvOutcome::Data(2));
//! ```
//!
//! Manual tick clock:
//! ```
//! use daedalus_core::clock::TickClock;
//! let clock = TickClock::default();
//! let t1 = clock.tick();
//! let t2 = clock.advance(5);
//! assert!(t2.value() > t1.value());
//! ```
//!
//! **Concurrency/typing notes:** channel factories enforce `Send` on payloads;
//! if your runtime requires `Sync` as well, enforce it at your boundary (e.g.,
//! `ChannelSend<Arc<T>>` with `T: Send + Sync`). IDs serialize as `"prefix:n"`
//! strings for stable planner/runtime diagnostics and goldens.

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
