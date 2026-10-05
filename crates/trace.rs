//! The crate's `tracing` instrumentation, compiled out without the `tracing` feature (implied by
//! `std`; see "Portability" in docs/development.md): `tracing-core` needs compare-and-swap.
//!
//! With the feature these are `tracing`'s macros. Without it the event macros expand to nothing
//! (their arguments are not evaluated) and `debug_span!` to an inert [`Span`].
//!
//! Shared module: each crate's `src/trace.rs` is a symlink to this file (like `portable.rs`).
#![allow(dead_code, unused_imports, unused_macros)]

#[cfg(feature = "tracing")]
pub(crate) use tracing::{debug, debug_span, error, trace, warn};

#[cfg(not(feature = "tracing"))]
macro_rules! noop {
    ($($tokens:tt)*) => {
        ()
    };
}
#[cfg(not(feature = "tracing"))]
pub(crate) use {noop as debug, noop as error, noop as trace, noop as warn};

#[cfg(not(feature = "tracing"))]
macro_rules! debug_span {
    ($($tokens:tt)*) => {
        $crate::trace::Span
    };
}
#[cfg(not(feature = "tracing"))]
pub(crate) use debug_span;

/// The `tracing::Span` subset these crates use, recording nothing.
#[cfg(not(feature = "tracing"))]
#[must_use]
pub(crate) struct Span;

#[cfg(not(feature = "tracing"))]
impl Span {
    pub(crate) fn enter(&self) -> Span {
        Span
    }

    pub(crate) fn entered(self) -> Span {
        self
    }
}
