//! The names an application needs. Re-exports only: this crate has no logic of its own.
//!
//! Start from [`prelude`]. The layered crates stay reachable under [`layers`] for anything the
//! prelude leaves out.

#![no_std]
#![forbid(unsafe_code)]

pub mod host;
pub mod prelude;

/// The runner's own names, for hosts that drive it directly.
#[cfg(feature = "runner")]
pub mod runner {
    pub use daedalus_runner::*;
}

/// The crates under the facade, for explicit imports.
pub mod layers {
    #[cfg(feature = "std")]
    pub use daedalus_graph as graph;
    pub use daedalus_host as host;
    #[cfg(feature = "std")]
    pub use daedalus_planner as planner;
    pub use daedalus_runner as runner;
    pub use daedalus_types as types;
    pub use daedalus_value as value;
}

/// The counting allocator behind allocation attribution.
#[cfg(feature = "alloc-probe")]
pub use daedalus_host::alloc_probe;
