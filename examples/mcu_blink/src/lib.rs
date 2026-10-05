//! The MCU blink graph (`graph.json`), planned on the host by `build.rs`, in the three device
//! modes:
//!
//! ```text
//! host.sample (u16, latest-only) --widen--> scale --> lowpass --> threshold --> rising --> host.rises (bounded 8)
//!                                                         |             \-----> blink  --> host.led   (latest-only)
//!                                                         \--> host.level (latest-only)
//! ```
//!
//! - [`graph`]: compiled, constants frozen ([`Graph`]).
//! - [`tunable`]: compiled with `lowpass.alpha`, `threshold.on`/`off` and `blink.period` as
//!   parameters ([`TunableGraph`]).
//! - [`loaded`]: the node library and two plan blobs for the interpreter: `graph.json`, and
//!   `graph_b.json` (no low-pass: the threshold and `level` see the scaled sample, and a faster
//!   blink).
//!
//! The application pushes raw ADC counts into `sample`, ticks and pops `led`, `level` and
//! `rises`. Everything lives in the graph value (or the interpreter's arena), with no heap.
#![no_std]

pub use daedalus_mcu_blink_nodes as nodes;

/// The compiled plan: `Graph`, `NODE_IDS`, `EDGES`, `PLAN_HASH`.
pub mod graph {
    include!(concat!(env!("OUT_DIR"), "/graph.rs"));
}

/// The compiled plan with tunable parameters: `TunableGraph`, `PARAM_NAMES`, `PARAMS`.
pub mod tunable {
    include!(concat!(env!("OUT_DIR"), "/tunable.rs"));
}

/// Loaded mode: the node library and the plan blobs.
pub mod loaded {
    include!(concat!(env!("OUT_DIR"), "/library.rs"));

    /// Arena bytes of the firmware's interpreter (both plans fit; the native tests check).
    pub const ARENA: usize = 512;
    /// The interpreter type of the firmware.
    pub type Interpreter = daedalus_mcu::loaded::Interpreter<ARENA>;
    /// `graph.json` compiled for [`LIBRARY`].
    pub const PLAN_A: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/plan_a.bin"));
    /// `graph_b.json` compiled for [`LIBRARY`].
    pub const PLAN_B: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/plan_b.bin"));
}

pub use graph::Graph;
pub use tunable::TunableGraph;
