//! The MCU blink graph (`graph.json`), planned on the host into `generated/` (by
//! `examples/generate.rs`, checked by `tests/generated.rs`), in the three device modes:
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
//! The fan-in graph (`graph_fanin.json`, `graph_fanin_b.json`) adds a host `command` byte through
//! the user adapter `command_active` as the first of two edges into `blink.active`; the threshold
//! feeds the second, so the command wins while it is pushed:
//!
//! ```text
//! host.command (u8) --command_active--\
//!                                      > blink.active  (fan-in, plan order)
//! host.sample -> scale -> lowpass -> threshold --------/
//! ```
//!
//! - [`fanin`], [`fanin_tunable`], [`fanin_loaded`]: the same three modes for this graph.
//!
//! The application pushes raw ADC counts into `sample`, ticks and pops `led`, `level` and
//! `rises`. Everything lives in the graph value (or the interpreter's arena), with no heap.
#![no_std]

pub use daedalus_mcu_blink_nodes as nodes;

/// The compiled plan: `Graph`, `NODE_IDS`, `EDGES`, `PLAN_HASH`.
pub mod graph {
    include!("../generated/graph.rs");
}

/// The compiled plan with tunable parameters: `TunableGraph`, `PARAM_NAMES`, `PARAMS`.
pub mod tunable {
    include!("../generated/tunable.rs");
}

/// Loaded mode: the node library and the plan blobs.
pub mod loaded {
    include!("../generated/library.rs");

    /// Arena bytes of the firmware's interpreter (both plans fit; the native tests check).
    pub const ARENA: usize = 512;
    /// The interpreter type of the firmware.
    pub type Interpreter = daedalus_mcu::loaded::Interpreter<ARENA>;
    /// `graph.json` compiled for [`LIBRARY`].
    pub const PLAN_A: &[u8] = include_bytes!("../generated/plan_a.bin");
    /// `graph_b.json` compiled for [`LIBRARY`].
    pub const PLAN_B: &[u8] = include_bytes!("../generated/plan_b.bin");
}

/// Compiled mode of the fan-in graph: `FaninGraph`, constants frozen.
pub mod fanin {
    include!("../generated/fanin.rs");
}

/// Compiled + tunable, fan-in graph: `FaninTunableGraph`.
pub mod fanin_tunable {
    include!("../generated/fanin_tunable.rs");
}

/// Loaded mode of the fan-in graph: the library (with the `command_active` adapter) and the two
/// plan blobs.
pub mod fanin_loaded {
    include!("../generated/fanin_library.rs");

    /// Arena bytes of the fan-in firmware's interpreter (the native tests check both plans fit).
    pub const ARENA: usize = 512;
    /// The interpreter type of the fan-in graph.
    pub type Interpreter = daedalus_mcu::loaded::Interpreter<ARENA>;
    /// `graph_fanin.json` compiled for [`LIBRARY`]: the command is the first edge of `blink.active`.
    pub const PLAN_A: &[u8] = include_bytes!("../generated/fanin_plan_a.bin");
    /// `graph_fanin_b.json`: the threshold is the first edge; the command supplies only the ticks
    /// on which the threshold has no value.
    pub const PLAN_B: &[u8] = include_bytes!("../generated/fanin_plan_b.bin");
}

pub use graph::Graph;
pub use tunable::TunableGraph;
