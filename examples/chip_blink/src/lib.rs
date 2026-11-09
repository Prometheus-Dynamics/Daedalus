//! The blink graph as generated code, and nothing else. See `graph.json`.
//!
//! ```text
//! host.sample (latest) -> scale -> lowpass -> threshold -> rising -> host.rises (bounded 8)
//!                                     |            \-----> blink  -> host.led
//!                                     \-> host.level (latest)
//! ```
//!
//! In three modes, all from one graph:
//!
//! - [`graph`] is compiled with its constants as literals.
//! - [`tunable`] is compiled with four constants changeable while it runs.
//! - [`loaded`] is the node library and stored plans for the small runner.
//!
//! `fanin` and `fanin_tunable` add a `command` input that overrides the threshold.

#![no_std]
#![forbid(unsafe_code)]

/// `Graph`, `NODE_IDS` and `EDGES`.
pub mod graph {
    include!("../generated/graph.rs");
}

/// The compiled graph with `PARAM_NAMES`, `PARAMS` and `impl Tunable`.
pub mod tunable {
    include!("../generated/tunable.rs");
}

/// The graph with a `command` input.
pub mod fanin {
    include!("../generated/fanin.rs");
}

/// The fan-in graph with parameters.
pub mod fanin_tunable {
    include!("../generated/fanin_tunable.rs");
}

/// The node library and the stored plans.
pub mod loaded {
    pub use daedalus_chip_blink_nodes::LIBRARY;

    /// Arena bytes of the firmware's runner. On a 32-bit chip plan A needs 505 and plan B 418.
    pub const ARENA: usize = 512;
    pub type Interpreter = daedalus_cpu::blob::Interpreter<ARENA>;
    /// Arena bytes the native tests run with. A 64-bit host needs more than a chip, and the
    /// fan-in plans need 644 and 561 on a chip.
    pub const FANIN_ARENA: usize = 768;

    pub const PLAN_A: &[u8] = include_bytes!("../generated/plan_a.bin");
    pub const PLAN_B: &[u8] = include_bytes!("../generated/plan_b.bin");
    pub const FANIN_A: &[u8] = include_bytes!("../generated/fanin_a.bin");
    pub const FANIN_B: &[u8] = include_bytes!("../generated/fanin_b.bin");
}

pub use graph::Graph;
