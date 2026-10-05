//! The MCU blink graph (`graph.json`), planned on the host by `build.rs`:
//!
//! ```text
//! host.sample (u16, latest-only) --widen--> scale --> lowpass --> threshold --> rising --> host.rises (bounded 8)
//!                                                         |             \-----> blink  --> host.led   (latest-only)
//!                                                         \--> host.level (latest-only)
//! ```
//!
//! The application pushes raw ADC counts with `push_sample`, calls `tick` and pops `led`,
//! `level` and `rises`. Everything lives in [`Graph`]: one fixed-capacity queue per edge and one
//! state slot per node, with no heap.
#![no_std]

pub use daedalus_mcu_blink_nodes as nodes;

/// The generated plan: `Graph`, `NODE_IDS`, `EDGES`, `PLAN_HASH`.
pub mod graph {
    include!(concat!(env!("OUT_DIR"), "/graph.rs"));
}

pub use graph::Graph;
