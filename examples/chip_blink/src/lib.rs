//! The blink graph as generated code, and nothing else. See `graph.json`.
//!
//! ```text
//! host.sample (latest) -> scale -> lowpass -> threshold -> rising -> host.rises (bounded 8)
//!                                     |            \-----> blink  -> host.led
//!                                     \-> host.level (latest)
//! ```

#![no_std]
#![forbid(unsafe_code)]

/// `Graph`, `NODE_IDS` and `EDGES`.
pub mod graph {
    include!("../generated/graph.rs");
}

pub use graph::Graph;
