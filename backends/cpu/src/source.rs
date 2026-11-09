//! The source form: a region as a Rust module that calls each node's function directly.

mod emit;
mod graph_struct;
#[cfg(feature = "tunable")]
mod tunable;

pub use emit::SourceBackend;
