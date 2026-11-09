//! The source form: a region as a Rust module that calls each node's function directly.

mod emit;
mod graph_struct;

pub use emit::SourceBackend;
