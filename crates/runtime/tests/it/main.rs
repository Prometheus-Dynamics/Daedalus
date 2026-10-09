//! Integration tests of `daedalus-runtime`: one module per area in a single test binary, so the
//! crate graph is linked and its generic code instantiated once rather than once per file.
//! `adaptive_mode` (wall-clock timing assertions) keeps its own binary.

mod backpressure;
mod executor;
mod executor_gpu;
mod fusion_equivalence;
mod graph_document_requirements;
mod node_error;
mod node_io_ports;
mod order_property;
mod parallel_invariants;
mod plugin_system;
mod policies;
mod runtime_plan;
mod runtime_plan_golden;
mod runtime_policy_application;
mod snapshot;
mod stream_graph;
mod stream_host_bridge;
