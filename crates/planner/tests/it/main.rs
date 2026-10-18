//! Integration tests of `daedalus-planner`: one module per area in a single test binary, so the
//! crate graph is linked and its generic code instantiated once rather than once per file.

mod adapter_routing_scale;
mod graph_document;
mod graph_document_schema;
mod malformed_declarations;
mod node_execution_kind;
mod optional_inputs;
