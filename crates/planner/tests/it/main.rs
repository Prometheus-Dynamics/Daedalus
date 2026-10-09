//! Integration tests of `daedalus-planner`: one module per area in a single test binary, so the
//! crate graph is linked and its generic code instantiated once rather than once per file.

mod graph_document;
mod graph_document_schema;
mod node_execution_kind;
mod optional_inputs;
