//! Integration tests of `daedalus-engine`: one module per area in a single test binary, so the
//! crate graph is linked and its generic code instantiated once rather than once per file.

mod config_validation;
mod graph_document;
mod host_graph_events;
mod host_graph_poll;
mod transport_run;
