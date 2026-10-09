//! Integration tests of `daedalus-rs`: one module per area in a single test binary, so the
//! crate graph is linked and its generic code instantiated once rather than once per file.

mod adapt_macro;
mod direct_const_inputs;
mod embedded_host_port_types;
mod enum_config_ports;
mod example_imports;
mod execution_domain;
mod fanin_inputs_macro;
mod fire_all_joins;
mod foreign_interfaces;
mod foreign_type_keys;
mod graph_document;
mod host_graph_introspection;
mod numeric_keys;
mod optional_inputs;
mod registry_type_index;
mod same_id_instances;
mod stateful_node_isolation;
mod typed_host_ports;
