//! `examples/plugins/node_groups` as a dynamic plugin (the `dylib` feature builds the export).

#[cfg(feature = "dylib")]
daedalus::export_plugin!(daedalus_plugins_node_groups::NodeGroupsPlugin);
