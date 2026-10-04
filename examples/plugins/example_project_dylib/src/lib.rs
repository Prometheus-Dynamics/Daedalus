//! `examples/plugins/example_project` as a dynamic plugin: build with `--features dylib` and load
//! it with `daedalus::PluginLibrary::load("libdaedalus_plugins_example_project_dylib.so")`.
//!
//! The export lives in this leaf `cdylib` crate, not in the example crate, because other plugins
//! link the example crate: `export_plugin!`'s `#[no_mangle]` symbols in its `rlib` would clash
//! with those of every dependent plugin exporting itself.

#[cfg(feature = "dylib")]
daedalus::export_plugin!(daedalus_plugins_example_project::ExampleProjectPlugin);
