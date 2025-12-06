# FFI Examples

This folder now only demonstrates loading Rust-authored plugins through the FFI boundary.

- `plugin_lib.rs`: Rust plugin built as a `cdylib` using the public `daedalus` facade and `export_plugin!`.
- `run_rust_plugin.rs`: Host that builds (if needed) and loads `plugin_lib`, then runs the demo graph.
