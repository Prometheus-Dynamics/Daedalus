# Daedalus FFI

Language-specific FFI surfaces are split by subdirectory.

- `lang/rust/` — Rust-authored plugins built as `cdylib` and loaded via `PluginLibrary`.
  - Example plugin: `crates/ffi/examples/plugin_lib.rs`
  - Host runner: `crates/ffi/examples/run_rust_plugin.rs`
- `lang/python/` — Python-side manifest emitter (`daedalus_py`) plus sample plugin.
  - Manifest loader example: `crates/ffi/examples/load_python_manifest.rs` (uses the language-dispatching loader)
- `lang/node/` — Node.js-side manifest emitter (`daedalus_node`) plus sample plugin.
  - Manifest loader example: `crates/ffi/examples/load_python_manifest.rs` (uses the language-dispatching loader)
- Mixed demo: `crates/ffi/examples/run_mixed_plugin.rs` loads the Rust plugin_lib and the Python manifest and runs a graph spanning both.
- `lang/java/` — Java-side manifest emitter + subprocess bridge.
- `lang/c_cpp/` — C/C++ manifest emitter + shared-library bridge.
