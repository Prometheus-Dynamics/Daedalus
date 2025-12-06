# Daedalus C/C++ Example Project

This folder is a minimal, copyable starting point for authoring Daedalus nodes in **C/C++** and emitting a `manifest.json` that Rust can load.

## What you get

- `nodes.cpp`: example nodes (stateless + multi-output + stateful) using the Daedalus C ABI.
- `build.sh`: builds a shared library and writes `example_cpp.manifest.json` next to it.

## Quickstart (from repo root)

```bash
bash crates/ffi/lang/c_cpp/examples/example_project/build.sh /tmp/example_cpp
```

This writes:

- `/tmp/example_cpp/example_cpp.manifest.json`
- `/tmp/example_cpp/libexample_cpp_nodes.{so|dylib|dll}` (platform-dependent)
- `/tmp/example_cpp/shaders/write_u32.wgsl` (shader used by `example_cpp:shader_write_u32`)

## Manifest-less loading (closer to Rust)

This dylib also exports `daedalus_cpp_manifest()`, so Rust can load it directly without any `manifest.json`:

- `daedalus_ffi::load_cpp_library_plugin("/tmp/example_cpp/libexample_cpp_nodes.so")`

## Notes

- C/C++ nodes execute in-process by `dlopen`/`dlsym` (no subprocess runtime).
- C/C++ nodes operate on JSON payloads and return JSON results; port types are still described by `TypeExpr` in the manifest so Rust can do conversions.
