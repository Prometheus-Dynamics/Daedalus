# Daedalus Rust FFI Example Project (cdylib plugin)

This folder is a minimal, copyable starting point for authoring Daedalus nodes in **Rust** as a **dynamic plugin** (`cdylib`) that the FFI loader can load via `PluginLibrary`.

Unlike Python/Node/Java, Rust plugins do **not** need a `manifest.json` — they export a plugin symbol directly.

## What you get

- `Cargo.toml`: template crate config for a `cdylib`
- `src/lib.rs`: fully-commented nodes + plugin export (`export_plugin!`)
- `assets/write_u32.wgsl`: tiny GPU shader used by an optional GPU node

## Use it

This directory is a *template*, not a workspace member. To use it:

1) Copy it somewhere (or rename it) and add it to your workspace members.
2) Build the dynamic library:

```bash
cargo build -p daedalus_rust_ffi_example
```

GPU build (adds `example_rust:write_u32_gpu`):

```bash
cargo build -p daedalus_rust_ffi_example --features gpu-wgpu
```

3) Load it with the host runner example:

```bash
cargo run -p daedalus-ffi --example run_rust_plugin -- path/to/target/debug/libdaedalus_rust_ffi_example.{so|dylib|dll}
```

## References in this repo

- Minimal plugin/host examples: `crates/ffi/examples/plugin_lib.rs`, `crates/ffi/examples/run_rust_plugin.rs`
- Signature matrix (many Rust `#[node]` shapes): `crates/ffi/examples/ffi_signature_matrix.rs`
