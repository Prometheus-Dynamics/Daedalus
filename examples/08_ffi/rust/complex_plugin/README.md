# Rust FFI Complex Plugin

`src/lib.rs` is the native baseline for the cross-language showcase. It uses Rust macro-style
declarations for typed ports, state, multi-output nodes, payloads, custom type keys, boundary
contracts, raw events, package artifacts, and typed errors.

Package build:

```bash
cargo run --package ffi-showcase-rust --bin build-package
```

How close to Rust: this is the reference. The remaining work is to connect the package builder to
the generated host smoke test once the example packages are executable fixtures.

Additional requested node shapes are included in `src/lib.rs`: `array_dynamic_sum`,
`node_io_complex`, `gpu_tint`, `internal_adapter_consume`, `external_adapter_consume`,
`zero_copy_len`, `shared_ref_len`, `cow_append_marker`, `mutable_brighten`, and
`owned_bytes_len`.
