# C++ FFI Complex Plugin

`src/showcase.cpp` mirrors the Rust baseline through the C++ header macro target and keeps the
runtime on the in-process ABI path.

Package build:

```bash
cmake --build build --target ffi_showcase_package
```

How close to Rust: C++ requires explicit ABI, ownership, and serializer declarations. Node bodies
remain ordinary functions once the macro metadata is declared.

Additional requested node shapes are included in `src/showcase.cpp`: `array_dynamic_sum`,
`node_io_complex`, `gpu_tint`, `internal_adapter_consume`, `external_adapter_consume`,
`zero_copy_len`, `shared_ref_len`, `cow_append_marker`, `mutable_brighten`, and
`owned_bytes_len`.
