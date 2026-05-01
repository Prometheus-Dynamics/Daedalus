# Python FFI Complex Plugin

`ffi_showcase.py` mirrors the Rust baseline with decorators, dataclasses, type-key metadata,
state objects, payload helpers, raw events, package artifacts, and typed errors.

Package build:

```bash
python build_package.py
```

How close to Rust: Python can keep node bodies compact, but explicit decorators remain necessary
for port names, package artifacts, payload ownership, and boundary contracts.

Additional requested node shapes are included in `ffi_showcase.py`: `array_dynamic_sum`,
`node_io_complex`, `gpu_tint`, `internal_adapter_consume`, `external_adapter_consume`,
`zero_copy_len`, `shared_ref_len`, `cow_append_marker`, `mutable_brighten`, and
`owned_bytes_len`.
