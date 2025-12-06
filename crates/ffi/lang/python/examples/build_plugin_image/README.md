# Python: Build Plugin (Image)

Builds a Rust plugin library from Python-defined nodes that accept/return the cross-language image carrier.

## Run (from repo root)

```bash
python crates/ffi/lang/python/examples/build_plugin_image/main.py /tmp/example_py_image.so
```

## Notes

- Image payloads are passed across the subprocess boundary as a small struct with base64 bytes.
- For high-performance paths prefer `encoding=="raw"` (raw pixels); PNG is supported only for compatibility.

