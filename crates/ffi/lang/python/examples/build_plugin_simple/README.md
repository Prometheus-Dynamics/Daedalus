# Python: Build Plugin (Simple)

Minimal example that **builds a Rust plugin library** (`.so`/`.dylib`/`.dll`) from Python-defined nodes.

## Run (from repo root)

```bash
python crates/ffi/lang/python/examples/build_plugin_simple/main.py /tmp/example_py_simple.so
```

## What this shows

- `Plugin.build(...)`: “build plugin, done” (no manifest emission unless you choose to).
- `@node_rs(...)`: Rust-like decorator ergonomics for defining ports/types from annotations.

