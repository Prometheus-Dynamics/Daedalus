# Daedalus Python Example Project

This folder is a minimal, copyable starting point for authoring Daedalus nodes in **Python** and building a Rust plugin (`.so`/`.dylib`/`.dll`).

## What you get

- `nodes.py`: example nodes (stateless + stateful + optional shader wiring)
- `build_plugin.py`: builds a plugin library (default path is up to you)
- `emit_manifest.py`: advanced: emits `example_py.manifest.json` for debugging/tests

## Quickstart (from repo root)

```bash
python crates/ffi/lang/python/examples/example_project/build_plugin.py /tmp/example_py.so
```

## Notes

- Python nodes execute in a subprocess bridge: the GPU work (WGSL) is done in Rust when the manifest includes shader specs.
- The Python side is responsible for describing ports/types/defaults/state and (optionally) pointing shader specs at WGSL files via `src_path`.
