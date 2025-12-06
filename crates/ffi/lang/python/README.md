# Python FFI

Python SDK for authoring Daedalus nodes and building a Rust `.so` plugin.

## Layout

- `daedalus_py/` — tiny helper module with `Plugin`, `NodeDef`, and a `@node` decorator.
- `examples/` — usage samples (simple `build()` flow, plus advanced manifest emitters for tests).

## Quickstart

From repo root:

```bash
python crates/ffi/lang/python/examples/plugin_demo.py
```

This builds a Rust plugin library (default: workspace `target/*/examples/lib<name>.so`) without
leaving a manifest on disk.

Key points:
- Types are inferred from function annotations; override using `type_overrides` or explicit `inputs`/`outputs`.
- Prefer the simple flow: pass `plugin=...` to `@node(...)` so registration is automatic.
- Rich metadata, feature flags, compute affinity, and sync groups are preserved in the manifest.
- Parameter default values become per-port `const_value` entries in the manifest (e.g., `def scale(x: int, factor: int = 2)` yields a default of `2` on `factor`).
- Stateful nodes: set `stateful=True` on the decorator; the Rust adapter will invoke the Python function with JSON-serializable `state` and expect back `{"state": <new_state>, "outputs": <value_or_list>}`.
 - Example: see `plugin_demo.py` for a stateful accumulator node.
