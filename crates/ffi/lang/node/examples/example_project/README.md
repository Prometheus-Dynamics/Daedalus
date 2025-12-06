# Daedalus Node/TypeScript Example Project

This folder is a minimal, copyable starting point for authoring Daedalus nodes in **Node.js** (JS) or **TypeScript** and building a Rust plugin (`.so`/`.dylib`/`.dll`).

## What you get

- `js/`: plain Node.js example (no TS build step)
- `ts/`: TypeScript example using `@nodeMethod(...)` decorators + tsc-driven emitter

## JS quickstart (from repo root)

```bash
node crates/ffi/lang/node/examples/example_project/js/build_plugin.mjs /tmp/example_node.so
```

## TS quickstart (from repo root)

This performs: **tsc emit → (internal manifest) → bundle → build .so**.

```bash
node crates/ffi/lang/node/daedalus_node/tools/pack_ts_project.mjs \
  --project crates/ffi/lang/node/examples/example_project/ts/tsconfig.json \
  --plugin-name example_ts \
  --emit-dir /tmp/daedalus_ts_emit \
  --manifest /tmp/example_ts.manifest.json \
  --out-name example_ts_bundle
```

## Notes

- Node/TS nodes execute in a subprocess bridge; shaders execute on the Rust GPU side when the manifest includes shader specs.
- For advanced workflows you can still emit a `manifest.json` (see `js/emit_manifest.mjs`), but the default path is “build plugin, done”.
