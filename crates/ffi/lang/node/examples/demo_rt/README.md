# Node: Demo Runtime (Roundtrip)

Used by `crates/ffi/tests/manifest_subprocess_roundtrip.rs`.

## Run (from repo root)

```bash
node crates/ffi/lang/node/examples/demo_rt/emit_manifest.mjs /tmp/demo_node.manifest.json
```

## Files

- `emit_manifest.mjs`: defines and emits the manifest (defaults to a temp file).
- `node_demo_module.mjs`: runtime module executed by the subprocess bridge.

