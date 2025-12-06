# Python: Demo Runtime (Roundtrip)

Used by `crates/ffi/tests/manifest_subprocess_roundtrip.rs`.

## Files

- `defs.py`: node definitions (imports `daedalus_py`, emits ports/types/metadata).
- `rt.py`: runtime module executed by the subprocess bridge (kept minimal).
- `emit_manifest.py`: writes a manifest to a path you provide (defaults to a temp file).

## Run (from repo root)

```bash
python crates/ffi/lang/python/examples/demo_rt/emit_manifest.py /tmp/demo_py_rt.manifest.json
```

