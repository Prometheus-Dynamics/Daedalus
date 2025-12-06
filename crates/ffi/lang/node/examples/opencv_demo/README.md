# Node: OpenCV Demo

Demonstrates running an OpenCV operation (Gaussian blur) inside the **Node subprocess node**.

## Requirements

- An OpenCV binding such as `opencv4nodejs` available at runtime.

## Run (from repo root)

```bash
node crates/ffi/lang/node/examples/opencv_demo/emit_manifest.mjs /tmp/demo_node_opencv.manifest.json
```

## Notes

- The preferred input/output format is `encoding=="raw"` (raw pixel bytes). PNG is supported as a fallback.
- `runtime.mjs` is the module that actually runs; `emit_manifest.mjs` just describes ports/types and points at it.

