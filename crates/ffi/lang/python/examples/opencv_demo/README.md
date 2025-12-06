# Python: OpenCV Demo

Demonstrates running an OpenCV operation (Gaussian blur) inside the **Python subprocess node**.

## Requirements

- `opencv-python` (`cv2`)
- `numpy`

## Run (from repo root)

```bash
python crates/ffi/lang/python/examples/opencv_demo/emit_manifest.py /tmp/demo_py_opencv.manifest.json
```

## Notes

- The preferred input/output format is `encoding=="raw"` (raw pixel bytes). PNG is supported as a fallback.
- The actual runtime is `rt.py`; `defs.py` only defines ports/types and points the node at the runtime file.

