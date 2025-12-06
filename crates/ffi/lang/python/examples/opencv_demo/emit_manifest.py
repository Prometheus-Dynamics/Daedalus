"""
Emit a manifest that demonstrates passing an image-like payload into Python, processing it with
OpenCV, and returning it back to Rust.

Notes:
- Requires `opencv-python` at runtime (cv2 import).
- The cross-language image carrier is a small struct with base64 bytes; prefer `encoding=="raw"`.
"""

from __future__ import annotations

import importlib.util
import os
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

if __name__ == "__main__":
    out = Path(tempfile.gettempdir()) / f"demo_py_opencv_{os.getpid()}.manifest.json"
    if len(sys.argv) > 1:
        out = Path(sys.argv[1])
    out = out.resolve()

    defs_path = Path(__file__).with_name("defs.py")
    spec = importlib.util.spec_from_file_location("demo_py_opencv_defs", defs_path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    mod.plugin.emit_manifest(out)
    print(out.as_posix())
