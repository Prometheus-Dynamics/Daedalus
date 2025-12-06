"""
Emit a minimal manifest for round-trip tests (no Rust build step).
"""

from __future__ import annotations

import importlib.util
import os
import sys
import tempfile
from pathlib import Path

# Allow running directly from repo root.
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

if __name__ == "__main__":
    out = Path(tempfile.gettempdir()) / f"demo_py_rt_{os.getpid()}.manifest.json"
    if len(sys.argv) > 1:
        out = Path(sys.argv[1])
    defs_path = Path(__file__).with_name("defs.py")
    spec = importlib.util.spec_from_file_location("demo_py_rt_defs", defs_path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    mod.plugin.emit_manifest(out)
    print(out.as_posix())
