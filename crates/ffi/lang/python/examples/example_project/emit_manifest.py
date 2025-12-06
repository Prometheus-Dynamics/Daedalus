"""
Emit a Daedalus manifest from Python nodes.

Run (from repo root):
  python crates/ffi/lang/python/examples/example_project/emit_manifest.py /tmp/example_py.manifest.json

This writes a manifest and copies any shader files next to it.
"""

from __future__ import annotations

import sys
import os
import tempfile
from pathlib import Path

# Import the in-repo SDK directly.
REPO_ROOT = Path(__file__).resolve().parents[6]
PY_SDK = REPO_ROOT / "crates" / "ffi" / "lang" / "python"
sys.path.insert(0, PY_SDK.as_posix())

from daedalus_py import Plugin  # noqa: E402

from nodes import register_all  # noqa: E402

def copy_shaders(out_dir: Path) -> None:
    src_dir = Path(__file__).with_name("shaders")
    dst_dir = out_dir / "shaders"
    dst_dir.mkdir(parents=True, exist_ok=True)
    for p in src_dir.glob("*.wgsl"):
        (dst_dir / p.name).write_text(p.read_text(encoding="utf-8"), encoding="utf-8")


def main() -> None:
    out = Path(tempfile.gettempdir()) / f"example_py_{os.getpid()}.manifest.json"
    if len(sys.argv) > 1:
        out = Path(sys.argv[1])
    out_dir = out.parent

    copy_shaders(out_dir)

    plugin = Plugin(name="example_py", version="0.1.0", description="Python example project")
    register_all(plugin)

    # Point nodes at a stable module file (no codegen step).
    nodes_path = Path(__file__).with_name("nodes.py").resolve().as_posix()
    for n in plugin.nodes:
        n.py_function = n.id.split(":")[-1]
        n.py_path = nodes_path

    # Optional: add a shader to a node by setting n.shader = {...} or using helper APIs.
    # Keep it file-backed: src_path should point to a real file next to the manifest.
    #
    # Example (not registered by default):
    # some_node.shader = {
    #   "src_path": "shaders/invert.wgsl",
    #   "entry": "main",
    #   "name": "invert",
    #   "invocations": [1, 1, 1],
    #   "bindings": [
    #     {"binding": 0, "kind": "storage_buffer", "access": "read_write", "readback": True, "to_port": "out", "size_bytes": 4},
    #   ],
    # }

    plugin.emit_manifest(out)
    print(out.as_posix())


if __name__ == "__main__":
    main()
