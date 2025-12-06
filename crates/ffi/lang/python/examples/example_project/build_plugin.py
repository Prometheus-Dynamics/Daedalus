"""
Build a Rust plugin (`.so`/`.dylib`/`.dll`) from Python nodes (no manifest output by default).

Run (from repo root):
  python crates/ffi/lang/python/examples/example_project/build_plugin.py /tmp/example_py.so
"""

from __future__ import annotations

import sys
from pathlib import Path

# Import the in-repo SDK directly.
REPO_ROOT = Path(__file__).resolve().parents[6]
PY_SDK = REPO_ROOT / "crates" / "ffi" / "lang" / "python"
sys.path.insert(0, PY_SDK.as_posix())

from daedalus_py import Plugin  # noqa: E402

from nodes import register_all  # noqa: E402


def main() -> None:
    out = Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else (Path.cwd() / "example_py.so")
    out.parent.mkdir(parents=True, exist_ok=True)

    plugin = Plugin(name="example_py", version="0.1.0", description="Python example project")
    register_all(plugin)

    # Point runtime at the nodes module.
    nodes_path = Path(__file__).with_name("nodes.py").resolve().as_posix()
    for n in plugin.nodes:
        n.py_function = n.id.split(":")[-1]
        n.py_path = nodes_path

    artifact = plugin.build(out_path=out, out_name="example_py", bundle=True, release=True)
    print(artifact.as_posix())


if __name__ == "__main__":
    main()
