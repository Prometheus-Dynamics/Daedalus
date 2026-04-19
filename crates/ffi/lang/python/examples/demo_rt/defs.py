from __future__ import annotations

from pathlib import Path

from daedalus_py import Plugin, node_rs as node

plugin = Plugin(name="demo_py_rt", version="1.0.0", description="Roundtrip demo")

# This example keeps “definitions” (this file) separate from “runtime” (`rt.py`).
# The subprocess bridge only needs the runtime file when executing nodes.
RUNTIME_PATH = Path(__file__).with_name("rt.py").resolve().as_posix()


@node(
    plugin=plugin,
    id="demo_py_rt:add",
    inputs=("a", "b"),
    outputs=("out",),
    py_path=RUNTIME_PATH,
    py_function="add",
)
def add(a: int = 2, b: int = 3) -> int:
    return a + b
