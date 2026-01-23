from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Dict, List

from daedalus_py import Plugin, node_rs as node

plugin = Plugin(name="demo_py_struct", version="0.1.1", description="Structured demo")

# This example keeps “definitions” (this file) separate from “runtime” (`rt.py`).
# The subprocess bridge only needs the runtime file when executing nodes.
RUNTIME_PATH = Path(__file__).with_name("rt.py").resolve().as_posix()


@dataclass
class Point:
    x: int
    y: int


class Mode(Enum):
    A = 1
    B = Point(0, 0)


@node(
    plugin=plugin,
    id="demo_py_struct:translate_point",
    inputs=("pt", "dx", "dy"),
    outputs=("out",),
    py_path=RUNTIME_PATH,
    py_function="translate_point",
)
def translate_point(pt: Point, dx: int = 1, dy: int = -1) -> Point:
    return Point(pt.x + dx, pt.y + dy)


@node(
    plugin=plugin,
    id="demo_py_struct:flip_mode",
    inputs=("mode",),
    outputs=("out",),
    py_path=RUNTIME_PATH,
    py_function="flip_mode",
)
def flip_mode(mode: Mode) -> Mode:
    name = mode.get("name") if isinstance(mode, dict) else None
    if name == "A":
        return {"name": "B", "value": {"x": 7, "y": 9}}
    return {"name": "A", "value": 1}


@node(
    plugin=plugin,
    id="demo_py_struct:map_len",
    inputs=("m",),
    outputs=("out",),
    py_path=RUNTIME_PATH,
    py_function="map_len",
)
def map_len(m: Dict[str, int]) -> int:
    return len(m)


@node(
    plugin=plugin,
    id="demo_py_struct:list_sum",
    inputs=("items",),
    outputs=("out",),
    py_path=RUNTIME_PATH,
    py_function="list_sum",
)
def list_sum(items: List[int]) -> int:
    return int(sum(items))
