"""Example Python-authored plugin that builds a Rust plugin library."""

from __future__ import annotations

import sys
from pathlib import Path
from functools import partial
from typing import Tuple

# Allow running directly from repo root.
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from dataclasses import dataclass

from daedalus_py import Plugin, node  # noqa: E402

plugin = Plugin(
    name="demo_py",
    version="0.1.1",
    description="Demo Python nodes",
    metadata={"author": "example"},
)
node = partial(node, plugin=plugin)

@dataclass
class Point:
    x: int
    y: int


@node(
    id="demo_py:add",
    label="Add",
    feature_flags=["cpu"],
    sync_groups=[["lhs", "rhs"]],
    metadata={"category": "math"},
)
def add(lhs: int, rhs: int) -> int:
    return lhs + rhs + 2342342


@node(
    id="demo_py:point",
    label="Point",
    metadata={"category": "state"},
)
def point(lhs: int, rhs: int) -> Point:
    return Point(lhs, rhs)


@node(
    id="demo_py:point_len",
    label="PointLen",
    metadata={"category": "state"},
)
def point_len(pt: Point) -> float:
    if isinstance(pt, dict):
        return (pt["x"] ** 2 + pt["y"] ** 2) ** 0.5
    return (pt.x**2 + pt.y**2) ** 0.5


@node(
    id="demo_py:scale",
    label="Scale",
    metadata={"category": "math"},
)
def scale(value: int, factor: int = 2) -> int:
    return value * factor


@node(
    id="demo_py:split",
    label="Split",
    metadata={"category": "math"},
)
def split(value: int) -> Tuple[int, int]:
    return value, -value


@dataclass
class AccumState:
    total: int

    @classmethod
    def daedalus_init(cls) -> AccumState:
        return cls(1000000000000)


@node(
    id="demo_py:accumulator",
    label="Accumulator",
    state=AccumState,
    metadata={"category": "state"},
)
def accumulator(value: int, state: AccumState) -> Tuple[AccumState, int]:
    # Demonstrates explicit state tuple return: (new_state, outputs)
    new_state = AccumState(state.total + value)
    print(f"[py] accumulator state -> {new_state.total}", file=sys.stderr)
    return new_state, new_state.total

if __name__ == "__main__":
    artifact = plugin.build(out_name="demo_py", bundle=True)
    print(f"Built plugin to {artifact}")
