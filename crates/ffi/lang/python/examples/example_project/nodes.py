"""
Daedalus Python nodes: minimal patterns.

This file is meant to be copied into your own repo/project.

Key idea:
- You define Python callables and annotate them with the Daedalus node decorator.
- A separate emitter script builds a Plugin and writes a manifest.json.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    from daedalus_py import Plugin, node, t
except ModuleNotFoundError:
    # Allow running directly from this repo without installing the Python SDK.
    # If you copy this file into your own project, install `daedalus_py` and this
    # fallback won't trigger.
    PY_SDK = Path(__file__).resolve().parents[1]
    sys.path.insert(0, PY_SDK.as_posix())
    from daedalus_py import Plugin, node, t  # type: ignore


@node(
    id="example_py:add",
    inputs=[{"name": "a", "ty": t.int()}, {"name": "b", "ty": t.int()}],
    outputs=[{"name": "out", "ty": t.int()}],
    metadata={"lang": "python", "kind": "stateless"},
)
def add(a: int, b: int) -> int:
    return int(a) + int(b)


@dataclass
class CounterState:
    """State type for a stateful node."""

    value: int

    @staticmethod
    def init(state_spec: Dict[str, Any]) -> "CounterState":
        # state_spec comes from the node's `state` field in the manifest.
        start = int(state_spec.get("start", 0))
        return CounterState(value=start)


@node(
    id="example_py:counter",
    inputs=[{"name": "inc", "ty": t.int()}],
    outputs=[{"name": "out", "ty": t.int()}],
    stateful=True,
    state={"start": 0},
    metadata={"lang": "python", "kind": "stateful"},
)
def counter(inv: Dict[str, Any]):
    """
    Stateful node invocation contract:
      inv = { args: [...], state: <previous state>, state_spec: <node.state> }

    You can return:
      { state: <new state>, outputs: <outputs> }
    or:
      [<new state>, <outputs>]
    or:
      <outputs> (state preserved).
    """

    args: List[Any] = inv["args"]
    state_spec: Dict[str, Any] = inv.get("state_spec", {}) or {}

    # State is JSON-shaped. If you want a typed object, reconstruct it.
    st_json = inv.get("state")
    if st_json is None:
        st = CounterState.init(state_spec)
    else:
        st = CounterState(value=int(st_json["value"]))

    inc = int(args[0])
    st.value += inc

    return {"state": {"value": st.value}, "outputs": {"out": st.value}}


@node(
    id="example_py:split_optional",
    inputs=[{"name": "value", "ty": t.optional(t.int())}],
    outputs=[{"name": "out0", "ty": t.optional(t.int())}, {"name": "out1", "ty": t.optional(t.int())}],
    metadata={"lang": "python", "kind": "optional"},
)
def split_optional(value: Optional[int]) -> Tuple[Optional[int], Optional[int]]:
    if value is None:
        return None, None
    v = int(value)
    return v, -v


def register_all(plugin: Plugin) -> None:
    """
    Convenience for an emitter script:
    register all nodes defined in this module into the Plugin.
    """

    for fn in [add, counter, split_optional]:
        plugin.register(fn.__daedalus_node__)
