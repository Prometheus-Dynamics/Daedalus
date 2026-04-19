"""
Emit a rich manifest fixture for end-to-end tests (no Rust build step).

This writes:
- <out>.manifest.json (the manifest)
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from enum import Enum
from functools import partial
from pathlib import Path
from typing import Optional, Tuple

# Allow running directly from repo root.
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from daedalus_py import (  # noqa: E402
    Plugin,
    NodeDef,
    node_rs as node,
    port,
    sync_group,
    SyncPolicy,
    BackpressureStrategy,
    CvImage,
)


plugin = Plugin(name="demo_py_feat", version="1.0.0", description="Python feature fixture")
node = partial(node, plugin=plugin)

@dataclass
class Cfg:
    factor: int


@dataclass
class Point:
    x: int
    y: int


class Mode(Enum):
    A = 1
    B = Point(7, 9)


@dataclass
class AccumState:
    total: int

    @classmethod
    def daedalus_init(cls) -> AccumState:
        return cls(10)


@node(
    id="demo_py_feat:add_defaults",
    label="AddDefaults",
    metadata={"category": "math", "lang": "python"},
    inputs=("a", "b"),
    outputs=("out",),
)
def add_defaults(a: int = 2, b: int = 3) -> int:
    return a + b


@node(
    id="demo_py_feat:split",
    label="Split",
    feature_flags=["cpu"],
    metadata={"category": "math"},
    inputs=("value",),
    outputs=("out0", "out1"),
)
def split(value: int) -> Tuple[int, int]:
    return value, -value


@node(
    id="demo_py_feat:scale_cfg",
    label="ScaleCfg",
    metadata={"category": "config"},
    inputs=("value", "cfg"),
    outputs=("out",),
)
def scale_cfg(value: int, cfg: Cfg) -> int:
    return value * cfg.factor


@node(
    id="demo_py_feat:make_point",
    label="MakePoint",
    metadata={"category": "struct"},
    inputs=("x", "y"),
    outputs=("out",),
)
def make_point(x: int, y: int) -> Point:
    return Point(x, y)


@node(
    id="demo_py_feat:enum_mode",
    label="EnumMode",
    metadata={"category": "enum"},
    inputs=("value",),
    outputs=("out",),
)
def enum_mode(value: int) -> Mode:
    return Mode.A if value >= 0 else Mode.B


@node(
    id="demo_py_feat:sync_a_only",
    label="SyncAOnly",
    sync_groups=[["a"]],
    metadata={"category": "sync"},
    inputs=("a", "b"),
    outputs=("out",),
)
def sync_a_only(a: int, b: Optional[int]) -> int:
    return a


@node(
    id="demo_py_feat:sync_a_only_obj",
    label="SyncAOnlyObj",
    sync_groups=[
        sync_group(
            "a",
            name="a_only",
            policy=SyncPolicy.Latest,
            backpressure=BackpressureStrategy.ErrorOnOverflow,
            capacity=2,
        )
    ],
    metadata={"category": "sync"},
    inputs=("a", "b"),
    outputs=("out",),
)
def sync_a_only_obj(a: int, b: Optional[int]) -> int:
    return a


@node(
    id="demo_py_feat:ctx_echo",
    label="CtxEcho",
    metadata={"category": "ctx"},
    inputs=("text",),
    outputs=("out",),
)
def ctx_echo(text: str, ctx=None, node=None) -> str:
    nid = None
    try:
        nid = (node or {}).get("id")
    except Exception:
        nid = None
    return f"{text}|{nid}"


@node(
    id="demo_py_feat:choose_mode_meta",
    label="ChooseModeMeta",
    metadata={"category": "meta"},
    input_ports=[port("mode", source="modes", default="quality")],
    outputs=("out",),
)
def choose_mode_meta(mode: str) -> str:
    return f"mode={mode}"


@node(
    id="demo_py_feat:accum",
    label="Accum",
    state=AccumState,
    metadata={"category": "state"},
    inputs=("value",),
    outputs=("out",),
)
def accum(value: int, state: AccumState) -> Tuple[AccumState, int]:
    new_state = AccumState(state.total + value)
    return new_state, new_state.total


@node(
    id="demo_py_feat:gpu_required_placeholder",
    label="GpuRequiredPlaceholder",
    default_compute="GpuRequired",
    metadata={"category": "gpu"},
    inputs=("x",),
    outputs=("out",),
)
def gpu_required_placeholder(x: int = 1) -> int:
    return x


@node(
    id="demo_py_feat:shader_invert",
    label="ShaderInvert",
    shader={"src_path": "invert.wgsl", "entry": "main", "name": "invert"},
    metadata={"category": "gpu"},
    inputs=("img",),
    outputs=("img",),
)
def shader_invert(img: CvImage) -> CvImage:
    return img


@node(
    id="demo_py_feat:shader_write_u32",
    label="ShaderWriteU32",
    shader={
        "src_path": "write_u32.wgsl",
        "entry": "main",
        "name": "write_u32",
        "invocations": [1, 1, 1],
        "bindings": [
            {
                "binding": 0,
                "kind": "storage_buffer",
                "access": "read_write",
                "readback": True,
                "to_port": "out",
                "size_bytes": 4,
            }
        ],
    },
    metadata={"category": "gpu"},
    inputs=tuple(),
    outputs=("out",),
)
def shader_write_u32() -> bytes:
    return b""

@node(
    id="demo_py_feat:shader_counter",
    label="ShaderCounter",
    stateful=True,
    shader={
        "src_path": "counter.wgsl",
        "entry": "main",
        "name": "counter",
        "invocations": [1, 1, 1],
        "bindings": [
            {
                "binding": 0,
                "kind": "storage_buffer",
                "access": "read_write",
                "from_state": "counter",
                "to_state": "counter",
                "readback": True,
                "to_port": "out",
                "size_bytes": 4,
            }
        ],
    },
    metadata={"category": "gpu"},
    inputs=tuple(),
    outputs=("out",),
)
def shader_counter() -> bytes:
    return b""

@node(
    id="demo_py_feat:shader_counter_cpu",
    label="ShaderCounterCpu",
    stateful=True,
    shader={
        "src_path": "counter.wgsl",
        "entry": "main",
        "name": "counter_cpu",
        "invocations": [1, 1, 1],
        "bindings": [
            {
                "binding": 0,
                "kind": "storage_buffer",
                "access": "read_write",
                "from_state": "counter_cpu",
                "to_state": "counter_cpu",
                "readback": True,
                "to_port": "out",
                "size_bytes": 4,
            }
        ],
    },
    metadata={"category": "gpu"},
    inputs=tuple(),
    outputs=("out",),
)
def shader_counter_cpu() -> bytes:
    return b""

@node(
    id="demo_py_feat:shader_counter_gpu",
    label="ShaderCounterGpu",
    stateful=True,
    shader={
        "src_path": "counter.wgsl",
        "entry": "main",
        "name": "counter_gpu",
        "invocations": [1, 1, 1],
        "bindings": [
            {
                "binding": 0,
                "kind": "storage_buffer",
                "access": "read_write",
                "state_backend": "gpu",
                "from_state": "counter_gpu",
                "readback": True,
                "to_port": "out",
                "size_bytes": 4,
            }
        ],
    },
    metadata={"category": "gpu"},
    inputs=tuple(),
    outputs=("out",),
)
def shader_counter_gpu() -> bytes:
    return b""

@node(
    id="demo_py_feat:shader_multi_write",
    label="ShaderMultiWrite",
    shader={
        "shaders": [
            {"name": "one", "src_path": "write_one.wgsl", "entry": "main"},
            {"name": "two", "src_path": "write_two.wgsl", "entry": "main"},
        ],
        "dispatch_from_port": "which",
        "invocations": [1, 1, 1],
        "bindings": [
            {
                "binding": 0,
                "kind": "storage_buffer",
                "access": "read_write",
                "readback": True,
                "to_port": "out",
                "size_bytes": 4,
            }
        ],
    },
    metadata={"category": "gpu"},
    inputs=("which",),
    outputs=("out",),
)
def shader_multi_write(which: str) -> bytes:
    return b""

@node(
    id="demo_py_feat:multi_emit",
    label="MultiEmit",
    raw_io=True,
    metadata={"category": "raw_io"},
    inputs=tuple(),
    outputs=("out",),
)
def multi_emit(io=None) -> int:
    if io is None:
        return 0
    io.push("out", 1)
    io.push("out", 2)
    return 0

cap_add = NodeDef(
    id="demo_py_feat:cap_add",
    label="CapAdd",
    capability="Add",
    metadata={"category": "capability"},
    inputs=[{"name": "a", "ty": {"Scalar": "Int"}}, {"name": "b", "ty": {"Scalar": "Int"}}],
    outputs=[{"name": "out", "ty": {"Scalar": "Int"}}],
)

plugin.register(cap_add)


if __name__ == "__main__":
    import os
    import tempfile

    out = Path(tempfile.gettempdir()) / f"demo_py_feat_{os.getpid()}.manifest.json"
    if len(sys.argv) > 1:
        out = Path(sys.argv[1])

    out.parent.mkdir(parents=True, exist_ok=True)
    shader_dir = ROOT.parent / "shaders"
    for name in ["invert.wgsl", "write_u32.wgsl", "counter.wgsl", "write_one.wgsl", "write_two.wgsl"]:
        (out.parent / name).write_bytes((shader_dir / name).read_bytes())

    plugin.emit_manifest(out)
    print(out.as_posix())
