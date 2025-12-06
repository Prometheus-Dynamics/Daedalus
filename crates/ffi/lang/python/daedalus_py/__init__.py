"""
Lightweight helpers for describing Daedalus nodes in Python and emitting a manifest
JSON that the Rust-side generator can consume.
"""

from __future__ import annotations

import inspect
import hashlib
import json
import os
import platform
import shutil
import subprocess
import sys
import importlib
import importlib.util
from enum import Enum
import base64
from dataclasses import dataclass, field, fields, is_dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Union, get_args, get_origin, get_type_hints

# Rust enums (serde) look like {"Scalar":"Int"} or {"Optional":{"Scalar":"Int"}}
_SCALAR_TYPES: Dict[Any, str] = {
    int: "Int",
    float: "Float",
    bool: "Bool",
    str: "String",
    bytes: "Bytes",
    type(None): "Unit",
}

class SyncPolicy(str, Enum):
    AllReady = "AllReady"
    Latest = "Latest"
    ZipByTag = "ZipByTag"


class BackpressureStrategy(str, Enum):
    NONE = "None"
    BoundedQueues = "BoundedQueues"
    ErrorOnOverflow = "ErrorOnOverflow"

    # A slightly more Rust-like alias (`None` is reserved in Python).
    None_ = "None"


@dataclass
class SyncGroup:
    ports: List[str]
    name: Optional[str] = None
    policy: SyncPolicy = SyncPolicy.AllReady
    backpressure: Optional[BackpressureStrategy] = None
    capacity: Optional[int] = None

    def to_manifest(self) -> Dict[str, Any]:
        doc: Dict[str, Any] = {
            "ports": list(self.ports),
            "policy": self.policy.value if isinstance(self.policy, Enum) else self.policy,
        }
        if self.name is not None:
            doc["name"] = self.name
        if self.backpressure is not None:
            doc["backpressure"] = (
                self.backpressure.value
                if isinstance(self.backpressure, Enum)
                else self.backpressure
            )
        if self.capacity is not None:
            doc["capacity"] = int(self.capacity)
        return doc


def sync_group(
    *ports: str,
    name: Optional[str] = None,
    policy: SyncPolicy = SyncPolicy.AllReady,
    backpressure: Optional[BackpressureStrategy] = None,
    capacity: Optional[int] = None,
) -> SyncGroup:
    return SyncGroup(
        ports=list(ports),
        name=name,
        policy=policy,
        backpressure=backpressure,
        capacity=capacity,
    )


def _normalize_json(obj: Any) -> Any:
    if obj is None:
        return None
    if isinstance(obj, Enum):
        return obj.value
    if is_dataclass(obj):
        if hasattr(obj, "to_manifest") and callable(getattr(obj, "to_manifest")):
            return _normalize_json(obj.to_manifest())
        out: Dict[str, Any] = {}
        for f in fields(obj):
            out[f.name] = _normalize_json(getattr(obj, f.name))
        return out
    if isinstance(obj, dict):
        return {k: _normalize_json(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_normalize_json(v) for v in obj]
    return obj


def port(name: str, *, source: Optional[str] = None, default: Any = inspect._empty) -> Dict[str, Any]:
    p: Dict[str, Any] = {"name": name}
    if source is not None:
        p["source"] = source
    if default is not inspect._empty:
        p["const_value"] = default
    return p


def inputs(*ports: Any) -> List[Any]:
    return list(ports)


def outputs(*ports: Any) -> List[Any]:
    return list(ports)


def wgsl(src_or_path: Union[str, Path]) -> str:
    p = Path(src_or_path)
    if p.exists():
        return p.read_text(encoding="utf-8")
    return str(src_or_path)


def shader_image(
    src_or_path: Union[str, Path],
    *,
    entry: str = "main",
    name: Optional[str] = None,
    workgroup_size: Optional[Tuple[int, int, int]] = None,
    input_binding: int = 0,
    output_binding: int = 1,
) -> Dict[str, Any]:
    p = Path(src_or_path)
    d: Dict[str, Any] = {
        "entry": entry,
        "input_binding": int(input_binding),
        "output_binding": int(output_binding),
    }
    if p.exists():
        d["src_path"] = str(p)
    else:
        d["src"] = str(src_or_path)
    if name is not None:
        d["name"] = name
    if workgroup_size is not None:
        d["workgroup_size"] = [int(workgroup_size[0]), int(workgroup_size[1]), int(workgroup_size[2])]
    return d


def shader_image_path(
    path: Union[str, Path],
    *,
    entry: str = "main",
    name: Optional[str] = None,
    workgroup_size: Optional[Tuple[int, int, int]] = None,
    input_binding: int = 0,
    output_binding: int = 1,
) -> Dict[str, Any]:
    p = Path(path)
    d: Dict[str, Any] = {
        "src_path": str(p),
        "entry": entry,
        "input_binding": int(input_binding),
        "output_binding": int(output_binding),
    }
    if name is not None:
        d["name"] = name
    if workgroup_size is not None:
        d["workgroup_size"] = [int(workgroup_size[0]), int(workgroup_size[1]), int(workgroup_size[2])]
    return d


@dataclass
class Field:
    name: str
    ty: Any

    def type_expr(self) -> Dict[str, Any]:
        return _type_expr(self.ty)


@dataclass
class Struct:
    name: str
    fields: List[Field]

    def type_expr(self) -> Dict[str, Any]:
        return {
            "Struct": [
                {"name": f.name, "ty": f.type_expr()} for f in self.fields
            ]
        }


def _type_expr(py_type: Any) -> Dict[str, Any]:
    """Translate a Python type annotation into a serialized `TypeExpr`."""
    if isinstance(py_type, type) and getattr(py_type, "DAEDALUS_IMAGE", False):
        return {
            "Struct": [
                {"name": "data_b64", "ty": {"Scalar": "String"}},
                {"name": "width", "ty": {"Scalar": "Int"}},
                {"name": "height", "ty": {"Scalar": "Int"}},
                {"name": "channels", "ty": {"Scalar": "Int"}},
                {"name": "dtype", "ty": {"Scalar": "String"}},
                {"name": "layout", "ty": {"Scalar": "String"}},
            ]
        }

    if isinstance(py_type, type) and issubclass(py_type, Enum):
        variants = []
        payload_ty = None
        for name, member in py_type.__members__.items():
            val = member.value
            member_ty = None
            for t, vname in _SCALAR_TYPES.items():
                if isinstance(val, t):
                    member_ty = {"Scalar": vname}
                    break
            if member_ty is None and is_dataclass(type(val)):
                member_ty = _type_expr(type(val))
            if payload_ty is None:
                payload_ty = member_ty
            elif payload_ty != member_ty:
                payload_ty = None
            variants.append({"name": name, "ty": member_ty})
        return {"Enum": variants}

    if is_dataclass(py_type):
        hints = get_type_hints(py_type)
        return {
            "Struct": [
                {"name": f.name, "ty": _type_expr(hints.get(f.name, f.type))}
                for f in fields(py_type)
            ]
        }
    if isinstance(py_type, Struct):
        return py_type.type_expr()
    if py_type in _SCALAR_TYPES:
        return {"Scalar": _SCALAR_TYPES[py_type]}

    origin = get_origin(py_type)
    args = get_args(py_type)

    if origin in (list, List, Iterable):
        if not args:
            raise TypeError("List type annotation must specify an element type")
        return {"List": _type_expr(args[0])}

    if origin in (tuple, Tuple):
        if not args:
            return {"Tuple": []}
        if len(args) == 2 and args[1] is Ellipsis:
            raise TypeError("Variable-length tuples are not supported in manifests")
        return {"Tuple": [_type_expr(arg) for arg in args]}

    if origin in (dict, Dict):
        if len(args) != 2:
            raise TypeError("Dict type annotation must specify key and value types")
        return {"Map": [_type_expr(args[0]), _type_expr(args[1])]}

    if origin is Union:
        non_none = [t for t in args if t is not type(None)]  # noqa: E721
        if len(non_none) == 1 and len(args) == 2:
            # typing.Optional
            return {"Optional": _type_expr(non_none[0])}
        raise TypeError(f"Unsupported union type {py_type!r}")

    raise TypeError(f"Unsupported type annotation {py_type!r}")


class CvImage:
    """Lightweight carrier for OpenCV images."""

    DAEDALUS_IMAGE = True

    def __init__(self, data_b64: str, width: int, height: int, channels: int, dtype: str = "u8", layout: str = "HWC"):
        self.data_b64 = data_b64
        self.width = width
        self.height = height
        self.channels = channels
        self.dtype = dtype
        self.layout = layout

    @classmethod
    def from_mat(cls, mat: Any) -> "CvImage":
        import numpy as np

        arr = np.asarray(mat)
        if not arr.flags["C_CONTIGUOUS"]:
            arr = np.ascontiguousarray(arr)
        h, w = arr.shape[:2]
        ch = 1 if len(arr.shape) == 2 else int(arr.shape[2])
        b64 = base64.b64encode(arr.tobytes(order="C")).decode("ascii")
        dtype = str(arr.dtype)
        return cls(b64, int(w), int(h), int(ch), dtype=dtype, layout="HWC")

    def to_mat(self) -> Any:
        import numpy as np
        buf = base64.b64decode(self.data_b64.encode("ascii"))
        dt = np.dtype(self.dtype)
        arr = np.frombuffer(buf, dtype=dt)
        if self.channels == 1:
            return arr.reshape((int(self.height), int(self.width)))
        return arr.reshape((int(self.height), int(self.width), int(self.channels)))

    def to_payload(self) -> Dict[str, Any]:
        return {
            "data_b64": self.data_b64,
            "width": self.width,
            "height": self.height,
            "channels": self.channels,
            "dtype": self.dtype,
            "layout": self.layout,
        }

    @classmethod
    def from_payload(cls, payload: Dict[str, Any]) -> "CvImage":
        return cls(
            payload["data_b64"],
            int(payload["width"]),
            int(payload["height"]),
            int(payload["channels"]),
            dtype=payload.get("dtype", "u8"),
            layout=payload.get("layout", "HWC"),
        )


@dataclass
class NodeDef:
    id: str
    label: Optional[str] = None
    py_module: Optional[str] = None
    py_path: Optional[str] = None
    py_function: Optional[str] = None
    raw_io: bool = False
    # Rust-like sugar: allow naming ports without fully specifying dict shapes.
    input_names: Optional[List[str]] = None
    output_names: Optional[List[str]] = None
    stateful: bool = False
    state_type: Optional[Any] = None
    state_py: Optional[Dict[str, str]] = None
    state: Optional[Dict[str, Any]] = None
    capability: Optional[str] = None
    shader: Optional[Dict[str, Any]] = None
    feature_flags: List[str] = field(default_factory=list)
    default_compute: str = "CpuOnly"
    sync_groups: List[Any] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    inputs: List[Dict[str, Any]] = field(default_factory=list)
    outputs: List[Dict[str, Any]] = field(default_factory=list)
    type_overrides: Dict[str, Any] = field(default_factory=dict)
    return_override: Optional[Any] = None

    def infer_from_fn(self, fn: Callable[..., Any]) -> "NodeDef":
        hints = get_type_hints(fn)
        sig = inspect.signature(fn)
        if self.py_path is None:
            try:
                self.py_path = inspect.getsourcefile(fn) or getattr(fn, "__code__", None).co_filename
            except Exception:
                self.py_path = getattr(getattr(fn, "__code__", None), "co_filename", None)
        if self.py_module is None:
            module = getattr(fn, "__module__", None)
            if module == "__main__":
                file = getattr(fn, "__code__", None)
                filename = getattr(file, "co_filename", None)
                if filename:
                    module = Path(filename).stem
            self.py_module = module
        if self.py_function is None:
            self.py_function = getattr(fn, "__name__", None)

        if not self.inputs:
            inputs: List[Dict[str, Any]] = []
            for name, param in sig.parameters.items():
                if name == "self":
                    continue
                if name in ("ctx", "node"):
                    # Rust-like injections (not ports).
                    continue
                if name == "io":
                    # Raw NodeIo-style injection (not a port).
                    continue
                if name == "state":
                    # Treat an explicit state parameter as the state carrier, not an input port.
                    ann = self.type_overrides.get(name, hints.get(name))
                    if ann is None and self.state_type is None and self.stateful:
                        raise TypeError("Stateful node requires a type hint for 'state'")
                    if self.state_type is None and ann is not None:
                        self.state_type = ann
                    continue
                if self.stateful and name == "state":
                    continue
                ann = self.type_overrides.get(name, hints.get(name))
                if ann is None:
                    raise TypeError(f"Missing type annotation for argument '{name}'")
                default = (
                    None
                    if param.default is inspect.Parameter.empty
                    else param.default
                )
                port = {"name": name, "ty": _type_expr(ann)}
                if default is not None:
                    port["const_value"] = default
                inputs.append(port)
            if self.input_names is not None:
                expected = list(self.input_names)
                got = [p["name"] for p in inputs]
                missing = [n for n in expected if n not in got]
                extra = [n for n in got if n not in expected]
                if missing or extra:
                    raise TypeError(
                        f"inputs mismatch for {self.id}: missing={missing} extra={extra}"
                    )
                order = {name: idx for idx, name in enumerate(expected)}
                inputs.sort(key=lambda p: order.get(p["name"], 10**9))
            self.inputs = inputs
        else:
            # If caller provided ports (Rust-like `inputs(port(...), ...)`) but omitted `ty`,
            # infer missing types from annotations.
            inferred: List[Dict[str, Any]] = []
            for p in self.inputs:
                if isinstance(p, str):
                    name = p
                    p = {"name": name}
                name = p.get("name")
                if not name:
                    raise TypeError("input port missing name")
                if "ty" not in p or p["ty"] is None:
                    ann = self.type_overrides.get(name, hints.get(name))
                    if ann is None:
                        raise TypeError(f"Missing type annotation for argument '{name}'")
                    p["ty"] = _type_expr(ann)
                inferred.append(p)
            self.inputs = inferred

        if not self.outputs:
            ret = self.return_override or hints.get("return")
            if ret is None or ret is type(None):
                self.outputs = []
            else:
                if (self.stateful or self.state_type is not None) and get_origin(ret) is tuple and len(get_args(ret)) == 2:
                    # Allow (State, Output) returns to mirror Rust tuple patterns.
                    state_candidate, out_candidate = get_args(ret)
                    if self.state_type is None:
                        self.state_type = state_candidate
                    ret = out_candidate
                origin = get_origin(ret)
                if origin in (tuple, Tuple):
                    args = get_args(ret)
                    if len(args) == 2 and args[1] is Ellipsis:
                        raise TypeError("Variable-length tuple return types are not supported")
                    if self.output_names is not None:
                        if len(self.output_names) != len(args):
                            raise TypeError(
                                f"outputs mismatch for {self.id}: expected {len(self.output_names)} names, got tuple arity {len(args)}"
                            )
                        self.outputs = [
                            {"name": name, "ty": _type_expr(arg)}
                            for name, arg in zip(self.output_names, args)
                        ]
                    else:
                        self.outputs = [
                            {"name": f"out{idx}", "ty": _type_expr(arg)} for idx, arg in enumerate(args)
                        ]
                else:
                    if self.output_names is not None:
                        if len(self.output_names) != 1:
                            raise TypeError(
                                f"outputs mismatch for {self.id}: expected 1 name for single return"
                            )
                        self.outputs = [{"name": self.output_names[0], "ty": _type_expr(ret)}]
                    else:
                        self.outputs = [{"name": "out", "ty": _type_expr(ret)}]
        else:
            # If caller provided outputs but omitted `ty`, infer from return annotation.
            # For tuple returns, order must match.
            ret = self.return_override or hints.get("return")
            if ret is None:
                raise TypeError("Missing return type annotation for outputs inference")
            if (self.stateful or self.state_type is not None) and get_origin(ret) is tuple and len(get_args(ret)) == 2:
                _state_candidate, out_candidate = get_args(ret)
                ret = out_candidate
            origin = get_origin(ret)
            if origin in (tuple, Tuple):
                args = get_args(ret)
                if len(args) == 2 and args[1] is Ellipsis:
                    raise TypeError("Variable-length tuple return types are not supported")
                if len(self.outputs) != len(args):
                    raise TypeError("Provided outputs arity does not match return tuple")
                for idx, arg in enumerate(args):
                    if "ty" not in self.outputs[idx] or self.outputs[idx]["ty"] is None:
                        self.outputs[idx]["ty"] = _type_expr(arg)
            else:
                if len(self.outputs) != 1:
                    raise TypeError("Provided outputs arity does not match single return")
                if "ty" not in self.outputs[0] or self.outputs[0]["ty"] is None:
                    self.outputs[0]["ty"] = _type_expr(ret)

        # If a state type was inferred or provided, mark the node stateful and capture the python type.
        if self.state_type is None:
            # Fallback: if the fn has a parameter called `state`, assume stateful.
            for name, param in sig.parameters.items():
                if name == "state":
                    ann = self.type_overrides.get(name, hints.get(name))
                    if ann is None:
                        raise TypeError("Stateful node requires a type hint for 'state'")
                    self.state_type = ann
                    break
        if self.state_type is not None:
            self.stateful = True
            ty = self.state_type
            if is_dataclass(ty):
                state_mod = ty.__module__
                if state_mod == "__main__":
                    if self.py_path:
                        self.state_py = {"path": self.py_path, "name": ty.__name__}
                    else:
                        if self.py_module:
                            state_mod = self.py_module
                        self.state_py = {"module": state_mod, "name": ty.__name__}
                else:
                    self.state_py = {"module": state_mod, "name": ty.__name__}
                init_method = getattr(ty, "daedalus_init", None)
                if callable(init_method):
                    if self.state is None:
                        self.state = {}
                    self.state["init"] = init_method.__name__
            if self.state is None:
                self.state = {}

        return self

    def to_manifest(self) -> Dict[str, Any]:
        state = None
        if self.stateful:
            state = {
                "ty": _type_expr(self.state_type or type(None)),
            }
            if self.state_py:
                state["py_dataclass"] = self.state_py
            if self.state:
                state.update(self.state)

        return {
            "id": self.id,
            "label": self.label,
            "py_module": self.py_module,
            "py_path": self.py_path,
            "py_function": self.py_function,
            "raw_io": bool(self.raw_io),
            "stateful": self.stateful,
            "state": state,
            "capability": self.capability,
            "shader": _normalize_json(self.shader),
            "feature_flags": self.feature_flags,
            "default_compute": self.default_compute,
            "sync_groups": _normalize_json(self.sync_groups),
            "metadata": _normalize_json(self.metadata),
            "inputs": self.inputs,
            "outputs": self.outputs,
        }


class Plugin:
    def __init__(
        self,
        name: str,
        version: str,
        description: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.name = name
        self.version = version
        self.description = description
        self.metadata = metadata or {}
        self.nodes: List[NodeDef] = []

    def register(self, node: Union[NodeDef, Callable[..., Any]]) -> None:
        if isinstance(node, NodeDef):
            self.nodes.append(node)
            return
        nd = getattr(node, "__daedalus_node__", None)
        if nd is None:
            raise TypeError("register() expects a NodeDef or a @node-decorated function")
        self.nodes.append(nd)

    def discover(self, *items: Union[str, Path], recursive: bool = True) -> None:
        """
        Register all `@node`-decorated callables found in the provided modules/files/dirs.

        - module name: `"my_pkg.nodes"`
        - file path: `Path("nodes.py")`
        - dir path: `Path("nodes/")` (walks `*.py` when `recursive=True`)
        """

        def load_module_from_path(p: Path):
            p = p.resolve()
            mod_name = f"_daedalus_user_{abs(hash(str(p)))}"
            spec = importlib.util.spec_from_file_location(mod_name, p.as_posix())
            if spec is None or spec.loader is None:
                raise ImportError(f"failed to load python module from path: {p}")
            mod = importlib.util.module_from_spec(spec)
            sys.modules[mod_name] = mod
            spec.loader.exec_module(mod)
            return mod

        def register_from_module(mod):
            for v in vars(mod).values():
                nd = None
                if isinstance(v, NodeDef):
                    nd = v
                else:
                    nd = getattr(v, "__daedalus_node__", None)
                if nd is not None:
                    self.register(nd)

        for it in items:
            if isinstance(it, Path) or (isinstance(it, str) and ("/" in it or it.endswith(".py"))):
                p = Path(it)
                if p.is_dir():
                    glob = p.rglob("*.py") if recursive else p.glob("*.py")
                    for f in glob:
                        if f.name.startswith("__"):
                            continue
                        register_from_module(load_module_from_path(f))
                else:
                    register_from_module(load_module_from_path(p))
            else:
                mod = importlib.import_module(str(it))
                register_from_module(mod)

    def emit_manifest(self, path: Union[str, Path]) -> Path:
        target = Path(path)
        doc = {
            "manifest_version": "1",
            "language": "python",
            "plugin": {
                "name": self.name,
                "version": self.version,
                "description": self.description,
                "metadata": self.metadata,
            },
            "nodes": [n.to_manifest() for n in self.nodes],
        }
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(doc, indent=2), encoding="utf-8")
        return target

    def pack(
        self,
        out_name: str = "generated_py_plugin",
        manifest_path: Optional[Union[str, Path]] = None,
        build: bool = True,
        bundle: bool = False,
        release: bool = False,
        lock: bool = True,
        vendor: bool = False,
    ) -> Path:
        """
        Emit a manifest, generate a Rust `cdylib` example that wraps it, and optionally build it.

        If `bundle` is True, referenced `py_path`/WGSL files are copied into a bundle directory and
        embedded into the generated Rust wrapper so the resulting `cdylib` can run without external
        files (closer to Rust plugin ergonomics).

        Returns the path to the built library if `build` is True, otherwise the path where it would be.
        """
        workspace = _find_workspace_root()
        ffi_root = workspace / "crates" / "ffi"
        if manifest_path is None:
            caller = inspect.stack()[1].filename if len(inspect.stack()) > 1 else None
            base = Path(caller).resolve().parent if caller else Path.cwd()
            manifest_path = base / f"{self.name}.manifest.json"
        manifest_path = Path(manifest_path)
        manifest_path = self.emit_manifest(manifest_path)

        lock_path = None
        if lock:
            try:
                lock_path = manifest_path.with_suffix(".lock")
                with lock_path.open("w", encoding="utf-8") as f:
                    subprocess.check_call(
                        [sys.executable, "-m", "pip", "freeze"],
                        stdout=f,
                        cwd=manifest_path.parent,
                    )
            except Exception:
                lock_path = None

        vendor_bundle_path = None
        if vendor:
            try:
                import zipfile

                vendor_bundle_path = manifest_path.with_suffix(".zip")
                with zipfile.ZipFile(vendor_bundle_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                    base = Path(__file__).resolve().parent / "examples"
                    for py in base.glob("*.py"):
                        zf.write(py, arcname=py.name)
            except Exception:
                vendor_bundle_path = None

        # Decorate manifest with hash/signature/lock/bundle references.
        try:
            manifest_bytes = manifest_path.read_bytes()
            digest = hashlib.sha256(manifest_bytes).hexdigest()
            doc = json.loads(manifest_bytes.decode("utf-8"))
            doc["manifest_hash"] = digest
            doc["signature"] = None
            if lock_path:
                doc["lockfile"] = os.path.relpath(lock_path, manifest_path.parent)
            if vendor_bundle_path:
                doc["bundle"] = os.path.relpath(vendor_bundle_path, manifest_path.parent)
            manifest_path.write_text(json.dumps(doc, indent=2), encoding="utf-8")
        except Exception:
            pass

        manifest_include_path = manifest_path
        base_dir = manifest_path.parent
        bundle_entries: List[Tuple[str, str]] = []

        if bundle:
            bundle_dir = ffi_root / "examples" / f"{out_name}_bundle"
            bundle_dir.mkdir(parents=True, exist_ok=True)

            doc = json.loads(manifest_path.read_text(encoding="utf-8"))
            nodes = doc.get("nodes") if isinstance(doc, dict) else None
            if not isinstance(nodes, list):
                nodes = []

            files_to_copy: List[Path] = []

            def add_file(p: Any) -> None:
                if not isinstance(p, str) or not p:
                    return
                fp = Path(p)
                abs_path = fp if fp.is_absolute() else (base_dir / fp).resolve()
                if abs_path.exists() and abs_path.is_file():
                    files_to_copy.append(abs_path)

            # Bundle lock/bundle references if present (optional metadata).
            add_file(doc.get("lockfile") if isinstance(doc, dict) else None)
            add_file(doc.get("bundle") if isinstance(doc, dict) else None)

            # Bundle node runtime files and shader src_path files.
            for n in nodes:
                if not isinstance(n, dict):
                    continue
                py_path = n.get("py_path")
                if n.get("py_module") and not py_path:
                    raise RuntimeError("bundle=True requires every Python node to set py_path")
                add_file(py_path)

                state = n.get("state")
                if isinstance(state, dict):
                    py_dc = state.get("py_dataclass")
                    if isinstance(py_dc, dict):
                        add_file(py_dc.get("path"))

                shader = n.get("shader")
                if isinstance(shader, dict):
                    add_file(shader.get("src_path"))
                    shaders = shader.get("shaders")
                    if isinstance(shaders, list):
                        for s in shaders:
                            if isinstance(s, dict):
                                add_file(s.get("src_path"))

            uniq_abs = []
            seen = set()
            for p in files_to_copy:
                key = p.resolve()
                if key in seen:
                    continue
                seen.add(key)
                uniq_abs.append(key)

            mapping: Dict[Path, str] = {}
            ext_idx = 0
            for abs_path in uniq_abs:
                try:
                    rel = abs_path.relative_to(base_dir.resolve())
                    rel_str = rel.as_posix()
                except Exception:
                    rel_str = Path("_external") / str(ext_idx) / abs_path.name
                    rel_str = rel_str.as_posix()
                    ext_idx += 1
                mapping[abs_path] = rel_str
                dest = bundle_dir / rel_str
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(abs_path, dest)
                bundle_entries.append((rel_str, dest.as_posix()))

            def rewrite_path(p: Any) -> Any:
                if not isinstance(p, str) or not p:
                    return p
                fp = Path(p)
                abs_path = fp if fp.is_absolute() else (base_dir / fp).resolve()
                return mapping.get(abs_path, p)

            if isinstance(doc, dict):
                if doc.get("lockfile"):
                    doc["lockfile"] = rewrite_path(doc.get("lockfile"))
                if doc.get("bundle"):
                    doc["bundle"] = rewrite_path(doc.get("bundle"))

            for n in nodes:
                if not isinstance(n, dict):
                    continue
                if n.get("py_path"):
                    n["py_path"] = rewrite_path(n.get("py_path"))
                state = n.get("state")
                if isinstance(state, dict):
                    py_dc = state.get("py_dataclass")
                    if isinstance(py_dc, dict) and py_dc.get("path"):
                        py_dc["path"] = rewrite_path(py_dc.get("path"))
                shader = n.get("shader")
                if isinstance(shader, dict) and shader.get("src_path"):
                    shader["src_path"] = rewrite_path(shader.get("src_path"))
                if isinstance(shader, dict):
                    shaders = shader.get("shaders")
                    if isinstance(shaders, list):
                        for s in shaders:
                            if isinstance(s, dict) and s.get("src_path"):
                                s["src_path"] = rewrite_path(s.get("src_path"))

            bundled_manifest = bundle_dir / "manifest.json"
            bundled_manifest.write_text(json.dumps(doc, indent=2), encoding="utf-8")
            manifest_include_path = bundled_manifest
            base_dir = bundle_dir

        example_path = ffi_root / "examples" / f"{out_name}.rs"
        manifest_str_path = manifest_include_path.as_posix()
        base_dir_str = base_dir.as_posix()
        bundle_code = ""
        if bundle:
            bundle_code = (
                "\n"
                "fn extract_bundle() -> std::path::PathBuf {\n"
                "    let nanos = std::time::SystemTime::now()\n"
                "        .duration_since(std::time::UNIX_EPOCH)\n"
                "        .unwrap()\n"
                "        .as_nanos();\n"
                f"    let dir = std::env::temp_dir().join(format!(\"daedalus_py_bundle_{out_name}_{{}}_{{}}\", std::process::id(), nanos));\n"
                "    std::fs::create_dir_all(&dir).expect(\"create bundle temp dir\");\n"
                "    for (rel, bytes) in BUNDLE_FILES {\n"
                "        let dest = dir.join(rel);\n"
                "        if let Some(parent) = dest.parent() {\n"
                "            let _ = std::fs::create_dir_all(parent);\n"
                "        }\n"
                "        std::fs::write(&dest, bytes).expect(\"write bundled file\");\n"
                "    }\n"
                "    dir\n"
                "}\n\n"
                "static BUNDLE_FILES: &[(&str, &[u8])] = &[\n"
                + "\n".join(
                    [
                        f"    ({json.dumps(rel)}, include_bytes!(r#\"{abs_path}\"#) as &[u8]),"
                        for (rel, abs_path) in bundle_entries
                    ]
                )
                + "\n];\n"
            )
        example_src = f"""#![crate_type = "cdylib"]
use daedalus_ffi::export_plugin;
use daedalus_ffi::{{PythonManifest, PythonManifestPlugin}};
use daedalus_runtime::plugins::{{Plugin, PluginRegistry}};
use serde_json;

static MANIFEST_JSON: &str = include_str!(r\"{manifest_str_path}\");
static BASE_DIR: &str = r\"{base_dir_str}\";

{bundle_code}

pub struct GeneratedPyPlugin {{
    inner: PythonManifestPlugin,
}}

impl Default for GeneratedPyPlugin {{
    fn default() -> Self {{
        let manifest: PythonManifest = serde_json::from_str(MANIFEST_JSON).expect("invalid embedded manifest");
        let base = if {str(bundle).lower()} {{ extract_bundle() }} else {{ std::path::PathBuf::from(BASE_DIR) }};
        Self {{
            inner: PythonManifestPlugin::from_manifest_with_base(manifest, Some(base)),
        }}
    }}
}}

impl Plugin for GeneratedPyPlugin {{
    fn id(&self) -> &'static str {{
        self.inner.id()
    }}

    fn install(&self, registry: &mut PluginRegistry) -> Result<(), &'static str> {{
        self.inner.install(registry)
    }}
}}

export_plugin!(GeneratedPyPlugin);
"""
        example_path.write_text(example_src, encoding="utf-8")

        profile = "release" if release else (os.environ.get("PROFILE") or "debug")
        target_dir = workspace / "target" / profile / "examples"
        prefix, ext = _lib_naming()
        artifact = target_dir / f"{prefix}{out_name}{ext}"

        if build:
            cmd = [
                "cargo",
                "build",
                "-p",
                "daedalus-ffi",
                "--example",
                out_name,
            ]
            if release:
                cmd.append("--release")
            env = os.environ.copy()
            env.setdefault("PYO3_USE_ABI3_FORWARD_COMPATIBILITY", "1")
            subprocess.check_call(cmd, cwd=workspace, env=env)
            if not artifact.exists():
                raise RuntimeError(f"expected artifact missing at {artifact}")
        return artifact

    def build(
        self,
        out_path: Optional[Union[str, Path]] = None,
        *,
        out_name: Optional[str] = None,
        bundle: bool = True,
        release: bool = True,
        lock: bool = False,
        vendor: bool = False,
        keep_intermediates: bool = False,
    ) -> Path:
        """
        Build a Rust `cdylib` plugin (`.so`/`.dylib`/`.dll`) with no persistent manifest by default.

        This is the "simple path": author nodes, call `plugin.build(...)`, get a shared library.
        """
        import tempfile

        workspace = _find_workspace_root()
        ffi_root = workspace / "crates" / "ffi"
        examples_dir = ffi_root / "examples"
        examples_dir.mkdir(parents=True, exist_ok=True)

        unique = out_name or f"{self.name}_{os.getpid()}"
        tmp = Path(tempfile.mkdtemp(prefix=f"daedalus_py_build_{unique}_"))
        manifest_tmp = tmp / f"{self.name}.manifest.json"

        artifact = self.pack(
            out_name=unique,
            manifest_path=manifest_tmp,
            build=True,
            bundle=bundle,
            release=release,
            lock=lock,
            vendor=vendor,
        )

        if out_path is not None:
            out_path = Path(out_path)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(artifact, out_path)
            artifact = out_path

        if not keep_intermediates:
            try:
                rs = examples_dir / f"{unique}.rs"
                if rs.exists():
                    rs.unlink()
            except Exception:
                pass
            try:
                bdir = examples_dir / f"{unique}_bundle"
                if bdir.exists():
                    shutil.rmtree(bdir, ignore_errors=True)
            except Exception:
                pass
            try:
                shutil.rmtree(tmp, ignore_errors=True)
            except Exception:
                pass

        return artifact


def _find_workspace_root() -> Path:
    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / "Cargo.lock").exists():
            return parent
    return here.parents[6]


def _lib_naming() -> Tuple[str, str]:
    sysname = platform.system().lower()
    if sysname == "windows":
        return ("", ".dll")
    if sysname == "darwin":
        return ("lib", ".dylib")
    return ("lib", ".so")


def node(plugin: Optional[Plugin] = None, **kwargs: Any) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        state_override = kwargs.pop("state", None)
        input_names = kwargs.pop("inputs", None)
        output_names = kwargs.pop("outputs", None)
        input_ports = kwargs.pop("input_ports", None)
        output_ports = kwargs.pop("output_ports", None)
        nd = NodeDef(**kwargs)
        # Rust-like port lists: inputs(port(...), "x") or output_ports=[...]
        if input_ports is not None and input_names is not None:
            raise TypeError("use only one of inputs=... or input_ports=...")
        if output_ports is not None and output_names is not None:
            raise TypeError("use only one of outputs=... or output_ports=...")

        if input_ports is None and isinstance(input_names, (list, tuple)) and input_names and isinstance(input_names[0], dict):
            input_ports = input_names
            input_names = None
        if output_ports is None and isinstance(output_names, (list, tuple)) and output_names and isinstance(output_names[0], dict):
            output_ports = output_names
            output_names = None

        if input_ports is not None:
            # We'll fill `ty` from annotations during infer, but preserve name/source/default.
            nd.inputs = list(input_ports)
        if output_ports is not None:
            nd.outputs = list(output_ports)

        if input_names is not None and not isinstance(input_names, list):
            input_names = list(input_names)
        if output_names is not None and not isinstance(output_names, list):
            output_names = list(output_names)
        if input_names is not None:
            nd.input_names = input_names
        if output_names is not None:
            nd.output_names = output_names
        if state_override is not None:
            nd.state_type = state_override
            nd.stateful = True
        nd = nd.infer_from_fn(fn)
        setattr(fn, "__daedalus_node__", nd)
        if plugin is not None:
            plugin.register(nd)
        return fn

    return decorator


def node_rs(plugin: Optional[Plugin] = None, **kwargs: Any) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """
    Rust-like alias for `node()` that supports naming ports like the Rust macro:

    - `inputs=("a","b")` to validate/order input ports
    - `outputs=("out",)` / `outputs=("x","y")` to name output ports
    """
    return node(plugin=plugin, **kwargs)


__all__ = [
    "Plugin",
    "NodeDef",
    "node",
    "node_rs",
    "port",
    "inputs",
    "outputs",
    "wgsl",
    "shader_image",
    "shader_image_path",
    "SyncPolicy",
    "BackpressureStrategy",
    "SyncGroup",
    "sync_group",
    "Struct",
    "Field",
    "CvImage",
]
