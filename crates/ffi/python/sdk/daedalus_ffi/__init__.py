from __future__ import annotations

import inspect
import json
import types
from dataclasses import dataclass, is_dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Callable, get_args, get_origin, get_type_hints

SCHEMA_VERSION = 1
_VALID_ACCESS = {"read", "view", "modify", "move"}
_VALID_RESIDENCY = {"cpu", "gpu"}
_VALID_BOUNDARY_CAPABILITIES = {"host_read", "worker_write", "borrow_ref", "borrow_mut", "shared_clone"}


def _scalar(name: str) -> dict[str, str]:
    return {"Scalar": name}


def _opaque(name: str) -> dict[str, str]:
    return {"Opaque": name}


def _type_key(value: Any) -> str:
    return getattr(value, "__daedalus_type_key__", f"{value.__module__}.{value.__qualname__}")


def _type_expr(annotation: Any) -> dict[str, Any]:
    if annotation is inspect.Signature.empty or annotation is Any:
        raise TypeError("missing or unsupported type annotation")
    if annotation is None or annotation is type(None):
        return _scalar("Unit")
    if annotation is bool:
        return _scalar("Bool")
    if annotation is int:
        return _scalar("Int")
    if annotation is float:
        return _scalar("Float")
    if annotation is str:
        return _scalar("String")
    if annotation is bytes or annotation is memoryview:
        return _scalar("Bytes")
    if isinstance(annotation, type) and issubclass(annotation, _BytesLike):
        return _scalar("Bytes")
    if isinstance(annotation, type) and issubclass(annotation, _ImageLike):
        return _scalar("Bytes")
    origin = get_origin(annotation)
    args = get_args(annotation)
    if origin is types.UnionType or origin is getattr(types, "UnionType", None):
        non_none = [arg for arg in args if arg is not type(None)]
        if len(non_none) == 1 and len(non_none) != len(args):
            return {"Optional": _type_expr(non_none[0])}
    if str(origin) == "typing.Union":
        non_none = [arg for arg in args if arg is not type(None)]
        if len(non_none) == 1 and len(non_none) != len(args):
            return {"Optional": _type_expr(non_none[0])}
    if origin is list:
        return {"List": _type_expr(args[0])}
    if origin is dict:
        return {"Map": [_type_expr(args[0]), _type_expr(args[1])]}
    if origin is tuple:
        return {"Tuple": [_type_expr(arg) for arg in args]}
    if inspect.isclass(annotation) and issubclass(annotation, Enum):
        return _opaque(_type_key(annotation))
    if inspect.isclass(annotation) and is_dataclass(annotation):
        return _opaque(_type_key(annotation))
    if inspect.isclass(annotation):
        return _opaque(_type_key(annotation))
    raise TypeError(f"unsupported type annotation: {annotation!r}")


def _port(
    name: str,
    annotation: Any,
    access: str = "read",
    residency: str | None = None,
    layout: str | None = None,
) -> dict[str, Any]:
    _validate_port_shape(name, access, residency, layout)
    port: dict[str, Any] = {
        "name": name,
        "ty": _type_expr(annotation),
        "optional": False,
        "access": access,
    }
    if inspect.isclass(annotation) and hasattr(annotation, "__daedalus_type_key__"):
        port["type_key"] = _type_key(annotation)
    if residency is not None:
        port["residency"] = residency
    if layout is not None:
        port["layout"] = layout
    return port


class Config:
    @staticmethod
    def port(default: Any = None, **_: Any) -> Any:
        return default


class State:
    pass


@dataclass
class AdapterDecl:
    id: str
    source: Any
    target: Any
    func: Callable[..., Any]


def adapter(id: str, source: Any, target: Any) -> Callable[[Callable[..., Any]], AdapterDecl]:
    def wrap(func: Callable[..., Any]) -> AdapterDecl:
        return AdapterDecl(id=id, source=source, target=target, func=func)

    return wrap


def type_key(key: str) -> Callable[[type], type]:
    def wrap(cls: type) -> type:
        cls.__daedalus_type_key__ = key
        return cls

    return wrap


@dataclass
class NodeDecl:
    id: str
    inputs: list[Any]
    outputs: list[str]
    func: Callable[..., Any]
    state: Any | None = None
    capability: str | None = None
    access: str = "read"
    residency: str | None = None
    layout: str | None = None
    transport: str | None = None

    def schema(self) -> dict[str, Any]:
        hints = get_type_hints(self.func)
        signature = inspect.signature(self.func)
        input_ports = []
        for item in self.inputs:
            if isinstance(item, str):
                if item not in signature.parameters:
                    raise TypeError(f"node `{self.id}` input `{item}` is not a function parameter")
                annotation = hints.get(item, signature.parameters[item].annotation)
                input_ports.append(
                    _port(item, annotation, self.access, self.residency, self.layout)
                )
            elif inspect.isclass(item):
                name = "config"
                input_ports.append(_port(name, item, self.access, self.residency, self.layout))
            else:
                raise TypeError(f"node `{self.id}` has unsupported input declaration {item!r}")

        output_annotation = hints.get("return", inspect.signature(self.func).return_annotation)
        output_types = _output_types(output_annotation, len(self.outputs), self.id)
        output_ports = [
            _port(name, annotation, "read", self.residency, self.layout)
            for name, annotation in zip(self.outputs, output_types, strict=True)
        ]
        return {
            "id": self.id,
            "backend": "python",
            "entrypoint": self.func.__name__,
            "stateful": self.state is not None,
            "feature_flags": [],
            "inputs": input_ports,
            "outputs": output_ports,
            "metadata": {
                key: value
                for key, value in {
                    "capability": self.capability,
                    "transport": self.transport,
                }.items()
                if value is not None
            },
        }


def _output_types(annotation: Any, count: int, node_id: str) -> list[Any]:
    if count == 0:
        return []
    origin = get_origin(annotation)
    args = get_args(annotation)
    if count > 1:
        if origin is tuple and len(args) == count:
            return list(args)
        raise TypeError(f"node `{node_id}` must return tuple[...] for {count} outputs")
    return [annotation]


def node(
    id: str,
    inputs: list[Any],
    outputs: list[str],
    state: Any | None = None,
    capability: str | None = None,
    access: str = "read",
    residency: str | None = None,
    layout: str | None = None,
    transport: str | None = None,
) -> Callable[[Callable[..., Any]], NodeDecl]:
    def wrap(func: Callable[..., Any]) -> NodeDecl:
        return NodeDecl(
            id=id,
            inputs=inputs,
            outputs=outputs,
            func=func,
            state=state,
            capability=capability,
            access=access,
            residency=residency,
            layout=layout,
            transport=transport,
        )

    return wrap


class PluginDecl:
    def __init__(self, name: str, nodes: list[NodeDecl]) -> None:
        self.name = name
        self.nodes = nodes
        self.adapters: list[AdapterDecl] = []
        self.artifacts: list[str] = []
        self.boundary_contracts: list[dict[str, Any]] = []
        self.transport_options: dict[str, Any] = {}

    def type_contract(self, type_key: str, capabilities: list[str]) -> PluginDecl:
        _validate_type_contract(type_key, capabilities)
        self.boundary_contracts.append(
            {
                "type_key": type_key,
                "rust_type_name": None,
                "abi_version": 1,
                "layout_hash": type_key,
                "capabilities": _boundary_capabilities(capabilities),
            }
        )
        return self

    def artifact(self, path: str) -> PluginDecl:
        self.artifacts.append(path)
        return self

    def adapter(self, adapter_decl: AdapterDecl) -> PluginDecl:
        self.adapters.append(adapter_decl)
        return self

    def transport(self, **options: Any) -> PluginDecl:
        self.transport_options.update(options)
        return self

    def descriptor(self) -> dict[str, Any]:
        nodes = [node.schema() for node in self.nodes]
        backends = {
            node["id"]: {
                "backend": "python",
                "runtime_model": "persistent_worker",
                "entry_module": "ffi_showcase.py",
                "entry_symbol": node["entrypoint"],
                "executable": "python",
                "args": [],
                "classpath": [],
                "native_library_paths": [],
                "env": {},
                "options": {"payload_transport": self.transport_options},
            }
            for node in nodes
        }
        descriptor = {
            "schema_version": SCHEMA_VERSION,
            "schema": {
                "schema_version": SCHEMA_VERSION,
                "plugin": {
                    "name": self.name,
                    "version": "1.0.0",
                    "description": None,
                    "metadata": {},
                },
                "dependencies": [],
                "required_host_capabilities": [],
                "feature_flags": [],
                "boundary_contracts": self.boundary_contracts,
                "nodes": nodes,
            },
            "backends": backends,
            "artifacts": [
                {
                    "path": path,
                    "kind": "source_file",
                    "backend": "python",
                    "platform": None,
                    "sha256": None,
                    "metadata": {},
                }
                for path in self.artifacts
            ],
            "lockfile": "plugin.lock.json",
            "manifest_hash": None,
            "signature": None,
            "metadata": {
                "language": "python",
                "package_builder": "daedalus_ffi.python",
                "adapters": [adapter.id for adapter in self.adapters],
            },
        }
        validate_descriptor(descriptor)
        return descriptor

    def write(self, path: str | Path) -> None:
        Path(path).write_text(
            json.dumps(self.descriptor(), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )


def _boundary_capabilities(capabilities: list[str]) -> dict[str, bool]:
    host_read = "host_read" in capabilities
    worker_write = "worker_write" in capabilities
    return {
        "owned_move": True,
        "shared_clone": host_read,
        "borrow_ref": host_read,
        "borrow_mut": worker_write,
        "metadata_read": host_read,
        "metadata_write": worker_write,
        "backing_read": host_read,
        "backing_write": worker_write,
    }


def _validate_type_contract(type_key: str, capabilities: list[str]) -> None:
    if not type_key:
        raise ValueError("boundary contract type_key must not be empty")
    invalid = sorted(set(capabilities) - _VALID_BOUNDARY_CAPABILITIES)
    if invalid:
        raise ValueError(f"unsupported boundary capabilities: {invalid}")


def _validate_port_shape(
    name: str,
    access: str,
    residency: str | None,
    layout: str | None,
) -> None:
    if not name:
        raise ValueError("port name must not be empty")
    if access not in _VALID_ACCESS:
        raise ValueError(f"unsupported access mode: {access}")
    if residency is not None and residency not in _VALID_RESIDENCY:
        raise ValueError(f"unsupported residency: {residency}")
    if layout is not None and residency is None:
        raise ValueError("layout requires residency")


def validate_descriptor(descriptor: dict[str, Any]) -> None:
    if descriptor.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("unsupported schema_version")
    schema = descriptor.get("schema")
    if not isinstance(schema, dict):
        raise ValueError("descriptor is missing schema")
    nodes = schema.get("nodes")
    backends = descriptor.get("backends")
    if not isinstance(nodes, list) or not isinstance(backends, dict):
        raise ValueError("descriptor is missing nodes or backends")
    node_ids = set()
    for node_decl in nodes:
        node_id = node_decl.get("id")
        if not node_id or node_id in node_ids:
            raise ValueError(f"duplicate or missing node id: {node_id}")
        node_ids.add(node_id)
        if node_decl.get("backend") != "python":
            raise ValueError(f"node `{node_id}` must use python backend")
        for port_decl in [*node_decl.get("inputs", []), *node_decl.get("outputs", [])]:
            _validate_port_shape(
                port_decl.get("name", ""),
                port_decl.get("access", "read"),
                port_decl.get("residency"),
                port_decl.get("layout"),
            )
        backend = backends.get(node_id)
        if not backend:
            raise ValueError(f"node `{node_id}` is missing backend config")
        if backend.get("runtime_model") != "persistent_worker":
            raise ValueError(f"node `{node_id}` must use persistent_worker")
    extra_backends = set(backends) - node_ids
    if extra_backends:
        raise ValueError(f"unexpected backend configs: {sorted(extra_backends)}")


def plugin(name: str, nodes: list[NodeDecl]) -> PluginDecl:
    return PluginDecl(name, nodes)


class _BytesLike:
    pass


class _ImageLike:
    pass


class bytes_payload:
    View = memoryview
    SharedView = memoryview

    class CowView(_BytesLike):
        def __init__(self, data: bytes = b"") -> None:
            self.data = data

        def with_appended(self, data: bytes) -> bytes:
            return self.data + data


class image:
    class Rgba8(_ImageLike):
        def map_pixels(self, func: Callable[..., Any]) -> image.Rgba8:
            return self

    class MutableRgba8(Rgba8):
        def map_pixels_in_place(self, func: Callable[..., Any]) -> image.MutableRgba8:
            return self


class gpu:
    class ImageRgba8(_ImageLike):
        def dispatch(self, shader: str) -> gpu.ImageRgba8:
            return self


class event:
    class Context:
        def info(self, topic: str, message: str) -> None:
            pass

    class TypedError(Exception):
        def __init__(self, code: str, message: str) -> None:
            self.code = code
            super().__init__(message)
