"""The in-process host: load plugins, compile a graph, push, tick and take, all in this process.

This is a thin Python face over `libdaedalus_capi`, the library `core/capi` builds, and it offers
what that C surface offers and no more:

- Values are integers, floats, booleans and bytes. A port of any other type is refused with
  `UnsupportedPort`, before the library is called. Strings and compound values are not offered.
- There is no cancel, no state and no fault detail. A tick reports how many steps ran and how many
  failed, and nothing else.
- A graph's ports are looked up by name or by index. An index is the port's place in the ports
  sorted by name.

Give the library's path to `Host(library=PATH)`, or set `DAEDALUS_HOST_LIB` to it. cargo builds it:
`cargo build -p daedalus-capi` leaves the shared library under `target/debug/`, named
`libdaedalus_capi` with the platform's extension (`.so`, `.dylib` or `.dll`).

Lifetime: a `Host` owns its graphs. `close()` closes the graphs still open, then the host, and
leaving a `with` block does the same. A `Graph` closes its own handle with `close()`. Calls on one
graph may come from any thread, one at a time, as the C surface says. The library keeps no buffer
of ours after a call returns: pushed bytes are copied by the library, and taken bytes are copied
into a buffer the call owns.
"""

from __future__ import annotations

import ctypes
import json
import os
import threading
from ctypes import POINTER, byref, c_double, c_int32, c_int64, c_size_t, c_uint8, c_uint32, c_uint64, c_void_p
from dataclasses import dataclass
from typing import Any, Optional, Union

from ._ffi import (
    STATUS_BUFFER_TOO_SMALL,
    STATUS_OK,
    DaedalusError,
    LibraryError,
    Native,
    declare,
    load_library,
    text_bytes,
)

__all__ = ["Graph", "Host", "Port", "Tick", "UnsupportedPort", "library_for"]

ENV_VAR = "DAEDALUS_HOST_LIB"
_BUILD_HINT = "Build it with `cargo build -p daedalus-capi`."
_PREFIX = "daedalus"

_INTEGER_TYPES = frozenset(
    ["i8", "i16", "i32", "i64", "isize", "u8", "u16", "u32", "u64", "usize"]
)
_FLOAT_TYPES = frozenset(["f32", "f64"])


class UnsupportedPort(TypeError):
    """A port carries a type this host API does not move (only integers, floats, bools and bytes),
    or a value of a type it does not take."""


@dataclass(frozen=True)
class Port:
    """One host port of a graph: its name, its type key (such as `u32`), and whether the caller
    pushes to it (`is_input`) or takes from it."""

    name: str
    type_key: str
    is_input: bool


@dataclass(frozen=True)
class Tick:
    """What one tick did: whether nothing was left to do, how many steps ran, and how many failed."""

    idle: bool
    step_calls: int
    faults: int


class _Version(ctypes.Structure):
    _fields_ = [("major", c_uint32), ("minor", c_uint32), ("layout_hash", c_uint64)]


class _PortStruct(ctypes.Structure):
    _fields_ = [
        ("name", c_void_p),
        ("name_len", c_size_t),
        ("type_key", c_void_p),
        ("type_key_len", c_size_t),
        ("is_input", c_uint8),
    ]


class _TickStruct(ctypes.Structure):
    _fields_ = [("idle", c_uint8), ("step_calls", c_uint32), ("faults", c_uint32)]


class _Telemetry(ctypes.Structure):
    _fields_ = [
        ("ticks", c_uint64),
        ("idle_ticks", c_uint64),
        ("step_calls", c_uint64),
        ("faults", c_uint64),
        ("pushes", c_uint64),
        ("takes", c_uint64),
    ]


_VP = c_void_p
_LEN = POINTER(c_size_t)
_STATUS = c_int32
_SIGNATURES = {
    "version": ("daedalus_version", [POINTER(_Version)]),
    "host_create": ("daedalus_host_create", [POINTER(c_uint64)]),
    "host_release": ("daedalus_host_release", [c_uint64]),
    "host_install": ("daedalus_host_install", [c_uint64, _VP, c_size_t]),
    "host_schemas": ("daedalus_host_schemas", [c_uint64, _VP, c_size_t, _LEN]),
    "graph_compile": ("daedalus_graph_compile", [c_uint64, _VP, c_size_t, POINTER(c_uint64)]),
    "graph_release": ("daedalus_graph_release", [c_uint64]),
    "graph_port_count": ("daedalus_graph_port_count", [c_uint64, POINTER(c_size_t)]),
    "graph_port": ("daedalus_graph_port", [c_uint64, c_size_t, POINTER(_PortStruct)]),
    "graph_port_index": ("daedalus_graph_port_index", [c_uint64, _VP, c_size_t, POINTER(c_size_t)]),
    "graph_explain": ("daedalus_graph_explain", [c_uint64, _VP, c_size_t, _LEN]),
    "graph_telemetry": ("daedalus_graph_telemetry", [c_uint64, POINTER(_Telemetry)]),
    "tick": ("daedalus_tick", [c_uint64, POINTER(_TickStruct)]),
    "push_i64": ("daedalus_push_i64", [c_uint64, _VP, c_size_t, c_int64]),
    "push_i64_at": ("daedalus_push_i64_at", [c_uint64, c_size_t, c_int64]),
    "push_f64": ("daedalus_push_f64", [c_uint64, _VP, c_size_t, c_double]),
    "push_f64_at": ("daedalus_push_f64_at", [c_uint64, c_size_t, c_double]),
    "push_bool": ("daedalus_push_bool", [c_uint64, _VP, c_size_t, c_uint8]),
    "push_bool_at": ("daedalus_push_bool_at", [c_uint64, c_size_t, c_uint8]),
    "push_bytes": ("daedalus_push_bytes", [c_uint64, _VP, c_size_t, _VP, c_size_t]),
    "push_bytes_at": ("daedalus_push_bytes_at", [c_uint64, c_size_t, _VP, c_size_t]),
    "take_i64": ("daedalus_take_i64", [c_uint64, _VP, c_size_t, POINTER(c_int64), POINTER(c_uint8)]),
    "take_i64_at": ("daedalus_take_i64_at", [c_uint64, c_size_t, POINTER(c_int64), POINTER(c_uint8)]),
    "take_f64": ("daedalus_take_f64", [c_uint64, _VP, c_size_t, POINTER(c_double), POINTER(c_uint8)]),
    "take_f64_at": ("daedalus_take_f64_at", [c_uint64, c_size_t, POINTER(c_double), POINTER(c_uint8)]),
    "take_bool": ("daedalus_take_bool", [c_uint64, _VP, c_size_t, POINTER(c_uint8), POINTER(c_uint8)]),
    "take_bool_at": ("daedalus_take_bool_at", [c_uint64, c_size_t, POINTER(c_uint8), POINTER(c_uint8)]),
    "take_bytes": (
        "daedalus_take_bytes",
        [c_uint64, _VP, c_size_t, _VP, c_size_t, _LEN, POINTER(c_uint8)],
    ),
    "take_bytes_at": (
        "daedalus_take_bytes_at",
        [c_uint64, c_size_t, _VP, c_size_t, _LEN, POINTER(c_uint8)],
    ),
}


class _Api:
    def __init__(self, library: ctypes.CDLL):
        self.native = Native(library, _PREFIX)
        for attribute, (name, arguments) in _SIGNATURES.items():
            setattr(self, attribute, declare(library, name, _STATUS, *arguments))


_LIBRARIES: dict[str, _Api] = {}
_LIBRARIES_LOCK = threading.Lock()


def library_for(path: Any = None) -> _Api:
    """The host library at `path`, or at `DAEDALUS_HOST_LIB`. Loaded once per path."""
    resolved = os.fspath(path) if path is not None else os.environ.get(ENV_VAR, "")
    if not resolved:
        raise LibraryError(f"no host library: give its path, or set {ENV_VAR}. {_BUILD_HINT}")
    with _LIBRARIES_LOCK:
        api = _LIBRARIES.get(resolved)
        if api is None:
            api = _Api(load_library(resolved, ENV_VAR, _BUILD_HINT))
            _LIBRARIES[resolved] = api
        return api


def _fail(api: _Api, status: int) -> DaedalusError:
    return api.native.failure(status)


def _check(api: _Api, status: int) -> None:
    if status != STATUS_OK:
        raise _fail(api, status)


def _sized_text(api: _Api, function: Any, *args: Any) -> str:
    return api.native.sized(lambda *rest: function(*args, *rest)).decode("utf-8")


class Host:
    """An in-process host: a set of loaded plugins, and the graphs compiled on it.

    Use it as a context manager, or call `close()`, which closes its graphs too."""

    def __init__(self, library: Any = None):
        self._api = library_for(library)
        handle = c_uint64(0)
        _check(self._api, self._api.host_create(byref(handle)))
        self._handle = handle.value
        self._graphs: list[Graph] = []
        self._closed = False

    def __enter__(self) -> "Host":
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def close(self) -> None:
        """Closes the graphs this host compiled that are still open, then the host."""
        if self._closed:
            return
        for graph in list(self._graphs):
            graph.close()
        self._closed = True
        _check(self._api, self._api.host_release(self._handle))

    @property
    def closed(self) -> bool:
        return self._closed

    def _check_open(self) -> None:
        if self._closed:
            raise DaedalusError(3, "stale_handle", "the host is closed")

    @staticmethod
    def version(library: Any = None) -> tuple[int, int, int]:
        """The library's surface version: its major and minor numbers, and its layout hash."""
        api = library_for(library)
        version = _Version()
        _check(api, api.version(byref(version)))
        return version.major, version.minor, version.layout_hash

    def install(self, path: Any) -> None:
        """Loads the plugin at `path`: its nodes join the host's, and its backends run them."""
        self._check_open()
        raw = text_bytes(os.fspath(path), "plugin path")
        _check(self._api, self._api.host_install(self._handle, raw, len(raw)))

    def schemas(self) -> Any:
        """The host's nodes, as parsed JSON."""
        self._check_open()
        text = _sized_text(self._api, self._api.host_schemas, self._handle)
        return json.loads(text)

    def compile(self, graph_json: str) -> "Graph":
        """Compiles a graph given as JSON text in the graph format. The graph is planned against
        this host's nodes before anything runs; a graph that does not plan is refused."""
        self._check_open()
        raw = text_bytes(graph_json, "graph document")
        handle = c_uint64(0)
        _check(self._api, self._api.graph_compile(self._handle, raw, len(raw), byref(handle)))
        graph = Graph(self._api, handle.value, self)
        self._graphs.append(graph)
        return graph

    def _forget(self, graph: "Graph") -> None:
        if graph in self._graphs:
            self._graphs.remove(graph)


class Graph:
    """A compiled graph: push values to its input ports, tick it, and take from its output ports.

    Ports are named by text, or by index. `ports` lists them."""

    def __init__(self, api: _Api, handle: int, host: Host):
        self._api = api
        self._handle = handle
        self._host = host
        self._closed = False
        self._ports: Optional[list[Port]] = None

    def __enter__(self) -> "Graph":
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def close(self) -> None:
        """Releases the graph. Its values are dropped."""
        if self._closed:
            return
        self._closed = True
        self._host._forget(self)
        _check(self._api, self._api.graph_release(self._handle))

    @property
    def closed(self) -> bool:
        return self._closed

    def _check_open(self) -> None:
        if self._closed:
            raise DaedalusError(3, "stale_handle", "the graph is closed")

    @property
    def ports(self) -> list[Port]:
        """The graph's host ports, in index order (sorted by name)."""
        self._check_open()
        if self._ports is None:
            count = c_size_t(0)
            _check(self._api, self._api.graph_port_count(self._handle, byref(count)))
            found = []
            for index in range(count.value):
                record = _PortStruct()
                _check(self._api, self._api.graph_port(self._handle, index, byref(record)))
                found.append(
                    Port(
                        name=ctypes.string_at(record.name, record.name_len).decode("utf-8"),
                        type_key=ctypes.string_at(record.type_key, record.type_key_len).decode("utf-8"),
                        is_input=bool(record.is_input),
                    )
                )
            self._ports = found
        return list(self._ports)

    def port_index(self, name: str) -> int:
        """The index of the host port called `name`."""
        self._check_open()
        raw = text_bytes(name, "port name")
        index = c_size_t(0)
        _check(self._api, self._api.graph_port_index(self._handle, raw, len(raw), byref(index)))
        return index.value

    def _index(self, port: Union[str, int]) -> int:
        if isinstance(port, bool) or not isinstance(port, (str, int)):
            raise TypeError("a port is named by text, or by its index")
        if isinstance(port, int):
            if port < 0:
                raise ValueError("a port index is not negative")
            return port
        return self.port_index(port)

    def _port_info(self, port: Union[str, int]) -> Port:
        index = self._index(port)
        ports = self.ports
        if index >= len(ports):
            raise DaedalusError(7, "unknown_port", f"no port {port!r}")
        return ports[index]

    def push(self, port: Union[str, int], value: Any) -> None:
        """Pushes a value into an input port. A bool goes to a bool port, an int to an integer
        port that fits it, a float to a float port, and bytes to a bytes port. Other values raise
        `UnsupportedPort`."""
        self._check_open()
        if not isinstance(value, (bool, int, float, bytes, bytearray, memoryview)):
            raise UnsupportedPort(f"this host takes ints, floats, bools and bytes, not {type(value).__name__}")
        index = self._index(port)
        api = self._api
        handle = self._handle
        if isinstance(value, bool):
            if isinstance(port, str):
                raw = text_bytes(port, "port name")
                _check(api, api.push_bool(handle, raw, len(raw), 1 if value else 0))
            else:
                _check(api, api.push_bool_at(handle, index, 1 if value else 0))
        elif isinstance(value, int):
            if isinstance(port, str):
                raw = text_bytes(port, "port name")
                _check(api, api.push_i64(handle, raw, len(raw), value))
            else:
                _check(api, api.push_i64_at(handle, index, value))
        elif isinstance(value, float):
            if isinstance(port, str):
                raw = text_bytes(port, "port name")
                _check(api, api.push_f64(handle, raw, len(raw), value))
            else:
                _check(api, api.push_f64_at(handle, index, value))
        elif isinstance(value, (bytes, bytearray, memoryview)):
            data = bytes(value)
            if isinstance(port, str):
                raw = text_bytes(port, "port name")
                _check(api, api.push_bytes(handle, raw, len(raw), data, len(data)))
            else:
                _check(api, api.push_bytes_at(handle, index, data, len(data)))

    def tick(self) -> Tick:
        """Runs one tick. A step that failed is counted; the graph keeps running."""
        self._check_open()
        out = _TickStruct()
        _check(self._api, self._api.tick(self._handle, byref(out)))
        return Tick(idle=bool(out.idle), step_calls=out.step_calls, faults=out.faults)

    def take(self, port: Union[str, int]) -> Any:
        """Takes the oldest value of an output port, or None when there is none. The port's type
        decides how it is read: an integer type gives an int, a float type a float, `bool` a bool
        and `bytes` bytes. Any other type raises `UnsupportedPort`."""
        self._check_open()
        info = self._port_info(port)
        key = info.type_key
        if key not in _INTEGER_TYPES and key not in _FLOAT_TYPES and key not in ("bool", "bytes"):
            raise UnsupportedPort(f"port {info.name} carries {key}, which this host API does not take")
        index = self._index(port)
        present = c_uint8(0)
        api = self._api
        handle = self._handle
        name = text_bytes(port, "port name") if isinstance(port, str) else None
        if key == "bytes":
            return self._take_bytes(name, index)
        if key == "bool":
            out_bool = c_uint8(0)
            if name is None:
                status = api.take_bool_at(handle, index, byref(out_bool), byref(present))
            else:
                status = api.take_bool(handle, name, len(name), byref(out_bool), byref(present))
            _check(api, status)
            return bool(out_bool.value) if present.value else None
        if key in _FLOAT_TYPES:
            out_float = c_double(0.0)
            if name is None:
                status = api.take_f64_at(handle, index, byref(out_float), byref(present))
            else:
                status = api.take_f64(handle, name, len(name), byref(out_float), byref(present))
            _check(api, status)
            return out_float.value if present.value else None
        out_int = c_int64(0)
        if name is None:
            status = api.take_i64_at(handle, index, byref(out_int), byref(present))
        else:
            status = api.take_i64(handle, name, len(name), byref(out_int), byref(present))
        _check(api, status)
        return out_int.value if present.value else None

    def _take_bytes(self, name: Optional[bytes], index: int) -> Optional[bytes]:
        api = self._api
        present = c_uint8(0)
        length = c_size_t(0)
        cap = 256
        for _ in range(16):
            buffer = (c_uint8 * cap)()
            if name is None:
                status = api.take_bytes_at(self._handle, index, buffer, cap, byref(length), byref(present))
            else:
                status = api.take_bytes(
                    self._handle, name, len(name), buffer, cap, byref(length), byref(present)
                )
            if status == STATUS_OK:
                return bytes(buffer[: length.value]) if present.value else None
            if status != STATUS_BUFFER_TOO_SMALL:
                raise _fail(api, status)
            cap = length.value
        raise DaedalusError(STATUS_BUFFER_TOO_SMALL, "buffer_too_small", "the value kept growing")

    def explain(self) -> Any:
        """How the graph was planned, as parsed JSON."""
        self._check_open()
        return json.loads(_sized_text(self._api, self._api.graph_explain, self._handle))

    def telemetry(self) -> dict:
        """What the graph has done since it was compiled."""
        self._check_open()
        out = _Telemetry()
        _check(self._api, self._api.graph_telemetry(self._handle, byref(out)))
        return {
            "ticks": out.ticks,
            "idle_ticks": out.idle_ticks,
            "step_calls": out.step_calls,
            "faults": out.faults,
            "pushes": out.pushes,
            "takes": out.takes,
        }
