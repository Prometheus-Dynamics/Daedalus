"""The service client: drives the daemon from Python, through the client C ABI with ctypes.

The daemon is the one that plans, runs and keeps graphs. This module is a Python face over
`libdaedalus_client`, the library `languages/client-capi` builds. Each request is one call into
that library, so the protocol, its error codes and its layout are the Rust client's. A value
crosses the boundary in its wire encoding, and the codec is this package's `wire` module.

Give the library's path to `Client.connect(..., library=PATH)`, or set `DAEDALUS_CLIENT_LIB` to it.
cargo builds it: `cargo build -p daedalus-client-capi` leaves the shared library under `target/debug/`,
named `libdaedalus_client_capi` with the platform's extension (`.so`, `.dylib` or `.dll`).

Lifetime and threads, as the ctypes layer assumes them:

- A `Client` owns a connection. `close()`, or leaving a `with` block, closes it. Closing any handle
  made from a connection (a session, say) closes the connection for all of them, as the C header
  says, so the root client is closed last.
- A `Watch` owns one subscription. `close()` releases it, and iterating it to its end, or to a
  timeout, closes it too. A watch that is never closed keeps its queue until the connection closes.
- Every call passes buffers that live until it returns. A call that is retried for a larger buffer
  keeps the value it took out for the retry, so no value is lost.
- One client may be used from several threads, as the C ABI allows: ctypes releases the GIL for
  each call. A close made while another thread calls on the same client makes that call fail with
  `stale_handle`.
"""

from __future__ import annotations

import ctypes
import json
import os
import threading
import time
from collections import namedtuple
from ctypes import POINTER, byref, c_char_p, c_int32, c_size_t, c_uint8, c_uint32, c_uint64, c_void_p
from dataclasses import dataclass
from typing import Any, Iterator, Optional, Sequence, Union

from . import wire
from ._ffi import (
    STATUS_BUFFER_TOO_SMALL,
    STATUS_OK,
    STATUS_STALE_HANDLE,
    DaedalusError,
    LibraryError,
    Native,
    ServiceError,
    declare,
    load_library,
    text_bytes,
    u32_range,
    u64_range,
)

__all__ = [
    "Client",
    "ClosedError",
    "DaedalusError",
    "EVENT_ALL",
    "EVENT_FAULT",
    "EVENT_TELEMETRY",
    "LibraryError",
    "Run",
    "SERVICE_CODES",
    "ServiceError",
    "StartConfig",
    "Typed",
    "Watch",
    "WatchTimeout",
    "library_for",
]

ENV_VAR = "DAEDALUS_CLIENT_LIB"
_BUILD_HINT = "Build it with `cargo build -p daedalus-client-capi`."
_PREFIX = "daedalus_client"

# The daemon's error codes by number, as the protocol gives them (1 to 12). 65535 is a code this
# library does not know, and reads as `other`. A refusal with no code is not from the daemon.
SERVICE_CODES = {
    1: "invalid_request",
    2: "unknown_graph",
    3: "unknown_run",
    4: "unknown_port",
    5: "value_type",
    6: "plugin_error",
    7: "planning_failed",
    8: "busy",
    9: "cancelled",
    10: "stopped",
    11: "frame_too_large",
    12: "unsupported",
    65535: "other",
}

_STATUS_SERVICE_ERROR = 21
# A well-formed endpoint this platform cannot serve: a unix socket path where there are no unix
# sockets. It is a DaedalusError whose message says so, not a bad endpoint.
STATUS_UNSUPPORTED = 30
_PLAN_FORMS = {"steps": 0, "blob": 1, "source": 2}
_TELEMETRY = {"off": 0, "basic": 1, "detailed": 2, "trace": 3}
_STOP_MODES = {"drain": 0, "immediate": 1}
EVENT_FAULT = 1
EVENT_TELEMETRY = 2
EVENT_ALL = 3
_EVENT_KINDS = {"fault": EVENT_FAULT, "telemetry": EVENT_TELEMETRY, "all": EVENT_ALL}
_ALL_RUNS = 0
_WATCH_EVENT = 1
_WATCH_ENDED = 2
_WAIT_SLICE_MS = 1000
_TAKE_CAP = 256


class ClosedError(DaedalusError):
    """The client or the watch was closed before this call."""

    def __init__(self, what: str):
        super().__init__(STATUS_STALE_HANDLE, "stale_handle", f"the {what} is closed")


class WatchTimeout(TimeoutError):
    """No event came from a watch within the time asked for. The watch is still open."""


@dataclass(frozen=True)
class StartConfig:
    """How a run is started, or a plan made: the protocol's start options.

    `telemetry` is `off`, `basic`, `detailed` or `trace`. `priority` names the backends in the
    order ties go to. The defaults are the protocol's: the host's own fifo and fault reporting,
    basic telemetry, merged steps and no priority."""

    fifo: int = 0
    faults: int = 0
    telemetry: str = "basic"
    unmerged: bool = False
    priority: Sequence[str] = ()

    def __post_init__(self) -> None:
        u32_range(self.fifo, "fifo")
        u32_range(self.faults, "faults")
        if self.telemetry not in _TELEMETRY:
            raise ValueError(f"telemetry must be one of {sorted(_TELEMETRY)}, not {self.telemetry!r}")


# A take's value: its wire type (a tuple, as `wire` reads it) and the Python value.
Typed = namedtuple("Typed", ["ty", "value"])


# ---- the library's prototypes -----------------------------------------------------------------


class _Text(ctypes.Structure):
    _fields_ = [("data", c_char_p), ("len", c_size_t)]


class _Version(ctypes.Structure):
    _fields_ = [("protocol", c_uint32), ("layout_hash", c_uint64)]


class _Config(ctypes.Structure):
    _fields_ = [
        ("fifo_capacity", c_uint32),
        ("fault_reports", c_uint32),
        ("telemetry", c_uint32),
        ("unmerged", c_uint8),
        ("priority", POINTER(_Text)),
        ("priority_len", c_size_t),
    ]


class _PlanOptions(ctypes.Structure):
    _fields_ = [
        ("form", c_uint32),
        ("inspect", c_uint8),
        ("priority", POINTER(_Text)),
        ("priority_len", c_size_t),
    ]


class _Step(ctypes.Structure):
    _fields_ = [
        ("tick", c_uint64),
        ("step_calls", c_uint64),
        ("faults", c_uint64),
        ("skipped", c_uint64),
        ("ticks", c_uint32),
        ("idle", c_uint8),
    ]


class _Stopped(ctypes.Structure):
    _fields_ = [("tick", c_uint64), ("dropped", c_uint32)]


class _Fed(ctypes.Structure):
    _fields_ = [("woke", c_uint32), ("dropped", c_uint8)]


class _Carry(ctypes.Structure):
    _fields_ = [
        ("carried", c_uint32),
        ("fresh", c_uint32),
        ("added", c_uint32),
        ("removed", c_uint32),
        ("removed_ports", c_uint32),
        ("dropped_values", c_uint32),
    ]


_VP = c_void_p  # a buffer or text the caller keeps alive for the call
_LEN = POINTER(c_size_t)
_STATUS = c_int32

# Attribute, C function, and the argument types after it. Every function returns a status.
_SIGNATURES = {
    "version": ("daedalus_client_version", [POINTER(_Version)]),
    "last_error_code": ("daedalus_client_last_error_code", [POINTER(c_uint32)]),
    "connect": ("daedalus_client_connect", [_VP, c_size_t, c_uint8, POINTER(c_uint64)]),
    "close": ("daedalus_client_close", [c_uint64]),
    "in_session": ("daedalus_client_in_session", [c_uint64, _VP, c_size_t, POINTER(c_uint64)]),
    "ping": ("daedalus_client_ping", [c_uint64, POINTER(c_uint32)]),
    "load_plugin_report": ("daedalus_client_load_plugin_report", [c_uint64, _VP, c_size_t]),
    "put_graph_report": (
        "daedalus_client_put_graph_report",
        [c_uint64, _VP, c_size_t, _VP, c_size_t],
    ),
    "patch_graph_report": (
        "daedalus_client_patch_graph_report",
        [c_uint64, _VP, c_size_t, _VP, c_size_t],
    ),
    "report": ("daedalus_client_report", [c_uint64, _VP, c_size_t, _LEN]),
    "plan_raw": (
        "daedalus_client_plan_raw",
        [c_uint64, _VP, c_size_t, POINTER(_PlanOptions), _VP, c_size_t, _LEN],
    ),
    "make_plan": (
        "daedalus_client_make_plan",
        [c_uint64, _VP, c_size_t, POINTER(_Config), POINTER(c_uint64), _VP, c_size_t, _LEN],
    ),
    "put_plan": ("daedalus_client_put_plan", [c_uint64, _VP, c_size_t, _VP, c_size_t]),
    "start": (
        "daedalus_client_start",
        [c_uint64, _VP, c_size_t, POINTER(_Config), POINTER(c_uint64)],
    ),
    "step": ("daedalus_client_step", [c_uint64, c_uint64, c_uint32, POINTER(_Step)]),
    "cancel": ("daedalus_client_cancel", [c_uint64, c_uint64]),
    "stop": ("daedalus_client_stop", [c_uint64, c_uint64, c_uint32, POINTER(_Stopped)]),
    "reload_report": ("daedalus_client_reload_report", [c_uint64, c_uint64, _VP, c_size_t]),
    "inspect_run": ("daedalus_client_inspect_run", [c_uint64, c_uint64, _VP, c_size_t, _LEN]),
    "inspect_graph": (
        "daedalus_client_inspect_graph",
        [c_uint64, _VP, c_size_t, _VP, c_size_t, _LEN],
    ),
    "graph_text": (
        "daedalus_client_graph_text",
        [c_uint64, _VP, c_size_t, _VP, c_size_t, _LEN],
    ),
    "inspect_cache": ("daedalus_client_inspect_cache", [c_uint64, _VP, c_size_t, _LEN]),
    "inspect_telemetry": (
        "daedalus_client_inspect_telemetry",
        [c_uint64, c_uint64, _VP, c_size_t, _LEN],
    ),
    "inspect_host": ("daedalus_client_inspect_host", [c_uint64, _VP, c_size_t, _LEN]),
    "cache_clear": ("daedalus_client_cache_clear", [c_uint64]),
    "trace": ("daedalus_client_trace", [c_uint64, c_uint32, _VP, c_size_t, _LEN]),
    "feed_wire": (
        "daedalus_client_feed_wire",
        [c_uint64, c_uint64, _VP, c_size_t, _VP, c_size_t, POINTER(_Fed)],
    ),
    "take_wire": (
        "daedalus_client_take_wire",
        [c_uint64, c_uint64, _VP, c_size_t, _VP, c_size_t, _LEN, POINTER(c_uint8)],
    ),
    "watch_open": ("daedalus_client_watch_open", [c_uint64, c_uint64, c_uint32, POINTER(c_uint64)]),
    "watch_next": (
        "daedalus_client_watch_next",
        [c_uint64, c_uint32, _VP, c_size_t, _LEN, POINTER(c_uint32)],
    ),
    "watch_close": ("daedalus_client_watch_close", [c_uint64]),
}


class _Api:
    """The library, with each function declared: its prototypes, and its status and error calls."""

    def __init__(self, library: ctypes.CDLL):
        self.library = library
        self.native = Native(library, _PREFIX)
        for attribute, (name, arguments) in _SIGNATURES.items():
            setattr(self, attribute, declare(library, name, _STATUS, *arguments))


_LIBRARIES: dict[str, _Api] = {}
_LIBRARIES_LOCK = threading.Lock()


def library_for(path: Any = None) -> _Api:
    """The client library at `path`, or at `DAEDALUS_CLIENT_LIB`. It is loaded once per path, and
    its protocol and layout must be this SDK's, or `LibraryError` is raised."""
    resolved = os.fspath(path) if path is not None else os.environ.get(ENV_VAR, "")
    if not resolved:
        raise LibraryError(f"no client library: give its path, or set {ENV_VAR}. {_BUILD_HINT}")
    with _LIBRARIES_LOCK:
        api = _LIBRARIES.get(resolved)
        if api is not None:
            return api
        api = _Api(load_library(resolved, ENV_VAR, _BUILD_HINT))
        version = _Version()
        status = api.version(byref(version))
        if status != STATUS_OK:
            raise api.native.failure(status)
        if version.protocol != wire.SERVICE_VERSION or version.layout_hash != wire.SERVICE_LAYOUT_HASH:
            raise LibraryError(
                f"the client library speaks service protocol {version.protocol} with layout "
                f"{version.layout_hash:#x}; this SDK speaks {wire.SERVICE_VERSION} with "
                f"{wire.SERVICE_LAYOUT_HASH:#x}"
            )
        _LIBRARIES[resolved] = api
        return api


# ---- argument helpers -------------------------------------------------------------------------


def _run_id(run: Any) -> int:
    """A run's number, from a `Run` or an int. Run numbers start at one."""
    if isinstance(run, Run):
        return run.id
    return u64_range(run, "run")


def _plan_form(form: str) -> int:
    if form not in _PLAN_FORMS:
        raise ValueError(f"the plan form must be one of {sorted(_PLAN_FORMS)}, not {form!r}")
    return _PLAN_FORMS[form]


def _texts(names: Sequence[str], what: str) -> tuple[Optional[Any], list]:
    """A C array of text structs for `names`, and the bytes they point into. Both must live for
    the call."""
    raw = [text_bytes(name, what) for name in names]
    items = [_Text(data=item, len=len(item)) for item in raw]
    if not items:
        return None, raw
    return (_Text * len(items))(*items), raw


def _start_config(config: Optional[StartConfig]) -> tuple[_Config, list]:
    """The C struct of a start config, and the objects it points into."""
    if config is None:
        config = StartConfig()
    if not isinstance(config, StartConfig):
        raise TypeError("the config must be a StartConfig")
    array, keep = _texts(config.priority, "priority name")
    struct = _Config(
        fifo_capacity=config.fifo,
        fault_reports=config.faults,
        telemetry=_TELEMETRY[config.telemetry],
        unmerged=1 if config.unmerged else 0,
        priority=array,
        priority_len=len(keep),
    )
    return struct, [array, keep]


def _typed_bytes(ty: tuple, value: Any) -> bytes:
    out = wire.Writer()
    wire.encode_type(ty, out)
    wire.encode_value(ty, value, wire.Schema(), out)
    return bytes(out.buf)


def _infer_type(value: Any) -> tuple:
    """The wire type of a value that was given none: unit, bool, i64, f64, string or bytes."""
    if value is None:
        return ("scalar", "unit")
    if isinstance(value, bool):
        return ("scalar", "bool")
    if isinstance(value, int):
        return ("scalar", "i64")
    if isinstance(value, float):
        return ("scalar", "f64")
    if isinstance(value, str):
        return ("scalar", "string")
    if isinstance(value, (bytes, bytearray, memoryview)):
        return ("scalar", "bytes")
    raise TypeError(f"no wire type for a {type(value).__name__}; pass ty=")


def _decode_typed(data: bytes) -> Typed:
    inp = wire.Reader(data)
    ty = wire.decode_type(inp)
    value = wire.decode_value(ty, inp, wire.Schema())
    inp.finish()
    return Typed(ty, value)


def _json(data: bytes) -> Any:
    return json.loads(data.decode("utf-8"))


# ---- the connection ---------------------------------------------------------------------------


class _Connection:
    """The connection behind a root client and its sessions. Any handle's close closes it, so
    closing is done once, for every handle."""

    def __init__(self, api: _Api):
        self.api = api
        self.lock = threading.Lock()
        self.handles: list[int] = []
        self.closed = False

    def close(self) -> None:
        with self.lock:
            if self.closed:
                return
            self.closed = True
            handles = list(reversed(self.handles))
        for handle in handles:
            self.api.close(handle)


class Client:
    """A connection to the daemon, and the calls it takes.

    Use it as a context manager, or call `close()`. A session made with `session()` shares the
    connection."""

    def __init__(self, api: _Api, connection: _Connection, handle: int):
        self._api = api
        self._connection = connection
        self._handle = handle
        self._closed = False

    @classmethod
    def connect(
        cls,
        endpoint: str,
        *,
        library: Any = None,
        reconnect: bool = False,
    ) -> "Client":
        """Connects to the daemon at `endpoint`: `unix:/path`, or `tcp:127.0.0.1:PORT`, loopback
        only. `library` is the client library's path (see the module doc). `reconnect` keeps the
        connection trying again after a drop.

        A `unix:` endpoint on a platform with no unix sockets raises `DaedalusError` with status
        `STATUS_UNSUPPORTED`, and its message says unix sockets are not available there. Use a
        `tcp:127.0.0.1:PORT` endpoint on such a platform."""
        if not isinstance(endpoint, str):
            raise TypeError("the endpoint must be a str")
        if not (endpoint.startswith("unix:") or endpoint.startswith("tcp:")):
            raise ValueError(f"the endpoint must be unix:PATH or tcp:ADDRESS, not {endpoint!r}")
        api = library_for(library)
        raw = endpoint.encode("utf-8")
        handle = c_uint64(0)
        status = api.connect(raw, len(raw), 1 if reconnect else 0, byref(handle))
        if status != STATUS_OK:
            raise api.native.failure(status)
        connection = _Connection(api)
        connection.handles.append(handle.value)
        return cls(api, connection, handle.value)

    # -- lifetime --

    def close(self) -> None:
        """Closes the connection: this client, and every session made from it."""
        if self._closed:
            return
        self._closed = True
        self._connection.close()

    def __enter__(self) -> "Client":
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    @property
    def closed(self) -> bool:
        return self._closed or self._connection.closed

    def _check_open(self) -> None:
        if self.closed:
            raise ClosedError("client")

    def _failure(self, status: int) -> DaedalusError:
        native = self._api.native
        if status != _STATUS_SERVICE_ERROR:
            return native.failure(status)
        number = c_uint32(0)
        code = None
        if self._api.last_error_code(byref(number)) == STATUS_OK and number.value:
            code = SERVICE_CODES.get(number.value, "other")
        return native.failure(status, code or "other")

    def _call(self, function: Any, *args: Any) -> None:
        self._check_open()
        status = function(self._handle, *args)
        if status != STATUS_OK:
            raise self._failure(status)

    def _text(self, function: Any, *args: Any) -> bytes:
        """The text a call writes into a buffer after its arguments (size-query convention)."""
        self._check_open()
        return self._api.native.sized(
            lambda *rest: function(self._handle, *args, *rest), failure=self._failure
        )

    def _json_call(self, function: Any, *args: Any) -> Any:
        return _json(self._text(function, *args))

    # -- sessions --

    def session(self, name: str = "default") -> "Client":
        """A client whose requests are in the session `name`. It shares this connection."""
        self._check_open()
        raw = text_bytes(name, "session name")
        handle = c_uint64(0)
        status = self._api.in_session(self._handle, raw, len(raw), byref(handle))
        if status != STATUS_OK:
            raise self._failure(status)
        self._connection.handles.append(handle.value)
        return Client(self._api, self._connection, handle.value)

    # -- plugins and graphs --

    def ping(self) -> int:
        """The daemon's protocol version."""
        version = c_uint32(0)
        self._call(self._api.ping, byref(version))
        return version.value

    def load_plugin(self, path: str) -> dict:
        """Loads the plugin at `path` into the daemon. Returns its record: `name`, `generation`,
        `nodes` and `backends`."""
        raw = text_bytes(path, "plugin path")
        self._call(self._api.load_plugin_report, raw, len(raw))
        return self._json_call(self._api.report)

    def put_graph(self, name: str, json_text: str) -> list:
        """Stores a graph document (JSON text) under `name`. Returns the lints the daemon gave, each
        a dict with `severity`, `code`, `message`, `node` and `port`."""
        raw_name = text_bytes(name, "graph name")
        raw_json = text_bytes(json_text, "graph document")
        self._call(self._api.put_graph_report, raw_name, len(raw_name), raw_json, len(raw_json))
        return self._json_call(self._api.report)

    def patch_graph(self, graph: str, patch_text: str) -> list:
        """Applies a patch document (JSON text) to a stored graph. Returns the lints."""
        raw_graph = text_bytes(graph, "graph name")
        raw_patch = text_bytes(patch_text, "patch document")
        self._call(
            self._api.patch_graph_report, raw_graph, len(raw_graph), raw_patch, len(raw_patch)
        )
        return self._json_call(self._api.report)

    def plan(
        self,
        graph: str,
        form: str = "steps",
        *,
        inspect: bool = False,
        priority: Sequence[str] = (),
    ) -> dict:
        """Plans a stored graph. The plan's `hash` (a hex string), `form`, counts and `diagnostics`.
        With `inspect`, `explain` is the planner's explanation as the text it wrote, so its keys keep
        their written order (`json.loads` reads it); None when there is none."""
        raw = text_bytes(graph, "graph name")
        array, keep = _texts(priority, "priority name")
        options = _PlanOptions(
            form=_plan_form(form),
            inspect=1 if inspect else 0,
            priority=array,
            priority_len=len(keep),
        )
        return self._json_call(self._api.plan_raw, raw, len(raw), byref(options))

    def make_plan(self, graph: str, config: Optional[StartConfig] = None) -> tuple[int, bytes]:
        """Plans a stored graph into a made plan. Returns its hash and its bytes. A made plan
        loads with no planner: `put_plan` stores it, and `start` runs it."""
        raw = text_bytes(graph, "graph name")
        struct, keep = _start_config(config)
        plan_hash = c_uint64(0)
        self._check_open()
        plan_bytes = self._api.native.sized(
            lambda *rest: self._api.make_plan(
                self._handle, raw, len(raw), byref(struct), byref(plan_hash), *rest
            ),
            failure=self._failure,
        )
        del keep  # the struct points into them until the call returns
        return plan_hash.value, plan_bytes

    def put_plan(self, name: str, data: bytes) -> None:
        """Stores a made plan's bytes under `name`. A run started on that name uses it."""
        raw_name = text_bytes(name, "plan name")
        raw = bytes(data)
        self._call(self._api.put_plan, raw_name, len(raw_name), raw, len(raw))

    # -- runs --

    def start(self, graph: str, config: Optional[StartConfig] = None) -> "Run":
        """Plans a stored graph (or a made plan stored under its name) and starts a run."""
        raw = text_bytes(graph, "graph name")
        struct, keep = _start_config(config)
        run = c_uint64(0)
        self._call(self._api.start, raw, len(raw), byref(struct), byref(run))
        del keep
        return Run(self, run.value)

    def step(self, run: Any, count: int = 1) -> dict:
        """Runs `count` ticks of a run. Returns what they did: `ticks`, `tick`, `step_calls`,
        `faults`, `skipped` and `idle`."""
        u32_range(count, "count")
        out = _Step()
        self._call(self._api.step, _run_id(run), count, byref(out))
        return {
            "ticks": out.ticks,
            "tick": out.tick,
            "step_calls": out.step_calls,
            "faults": out.faults,
            "skipped": out.skipped,
            "idle": bool(out.idle),
        }

    def cancel(self, run: Any) -> None:
        """Cancels a run. It stays until it is stopped."""
        self._call(self._api.cancel, _run_id(run))

    def stop(self, run: Any, mode: str = "drain") -> dict:
        """Stops a run. `drain` finishes what is waiting; `immediate` drops it. Returns `tick` and
        `dropped`."""
        if mode not in _STOP_MODES:
            raise ValueError(f"the stop mode must be one of {sorted(_STOP_MODES)}, not {mode!r}")
        out = _Stopped()
        self._call(self._api.stop, _run_id(run), _STOP_MODES[mode], byref(out))
        return {"tick": out.tick, "dropped": out.dropped}

    def feed(self, run: Any, port: str, value: Any, ty: Optional[tuple] = None) -> dict:
        """Feeds a value to an input port of a run. `ty` is its wire type, such as `wire.T.u32`.
        Without it the type is read from the value: unit, bool, i64, f64, string or bytes.

        A value that does not fit `ty` raises `wire.WireError` before anything is sent. Returns
        `woke` (the steps woken) and `dropped`."""
        raw_port = text_bytes(port, "port")
        encoded = _typed_bytes(ty if ty is not None else _infer_type(value), value)
        out = _Fed()
        self._call(
            self._api.feed_wire,
            _run_id(run),
            raw_port,
            len(raw_port),
            encoded,
            len(encoded),
            byref(out),
        )
        return {"woke": out.woke, "dropped": bool(out.dropped)}

    def take(self, run: Any, port: str) -> Optional[Typed]:
        """Takes the oldest value of an output port: a `Typed` (its wire type and value), or None
        when there is none. A value a buffer is too small for waits for the next take."""
        self._check_open()
        raw_port = text_bytes(port, "port")
        run_id = _run_id(run)
        present = c_uint8(0)
        length = c_size_t(0)
        cap = _TAKE_CAP
        for _ in range(16):
            buffer = (c_uint8 * cap)()
            status = self._api.take_wire(
                self._handle, run_id, raw_port, len(raw_port), buffer, cap, byref(length), byref(present)
            )
            if status == STATUS_OK:
                if not present.value:
                    return None
                return _decode_typed(bytes(buffer[: length.value]))
            if status != STATUS_BUFFER_TOO_SMALL:
                raise self._failure(status)
            cap = length.value
        raise DaedalusError(STATUS_BUFFER_TOO_SMALL, "buffer_too_small", "the value kept growing")

    def reload(self, run: Any, graph: str) -> dict:
        """Replaces a run's graph with a stored one. Returns the report: `carried`, `fresh`,
        `added`, `removed` and `removed_ports` (each `names` and `total`), and `dropped_values`."""
        raw = text_bytes(graph, "graph name")
        self._call(self._api.reload_report, _run_id(run), raw, len(raw))
        return self._json_call(self._api.report)

    # -- inspection --

    def inspect_run(self, run: Any) -> dict:
        return self._json_call(self._api.inspect_run, _run_id(run))

    def inspect_graph(self, name: str) -> dict:
        raw = text_bytes(name, "graph name")
        return self._json_call(self._api.inspect_graph, raw, len(raw))

    def graph_text(self, name: str) -> str:
        """A stored graph's document, as the text the daemon holds: not re-written, so its key order
        and layout are the ones it was stored with."""
        raw = text_bytes(name, "graph name")
        return self._text(self._api.graph_text, raw, len(raw)).decode("utf-8")

    def inspect_cache(self) -> dict:
        return self._json_call(self._api.inspect_cache)

    def inspect_telemetry(self, run: Any) -> dict:
        return self._json_call(self._api.inspect_telemetry, _run_id(run))

    def inspect_host(self) -> dict:
        return self._json_call(self._api.inspect_host)

    def cache_clear(self) -> None:
        """Empties the daemon's plan cache."""
        self._call(self._api.cache_clear)

    def trace(self, limit: int = 0) -> dict:
        """The newest `limit` requests the daemon served, oldest first. Zero is all it keeps."""
        u32_range(limit, "limit")
        return self._json_call(self._api.trace, limit)

    # -- events --

    def watch(
        self,
        run: Any = None,
        kinds: Union[str, int] = "all",
        *,
        timeout: Optional[float] = None,
    ) -> "Watch":
        """Subscribes to the events of `run` (every run when None) of `kinds`: `fault`,
        `telemetry` or `all`. Iterating the watch gives its events. The iteration stops when the
        stream ends, or, given `timeout` in seconds, when no event comes in that time; the watch
        then closes itself."""
        self._check_open()
        if isinstance(kinds, str):
            if kinds not in _EVENT_KINDS:
                raise ValueError(f"the kinds must be one of {sorted(_EVENT_KINDS)}, not {kinds!r}")
            bits = _EVENT_KINDS[kinds]
        else:
            bits = u32_range(kinds, "kinds")
        if run is None:
            target = _ALL_RUNS
        else:
            target = _run_id(run)
            if target == _ALL_RUNS:
                raise ValueError("run numbers start at 1; pass run=None to watch every run")
        handle = c_uint64(0)
        status = self._api.watch_open(self._handle, target, bits, byref(handle))
        if status != STATUS_OK:
            raise self._failure(status)
        return Watch(self._api, handle.value, timeout)


class Run:
    """A run started on a client: its number, and the calls that act on it."""

    def __init__(self, client: Client, run: int):
        self._client = client
        self.id = run

    def __repr__(self) -> str:
        return f"Run({self.id})"

    def feed(self, port: str, value: Any, ty: Optional[tuple] = None) -> dict:
        return self._client.feed(self, port, value, ty)

    def take(self, port: str) -> Optional[Typed]:
        return self._client.take(self, port)

    def step(self, count: int = 1) -> dict:
        return self._client.step(self, count)

    def cancel(self) -> None:
        self._client.cancel(self)

    def stop(self, mode: str = "drain") -> dict:
        return self._client.stop(self, mode)

    def reload(self, graph: str) -> dict:
        return self._client.reload(self, graph)

    def inspect(self) -> dict:
        return self._client.inspect_run(self)

    def telemetry(self) -> dict:
        return self._client.inspect_telemetry(self)

    def watch(self, kinds: Union[str, int] = "all", *, timeout: Optional[float] = None) -> "Watch":
        return self._client.watch(self, kinds, timeout=timeout)


class Watch:
    """One subscription to events.

    `next_event` gives the next event, `drain` the ones already queued, and `close` releases the
    subscription. As an iterator it gives events until the stream ends, or until the timeout passes
    with none, and then it closes itself."""

    def __init__(self, api: _Api, handle: int, timeout: Optional[float]):
        self._api = api
        self._handle = handle
        self._timeout = timeout
        self._closed = False
        self._lock = threading.Lock()

    @property
    def closed(self) -> bool:
        return self._closed

    def close(self) -> None:
        """Releases the watch. Events it has not given are dropped."""
        with self._lock:
            if self._closed:
                return
            self._closed = True
            status = self._api.watch_close(self._handle)
        if status != STATUS_OK:
            raise self._api.native.failure(status)

    def __enter__(self) -> "Watch":
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def _poll(self, wait_ms: int) -> tuple[int, Optional[dict]]:
        """One poll of the watch: its state, and the event when there is one."""
        if self._closed:
            raise ClosedError("watch")
        state = c_uint32(0)
        length = c_size_t(0)
        cap = 1024
        for _ in range(16):
            buffer = (c_uint8 * cap)()
            status = self._api.watch_next(
                self._handle, wait_ms, buffer, cap, byref(length), byref(state)
            )
            if status == STATUS_OK:
                if state.value == _WATCH_EVENT:
                    return state.value, _json(bytes(buffer[: length.value]))
                return state.value, None
            if status != STATUS_BUFFER_TOO_SMALL:
                raise self._api.native.failure(status)
            cap = length.value
        raise DaedalusError(STATUS_BUFFER_TOO_SMALL, "buffer_too_small", "the event kept growing")

    def next_event(self, timeout: Optional[float] = None) -> Optional[dict]:
        """The next event, in the order the daemon sent them. None when the stream has ended.

        With `timeout` (seconds) it raises `WatchTimeout` when no event came in time. Without one
        it waits until an event or the end."""
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            if deadline is None:
                wait = _WAIT_SLICE_MS
            else:
                left = deadline - time.monotonic()
                if left <= 0:
                    raise WatchTimeout(f"no event within {timeout} s")
                wait = min(_WAIT_SLICE_MS, max(1, int(left * 1000)))
            state, event = self._poll(wait)
            if state == _WATCH_EVENT:
                return event
            if state == _WATCH_ENDED:
                return None

    def drain(self) -> list[dict]:
        """The events already queued, without waiting. It stops at the first one not there yet, or
        at the end of the stream."""
        events: list[dict] = []
        while True:
            state, event = self._poll(0)
            if state != _WATCH_EVENT or event is None:
                return events
            events.append(event)

    def __iter__(self) -> Iterator[dict]:
        return self

    def __next__(self) -> dict:
        if self._closed:
            raise StopIteration
        try:
            event = self.next_event(self._timeout)
        except WatchTimeout:
            self.close()
            raise StopIteration from None
        if event is None:
            self.close()
            raise StopIteration
        return event
