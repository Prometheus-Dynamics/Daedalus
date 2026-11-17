"""The worker loop: reads host messages, runs node bodies, and answers in the wire protocol.

Stdout carries packets only. Node bodies see `sys.stdout` replaced by stderr, so a print cannot
corrupt the stream. A node raises `NodeError(code, message)` for a fault with that code; any other
exception is a `node_error` fault whose reason is the traceback, cut to 512 bytes.

A reader thread takes the host's packets off stdin while a node body runs. A cancel that names the
run going on sets the flag that `cx.cancelled()` reads, so a node that polls it sees the cancel
promptly. The run then ends with status `cancelled`. A cancel between runs has nothing to stop.

Large values travel as plain packets. This SDK has no shared-memory transport, by ruling: a second
implementation of the ring protocol in Python is not wanted. If shared memory in Python workers is
wanted later, it comes through the Rust implementation behind a C ABI.
"""

import inspect
import queue
import sys
import threading
import traceback
from typing import Any, Callable, Optional

from . import wire
from .wire import NodeError

# The state of a node that has none set yet. An export of such a node is a fault, never a value.
UNSET = object()


class _Gate:
    """The run going on, and whether the host has cancelled it. The reader thread sets it."""

    def __init__(self):
        self.lock = threading.Lock()
        self.run = None
        self.cancelled = threading.Event()


class Context:
    """What a node body can report. Events go out, in order, before the run's outputs."""

    def __init__(self, state: Any, cancelled: Optional[threading.Event] = None) -> None:
        self.events = []
        self.state = state
        self.new_state = state
        self._cancelled = cancelled

    def cancelled(self) -> bool:
        """True once the host has cancelled the run. A long body polls this and returns early."""
        return self._cancelled is not None and self._cancelled.is_set()

    def progress(self, fraction: float, message: str = "") -> None:
        self.events.append(("progress", float(fraction), message))

    def log(self, level: str, text: str) -> None:
        self.events.append(("log", level, text))

    def detail(self, text: object) -> None:
        """Gives the host more about a failure than the fault's reason holds. Call it any number
        of times before raising, and the pieces are kept in order with the step's fault. What a
        node writes and then doesn't fail is dropped by the host."""
        self.events.append(("detail", str(text)))

    def fault_code(self, code: int) -> None:
        """Gives this failure a number of the node's own, for a host that acts on which failure
        it was without reading the words. Call it before raising. The number means what this
        node's author says: the host passes it through untouched, with the node's id."""
        self.events.append(("fault_code", int(code)))


class Node:
    def __init__(self, id, inputs, outputs, fn, state=None):
        self.id = id
        self.inputs = inputs
        self.outputs = outputs
        self.fn = fn
        self.state = state
        self.state_value = UNSET
        self.wants_context = "cx" in inspect.signature(fn).parameters


def node(id: str, inputs: list, outputs: list, state: Any = None) -> Callable[[Callable], Callable]:
    """Declares a node. `inputs` and `outputs` are lists of (port name, type). A body that takes a
    parameter named `cx` gets a Context. The node is returned unchanged, and is kept in `NODES`."""

    def wrap(fn):
        for name, ty in list(inputs) + list(outputs):
            wire._check_type(ty)
        if state is not None:
            wire._check_type(state)
        declared = Node(id, list(inputs), list(outputs), fn, state)
        NODES.append(declared)
        return fn

    return wrap


NODES = []


def _opaque_keys(nodes):
    keys = []

    def walk(ty):
        if ty[0] == "opaque":
            keys.append(ty[1])
        elif ty[0] in ("optional", "list"):
            walk(ty[1])
        elif ty[0] == "map":
            walk(ty[1])
            walk(ty[2])
        elif ty[0] == "tuple":
            for item in ty[1]:
                walk(item)
        elif ty[0] == "struct":
            for _, item in ty[1]:
                walk(item)
        elif ty[0] == "enum":
            for _, payload in ty[1]:
                if payload is not None:
                    walk(payload)

    for declared in nodes:
        for _, ty in declared.inputs + declared.outputs:
            walk(ty)
        if declared.state is not None:
            walk(declared.state)
    return keys


def _fault_for(error):
    if isinstance(error, NodeError):
        return error.code, error.message
    return "node_error", traceback.format_exc()


class _Exact:
    """Reads exactly the bytes asked for, or fewer only at the end of the stream, from a raw stream.

    The reader thread blocks in these reads. It reads the raw file, not the buffered wrapper: a
    buffered read holds the wrapper's lock, and an interpreter that exits while the thread is still
    blocked then aborts with a fatal error. The raw file holds no lock."""

    def __init__(self, raw):
        self._raw = raw

    def read(self, count):
        chunks = []
        left = count
        while left:
            chunk = self._raw.read(left)
            if not chunk:
                break
            chunks.append(chunk)
            left -= len(chunk)
        return b"".join(chunks)


class _Reader:
    """Takes the host's packets off the stream on a thread of its own, in order. It notes which run
    is going on and sets the gate's flag when the host cancels that run. Every packet is then
    handed to the loop through a queue."""

    def __init__(self, stream, gate):
        self.packets = queue.Queue()
        # A buffered stream's raw file is read directly (see _Exact); any other stream is read as it is.
        self._source = _Exact(getattr(stream, "raw", stream))
        self._gate = gate
        self._thread = threading.Thread(target=self._read, name="daedalus-reader", daemon=True)
        self._thread.start()

    def _read(self):
        while True:
            try:
                body = wire.read_packet(self._source)
            except wire.WireError as error:
                self.packets.put(("error", error))
                return
            if body is None:
                self.packets.put(("end", None))
                return
            self._note(body)
            self.packets.put(("packet", body))

    def _note(self, body):
        if len(body) < 9 or body[0] not in (wire.KIND_RUN, wire.KIND_CANCEL):
            return
        run = int.from_bytes(body[1:9], "little")
        with self._gate.lock:
            if body[0] == wire.KIND_RUN:
                self._gate.run = run
                self._gate.cancelled.clear()
            elif run == self._gate.run:
                self._gate.cancelled.set()


def serve(
    nodes: Optional[list] = None,
    worker: str = "daedalus-python-worker",
    stdin: Any = None,
    stdout: Any = None,
) -> None:
    """Runs the worker loop until the host shuts it down or closes the pipe."""
    nodes = NODES if nodes is None else nodes
    stdin = stdin if stdin is not None else sys.stdin.buffer
    real_stdout = stdout if stdout is not None else sys.stdout.buffer
    sys.stdout = sys.stderr  # node bodies print to stderr

    by_id = {node.id: node for node in nodes}
    schema = wire.Schema(codecs=_opaque_keys(nodes))
    for declared in nodes:
        for name, ty in declared.inputs:
            schema.port(declared.id, "input", name, ty)
        for name, ty in declared.outputs:
            schema.port(declared.id, "output", name, ty)
        if declared.state is not None:
            schema.port(declared.id, "state", "", declared.state)
    hello = wire.encode_hello(
        worker,
        [
            {
                "id": declared.id,
                "inputs": declared.inputs,
                "outputs": declared.outputs,
                "state": declared.state,
            }
            for declared in nodes
        ],
    )
    wire.write_packet(real_stdout, hello)

    gate = _Gate()
    reader = _Reader(stdin, gate)
    while True:
        kind, value = reader.packets.get()
        if kind == "end":
            return
        if kind == "error":
            raise value
        body = value
        try:
            msg = wire.decode_host(body, schema)
        except wire.WireError as error:
            reason = str(error)
            wire.write_packet(real_stdout, wire.encode_fault(0, None, "malformed_packet", reason))
            continue
        kind = msg[0]
        if kind == "run":
            _run(msg, by_id, schema, real_stdout, gate)
        elif kind == "ping":
            wire.write_packet(real_stdout, wire.encode_pong())
        elif kind == "state_export":
            _export_state(msg[1], by_id, schema, real_stdout)
        elif kind == "state_import":
            declared = by_id.get(msg[1])
            if declared is None or declared.state is None:
                _state_fault(msg[1], "the node has no state to import", real_stdout)
            else:
                declared.state_value = msg[2]
        elif kind == "shutdown":
            return
        # A cancel between runs has nothing to stop.


def _state_fault(node_id, reason, out):
    wire.write_packet(out, wire.encode_fault(0, node_id, "bad_input", reason))


def _export_state(node_id, by_id, schema, out):
    declared = by_id.get(node_id)
    if declared is None or declared.state is None or declared.state_value is UNSET:
        _state_fault(node_id, "the node has no state to export", out)
        return
    wire.write_packet(
        out, wire.encode_state_value(node_id, declared.state, declared.state_value, schema)
    )


def _run(msg, by_id, schema, out, gate):
    _, run, _region, node_ids, inputs = msg
    outputs = []
    status = "ok"
    for node_id in node_ids:
        if gate.cancelled.is_set():
            status = "cancelled"
            break
        declared = by_id.get(node_id)
        if declared is None:
            wire.write_packet(out, wire.encode_fault(run, node_id, "unknown_node", f"no node {node_id}"))
            status = "fault"
            break
        values = {port: value for (n, port, value) in inputs if n == node_id}
        missing = [name for name, _ in declared.inputs if name not in values]
        if missing:
            wire.write_packet(
                out, wire.encode_fault(run, node_id, "bad_input", f"{node_id} has no input {missing[0]}")
            )
            status = "fault"
            break
        state = None if declared.state_value is UNSET else declared.state_value
        context = Context(state, gate.cancelled)
        args = dict(values)
        if declared.wants_context:
            args["cx"] = context
        try:
            result = declared.fn(**args)
        except BaseException as error:  # noqa: BLE001 - every failure is a fault
            code, reason = _fault_for(error)
            _flush_events(run, context, out)
            wire.write_packet(out, wire.encode_fault(run, node_id, code, reason))
            status = "fault"
            break
        _flush_events(run, context, out)
        if declared.state is not None and (
            context.new_state is not None or declared.state_value is not UNSET
        ):
            declared.state_value = context.new_state
        if len(declared.outputs) == 1:
            result = (result,)
        elif len(declared.outputs) == 0:
            result = ()
        if len(result) != len(declared.outputs):
            wire.write_packet(
                out,
                wire.encode_fault(run, node_id, "internal", f"{node_id} returned {len(result)} values"),
            )
            status = "fault"
            break
        for (port, _ty), value in zip(declared.outputs, result):
            outputs.append((node_id, port, value))
        if gate.cancelled.is_set():
            status = "cancelled"
            break
    if status != "ok":
        outputs = []
    wire.write_packet(out, wire.encode_outputs(run, status, outputs, schema))


def _flush_events(run, context, out):
    for event in context.events:
        if event[0] == "progress":
            wire.write_packet(out, wire.encode_progress(run, 0, event[2], event[1]))
        elif event[0] == "detail":
            wire.write_packet(out, wire.encode_detail(run, event[1]))
        elif event[0] == "fault_code":
            wire.write_packet(out, wire.encode_node_code(run, event[1]))
        else:
            wire.write_packet(out, wire.encode_log(event[1], event[2]))
    context.events = []
