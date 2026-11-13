"""The worker loop: reads host messages, runs node bodies, and answers in the wire protocol.

Stdout carries packets only. Node bodies see `sys.stdout` replaced by stderr, so a print cannot
corrupt the stream. A node raises `NodeError(code, message)` for a fault with that code; any other
exception is a `node_error` fault whose reason is the traceback, cut to 512 bytes.

A worker reads the host's packets one at a time, so a cancel that arrives while a run is going
on is read after the run. A cancel between runs has nothing to stop.
"""

import inspect
import sys
import traceback

from . import wire
from .wire import NodeError


class Context:
    """What a node body can report. Events go out, in order, before the run's outputs."""

    def __init__(self, state):
        self.events = []
        self.state = state
        self.new_state = state

    def cancelled(self):
        return False

    def progress(self, fraction, message=""):
        self.events.append(("progress", float(fraction), message))

    def log(self, level, text):
        self.events.append(("log", level, text))

    def detail(self, text):
        """Gives the host more about a failure than the fault's reason holds. Call it any number
        of times before raising, and the pieces are kept in order with the step's fault. What a
        node writes and then doesn't fail is dropped by the host."""
        self.events.append(("detail", str(text)))


class Node:
    def __init__(self, id, inputs, outputs, fn, state=None):
        self.id = id
        self.inputs = inputs
        self.outputs = outputs
        self.fn = fn
        self.state = state
        self.wants_context = "cx" in inspect.signature(fn).parameters


def node(id, inputs, outputs, state=None):
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


def serve(nodes=None, worker="daedalus-python-worker", stdin=None, stdout=None):
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

    while True:
        body = wire.read_packet(stdin)
        if body is None:
            return
        try:
            msg = wire.decode_host(body, schema)
        except wire.WireError as error:
            reason = str(error)
            wire.write_packet(real_stdout, wire.encode_fault(0, None, "malformed_packet", reason))
            continue
        kind = msg[0]
        if kind == "run":
            _run(msg, by_id, schema, real_stdout)
        elif kind == "ping":
            wire.write_packet(real_stdout, wire.encode_pong())
        elif kind == "state_export":
            declared = by_id.get(msg[1])
            if declared is not None and declared.state is not None:
                value = getattr(declared, "state_value", 0)
                wire.write_packet(
                    real_stdout, wire.encode_state_value(msg[1], declared.state, value, schema)
                )
        elif kind == "state_import":
            declared = by_id.get(msg[1])
            if declared is not None:
                declared.state_value = msg[2]
        elif kind == "shutdown":
            return
        # A cancel between runs has nothing to stop.


def _run(msg, by_id, schema, out):
    _, run, _region, node_ids, inputs = msg
    outputs = []
    status = "ok"
    for node_id in node_ids:
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
        context = Context(getattr(declared, "state_value", None))
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
        if declared.state is not None:
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
    if status == "fault":
        outputs = []
    wire.write_packet(out, wire.encode_outputs(run, status, outputs, schema))


def _flush_events(run, context, out):
    for event in context.events:
        if event[0] == "progress":
            wire.write_packet(out, wire.encode_progress(run, 0, event[2], event[1]))
        elif event[0] == "detail":
            wire.write_packet(out, wire.encode_detail(run, event[1]))
        else:
            wire.write_packet(out, wire.encode_log(event[1], event[2]))
    context.events = []
