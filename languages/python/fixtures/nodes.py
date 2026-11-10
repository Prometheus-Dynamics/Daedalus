"""The thirteen canonical fixture nodes, as the Python worker serves them. Their ports and types
match testing/crates/worker-conformance/src/fixtures.rs exactly."""

from daedalus import NodeError, T, node

CUSTOM = "fixture.custom"


@node("fixture.scalar_add", inputs=[("a", T.i64), ("b", T.i64)], outputs=[("out", T.i64)])
def scalar_add(a, b):
    return a + b


_BYTES = T.bytes
_STRUCT = T.struct(("name", T.string), ("count", T.i64))
_ENUM = T.enum(("idle", None), ("busy", T.u32))
_LIST = T.list(T.u32)
_TUPLE = T.tuple(T.string, T.bool)
_MAP = T.map(T.string, T.i64)
_OPT = T.optional(T.i64)
_CUSTOM = T.opaque(CUSTOM)


def _echo(node_id, ty):
    def body(**ports):
        return ports["in"]
    node(node_id, inputs=[("in", ty)], outputs=[("out", ty)])(body)


_echo("fixture.bytes_echo", _BYTES)
_echo("fixture.struct_record", _STRUCT)
_echo("fixture.enum_choice", _ENUM)
_echo("fixture.optional_value", _OPT)
_echo("fixture.tuple_pair", _TUPLE)
_echo("fixture.map_pairs", _MAP)
_echo("fixture.custom_type_key", _CUSTOM)


@node("fixture.list_reverse", inputs=[("in", _LIST)], outputs=[("out", _LIST)])
def list_reverse(**ports):
    return list(reversed(ports["in"]))


@node("fixture.multi_output", inputs=[("x", T.u32)], outputs=[("a", T.u32), ("b", T.u32)])
def multi_output(x):
    return x, (x * 2) & 0xFFFFFFFF


@node("fixture.raw_io", inputs=[("payload", _BYTES)], outputs=[("out", _BYTES)])
def raw_io(payload):
    return bytes(reversed(payload))


@node(
    "fixture.stateful_counter",
    inputs=[("delta", T.i64)],
    outputs=[("total", T.i64)],
    state=T.i64,
)
def stateful_counter(delta, cx):
    current = cx.state if cx.state is not None else 0
    total = current + delta
    cx.new_state = total
    return total


@node("fixture.failure_diagnostic", inputs=[("value", T.i64)], outputs=[("out", T.i64)])
def failure_diagnostic(value):
    raise NodeError("node_error", "diagnostic: the value is rejected")
