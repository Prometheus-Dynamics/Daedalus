"""The showcase's nodes that need neither a config, a state type beyond an i64, an adapter, a
custom transport, nor an image or GPU type. The rest of the showcase the Python SDK once had
is not here."""

from daedalus import NodeError, T, node


@node("showcase.scalar_add", inputs=[("a", T.i64), ("b", T.i64)], outputs=[("out", T.i64)])
def scalar_add(a, b):
    return a + b


@node("showcase.split_sign", inputs=[("value", T.i64)], outputs=[("positive", T.i64), ("negative", T.i64)])
def split_sign(value):
    return (value, 0) if value >= 0 else (0, value)


@node("showcase.checked_divide", inputs=[("a", T.i64), ("b", T.i64)], outputs=[("out", T.i64)])
def checked_divide(a, b):
    if b == 0:
        raise NodeError("node_error", "division by zero")
    return int(a / b)


@node("showcase.array_dynamic_sum", inputs=[("values", T.list(T.i64))], outputs=[("sum", T.i64)])
def array_dynamic_sum(values):
    return sum(values)


@node("showcase.bytes_len", inputs=[("payload", T.bytes)], outputs=[("len", T.u64)])
def bytes_len(payload):
    return len(payload)


@node("showcase.owned_bytes_len", inputs=[("blob", T.bytes)], outputs=[("len", T.u64)])
def owned_bytes_len(blob):
    return len(blob)


@node("showcase.accumulate", inputs=[("value", T.i64)], outputs=[("sum", T.i64)], state=T.i64)
def accumulate(value, cx):
    total = (cx.state if cx.state is not None else 0) + value
    cx.new_state = total
    return total
