"""The worker wire protocol, in Python: framing, types, values and messages.

A type is a tuple: ("scalar", name), ("opaque", key), ("optional", t), ("list", t), ("map", k, v),
("tuple", (t, ...)), ("struct", ((name, t), ...)), or ("enum", ((name, t or None), ...)).
A value is a Python value: an int, bool, str or bytes for the scalars (bytes or memoryview for
bytes), None or the value for optional, a list, a dict for a map, a tuple for a tuple, a dict by
field name for a struct, and (variant name, payload or None) for an enum. An opaque value is bytes.
"""

import struct as _struct

PROTOCOL_VERSION = 1
REASON_CAP = 512
DEFAULT_CAP = 16 * 1024 * 1024
MAX_DEPTH = 32
U64_MAX = 2**64 - 1

# The layout hash, FNV-1a over the same words as core/wire's LAYOUT. A hello carries it.
_LAYOUT = [
    PROTOCOL_VERSION, REASON_CAP, DEFAULT_CAP, MAX_DEPTH,
    0x01, 0x02, 0x03, 0x04, 0x05, 0x06,
    0x40, 0x41, 0x42, 0x43, 0x44, 0x45, 0x46,
    16, 8,
]


def _layout_hash(words):
    h = 0xCBF29CE484222325
    for word in words:
        for byte in (word & U64_MAX).to_bytes(8, "little"):
            h ^= byte
            h = (h * 0x100000001B3) & U64_MAX
    return h


LAYOUT_HASH = _layout_hash(_LAYOUT)

KIND_RUN, KIND_CANCEL, KIND_STATE_EXPORT, KIND_STATE_IMPORT, KIND_PING, KIND_SHUTDOWN = 1, 2, 3, 4, 5, 6
KIND_HELLO, KIND_OUTPUTS, KIND_PROGRESS, KIND_LOG, KIND_FAULT, KIND_STATE_VALUE, KIND_PONG = (
    0x40, 0x41, 0x42, 0x43, 0x44, 0x45, 0x46)

FAULT_CODES = {
    "node_error": 1, "node_panic": 2, "bad_input": 3, "unknown_node": 4,
    "unsupported": 5, "internal": 6, "malformed_packet": 7,
}
LOG_LEVELS = {"error": 0, "warn": 1, "info": 2, "debug": 3, "trace": 4}

SCALARS = [
    "unit", "bool", "i8", "i16", "i32", "i64", "isize", "u8", "u16", "u32", "u64", "usize",
    "f32", "f64", "string", "bytes",
]


class WireError(Exception):
    """A message, value or type that cannot be encoded or decoded."""


class NodeError(Exception):
    """Raise from a node body to fault the run with this code and message."""

    def __init__(self, code, message):
        super().__init__(message)
        if code not in FAULT_CODES:
            raise ValueError(f"unknown fault code {code!r}")
        self.code = code
        self.message = message


class T:
    """Type constructors."""

    @staticmethod
    def scalar(name):
        if name not in SCALARS:
            raise ValueError(f"unknown scalar {name!r}")
        return ("scalar", name)

    unit = ("scalar", "unit")
    bool = ("scalar", "bool")
    i8 = ("scalar", "i8")
    i16 = ("scalar", "i16")
    i32 = ("scalar", "i32")
    i64 = ("scalar", "i64")
    u8 = ("scalar", "u8")
    u16 = ("scalar", "u16")
    u32 = ("scalar", "u32")
    u64 = ("scalar", "u64")
    f32 = ("scalar", "f32")
    f64 = ("scalar", "f64")
    string = ("scalar", "string")
    bytes = ("scalar", "bytes")

    @staticmethod
    def opaque(key):
        return ("opaque", key)

    @staticmethod
    def optional(inner):
        return ("optional", inner)

    @staticmethod
    def list(inner):
        return ("list", inner)

    @staticmethod
    def map(key, value):
        return ("map", key, value)

    @staticmethod
    def tuple(*items):
        return ("tuple", tuple(items))

    @staticmethod
    def struct(*fields):
        return ("struct", tuple(fields))

    @staticmethod
    def enum(*variants):
        return ("enum", tuple(variants))


def _check_type(ty, depth=0):
    if depth > MAX_DEPTH:
        raise WireError("type nests too deep")
    kind = ty[0]
    if kind == "scalar":
        if ty[1] not in SCALARS:
            raise WireError(f"unknown scalar {ty[1]!r}")
    elif kind == "opaque":
        if not isinstance(ty[1], str):
            raise WireError("an opaque key is text")
    elif kind in ("optional", "list"):
        _check_type(ty[1], depth + 1)
    elif kind == "map":
        _check_type(ty[1], depth + 1)
        _check_type(ty[2], depth + 1)
    elif kind == "tuple":
        for item in ty[1]:
            _check_type(item, depth + 1)
    elif kind == "struct":
        for name, item in ty[1]:
            _check_type(item, depth + 1)
    elif kind == "enum":
        for name, payload in ty[1]:
            if payload is not None:
                _check_type(payload, depth + 1)
    else:
        raise WireError(f"unknown type {kind!r}")


class Writer:
    def __init__(self):
        self.buf = bytearray()

    def u8(self, value):
        self.buf.append(value)

    def u16(self, value):
        self.buf += _struct.pack("<H", value)

    def u32(self, value):
        self.buf += _struct.pack("<I", value)

    def u64(self, value):
        if not 0 <= value <= U64_MAX:
            raise WireError(f"{value} is outside u64")
        self.buf += _struct.pack("<Q", value)

    def i8(self, value):
        self.buf += _struct.pack("<b", value)

    def i16(self, value):
        self.buf += _struct.pack("<h", value)

    def i32(self, value):
        self.buf += _struct.pack("<i", value)

    def i64(self, value):
        self.buf += _struct.pack("<q", value)

    def f32(self, value):
        self.buf += _struct.pack("<f", value)

    def f64(self, value):
        self.buf += _struct.pack("<d", value)

    def bool(self, value):
        self.u8(1 if value else 0)

    def raw(self, data):
        self.buf += data

    def bytes(self, data):
        self.u32(len(data))
        self.buf += data

    def str(self, text):
        self.bytes(text.encode("utf-8"))


class Reader:
    def __init__(self, data):
        self.data = memoryview(bytes(data))
        self.pos = 0

    def remaining(self):
        return len(self.data) - self.pos

    def take(self, count):
        if count > self.remaining():
            raise WireError(f"input ends early: needs {count}, {self.remaining()} left")
        out = self.data[self.pos:self.pos + count]
        self.pos += count
        return bytes(out)

    def u8(self):
        return self.take(1)[0]

    def bool(self):
        tag = self.u8()
        if tag > 1:
            raise WireError(f"bad bool {tag}")
        return tag == 1

    def u16(self):
        return _struct.unpack("<H", self.take(2))[0]

    def u32(self):
        return _struct.unpack("<I", self.take(4))[0]

    def u64(self):
        return _struct.unpack("<Q", self.take(8))[0]

    def i8(self):
        return _struct.unpack("<b", self.take(1))[0]

    def i16(self):
        return _struct.unpack("<h", self.take(2))[0]

    def i32(self):
        return _struct.unpack("<i", self.take(4))[0]

    def i64(self):
        return _struct.unpack("<q", self.take(8))[0]

    def f32(self):
        return _struct.unpack("<f", self.take(4))[0]

    def f64(self):
        return _struct.unpack("<d", self.take(8))[0]

    def count(self):
        count = self.u32()
        if count > self.remaining():
            raise WireError(f"count {count} cannot fit in {self.remaining()} bytes")
        return count

    def bytes(self):
        return self.take(self.count())

    def str(self):
        try:
            return self.bytes().decode("utf-8")
        except UnicodeDecodeError as error:
            raise WireError("a string field is not UTF-8") from error

    def finish(self):
        if self.remaining():
            raise WireError(f"{self.remaining()} bytes after the message")


# ---- types -------------------------------------------------------------------------------------


def encode_type(ty, out, depth=0):
    if depth > MAX_DEPTH:
        raise WireError("type nests too deep")
    deeper = depth + 1
    kind = ty[0]
    if kind == "scalar":
        out.u8(0)
        out.u8(SCALARS.index(ty[1]))
    elif kind == "opaque":
        out.u8(1)
        out.str(ty[1])
    elif kind == "optional":
        out.u8(2)
        encode_type(ty[1], out, deeper)
    elif kind == "list":
        out.u8(3)
        encode_type(ty[1], out, deeper)
    elif kind == "map":
        out.u8(4)
        encode_type(ty[1], out, deeper)
        encode_type(ty[2], out, deeper)
    elif kind == "tuple":
        out.u8(5)
        out.u32(len(ty[1]))
        for item in ty[1]:
            encode_type(item, out, deeper)
    elif kind == "struct":
        out.u8(6)
        out.u32(len(ty[1]))
        for name, item in ty[1]:
            out.str(name)
            encode_type(item, out, deeper)
    elif kind == "enum":
        out.u8(7)
        out.u32(len(ty[1]))
        for name, payload in ty[1]:
            out.str(name)
            if payload is None:
                out.u8(0)
            else:
                out.u8(1)
                encode_type(payload, out, deeper)
    else:
        raise WireError(f"unknown type {kind!r}")


def decode_type(inp, depth=0):
    if depth > MAX_DEPTH:
        raise WireError("type nests too deep")
    tag = inp.u8()
    deeper = depth + 1
    if tag == 0:
        index = inp.u8()
        if index >= len(SCALARS):
            raise WireError(f"bad scalar tag {index}")
        return ("scalar", SCALARS[index])
    if tag == 1:
        return ("opaque", inp.str())
    if tag == 2:
        return ("optional", decode_type(inp, deeper))
    if tag == 3:
        return ("list", decode_type(inp, deeper))
    if tag == 4:
        return ("map", decode_type(inp, deeper), decode_type(inp, deeper))
    if tag == 5:
        return ("tuple", tuple(decode_type(inp, deeper) for _ in range(inp.count())))
    if tag == 6:
        return ("struct", tuple((inp.str(), decode_type(inp, deeper)) for _ in range(inp.count())))
    if tag == 7:
        variants = []
        for _ in range(inp.count()):
            name = inp.str()
            flag = inp.u8()
            variants.append((name, decode_type(inp, deeper) if flag else None))
        return ("enum", tuple(variants))
    raise WireError(f"bad type tag {tag}")


# ---- values ------------------------------------------------------------------------------------

_INT_RANGES = {
    "i8": (-2**7, 2**7 - 1), "i16": (-2**15, 2**15 - 1), "i32": (-2**31, 2**31 - 1),
    "i64": (-2**63, 2**63 - 1), "isize": (-2**63, 2**63 - 1),
    "u8": (0, 2**8 - 1), "u16": (0, 2**16 - 1), "u32": (0, 2**32 - 1),
    "u64": (0, U64_MAX), "usize": (0, U64_MAX),
}


def _int(name, value):
    if isinstance(value, bool) or not isinstance(value, int):
        raise WireError(f"{name} wants an int, got {type(value).__name__}")
    low, high = _INT_RANGES[name]
    if not low <= value <= high:
        raise WireError(f"{value} is outside {name}")
    return value


def encode_value(ty, value, schema, out, depth=0):
    if depth > MAX_DEPTH:
        raise WireError("value nests too deep")
    deeper = depth + 1
    kind = ty[0]
    if kind == "scalar":
        name = ty[1]
        if name == "unit":
            if value is not None:
                raise WireError("unit wants None")
        elif name == "bool":
            if not isinstance(value, bool):
                raise WireError("bool wants a bool")
            out.bool(value)
        elif name in _INT_RANGES:
            value = _int(name, value)
            {"i8": out.i8, "i16": out.i16, "i32": out.i32, "i64": out.i64, "isize": out.i64,
             "u8": out.u8, "u16": out.u16, "u32": out.u32, "u64": out.u64, "usize": out.u64}[name](value)
        elif name == "f32":
            out.f32(float(value))
        elif name == "f64":
            out.f64(float(value))
        elif name == "string":
            if not isinstance(value, str):
                raise WireError("string wants a str")
            out.str(value)
        elif name == "bytes":
            if isinstance(value, memoryview):
                value = value.tobytes()
            if not isinstance(value, (bytes, bytearray)):
                raise WireError("bytes wants bytes or a memoryview")
            out.bytes(bytes(value))
    elif kind == "opaque":
        if not isinstance(value, (bytes, bytearray)):
            raise WireError("an opaque value is bytes")
        if not schema.has_codec(ty[1]):
            raise WireError(f"type {ty[1]} has no codec on this side")
        out.bytes(bytes(value))
    elif kind == "optional":
        if value is None:
            out.u8(0)
        else:
            out.u8(1)
            encode_value(ty[1], value, schema, out, deeper)
    elif kind == "list":
        if not isinstance(value, (list, tuple)):
            raise WireError("a list wants a list")
        out.u32(len(value))
        for item in value:
            encode_value(ty[1], item, schema, out, deeper)
    elif kind == "map":
        pairs = list(value.items()) if isinstance(value, dict) else list(value)
        out.u32(len(pairs))
        for key, item in pairs:
            encode_value(ty[1], key, schema, out, deeper)
            encode_value(ty[2], item, schema, out, deeper)
    elif kind == "tuple":
        if len(value) != len(ty[1]):
            raise WireError("a tuple has the wrong length")
        for item_ty, item in zip(ty[1], value):
            encode_value(item_ty, item, schema, out, deeper)
    elif kind == "struct":
        for name, item_ty in ty[1]:
            if name not in value:
                raise WireError(f"struct has no field {name}")
            encode_value(item_ty, value[name], schema, out, deeper)
    elif kind == "enum":
        name, payload = value
        index = [variant for variant, _ in ty[1]].index(name)
        payload_ty = ty[1][index][1]
        out.u32(index)
        if payload_ty is not None:
            encode_value(payload_ty, payload, schema, out, deeper)
    else:
        raise WireError(f"unknown type {kind!r}")


def decode_value(ty, inp, schema, depth=0):
    if depth > MAX_DEPTH:
        raise WireError("value nests too deep")
    deeper = depth + 1
    kind = ty[0]
    if kind == "scalar":
        name = ty[1]
        readers = {
            "unit": lambda: None, "bool": inp.bool, "i8": inp.i8, "i16": inp.i16,
            "i32": inp.i32, "i64": inp.i64, "isize": inp.i64, "u8": inp.u8, "u16": inp.u16,
            "u32": inp.u32, "u64": inp.u64, "usize": inp.u64, "f32": inp.f32, "f64": inp.f64,
            "string": inp.str, "bytes": inp.bytes,
        }
        return readers[name]()
    if kind == "opaque":
        if not schema.has_codec(ty[1]):
            raise WireError(f"type {ty[1]} has no codec on this side")
        return inp.bytes()
    if kind == "optional":
        flag = inp.u8()
        if flag == 0:
            return None
        if flag == 1:
            return decode_value(ty[1], inp, schema, deeper)
        raise WireError(f"bad optional flag {flag}")
    if kind == "list":
        return [decode_value(ty[1], inp, schema, deeper) for _ in range(inp.count())]
    if kind == "map":
        out = {}
        for _ in range(inp.count()):
            key = decode_value(ty[1], inp, schema, deeper)
            out[key] = decode_value(ty[2], inp, schema, deeper)
        return out
    if kind == "tuple":
        return tuple(decode_value(item, inp, schema, deeper) for item in ty[1])
    if kind == "struct":
        return {name: decode_value(item, inp, schema, deeper) for name, item in ty[1]}
    if kind == "enum":
        index = inp.u32()
        if index >= len(ty[1]):
            raise WireError(f"bad variant {index}")
        name, payload_ty = ty[1][index]
        payload = decode_value(payload_ty, inp, schema, deeper) if payload_ty is not None else None
        return (name, payload)
    raise WireError(f"unknown type {kind!r}")


# ---- schema ------------------------------------------------------------------------------------


class Schema:
    """The ports and state a worker declared, and the opaque codecs it has."""

    def __init__(self, codecs=()):
        self.ports = {}
        self.codecs = set(codecs)

    def port(self, node, direction, name, ty):
        self.ports[(node, direction, name)] = ty

    def has_codec(self, key):
        return key in self.codecs

    def lookup(self, node, direction, name):
        try:
            return self.ports[(node, direction, name)]
        except KeyError:
            raise WireError(f"node {node} has no {direction} {name}") from None


# ---- framing -----------------------------------------------------------------------------------


def write_packet(stream, body, cap=DEFAULT_CAP):
    if len(body) > cap:
        raise WireError(f"packet of {len(body)} bytes is over the cap of {cap}")
    stream.write(_struct.pack("<I", len(body)))
    stream.write(body)
    stream.flush()


def read_packet(stream, cap=DEFAULT_CAP):
    """One packet's body, or None when the stream ended between packets."""
    header = stream.read(4)
    if not header:
        return None
    if len(header) < 4:
        raise WireError("the stream ended inside a packet header")
    (length,) = _struct.unpack("<I", header)
    if length > cap:
        raise WireError(f"packet of {length} bytes is over the cap of {cap}")
    body = stream.read(length)
    if len(body) < length:
        raise WireError("the stream ended inside a packet")
    return body


# ---- messages ----------------------------------------------------------------------------------


def encode_hello(worker, nodes, packet_cap=DEFAULT_CAP):
    """`nodes` is a list of dicts with id, inputs, outputs (lists of (name, type)), and state."""
    out = Writer()
    out.u8(KIND_HELLO)
    out.u32(PROTOCOL_VERSION)
    out.u64(LAYOUT_HASH)
    out.str(worker)
    out.u32(packet_cap)
    out.u32(len(nodes))
    for node in nodes:
        out.str(node["id"])
        for ports in (node["inputs"], node["outputs"], []):
            out.u32(len(ports))
            for name, ty in ports:
                out.str(name)
                encode_type(ty, out)
                out.u8(0)  # no default
        if node.get("state") is None:
            out.u8(0)
        else:
            out.u8(1)
            encode_type(node["state"], out)
        out.u32(0)  # capabilities
    return bytes(out.buf)


def decode_host(body, schema):
    """A host message: ("run", run, region, nodes, inputs), ("cancel", run), ("ping",), ..."""
    inp = Reader(body)
    kind = inp.u8()
    if kind == KIND_RUN:
        run = inp.u64()
        region = inp.u32()
        nodes = [inp.str() for _ in range(inp.count())]
        inputs = []
        for _ in range(inp.count()):
            node = inp.str()
            port = inp.str()
            value = decode_value(schema.lookup(node, "input", port), inp, schema)
            inputs.append((node, port, value))
        inp.finish()
        return ("run", run, region, nodes, inputs)
    if kind == KIND_CANCEL:
        run = inp.u64()
        inp.finish()
        return ("cancel", run)
    if kind == KIND_STATE_EXPORT:
        node = inp.str()
        inp.finish()
        return ("state_export", node)
    if kind == KIND_STATE_IMPORT:
        node = inp.str()
        value = decode_value(schema.lookup(node, "state", ""), inp, schema)
        inp.finish()
        return ("state_import", node, value)
    if kind == KIND_PING:
        inp.finish()
        return ("ping",)
    if kind == KIND_SHUTDOWN:
        inp.finish()
        return ("shutdown",)
    raise WireError(f"unknown message kind {kind:#04x}")


def encode_outputs(run, status, outputs, schema):
    """`outputs` is a list of (node, port, value). status is "ok", "cancelled" or "fault"."""
    out = Writer()
    out.u8(KIND_OUTPUTS)
    out.u64(run)
    out.u8({"ok": 0, "cancelled": 1, "fault": 2}[status])
    out.u32(len(outputs))
    for node, port, value in outputs:
        out.str(node)
        out.str(port)
        encode_value(schema.lookup(node, "output", port), value, schema, out)
    return bytes(out.buf)


def encode_fault(run, node, code, reason):
    out = Writer()
    out.u8(KIND_FAULT)
    out.u64(run)
    if node is None:
        out.u8(0)
    else:
        out.u8(1)
        out.str(node)
    out.u16(FAULT_CODES[code])
    out.str(_truncate(reason, REASON_CAP))
    return bytes(out.buf)


def encode_progress(run, node_index, message, fraction):
    out = Writer()
    out.u8(KIND_PROGRESS)
    out.u64(run)
    out.u32(node_index)
    out.str(message)
    if fraction is None:
        out.u8(0)
    else:
        out.u8(1)
        out.f32(fraction)
    return bytes(out.buf)


def encode_log(level, text):
    out = Writer()
    out.u8(KIND_LOG)
    out.u8(LOG_LEVELS[level])
    out.str(text)
    return bytes(out.buf)


def encode_pong():
    return bytes([KIND_PONG])


def _truncate(text, cap):
    data = text.encode("utf-8")
    if len(data) <= cap:
        return text
    cut = cap
    while cut > 0 and (data[cut] & 0xC0) == 0x80:
        cut -= 1
    return data[:cut].decode("utf-8")


def encode_state_value(node, ty, value, schema):
    out = Writer()
    out.u8(KIND_STATE_VALUE)
    out.str(node)
    encode_value(ty, value, schema, out)
    return bytes(out.buf)
