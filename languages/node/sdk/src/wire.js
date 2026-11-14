// The worker wire protocol in JavaScript: framing, types, values and messages.
//
// A type is an object: { scalar: "i64" }, { opaque: key }, { optional: t }, { list: t },
// { map: [k, v] }, { tuple: [t...] }, { struct: [[name, t]...] }, { enum: [[name, t|null]...] }.
// A value: a number (i8..u32, f32, f64), a BigInt or safe number (i64, u64, isize, usize), a
// boolean, a string, a Buffer or Uint8Array (bytes), null or the value (optional), an array, a
// Map (map, keys as values), an array (tuple), an object by field name (struct), or
// { variant, payload } (enum). An opaque value is a Buffer.

export const PROTOCOL_VERSION = 1;
export const REASON_CAP = 512;
export const DEFAULT_CAP = 16 * 1024 * 1024;
export const MAX_DEPTH = 32;
const U64_MAX = (1n << 64n) - 1n;

export const SCALARS = [
  "unit", "bool", "i8", "i16", "i32", "i64", "isize", "u8", "u16", "u32", "u64", "usize",
  "f32", "f64", "string", "bytes",
];

export const KIND = {
  RUN: 1, CANCEL: 2, STATE_EXPORT: 3, STATE_IMPORT: 4, PING: 5, SHUTDOWN: 6,
  HELLO: 0x40, OUTPUTS: 0x41, PROGRESS: 0x42, LOG: 0x43, FAULT: 0x44, STATE_VALUE: 0x45, PONG: 0x46,
  // Added after the layout was fixed, and not part of its hash: a host ignores a kind it doesn't know.
  DETAIL: 0x47,
  NODE_CODE: 0x48,
};

export const FAULT_CODES = {
  node_error: 1, node_panic: 2, bad_input: 3, unknown_node: 4, unsupported: 5, internal: 6,
  malformed_packet: 7,
};
const LOG_LEVELS = { error: 0, warn: 1, info: 2, debug: 3, trace: 4 };

export class WireError extends Error {}

export class NodeError extends Error {
  constructor(code, message) {
    super(message);
    if (!(code in FAULT_CODES)) throw new Error(`unknown fault code ${code}`);
    this.code = code;
  }
}

// Type constructors.
export const T = {
  unit: { scalar: "unit" }, bool: { scalar: "bool" }, i8: { scalar: "i8" }, i16: { scalar: "i16" },
  i32: { scalar: "i32" }, i64: { scalar: "i64" }, isize: { scalar: "isize" }, u8: { scalar: "u8" },
  u16: { scalar: "u16" }, u32: { scalar: "u32" }, u64: { scalar: "u64" }, usize: { scalar: "usize" },
  f32: { scalar: "f32" }, f64: { scalar: "f64" }, string: { scalar: "string" },
  bytes: { scalar: "bytes" },
  opaque: (key) => ({ opaque: key }),
  optional: (inner) => ({ optional: inner }),
  list: (inner) => ({ list: inner }),
  map: (key, value) => ({ map: [key, value] }),
  tuple: (...items) => ({ tuple: items }),
  struct: (...fields) => ({ struct: fields }),
  enum: (...variants) => ({ enum: variants }),
};

export function checkType(ty, depth = 0) {
  if (depth > MAX_DEPTH) throw new WireError("type nests too deep");
  if (ty.scalar !== undefined) {
    if (!SCALARS.includes(ty.scalar)) throw new WireError(`unknown scalar ${ty.scalar}`);
  } else if (ty.opaque !== undefined) {
    if (typeof ty.opaque !== "string") throw new WireError("an opaque key is text");
  } else if (ty.optional !== undefined) checkType(ty.optional, depth + 1);
  else if (ty.list !== undefined) checkType(ty.list, depth + 1);
  else if (ty.map !== undefined) { checkType(ty.map[0], depth + 1); checkType(ty.map[1], depth + 1); }
  else if (ty.tuple !== undefined) ty.tuple.forEach((item) => checkType(item, depth + 1));
  else if (ty.struct !== undefined) ty.struct.forEach(([, item]) => checkType(item, depth + 1));
  else if (ty.enum !== undefined) ty.enum.forEach(([, payload]) => payload && checkType(payload, depth + 1));
  else throw new WireError("unknown type");
}

export class Writer {
  constructor() { this.chunks = []; this.size = 0; }
  push(buf) { this.chunks.push(buf); this.size += buf.length; }
  u8(v) { const b = Buffer.alloc(1); b.writeUInt8(v); this.push(b); }
  bool(v) { this.u8(v ? 1 : 0); }
  u16(v) { const b = Buffer.alloc(2); b.writeUInt16LE(v); this.push(b); }
  u32(v) { const b = Buffer.alloc(4); b.writeUInt32LE(v); this.push(b); }
  u64(v) {
    const big = toU64(v);
    const b = Buffer.alloc(8); b.writeBigUInt64LE(big); this.push(b);
  }
  i8(v) { const b = Buffer.alloc(1); b.writeInt8(v); this.push(b); }
  i16(v) { const b = Buffer.alloc(2); b.writeInt16LE(v); this.push(b); }
  i32(v) { const b = Buffer.alloc(4); b.writeInt32LE(v); this.push(b); }
  i64(v) { const b = Buffer.alloc(8); b.writeBigInt64LE(toI64(v)); this.push(b); }
  f32(v) { const b = Buffer.alloc(4); b.writeFloatLE(v); this.push(b); }
  f64(v) { const b = Buffer.alloc(8); b.writeDoubleLE(v); this.push(b); }
  raw(buf) { this.push(Buffer.from(buf)); }
  bytes(buf) { this.u32(buf.length); this.push(Buffer.from(buf)); }
  str(text) { this.bytes(Buffer.from(text, "utf8")); }
  finish() { return Buffer.concat(this.chunks, this.size); }
}

export class Reader {
  constructor(buf) { this.buf = Buffer.from(buf); this.pos = 0; }
  remaining() { return this.buf.length - this.pos; }
  take(count) {
    if (count > this.remaining()) throw new WireError(`input ends early: needs ${count}, ${this.remaining()} left`);
    const out = this.buf.subarray(this.pos, this.pos + count);
    this.pos += count;
    return out;
  }
  u8() { return this.take(1).readUInt8(0); }
  bool() { const tag = this.u8(); if (tag > 1) throw new WireError(`bad bool ${tag}`); return tag === 1; }
  u16() { return this.take(2).readUInt16LE(0); }
  u32() { return this.take(4).readUInt32LE(0); }
  u64() { return this.take(8).readBigUInt64LE(0); }
  i8() { return this.take(1).readInt8(0); }
  i16() { return this.take(2).readInt16LE(0); }
  i32() { return this.take(4).readInt32LE(0); }
  i64() { return this.take(8).readBigInt64LE(0); }
  f32() { return this.take(4).readFloatLE(0); }
  f64() { return this.take(8).readDoubleLE(0); }
  count() {
    const count = this.u32();
    if (count > this.remaining()) throw new WireError(`count ${count} cannot fit in ${this.remaining()} bytes`);
    return count;
  }
  bytes() { return Buffer.from(this.take(this.count())); }
  str() {
    const raw = this.bytes();
    try {
      return new TextDecoder("utf-8", { fatal: true }).decode(raw);
    } catch {
      throw new WireError("a string field is not UTF-8");
    }
  }
  finish() { if (this.remaining()) throw new WireError(`${this.remaining()} bytes after the message`); }
}

// u64 accepts a BigInt in range, or a safe non-negative number. Anything else is refused.
export function toU64(v) {
  if (typeof v === "bigint") {
    if (v < 0n || v > U64_MAX) throw new WireError(`${v} is outside u64`);
    return v;
  }
  if (typeof v === "number" && Number.isSafeInteger(v) && v >= 0) return BigInt(v);
  throw new WireError(`${v} is not a u64`);
}

export function toI64(v) {
  const min = -(1n << 63n), max = (1n << 63n) - 1n;
  if (typeof v === "bigint") {
    if (v < min || v > max) throw new WireError(`${v} is outside i64`);
    return v;
  }
  if (typeof v === "number" && Number.isSafeInteger(v)) return BigInt(v);
  throw new WireError(`${v} is not an i64`);
}

function smallInt(name, v, low, high) {
  if (typeof v !== "number" || !Number.isInteger(v) || v < low || v > high) {
    throw new WireError(`${v} is not a valid ${name}`);
  }
  return v;
}

export function encodeType(ty, out, depth = 0) {
  if (depth > MAX_DEPTH) throw new WireError("type nests too deep");
  const deeper = depth + 1;
  if (ty.scalar !== undefined) { out.u8(0); out.u8(SCALARS.indexOf(ty.scalar)); }
  else if (ty.opaque !== undefined) { out.u8(1); out.str(ty.opaque); }
  else if (ty.optional !== undefined) { out.u8(2); encodeType(ty.optional, out, deeper); }
  else if (ty.list !== undefined) { out.u8(3); encodeType(ty.list, out, deeper); }
  else if (ty.map !== undefined) { out.u8(4); encodeType(ty.map[0], out, deeper); encodeType(ty.map[1], out, deeper); }
  else if (ty.tuple !== undefined) {
    out.u8(5); out.u32(ty.tuple.length);
    ty.tuple.forEach((item) => encodeType(item, out, deeper));
  } else if (ty.struct !== undefined) {
    out.u8(6); out.u32(ty.struct.length);
    ty.struct.forEach(([name, item]) => { out.str(name); encodeType(item, out, deeper); });
  } else if (ty.enum !== undefined) {
    out.u8(7); out.u32(ty.enum.length);
    ty.enum.forEach(([name, payload]) => {
      out.str(name);
      if (payload == null) out.u8(0);
      else { out.u8(1); encodeType(payload, out, deeper); }
    });
  } else throw new WireError("unknown type");
}

export function decodeType(inp, depth = 0) {
  if (depth > MAX_DEPTH) throw new WireError("type nests too deep");
  const deeper = depth + 1;
  const tag = inp.u8();
  switch (tag) {
    case 0: {
      const index = inp.u8();
      if (index >= SCALARS.length) throw new WireError(`bad scalar tag ${index}`);
      return { scalar: SCALARS[index] };
    }
    case 1: return { opaque: inp.str() };
    case 2: return { optional: decodeType(inp, deeper) };
    case 3: return { list: decodeType(inp, deeper) };
    case 4: return { map: [decodeType(inp, deeper), decodeType(inp, deeper)] };
    case 5: {
      const n = inp.count(); const items = [];
      for (let i = 0; i < n; i++) items.push(decodeType(inp, deeper));
      return { tuple: items };
    }
    case 6: {
      const n = inp.count(); const fields = [];
      for (let i = 0; i < n; i++) { const name = inp.str(); fields.push([name, decodeType(inp, deeper)]); }
      return { struct: fields };
    }
    case 7: {
      const n = inp.count(); const variants = [];
      for (let i = 0; i < n; i++) {
        const name = inp.str();
        const flag = inp.u8();
        variants.push([name, flag ? decodeType(inp, deeper) : null]);
      }
      return { enum: variants };
    }
    default: throw new WireError(`bad type tag ${tag}`);
  }
}

export class Schema {
  constructor(codecs = []) { this.ports = new Map(); this.codecs = new Set(codecs); }
  port(node, direction, name, ty) { this.ports.set(`${node}\u0000${direction}\u0000${name}`, ty); }
  hasCodec(key) { return this.codecs.has(key); }
  lookup(node, direction, name) {
    const ty = this.ports.get(`${node}\u0000${direction}\u0000${name}`);
    if (!ty) throw new WireError(`node ${node} has no ${direction} ${name}`);
    return ty;
  }
}

export function encodeValue(ty, value, schema, out, depth = 0) {
  if (depth > MAX_DEPTH) throw new WireError("value nests too deep");
  const deeper = depth + 1;
  if (ty.scalar !== undefined) {
    switch (ty.scalar) {
      case "unit": break;
      case "bool": if (typeof value !== "boolean") throw new WireError("bool wants a boolean"); out.bool(value); break;
      case "i8": out.i8(smallInt("i8", value, -128, 127)); break;
      case "i16": out.i16(smallInt("i16", value, -32768, 32767)); break;
      case "i32": out.i32(smallInt("i32", value, -2147483648, 2147483647)); break;
      case "i64": case "isize": out.i64(value); break;
      case "u8": out.u8(smallInt("u8", value, 0, 255)); break;
      case "u16": out.u16(smallInt("u16", value, 0, 65535)); break;
      case "u32": out.u32(smallInt("u32", value, 0, 4294967295)); break;
      case "u64": case "usize": out.u64(value); break;
      case "f32": out.f32(Number(value)); break;
      case "f64": out.f64(Number(value)); break;
      case "string":
        if (typeof value !== "string") throw new WireError("string wants a string");
        out.str(value); break;
      case "bytes":
        if (!(value instanceof Uint8Array)) throw new WireError("bytes wants a Buffer or Uint8Array");
        out.bytes(value); break;
      default: throw new WireError("unknown scalar");
    }
  } else if (ty.opaque !== undefined) {
    if (!(value instanceof Uint8Array)) throw new WireError("an opaque value is bytes");
    if (!schema.hasCodec(ty.opaque)) throw new WireError(`type ${ty.opaque} has no codec on this side`);
    out.bytes(value);
  } else if (ty.optional !== undefined) {
    if (value == null) out.u8(0);
    else { out.u8(1); encodeValue(ty.optional, value, schema, out, deeper); }
  } else if (ty.list !== undefined) {
    if (!Array.isArray(value)) throw new WireError("a list wants an array");
    out.u32(value.length);
    value.forEach((item) => encodeValue(ty.list, item, schema, out, deeper));
  } else if (ty.map !== undefined) {
    const pairs = value instanceof Map ? [...value.entries()] : value;
    out.u32(pairs.length);
    pairs.forEach(([k, v]) => {
      encodeValue(ty.map[0], k, schema, out, deeper);
      encodeValue(ty.map[1], v, schema, out, deeper);
    });
  } else if (ty.tuple !== undefined) {
    if (!Array.isArray(value) || value.length !== ty.tuple.length) throw new WireError("a tuple has the wrong length");
    ty.tuple.forEach((item, i) => encodeValue(item, value[i], schema, out, deeper));
  } else if (ty.struct !== undefined) {
    ty.struct.forEach(([name, item]) => {
      if (!(name in value)) throw new WireError(`struct has no field ${name}`);
      encodeValue(item, value[name], schema, out, deeper);
    });
  } else if (ty.enum !== undefined) {
    const index = ty.enum.findIndex(([name]) => name === value.variant);
    if (index < 0) throw new WireError(`no variant ${value.variant}`);
    const payloadTy = ty.enum[index][1];
    out.u32(index);
    if (payloadTy) encodeValue(payloadTy, value.payload, schema, out, deeper);
  } else throw new WireError("unknown type");
}

export function decodeValue(ty, inp, schema, depth = 0) {
  if (depth > MAX_DEPTH) throw new WireError("value nests too deep");
  const deeper = depth + 1;
  if (ty.scalar !== undefined) {
    switch (ty.scalar) {
      case "unit": return null;
      case "bool": return inp.bool();
      case "i8": return inp.i8();
      case "i16": return inp.i16();
      case "i32": return inp.i32();
      case "i64": case "isize": return inp.i64();
      case "u8": return inp.u8();
      case "u16": return inp.u16();
      case "u32": return inp.u32();
      case "u64": case "usize": return inp.u64();
      case "f32": return inp.f32();
      case "f64": return inp.f64();
      case "string": return inp.str();
      case "bytes": return inp.bytes();
      default: throw new WireError("unknown scalar");
    }
  }
  if (ty.opaque !== undefined) {
    if (!schema.hasCodec(ty.opaque)) throw new WireError(`type ${ty.opaque} has no codec on this side`);
    return inp.bytes();
  }
  if (ty.optional !== undefined) {
    const flag = inp.u8();
    if (flag === 0) return null;
    if (flag === 1) return decodeValue(ty.optional, inp, schema, deeper);
    throw new WireError(`bad optional flag ${flag}`);
  }
  if (ty.list !== undefined) {
    const n = inp.count(); const items = [];
    for (let i = 0; i < n; i++) items.push(decodeValue(ty.list, inp, schema, deeper));
    return items;
  }
  if (ty.map !== undefined) {
    const n = inp.count(); const out = new Map();
    for (let i = 0; i < n; i++) {
      const k = decodeValue(ty.map[0], inp, schema, deeper);
      out.set(k, decodeValue(ty.map[1], inp, schema, deeper));
    }
    return out;
  }
  if (ty.tuple !== undefined) return ty.tuple.map((item) => decodeValue(item, inp, schema, deeper));
  if (ty.struct !== undefined) {
    const out = {};
    ty.struct.forEach(([name, item]) => { out[name] = decodeValue(item, inp, schema, deeper); });
    return out;
  }
  if (ty.enum !== undefined) {
    const index = inp.u32();
    if (index >= ty.enum.length) throw new WireError(`bad variant ${index}`);
    const [name, payloadTy] = ty.enum[index];
    return { variant: name, payload: payloadTy ? decodeValue(payloadTy, inp, schema, deeper) : null };
  }
  throw new WireError("unknown type");
}

// Framing over file descriptors: blocking reads and writes, one packet at a time.
export function readPacket(fd, cap = DEFAULT_CAP, fsRead) {
  const header = readExact(fd, 4, fsRead, true);
  if (header === null) return null;
  const len = header.readUInt32LE(0);
  if (len > cap) throw new WireError(`packet of ${len} bytes is over the cap of ${cap}`);
  const body = readExact(fd, len, fsRead, false);
  if (body === null) throw new WireError("the stream ended inside a packet");
  return body;
}

function readExact(fd, count, fsRead, allowEof) {
  const buf = Buffer.alloc(count);
  let got = 0;
  while (got < count) {
    const n = fsRead(fd, buf, got, count - got, null);
    if (n === 0) {
      if (allowEof && got === 0) return null;
      throw new WireError("the stream ended inside a packet");
    }
    got += n;
  }
  return buf;
}

export function writePacket(fd, body, fsWrite, cap = DEFAULT_CAP) {
  if (body.length > cap) throw new WireError(`packet of ${body.length} bytes is over the cap of ${cap}`);
  const header = Buffer.alloc(4);
  header.writeUInt32LE(body.length);
  const all = Buffer.concat([header, body]);
  let off = 0;
  while (off < all.length) off += fsWrite(fd, all, off, all.length - off);
}

// Messages.

export function encodeHello(worker, nodes, packetCap = DEFAULT_CAP, layoutHash) {
  const out = new Writer();
  out.u8(KIND.HELLO);
  out.u32(PROTOCOL_VERSION);
  out.u64(layoutHash);
  out.str(worker);
  out.u32(packetCap);
  out.u32(nodes.length);
  for (const node of nodes) {
    out.str(node.id);
    for (const ports of [node.inputs, node.outputs, []]) {
      out.u32(ports.length);
      for (const [name, ty] of ports) { out.str(name); encodeType(ty, out); out.u8(0); }
    }
    if (node.state == null) out.u8(0);
    else { out.u8(1); encodeType(node.state, out); }
    out.u32(0);
  }
  return out.finish();
}

export function decodeHost(body, schema) {
  const inp = new Reader(body);
  const kind = inp.u8();
  switch (kind) {
    case KIND.RUN: {
      const run = inp.u64();
      const region = inp.u32();
      const nodes = []; const n = inp.count();
      for (let i = 0; i < n; i++) nodes.push(inp.str());
      const inputs = []; const m = inp.count();
      for (let i = 0; i < m; i++) {
        const node = inp.str(); const port = inp.str();
        inputs.push([node, port, decodeValue(schema.lookup(node, "input", port), inp, schema)]);
      }
      inp.finish();
      return { kind: "run", run, region, nodes, inputs };
    }
    case KIND.CANCEL: { const run = inp.u64(); inp.finish(); return { kind: "cancel", run }; }
    case KIND.STATE_EXPORT: { const node = inp.str(); inp.finish(); return { kind: "state_export", node }; }
    case KIND.STATE_IMPORT: {
      const node = inp.str();
      const value = decodeValue(schema.lookup(node, "state", ""), inp, schema);
      inp.finish();
      return { kind: "state_import", node, value };
    }
    case KIND.PING: inp.finish(); return { kind: "ping" };
    case KIND.SHUTDOWN: inp.finish(); return { kind: "shutdown" };
    default: throw new WireError(`unknown message kind ${kind}`);
  }
}

export function encodeOutputs(run, status, outputs, schema) {
  const out = new Writer();
  out.u8(KIND.OUTPUTS);
  out.u64(run);
  out.u8({ ok: 0, cancelled: 1, fault: 2 }[status]);
  out.u32(outputs.length);
  for (const [node, port, value] of outputs) {
    out.str(node); out.str(port);
    encodeValue(schema.lookup(node, "output", port), value, schema, out);
  }
  return out.finish();
}

function truncate(text, cap) {
  const data = Buffer.from(text, "utf8");
  if (data.length <= cap) return text;
  let cut = cap;
  while (cut > 0 && (data[cut] & 0xc0) === 0x80) cut -= 1;
  return data.subarray(0, cut).toString("utf8");
}

export function encodeFault(run, node, code, reason) {
  const out = new Writer();
  out.u8(KIND.FAULT);
  out.u64(run);
  if (node == null) out.u8(0);
  else { out.u8(1); out.str(node); }
  out.u16(FAULT_CODES[code]);
  out.str(truncate(reason, REASON_CAP));
  return out.finish();
}

export function encodeProgress(run, nodeIndex, message, fraction) {
  const out = new Writer();
  out.u8(KIND.PROGRESS);
  out.u64(run);
  out.u32(nodeIndex);
  out.str(message);
  if (fraction == null) out.u8(0);
  else { out.u8(1); out.f32(fraction); }
  return out.finish();
}

export function encodeLog(level, text) {
  const out = new Writer();
  out.u8(KIND.LOG);
  out.u8(LOG_LEVELS[level]);
  out.str(text);
  return out.finish();
}

export function encodeDetail(run, text) {
  const out = new Writer();
  out.u8(KIND.DETAIL);
  out.u64(run);
  out.str(text);
  return out.finish();
}

export function encodeNodeCode(run, code) {
  const out = new Writer();
  out.u8(KIND.NODE_CODE);
  out.u64(run);
  out.u32(code);
  return out.finish();
}

export function encodePong() {
  return Buffer.from([KIND.PONG]);
}

export function encodeStateValue(node, ty, value, schema) {
  const out = new Writer();
  out.u8(KIND.STATE_VALUE);
  out.str(node);
  encodeValue(ty, value, schema, out);
  return out.finish();
}

// The layout hash: FNV-1a over the same words as core/wire's LAYOUT, each as 8 little-endian
// bytes. A hello carries it, and the host refuses a worker whose hash is another.
const LAYOUT_WORDS = [
  1n, 512n, 16777216n, 32n,
  1n, 2n, 3n, 4n, 5n, 6n,
  0x40n, 0x41n, 0x42n, 0x43n, 0x44n, 0x45n, 0x46n,
  16n, 8n,
];
export const LAYOUT_HASH = (() => {
  const mask = (1n << 64n) - 1n;
  let h = 0xcbf29ce484222325n;
  for (const word of LAYOUT_WORDS) {
    const bytes = Buffer.alloc(8);
    bytes.writeBigUInt64LE(word);
    for (const byte of bytes) {
      h ^= BigInt(byte);
      h = (h * 0x100000001b3n) & mask;
    }
  }
  return h;
})();
