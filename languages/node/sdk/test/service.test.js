// The service codec's edge cases, with no daemon: what a reader must refuse, and what it must
// skip. The rules are the ones in core/wire/src/service.rs.
import { test } from "node:test";
import assert from "node:assert/strict";
import * as svc from "../src/service.js";
import * as wire from "../src/wire.js";

const SERVER_HELLO = {
  kind: "hello",
  hello: {
    version: 1,
    layout: svc.SERVICE_LAYOUT_HASH,
    packetCap: 16 * 1024 * 1024,
    capabilities: { ops: svc.OP_NAMES, codecs: ["acme.blob"], maxRuns: 16 },
    knows: [],
    needs: [],
  },
};

test("the layout hash is the one core/wire pins", () => {
  assert.equal(svc.SERVICE_LAYOUT_HASH, 18420071168406881561n);
});

test("a server hello round trips, and an op this side does not have is dropped", () => {
  const body = svc.encodeServer(SERVER_HELLO);
  assert.deepEqual(svc.decodeServer(body), SERVER_HELLO);
  // A newer server lists an op (200) this side does not have: the count goes up, and the reader drops it.
  const at = 1 + 4 + 8 + 4;
  const count = body.readUInt32LE(at);
  const newer = Buffer.concat([body.subarray(0, at + 4), Buffer.from([200]), body.subarray(at + 4)]);
  newer.writeUInt32LE(count + 1, at);
  const read = svc.decodeServer(newer);
  assert.deepEqual(read.hello.capabilities.ops, svc.OP_NAMES);
});

test("a truncated message is a typed error, never a partial value", () => {
  const body = svc.encodeServer({ kind: "response", id: 7n, reply: { reply: "started", run: 3n } });
  for (let cut = 1; cut < body.length; cut++) {
    assert.throws(() => svc.decodeServer(body.subarray(0, cut)), wire.WireError, `cut at ${cut}`);
  }
});

test("an event of a kind this side does not have is unknown, and its fields are not read", () => {
  const body = Buffer.concat([Buffer.from([svc.KIND.EVENT]), Buffer.alloc(8), Buffer.from([77, 5, 6, 7])]);
  body.writeBigUInt64LE(9n, 1);
  assert.deepEqual(svc.decodeServer(body), { kind: "event", sub: 9n, body: { type: "unknown", eventKind: 77 } });
});

test("an error code this side does not have is other, and keeps its message", () => {
  const body = svc.encodeServer({ kind: "response", id: 3n, error: { code: "busy", message: "the pool is full", diagnostics: [] } });
  body.writeUInt16LE(900, 1 + 8 + 1);
  const read = svc.decodeServer(body);
  assert.equal(read.error.code, "other");
  assert.equal(read.error.message, "the pool is full");
});

test("trailing bytes after a whole message are ignored, and so are fields after a record's known ones", () => {
  const message = { kind: "response", id: 1n, reply: { reply: "trace", entries: [{ seq: 4n, id: 2n, session: "s", op: "ping", target: "", error: null }] } };
  const plain = svc.encodeServer(message);
  assert.deepEqual(svc.decodeServer(Buffer.concat([plain, Buffer.from([0xee, 0xee, 0xee])])), message);
  // The entry is a record: a longer one, with fields this side doesn't know at its end, reads the same.
  const inner = plain.subarray(1 + 8 + 1 + 1 + 4);
  const count = plain.subarray(1 + 8 + 1 + 1, 1 + 8 + 1 + 1 + 4);
  assert.equal(count.readUInt32LE(0), 1);
  const recordLen = inner.readUInt32LE(0);
  const padded = Buffer.concat([plain.subarray(0, 1 + 8 + 1 + 1 + 4), Buffer.alloc(4 + recordLen + 6)]);
  padded.writeUInt32LE(recordLen + 6, 1 + 8 + 1 + 1 + 4);
  inner.copy(padded, 1 + 8 + 1 + 1 + 4, 0, 4 + recordLen);
  padded.fill(0xa5, 1 + 8 + 1 + 1 + 4 + 4 + recordLen);
  assert.deepEqual(svc.decodeServer(padded), message);
});

test("a count larger than the bytes left is refused before anything is read", () => {
  const body = Buffer.from([svc.KIND.RESPONSE, ...Buffer.alloc(8), 0, 13, 0xff, 0xff, 0xff, 0xff]);
  assert.throws(() => svc.decodeServer(body), wire.WireError);
});

test("a string that is not UTF-8 is refused", () => {
  const body = Buffer.concat([Buffer.from([svc.KIND.RESPONSE]), Buffer.alloc(8), Buffer.from([1, 0xff, 0, 0, 0, 0, 0, 0, 0, 0])]);
  body.writeUInt32LE(2, 1 + 8 + 1 + 2);
  body.writeUInt16LE(1, 1 + 8 + 1);
  body.writeUInt32LE(0, 1 + 8 + 1 + 2);
  assert.throws(() => svc.decodeServer(body), wire.WireError);
});

test("a request for an op this side does not have is refused, as its codec has no reading of it", () => {
  const body = Buffer.concat([Buffer.from([svc.KIND.REQUEST]), Buffer.alloc(8), Buffer.from([0, 0, 0, 0, 200])]);
  assert.throws(() => svc.decodeClient(body), wire.WireError);
});

test("a feed value round trips with its type, and a type with no codec is refused", () => {
  const feed = { kind: "request", request: { id: 5n, session: "tools", op: { op: "feed", run: 7n, port: "in", value: { ty: { scalar: "u32" }, value: 41 } } } };
  const body = svc.encodeClient(feed);
  assert.deepEqual(svc.decodeClient(body), feed);
  const opaque = { kind: "request", request: { id: 5n, session: "default", op: { op: "feed", run: 1n, port: "in", value: { ty: { opaque: "acme.blob" }, value: Buffer.from([1]) } } } };
  assert.throws(() => svc.encodeClient(opaque), wire.WireError);
  assert.throws(() => svc.decodeClient(svc.encodeClient(opaque, new wire.Schema(["acme.blob"]))), wire.WireError);
});

test("a fault from a first-version server has no code: it reads as zero, and is re-encoded with the default", () => {
  const body = svc.encodeServer({
    kind: "event",
    sub: 1n,
    body: { type: "fault", run: 1n, tick: 1, step: 0, node: "io", covers: ["work"], consecutive: 1, reason: "boom", reasonCut: false, detail: "", detailCut: false, code: 0, ownCode: false },
  });
  // Cut the two added fields (a u32 and a flag) from the end: what a first-version server sent.
  const first = body.subarray(0, body.length - 5);
  const read = svc.decodeServer(first);
  assert.equal(read.body.code, 0);
  assert.equal(read.body.ownCode, false);
  assert.deepEqual(svc.encodeServer(read), body);
});

test("a watch with bits no event kind has is refused", () => {
  const body = Buffer.concat([Buffer.from([svc.KIND.REQUEST]), Buffer.alloc(8), Buffer.from([0]), Buffer.from([9, 0, 0, 0, 0, 0, 0, 0, 0, 8])]);
  assert.throws(() => svc.decodeClient(body), wire.WireError);
});
