// The codec: round trips and the refusals the protocol names.
import { test } from "node:test";
import assert from "node:assert/strict";
import * as wire from "../src/wire.js";

const schema = new wire.Schema(["fixture.custom"]);

function roundTrip(ty, value) {
  const out = new wire.Writer();
  wire.encodeValue(ty, value, schema, out);
  const inp = new wire.Reader(out.finish());
  const back = wire.decodeValue(ty, inp, schema);
  inp.finish();
  return back;
}

test("u64 max round trips as a BigInt", () => {
  assert.equal(roundTrip(wire.T.u64, (1n << 64n) - 1n), (1n << 64n) - 1n);
});

test("a Number above the safe range is refused for u64", () => {
  const out = new wire.Writer();
  assert.throws(() => wire.encodeValue(wire.T.u64, 2 ** 60 + 1, schema, out), wire.WireError);
});

test("a negative u64 is refused", () => {
  const out = new wire.Writer();
  assert.throws(() => wire.encodeValue(wire.T.u64, -1n, schema, out), wire.WireError);
});

test("bytes from a Buffer and from a Uint8Array round trip", () => {
  assert.deepEqual(roundTrip(wire.T.bytes, Buffer.from([0, 1, 255])), Buffer.from([0, 1, 255]));
  assert.deepEqual(roundTrip(wire.T.bytes, new Uint8Array([9, 8])), Buffer.from([9, 8]));
});

test("compound values round trip", () => {
  const struct = wire.T.struct(["name", wire.T.string], ["count", wire.T.i64]);
  assert.deepEqual(roundTrip(struct, { name: "demo", count: 3n }), { name: "demo", count: 3n });
  const enumTy = wire.T.enum(["idle", null], ["busy", wire.T.u32]);
  assert.deepEqual(roundTrip(enumTy, { variant: "busy", payload: 7 }), { variant: "busy", payload: 7 });
  assert.deepEqual(roundTrip(wire.T.list(wire.T.u32), [1, 2, 3]), [1, 2, 3]);
});

test("an opaque value needs a codec", () => {
  const out = new wire.Writer();
  assert.throws(
    () => wire.encodeValue(wire.T.opaque("other.key"), Buffer.from([1]), schema, out),
    wire.WireError,
  );
});

test("the layout hash is the Rust constant", () => {
  assert.equal(wire.LAYOUT_HASH, 16986761858374539365n);
});

test("a type nested past the limit is refused", () => {
  let ty = wire.T.bool;
  for (let i = 0; i < 40; i++) ty = wire.T.optional(ty);
  assert.throws(() => wire.encodeType(ty, new wire.Writer()), wire.WireError);
});
