import assert from "node:assert/strict";
import test from "node:test";

import {
  adapter, bytes, config, fromWire, gpu, node, parseWire, plugin, state, stringifyWire, toWire, typeKey, validateDescriptor,
} from "../src/index.js";

// What the Rust host serializes for `WireValue::UInt(u64::MAX)`.
const UINT_MAX_JSON = '{"kind":"uint","value":18446744073709551615}';
const U64_MAX = (1n << 64n) - 1n;

test("unsigned integers round trip as uint through BigInt", () => {
  assert.equal(stringifyWire(toWire(U64_MAX)), UINT_MAX_JSON);
  assert.equal(fromWire(parseWire(UINT_MAX_JSON)), U64_MAX);
  assert.deepEqual(toWire(5, "u64"), { kind: "uint", value: 5 });
  assert.deepEqual(toWire(5n, "u64"), { kind: "uint", value: 5n });
  assert.deepEqual(toWire(5), { kind: "int", value: 5 });
  assert.deepEqual(toWire(1n << 63n), { kind: "uint", value: 1n << 63n });
  assert.deepEqual(toWire(2, "f64"), { kind: "float", value: 2 });
  assert.deepEqual(toWire([1, 2], "list<u64>").value.map((item) => item.kind), ["uint", "uint"]);
  assert.deepEqual(toWire(3, "optional<u64>"), { kind: "uint", value: 3 });
  const nested = { big: [U64_MAX, -1], raw: Uint8Array.of(1), none: null, ok: true, text: "x" };
  assert.deepEqual(fromWire(parseWire(stringifyWire(toWire(nested)))), nested);
  assert.equal(fromWire(parseWire('{"kind":"int","value":-9223372036854775808}')), -(1n << 63n));
  for (const [value, type] of [[-1, "u64"], [U64_MAX + 1n, undefined], [-(1n << 63n) - 1n, undefined], [2 ** 60, "u64"]]) {
    assert.throws(() => toWire(value, type), RangeError);
  }
});

test("node sdk emits validated schema and package descriptors", () => {
  const ScaleConfig = config("ScaleConfig", {
    factor: config.port("i64", { default: 2 }),
  });
  const AccumState = state("AccumState", { sum: "i64" }, () => ({ sum: 0 }));
  const Point = typeKey("test.Point", { x: "f64", y: "f64" });
  const Mode = typeKey.enum("test.Mode", ["fast", "precise"]);
  const convert = adapter("test.point_to_i64").source(Point).target("i64").run(() => 1);

  const shape = node("shape")
    .inputs({
      point: Point,
      mode: Mode,
      maybe: "optional<i64>",
      items: "list<i64>",
      labels: "map<string,i64>",
      pair: "tuple<i64,bool>",
      config: ScaleConfig,
    })
    .outputs({ summary: "string" })
    .run(() => ({ summary: "ok" }));
  const accum = node("accum")
    .state(AccumState)
    .inputs({ value: "i64" })
    .outputs({ sum: "i64" })
    .run(() => ({ sum: 1 }));
  const gpuTint = node("gpu_tint")
    .inputs({ rgba8: gpu.rgba8({ residency: "gpu", layout: "rgba8-hwc" }) })
    .outputs({ rgba8: gpu.rgba8({ residency: "gpu", layout: "rgba8-hwc" }) })
    .run(({ rgba8 }) => ({ rgba8 }));

  const descriptor = plugin("test_plugin", [shape, accum, gpuTint])
    .typeContract("test.Point", ["host_read", "worker_write"])
    .adapter(convert)
    .transport({ buffer: true, sharedMemory: true })
    .artifact("_bundle/src/plugin.ts")
    .descriptor();

  validateDescriptor(descriptor);
  const nodes = Object.fromEntries(descriptor.schema.nodes.map((nodeDecl) => [nodeDecl.id, nodeDecl]));
  assert.equal(nodes.shape.inputs[0].type_key, "test.Point");
  assert.deepEqual(nodes.shape.inputs[2].ty, { Optional: { Scalar: "Int" } });
  assert.deepEqual(nodes.shape.inputs[3].ty, { List: { Scalar: "Int" } });
  assert.deepEqual(nodes.shape.inputs[4].ty, { Map: [{ Scalar: "String" }, { Scalar: "Int" }] });
  assert.deepEqual(nodes.shape.inputs[5].ty, { Tuple: [{ Scalar: "Int" }, { Scalar: "Bool" }] });
  assert.equal(nodes.accum.stateful, true);
  assert.equal(nodes.gpu_tint.inputs[0].residency, "gpu");
  assert.deepEqual(descriptor.metadata.adapters, ["test.point_to_i64"]);
});

test("node sdk records payload helper access modes", () => {
  const payload = node("payload")
    .inputs({
      view: bytes.view(),
      shared: bytes.sharedBuffer({ access: "read" }),
      cow: bytes.cowBuffer({ access: "modify" }),
      moved: bytes.buffer({ access: "move" }),
    })
    .outputs({ len: "u64" })
    .run(() => ({ len: 1 }));

  const descriptor = plugin("payload_plugin", [payload]).descriptor();
  const inputs = descriptor.schema.nodes[0].inputs;
  assert.equal(inputs[0].access, "read");
  assert.equal(inputs[1].access, "read");
  assert.equal(inputs[2].access, "modify");
  assert.equal(inputs[3].access, "move");
});

test("node sdk rejects invalid descriptors and unsupported types", () => {
  assert.throws(() => node("bad").inputs({ value: "set<i64>" }).outputs({ out: "i64" }).schema(), /unsupported type/);

  const add = node("add").inputs({ a: "i64", b: "i64" }).outputs({ out: "i64" }).run(() => ({ out: 1 }));
  const descriptor = plugin("bad_plugin", [add]).descriptor();
  delete descriptor.backends.add;
  assert.throws(() => validateDescriptor(descriptor), /missing backend/);

  const duplicate = plugin("bad_plugin", [add, add]);
  assert.throws(() => duplicate.descriptor(), /duplicate/);

  assert.throws(
    () => node("bad_access").inputs({ value: { __typeExpr: { Scalar: "Int" }, access: "project" } }).outputs({ out: "i64" }).schema(),
    /unsupported access/,
  );
  assert.throws(
    () => node("bad_residency").inputs({ value: { __typeExpr: { Scalar: "Bytes" }, residency: "disk" } }).outputs({ out: "i64" }).schema(),
    /unsupported residency/,
  );
  assert.throws(
    () => node("bad_layout").inputs({ value: { __typeExpr: { Scalar: "Bytes" }, layout: "rgba8-hwc" } }).outputs({ out: "i64" }).schema(),
    /layout requires residency/,
  );
  assert.throws(() => plugin("bad_plugin", []).typeContract("", ["host_read"]), /type_key/);
  assert.throws(() => plugin("bad_plugin", []).typeContract("test.Bad", ["teleport"]), /unsupported boundary/);
});
