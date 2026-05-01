import { adapter, bytes, config, event, gpu, image, node, plugin, state, typeKey } from "../../../../../crates/ffi/node/sdk/src/index.js";

const ScaleConfig = config("ScaleConfig", {
  factor: config.port("i64", { default: 2, min: 1, max: 16, policy: "clamp" }),
});

const AccumState = state("AccumState", { sum: "i64" }, () => ({ sum: 0 }));

const Point = typeKey("ffi.showcase.Point", {
  x: "f64",
  y: "f64",
});

const Mode = typeKey.enum("ffi.showcase.Mode", ["fast", "precise"]);

const InternalCount = typeKey("ffi.showcase.InternalCount", { value: "i64" });
const ExternalLegacyCount = typeKey("ffi.external.LegacyCount", { raw: "string" });

const internalCountToI64 = adapter("ffi.showcase.internal_count_to_i64")
  .source(InternalCount)
  .target("i64")
  .run((value) => value.value);

const externalLegacyCountToI64 = adapter("ffi.showcase.external_legacy_count_to_i64")
  .source(ExternalLegacyCount)
  .target("i64")
  .run((value) => Number.parseInt(value.raw, 10));

const scalarAdd = node("scalar_add")
  .inputs({ a: "i64", b: "i64" })
  .outputs({ out: "i64" })
  .run(({ a, b }) => ({ out: a + b }));

const splitSign = node("split_sign")
  .inputs({ value: "i64" })
  .outputs({ positive: "i64", negative: "i64" })
  .run(({ value }) => ({ positive: value, negative: -value }));

const scale = node("scale")
  .inputs({ value: "i64", config: ScaleConfig })
  .outputs({ out: "i64" })
  .run(({ value, config }) => ({ out: value * config.factor }));

const accumulate = node("accumulate")
  .state(AccumState)
  .inputs({ value: "i64" })
  .outputs({ sum: "i64" })
  .run(({ value }, state) => {
    state.sum += value;
    return { sum: state.sum };
  });

const bytesLen = node("bytes_len")
  .inputs({ payload: bytes.view() })
  .outputs({ len: "u64" })
  .run(({ payload }) => ({ len: payload.byteLength }));

const imageBoost = node("image_boost")
  .inputs({ rgba8: image.rgba8() })
  .outputs({ rgba8: image.rgba8() })
  .run(({ rgba8 }) => ({ rgba8: rgba8.mapPixels(([r, g, b, a]) => [Math.min(r + 8, 255), g, b, a]) }));

const shapeSummary = node("shape_summary")
  .inputs({
    point: Point,
    mode: Mode,
    maybe: "optional<i64>",
    items: "list<i64>",
    labels: "map<string,i64>",
    pair: "tuple<i64,bool>",
    unit: "unit",
  })
  .outputs({ summary: "string" })
  .run(({ point, mode, maybe, items, labels, pair }) => ({
    summary: `${mode}:${point.x.toFixed(1)},${point.y.toFixed(1)}:${maybe ?? 0}:${items.length}:${Object.keys(labels).length}:${pair[0]}`,
  }));

const emitEvent = node("emit_event")
  .inputs({ message: "string" })
  .outputs({ ok: "bool" })
  .run(({ message }, ctx) => {
    ctx.info("showcase.event", message);
    return { ok: true };
  });

const capabilityAdd = node("capability_add")
  .capability("Add")
  .inputs({ a: "i64", b: "i64" })
  .outputs({ out: "i64" })
  .run(({ a, b }) => ({ out: a + b }));

const checkedDivide = node("checked_divide")
  .inputs({ a: "i64", b: "i64" })
  .outputs({ out: "i64" })
  .run(({ a, b }) => {
    if (b === 0) throw event.typedError("division_by_zero", "b must not be zero");
    return { out: Math.trunc(a / b) };
  });

const arrayDynamicSum = node("array_dynamic_sum")
  .inputs({ values: "list<i64>" })
  .outputs({ sum: "i64" })
  .run(({ values }) => ({ sum: values.reduce((total, value) => total + value, 0) }));

const nodeIoComplex = node("node_io_complex")
  .inputs({ point: Point, weights: "list<f64>", metadata: "map<string,string>" })
  .outputs({ score: "f64", label: "string", point: Point })
  .run(({ point, weights, metadata }) => ({
    score: point.x + point.y + weights.reduce((total, value) => total + value, 0),
    label: metadata.label ?? "unlabeled",
    point,
  }));

const gpuTint = node("gpu_tint")
  .inputs({ rgba8: gpu.rgba8({ residency: "gpu", layout: "rgba8-hwc" }) })
  .outputs({ rgba8: gpu.rgba8({ residency: "gpu", layout: "rgba8-hwc" }) })
  .run(({ rgba8 }) => ({ rgba8: rgba8.dispatch("ffi_showcase_tint") }));

const internalAdapterConsume = node("internal_adapter_consume")
  .inputs({ count: "i64" })
  .outputs({ out: "i64" })
  .run(({ count }) => ({ out: count + 1 }));

const externalAdapterConsume = node("external_adapter_consume")
  .inputs({ count: "i64" })
  .outputs({ out: "i64" })
  .run(({ count }) => ({ out: count * 2 }));

const zeroCopyLen = node("zero_copy_len")
  .inputs({ frame: bytes.buffer({ access: "view", sharedMemory: true }) })
  .outputs({ len: "u64" })
  .run(({ frame }) => ({ len: frame.byteLength }));

const sharedRefLen = node("shared_ref_len")
  .inputs({ frame: bytes.sharedBuffer({ access: "read" }) })
  .outputs({ len: "u64" })
  .run(({ frame }) => ({ len: frame.byteLength }));

const cowAppendMarker = node("cow_append_marker")
  .inputs({ frame: bytes.cowBuffer({ access: "modify" }) })
  .outputs({ frame: bytes.buffer() })
  .run(({ frame }) => ({ frame: frame.withAppended(Buffer.from([255])) }));

const mutableBrighten = node("mutable_brighten")
  .inputs({ rgba8: image.mutableRgba8({ access: "modify" }) })
  .outputs({ rgba8: image.rgba8() })
  .run(({ rgba8 }) => ({ rgba8: rgba8.mapPixelsInPlace(([r, g, b, a]) => [Math.min(r + 1, 255), g, b, a]) }));

const ownedBytesLen = node("owned_bytes_len")
  .inputs({ blob: bytes.buffer({ access: "move" }) })
  .outputs({ len: "u64" })
  .run(({ blob }) => ({ len: blob.byteLength }));

export const showcasePlugin = plugin("ffi_showcase", [
  scalarAdd,
  splitSign,
  scale,
  accumulate,
  bytesLen,
  imageBoost,
  shapeSummary,
  emitEvent,
  capabilityAdd,
  checkedDivide,
  arrayDynamicSum,
  nodeIoComplex,
  gpuTint,
  internalAdapterConsume,
  externalAdapterConsume,
  zeroCopyLen,
  sharedRefLen,
  cowAppendMarker,
  mutableBrighten,
  ownedBytesLen,
])
  .typeContract("ffi.showcase.Point", ["host_read", "worker_write"])
  .adapter(internalCountToI64)
  .adapter(externalLegacyCountToI64)
  .transport({ buffer: true, sharedMemory: true })
  .artifact("_bundle/src/plugin.ts");
