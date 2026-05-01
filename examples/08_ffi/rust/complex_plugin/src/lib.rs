use std::collections::BTreeMap;
use std::sync::Arc;

use daedalus::macros::{NodeConfig, node};
use daedalus::{PluginRegistry, adapt, declare_plugin, runtime::NodeError, transport::TransportError};

#[derive(Clone, Debug, NodeConfig)]
struct ScaleConfig {
    #[port(default = 2, min = 1, max = 16, policy = "clamp")]
    factor: i64,
}

#[derive(Clone, Debug)]
#[daedalus(type_key = "ffi.showcase.Point")]
struct Point {
    x: f64,
    y: f64,
}

#[derive(Clone, Debug)]
enum Mode {
    Fast,
    Precise,
}

#[derive(Default)]
struct AccumState {
    sum: i64,
}

#[derive(Clone, Debug)]
#[daedalus(type_key = "ffi.showcase.InternalCount")]
struct InternalCount(i64);

#[derive(Clone, Debug)]
#[daedalus(type_key = "ffi.external.LegacyCount")]
struct ExternalLegacyCount {
    raw: String,
}

#[derive(Clone, Debug)]
#[daedalus(type_key = "ffi.showcase.FrameBytes")]
struct FrameBytes(Arc<Vec<u8>>);

#[derive(Clone, Debug)]
#[daedalus(type_key = "ffi.showcase.OwnedBlob")]
struct OwnedBlob(Vec<u8>);

#[adapt(id = "ffi.showcase.internal_count_to_i64", kind = daedalus::transport::AdapterKind::Reinterpret)]
fn internal_count_to_i64(value: &InternalCount) -> Result<i64, TransportError> {
    Ok(value.0)
}

#[adapt(id = "ffi.showcase.external_legacy_count_to_i64", kind = daedalus::transport::AdapterKind::Materialize)]
fn external_legacy_count_to_i64(value: &ExternalLegacyCount) -> Result<i64, TransportError> {
    value.raw.parse::<i64>().map_err(|err| TransportError::Adapter {
        message: err.to_string(),
    })
}

#[node(id = "scalar_add", inputs("a", "b"), outputs("out"))]
fn scalar_add(a: i64, b: i64) -> Result<i64, NodeError> {
    Ok(a + b)
}

#[node(id = "split_sign", inputs("value"), outputs("positive", "negative"))]
fn split_sign(value: i64) -> Result<(i64, i64), NodeError> {
    Ok((value, -value))
}

#[node(id = "scale", inputs("value", config = ScaleConfig), outputs("out"))]
fn scale(value: i64, config: ScaleConfig) -> Result<i64, NodeError> {
    Ok(value * config.factor)
}

#[node(id = "accumulate", inputs("value"), outputs("sum"), state(AccumState))]
fn accumulate(value: i64, state: &mut AccumState) -> Result<i64, NodeError> {
    state.sum += value;
    Ok(state.sum)
}

#[node(id = "bytes_len", inputs("payload"), outputs("len"))]
fn bytes_len(payload: &[u8]) -> Result<u64, NodeError> {
    Ok(payload.len() as u64)
}

#[node(id = "image_boost", inputs("rgba8"), outputs("rgba8"))]
fn image_boost(mut rgba8: daedalus::ImageRgba8) -> Result<daedalus::ImageRgba8, NodeError> {
    for pixel in rgba8.pixels_mut() {
        pixel.r = pixel.r.saturating_add(8);
    }
    Ok(rgba8)
}

#[node(id = "shape_summary", inputs("point", "mode", "maybe", "items", "labels", "pair", "unit"), outputs("summary"))]
fn shape_summary(
    point: Point,
    mode: Mode,
    maybe: Option<i64>,
    items: Vec<i64>,
    labels: BTreeMap<String, i64>,
    pair: (i64, bool),
    _unit: (),
) -> Result<String, NodeError> {
    Ok(format!(
        "{mode:?}:{:.1},{:.1}:{}:{}:{}:{}",
        point.x,
        point.y,
        maybe.unwrap_or_default(),
        items.len(),
        labels.len(),
        pair.0
    ))
}

#[node(id = "emit_event", inputs("message"), outputs("ok"))]
fn emit_event(message: String, ctx: daedalus::EventContext) -> Result<bool, NodeError> {
    ctx.info("showcase.event", message);
    Ok(true)
}

#[node(id = "capability_add", capability = "Add", inputs("a", "b"), outputs("out"))]
fn capability_add<T>(a: T, b: T) -> Result<T, NodeError>
where
    T: std::ops::Add<Output = T> + Clone + Send + Sync + 'static,
{
    Ok(a + b)
}

#[node(id = "checked_divide", inputs("a", "b"), outputs("out"))]
fn checked_divide(a: i64, b: i64) -> Result<i64, NodeError> {
    if b == 0 {
        return Err(NodeError::typed("division_by_zero", "b must not be zero"));
    }
    Ok(a / b)
}

#[node(id = "array_dynamic_sum", inputs("values"), outputs("sum"))]
fn array_dynamic_sum(values: Vec<i64>) -> Result<i64, NodeError> {
    Ok(values.into_iter().sum())
}

#[node(id = "node_io_complex", inputs("point", "weights", "metadata"), outputs("score", "label", "point"))]
fn node_io_complex(
    point: Point,
    weights: Vec<f64>,
    metadata: BTreeMap<String, String>,
) -> Result<(f64, String, Point), NodeError> {
    let weight_sum: f64 = weights.into_iter().sum();
    let label = metadata.get("label").cloned().unwrap_or_else(|| "unlabeled".into());
    Ok((point.x + point.y + weight_sum, label, point))
}

#[node(id = "gpu_tint", inputs("rgba8"), outputs("rgba8"), residency = "gpu", layout = "rgba8-hwc")]
fn gpu_tint(rgba8: daedalus::GpuImageRgba8) -> Result<daedalus::GpuImageRgba8, NodeError> {
    Ok(rgba8.dispatch_wgsl("ffi_showcase_tint"))
}

#[node(id = "internal_adapter_consume", inputs("count"), outputs("out"))]
fn internal_adapter_consume(count: i64) -> Result<i64, NodeError> {
    Ok(count + 1)
}

#[node(id = "external_adapter_consume", inputs("count"), outputs("out"))]
fn external_adapter_consume(count: i64) -> Result<i64, NodeError> {
    Ok(count * 2)
}

#[node(id = "zero_copy_len", inputs("frame"), outputs("len"), access = "view")]
fn zero_copy_len(frame: &FrameBytes) -> Result<u64, NodeError> {
    Ok(frame.0.len() as u64)
}

#[node(id = "shared_ref_len", inputs("frame"), outputs("len"), access = "read")]
fn shared_ref_len(frame: Arc<FrameBytes>) -> Result<u64, NodeError> {
    Ok(frame.0.len() as u64)
}

#[node(id = "cow_append_marker", inputs("frame"), outputs("frame"), access = "modify")]
fn cow_append_marker(mut frame: std::borrow::Cow<'_, FrameBytes>) -> Result<FrameBytes, NodeError> {
    Arc::make_mut(&mut frame.to_mut().0).push(255);
    Ok(frame.into_owned())
}

#[node(id = "mutable_brighten", inputs("rgba8"), outputs("rgba8"), access = "modify")]
fn mutable_brighten(rgba8: &mut daedalus::ImageRgba8) -> Result<(), NodeError> {
    for pixel in rgba8.pixels_mut() {
        pixel.r = pixel.r.saturating_add(1);
    }
    Ok(())
}

#[node(id = "owned_bytes_len", inputs("blob"), outputs("len"), access = "move")]
fn owned_bytes_len(blob: OwnedBlob) -> Result<u64, NodeError> {
    Ok(blob.0.len() as u64)
}

fn install(registry: &mut PluginRegistry) {
    registry.register_capability_typed::<i64, _>("Add", |a, b| Ok(*a + *b));
    registry.register_boundary_contract("ffi.showcase.Point", ["host_read", "worker_write"]);
    registry.register_boundary_contract("ffi.showcase.FrameBytes", ["borrow_ref", "borrow_mut", "shared_clone"]);
    registry.register_adapter(internal_count_to_i64);
    registry.register_adapter(external_legacy_count_to_i64);
    registry.register_package_artifact("_bundle/native/any/libffi_showcase.so");
}

declare_plugin!(
    FfiShowcasePlugin,
    "ffi_showcase",
    [
        scalar_add,
        split_sign,
        scale,
        accumulate,
        bytes_len,
        image_boost,
        shape_summary,
        emit_event,
        capability_add,
        checked_divide,
        array_dynamic_sum,
        node_io_complex,
        gpu_tint,
        internal_adapter_consume,
        external_adapter_consume,
        zero_copy_len,
        shared_ref_len,
        cow_append_marker,
        mutable_brighten,
        owned_bytes_len
    ],
    install = install
);
