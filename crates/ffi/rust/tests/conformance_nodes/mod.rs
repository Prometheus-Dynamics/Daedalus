//! The nodes of the canonical fixtures (`daedalus-ffi-core`), one plugin per fixture, served by the
//! conformance worker. Each node is an ordinary `#[node]`; the custom types declare how their
//! `Value` form decodes (`register_const_coercer`) and encodes (`register_to_value_serializer`).

use std::collections::BTreeMap;

use daedalus::data::model::{EnumValue, StructFieldValue, Value};
use daedalus::macros::{DaedalusToValue, DaedalusTypeExpr, node};
use daedalus::runtime::NodeError;
use daedalus::{PluginRegistry, declare_plugin};
use daedalus_ffi_core::{ImagePayload, InvokeEventLevel, WirePayloadHandle};
use daedalus_ffi_rust::{emit_event, event_debug};

fn field<'a>(fields: &'a [StructFieldValue], name: &str) -> Option<&'a Value> {
    fields
        .iter()
        .find(|field| field.name == name)
        .map(|field| &field.value)
}

// ---- struct_update ----

#[derive(Clone, Debug, Default, DaedalusTypeExpr, DaedalusToValue)]
pub struct Demo {
    pub name: String,
    pub count: i64,
}

fn decode_demo(value: &Value) -> Option<Demo> {
    let Value::Struct(fields) = value else {
        return None;
    };
    let Value::String(name) = field(fields, "name")? else {
        return None;
    };
    let Value::Int(count) = field(fields, "count")? else {
        return None;
    };
    Some(Demo {
        name: name.to_string(),
        count: *count,
    })
}

#[node(id = "run", inputs("input"), outputs("out"))]
fn struct_update(input: Demo) -> Result<Demo, NodeError> {
    Ok(Demo {
        count: input.count + 1,
        ..input
    })
}

declare_plugin!(
    StructUpdatePlugin,
    "ffi.conformance.struct_update",
    [struct_update],
    install = |registry| { install_struct_update(registry) }
);

fn install_struct_update(registry: &mut PluginRegistry) {
    registry.register_const_coercer::<Demo, _>(decode_demo);
    registry.register_to_value_serializer::<Demo>();
}

// ---- enum_roundtrip ----

#[derive(Clone, Debug, DaedalusTypeExpr, DaedalusToValue)]
pub enum Choice {
    Ready,
    Value(i64),
}

fn decode_choice(value: &Value) -> Option<Choice> {
    let Value::Enum(EnumValue { name, value }) = value else {
        return None;
    };
    match (name.as_str(), value.as_deref()) {
        ("Ready", None) => Some(Choice::Ready),
        ("Value", Some(Value::Int(number))) => Some(Choice::Value(*number)),
        _ => None,
    }
}

#[node(id = "run", inputs("input"), outputs("out"))]
fn enum_roundtrip(input: Choice) -> Result<Choice, NodeError> {
    Ok(input)
}

declare_plugin!(
    EnumRoundtripPlugin,
    "ffi.conformance.enum_roundtrip",
    [enum_roundtrip],
    install = |registry| { install_enum(registry) }
);

fn install_enum(registry: &mut PluginRegistry) {
    registry.register_const_coercer::<Choice, _>(decode_choice);
    registry.register_to_value_serializer::<Choice>();
}

// ---- optional_default ----

#[node(id = "run", inputs("maybe"), outputs("out"))]
fn optional_default(maybe: Option<i64>) -> Result<i64, NodeError> {
    Ok(maybe.unwrap_or(5))
}

declare_plugin!(
    OptionalDefaultPlugin,
    "ffi.conformance.optional_default",
    [optional_default],
    install = |registry| { install_optional(registry) }
);

fn install_optional(registry: &mut PluginRegistry) {
    registry.register_const_coercer::<Option<i64>, _>(|value| match value {
        Value::Unit => Some(None),
        Value::Int(number) => Some(Some(*number)),
        _ => None,
    });
}

// ---- list_sum ----

#[node(id = "run", inputs("items"), outputs("out"))]
fn list_sum(items: Vec<i64>) -> Result<i64, NodeError> {
    Ok(items.iter().sum())
}

declare_plugin!(
    ListSumPlugin,
    "ffi.conformance.list_sum",
    [list_sum],
    install = |registry| { install_list(registry) }
);

fn install_list(registry: &mut PluginRegistry) {
    registry.register_const_coercer::<Vec<i64>, _>(|value| match value {
        Value::List(items) => items
            .iter()
            .map(|item| match item {
                Value::Int(number) => Some(*number),
                _ => None,
            })
            .collect(),
        _ => None,
    });
}

// ---- tuple_unpack ----

#[node(id = "run", inputs("pair"), outputs("out"))]
fn tuple_unpack(pair: (String, bool)) -> Result<String, NodeError> {
    Ok(format!("{}:{}", pair.0, pair.1))
}

declare_plugin!(
    TupleUnpackPlugin,
    "ffi.conformance.tuple_unpack",
    [tuple_unpack],
    install = |registry| { install_tuple(registry) }
);

fn install_tuple(registry: &mut PluginRegistry) {
    registry.register_const_coercer::<(String, bool), _>(|value| match value {
        Value::List(items) => match items.as_slice() {
            [Value::String(text), Value::Bool(flag)] => Some((text.to_string(), *flag)),
            _ => None,
        },
        _ => None,
    });
}

// ---- map_lookup ----

#[node(id = "run", inputs("items"), outputs("out"))]
fn map_lookup(items: BTreeMap<String, i64>) -> Result<i64, NodeError> {
    Ok(items.get("b").copied().unwrap_or_default())
}

declare_plugin!(
    MapLookupPlugin,
    "ffi.conformance.map_lookup",
    [map_lookup],
    install = |registry| { install_map(registry) }
);

fn install_map(registry: &mut PluginRegistry) {
    // A wire record is a struct value; its fields are the map's entries.
    registry.register_const_coercer::<BTreeMap<String, i64>, _>(|value| match value {
        Value::Struct(fields) => fields
            .iter()
            .map(|field| match &field.value {
                Value::Int(number) => Some((field.name.clone(), *number)),
                _ => None,
            })
            .collect(),
        _ => None,
    });
}

// ---- image_passthrough ----

#[node(
    id = "run",
    inputs(port(name = "image", type_key = "ffi:image:u8")),
    outputs(port(name = "image", type_key = "ffi:image:u8"))
)]
fn image_passthrough(image: ImagePayload) -> Result<ImagePayload, NodeError> {
    Ok(image)
}

declare_plugin!(
    ImagePassthroughPlugin,
    "ffi.conformance.image_passthrough",
    [image_passthrough]
);

// ---- stateful_counter ----

#[derive(Default, DaedalusToValue)]
pub struct CounterState {
    pub count: i64,
}

pub fn decode_counter(value: &Value) -> Option<CounterState> {
    let Value::Struct(fields) = value else {
        return None;
    };
    let Value::Int(count) = field(fields, "count")? else {
        return None;
    };
    Some(CounterState { count: *count })
}

#[node(id = "run", inputs("delta"), outputs("count"), state(CounterState))]
fn stateful_counter(delta: i64, state: &mut CounterState) -> Result<i64, NodeError> {
    state.count += delta;
    Ok(state.count)
}

declare_plugin!(
    StatefulCounterPlugin,
    "ffi.conformance.stateful_counter",
    [stateful_counter],
    install = |registry| { install_counter(registry) }
);

fn install_counter(_registry: &mut PluginRegistry) {}

// ---- shader_write_u32 and package_shader_asset ----
// The fixtures' expected values are the CPU form of the shader arithmetic: the worker computes the
// result and does not dispatch the WGSL asset.

#[node(id = "run", inputs("input"), outputs("out"))]
fn shader_write_u32(input: u32) -> Result<u32, NodeError> {
    Ok(input + 1)
}

declare_plugin!(
    ShaderWriteU32Plugin,
    "ffi.conformance.shader_write_u32",
    [shader_write_u32]
);

#[node(id = "run", inputs("value"), outputs("out"))]
fn package_shader_asset(value: u32) -> Result<u32, NodeError> {
    Ok(value + 1)
}

declare_plugin!(
    PackageShaderAssetPlugin,
    "ffi.conformance.package_shader_asset",
    [package_shader_asset]
);

// ---- capability_camera_frame ----

#[node(
    id = "run",
    inputs(port(name = "frame", type_key = "ffi:frame")),
    outputs("ok")
)]
fn capability_camera_frame(frame: WirePayloadHandle) -> Result<bool, NodeError> {
    Ok(frame
        .capabilities
        .iter()
        .any(|capability| capability == "camera.read"))
}

declare_plugin!(
    CapabilityCameraFramePlugin,
    "ffi.conformance.capability_camera_frame",
    [capability_camera_frame]
);

// ---- custom_point_type ----

#[derive(Clone, Debug, DaedalusTypeExpr, DaedalusToValue)]
#[daedalus(type_key = "example.Point")]
pub struct Point {
    pub x: f64,
    pub y: f64,
}

fn decode_point(value: &Value) -> Option<Point> {
    let Value::Struct(fields) = value else {
        return None;
    };
    let (Value::Float(x), Value::Float(y)) = (field(fields, "x")?, field(fields, "y")?) else {
        return None;
    };
    Some(Point { x: *x, y: *y })
}

#[node(
    id = "run",
    inputs(port(name = "point", type_key = "example.Point")),
    outputs("magnitude")
)]
fn custom_point_type(point: Point) -> Result<f64, NodeError> {
    emit_event(
        InvokeEventLevel::Info,
        "custom type key resolved",
        BTreeMap::from([("type_key".into(), serde_json::json!("example.Point"))]),
    );
    Ok(point.x.abs() + point.y.abs())
}

declare_plugin!(
    CustomPointTypePlugin,
    "ffi.conformance.custom_point_type",
    [custom_point_type],
    install = |registry| { install_point(registry) }
);

fn install_point(registry: &mut PluginRegistry) {
    registry.register_const_coercer::<Point, _>(decode_point);
}

// ---- boundary_frame_modify ----

#[node(
    id = "run",
    inputs(port(name = "frame", type_key = "example.Frame")),
    outputs(port(name = "frame", type_key = "example.Frame"))
)]
fn boundary_frame_modify(frame: WirePayloadHandle) -> Result<WirePayloadHandle, NodeError> {
    Ok(frame)
}

declare_plugin!(
    BoundaryFrameModifyPlugin,
    "ffi.conformance.boundary_frame_modify",
    [boundary_frame_modify],
    install = |registry| { install_boundary(registry) }
);

fn install_boundary(registry: &mut PluginRegistry) {
    // The contract the macros would derive for a port of this type: the layout hash is over the
    // port's `TypeExpr` label, as `#[node]` descriptors do.
    let port_ty = daedalus::data::model::TypeExpr::opaque("example.Frame");
    let contract = daedalus::transport::BoundaryTypeContract::for_schema::<WirePayloadHandle>(
        "example.Frame",
        port_ty.canonical_label(),
        daedalus::transport::BoundaryCapabilities::rust_value(),
    );
    registry
        .register_boundary_contract(contract)
        .expect("boundary contract");
}

// ---- scalar_add, bytes_echo, multi_output, raw_io, typed_failure ----

#[node(id = "add", inputs("a", "b"), outputs("out"))]
fn scalar_add(a: i64, b: i64) -> Result<i64, NodeError> {
    Ok(a + b)
}

declare_plugin!(ScalarAddPlugin, "ffi.conformance.scalar_add", [scalar_add]);

#[node(id = "run", inputs("payload"), outputs("out"))]
fn bytes_echo(payload: Vec<u8>) -> Result<Vec<u8>, NodeError> {
    Ok(payload)
}

declare_plugin!(BytesEchoPlugin, "ffi.conformance.bytes_echo", [bytes_echo]);

#[node(id = "split", inputs("value"), outputs("double", "label"))]
fn multi_output(value: i64) -> Result<(i64, String), NodeError> {
    Ok((value * 2, format!("value:{value}")))
}

declare_plugin!(
    MultiOutputPlugin,
    "ffi.conformance.multi_output",
    [multi_output]
);

#[node(id = "event", inputs("payload"), outputs("out"))]
fn raw_io_event(payload: Vec<u8>) -> Result<Vec<u8>, NodeError> {
    let bytes = payload.len();
    emit_event(
        InvokeEventLevel::Info,
        "raw payload accepted",
        BTreeMap::from([("bytes".into(), serde_json::json!(bytes))]),
    );
    Ok(payload)
}

declare_plugin!(RawIoPlugin, "ffi.conformance.raw_io", [raw_io_event]);

#[node(id = "run", inputs("value"), outputs("ok"))]
fn typed_failure(value: i64) -> Result<bool, NodeError> {
    if value < 0 {
        emit_event(
            InvokeEventLevel::Error,
            "value must be non-negative",
            BTreeMap::from([
                ("code".into(), serde_json::json!("invalid_argument")),
                ("field".into(), serde_json::json!("value")),
            ]),
        );
        return Ok(false);
    }
    event_debug("value accepted");
    Ok(true)
}

declare_plugin!(
    TypedFailurePlugin,
    "ffi.conformance.typed_failure",
    [typed_failure]
);

/// Every conformance plugin, installed into one registry.
pub fn install_all(registry: &mut PluginRegistry) -> Result<(), String> {
    macro_rules! install {
        ($($plugin:ident),* $(,)?) => {
            $( registry.install(&$plugin::new()).map_err(|err| err.to_string())?; )*
        };
    }
    install!(
        ScalarAddPlugin,
        BytesEchoPlugin,
        MultiOutputPlugin,
        RawIoPlugin,
        TypedFailurePlugin,
        StructUpdatePlugin,
        EnumRoundtripPlugin,
        OptionalDefaultPlugin,
        ListSumPlugin,
        TupleUnpackPlugin,
        MapLookupPlugin,
        ImagePassthroughPlugin,
        StatefulCounterPlugin,
        ShaderWriteU32Plugin,
        PackageShaderAssetPlugin,
        CapabilityCameraFramePlugin,
        CustomPointTypePlugin,
        BoundaryFrameModifyPlugin,
    );
    Ok(())
}
