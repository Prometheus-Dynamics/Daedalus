//! Typed host ports, const type keys, derived value types registered through `#[plugin]`, and
//! borrowing value serializers, all through the `daedalus` facade only.

use std::sync::Arc;

use daedalus::{
    DaedalusToValue, DaedalusTypeExpr, adapt,
    data::{
        daedalus_type::DaedalusTypeExpr as _,
        model::{TypeExpr, Value},
        named_types::HostExportPolicy,
        to_value::ToValue,
    },
    engine::{Engine, EngineConfig, EngineError, HostGraph},
    macros::{node, plugin},
    runtime::{NodeError, handler_registry::HandlerRegistry, plugins::PluginRegistry},
    transport::{Payload, TransportError},
    type_key,
};

const FRAME_KEY: &str = "test:typed_host:frame";
const META_KEY: &str = "test:typed_host:meta";

/// Deliberately not `Clone`: payloads share it and the serializer only borrows it.
#[type_key(FRAME_KEY)]
struct Frame {
    pixels: Vec<u8>,
    width: u32,
}

#[derive(Clone, Debug, PartialEq, DaedalusTypeExpr, DaedalusToValue)]
#[daedalus(type_key = META_KEY)]
struct Meta {
    width: u32,
    planes: Vec<Plane>,
}

#[derive(Clone, Debug, PartialEq, DaedalusTypeExpr, DaedalusToValue)]
#[daedalus(type_key = "test:typed_host:plane")]
struct Plane {
    len: u64,
}

impl Frame {
    fn meta(&self) -> Meta {
        Meta {
            width: self.width,
            planes: vec![Plane {
                len: self.pixels.len() as u64,
            }],
        }
    }
}

#[adapt(id = "test.typed_host.frame_to_meta", from = FRAME_KEY, to = META_KEY, kind = "metadata_only")]
fn frame_to_meta(frame: &Frame) -> Result<Meta, TransportError> {
    Ok(frame.meta())
}

#[node(id = "test.typed_host.sum", inputs("frame"), outputs("sum"))]
fn sum(frame: &Frame) -> Result<i64, NodeError> {
    Ok(frame.pixels.iter().map(|&p| i64::from(p)).sum())
}

#[node(id = "test.typed_host.width", inputs("meta"), outputs("width"))]
fn width(meta: &Meta) -> Result<i64, NodeError> {
    Ok(i64::from(meta.width))
}

fn install(registry: &mut PluginRegistry) -> daedalus::runtime::plugins::PluginResult<()> {
    registry.register_value_serializer::<Frame, _>(|frame| frame.meta().to_value());
    Ok(())
}

#[plugin(
    id = "test.typed_host",
    install = install,
    types(Frame),
    values(Meta),
    nodes(sum, width),
    adapters(frame_to_meta)
)]
struct TypedHostPlugin;

fn installed() -> (PluginRegistry, TypedHostPlugin) {
    let mut registry = PluginRegistry::new();
    let plugin = TypedHostPlugin::new();
    registry.install(&plugin).expect("install plugin");
    (registry, plugin)
}

fn fan_out_graph(typed: bool) -> Result<HostGraph<HandlerRegistry>, EngineError> {
    let (registry, plugin) = installed();
    let sum = plugin.sum.alias("sum");
    let width = plugin.width.alias("width");
    let mut builder = registry.graph_builder().expect("graph builder");
    if typed {
        builder = builder.input_typed::<Frame>("frame");
    }
    let graph = builder
        .output_typed::<i64>("width")
        .try_node(&sum)
        .and_then(|b| b.try_node(&width))
        .and_then(|b| b.try_connect("frame", &sum.inputs.frame))
        .and_then(|b| b.try_connect("frame", &width.inputs.meta))
        .and_then(|b| b.try_connect(&sum.outputs.sum, "sum"))
        .and_then(|b| b.try_connect(&width.outputs.width, "width"))
        .expect("wire graph")
        .build();
    Engine::new(EngineConfig::default())?.compile_registry(&registry, graph)
}

#[test]
fn typed_host_input_fans_out_to_different_types_through_an_adapter() {
    let mut host = fan_out_graph(true).expect("compile typed graph");

    let inputs = host.host_inputs();
    assert_eq!(inputs[0].type_expr, Some(TypeExpr::opaque(FRAME_KEY)));
    let adapted: Vec<_> = host
        .explain_plan()
        .edges
        .into_iter()
        .filter(|edge| !edge.adapter_steps.is_empty())
        .map(|edge| (edge.to_port, edge.adapter_steps))
        .collect();
    assert_eq!(adapted.len(), 1, "{adapted:?}");
    assert_eq!(adapted[0].0, "meta");
    assert_eq!(adapted[0].1[0].as_str(), "test.typed_host.frame_to_meta");

    let frame = Frame {
        pixels: vec![1, 2, 3],
        width: 3,
    };
    host.push_payload("frame", Payload::shared(FRAME_KEY, Arc::new(frame)));
    host.tick().expect("tick");
    assert_eq!(host.take::<i64>("sum"), Some(6));
    assert_eq!(host.take::<i64>("width"), Some(3));
}

#[test]
fn generic_host_input_cannot_fan_out_to_different_types() {
    let err = fan_out_graph(false)
        .err()
        .expect("generic host input must conflict");
    assert!(err.to_string().contains("conflicting types"), "{err}");
}

#[test]
fn plugin_values_register_derived_types_with_nested_types_first() {
    let (registry, _) = installed();
    let named = &registry.named_type_registry;

    let plane = named
        .lookup(Plane::TYPE_KEY)
        .expect("nested type registered");
    assert_eq!(plane.export, HostExportPolicy::Value);
    let meta = named.lookup(META_KEY).expect("value type registered");
    assert_eq!(meta.export, HostExportPolicy::Value);
    let TypeExpr::Struct(fields) = meta.expr else {
        panic!("struct schema expected: {:?}", meta.expr);
    };
    let planes = fields.iter().find(|field| field.name == "planes");
    assert_eq!(
        planes.map(|field| &field.ty),
        Some(&TypeExpr::list(TypeExpr::opaque(Plane::TYPE_KEY)))
    );

    let frame = named.lookup(FRAME_KEY).expect("type_key type registered");
    assert_eq!(frame.export, HostExportPolicy::None);
}

#[test]
fn value_serializer_borrows_non_clone_types() {
    let host = fan_out_graph(true).expect("compile typed graph");
    let frame = Frame {
        pixels: vec![0; 4],
        width: 2,
    };
    let inspected = host.inspect_payload(&Payload::shared(FRAME_KEY, Arc::new(frame)));
    let Some(Value::Struct(fields)) = inspected.value() else {
        panic!("serialized value expected: {inspected:?}");
    };
    let width = fields.iter().find(|field| field.name == "width");
    assert_eq!(width.map(|field| &field.value), Some(&Value::Int(2)));
}
