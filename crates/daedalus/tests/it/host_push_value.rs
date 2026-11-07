//! `HostGraph::push_value`: a host pushes a `Value` into a keyed input and the registry converts
//! it to the type the port declares, through the type's const coercer, the way a graph constant
//! reaches a typed node input. The host needs no Rust type for the value.

use daedalus::{
    DaedalusToValue, DaedalusTypeExpr,
    data::model::{StructFieldValue, TypeExpr, Value, ValueType},
    engine::{Engine, EngineConfig, HostGraph},
    macros::{node, plugin},
    runtime::{NodeError, handler_registry::HandlerRegistry, plugins::PluginRegistry},
    transport::{FeedOutcome, TypeKeyError},
};

const CALIB_KEY: &str = "test:push_value:calib";

/// Coerced from a `Value` through serde, as a struct constant is.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, DaedalusTypeExpr, DaedalusToValue)]
#[daedalus(type_key = CALIB_KEY)]
struct Calib {
    width: i64,
    scale: i64,
}

#[node(id = "test.push_value.area", inputs("calib"), outputs("area"))]
fn area(calib: &Calib) -> Result<i64, NodeError> {
    Ok(calib.width * calib.scale)
}

#[plugin(id = "test.push_value", values(Calib), nodes(area))]
struct PushValuePlugin;

fn graph() -> HostGraph<HandlerRegistry> {
    let mut registry = PluginRegistry::new();
    let plugin = PushValuePlugin::new();
    registry.install(&plugin).expect("install plugin");
    let area = plugin.area.alias("area");
    let builder = registry
        .graph_builder()
        .expect("graph builder")
        .input_as("calib", TypeExpr::opaque(CALIB_KEY))
        .output_as("area", TypeExpr::Scalar(ValueType::Int))
        .try_node(&area)
        .and_then(|b| b.try_connect("calib", &area.inputs.calib))
        .and_then(|b| b.try_connect(&area.outputs.area, "area"))
        .expect("wire graph")
        .build();
    Engine::new(EngineConfig::default())
        .expect("engine")
        .compile_registry(&registry, builder)
        .expect("compile")
}

fn field(name: &str, value: i64) -> StructFieldValue {
    StructFieldValue {
        name: name.into(),
        value: Value::Int(value),
    }
}

#[test]
fn a_value_is_coerced_to_the_port_type_and_reaches_the_node_typed() {
    let mut host = graph();
    let input = host
        .host_inputs()
        .into_iter()
        .find(|port| port.name() == "calib")
        .expect("calib input");
    assert_eq!(
        input.type_key.as_ref().map(|key| key.as_str()),
        Some(CALIB_KEY)
    );

    let shaped = Value::Struct(vec![field("width", 3), field("scale", 4)]);
    let outcome = host.push_value("calib", shaped);
    assert!(
        matches!(outcome, FeedOutcome::Accepted { .. }),
        "{outcome:?}"
    );
    host.tick().expect("tick");
    assert_eq!(host.take::<i64>("area"), Some(12));
}

#[test]
fn a_wrong_shaped_value_is_refused_at_push_with_the_port_and_key() {
    let mut host = graph();
    let outcome = host.push_value("calib", Value::Int(3));
    let error = match outcome {
        FeedOutcome::Rejected(error) => *error,
        other => panic!("expected a rejection, got {other:?}"),
    };
    assert!(
        matches!(
            error,
            TypeKeyError::ValueRejected { ref port, ref type_key, .. }
                if port == "calib" && type_key.as_str() == CALIB_KEY
        ),
        "{error}"
    );
    host.tick().expect("tick");
    assert_eq!(host.take::<i64>("area"), None, "nothing reached the node");
}

#[test]
fn a_struct_missing_a_field_is_refused() {
    let host = graph();
    let partial = Value::Struct(vec![field("width", 3)]);
    assert!(!matches!(
        host.push_value("calib", partial),
        FeedOutcome::Accepted { .. }
    ));
}
