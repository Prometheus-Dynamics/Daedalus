use daedalus::{
    data::model::{TypeExpr, Value, ValueType},
    engine::{Engine, EngineConfig, HostGraph, HostPortDirection},
    macros::{node, plugin},
    runtime::{NodeError, handler_registry::HandlerRegistry, plugins::PluginRegistry},
};

#[node(id = "test.scale", inputs("value"), outputs("scaled"))]
fn scale(value: i64) -> Result<f64, NodeError> {
    Ok(value as f64 * 0.5)
}

#[plugin(id = "test.host_graph_introspection", nodes(scale))]
struct IntrospectionPlugin;

fn compile() -> HostGraph<HandlerRegistry> {
    let mut registry = PluginRegistry::new();
    let plugin = IntrospectionPlugin::new();
    registry.install(&plugin).expect("install plugin");

    let scale = plugin.scale.alias("scaler");
    let graph = registry
        .graph_builder()
        .expect("graph builder")
        .try_node(&scale)
        .expect("scale node")
        .try_connect("frame_in", &scale.inputs.value)
        .expect("input edge")
        .try_connect(&scale.outputs.scaled, "scaled_out")
        .expect("output edge")
        .build();

    Engine::new(EngineConfig::default())
        .expect("engine")
        .compile_registry(&registry, graph)
        .expect("compile graph")
}

#[test]
fn generic_host_bridge_ports_resolve_types_from_plan() {
    let graph = compile();

    let inputs = graph.host_inputs();
    assert_eq!(inputs.len(), 1, "{inputs:?}");
    assert_eq!(inputs[0].name(), "frame_in");
    assert_eq!(inputs[0].direction, HostPortDirection::Input);
    assert_eq!(
        inputs[0].type_expr,
        Some(TypeExpr::Scalar(ValueType::Int)),
        "{inputs:?}"
    );
    assert!(inputs[0].type_key.is_some());
    assert_eq!(
        inputs[0].connections[0].node_label.as_deref(),
        Some("scaler")
    );
    assert_eq!(inputs[0].connections[0].port.as_str(), "value");

    let outputs = graph.host_outputs();
    assert_eq!(outputs.len(), 1, "{outputs:?}");
    assert_eq!(outputs[0].name(), "scaled_out");
    assert_eq!(
        outputs[0].type_expr,
        Some(TypeExpr::Scalar(ValueType::Float)),
        "{outputs:?}"
    );
}

#[test]
fn host_outputs_inspect_through_builtin_serializers() {
    let mut graph = compile();
    graph.push("frame_in", 3_i64);
    graph.tick_until_idle().expect("tick");
    let payloads = graph.drain_payloads("scaled_out");
    assert_eq!(payloads.len(), 1);
    let inspection = graph.inspect_payload(&payloads[0]);
    assert_eq!(inspection.value(), Some(&Value::Float(1.5)));
    assert_eq!(inspection.to_json(), serde_json::json!(1.5));
}
