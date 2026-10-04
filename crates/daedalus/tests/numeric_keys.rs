//! Builtin numbers have one transport key per Rust type (`i32` and `i64` no longer share
//! `Int`): the planner widens losslessly on its own (`i32 -> i64`), never narrows implicitly,
//! and graph constants convert to the port's exact width, range-checked at plan time.

use daedalus::{
    data::model::Value,
    engine::{Engine, EngineConfig, EngineError, HostGraph},
    macros::{node, plugin},
    runtime::{NodeError, handler_registry::HandlerRegistry, plugins::PluginRegistry},
    transport::FeedOutcome,
};

#[node(id = "wide.double", inputs("x"), outputs("out"))]
fn double(x: i64) -> Result<i64, NodeError> {
    Ok(x * 2)
}

#[node(id = "wide.negate", inputs("x"), outputs("out"))]
fn negate(x: &i64) -> Result<i64, NodeError> {
    Ok(-x)
}

#[node(id = "wide.scale", inputs("x", "factor", "gain"), outputs("out"))]
fn scale(x: i64, factor: u8, gain: f64) -> Result<f64, NodeError> {
    Ok((x * i64::from(factor)) as f64 * gain)
}

#[plugin(id = "wide", nodes(double, negate, scale))]
struct WidePlugin;

#[node(id = "narrow.inc", inputs("x"), outputs("out"))]
fn inc(x: i32) -> Result<i32, NodeError> {
    Ok(x + 1)
}

#[plugin(id = "narrow", nodes(inc))]
struct NarrowPlugin;

/// Both plugins in one registry: before, their `i64` and `i32` ports shared the `Int` key.
fn installed() -> (PluginRegistry, WidePlugin, NarrowPlugin) {
    let mut registry = PluginRegistry::new();
    let (wide, narrow) = (WidePlugin::new(), NarrowPlugin::new());
    registry.install(&wide).expect("install i64 plugin");
    registry.install(&narrow).expect("install i32 plugin");
    (registry, wide, narrow)
}

fn compile(
    registry: &PluginRegistry,
    graph: daedalus::planner::Graph,
) -> Result<HostGraph<HandlerRegistry>, EngineError> {
    Engine::new(EngineConfig::default())?.compile_registry(registry, graph)
}

fn adapter_steps(host: &HostGraph<HandlerRegistry>) -> Vec<String> {
    let edges = host.explain_plan().edges.into_iter();
    edges
        .flat_map(|edge| edge.adapter_steps)
        .map(|step| step.as_str().to_string())
        .collect()
}

#[test]
fn i64_and_i32_plugins_coexist_with_distinct_keys() {
    installed();
    let wide = DoubleNode::node_decl().expect("i64 node");
    let narrow = IncNode::node_decl().expect("i32 node");
    assert_ne!(wide.inputs[0].type_key, narrow.inputs[0].type_key);
}

#[test]
fn i32_output_widens_into_an_i64_input() {
    let (registry, wide, narrow) = installed();
    let (inc, double) = (narrow.inc.alias("inc"), wide.double.alias("double"));
    let graph = registry
        .graph_builder()
        .expect("graph builder")
        .input_typed::<i32>("x")
        .and_then(|b| b.try_node(&inc))
        .and_then(|b| b.try_node(&double))
        .and_then(|b| b.try_connect("x", &inc.inputs.x))
        .and_then(|b| b.try_connect(&inc.outputs.out, &double.inputs.x))
        .and_then(|b| b.try_connect(&double.outputs.out, "out"))
        .expect("wire graph")
        .build();
    let mut host = compile(&registry, graph).expect("compile");
    assert_eq!(adapter_steps(&host), ["daedalus.builtin.widen.i32_to_i64"]);
    assert_eq!(host.run_once::<_, i64>(("x", 20_i32), "out").unwrap(), [42]);
}

#[test]
fn i64_host_input_fans_out_to_two_i64_nodes() {
    let (registry, wide, _) = installed();
    let (double, negate) = (wide.double.alias("double"), wide.negate.alias("negate"));
    let graph = registry
        .graph_builder()
        .expect("graph builder")
        .input_typed::<i64>("x")
        .and_then(|b| b.try_node(&double))
        .and_then(|b| b.try_node(&negate))
        .and_then(|b| b.try_connect("x", &double.inputs.x))
        .and_then(|b| b.try_connect("x", &negate.inputs.x))
        .and_then(|b| b.try_connect(&double.outputs.out, "doubled"))
        .and_then(|b| b.try_connect(&negate.outputs.out, "negated"))
        .expect("wire graph")
        .build();
    let mut host = compile(&registry, graph).expect("compile");
    let fed = host.push("x", 21_i64);
    assert!(matches!(fed, FeedOutcome::Accepted { .. }), "{fed:?}");
    host.tick().expect("tick");
    assert_eq!(host.take::<i64>("doubled"), Some(42));
    assert_eq!(host.take::<i64>("negated"), Some(-21));
}

#[test]
fn narrowing_is_rejected_at_plan_time() {
    let (registry, wide, narrow) = installed();
    let (double, inc) = (wide.double.alias("double"), narrow.inc.alias("inc"));
    let graph = registry
        .graph_builder()
        .expect("graph builder")
        .try_node(&double)
        .and_then(|b| b.try_node(&inc))
        .and_then(|b| b.try_connect("x", &double.inputs.x))
        .and_then(|b| b.try_connect(&double.outputs.out, &inc.inputs.x))
        .and_then(|b| b.try_connect(&inc.outputs.out, "out"))
        .expect("wire graph")
        .build();
    let err = compile(&registry, graph).err().expect("i64 -> i32 must fail");
    let message = err.to_string();
    assert!(message.contains("i64 -> i32 is not lossless"), "{message}");
}

fn scale_graph(factor: i64, gain: Value) -> Result<HostGraph<HandlerRegistry>, EngineError> {
    let (registry, wide, _) = installed();
    let scale = wide.scale.alias("scale");
    let graph = registry
        .graph_builder()
        .expect("graph builder")
        .try_node(&scale)
        .and_then(|b| b.try_connect("x", &scale.inputs.x))
        .and_then(|b| b.try_connect(&scale.outputs.out, "out"))
        .expect("wire graph")
        .const_input(&scale.inputs.factor, Some(Value::Int(factor)))
        .const_input(&scale.inputs.gain, Some(gain))
        .build();
    compile(&registry, graph)
}

#[test]
fn constants_take_the_ports_exact_width() {
    // `Value::Int` converts to `u8`, and to `f64` when it represents it exactly.
    let mut host = scale_graph(3, Value::Int(2)).expect("compile");
    assert_eq!(host.run_once::<_, f64>(("x", 7_i64), "out").unwrap(), [42.0]);

    let err = scale_graph(300, Value::Float(1.0))
        .err()
        .expect("300 does not fit u8");
    let message = err.to_string();
    assert!(message.contains("const input `factor`"), "{message}");
    assert!(message.contains("300 is out of range for u8 (0..=255)"), "{message}");

    let err = scale_graph(3, Value::Bool(true)).err().expect("not a number");
    assert!(err.to_string().contains("expected a number for f64"), "{err}");
}
