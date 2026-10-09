use daedalus::{
    data::model::Value,
    engine::{Engine, EngineConfig, HostGraph},
    macros::{node, plugin},
    runtime::{NodeError, handler_registry::HandlerRegistry, plugins::PluginRegistry},
};

#[derive(Default)]
struct CounterState {
    value: i64,
}

#[node(
    id = "test.stateful_counter",
    inputs("step"),
    outputs("value"),
    state(CounterState)
)]
fn stateful_counter(step: i64, state: &mut CounterState) -> Result<i64, NodeError> {
    state.value += step;
    Ok(state.value)
}

/// Three reference parameters: a typed stateful node, not the low-level `(node, ctx, io)` form.
#[node(
    id = "test.weighted_sum",
    inputs("value", "weight"),
    outputs("sum"),
    state(CounterState)
)]
fn weighted_sum(value: &i64, weight: &i64, state: &mut CounterState) -> Result<i64, NodeError> {
    state.value += value * weight;
    Ok(state.value)
}

#[plugin(
    id = "test.stateful_node_isolation",
    nodes(stateful_counter, weighted_sum)
)]
struct StatefulPlugin;

fn compile_counter_graph() -> HostGraph<HandlerRegistry> {
    let mut registry = PluginRegistry::new();
    let plugin = StatefulPlugin::new();
    registry.install(&plugin).expect("install plugin");

    let counter = plugin.stateful_counter.alias("counter");
    let graph = registry
        .graph_builder()
        .expect("graph builder")
        .try_node(&counter)
        .expect("counter node")
        .try_connect("in", &counter.inputs.step)
        .expect("input edge")
        .try_connect(&counter.outputs.value, "out")
        .expect("output edge")
        .build();

    Engine::new(EngineConfig::default())
        .expect("engine")
        .compile_registry(&registry, graph)
        .expect("compile graph")
}

#[test]
fn generated_stateful_node_state_is_executor_local() {
    let mut first = compile_counter_graph();
    assert_eq!(
        first
            .run_direct_once::<_, i64>("in", "out", 1_i64)
            .expect("first tick"),
        Some(1)
    );
    assert_eq!(
        first
            .run_direct_once::<_, i64>("in", "out", 1_i64)
            .expect("second tick"),
        Some(2)
    );

    let mut second = compile_counter_graph();
    assert_eq!(
        second
            .run_direct_once::<_, i64>("in", "out", 1_i64)
            .expect("isolated tick"),
        Some(1)
    );
}

#[test]
fn three_reference_parameters_are_a_typed_node() {
    let mut registry = PluginRegistry::new();
    let plugin = StatefulPlugin::new();
    registry.install(&plugin).expect("install plugin");
    let sum = plugin.weighted_sum.alias("sum");
    let graph = registry
        .graph_builder()
        .expect("graph builder")
        .try_node(&sum)
        .expect("sum node")
        .try_connect("in", &sum.inputs.value)
        .expect("input edge")
        .try_connect(&sum.outputs.sum, "out")
        .expect("output edge")
        .const_input(&sum.inputs.weight, Some(Value::Int(3)))
        .build();
    let mut host = Engine::new(EngineConfig::default())
        .expect("engine")
        .compile_registry(&registry, graph)
        .expect("compile graph");
    for expected in [6, 12] {
        let out = host.run_once::<_, i64>(("in", 2_i64), "out");
        assert_eq!(out.expect("tick"), [expected]);
    }
    // The direct path delivers the const `weight` too.
    let out = host.run_direct_once::<_, i64>("in", "out", 2_i64);
    assert_eq!(out.expect("direct tick"), Some(18));
}
