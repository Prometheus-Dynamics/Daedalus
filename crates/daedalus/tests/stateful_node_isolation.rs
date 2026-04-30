use daedalus::{
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

#[plugin(id = "test.stateful_node_isolation", nodes(stateful_counter))]
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
