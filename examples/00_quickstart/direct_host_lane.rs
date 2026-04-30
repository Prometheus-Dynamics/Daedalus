use daedalus::{
    engine::{Engine, EngineConfig},
    macros::{node, plugin},
    runtime::{NodeError, plugins::PluginRegistry},
};

#[node(id = "quickstart.increment", inputs("value"), outputs("value"))]
fn increment(value: i64) -> Result<i64, NodeError> {
    Ok(value + 1)
}

#[plugin(id = "quickstart.direct_host_lane", nodes(increment))]
struct QuickstartPlugin;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut registry = PluginRegistry::new();
    let plugin = QuickstartPlugin::new();
    registry.install(&plugin)?;

    let increment = plugin.increment.alias("increment");
    let builder = registry.graph_builder()?;
    let input = builder.input("in");
    let output = builder.output("out");
    let graph = builder
        .try_node_handle_like(&increment)?
        .try_connect(&input, &increment.inputs.value)?
        .try_connect(&increment.outputs.value, &output)?
        .build();

    let engine = Engine::new(EngineConfig::default())?;
    let mut graph = engine.compile_registry(&registry, graph)?;

    println!(
        "out={:?}",
        graph.run_direct_once::<_, i64>("in", "out", 41_i64)?
    );
    Ok(())
}
