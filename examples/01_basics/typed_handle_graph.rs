use daedalus::{
    engine::{Engine, EngineConfig},
    macros::{node, plugin},
    runtime::{NodeError, plugins::PluginRegistry},
};

#[node(id = "basics.inc", inputs("value"), outputs("value"))]
fn inc(value: i64) -> Result<i64, NodeError> {
    Ok(value + 1)
}

#[plugin(id = "basics.typed_handle_graph", nodes(inc))]
struct TypedHandlePlugin;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut registry = PluginRegistry::new();
    let plugin = TypedHandlePlugin::new();
    registry.install(&plugin)?;

    let inc = plugin.inc.alias("inc");
    let builder = registry.graph_builder()?;
    let input = builder.input("in");
    let output = builder.output("out");
    let graph = builder
        .node_handle_like(&inc)
        .connect(&input, &inc.inputs.value)
        .connect(&inc.outputs.value, &output)
        .build();

    let engine = Engine::new(EngineConfig::default())?;
    let mut graph = engine.compile_registry(&registry, graph)?;
    println!(
        "out={:?}",
        graph.run_direct_once::<_, i64>("in", "out", 41_i64)?
    );

    Ok(())
}
