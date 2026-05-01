use daedalus::prelude::*;

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
    let graph = registry
        .graph_builder()?
        .try_single_node_roundtrip(
            increment.clone(),
            "in",
            &increment.inputs.value,
            &increment.outputs.value,
            "out",
        )?
        .build();

    let engine = Engine::new(EngineConfig::default())?;
    let mut graph = engine.compile_registry(&registry, graph)?;

    println!(
        "out={:?}",
        graph.run_direct_once::<_, i64>("in", "out", 41_i64)?
    );
    Ok(())
}
