use daedalus::prelude::*;

#[node(id = "quickstart.double", inputs("value"), outputs("value"))]
fn double(value: i64) -> Result<i64, NodeError> {
    Ok(value * 2)
}

#[plugin(id = "quickstart.typed_cpu_graph", nodes(double))]
struct QuickstartPlugin;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut registry = PluginRegistry::new();
    let plugin = QuickstartPlugin::new();
    registry.install(&plugin)?;

    let double = plugin.double.alias("double");
    let graph = registry
        .graph_builder()?
        .try_single_node_roundtrip(
            double.clone(),
            "in",
            &double.inputs.value,
            &double.outputs.value,
            "out",
        )?
        .build();

    let engine = Engine::new(EngineConfig::default())?;
    let mut graph = engine.compile_registry(&registry, graph)?;

    println!(
        "out={:?}",
        graph.run_direct_once::<_, i64>("in", "out", 21_i64)?
    );
    Ok(())
}
