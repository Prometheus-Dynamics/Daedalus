use daedalus::{
    engine::{Engine, EngineConfig},
    macros::{node, plugin},
    runtime::{NodeError, plugins::PluginRegistry},
};

#[node(id = "quickstart.strict.double", inputs("value"), outputs("value"))]
fn double(value: i64) -> Result<i64, NodeError> {
    Ok(value * 2)
}

#[plugin(id = "quickstart.strict_builder_roundtrip", nodes(double))]
struct QuickstartPlugin;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut registry = PluginRegistry::new();
    let plugin = QuickstartPlugin::new();
    registry.install(&plugin)?;

    let double = plugin.double.alias("double");
    let builder = registry.graph_builder()?;
    let input = builder.input("in");
    let output = builder.output("out");
    let graph = builder
        .try_node_handle_like(&double)?
        .try_connect(&input, &double.inputs.value)?
        .try_connect(&double.outputs.value, &output)?
        .build();

    let engine = Engine::new(EngineConfig::default())?;
    let mut runtime = engine.compile_registry(&registry, graph)?;

    runtime.push("in", 21_i64);
    runtime.tick()?;
    println!("out={:?}", runtime.drain_owned::<i64>("out")?);
    Ok(())
}
