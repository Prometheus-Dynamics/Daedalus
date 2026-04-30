use daedalus::{
    engine::{Engine, EngineConfig, GpuBackend, MetricsLevel},
    macros::{node, plugin},
    runtime::{NodeError, plugins::PluginRegistry},
};

#[node(id = "debug.add", inputs("value"), outputs("value"))]
fn add(value: &i64) -> Result<i64, NodeError> {
    Ok(*value + 1)
}

#[plugin(id = "example.debug", nodes(add))]
struct DebugPlugin;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut registry = PluginRegistry::new();
    let plugin = DebugPlugin::new();
    registry.install(&plugin)?;
    let add = plugin.add.alias("add");
    let graph = registry
        .graph_builder()?
        .try_single_node_io(&add, "in", "value", "value", "out")?
        .build();
    let engine = Engine::new(
        EngineConfig::from(GpuBackend::Cpu).with_metrics_level(MetricsLevel::Detailed),
    )?;
    let mut runtime = engine.compile_registry(&registry, graph)?;
    runtime.push("in", 10_i64);
    let telemetry = runtime.tick()?;
    println!("out: {:?}", runtime.drain_owned::<i64>("out")?);
    println!("{:#?}", runtime.explain_plan());
    println!("{}", telemetry.compact_snapshot());
    Ok(())
}
