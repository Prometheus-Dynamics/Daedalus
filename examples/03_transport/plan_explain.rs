use daedalus::{
    engine::{Engine, EngineConfig, GpuBackend, MetricsLevel},
    macros::{node, plugin},
    runtime::{NodeError, plugins::PluginRegistry},
};

#[node(id = "plan.add", inputs("value"), outputs("value"))]
fn add(value: i64) -> Result<i64, NodeError> {
    Ok(value + 1)
}

#[plugin(id = "example.plan", nodes(add))]
struct PlanPlugin;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut registry = PluginRegistry::new();
    let plugin = PlanPlugin::new();
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
    runtime.push("in", 4_i64);
    let telemetry = runtime.tick_until_idle()?.unwrap_or_default();
    println!("out: {:?}", runtime.take::<i64>("out"));
    println!("{:#?}", runtime.explain_plan());
    println!("{}", telemetry.compact_snapshot());
    Ok(())
}
