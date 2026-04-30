use daedalus::{
    engine::{Engine, EngineConfig, GpuBackend, MetricsLevel},
    macros::{node, plugin},
    runtime::{NodeError, plugins::PluginRegistry},
};

#[node(id = "metrics.scale", inputs("value"), outputs("value"))]
fn scale(value: &i64) -> Result<i64, NodeError> {
    Ok(*value * 3)
}

#[node(id = "metrics.offset", inputs("value"), outputs("value"))]
fn offset(value: &i64) -> Result<i64, NodeError> {
    Ok(*value + 7)
}

#[plugin(id = "example.metrics.runtime", nodes(scale, offset))]
struct RuntimeMetricsPlugin;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut registry = PluginRegistry::new();
    let plugin = RuntimeMetricsPlugin::new();
    registry.install(&plugin)?;

    let scale = plugin.scale.alias("scale");
    let offset = plugin.offset.alias("offset");
    let graph = registry
        .graph_builder()?
        .inputs(|g| {
            g.input("in");
        })
        .outputs(|g| {
            g.output("out");
        })
        .nodes(|g| {
            g.add_handle(&scale);
            g.add_handle(&offset);
        })
        .try_edges(|g| {
            let scale = g.node("scale");
            let offset = g.node("offset");
            g.try_connect("in", &scale.input("value"))?;
            g.try_connect(&scale.output("value"), &offset.input("value"))?;
            g.try_connect(&offset.output("value"), "out")?;
            Ok(())
        })?
        .build();

    let engine = Engine::new(
        EngineConfig::from(GpuBackend::Cpu).with_metrics_level(MetricsLevel::Detailed),
    )?;
    let mut runtime = engine.compile_registry(&registry, graph)?;
    runtime.push("in", 5_i64);
    let telemetry = runtime.tick()?;
    println!("out: {:?}", runtime.drain_owned::<i64>("out")?);
    println!("{}", telemetry.report().to_table());
    println!("{}", telemetry.compact_snapshot());
    Ok(())
}
