use daedalus::{
    engine::{Engine, EngineConfig, GpuBackend, MetricsLevel},
    macros::{node, plugin},
    runtime::{NodeError, plugins::PluginRegistry},
};

#[node(id = "shared_fanout.add", inputs("value"), outputs("value"))]
fn add(value: &i64) -> Result<i64, NodeError> {
    Ok(*value + 10)
}

#[node(id = "shared_fanout.double", inputs("value"), outputs("value"))]
fn double(value: &i64) -> Result<i64, NodeError> {
    Ok(*value * 2)
}

#[plugin(id = "example.shared_fanout", nodes(add, double))]
struct SharedFanoutPlugin;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut registry = PluginRegistry::new();
    let plugin = SharedFanoutPlugin::new();
    registry.install(&plugin)?;
    let add = plugin.add.alias("add");
    let double = plugin.double.alias("double");
    let graph = registry
        .graph_builder()?
        .inputs(|g| {
            g.input("in");
        })
        .outputs(|g| {
            g.output("add_out");
            g.output("double_out");
        })
        .nodes(|g| {
            g.add_handle(&add);
            g.add_handle(&double);
        })
        .try_edges(|g| {
            let add = g.node("add");
            let double = g.node("double");
            g.try_connect("in", &add.input("value"))?;
            g.try_connect("in", &double.input("value"))?;
            g.try_connect(&add.output("value"), "add_out")?;
            g.try_connect(&double.output("value"), "double_out")?;
            Ok(())
        })?
        .build();

    let engine = Engine::new(
        EngineConfig::from(GpuBackend::Cpu).with_metrics_level(MetricsLevel::Detailed),
    )?;
    let mut runtime = engine.compile_registry(&registry, graph)?;
    runtime.push("in", 7_i64);
    let telemetry = runtime.tick_until_idle()?.unwrap_or_default();
    let plan = runtime.explain_plan();
    println!(
        "outputs: add={:?} double={:?}",
        runtime.take::<i64>("add_out"),
        runtime.take::<i64>("double_out")
    );
    for edge in plan.edges.iter().filter(|edge| edge.from_port == "in") {
        println!(
            "fanout edge {} handoff={:?} reason={}",
            edge.index, edge.handoff, edge.handoff_reason
        );
    }
    println!("{}", telemetry.compact_snapshot());
    Ok(())
}
