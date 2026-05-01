use std::sync::Arc;

use daedalus::{
    engine::{Engine, EngineConfig, GpuBackend, MetricsLevel},
    macros::{node, plugin},
    runtime::{NodeError, plugins::PluginRegistry},
    type_key,
};

#[type_key("example:bytes")]
#[derive(Clone)]
struct Bytes(Arc<Vec<u8>>);

#[node(id = "zero_copy.pass_a", inputs("bytes"), outputs("bytes"))]
fn pass_a(bytes: &Bytes) -> Result<Bytes, NodeError> {
    Ok(bytes.clone())
}

#[node(id = "zero_copy.pass_b", inputs("bytes"), outputs("bytes"))]
fn pass_b(bytes: &Bytes) -> Result<Bytes, NodeError> {
    Ok(bytes.clone())
}

#[node(id = "zero_copy.len", inputs("bytes"), outputs("len"))]
fn len(bytes: &Bytes) -> Result<usize, NodeError> {
    Ok(bytes.0.len())
}

#[plugin(id = "example.zero_copy", types(Bytes), nodes(pass_a, pass_b, len))]
struct ZeroCopyPlugin;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut registry = PluginRegistry::new();
    let plugin = ZeroCopyPlugin::new();
    registry.install(&plugin)?;
    let pass_a = plugin.pass_a.alias("pass_a");
    let pass_b = plugin.pass_b.alias("pass_b");
    let len = plugin.len.alias("len");
    let graph = registry
        .graph_builder()?
        .inputs(|g| {
            g.input("in");
        })
        .outputs(|g| {
            g.output("out");
        })
        .nodes(|g| {
            g.add_handle(&pass_a);
            g.add_handle(&pass_b);
            g.add_handle(&len);
        })
        .try_edges(|g| {
            let pass_a = g.node("pass_a");
            let pass_b = g.node("pass_b");
            let len = g.node("len");
            g.try_connect("in", &pass_a.input("bytes"))?;
            g.try_connect(&pass_a.output("bytes"), &pass_b.input("bytes"))?;
            g.try_connect(&pass_b.output("bytes"), &len.input("bytes"))?;
            g.try_connect(&len.output("len"), "out")?;
            Ok(())
        })?
        .build();

    let engine = Engine::new(
        EngineConfig::from(GpuBackend::Cpu).with_metrics_level(MetricsLevel::Detailed),
    )?;
    let mut runtime = engine.compile_registry(&registry, graph)?;
    runtime.push("in", Bytes(Arc::new(vec![1, 2, 3, 4])));
    let telemetry = runtime.tick_until_idle()?.unwrap_or_default();
    let plan = runtime.explain_plan();
    println!("out: {:?}", runtime.take::<usize>("out"));
    for edge in &plan.edges {
        println!(
            "edge {} {} -> {} handoff={:?} reason={}",
            edge.index, edge.from_port, edge.to_port, edge.handoff, edge.handoff_reason
        );
    }
    println!("{}", telemetry.compact_snapshot());
    Ok(())
}
