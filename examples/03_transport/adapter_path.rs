use daedalus::{
    adapt,
    engine::{Engine, EngineConfig, GpuBackend, MetricsLevel},
    macros::{node, plugin},
    runtime::{NodeError, plugins::PluginRegistry},
    transport::TransportError,
    type_key,
};

#[type_key("example:count")]
#[derive(Clone)]
struct Count(i64);

#[type_key("example:count_label")]
#[derive(Clone)]
struct CountLabel(String);

#[adapt(id = "example.count_to_label", kind = daedalus::transport::AdapterKind::Materialize)]
fn count_to_label(value: &Count) -> Result<CountLabel, TransportError> {
    Ok(CountLabel(format!("count={}", value.0)))
}

#[node(id = "adapter.bump", inputs("count"), outputs("count"))]
fn bump(count: &Count) -> Result<Count, NodeError> {
    Ok(Count(count.0 + 1))
}

#[node(id = "adapter.render", inputs("label"), outputs("text"))]
fn render(label: &CountLabel) -> Result<String, NodeError> {
    Ok(label.0.clone())
}

#[plugin(
    id = "example.adapter_path",
    types(Count, CountLabel),
    nodes(bump, render),
    adapters(count_to_label)
)]
struct AdapterPathPlugin;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut registry = PluginRegistry::new();
    let plugin = AdapterPathPlugin::new();
    registry.install(&plugin)?;
    let bump = plugin.bump.alias("bump");
    let render = plugin.render.alias("render");
    let graph = registry
        .graph_builder()?
        .inputs(|g| {
            g.input("in");
        })
        .outputs(|g| {
            g.output("out");
        })
        .nodes(|g| {
            g.add_handle(&bump);
            g.add_handle(&render);
        })
        .try_edges(|g| {
            let bump = g.node("bump");
            let render = g.node("render");
            g.try_connect("in", &bump.input("count"))?;
            g.try_connect(&bump.output("count"), &render.input("label"))?;
            g.try_connect(&render.output("text"), "out")?;
            Ok(())
        })?
        .build();

    let engine = Engine::new(
        EngineConfig::from(GpuBackend::Cpu).with_metrics_level(MetricsLevel::Detailed),
    )?;
    let mut runtime = engine.compile_registry(&registry, graph)?;
    runtime.push("in", Count(4));
    let telemetry = runtime.tick_until_idle()?.unwrap_or_default();
    println!("out: {:?}", runtime.take::<String>("out"));
    println!("{}", runtime.explain_plan().edges[1].handoff_reason);
    println!("{}", telemetry.compact_snapshot());
    Ok(())
}
