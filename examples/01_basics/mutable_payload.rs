use daedalus::{
    engine::{Engine, EngineConfig, GpuBackend, MetricsLevel},
    macros::{node, plugin},
    runtime::{NodeError, plugins::PluginRegistry},
    type_key,
};

#[type_key("example:mutable_bytes")]
#[derive(Clone, Debug)]
struct MutableBytes(Vec<u8>);

#[node(id = "mutable.append", inputs("bytes"), outputs("bytes"))]
fn append(bytes: &MutableBytes) -> Result<MutableBytes, NodeError> {
    let mut next = bytes.clone();
    next.0.push(4);
    Ok(next)
}

#[node(id = "mutable.reverse", inputs("bytes"), outputs("bytes"))]
fn reverse(bytes: &MutableBytes) -> Result<MutableBytes, NodeError> {
    let mut next = bytes.clone();
    next.0.reverse();
    Ok(next)
}

#[plugin(id = "example.mutable", types(MutableBytes), nodes(append, reverse))]
struct MutablePlugin;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut registry = PluginRegistry::new();
    let plugin = MutablePlugin::new();
    registry.install(&plugin)?;

    let append = plugin.append.alias("append");
    let reverse = plugin.reverse.alias("reverse");
    let graph = registry
        .graph_builder()?
        .inputs(|g| {
            g.input("in");
        })
        .outputs(|g| {
            g.output("out");
        })
        .nodes(|g| {
            g.add_handle(&append);
            g.add_handle(&reverse);
        })
        .try_edges(|g| {
            let append = g.node("append");
            let reverse = g.node("reverse");
            g.try_connect("in", &append.input("bytes"))?;
            g.try_connect(&append.output("bytes"), &reverse.input("bytes"))?;
            g.try_connect(&reverse.output("bytes"), "out")?;
            Ok(())
        })?
        .build();

    let engine = Engine::new(
        EngineConfig::from(GpuBackend::Cpu).with_metrics_level(MetricsLevel::Detailed),
    )?;
    let mut runtime = engine.compile_registry(&registry, graph)?;
    runtime.push("in", MutableBytes(vec![1, 2, 3]));
    let telemetry = runtime.tick_until_idle()?.unwrap_or_default();
    println!("mutable output: {:?}", runtime.take::<MutableBytes>("out"));
    println!("{}", telemetry.compact_snapshot());
    Ok(())
}
