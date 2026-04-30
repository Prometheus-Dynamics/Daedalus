use daedalus::{
    adapt,
    engine::{Engine, EngineConfig, GpuBackend, MetricsLevel},
    macros::{node, plugin},
    runtime::{NodeError, plugins::PluginRegistry},
    transport::{TransportError, TypeKey},
    type_key,
};

#[type_key("example:byte_block")]
#[derive(Clone)]
struct ByteBlock(Vec<u8>);

#[type_key("example:byte_summary")]
#[derive(Clone)]
struct ByteSummary {
    sum: usize,
}

#[adapt(id = "metrics.bytes_to_summary", kind = daedalus::transport::AdapterKind::Materialize)]
fn bytes_to_summary(value: &ByteBlock) -> Result<ByteSummary, TransportError> {
    Ok(ByteSummary {
        sum: value.0.iter().map(|byte| *byte as usize).sum(),
    })
}

#[node(id = "metrics.bytes_passthrough", inputs("value"), outputs("value"))]
fn bytes_passthrough(value: &ByteBlock) -> Result<ByteBlock, NodeError> {
    Ok(value.clone())
}

#[node(id = "metrics.right", inputs("value"), outputs("value"))]
fn right(value: &ByteSummary) -> Result<usize, NodeError> {
    Ok(value.sum)
}

#[plugin(
    id = "example.metrics.transport",
    types(ByteBlock, ByteSummary),
    nodes(bytes_passthrough, right),
    adapters(bytes_to_summary)
)]
struct TransportMetricsPlugin;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut registry = PluginRegistry::new();
    let plugin = TransportMetricsPlugin::new();
    registry.install(&plugin)?;

    let bytes_passthrough = plugin.bytes_passthrough.alias("bytes_passthrough");
    let right = plugin.right.alias("right");
    let graph = registry
        .graph_builder()?
        .inputs(|g| {
            g.input("bytes");
        })
        .outputs(|g| {
            g.output("sum");
        })
        .nodes(|g| {
            g.add_handle(&bytes_passthrough);
            g.add_handle(&right);
        })
        .try_edges(|g| {
            let bytes_passthrough = g.node("bytes_passthrough");
            let right = g.node("right");
            g.try_connect("bytes", &bytes_passthrough.input("value"))?;
            g.try_connect(&bytes_passthrough.output("value"), &right.input("value"))?;
            g.try_connect(&right.output("value"), "sum")?;
            Ok(())
        })?
        .build();

    let engine =
        Engine::new(EngineConfig::from(GpuBackend::Cpu).with_metrics_level(MetricsLevel::Trace))?;
    let mut runtime = engine.compile_registry(&registry, graph)?;
    runtime.push_as(
        "bytes",
        TypeKey::new("example:byte_block"),
        ByteBlock(vec![1_u8, 2, 3, 4, 5]),
    );
    let telemetry = runtime.tick()?;
    println!("output sum={:?}", runtime.drain_owned::<usize>("sum")?);
    let report = telemetry.report();
    println!("adapter paths:");
    for path in &report.adapter_paths {
        println!(
            "  edge={:?} node={:?} port={:?} correlation={} steps={:?} detail={:?}",
            path.edge, path.node, path.port, path.correlation_id, path.steps, path.detail
        );
    }
    println!("{}", report.to_json()?);
    Ok(())
}
