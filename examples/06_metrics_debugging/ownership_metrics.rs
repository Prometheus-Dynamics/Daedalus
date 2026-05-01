use daedalus::{
    engine::{Engine, EngineConfig, GpuBackend, MetricsLevel},
    macros::{node, plugin},
    runtime::{NodeError, TelemetryReportFilter, plugins::PluginRegistry},
};

#[derive(Clone)]
struct ByteBlock(Vec<u8>);

#[node(id = "metrics.preview", inputs("value"), outputs("value"))]
fn preview(value: &ByteBlock) -> Result<usize, NodeError> {
    Ok(value.0.first().copied().unwrap_or_default() as usize)
}

#[node(id = "metrics.archive", inputs("value"), outputs("value"))]
fn archive(value: &ByteBlock) -> Result<usize, NodeError> {
    Ok(value.0.len())
}

#[plugin(id = "example.metrics.ownership", nodes(preview, archive))]
struct OwnershipMetricsPlugin;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut registry = PluginRegistry::new();
    let plugin = OwnershipMetricsPlugin::new();
    registry.install(&plugin)?;

    let preview = plugin.preview.alias("preview");
    let archive = plugin.archive.alias("archive");
    let graph = registry
        .graph_builder()?
        .inputs(|g| {
            g.input("bytes");
        })
        .outputs(|g| {
            g.output("preview");
            g.output("archive");
        })
        .nodes(|g| {
            g.add_handle(&preview);
            g.add_handle(&archive);
        })
        .try_edges(|g| {
            let preview = g.node("preview");
            let archive = g.node("archive");
            g.try_connect("bytes", &preview.input("value"))?;
            g.try_connect("bytes", &archive.input("value"))?;
            g.try_connect(&preview.output("value"), "preview")?;
            g.try_connect(&archive.output("value"), "archive")?;
            Ok(())
        })?
        .build();

    let engine = Engine::new(
        EngineConfig::from(GpuBackend::Cpu).with_metrics_level(MetricsLevel::Detailed),
    )?;
    let mut runtime = engine.compile_registry(&registry, graph)?;
    runtime.push("bytes", ByteBlock(vec![9_u8; 1024]));
    let telemetry = runtime.tick()?;
    println!(
        "outputs preview={:?} archive={:?}",
        runtime.drain_owned::<usize>("preview")?,
        runtime.drain_owned::<usize>("archive")?
    );
    let report = telemetry.report();
    println!("all ownership: {:#?}", report.ownership);
    println!(
        "edge 0 ownership: {:#?}",
        report
            .clone()
            .filter(&TelemetryReportFilter {
                edge: Some(0),
                ..TelemetryReportFilter::default()
            })
            .ownership
    );
    println!("{}", telemetry.compact_snapshot());
    Ok(())
}
