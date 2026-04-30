use daedalus::{
    engine::{Engine, EngineConfig, MetricsLevel},
    macros::{node, plugin},
    runtime::{ExecutionTelemetry, NodeError, plugins::PluginRegistry},
};

#[node(id = "metrics_levels.square", inputs("value"), outputs("value"))]
fn square(value: i64) -> Result<i64, NodeError> {
    Ok(value * value)
}

#[plugin(id = "example.metrics_levels", nodes(square))]
struct MetricsPlugin;

fn print_level_summary(level: MetricsLevel, telemetry: &ExecutionTelemetry) {
    let node_histograms = telemetry
        .node_metrics
        .values()
        .filter(|node| node.duration_histogram.is_some())
        .count();
    let node_transport = telemetry
        .node_metrics
        .values()
        .filter(|node| node.transport.is_some())
        .count();
    let edge_histograms = telemetry
        .edge_metrics
        .values()
        .filter(|edge| {
            edge.wait_histogram.is_some()
                || edge.transport_apply_histogram.is_some()
                || edge.adapter_histogram.is_some()
                || edge.depth_histogram.is_some()
        })
        .count();
    let hardware_samples = telemetry
        .node_metrics
        .values()
        .filter(|node| node.perf.is_some())
        .count();
    let trace_events = telemetry.trace.as_ref().map_or(0, Vec::len);
    println!(
        "{level:?}: nodes={} edges={} node_histograms={} edge_histograms={} transport_nodes={} hardware_samples={} lifecycle_events={} trace_events={}",
        telemetry.node_metrics.len(),
        telemetry.edge_metrics.len(),
        node_histograms,
        edge_histograms,
        node_transport,
        hardware_samples,
        telemetry.data_lifecycle.len(),
        trace_events
    );
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    for level in [
        MetricsLevel::Off,
        MetricsLevel::Basic,
        MetricsLevel::Detailed,
        MetricsLevel::Profile,
        MetricsLevel::Trace,
    ] {
        let mut registry = PluginRegistry::new();
        let plugin = MetricsPlugin::new();
        registry.install(&plugin)?;
        let square = plugin.square.alias("square");
        let graph = registry
            .graph_builder()?
            .inputs(|g| {
                g.input("in");
            })
            .outputs(|g| {
                g.output("out");
            })
            .nodes(|g| {
                g.add_handle(&square);
            })
            .try_edges(|g| {
                let square = g.node("square");
                g.try_connect("in", &square.input("value"))?;
                g.try_connect(&square.output("value"), "out")?;
                Ok(())
            })?
            .build();
        let engine = Engine::new(EngineConfig::default().with_metrics_level(level))?;
        let mut runtime = engine.compile_registry(&registry, graph)?;
        runtime.push("in", 6_i64);
        let telemetry = runtime.tick_until_idle()?.unwrap_or_default();
        println!("{level:?}: result={:?}", runtime.take::<i64>("out"));
        print_level_summary(level, &telemetry);
        println!("{}", telemetry.compact_snapshot());
    }
    Ok(())
}
