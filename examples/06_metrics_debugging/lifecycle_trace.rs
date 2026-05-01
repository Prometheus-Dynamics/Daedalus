use daedalus::{
    engine::{Engine, EngineConfig, GpuBackend, MetricsLevel},
    macros::{node, plugin},
    runtime::{NodeError, TelemetryReportFilter, plugins::PluginRegistry},
};

#[node(id = "metrics.decode", inputs("value"), outputs("value"))]
fn decode(value: &i64) -> Result<i64, NodeError> {
    Ok(*value + 1)
}

#[node(id = "metrics.encode", inputs("value"), outputs("value"))]
fn encode(value: &i64) -> Result<String, NodeError> {
    Ok(format!("encoded:{value}"))
}

#[plugin(id = "example.metrics.lifecycle", nodes(decode, encode))]
struct LifecycleMetricsPlugin;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut registry = PluginRegistry::new();
    let plugin = LifecycleMetricsPlugin::new();
    registry.install(&plugin)?;

    let decode = plugin.decode.alias("decode");
    let encode = plugin.encode.alias("encode");
    let graph = registry
        .graph_builder()?
        .inputs(|g| {
            g.input("in");
        })
        .outputs(|g| {
            g.output("out");
        })
        .nodes(|g| {
            g.add_handle(&decode);
            g.add_handle(&encode);
        })
        .try_edges(|g| {
            let decode = g.node("decode");
            let encode = g.node("encode");
            g.try_connect("in", &decode.input("value"))?;
            g.try_connect(&decode.output("value"), &encode.input("value"))?;
            g.try_connect(&encode.output("value"), "out")?;
            Ok(())
        })?
        .build();

    let engine =
        Engine::new(EngineConfig::from(GpuBackend::Cpu).with_metrics_level(MetricsLevel::Trace))?;
    let mut runtime = engine.compile_registry(&registry, graph)?;
    runtime.push("in", 41_i64);
    let telemetry = runtime.tick()?;
    println!("out: {:?}", runtime.drain_owned::<String>("out")?);

    let report = telemetry.report();
    println!("lifecycle events: {}", report.lifecycle.len());
    if let Some(correlation_id) = report.lifecycle.first().map(|event| event.correlation_id) {
        let filtered = report.filter(&TelemetryReportFilter {
            correlation_id: Some(correlation_id),
            ..TelemetryReportFilter::default()
        });
        println!(
            "correlation {correlation_id} lifecycle events: {}",
            filtered.lifecycle.len()
        );
    }
    println!("{}", telemetry.compact_snapshot());
    Ok(())
}
