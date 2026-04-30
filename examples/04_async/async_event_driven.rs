use daedalus::{
    engine::{Engine, EngineConfig, GpuBackend, MetricsLevel},
    macros::{node, plugin},
    runtime::{NodeError, plugins::PluginRegistry},
};
use tokio::sync::mpsc;

#[node(id = "async.normalize", inputs("value"), outputs("value"))]
fn normalize(value: i64) -> Result<i64, NodeError> {
    Ok(value.clamp(0, 255))
}

#[node(id = "async.gain", inputs("value"), outputs("value"))]
fn gain(value: i64) -> Result<i64, NodeError> {
    Ok(value * 2)
}

#[node(id = "async.pack", inputs("value"), outputs("value"))]
fn pack(value: i64) -> Result<String, NodeError> {
    Ok(format!("sample={value}"))
}

#[plugin(id = "example.async.event_driven", nodes(normalize, gain, pack))]
struct AsyncEventPlugin;

fn build_runtime() -> Result<
    daedalus::engine::HostGraph<daedalus::runtime::handler_registry::HandlerRegistry>,
    Box<dyn std::error::Error>,
> {
    let mut registry = PluginRegistry::new();
    let plugin = AsyncEventPlugin::new();
    registry.install(&plugin)?;

    let normalize = plugin.normalize.alias("normalize");
    let gain = plugin.gain.alias("gain");
    let pack = plugin.pack.alias("pack");
    let graph = registry
        .graph_builder()?
        .inputs(|g| {
            g.input("in");
        })
        .outputs(|g| {
            g.output("out");
        })
        .nodes(|g| {
            g.add_handle(&normalize);
            g.add_handle(&gain);
            g.add_handle(&pack);
        })
        .try_edges(|g| {
            let normalize = g.node("normalize");
            let gain = g.node("gain");
            let pack = g.node("pack");
            g.try_connect("in", &normalize.input("value"))?;
            g.try_connect(&normalize.output("value"), &gain.input("value"))?;
            g.try_connect(&gain.output("value"), &pack.input("value"))?;
            g.try_connect(&pack.output("value"), "out")?;
            Ok(())
        })?
        .build();

    let engine = Engine::new(
        EngineConfig::from(GpuBackend::Cpu).with_metrics_level(MetricsLevel::Detailed),
    )?;
    Ok(engine.compile_registry(&registry, graph)?)
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut runtime = build_runtime()?;
    let (event_tx, mut event_rx) = mpsc::channel::<i64>(8);
    let (output_tx, mut output_rx) = mpsc::channel::<String>(8);

    let producer = tokio::spawn(async move {
        for value in [-3_i64, 12, 140, 300] {
            if event_tx.send(value).await.is_err() {
                break;
            }
        }
    });

    while let Some(value) = event_rx.recv().await {
        runtime.push("in", value);
        let telemetry = runtime.tick()?;
        for output in runtime.drain_owned::<String>("out")? {
            output_tx.send(output).await?;
        }
        println!("event input={value}");
        println!("{}", telemetry.compact_snapshot());
    }
    drop(output_tx);
    producer.await?;

    while let Some(output) = output_rx.recv().await {
        println!("async output: {output}");
    }
    Ok(())
}
