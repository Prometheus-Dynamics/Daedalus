use std::sync::atomic::{AtomicUsize, Ordering};

use daedalus::{
    engine::{Engine, EngineConfig, GpuBackend, MetricsLevel},
    macros::{node, plugin},
    runtime::{NodeError, RuntimeSink, plugins::PluginRegistry},
};
use tokio::sync::mpsc;

static FAST_CALLS: AtomicUsize = AtomicUsize::new(0);
static SLOW_CALLS: AtomicUsize = AtomicUsize::new(0);

#[node(id = "async.fast_preview", inputs("value"), outputs("value"))]
fn fast_preview(value: &i64) -> Result<i64, NodeError> {
    FAST_CALLS.fetch_add(1, Ordering::Relaxed);
    Ok(*value + 1)
}

#[node(id = "async.slow_archive", inputs("value"), outputs("value"))]
fn slow_archive(value: &i64) -> Result<i64, NodeError> {
    SLOW_CALLS.fetch_add(1, Ordering::Relaxed);
    std::thread::sleep(std::time::Duration::from_millis(2));
    Ok(*value * 1000)
}

#[plugin(id = "example.async.demand", nodes(fast_preview, slow_archive))]
struct AsyncDemandPlugin;

fn build_runtime() -> Result<
    daedalus::engine::HostGraph<daedalus::runtime::handler_registry::HandlerRegistry>,
    Box<dyn std::error::Error>,
> {
    let mut registry = PluginRegistry::new();
    let plugin = AsyncDemandPlugin::new();
    registry.install(&plugin)?;

    let fast = plugin.fast_preview.alias("fast");
    let slow = plugin.slow_archive.alias("slow");
    let graph = registry
        .graph_builder()?
        .inputs(|g| {
            g.input("in");
        })
        .outputs(|g| {
            g.output("preview");
            g.output("archive");
        })
        .nodes(|g| {
            g.add_handle(&fast);
            g.add_handle(&slow);
        })
        .try_edges(|g| {
            let fast = g.node("fast");
            let slow = g.node("slow");
            g.try_connect("in", &fast.input("value"))?;
            g.try_connect("in", &slow.input("value"))?;
            g.try_connect(&fast.output("value"), "preview")?;
            g.try_connect(&slow.output("value"), "archive")?;
            Ok(())
        })?
        .build();

    let engine = Engine::new(
        EngineConfig::from(GpuBackend::Cpu).with_metrics_level(MetricsLevel::Detailed),
    )?;
    Ok(engine.compile_registry(&registry, graph)?)
}

fn reset_calls() {
    FAST_CALLS.store(0, Ordering::Relaxed);
    SLOW_CALLS.store(0, Ordering::Relaxed);
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut runtime = build_runtime()?;
    let (tx, mut rx) = mpsc::channel::<i64>(4);

    tokio::spawn(async move {
        for value in [1_i64, 2, 3] {
            if tx.send(value).await.is_err() {
                break;
            }
        }
    });

    let preview_sink = RuntimeSink::node_id("io.host_bridge").port("preview");
    let archive_sink = RuntimeSink::node_id("io.host_bridge").port("archive");

    while let Some(value) = rx.recv().await {
        reset_calls();
        runtime.push("in", value);
        let telemetry = runtime.tick_selected([preview_sink.clone()])?;
        let preview = runtime.drain_owned::<i64>("preview")?;
        let archive = runtime.drain_owned::<i64>("archive")?;
        println!(
            "preview-only input={value} preview={preview:?} archive={archive:?} fast_calls={} slow_calls={}",
            FAST_CALLS.load(Ordering::Relaxed),
            SLOW_CALLS.load(Ordering::Relaxed)
        );
        println!("{}", telemetry.compact_snapshot());
    }

    reset_calls();
    runtime.push("in", 9_i64);
    let telemetry = runtime.tick_selected([preview_sink, archive_sink])?;
    println!(
        "both-sinks preview={:?} archive={:?} fast_calls={} slow_calls={}",
        runtime.drain_owned::<i64>("preview")?,
        runtime.drain_owned::<i64>("archive")?,
        FAST_CALLS.load(Ordering::Relaxed),
        SLOW_CALLS.load(Ordering::Relaxed)
    );
    println!("{}", telemetry.compact_snapshot());
    Ok(())
}
