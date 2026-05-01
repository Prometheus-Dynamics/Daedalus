use std::{
    sync::Mutex,
    thread,
    time::{Duration, Instant},
};

use daedalus::{
    engine::{Engine, EngineConfig, MetricsLevel, RuntimeMode},
    macros::{node, plugin},
    runtime::{NodeError, plugins::PluginRegistry},
};

static THREAD_LOG: Mutex<Vec<(String, String)>> = Mutex::new(Vec::new());

fn record(label: &str) {
    THREAD_LOG
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .push((label.to_string(), format!("{:?}", thread::current().id())));
}

#[node(id = "runtime_modes.left", inputs("value"), outputs("value"))]
fn left(value: &i64) -> Result<i64, NodeError> {
    record("left");
    thread::sleep(Duration::from_millis(25));
    Ok(*value + 1)
}

#[node(id = "runtime_modes.right", inputs("value"), outputs("value"))]
fn right(value: &i64) -> Result<i64, NodeError> {
    record("right");
    thread::sleep(Duration::from_millis(25));
    Ok(*value * 2)
}

#[plugin(id = "example.runtime_modes", nodes(left, right))]
struct RuntimeModesPlugin;

fn run_mode(mode: RuntimeMode) -> Result<(), Box<dyn std::error::Error>> {
    THREAD_LOG
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clear();
    let mut registry = PluginRegistry::new();
    let plugin = RuntimeModesPlugin::new();
    registry.install(&plugin)?;
    let left = plugin.left.alias("left");
    let right = plugin.right.alias("right");
    let graph = registry
        .graph_builder()?
        .inputs(|g| {
            g.input("in");
        })
        .outputs(|g| {
            g.output("left_out");
            g.output("right_out");
        })
        .nodes(|g| {
            g.add_handle(&left);
            g.add_handle(&right);
        })
        .try_edges(|g| {
            let left = g.node("left");
            let right = g.node("right");
            g.try_connect("in", &left.input("value"))?;
            g.try_connect("in", &right.input("value"))?;
            g.try_connect(&left.output("value"), "left_out")?;
            g.try_connect(&right.output("value"), "right_out")?;
            Ok(())
        })?
        .build();

    let mut config = EngineConfig::default()
        .with_runtime_mode(mode.clone())
        .with_metrics_level(MetricsLevel::Detailed);
    if !matches!(mode, RuntimeMode::Serial) {
        config = config.with_pool_size(4);
    }
    let engine = Engine::new(config)?;
    let mut runtime = engine.compile_registry(&registry, graph)?;
    runtime.push("in", 20_i64);
    let wall = Instant::now();
    let telemetry = runtime.tick_until_idle()?.unwrap_or_default();
    let wall = wall.elapsed();
    let thread_log = THREAD_LOG
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone();
    let unique_threads = thread_log
        .iter()
        .map(|(_, thread_id)| thread_id)
        .collect::<std::collections::BTreeSet<_>>()
        .len();
    println!(
        "{mode:?}: outputs left={:?} right={:?} nodes={} wall={wall:?} unique_worker_threads={unique_threads} thread_log={thread_log:?}",
        runtime.take::<i64>("left_out"),
        runtime.take::<i64>("right_out"),
        telemetry.nodes_executed
    );
    println!("{}", telemetry.compact_snapshot());
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    run_mode(RuntimeMode::Serial)?;
    run_mode(RuntimeMode::Parallel)?;
    run_mode(RuntimeMode::Adaptive)?;
    Ok(())
}
