use std::{
    collections::BTreeSet,
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

#[node(id = "pool.work", inputs("value"), outputs("value"))]
fn work(value: &i64) -> Result<i64, NodeError> {
    THREAD_LOG
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .push(("work".to_string(), format!("{:?}", thread::current().id())));
    thread::sleep(Duration::from_millis(20));
    Ok(*value + 1)
}

#[plugin(id = "example.pool", nodes(work))]
struct PoolPlugin;

fn run_pool(pool_size: usize) -> Result<(), Box<dyn std::error::Error>> {
    THREAD_LOG
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clear();
    let mut registry = PluginRegistry::new();
    let plugin = PoolPlugin::new();
    registry.install(&plugin)?;
    let work_0 = plugin.work.clone().alias("work_0");
    let work_1 = plugin.work.clone().alias("work_1");
    let work_2 = plugin.work.clone().alias("work_2");
    let work_3 = plugin.work.alias("work_3");
    let graph = registry
        .graph_builder()?
        .inputs(|g| {
            g.input("in");
        })
        .outputs(|g| {
            g.output("out_0");
            g.output("out_1");
            g.output("out_2");
            g.output("out_3");
        })
        .nodes(|g| {
            g.add_handle(&work_0);
            g.add_handle(&work_1);
            g.add_handle(&work_2);
            g.add_handle(&work_3);
        })
        .try_edges(|g| {
            let work_0 = g.node("work_0");
            let work_1 = g.node("work_1");
            let work_2 = g.node("work_2");
            let work_3 = g.node("work_3");
            g.try_connect("in", &work_0.input("value"))?;
            g.try_connect("in", &work_1.input("value"))?;
            g.try_connect("in", &work_2.input("value"))?;
            g.try_connect("in", &work_3.input("value"))?;
            g.try_connect(&work_0.output("value"), "out_0")?;
            g.try_connect(&work_1.output("value"), "out_1")?;
            g.try_connect(&work_2.output("value"), "out_2")?;
            g.try_connect(&work_3.output("value"), "out_3")?;
            Ok(())
        })?
        .build();

    let engine = Engine::new(
        EngineConfig::default()
            .with_runtime_mode(RuntimeMode::Parallel)
            .with_pool_size(pool_size)
            .with_metrics_level(MetricsLevel::Detailed),
    )?;
    let mut runtime = engine.compile_registry(&registry, graph)?;
    runtime.push("in", 3_i64);
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
        .collect::<BTreeSet<_>>()
        .len();
    println!(
        "pool_size={pool_size}: outputs={:?} {:?} {:?} {:?} nodes={} wall={wall:?} unique_worker_threads={unique_threads}",
        runtime.take::<i64>("out_0"),
        runtime.take::<i64>("out_1"),
        runtime.take::<i64>("out_2"),
        runtime.take::<i64>("out_3"),
        telemetry.nodes_executed
    );
    println!("pool_size={pool_size}: thread_log={thread_log:?}");
    println!("{}", telemetry.compact_snapshot());
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    run_pool(1)?;
    run_pool(4)?;
    Ok(())
}
