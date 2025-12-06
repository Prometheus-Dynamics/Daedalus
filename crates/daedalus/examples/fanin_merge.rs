//! Demonstrates indexed fan-in inputs via `FanIn<T>`.
//! Ports are named `{prefix}{index}` (e.g. `items0`, `items1`, ...), and values are ordered by index.
//! Run with: `cargo run -p daedalus-rs --features "engine,plugins" --example fanin_merge`

#![cfg(all(feature = "engine", feature = "plugins"))]

use daedalus::{
    FanIn, declare_plugin,
    engine::{Engine, EngineConfig, GpuBackend, RuntimeMode},
    graph_builder::GraphBuilder,
    macros::node,
    runtime::{
        BackpressureStrategy, EdgePolicyKind, NodeError,
        plugins::{PluginRegistry, RegistryPluginExt},
    },
};

#[node(id = "src0", outputs("out"))]
fn src0() -> Result<Vec<i64>, NodeError> {
    Ok(vec![1, 2])
}

#[node(id = "src1", outputs("out"))]
fn src1() -> Result<Vec<i64>, NodeError> {
    Ok(vec![3, 4])
}

#[node(id = "src2", outputs("out"))]
fn src2() -> Result<Vec<i64>, NodeError> {
    Ok(vec![5, 6])
}

#[node(id = "merge", inputs("items"), outputs("out"))]
fn merge(items: FanIn<Vec<i64>>) -> Result<Vec<i64>, NodeError> {
    Ok(items.into_iter().flatten().collect())
}

#[node(id = "sink", inputs("vals"))]
fn sink(vals: Vec<i64>) -> Result<(), NodeError> {
    println!("merged: {vals:?}");
    Ok(())
}

declare_plugin!(
    FanInDemoPlugin,
    "example.fanin",
    [src0, src1, src2, merge, sink]
);

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut reg = PluginRegistry::new();
    let plugin = FanInDemoPlugin::new();
    reg.install_plugin(&plugin)?;
    let handlers = reg.take_handlers();

    let s0 = plugin.src0.alias("s0");
    let s1 = plugin.src1.alias("s1");
    let s2 = plugin.src2.alias("s2");
    let m = plugin.merge.alias("m");
    let sk = plugin.sink.alias("sink");

    let graph = GraphBuilder::new(&reg.registry)
        .node(&s0)
        .node(&s1)
        .node(&s2)
        .node(&m)
        .node(&sk)
        .connect(&s0.outputs.out, "m:items0")
        .connect(&s1.outputs.out, "m:items2")
        .connect(&s2.outputs.out, "m:items1")
        .connect("m:out", "sink:vals")
        .build();

    let mut cfg = EngineConfig::default();
    cfg.runtime.mode = RuntimeMode::Parallel;
    cfg.runtime.pool_size = None;
    cfg.runtime.default_policy = EdgePolicyKind::Fifo;
    cfg.runtime.backpressure = BackpressureStrategy::None;
    cfg.gpu = GpuBackend::Cpu;
    let engine = Engine::new(cfg)?;

    engine.run(&reg.registry, graph, handlers)?;
    Ok(())
}

#[cfg(not(all(feature = "engine", feature = "plugins")))]
fn main() {
    eprintln!("enable `engine` and `plugins` features to run this example");
}
