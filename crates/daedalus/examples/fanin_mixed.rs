//! Demonstrates a node with both fan-in and normal (single) ports.
//! Fan-in ports are indexed by numeric suffix and ordered by index.
//! Run with: `cargo run -p daedalus-rs --features "engine,plugins" --example fanin_mixed`
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

#[node(id = "src_a", outputs("out"))]
fn src_a() -> Result<i64, NodeError> {
    Ok(10)
}

#[node(id = "src_b", outputs("out"))]
fn src_b() -> Result<i64, NodeError> {
    Ok(20)
}

#[node(id = "scale_src", outputs("out"))]
fn scale_src() -> Result<i64, NodeError> {
    Ok(3)
}

// This node has:
// - fan-in input group: `items0`, `items1`, ... (collected as `FanIn<i64> items`)
// - normal input: `scale`
#[node(id = "mix", inputs("items", "scale"), outputs("out"))]
fn mix(items: FanIn<i64>, scale: i64) -> Result<i64, NodeError> {
    let sum: i64 = items.into_iter().sum();
    Ok(sum * scale)
}

#[node(id = "sink", inputs("vals"))]
fn sink(vals: i64) -> Result<(), NodeError> {
    println!("mixed result: {vals}");
    Ok(())
}

declare_plugin!(
    FanInMixedPlugin,
    "example.fanin_mixed",
    [src_a, src_b, scale_src, mix, sink]
);

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut reg = PluginRegistry::new();
    let plugin = FanInMixedPlugin::new();
    reg.install_plugin(&plugin)?;
    let handlers = reg.take_handlers();

    let a = plugin.src_a.alias("a");
    let b = plugin.src_b.alias("b");
    let s = plugin.scale_src.alias("s");
    let m = plugin.mix.alias("m");
    let sk = plugin.sink.alias("sink");

    let graph = GraphBuilder::new(&reg.registry)
        .node(&a)
        .node(&b)
        .node(&s)
        .node(&m)
        .node(&sk)
        // Fan-in: out-of-order indices; `FanIn<i64>` receives in index order (0 then 2).
        .connect(&a.outputs.out, "m:items0")
        .connect(&b.outputs.out, "m:items2")
        // Normal input.
        .connect(&s.outputs.out, "m:scale")
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
