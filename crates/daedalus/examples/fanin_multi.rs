//! Demonstrates a node with multiple independent fan-in groups.
//! Fan-in groups are keyed by the `inputs(...)` prefixes and ordered by numeric suffix.
//! Run with: `cargo run -p daedalus-rs --features "engine,plugins" --example fanin_multi`
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

#[node(id = "src1", outputs("out"))]
fn src1() -> Result<i32, NodeError> {
    Ok(1)
}

#[node(id = "src2", outputs("out"))]
fn src2() -> Result<i32, NodeError> {
    Ok(2)
}

#[node(id = "src10", outputs("out"))]
fn src10() -> Result<i32, NodeError> {
    Ok(10)
}

// Two fan-in groups: `lhs{N}` and `rhs{N}`.
#[node(
    id = "combine",
    inputs("lhs", "rhs"),
    outputs("sum_left", "sum_right", "total")
)]
fn combine(lhs: FanIn<i32>, rhs: FanIn<i32>) -> Result<(i32, i32, i32), NodeError> {
    let sum_left: i32 = lhs.into_iter().sum();
    let sum_right: i32 = rhs.into_iter().sum();
    Ok((sum_left, sum_right, sum_left + sum_right))
}

#[node(id = "sink", inputs("l", "r", "t"))]
fn sink(l: i32, r: i32, t: i32) -> Result<(), NodeError> {
    println!("left={l} right={r} total={t}");
    Ok(())
}

declare_plugin!(
    FanInMultiPlugin,
    "example.fanin_multi",
    [src1, src2, src10, combine, sink]
);

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut reg = PluginRegistry::new();
    let plugin = FanInMultiPlugin::new();
    reg.install_plugin(&plugin)?;
    let handlers = reg.take_handlers();

    let a = plugin.src1.alias("a");
    let b = plugin.src2.alias("b");
    let c = plugin.src10.alias("c");
    let comb = plugin.combine.alias("comb");
    let sk = plugin.sink.alias("sink");

    let graph = GraphBuilder::new(&reg.registry)
        .node(&a)
        .node(&b)
        .node(&c)
        .node(&comb)
        .node(&sk)
        // left fan-in: indices 2 then 0 (collected as [0,2]).
        .connect(&c.outputs.out, "comb:lhs2")
        .connect(&a.outputs.out, "comb:lhs0")
        // right fan-in: indices 1 then 0 (collected as [0,1]).
        .connect(&b.outputs.out, "comb:rhs1")
        .connect(&a.outputs.out, "comb:rhs0")
        .connect("comb:sum_left", "sink:l")
        .connect("comb:sum_right", "sink:r")
        .connect("comb:total", "sink:t")
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
