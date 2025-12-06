//! Demonstrates fan-out and merge with simple integer nodes.
//! Run with:
//!   cargo run -p daedalus-rs --features "engine,plugins" --example cpu_branch
#![cfg(all(feature = "engine", feature = "plugins"))]

use daedalus::{
    declare_plugin,
    engine::{Engine, EngineConfig, GpuBackend, RuntimeMode},
    graph_builder::GraphBuilder,
    macros::node,
    runtime::{
        BackpressureStrategy, EdgePolicyKind, NodeError,
        plugins::{PluginRegistry, RegistryPluginExt},
    },
};

#[node(id = "source", outputs("n"))]
fn source() -> Result<i32, NodeError> {
    Ok(5)
}

#[node(id = "double", inputs("n"), outputs("n"))]
fn double(n: i32) -> Result<i32, NodeError> {
    Ok(n * 2)
}

#[node(id = "triple", inputs("n"), outputs("n"))]
fn triple(n: i32) -> Result<i32, NodeError> {
    Ok(n * 3)
}

#[node(id = "branch_sum", inputs("a", "b"), outputs("n"))]
fn sum(a: i32, b: i32) -> Result<i32, NodeError> {
    Ok(a + b)
}

#[node(id = "sink", inputs("n"))]
fn sink(n: i32) -> Result<(), NodeError> {
    println!("branch result: {}", n);
    Ok(())
}

declare_plugin!(
    BranchPlugin,
    "example.branch",
    [source, double, triple, sum, sink]
);

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut reg = PluginRegistry::new();
    let plugin = BranchPlugin::new();
    reg.install_plugin(&plugin)?;
    let handlers = reg.take_handlers();

    let src = plugin.source.alias("src");
    let dbl = plugin.double.alias("dbl");
    let tpl = plugin.triple.alias("tpl");
    let sm = plugin.sum.alias("sum");
    let sk = plugin.sink.alias("sink");

    let graph = GraphBuilder::new(&reg.registry)
        .node(&src)
        .node(&dbl)
        .node(&tpl)
        .node(&sm)
        .node(&sk)
        // fan-out to two branches
        .connect(&src.outputs.n, &dbl.inputs.n)
        .connect(&src.outputs.n, &tpl.inputs.n)
        // merge at sum
        .connect(&dbl.outputs.n, &sm.inputs.a)
        .connect(&tpl.outputs.n, &sm.inputs.b)
        .connect(&sm.outputs.n, &sk.inputs.n)
        .build();

    let mut cfg = EngineConfig::default();
    cfg.runtime.mode = RuntimeMode::Parallel;
    cfg.runtime.default_policy = EdgePolicyKind::Fifo;
    cfg.runtime.backpressure = BackpressureStrategy::None;
    cfg.gpu = GpuBackend::Cpu;
    let engine = Engine::new(cfg)?;

    let result = engine.run(&reg.registry, graph, handlers)?;
    println!("telemetry: {:?}", result.telemetry);
    Ok(())
}

#[cfg(not(all(feature = "engine", feature = "plugins")))]
compile_error!("Enable `engine` and `plugins` features to build this example");
