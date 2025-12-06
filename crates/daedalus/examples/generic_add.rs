//! Demonstrates a single generic add node that dispatches via the capability registry.
//! Concrete types (`i32`, `f64`) are registered at plugin install; the graph uses
//! one node id for both pipelines.
//! Run with:
//!   cargo run -p daedalus-rs --features "engine,plugins" --example generic_add

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
use std::ops::Add;

#[node(id = "add", capability = "Add", inputs("a", "b"), outputs("sum"))]
fn add<T: Add<Output = T> + Clone + Send + Sync + 'static>(a: T, b: T) -> Result<T, NodeError> {
    Ok(a + b)
}

#[node(id = "src_i32", outputs("a", "b"))]
fn src_i32() -> Result<(i32, i32), NodeError> {
    Ok((1, 2))
}

#[node(id = "src_f64", outputs("a", "b"))]
fn src_f64() -> Result<(f64, f64), NodeError> {
    Ok((0.5, 1.5))
}

#[node(id = "sink_i32", inputs("sum"))]
fn sink_i32(sum: i32) -> Result<(), NodeError> {
    println!("i32 sum: {sum}");
    Ok(())
}

#[node(id = "sink_f64", inputs("sum"))]
fn sink_f64(sum: f64) -> Result<(), NodeError> {
    println!("f64 sum: {sum}");
    Ok(())
}

declare_plugin!(
    GenericAddPlugin,
    "example.generic_add",
    [src_i32, src_f64, sink_i32, sink_f64, add]
);

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut reg = PluginRegistry::new();
    let plugin = GenericAddPlugin::new();
    reg.register_capability_typed::<i32, _>("Add", |a, b| Ok(*a + *b));
    reg.register_capability_typed::<f64, _>("Add", |a, b| Ok(*a + *b));
    reg.install_plugin(&plugin)?;

    let add_i32 = plugin.add.clone().alias("add_i32");
    let add_f64 = plugin.add.alias("add_f64");

    let i32_src = plugin.src_i32.alias("i32_src");
    let f64_src = plugin.src_f64.alias("f64_src");
    let sink_i32_handle = plugin.sink_i32.alias("sink_i32");
    let sink_f64_handle = plugin.sink_f64.alias("sink_f64");

    let graph = GraphBuilder::new(&reg.registry)
        .node(&i32_src)
        .node(&f64_src)
        .node(&add_i32)
        .node(&add_f64)
        .node(&sink_i32_handle)
        .node(&sink_f64_handle)
        .connect(&i32_src.outputs.a, &add_i32.inputs.a)
        .connect(&i32_src.outputs.b, &add_i32.inputs.b)
        .connect(&f64_src.outputs.a, &add_f64.inputs.a)
        .connect(&f64_src.outputs.b, &add_f64.inputs.b)
        .connect(&add_i32.outputs.sum, &sink_i32_handle.inputs.sum)
        .connect(&add_f64.outputs.sum, &sink_f64_handle.inputs.sum)
        .build();

    let handlers = reg.take_handlers();

    let mut cfg = EngineConfig::default();
    cfg.runtime.mode = RuntimeMode::Parallel;
    cfg.runtime.pool_size = None;
    cfg.runtime.default_policy = EdgePolicyKind::Fifo;
    cfg.runtime.backpressure = BackpressureStrategy::None;
    cfg.gpu = GpuBackend::Cpu;
    let engine = Engine::new(cfg)?;

    let result = engine.run(&reg.registry, graph, handlers)?;
    println!("telemetry: {:?}", result.telemetry);
    Ok(())
}

#[cfg(not(all(feature = "engine", feature = "plugins")))]
fn main() {
    eprintln!("enable `engine` and `plugins` features to run this example");
}
