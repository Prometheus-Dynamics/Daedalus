//! Graph-backed node example (node body defines wiring via GraphCtx).
//! Run with:
//!   cargo run -p daedalus-rs --features "engine,plugins" --example graph_node
#![cfg(all(feature = "engine", feature = "plugins"))]

use daedalus::{
    declare_plugin,
    engine::{Engine, EngineConfig, GpuBackend, RuntimeMode},
    graph_builder::{GraphBuilder, GraphCtx},
    macros::node,
    runtime::{
        BackpressureStrategy, EdgePolicyKind, NodeError,
        plugins::{PluginRegistry, RegistryPluginExt},
    },
};

#[node(id = "graph_node.source", outputs("out"))]
fn source() -> Result<i32, NodeError> {
    Ok(1)
}

#[node(id = "graph_node.add1", inputs("value"), outputs("out"))]
fn add1(value: i32) -> Result<i32, NodeError> {
    Ok(value + 1)
}

#[node(id = "graph_node.double_add", inputs("value"), outputs("out"))]
fn double_add(g: &mut GraphCtx, value: i32) -> i32 {
    let add_a = g.node_as("graph_node.add1", "add_a");
    let add_b = g.node_as("graph_node.add1", "add_b");
    g.connect(&value, &add_a.input("value"));
    g.connect(&add_a.output("out"), &add_b.input("value"));
    add_b.output("out")
}

#[node(id = "graph_node.sink", inputs("value"))]
fn sink(value: i32) -> Result<(), NodeError> {
    println!("graph node result: {}", value);
    Ok(())
}

declare_plugin!(
    GraphNodePlugin,
    "example.graph_node",
    [source, add1, double_add, sink]
);

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut reg = PluginRegistry::new();
    let plugin = GraphNodePlugin::new();
    reg.install_plugin(&plugin)?;
    let handlers = reg.take_handlers();

    let src = plugin.source.alias("src");
    let dbl = plugin.double_add.alias("dbl");
    let sk = plugin.sink.alias("sink");

    let graph = GraphBuilder::new(&reg.registry)
        .node(&src)
        .node(&dbl)
        .node(&sk)
        .connect(&src.outputs.out, &dbl.inputs.value)
        .connect(&dbl.outputs.out, &sk.inputs.value)
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
compile_error!("Enable the `engine` and `plugins` features to build this example");
