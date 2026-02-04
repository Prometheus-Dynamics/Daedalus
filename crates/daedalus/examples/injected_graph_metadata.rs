//! Demonstrates graph-level metadata visible to nodes at runtime.
//!
//! `Graph.metadata` (a typed `Value` map) is propagated into every node's
//! `ExecutionContext.graph_metadata`. This avoids "broadcasting" the same key/value into every
//! node's metadata.
//!
//! Run with:
//!   cargo run -p daedalus-rs --features "engine,plugins" --example injected_graph_metadata
#![cfg(all(feature = "engine", feature = "plugins"))]

use daedalus::{
    data::model::Value,
    declare_plugin,
    engine::{Engine, EngineConfig, GpuBackend},
    graph_builder::GraphBuilder,
    macros::node,
    runtime::{
        BackpressureStrategy, EdgePolicyKind, NodeError,
        plugins::{PluginRegistry, RegistryPluginExt},
        state::ExecutionContext,
    },
};

#[node(id = "src", outputs("x"))]
fn src() -> Result<i64, NodeError> {
    Ok(7)
}

#[node(id = "mul", inputs("x"), outputs("out"))]
fn mul(x: i64, ctx: &ExecutionContext) -> Result<i64, NodeError> {
    let multiplier = match ctx.graph_metadata.get("multiplier") {
        Some(Value::Int(v)) => *v,
        _ => 1,
    };
    Ok(x * multiplier)
}

#[node(id = "sink", inputs("x"))]
fn sink(x: i64, ctx: &ExecutionContext) -> Result<(), NodeError> {
    let run_id = match ctx.graph_metadata.get("graph_run_id") {
        Some(Value::String(s)) => s.as_ref(),
        _ => "<missing>",
    };
    println!("run_id={run_id} out={x}");
    Ok(())
}

declare_plugin!(
    InjectedMetaPlugin,
    "example.injected_graph_metadata",
    [src, mul, sink]
);

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut plugins = PluginRegistry::new();
    let plugin = InjectedMetaPlugin::new();
    plugins.install_plugin(&plugin)?;
    let handlers = plugins.take_handlers();

    let run_id = "run-123";

    let src_handle = plugin.src.clone().alias("src");
    let mul_handle = plugin.mul.clone().alias("mul");
    let sink_handle = plugin.sink.clone().alias("sink");

    let graph = GraphBuilder::new(&plugins.registry)
        .graph_metadata_value("graph_run_id", Value::String(run_id.into()))
        .graph_metadata_value("multiplier", Value::Int(3))
        .node(&src_handle)
        .node(&mul_handle)
        .node(&sink_handle)
        .connect(&src_handle.outputs.x, &mul_handle.inputs.x)
        .connect(&mul_handle.outputs.out, &sink_handle.inputs.x)
        .build();

    let mut cfg = EngineConfig {
        gpu: GpuBackend::Cpu,
        ..Default::default()
    };
    cfg.runtime.default_policy = EdgePolicyKind::Fifo;
    cfg.runtime.backpressure = BackpressureStrategy::None;
    let engine = Engine::new(cfg)?;
    engine.run(&plugins.registry, graph, handlers)?;
    Ok(())
}

#[cfg(not(all(feature = "engine", feature = "plugins")))]
fn main() {
    eprintln!("enable `engine` and `plugins` features to run this example");
}
