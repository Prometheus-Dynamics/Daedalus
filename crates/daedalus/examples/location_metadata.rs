//! Demonstrates attaching custom node metadata (e.g., UI positions), saving to JSON,
//! and loading it back before execution.
//! Run with: `cargo run -p daedalus-rs --features "engine,plugins" --example location_metadata`
#![cfg(all(feature = "engine", feature = "plugins"))]

use daedalus::{
    data::model::{StructFieldValue, Value},
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
use std::collections::BTreeMap;

#[node(id = "source", outputs("text"))]
fn source() -> Result<String, NodeError> {
    Ok("hello world".to_string())
}

#[node(id = "shout", inputs("text"), outputs("text"))]
fn shout(text: String) -> Result<String, NodeError> {
    Ok(text.to_uppercase())
}

#[node(id = "sink", inputs("text"))]
fn sink(text: String, ctx: &ExecutionContext) -> Result<(), NodeError> {
    let loc = read_location(&ctx.metadata);
    println!("sink saw `{text}` at {:?}", loc);
    Ok(())
}

fn read_location(meta: &BTreeMap<String, Value>) -> Option<(i64, i64)> {
    match meta.get("location") {
        Some(Value::Struct(fields)) => {
            let mut x = None;
            let mut y = None;
            for f in fields {
                match (f.name.as_str(), &f.value) {
                    ("x", Value::Int(v)) => x = Some(*v),
                    ("y", Value::Int(v)) => y = Some(*v),
                    _ => {}
                }
            }
            match (x, y) {
                (Some(x), Some(y)) => Some((x, y)),
                _ => None,
            }
        }
        _ => None,
    }
}

declare_plugin!(
    LocationPlugin,
    "example.location_metadata",
    [source, shout, sink]
);

fn loc(x: i64, y: i64) -> Value {
    Value::Struct(vec![
        StructFieldValue {
            name: "x".into(),
            value: Value::Int(x),
        },
        StructFieldValue {
            name: "y".into(),
            value: Value::Int(y),
        },
    ])
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut plugins = PluginRegistry::new();
    let plugin = LocationPlugin::new();
    plugins.install_plugin(&plugin)?;
    let handlers = plugins.take_handlers();

    let src_handle = plugin.source.alias("src");
    let shout_handle = plugin.shout.alias("shout");
    let sink_handle = plugin.sink.alias("sink");

    // Build graph with custom per-node metadata (positions here).
    let graph = GraphBuilder::new(&plugins.registry)
        .node(&src_handle)
        .node_metadata(&src_handle, "location", loc(100, 200))
        .node(&shout_handle)
        .node_metadata(&shout_handle, "location", loc(400, 200))
        .node(&sink_handle)
        .node_metadata(&sink_handle, "location", loc(700, 200))
        .connect(&src_handle.outputs.text, &shout_handle.inputs.text)
        .connect(&shout_handle.outputs.text, &sink_handle.inputs.text)
        .build();

    // Persist the graph (including metadata) to JSON.
    let json = serde_json::to_string_pretty(&graph)?;
    println!("saved graph JSON:\n{json}");

    // Load it back and run; metadata is surfaced on ExecutionContext for each node.
    let loaded: daedalus::planner::Graph = serde_json::from_str(&json)?;

    let mut cfg = EngineConfig {
        gpu: GpuBackend::Cpu,
        ..Default::default()
    };
    cfg.runtime.default_policy = EdgePolicyKind::Fifo;
    cfg.runtime.backpressure = BackpressureStrategy::None;
    let engine = Engine::new(cfg)?;

    let result = engine.run(&plugins.registry, loaded, handlers)?;
    println!("telemetry: {:?}", result.telemetry);
    Ok(())
}

#[cfg(not(all(feature = "engine", feature = "plugins")))]
fn main() {
    eprintln!("enable `engine` and `plugins` features to run this example");
}
