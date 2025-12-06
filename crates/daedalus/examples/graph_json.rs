//! Build a small graph, save it to JSON, load it back, and run it.
//! Run with:
//!   cargo run -p daedalus-rs --features "engine,plugins" --example graph_json
#![cfg(all(feature = "engine", feature = "plugins"))]

use daedalus::{
    declare_plugin,
    engine::{Engine, EngineConfig, GpuBackend, RuntimeMode},
    graph_builder::GraphBuilder,
    macros::node,
    planner::Graph,
    runtime::{
        BackpressureStrategy, EdgePolicyKind, NodeError,
        plugins::{PluginRegistry, RegistryPluginExt},
    },
};
use std::{error::Error, fs::File, path::Path};

#[node(id = "json_src", outputs("out"))]
fn json_source() -> Result<i32, NodeError> {
    Ok(21)
}

#[node(id = "json_double", inputs("inp"), outputs("out"))]
fn json_double(inp: i32) -> Result<i32, NodeError> {
    Ok(inp * 2)
}

#[node(id = "json_sink", inputs("inp"))]
fn json_sink(val: i32) -> Result<(), NodeError> {
    println!("graph_json result: {}", val);
    Ok(())
}

declare_plugin!(
    GraphJsonPlugin,
    "example.graph_json",
    [json_source, json_double, json_sink]
);

fn save_graph(path: &Path, graph: &Graph) -> Result<(), Box<dyn Error>> {
    let file = File::create(path)?;
    serde_json::to_writer_pretty(file, graph)?;
    Ok(())
}

fn load_graph(path: &Path) -> Result<Graph, Box<dyn Error>> {
    let file = File::open(path)?;
    Ok(serde_json::from_reader(file)?)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Prepare the registry + handlers.
    let mut reg = PluginRegistry::new();
    let plugin = GraphJsonPlugin::new();
    reg.install_plugin(&plugin)?;
    let handlers = reg.take_handlers();

    // Build the graph via the registry, then write it to disk.
    let src = plugin.json_source.alias("src");
    let dbl = plugin.json_double.alias("double");
    let sk = plugin.json_sink.alias("sink");
    let graph_path = std::env::temp_dir().join("daedalus_graph.json");

    let graph = GraphBuilder::new(&reg.registry)
        .node(&src)
        .node(&dbl)
        .node(&sk)
        .connect(&src.outputs.out, &dbl.inputs.inp)
        .connect(&dbl.outputs.out, &sk.inputs.inp)
        .build();

    save_graph(&graph_path, &graph)?;
    println!("saved graph JSON to {}", graph_path.display());

    // Load the graph back and prove it runs.
    let loaded_graph = load_graph(&graph_path)?;
    assert_eq!(
        graph, loaded_graph,
        "loaded graph should match the original"
    );

    let mut cfg = EngineConfig {
        gpu: GpuBackend::Cpu,
        ..Default::default()
    };
    cfg.runtime.mode = RuntimeMode::Parallel;
    cfg.runtime.default_policy = EdgePolicyKind::Fifo;
    cfg.runtime.backpressure = BackpressureStrategy::None;
    let engine = Engine::new(cfg)?;

    let result = engine.run(&reg.registry, loaded_graph, handlers)?;
    println!("telemetry: {:?}", result.telemetry);
    Ok(())
}

#[cfg(not(all(feature = "engine", feature = "plugins")))]
fn main() {
    eprintln!("enable `engine` and `plugins` features to run this example");
}
