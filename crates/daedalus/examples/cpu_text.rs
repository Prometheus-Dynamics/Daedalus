//! Simple string processing pipeline demonstrating fan-out, map, and reduce on CPU nodes.
//! Run with:
//!   cargo run -p daedalus-rs --features "engine,plugins" --example cpu_text
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

#[node(id = "text_source", outputs("lines"))]
fn text_source() -> Result<Vec<String>, NodeError> {
    Ok(vec![
        "  hello ".into(),
        "world".into(),
        "".into(),
        "daedalus".into(),
    ])
}

#[node(id = "text_trim", inputs("lines"), outputs("lines"))]
fn text_trim(lines: Vec<String>) -> Result<Vec<String>, NodeError> {
    Ok(lines
        .into_iter()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect())
}

#[node(id = "text_upper", inputs("lines"), outputs("lines"))]
fn text_upper(lines: Vec<String>) -> Result<Vec<String>, NodeError> {
    Ok(lines.into_iter().map(|s| s.to_uppercase()).collect())
}

#[node(id = "text_join", inputs("lines"), outputs("text"))]
fn text_join(lines: Vec<String>) -> Result<String, NodeError> {
    Ok(lines.join(", "))
}

#[node(id = "text_sink", inputs("text"))]
fn text_sink(text: String) -> Result<(), NodeError> {
    let path = format!(
        "{}/examples/assets/output_text.txt",
        env!("CARGO_MANIFEST_DIR")
    );
    std::fs::write(&path, text).map_err(|e| NodeError::Handler(e.to_string()))
}

declare_plugin!(
    TextPlugin,
    "example.text",
    [text_source, text_trim, text_upper, text_join, text_sink]
);

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut reg = PluginRegistry::new();
    let plugin = TextPlugin::new();
    reg.install_plugin(&plugin)?;
    let handlers = reg.take_handlers();

    let src = plugin.text_source.alias("source");
    let trim = plugin.text_trim.alias("trim");
    let upper = plugin.text_upper.alias("upper");
    let join = plugin.text_join.alias("join");
    let sink = plugin.text_sink.alias("sink");

    // Pipeline: source -> trim -> upper -> join -> sink
    let graph = GraphBuilder::new(&reg.registry)
        .node(&src)
        .node(&trim)
        .node(&upper)
        .node(&join)
        .node(&sink)
        .connect(&src.outputs.lines, &trim.inputs.lines)
        .connect(&trim.outputs.lines, &upper.inputs.lines)
        .connect(&upper.outputs.lines, &join.inputs.lines)
        .connect(&join.outputs.text, &sink.inputs.text)
        .build();

    let mut cfg = EngineConfig::default();
    cfg.runtime.mode = RuntimeMode::Parallel;
    cfg.runtime.default_policy = EdgePolicyKind::Fifo;
    cfg.runtime.backpressure = BackpressureStrategy::None;
    cfg.gpu = GpuBackend::Cpu;
    let engine = Engine::new(cfg)?;

    let result = engine.run(&reg.registry, graph, handlers)?;
    println!(
        "wrote examples/assets/output_text.txt, telemetry: {:?}",
        result.telemetry
    );
    Ok(())
}

#[cfg(not(all(feature = "engine", feature = "plugins")))]
compile_error!("Enable `engine` and `plugins` features to build this example");
