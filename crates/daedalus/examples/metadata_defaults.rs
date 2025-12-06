//! Demonstrates metadata-only usage: ports with `source` options and no const overrides.
//! Run with: `cargo run -p daedalus-rs --features "engine,plugins" --example metadata_defaults`
#![cfg(all(feature = "engine", feature = "plugins"))]

use daedalus::{
    declare_plugin,
    engine::{Engine, EngineConfig, GpuBackend},
    graph_builder::GraphBuilder,
    macros::node,
    runtime::{
        BackpressureStrategy, EdgePolicyKind, NodeError,
        plugins::{PluginRegistry, RegistryPluginExt},
    },
};

/// Options provider for the `mode` port.
pub fn modes() -> Vec<&'static str> {
    vec!["fast", "balanced", "quality"]
}

#[node(id = "mode_source", outputs("mode"))]
fn mode_source() -> Result<String, NodeError> {
    Ok("fast".to_string())
}

#[node(
    id = "choose_mode_meta",
    inputs(port(name = "mode", source = "modes", default = "quality")),
    outputs("out")
)]
fn choose_mode_meta(mode: String) -> Result<String, NodeError> {
    Ok(format!("mode={mode}"))
}

#[node(id = "print_meta", inputs("text"))]
fn print_node(text: String) -> Result<(), NodeError> {
    println!("Chosen mode (metadata-only): {text}");
    Ok(())
}

declare_plugin!(
    MetadataDefaultsPlugin,
    "example.metadata.defaults",
    [mode_source, choose_mode_meta, print_node]
);

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Install plugin.
    let mut plugins = PluginRegistry::new();
    let plugin = MetadataDefaultsPlugin::new();
    plugins.install_plugin(&plugin)?;
    let handlers = plugins.take_handlers();

    let choose = plugin.choose_mode_meta.alias("choose");
    let print = plugin.print_node.alias("print");
    let src = plugin.mode_source.alias("src");

    // Build graph using only metadata defaults; options come from `source`, and the
    // default value is provided via the macro metadata (no const overrides).
    let graph = GraphBuilder::new(&plugins.registry)
        .node(&src)
        .node(&choose)
        .node(&print)
        .connect(&src.outputs.mode, &choose.inputs.mode)
        .connect(&choose.outputs.out, &print.inputs.text)
        .build();

    let mut cfg = EngineConfig {
        gpu: GpuBackend::Cpu,
        ..Default::default()
    };
    cfg.runtime.default_policy = EdgePolicyKind::Fifo;
    cfg.runtime.backpressure = BackpressureStrategy::None;
    let engine = Engine::new(cfg)?;

    let result = engine.run(&plugins.registry, graph, handlers)?;
    println!("telemetry (metadata only): {:?}", result.telemetry);
    Ok(())
}
