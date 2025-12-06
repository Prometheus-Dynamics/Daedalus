//! Demonstrates port `source` metadata for option lists and how to read it from the registry.
//! Run with: `cargo run -p daedalus-rs --features "engine,plugins" --example metadata_source`
#![cfg(all(feature = "engine", feature = "plugins"))]

use daedalus::data::model::Value;
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
use std::sync::{OnceLock, RwLock};

static MODES: OnceLock<RwLock<Vec<&'static str>>> = OnceLock::new();

/// Options provider for the `mode` port. In a UI this would drive a dropdown.
pub fn modes() -> Vec<&'static str> {
    let store = MODES.get_or_init(|| RwLock::new(vec!["fast", "balanced", "quality"]));
    store.read().unwrap().clone()
}

fn set_modes(new_modes: Vec<&'static str>) {
    let store = MODES.get_or_init(|| RwLock::new(vec!["fast", "balanced", "quality"]));
    *store.write().unwrap() = new_modes;
}

#[node(id = "mode_source", outputs("mode"))]
fn mode_source() -> Result<String, NodeError> {
    let current = modes().first().copied().unwrap_or("fast");
    Ok(current.to_string())
}

#[node(
    id = "choose_mode",
    inputs(port(name = "mode", source = "modes")),
    outputs("out")
)]
fn choose_mode(mode: String) -> Result<String, NodeError> {
    Ok(format!("mode={mode}"))
}

#[node(id = "print", inputs("text"))]
fn print_node(text: String) -> Result<(), NodeError> {
    println!("Chosen mode: {text}");
    Ok(())
}

declare_plugin!(
    MetadataPlugin,
    "example.metadata",
    [mode_source, choose_mode, print_node]
);

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Install plugin.
    let mut plugins = PluginRegistry::new();
    let plugin = MetadataPlugin::new();
    plugins.install_plugin(&plugin)?;
    let handlers = plugins.take_handlers();

    let src = plugin.mode_source.alias("src");
    let choose = plugin.choose_mode.alias("choose");
    let print = plugin.print_node.alias("print");

    // Inspect registry metadata to show the `source` value, and current options.
    let view = plugins.registry.view();
    for (id, node) in view.nodes {
        println!("node: {}", id.0);
        for port in node.inputs {
            println!(
                "  input {} source: {}",
                port.name,
                port.source.unwrap_or_else(|| "None".into())
            );
        }
    }
    println!("available modes (initial): {:?}", modes());

    // Build a tiny graph using the nodes.
    // Graph using only metadata (source) defaults: no const override.
    let graph_no_const = GraphBuilder::new(&plugins.registry)
        .node(&src)
        .node(&choose)
        .node(&print)
        .connect(&src.outputs.mode, &choose.inputs.mode)
        .connect(&choose.outputs.out, &print.inputs.text)
        .build();

    // Graph with a const override to demonstrate external set/unset.
    let graph_const = GraphBuilder::new(&plugins.registry)
        .node(&src)
        .node(&choose)
        .node(&print)
        .connect(&src.outputs.mode, &choose.inputs.mode)
        .const_input(&choose.inputs.mode, Some(Value::String("quality".into())))
        .connect(&choose.outputs.out, &print.inputs.text)
        .build();

    let mut cfg = EngineConfig {
        gpu: GpuBackend::Cpu,
        ..Default::default()
    };
    cfg.runtime.default_policy = EdgePolicyKind::Fifo;
    cfg.runtime.backpressure = BackpressureStrategy::None;
    let engine = Engine::new(cfg)?;

    let handlers_const = handlers.clone_arc();
    let result = engine.run(&plugins.registry, graph_no_const.clone(), handlers_const)?;
    println!("telemetry (run 1, metadata only): {:?}", result.telemetry);

    // Simulate dynamic options update and rerun with const override to show it taking precedence.
    let handlers_const2 = handlers.clone_arc();
    let result_const = engine.run(&plugins.registry, graph_const, handlers_const2)?;
    println!(
        "telemetry (run 2, const override): {:?}",
        result_const.telemetry
    );

    // Simulate dynamic options update and rerun to show different selection when const is unset.
    set_modes(vec!["turbo", "eco"]);
    println!("available modes (after update): {:?}", modes());
    let result2 = engine.run(&plugins.registry, graph_no_const, handlers)?;
    println!(
        "telemetry (run 3, metadata after update): {:?}",
        result2.telemetry
    );
    Ok(())
}
