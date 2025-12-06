//! Host runner that loads the nested plugin (`plugin_nested`) and builds a tiny graph
//! using the composed IDs (`cv:child.blur`).
//!
//! Run:
//!   cargo run -p daedalus-ffi --example plugin_nested_host
//!
//! This builds `plugin_nested` if needed, loads it, registers nodes, then executes
//! a simple graph: source -> blur -> sink.

use daedalus::graph_builder::GraphBuilder;
use daedalus::macros::node;
use daedalus::runtime::{NodeError, handles::NodeHandle, plugins::PluginRegistry};
use daedalus_ffi::PluginLibrary;

#[node(id = "host.source", outputs("x"))]
fn source() -> Result<i32, NodeError> {
    Ok(10)
}

#[node(id = "host.sink", inputs("x"))]
fn sink(x: i32) -> Result<(), NodeError> {
    println!("sink received {x}");
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Ensure the nested plugin is built (debug profile).
    let plugin_path = ensure_plugin_built()?;

    let mut plugins = PluginRegistry::new();
    // Load dynamic lib and install plugin; this composes IDs under `cv:child:*`.
    let lib = unsafe { PluginLibrary::load(&plugin_path)? };
    lib.install_into(&mut plugins)?;

    // Register host nodes.
    plugins.merge::<source>()?;
    plugins.merge::<sink>()?;

    let handlers = plugins.take_handlers();

    // Build a graph using the composed IDs.
    let reg = &plugins.registry;
    let src = NodeHandle::new("host.source").alias("src");
    let blur = NodeHandle::new("cv:child.blur").alias("blur");
    let snk = NodeHandle::new("host.sink").alias("snk");
    let graph = GraphBuilder::new(reg)
        .node(&src)
        .node(&blur)
        .node(&snk)
        .connect(&src.output("x"), &blur.input("x"))
        .connect(&blur.output("x"), &snk.input("x"))
        .build();

    // Run via engine facade for simplicity.
    let engine = daedalus::engine::Engine::new(daedalus::engine::EngineConfig::default())?;
    let result = engine.run(reg, graph, handlers)?;
    println!("runtime telemetry: {:?}", result.telemetry);
    Ok(())
}

fn ensure_plugin_built() -> Result<std::path::PathBuf, Box<dyn std::error::Error>> {
    let out = std::process::Command::new("cargo")
        .args(["build", "-p", "daedalus-ffi", "--example", "plugin_nested"])
        .status()?;
    if !out.success() {
        return Err("failed to build plugin_nested example".into());
    }
    let mut path = std::env::current_dir()?;
    path.push("target");
    path.push("debug");
    let (dll_prefix, dll_ext) = if cfg!(target_os = "windows") {
        ("", "dll")
    } else if cfg!(target_os = "macos") {
        ("lib", "dylib")
    } else {
        ("lib", "so")
    };
    path.push("examples");
    path.push(format!("{dll_prefix}plugin_nested.{dll_ext}"));
    Ok(path)
}
