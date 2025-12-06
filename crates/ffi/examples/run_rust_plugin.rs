//! Build and run the Rust FFI plugin (`plugin_lib`) to prove the Rust plugin
//! path works end-to-end (cdylib + `PluginLibrary`).

use daedalus::{
    engine::{Engine, EngineConfig, RuntimeMode},
    ffi::PluginLibrary,
    graph_builder::GraphBuilder,
    runtime::{handles::NodeHandle, plugins::PluginRegistry},
};
use std::env;
use std::path::PathBuf;
use std::process::Command;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let plugin_path = ensure_plugin_built()?;
    let mut plugins = PluginRegistry::new();

    println!("Loading Rust plugin from {}", plugin_path.display());
    let lib = unsafe { PluginLibrary::load(&plugin_path)? };
    lib.install_into(&mut plugins)?;

    // Build a graph matching the nodes exposed by plugin_lib (source -> sum -> sum_as_float -> sink).
    let source = NodeHandle::new("ffi.demo:source");
    let sum = NodeHandle::new("ffi.demo:sum");
    let sum_f = NodeHandle::new("ffi.demo:sum_as_float");
    let sink = NodeHandle::new("ffi.demo:sink");

    let graph = GraphBuilder::new(&plugins.registry)
        .node(&source)
        .node(&sum)
        .node(&sum_f)
        .node(&sink)
        .connect(&source.output("left"), &sum.input("a"))
        .connect(&source.output("right"), &sum.input("b"))
        .connect(&sum.output("sum"), &sum_f.input("value"))
        .connect(&sum.output("sum"), &sink.input("int_sum"))
        .connect(&sum_f.output("out"), &sink.input("float_sum"))
        .build();

    let handlers = plugins.take_handlers();
    let mut cfg = EngineConfig::default();
    cfg.runtime.mode = RuntimeMode::Parallel;

    let engine = Engine::new(cfg)?;
    let result = engine.run(&plugins.registry, graph, handlers)?;

    println!(
        "Graph finished. Telemetry: nodes={}, cpu_segments={}, gpu_segments={}",
        result.telemetry.nodes_executed,
        result.telemetry.cpu_segments,
        result.telemetry.gpu_segments
    );

    Ok(())
}

fn ensure_plugin_built() -> Result<PathBuf, Box<dyn std::error::Error>> {
    let path = plugin_path();
    if path.exists() {
        return Ok(path);
    }

    println!(
        "Plugin artifact not found at {}. Building plugin_lib example...",
        path.display()
    );

    let mut workspace_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    workspace_root.pop(); // crates
    workspace_root.pop(); // workspace root

    let mut cmd = Command::new("cargo");
    cmd.current_dir(&workspace_root)
        .arg("build")
        .arg("-p")
        .arg("daedalus-ffi")
        .arg("--example")
        .arg("plugin_lib");
    if current_profile() == "release" {
        cmd.arg("--release");
    }

    let status = cmd.status()?;
    if !status.success() {
        return Err("failed to build plugin_lib example".into());
    }

    let built = plugin_path();
    if built.exists() {
        Ok(built)
    } else {
        Err("plugin_lib artifact missing after build".into())
    }
}

fn plugin_path() -> PathBuf {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.pop(); // crates/ffi -> crates
    path.pop(); // crates -> workspace root
    path.push("target");
    path.push(current_profile());
    path.push("examples");
    let (prefix, ext) = library_naming();
    path.push(format!("{prefix}plugin_lib{ext}"));
    path
}

fn current_profile() -> String {
    env::var("PROFILE")
        .ok()
        .or_else(|| option_env!("PROFILE").map(|s| s.to_string()))
        .unwrap_or_else(|| "debug".to_string())
}

fn library_naming() -> (&'static str, &'static str) {
    #[cfg(target_os = "windows")]
    {
        ("", ".dll")
    }
    #[cfg(target_os = "macos")]
    {
        ("lib", ".dylib")
    }
    #[cfg(all(not(target_os = "windows"), not(target_os = "macos")))]
    {
        ("lib", ".so")
    }
}
