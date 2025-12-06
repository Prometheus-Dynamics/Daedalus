//! Load the Rust FFI plugin (dynamic library) and a Python-authored plugin
//! (packaged into a Rust `cdylib`), then build a mixed graph that routes data
//! between both.

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
    let py_plugin_path = ensure_py_plugin_built()?;
    let mut plugins = PluginRegistry::new();

    println!("Loading Rust plugin from {}", plugin_path.display());
    let lib = unsafe { PluginLibrary::load(&plugin_path)? };
    lib.install_into(&mut plugins)?;

    println!("Loading Python plugin from {}", py_plugin_path.display());
    let py_lib = unsafe { PluginLibrary::load(&py_plugin_path)? };
    py_lib.install_into(&mut plugins)?;

    // Mixed graph: source (Rust) -> add (Python) -> widen (Rust) -> sink (Rust)
    let source = NodeHandle::new("ffi.demo:source");
    let add = NodeHandle::new("demo_py:add");
    let point = NodeHandle::new("demo_py:point");
    let point_len = NodeHandle::new("demo_py:point_len");
    let accum = NodeHandle::new("demo_py:accumulator");
    let scale = NodeHandle::new("demo_py:scale");
    let split = NodeHandle::new("demo_py:split");
    let widen = NodeHandle::new("ffi.demo:sum_as_float");
    let sink = NodeHandle::new("ffi.demo:sink");
    let log_int = NodeHandle::new("ffi.demo:log_int");
    let log_float = NodeHandle::new("ffi.demo:log_float");

    // Build a graph that exercises all Python nodes; scale uses its default factor (2).
    let graph = {
        let mut gb = GraphBuilder::new(&plugins.registry);
        gb = gb
            .node(&source)
            .node(&add)
            .node(&point)
            .node(&point_len)
            .node(&accum)
            .node(&scale)
            .node(&split)
            .node(&widen)
            .node(&sink)
            .node(&log_int)
            .node(&log_float)
            .connect(&source.output("left"), &add.input("lhs"))
            .connect(&source.output("right"), &add.input("rhs"))
            .connect(&add.output("out"), &scale.input("value"))
            .connect(&scale.output("out"), &split.input("value"))
            .connect(&split.output("out0"), &accum.input("value"))
            .connect(&split.output("out1"), &log_int.input("value"))
            .connect(&source.output("left"), &point.input("lhs"))
            .connect(&source.output("right"), &point.input("rhs"))
            .connect(&point.output("out"), &point_len.input("pt"))
            .connect(&accum.output("out"), &widen.input("value"))
            .connect(&accum.output("out"), &sink.input("int_sum"))
            .connect(&widen.output("out"), &sink.input("float_sum"))
            .connect(&point_len.output("out"), &log_float.input("value"));
        gb.build()
    };

    let handlers = plugins.take_handlers();
    let mut cfg = EngineConfig::default();
    cfg.runtime.mode = RuntimeMode::Parallel;

    let engine = Engine::new(cfg)?;
    for run in 1..=5 {
        println!(
            "--- Run {run}: Python scale uses default factor (2); accumulator state persists ---"
        );
        let result = engine.run(&plugins.registry, graph.clone(), handlers.clone_arc())?;
        println!(
            "Run{run} telemetry: nodes={}, cpu_segments={}, gpu_segments={}",
            result.telemetry.nodes_executed,
            result.telemetry.cpu_segments,
            result.telemetry.gpu_segments
        );
    }

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

fn manifest_path() -> PathBuf {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.pop(); // crates/ffi -> crates
    path.pop(); // crates -> workspace root
    path.push("target");
    path.push(current_profile());
    path.push("examples");
    let (prefix, ext) = library_naming();
    path.push(format!("{prefix}generated_py_plugin{ext}"));
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

fn ensure_py_plugin_built() -> Result<PathBuf, Box<dyn std::error::Error>> {
    let path = manifest_path();
    if path.exists() {
        return Ok(path);
    }
    println!(
        "Python plugin artifact not found at {}. Building via plugin_demo.py...",
        path.display()
    );
    let mut workspace_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    workspace_root.pop(); // crates
    workspace_root.pop(); // workspace root
    let script = workspace_root
        .join("crates")
        .join("ffi")
        .join("lang")
        .join("python")
        .join("examples")
        .join("plugin_demo.py");
    let status = Command::new("python").arg(script).status()?;
    if !status.success() {
        return Err("failed to build python plugin".into());
    }
    if path.exists() {
        Ok(path)
    } else {
        Err("python plugin artifact missing after build".into())
    }
}
