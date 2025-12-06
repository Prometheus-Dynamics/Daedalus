use std::path::PathBuf;
use std::process::Command;

use daedalus::{
    engine::{Engine, EngineConfig},
    graph_builder::GraphBuilder,
    runtime::{handles::NodeHandle, plugins::RegistryPluginExt},
};
use daedalus_ffi::load_manifest_plugin;
use daedalus_runtime::plugins::PluginRegistry;

fn workspace_root() -> PathBuf {
    let mut root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    root.pop(); // crates/ffi
    root.pop(); // crates
    root
}

fn temp_dir(prefix: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir =
        std::env::temp_dir().join(format!("daedalus_{prefix}_{nanos}_{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

#[test]
fn python_manifest_roundtrip_executes() {
    let python = std::env::var("PYTHON").unwrap_or_else(|_| "python".to_string());
    if Command::new(&python).arg("--version").output().is_err() {
        eprintln!("skipping: python interpreter not found");
        return;
    }

    let out_dir = temp_dir("py_manifest");
    let manifest_path = out_dir.join("demo_py_rt.manifest.json");
    let script = workspace_root().join("crates/ffi/lang/python/examples/demo_rt/emit_manifest.py");
    let status = Command::new(&python)
        .arg(script)
        .arg(&manifest_path)
        .status()
        .expect("run python manifest emitter");
    assert!(status.success());

    let plugin = load_manifest_plugin(&manifest_path).expect("load manifest");
    let mut plugins = PluginRegistry::new();
    plugins.install_plugin(&plugin).expect("install plugin");

    let add = NodeHandle::new("demo_py_rt:add").alias("add");
    let graph = GraphBuilder::new(&plugins.registry).node(&add).build();
    let handlers = plugins.take_handlers();
    let engine = Engine::new(EngineConfig::default()).expect("engine");
    let result = engine.run(&plugins.registry, graph, handlers).expect("run");
    assert_eq!(result.telemetry.nodes_executed, 1);
}

#[test]
fn node_manifest_roundtrip_executes() {
    let node = std::env::var("NODE").unwrap_or_else(|_| "node".to_string());
    if Command::new(&node).arg("--version").output().is_err() {
        eprintln!("skipping: node interpreter not found");
        return;
    }

    let out_dir = temp_dir("node_manifest");
    let manifest_path = out_dir.join("demo_node.manifest.json");
    let script = workspace_root().join("crates/ffi/lang/node/examples/demo_rt/emit_manifest.mjs");
    let status = Command::new(&node)
        .arg(script)
        .arg(&manifest_path)
        .status()
        .expect("run node manifest emitter");
    assert!(status.success());

    let plugin = load_manifest_plugin(&manifest_path).expect("load manifest");
    let mut plugins = PluginRegistry::new();
    plugins.install_plugin(&plugin).expect("install plugin");

    let add = NodeHandle::new("demo_node:add").alias("add");
    let graph = GraphBuilder::new(&plugins.registry).node(&add).build();
    let handlers = plugins.take_handlers();
    let engine = Engine::new(EngineConfig::default()).expect("engine");
    let result = engine.run(&plugins.registry, graph, handlers).expect("run");
    assert_eq!(result.telemetry.nodes_executed, 1);
}

#[test]
fn java_manifest_roundtrip_executes() {
    let javac = std::env::var("JAVAC").unwrap_or_else(|_| "javac".to_string());
    let java = std::env::var("JAVA").unwrap_or_else(|_| "java".to_string());
    if Command::new(&javac).arg("--version").output().is_err()
        || Command::new(&java).arg("-version").output().is_err()
    {
        eprintln!("skipping: java/javac not found");
        return;
    }

    fn collect_java_sources(dir: &std::path::Path) -> Vec<PathBuf> {
        let mut out = Vec::new();
        let mut stack = vec![dir.to_path_buf()];
        while let Some(d) = stack.pop() {
            let Ok(rd) = std::fs::read_dir(&d) else {
                continue;
            };
            for entry in rd.flatten() {
                let p = entry.path();
                if p.is_dir() {
                    stack.push(p);
                } else if p.extension().and_then(|s| s.to_str()) == Some("java") {
                    out.push(p);
                }
            }
        }
        out
    }

    let out_dir = temp_dir("java_manifest");
    let manifest_path = out_dir.join("demo_java_rt.manifest.json");

    // Compile the Java SDK + examples into the output directory, then run the demo emitter.
    let sdk_dir = workspace_root().join("crates/ffi/lang/java/sdk");
    let ex_dir = workspace_root().join("crates/ffi/lang/java/examples/demo_rt");
    let mut sources = collect_java_sources(&sdk_dir);
    sources.extend(collect_java_sources(&ex_dir));
    assert!(!sources.is_empty(), "no java sources found to compile");

    let mut cmd = Command::new(&javac);
    cmd.arg("-d").arg(&out_dir);
    for src in &sources {
        cmd.arg(src);
    }
    let status = cmd.status().expect("javac compile java fixtures");
    assert!(status.success(), "javac failed");

    let status = Command::new(&java)
        .arg("-cp")
        .arg(&out_dir)
        .arg("daedalus.examples.EmitManifestDemo")
        .arg(&manifest_path)
        .status()
        .expect("run java manifest emitter");
    assert!(status.success());

    let plugin = load_manifest_plugin(&manifest_path).expect("load manifest");
    let mut plugins = PluginRegistry::new();
    plugins.install_plugin(&plugin).expect("install plugin");

    let add = NodeHandle::new("demo_java_rt:add").alias("add");
    let graph = GraphBuilder::new(&plugins.registry).node(&add).build();
    let handlers = plugins.take_handlers();
    let engine = Engine::new(EngineConfig::default()).expect("engine");
    let result = engine.run(&plugins.registry, graph, handlers).expect("run");
    assert_eq!(result.telemetry.nodes_executed, 1);
}
