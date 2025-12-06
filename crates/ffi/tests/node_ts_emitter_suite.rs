use std::path::{Path, PathBuf};
use std::process::Command;

use daedalus::{
    engine::{Engine, EngineConfig},
    graph_builder::GraphBuilder,
    runtime::plugins::{PluginRegistry, RegistryPluginExt},
};
use daedalus_data::model::{TypeExpr, ValueType};
use daedalus_ffi::load_manifest_plugin;
use daedalus_registry::store::NodeDescriptorBuilder;
use daedalus_runtime::NodeError;

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
    let dir = std::env::temp_dir().join(format!(
        "daedalus_node_ts_emitter_suite_{prefix}_{nanos}_{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

fn ensure_node_tools() {
    let node = std::env::var("NODE").unwrap_or_else(|_| "node".to_string());
    if Command::new(&node).arg("--version").output().is_err() {
        eprintln!("skipping: node interpreter not found");
        return;
    }

    let npm = std::env::var("NPM").unwrap_or_else(|_| "npm".to_string());
    if Command::new(&npm).arg("--version").output().is_err() {
        eprintln!("skipping: npm not found");
        return;
    }

    let pkg_dir = workspace_root().join("crates/ffi/lang/node/daedalus_node");
    let node_modules = pkg_dir.join("node_modules");
    if node_modules.exists() {
        return;
    }

    let status = Command::new(&npm)
        .current_dir(&pkg_dir)
        .arg("install")
        .arg("--no-audit")
        .arg("--no-fund")
        .status()
        .expect("run npm install for daedalus_node");
    assert!(status.success());
}

fn emit_ts_manifest(out_path: &Path, emit_dir: &Path) {
    let node = std::env::var("NODE").unwrap_or_else(|_| "node".to_string());
    if Command::new(&node).arg("--version").output().is_err() {
        eprintln!("skipping: node interpreter not found");
        return;
    }
    ensure_node_tools();

    let tool =
        workspace_root().join("crates/ffi/lang/node/daedalus_node/tools/emit_manifest_ts.mjs");
    let project = workspace_root().join("crates/ffi/lang/node/examples/ts_infer/tsconfig.json");
    let status = Command::new(&node)
        .arg(tool)
        .arg("--project")
        .arg(project)
        .arg("--emit-dir")
        .arg(emit_dir)
        .arg("--out")
        .arg(out_path)
        .arg("--plugin-name")
        .arg("demo_ts")
        .status()
        .expect("run ts manifest emitter");
    assert!(status.success());
}

fn install_manifest(manifest_path: &Path) -> PluginRegistry {
    let plugin = load_manifest_plugin(manifest_path).expect("load manifest plugin");
    let mut plugins = PluginRegistry::new();
    plugins
        .install_plugin(&plugin)
        .expect("install manifest plugin");
    plugins
}

#[test]
fn node_ts_emitter_infers_and_executes() {
    if std::env::var_os("DAEDALUS_TEST_NODE_TSC").is_none() {
        eprintln!("DAEDALUS_TEST_NODE_TSC not set; skipping");
        return;
    }

    let out_dir = temp_dir("demo_ts");
    let emit_dir = out_dir.join("emit");
    std::fs::create_dir_all(&emit_dir).expect("create emit dir");
    let manifest_path = out_dir.join("demo_ts.manifest.json");
    emit_ts_manifest(&manifest_path, &emit_dir);
    if !manifest_path.exists() {
        return;
    }

    // 1) Confirm `DemoTsNodes.add` can be called via dotted export resolution.
    {
        let mut plugins = install_manifest(&manifest_path);
        let src_id = "test:int_pair";
        let desc = NodeDescriptorBuilder::new(src_id)
            .output("a", TypeExpr::Scalar(ValueType::Int))
            .output("b", TypeExpr::Scalar(ValueType::Int))
            .build()
            .unwrap();
        plugins.registry.register_node(desc).unwrap();
        plugins.handlers.on(src_id, move |_, _, io| {
            io.push_any(Some("a"), 7i32);
            io.push_any(Some("b"), 9i32);
            Ok(())
        });

        let sink_id = "test:sink";
        let desc = NodeDescriptorBuilder::new(sink_id)
            .input("x", TypeExpr::Scalar(ValueType::Int))
            .build()
            .unwrap();
        plugins.registry.register_node(desc).unwrap();
        let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::<i32>::new()));
        let seen_cloned = seen.clone();
        plugins.handlers.on(sink_id, move |_, _, io| {
            let v = io
                .get_any::<i32>("x")
                .ok_or_else(|| NodeError::InvalidInput("missing x".into()))?;
            seen_cloned.lock().unwrap().push(v);
            Ok(())
        });

        let graph = GraphBuilder::new(&plugins.registry)
            .node_pair((src_id, "1.0.0"), "src")
            .node_pair(("demo_ts:add", "1.0.0"), "add")
            .node_pair((sink_id, "1.0.0"), "sink")
            .connect("src:a", "add:a")
            .connect("src:b", "add:b")
            .connect("add:out", "sink:x")
            .build();

        let handlers = plugins.take_handlers();
        let engine = Engine::new(EngineConfig::default()).expect("engine");
        engine.run(&plugins.registry, graph, handlers).expect("run");
        assert_eq!(&*seen.lock().unwrap(), &[16]);
    }

    // 2) Tuple return becomes multi-output ports (out0/out1).
    {
        let mut plugins = install_manifest(&manifest_path);
        let src_id = "test:int_src";
        let desc = NodeDescriptorBuilder::new(src_id)
            .output("out", TypeExpr::Scalar(ValueType::Int))
            .build()
            .unwrap();
        plugins.registry.register_node(desc).unwrap();
        plugins.handlers.on(src_id, move |_, _, io| {
            io.push_any(Some("out"), 5i32);
            Ok(())
        });

        let sink0 = "test:sink0";
        let sink1 = "test:sink1";
        for (id, port) in [(sink0, "x"), (sink1, "x")] {
            let desc = NodeDescriptorBuilder::new(id)
                .input(port, TypeExpr::Scalar(ValueType::Int))
                .build()
                .unwrap();
            plugins.registry.register_node(desc).unwrap();
        }
        let s0 = std::sync::Arc::new(std::sync::Mutex::new(Vec::<i32>::new()));
        let s1 = std::sync::Arc::new(std::sync::Mutex::new(Vec::<i32>::new()));
        {
            let s0c = s0.clone();
            plugins.handlers.on(sink0, move |_, _, io| {
                let v = io
                    .get_any::<i32>("x")
                    .ok_or_else(|| NodeError::InvalidInput("missing x".into()))?;
                s0c.lock().unwrap().push(v);
                Ok(())
            });
        }
        {
            let s1c = s1.clone();
            plugins.handlers.on(sink1, move |_, _, io| {
                let v = io
                    .get_any::<i32>("x")
                    .ok_or_else(|| NodeError::InvalidInput("missing x".into()))?;
                s1c.lock().unwrap().push(v);
                Ok(())
            });
        }

        let graph = GraphBuilder::new(&plugins.registry)
            .node_pair((src_id, "1.0.0"), "src")
            .node_pair(("demo_ts:split", "1.0.0"), "split")
            .node_pair((sink0, "1.0.0"), "s0")
            .node_pair((sink1, "1.0.0"), "s1")
            .connect("src:out", "split:value")
            .connect("split:out0", "s0:x")
            .connect("split:out1", "s1:x")
            .build();

        let handlers = plugins.take_handlers();
        let engine = Engine::new(EngineConfig::default()).expect("engine");
        engine.run(&plugins.registry, graph, handlers).expect("run");
        assert_eq!(&*s0.lock().unwrap(), &[5]);
        assert_eq!(&*s1.lock().unwrap(), &[-5]);
    }

    // 3) Record<string,T> infers Map<String,T>.
    {
        let plugins = install_manifest(&manifest_path);
        let view = plugins.registry.view();
        let desc = view
            .nodes
            .get(&daedalus_registry::ids::NodeId::new("demo_ts:map_len"))
            .expect("missing descriptor for demo_ts:map_len");
        assert_eq!(
            desc.inputs
                .iter()
                .map(|p| p.name.as_str())
                .collect::<Vec<_>>(),
            vec!["m"]
        );
        assert_eq!(
            desc.inputs[0].ty,
            TypeExpr::Map(
                Box::new(TypeExpr::Scalar(ValueType::String)),
                Box::new(TypeExpr::Scalar(ValueType::Int))
            )
        );
    }
}
