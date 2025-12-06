use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Mutex};

use daedalus::{
    engine::{Engine, EngineConfig},
    graph_builder::GraphBuilder,
    runtime::{
        NodeError,
        handles::NodeHandle,
        plugins::{PluginRegistry, RegistryPluginExt},
    },
};
use daedalus_data::model::{TypeExpr, Value, ValueType};
use daedalus_ffi::load_manifest_plugin;
use daedalus_registry::store::NodeDescriptorBuilder;

#[derive(Clone, Copy, Debug)]
enum Lang {
    Python,
    Node,
    Java,
}

fn plugin_prefix(lang: Lang) -> &'static str {
    match lang {
        Lang::Python => "demo_py_struct",
        Lang::Node => "demo_node_struct",
        Lang::Java => "demo_java_struct",
    }
}

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
        "daedalus_struct_{prefix}_{nanos}_{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

fn ensure_cmd(cmd: &str, arg: &str) -> bool {
    Command::new(cmd).arg(arg).output().is_ok()
}

fn emit_manifest(lang: Lang, out_path: &Path) {
    match lang {
        Lang::Python => {
            let python = std::env::var("PYTHON").unwrap_or_else(|_| "python".to_string());
            if !ensure_cmd(&python, "--version") {
                eprintln!("skipping: python interpreter not found");
                return;
            }
            let script = workspace_root()
                .join("crates/ffi/lang/python/examples/structured_demo/emit_manifest.py");
            let status = Command::new(&python)
                .arg(script)
                .arg(out_path)
                .status()
                .expect("run python structured manifest emitter");
            assert!(status.success());
        }
        Lang::Node => {
            let node = std::env::var("NODE").unwrap_or_else(|_| "node".to_string());
            if !ensure_cmd(&node, "--version") {
                eprintln!("skipping: node interpreter not found");
                return;
            }
            let script = workspace_root()
                .join("crates/ffi/lang/node/examples/structured_demo/emit_manifest.mjs");
            let status = Command::new(&node)
                .arg(script)
                .arg(out_path)
                .status()
                .expect("run node structured manifest emitter");
            assert!(status.success());
        }
        Lang::Java => {
            let javac = std::env::var("JAVAC").unwrap_or_else(|_| "javac".to_string());
            let java = std::env::var("JAVA").unwrap_or_else(|_| "java".to_string());
            if !ensure_cmd(&javac, "--version") || !ensure_cmd(&java, "-version") {
                eprintln!("skipping: java/javac not found");
                return;
            }

            fn collect_java_sources(dir: &Path) -> Vec<PathBuf> {
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

            let out_dir = out_path.parent().expect("manifest parent");
            let sdk_dir = workspace_root().join("crates/ffi/lang/java/sdk");
            let ex_dir = workspace_root().join("crates/ffi/lang/java/examples/structured_demo");
            let mut sources = collect_java_sources(&sdk_dir);
            sources.extend(collect_java_sources(&ex_dir));
            assert!(!sources.is_empty(), "no java sources found to compile");

            let mut cmd = Command::new(&javac);
            cmd.arg("-d").arg(out_dir);
            for src in &sources {
                cmd.arg(src);
            }
            let status = cmd.status().expect("javac compile java fixtures");
            assert!(status.success(), "javac failed");

            let status = Command::new(&java)
                .arg("-cp")
                .arg(out_dir)
                .arg("daedalus.examples.EmitManifestStructuredDemo")
                .arg(out_path)
                .status()
                .expect("run java structured manifest emitter");
            assert!(status.success());
        }
    }
}

fn install_manifest(manifest_path: &Path) -> PluginRegistry {
    let plugin = load_manifest_plugin(manifest_path).expect("load manifest plugin");
    let mut plugins = PluginRegistry::new();
    // GraphBuilder always injects a host bridge node; register it so planning doesn't fail.
    daedalus::install_host_bridge(
        &mut plugins,
        daedalus::runtime::host_bridge::HostBridgeManager::new(),
    )
    .expect("install host bridge");
    plugins
        .install_plugin(&plugin)
        .expect("install manifest plugin");
    plugins
}

fn register_capture_json(
    plugins: &mut PluginRegistry,
    id: &str,
    in_port: &str,
    in_ty: TypeExpr,
) -> Arc<Mutex<Vec<serde_json::Value>>> {
    let store = Arc::new(Mutex::new(Vec::new()));
    let desc = NodeDescriptorBuilder::new(id)
        .input(in_port, in_ty)
        .build()
        .unwrap();
    plugins.registry.register_node(desc).unwrap();
    let in_port = in_port.to_string();
    let store_cloned = store.clone();
    plugins.handlers.on(id, move |_, _, io| {
        let any = io
            .get_any_raw(&in_port)
            .ok_or_else(|| NodeError::InvalidInput(format!("missing {in_port}")))?;
        if let Some(j) = any.downcast_ref::<serde_json::Value>() {
            store_cloned.lock().unwrap().push(j.clone());
            return Ok(());
        }
        if let Some(v) = any.downcast_ref::<Value>() {
            store_cloned
                .lock()
                .unwrap()
                .push(serde_json::to_value(v).unwrap_or(serde_json::Value::Null));
            return Ok(());
        }
        Err(NodeError::InvalidInput("expected serde_json::Value".into()))
    });
    store
}

fn point_ty() -> TypeExpr {
    TypeExpr::Struct(vec![
        daedalus_data::model::StructField {
            name: "x".into(),
            ty: TypeExpr::Scalar(ValueType::Int),
        },
        daedalus_data::model::StructField {
            name: "y".into(),
            ty: TypeExpr::Scalar(ValueType::Int),
        },
    ])
}

fn mode_ty() -> TypeExpr {
    TypeExpr::Enum(vec![
        daedalus_data::model::EnumVariant {
            name: "A".into(),
            ty: Some(TypeExpr::Scalar(ValueType::Int)),
        },
        daedalus_data::model::EnumVariant {
            name: "B".into(),
            ty: Some(point_ty()),
        },
    ])
}

#[test]
fn structured_manifest_roundtrip_executes_and_moves_data() {
    for lang in [Lang::Python, Lang::Node, Lang::Java] {
        let out_dir = temp_dir("manifest");
        let manifest_path = out_dir.join(format!("demo_struct_{lang:?}.manifest.json"));
        emit_manifest(lang, &manifest_path);
        if !manifest_path.exists() {
            // skipped due to missing toolchain
            continue;
        }

        let mut plugins = install_manifest(&manifest_path);

        // translate_point: point + defaults -> point
        let point_store = register_capture_json(&mut plugins, "cap_point", "inp", point_ty());
        let translate =
            NodeHandle::new(format!("{}:translate_point", plugin_prefix(lang))).alias("translate");
        let cap_point = NodeHandle::new("cap_point").alias("cap_point");
        let pt = Value::Map(vec![
            (Value::String("x".into()), Value::Int(5)),
            (Value::String("y".into()), Value::Int(9)),
        ]);
        let graph = GraphBuilder::new(&plugins.registry)
            .node(&translate)
            .node(&cap_point)
            .const_input_by_id("translate", "pt", Some(pt))
            .connect_by_id(("translate", "out"), ("cap_point", "inp"))
            .build();
        let handlers = plugins.take_handlers();
        let engine = Engine::new(EngineConfig::default()).expect("engine");
        engine.run(&plugins.registry, graph, handlers).expect("run");
        let got = point_store.lock().unwrap().clone();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0], serde_json::json!({"x": 6, "y": 8}));

        // flip_mode: enum -> enum
        let mut plugins = install_manifest(&manifest_path);
        let mode_store = register_capture_json(&mut plugins, "cap_mode", "inp", mode_ty());
        let flip = NodeHandle::new(format!("{}:flip_mode", plugin_prefix(lang))).alias("flip");
        let cap_mode = NodeHandle::new("cap_mode").alias("cap_mode");
        let mode = Value::Map(vec![
            (Value::String("name".into()), Value::String("A".into())),
            (Value::String("value".into()), Value::Int(1)),
        ]);
        let graph = GraphBuilder::new(&plugins.registry)
            .node(&flip)
            .node(&cap_mode)
            .const_input_by_id("flip", "mode", Some(mode))
            .connect_by_id(("flip", "out"), ("cap_mode", "inp"))
            .build();
        let handlers = plugins.take_handlers();
        let engine = Engine::new(EngineConfig::default()).expect("engine");
        engine.run(&plugins.registry, graph, handlers).expect("run");
        let got = mode_store.lock().unwrap().clone();
        assert_eq!(got.len(), 1);
        assert_eq!(
            got[0],
            serde_json::json!({"name":"B","value":{"x":7,"y":9}})
        );

        // map_len: Map<String,Int> -> Int
        let mut plugins = install_manifest(&manifest_path);
        let int_store = Arc::new(Mutex::new(Vec::<i64>::new()));
        let desc = NodeDescriptorBuilder::new("cap_int")
            .input("inp", TypeExpr::Scalar(ValueType::Int))
            .build()
            .unwrap();
        plugins.registry.register_node(desc).unwrap();
        let store_cloned = int_store.clone();
        plugins.handlers.on("cap_int", move |_, _, io| {
            let v = io
                .get_any::<i64>("inp")
                .ok_or_else(|| NodeError::InvalidInput("missing inp".into()))?;
            store_cloned.lock().unwrap().push(v);
            Ok(())
        });

        let map_len = NodeHandle::new(format!("{}:map_len", plugin_prefix(lang))).alias("map_len");
        let cap_int = NodeHandle::new("cap_int").alias("cap_int");
        let m = Value::Map(vec![
            (Value::String("a".into()), Value::Int(1)),
            (Value::String("b".into()), Value::Int(2)),
            (Value::String("c".into()), Value::Int(3)),
        ]);
        let graph = GraphBuilder::new(&plugins.registry)
            .node(&map_len)
            .node(&cap_int)
            .const_input_by_id("map_len", "m", Some(m))
            .connect_by_id(("map_len", "out"), ("cap_int", "inp"))
            .build();
        let handlers = plugins.take_handlers();
        let engine = Engine::new(EngineConfig::default()).expect("engine");
        engine.run(&plugins.registry, graph, handlers).expect("run");
        assert_eq!(int_store.lock().unwrap().as_slice(), &[3]);

        // list_sum: List<Int> -> Int
        let mut plugins = install_manifest(&manifest_path);
        let int_store = Arc::new(Mutex::new(Vec::<i64>::new()));
        let desc = NodeDescriptorBuilder::new("cap_int2")
            .input("inp", TypeExpr::Scalar(ValueType::Int))
            .build()
            .unwrap();
        plugins.registry.register_node(desc).unwrap();
        let store_cloned = int_store.clone();
        plugins.handlers.on("cap_int2", move |_, _, io| {
            let v = io
                .get_any::<i64>("inp")
                .ok_or_else(|| NodeError::InvalidInput("missing inp".into()))?;
            store_cloned.lock().unwrap().push(v);
            Ok(())
        });

        let list_sum =
            NodeHandle::new(format!("{}:list_sum", plugin_prefix(lang))).alias("list_sum");
        let cap_int2 = NodeHandle::new("cap_int2").alias("cap_int2");
        let items = Value::List(vec![
            Value::Int(1),
            Value::Int(2),
            Value::Int(3),
            Value::Int(4),
        ]);
        let graph = GraphBuilder::new(&plugins.registry)
            .node(&list_sum)
            .node(&cap_int2)
            .const_input_by_id("list_sum", "items", Some(items))
            .connect_by_id(("list_sum", "out"), ("cap_int2", "inp"))
            .build();
        let handlers = plugins.take_handlers();
        let engine = Engine::new(EngineConfig::default()).expect("engine");
        engine.run(&plugins.registry, graph, handlers).expect("run");
        assert_eq!(int_store.lock().unwrap().as_slice(), &[10]);
    }
}
