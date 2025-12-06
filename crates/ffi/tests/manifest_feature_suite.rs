use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Mutex};

use daedalus::{
    BackpressureStrategy, SyncPolicy,
    engine::{Engine, EngineConfig},
    graph_builder::GraphBuilder,
    runtime::{
        handles::NodeHandle,
        plugins::{PluginRegistry, RegistryPluginExt},
    },
};
use daedalus_data::model::{StructField, TypeExpr, Value, ValueType};
use daedalus_ffi::load_manifest_plugin;
use daedalus_registry::ids::NodeId;
use daedalus_registry::store::NodeDescriptorBuilder;
use daedalus_runtime::NodeError;

#[derive(Clone, Copy, Debug)]
enum Lang {
    Python,
    Java,
    Node,
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
        "daedalus_manifest_feature_suite_{prefix}_{nanos}_{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

fn emit_manifest(lang: Lang, out_path: &Path) {
    match lang {
        Lang::Python => {
            let python = std::env::var("PYTHON").unwrap_or_else(|_| "python".to_string());
            if Command::new(&python).arg("--version").output().is_err() {
                eprintln!("skipping: python interpreter not found");
                return;
            }
            let script = workspace_root()
                .join("crates/ffi/lang/python/examples/feature_fixture/emit_manifest.py");
            let status = Command::new(&python)
                .arg(script)
                .arg(out_path)
                .status()
                .expect("run python manifest emitter");
            assert!(status.success());
        }
        Lang::Java => {
            let javac = std::env::var("JAVAC").unwrap_or_else(|_| "javac".to_string());
            let java = std::env::var("JAVA").unwrap_or_else(|_| "java".to_string());
            if Command::new(&javac).arg("--version").output().is_err()
                || Command::new(&java).arg("-version").output().is_err()
            {
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

            let out_dir = out_path
                .parent()
                .unwrap_or_else(|| panic!("manifest out_path has no parent: {out_path:?}"));

            let sdk_dir = workspace_root().join("crates/ffi/lang/java/sdk");
            let ex_dir = workspace_root().join("crates/ffi/lang/java/examples/feature_fixture");
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

            let wgsl_dir = workspace_root().join("crates/ffi/lang/shaders");
            let status = Command::new(&java)
                .env("DAEDALUS_WGSL_DIR", &wgsl_dir)
                .arg("-cp")
                .arg(out_dir)
                .arg("daedalus.examples.EmitManifestFeatures")
                .arg(out_path)
                .status()
                .expect("run java manifest emitter");
            assert!(status.success());
        }
        Lang::Node => {
            let node = std::env::var("NODE").unwrap_or_else(|_| "node".to_string());
            if Command::new(&node).arg("--version").output().is_err() {
                eprintln!("skipping: node interpreter not found");
                return;
            }
            let script = workspace_root()
                .join("crates/ffi/lang/node/examples/feature_fixture/emit_manifest.mjs");
            let status = Command::new(&node)
                .arg(script)
                .arg(out_path)
                .status()
                .expect("run node manifest emitter");
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

fn register_int_source(plugins: &mut PluginRegistry, id: &str, out_port: &str, value: i32) {
    let desc = NodeDescriptorBuilder::new(id)
        .output(out_port, TypeExpr::Scalar(ValueType::Int))
        .build()
        .unwrap();
    plugins.registry.register_node(desc).unwrap();
    let out_port = out_port.to_string();
    plugins.handlers.on(id, move |_, _, io| {
        io.push_any(Some(&out_port), value);
        Ok(())
    });
}

fn register_value_and_cfg_source(plugins: &mut PluginRegistry, id: &str, value: i32, factor: i32) {
    let cfg_ty = TypeExpr::Struct(vec![StructField {
        name: "factor".into(),
        ty: TypeExpr::Scalar(ValueType::Int),
    }]);
    let desc = NodeDescriptorBuilder::new(id)
        .output("value", TypeExpr::Scalar(ValueType::Int))
        .output("cfg", cfg_ty)
        .build()
        .unwrap();
    plugins.registry.register_node(desc).unwrap();
    let cfg_payload = serde_json::json!({ "factor": factor });
    plugins.handlers.on(id, move |_, _, io| {
        io.push_any(Some("value"), value);
        io.push_any(Some("cfg"), cfg_payload.clone());
        Ok(())
    });
}

fn register_xy_source(plugins: &mut PluginRegistry, id: &str, x: i32, y: i32) {
    let desc = NodeDescriptorBuilder::new(id)
        .output("x", TypeExpr::Scalar(ValueType::Int))
        .output("y", TypeExpr::Scalar(ValueType::Int))
        .build()
        .unwrap();
    plugins.registry.register_node(desc).unwrap();
    plugins.handlers.on(id, move |_, _, io| {
        io.push_any(Some("x"), x);
        io.push_any(Some("y"), y);
        Ok(())
    });
}

fn register_dead_source_optional_int(plugins: &mut PluginRegistry, id: &str, out_port: &str) {
    let desc = NodeDescriptorBuilder::new(id)
        .output(
            out_port,
            TypeExpr::Optional(Box::new(TypeExpr::Scalar(ValueType::Int))),
        )
        .build()
        .unwrap();
    plugins.registry.register_node(desc).unwrap();
    // Intentionally no handler registered for this node.
}

fn register_capture_int(
    plugins: &mut PluginRegistry,
    id: &str,
    in_port: &str,
) -> Arc<Mutex<Vec<i32>>> {
    let store = Arc::new(Mutex::new(Vec::new()));
    let desc = NodeDescriptorBuilder::new(id)
        .input(in_port, TypeExpr::Scalar(ValueType::Int))
        .build()
        .unwrap();
    plugins.registry.register_node(desc).unwrap();
    let store_cloned = store.clone();
    let in_port = in_port.to_string();
    plugins.handlers.on(id, move |_, _, io| {
        let v = io
            .get_any::<i32>(&in_port)
            .ok_or_else(|| NodeError::InvalidInput(format!("missing {in_port}")))?;
        store_cloned.lock().unwrap().push(v);
        Ok(())
    });
    store
}

fn register_capture_json(
    plugins: &mut PluginRegistry,
    id: &str,
    in_port: &str,
    ty: TypeExpr,
) -> Arc<Mutex<Vec<serde_json::Value>>> {
    let store = Arc::new(Mutex::new(Vec::new()));
    let desc = NodeDescriptorBuilder::new(id)
        .input(in_port, ty)
        .build()
        .unwrap();
    plugins.registry.register_node(desc).unwrap();
    let store_cloned = store.clone();
    let in_port = in_port.to_string();
    plugins.handlers.on(id, move |_, _, io| {
        let v = io
            .get_any::<serde_json::Value>(&in_port)
            .ok_or_else(|| NodeError::InvalidInput(format!("missing {in_port}")))?;
        store_cloned.lock().unwrap().push(v);
        Ok(())
    });
    store
}

fn output_ty(plugins: &PluginRegistry, node_id: &str, out_port: &str) -> TypeExpr {
    let view = plugins.registry.view();
    let desc = view
        .nodes
        .get(&NodeId::new(node_id))
        .unwrap_or_else(|| panic!("missing descriptor for {node_id}"));
    desc.outputs
        .iter()
        .find(|p| p.name == out_port)
        .unwrap_or_else(|| panic!("missing output {out_port} on {node_id}"))
        .ty
        .clone()
}

fn assert_metadata_string(plugins: &PluginRegistry, node_id: &str, key: &str, expected: &str) {
    let view = plugins.registry.view();
    let desc = view
        .nodes
        .get(&NodeId::new(node_id))
        .unwrap_or_else(|| panic!("missing descriptor for {node_id}"));
    let v = desc
        .metadata
        .get(key)
        .unwrap_or_else(|| panic!("missing metadata {key} on {node_id}"));
    match v {
        Value::String(s) => assert_eq!(s.as_ref(), expected),
        other => panic!("metadata {key} on {node_id} is not a string: {other:?}"),
    }
}

fn assert_input_source(plugins: &PluginRegistry, node_id: &str, port: &str, expected: &str) {
    let view = plugins.registry.view();
    let desc = view
        .nodes
        .get(&NodeId::new(node_id))
        .unwrap_or_else(|| panic!("missing descriptor for {node_id}"));
    let p = desc
        .inputs
        .iter()
        .find(|p| p.name == port)
        .unwrap_or_else(|| panic!("missing input {port} on {node_id}"));
    assert_eq!(p.source.as_deref(), Some(expected));
}

fn assert_sync_group(
    plugins: &PluginRegistry,
    node_id: &str,
    group_name: &str,
    expected_policy: SyncPolicy,
    expected_backpressure: Option<BackpressureStrategy>,
    expected_capacity: Option<usize>,
    expected_ports: &[&str],
) {
    let view = plugins.registry.view();
    let desc = view
        .nodes
        .get(&NodeId::new(node_id))
        .unwrap_or_else(|| panic!("missing descriptor for {node_id}"));
    let g = desc
        .sync_groups
        .iter()
        .find(|g| g.name == group_name)
        .unwrap_or_else(|| panic!("missing sync group {group_name} on {node_id}"));
    assert_eq!(g.policy, expected_policy);
    assert_eq!(g.backpressure, expected_backpressure);
    assert_eq!(g.capacity, expected_capacity);
    assert_eq!(
        g.ports.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
        expected_ports
    );
}

fn run_graph(
    plugins: &PluginRegistry,
    graph: daedalus::planner::Graph,
    handlers: daedalus_runtime::handler_registry::HandlerRegistry,
) {
    let engine = Engine::new(EngineConfig::default()).expect("engine");
    engine
        .run(&plugins.registry, graph, handlers)
        .expect("run graph");
}

fn run_feature_suite(lang: Lang, prefix: &str) {
    let out_dir = temp_dir(prefix);
    let manifest_path = out_dir.join(format!("{prefix}.manifest.json"));
    emit_manifest(lang, &manifest_path);

    // If the interpreter isn't available, the emitter returns early (skip).
    if !manifest_path.exists() {
        return;
    }

    // Ensure the capability registry has basic arithmetic for our tests.
    {
        let mut reg = daedalus_runtime::capabilities::global()
            .write()
            .expect("capability registry lock");
        reg.register_typed::<i32, _>("Add", |a, b| Ok(*a + *b));
    }

    // 1) Const defaults + metadata are preserved and executable.
    {
        let mut plugins = install_manifest(&manifest_path);
        let capture = register_capture_int(&mut plugins, "test:capture_add", "x");

        let add_id = format!("{prefix}:add_defaults");
        assert_metadata_string(
            &plugins,
            &add_id,
            "lang",
            match lang {
                Lang::Python => "python",
                Lang::Java => "java",
                Lang::Node => "node",
            },
        );

        let add = NodeHandle::new(add_id).alias("add");
        let cap = NodeHandle::new("test:capture_add").alias("cap");
        let graph = GraphBuilder::new(&plugins.registry)
            .node(&add)
            .node(&cap)
            .connect(&add.output("out"), &cap.input("x"))
            .build();

        let handlers = plugins.take_handlers();
        run_graph(&plugins, graph, handlers);
        assert_eq!(&*capture.lock().unwrap(), &[5]);
    }

    // 2) Multi-output works.
    {
        let mut plugins = install_manifest(&manifest_path);
        register_int_source(&mut plugins, "test:int_source", "out", 5);
        let cap0 = register_capture_int(&mut plugins, "test:capture_split0", "x");
        let cap1 = register_capture_int(&mut plugins, "test:capture_split1", "x");

        let split = NodeHandle::new(format!("{prefix}:split")).alias("split");
        let src = NodeHandle::new("test:int_source").alias("src");
        let c0 = NodeHandle::new("test:capture_split0").alias("c0");
        let c1 = NodeHandle::new("test:capture_split1").alias("c1");
        let graph = GraphBuilder::new(&plugins.registry)
            .node(&src)
            .node(&split)
            .node(&c0)
            .node(&c1)
            .connect(&src.output("out"), &split.input("value"))
            .connect(&split.output("out0"), &c0.input("x"))
            .connect(&split.output("out1"), &c1.input("x"))
            .build();

        let handlers = plugins.take_handlers();
        run_graph(&plugins, graph, handlers);
        assert_eq!(&*cap0.lock().unwrap(), &[5]);
        assert_eq!(&*cap1.lock().unwrap(), &[-5]);
    }

    // 3) Config-like struct input works.
    {
        let mut plugins = install_manifest(&manifest_path);
        register_value_and_cfg_source(&mut plugins, "test:value_cfg_source", 5, 3);
        let cap = register_capture_int(&mut plugins, "test:capture_scale_cfg", "x");

        let scale = NodeHandle::new(format!("{prefix}:scale_cfg")).alias("scale");
        let src = NodeHandle::new("test:value_cfg_source").alias("src");
        let sink = NodeHandle::new("test:capture_scale_cfg").alias("sink");
        let graph = GraphBuilder::new(&plugins.registry)
            .node(&src)
            .node(&scale)
            .node(&sink)
            .connect(&src.output("value"), &scale.input("value"))
            .connect(&src.output("cfg"), &scale.input("cfg"))
            .connect(&scale.output("out"), &sink.input("x"))
            .build();

        let handlers = plugins.take_handlers();
        run_graph(&plugins, graph, handlers);
        assert_eq!(&*cap.lock().unwrap(), &[15]);
    }

    // 4) Struct + enum outputs round-trip as JSON values.
    {
        let mut plugins = install_manifest(&manifest_path);
        register_int_source(&mut plugins, "test:int_source", "out", 5);

        // make_point(x,y) needs correlated inputs; emit both from the same source node.
        register_xy_source(&mut plugins, "test:xy_source", 7, 9);

        let point_ty = output_ty(&plugins, &format!("{prefix}:make_point"), "out");
        let enum_ty = output_ty(&plugins, &format!("{prefix}:enum_mode"), "out");
        let cap_point = register_capture_json(&mut plugins, "test:capture_point", "x", point_ty);
        let cap_enum = register_capture_json(&mut plugins, "test:capture_enum", "x", enum_ty);

        let mk = NodeHandle::new(format!("{prefix}:make_point")).alias("mk");
        let em = NodeHandle::new(format!("{prefix}:enum_mode")).alias("em");
        let sxy = NodeHandle::new("test:xy_source").alias("sxy");
        let sv = NodeHandle::new("test:int_source").alias("sv");
        let cp = NodeHandle::new("test:capture_point").alias("cp");
        let ce = NodeHandle::new("test:capture_enum").alias("ce");
        let graph = GraphBuilder::new(&plugins.registry)
            .node(&sxy)
            .node(&sv)
            .node(&mk)
            .node(&em)
            .node(&cp)
            .node(&ce)
            .connect(&sxy.output("x"), &mk.input("x"))
            .connect(&sxy.output("y"), &mk.input("y"))
            .connect(&mk.output("out"), &cp.input("x"))
            .connect(&sv.output("out"), &em.input("value"))
            .connect(&em.output("out"), &ce.input("x"))
            .build();

        let handlers = plugins.take_handlers();
        run_graph(&plugins, graph, handlers);

        let point_vals = cap_point.lock().unwrap().clone();
        assert_eq!(point_vals.len(), 1);
        assert_eq!(point_vals[0]["x"], 7);
        assert_eq!(point_vals[0]["y"], 9);

        let enum_vals = cap_enum.lock().unwrap().clone();
        assert_eq!(enum_vals.len(), 1);
        assert!(enum_vals[0].get("name").is_some());
        assert!(enum_vals[0].get("value").is_some());
    }

    // 5) Stateful nodes persist state across runs (via returned JSON state).
    {
        let mut plugins = install_manifest(&manifest_path);
        register_int_source(&mut plugins, "test:int_source", "out", 5);
        let cap = register_capture_int(&mut plugins, "test:capture_accum", "x");

        let accum = NodeHandle::new(format!("{prefix}:accum")).alias("accum");
        let src = NodeHandle::new("test:int_source").alias("src");
        let sink = NodeHandle::new("test:capture_accum").alias("sink");
        let graph = GraphBuilder::new(&plugins.registry)
            .node(&src)
            .node(&accum)
            .node(&sink)
            .connect(&src.output("out"), &accum.input("value"))
            .connect(&accum.output("out"), &sink.input("x"))
            .build();

        let handlers = plugins.take_handlers();
        let engine = Engine::new(EngineConfig::default()).expect("engine");
        engine
            .run(&plugins.registry, graph.clone(), handlers.clone_arc())
            .expect("run1");
        engine
            .run(&plugins.registry, graph, handlers.clone_arc())
            .expect("run2");

        let vals = cap.lock().unwrap().clone();
        assert_eq!(vals.len(), 2);
        assert!(vals[1] > vals[0]);
    }

    // 6) Sync groups from the descriptor are honored (partial invocation + null ports).
    {
        let mut plugins = install_manifest(&manifest_path);
        register_int_source(&mut plugins, "test:a_source", "out", 5);
        register_dead_source_optional_int(&mut plugins, "test:dead_b_source", "out");
        let cap = register_capture_int(&mut plugins, "test:capture_sync", "x");

        let sync_node = NodeHandle::new(format!("{prefix}:sync_a_only")).alias("sync");
        let a = NodeHandle::new("test:a_source").alias("a");
        let b = NodeHandle::new("test:dead_b_source").alias("b");
        let sink = NodeHandle::new("test:capture_sync").alias("sink");
        let graph = GraphBuilder::new(&plugins.registry)
            .node(&a)
            .node(&b)
            .node(&sync_node)
            .node(&sink)
            .connect(&a.output("out"), &sync_node.input("a"))
            .connect(&b.output("out"), &sync_node.input("b"))
            .connect(&sync_node.output("out"), &sink.input("x"))
            .build();

        let handlers = plugins.take_handlers();
        run_graph(&plugins, graph, handlers);
        assert_eq!(&*cap.lock().unwrap(), &[5]);
    }

    // 6b) Rich sync group specs (policy/backpressure/capacity) roundtrip from the manifest.
    {
        let mut plugins = install_manifest(&manifest_path);
        register_int_source(&mut plugins, "test:a_source", "out", 5);
        register_dead_source_optional_int(&mut plugins, "test:dead_b_source", "out");
        let cap = register_capture_int(&mut plugins, "test:capture_sync_obj", "x");

        let sync_node = NodeHandle::new(format!("{prefix}:sync_a_only_obj")).alias("sync");
        let a = NodeHandle::new("test:a_source").alias("a");
        let b = NodeHandle::new("test:dead_b_source").alias("b");
        let sink = NodeHandle::new("test:capture_sync_obj").alias("sink");
        let graph = GraphBuilder::new(&plugins.registry)
            .node(&a)
            .node(&b)
            .node(&sync_node)
            .node(&sink)
            .connect(&a.output("out"), &sync_node.input("a"))
            .connect(&b.output("out"), &sync_node.input("b"))
            .connect(&sync_node.output("out"), &sink.input("x"))
            .build();

        assert_sync_group(
            &plugins,
            &format!("{prefix}:sync_a_only_obj"),
            "a_only",
            SyncPolicy::Latest,
            Some(BackpressureStrategy::ErrorOnOverflow),
            Some(2),
            &["a"],
        );

        let handlers = plugins.take_handlers();
        run_graph(&plugins, graph, handlers);
        assert_eq!(&*cap.lock().unwrap(), &[5]);
    }

    // 7) Rust-like ExecutionContext/RuntimeNode injection is available (ctx/node passed through bridge).
    {
        let mut plugins = install_manifest(&manifest_path);
        register_int_source(&mut plugins, "test:text_source", "out", 0);
        // Provide a string source instead.
        let desc = NodeDescriptorBuilder::new("test:str_source")
            .output("out", TypeExpr::Scalar(ValueType::String))
            .build()
            .unwrap();
        plugins.registry.register_node(desc).unwrap();
        plugins.handlers.on("test:str_source", move |_, _, io| {
            io.push_any(Some("out"), "hello".to_string());
            Ok(())
        });
        let cap = register_capture_int(&mut plugins, "test:capture_ctx_echo_len", "x");

        // ctx_echo returns "hello|<nodeid>", assert length > "hello|".
        let ctx_echo = NodeHandle::new(format!("{prefix}:ctx_echo")).alias("ctx");
        let src = NodeHandle::new("test:str_source").alias("src");

        let desc_len = NodeDescriptorBuilder::new("test:str_len")
            .input("inp", TypeExpr::Scalar(ValueType::String))
            .output("out", TypeExpr::Scalar(ValueType::Int))
            .build()
            .unwrap();
        plugins.registry.register_node(desc_len).unwrap();
        plugins.handlers.on("test:str_len", move |_, _, io| {
            let s = io
                .get_any::<String>("inp")
                .ok_or_else(|| NodeError::InvalidInput("missing inp".into()))?;
            io.push_any(Some("out"), s.len() as i32);
            Ok(())
        });

        let len = NodeHandle::new("test:str_len").alias("len");
        let sink = NodeHandle::new("test:capture_ctx_echo_len").alias("sink");
        let graph = GraphBuilder::new(&plugins.registry)
            .node(&src)
            .node(&ctx_echo)
            .node(&len)
            .node(&sink)
            .connect(&src.output("out"), &ctx_echo.input("text"))
            .connect(&ctx_echo.output("out"), &len.input("inp"))
            .connect(&len.output("out"), &sink.input("x"))
            .build();

        let handlers = plugins.take_handlers();
        run_graph(&plugins, graph, handlers);
        let got = cap.lock().unwrap().clone();
        assert_eq!(got.len(), 1);
        assert!(got[0] > "hello|".len() as i32);
    }

    // 8) Port source metadata is preserved (Rust-like `port(name=..., source=..., default=...)`).
    {
        let plugins = install_manifest(&manifest_path);
        assert_input_source(
            &plugins,
            &format!("{prefix}:choose_mode_meta"),
            "mode",
            "modes",
        );
    }

    // 9) Capability-dispatch nodes work (mirrors Rust `#[node(capability = "Add")]`).
    {
        let mut plugins = install_manifest(&manifest_path);
        register_xy_source(&mut plugins, "test:cap_src_pair", 7, 9);
        // Use an explicit one-port sync group so the sink doesn't fire before its input exists.
        let cap = {
            let store = Arc::new(Mutex::new(Vec::new()));
            let desc = NodeDescriptorBuilder::new("test:capture_cap_add")
                .input("x", TypeExpr::Scalar(ValueType::Int))
                .sync_group(daedalus::SyncGroup {
                    name: "x".into(),
                    policy: SyncPolicy::AllReady,
                    backpressure: None,
                    capacity: None,
                    ports: vec!["x".into()],
                })
                .build()
                .unwrap();
            plugins.registry.register_node(desc).unwrap();
            let store_cloned = store.clone();
            plugins
                .handlers
                .on("test:capture_cap_add", move |_, _, io| {
                    let v = io
                        .get_any::<i32>("x")
                        .ok_or_else(|| NodeError::InvalidInput("missing x".into()))?;
                    store_cloned.lock().unwrap().push(v);
                    Ok(())
                });
            store
        };

        let add = NodeHandle::new(format!("{prefix}:cap_add")).alias("add");
        let src = NodeHandle::new("test:cap_src_pair").alias("src");
        let sink = NodeHandle::new("test:capture_cap_add").alias("sink");
        let graph = GraphBuilder::new(&plugins.registry)
            .node(&src)
            .node(&add)
            .node(&sink)
            .connect(&src.output("x"), &add.input("a"))
            .connect(&src.output("y"), &add.input("b"))
            .connect(&add.output("out"), &sink.input("x"))
            .build();

        let handlers = plugins.take_handlers();
        run_graph(&plugins, graph, handlers);
        assert_eq!(&*cap.lock().unwrap(), &[16]);
    }

    // 10) GPU-required nodes fail planning when GPU is disabled (shader/GPU gating).
    {
        let plugins = install_manifest(&manifest_path);
        let gpu = NodeHandle::new(format!("{prefix}:gpu_required_placeholder")).alias("gpu");
        let graph = GraphBuilder::new(&plugins.registry).node(&gpu).build();
        let engine = Engine::new(EngineConfig::default()).expect("engine");
        assert!(engine.plan(&plugins.registry, graph).is_err());
    }

    // 11) Raw NodeIo-style mode can push multiple outputs/events per tick.
    {
        let mut plugins = install_manifest(&manifest_path);
        let store = Arc::new(Mutex::new(Vec::<i32>::new()));
        let store_cloned = store.clone();
        let desc = NodeDescriptorBuilder::new("test:capture_multi_emit")
            .input("x", TypeExpr::Scalar(ValueType::Int))
            .build()
            .unwrap();
        plugins.registry.register_node(desc).unwrap();
        plugins
            .handlers
            .on("test:capture_multi_emit", move |_, _, io| {
                let vals = io.get_any_all::<i32>("x");
                if vals.is_empty() {
                    return Err(NodeError::InvalidInput("missing x".into()));
                }
                store_cloned.lock().unwrap().extend(vals);
                Ok(())
            });

        let multi = NodeHandle::new(format!("{prefix}:multi_emit")).alias("multi");
        let sink = NodeHandle::new("test:capture_multi_emit").alias("sink");
        let graph = GraphBuilder::new(&plugins.registry)
            .node(&multi)
            .node(&sink)
            .connect(&multi.output("out"), &sink.input("x"))
            .build();

        let handlers = plugins.take_handlers();
        run_graph(&plugins, graph, handlers);
        assert_eq!(&*store.lock().unwrap(), &[1, 2]);
    }
}

#[test]
fn python_manifest_feature_suite() {
    run_feature_suite(Lang::Python, "demo_py_feat");
}

#[test]
fn java_manifest_feature_suite() {
    run_feature_suite(Lang::Java, "demo_java_feat");
}

#[test]
fn node_manifest_feature_suite() {
    run_feature_suite(Lang::Node, "demo_node_feat");
}
