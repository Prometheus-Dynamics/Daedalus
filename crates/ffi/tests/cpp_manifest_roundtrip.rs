use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use daedalus::{
    engine::{Engine, EngineConfig},
    graph_builder::GraphBuilder,
    runtime::plugins::{PluginRegistry, RegistryPluginExt},
};
use daedalus_data::model::{TypeExpr, ValueType};
use daedalus_ffi::{load_cpp_library_plugin, load_manifest_plugin};
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
        "daedalus_cpp_manifest_{prefix}_{nanos}_{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

fn has_cmd(cmd: &str, arg: &str) -> bool {
    Command::new(cmd).arg(arg).output().is_ok()
}

fn dylib_ext() -> &'static str {
    if cfg!(target_os = "windows") {
        "dll"
    } else if cfg!(target_os = "macos") {
        "dylib"
    } else {
        "so"
    }
}

fn compile_cpp_lib(
    out_dir: &Path,
    lib_stem: &str,
    node_prefix: &str,
    plugin_name: &str,
) -> PathBuf {
    let cxx = std::env::var("CXX").unwrap_or_else(|_| "c++".to_string());
    if !has_cmd(&cxx, "--version") {
        eprintln!("skipping: C++ compiler not found (set CXX or install c++)");
        return PathBuf::new();
    }

    let hdr_dir = workspace_root().join("crates/ffi/lang/c_cpp/sdk");
    let src = out_dir.join(format!("{lib_stem}.cpp"));
    let lib = out_dir.join(format!("lib{lib_stem}.{}", dylib_ext()));

    let code = format!(
        r#"#include "daedalus.hpp"
#include <cstdint>
#include <tuple>

static int32_t add_i32(int32_t a, int32_t b) {{ return a + b; }}
static std::tuple<int32_t, int32_t> split_i32(int32_t v) {{ return {{v, (int32_t)(-v)}}; }}

DAEDALUS_STRUCT(ScaleCfg, (factor, int32_t, 2));
static int32_t scale_cfg_i32(int32_t v, ScaleCfg cfg) {{ return (int32_t)(v * cfg.factor); }}

DAEDALUS_STRUCT(CounterSpec, (start, int64_t, 0));
DAEDALUS_STRUCT(CounterState, (value, int64_t, 0));

DAEDALUS_STRUCT(Point, (x, int32_t, 0), (y, int32_t, 0));
DAEDALUS_ENUM(Mode, (A, int32_t), (B, Point));
static Mode enum_mode_i32(int32_t value) {{
  if (value >= 0) return Mode::A(1);
  Point p;
  p.x = 7;
  p.y = 9;
  return Mode::B(p);
}}

static int32_t sync_a_only_i32(int32_t a, std::optional<int32_t> _b) {{ return a; }}

static daedalus::StatefulResult<int32_t> counter_i32(const daedalus::StatefulContext& ctx, int32_t inc) {{
  const int64_t start = ctx.state_spec<CounterSpec>().value_or(CounterSpec{{}}).start;
  const int64_t prev = ctx.state<CounterState>().value_or(CounterState{{}}).value;
  const int64_t next = prev + (int64_t)inc;
  daedalus::StatefulResult<int32_t> r;
  CounterState st;
  st.value = next;
  r.state_json = daedalus::Codec<CounterState>::encode(st);
  r.outputs = (int32_t)next;
  return r;
}}

DAEDALUS_REGISTER_NODE({node_prefix}_add, "{node_prefix}:add", add_i32, DAEDALUS_PORTS(a,b), DAEDALUS_PORTS(out))
DAEDALUS_REGISTER_NODE({node_prefix}_split, "{node_prefix}:split", split_i32, DAEDALUS_PORTS(value), DAEDALUS_PORTS(out0,out1))
DAEDALUS_REGISTER_NODE({node_prefix}_enum_mode, "{node_prefix}:enum_mode", enum_mode_i32, DAEDALUS_PORTS(value), DAEDALUS_PORTS(out))
DAEDALUS_REGISTER_NODE_WITH({node_prefix}_sync_a_only,
                            "{node_prefix}:sync_a_only",
                            sync_a_only_i32,
                            DAEDALUS_PORTS(a,b),
                            DAEDALUS_PORTS(out),
                            {{
                              def.add_sync_group_ports(DAEDALUS_PORTS(a));
                            }})
DAEDALUS_REGISTER_NODE_WITH({node_prefix}_scale_cfg,
                            "{node_prefix}:scale_cfg",
                            scale_cfg_i32,
                            DAEDALUS_PORTS(value,cfg),
                            DAEDALUS_PORTS(out),
                            {{
                              const std::string json = daedalus::Codec<ScaleCfg>::encode(ScaleCfg{{}});
                              def.set_input_const_json("cfg", json.c_str());
                              def.set_label("ScaleCfg");
                              def.set_metadata_json("{{\\\"category\\\":\\\"config\\\"}}");
                            }})
DAEDALUS_REGISTER_STATEFUL_NODE({node_prefix}_counter, "{node_prefix}:counter", counter_i32, DAEDALUS_PORTS(inc), DAEDALUS_PORTS(out), "{{\"start\":0}}")
DAEDALUS_REGISTER_SHADER_NODE_T({node_prefix}_shader_write_u32,
                                "{node_prefix}:shader_write_u32",
                                DAEDALUS_NAMES(),
                                std::tuple<>{{}},
                                DAEDALUS_PORTS(out),
                                std::tuple<uint32_t>{{}},
                                daedalus::shader().file("dummy.wgsl").shader_name("write_u32").invocations(1,1,1).storage_u32_rw(0, "out", 4, true))

DAEDALUS_PLUGIN("{plugin_name}", "0.1.1", "cpp test plugin")
"#
    );

    fs::write(&src, code).expect("write cpp source");

    let status = Command::new(&cxx)
        .current_dir(out_dir)
        .args([
            "-std=c++17",
            "-O2",
            "-fPIC",
            "-shared",
            &format!("-I{}", hdr_dir.display()),
            src.to_string_lossy().as_ref(),
            "-o",
            lib.to_string_lossy().as_ref(),
        ])
        .status()
        .expect("compile c++ dylib");
    assert!(status.success(), "c++ compile failed");

    lib
}

fn compile_cpp_lib_auto_symbols(out_dir: &Path, lib_stem: &str, plugin_name: &str) -> PathBuf {
    let cxx = std::env::var("CXX").unwrap_or_else(|_| "c++".to_string());
    if !has_cmd(&cxx, "--version") {
        eprintln!("skipping: C++ compiler not found (set CXX or install c++)");
        return PathBuf::new();
    }

    let hdr_dir = workspace_root().join("crates/ffi/lang/c_cpp/sdk");
    let src = out_dir.join(format!("{lib_stem}.cpp"));
    let lib = out_dir.join(format!("lib{lib_stem}.{}", dylib_ext()));

    let code = format!(
        r#"#include "daedalus.hpp"
#include <cstdint>

static int32_t add_i32(int32_t a, int32_t b) {{ return a + b; }}

DAEDALUS_NODE_WITH("auto_cpp:add", add_i32, DAEDALUS_PORTS(a,b), DAEDALUS_PORTS(out), {{
  def.set_label("Add");
  def.set_metadata_json("{{\\\"category\\\":\\\"math\\\"}}");
}})

DAEDALUS_PLUGIN("{plugin_name}", "0.1.1", "cpp auto-symbol test plugin")
"#
    );

    fs::write(&src, code).expect("write cpp source");

    let status = Command::new(&cxx)
        .current_dir(out_dir)
        .args([
            "-std=c++17",
            "-O2",
            "-fPIC",
            "-shared",
            &format!("-I{}", hdr_dir.display()),
            src.to_string_lossy().as_ref(),
            "-o",
            lib.to_string_lossy().as_ref(),
        ])
        .status()
        .expect("compile c++ dylib");
    assert!(status.success(), "c++ compile failed");

    lib
}

fn write_manifest(out_dir: &Path, lib_path: &Path, prefix: &str) -> PathBuf {
    let manifest_path = out_dir.join(format!("{prefix}.manifest.json"));
    let lib_name = lib_path.file_name().unwrap().to_string_lossy();
    let point_ty = serde_json::json!({
      "Struct": [
        {"name":"x","ty":{"Scalar":"I32"}},
        {"name":"y","ty":{"Scalar":"I32"}},
      ]
    });
    let mode_ty = serde_json::json!({
      "Enum": [
        {"name":"A","ty":{"Scalar":"I32"}},
        {"name":"B","ty": point_ty},
      ]
    });
    let doc = serde_json::json!({
      "manifest_version": "1",
      "language": "c_cpp",
      "plugin": { "name": prefix, "version": "0.1.1", "description": "cpp test", "metadata": {} },
      "nodes": [
        {
          "id": format!("{prefix}:add"),
          "cc_path": lib_name,
          "cc_function": format!("{prefix}_add"),
          "inputs": [{"name":"a","ty":{"Scalar":"I32"}},{"name":"b","ty":{"Scalar":"I32"}}],
          "outputs": [{"name":"out","ty":{"Scalar":"I32"}}],
        },
        {
          "id": format!("{prefix}:split"),
          "cc_path": lib_name,
          "cc_function": format!("{prefix}_split"),
          "inputs": [{"name":"value","ty":{"Scalar":"I32"}}],
          "outputs": [{"name":"out0","ty":{"Scalar":"I32"}},{"name":"out1","ty":{"Scalar":"I32"}}],
        },
        {
          "id": format!("{prefix}:enum_mode"),
          "cc_path": lib_name,
          "cc_function": format!("{prefix}_enum_mode"),
          "inputs": [{"name":"value","ty":{"Scalar":"I32"}}],
          "outputs": [{"name":"out","ty": mode_ty}]
        },
        {
          "id": format!("{prefix}:sync_a_only"),
          "cc_path": lib_name,
          "cc_function": format!("{prefix}_sync_a_only"),
          "sync_groups": [["a"]],
          "inputs": [{"name":"a","ty":{"Scalar":"I32"}},{"name":"b","ty":{"Optional":{"Scalar":"I32"}}}],
          "outputs": [{"name":"out","ty":{"Scalar":"I32"}}],
        },
        {
          "id": format!("{prefix}:scale_cfg"),
          "cc_path": lib_name,
          "cc_function": format!("{prefix}_scale_cfg"),
          "inputs": [
            {"name":"value","ty":{"Scalar":"I32"}},
            {"name":"cfg","ty":{"Struct":[{"name":"factor","ty":{"Scalar":"I32"}}]}, "const_value": {"factor": 2}}
          ],
          "outputs": [{"name":"out","ty":{"Scalar":"I32"}}],
        },
        {
          "id": format!("{prefix}:counter"),
          "cc_path": lib_name,
          "cc_function": format!("{prefix}_counter"),
          "stateful": true,
          "state": {"start": 0},
          "inputs": [{"name":"inc","ty":{"Scalar":"I32"}}],
          "outputs": [{"name":"out","ty":{"Scalar":"I32"}}],
        }
        ,
        {
          "id": format!("{prefix}:shader_write_u32"),
          "shader": {
            "src_path": "dummy.wgsl",
            "entry": "main",
            "name": "write_u32",
            "invocations": [1, 1, 1],
            "bindings": [
              {"binding": 0, "kind": "storage_buffer", "access": "read_write", "readback": true, "to_port": "out", "size_bytes": 4}
            ]
          },
          "outputs": [{"name":"out","ty":{"Scalar":"U32"}}]
        }
      ]
    });
    fs::write(&manifest_path, serde_json::to_vec_pretty(&doc).unwrap()).expect("write manifest");
    manifest_path
}

#[test]
fn cpp_manifest_roundtrip_executes() {
    if std::env::var_os("DAEDALUS_TEST_CPP").is_none() {
        eprintln!("DAEDALUS_TEST_CPP not set; skipping");
        return;
    }

    let out_dir = temp_dir("rt");
    let lib_path = compile_cpp_lib(&out_dir, "example_cpp_nodes", "demo_cpp", "demo_cpp");
    if lib_path.as_os_str().is_empty() {
        return;
    }
    let manifest_path = write_manifest(&out_dir, &lib_path, "demo_cpp");

    let plugin = load_manifest_plugin(&manifest_path).expect("load manifest");
    let mut plugins = PluginRegistry::new();
    plugins.install_plugin(&plugin).expect("install plugin");

    // add
    {
        let src_id = "test:int_pair";
        let desc = NodeDescriptorBuilder::new(src_id)
            .output("a", TypeExpr::Scalar(ValueType::I32))
            .output("b", TypeExpr::Scalar(ValueType::I32))
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
            .input("x", TypeExpr::Scalar(ValueType::I32))
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
            .node_pair(("demo_cpp:add", "1.0.0"), "add")
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

    // split
    {
        let mut plugins = PluginRegistry::new();
        plugins.install_plugin(&plugin).expect("install plugin");

        let src_id = "test:int_src";
        let desc = NodeDescriptorBuilder::new(src_id)
            .output("out", TypeExpr::Scalar(ValueType::I32))
            .build()
            .unwrap();
        plugins.registry.register_node(desc).unwrap();
        plugins.handlers.on(src_id, move |_, _, io| {
            io.push_any(Some("out"), 5i32);
            Ok(())
        });

        let sink0 = "test:sink0";
        let sink1 = "test:sink1";
        for id in [sink0, sink1] {
            let desc = NodeDescriptorBuilder::new(id)
                .input("x", TypeExpr::Scalar(ValueType::I32))
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
            .node_pair(("demo_cpp:split", "1.0.0"), "split")
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

    // counter persists across runs
    {
        let mut plugins = PluginRegistry::new();
        plugins.install_plugin(&plugin).expect("install plugin");

        let src_id = "test:inc_src";
        let desc = NodeDescriptorBuilder::new(src_id)
            .output("out", TypeExpr::Scalar(ValueType::I32))
            .build()
            .unwrap();
        plugins.registry.register_node(desc).unwrap();
        plugins.handlers.on(src_id, move |_, _, io| {
            io.push_any(Some("out"), 5i32);
            Ok(())
        });

        let sink_id = "test:sink_counter";
        let desc = NodeDescriptorBuilder::new(sink_id)
            .input("x", TypeExpr::Scalar(ValueType::I32))
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
            .node_pair(("demo_cpp:counter", "1.0.0"), "ctr")
            .node_pair((sink_id, "1.0.0"), "sink")
            .connect("src:out", "ctr:inc")
            .connect("ctr:out", "sink:x")
            .build();

        let handlers = plugins.take_handlers();
        let engine = Engine::new(EngineConfig::default()).expect("engine");
        engine
            .run(&plugins.registry, graph.clone(), handlers.clone_arc())
            .expect("run1");
        engine
            .run(&plugins.registry, graph, handlers.clone_arc())
            .expect("run2");
        let vals = seen.lock().unwrap().clone();
        assert_eq!(vals.len(), 2);
        assert!(vals[1] > vals[0]);
    }

    // typed config + const_value injection
    {
        let mut plugins = PluginRegistry::new();
        plugins.install_plugin(&plugin).expect("install plugin");

        let src_id = "test:val_src";
        let desc = NodeDescriptorBuilder::new(src_id)
            .output("out", TypeExpr::Scalar(ValueType::I32))
            .build()
            .unwrap();
        plugins.registry.register_node(desc).unwrap();
        plugins.handlers.on(src_id, move |_, _, io| {
            io.push_any(Some("out"), 5i32);
            Ok(())
        });

        let sink_id = "test:sink_scale";
        let desc = NodeDescriptorBuilder::new(sink_id)
            .input("x", TypeExpr::Scalar(ValueType::I32))
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
            .node_pair(("demo_cpp:scale_cfg", "1.0.0"), "scale")
            .node_pair((sink_id, "1.0.0"), "sink")
            .connect("src:out", "scale:value")
            .connect("scale:out", "sink:x")
            .build();

        let handlers = plugins.take_handlers();
        let engine = Engine::new(EngineConfig::default()).expect("engine");
        engine.run(&plugins.registry, graph, handlers).expect("run");
        assert_eq!(&*seen.lock().unwrap(), &[10]);
    }

    // typed enum output
    {
        let mut plugins = PluginRegistry::new();
        plugins.install_plugin(&plugin).expect("install plugin");

        let src_id = "test:val_src_enum";
        let desc = NodeDescriptorBuilder::new(src_id)
            .output("out", TypeExpr::Scalar(ValueType::I32))
            .build()
            .unwrap();
        plugins.registry.register_node(desc).unwrap();
        plugins.handlers.on(src_id, move |_, _, io| {
            io.push_any(Some("out"), -1i32);
            Ok(())
        });

        let sink_id = "test:sink_enum";
        let desc = NodeDescriptorBuilder::new(sink_id)
            .input(
                "x",
                TypeExpr::Enum(vec![
                    daedalus_data::model::EnumVariant {
                        name: "A".into(),
                        ty: Some(TypeExpr::Scalar(ValueType::I32)),
                    },
                    daedalus_data::model::EnumVariant {
                        name: "B".into(),
                        ty: Some(TypeExpr::Struct(vec![
                            daedalus_data::model::StructField {
                                name: "x".into(),
                                ty: TypeExpr::Scalar(ValueType::I32),
                            },
                            daedalus_data::model::StructField {
                                name: "y".into(),
                                ty: TypeExpr::Scalar(ValueType::I32),
                            },
                        ])),
                    },
                ]),
            )
            .build()
            .unwrap();
        plugins.registry.register_node(desc).unwrap();
        let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::<serde_json::Value>::new()));
        let seen_cloned = seen.clone();
        plugins.handlers.on(sink_id, move |_, _, io| {
            let v = io
                .get_any::<serde_json::Value>("x")
                .ok_or_else(|| NodeError::InvalidInput("missing x".into()))?;
            seen_cloned.lock().unwrap().push(v);
            Ok(())
        });

        let graph = GraphBuilder::new(&plugins.registry)
            .node_pair((src_id, "1.0.0"), "src")
            .node_pair(("demo_cpp:enum_mode", "1.0.0"), "enm")
            .node_pair((sink_id, "1.0.0"), "sink")
            .connect("src:out", "enm:value")
            .connect("enm:out", "sink:x")
            .build();

        let handlers = plugins.take_handlers();
        let engine = Engine::new(EngineConfig::default()).expect("engine");
        engine.run(&plugins.registry, graph, handlers).expect("run");
        assert_eq!(
            &*seen.lock().unwrap(),
            &[serde_json::json!({"name":"B","value":{"x":7,"y":9}})]
        );
    }

    // sync groups allow missing optional port
    {
        let mut plugins = PluginRegistry::new();
        plugins.install_plugin(&plugin).expect("install plugin");

        let src_id = "test:sync_src";
        let desc = NodeDescriptorBuilder::new(src_id)
            .output("out", TypeExpr::Scalar(ValueType::I32))
            .build()
            .unwrap();
        plugins.registry.register_node(desc).unwrap();
        plugins.handlers.on(src_id, move |_, _, io| {
            io.push_any(Some("out"), 123i32);
            Ok(())
        });

        let sink_id = "test:sink_sync";
        let desc = NodeDescriptorBuilder::new(sink_id)
            .input("x", TypeExpr::Scalar(ValueType::I32))
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
            .node_pair(("demo_cpp:sync_a_only", "1.0.0"), "sync")
            .node_pair((sink_id, "1.0.0"), "sink")
            .connect("src:out", "sync:a")
            .connect("sync:out", "sink:x")
            .build();

        let handlers = plugins.take_handlers();
        let engine = Engine::new(EngineConfig::default()).expect("engine");
        engine.run(&plugins.registry, graph, handlers).expect("run");
        assert_eq!(&*seen.lock().unwrap(), &[123]);
    }

    // shader nodes are present but planning fails without GPU.
    {
        let plugins = {
            let mut plugins = PluginRegistry::new();
            plugins.install_plugin(&plugin).expect("install plugin");
            plugins
        };
        let gpu =
            daedalus::runtime::handles::NodeHandle::new("demo_cpp:shader_write_u32").alias("gpu");
        let graph = daedalus::graph_builder::GraphBuilder::new(&plugins.registry)
            .node(&gpu)
            .build();
        let engine = Engine::new(EngineConfig::default()).expect("engine");
        assert!(engine.plan(&plugins.registry, graph).is_err());
    }
}

#[test]
fn cpp_library_manifest_roundtrip_executes() {
    if std::env::var_os("DAEDALUS_TEST_CPP").is_none() {
        eprintln!("DAEDALUS_TEST_CPP not set; skipping");
        return;
    }

    let out_dir = temp_dir("rt_lib");
    let lib_path = compile_cpp_lib(&out_dir, "example_cpp_nodes2", "demo_cpp2", "demo_cpp2");
    if lib_path.as_os_str().is_empty() {
        return;
    }

    let plugin = load_cpp_library_plugin(&lib_path).expect("load cpp library plugin");
    let mut plugins = PluginRegistry::new();
    plugins.install_plugin(&plugin).expect("install plugin");

    // add
    {
        let src_id = "test:int_pair";
        let desc = NodeDescriptorBuilder::new(src_id)
            .output("a", TypeExpr::Scalar(ValueType::I32))
            .output("b", TypeExpr::Scalar(ValueType::I32))
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
            .input("x", TypeExpr::Scalar(ValueType::I32))
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
            .node_pair(("demo_cpp2:add", "1.0.0"), "add")
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

    // typed config + const_value injection (from dylib manifest).
    {
        let src_id = "test:val_src";
        let desc = NodeDescriptorBuilder::new(src_id)
            .output("out", TypeExpr::Scalar(ValueType::I32))
            .build()
            .unwrap();
        plugins.registry.register_node(desc).unwrap();
        plugins.handlers.on(src_id, move |_, _, io| {
            io.push_any(Some("out"), 6i32);
            Ok(())
        });

        let sink_id = "test:sink_scale";
        let desc = NodeDescriptorBuilder::new(sink_id)
            .input("x", TypeExpr::Scalar(ValueType::I32))
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
            .node_pair(("demo_cpp2:scale_cfg", "1.0.0"), "scale")
            .node_pair((sink_id, "1.0.0"), "sink")
            .connect("src:out", "scale:value")
            .connect("scale:out", "sink:x")
            .build();

        let handlers = plugins.take_handlers();
        let engine = Engine::new(EngineConfig::default()).expect("engine");
        engine.run(&plugins.registry, graph, handlers).expect("run");
        assert_eq!(&*seen.lock().unwrap(), &[12]);
    }

    // typed enum output (from dylib manifest).
    {
        let src_id = "test:val_src_enum";
        let desc = NodeDescriptorBuilder::new(src_id)
            .output("out", TypeExpr::Scalar(ValueType::I32))
            .build()
            .unwrap();
        plugins.registry.register_node(desc).unwrap();
        plugins.handlers.on(src_id, move |_, _, io| {
            io.push_any(Some("out"), -1i32);
            Ok(())
        });

        let sink_id = "test:sink_enum";
        let desc = NodeDescriptorBuilder::new(sink_id)
            .input(
                "x",
                TypeExpr::Enum(vec![
                    daedalus_data::model::EnumVariant {
                        name: "A".into(),
                        ty: Some(TypeExpr::Scalar(ValueType::I32)),
                    },
                    daedalus_data::model::EnumVariant {
                        name: "B".into(),
                        ty: Some(TypeExpr::Struct(vec![
                            daedalus_data::model::StructField {
                                name: "x".into(),
                                ty: TypeExpr::Scalar(ValueType::I32),
                            },
                            daedalus_data::model::StructField {
                                name: "y".into(),
                                ty: TypeExpr::Scalar(ValueType::I32),
                            },
                        ])),
                    },
                ]),
            )
            .build()
            .unwrap();
        plugins.registry.register_node(desc).unwrap();
        let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::<serde_json::Value>::new()));
        let seen_cloned = seen.clone();
        plugins.handlers.on(sink_id, move |_, _, io| {
            let v = io
                .get_any::<serde_json::Value>("x")
                .ok_or_else(|| NodeError::InvalidInput("missing x".into()))?;
            seen_cloned.lock().unwrap().push(v);
            Ok(())
        });

        let graph = GraphBuilder::new(&plugins.registry)
            .node_pair((src_id, "1.0.0"), "src")
            .node_pair(("demo_cpp2:enum_mode", "1.0.0"), "enm")
            .node_pair((sink_id, "1.0.0"), "sink")
            .connect("src:out", "enm:value")
            .connect("enm:out", "sink:x")
            .build();

        let handlers = plugins.take_handlers();
        let engine = Engine::new(EngineConfig::default()).expect("engine");
        engine.run(&plugins.registry, graph, handlers).expect("run");
        assert_eq!(
            &*seen.lock().unwrap(),
            &[serde_json::json!({"name":"B","value":{"x":7,"y":9}})]
        );
    }

    // sync groups allow missing optional port (from dylib manifest).
    {
        let src_id = "test:sync_src";
        let desc = NodeDescriptorBuilder::new(src_id)
            .output("out", TypeExpr::Scalar(ValueType::I32))
            .build()
            .unwrap();
        plugins.registry.register_node(desc).unwrap();
        plugins.handlers.on(src_id, move |_, _, io| {
            io.push_any(Some("out"), 123i32);
            Ok(())
        });

        let sink_id = "test:sink_sync";
        let desc = NodeDescriptorBuilder::new(sink_id)
            .input("x", TypeExpr::Scalar(ValueType::I32))
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
            .node_pair(("demo_cpp2:sync_a_only", "1.0.0"), "sync")
            .node_pair((sink_id, "1.0.0"), "sink")
            .connect("src:out", "sync:a")
            .connect("sync:out", "sink:x")
            .build();

        let handlers = plugins.take_handlers();
        let engine = Engine::new(EngineConfig::default()).expect("engine");
        engine.run(&plugins.registry, graph, handlers).expect("run");
        assert_eq!(&*seen.lock().unwrap(), &[123]);
    }

    // shader nodes are present but planning fails without GPU.
    {
        let gpu =
            daedalus::runtime::handles::NodeHandle::new("demo_cpp2:shader_write_u32").alias("gpu");
        let graph = daedalus::graph_builder::GraphBuilder::new(&plugins.registry)
            .node(&gpu)
            .build();
        let engine = Engine::new(EngineConfig::default()).expect("engine");
        assert!(engine.plan(&plugins.registry, graph).is_err());
    }
}

#[test]
fn cpp_library_manifest_auto_symbols_executes() {
    if std::env::var_os("DAEDALUS_TEST_CPP").is_none() {
        eprintln!("DAEDALUS_TEST_CPP not set; skipping");
        return;
    }

    let out_dir = temp_dir("rt_lib_auto");
    let lib_path =
        compile_cpp_lib_auto_symbols(&out_dir, "example_cpp_nodes_auto", "demo_cpp_auto");
    if lib_path.as_os_str().is_empty() {
        return;
    }

    let plugin = load_cpp_library_plugin(&lib_path).expect("load cpp library plugin");
    let mut plugins = PluginRegistry::new();
    plugins.install_plugin(&plugin).expect("install plugin");

    let src_id = "test:int_pair";
    let desc = NodeDescriptorBuilder::new(src_id)
        .output("a", TypeExpr::Scalar(ValueType::I32))
        .output("b", TypeExpr::Scalar(ValueType::I32))
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
        .input("x", TypeExpr::Scalar(ValueType::I32))
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
        .node_pair(("auto_cpp:add", "1.0.0"), "add")
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
