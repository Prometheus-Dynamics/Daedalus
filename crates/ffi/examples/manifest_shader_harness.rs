//! GPU shader manifest harness (runs in a subprocess to avoid crashing the test runner).
//!
//! Run with:
//! `DAEDALUS_TEST_GPU=1 cargo run -p daedalus-ffi --features gpu-wgpu --example manifest_shader_harness -- python`
//! `DAEDALUS_TEST_GPU=1 cargo run -p daedalus-ffi --features gpu-wgpu --example manifest_shader_harness -- node`
//! `DAEDALUS_TEST_GPU=1 cargo run -p daedalus-ffi --features gpu-wgpu --example manifest_shader_harness -- java`

#[cfg(feature = "gpu-wgpu")]
use std::path::{Path, PathBuf};
#[cfg(feature = "gpu-wgpu")]
use std::process::Command;

#[cfg(feature = "gpu-wgpu")]
use daedalus::{
    engine::{Engine, EngineConfig, GpuBackend},
    graph_builder::GraphBuilder,
    runtime::plugins::{PluginRegistry, RegistryPluginExt},
};
#[cfg(feature = "gpu-wgpu")]
use daedalus_data::model::{StructField, TypeExpr, ValueType};
#[cfg(feature = "gpu-wgpu")]
use daedalus_ffi::load_manifest_plugin;
#[cfg(feature = "gpu-wgpu")]
use daedalus_registry::store::NodeDescriptorBuilder;
#[cfg(feature = "gpu-wgpu")]
use daedalus_runtime::NodeError;

#[cfg(feature = "gpu-wgpu")]
#[derive(Clone, Copy, Debug)]
enum Lang {
    Python,
    Java,
    Node,
}

#[cfg(feature = "gpu-wgpu")]
fn workspace_root() -> PathBuf {
    let mut root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    root.pop(); // crates/ffi
    root.pop(); // crates
    root
}

#[cfg(feature = "gpu-wgpu")]
fn temp_dir(prefix: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "daedalus_manifest_shader_harness_{prefix}_{nanos}_{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

#[cfg(feature = "gpu-wgpu")]
fn emit_manifest(lang: Lang, out_path: &Path) {
    match lang {
        Lang::Python => {
            let python = std::env::var("PYTHON").unwrap_or_else(|_| "python".to_string());
            let script =
                workspace_root().join("crates/ffi/lang/python/examples/emit_manifest_features.py");
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
            let ex_dir = workspace_root().join("crates/ffi/lang/java/examples");
            let mut sources = collect_java_sources(&sdk_dir);
            sources.extend(collect_java_sources(&ex_dir));

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
            let script =
                workspace_root().join("crates/ffi/lang/node/examples/emit_manifest_features.mjs");
            let status = Command::new(&node)
                .arg(script)
                .arg(out_path)
                .status()
                .expect("run node manifest emitter");
            assert!(status.success());
        }
    }
}

#[cfg(feature = "gpu-wgpu")]
fn install_manifest(manifest_path: &Path) -> PluginRegistry {
    let plugin = load_manifest_plugin(manifest_path).expect("load manifest plugin");
    let mut plugins = PluginRegistry::new();
    plugins
        .install_plugin(&plugin)
        .expect("install manifest plugin");
    plugins
}

#[cfg(feature = "gpu-wgpu")]
fn image_type_expr() -> TypeExpr {
    TypeExpr::Struct(vec![
        StructField {
            name: "data_b64".into(),
            ty: TypeExpr::Scalar(ValueType::String),
        },
        StructField {
            name: "width".into(),
            ty: TypeExpr::Scalar(ValueType::Int),
        },
        StructField {
            name: "height".into(),
            ty: TypeExpr::Scalar(ValueType::Int),
        },
        StructField {
            name: "channels".into(),
            ty: TypeExpr::Scalar(ValueType::Int),
        },
        StructField {
            name: "dtype".into(),
            ty: TypeExpr::Scalar(ValueType::String),
        },
        StructField {
            name: "layout".into(),
            ty: TypeExpr::Scalar(ValueType::String),
        },
    ])
}

#[cfg(feature = "gpu-wgpu")]
fn make_test_image_payload() -> serde_json::Value {
    use base64::Engine as _;
    use image::ImageEncoder;

    let mut img = image::RgbaImage::new(4, 4);
    for y in 0..4 {
        for x in 0..4 {
            img.put_pixel(x, y, image::Rgba([x as u8 * 10, y as u8 * 20, 30, 255]));
        }
    }
    let dyn_img = image::DynamicImage::ImageRgba8(img);
    let mut buf: Vec<u8> = Vec::new();
    let rgba = dyn_img.to_rgba8();
    let mut cursor = std::io::Cursor::new(&mut buf);
    image::codecs::png::PngEncoder::new(&mut cursor)
        .write_image(
            &rgba,
            rgba.width(),
            rgba.height(),
            image::ColorType::Rgba8.into(),
        )
        .expect("encode png");
    let b64 = base64::engine::general_purpose::STANDARD.encode(&buf);
    serde_json::json!({
        "data_b64": b64,
        "width": 4,
        "height": 4,
        "channels": 4,
        "dtype": "u8",
        "layout": "HWC",
    })
}

#[cfg(feature = "gpu-wgpu")]
fn decode_payload_rgba(payload: &serde_json::Value) -> Vec<u8> {
    use base64::Engine as _;
    let b64 = payload.get("data_b64").unwrap().as_str().unwrap();
    let png = base64::engine::general_purpose::STANDARD
        .decode(b64.as_bytes())
        .expect("decode base64");
    let img = image::load_from_memory(&png)
        .expect("decode png")
        .to_rgba8();
    img.into_raw()
}

#[cfg(feature = "gpu-wgpu")]
fn main() {
    if std::env::var_os("DAEDALUS_TEST_GPU").is_none() {
        eprintln!("DAEDALUS_TEST_GPU not set; nothing to do");
        return;
    }

    let lang = match std::env::args().nth(1).as_deref() {
        Some("python") => Lang::Python,
        Some("java") => Lang::Java,
        Some("node") => Lang::Node,
        other => {
            eprintln!("usage: manifest_shader_harness <python|node|java> (got {other:?})");
            std::process::exit(2);
        }
    };

    let prefix = match lang {
        Lang::Python => "demo_py_feat",
        Lang::Java => "demo_java_feat",
        Lang::Node => "demo_node_feat",
    };

    let out_dir = temp_dir(prefix);
    let manifest_path = out_dir.join(format!("{prefix}.manifest.json"));
    emit_manifest(lang, &manifest_path);
    let mut plugins = install_manifest(&manifest_path);

    let backend = GpuBackend::Device;

    // 1) Image shader invert: validate alpha unchanged + RGB inverted.
    {
        eprintln!("[harness] 1) image invert");
        let img_ty = image_type_expr();
        let input_payload = make_test_image_payload();
        let expected_in = decode_payload_rgba(&input_payload);

        let src_id = "test:image_source";
        let desc = NodeDescriptorBuilder::new(src_id)
            .output("img", img_ty.clone())
            .build()
            .unwrap();
        plugins.registry.register_node(desc).unwrap();
        plugins.handlers.on(src_id, move |_, _, io| {
            io.push_any(Some("img"), input_payload.clone());
            Ok(())
        });

        let sink_id = "test:image_sink";
        let desc = NodeDescriptorBuilder::new(sink_id)
            .input("img", img_ty)
            .build()
            .unwrap();
        plugins.registry.register_node(desc).unwrap();
        let expected_in = std::sync::Arc::new(expected_in);
        plugins.handlers.on(sink_id, move |_, _, io| {
            let out = io
                .get_any::<serde_json::Value>("img")
                .ok_or_else(|| NodeError::InvalidInput("missing img".into()))?;
            let out_rgba = decode_payload_rgba(&out);
            let inp = expected_in.as_ref();
            for (i, (&a, &b)) in inp.iter().zip(out_rgba.iter()).enumerate() {
                let ch = i % 4;
                let expected = if ch == 3 { a } else { 255u8.saturating_sub(a) };
                assert_eq!(b, expected);
            }
            Ok(())
        });

        let shader_id = format!("{prefix}:shader_invert");
        let graph = GraphBuilder::new(&plugins.registry)
            .node_pair((src_id, "1.0.0"), "src")
            .node_pair((shader_id.as_str(), "1.0.0"), "shader")
            .node_pair((sink_id, "1.0.0"), "sink")
            .connect("src:img", "shader:img")
            .connect("shader:img", "sink:img")
            .build();

        let handlers = plugins.take_handlers();
        let mut cfg = EngineConfig::default();
        cfg.planner.enable_gpu = true;
        cfg.gpu = backend.clone();
        let engine = Engine::new(cfg).expect("engine");
        engine.run(&plugins.registry, graph, handlers).expect("run");
    }

    // 2) Buffer-only shader: emits 0x12345678 into a storage buffer and returns it as Bytes.
    {
        eprintln!("[harness] 2) buffer write_u32");
        let mut plugins = install_manifest(&manifest_path);
        let out_ty = TypeExpr::Scalar(ValueType::Bytes);
        let sink_id = "test:bytes_sink";
        let desc = NodeDescriptorBuilder::new(sink_id)
            .input("out", out_ty)
            .build()
            .unwrap();
        plugins.registry.register_node(desc).unwrap();
        plugins.handlers.on(sink_id, move |_, _, io| {
            let bytes = io
                .get_any::<Vec<u8>>("out")
                .ok_or_else(|| NodeError::InvalidInput("missing out".into()))?;
            assert_eq!(&bytes, &[0x78, 0x56, 0x34, 0x12]);
            Ok(())
        });

        let shader_id = format!("{prefix}:shader_write_u32");
        let graph = GraphBuilder::new(&plugins.registry)
            .node_pair((shader_id.as_str(), "1.0.0"), "shader")
            .node_pair((sink_id, "1.0.0"), "sink")
            .connect("shader:out", "sink:out")
            .build();

        let handlers = plugins.take_handlers();
        let mut cfg = EngineConfig::default();
        cfg.planner.enable_gpu = true;
        cfg.gpu = backend.clone();
        let engine = Engine::new(cfg).expect("engine");
        engine.run(&plugins.registry, graph, handlers).expect("run");
    }

    // 3) Stateful shader: counter increments using from_state/to_state.
    {
        eprintln!("[harness] 3) stateful counter (cpu bytes)");
        let mut plugins = install_manifest(&manifest_path);
        let out_ty = TypeExpr::Scalar(ValueType::Bytes);
        let sink_id = "test:counter_sink";
        let desc = NodeDescriptorBuilder::new(sink_id)
            .input("out", out_ty)
            .build()
            .unwrap();
        plugins.registry.register_node(desc).unwrap();
        let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::<u32>::new()));
        let seen_cloned = seen.clone();
        plugins.handlers.on(sink_id, move |_, _, io| {
            let bytes = io
                .get_any::<Vec<u8>>("out")
                .ok_or_else(|| NodeError::InvalidInput("missing out".into()))?;
            if bytes.len() < 4 {
                return Err(NodeError::InvalidInput("expected 4 bytes".into()));
            }
            let v = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
            seen_cloned.lock().unwrap().push(v);
            Ok(())
        });

        let shader_id = format!("{prefix}:shader_counter_cpu");
        let graph = GraphBuilder::new(&plugins.registry)
            .node_pair((shader_id.as_str(), "1.0.0"), "shader")
            .node_pair((sink_id, "1.0.0"), "sink")
            .connect("shader:out", "sink:out")
            .build();

        let handlers = plugins.take_handlers();
        let mut cfg = EngineConfig::default();
        cfg.planner.enable_gpu = true;
        cfg.gpu = backend.clone();
        let engine = Engine::new(cfg).expect("engine");
        engine
            .run(&plugins.registry, graph.clone(), handlers.clone_arc())
            .expect("run1");
        engine
            .run(&plugins.registry, graph, handlers.clone_arc())
            .expect("run2");
        let got = seen.lock().unwrap().clone();
        assert_eq!(got.len(), 2);
        assert!(got[1] > got[0]);
    }

    // 4) GPU-resident state: counter increments without to_state/readback persistence.
    {
        eprintln!("[harness] 4) stateful counter (gpu buffer)");
        let mut plugins = install_manifest(&manifest_path);
        let out_ty = TypeExpr::Scalar(ValueType::Bytes);
        let sink_id = "test:counter_gpu_sink";
        let desc = NodeDescriptorBuilder::new(sink_id)
            .input("out", out_ty)
            .build()
            .unwrap();
        plugins.registry.register_node(desc).unwrap();
        let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::<u32>::new()));
        let seen_cloned = seen.clone();
        plugins.handlers.on(sink_id, move |_, _, io| {
            let bytes = io
                .get_any::<Vec<u8>>("out")
                .ok_or_else(|| NodeError::InvalidInput("missing out".into()))?;
            if bytes.len() < 4 {
                return Err(NodeError::InvalidInput("expected 4 bytes".into()));
            }
            let v = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
            seen_cloned.lock().unwrap().push(v);
            Ok(())
        });

        let shader_id = format!("{prefix}:shader_counter_gpu");
        let graph = GraphBuilder::new(&plugins.registry)
            .node_pair((shader_id.as_str(), "1.0.0"), "shader")
            .node_pair((sink_id, "1.0.0"), "sink")
            .connect("shader:out", "sink:out")
            .build();

        let handlers = plugins.take_handlers();
        let mut cfg = EngineConfig::default();
        cfg.planner.enable_gpu = true;
        cfg.gpu = backend.clone();
        let engine = Engine::new(cfg).expect("engine");
        engine
            .run(&plugins.registry, graph.clone(), handlers.clone_arc())
            .expect("run1");
        engine
            .run(&plugins.registry, graph, handlers.clone_arc())
            .expect("run2");
        let got = seen.lock().unwrap().clone();
        assert_eq!(got, vec![1, 2]);
    }

    // 5) Multi-shader dispatch by name via an input port.
    {
        eprintln!("[harness] 5) multi shader dispatch");
        let mut plugins = install_manifest(&manifest_path);

        let src_id = "test:mode_source";
        let desc = NodeDescriptorBuilder::new(src_id)
            .output("which", TypeExpr::Scalar(ValueType::String))
            .build()
            .unwrap();
        plugins.registry.register_node(desc).unwrap();
        let modes = std::sync::Arc::new(std::sync::Mutex::new(vec![
            "one".to_string(),
            "two".to_string(),
        ]));
        let modes_cloned = modes.clone();
        plugins.handlers.on(src_id, move |_, _, io| {
            let mut guard = modes_cloned.lock().unwrap();
            let v = guard.remove(0);
            io.push_any(Some("which"), v);
            Ok(())
        });

        let sink_id = "test:multi_sink";
        let desc = NodeDescriptorBuilder::new(sink_id)
            .input("out", TypeExpr::Scalar(ValueType::Bytes))
            .build()
            .unwrap();
        plugins.registry.register_node(desc).unwrap();
        let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::<u32>::new()));
        let seen_cloned = seen.clone();
        plugins.handlers.on(sink_id, move |_, _, io| {
            let bytes = io
                .get_any::<Vec<u8>>("out")
                .ok_or_else(|| NodeError::InvalidInput("missing out".into()))?;
            if bytes.len() < 4 {
                return Err(NodeError::InvalidInput("expected 4 bytes".into()));
            }
            let v = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
            seen_cloned.lock().unwrap().push(v);
            Ok(())
        });

        let shader_id = format!("{prefix}:shader_multi_write");
        let graph = GraphBuilder::new(&plugins.registry)
            .node_pair((src_id, "1.0.0"), "src")
            .node_pair((shader_id.as_str(), "1.0.0"), "shader")
            .node_pair((sink_id, "1.0.0"), "sink")
            .connect("src:which", "shader:which")
            .connect("shader:out", "sink:out")
            .build();

        let handlers = plugins.take_handlers();
        let mut cfg = EngineConfig::default();
        cfg.planner.enable_gpu = true;
        cfg.gpu = backend.clone();
        let engine = Engine::new(cfg).expect("engine");
        engine
            .run(&plugins.registry, graph.clone(), handlers.clone_arc())
            .expect("run1");
        engine
            .run(&plugins.registry, graph, handlers.clone_arc())
            .expect("run2");
        let got = seen.lock().unwrap().clone();
        assert_eq!(got, vec![1, 2]);
    }
}

#[cfg(not(feature = "gpu-wgpu"))]
fn main() {
    eprintln!("enable `--features gpu-wgpu` to run this harness");
}
