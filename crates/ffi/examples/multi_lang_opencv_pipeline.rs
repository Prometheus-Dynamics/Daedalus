//! Build + load OpenCV plugins in multiple languages, run a single graph, and save each stage.
//!
//! Stages (in order):
//! - Rust loads `crates/ffi/examples/input.png`
//! - Python (OpenCV): `demo_py_image:blur`
//! - Node (OpenCV): `demo_node_opencv:blur`
//! - Java (OpenCV): `demo_java_opencv:blur`
//! - C++ (OpenCV): `demo_cpp_opencv:blur`
//! - Rust saves each stage as PNG
//!
//! Run:
//!   cargo run -p daedalus-ffi --example multi_lang_opencv_pipeline --features "engine,plugins,ffi" -- /tmp/daedalus_multi_lang
//!
//! Requirements (runtime):
//! - Python + OpenCV (`cv2`) for the Python stage
//! - Node + `opencv4nodejs` for the Node stage
//! - Java + OpenCV Java bindings (set `OPENCV_JAR=/path/to/opencv.jar`)
//! - C++ OpenCV dev libs (pkg-config `opencv4` or `opencv`)

use base64::Engine as _;
use daedalus::{
    NodeInstall,
    data::model::{StructField, TypeExpr, ValueType},
    engine::{Engine, EngineConfig, RuntimeMode},
    ffi::PluginLibrary,
    graph_builder::GraphBuilder,
    macros::node,
    runtime::{
        NodeError,
        handles::NodeHandle,
        plugins::{PluginRegistry, RegistryPluginExt},
    },
};
use daedalus_ffi::load_cpp_library_plugin;
use daedalus_registry::store::NodeDescriptorBuilder;
use image::DynamicImage;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

fn dynamic_image_to_json(img: &DynamicImage) -> serde_json::Value {
    // Prefer the fast path: raw RGBA8 bytes (no PNG encoding).
    let rgba = img.to_rgba8();
    let bytes = rgba.into_raw();
    let b64 = base64::engine::general_purpose::STANDARD.encode(&bytes);
    serde_json::json!({
        "data_b64": b64,
        "width": img.width() as i64,
        "height": img.height() as i64,
        "channels": 4,
        "dtype": "u8",
        "layout": "HWC",
        "encoding": "raw",
    })
}

fn classpath_sep() -> char {
    #[cfg(target_os = "windows")]
    {
        ';'
    }
    #[cfg(not(target_os = "windows"))]
    {
        ':'
    }
}

fn image_ty() -> TypeExpr {
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
        StructField {
            name: "encoding".into(),
            ty: TypeExpr::Scalar(ValueType::String),
        },
    ])
}

fn output_dir() -> PathBuf {
    env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| env::temp_dir().join("daedalus_multi_lang_opencv"))
}

fn input_path() -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("examples");
    p.push("input.png");
    p
}

fn load_image() -> DynamicImage {
    image::open(input_path()).expect("failed to open crates/ffi/examples/input.png")
}

fn decode_image_from_json(value: serde_json::Value) -> Result<DynamicImage, NodeError> {
    let obj = value
        .as_object()
        .ok_or_else(|| NodeError::InvalidInput("expected image json object".into()))?;
    let b64 = obj
        .get("data_b64")
        .and_then(|v| v.as_str())
        .ok_or_else(|| NodeError::InvalidInput("missing data_b64".into()))?;
    let width = obj
        .get("width")
        .and_then(|v| v.as_i64())
        .ok_or_else(|| NodeError::InvalidInput("missing width".into()))? as u32;
    let height = obj
        .get("height")
        .and_then(|v| v.as_i64())
        .ok_or_else(|| NodeError::InvalidInput("missing height".into()))? as u32;
    let channels = obj.get("channels").and_then(|v| v.as_i64()).unwrap_or(4) as usize;
    let encoding = obj
        .get("encoding")
        .and_then(|v| v.as_str())
        .unwrap_or("raw");

    let bytes = base64::engine::general_purpose::STANDARD
        .decode(b64.as_bytes())
        .map_err(|e| NodeError::Handler(e.to_string()))?;

    if encoding.eq_ignore_ascii_case("png") {
        return image::load_from_memory(&bytes).map_err(|e| NodeError::Handler(e.to_string()));
    }

    let expected = (width as usize)
        .checked_mul(height as usize)
        .and_then(|n| n.checked_mul(channels))
        .ok_or_else(|| NodeError::InvalidInput("image size overflow".into()))?;
    if bytes.len() < expected {
        return Err(NodeError::InvalidInput(format!(
            "raw payload too small: got {} expected {}",
            bytes.len(),
            expected
        )));
    }

    let img = match channels {
        4 => {
            let rgba = image::RgbaImage::from_raw(width, height, bytes[..expected].to_vec())
                .ok_or_else(|| NodeError::Handler("failed to create rgba image".into()))?;
            DynamicImage::ImageRgba8(rgba)
        }
        3 => {
            let rgb = image::RgbImage::from_raw(width, height, bytes[..expected].to_vec())
                .ok_or_else(|| NodeError::Handler("failed to create rgb image".into()))?;
            DynamicImage::ImageRgb8(rgb)
        }
        1 => {
            let luma = image::GrayImage::from_raw(width, height, bytes[..expected].to_vec())
                .ok_or_else(|| NodeError::Handler("failed to create gray image".into()))?;
            DynamicImage::ImageLuma8(luma)
        }
        other => {
            return Err(NodeError::InvalidInput(format!(
                "unsupported channels={other}"
            )));
        }
    };
    Ok(img)
}

fn save_stage(stage: &str, img: &DynamicImage) -> Result<(), NodeError> {
    let out_dir = output_dir();
    fs::create_dir_all(&out_dir).map_err(|e| NodeError::Handler(e.to_string()))?;
    let out = out_dir.join(format!("{stage}.png"));
    img.save(&out)
        .map_err(|e| NodeError::Handler(e.to_string()))?;
    println!("saved {stage} -> {}", out.display());
    Ok(())
}

#[node(id = "rust:image_source", outputs("out"))]
fn image_source() -> Result<DynamicImage, NodeError> {
    Ok(load_image())
}

fn register_image_to_json(plugins: &mut PluginRegistry) -> Result<(), Box<dyn std::error::Error>> {
    let desc = NodeDescriptorBuilder::new("rust:image_to_json")
        .input("img", TypeExpr::opaque("image:dynamic"))
        .output("out", image_ty())
        .build()
        .unwrap();
    plugins.registry.register_node(desc)?;

    plugins.handlers.on("rust:image_to_json", move |_, _, io| {
        let img = io
            .get_any::<DynamicImage>("img")
            .ok_or_else(|| NodeError::InvalidInput("missing img".into()))?;
        io.push_any(Some("out"), dynamic_image_to_json(&img));
        Ok(())
    });
    Ok(())
}

fn register_save_node(
    plugins: &mut PluginRegistry,
    id: &'static str,
    stage: &'static str,
) -> Result<(), Box<dyn std::error::Error>> {
    let desc = NodeDescriptorBuilder::new(id)
        .input("img", image_ty())
        .build()
        .unwrap();
    plugins.registry.register_node(desc)?;

    plugins.handlers.on(id, move |_, _, io| {
        let v = io
            .get_any::<serde_json::Value>("img")
            .ok_or_else(|| NodeError::InvalidInput("missing img".into()))?;
        let img = decode_image_from_json(v)?;
        save_stage(stage, &img)?;
        Ok(())
    });
    Ok(())
}

fn build_python_plugin() -> Result<PathBuf, Box<dyn std::error::Error>> {
    let python = env::var("PYTHON").unwrap_or_else(|_| "python".to_string());
    let script =
        workspace_root().join("crates/ffi/lang/python/examples/build_plugin_image/main.py");
    let out = Command::new(&python)
        .arg(script)
        .output()
        .expect("run python build");
    if !out.status.success() {
        return Err(format!(
            "python build failed: {}",
            String::from_utf8_lossy(&out.stderr)
        )
        .into());
    }
    let stdout = String::from_utf8_lossy(&out.stdout);
    let path = stdout
        .lines()
        .rev()
        .find_map(|line| line.strip_prefix("Built plugin to "))
        .map(|s| PathBuf::from(s.trim()))
        .ok_or("failed to parse python artifact path")?;
    Ok(path)
}

fn build_node_plugin(out_path: &Path) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let node = env::var("NODE").unwrap_or_else(|_| "node".to_string());
    let script =
        workspace_root().join("crates/ffi/lang/node/examples/opencv_demo/build_plugin.mjs");
    let out = Command::new(&node)
        .arg(script)
        .arg(out_path)
        .output()
        .expect("run node build");
    if !out.status.success() {
        return Err(format!(
            "node build failed: {}",
            String::from_utf8_lossy(&out.stderr)
        )
        .into());
    }
    let stdout = String::from_utf8_lossy(&out.stdout);
    Ok(PathBuf::from(stdout.trim()))
}

fn build_java_plugin(out_path: &Path) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let javac = env::var("JAVAC").unwrap_or_else(|_| "javac".to_string());
    let java = env::var("JAVA").unwrap_or_else(|_| "java".to_string());
    let opencv_jar = env::var("OPENCV_JAR").map(PathBuf::from)?;
    let classes = output_dir().join("java_classes");
    fs::create_dir_all(&classes)?;

    let sdk_dir = workspace_root().join("crates/ffi/lang/java/sdk");
    let ex_dir = workspace_root().join("crates/ffi/lang/java/examples/opencv_demo");
    let sources = collect_java_sources(&sdk_dir)
        .into_iter()
        .chain(collect_java_sources(&ex_dir))
        .collect::<Vec<_>>();

    let mut cmd = Command::new(&javac);
    cmd.arg("-cp").arg(&opencv_jar).arg("-d").arg(&classes);
    for src in &sources {
        cmd.arg(src);
    }
    let status = cmd.status()?;
    if !status.success() {
        return Err("javac failed".into());
    }

    let classpath = format!(
        "{}{}{}",
        classes.display(),
        classpath_sep(),
        opencv_jar.display()
    );
    let out = Command::new(&java)
        .arg("-cp")
        .arg(classpath)
        .arg("daedalus.examples.BuildPluginOpenCvDemo")
        .arg(out_path)
        .output()
        .expect("run java builder");
    if !out.status.success() {
        return Err(format!(
            "java build failed: {}",
            String::from_utf8_lossy(&out.stderr)
        )
        .into());
    }
    Ok(PathBuf::from(String::from_utf8_lossy(&out.stdout).trim()))
}

fn build_cpp_plugin(out_dir: &Path) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let script = workspace_root().join("crates/ffi/lang/c_cpp/examples/opencv_demo/build.sh");
    let status = Command::new("bash").arg(script).arg(out_dir).status()?;
    if !status.success() {
        return Err("c++ build failed".into());
    }
    Ok(out_dir.join(format!("libdemo_cpp_opencv{}", lib_ext())))
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

fn workspace_root() -> PathBuf {
    let mut root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    root.pop(); // crates/ffi
    root.pop(); // crates
    root
}

fn lib_ext() -> &'static str {
    #[cfg(target_os = "windows")]
    {
        ".dll"
    }
    #[cfg(target_os = "macos")]
    {
        ".dylib"
    }
    #[cfg(all(not(target_os = "windows"), not(target_os = "macos")))]
    {
        ".so"
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = output_dir();
    fs::create_dir_all(&out_dir)?;

    let py_so = build_python_plugin()?;
    let node_so = build_node_plugin(&out_dir.join(format!("demo_node_opencv{}", lib_ext())))?;
    let java_so = build_java_plugin(&out_dir.join(format!("demo_java_opencv{}", lib_ext())))?;
    let cpp_so = build_cpp_plugin(&out_dir.join("cpp"))?;

    let mut plugins = PluginRegistry::new();

    println!("loading python plugin: {}", py_so.display());
    unsafe { PluginLibrary::load(&py_so) }?.install_into(&mut plugins)?;
    println!("loading node plugin: {}", node_so.display());
    unsafe { PluginLibrary::load(&node_so) }?.install_into(&mut plugins)?;
    println!("loading java plugin: {}", java_so.display());
    unsafe { PluginLibrary::load(&java_so) }?.install_into(&mut plugins)?;

    println!("loading c++ plugin: {}", cpp_so.display());
    let cpp_plugin = load_cpp_library_plugin(&cpp_so)?;
    plugins.install_plugin(&cpp_plugin)?;

    // Rust nodes.
    image_source::register(&mut plugins)?;
    register_image_to_json(&mut plugins)?;
    register_save_node(&mut plugins, "rust:save_py", "py")?;
    register_save_node(&mut plugins, "rust:save_node", "node")?;
    register_save_node(&mut plugins, "rust:save_java", "java")?;
    register_save_node(&mut plugins, "rust:save_cpp", "cpp")?;

    let source = NodeHandle::new("rust:image_source").alias("src");
    let to_json = NodeHandle::new("rust:image_to_json").alias("to_json");
    let py = NodeHandle::new("demo_py_image:blur").alias("py");
    let node = NodeHandle::new("demo_node_opencv:blur").alias("node");
    let java = NodeHandle::new("demo_java_opencv:blur").alias("java");
    let cpp = NodeHandle::new("demo_cpp_opencv:blur").alias("cpp");

    let save_py = NodeHandle::new("rust:save_py").alias("save_py");
    let save_node = NodeHandle::new("rust:save_node").alias("save_node");
    let save_java = NodeHandle::new("rust:save_java").alias("save_java");
    let save_cpp = NodeHandle::new("rust:save_cpp").alias("save_cpp");

    let graph = GraphBuilder::new(&plugins.registry)
        .node(&source)
        .node(&to_json)
        .node(&py)
        .node(&node)
        .node(&java)
        .node(&cpp)
        .node(&save_py)
        .node(&save_node)
        .node(&save_java)
        .node(&save_cpp)
        .connect(&source.output("out"), &to_json.input("img"))
        .connect(&to_json.output("out"), &py.input("img"))
        .connect(&py.output("out"), &save_py.input("img"))
        .connect(&py.output("out"), &node.input("img"))
        .connect(&node.output("out"), &save_node.input("img"))
        .connect(&node.output("out"), &java.input("img"))
        .connect(&java.output("out"), &save_java.input("img"))
        .connect(&java.output("out"), &cpp.input("img"))
        .connect(&cpp.output("out"), &save_cpp.input("img"))
        .build();

    let handlers = plugins.take_handlers();
    let mut cfg = EngineConfig::default();
    cfg.runtime.mode = RuntimeMode::Parallel;

    let engine = Engine::new(cfg)?;
    let result = engine.run(&plugins.registry, graph, handlers)?;
    println!(
        "done: nodes_executed={}, cpu_segments={}, gpu_segments={}",
        result.telemetry.nodes_executed,
        result.telemetry.cpu_segments,
        result.telemetry.gpu_segments
    );
    Ok(())
}
