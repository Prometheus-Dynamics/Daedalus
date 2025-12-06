//! Load an input image in Rust, send it through a Python OpenCV blur node, and save the result.

use base64::Engine as _;
use daedalus::{
    NodeInstall,
    engine::{Engine, EngineConfig, RuntimeMode},
    ffi::PluginLibrary,
    graph_builder::GraphBuilder,
    macros::node,
    runtime::{NodeError, handles::NodeHandle, plugins::PluginRegistry},
};
use daedalus_data::model::{StructField, TypeExpr, ValueType};
use daedalus_registry::store::NodeDescriptorBuilder;
use image::DynamicImage;
use image::ImageEncoder;
use std::env;
use std::fs;
use std::path::PathBuf;
use std::process::Command;

#[node(id = "rust:image_source", outputs("out"))]
fn image_source() -> Result<DynamicImage, NodeError> {
    let img = load_image();
    println!(
        "image_source: loaded image {}x{}",
        img.width(),
        img.height()
    );
    Ok(img)
}

#[node(id = "rust:image_sink", inputs("img"))]
fn image_sink(img: DynamicImage) -> Result<(), NodeError> {
    let out = output_path();
    if let Some(parent) = out.parent() {
        fs::create_dir_all(parent).ok();
    }
    img.save(&out)
        .map_err(|e| NodeError::Handler(e.to_string()))?;
    println!("Saved blurred image to {}", out.display());
    Ok(())
}

fn register_image_from_json(plugins: &mut PluginRegistry) -> Result<(), NodeError> {
    let ty = TypeExpr::Struct(vec![
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
    ]);
    let desc = NodeDescriptorBuilder::new("rust:image_from_json")
        .label("rust:image_from_json")
        .input("img", ty.clone())
        .output("out", ty)
        .build()
        .map_err(|e| NodeError::Handler(e.to_string()))?;
    plugins
        .registry
        .register_node(desc)
        .map_err(|e| NodeError::Handler(e.to_string()))?;
    plugins
        .handlers
        .on("rust:image_from_json", move |_, _, io| {
            let raw = io
                .get_any_raw("img")
                .ok_or_else(|| NodeError::InvalidInput("missing img".into()))?;
            let json = raw
                .downcast_ref::<serde_json::Value>()
                .ok_or_else(|| NodeError::InvalidInput("img not json".into()))?;
            let data_b64 = json
                .get("data_b64")
                .and_then(|v| v.as_str())
                .ok_or_else(|| NodeError::Handler("missing data_b64".into()))?;
            let width = json.get("width").and_then(|v| v.as_i64()).unwrap_or(0) as u32;
            let height = json.get("height").and_then(|v| v.as_i64()).unwrap_or(0) as u32;
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(data_b64.as_bytes())
                .map_err(|e| NodeError::Handler(e.to_string()))?;
            let img =
                image::load_from_memory(&bytes).map_err(|e| NodeError::Handler(e.to_string()))?;
            if img.width() != width || img.height() != height {
                eprintln!(
                    "warning: decoded image dimensions {}x{} differ from manifest {}x{}",
                    img.width(),
                    img.height(),
                    width,
                    height
                );
            }
            io.push_any(Some("out"), img);
            Ok(())
        });
    Ok(())
}

fn register_image_to_json(plugins: &mut PluginRegistry) -> Result<(), NodeError> {
    let ty = TypeExpr::Struct(vec![
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
    ]);
    let desc = NodeDescriptorBuilder::new("rust:image_to_json")
        .label("rust:image_to_json")
        .input("img", ty.clone())
        .output("out", ty)
        .build()
        .map_err(|e| NodeError::Handler(e.to_string()))?;
    plugins
        .registry
        .register_node(desc)
        .map_err(|e| NodeError::Handler(e.to_string()))?;
    plugins.handlers.on("rust:image_to_json", move |_, _, io| {
        let img = io
            .get_any::<DynamicImage>("img")
            .ok_or_else(|| NodeError::InvalidInput("missing img".into()))?;
        let mut buf: Vec<u8> = Vec::new();
        let rgba = img.to_rgba8();
        let mut cursor = std::io::Cursor::new(&mut buf);
        image::codecs::png::PngEncoder::new(&mut cursor)
            .write_image(
                &rgba,
                rgba.width(),
                rgba.height(),
                image::ColorType::Rgba8.into(),
            )
            .map_err(|e| NodeError::Handler(e.to_string()))?;
        let b64 = base64::engine::general_purpose::STANDARD.encode(&buf);
        let json = serde_json::json!({
            "data_b64": b64,
            "width": img.width() as i64,
            "height": img.height() as i64,
            "channels": 4,
            "dtype": "u8",
            "layout": "HWC",
        });
        io.push_any(Some("out"), json);
        Ok(())
    });
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let py_plugin_path = ensure_py_image_plugin_built()?;
    let mut plugins = PluginRegistry::new();

    println!(
        "Loading Python image plugin from {}",
        py_plugin_path.display()
    );
    let py_lib = unsafe { PluginLibrary::load(&py_plugin_path)? };
    py_lib.install_into(&mut plugins)?;

    // Register Rust nodes generated by the macro.
    image_source::register(&mut plugins)?;
    image_sink::register(&mut plugins)?;
    register_image_from_json(&mut plugins)?;
    register_image_to_json(&mut plugins)?;

    let source = NodeHandle::new("rust:image_source");
    let to_json = NodeHandle::new("rust:image_to_json");
    let blur = NodeHandle::new("demo_py_image:blur");
    let convert = NodeHandle::new("rust:image_from_json");
    let sink = NodeHandle::new("rust:image_sink");

    let graph = GraphBuilder::new(&plugins.registry)
        .node(&source)
        .node(&to_json)
        .node(&blur)
        .node(&convert)
        .node(&sink)
        .connect(&source.output("out"), &to_json.input("img"))
        .connect(&to_json.output("out"), &blur.input("img"))
        .connect(&blur.output("out"), &convert.input("img"))
        .connect(&convert.output("out"), &sink.input("img"))
        .build();

    let handlers = plugins.take_handlers();
    let mut cfg = EngineConfig::default();
    cfg.runtime.mode = RuntimeMode::Parallel;

    let engine = Engine::new(cfg)?;
    let result = engine.run(&plugins.registry, graph, handlers)?;
    println!(
        "Done. Telemetry: nodes={}, cpu_segments={}, gpu_segments={}",
        result.telemetry.nodes_executed,
        result.telemetry.cpu_segments,
        result.telemetry.gpu_segments
    );
    let out = output_path();
    if out.exists() {
        println!("Blurred image written to {}", out.display());
    } else {
        eprintln!("Blurred image expected at {} but missing", out.display());
    }
    Ok(())
}

fn ensure_py_image_plugin_built() -> Result<PathBuf, Box<dyn std::error::Error>> {
    let path = py_plugin_path();
    if path.exists() {
        return Ok(path);
    }
    println!(
        "Python image plugin artifact not found at {}. Building via plugin_image.py...",
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
        .join("plugin_image.py");
    let status = Command::new("python").arg(script).status()?;
    if !status.success() {
        return Err("failed to build python image plugin".into());
    }
    if path.exists() {
        Ok(path)
    } else {
        Err("python image plugin artifact missing after build".into())
    }
}

fn py_plugin_path() -> PathBuf {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.pop(); // crates/ffi -> crates
    path.pop(); // crates -> workspace root
    path.push("target");
    path.push(current_profile());
    path.push("examples");
    let (prefix, ext) = library_naming();
    path.push(format!("{prefix}generated_py_image_plugin{ext}"));
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

fn load_image() -> DynamicImage {
    // Prefer an input.png next to this example; fallback to workspace root; otherwise synthesize one.
    let mut local = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    local.push("examples");
    local.push("input.png");
    if let Ok(img) = image::open(&local) {
        return img;
    }

    let mut root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    root.pop(); // crates/ffi -> crates
    root.pop(); // crates -> workspace root
    root.push("input.png");
    if let Ok(img) = image::open(&root) {
        return img;
    }

    // Synthesize a simple gradient image and write it to the example dir for visibility.
    let mut synthesized = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    synthesized.push("examples");
    synthesized.push("input.png");
    let img = image::RgbImage::from_fn(128, 128, |x, y| {
        let r = (x as u8).wrapping_mul(2);
        let g = (y as u8).wrapping_mul(2);
        let b = 128u8;
        image::Rgb([r, g, b])
    });
    let _ = fs::create_dir_all(synthesized.parent().unwrap());
    let _ = img.save(&synthesized);
    eprintln!(
        "input.png not found; synthesized gradient at {}",
        synthesized.display()
    );
    DynamicImage::ImageRgb8(img)
}

fn output_path() -> PathBuf {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("examples");
    path.push("blurred.png");
    path
}
