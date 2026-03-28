//! Execute a graph loaded from JSON with a single host-bridge image input.

use daedalus::{
    DataCell, PluginLibrary,
    engine::{Engine, EngineConfig, GpuBackend, RuntimeMode},
    gpu::Compute as GpuCompute,
    host_bridge::install_host_bridge,
    runtime::plugins::PluginRegistry,
    runtime::{executor::RuntimeValue, host_bridge::HostBridgeManager},
};
use daedalus_data::model::Value as DaedalusValue;
use daedalus_planner::Graph;
use daedalus_runtime::executor::Executor;
use image::{DynamicImage, GrayImage, RgbImage, RgbaImage};
use std::collections::HashSet;
use std::env;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let plugin_path = resolve_plugin_path()?;
    let graph_path = resolve_graph_path()?;
    let image_path = resolve_image_path()?;

    let graph_text = fs::read_to_string(&graph_path)?;
    let graph: Graph = serde_json::from_str(&graph_text)?;
    let graph = sanitize_graph(graph);
    let input_alias = find_host_alias_output(&graph, "frame").unwrap_or_else(|| "host".to_string());
    let output_alias = find_host_alias_input(&graph, "json").unwrap_or_else(|| input_alias.clone());

    let mut plugins = PluginRegistry::new();
    let host_mgr = HostBridgeManager::new();
    install_host_bridge(&mut plugins, host_mgr.clone())?;

    let lib = unsafe { PluginLibrary::load(std::path::Path::new(&plugin_path))? };
    lib.install_into(&mut plugins)?;

    let mut cfg = EngineConfig::default();
    cfg.runtime.mode = RuntimeMode::Serial;
    cfg.gpu = GpuBackend::Cpu;
    cfg.planner.enable_gpu = false;
    let engine = Engine::new(cfg)?;

    let plan_out = engine.plan(&plugins.registry, graph)?;
    let runtime_plan = engine.build_runtime_plan(&plan_out.plan)?;
    let mgr = HostBridgeManager::from_plan(&runtime_plan);
    let input = mgr
        .handle(&input_alias)
        .ok_or("host bridge input handle missing")?;
    let output = mgr
        .handle(&output_alias)
        .ok_or("host bridge output handle missing")?;

    let img = image::open(&image_path)
        .map_err(|e| format!("failed to open image {}: {e}", image_path.display()))?
        .to_rgb8();
    let img = DynamicImage::ImageRgb8(img);
    let ep = DataCell::from_cpu::<DynamicImage>(img);
    input.push("frame", RuntimeValue::Data(ep), None);

    let handlers = plugins.take_handlers();
    let exec = Executor::new(&runtime_plan, handlers)
        .with_host_bridges(mgr)
        .with_const_coercers(plugins.const_coercers.clone());
    let telemetry = exec.run()?;
    eprintln!("ok: executed graph, telemetry={:?}", telemetry);
    let incoming = output.incoming_port_names();
    if incoming.is_empty() {
        eprintln!("warning: no host output ports found");
    }
    for port in &incoming {
        let ty = output
            .incoming_port_type(port)
            .map(|t| format!("{t:?}"))
            .unwrap_or_else(|| "unknown".to_string());
        eprintln!("host output port: {port} ({ty})");
    }
    let output_dir = resolve_output_dir();
    if let Some(dir) = &output_dir {
        fs::create_dir_all(dir)?;
    }
    let prefix = resolve_output_prefix();

    let mut emitted = 0usize;
    let mut image_ports = HashSet::new();
    for port in &incoming {
        if port != "frame" && port != "mask" {
            continue;
        }
        let mut idx = 0usize;
        while let Some(payload) = output.try_pop(port) {
            if let Some(image) = payload_to_image(payload.inner.clone()) {
                emitted += 1;
                image_ports.insert(port.to_string());
                if let Some(dir) = &output_dir {
                    let filename = format!("{prefix}_{}_{}.png", port, idx);
                    let path = dir.join(filename);
                    image.save(&path)?;
                    eprintln!("saved image: {}", path.display());
                }
                idx += 1;
            } else {
                eprintln!(
                    "note: dropped non-image payload on {port}: {:?}",
                    payload.inner
                );
            }
        }
    }

    for port in incoming {
        if image_ports.contains(&port) {
            continue;
        }
        if port != "json" && port != "debug_json" {
            continue;
        }
        while let Some(payload) = output.try_pop_serialized(&port)? {
            emitted += 1;
            match payload.payload {
                daedalus_runtime::host_bridge::HostBridgeSerialized::Json(json) => {
                    if let Some(rendered) = unwrap_json_string(&json) {
                        println!("[{port}] {rendered}");
                    } else {
                        println!("[{port}] {json}");
                    }
                }
                daedalus_runtime::host_bridge::HostBridgeSerialized::Bytes(bytes) => {
                    let text = String::from_utf8_lossy(&bytes);
                    println!("[{port}] {text}");
                }
            }
        }
    }
    if emitted == 0 {
        eprintln!("warning: no host output payloads emitted");
    }

    Ok(())
}

fn unwrap_json_string(raw: &str) -> Option<String> {
    let parsed: serde_json::Value = serde_json::from_str(raw).ok()?;
    let obj = parsed.as_object()?;
    let ty = obj.get("type")?.as_str()?;
    if ty != "string" {
        return None;
    }
    let value = obj.get("value")?.as_str()?;
    Some(value.to_string())
}

fn find_host_alias_output(graph: &Graph, port: &str) -> Option<String> {
    let target = port.to_ascii_lowercase();
    graph.nodes.iter().find_map(|node| {
        let is_host = matches!(
            node.metadata.get("host_bridge"),
            Some(DaedalusValue::Bool(true))
        );
        if !is_host {
            return None;
        }
        if !node.outputs.iter().any(|p| p.eq_ignore_ascii_case(&target)) {
            return None;
        }
        Some(node.label.clone().unwrap_or_else(|| node.id.0.to_string()))
    })
}

fn find_host_alias_input(graph: &Graph, port: &str) -> Option<String> {
    let target = port.to_ascii_lowercase();
    graph.nodes.iter().find_map(|node| {
        let is_host = matches!(
            node.metadata.get("host_bridge"),
            Some(DaedalusValue::Bool(true))
        );
        if !is_host {
            return None;
        }
        if !node.inputs.iter().any(|p| p.eq_ignore_ascii_case(&target)) {
            return None;
        }
        Some(node.label.clone().unwrap_or_else(|| node.id.0.to_string()))
    })
}

fn sanitize_graph(mut graph: Graph) -> Graph {
    for node in &mut graph.nodes {
        if node.id.0 == "io.host_output" {
            node.id.0 = "io.host_bridge".to_string();
        }
    }

    let mut edges = Vec::with_capacity(graph.edges.len());
    for edge in graph.edges.into_iter() {
        if edge.from.port.eq_ignore_ascii_case("stats")
            || edge.to.port.eq_ignore_ascii_case("decode_stats")
        {
            continue;
        }
        edges.push(edge);
    }
    graph.edges = edges;
    graph
}

fn resolve_plugin_path() -> Result<String, Box<dyn std::error::Error>> {
    if let Ok(path) = env::var("DAEDALUS_PLUGIN_PATH") {
        return Ok(path);
    }
    let candidates = [
        "/run/media/sozo/bd1d96d9-fa81-4fac-b25e-193cfcac2dcb/Github/HeliOS/target/debug/libhelios_daedalus_cv_plugin.so",
        "/run/media/sozo/bd1d96d9-fa81-4fac-b25e-193cfcac2dcb/Github/HeliOS/target/x86_64-unknown-linux-gnu/debug/libhelios_daedalus_cv_plugin.so",
        "/run/media/sozo/bd1d96d9-fa81-4fac-b25e-193cfcac2dcb/Github/HeliOS/output/cm5/plugins/daedalus/libhelios_daedalus_cv_plugin.so",
    ];
    for candidate in candidates {
        let path = PathBuf::from(candidate);
        if path.exists() {
            return Ok(path.display().to_string());
        }
    }
    Err("DAEDALUS_PLUGIN_PATH is required (libhelios_daedalus_cv_plugin.so not found)".into())
}

fn resolve_graph_path() -> Result<String, Box<dyn std::error::Error>> {
    if let Ok(path) = env::var("DAEDALUS_GRAPH_PATH") {
        return Ok(path);
    }
    let default = PathBuf::from(
        "/run/media/sozo/bd1d96d9-fa81-4fac-b25e-193cfcac2dcb/Github/HeliOS/helios-backend/src/helios-engine/resources/daedalus_aruco_opencv_defaults.json",
    );
    if default.exists() {
        return Ok(default.display().to_string());
    }
    Err("DAEDALUS_GRAPH_PATH is required (default OpenCV graph not found)".into())
}

fn resolve_image_path() -> Result<PathBuf, Box<dyn std::error::Error>> {
    if let Ok(path) = env::var("DAEDALUS_IMG_PATH") {
        return Ok(PathBuf::from(path));
    }
    let default = PathBuf::from(
        "/run/media/sozo/bd1d96d9-fa81-4fac-b25e-193cfcac2dcb/Github/HeliOS/files/opencv_compare_now.jpg",
    );
    if default.exists() {
        return Ok(default);
    }
    Err("DAEDALUS_IMG_PATH is required (default image not found)".into())
}

fn resolve_output_dir() -> Option<PathBuf> {
    env::var("DAEDALUS_OUTPUT_DIR").ok().map(PathBuf::from)
}

fn resolve_output_prefix() -> String {
    env::var("DAEDALUS_OUTPUT_PREFIX").unwrap_or_else(|_| "stage".to_string())
}

fn payload_to_image(payload: RuntimeValue) -> Option<DynamicImage> {
    match payload {
        RuntimeValue::Any(any) => {
            if let Some(inner) = any.downcast_ref::<Arc<dyn std::any::Any + Send + Sync>>() {
                let inner_ref = inner.as_ref();
                if let Some(img) = inner_ref.downcast_ref::<DynamicImage>().cloned() {
                    return Some(img);
                }
                if let Some(img) = inner_ref.downcast_ref::<GrayImage>() {
                    return Some(DynamicImage::ImageLuma8(img.clone()));
                }
                if let Some(img) = inner_ref.downcast_ref::<RgbImage>() {
                    return Some(DynamicImage::ImageRgb8(img.clone()));
                }
                if let Some(img) = inner_ref.downcast_ref::<RgbaImage>() {
                    return Some(DynamicImage::ImageRgba8(img.clone()));
                }
                if let Some(payload) = inner_ref.downcast_ref::<GpuCompute<DynamicImage>>()
                    && let Some(cpu) = payload.as_cpu()
                {
                    return Some(cpu.clone());
                }
                if let Some(payload) = inner_ref.downcast_ref::<DataCell>()
                    && let Some(img) = payload.as_cpu::<DynamicImage>()
                {
                    return Some(img.clone());
                }
            }
            if let Some(payload) = any.downcast_ref::<DataCell>() {
                if let Some(img) = payload.as_cpu::<DynamicImage>() {
                    return Some(img.clone());
                }
                if let Some(img) = payload.try_downcast_cpu_any::<GrayImage>() {
                    return Some(DynamicImage::ImageLuma8(img));
                }
                if let Some(img) = payload.try_downcast_cpu_any::<RgbImage>() {
                    return Some(DynamicImage::ImageRgb8(img));
                }
                if let Some(img) = payload.try_downcast_cpu_any::<RgbaImage>() {
                    return Some(DynamicImage::ImageRgba8(img));
                }
            }
            if let Some(img) = any.downcast_ref::<DynamicImage>().cloned() {
                return Some(img);
            }
            if let Some(img) = any.downcast_ref::<Arc<DynamicImage>>() {
                return Some((**img).clone());
            }
            if let Some(img) = any.downcast_ref::<GrayImage>() {
                return Some(DynamicImage::ImageLuma8(img.clone()));
            }
            if let Some(img) = any.downcast_ref::<RgbImage>() {
                return Some(DynamicImage::ImageRgb8(img.clone()));
            }
            if let Some(img) = any.downcast_ref::<RgbaImage>() {
                return Some(DynamicImage::ImageRgba8(img.clone()));
            }
            if let Some(payload) = any.downcast_ref::<GpuCompute<DynamicImage>>()
                && let Some(cpu) = payload.as_cpu()
            {
                return Some(cpu.clone());
            }
            None
        }
        RuntimeValue::Data(payload) => {
            if let Some(img) = payload.as_cpu::<DynamicImage>() {
                return Some(img.clone());
            }
            if let Some(img) = payload.try_downcast_cpu_any::<GrayImage>() {
                return Some(DynamicImage::ImageLuma8(img));
            }
            if let Some(img) = payload.try_downcast_cpu_any::<RgbImage>() {
                return Some(DynamicImage::ImageRgb8(img));
            }
            if let Some(img) = payload.try_downcast_cpu_any::<RgbaImage>() {
                return Some(DynamicImage::ImageRgba8(img));
            }
            None
        }
        _ => None,
    }
}
