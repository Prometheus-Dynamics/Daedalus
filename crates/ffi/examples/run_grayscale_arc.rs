//! Build + load the `plugin_grayscale_arc` Rust cdylib and send a CPU DynamicImage payload.

use daedalus::{
    ErasedPayload, NodeHandle, PluginLibrary, PortHandle,
    engine::{Engine, EngineConfig, GpuBackend, RuntimeMode},
    graph_builder::GraphBuilder,
    host_bridge::install_host_bridge,
    runtime::plugins::PluginRegistry,
    runtime::{executor::EdgePayload, host_bridge::HostBridgeManager},
};
use image::{DynamicImage, ImageBuffer, Rgba};
use std::env;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let plugin_path = plugin_dylib_path();
    eprintln!("Loading grayscale plugin from {}", plugin_path.display());

    let mut plugins = PluginRegistry::new();
    let host_mgr = HostBridgeManager::new();
    install_host_bridge(&mut plugins, host_mgr.clone())?;

    let lib = unsafe { PluginLibrary::load(&plugin_path)? };
    lib.install_into(&mut plugins)?;

    let node_id = env::var("DAEDALUS_NODE_ID")
        .unwrap_or_else(|_| "ffi.cv_grayscale_arc:grayscale_arc".to_string());
    let grayscale = NodeHandle::new(&node_id).alias("gray");

    let graph = GraphBuilder::new(&plugins.registry)
        .host_bridge("host")
        .node(&grayscale)
        .connect(&PortHandle::new("host", "frame"), &grayscale.input("frame"))
        .connect(&grayscale.output("mask"), &PortHandle::new("host", "mask"))
        .const_input(
            &grayscale.input("mode"),
            Some(daedalus::data::model::Value::Int(0)),
        )
        .build();

    let mut cfg = EngineConfig::default();
    cfg.runtime.mode = RuntimeMode::Serial;
    cfg.gpu = GpuBackend::Cpu;
    cfg.planner.enable_gpu = false;
    let engine = Engine::new(cfg)?;

    let plan_out = engine.plan(&plugins.registry, graph)?;
    let runtime_plan = engine.build_runtime_plan(&plan_out.plan)?;
    let mgr = HostBridgeManager::from_plan(&runtime_plan);
    let handle = mgr.handle("host").expect("host bridge handle");

    let width = env::var("DAEDALUS_IMG_W")
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(2);
    let height = env::var("DAEDALUS_IMG_H")
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(2);
    let img =
        DynamicImage::ImageRgba8(ImageBuffer::from_pixel(width, height, Rgba([7, 8, 9, 255])));
    let ep = ErasedPayload::from_cpu::<DynamicImage>(img);
    handle.push("frame", EdgePayload::Payload(ep), None);

    let handlers = plugins.take_handlers();
    let telemetry = engine.execute_with_host(runtime_plan, mgr, handlers)?;
    eprintln!("ok: executed grayscale arc, telemetry={:?}", telemetry);

    Ok(())
}

fn plugin_dylib_path() -> PathBuf {
    if let Ok(path) = env::var("DAEDALUS_PLUGIN_PATH") {
        return PathBuf::from(path);
    }
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.pop(); // crates/ffi -> crates
    path.pop(); // crates -> workspace root
    path.push("target");
    path.push(current_profile());
    path.push("examples");
    let (prefix, ext) = library_naming();
    path.push(format!("{prefix}plugin_grayscale_arc{ext}"));
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
