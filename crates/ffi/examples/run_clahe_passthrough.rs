//! Build + load the `plugin_clahe_passthrough` Rust cdylib and send a CPU DynamicImage payload
//! into it via the host bridge, verifying the node can decode `Payload<DynamicImage>`.

use daedalus::runtime::executor::EdgePayload;
use daedalus::{
    ErasedPayload, NodeHandle, PluginLibrary, PortHandle,
    data::model::Value,
    engine::{Engine, EngineConfig, GpuBackend, RuntimeMode},
    graph_builder::GraphBuilder,
    host_bridge::install_host_bridge,
    runtime::host_bridge::HostBridgeManager,
    runtime::plugins::PluginRegistry,
};
use image::{DynamicImage, ImageBuffer, Rgba};
use std::env;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let plugin_path = plugin_dylib_path();
    eprintln!("Loading passthrough plugin from {}", plugin_path.display());

    let mut plugins = PluginRegistry::new();
    let host_mgr = HostBridgeManager::new();
    install_host_bridge(&mut plugins, host_mgr.clone())?;
    eprintln!("Installed host bridge");

    eprintln!("Loading plugin dylib");
    let lib = unsafe { PluginLibrary::load(&plugin_path)? };
    eprintln!("Installing plugin into registry");
    lib.install_into(&mut plugins)?;
    eprintln!("Installed plugin");

    let clahe1 = NodeHandle::new("ffi.cv_clahe_passthrough:clahe").alias("clahe1");
    let clahe2 = NodeHandle::new("ffi.cv_clahe_passthrough:clahe").alias("clahe2");
    let clahe3 = NodeHandle::new("ffi.cv_clahe_passthrough:clahe").alias("clahe3");
    eprintln!("Created clahe handles");

    eprintln!("Building graph");
    let graph_builder = GraphBuilder::new(&plugins.registry);
    eprintln!("Created GraphBuilder");
    let graph = graph_builder
        .host_bridge("host")
        .node(&clahe1)
        .node(&clahe2)
        .node(&clahe3)
        .connect(&PortHandle::new("host", "mask"), &clahe1.input("mask"))
        .connect(&clahe1.output("mask"), &clahe2.input("mask"))
        .connect(&clahe2.output("mask"), &clahe3.input("mask"))
        .const_input(&clahe1.input("tile_size"), Some(Value::Int(8)))
        .const_input(&clahe1.input("clip_limit"), Some(Value::Float(2.0)))
        .const_input(&clahe2.input("tile_size"), Some(Value::Int(8)))
        .const_input(&clahe2.input("clip_limit"), Some(Value::Float(2.0)))
        .const_input(&clahe3.input("tile_size"), Some(Value::Int(8)))
        .const_input(&clahe3.input("clip_limit"), Some(Value::Float(2.0)))
        .build();

    let mut cfg = EngineConfig::default();
    cfg.runtime.mode = RuntimeMode::Serial;
    cfg.gpu = GpuBackend::Cpu;
    cfg.planner.enable_gpu = false;
    let engine = Engine::new(cfg)?;

    eprintln!("Planning graph");
    let plan_out = engine.plan(&plugins.registry, graph)?;
    eprintln!("Building runtime plan");
    let runtime_plan = engine.build_runtime_plan(&plan_out.plan)?;
    debug_runtime_plan(&runtime_plan);
    eprintln!("Deriving host manager from runtime plan");
    let mgr = HostBridgeManager::from_plan(&runtime_plan);
    let handle = mgr.handle("host").expect("host bridge handle");

    eprintln!("Pushing CPU image as EdgePayload::Payload(ErasedPayload)");
    let img = DynamicImage::ImageRgba8(ImageBuffer::from_pixel(2, 2, Rgba([7, 8, 9, 255])));
    let ep = ErasedPayload::from_cpu::<DynamicImage>(img);
    handle.push("mask", EdgePayload::Payload(ep), None);

    let handlers = plugins.take_handlers();
    eprintln!("Executing with host bridge");
    let telemetry = engine.execute_with_host(runtime_plan, mgr, handlers)?;
    eprintln!("ok: executed clahe passthrough, telemetry={:?}", telemetry);

    Ok(())
}

fn debug_runtime_plan(runtime_plan: &daedalus::runtime::RuntimePlan) {
    let mut host_idx: Option<usize> = None;
    for (idx, node) in runtime_plan.nodes.iter().enumerate() {
        if matches!(node.metadata.get("host_bridge"), Some(Value::Bool(true))) {
            host_idx = Some(idx);
            eprintln!(
                "runtime_plan host_bridge idx={} id={} label={:?} outputs_meta={:?}",
                idx,
                node.id,
                node.label,
                node.metadata.get("dynamic_outputs")
            );
        }
    }
    let Some(host_idx) = host_idx else {
        eprintln!("runtime_plan has no host_bridge nodes");
        return;
    };
    let mut edges: Vec<(String, String)> = Vec::new();
    for (from, from_port, to, to_port, _policy) in &runtime_plan.edges {
        if from.0 == host_idx {
            edges.push((
                from_port.clone(),
                format!("{}:{}", runtime_plan.nodes[to.0].id, to_port),
            ));
        }
    }
    edges.sort();
    eprintln!("runtime_plan host outgoing edges: {:?}", edges);
}

fn plugin_dylib_path() -> PathBuf {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.pop(); // crates/ffi -> crates
    path.pop(); // crates -> workspace root
    path.push("target");
    path.push(current_profile());
    path.push("examples");
    let (prefix, ext) = library_naming();
    path.push(format!("{prefix}plugin_clahe_passthrough{ext}"));
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
