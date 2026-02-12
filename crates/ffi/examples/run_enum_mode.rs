//! Build + load the enum-mode plugin and feed an ExecMode through the host bridge to reproduce
//! enum binding issues.

use daedalus::{
    NodeHandle, PluginLibrary, PortHandle,
    data::json,
    data::model::{EnumValue, Value},
    engine::{Engine, EngineConfig, GpuBackend, RuntimeMode},
    graph_builder::GraphBuilder,
    host_bridge::install_host_bridge,
    runtime::host_bridge::{HostBridgeManager, HostBridgeSerialized},
    runtime::plugins::PluginRegistry,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let plugin_path = plugin_dylib_path();
    eprintln!("Loading enum-mode plugin from {}", plugin_path.display());

    let mut plugins = PluginRegistry::new();
    let host_mgr = HostBridgeManager::new();
    install_host_bridge(&mut plugins, host_mgr.clone())?;

    let lib = unsafe { PluginLibrary::load(&plugin_path)? };
    lib.install_into(&mut plugins)?;

    let enum_node = NodeHandle::new("ffi.enum_mode:enum_mode");

    let graph = GraphBuilder::new(&plugins.registry)
        .host_bridge("host")
        .node(&enum_node)
        .connect(&PortHandle::new("host", "mode"), &enum_node.input("mode"))
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

    // Push mode as typed JSON using the runtime Value encoder.
    let mode_value = Value::Enum(EnumValue {
        name: "gpu".to_string(),
        value: None,
    });
    let mode_json = json::to_json(&mode_value).expect("serialize mode");
    handle.push_serialized("mode", HostBridgeSerialized::Json(mode_json), None)?;

    let handlers = plugins.take_handlers();
    let telemetry = engine.execute_with_host(runtime_plan, mgr, handlers)?;
    eprintln!("ok: executed enum_mode, telemetry={:?}", telemetry);
    Ok(())
}

fn plugin_dylib_path() -> std::path::PathBuf {
    let mut path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.pop(); // crates/ffi -> crates
    path.pop(); // crates -> workspace root
    path.push("target");
    path.push(current_profile());
    path.push("examples");
    let (prefix, ext) = library_naming();
    path.push(format!("{prefix}plugin_enum_mode{ext}"));
    path
}

fn current_profile() -> String {
    std::env::var("PROFILE")
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
