//! The engine owns the on-disk pipeline cache (`EngineConfig::with_gpu_pipeline_cache`): a node
//! building a pipeline in its first tick sees it installed, the graph saves it after that tick,
//! and the next engine loads it. Skipped without a GPU adapter or a Vulkan pipeline cache.
#![allow(clippy::print_stderr)] // the skip reason
use daedalus::{
    engine::{Engine, EngineConfig, GpuBackend, HostGraph},
    gpu::{FragmentPass, pipeline_cache_for, wgpu},
    macros::{node, plugin},
    runtime::{
        ExecutionContext, NodeError, handler_registry::HandlerRegistry, plugins::PluginRegistry,
    },
};

#[derive(Default)]
struct State {
    pass: Option<FragmentPass>,
}

/// Builds a fragment pipeline on its first tick; outputs whether a cache was installed for the
/// engine's device.
#[node(id = "test.cached_pass", inputs("go"), outputs("cached"), state(State))]
fn cached_pass(go: i64, ctx: &ExecutionContext, state: &mut State) -> Result<bool, NodeError> {
    let _ = go;
    let gpu = ctx
        .gpu
        .as_ref()
        .ok_or_else(|| NodeError::Handler("no GPU".into()))?;
    if state.pass.is_none() {
        let pass = FragmentPass::new(
            gpu,
            "@fragment fn fs() -> @location(0) vec4<f32> { return vec4<f32>(0.125, 0.0, 0.0, 1.0); }",
            &[wgpu::TextureFormat::Rgba8Unorm],
        )
        .map_err(|err| NodeError::Handler(err.to_string()))?;
        state.pass = Some(pass);
    }
    let (device, _) = gpu
        .wgpu()
        .ok_or_else(|| NodeError::Handler("no wgpu".into()))?;
    Ok(pipeline_cache_for(device).is_some())
}

#[plugin(id = "test.gpu_pipeline_cache", nodes(cached_pass))]
struct CachePlugin;

fn engine_and_graph(dir: &std::path::Path) -> Option<(Engine, HostGraph<HandlerRegistry>)> {
    let config =
        EngineConfig::gpu(GpuBackend::Device).with_gpu_pipeline_cache(dir.display().to_string());
    let engine = Engine::new(config)
        .map_err(|err| eprintln!("skipping: no GPU engine ({err})"))
        .ok()?;
    let mut registry = PluginRegistry::new();
    let plugin = CachePlugin::new();
    registry.install(&plugin).expect("install plugin");
    let node = plugin.cached_pass.alias("pass");
    let graph = registry
        .graph_builder()
        .expect("graph builder")
        .try_node(&node)
        .expect("node")
        .try_connect("go", &node.inputs.go)
        .expect("input edge")
        .try_connect(&node.outputs.cached, "cached")
        .expect("output edge")
        .build();
    let runtime = engine
        .compile_registry(&registry, graph)
        .map_err(|err| eprintln!("skipping: no GPU adapter ({err})"))
        .ok()?;
    Some((engine, runtime))
}

#[test]
fn engine_pipeline_cache_is_installed_saved_and_reloaded() {
    let dir = std::env::temp_dir().join(format!("daedalus-engine-pcache-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let Some((engine, mut runtime)) = engine_and_graph(&dir) else {
        return;
    };
    let status = runtime
        .pipeline_cache_status()
        .expect("a cache is configured");
    assert_eq!(engine.pipeline_cache_status(), Some(status.clone()));
    if let Some(err) = &status.error {
        return eprintln!("skipping: no pipeline cache on this device ({err})");
    }
    assert_eq!(status.loaded_bytes, 0, "a new directory");

    runtime.push("go", 1i64);
    runtime.tick().expect("tick 1");
    let cached = runtime.drain_owned::<bool>("cached").expect("drain");
    assert_eq!(
        cached,
        [true],
        "the node's device has the engine's cache installed"
    );
    // The save after tick 1 runs as tick 2 starts.
    runtime.push("go", 2i64);
    runtime.tick().expect("tick 2");
    let status = runtime.pipeline_cache_status().expect("status");
    assert_eq!(status.saves, 1, "saved after the first tick: {status:?}");
    assert!(status.file_bytes > 0);
    let path = status.path.clone().expect("cache file");
    assert_eq!(
        std::fs::metadata(&path).expect("cache file written").len() as usize,
        status.file_bytes
    );
    assert_eq!(
        runtime.save_pipeline_cache().expect("save"),
        None,
        "nothing new"
    );
    let saved = status.file_bytes;
    drop(runtime);
    drop(engine);

    let Some((_engine, runtime)) = engine_and_graph(&dir) else {
        panic!("the second engine opens like the first");
    };
    let status = runtime.pipeline_cache_status().expect("status");
    assert_eq!(
        status.loaded_bytes, saved,
        "the next engine loads the saved cache"
    );
    drop(runtime);
    let _ = std::fs::remove_dir_all(&dir);
}
