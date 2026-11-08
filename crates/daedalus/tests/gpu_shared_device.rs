//! Two engines with a device backend in one process run their nodes on one wgpu device, the
//! process's (`daedalus::gpu::shared_context`), and a library asking for the process's device
//! (as Styx's GPU ISP does) gets the same one. Skipped without a hardware GPU adapter.
#![allow(clippy::print_stderr)] // the skip reason
use std::sync::Mutex;

use daedalus::{
    engine::{Engine, EngineConfig, GpuBackend},
    gpu::{devices_created, process_context, wgpu},
    macros::{node, plugin},
    runtime::{ExecutionContext, NodeError, plugins::PluginRegistry},
};

/// The device each node run saw.
static SEEN: Mutex<Vec<wgpu::Device>> = Mutex::new(Vec::new());

#[node(id = "test.device_seen", inputs("tick"), outputs("seen"))]
fn device_seen(tick: i64, ctx: &ExecutionContext) -> Result<i64, NodeError> {
    let gpu = ctx
        .gpu()?
        .ok_or_else(|| NodeError::Handler("no GPU".into()))?;
    let (device, _) = gpu
        .wgpu()
        .ok_or_else(|| NodeError::Handler("not wgpu".into()))?;
    SEEN.lock().expect("seen").push(device.clone());
    Ok(tick)
}

#[plugin(id = "test.device_seen", nodes(device_seen))]
struct DeviceSeenPlugin;

#[test]
fn two_engines_and_a_library_share_one_device() {
    let mut runtimes = Vec::new();
    for _ in 0..2 {
        let engine = match Engine::new(EngineConfig::gpu(GpuBackend::Device)) {
            Ok(engine) => engine,
            Err(err) => return eprintln!("skipping: no GPU engine ({err})"),
        };
        let mut registry = PluginRegistry::new();
        let plugin = DeviceSeenPlugin::new();
        registry.install(&plugin).expect("install plugin");
        let node = plugin.device_seen.alias("seen");
        let graph = registry
            .graph_builder()
            .expect("graph builder")
            .try_node(&node)
            .expect("node")
            .try_connect("in", &node.inputs.tick)
            .expect("input edge")
            .try_connect(&node.outputs.seen, "out")
            .expect("output edge")
            .build();
        match engine.compile_registry(&registry, graph) {
            Ok(runtime) => runtimes.push(runtime),
            Err(err) => return eprintln!("skipping: no GPU adapter ({err})"),
        }
    }
    for (i, runtime) in runtimes.iter_mut().enumerate() {
        let out = runtime
            .run_direct_once::<i64, i64>("in", "out", 7)
            .unwrap_or_else(|e| panic!("runtime {i}: {e:?}"));
        assert_eq!(out, Some(7));
    }
    let seen = SEEN.lock().expect("seen").clone();
    assert_eq!(seen.len(), 2);
    assert!(seen[0] == seen[1], "both engines run on one device");
    let library = process_context().expect("the process's device is open");
    assert!(library.wgpu().expect("wgpu").0 == &seen[0]);
    assert_eq!(devices_created(), 1, "one device in the process");
}
