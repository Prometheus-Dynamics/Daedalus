//! The consumer chooses each node's device; `explain_plan()` reports the device it runs on and
//! why: the CPU fallback (naming the GPU node) without a GPU backend, a GPU-preferring node on
//! the CPU, and a GPU-requiring node that fails to plan. The GPU resolution itself is unit-tested
//! in the engine (`record_devices`).
use daedalus::{
    engine::{Engine, EngineConfig},
    macros::{node, plugin},
    planner::ComputeAffinity,
    runtime::{NodeDevice, NodeError, plugins::PluginRegistry},
    transport::{Cpu, Gpu},
};

#[node(id = "dev.cpu", inputs("frame"), outputs("frame"))]
fn cpu_filter(frame: Cpu<i64>) -> Result<Cpu<i64>, NodeError> {
    Ok(frame)
}

#[node(
    id = "dev.gpu",
    fallback = "dev.cpu",
    inputs("frame"),
    outputs("frame")
)]
fn gpu_filter(frame: Gpu<i64>) -> Result<Gpu<i64>, NodeError> {
    Ok(frame)
}

#[node(id = "dev.pre", inputs("in"), outputs("out"))]
fn pre(value: &i64) -> Result<Cpu<i64>, NodeError> {
    Ok(Cpu::new(*value))
}

#[plugin(id = "dev", nodes(pre, cpu_filter, gpu_filter))]
struct DevicePlugin;

/// `pre -> filter`, the filter with `compute`; returns the filter's (device, reason) or the
/// compile error.
fn resolve(
    config: EngineConfig,
    gpu_node: bool,
    compute: ComputeAffinity,
) -> Result<(NodeDevice, String, String), String> {
    let mut registry = PluginRegistry::new();
    let plugin = DevicePlugin::new();
    registry.install(&plugin).unwrap();
    let pre = plugin.pre.clone().alias("pre");
    let gpu = plugin.gpu_filter.clone().alias("filter");
    let cpu = plugin.cpu_filter.clone().alias("filter");
    let graph = registry
        .graph_builder()
        .unwrap()
        .inputs(|g| {
            g.input("in");
        })
        .outputs(|g| {
            g.output("out");
        })
        .nodes(|g| {
            g.add_handle(&pre);
            if gpu_node {
                g.add_handle_with_compute(&gpu, compute);
            } else {
                g.add_handle_with_compute(&cpu, compute);
            }
        })
        .try_edges(|g| {
            let p = g.node("pre");
            let n = g.node("filter");
            g.try_connect("in", &p.input("in"))?;
            g.try_connect(&p.output("out"), &n.input("frame"))?;
            g.try_connect(&n.output("frame"), "out")?;
            Ok(())
        })
        .unwrap()
        .build();
    let engine = Engine::new(config).map_err(|err| err.to_string())?;
    let runtime = engine
        .compile_registry(&registry, graph)
        .map_err(|err| err.to_string())?;
    let explanation = runtime.explain_plan();
    let node = explanation
        .nodes
        .iter()
        .find(|n| n.label.as_deref() == Some("filter"))
        .expect("filter node");
    Ok((
        node.device,
        node.id.clone(),
        node.device_reason.clone().unwrap_or_default(),
    ))
}

#[test]
fn cpu_nodes_run_on_the_cpu() {
    let (device, _, reason) =
        resolve(EngineConfig::default(), false, ComputeAffinity::CpuOnly).unwrap();
    assert_eq!(device, NodeDevice::Cpu);
    assert_eq!(reason, "compute = cpu_only");
}

#[test]
fn without_a_gpu_backend_the_fallback_runs_and_says_whose() {
    let (device, id, reason) =
        resolve(EngineConfig::default(), true, ComputeAffinity::GpuPreferred).unwrap();
    assert_eq!(device, NodeDevice::Cpu);
    assert!(id.ends_with("dev.cpu"), "{id}");
    assert!(
        reason.contains("cpu fallback for") && reason.contains("dev.gpu"),
        "{reason}"
    );
}

#[test]
fn a_gpu_preferring_node_without_gpu_reports_the_cpu() {
    let (device, _, reason) = resolve(
        EngineConfig::default(),
        false,
        ComputeAffinity::GpuPreferred,
    )
    .unwrap();
    assert_eq!(device, NodeDevice::Cpu);
    assert!(
        reason.contains("gpu_preferred") && reason.contains("CPU"),
        "{reason}"
    );
}

#[test]
fn a_gpu_requiring_node_without_gpu_fails_to_plan() {
    let err = resolve(EngineConfig::default(), false, ComputeAffinity::GpuRequired).unwrap_err();
    assert!(err.contains("requires GPU"), "{err}");
}
