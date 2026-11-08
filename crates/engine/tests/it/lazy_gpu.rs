//! The GPU context exists only for a plan that runs on a GPU: a CPU-only graph plans and compiles
//! without creating it (`Engine::planner_config_for`, `Engine::gpu_handle_if`).
#![cfg(all(feature = "gpu", feature = "plugins"))]

use daedalus_engine::{Engine, EngineConfig, EngineError, GpuBackend};
use daedalus_planner::{
    ComputeAffinity, DiagnosticCode, ExecutionPlan, Graph, NodeInstance, PlannerOutput,
};
use daedalus_registry::capability::NodeDecl;
use daedalus_runtime::executor::{NodeError, NodeHandler};
use daedalus_runtime::plugins::PluginRegistry;
use daedalus_runtime::{RuntimeNode, SchedulerConfig, build_runtime};

struct Noop;

impl NodeHandler for Noop {
    fn run(
        &self,
        _node: &RuntimeNode,
        _ctx: &daedalus_runtime::state::ExecutionContext,
        _io: &mut daedalus_runtime::io::NodeIo,
    ) -> Result<(), NodeError> {
        Ok(())
    }
}

fn engine() -> Engine {
    // The mock backend: no device, so the test runs on any host.
    let config = EngineConfig::default()
        .with_gpu(GpuBackend::Mock)
        .with_planner_gpu(true);
    Engine::new(config).expect("engine")
}

/// Plans `graph` (one node, `n0`) against a registry that declares `n0`.
fn plan(engine: &Engine, graph: Graph) -> Result<PlannerOutput, EngineError> {
    let mut plugins = PluginRegistry::new();
    plugins
        .register_node_decl(NodeDecl::new("n0"))
        .expect("declare n0");
    engine.plan_plugin_registry(&plugins, graph)
}

fn graph(compute: ComputeAffinity) -> Graph {
    let mut graph = Graph::default();
    let mut node = NodeInstance::new("n0");
    node.compute = compute;
    graph.nodes.push(node);
    graph
}

fn runtime_plan(compute: ComputeAffinity) -> daedalus_runtime::RuntimePlan {
    build_runtime(
        &ExecutionPlan::new(graph(compute), vec![]),
        &SchedulerConfig::default(),
    )
}

#[test]
fn cpu_only_graph_plans_and_compiles_without_a_gpu_context() {
    let engine = engine();
    plan(&engine, graph(ComputeAffinity::CpuOnly)).expect("plan");
    assert!(
        !engine.gpu_context_created(),
        "planning a CPU-only graph opened the device"
    );

    let plan = runtime_plan(ComputeAffinity::CpuOnly);
    let _compiled = engine.compile_runtime_plan(plan, Noop).expect("compile");
    assert!(
        !engine.gpu_context_created(),
        "compiling a CPU-only plan opened the device"
    );
}

#[test]
fn planning_a_gpu_graph_opens_no_device() {
    let engine = engine();
    // The mock's caps (no transfer queue) are planned without a device: the planner's GPU pass
    // rejects the node from the caps source's answer, and planning opens nothing.
    let err = plan(&engine, graph(ComputeAffinity::GpuRequired))
        .expect_err("the mock has no transfer queue, so the GPU node is rejected");
    let diagnostics = match err {
        EngineError::Planner(diagnostics) => diagnostics,
        other => panic!("expected a planner error, got {other}"),
    };
    assert!(
        diagnostics
            .iter()
            .any(|d| d.code == DiagnosticCode::GpuUnsupported),
        "{diagnostics:?}"
    );
    assert!(
        !engine.gpu_context_created(),
        "planning a GPU graph opened the device"
    );
}

#[test]
fn compiling_a_gpu_plan_opens_no_device() {
    // The device opens on a request (a node's `ExecutionContext::gpu` call, or the planner's caps
    // request), not when a plan is compiled: a compiled GPU plan that never runs one creates none.
    let engine = engine();
    let gpu = runtime_plan(ComputeAffinity::GpuPreferred);
    let _gpu_run = engine.compile_runtime_plan(gpu, Noop).expect("gpu compile");
    assert!(
        !engine.gpu_context_created(),
        "compiling a GPU plan opened the device"
    );
    let cpu = runtime_plan(ComputeAffinity::CpuOnly);
    let _cpu_run = engine.compile_runtime_plan(cpu, Noop).expect("cpu compile");
    assert!(!engine.gpu_context_created());
}
#[test]
fn a_cpu_engine_creates_no_device_for_gpu_nodes() {
    // gpu = Cpu (the default; the planner GPU setting must stay off with it, which the engine
    // enforces): a CPU-only plan plans, and a GPU-required runtime plan compiles, with no device.
    let before = daedalus_gpu::devices_created();
    let engine = Engine::new(EngineConfig::default()).expect("engine");
    plan(&engine, graph(ComputeAffinity::CpuOnly)).expect("a CPU-only plan");
    let gpu_plan = runtime_plan(ComputeAffinity::GpuRequired);
    let _compiled = engine
        .compile_runtime_plan(gpu_plan, Noop)
        .expect("compile");
    assert!(
        !engine.gpu_context_created(),
        "a Cpu engine opened a GPU context"
    );
    assert_eq!(
        daedalus_gpu::devices_created(),
        before,
        "a Cpu engine created a wgpu device"
    );
}
