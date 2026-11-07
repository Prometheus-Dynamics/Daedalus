//! The GPU context exists only for a plan that runs on a GPU: a CPU-only graph plans and compiles
//! without creating it (`Engine::planner_config_for`, `Engine::gpu_handle_if`).
#![cfg(feature = "gpu")]

use daedalus_engine::{Engine, EngineConfig, GpuBackend};
use daedalus_planner::{ComputeAffinity, ExecutionPlan, Graph, NodeInstance};
use daedalus_runtime::executor::{NodeError, NodeHandler};
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
    engine.plan(graph(ComputeAffinity::CpuOnly)).expect("plan");
    assert!(!engine.gpu_context_created(), "planning a CPU-only graph opened the device");

    let plan = runtime_plan(ComputeAffinity::CpuOnly);
    let _compiled = engine.compile_runtime_plan(plan, Noop).expect("compile");
    assert!(!engine.gpu_context_created(), "compiling a CPU-only plan opened the device");
}

#[test]
fn gpu_graph_creates_the_gpu_context() {
    let engine = engine();
    engine
        .plan(graph(ComputeAffinity::GpuRequired))
        .expect("plan");
    assert!(engine.gpu_context_created(), "a GPU-required node must open the device");
}

#[test]
fn redeploying_a_cpu_plan_keeps_the_created_context() {
    // A created context stays with the engine: executors and GPU payloads hold clones of it, and
    // the device is shared process-wide, so the engine cannot release it while they may use it.
    let engine = engine();
    let gpu = runtime_plan(ComputeAffinity::GpuPreferred);
    let _gpu_run = engine.compile_runtime_plan(gpu, Noop).expect("gpu compile");
    assert!(engine.gpu_context_created());

    let cpu = runtime_plan(ComputeAffinity::CpuOnly);
    let _cpu_run = engine.compile_runtime_plan(cpu, Noop).expect("cpu compile");
    assert!(engine.gpu_context_created(), "kept: see the comment above");
}
