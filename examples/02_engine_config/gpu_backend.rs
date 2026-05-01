use daedalus::{
    ComputeAffinity,
    engine::{Engine, EngineConfig, GpuBackend, MetricsLevel},
    planner::{ExecutionPlan, Graph, NodeInstance},
    registry::ids::NodeId,
    runtime::{
        NodeError, NodeHandler, RuntimeEdgePolicy, RuntimeNode, SchedulerConfig, build_runtime,
    },
};

struct GpuMarkedHandler;

impl NodeHandler for GpuMarkedHandler {
    fn run(
        &self,
        node: &RuntimeNode,
        _ctx: &daedalus::runtime::ExecutionContext,
        _io: &mut daedalus::runtime::NodeIo,
    ) -> Result<(), NodeError> {
        println!("handler ran node={} compute={:?}", node.id, node.compute);
        Ok(())
    }
}

fn gpu_marked_plan(compute: ComputeAffinity) -> daedalus::runtime::RuntimePlan {
    let mut graph = Graph::default();
    graph.nodes.push(NodeInstance {
        id: NodeId::new("gpu_marked_node"),
        bundle: None,
        label: None,
        inputs: vec![],
        outputs: vec![],
        compute,
        const_inputs: vec![],
        sync_groups: vec![],
        metadata: Default::default(),
    });
    let plan = ExecutionPlan::new(graph, vec![]);
    build_runtime(
        &plan,
        &SchedulerConfig {
            default_policy: RuntimeEdgePolicy::fifo(),
            backpressure: Default::default(),
        },
    )
}

fn run_backend(
    gpu: GpuBackend,
    compute: ComputeAffinity,
) -> Result<(), Box<dyn std::error::Error>> {
    let config = EngineConfig::gpu(gpu.clone()).with_metrics_level(MetricsLevel::Detailed);
    let engine = match Engine::new(config) {
        Ok(engine) => engine,
        Err(err) => {
            println!("{gpu:?} with {compute:?}: backend unavailable in this build: {err}");
            return Ok(());
        }
    };
    let result = engine.execute(gpu_marked_plan(compute), GpuMarkedHandler);
    match result {
        Ok(telemetry) => {
            println!(
                "{gpu:?} with {compute:?}: gpu_segments={} gpu_fallbacks={}",
                telemetry.gpu_segments, telemetry.gpu_fallbacks
            );
            println!("{}", telemetry.compact_snapshot());
        }
        Err(err) => {
            println!("{gpu:?} with {compute:?}: runtime error: {err}");
        }
    }
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    run_backend(GpuBackend::Cpu, ComputeAffinity::CpuOnly)?;
    run_backend(GpuBackend::Cpu, ComputeAffinity::GpuPreferred)?;
    run_backend(GpuBackend::Cpu, ComputeAffinity::GpuRequired)?;
    run_backend(GpuBackend::Mock, ComputeAffinity::GpuPreferred)?;
    run_backend(GpuBackend::Device, ComputeAffinity::GpuPreferred)?;
    Ok(())
}
