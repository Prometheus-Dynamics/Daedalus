#![cfg(feature = "gpu-mock")]

use daedalus_planner::{ComputeAffinity, Edge, ExecutionPlan, Graph, NodeInstance};
use daedalus_runtime::{
    BackpressureStrategy, Executor, NodeHandler, RuntimeEdgePolicy, RuntimeNode, SchedulerConfig,
    build_runtime, executor::NodeError,
};

struct LogHandler {
    log: std::sync::Arc<parking_lot::Mutex<Vec<String>>>,
}

impl NodeHandler for LogHandler {
    fn run(
        &self,
        node: &RuntimeNode,
        _ctx: &daedalus_runtime::state::ExecutionContext,
        _io: &mut daedalus_runtime::io::NodeIo,
    ) -> Result<(), NodeError> {
        self.log.lock().push(node.id.clone());
        Ok(())
    }
}

fn gpu_plan() -> ExecutionPlan {
    let mut graph = Graph::default();
    graph.nodes.push(NodeInstance::new("cpu0"));
    graph
        .nodes
        .push(NodeInstance::new("gpu_req").with_compute(ComputeAffinity::GpuRequired));
    graph
        .nodes
        .push(NodeInstance::new("gpu_pref").with_compute(ComputeAffinity::GpuPreferred));
    graph
        .nodes
        .push(NodeInstance::new("cpu1").with_inputs(["in"]));
    graph.edges.push(Edge::new(0, "out", 1, "in"));
    graph.edges.push(Edge::new(1, "out", 2, "in"));
    graph.edges.push(Edge::new(2, "out", 3, "in"));
    ExecutionPlan::new(graph, vec![])
}

#[test]
fn gpu_segments_execute_with_mock_backend() {
    let exec = gpu_plan();
    let rt = build_runtime(
        &exec,
        &SchedulerConfig {
            default_policy: RuntimeEdgePolicy::default(),
            backpressure: BackpressureStrategy::None,
        },
    );
    // Ensure GPU entry/exit sets are computed (cpu0->gpu_req entry, gpu_pref->cpu1 exit).
    assert_eq!(rt.gpu_segments.len(), 1);
    assert_eq!(rt.gpu_entries, vec![0]);
    assert_eq!(rt.gpu_exits, vec![2]);

    let gpu = daedalus_gpu::select_backend(&daedalus_gpu::GpuOptions {
        preferred_backend: Some(daedalus_gpu::GpuBackendKind::Mock),
        adapter_label: None,
        allow_software: true,
    })
    .expect("mock gpu backend");

    let log = std::sync::Arc::new(parking_lot::Mutex::new(Vec::new()));
    let handler = LogHandler { log: log.clone() };
    let telemetry = Executor::new(&rt, handler)
        .with_gpu(gpu)
        .run()
        .expect("exec ok");

    assert_eq!(telemetry.gpu_segments, 2);
    assert_eq!(telemetry.gpu_fallbacks, 0);
    assert_eq!(telemetry.nodes_executed, 4);
    let log = log.lock().clone();
    assert!(log.contains(&"gpu_req".to_string()));
    assert!(log.contains(&"gpu_pref".to_string()));
}
