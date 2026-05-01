use std::sync::{Arc, Mutex};

use daedalus::{
    ComputeAffinity,
    engine::{Engine, EngineConfig, MetricsLevel},
    planner::{ExecutionPlan, Graph, NodeInstance},
    registry::ids::NodeId,
    runtime::{
        NodeError, NodeHandler, RuntimeEdgePolicy, RuntimeNode, SchedulerConfig, build_runtime,
    },
};

#[derive(Clone)]
struct Harness {
    calls: Arc<Mutex<Vec<String>>>,
}

impl NodeHandler for Harness {
    fn run(
        &self,
        node: &RuntimeNode,
        _ctx: &daedalus::runtime::ExecutionContext,
        _io: &mut daedalus::runtime::NodeIo,
    ) -> Result<(), NodeError> {
        self.calls
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(node.id.clone());
        if node.id == "fail" {
            return Err(NodeError::InvalidInput("intentional failure".into()));
        }
        Ok(())
    }
}

fn plan() -> daedalus::runtime::RuntimePlan {
    let mut graph = Graph::default();
    graph.nodes.push(NodeInstance {
        id: NodeId::new("fail"),
        bundle: None,
        label: None,
        inputs: vec![],
        outputs: vec![],
        compute: ComputeAffinity::CpuOnly,
        const_inputs: vec![],
        sync_groups: vec![],
        metadata: Default::default(),
    });
    graph.nodes.push(NodeInstance {
        id: NodeId::new("independent"),
        bundle: None,
        label: None,
        inputs: vec![],
        outputs: vec![],
        compute: ComputeAffinity::CpuOnly,
        const_inputs: vec![],
        sync_groups: vec![],
        metadata: Default::default(),
    });
    build_runtime(
        &ExecutionPlan::new(graph, vec![]),
        &SchedulerConfig {
            default_policy: RuntimeEdgePolicy::fifo(),
            backpressure: Default::default(),
        },
    )
}

fn run_case(fail_fast: bool) -> Result<(), Box<dyn std::error::Error>> {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let engine = Engine::new(
        EngineConfig::default()
            .with_fail_fast(fail_fast)
            .with_metrics_level(MetricsLevel::Detailed),
    )?;
    let result = engine.execute(
        plan(),
        Harness {
            calls: calls.clone(),
        },
    );
    let telemetry = result.as_ref().ok().cloned().unwrap_or_default();
    println!(
        "fail_fast={fail_fast}: result={:?}; calls={:?}; telemetry_errors={}",
        result.as_ref().map(|_| ()),
        calls
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()),
        telemetry.errors.len()
    );
    println!("{}", telemetry.compact_snapshot());
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    run_case(true)?;
    run_case(false)?;
    Ok(())
}
