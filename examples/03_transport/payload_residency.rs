use std::sync::{Arc, Mutex};

use daedalus::{
    ComputeAffinity,
    planner::{Edge, ExecutionPlan, Graph, NodeInstance, NodeRef, PortRef},
    registry::ids::NodeId,
    runtime::{
        BackpressureStrategy, Executor, MetricsLevel, NodeError, NodeHandler, RuntimeEdgePolicy,
        RuntimeNode, SchedulerConfig, build_runtime,
    },
    transport::{Payload, Residency},
};

#[derive(Debug)]
struct GpuFrame(Vec<u8>);

struct ResidencyHandler {
    seen: Arc<Mutex<Vec<String>>>,
}

impl NodeHandler for ResidencyHandler {
    fn run(
        &self,
        node: &RuntimeNode,
        _ctx: &daedalus::runtime::ExecutionContext,
        io: &mut daedalus::runtime::NodeIo,
    ) -> Result<(), NodeError> {
        match node.id.as_str() {
            "producer" => {
                let cpu = Payload::owned("example:frame", vec![1_u8, 2, 3]);
                let gpu = Payload::shared_with(
                    "example:frame@gpu",
                    Arc::new(GpuFrame(vec![1, 2, 3])),
                    Residency::Gpu,
                    None,
                    Some(3),
                )
                .with_cached_resident(cpu);
                io.push_payload("frame", gpu);
            }
            "inspector" => {
                let mut seen = self
                    .seen
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                for payload in io.inputs_for("frame") {
                    let gpu_frame = payload
                        .inner
                        .get_ref::<GpuFrame>()
                        .ok_or_else(|| NodeError::InvalidInput("expected gpu frame".into()))?;
                    seen.push(format!(
                        "residency={:?} cached={} bytes={}",
                        payload.inner.residency(),
                        payload.inner.cached_residencies().count(),
                        gpu_frame.0.len()
                    ));
                }
            }
            _ => {}
        }
        Ok(())
    }
}

fn plan() -> daedalus::runtime::RuntimePlan {
    let mut graph = Graph::default();
    graph.nodes.push(NodeInstance {
        id: NodeId::new("producer"),
        bundle: None,
        label: None,
        inputs: vec![],
        outputs: vec!["frame".into()],
        compute: ComputeAffinity::CpuOnly,
        const_inputs: vec![],
        sync_groups: vec![],
        metadata: Default::default(),
    });
    graph.nodes.push(NodeInstance {
        id: NodeId::new("inspector"),
        bundle: None,
        label: None,
        inputs: vec!["frame".into()],
        outputs: vec![],
        compute: ComputeAffinity::CpuOnly,
        const_inputs: vec![],
        sync_groups: vec![],
        metadata: Default::default(),
    });
    graph.edges.push(Edge {
        from: PortRef {
            node: NodeRef(0),
            port: "frame".into(),
        },
        to: PortRef {
            node: NodeRef(1),
            port: "frame".into(),
        },
        metadata: Default::default(),
    });
    build_runtime(
        &ExecutionPlan::new(graph, vec![]),
        &SchedulerConfig {
            default_policy: RuntimeEdgePolicy::fifo(),
            backpressure: BackpressureStrategy::None,
        },
    )
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let runtime = plan();
    let telemetry = Executor::new(&runtime, ResidencyHandler { seen: seen.clone() })
        .with_metrics_level(MetricsLevel::Detailed)
        .run()?;
    println!(
        "inspector: {:?}",
        seen.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    );
    println!("{}", telemetry.compact_snapshot());
    Ok(())
}
