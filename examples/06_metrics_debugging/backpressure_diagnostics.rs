use std::sync::{Arc, Mutex};

use daedalus::{
    ComputeAffinity,
    planner::{Edge, ExecutionPlan, Graph, NodeInstance, NodeRef, PortRef},
    registry::ids::NodeId,
    runtime::{
        BackpressureStrategy, Executor, MetricsLevel, NodeError, NodeHandler, RuntimeEdgePolicy,
        RuntimeNode, SchedulerConfig, build_runtime,
    },
    transport::Payload,
};
use tracing_subscriber::EnvFilter;

#[derive(Clone)]
struct BurstHandler {
    seen: Arc<Mutex<Vec<i64>>>,
}

impl NodeHandler for BurstHandler {
    fn run(
        &self,
        node: &RuntimeNode,
        _ctx: &daedalus::runtime::ExecutionContext,
        io: &mut daedalus::runtime::NodeIo,
    ) -> Result<(), NodeError> {
        match node.id.as_str() {
            "producer" => {
                for value in 1_i64..=4 {
                    io.push_payload("out", Payload::owned("example:i64", value));
                }
            }
            "consumer" => {
                let mut seen = self
                    .seen
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                for payload in io.inputs_for("in") {
                    if let Some(value) = payload.inner.get_ref::<i64>() {
                        seen.push(*value);
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("daedalus_runtime=warn"));
    let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
}

fn burst_plan() -> ExecutionPlan {
    let mut graph = Graph::default();
    graph.nodes.push(NodeInstance {
        id: NodeId::new("producer"),
        bundle: None,
        label: None,
        inputs: vec![],
        outputs: vec!["out".into()],
        compute: ComputeAffinity::CpuOnly,
        const_inputs: vec![],
        sync_groups: vec![],
        metadata: Default::default(),
    });
    graph.nodes.push(NodeInstance {
        id: NodeId::new("consumer"),
        bundle: None,
        label: None,
        inputs: vec!["in".into()],
        outputs: vec![],
        compute: ComputeAffinity::CpuOnly,
        const_inputs: vec![],
        sync_groups: vec![],
        metadata: Default::default(),
    });
    graph.edges.push(Edge {
        from: PortRef {
            node: NodeRef(0),
            port: "out".into(),
        },
        to: PortRef {
            node: NodeRef(1),
            port: "in".into(),
        },
        metadata: Default::default(),
    });
    ExecutionPlan::new(graph, vec![])
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();

    let seen = Arc::new(Mutex::new(Vec::new()));
    let runtime = build_runtime(
        &burst_plan(),
        &SchedulerConfig {
            default_policy: RuntimeEdgePolicy::bounded(1),
            backpressure: BackpressureStrategy::BoundedQueues,
        },
    );

    let telemetry = Executor::new(
        &runtime,
        BurstHandler {
            seen: Arc::clone(&seen),
        },
    )
    .with_metrics_level(MetricsLevel::Detailed)
    .run()?;

    println!(
        "consumer values: {:?}",
        seen.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    );
    println!("{}", telemetry.compact_snapshot());

    for (edge_idx, metrics) in &telemetry.edge_metrics {
        println!(
            "edge={edge_idx} capacity={:?} depth={}/{} drops={} pressure_total={} backpressure={} error_overflow={}",
            metrics.capacity,
            metrics.current_depth,
            metrics.max_depth,
            metrics.drops,
            metrics.pressure_events.total,
            metrics.pressure_events.backpressure,
            metrics.pressure_events.error_overflow
        );
    }

    Ok(())
}
