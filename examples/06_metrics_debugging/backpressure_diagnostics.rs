use parking_lot::Mutex;
use std::sync::Arc;

use daedalus::{
    planner::{Edge, ExecutionPlan, Graph, NodeInstance},
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
                let mut seen = self.seen.lock();
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
    graph
        .nodes
        .push(NodeInstance::new("producer").with_outputs(["out"]));
    graph
        .nodes
        .push(NodeInstance::new("consumer").with_inputs(["in"]));
    graph.edges.push(Edge::new(0, "out", 1, "in"));
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

    println!("consumer values: {:?}", seen.lock());
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
