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

fn run_strategy(strategy: BackpressureStrategy) -> Result<(), Box<dyn std::error::Error>> {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let runtime = build_runtime(
        &burst_plan(),
        &SchedulerConfig {
            default_policy: RuntimeEdgePolicy::bounded(1),
            backpressure: strategy.clone(),
        },
    );
    let result = Executor::new(&runtime, BurstHandler { seen: seen.clone() })
        .with_metrics_level(MetricsLevel::Detailed)
        .run();
    println!("{strategy:?}: consumer saw {:?}", seen.lock());
    match result {
        Ok(telemetry) => println!("{}", telemetry.compact_snapshot()),
        Err(error) => println!("{strategy:?}: execution failed as configured: {error}"),
    }
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("producer emits [1, 2, 3, 4] into one bounded-capacity edge");
    println!("BoundedQueues rejects incoming overflow without blocking the graph tick");
    println!("ErrorOnOverflow fails execution on the first bounded overflow");
    run_strategy(BackpressureStrategy::None)?;
    run_strategy(BackpressureStrategy::BoundedQueues)?;
    run_strategy(BackpressureStrategy::ErrorOnOverflow)?;
    Ok(())
}
