//! Merged telemetry of parallel ticks matches serial ticks of the same graph: per-node calls,
//! per-edge samples and per-group totals agree tick by tick, with each tick's telemetry handed
//! back to the owned executor so the recycled storage is what the parallel merge reuses.
//!
//! Timings are wall clock, so they cannot agree to the nanosecond across threads: the test checks
//! that every node that ran has a timing, and that each group's total is the sum of its nodes'.

use std::collections::BTreeMap;
use std::sync::Arc;

use daedalus_data::model::Value;
use daedalus_planner::{Edge, ExecutionPlan, GROUP_ID_KEY, Graph, NodeInstance};
use daedalus_runtime::executor::{NodeError, OwnedExecutor};
use daedalus_runtime::io::NodeIo;
use daedalus_runtime::state::ExecutionContext;
use daedalus_runtime::{
    ExecutionTelemetry, MetricsLevel, NodeHandler, RuntimeNode, SchedulerConfig, build_runtime,
};
use daedalus_transport::Payload;

/// `source` feeds `left` and `right`, which both join `sink`; `left` and `right` share a group.
struct Relay;

impl NodeHandler for Relay {
    fn run(
        &self,
        node: &RuntimeNode,
        _ctx: &ExecutionContext,
        io: &mut NodeIo,
    ) -> Result<(), NodeError> {
        match node.id.as_str() {
            "source" => io.push_payload("out", Payload::owned("test:i32", 40_i32)),
            "left" | "right" => {
                let value = io.get_typed::<i32>("in").unwrap_or(0);
                io.push_payload("out", Payload::owned("test:i32", value + 1));
            }
            _ => {}
        }
        Ok(())
    }
}

fn grouped_plan() -> ExecutionPlan {
    let group = || Value::String("fan".into());
    let mut graph = Graph::default();
    graph
        .nodes
        .push(NodeInstance::new("source").with_outputs(["out"]));
    graph.nodes.push(
        NodeInstance::new("left")
            .with_inputs(["in"])
            .with_outputs(["out"])
            .with_metadata(GROUP_ID_KEY, group()),
    );
    graph.nodes.push(
        NodeInstance::new("right")
            .with_inputs(["in"])
            .with_outputs(["out"])
            .with_metadata(GROUP_ID_KEY, group()),
    );
    graph
        .nodes
        .push(NodeInstance::new("sink").with_inputs(["left", "right"]));
    for (from, from_port, to, to_port) in [
        (0, "out", 1, "in"),
        (0, "out", 2, "in"),
        (1, "out", 3, "left"),
        (2, "out", 3, "right"),
    ] {
        graph.edges.push(Edge::new(from, from_port, to, to_port));
    }
    ExecutionPlan::new(graph, vec![])
}

/// One tick's telemetry, reduced to what must agree between serial and parallel runs.
#[derive(Debug, PartialEq)]
struct Shape {
    nodes_executed: usize,
    calls: BTreeMap<usize, usize>,
    edge_samples: BTreeMap<usize, usize>,
    group_calls: BTreeMap<String, usize>,
}

fn shape(telemetry: &ExecutionTelemetry) -> Shape {
    Shape {
        nodes_executed: telemetry.nodes_executed,
        calls: telemetry
            .node_metrics
            .iter()
            .map(|(idx, m)| (idx, m.calls))
            .collect(),
        edge_samples: telemetry
            .edge_metrics
            .iter()
            .map(|(idx, m)| (*idx, m.samples))
            .collect(),
        group_calls: telemetry
            .group_metrics
            .iter()
            .map(|(name, m)| (name.clone(), m.calls))
            .collect(),
    }
}

/// Every executed node has a timing, and each group's total is the sum of its nodes'.
fn check_timings(telemetry: &ExecutionTelemetry) {
    for (idx, metrics) in telemetry.node_metrics.iter() {
        assert!(metrics.calls > 0, "node {idx} ran");
        assert!(
            metrics.total_duration > core::time::Duration::ZERO,
            "node {idx} has a timing"
        );
    }
    let group = telemetry.group_metrics.get("fan").expect("fan group");
    let nodes = [1usize, 2]
        .iter()
        .filter_map(|&idx| telemetry.node_metrics.get(idx))
        .fold(core::time::Duration::ZERO, |sum, m| sum + m.total_duration);
    assert_eq!(group.total_duration, nodes, "group total is its nodes' sum");
}

/// Enough ticks that a worker often takes the telemetry another worker of the same tick gave back.
const TICKS: usize = 40;

fn ticks(parallel: bool, level: MetricsLevel) -> Vec<Shape> {
    let rt = build_runtime(&grouped_plan(), &SchedulerConfig::default());
    let mut exec = OwnedExecutor::new(Arc::new(rt), Relay)
        .with_pool_size(Some(4))
        .with_node_fusion(false)
        .with_metrics_level(level);
    (0..TICKS)
        .map(|_| {
            let telemetry = if parallel {
                exec.run_parallel_in_place()
            } else {
                exec.run_in_place()
            }
            .expect("tick");
            let report = shape(&telemetry);
            check_timings(&telemetry);
            exec.recycle_telemetry(telemetry);
            report
        })
        .collect()
}

#[test]
fn parallel_ticks_merge_to_the_serial_telemetry() {
    for level in [MetricsLevel::Basic, MetricsLevel::Detailed] {
        let serial = ticks(false, level);
        let parallel = ticks(true, level);
        assert_eq!(parallel, serial, "{level:?}: merged telemetry differs");
        assert_eq!(serial.len(), TICKS);
        assert_eq!(serial[0].calls.len(), 4, "every node ran");
        assert_eq!(
            serial[0].group_calls["fan"], 2,
            "left and right are one group"
        );
    }
}
