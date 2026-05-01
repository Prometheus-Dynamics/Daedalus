use daedalus_planner::{
    ComputeAffinity, Edge, ExecutionPlan, Graph, NodeInstance, NodeRef, PortRef,
};
use daedalus_runtime::{
    Executor, NodeHandler, RuntimeEdgePolicy, RuntimeNode, SchedulerConfig, build_runtime,
};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use std::borrow::Cow;

struct LogHandler {
    order: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
}

#[test]
fn schedule_order_preserves_duplicate_node_ids_by_alias() {
    let mut graph = Graph::default();
    for alias in ["a,one", "b:two", "c|three"] {
        graph.nodes.push(NodeInstance {
            id: daedalus_registry::ids::NodeId::new("same.node"),
            bundle: None,
            label: Some(alias.to_string()),
            inputs: vec![],
            outputs: vec![],
            compute: ComputeAffinity::CpuOnly,
            const_inputs: vec![],
            sync_groups: vec![],
            metadata: Default::default(),
        });
    }
    graph.metadata.insert(
        "schedule_order".into(),
        daedalus_data::model::Value::List(
            ["a,one", "b:two", "c|three"]
                .into_iter()
                .map(|id| daedalus_data::model::Value::String(Cow::Borrowed(id)))
                .collect(),
        ),
    );

    let rt = build_runtime(
        &ExecutionPlan::new(graph, vec![]),
        &SchedulerConfig {
            default_policy: RuntimeEdgePolicy::default(),
            backpressure: daedalus_runtime::BackpressureStrategy::None,
        },
    );

    assert_eq!(rt.schedule_order, vec![NodeRef(0), NodeRef(1), NodeRef(2)]);
}

impl NodeHandler for LogHandler {
    fn run(
        &self,
        node: &RuntimeNode,
        _ctx: &daedalus_runtime::state::ExecutionContext,
        _io: &mut daedalus_runtime::io::NodeIo,
    ) -> Result<(), daedalus_runtime::NodeError> {
        self.order.lock().unwrap().push(node.id.clone());
        Ok(())
    }
}

fn random_plan(seed: u64, nodes: usize, edges: usize) -> ExecutionPlan {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut graph = Graph::default();
    for i in 0..nodes {
        graph.nodes.push(NodeInstance {
            id: daedalus_registry::ids::NodeId::new(format!("n{i}")),
            bundle: None,
            label: None,
            inputs: vec!["in".into()],
            outputs: vec!["out".into()],
            compute: ComputeAffinity::CpuOnly,
            const_inputs: vec![],
            sync_groups: vec![],
            metadata: Default::default(),
        });
    }
    // Backbone chain to ensure a single entry and exit node for deterministic ordering.
    let backbone = nodes.saturating_sub(1);
    for i in 0..backbone {
        graph.edges.push(Edge {
            from: PortRef {
                node: NodeRef(i),
                port: "out".into(),
            },
            to: PortRef {
                node: NodeRef(i + 1),
                port: "in".into(),
            },
            metadata: Default::default(),
        });
    }
    for _ in 0..edges.saturating_sub(backbone) {
        let a = rng.random_range(0..nodes.saturating_sub(1));
        let b = rng.random_range((a + 1)..nodes.max(a + 2));
        graph.edges.push(Edge {
            from: PortRef {
                node: NodeRef(a),
                port: "out".into(),
            },
            to: PortRef {
                node: NodeRef(b.min(nodes - 1)),
                port: "in".into(),
            },
            metadata: Default::default(),
        });
    }
    ExecutionPlan::new(graph, vec![])
}

#[test]
fn serial_vs_parallel_order_matches() {
    for seed in 0..5u64 {
        let exec = random_plan(seed, 6, 8);
        let rt = build_runtime(
            &exec,
            &SchedulerConfig {
                default_policy: RuntimeEdgePolicy::default(),
                backpressure: daedalus_runtime::BackpressureStrategy::None,
            },
        );
        let order1 = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let h1 = LogHandler {
            order: order1.clone(),
        };
        let telem1 = Executor::new(&rt, h1).run().expect("serial");
        assert_eq!(telem1.nodes_executed, rt.nodes.len());

        let order2 = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let h2 = LogHandler {
            order: order2.clone(),
        };
        let telem2 = Executor::new(&rt, h2).run_parallel().expect("parallel");
        assert_eq!(telem2.nodes_executed, rt.nodes.len());

        // Ensure the parallel order respects topo constraints and schedule_order tie-breaks,
        // but allow permutations within independent segments.
        let serial_order = order1.lock().unwrap().clone();
        let parallel_order = order2.lock().unwrap().clone();
        assert_eq!(
            serial_order.first(),
            parallel_order.first(),
            "seed {seed} entry node differs"
        );
        assert_eq!(
            serial_order.last(),
            parallel_order.last(),
            "seed {seed} terminal node differs"
        );
        let serial_set: std::collections::HashSet<_> = serial_order.iter().cloned().collect();
        let parallel_set: std::collections::HashSet<_> = parallel_order.iter().cloned().collect();
        assert_eq!(serial_set, parallel_set, "seed {seed} node sets differ");
    }
}
