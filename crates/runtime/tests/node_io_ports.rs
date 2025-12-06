use daedalus_planner::{
    ComputeAffinity, Edge, ExecutionPlan, Graph, NodeInstance, NodeRef, PortRef,
};
use daedalus_runtime::{
    BackpressureStrategy, EdgePolicyKind, Executor, NodeHandler, RuntimeNode, SchedulerConfig,
    build_runtime,
    executor::{EdgePayload, NodeError},
    io::NodeIo,
};

struct Handler {
    seen_ports: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
}

impl NodeHandler for Handler {
    fn run(
        &self,
        node: &RuntimeNode,
        _ctx: &daedalus_runtime::state::ExecutionContext,
        io: &mut NodeIo,
    ) -> Result<(), NodeError> {
        match node.id.as_str() {
            "prod" => {
                io.push_output(
                    Some("a"),
                    EdgePayload::Bytes(std::sync::Arc::from(&b"hello"[..])),
                );
            }
            "cons" => {
                if let Some((port, _)) = io.inputs().first() {
                    self.seen_ports.lock().unwrap().push(port.clone());
                }
            }
            _ => {}
        }
        Ok(())
    }
}

#[test]
fn node_io_respects_ports_and_policies() {
    // Graph: producer has two outputs (a -> consumer, b -> unused)
    let mut graph = Graph::default();
    graph.nodes.push(NodeInstance {
        id: daedalus_registry::ids::NodeId::new("prod"),
        bundle: None,
        label: None,
        inputs: vec![],
        outputs: vec!["a".into(), "b".into()],
        compute: ComputeAffinity::CpuOnly,
        const_inputs: vec![],
        sync_groups: vec![],
        metadata: Default::default(),
    });
    graph.nodes.push(NodeInstance {
        id: daedalus_registry::ids::NodeId::new("cons"),
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
            port: "a".into(),
        },
        to: PortRef {
            node: NodeRef(1),
            port: "in".into(),
        },
        metadata: Default::default(),
    });

    let exec = ExecutionPlan::new(graph, vec![]);
    let rt = build_runtime(
        &exec,
        &SchedulerConfig {
            default_policy: EdgePolicyKind::Fifo,
            backpressure: BackpressureStrategy::None,
            lockfree_queues: false,
        },
    );

    let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let handler = Handler {
        seen_ports: seen.clone(),
    };
    let telemetry = Executor::new(&rt, handler).run().expect("runtime run");
    assert_eq!(telemetry.nodes_executed, 2);
    let ports = seen.lock().unwrap().clone();
    assert_eq!(ports, vec!["in".to_string()]);
}
