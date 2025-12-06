use std::sync::{Arc, Mutex};

use daedalus_planner::{
    ComputeAffinity, Edge, ExecutionPlan, Graph, NodeInstance, NodeRef, PortRef,
};
use daedalus_runtime::{
    EdgePolicyKind, Executor, NodeHandler, RuntimeNode, SchedulerConfig, build_runtime,
};

#[derive(Clone)]
struct LogHandler {
    log: Arc<Mutex<Vec<String>>>,
}

impl NodeHandler for LogHandler {
    fn run(
        &self,
        node: &RuntimeNode,
        _ctx: &daedalus_runtime::state::ExecutionContext,
        _io: &mut daedalus_runtime::io::NodeIo,
    ) -> Result<(), daedalus_runtime::NodeError> {
        self.log.lock().unwrap().push(node.id.clone());
        Ok(())
    }
}

fn chain_plan() -> ExecutionPlan {
    let mut graph = Graph::default();
    graph.nodes.push(NodeInstance {
        id: daedalus_registry::ids::NodeId::new("n0"),
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
        id: daedalus_registry::ids::NodeId::new("n1"),
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

#[test]
fn serial_and_parallel_scope_align() {
    let plan = chain_plan();
    let rt = build_runtime(
        &plan,
        &SchedulerConfig {
            default_policy: EdgePolicyKind::Fifo,
            backpressure: daedalus_runtime::BackpressureStrategy::None,
            lockfree_queues: false,
        },
    );

    let log1 = Arc::new(Mutex::new(Vec::new()));
    let h1 = LogHandler { log: log1.clone() };
    let telem1 = Executor::new(&rt, h1).run().expect("serial run");
    assert_eq!(telem1.nodes_executed, 2);

    let log2 = Arc::new(Mutex::new(Vec::new()));
    let h2 = LogHandler { log: log2.clone() };
    let telem2 = Executor::new(&rt, h2).run_parallel().expect("parallel run");
    assert_eq!(telem2.nodes_executed, 2);

    assert_eq!(*log1.lock().unwrap(), *log2.lock().unwrap());
}
