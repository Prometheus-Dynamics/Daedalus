use daedalus_engine::{Engine, EngineConfig};
use daedalus_planner::{ComputeAffinity, Edge, Graph, NodeInstance, NodeRef, PortRef};
use daedalus_registry::store::{NodeDescriptor, Port, Registry};

fn setup() -> (Engine, Registry, Graph) {
    let mut reg = Registry::new();
    let ty = daedalus_data::model::TypeExpr::Scalar(daedalus_data::model::ValueType::Int);
    let a = NodeDescriptor {
        id: daedalus_registry::ids::NodeId::new("a"),
        feature_flags: vec![],
        label: None,
        inputs: vec![],
        fanin_inputs: vec![],
        outputs: vec![Port {
            name: "out".into(),
            ty: ty.clone(),
            source: None,
            const_value: None,
        }],
        default_compute: ComputeAffinity::CpuOnly,
        sync_groups: Vec::new(),
        metadata: Default::default(),
    };
    let b = NodeDescriptor {
        id: daedalus_registry::ids::NodeId::new("b"),
        feature_flags: vec![],
        label: None,
        inputs: vec![Port {
            name: "in".into(),
            ty,
            source: None,
            const_value: None,
        }],
        fanin_inputs: vec![],
        outputs: vec![],
        default_compute: ComputeAffinity::CpuOnly,
        sync_groups: Vec::new(),
        metadata: Default::default(),
    };
    reg.register_node(a).unwrap();
    reg.register_node(b).unwrap();
    let graph = Graph {
        nodes: vec![
            NodeInstance {
                id: daedalus_registry::ids::NodeId::new("a"),
                bundle: None,
                label: None,
                inputs: vec![],
                outputs: vec!["out".into()],
                compute: ComputeAffinity::CpuOnly,
                const_inputs: vec![],
                sync_groups: vec![],
                metadata: Default::default(),
            },
            NodeInstance {
                id: daedalus_registry::ids::NodeId::new("b"),
                bundle: None,
                label: None,
                inputs: vec!["in".into()],
                outputs: vec![],
                compute: ComputeAffinity::CpuOnly,
                const_inputs: vec![],
                sync_groups: vec![],
                metadata: Default::default(),
            },
        ],
        edges: vec![Edge {
            from: PortRef {
                node: NodeRef(0),
                port: "out".into(),
            },
            to: PortRef {
                node: NodeRef(1),
                port: "in".into(),
            },
            metadata: Default::default(),
        }],
        metadata: Default::default(),
    };
    (Engine::new(EngineConfig::default()).unwrap(), reg, graph)
}

#[test]
fn planner_json_snapshot_is_stable() {
    let (engine, reg, graph) = setup();
    let plan = engine.plan(&reg, graph).unwrap().plan;
    // Snapshot on stable summary rather than full JSON to reduce churn.
    let summary = serde_json::json!({
        "hash": plan.hash,
        "node_ids": plan.graph.nodes.iter().map(|n| n.id.0.clone()).collect::<Vec<_>>(),
        "edge_count": plan.graph.edges.len(),
    });
    let expected = serde_json::json!({
        "hash": 15685890973402632871u64,
        "node_ids": ["a", "b"],
        "edge_count": 1,
    });
    assert_eq!(summary, expected);
}

#[test]
fn runtime_json_snapshot_is_stable() {
    let (engine, reg, graph) = setup();
    let runtime = engine
        .build_runtime_plan(&engine.plan(&reg, graph).unwrap().plan)
        .unwrap();
    let summary = serde_json::json!({
        "default_policy": format!("{:?}", runtime.default_policy),
        "backpressure": format!("{:?}", runtime.backpressure),
        "nodes": runtime.nodes.iter().map(|n| n.id.clone()).collect::<Vec<_>>(),
        "edges": runtime.edges.len(),
        "segments": runtime.segments.len(),
    });
    let expected = serde_json::json!({
        "default_policy": "Fifo",
        "backpressure": "None",
        "nodes": ["a", "b"],
        "edges": 1,
        "segments": 2,
    });
    assert_eq!(summary, expected);
}
