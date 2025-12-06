use daedalus_engine::{Engine, EngineConfig};
use daedalus_planner::{ComputeAffinity, Edge, Graph, NodeInstance, NodeRef, PortRef};
use daedalus_registry::store::{NodeDescriptor, Port, Registry};

fn registry_with_two() -> Registry {
    let mut reg = Registry::new();
    let ty = daedalus_data::model::TypeExpr::Scalar(daedalus_data::model::ValueType::Int);
    let n1 = NodeDescriptor {
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
    let n2 = NodeDescriptor {
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
    reg.register_node(n1).unwrap();
    reg.register_node(n2).unwrap();
    reg
}

#[test]
fn planner_dump_is_stable() {
    let reg = registry_with_two();
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
        metadata_values: Default::default(),
    };
    let engine = Engine::new(EngineConfig::default()).unwrap();
    let plan = engine.plan(&reg, graph).unwrap().plan;
    let dump = serde_json::to_string_pretty(&plan).unwrap();
    let again = serde_json::to_string_pretty(&plan).unwrap();
    assert_eq!(dump, again);
}
