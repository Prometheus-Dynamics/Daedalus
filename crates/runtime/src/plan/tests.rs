use super::*;
use daedalus_planner::{Edge, Graph, NodeInstance, PortRef};

fn test_node(id: &str, stable_id: u128) -> RuntimeNode {
    RuntimeNode {
        id: id.to_string(),
        stable_id,
        bundle: None,
        label: None,
        compute: ComputeAffinity::CpuOnly,
        const_inputs: Vec::new(),
        sync_groups: Vec::new(),
        metadata: BTreeMap::new(),
    }
}

#[test]
fn stable_id_collision_returns_typed_runtime_plan_error() {
    let err = ensure_unique_stable_ids(&[test_node("first", 42), test_node("second", 42)])
        .expect_err("duplicate stable ids should fail");

    assert_eq!(
        err,
        RuntimePlanError::StableIdCollision {
            previous: "first".into(),
            current: "second".into(),
            stable_id: 42,
        }
    );
}

#[test]
fn try_from_execution_builds_empty_plan() {
    let plan = RuntimePlan::try_from_execution(&ExecutionPlan::new(Graph::default(), vec![]))
        .expect("empty plan should build");

    assert!(plan.nodes.is_empty());
}

#[test]
fn try_from_execution_reports_unknown_edge_pressure_policy() {
    let mut graph = Graph::default();
    graph.nodes.push(NodeInstance {
        id: daedalus_registry::ids::NodeId::new("src"),
        bundle: None,
        label: None,
        inputs: Vec::new(),
        outputs: vec!["out".into()],
        compute: ComputeAffinity::CpuOnly,
        const_inputs: Vec::new(),
        sync_groups: Vec::new(),
        metadata: BTreeMap::new(),
    });
    graph.nodes.push(NodeInstance {
        id: daedalus_registry::ids::NodeId::new("sink"),
        bundle: None,
        label: None,
        inputs: vec!["in".into()],
        outputs: Vec::new(),
        compute: ComputeAffinity::CpuOnly,
        const_inputs: Vec::new(),
        sync_groups: Vec::new(),
        metadata: BTreeMap::new(),
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
        metadata: BTreeMap::from([(
            EDGE_PRESSURE_POLICY_KEY.to_string(),
            Value::String("latest-typo".into()),
        )]),
    });

    let err = RuntimePlan::try_from_execution(&ExecutionPlan::new(graph, vec![]))
        .expect_err("unknown edge pressure policy should fail");

    assert_eq!(
        err,
        RuntimePlanError::UnknownEdgePressurePolicy {
            edge_index: 0,
            policy: "latest-typo".into(),
        }
    );
}
