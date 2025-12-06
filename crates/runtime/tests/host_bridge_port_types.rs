use std::borrow::Cow;

use daedalus_data::model::{TypeExpr, Value};
use daedalus_planner::{
    ComputeAffinity, Edge, Graph, NodeInstance, NodeRef, PlannerConfig, PlannerInput, PortRef,
    build_plan,
};
use daedalus_registry::store::{NodeDescriptorBuilder, Registry};
use daedalus_runtime::RuntimePlan;
use daedalus_runtime::host_bridge::HostBridgeManager;

#[test]
fn host_bridge_manager_registers_inferred_port_types_from_plan() {
    let mut registry = Registry::new();
    registry
        .register_node(
            NodeDescriptorBuilder::new("io.host_bridge")
                .metadata("host_bridge", Value::Bool(true))
                .metadata("dynamic_inputs", Value::String(Cow::from("generic")))
                .metadata("dynamic_outputs", Value::String(Cow::from("generic")))
                .build()
                .unwrap(),
        )
        .unwrap();
    registry
        .register_node(
            NodeDescriptorBuilder::new("sink")
                .input("frame", TypeExpr::opaque("image:dynamic"))
                .build()
                .unwrap(),
        )
        .unwrap();

    let mut graph = Graph::default();
    graph.nodes.push(NodeInstance {
        id: daedalus_registry::ids::NodeId::new("io.host_bridge"),
        bundle: None,
        label: Some("host".into()),
        inputs: vec![],
        outputs: vec!["frame".into()],
        compute: ComputeAffinity::CpuOnly,
        const_inputs: vec![],
        sync_groups: vec![],
        metadata: std::collections::BTreeMap::from([("host_bridge".into(), Value::Bool(true))]),
    });
    graph.nodes.push(NodeInstance {
        id: daedalus_registry::ids::NodeId::new("sink"),
        bundle: None,
        label: None,
        inputs: vec!["frame".into()],
        outputs: vec![],
        compute: ComputeAffinity::CpuOnly,
        const_inputs: vec![],
        sync_groups: vec![],
        metadata: Default::default(),
    });
    graph.edges.push(Edge {
        from: PortRef {
            node: NodeRef(0),
            port: "frame".into(),
        },
        to: PortRef {
            node: NodeRef(1),
            port: "frame".into(),
        },
        metadata: Default::default(),
    });

    let planned = build_plan(
        PlannerInput {
            graph,
            registry: &registry,
        },
        PlannerConfig::default(),
    );
    let runtime_plan = RuntimePlan::from_execution(&planned.plan);
    let mgr = HostBridgeManager::from_plan(&runtime_plan);
    let handle = mgr.handle("host").expect("expected host bridge handle");
    assert_eq!(
        handle.outgoing_port_type("frame"),
        Some(&TypeExpr::opaque("image:dynamic"))
    );
}
