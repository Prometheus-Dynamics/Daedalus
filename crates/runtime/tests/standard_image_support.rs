#![cfg(feature = "plugins")]

use daedalus_data::model::TypeExpr;
use daedalus_planner::{
    ComputeAffinity, Edge, Graph, NodeInstance, NodeRef, PlannerConfig, PlannerInput, PortRef,
    build_plan,
};
use daedalus_registry::ids::NodeId;
use daedalus_registry::store::NodeDescriptorBuilder;
use daedalus_runtime::plugins::PluginRegistry;

#[test]
fn standard_image_support_allows_gray_outputs_to_feed_dynamic_inputs() {
    let mut registry = PluginRegistry::new();
    registry
        .register_standard_image_support()
        .expect("register standard image support");
    registry
        .registry
        .register_node(
            NodeDescriptorBuilder::new("source.gray")
                .output("frame", TypeExpr::opaque("image:gray8"))
                .build()
                .expect("gray source descriptor"),
        )
        .expect("register gray source");
    registry
        .registry
        .register_node(
            NodeDescriptorBuilder::new("sink.dynamic")
                .input("frame", TypeExpr::opaque("image:dynamic"))
                .build()
                .expect("dynamic sink descriptor"),
        )
        .expect("register dynamic sink");

    let mut graph = Graph::default();
    graph.nodes.push(NodeInstance {
        id: NodeId::new("source.gray"),
        bundle: None,
        label: None,
        inputs: vec![],
        outputs: vec!["frame".into()],
        compute: ComputeAffinity::CpuOnly,
        const_inputs: vec![],
        sync_groups: vec![],
        metadata: Default::default(),
    });
    graph.nodes.push(NodeInstance {
        id: NodeId::new("sink.dynamic"),
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
            registry: &registry.registry,
        },
        PlannerConfig::default(),
    );

    assert!(
        planned.diagnostics.is_empty(),
        "expected image family compatibility without converter nodes, got diagnostics: {:?}",
        planned.diagnostics
    );
}
