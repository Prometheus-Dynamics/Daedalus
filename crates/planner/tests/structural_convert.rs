use daedalus_data::model::{EnumVariant, StructField, TypeExpr, ValueType};
use daedalus_planner::{
    ComputeAffinity, Edge, Graph, NodeInstance, NodeRef, PlannerConfig, PlannerInput, PortRef,
    build_plan,
};
use daedalus_registry::store::{NodeDescriptorBuilder, Registry};

#[test]
fn allows_struct_field_scalar_width_changes() {
    let mut registry = Registry::new();
    registry
        .register_node(
            NodeDescriptorBuilder::new("src")
                .output(
                    "cfg",
                    TypeExpr::Struct(vec![StructField {
                        name: "factor".into(),
                        ty: TypeExpr::Scalar(ValueType::Int),
                    }]),
                )
                .build()
                .unwrap(),
        )
        .unwrap();
    registry
        .register_node(
            NodeDescriptorBuilder::new("sink")
                .input(
                    "cfg",
                    TypeExpr::Struct(vec![StructField {
                        name: "factor".into(),
                        ty: TypeExpr::Scalar(ValueType::I32),
                    }]),
                )
                .build()
                .unwrap(),
        )
        .unwrap();

    let mut graph = Graph::default();
    graph.nodes.push(NodeInstance {
        id: daedalus_registry::ids::NodeId::new("src"),
        bundle: None,
        label: None,
        inputs: vec![],
        outputs: vec!["cfg".into()],
        compute: ComputeAffinity::CpuOnly,
        const_inputs: vec![],
        sync_groups: vec![],
        metadata: Default::default(),
    });
    graph.nodes.push(NodeInstance {
        id: daedalus_registry::ids::NodeId::new("sink"),
        bundle: None,
        label: None,
        inputs: vec!["cfg".into()],
        outputs: vec![],
        compute: ComputeAffinity::CpuOnly,
        const_inputs: vec![],
        sync_groups: vec![],
        metadata: Default::default(),
    });
    graph.edges.push(Edge {
        from: PortRef {
            node: NodeRef(0),
            port: "cfg".into(),
        },
        to: PortRef {
            node: NodeRef(1),
            port: "cfg".into(),
        },
        metadata: Default::default(),
    });

    let output = build_plan(
        PlannerInput {
            graph,
            registry: &registry,
        },
        PlannerConfig::default(),
    );
    assert!(
        !output
            .diagnostics
            .iter()
            .any(|d| matches!(d.code, daedalus_planner::DiagnosticCode::ConverterMissing)),
        "expected no ConverterMissing diagnostics, got: {:?}",
        output.diagnostics
    );
}

#[test]
fn allows_enum_variant_payload_scalar_width_changes() {
    let mut registry = Registry::new();
    let from = TypeExpr::Enum(vec![EnumVariant {
        name: "A".into(),
        ty: Some(TypeExpr::Scalar(ValueType::Int)),
    }]);
    let to = TypeExpr::Enum(vec![EnumVariant {
        name: "A".into(),
        ty: Some(TypeExpr::Scalar(ValueType::I32)),
    }]);
    registry
        .register_node(
            NodeDescriptorBuilder::new("src")
                .output("mode", from)
                .build()
                .unwrap(),
        )
        .unwrap();
    registry
        .register_node(
            NodeDescriptorBuilder::new("sink")
                .input("mode", to)
                .build()
                .unwrap(),
        )
        .unwrap();

    let mut graph = Graph::default();
    graph.nodes.push(NodeInstance {
        id: daedalus_registry::ids::NodeId::new("src"),
        bundle: None,
        label: None,
        inputs: vec![],
        outputs: vec!["mode".into()],
        compute: ComputeAffinity::CpuOnly,
        const_inputs: vec![],
        sync_groups: vec![],
        metadata: Default::default(),
    });
    graph.nodes.push(NodeInstance {
        id: daedalus_registry::ids::NodeId::new("sink"),
        bundle: None,
        label: None,
        inputs: vec!["mode".into()],
        outputs: vec![],
        compute: ComputeAffinity::CpuOnly,
        const_inputs: vec![],
        sync_groups: vec![],
        metadata: Default::default(),
    });
    graph.edges.push(Edge {
        from: PortRef {
            node: NodeRef(0),
            port: "mode".into(),
        },
        to: PortRef {
            node: NodeRef(1),
            port: "mode".into(),
        },
        metadata: Default::default(),
    });

    let output = build_plan(
        PlannerInput {
            graph,
            registry: &registry,
        },
        PlannerConfig::default(),
    );
    assert!(
        !output
            .diagnostics
            .iter()
            .any(|d| matches!(d.code, daedalus_planner::DiagnosticCode::ConverterMissing)),
        "expected no ConverterMissing diagnostics, got: {:?}",
        output.diagnostics
    );
}
