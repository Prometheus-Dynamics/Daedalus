use std::borrow::Cow;

use daedalus_data::model::{TypeExpr, Value, ValueType};
use daedalus_planner::{
    ComputeAffinity, DiagnosticCode, Edge, Graph, NodeInstance, NodeRef, PlannerConfig,
    PlannerInput, PortRef, build_plan,
};
use daedalus_registry::store::{NodeDescriptorBuilder, Registry};

fn host_bridge_descriptor() -> daedalus_registry::store::NodeDescriptor {
    NodeDescriptorBuilder::new("io.host_bridge")
        .metadata("host_bridge", Value::Bool(true))
        .metadata("dynamic_inputs", Value::String(Cow::from("generic")))
        .metadata("dynamic_outputs", Value::String(Cow::from("generic")))
        .build()
        .unwrap()
}

#[test]
fn infers_host_bridge_output_type_from_connection() {
    let mut registry = Registry::new();
    registry.register_node(host_bridge_descriptor()).unwrap();
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
        // Untrusted clients might try to set `dynamic_outputs` directly to force a type.
        // The planner must ignore this and solve from edges.
        metadata: std::collections::BTreeMap::from([(
            "dynamic_outputs".into(),
            Value::String(Cow::from("bytes")),
        )]),
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
            .any(|d| matches!(d.code, DiagnosticCode::TypeMismatch)),
        "unexpected TypeMismatch diagnostics: {:?}",
        output.diagnostics
    );
    assert!(
        !output
            .diagnostics
            .iter()
            .any(|d| matches!(d.code, DiagnosticCode::ConverterMissing)),
        "unexpected ConverterMissing diagnostics: {:?}",
        output.diagnostics
    );

    let host = &output.plan.graph.nodes[0];
    let types = host
        .metadata
        .get("dynamic_output_types")
        .expect("expected dynamic_output_types in host metadata");
    let daedalus_data::model::Value::Map(entries) = types else {
        panic!("expected dynamic_output_types to be a map, got {types:?}");
    };
    let (_, json) = entries
        .iter()
        .find(|(k, _)| matches!(k, Value::String(s) if s.eq_ignore_ascii_case("frame")))
        .expect("expected inferred frame entry");
    let Value::String(json) = json else {
        panic!("expected inferred entry to be json string, got {json:?}");
    };
    let ty: TypeExpr = serde_json::from_str(json).expect("parse inferred TypeExpr");
    assert_eq!(ty, TypeExpr::opaque("image:dynamic"));
}

#[test]
fn rejects_conflicting_inference_for_single_generic_port() {
    let mut registry = Registry::new();
    registry.register_node(host_bridge_descriptor()).unwrap();
    registry
        .register_node(
            NodeDescriptorBuilder::new("sink_a")
                .input("x", TypeExpr::Scalar(ValueType::Int))
                .build()
                .unwrap(),
        )
        .unwrap();
    registry
        .register_node(
            NodeDescriptorBuilder::new("sink_b")
                .input("x", TypeExpr::Scalar(ValueType::Float))
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
        outputs: vec!["x".into()],
        compute: ComputeAffinity::CpuOnly,
        const_inputs: vec![],
        sync_groups: vec![],
        metadata: Default::default(),
    });
    graph.nodes.push(NodeInstance {
        id: daedalus_registry::ids::NodeId::new("sink_a"),
        bundle: None,
        label: None,
        inputs: vec!["x".into()],
        outputs: vec![],
        compute: ComputeAffinity::CpuOnly,
        const_inputs: vec![],
        sync_groups: vec![],
        metadata: Default::default(),
    });
    graph.nodes.push(NodeInstance {
        id: daedalus_registry::ids::NodeId::new("sink_b"),
        bundle: None,
        label: None,
        inputs: vec!["x".into()],
        outputs: vec![],
        compute: ComputeAffinity::CpuOnly,
        const_inputs: vec![],
        sync_groups: vec![],
        metadata: Default::default(),
    });
    graph.edges.push(Edge {
        from: PortRef {
            node: NodeRef(0),
            port: "x".into(),
        },
        to: PortRef {
            node: NodeRef(1),
            port: "x".into(),
        },
        metadata: Default::default(),
    });
    graph.edges.push(Edge {
        from: PortRef {
            node: NodeRef(0),
            port: "x".into(),
        },
        to: PortRef {
            node: NodeRef(2),
            port: "x".into(),
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
        output
            .diagnostics
            .iter()
            .any(|d| matches!(d.code, DiagnosticCode::TypeMismatch)),
        "expected TypeMismatch diagnostics, got: {:?}",
        output.diagnostics
    );
}
