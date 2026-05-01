use super::*;
use daedalus_data::model::{TypeExpr, ValueType};
use daedalus_registry::capability::{CapabilityRegistry, NodeDecl, PortDecl};

fn caps(nodes: impl IntoIterator<Item = NodeDecl>) -> CapabilityRegistry {
    let mut capabilities = CapabilityRegistry::new();
    for node in nodes {
        capabilities.register_node(node).unwrap();
    }
    capabilities
}

#[test]
fn graph_scope_try_node_helpers_report_missing_node_id() {
    let capabilities = caps([NodeDecl::new("demo.known")]);

    let err = match GraphBuilder::new(capabilities).try_nodes(|scope| {
        scope.try_add_node("known", "demo.known")?;
        scope.try_add_node("missing", "demo.missing")?;
        Ok(())
    }) {
        Ok(_) => panic!("missing scoped node should fail"),
        Err(err) => err,
    };

    assert_eq!(
        err,
        GraphBuildError::MissingNodeId {
            id: "demo.missing".into(),
        }
    );
}

#[test]
fn graph_scope_try_node_helpers_report_invalid_node_id() {
    let capabilities = caps([NodeDecl::new("demo.known")]);

    let err = match GraphBuilder::new(capabilities).try_nodes(|scope| {
        scope.try_add_node("bad", "Demo.Node")?;
        Ok(())
    }) {
        Ok(_) => panic!("invalid scoped node should fail"),
        Err(err) => err,
    };

    assert!(matches!(err, GraphBuildError::InvalidNodeId { id, .. } if id == "Demo.Node"));
}

#[test]
fn graph_scope_fallible_sections_build_without_panic_helpers() {
    let capabilities = caps([
        NodeDecl::new("demo.src").output(
            PortDecl::new("out", "typeexpr:{\"Scalar\":\"Bool\"}")
                .schema(TypeExpr::Scalar(ValueType::Bool)),
        ),
        NodeDecl::new("demo.sink").input(
            PortDecl::new("in", "typeexpr:{\"Scalar\":\"Bool\"}")
                .schema(TypeExpr::Scalar(ValueType::Bool)),
        ),
    ]);

    let graph = GraphBuilder::new(capabilities)
        .try_inputs(|scope| {
            let _ = scope.input("input");
            Ok(())
        })
        .expect("input section")
        .try_outputs(|scope| {
            let _ = scope.output("output");
            Ok(())
        })
        .expect("output section")
        .try_nodes(|scope| {
            scope.try_add_node("src", "demo.src")?;
            scope.try_add_node("sink", "demo.sink")?;
            Ok(())
        })
        .expect("node section")
        .try_edges(|scope| {
            let src = scope.node("src");
            let sink = scope.node("sink");
            scope.try_connect(&src.output("out"), &sink.input("in"))
        })
        .expect("edge section")
        .build();

    assert_eq!(graph.nodes.len(), 2);
    assert_eq!(graph.edges.len(), 1);
}
