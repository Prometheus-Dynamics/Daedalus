use daedalus_graph::*;
use daedalus_types::Literal;

const GOLDEN: &str = include_str!("golden/graph.json");

#[test]
fn golden_document_round_trips_to_the_same_bytes() {
    let doc = GraphDoc::parse(GOLDEN).unwrap();
    assert_eq!(doc.to_json().unwrap(), GOLDEN);
    assert_eq!(doc.graph.nodes[1].backend.as_deref(), Some("cpu"));
    assert_eq!(
        doc.graph.nodes[1].consts["blob"],
        Lit::Bytes(vec![0, 255, 16])
    );
}

#[test]
fn unknown_fields_are_rejected_with_their_path() {
    let bad = GOLDEN.replace("\"label\": \"Source\"", "\"lable\": \"Source\"");
    match GraphDoc::parse(&bad).unwrap_err() {
        DocError::Invalid { path, message } => {
            assert_eq!(path, "graph.nodes[0].lable");
            assert!(message.contains("unknown field"), "{message}");
        }
        other => panic!("{other:?}"),
    }
    let top = GOLDEN.replacen("\"format\"", "\"extra\": 1,\n  \"format\"", 1);
    assert!(matches!(
        GraphDoc::parse(&top),
        Err(DocError::Invalid { .. })
    ));
}

#[test]
fn format_and_version_are_checked() {
    let wrong = GOLDEN.replace("daedalus.graph", "other.graph");
    assert!(matches!(
        GraphDoc::parse(&wrong),
        Err(DocError::UnknownFormat { .. })
    ));
    let newer = GOLDEN.replace("\"schema_version\": 1", "\"schema_version\": 2");
    assert_eq!(
        GraphDoc::parse(&newer).unwrap_err(),
        DocError::UnsupportedVersion { found: 2 }
    );
}

#[test]
fn bad_hex_is_rejected() {
    let odd = GOLDEN.replace("00ff10", "00f");
    assert!(GraphDoc::parse(&odd).is_err());
    let letters = GOLDEN.replace("00ff10", "zz");
    assert!(GraphDoc::parse(&letters).is_err());
}

#[test]
fn groups_round_trip() {
    let doc = GraphDoc::parse(GOLDEN).unwrap();
    let group = &doc.graph.groups[0];
    assert_eq!(group.stages["b"], "demo.gain");
    assert_eq!(group.connects[0].1.port, "in");
    let again = GraphDoc::parse(&doc.to_json().unwrap()).unwrap();
    assert_eq!(again.graph.groups, doc.graph.groups);
}

#[test]
fn patch_round_trips() {
    let patch = GraphPatch {
        version: 1,
        ops: vec![
            PatchOp::SetConst {
                node: NodeSelector {
                    index: Some(1),
                    id: None,
                },
                port: "gain".into(),
                value: Some(Lit::Int(3)),
            },
            PatchOp::ReplaceNodeId {
                node: NodeSelector {
                    index: None,
                    id: Some("a.b".into()),
                },
                new_id: "a.c".into(),
            },
            PatchOp::DeleteNode {
                node: NodeSelector::default(),
            },
        ],
    };
    assert_eq!(GraphPatch::parse(&patch.to_json().unwrap()).unwrap(), patch);
    assert!(matches!(
        GraphPatch::parse(r#"{ "ops": [{ "type": "delete_node", "node": {}, "x": 1 }] }"#),
        Err(DocError::Invalid { .. })
    ));
}

#[test]
fn checked_in_schema_matches_the_types() {
    let mut generated = serde_json::to_string_pretty(&json_schema()).unwrap();
    generated.push('\n');
    let file = include_str!("../../../docs/schema/daedalus.graph.v1.schema.json");
    assert_eq!(
        generated, file,
        "run: cargo run -p daedalus-graph --bin graph_schema"
    );
}

const SMALL: GraphDef<'static> = GraphDef {
    nodes: &[NodeDef {
        consts: &[("k", Literal::Int(1))],
        ..NodeDef::new("a.b")
    }],
    edges: &[],
    groups: &[],
};

#[test]
fn a_graph_is_a_constant() {
    assert_eq!(SMALL.nodes[0].id, "a.b");
    assert_eq!(SMALL.nodes[0].kind, NodeKind::Node);
    assert_eq!(SMALL.nodes[0].consts[0].1, Literal::Int(1));
}
