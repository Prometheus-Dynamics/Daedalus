use daedalus_data::model::Value;
use daedalus_planner::{
    Edge, Graph, GraphDocument, GraphDocumentError, NodeInstance, NodeRef, PluginRequirement,
    PortRef, UnmetReason, check_plugin_requirements,
};
use daedalus_registry::ids::NodeId;

fn node(id: &str, inputs: &[&str], outputs: &[&str]) -> NodeInstance {
    NodeInstance {
        id: NodeId::new(id),
        bundle: None,
        label: None,
        inputs: inputs.iter().map(|s| s.to_string()).collect(),
        outputs: outputs.iter().map(|s| s.to_string()).collect(),
        compute: Default::default(),
        const_inputs: vec![("k".into(), Value::Int(3))],
        sync_groups: Vec::new(),
        metadata: Default::default(),
    }
}

fn sample_graph() -> Graph {
    let mut graph = Graph::default();
    graph.nodes.push(node("demo.math:source", &[], &["out"]));
    graph.nodes.push(node("demo.math:sink", &["in"], &[]));
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
    graph
        .metadata
        .insert("rate".into(), Value::String("30hz".into()));
    graph
}

fn sample_document() -> GraphDocument {
    GraphDocument::new(sample_graph())
        .require(PluginRequirement::new("demo.math").with_version(">=1.2.0"))
        .require(PluginRequirement::new("demo.io"))
        .with_metadata("title", "Example")
        .with_metadata("editor", serde_json::json!({"zoom": 2, "x": 1}))
}

#[test]
fn versioned_document_roundtrips() {
    let doc = sample_document();
    let json = doc.to_json_pretty().unwrap();
    let parsed = GraphDocument::from_json(&json).unwrap();
    assert_eq!(parsed, doc);
    assert_eq!(parsed.to_json_pretty().unwrap(), json);

    let compact = doc.to_json().unwrap();
    assert_eq!(GraphDocument::from_json(&compact).unwrap(), doc);
    // Strict serde impl accepts the versioned form too.
    let via_serde: GraphDocument = serde_json::from_str(&compact).unwrap();
    assert_eq!(via_serde, doc);
}

#[test]
fn deterministic_json_snapshot() {
    let json = sample_document().to_json_pretty().unwrap();
    let expected = r#"{
  "format": "daedalus.graph",
  "schema_version": 1,
  "requires": [
    {
      "id": "demo.math",
      "version": ">=1.2.0"
    },
    {
      "id": "demo.io"
    }
  ],
  "metadata": {
    "editor": {
      "x": 1,
      "zoom": 2
    },
    "title": "Example"
  },
  "graph": {
    "nodes": [
      {
        "id": "demo.math:source",
        "bundle": null,
        "label": null,
        "inputs": [],
        "outputs": [
          "out"
        ],
        "compute": "CpuOnly",
        "const_inputs": [
          [
            "k",
            {
              "type": "Int",
              "value": 3
            }
          ]
        ]
      },
      {
        "id": "demo.math:sink",
        "bundle": null,
        "label": null,
        "inputs": [
          "in"
        ],
        "outputs": [],
        "compute": "CpuOnly",
        "const_inputs": [
          [
            "k",
            {
              "type": "Int",
              "value": 3
            }
          ]
        ]
      }
    ],
    "edges": [
      {
        "from": {
          "node": 0,
          "port": "out"
        },
        "to": {
          "node": 1,
          "port": "in"
        }
      }
    ],
    "metadata": {
      "rate": "30hz"
    }
  }
}"#;
    assert_eq!(json, expected);
    // Building the same document twice yields byte-identical output.
    assert_eq!(sample_document().to_json_pretty().unwrap(), json);
}

#[test]
fn bare_graph_is_rejected() {
    let json = serde_json::to_string(&sample_graph()).unwrap();
    let err = GraphDocument::from_json(&json).unwrap_err();
    assert!(matches!(err, GraphDocumentError::UnknownFormat { found } if found == "<missing>"));
}

#[test]
fn future_schema_version_is_rejected() {
    let json = r#"{"format":"daedalus.graph","schema_version":2,"graph":{"nodes":[],"edges":[]}}"#;
    let err = GraphDocument::from_json(json).unwrap_err();
    assert!(matches!(
        err,
        GraphDocumentError::UnsupportedSchemaVersion {
            found: 2,
            supported: 1
        }
    ));
    assert!(serde_json::from_str::<GraphDocument>(json).is_err());
}

#[test]
fn bad_documents_report_typed_errors() {
    let err = GraphDocument::from_json(r#"{"format":"other","schema_version":1}"#).unwrap_err();
    assert!(matches!(err, GraphDocumentError::UnknownFormat { found } if found == "other"));

    let err = GraphDocument::from_json(r#"{"format":"daedalus.graph"}"#).unwrap_err();
    assert!(matches!(err, GraphDocumentError::MissingSchemaVersion));

    let err = GraphDocument::from_json("[1,2]").unwrap_err();
    assert!(matches!(err, GraphDocumentError::NotAnObject));

    let err = GraphDocument::from_json("{not json").unwrap_err();
    assert!(matches!(err, GraphDocumentError::Syntax(_)));

    let json = r#"{"format":"daedalus.graph","schema_version":1,
        "graph":{"nodes":[{"id":"a","bundle":null,"label":null,"inputs":[],"outputs":7}],"edges":[]}}"#;
    let err = GraphDocument::from_json(json).unwrap_err();
    match &err {
        GraphDocumentError::Invalid { path, .. } => assert_eq!(path, "graph.nodes[0].outputs"),
        other => panic!("unexpected error: {other:?}"),
    }

    let json = r#"{"format":"daedalus.graph","schema_version":1,
        "requires":[{"id":"demo.math","version":">=abc"}],"graph":{"nodes":[],"edges":[]}}"#;
    let err = GraphDocument::from_json(json).unwrap_err();
    assert!(matches!(
        err,
        GraphDocumentError::InvalidRequirement { index: 0, .. }
    ));
}

fn invalid_path(json: &str) -> String {
    match GraphDocument::from_json(json).unwrap_err() {
        GraphDocumentError::Invalid { path, source } => {
            assert!(source.to_string().contains("unknown field"), "{source}");
            path
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn unknown_fields_are_rejected_with_their_path() {
    let doc = serde_json::to_value(sample_document()).unwrap();
    let with = |pointer: &str, key: &str| {
        let mut doc = doc.clone();
        doc.pointer_mut(pointer)
            .unwrap()
            .as_object_mut()
            .unwrap()
            .insert(key.into(), serde_json::json!(true));
        doc.to_string()
    };
    assert_eq!(invalid_path(&with("", "grpah")), "grpah");
    assert_eq!(
        invalid_path(&with("/requires/0", "verison")),
        "requires[0].verison"
    );
    assert_eq!(invalid_path(&with("/graph", "edgse")), "graph.edgse");
    assert_eq!(
        invalid_path(&with("/graph/nodes/1", "lable")),
        "graph.nodes[1].lable"
    );
    assert_eq!(
        invalid_path(&with("/graph/edges/0", "form")),
        "graph.edges[0].form"
    );
    assert_eq!(
        invalid_path(&with("/graph/edges/0/to", "prot")),
        "graph.edges[0].to.prot"
    );
    // Free-form metadata maps stay open.
    GraphDocument::from_json(&with("/metadata", "anything")).unwrap();
    GraphDocument::from_json(&with("/graph/metadata", "anything")).unwrap();
    // The serde impl is equally strict.
    assert!(serde_json::from_str::<GraphDocument>(&with("", "grpah")).is_err());
}

#[test]
fn header_is_checked_before_the_rest_of_the_document() {
    // Format/version errors win regardless of field order or other problems.
    let json = r#"{"graph":{"nodes":7},"extra":1,"schema_version":1,"format":"other"}"#;
    let err = GraphDocument::from_json(json).unwrap_err();
    assert!(matches!(err, GraphDocumentError::UnknownFormat { found } if found == "other"));

    let json = r#"{"graph":{"nodes":7},"format":"daedalus.graph","schema_version":0}"#;
    let err = GraphDocument::from_json(json).unwrap_err();
    assert!(matches!(
        err,
        GraphDocumentError::UnsupportedSchemaVersion { found: 0, .. }
    ));

    let err = GraphDocument::from_json(r#"{"format":7}"#).unwrap_err();
    assert!(matches!(err, GraphDocumentError::UnknownFormat { found } if found == "7"));

    let err = GraphDocument::from_json(r#"{"format":"daedalus.graph","schema_version":"1"}"#)
        .unwrap_err();
    assert!(matches!(err, GraphDocumentError::MissingSchemaVersion));
}

#[test]
fn requirement_check_reports_unmet_plugins() {
    let requires = vec![
        PluginRequirement::new("a").with_version(">=1.2"),
        PluginRequirement::new("b").with_version("=2.0.0"),
        PluginRequirement::new("c"),
        PluginRequirement::new("d").with_version("1.0.0"),
        PluginRequirement::new("e").with_version("*"),
    ];
    let installed = |id: &str| -> Option<Option<&'static str>> {
        match id {
            "a" => Some(Some("1.10.0")),
            "b" => Some(Some("2.0.1")),
            "d" => Some(None),
            "e" => Some(None),
            _ => None,
        }
    };
    let err = check_plugin_requirements(&requires, installed).unwrap_err();
    assert_eq!(err.ids(), vec!["b", "c", "d"]);
    assert_eq!(
        err.unmet[0].reason,
        UnmetReason::VersionMismatch {
            installed: "2.0.1".into()
        }
    );
    assert_eq!(err.unmet[1].reason, UnmetReason::NotInstalled);
    assert_eq!(err.unmet[2].reason, UnmetReason::VersionUnknown);
    assert!(err.to_string().contains("c [not loaded]"));

    let all = |_: &str| -> Option<Option<&'static str>> { Some(Some("2.0.0")) };
    check_plugin_requirements(&requires, all).unwrap();
}
