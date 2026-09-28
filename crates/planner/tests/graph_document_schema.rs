#![cfg(feature = "schema")]

use std::collections::BTreeMap;

use daedalus_core::sync::SyncGroup;
use daedalus_data::model::Value;
use daedalus_planner::{
    Edge, GRAPH_DOCUMENT_FORMAT, GRAPH_DOCUMENT_SCHEMA_VERSION, Graph, GraphDocument, NodeInstance,
    NodeRef, PluginRequirement, PortRef,
};

const CHECKED_IN: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../docs/schema/daedalus.graph.v1.schema.json"
);

#[test]
fn checked_in_schema_is_up_to_date() {
    let generated = format!("{:#}\n", GraphDocument::json_schema());
    let checked_in = std::fs::read_to_string(CHECKED_IN).unwrap_or_default();
    assert!(
        generated == checked_in,
        "{CHECKED_IN} is out of date; regenerate it with\n  cargo run -p daedalus-planner \
         --features schema --bin graph_document_schema > docs/schema/daedalus.graph.v1.schema.json"
    );
}

#[test]
fn schema_describes_the_document_envelope() {
    let schema = GraphDocument::json_schema();
    assert_eq!(
        schema["properties"]["format"]["const"],
        GRAPH_DOCUMENT_FORMAT
    );
    assert_eq!(
        schema["properties"]["schema_version"]["const"],
        GRAPH_DOCUMENT_SCHEMA_VERSION
    );
    assert_eq!(
        schema["required"],
        serde_json::json!(["format", "schema_version", "graph"])
    );
    for def in ["plugin_requirement", "graph", "node", "edge", "port_ref"] {
        assert_eq!(
            schema["$defs"][def]["additionalProperties"], false,
            "{def} must be closed"
        );
    }
}

#[test]
fn schema_declares_every_serialized_field() {
    let port = |node, port: &str| PortRef {
        node: NodeRef(node),
        port: port.into(),
    };
    let meta = BTreeMap::from([("k".to_string(), Value::Bool(true))]);
    let mut graph = Graph::default();
    graph.nodes.push(NodeInstance {
        metadata: meta.clone(),
        ..NodeInstance::new("demo:node")
            .with_bundle("b")
            .with_label("l")
            .with_inputs(["in"])
            .with_outputs(["out"])
            .with_const_input("in", Value::Int(1))
            .with_sync_group(SyncGroup::default())
    });
    graph.edges.push(Edge {
        from: port(0, "out"),
        to: port(0, "in"),
        metadata: meta.clone(),
    });
    graph.metadata = meta;
    let doc = GraphDocument::new(graph)
        .require(PluginRequirement::new("demo").with_version("1.0"))
        .with_metadata("title", "t");
    let json = serde_json::to_value(&doc).unwrap();

    let schema = GraphDocument::json_schema();
    let assert_declared = |def: &str, object: &serde_json::Value| {
        let props = if def.is_empty() {
            &schema["properties"]
        } else {
            &schema["$defs"][def]["properties"]
        };
        for key in object.as_object().unwrap().keys() {
            assert!(
                props.get(key).is_some(),
                "`{key}` missing from schema `{def}`"
            );
        }
    };
    assert_declared("", &json);
    assert_declared("plugin_requirement", &json["requires"][0]);
    assert_declared("graph", &json["graph"]);
    assert_declared("node", &json["graph"]["nodes"][0]);
    assert_declared("sync_group", &json["graph"]["nodes"][0]["sync_groups"][0]);
    assert_declared("edge", &json["graph"]["edges"][0]);
    assert_declared("port_ref", &json["graph"]["edges"][0]["from"]);
}
