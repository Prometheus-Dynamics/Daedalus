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

/// Every value of the enums a document carries (compute affinities, sync policies, backpressure
/// strategies, set or not) serializes to JSON the checked schema accepts, so the schema cannot
/// drift from `daedalus-core` (its enum lists are generated from each type's `ALL`).
#[test]
fn every_enum_variant_validates_against_the_schema() {
    use daedalus_core::{compute::ComputeAffinity, policy::BackpressureStrategy, sync::SyncPolicy};

    let backpressure = std::iter::once(None).chain(BackpressureStrategy::ALL.map(Some));
    let groups: Vec<SyncGroup> = SyncPolicy::ALL
        .into_iter()
        .flat_map(|policy| {
            backpressure.clone().map(move |backpressure| SyncGroup {
                name: format!("{policy:?}"),
                policy,
                backpressure,
                capacity: Some(2),
                ports: vec!["in".into()],
            })
        })
        .collect();
    let mut graph = Graph::default();
    for compute in ComputeAffinity::ALL {
        let mut node = NodeInstance::new("demo:node")
            .with_inputs(["in"])
            .with_outputs(["out"])
            .with_compute(compute)
            .with_const_input(
                "in",
                Value::List(vec![Value::Int(1), Value::String("s".into())]),
            );
        node.sync_groups = groups.clone();
        node.metadata
            .insert("daedalus.node.fire".into(), Value::String("all".into()));
        graph.nodes.push(node);
    }
    graph.edges.push(Edge::new(0, "out", 1, "in"));
    let json = serde_json::to_value(GraphDocument::new(graph)).unwrap();
    let schema = GraphDocument::json_schema();
    let errors = validate(&schema, &schema, &json, "$");
    assert!(errors.is_empty(), "{errors:#?}");

    // The validator itself rejects a variant the schema does not list.
    let mut bogus = json.clone();
    bogus["graph"]["nodes"][0]["sync_groups"][0]["policy"] = "Bogus".into();
    assert!(!validate(&schema, &schema, &bogus, "$").is_empty());
}

/// A minimal JSON Schema validator for the keywords the document schema uses (`pattern` is not
/// checked); returns one message per violation.
fn validate(
    root: &serde_json::Value,
    schema: &serde_json::Value,
    value: &serde_json::Value,
    path: &str,
) -> Vec<String> {
    use serde_json::Value as J;
    let mut errors = Vec::new();
    let at = |message: String| format!("{path}: {message}");
    if let Some(reference) = schema.get("$ref").and_then(J::as_str) {
        let target = reference
            .strip_prefix("#/")
            .unwrap_or_default()
            .split('/')
            .fold(root, |node, key| &node[key]);
        return validate(root, target, value, path);
    }
    if let Some(types) = schema.get("type") {
        let types: Vec<&str> = match types {
            J::String(ty) => vec![ty.as_str()],
            J::Array(tys) => tys.iter().filter_map(J::as_str).collect(),
            _ => Vec::new(),
        };
        let matches = |ty: &str| match ty {
            "null" => value.is_null(),
            "boolean" => value.is_boolean(),
            "integer" => value.is_i64() || value.is_u64(),
            "number" => value.is_number(),
            "string" => value.is_string(),
            "array" => value.is_array(),
            "object" => value.is_object(),
            _ => false,
        };
        if !types.iter().any(|ty| matches(ty)) {
            errors.push(at(format!("{value} is not of type {types:?}")));
            return errors;
        }
    }
    if let Some(options) = schema.get("enum").and_then(J::as_array)
        && !options.contains(value)
    {
        errors.push(at(format!("{value} is not one of {options:?}")));
    }
    if let Some(expected) = schema.get("const")
        && expected != value
    {
        errors.push(at(format!("{value} is not {expected}")));
    }
    if let Some(minimum) = schema.get("minimum").and_then(J::as_f64)
        && value.as_f64().is_some_and(|v| v < minimum)
    {
        errors.push(at(format!("{value} is below {minimum}")));
    }
    if let Some(maximum) = schema.get("maximum").and_then(J::as_f64)
        && value.as_f64().is_some_and(|v| v > maximum)
    {
        errors.push(at(format!("{value} is above {maximum}")));
    }
    for (keyword, exactly_one) in [("oneOf", true), ("anyOf", false)] {
        if let Some(options) = schema.get(keyword).and_then(J::as_array) {
            let passing = options
                .iter()
                .filter(|option| validate(root, option, value, path).is_empty())
                .count();
            if passing == 0 || (exactly_one && passing > 1) {
                errors.push(at(format!(
                    "{value} matches {passing} of the {keyword} options"
                )));
            }
        }
    }
    if let Some(object) = value.as_object() {
        let props = schema.get("properties").and_then(J::as_object);
        for required in schema
            .get("required")
            .and_then(J::as_array)
            .into_iter()
            .flatten()
        {
            if !object.contains_key(required.as_str().unwrap_or_default()) {
                errors.push(at(format!("missing required {required}")));
            }
        }
        for (key, item) in object {
            let item_path = format!("{path}.{key}");
            match (
                props.and_then(|props| props.get(key)),
                schema.get("additionalProperties"),
            ) {
                (Some(prop), _) => errors.extend(validate(root, prop, item, &item_path)),
                (None, Some(J::Bool(false))) => errors.push(format!("{item_path}: not allowed")),
                (None, Some(extra)) => errors.extend(validate(root, extra, item, &item_path)),
                (None, None) => {}
            }
        }
    }
    if let Some(array) = value.as_array() {
        if let Some(min) = schema.get("minItems").and_then(J::as_u64)
            && (array.len() as u64) < min
        {
            errors.push(at(format!("fewer than {min} items")));
        }
        let prefix = schema
            .get("prefixItems")
            .and_then(J::as_array)
            .map_or(&[][..], Vec::as_slice);
        for (idx, item) in array.iter().enumerate() {
            let item_path = format!("{path}[{idx}]");
            match (prefix.get(idx), schema.get("items")) {
                (Some(prop), _) => errors.extend(validate(root, prop, item, &item_path)),
                (None, Some(J::Bool(false))) => errors.push(format!("{item_path}: not allowed")),
                (None, Some(items)) => errors.extend(validate(root, items, item, &item_path)),
                (None, None) => {}
            }
        }
    }
    errors
}
