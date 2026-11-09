//! JSON Schema (draft 2020-12) of the document, for editors and tooling. The checked-in copy is
//! `docs/schema/daedalus.graph.v1.schema.json`, and a test keeps the two equal.

#![cfg(feature = "schema")]

use alloc::format;

use serde_json::{Value, json};

use super::{FORMAT, SCHEMA_VERSION};

fn object(properties: Value, required: &[&str]) -> Value {
    json!({
        "type": "object",
        "properties": properties,
        "required": required,
        "additionalProperties": false,
    })
}

pub fn json_schema() -> Value {
    let text = json!({ "type": "string" });
    let lit = json!({ "$ref": "#/$defs/lit" });
    let stage = json!({ "$ref": "#/$defs/stage_port" });
    let port = json!({ "$ref": "#/$defs/port_ref" });
    let lits = json!({ "type": "object", "additionalProperties": lit });
    let tagged = |name: &str, value: Value| {
        object(
            json!({ "type": { "const": name }, "value": value }),
            &["type", "value"],
        )
    };
    json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": format!("Daedalus graph document v{SCHEMA_VERSION}"),
        "type": "object",
        "properties": {
            "format": { "const": FORMAT },
            "schema_version": { "const": SCHEMA_VERSION },
            "requires": { "type": "array", "items": { "$ref": "#/$defs/requirement" } },
            "metadata": { "type": "object" },
            "graph": { "$ref": "#/$defs/graph" },
        },
        "required": ["format", "schema_version", "graph"],
        "additionalProperties": false,
        "$defs": {
            "requirement": object(json!({ "id": text, "version": text }), &["id"]),
            "lit": { "oneOf": [
                object(json!({ "type": { "const": "unit" } }), &["type"]),
                tagged("bool", json!({ "type": "boolean" })),
                tagged("int", json!({ "type": "integer" })),
                tagged("float", json!({ "type": "number" })),
                tagged("str", text.clone()),
                tagged("bytes", json!({ "type": "string", "pattern": "^([0-9a-fA-F]{2})*$" })),
                tagged("list", json!({ "type": "array", "items": lit })),
                tagged("struct", json!({ "type": "array", "items": object(
                    json!({ "name": text, "value": lit }), &["name", "value"]) })),
                tagged("enum", object(json!({ "name": text, "payload": lit }), &["name"])),
            ] },
            "node": object(json!({
                "id": text,
                "kind": { "enum": ["node", "group", "host_bridge", "control"] },
                "label": text,
                "backend": text,
                "consts": lits,
                "fire": text,
                "reusable": { "type": "boolean" },
                "rate": object(
                    json!({ "num": { "type": "integer" }, "den": { "type": "integer" } }),
                    &["num", "den"],
                ),
                "shareable": { "type": "boolean" },
                "ui_id": text,
                "port_types": { "type": "object", "additionalProperties": text },
                "inputs": { "type": "array", "items": text },
                "outputs": { "type": "array", "items": text },
                "metadata": lits,
            }), &["id"]),
            "port_ref": object(
                json!({ "node": { "type": "integer" }, "port": text }),
                &["node", "port"],
            ),
            "edge_policy": { "oneOf": [
                object(json!({ "kind": { "const": "fifo" } }), &["kind"]),
                object(json!({ "kind": { "const": "latest_only" } }), &["kind"]),
                object(json!({
                    "kind": { "const": "bounded" },
                    "capacity": { "type": "integer" },
                    "overflow": text,
                }), &["kind", "capacity", "overflow"]),
            ] },
            "edge": object(json!({
                "from": port,
                "to": port,
                "policy": { "$ref": "#/$defs/edge_policy" },
            }), &["from", "to"]),
            "stage_port": object(json!({ "stage": text, "port": text }), &["stage", "port"]),
            "group": object(json!({
                "id": text,
                "stages": { "type": "object", "additionalProperties": text },
                "connects": { "type": "array", "items": {
                    "type": "array",
                    "prefixItems": [stage, stage],
                    "minItems": 2,
                    "maxItems": 2,
                } },
                "inputs": { "type": "object", "additionalProperties": {
                    "type": "array",
                    "items": stage,
                } },
                "outputs": { "type": "object", "additionalProperties": stage },
                "consts": { "type": "array", "items": object(
                    json!({ "at": stage, "value": lit }), &["at", "value"]) },
            }), &["id", "stages"]),
            "graph": object(json!({
                "nodes": { "type": "array", "items": { "$ref": "#/$defs/node" } },
                "edges": { "type": "array", "items": { "$ref": "#/$defs/edge" } },
                "groups": { "type": "array", "items": { "$ref": "#/$defs/group" } },
            }), &["nodes"]),
        },
    })
}
