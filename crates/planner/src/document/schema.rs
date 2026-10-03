//! JSON Schema (draft 2020-12) for the persisted graph document format.

use serde_json::{Value as JsonValue, json};

use super::{GRAPH_DOCUMENT_FORMAT, GRAPH_DOCUMENT_SCHEMA_VERSION, GraphDocument};

impl GraphDocument {
    /// JSON Schema (draft 2020-12) of the current document version, for editors and tooling.
    ///
    /// The checked-in copy lives at `docs/schema/daedalus.graph.v1.schema.json`.
    pub fn json_schema() -> JsonValue {
        let value = json!({ "$ref": "#/$defs/value" });
        let nullable_string = json!({ "type": ["string", "null"] });
        let strings = json!({ "type": "array", "items": { "type": "string" } });
        let value_map = json!({ "type": "object", "additionalProperties": value });
        let port_ref = json!({ "$ref": "#/$defs/port_ref" });
        json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "title": format!("Daedalus graph document v{GRAPH_DOCUMENT_SCHEMA_VERSION}"),
            "type": "object",
            "properties": {
                "format": { "const": GRAPH_DOCUMENT_FORMAT },
                "schema_version": { "const": GRAPH_DOCUMENT_SCHEMA_VERSION },
                "requires": {
                    "description": "Plugins that must be loaded for this graph to plan.",
                    "type": "array",
                    "items": { "$ref": "#/$defs/plugin_requirement" },
                },
                "metadata": {
                    "description": "Free-form application metadata (editor state, titles, ...).",
                    "type": "object",
                },
                "graph": { "$ref": "#/$defs/graph" },
            },
            "required": ["format", "schema_version", "graph"],
            "additionalProperties": false,
            "$defs": {
                "plugin_requirement": {
                    "type": "object",
                    "properties": {
                        "id": { "type": "string", "pattern": "\\S" },
                        "version": {
                            "description": "`=x.y.z` (exact), `>=x.y.z` or bare `x.y.z` (at least), `*` or empty (any).",
                            "type": ["string", "null"],
                            "pattern": "^\\s*(\\*|(>=|=)?\\s*[0-9]+(\\.[0-9]+)*([-+].*)?\\s*)?$",
                        },
                    },
                    "required": ["id"],
                    "additionalProperties": false,
                },
                "graph": {
                    "type": "object",
                    "properties": {
                        "nodes": { "type": "array", "items": { "$ref": "#/$defs/node" } },
                        "edges": { "type": "array", "items": { "$ref": "#/$defs/edge" } },
                        "metadata": {
                            "description": "Graph metadata visible to nodes at runtime, as plain JSON.",
                            "type": "object",
                        },
                    },
                    "required": ["nodes", "edges"],
                    "additionalProperties": false,
                },
                "node": {
                    "type": "object",
                    "properties": {
                        "id": { "description": "Registry node id.", "type": "string" },
                        "bundle": nullable_string,
                        "label": nullable_string,
                        "inputs": strings,
                        "outputs": strings,
                        "compute": { "enum": ["CpuOnly", "GpuPreferred", "GpuRequired"] },
                        "const_inputs": {
                            "type": "array",
                            "items": {
                                "type": "array",
                                "prefixItems": [{ "type": "string" }, value],
                                "items": false,
                                "minItems": 2,
                            },
                        },
                        "sync_groups": { "type": "array", "items": { "$ref": "#/$defs/sync_group" } },
                        "metadata": value_map,
                    },
                    "required": ["id", "inputs", "outputs"],
                    "additionalProperties": false,
                },
                "edge": {
                    "type": "object",
                    "properties": { "from": port_ref, "to": port_ref, "metadata": value_map },
                    "required": ["from", "to"],
                    "additionalProperties": false,
                },
                "port_ref": {
                    "type": "object",
                    "properties": {
                        "node": { "description": "Index into `graph.nodes`.", "type": "integer", "minimum": 0 },
                        "port": { "type": "string" },
                    },
                    "required": ["node", "port"],
                    "additionalProperties": false,
                },
                "sync_group": {
                    "type": "object",
                    "properties": {
                        "name": { "type": "string" },
                        "policy": { "enum": ["AllReady", "Latest", "ZipByTag"] },
                        "backpressure": { "enum": ["None", "BoundedQueues", "ErrorOnOverflow", null] },
                        "capacity": { "type": ["integer", "null"], "minimum": 0 },
                        "ports": strings,
                    },
                    "required": ["name", "policy", "ports"],
                },
                "value": daedalus_data::schema::value_json_schema("#/$defs/value"),
            },
        })
    }
}
