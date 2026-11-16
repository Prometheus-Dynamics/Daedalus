//! What a command prints. Every command's result is one JSON object with a fixed shape. With
//! `--json` it is printed as it is, inside an envelope. Without, the same object is printed as
//! indented `key: value` lines. The JSON of each value is the client library's (`daedalus_client::json`);
//! this file adds the envelope and the text form only.

use daedalus_client::json::diagnostics;
use daedalus_client::protocol::ServiceError;
use serde_json::{Map, Value, json};

pub(crate) fn service_error(error: &ServiceError) -> Value {
    json!({
        "kind": "daemon",
        "code": error.code.name(),
        "message": error.message,
        "diagnostics": diagnostics(&error.diagnostics),
    })
}

/// The envelope of a result that succeeded.
pub fn success(command: &str, result: Value) -> Value {
    json!({ "ok": true, "command": command, "result": result })
}

/// The envelope of a failure. `kind` is `daemon`, `connect` or `usage`.
pub fn failure(command: &str, error: Value) -> Value {
    json!({ "ok": false, "command": command, "error": error })
}

fn scalar_text(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        other => other.to_string(),
    }
}

fn text_into(out: &mut String, key: &str, value: &Value, depth: usize) {
    let pad = "  ".repeat(depth);
    match value {
        Value::Object(fields) if fields.is_empty() => out.push_str(&format!("{pad}{key}: {{}}\n")),
        Value::Object(fields) => {
            out.push_str(&format!("{pad}{key}:\n"));
            object_into(out, fields, depth + 1);
        }
        Value::Array(items) if items.is_empty() => out.push_str(&format!("{pad}{key}: []\n")),
        Value::Array(items)
            if items
                .iter()
                .all(|item| !item.is_object() && !item.is_array()) =>
        {
            let items: Vec<String> = items.iter().map(scalar_text).collect();
            out.push_str(&format!("{pad}{key}: {}\n", items.join(", ")));
        }
        Value::Array(items) => {
            out.push_str(&format!("{pad}{key}:\n"));
            for item in items {
                match item {
                    Value::Object(fields) => {
                        let mut block = String::new();
                        object_into(&mut block, fields, depth + 2);
                        // The first line of each item carries the list marker.
                        let marker = format!("{}- ", "  ".repeat(depth + 1));
                        let body = block.trim_start_matches(' ');
                        out.push_str(&marker);
                        out.push_str(body);
                    }
                    other => out.push_str(&format!("{}- {}\n", "  ".repeat(depth + 1), other)),
                }
            }
        }
        scalar => out.push_str(&format!("{pad}{key}: {}\n", scalar_text(scalar))),
    }
}

fn object_into(out: &mut String, fields: &Map<String, Value>, depth: usize) {
    for (key, value) in fields {
        text_into(out, key, value, depth);
    }
}

/// A result as text: one `key: value` line per field, nested ones indented.
pub fn text(result: &Value) -> String {
    let mut out = String::new();
    match result {
        Value::Object(fields) if fields.is_empty() => out.push_str("ok\n"),
        Value::Object(fields) => object_into(&mut out, fields, 0),
        other => out.push_str(&format!("{}\n", scalar_text(other))),
    }
    out
}
