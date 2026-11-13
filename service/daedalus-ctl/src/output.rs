//! What a command prints. Every command's result is one JSON object with a fixed shape. With
//! `--json` it is printed as it is, inside an envelope. Without, the same object is printed as
//! indented `key: value` lines. The JSON is this tool's own: it is not the wire.

use daedalus_client::protocol::{
    CarryReport, Diagnostic, ErrorCode, Event, EventBody, Inspection, Names, PlanSummary,
    PluginInfo, RunState, ServiceError, Severity, StepSummary, TraceEntry, TypedValue,
};
use daedalus_client::{WireType, WireValue};
use daedalus_types::Scalar;
use serde_json::{Map, Value, json};

/// The name of a wire type, as `feed` takes it and `take` shows it.
pub fn type_name(ty: &WireType) -> String {
    match ty {
        WireType::Scalar(scalar) => scalar_name(*scalar).to_owned(),
        WireType::Opaque(key) => format!("opaque({key})"),
        WireType::Optional(inner) => format!("optional<{}>", type_name(inner)),
        WireType::List(inner) => format!("list<{}>", type_name(inner)),
        WireType::Map(key, value) => format!("map<{}, {}>", type_name(key), type_name(value)),
        WireType::Tuple(items) => {
            let items: Vec<String> = items.iter().map(type_name).collect();
            format!("tuple<{}>", items.join(", "))
        }
        WireType::Struct(_) => "struct".to_owned(),
        WireType::Enum(_) => "enum".to_owned(),
    }
}

pub(crate) const SCALAR_NAMES: [(&str, Scalar); 16] = [
    ("unit", Scalar::Unit),
    ("bool", Scalar::Bool),
    ("i8", Scalar::I8),
    ("i16", Scalar::I16),
    ("i32", Scalar::I32),
    ("i64", Scalar::Int),
    ("isize", Scalar::ISize),
    ("u8", Scalar::U8),
    ("u16", Scalar::U16),
    ("u32", Scalar::U32),
    ("u64", Scalar::U64),
    ("usize", Scalar::USize),
    ("f32", Scalar::F32),
    ("f64", Scalar::Float),
    ("string", Scalar::String),
    ("bytes", Scalar::Bytes),
];

fn scalar_name(scalar: Scalar) -> &'static str {
    SCALAR_NAMES
        .iter()
        .find(|(_, known)| *known == scalar)
        .map_or("scalar", |(name, _)| name)
}

pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// A wire value as JSON. Bytes are hex text, a struct is an object by field name, an enum is
/// its variant's name with its payload, and a map is a list of pairs.
pub(crate) fn value_json(ty: &WireType, value: &WireValue) -> Value {
    match (ty, value) {
        (_, WireValue::Unit) => Value::Null,
        (_, WireValue::Bool(v)) => json!(v),
        (_, WireValue::I8(v)) => json!(v),
        (_, WireValue::I16(v)) => json!(v),
        (_, WireValue::I32(v)) => json!(v),
        (_, WireValue::I64(v) | WireValue::ISize(v)) => json!(v),
        (_, WireValue::U8(v)) => json!(v),
        (_, WireValue::U16(v)) => json!(v),
        (_, WireValue::U32(v)) => json!(v),
        (_, WireValue::U64(v) | WireValue::USize(v)) => json!(v),
        (_, WireValue::F32(v)) => json!(v),
        (_, WireValue::F64(v)) => json!(v),
        (_, WireValue::String(v)) => json!(v),
        (_, WireValue::Bytes(v) | WireValue::Opaque(v)) => json!(hex(v)),
        (WireType::Optional(_), WireValue::Optional(None)) => Value::Null,
        (WireType::Optional(inner), WireValue::Optional(Some(item))) => value_json(inner, item),
        (WireType::List(inner), WireValue::List(items)) => {
            Value::Array(items.iter().map(|item| value_json(inner, item)).collect())
        }
        (WireType::Map(key_ty, value_ty), WireValue::Map(pairs)) => Value::Array(
            pairs
                .iter()
                .map(|(key, item)| json!([value_json(key_ty, key), value_json(value_ty, item)]))
                .collect(),
        ),
        (WireType::Tuple(types), WireValue::Tuple(items)) => Value::Array(
            types
                .iter()
                .zip(items)
                .map(|(ty, item)| value_json(ty, item))
                .collect(),
        ),
        (WireType::Struct(fields), WireValue::Struct(items)) => Value::Object(
            fields
                .iter()
                .zip(items)
                .map(|((name, ty), item)| (name.clone(), value_json(ty, item)))
                .collect(),
        ),
        (WireType::Enum(variants), WireValue::Enum { variant, payload }) => {
            let (name, payload_ty) = variants
                .get(*variant as usize)
                .map_or(("?", None), |(name, ty)| (name.as_str(), ty.as_ref()));
            match (payload_ty, payload) {
                (Some(ty), Some(item)) => {
                    json!({ "variant": name, "payload": value_json(ty, item) })
                }
                _ => json!({ "variant": name }),
            }
        }
        // A value that does not have the shape of its type was refused before it got here.
        _ => Value::Null,
    }
}

pub fn typed(value: &Option<TypedValue>) -> Value {
    match value {
        None => json!({ "value": null }),
        Some(typed) => json!({
            "value": { "type": type_name(&typed.ty), "value": value_json(&typed.ty, &typed.value) }
        }),
    }
}

pub fn diagnostics(found: &[Diagnostic]) -> Value {
    Value::Array(
        found
            .iter()
            .map(|item| {
                json!({
                    "severity": match item.severity {
                        Severity::Error => "error",
                        Severity::Warning => "warning",
                    },
                    "code": item.code,
                    "message": item.message,
                    "node": item.node,
                    "port": item.port,
                })
            })
            .collect(),
    )
}

pub fn plugin(info: &PluginInfo) -> Value {
    json!({
        "name": info.name,
        "generation": info.generation,
        "nodes": info.nodes,
        "backends": info.backends,
    })
}

pub fn plan(summary: &PlanSummary) -> Value {
    // The explanation is the planner's JSON. It is shown as JSON, not as text inside text.
    let explain = summary
        .explain
        .as_ref()
        .map(|text| serde_json::from_str(text).unwrap_or_else(|_| json!(text)));
    json!({
        "graph": summary.graph,
        "hash": format!("{:#018x}", summary.hash),
        "form": summary.form.name(),
        "nodes": summary.nodes,
        "edges": summary.edges,
        "regions": summary.regions,
        "steps": summary.steps,
        "cached": summary.cached,
        "diagnostics": diagnostics(&summary.diagnostics),
        "explain": explain,
    })
}

pub fn stepped(summary: &StepSummary) -> Value {
    json!({
        "ticks": summary.ticks,
        "tick": summary.tick,
        "step_calls": summary.step_calls,
        "faults": summary.faults,
        "skipped": summary.skipped,
        "idle": summary.idle,
    })
}

fn names(list: &Names) -> Value {
    json!({ "names": list.names, "total": list.total })
}

pub fn carry(report: &CarryReport) -> Value {
    json!({
        "carried": names(&report.carried),
        "fresh": names(&report.fresh),
        "added": names(&report.added),
        "removed": names(&report.removed),
        "removed_ports": names(&report.removed_ports),
        "dropped_values": report.dropped_values,
    })
}

fn state(state: RunState) -> &'static str {
    state.name()
}

pub fn inspection(found: &Inspection) -> Value {
    match found {
        Inspection::Run(info) => json!({
            "run": info.run,
            "session": info.session,
            "graph": info.graph,
            "state": state(info.state),
            "tick": info.tick,
            "faults": info.faults,
            "ports": info.ports.iter().map(|port| json!({
                "name": port.name,
                "input": port.input,
                "type": port.type_key,
            })).collect::<Vec<_>>(),
            "nodes": info.nodes,
        }),
        Inspection::Graph { name, json } => json!({
            "name": name,
            "graph": serde_json::from_str::<Value>(json).unwrap_or_else(|_| json!(json)),
        }),
        Inspection::Cache(info) => json!({
            "entries": info.entries,
            "hits": info.hits,
            "misses": info.misses,
        }),
        Inspection::Telemetry(info) => json!({
            "run": info.run,
            "level": info.level.name(),
            "tick": info.tick,
            "step_calls": info.step_calls,
            "faults": info.faults,
            "skipped": info.skipped,
            "rows": info.rows.iter().map(|row| json!({
                "step": row.step,
                "nodes": row.nodes,
                "calls": row.calls,
                "failures": row.failures,
                "backing_off": row.backing_off,
                "disabled": row.disabled,
                "auto_off": row.auto_off,
            })).collect::<Vec<_>>(),
        }),
        Inspection::Host(info) => json!({
            "sessions": info.sessions,
            "graphs": info.graphs,
            "runs": info.runs.iter().map(|run| json!({
                "run": run.run,
                "session": run.session,
                "graph": run.graph,
                "state": state(run.state),
            })).collect::<Vec<_>>(),
            "plugins": info.plugins,
        }),
    }
}

pub fn trace(entries: &[TraceEntry]) -> Value {
    json!({
        "entries": entries.iter().map(|entry| json!({
            "seq": entry.seq,
            "id": entry.id,
            "session": entry.session,
            "op": entry.op.name(),
            "target": entry.target,
            "error": entry.error.map(ErrorCode::name),
        })).collect::<Vec<_>>(),
    })
}

/// One watch event. Every kind has `event`, `sub` and `run`.
pub fn event(event: &Event) -> Value {
    let sub = event.sub;
    match &event.body {
        EventBody::Fault(fault) => json!({
            "event": "fault",
            "sub": sub,
            "run": fault.run,
            "tick": fault.tick,
            "step": fault.step,
            "node": fault.node,
            "covers": fault.covers,
            "consecutive": fault.consecutive,
            "reason": fault.reason,
            "reason_cut": fault.reason_cut,
            "detail": fault.detail,
            "detail_cut": fault.detail_cut,
        }),
        EventBody::Telemetry(telemetry) => json!({
            "event": "telemetry",
            "sub": sub,
            "run": telemetry.run,
            "tick": telemetry.tick,
            "step_calls": telemetry.step_calls,
            "faults": telemetry.faults,
            "skipped": telemetry.skipped,
        }),
        EventBody::Lagged { dropped } => json!({
            "event": "lagged",
            "sub": sub,
            "run": null,
            "dropped": dropped,
        }),
        EventBody::Ended { run, reason } => json!({
            "event": "ended",
            "sub": sub,
            "run": run,
            "reason": reason.name(),
        }),
        // From a newer daemon. It is printed as what it is, so nothing is taken for another event.
        EventBody::Unknown { kind } => json!({
            "event": "unknown",
            "sub": sub,
            "run": null,
            "kind": kind,
        }),
    }
}

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
