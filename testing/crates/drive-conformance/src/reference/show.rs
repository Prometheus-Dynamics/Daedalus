//! The canonical text of each answer. Every function here is the format the crate's contract
//! states, so a driver in another language prints the same bytes for the same answer.

use daedalus_client::protocol::{
    CacheInfo, CarryReport, Diagnostic, ErrorCode, Event, EventBody, FaultEvent, Inspection, Names,
    PlanSummary, PluginInfo, Severity, StepSummary, TelemetryInfo, TraceEntry, TypedValue,
};
use daedalus_client::{WireType, WireValue};

/// A string in quotes. Inside: `\"`, `\\`, `\n`, `\r`, `\t`; other characters outside printable
/// ASCII (0x20 to 0x7e) as `\u{H}`, with the code point in lowercase hex.
pub fn quote(text: &str) -> String {
    let mut out = String::with_capacity(text.len() + 2);
    out.push('"');
    for ch in text.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            ' '..='~' => out.push(ch),
            other => out.push_str(&format!("\\u{{{:x}}}", other as u32)),
        }
    }
    out.push('"');
    out
}

/// A list: `[` quoted items joined by commas, `]`. An empty list is `[]`.
pub fn list(items: &[String]) -> String {
    let quoted: Vec<String> = items.iter().map(|item| quote(item)).collect();
    format!("[{}]", quoted.join(","))
}

fn bool_text(flag: bool) -> &'static str {
    if flag { "true" } else { "false" }
}

/// A value as `TYPE:VALUE`. A bool is `true` or `false`, bytes are lowercase hex, a string is its
/// text. A value of another kind (a float or a compound) is `other`: the suite has none.
pub fn value(typed: &TypedValue) -> String {
    if !matches!(typed.ty, WireType::Scalar(_)) {
        return "other".to_owned();
    }
    match &typed.value {
        WireValue::Unit => "unit:".to_owned(),
        WireValue::Bool(v) => format!("bool:{}", bool_text(*v)),
        WireValue::I8(v) => format!("i8:{v}"),
        WireValue::I16(v) => format!("i16:{v}"),
        WireValue::I32(v) => format!("i32:{v}"),
        WireValue::I64(v) => format!("i64:{v}"),
        WireValue::ISize(v) => format!("isize:{v}"),
        WireValue::U8(v) => format!("u8:{v}"),
        WireValue::U16(v) => format!("u16:{v}"),
        WireValue::U32(v) => format!("u32:{v}"),
        WireValue::U64(v) => format!("u64:{v}"),
        WireValue::USize(v) => format!("usize:{v}"),
        WireValue::String(text) => format!("string:{text}"),
        WireValue::Bytes(bytes) => format!("bytes:{}", hex(bytes)),
        _ => "other".to_owned(),
    }
}

/// Lowercase hex, two digits a byte.
pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// A diagnostic as `severity|code`.
fn diagnostics(found: &[Diagnostic]) -> String {
    let items: Vec<String> = found
        .iter()
        .map(|item| {
            let severity = match item.severity {
                Severity::Error => "error",
                Severity::Warning => "warning",
            };
            format!("{severity}|{}", item.code)
        })
        .collect();
    list(&items)
}

/// The error line for a code. The reference prints the code's name, and a mutated driver can
/// print another.
pub fn error_code(code: ErrorCode) -> String {
    format!("error {}", code.name())
}

pub fn pong(version: u32) -> String {
    format!("pong version={version}")
}

pub fn session(name: &str) -> String {
    format!("session name={}", quote(name))
}

pub fn plugin(info: &PluginInfo) -> String {
    format!(
        "plugin name={} generation={} nodes={} backends={}",
        quote(&info.name),
        info.generation,
        list(&info.nodes),
        list(&info.backends)
    )
}

pub fn graph(name: &str, found: &[Diagnostic]) -> String {
    format!(
        "graph name={} diagnostics={}",
        quote(name),
        diagnostics(found)
    )
}

pub fn plan(summary: &PlanSummary) -> String {
    let explain = match &summary.explain {
        Some(text) => quote(text),
        None => "none".to_owned(),
    };
    format!(
        "plan graph={} hash={} form={} nodes={} edges={} regions={} steps={} cached={} \
         diagnostics={} explain={}",
        quote(&summary.graph),
        summary.hash,
        summary.form.name(),
        summary.nodes,
        summary.edges,
        summary.regions,
        summary.steps,
        bool_text(summary.cached),
        diagnostics(&summary.diagnostics),
        explain
    )
}

pub fn blob(name: &str, len: usize) -> String {
    format!("blob name={} bytes={len}", quote(name))
}

pub fn made(hash: u64, len: usize) -> String {
    format!("made hash={hash} bytes={len}")
}

pub fn started(run: u64) -> String {
    format!("started run={run}")
}

pub fn fed(woke: u32, dropped: bool) -> String {
    format!("fed woke={woke} dropped={}", bool_text(dropped))
}

pub fn stepped(summary: &StepSummary) -> String {
    format!(
        "stepped ticks={} tick={} step_calls={} faults={} skipped={} idle={}",
        summary.ticks,
        summary.tick,
        summary.step_calls,
        summary.faults,
        summary.skipped,
        bool_text(summary.idle)
    )
}

pub fn taken(value: &Option<TypedValue>) -> String {
    match value {
        None => "value none".to_owned(),
        Some(typed) => format!("value {}", self::value(typed)),
    }
}

pub fn watching(id: u64) -> String {
    format!("watching {id}")
}

pub fn unwatched(id: u64) -> String {
    format!("unwatched {id}")
}

pub fn stream_ended(id: u64) -> String {
    format!("stream {id} ended")
}

fn names(found: &Names) -> String {
    format!("{}/{}", list(&found.names), found.total)
}

pub fn reloaded(report: &CarryReport) -> String {
    format!(
        "reloaded carried={} fresh={} added={} removed={} removed_ports={} dropped_values={}",
        names(&report.carried),
        names(&report.fresh),
        names(&report.added),
        names(&report.removed),
        names(&report.removed_ports),
        report.dropped_values
    )
}

pub fn ack() -> String {
    "ack".to_owned()
}

pub fn stopped(tick: u64, dropped: u32) -> String {
    format!("stopped tick={tick} dropped={dropped}")
}

pub fn cache(info: &CacheInfo) -> String {
    format!(
        "inspect cache entries={} hits={} misses={}",
        info.entries, info.hits, info.misses
    )
}

pub fn inspection(found: &Inspection) -> String {
    match found {
        Inspection::Run(info) => {
            let ports: Vec<String> = info
                .ports
                .iter()
                .map(|port| {
                    let dir = if port.input { "input" } else { "output" };
                    format!("{}|{dir}|{}", port.name, port.type_key)
                })
                .collect();
            format!(
                "inspect run run={} session={} graph={} state={} tick={} faults={} ports={} \
                 nodes={}",
                info.run,
                quote(&info.session),
                quote(&info.graph),
                info.state.name(),
                info.tick,
                info.faults,
                list(&ports),
                list(&info.nodes)
            )
        }
        Inspection::Graph { name, json } => {
            format!("inspect graph name={} json={}", quote(name), quote(json))
        }
        Inspection::Cache(info) => cache(info),
        Inspection::Telemetry(info) => telemetry(info),
        Inspection::Host(info) => {
            let runs: Vec<String> = info
                .runs
                .iter()
                .map(|run| {
                    format!(
                        "{}|{}|{}|{}",
                        run.run,
                        run.session,
                        run.graph,
                        run.state.name()
                    )
                })
                .collect();
            format!(
                "inspect host sessions={} graphs={} runs={} plugins={}",
                list(&info.sessions),
                list(&info.graphs),
                list(&runs),
                list(&info.plugins)
            )
        }
    }
}

fn telemetry(info: &TelemetryInfo) -> String {
    // Timings and byte counts are measured, so they are left out of the transcript.
    let rows: Vec<String> = info
        .rows
        .iter()
        .map(|row| {
            format!(
                "{}|{}|{}|{}|{}|{}|{}",
                row.step,
                row.nodes.join("+"),
                row.calls,
                row.failures,
                bool_text(row.backing_off),
                bool_text(row.disabled),
                bool_text(row.auto_off)
            )
        })
        .collect();
    format!(
        "inspect telemetry run={} level={} tick={} step_calls={} faults={} skipped={} rows={}",
        info.run,
        info.level.name(),
        info.tick,
        info.step_calls,
        info.faults,
        info.skipped,
        list(&rows)
    )
}

pub fn trace(entries: &[TraceEntry]) -> String {
    let items: Vec<String> = entries
        .iter()
        .map(|entry| {
            let error = match entry.error {
                Some(code) => code.name(),
                None => "none",
            };
            format!("{}|{}|{}|{error}", entry.seq, entry.op.name(), entry.target)
        })
        .collect();
    format!("trace entries={}", list(&items))
}

fn fault(id: u64, event: &FaultEvent) -> String {
    let node = match &event.node {
        Some(node) => quote(node),
        None => "none".to_owned(),
    };
    format!(
        "event {id} fault run={} tick={} step={} node={node} covers={} consecutive={} reason={} \
         reason_cut={} detail={} detail_cut={} code={} own_code={}",
        event.run,
        event.tick,
        event.step,
        list(&event.covers),
        event.consecutive,
        quote(&event.reason),
        bool_text(event.reason_cut),
        quote(&event.detail),
        bool_text(event.detail_cut),
        event.code,
        bool_text(event.own_code)
    )
}

/// The line for an event read from watch `id`.
pub fn event(id: u64, event: &Event) -> String {
    match &event.body {
        EventBody::Fault(fault_event) => fault(id, fault_event),
        EventBody::Telemetry(t) => format!(
            "event {id} telemetry run={} tick={} step_calls={} faults={} skipped={}",
            t.run, t.tick, t.step_calls, t.faults, t.skipped
        ),
        EventBody::Lagged { dropped } => format!("event {id} lagged dropped={dropped}"),
        EventBody::Ended { run, reason } => {
            format!("event {id} ended run={run} reason={}", reason.name())
        }
        EventBody::Unknown { kind } => format!("event {id} unknown kind={kind}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_string_is_quoted_with_the_escapes_the_contract_names() {
        assert_eq!(quote("a\"b\\c\n\r\t"), "\"a\\\"b\\\\c\\n\\r\\t\"");
        assert_eq!(quote("\u{e9}"), "\"\\u{e9}\"");
        assert_eq!(quote("~ "), "\"~ \"");
    }

    #[test]
    fn a_list_has_no_space_and_an_empty_one_is_brackets() {
        assert_eq!(list(&[]), "[]");
        assert_eq!(list(&["a".to_owned(), "b c".to_owned()]), "[\"a\",\"b c\"]");
    }

    #[test]
    fn bytes_and_bools_have_their_words() {
        let bytes = TypedValue::new(
            WireType::Scalar(daedalus_types::Scalar::Bytes),
            WireValue::Bytes(vec![0, 255]),
        );
        assert_eq!(value(&bytes), "bytes:00ff");
        let flag = TypedValue::new(
            WireType::Scalar(daedalus_types::Scalar::Bool),
            WireValue::Bool(true),
        );
        assert_eq!(value(&flag), "bool:true");
    }
}
