//! The C ABI's JSON read back into the protocol's values. The shapes are the ones
//! `daedalus_client::json` writes, the contract of `daedalus-ctl --json` and of this surface. A
//! field that is missing or of another kind reads as `None`, which the driver answers `error local`.

use daedalus_wire::service::{
    CacheInfo, CarryReport, Diagnostic, EndReason, ErrorCode, Event, EventBody, FaultEvent,
    HostInfo, Names, OpKind, PlanForm, PlanSummary, PluginInfo, PortInfo, RunBrief, RunInfo,
    RunState, Severity, TelemetryEvent, TelemetryInfo, TelemetryLevel, TelemetryRow, TraceEntry,
};
use serde_json::Value;

fn text(found: &Value, key: &str) -> Option<String> {
    found.get(key)?.as_str().map(str::to_owned)
}

fn number<T: TryFrom<u64>>(found: &Value, key: &str) -> Option<T> {
    T::try_from(found.get(key)?.as_u64()?).ok()
}

fn flag(found: &Value, key: &str) -> Option<bool> {
    found.get(key)?.as_bool()
}

/// A string, or null for none.
fn maybe_text(found: &Value, key: &str) -> Option<Option<String>> {
    match found.get(key)? {
        Value::Null => Some(None),
        Value::String(text) => Some(Some(text.clone())),
        _ => None,
    }
}

fn texts(found: &Value, key: &str) -> Option<Vec<String>> {
    items(found, key)?
        .iter()
        .map(|item| item.as_str().map(str::to_owned))
        .collect()
}

fn items<'a>(found: &'a Value, key: &str) -> Option<&'a Vec<Value>> {
    found.get(key)?.as_array()
}

/// The one of `all` whose name is the field's text.
fn named<T: Copy>(
    found: &Value,
    key: &str,
    all: impl IntoIterator<Item = T>,
    name: fn(T) -> &'static str,
) -> Option<T> {
    let word = found.get(key)?.as_str()?;
    all.into_iter().find(|item| name(*item) == word)
}

pub fn plugin(found: &Value) -> Option<PluginInfo> {
    Some(PluginInfo {
        name: text(found, "name")?,
        generation: number(found, "generation")?,
        nodes: texts(found, "nodes")?,
        backends: texts(found, "backends")?,
    })
}

/// A JSON array of diagnostics.
pub fn diagnostics(found: &Value) -> Option<Vec<Diagnostic>> {
    found
        .as_array()?
        .iter()
        .map(|item| {
            Some(Diagnostic {
                severity: match item.get("severity")?.as_str()? {
                    "error" => Severity::Error,
                    "warning" => Severity::Warning,
                    _ => return None,
                },
                code: text(item, "code")?,
                message: text(item, "message")?,
                node: maybe_text(item, "node")?,
                port: maybe_text(item, "port")?,
            })
        })
        .collect()
}

/// A plan as `daedalus_client_plan_raw` gives it: the explanation is the planner's own text.
pub fn plan(found: &Value) -> Option<PlanSummary> {
    let hash = text(found, "hash")?;
    Some(PlanSummary {
        graph: text(found, "graph")?,
        hash: u64::from_str_radix(hash.strip_prefix("0x")?, 16).ok()?,
        form: named(found, "form", PlanForm::ALL, PlanForm::name)?,
        nodes: number(found, "nodes")?,
        edges: number(found, "edges")?,
        regions: number(found, "regions")?,
        steps: number(found, "steps")?,
        cached: flag(found, "cached")?,
        diagnostics: diagnostics(found.get("diagnostics")?)?,
        explain: maybe_text(found, "explain")?,
    })
}

fn names(found: &Value, key: &str) -> Option<Names> {
    let part = found.get(key)?;
    Some(Names {
        names: texts(part, "names")?,
        total: number(part, "total")?,
    })
}

pub fn carry(found: &Value) -> Option<CarryReport> {
    Some(CarryReport {
        carried: names(found, "carried")?,
        fresh: names(found, "fresh")?,
        added: names(found, "added")?,
        removed: names(found, "removed")?,
        removed_ports: names(found, "removed_ports")?,
        dropped_values: number(found, "dropped_values")?,
    })
}

fn state(found: &Value) -> Option<RunState> {
    named(found, "state", RunState::ALL, RunState::name)
}

pub fn run(found: &Value) -> Option<RunInfo> {
    let ports = items(found, "ports")?
        .iter()
        .map(|port| {
            Some(PortInfo {
                name: text(port, "name")?,
                input: flag(port, "input")?,
                type_key: text(port, "type")?,
            })
        })
        .collect::<Option<Vec<_>>>()?;
    Some(RunInfo {
        run: number(found, "run")?,
        session: text(found, "session")?,
        graph: text(found, "graph")?,
        state: state(found)?,
        tick: number(found, "tick")?,
        faults: number(found, "faults")?,
        ports,
        nodes: texts(found, "nodes")?,
    })
}

pub fn cache(found: &Value) -> Option<CacheInfo> {
    Some(CacheInfo {
        entries: number(found, "entries")?,
        hits: number(found, "hits")?,
        misses: number(found, "misses")?,
    })
}

pub fn telemetry(found: &Value) -> Option<TelemetryInfo> {
    let rows = items(found, "rows")?
        .iter()
        .map(|row| {
            Some(TelemetryRow {
                step: number(row, "step")?,
                nodes: texts(row, "nodes")?,
                calls: number(row, "calls")?,
                failures: number(row, "failures")?,
                backing_off: flag(row, "backing_off")?,
                disabled: flag(row, "disabled")?,
                auto_off: flag(row, "auto_off")?,
                timed_runs: number(row, "timed_runs")?,
                time_ns: number(row, "time_ns")?,
                bytes: number(row, "bytes")?,
            })
        })
        .collect::<Option<Vec<_>>>()?;
    Some(TelemetryInfo {
        run: number(found, "run")?,
        level: named(found, "level", TelemetryLevel::ALL, TelemetryLevel::name)?,
        tick: number(found, "tick")?,
        step_calls: number(found, "step_calls")?,
        faults: number(found, "faults")?,
        skipped: number(found, "skipped")?,
        rows,
    })
}

pub fn host(found: &Value) -> Option<HostInfo> {
    let runs = items(found, "runs")?
        .iter()
        .map(|run| {
            Some(RunBrief {
                run: number(run, "run")?,
                session: text(run, "session")?,
                graph: text(run, "graph")?,
                state: state(run)?,
            })
        })
        .collect::<Option<Vec<_>>>()?;
    Some(HostInfo {
        sessions: texts(found, "sessions")?,
        graphs: texts(found, "graphs")?,
        runs,
        plugins: texts(found, "plugins")?,
    })
}

/// An error code by its name; a name this side does not have is `other`.
fn code_named(word: &str) -> ErrorCode {
    ErrorCode::ALL
        .into_iter()
        .find(|code| code.name() == word)
        .unwrap_or(ErrorCode::Other)
}

pub fn trace(found: &Value) -> Option<Vec<TraceEntry>> {
    items(found, "entries")?
        .iter()
        .map(|entry| {
            let op = entry.get("op")?.as_str()?;
            Some(TraceEntry {
                seq: number(entry, "seq")?,
                id: number(entry, "id")?,
                session: text(entry, "session")?,
                op: OpKind::ALL
                    .into_iter()
                    .find(|kind| kind.name() == op)
                    .unwrap_or(OpKind::Other),
                target: text(entry, "target")?,
                error: maybe_text(entry, "error")?.as_deref().map(code_named),
            })
        })
        .collect()
}

pub fn event(found: &Value) -> Option<Event> {
    let body = match found.get("event")?.as_str()? {
        "fault" => EventBody::Fault(FaultEvent {
            run: number(found, "run")?,
            tick: number(found, "tick")?,
            step: number(found, "step")?,
            node: maybe_text(found, "node")?,
            covers: texts(found, "covers")?,
            consecutive: number(found, "consecutive")?,
            reason: text(found, "reason")?,
            reason_cut: flag(found, "reason_cut")?,
            detail: text(found, "detail")?,
            detail_cut: flag(found, "detail_cut")?,
            code: number(found, "code")?,
            own_code: flag(found, "own_code")?,
        }),
        "telemetry" => EventBody::Telemetry(TelemetryEvent {
            run: number(found, "run")?,
            tick: number(found, "tick")?,
            step_calls: number(found, "step_calls")?,
            faults: number(found, "faults")?,
            skipped: number(found, "skipped")?,
        }),
        "lagged" => EventBody::Lagged {
            dropped: number(found, "dropped")?,
        },
        "ended" => EventBody::Ended {
            run: number(found, "run")?,
            reason: named(found, "reason", EndReason::ALL, EndReason::name)?,
        },
        "unknown" => EventBody::Unknown {
            kind: number(found, "kind")?,
        },
        _ => return None,
    };
    Some(Event {
        sub: number(found, "sub")?,
        body,
    })
}
