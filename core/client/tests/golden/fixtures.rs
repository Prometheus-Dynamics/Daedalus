// Fixed values for the JSON golden test (tests/json.rs). The golden file, golden/json.txt, was made
// from daedalus-ctl's output before its rendering moved into this crate, so the library must still
// print each value byte for byte.

use daedalus_client::protocol::{
    CacheInfo, CarryReport, Diagnostic, ErrorCode, Event, EventBody, EndReason, FaultEvent,
    HostInfo, Inspection, Names, OpKind, PlanForm, PlanSummary, PluginInfo, PortInfo, RunBrief,
    RunInfo, RunState, Severity, StepSummary, TelemetryEvent, TelemetryInfo, TelemetryLevel,
    TelemetryRow, TraceEntry, TypedValue,
};
use daedalus_client::{WireType, WireValue};
use daedalus_types::Scalar;

/// One value of each thing the JSON renders.
pub enum Fixture {
    Event(Event),
    Inspection(Inspection),
    Plan(PlanSummary),
    Diagnostics(Vec<Diagnostic>),
    Plugin(PluginInfo),
    Stepped(StepSummary),
    Carry(CarryReport),
    Trace(Vec<TraceEntry>),
    Typed(Option<TypedValue>),
}

fn names(list: &[&str], total: u32) -> Names {
    Names {
        names: list.iter().map(|name| (*name).to_owned()).collect(),
        total,
    }
}

fn diagnostics() -> Vec<Diagnostic> {
    vec![
        Diagnostic {
            severity: Severity::Warning,
            code: "W1".to_owned(),
            message: "an unused port".to_owned(),
            node: Some("io".to_owned()),
            port: Some("in".to_owned()),
        },
        Diagnostic {
            severity: Severity::Error,
            code: "E2".to_owned(),
            message: "a cycle".to_owned(),
            node: None,
            port: None,
        },
    ]
}

/// A compound value: a struct with a list, a tuple, a map, an optional, and both kinds of enum
/// variant, so every branch of the value rendering runs.
fn compound() -> TypedValue {
    let ty = WireType::Struct(vec![
        ("count".to_owned(), WireType::Scalar(Scalar::Int)),
        (
            "tags".to_owned(),
            WireType::List(Box::new(WireType::Scalar(Scalar::String))),
        ),
        (
            "pair".to_owned(),
            WireType::Tuple(vec![
                WireType::Scalar(Scalar::Bool),
                WireType::Scalar(Scalar::F32),
            ]),
        ),
        (
            "map".to_owned(),
            WireType::Map(
                Box::new(WireType::Scalar(Scalar::U8)),
                Box::new(WireType::Scalar(Scalar::Bytes)),
            ),
        ),
        (
            "maybe".to_owned(),
            WireType::Optional(Box::new(WireType::Scalar(Scalar::U64))),
        ),
        (
            "mode".to_owned(),
            WireType::Enum(vec![
                ("off".to_owned(), None),
                (
                    "scaled".to_owned(),
                    Some(WireType::Scalar(Scalar::Float)),
                ),
            ]),
        ),
    ]);
    let value = WireValue::Struct(vec![
        WireValue::I64(-5),
        WireValue::List(vec![
            WireValue::String("a".to_owned()),
            WireValue::String("b\"c".to_owned()),
        ]),
        WireValue::Tuple(vec![WireValue::Bool(true), WireValue::F32(0.5)]),
        WireValue::Map(vec![(WireValue::U8(7), WireValue::Bytes(vec![1, 255]))]),
        WireValue::Optional(None),
        WireValue::Enum {
            variant: 1,
            payload: Some(Box::new(WireValue::F64(2.25))),
        },
    ]);
    TypedValue::new(ty, value)
}

pub fn fixtures() -> Vec<(&'static str, Fixture)> {
    vec![
        (
            "event fault",
            Fixture::Event(Event {
                sub: 9,
                body: EventBody::Fault(FaultEvent {
                    run: 4,
                    tick: 3,
                    step: 2,
                    node: Some("work".to_owned()),
                    covers: vec!["work".to_owned(), "io".to_owned()],
                    consecutive: 1,
                    reason: "boom".to_owned(),
                    reason_cut: false,
                    detail: "in 2 pieces".to_owned(),
                    detail_cut: true,
                    code: 42,
                    own_code: false,
                }),
            }),
        ),
        (
            "event telemetry",
            Fixture::Event(Event {
                sub: 1,
                body: EventBody::Telemetry(TelemetryEvent {
                    run: 1,
                    tick: 8,
                    step_calls: 16,
                    faults: 0,
                    skipped: 2,
                }),
            }),
        ),
        (
            "event lagged",
            Fixture::Event(Event {
                sub: 2,
                body: EventBody::Lagged { dropped: 5 },
            }),
        ),
        (
            "event ended",
            Fixture::Event(Event {
                sub: 3,
                body: EventBody::Ended {
                    run: 1,
                    reason: EndReason::Stopped,
                },
            }),
        ),
        (
            "event unknown",
            Fixture::Event(Event {
                sub: 4,
                body: EventBody::Unknown { kind: 200 },
            }),
        ),
        (
            "inspect run",
            Fixture::Inspection(Inspection::Run(RunInfo {
                run: 3,
                session: "default".to_owned(),
                graph: "g".to_owned(),
                state: RunState::Running,
                tick: 12,
                faults: 1,
                ports: vec![
                    PortInfo {
                        name: "in".to_owned(),
                        input: true,
                        type_key: "u32".to_owned(),
                    },
                    PortInfo {
                        name: "out".to_owned(),
                        input: false,
                        type_key: "u32".to_owned(),
                    },
                ],
                nodes: vec!["io".to_owned(), "work".to_owned()],
            })),
        ),
        (
            "inspect graph",
            Fixture::Inspection(Inspection::Graph {
                name: "g".to_owned(),
                json: r#"{"format": "daedalus.graph"}"#.to_owned(),
            }),
        ),
        (
            "inspect graph text",
            Fixture::Inspection(Inspection::Graph {
                name: "h".to_owned(),
                json: "not json".to_owned(),
            }),
        ),
        (
            "inspect cache",
            Fixture::Inspection(Inspection::Cache(CacheInfo {
                entries: 2,
                hits: 5,
                misses: 3,
            })),
        ),
        (
            "inspect telemetry",
            Fixture::Inspection(Inspection::Telemetry(TelemetryInfo {
                run: 1,
                level: TelemetryLevel::Detailed,
                tick: 4,
                step_calls: 8,
                faults: 0,
                skipped: 1,
                rows: vec![TelemetryRow {
                    step: 0,
                    nodes: vec!["work".to_owned()],
                    calls: 4,
                    failures: 0,
                    backing_off: false,
                    disabled: false,
                    auto_off: true,
                    timed_runs: 3,
                    time_ns: 900,
                    bytes: 64,
                }],
            })),
        ),
        (
            "inspect host",
            Fixture::Inspection(Inspection::Host(HostInfo {
                sessions: vec!["default".to_owned()],
                graphs: vec!["g".to_owned()],
                runs: vec![RunBrief {
                    run: 1,
                    session: "default".to_owned(),
                    graph: "g".to_owned(),
                    state: RunState::Stopped,
                }],
                plugins: vec!["dylib-fixture".to_owned()],
            })),
        ),
        (
            "plan with explanation",
            Fixture::Plan(PlanSummary {
                graph: "g".to_owned(),
                hash: 0x1234_5678_9abc_def0,
                form: PlanForm::Steps,
                nodes: 2,
                edges: 1,
                regions: 1,
                steps: 2,
                cached: false,
                diagnostics: diagnostics(),
                explain: Some(r#"{"edges": []}"#.to_owned()),
            }),
        ),
        (
            "plan with text explanation",
            Fixture::Plan(PlanSummary {
                graph: "h".to_owned(),
                hash: 1,
                form: PlanForm::Source,
                nodes: 0,
                edges: 0,
                regions: 0,
                steps: 0,
                cached: true,
                diagnostics: Vec::new(),
                explain: Some("plain words".to_owned()),
            }),
        ),
        ("diagnostics", Fixture::Diagnostics(diagnostics())),
        (
            "plugin",
            Fixture::Plugin(PluginInfo {
                name: "dylib-fixture".to_owned(),
                generation: 2,
                nodes: vec!["fixture.pass".to_owned()],
                backends: vec!["fixture.cpu".to_owned()],
            }),
        ),
        (
            "stepped",
            Fixture::Stepped(StepSummary {
                ticks: 3,
                tick: 9,
                step_calls: 6,
                faults: 1,
                skipped: 0,
                idle: true,
            }),
        ),
        (
            "carry",
            Fixture::Carry(CarryReport {
                carried: names(&["io"], 1),
                fresh: names(&[], 0),
                added: names(&["new"], 1),
                removed: names(&["old"], 1),
                removed_ports: names(&["gone"], 1),
                dropped_values: 2,
            }),
        ),
        (
            "trace",
            Fixture::Trace(vec![
                TraceEntry {
                    seq: 7,
                    id: 3,
                    session: "default".to_owned(),
                    op: OpKind::Inspect,
                    target: "run 1".to_owned(),
                    error: None,
                },
                TraceEntry {
                    seq: 8,
                    id: 4,
                    session: "default".to_owned(),
                    op: OpKind::Feed,
                    target: "run 9".to_owned(),
                    error: Some(ErrorCode::UnknownRun),
                },
            ]),
        ),
        (
            "typed scalar",
            Fixture::Typed(Some(TypedValue::new(
                WireType::Scalar(Scalar::U32),
                WireValue::U32(41),
            ))),
        ),
        ("typed compound", Fixture::Typed(Some(compound()))),
        ("typed none", Fixture::Typed(None)),
    ]
}
