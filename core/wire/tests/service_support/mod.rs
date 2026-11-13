//! The messages the service tests round trip: one of every op, reply, inspection, event and error.

#![allow(dead_code)] // Each test uses part of this module.

use daedalus_types::Scalar;
use daedalus_wire::service::{
    CacheAction, CacheInfo, Capabilities, CarryReport, ClientMsg, DEFAULT_SESSION, Diagnostic,
    EndReason, ErrorCode, Event, EventBody, EventKinds, FaultEvent, HostInfo, InspectTarget,
    Inspection, Names, Op, OpKind, PlanForm, PlanOptions, PlanSummary, PluginInfo, PortInfo, Reply,
    Request, Response, RunBrief, RunInfo, RunState, SERVICE_VERSION, ServerMsg, ServiceError,
    ServiceHello, Severity, StartConfig, StepSummary, StopMode, TelemetryEvent, TelemetryInfo,
    TelemetryLevel, TelemetryRow, TraceEntry, TypedValue, WatchTarget,
};
use daedalus_wire::{WireType, WireValue};

pub fn value() -> TypedValue {
    TypedValue::new(
        WireType::Struct(vec![
            ("name".to_owned(), WireType::Scalar(Scalar::String)),
            (
                "samples".to_owned(),
                WireType::List(Box::new(WireType::Scalar(Scalar::U64))),
            ),
        ]),
        WireValue::Struct(vec![
            WireValue::String("héllo".to_owned()),
            WireValue::List(vec![WireValue::U64(u64::MAX), WireValue::U64(0)]),
        ]),
    )
}

pub fn ops() -> Vec<Op> {
    vec![
        Op::LoadPlugin {
            path: "/usr/lib/daedalusd/plugins/libdemo.so".to_owned(),
        },
        Op::PutGraph {
            name: "g".to_owned(),
            json: "{\"nodes\":[]}".to_owned(),
        },
        Op::PatchGraph {
            graph: "g".to_owned(),
            patch: "{\"ops\":[]}".to_owned(),
        },
        Op::Plan {
            graph: "g".to_owned(),
            options: PlanOptions {
                form: PlanForm::Source,
                inspect: true,
                priority: vec!["cpu".to_owned(), "sim".to_owned()],
            },
        },
        Op::Start {
            graph: "g".to_owned(),
            config: StartConfig {
                fifo_capacity: 8,
                fault_reports: 16,
                telemetry: TelemetryLevel::Trace,
                unmerged: true,
                priority: vec!["cpu".to_owned()],
            },
        },
        Op::Feed {
            run: 7,
            port: "in".to_owned(),
            value: value(),
        },
        Op::Step { run: 7, count: 3 },
        Op::Take {
            run: 7,
            port: "out".to_owned(),
        },
        Op::Watch {
            target: WatchTarget::All,
            kinds: EventKinds::ALL,
        },
        Op::Watch {
            target: WatchTarget::Run(7),
            kinds: EventKinds::FAULT,
        },
        Op::Unwatch { sub: 2 },
        Op::Reload {
            run: 7,
            graph: "g2".to_owned(),
        },
        Op::Cancel { run: 7 },
        Op::Stop {
            run: 7,
            mode: StopMode::Drain,
        },
        Op::Stop {
            run: u64::MAX,
            mode: StopMode::Immediate,
        },
        Op::Inspect {
            target: InspectTarget::Run(7),
        },
        Op::Inspect {
            target: InspectTarget::Graph("g".to_owned()),
        },
        Op::Inspect {
            target: InspectTarget::Cache,
        },
        Op::Inspect {
            target: InspectTarget::Telemetry(7),
        },
        Op::Inspect {
            target: InspectTarget::Host,
        },
        Op::Cache {
            action: CacheAction::Clear,
        },
        Op::Trace { limit: 10 },
        Op::Ping,
    ]
}

pub fn diagnostic() -> Diagnostic {
    Diagnostic {
        severity: Severity::Warning,
        code: "unused_output".to_owned(),
        message: "output `spare` of `demo.split` goes nowhere".to_owned(),
        node: Some("demo.split".to_owned()),
        port: None,
    }
}

pub fn names(items: &[&str], total: u32) -> Names {
    Names {
        names: items.iter().map(|item| (*item).to_owned()).collect(),
        total,
    }
}

pub fn inspections() -> Vec<Inspection> {
    vec![
        Inspection::Run(RunInfo {
            run: 7,
            session: "s".to_owned(),
            graph: "g".to_owned(),
            state: RunState::Cancelled,
            tick: 99,
            faults: 2,
            ports: vec![PortInfo {
                name: "in".to_owned(),
                input: true,
                type_key: "u32".to_owned(),
            }],
            nodes: vec!["demo.add#0".to_owned()],
        }),
        Inspection::Graph {
            name: "g".to_owned(),
            json: "{}".to_owned(),
        },
        Inspection::Cache(CacheInfo {
            entries: 3,
            hits: 10,
            misses: 4,
        }),
        Inspection::Telemetry(TelemetryInfo {
            run: 7,
            level: TelemetryLevel::Detailed,
            tick: 12,
            step_calls: 36,
            faults: 1,
            skipped: 2,
            rows: vec![TelemetryRow {
                step: 0,
                nodes: vec!["a".to_owned(), "b".to_owned()],
                calls: 12,
                failures: 1,
                backing_off: true,
                disabled: false,
                auto_off: false,
            }],
        }),
        Inspection::Host(HostInfo {
            sessions: vec![DEFAULT_SESSION.to_owned()],
            graphs: vec!["g".to_owned()],
            runs: vec![RunBrief {
                run: 7,
                session: "s".to_owned(),
                graph: "g".to_owned(),
                state: RunState::Running,
            }],
            plugins: vec!["demo".to_owned()],
        }),
    ]
}

pub fn replies() -> Vec<Reply> {
    let mut replies = vec![
        Reply::Pong {
            version: SERVICE_VERSION,
        },
        Reply::Ack,
        Reply::Plugin(PluginInfo {
            name: "demo".to_owned(),
            generation: 2,
            nodes: vec!["demo.pass".to_owned()],
            backends: vec!["demo.cpu".to_owned()],
        }),
        Reply::Graph {
            name: "g".to_owned(),
            diagnostics: vec![diagnostic()],
        },
        Reply::Plan(PlanSummary {
            graph: "g".to_owned(),
            hash: u64::MAX,
            form: PlanForm::Blob,
            nodes: 4,
            edges: 3,
            regions: 2,
            steps: 5,
            cached: true,
            diagnostics: vec![diagnostic()],
            explain: Some("region 0: cpu".to_owned()),
        }),
        Reply::Started { run: 7 },
        Reply::Fed {
            woke: 2,
            dropped: true,
        },
        Reply::Stepped(StepSummary {
            ticks: 3,
            tick: 12,
            step_calls: 9,
            faults: 1,
            skipped: 0,
            idle: true,
        }),
        Reply::Value(None),
        Reply::Value(Some(value())),
        Reply::Watching { sub: 2 },
        Reply::Reloaded(CarryReport {
            carried: names(&["a"], 1),
            fresh: names(&["b"], 40),
            added: names(&[], 0),
            removed: names(&["c", "d"], 2),
            removed_ports: names(&["old"], 1),
            dropped_values: 5,
        }),
        Reply::Stopped {
            run: 7,
            tick: 12,
            dropped: 1,
        },
        Reply::Trace(vec![TraceEntry {
            seq: 4,
            id: 9,
            session: "s".to_owned(),
            op: OpKind::Feed,
            target: "7:in".to_owned(),
            error: Some(ErrorCode::UnknownPort),
        }]),
    ];
    replies.extend(inspections().into_iter().map(Reply::Inspection));
    replies
}

pub fn events() -> Vec<EventBody> {
    vec![
        EventBody::Fault(FaultEvent {
            run: 7,
            tick: 3,
            step: 1,
            node: Some("demo.boom".to_owned()),
            covers: vec!["demo.pass".to_owned(), "demo.boom".to_owned()],
            consecutive: 2,
            reason: "boom".to_owned(),
            reason_cut: true,
        }),
        EventBody::Telemetry(TelemetryEvent {
            run: 7,
            tick: 3,
            step_calls: 4,
            faults: 1,
            skipped: 0,
        }),
        EventBody::Lagged { dropped: 12 },
        EventBody::Ended {
            run: 7,
            reason: EndReason::Stopped,
        },
        EventBody::Ended {
            run: 0,
            reason: EndReason::Shutdown,
        },
    ]
}

pub fn server_messages() -> Vec<ServerMsg> {
    let mut messages = vec![ServerMsg::Hello(ServiceHello::server(Capabilities {
        ops: OpKind::ALL.to_vec(),
        codecs: vec!["acme.blob".to_owned()],
        max_runs: 16,
    }))];
    for (at, reply) in replies().into_iter().enumerate() {
        messages.push(ServerMsg::Response(Response {
            id: at as u64,
            result: Ok(reply),
        }));
    }
    for code in ErrorCode::ALL {
        let error = match code {
            ErrorCode::PlanningFailed => ServiceError::planning("no backend", vec![diagnostic()]),
            code => ServiceError::new(code, format!("refused: {code}")),
        };
        messages.push(ServerMsg::Response(Response {
            id: u64::from(code.code()),
            result: Err(error),
        }));
    }
    for (sub, body) in events().into_iter().enumerate() {
        messages.push(ServerMsg::Event(Event {
            sub: sub as u64,
            body,
        }));
    }
    messages
}

pub fn client_messages() -> Vec<ClientMsg> {
    let mut messages = vec![ClientMsg::Hello(ServiceHello::client())];
    for (id, op) in ops().into_iter().enumerate() {
        messages.push(ClientMsg::Request(Request {
            id: id as u64,
            session: "tools".to_owned(),
            op,
        }));
    }
    messages
}
