//! A small worker that speaks the protocol and serves the behaviour suite's nodes. Honest by
//! default. With `DAEDALUS_SABOTAGE` set to a behaviour's name it is wrong in exactly that way, so
//! the self-check can show each behaviour fails a worker that breaks it.
//!
//! It is not an SDK. It exists so the suite is not vacuous: every sabotage must fail its case.

#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::io::{self, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::channel;
use std::thread;
use std::time::{Duration, Instant};

use daedalus_wire::codec::Writer;
use daedalus_wire::{
    BoundaryValue, DEFAULT_CAP, Fault, FaultCode, Hello, HostMsg, LogLevel, Outputs, Progress,
    RunStatus, Schema, WireValue, WorkerMsg, kind, read_packet, write_packet,
};
use daedalus_worker_conformance::fixtures::{
    CHATTY, EVENTS, STATEFUL_LABEL, WAIT_CANCEL, behaviour_declarations, declarations,
};

const WORKER: &str = "daedalus-worker-sabotage";
const SCALAR_ADD: &str = "fixture.scalar_add";
const COUNTER: &str = "fixture.stateful_counter";
const RAW_IO: &str = "fixture.raw_io";

fn main() {
    let sabotage = std::env::var("DAEDALUS_SABOTAGE").unwrap_or_default();
    let mut nodes = declarations();
    nodes.extend(behaviour_declarations());
    let hello = Hello::new(WORKER, DEFAULT_CAP, nodes);
    let schema = Schema::from_hello(&hello, &[]);
    send(&WorkerMsg::Hello(hello), &Schema::new());

    let cancel = Arc::new(AtomicBool::new(false));
    let (sender, packets) = channel::<Vec<u8>>();
    let reader_cancel = cancel.clone();
    thread::spawn(move || {
        let mut stdin = io::stdin().lock();
        loop {
            let mut buf = Vec::new();
            if read_packet(&mut stdin, DEFAULT_CAP, &mut buf).is_err() {
                return;
            }
            match buf.first() {
                Some(&kind::RUN) => reader_cancel.store(false, Ordering::SeqCst),
                Some(&kind::CANCEL) => reader_cancel.store(true, Ordering::SeqCst),
                _ => {}
            }
            if sender.send(buf).is_err() {
                return;
            }
        }
    });

    let mut state: HashMap<String, WireValue> = HashMap::new();
    while let Ok(buf) = packets.recv() {
        let msg = match HostMsg::decode(&buf, &schema) {
            Ok(msg) => msg,
            Err(error) => {
                if sabotage == "malformed" {
                    // The worker dies on a bad packet instead of answering it.
                    std::process::exit(1);
                }
                send(
                    &WorkerMsg::Fault(Fault {
                        run: 0,
                        node: None,
                        code: FaultCode::MalformedPacket,
                        reason: error.to_string(),
                    }),
                    &schema,
                );
                continue;
            }
        };
        match msg {
            HostMsg::Run(run) => {
                let Some(node) = run.nodes.first().cloned() else {
                    continue;
                };
                let inputs: HashMap<String, WireValue> = run
                    .inputs
                    .into_iter()
                    .map(|entry| (entry.port, entry.value))
                    .collect();
                let input = |port: &str| inputs.get(port).cloned().unwrap_or(WireValue::I64(0));
                let mut status = RunStatus::Ok;
                let mut outputs: Vec<(&str, WireValue)> = Vec::new();
                match node.as_str() {
                    SCALAR_ADD => {
                        let (WireValue::I64(a), WireValue::I64(b)) = (input("a"), input("b"))
                        else {
                            continue;
                        };
                        outputs.push(("out", WireValue::I64(a.wrapping_add(b))));
                    }
                    EVENTS => {
                        let mut events = vec![
                            progress(run.run, 0.5, "half"),
                            WorkerMsg::Log {
                                level: LogLevel::Info,
                                text: "after the progress".to_owned(),
                            },
                            WorkerMsg::Detail {
                                run: run.run,
                                text: "a detail line".to_owned(),
                            },
                            progress(run.run, 1.0, "done"),
                        ];
                        if sabotage == "events" {
                            // The last progress goes out first.
                            events.rotate_right(1);
                        }
                        for event in &events {
                            send(event, &schema);
                        }
                        outputs.push(("y", input("x")));
                    }
                    CHATTY => {
                        if sabotage == "chatty" {
                            let mut stdout = io::stdout();
                            let _ = stdout.write_all(b"a stray line on the stream\n");
                            let _ = stdout.flush();
                        }
                        let WireValue::I64(x) = input("x") else {
                            continue;
                        };
                        outputs.push(("y", WireValue::I64(x + 1)));
                    }
                    WAIT_CANCEL => {
                        if sabotage == "cancel" {
                            // Ignores the cancel: it runs on for a while, then gives up.
                            thread::sleep(Duration::from_secs(3));
                        } else {
                            let started = Instant::now();
                            while !cancel.load(Ordering::SeqCst) && started.elapsed().as_secs() < 60
                            {
                                thread::sleep(Duration::from_millis(2));
                            }
                        }
                        if cancel.load(Ordering::SeqCst) && sabotage != "cancel" {
                            status = RunStatus::Cancelled;
                        }
                        outputs.push(("y", input("x")));
                    }
                    COUNTER => {
                        let (WireValue::I64(delta), held) = (input("delta"), state.get(COUNTER))
                        else {
                            continue;
                        };
                        let total = match held {
                            Some(WireValue::I64(total)) => total + delta,
                            _ => delta,
                        };
                        state.insert(COUNTER.to_owned(), WireValue::I64(total));
                        outputs.push(("total", WireValue::I64(total)));
                    }
                    STATEFUL_LABEL => {
                        let WireValue::String(text) = input("in") else {
                            continue;
                        };
                        let mut held = match state.get(STATEFUL_LABEL) {
                            Some(WireValue::String(held)) => held.clone(),
                            _ => String::new(),
                        };
                        held.push_str(&text);
                        state.insert(STATEFUL_LABEL.to_owned(), WireValue::String(held.clone()));
                        outputs.push(("out", WireValue::String(held)));
                    }
                    RAW_IO => {
                        let WireValue::Bytes(payload) = input("payload") else {
                            continue;
                        };
                        let mut out: Vec<u8> = payload.iter().rev().copied().collect();
                        if sabotage == "large" {
                            out = payload;
                        }
                        outputs.push(("out", WireValue::Bytes(out)));
                    }
                    _ => {
                        send(
                            &WorkerMsg::Fault(Fault {
                                run: run.run,
                                node: Some(node),
                                code: FaultCode::UnknownNode,
                                reason: "no such node".to_owned(),
                            }),
                            &schema,
                        );
                        continue;
                    }
                }
                let values = outputs
                    .into_iter()
                    .map(|(port, value)| BoundaryValue {
                        node: node.clone(),
                        port: port.to_owned(),
                        value,
                    })
                    .collect();
                send(
                    &WorkerMsg::Outputs(Outputs {
                        run: run.run,
                        status,
                        outputs: values,
                    }),
                    &schema,
                );
            }
            HostMsg::Cancel { .. } => {}
            HostMsg::Ping => send(&WorkerMsg::Pong, &schema),
            HostMsg::Shutdown => return,
            HostMsg::StateExport { node } => match state.get(&node) {
                Some(value) => send(
                    &WorkerMsg::StateValue {
                        node,
                        value: value.clone(),
                    },
                    &schema,
                ),
                None if sabotage == "state" && node == STATEFUL_LABEL => {
                    // The bug: an unset state exports 0, whatever its type.
                    let mut out = Writer::new();
                    out.u8(kind::STATE_VALUE);
                    if out.str(&node).is_ok() {
                        out.i64(0);
                        let _ = write_packet(&mut io::stdout(), &out.buf, DEFAULT_CAP);
                    }
                }
                None => send(
                    &WorkerMsg::Fault(Fault {
                        run: 0,
                        node: Some(node),
                        code: FaultCode::BadInput,
                        reason: "the node has no state to export".to_owned(),
                    }),
                    &schema,
                ),
            },
            HostMsg::StateImport { node, value } => {
                state.insert(node, value);
            }
        }
    }
}

fn progress(run: u64, fraction: f32, message: &str) -> WorkerMsg {
    WorkerMsg::Progress(Progress {
        run,
        node_index: 0,
        message: message.to_owned(),
        fraction: Some(fraction),
    })
}

/// Writes one message to stdout. The worker has nowhere else to report a failure, so a failed
/// write ends it.
fn send(msg: &WorkerMsg, schema: &Schema) {
    let mut out = Writer::new();
    if msg.encode(schema, &mut out).is_err() {
        std::process::exit(2);
    }
    if write_packet(&mut io::stdout(), &out.buf, DEFAULT_CAP).is_err() {
        std::process::exit(3);
    }
}
