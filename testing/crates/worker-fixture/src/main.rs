//! A worker process that serves the canonical fixtures over the wire protocol on stdin and stdout.
//!
//! `--misbehave <mode>` makes it fail in one way, for the supervision tests:
//! `exit` exits on each run, `exit-once:<marker>` exits once (the marker file records it),
//! `hang` never answers a run, `oversize` writes a packet over the cap, `stderr-flood`
//! writes a megabyte to stderr before each run, `log` sends two log messages before each run's
//! outputs, `log-exit` sends one and exits, and `long-fault` fails each run with a reason of
//! several lines.

#![forbid(unsafe_code)]

mod nodes;

use std::collections::BTreeMap;
use std::io::{self, Write};
use std::path::PathBuf;
use std::process;
use std::time::Duration;

use daedalus_wire::codec::Writer;
use daedalus_wire::{
    DEFAULT_CAP, Fault, FaultCode, HostMsg, LogLevel, Outputs, PacketError, Run, RunStatus, Schema,
    WireValue, WorkerMsg, read_packet, write_packet,
};
use daedalus_worker_conformance::{CUSTOM_KEY, DIAGNOSTIC_CODE, DIAGNOSTIC_DETAIL, hello};

enum Mode {
    Normal,
    Exit,
    ExitOnce(PathBuf),
    Hang,
    Oversize,
    StderrFlood,
    Log,
    LogExit,
    LongFault,
}

fn mode_from_args() -> Mode {
    let args: Vec<String> = std::env::args().collect();
    let Some(at) = args.iter().position(|arg| arg == "--misbehave") else {
        return Mode::Normal;
    };
    match args.get(at + 1).map(String::as_str) {
        Some("exit") => Mode::Exit,
        Some("hang") => Mode::Hang,
        Some("oversize") => Mode::Oversize,
        Some("stderr-flood") => Mode::StderrFlood,
        Some("log") => Mode::Log,
        Some("log-exit") => Mode::LogExit,
        Some("long-fault") => Mode::LongFault,
        Some(other) if other.starts_with("exit-once:") => {
            Mode::ExitOnce(PathBuf::from(&other["exit-once:".len()..]))
        }
        _ => Mode::Normal,
    }
}

fn send(out: &mut impl Write, msg: &WorkerMsg, schema: &Schema) -> Result<(), PacketError> {
    let mut writer = Writer::new();
    msg.encode(schema, &mut writer)
        .expect("a worker message encodes");
    write_packet(out, &writer.buf, DEFAULT_CAP)
}

fn run(
    run: Run,
    schema: &Schema,
    state: &mut BTreeMap<String, WireValue>,
    out: &mut impl Write,
) -> Result<(), PacketError> {
    let mut outputs = Vec::new();
    let mut status = RunStatus::Ok;
    for node in &run.nodes {
        match nodes::execute(node, &run.inputs, state) {
            Ok(values) => outputs.extend(values),
            Err((code, reason)) => {
                if node == "fixture.failure_diagnostic" {
                    // More than the reason holds, in two pieces.
                    let (first, rest) = DIAGNOSTIC_DETAIL
                        .split_at(DIAGNOSTIC_DETAIL.find(", ").map_or(0, |at| at + 2));
                    for text in [first, rest] {
                        let piece = WorkerMsg::Detail {
                            run: run.run,
                            text: text.to_owned(),
                        };
                        send(out, &piece, schema)?;
                    }
                    let number = WorkerMsg::NodeCode {
                        run: run.run,
                        code: DIAGNOSTIC_CODE,
                    };
                    send(out, &number, schema)?;
                }
                send(
                    out,
                    &WorkerMsg::Fault(Fault {
                        run: run.run,
                        node: Some(node.clone()),
                        code,
                        reason,
                    }),
                    schema,
                )?;
                status = RunStatus::Fault;
                outputs.clear();
                break;
            }
        }
    }
    send(
        out,
        &WorkerMsg::Outputs(Outputs {
            run: run.run,
            status,
            outputs,
        }),
        schema,
    )
}

fn main() {
    let mode = mode_from_args();
    let hello = hello(DEFAULT_CAP);
    let mut schema = Schema::from_hello(&hello, &[CUSTOM_KEY]);
    schema.codec(CUSTOM_KEY);
    let stdout = io::stdout();
    let mut out = stdout.lock();
    if send(&mut out, &WorkerMsg::Hello(hello), &Schema::new()).is_err() {
        return;
    }
    let stdin = io::stdin();
    let mut input = stdin.lock();
    let mut buf = Vec::new();
    let mut state: BTreeMap<String, WireValue> = BTreeMap::new();
    loop {
        match read_packet(&mut input, DEFAULT_CAP, &mut buf) {
            Ok(()) => {}
            Err(_) => return,
        }
        let msg = match HostMsg::decode(&buf, &schema) {
            Ok(msg) => msg,
            Err(error) => {
                let _ = send(
                    &mut out,
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
            HostMsg::Run(run_msg) => {
                match &mode {
                    Mode::Exit => process::exit(3),
                    Mode::ExitOnce(marker) => {
                        if !marker.exists() {
                            let _ = std::fs::write(marker, b"exited");
                            process::exit(3);
                        }
                    }
                    Mode::Hang => loop {
                        std::thread::sleep(Duration::from_secs(3600));
                    },
                    Mode::Oversize => {
                        let _ = out.write_all(&(2u32 << 20).to_le_bytes());
                        let _ = out.flush();
                        loop {
                            std::thread::sleep(Duration::from_secs(3600));
                        }
                    }
                    Mode::StderrFlood => {
                        let flood = vec![b'x'; 1 << 20];
                        let _ = io::stderr().write_all(&flood);
                    }
                    Mode::Log => {
                        for (level, text) in [
                            (LogLevel::Info, "starting the sum"),
                            (LogLevel::Warn, "the sum is large"),
                        ] {
                            let line = WorkerMsg::Log {
                                level,
                                text: text.to_owned(),
                            };
                            let _ = send(&mut out, &line, &schema);
                        }
                    }
                    Mode::LogExit => {
                        let line = WorkerMsg::Log {
                            level: LogLevel::Error,
                            text: "the device went away".to_owned(),
                        };
                        let _ = send(&mut out, &line, &schema);
                        let _ = out.flush();
                        process::exit(3);
                    }
                    Mode::LongFault => {
                        let reason: String = (0..8)
                            .map(|line| format!("the check found line {line} out of place\n"))
                            .collect();
                        let fault = WorkerMsg::Fault(Fault {
                            run: run_msg.run,
                            node: run_msg.nodes.first().cloned(),
                            code: FaultCode::NodeError,
                            reason,
                        });
                        let done = WorkerMsg::Outputs(Outputs {
                            run: run_msg.run,
                            status: RunStatus::Fault,
                            outputs: Vec::new(),
                        });
                        if send(&mut out, &fault, &schema).is_err()
                            || send(&mut out, &done, &schema).is_err()
                        {
                            return;
                        }
                        continue;
                    }
                    Mode::Normal => {}
                }
                if run(run_msg, &schema, &mut state, &mut out).is_err() {
                    return;
                }
            }
            // A cancel that arrives between runs has nothing to stop.
            HostMsg::Cancel { .. } => {}
            HostMsg::Ping => {
                let _ = send(&mut out, &WorkerMsg::Pong, &schema);
            }
            HostMsg::StateExport { node } => {
                let value = state.get(&node).cloned().unwrap_or(WireValue::I64(0));
                let _ = send(&mut out, &WorkerMsg::StateValue { node, value }, &schema);
            }
            HostMsg::StateImport { node, value } => {
                state.insert(node, value);
            }
            HostMsg::Shutdown => return,
        }
    }
}
