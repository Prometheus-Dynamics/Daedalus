//! Serves one plugin library's nodes over the wire protocol. A plugin's crash ends only this
//! process, and the host's supervisor reports and respawns it.
//!
//! Usage: `daedalus-plugin-worker [--lock FILE [--record]] <plugin library>`. With `--lock`, the
//! library loads only if the lock approves its digest; with `--record`, it is written into the lock
//! once it has loaded.

#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::io::{self, Write};
use std::path::PathBuf;
use std::process;

use daedalus_abi::{BoundaryPort, Slot, SlotState, Slots, Step, StepCx, StepEnd};
use daedalus_integrity::Integrity;
use daedalus_loader::{Loader, Plugin, RegionIn, run_step};
use daedalus_process_shm::{Inbox, Incoming, Lent, PLAIN_LINE};
use daedalus_types::{Scalar, TypeKey};
use daedalus_wire::codec::{Reader, Writer};
use daedalus_wire::{
    BoundaryValue, DEFAULT_CAP, Declaration, Fault, FaultCode, Hello, HostMsg, LogLevel, Outputs,
    PacketError, PortSpec, RunStatus, Schema, WireType, WireValue, WorkerMsg, decode_value,
    encode_value, write_packet,
};

/// A node of the plugin, compiled once: its step, and its port types in declaration order.
struct Served {
    step: Step,
    inputs: Vec<(String, WireType)>,
    outputs: Vec<(String, WireType)>,
}

/// A port's wire type from its registered type key: a scalar's key, or an opaque key.
fn wire_type(key: &TypeKey) -> WireType {
    let text = key.as_str();
    const SCALARS: [Scalar; 16] = [
        Scalar::Unit,
        Scalar::Bool,
        Scalar::I8,
        Scalar::I16,
        Scalar::I32,
        Scalar::Int,
        Scalar::ISize,
        Scalar::U8,
        Scalar::U16,
        Scalar::U32,
        Scalar::U64,
        Scalar::USize,
        Scalar::F32,
        Scalar::Float,
        Scalar::String,
        Scalar::Bytes,
    ];
    match SCALARS.iter().find(|scalar| scalar.key().as_str() == text) {
        Some(scalar) => WireType::Scalar(*scalar),
        None => WireType::Opaque(text.to_owned()),
    }
}

/// The slot form of a wire value: scalars are their own bytes, and compounds their wire form.
fn to_slot(ty: &WireType, value: &WireValue) -> Option<Vec<u8>> {
    Some(match value {
        WireValue::Unit => Vec::new(),
        WireValue::Bool(v) => vec![u8::from(*v)],
        WireValue::I8(v) => v.to_le_bytes().to_vec(),
        WireValue::I16(v) => v.to_le_bytes().to_vec(),
        WireValue::I32(v) => v.to_le_bytes().to_vec(),
        WireValue::I64(v) | WireValue::ISize(v) => v.to_le_bytes().to_vec(),
        WireValue::U8(v) => v.to_le_bytes().to_vec(),
        WireValue::U16(v) => v.to_le_bytes().to_vec(),
        WireValue::U32(v) => v.to_le_bytes().to_vec(),
        WireValue::U64(v) | WireValue::USize(v) => v.to_le_bytes().to_vec(),
        WireValue::F32(v) => v.to_le_bytes().to_vec(),
        WireValue::F64(v) => v.to_le_bytes().to_vec(),
        WireValue::String(text) => text.as_bytes().to_vec(),
        WireValue::Bytes(bytes) | WireValue::Opaque(bytes) => bytes.clone(),
        _ => {
            let mut out = Writer::new();
            encode_value(ty, value, &Schema::new(), &mut out).ok()?;
            out.buf
        }
    })
}

/// The wire value of a slot's bytes.
fn from_slot(ty: &WireType, bytes: &[u8]) -> Option<WireValue> {
    let fixed = |n: usize| -> Option<[u8; 8]> {
        let mut out = [0u8; 8];
        (bytes.len() == n).then(|| {
            out[..n].copy_from_slice(bytes);
            out
        })
    };
    Some(match ty {
        WireType::Scalar(Scalar::Unit) => WireValue::Unit,
        WireType::Scalar(Scalar::Bool) => WireValue::Bool(*bytes.first()? != 0),
        WireType::Scalar(Scalar::I8) => WireValue::I8(i8::from_le_bytes([*bytes.first()?])),
        WireType::Scalar(Scalar::U8) => WireValue::U8(*bytes.first()?),
        WireType::Scalar(Scalar::I16) => {
            WireValue::I16(i16::from_le_bytes(fixed(2)?[..2].try_into().ok()?))
        }
        WireType::Scalar(Scalar::U16) => {
            WireValue::U16(u16::from_le_bytes(fixed(2)?[..2].try_into().ok()?))
        }
        WireType::Scalar(Scalar::I32) => {
            WireValue::I32(i32::from_le_bytes(fixed(4)?[..4].try_into().ok()?))
        }
        WireType::Scalar(Scalar::U32) => {
            WireValue::U32(u32::from_le_bytes(fixed(4)?[..4].try_into().ok()?))
        }
        WireType::Scalar(Scalar::F32) => {
            WireValue::F32(f32::from_le_bytes(fixed(4)?[..4].try_into().ok()?))
        }
        WireType::Scalar(Scalar::Int) => WireValue::I64(i64::from_le_bytes(fixed(8)?)),
        WireType::Scalar(Scalar::ISize) => WireValue::ISize(i64::from_le_bytes(fixed(8)?)),
        WireType::Scalar(Scalar::U64) => WireValue::U64(u64::from_le_bytes(fixed(8)?)),
        WireType::Scalar(Scalar::USize) => WireValue::USize(u64::from_le_bytes(fixed(8)?)),
        WireType::Scalar(Scalar::Float) => WireValue::F64(f64::from_le_bytes(fixed(8)?)),
        WireType::Scalar(Scalar::String) => {
            WireValue::String(String::from_utf8(bytes.to_vec()).ok()?)
        }
        WireType::Scalar(Scalar::Bytes) => WireValue::Bytes(bytes.to_vec()),
        WireType::Opaque(_) => WireValue::Opaque(bytes.to_vec()),
        _ => decode_value(ty, &mut Reader::new(bytes), &Schema::new()).ok()?,
    })
}

fn send(out: &mut impl Write, msg: &WorkerMsg, schema: &Schema) -> Result<(), PacketError> {
    let mut writer = Writer::new();
    msg.encode(schema, &mut writer)
        .map_err(|_| PacketError::TooLarge {
            len: 0,
            cap: DEFAULT_CAP,
        })?;
    write_packet(out, &writer.buf, DEFAULT_CAP)
}

/// Runs one node's step on the run's inputs.
fn execute(
    served: &mut Served,
    node: &str,
    inputs: &[BoundaryValue],
    kept: &'static daedalus_abi::KeptDetail,
    node_code: &mut Option<u32>,
) -> Result<Vec<BoundaryValue>, (FaultCode, String)> {
    // Input bytes live here for the call; the cells point into them.
    let mut bufs = Vec::new();
    for (port, ty) in &served.inputs {
        let value = inputs
            .iter()
            .find(|entry| entry.node == node && entry.port == *port)
            .ok_or((FaultCode::BadInput, format!("{node} has no input {port}")))?;
        let bytes = to_slot(ty, &value.value)
            .ok_or((FaultCode::BadInput, format!("{port} does not fit its type")))?;
        bufs.push(bytes);
    }
    let mut cells: Vec<Slot> = bufs
        .iter_mut()
        .map(|buf| Slot {
            state: SlotState::Full as u32,
            data: buf.as_mut_ptr(),
            len: buf.len(),
            cap: buf.len(),
            ..Slot::EMPTY
        })
        .collect();
    cells.extend(std::iter::repeat_n(Slot::EMPTY, served.outputs.len()));
    let n_in = served.inputs.len();
    let mut cx = StepCx::new();
    // What the node gives `detail` is kept here, and sent to the host if it fails.
    kept.attach(&mut cx);
    let end = run_step(&mut served.step, &mut Slots::of(&mut cells), &mut cx);
    if !matches!(end, StepEnd::Done) {
        // A panic is the host's to name, and travels as that kind of fault. Any other code is
        // the node's own, and is sent as it is.
        let panicked = cx.code_is_own() && cx.code == daedalus_abi::own::PANIC;
        if !cx.code_is_own() && cx.code != 0 {
            *node_code = Some(cx.code);
        }
        let kind = if panicked {
            FaultCode::NodePanic
        } else {
            FaultCode::NodeError
        };
        return Err((kind, cx.reason().to_owned()));
    }
    let mut outputs = Vec::new();
    for (index, (port, ty)) in served.outputs.iter().enumerate() {
        let bytes = cells[n_in + index]
            .bytes()
            .ok_or((FaultCode::Internal, format!("{port} was not written")))?;
        let value =
            from_slot(ty, bytes).ok_or((FaultCode::Internal, format!("{port} does not decode")))?;
        outputs.push(BoundaryValue {
            node: node.to_owned(),
            port: port.clone(),
            value,
        });
    }
    Ok(outputs)
}

const USAGE: &str = "usage: daedalus-plugin-worker [--lock FILE [--record]] <plugin library>";

/// The integrity mode and the library, from the arguments.
fn arguments(args: &[String]) -> Result<(Integrity, PathBuf), String> {
    let mut lock = None;
    let mut record = false;
    let mut library = None;
    let mut rest = args.iter();
    while let Some(arg) = rest.next() {
        match arg.as_str() {
            "--lock" => {
                let file = rest.next().ok_or("--lock needs a file")?;
                lock = Some(PathBuf::from(file));
            }
            "--record" => record = true,
            _ if library.is_none() => library = Some(PathBuf::from(arg)),
            _ => return Err("one plugin library at a time".into()),
        }
    }
    let library = library.ok_or("no plugin library given")?;
    let integrity = match (lock, record) {
        (None, false) => Integrity::off(),
        (None, true) => return Err("--record needs --lock".into()),
        (Some(lock), false) => Integrity::locked(lock).map_err(|error| error.to_string())?,
        (Some(lock), true) => Integrity::record(lock).map_err(|error| error.to_string())?,
    };
    Ok((integrity, library))
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (integrity, path) = match arguments(&args) {
        Ok(parsed) => parsed,
        Err(message) => {
            let _ = writeln!(io::stderr(), "{message}\n{USAGE}");
            process::exit(2);
        }
    };
    let mut loader = Loader::with_integrity(integrity);
    let plugin: Plugin = match loader.open(&path) {
        Ok(plugin) => plugin,
        Err(error) => {
            let _ = writeln!(io::stderr(), "{error}");
            process::exit(2);
        }
    };
    let (hello, mut served) = describe(&plugin);
    let schema = Schema::from_hello(&hello, &[]);
    // The packets go to a copy of stdout, so a plugin node that prints cannot corrupt them.
    let mut out = match daedalus_worker_io::packet_stream() {
        Ok(out) => out,
        Err(error) => {
            let _ = writeln!(io::stderr(), "{error}");
            process::exit(2);
        }
    };
    if send(&mut out, &WorkerMsg::Hello(hello), &Schema::new()).is_err() {
        return;
    }
    // Shared memory, when the host lends it: large runs and outputs travel in lent buffers.
    let lent = Lent::default();
    let mut unanswered = lent.ask(&mut out).unwrap_or(false);
    let mut inbox = Inbox::new(lent.clone());
    let mut buf = Vec::new();
    let kept = daedalus_abi::KeptDetail::forever();
    loop {
        let decoded = match inbox.next(&mut buf) {
            Ok(Incoming::Packet(len)) => {
                if unanswered && buf.first() == Some(&daedalus_wire::kind::RUN) {
                    unanswered = false;
                    log(&mut out, PLAIN_LINE, &schema);
                }
                HostMsg::decode(&buf[..len], &schema)
            }
            Ok(Incoming::Run { len, .. }) => {
                match lent.run(len, |bytes| HostMsg::decode(bytes, &schema)) {
                    Some(decoded) => decoded,
                    None => continue,
                }
            }
            Ok(Incoming::Granted) => {
                unanswered = false;
                continue;
            }
            Ok(Incoming::Plain(reason)) => {
                unanswered = false;
                log(&mut out, &format!("{PLAIN_LINE}: {reason}"), &schema);
                continue;
            }
            Err(_) => return,
        };
        let msg = match decoded {
            Ok(msg) => msg,
            Err(error) => {
                let fault = WorkerMsg::Fault(Fault {
                    run: 0,
                    node: None,
                    code: FaultCode::MalformedPacket,
                    reason: error.to_string(),
                });
                let _ = send(&mut out, &fault, &schema);
                continue;
            }
        };
        match msg {
            HostMsg::Run(run) => {
                let mut outputs = Vec::new();
                let mut status = RunStatus::Ok;
                for node in &run.nodes {
                    let Some(entry) = served.get_mut(node) else {
                        let fault = WorkerMsg::Fault(Fault {
                            run: run.run,
                            node: Some(node.clone()),
                            code: FaultCode::UnknownNode,
                            reason: format!("no node {node}"),
                        });
                        let _ = send(&mut out, &fault, &schema);
                        status = RunStatus::Fault;
                        break;
                    };
                    let mut node_code = None;
                    let result = execute(entry, node, &run.inputs, kept, &mut node_code);
                    let detail = kept.take();
                    match result {
                        Ok(values) => outputs.extend(values),
                        Err((code, reason)) => {
                            if !detail.is_empty() {
                                let more = WorkerMsg::Detail {
                                    run: run.run,
                                    text: detail,
                                };
                                let _ = send(&mut out, &more, &schema);
                            }
                            if let Some(code) = node_code {
                                let number = WorkerMsg::NodeCode { run: run.run, code };
                                let _ = send(&mut out, &number, &schema);
                            }
                            let fault = WorkerMsg::Fault(Fault {
                                run: run.run,
                                node: Some(node.clone()),
                                code,
                                reason,
                            });
                            let _ = send(&mut out, &fault, &schema);
                            status = RunStatus::Fault;
                            outputs.clear();
                            break;
                        }
                    }
                }
                let reply = WorkerMsg::Outputs(Outputs {
                    run: run.run,
                    status,
                    outputs,
                });
                let mut writer = Writer::new();
                if reply.encode(&schema, &mut writer).is_err()
                    || lent.send_outputs(&writer.buf, &mut out).is_err()
                {
                    return;
                }
            }
            HostMsg::Ping => {
                let _ = send(&mut out, &WorkerMsg::Pong, &schema);
            }
            HostMsg::Shutdown => return,
            // A plugin's nodes keep no state through this worker, and a cancel between runs has
            // nothing to stop.
            HostMsg::Cancel { .. } | HostMsg::StateExport { .. } | HostMsg::StateImport { .. } => {}
        }
    }
}

/// Sends one log line to the host.
fn log(out: &mut impl Write, text: &str, schema: &Schema) {
    let line = WorkerMsg::Log {
        level: LogLevel::Info,
        text: text.to_owned(),
    };
    let _ = send(out, &line, schema);
}

/// The hello for a plugin's nodes, with each node compiled once.
fn describe(plugin: &Plugin) -> (Hello, BTreeMap<String, Served>) {
    let mut declarations = Vec::new();
    let mut served = BTreeMap::new();
    for decl in plugin.nodes() {
        let port = |name: &str, key: &TypeKey| PortSpec::new(name, wire_type(key));
        let inputs: Vec<PortSpec> = decl
            .inputs
            .iter()
            .map(|p| match &p.ty {
                daedalus_registry::TypeRef::Key(key) => port(&p.name, key),
                _ => PortSpec::new(&p.name, WireType::Opaque(String::new())),
            })
            .collect();
        let outputs: Vec<PortSpec> = decl
            .outputs
            .iter()
            .map(|p| match &p.ty {
                daedalus_registry::TypeRef::Key(key) => port(&p.name, key),
                _ => PortSpec::new(&p.name, WireType::Opaque(String::new())),
            })
            .collect();
        let Some(backend) = plugin.backend(&decl.body.backend) else {
            continue;
        };
        let ports_in: Vec<BoundaryPort> = (0..inputs.len())
            .map(|i| BoundaryPort {
                node: 0,
                port: i as u16,
                reserved: 0,
                type_index: 0,
                slot: 0,
            })
            .collect();
        let ports_out: Vec<BoundaryPort> = (0..outputs.len())
            .map(|i| BoundaryPort {
                node: 0,
                port: i as u16,
                reserved: 0,
                type_index: 0,
                slot: 0,
            })
            .collect();
        let region = RegionIn {
            nodes: &[decl.id.as_str()],
            inputs: &ports_in,
            outputs: &ports_out,
            consts: &[],
        };
        let Ok(mut units) = backend.compile(&region) else {
            continue;
        };
        let Some(unit) = units.pop() else {
            continue;
        };
        served.insert(
            decl.id.clone(),
            Served {
                step: unit.step,
                inputs: inputs
                    .iter()
                    .map(|p| (p.name.clone(), p.ty.clone()))
                    .collect(),
                outputs: outputs
                    .iter()
                    .map(|p| (p.name.clone(), p.ty.clone()))
                    .collect(),
            },
        );
        declarations.push(Declaration {
            id: decl.id.clone(),
            inputs,
            outputs,
            config: Vec::new(),
            state: None,
            capabilities: Vec::new(),
        });
    }
    (
        Hello::new("daedalus-plugin-worker", DEFAULT_CAP, declarations),
        served,
    )
}
