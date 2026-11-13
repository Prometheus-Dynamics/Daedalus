//! The worker loop. A reader thread takes packets from stdin, so a cancel can reach a running
//! node; the loop runs each node under a panic guard, and sends its events and outputs in order.

use std::io::{self, Write};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, channel};
use std::thread;

use daedalus_registry::{NodeRecord, PortDecl, TypeRef, port_type};
use daedalus_types::{Scalar, TypeKey, Typed};
use daedalus_wire::codec::Writer;
use daedalus_wire::{
    BoundaryValue, DEFAULT_CAP, Declaration, Fault, FaultCode, Hello, HostMsg, Outputs,
    PacketError, PortSpec, Progress, RunStatus, Schema, WireType, WireValue, WorkerMsg,
    read_packet, write_packet,
};

use crate::context::{Context, Event};

/// A node's body: its inputs by port name, and its context. It returns one value per output port.
pub type NodeFn =
    fn(&[(&'static str, WireValue)], &mut Context) -> Result<Vec<WireValue>, NodeError>;

/// What a Rust node's body is: the function the worker calls, and the type of its state if it keeps
/// one. A stateful node names its state type with [`state_type`], so the hello declares it.
#[derive(Clone, Copy)]
pub struct RustBody {
    pub run: NodeFn,
    pub state: Option<fn() -> TypeRef>,
}

/// The type of a node's state: the port type of `T`, which the host declares in the hello.
pub const fn state_type<T: Typed>() -> TypeRef {
    port_type::<T>()
}

/// A node that failed: the fault code and its reason.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeError {
    pub code: FaultCode,
    pub reason: String,
}

impl std::fmt::Display for NodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.reason)
    }
}

impl std::error::Error for NodeError {}

impl NodeError {
    pub fn new(code: FaultCode, reason: impl Into<String>) -> NodeError {
        NodeError {
            code,
            reason: reason.into(),
        }
    }

    pub fn bad_input(reason: impl Into<String>) -> NodeError {
        NodeError::new(FaultCode::BadInput, reason)
    }
}

/// One served node, with the port types the hello declared.
struct Served {
    id: &'static str,
    run: NodeFn,
    inputs: Vec<(&'static str, WireType)>,
    outputs: Vec<(&'static str, WireType)>,
    /// The state's wire type, when the node keeps state.
    state_type: Option<WireType>,
    /// The state the node holds between runs. Set by a run, or by an import from the host.
    state: Option<WireValue>,
}

/// A port's wire type: a scalar key, an opaque key, or a full type expression.
fn port_wire_type(port: &PortDecl) -> WireType {
    wire_type(&port.ty)
}

fn wire_type(ty: &TypeRef) -> WireType {
    match ty {
        TypeRef::Key(key) => key_type(key),
        TypeRef::Expr(expr) => WireType::from_expr(expr),
        TypeRef::Body => WireType::Opaque(String::new()),
    }
}

fn key_type(key: &TypeKey) -> WireType {
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
    match SCALARS
        .iter()
        .find(|scalar| scalar.key().as_str() == key.as_str())
    {
        Some(scalar) => WireType::Scalar(*scalar),
        None => WireType::Opaque(key.as_str().to_owned()),
    }
}

/// Every opaque type key a wire type names, once each, so the schema has a codec for each.
fn opaque_keys(ty: &WireType, keys: &mut Vec<String>) {
    match ty {
        WireType::Opaque(key) => {
            if !keys.contains(key) {
                keys.push(key.clone());
            }
        }
        WireType::Scalar(_) => {}
        WireType::Optional(inner) | WireType::List(inner) => opaque_keys(inner, keys),
        WireType::Map(key, value) => {
            opaque_keys(key, keys);
            opaque_keys(value, keys);
        }
        WireType::Tuple(items) => items.iter().for_each(|item| opaque_keys(item, keys)),
        WireType::Struct(fields) => fields
            .iter()
            .for_each(|(_, field)| opaque_keys(field, keys)),
        WireType::Enum(variants) => {
            for (_, payload) in variants {
                if let Some(payload) = payload {
                    opaque_keys(payload, keys);
                }
            }
        }
    }
}

/// The nodes a worker serves, the hello that declares them, and the opaque keys they use. A record
/// whose body is not a Rust body is refused.
fn served(nodes: &[&'static NodeRecord]) -> Result<(Vec<Served>, Hello, Vec<String>), String> {
    let mut out = Vec::new();
    let mut declarations = Vec::new();
    let mut keys = Vec::new();
    for record in nodes {
        let body = record
            .body
            .code
            .downcast_ref::<RustBody>()
            .ok_or_else(|| format!("node {} has no Rust body", record.decl.id))?;
        let inputs: Vec<(&'static str, WireType)> = record
            .decl
            .inputs
            .iter()
            .map(|port| (port.name, port_wire_type(port)))
            .collect();
        let outputs: Vec<(&'static str, WireType)> = record
            .decl
            .outputs
            .iter()
            .map(|port| (port.name, port_wire_type(port)))
            .collect();
        let state_type = body.state.map(|state| wire_type(&state()));
        for (_, ty) in inputs.iter().chain(&outputs) {
            opaque_keys(ty, &mut keys);
        }
        if let Some(ty) = &state_type {
            opaque_keys(ty, &mut keys);
        }
        declarations.push(Declaration {
            id: record.decl.id.to_owned(),
            inputs: inputs
                .iter()
                .map(|(name, ty)| PortSpec::new(name, ty.clone()))
                .collect(),
            outputs: outputs
                .iter()
                .map(|(name, ty)| PortSpec::new(name, ty.clone()))
                .collect(),
            config: Vec::new(),
            state: state_type.clone(),
            capabilities: Vec::new(),
        });
        out.push(Served {
            id: record.decl.id,
            run: body.run,
            inputs,
            outputs,
            state_type,
            state: None,
        });
    }
    Ok((
        out,
        Hello::new("daedalus-lang-rust", DEFAULT_CAP, declarations),
        keys,
    ))
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

/// Runs the worker loop on stdin and stdout until the host shuts it down or closes the pipe.
/// Each node runs under a panic guard: a panic is a `node_panic` fault, and the loop keeps going.
pub fn serve(nodes: &[&'static NodeRecord]) -> io::Result<()> {
    let (mut served, hello, keys) = served(nodes).map_err(io::Error::other)?;
    let keys: Vec<&str> = keys.iter().map(String::as_str).collect();
    let schema = Schema::from_hello(&hello, &keys);
    let stdout = io::stdout();
    let mut out = stdout.lock();
    send(&mut out, &WorkerMsg::Hello(hello), &Schema::new()).map_err(io::Error::other)?;

    let current = Arc::new(AtomicU64::new(0));
    let cancel = Arc::new(AtomicBool::new(false));
    let packets = read_in_thread(current.clone(), cancel.clone());

    while let Ok(packet) = packets.recv() {
        let packet = match packet {
            Ok(packet) => packet,
            Err(_) => return Ok(()),
        };
        let msg = match HostMsg::decode(&packet, &schema) {
            Ok(msg) => msg,
            Err(error) => {
                let fault = WorkerMsg::Fault(Fault {
                    run: 0,
                    node: None,
                    code: FaultCode::MalformedPacket,
                    reason: error.to_string(),
                });
                send(&mut out, &fault, &schema).map_err(io::Error::other)?;
                continue;
            }
        };
        match msg {
            HostMsg::Run(run) => {
                let mut outputs = Vec::new();
                let mut status = RunStatus::Ok;
                for id in &run.nodes {
                    let Some(node) = served.iter_mut().find(|node| node.id == id) else {
                        let fault = WorkerMsg::Fault(Fault {
                            run: run.run,
                            node: Some(id.clone()),
                            code: FaultCode::UnknownNode,
                            reason: format!("no node {id}"),
                        });
                        send(&mut out, &fault, &schema).map_err(io::Error::other)?;
                        status = RunStatus::Fault;
                        break;
                    };
                    match run_node(node, &run, &cancel, &schema, &mut out)? {
                        Ok(values) => outputs.extend(values),
                        Err(error) => {
                            let fault = WorkerMsg::Fault(Fault {
                                run: run.run,
                                node: Some(id.clone()),
                                code: error.code,
                                reason: error.reason,
                            });
                            send(&mut out, &fault, &schema).map_err(io::Error::other)?;
                            status = RunStatus::Fault;
                            outputs.clear();
                            break;
                        }
                    }
                }
                if status == RunStatus::Cancelled || cancel.load(Ordering::SeqCst) {
                    status = RunStatus::Cancelled;
                }
                let reply = WorkerMsg::Outputs(Outputs {
                    run: run.run,
                    status,
                    outputs,
                });
                send(&mut out, &reply, &schema).map_err(io::Error::other)?;
            }
            HostMsg::Ping => send(&mut out, &WorkerMsg::Pong, &schema).map_err(io::Error::other)?,
            HostMsg::Shutdown => return Ok(()),
            // A cancel between runs has nothing to stop.
            HostMsg::Cancel { .. } => {}
            HostMsg::StateExport { node } => {
                let state = served
                    .iter()
                    .find(|served| served.id == node)
                    .filter(|served| served.state_type.is_some())
                    .and_then(|served| served.state.clone());
                let reply = match state {
                    Some(value) => WorkerMsg::StateValue { node, value },
                    None => WorkerMsg::Fault(Fault {
                        run: 0,
                        node: Some(node),
                        code: FaultCode::BadInput,
                        reason: "the node has no state to export".to_owned(),
                    }),
                };
                send(&mut out, &reply, &schema).map_err(io::Error::other)?;
            }
            HostMsg::StateImport { node, value } => {
                match served.iter_mut().find(|served| served.id == node) {
                    Some(served) if served.state_type.is_some() => served.state = Some(value),
                    _ => {
                        let fault = WorkerMsg::Fault(Fault {
                            run: 0,
                            node: Some(node),
                            code: FaultCode::BadInput,
                            reason: "the node has no state to import".to_owned(),
                        });
                        send(&mut out, &fault, &schema).map_err(io::Error::other)?;
                    }
                }
            }
        }
    }
    Ok(())
}

/// Runs one node. Its events go out before anything else, and a panic becomes a fault.
fn run_node(
    node: &mut Served,
    run: &daedalus_wire::Run,
    cancel: &Arc<AtomicBool>,
    schema: &Schema,
    out: &mut impl Write,
) -> io::Result<Result<Vec<BoundaryValue>, NodeError>> {
    let inputs: Vec<(&'static str, WireValue)> = node
        .inputs
        .iter()
        .filter_map(|(name, _)| {
            run.inputs
                .iter()
                .find(|entry| entry.node == node.id && entry.port == *name)
                .map(|entry| (*name, entry.value.clone()))
        })
        .collect();
    let mut context = Context::new(cancel.clone(), node.state.take());
    let run_fn = node.run;
    let result = catch_unwind(AssertUnwindSafe(|| run_fn(&inputs, &mut context)));
    let (state, events) = context.take();
    node.state = state;
    for event in events {
        let msg = match event {
            Event::Progress { fraction, message } => WorkerMsg::Progress(Progress {
                run: run.run,
                node_index: 0,
                message,
                fraction: Some(fraction),
            }),
            Event::Log { level, text } => WorkerMsg::Log { level, text },
            Event::Detail { text } => WorkerMsg::Detail { run: run.run, text },
        };
        send(out, &msg, schema).map_err(io::Error::other)?;
    }
    let values = match result {
        Err(payload) => {
            // What the panic said travels with the fault: the host reads it as the reason.
            let said = if let Some(text) = payload.downcast_ref::<&str>() {
                text
            } else if let Some(text) = payload.downcast_ref::<String>() {
                text
            } else {
                "a panic with no message"
            };
            return Ok(Err(NodeError::new(
                FaultCode::NodePanic,
                format!("node panicked: {said}"),
            )));
        }
        Ok(Err(error)) => return Ok(Err(error)),
        Ok(Ok(values)) => values,
    };
    if values.len() != node.outputs.len() {
        return Ok(Err(NodeError::new(
            FaultCode::Internal,
            format!(
                "{} returned {} values for {} outputs",
                node.id,
                values.len(),
                node.outputs.len()
            ),
        )));
    }
    Ok(Ok(node
        .outputs
        .iter()
        .zip(values)
        .map(|((name, _), value)| BoundaryValue {
            node: node.id.to_owned(),
            port: (*name).to_owned(),
            value,
        })
        .collect()))
}

/// The stdin reader. It forwards every packet in order. A run starts a new cancel window, and a
/// cancel that names the run going on sets the cancel flag, so a cancel that arrives right after its
/// run is never lost.
fn read_in_thread(
    current: Arc<AtomicU64>,
    cancel: Arc<AtomicBool>,
) -> Receiver<Result<Vec<u8>, PacketError>> {
    let (tx, rx) = channel();
    thread::spawn(move || {
        let stdin = io::stdin();
        let mut input = stdin.lock();
        let mut buf = Vec::new();
        loop {
            match read_packet(&mut input, DEFAULT_CAP, &mut buf) {
                Ok(()) => {
                    let run_id = buf
                        .get(1..9)
                        .map(|raw| u64::from_le_bytes(raw.try_into().unwrap_or([0; 8])));
                    match (buf.first(), run_id) {
                        (Some(&daedalus_wire::kind::RUN), Some(run)) => {
                            current.store(run, Ordering::SeqCst);
                            cancel.store(false, Ordering::SeqCst);
                        }
                        (Some(&daedalus_wire::kind::CANCEL), Some(run))
                            if run == current.load(Ordering::SeqCst) =>
                        {
                            cancel.store(true, Ordering::SeqCst);
                        }
                        _ => {}
                    }
                    let packet = std::mem::take(&mut buf);
                    if tx.send(Ok(packet)).is_err() {
                        return;
                    }
                }
                Err(error) => {
                    let _ = tx.send(Err(error));
                    return;
                }
            }
        }
    });
    rx
}
