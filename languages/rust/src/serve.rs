//! The worker loop. A reader thread takes packets from stdin, so a cancel can reach a running
//! node; the loop runs each node under a panic guard, and sends its events and outputs in order.
//!
//! Right after its hello the worker asks the host for shared memory. A host that lends it some
//! sends large runs in a buffer and takes large outputs in another (`daedalus-process-shm`); one
//! that lends none, or an older host that doesn't answer, leaves the worker on the plain path,
//! and the worker logs one line saying so.
//!
//! It asks for values by reference too. A host that agrees sends a large `bytes` or opaque input
//! as its place: in the host's buffer, or in a buffer by descriptor (a dma-buf), which the worker
//! maps read-only and reads inside the kernel's sync brackets. A node's `&[u8]` parameter borrows
//! those bytes for the call. A large `bytes` output is written into the value buffer the host named
//! for the run, and the host's cell holds it there.

use std::io::{self, Write};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, channel};
use std::thread;

use daedalus_process_shm::{Inbox, Incoming, Lent, PLAIN_LINE, Reads, View, Writes};
use daedalus_registry::{NodeRecord, PortDecl, TypeRef, port_type};
use daedalus_types::{Scalar, TypeKey, Typed};
use daedalus_wire::codec::Writer;
use daedalus_wire::{
    BoundaryValue, DEFAULT_CAP, Declaration, Dir, Fault, FaultCode, Hello, HostMsg, LogLevel,
    Outputs, PacketError, PortSpec, Progress, RunStatus, Schema, WireType, WireValue, WorkerMsg,
    encode_bare, write_packet,
};

use crate::codec::Value;
use crate::context::{Context, Event};

/// A node's body: its inputs by port name, and its context. It returns one value per output port.
pub type NodeFn = fn(&crate::codec::Inputs<'_>, &mut Context) -> Result<Vec<WireValue>, NodeError>;

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
    let mut schema = Schema::from_hello(&hello, &keys);
    // The packets go to a copy of stdout. After this, stdout is stderr: a node that prints goes there.
    let mut out = daedalus_worker_io::packet_stream()?;
    send(&mut out, &WorkerMsg::Hello(hello), &Schema::new()).map_err(io::Error::other)?;
    let lent = Lent::default();
    // Until the host answers, a run means it never will: it is an older host.
    let features = daedalus_wire::shared::BY_REFERENCE | daedalus_wire::shared::ANY_TYPE;
    let mut unanswered = lent.ask_for(features, &mut out).map_err(io::Error::other)?;

    let current = Arc::new(AtomicU64::new(0));
    let cancel = Arc::new(AtomicBool::new(false));
    let packets = read_in_thread(current.clone(), cancel.clone(), Inbox::new(lent.clone()));

    while let Ok(packet) = packets.recv() {
        let decoded = match packet {
            Ok(Packet::Bytes(packet)) => {
                if unanswered && packet.first() == Some(&daedalus_wire::kind::RUN) {
                    unanswered = false;
                    log(&mut out, PLAIN_LINE, &schema)?;
                }
                HostMsg::decode(&packet, &schema)
            }
            Ok(Packet::Run(len)) => match lent.run(len, |bytes| HostMsg::decode(bytes, &schema)) {
                Some(decoded) => decoded,
                None => continue,
            },
            Ok(Packet::Answer(None)) => {
                unanswered = false;
                schema.set_features(lent.features());
                continue;
            }
            Ok(Packet::Answer(Some(reason))) => {
                unanswered = false;
                log(&mut out, &format!("{PLAIN_LINE}: {reason}"), &schema)?;
                continue;
            }
            Err(_) => return Ok(()),
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
                send(&mut out, &fault, &schema).map_err(io::Error::other)?;
                continue;
            }
        };
        match msg {
            HostMsg::Run(run) => {
                let (mut status, outputs) = lent
                    .view(|view| run_nodes(&mut served, &run, view, &cancel, &schema, &mut out))?;
                if status == RunStatus::Cancelled || cancel.load(Ordering::SeqCst) {
                    status = RunStatus::Cancelled;
                }
                let reply = WorkerMsg::Outputs(Outputs {
                    run: run.run,
                    status,
                    outputs,
                });
                let mut writer = Writer::new();
                reply
                    .encode(&schema, &mut writer)
                    .map_err(|error| io::Error::other(error.to_string()))?;
                lent.send_outputs(&writer.buf, &mut out)
                    .map_err(io::Error::other)?;
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

/// Runs a run's nodes in order, with its inputs by reference where the host sent them so. Every
/// read of a buffer by descriptor is inside the kernel's sync brackets. Gives the run's status and
/// its outputs; a node that fails sends its fault, and the run ends there.
fn run_nodes(
    served: &mut [Served],
    run: &daedalus_wire::Run,
    view: &mut View<'_>,
    cancel: &Arc<AtomicBool>,
    schema: &Schema,
    out: &mut impl Write,
) -> io::Result<(RunStatus, Vec<BoundaryValue>)> {
    let slots: Vec<u32> = run
        .inputs
        .iter()
        .filter_map(|entry| match &entry.value {
            WireValue::Placed(daedalus_wire::shared::Place {
                at: daedalus_wire::shared::At::Fd(slot),
                ..
            }) => Some(*slot),
            _ => None,
        })
        .collect();
    let mut begun = 0;
    let mut result = Ok((RunStatus::Ok, Vec::new()));
    for slot in &slots {
        if let Err(reason) = view.begin_read(*slot) {
            result = fault(run, None, FaultCode::Internal, reason, schema, out);
            break;
        }
        begun += 1;
    }
    if begun == slots.len() {
        let threshold = view.threshold();
        let (reads, mut writes) = view.split();
        result = run_each(
            served,
            run,
            &reads,
            &mut writes,
            threshold,
            cancel,
            schema,
            out,
        );
    }
    for slot in &slots[..begun] {
        view.end_read(*slot);
    }
    result
}

#[allow(clippy::too_many_arguments)]
fn run_each(
    served: &mut [Served],
    run: &daedalus_wire::Run,
    reads: &Reads<'_>,
    writes: &mut Writes<'_>,
    threshold: usize,
    cancel: &Arc<AtomicBool>,
    schema: &Schema,
    out: &mut impl Write,
) -> io::Result<(RunStatus, Vec<BoundaryValue>)> {
    let mut outputs = Vec::new();
    for id in &run.nodes {
        let Some(node) = served.iter_mut().find(|node| node.id == id) else {
            let reason = format!("no node {id}");
            return fault(run, Some(id), FaultCode::UnknownNode, reason, schema, out);
        };
        match run_node(node, run, reads, cancel, schema, out)? {
            Ok(values) => outputs.extend(values),
            Err(error) => return fault(run, Some(id), error.code, error.reason, schema, out),
        }
    }
    // A large output with a form goes into the run's value buffer, where the host's cell holds
    // it, with its items aligned; the packet carries its place.
    for output in &mut outputs {
        let Ok(ty) = schema.lookup(&output.node, Dir::Output, &output.port) else {
            continue;
        };
        if !schema.has_form(ty) {
            continue;
        }
        if let WireValue::Bytes(bytes) | WireValue::Opaque(bytes) = &output.value {
            if bytes.len() >= threshold
                && let Some((place, room)) = writes.room(bytes.len())
            {
                room.copy_from_slice(bytes);
                output.value = WireValue::Placed(place);
            }
            continue;
        }
        let mut bare = Writer::new();
        if encode_bare(ty, &output.value, schema, &mut bare).is_err() || bare.buf.len() < threshold
        {
            continue;
        }
        let items_at = ty.layout().items_at().unwrap_or(0);
        if let Some((place, room)) = writes.room_for(bare.buf.len(), items_at) {
            room.copy_from_slice(&bare.buf);
            output.value = WireValue::Placed(place);
        }
    }
    Ok((RunStatus::Ok, outputs))
}

/// Sends a node's fault, and gives the run's status after it.
fn fault(
    run: &daedalus_wire::Run,
    node: Option<&String>,
    code: FaultCode,
    reason: String,
    schema: &Schema,
    out: &mut impl Write,
) -> io::Result<(RunStatus, Vec<BoundaryValue>)> {
    let fault = WorkerMsg::Fault(Fault {
        run: run.run,
        node: node.cloned(),
        code,
        reason,
    });
    send(out, &fault, schema).map_err(io::Error::other)?;
    Ok((RunStatus::Fault, Vec::new()))
}

/// Runs one node. Its events go out before anything else, and a panic becomes a fault.
fn run_node(
    node: &mut Served,
    run: &daedalus_wire::Run,
    reads: &Reads<'_>,
    cancel: &Arc<AtomicBool>,
    schema: &Schema,
    out: &mut impl Write,
) -> io::Result<Result<Vec<BoundaryValue>, NodeError>> {
    let mut inputs: Vec<(&'static str, Value<'_>)> = Vec::with_capacity(node.inputs.len());
    for (name, ty) in &node.inputs {
        let Some(entry) = run
            .inputs
            .iter()
            .find(|entry| entry.node == node.id && entry.port == *name)
        else {
            continue;
        };
        let value = match &entry.value {
            WireValue::Placed(place) => match reads.bytes(place) {
                Some(bytes)
                    if matches!(ty, WireType::Scalar(Scalar::Bytes) | WireType::Opaque(_)) =>
                {
                    Value::Bytes(bytes)
                }
                Some(bytes) => Value::Placed { ty, bytes, schema },
                None => {
                    let reason = format!("input {name} is at {place:?}, not in lent memory");
                    return Ok(Err(NodeError::bad_input(reason)));
                }
            },
            value => Value::Wire(value),
        };
        inputs.push((*name, value));
    }
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
            Event::FaultCode { code } => WorkerMsg::NodeCode { run: run.run, code },
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

/// Sends one log line to the host.
fn log(out: &mut impl Write, text: &str, schema: &Schema) -> io::Result<()> {
    let line = WorkerMsg::Log {
        level: LogLevel::Info,
        text: text.to_owned(),
    };
    send(out, &line, schema).map_err(io::Error::other)
}

/// What the reader thread passes on.
enum Packet {
    Bytes(Vec<u8>),
    /// A run in the lent buffer, this many bytes.
    Run(usize),
    /// The host answered the ask: lent memory (`None`), or the reason it lends none.
    Answer(Option<String>),
}

/// The stdin reader. It forwards every packet in order. A run starts a new cancel window, and a
/// cancel that names the run going on sets the cancel flag, so a cancel that arrives right after its
/// run is never lost.
fn read_in_thread(
    current: Arc<AtomicU64>,
    cancel: Arc<AtomicBool>,
    mut inbox: Inbox,
) -> Receiver<Result<Packet, PacketError>> {
    let (tx, rx) = channel();
    thread::spawn(move || {
        let mut buf = Vec::new();
        loop {
            let len = match inbox.next(&mut buf) {
                Ok(Incoming::Packet(len)) => len,
                Ok(Incoming::Run { len, run }) => {
                    current.store(run, Ordering::SeqCst);
                    cancel.store(false, Ordering::SeqCst);
                    if tx.send(Ok(Packet::Run(len))).is_err() {
                        return;
                    }
                    continue;
                }
                Ok(Incoming::Granted) => {
                    if tx.send(Ok(Packet::Answer(None))).is_err() {
                        return;
                    }
                    continue;
                }
                Ok(Incoming::Plain(reason)) => {
                    if tx.send(Ok(Packet::Answer(Some(reason)))).is_err() {
                        return;
                    }
                    continue;
                }
                Err(error) => {
                    let _ = tx.send(Err(error));
                    return;
                }
            };
            buf.truncate(len);
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
            if tx.send(Ok(Packet::Bytes(packet))).is_err() {
                return;
            }
        }
    });
    rx
}
