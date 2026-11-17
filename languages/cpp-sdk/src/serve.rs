//! The worker loop: the hello, then one packet at a time until the host shuts the worker down or
//! closes the pipe. Runs are answered in `run.rs`. A state export or import is answered here.

use std::io::{self, Read, Write};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use daedalus_wire::codec::Reader;
use daedalus_wire::{
    DEFAULT_CAP, Declaration, FaultCode, Hello, PortSpec, Schema, WireType, WireValue, WorkerMsg,
    decode_value, kind,
};
use daedalus_worker_io::{Arrival, Transport};

use crate::convert::{Refused, c_text, opaque_keys, ports, wire_type};
use crate::ffi::{DdNode, DdNodeFn, MAX_NODES};
use crate::run::handle_run;
use crate::send::{Packets, encode, send_fault};

/// A node the worker serves: its id, its run function, and the wire types its ports and state have.
pub struct Served {
    pub id: String,
    pub run: Option<DdNodeFn>,
    pub inputs: Vec<(String, WireType)>,
    pub outputs: Vec<(String, WireType)>,
    pub state: Option<WireType>,
}

/// A node's stored state: the value, or a value the node set that does not fit its type. The C
/// loop kept that too, and failed the export of it.
pub enum Slot {
    Value(WireValue),
    Invalid,
}

/// What the loop keeps between runs.
pub struct Worker {
    pub served: Vec<Served>,
    /// Each node's stored state, by node index.
    pub slots: Vec<Option<Slot>>,
    /// The schema the host's values and the worker's values are read and written with. It gains the
    /// host's agreed features when the host lends memory.
    pub schema: Schema,
    /// The host's cancel flag: set while the run going on is cancelled.
    pub cancel: Arc<AtomicBool>,
}

impl Worker {
    /// The index of the node with this id, the first one if an id repeats.
    pub fn find_node(&self, id: &[u8]) -> Option<usize> {
        self.served.iter().position(|node| node.id.as_bytes() == id)
    }

    /// The node and input port with these names.
    pub fn find_port(&self, node: &[u8], port: &[u8]) -> Option<(usize, usize)> {
        let index = self.find_node(node)?;
        let port_index = self.served[index]
            .inputs
            .iter()
            .position(|(name, _)| name.as_bytes() == port)?;
        Some((index, port_index))
    }

    /// Answers a state export: the node's state, as a state value.
    pub fn export(&self, packets: &mut Packets, message: &[u8]) {
        let mut input = Reader::new(message);
        let (Ok(id), Ok(())) = (input.bytes(), input.finish()) else {
            send_fault(
                packets,
                0,
                None,
                FaultCode::MalformedPacket,
                "a state export names one node",
            );
            return;
        };
        let named = String::from_utf8_lossy(&id).into_owned();
        let Some(index) = self.find_node(&id) else {
            send_fault(
                packets,
                0,
                Some(named.as_str()),
                FaultCode::BadInput,
                "the node has no state to export",
            );
            return;
        };
        let node = &self.served[index];
        let msg = match (&node.state, &self.slots[index]) {
            (Some(_), Some(Slot::Value(value))) => WorkerMsg::StateValue {
                node: node.id.clone(),
                value: value.clone(),
            },
            (Some(_), Some(Slot::Invalid)) => {
                send_fault(
                    packets,
                    0,
                    Some(named.as_str()),
                    FaultCode::Internal,
                    "the node's state does not fit its type",
                );
                return;
            }
            _ => {
                send_fault(
                    packets,
                    0,
                    Some(named.as_str()),
                    FaultCode::BadInput,
                    "the node has no state to export",
                );
                return;
            }
        };
        match encode(&msg, &self.schema) {
            Ok(body) => {
                let _ = packets.write(&body);
            }
            Err(_) => {
                send_fault(
                    packets,
                    0,
                    Some(named.as_str()),
                    FaultCode::Internal,
                    "the node's state does not fit its type",
                );
            }
        }
    }

    /// Answers a state import: the host's value becomes the node's state, which the next run starts
    /// from. A value that does not match the state type leaves the stored state as it was.
    pub fn import(&mut self, packets: &mut Packets, message: &[u8]) {
        let mut input = Reader::new(message);
        let Ok(id) = input.bytes() else {
            send_fault(
                packets,
                0,
                None,
                FaultCode::MalformedPacket,
                "a state import names no node",
            );
            return;
        };
        let named = String::from_utf8_lossy(&id).into_owned();
        let Some(index) = self.find_node(&id) else {
            send_fault(
                packets,
                0,
                Some(named.as_str()),
                FaultCode::BadInput,
                "the node has no state to import",
            );
            return;
        };
        let Some(ty) = self.served[index].state.as_ref() else {
            send_fault(
                packets,
                0,
                Some(named.as_str()),
                FaultCode::BadInput,
                "the node has no state to import",
            );
            return;
        };
        let value = match decode_value(ty, &mut input, &self.schema) {
            Ok(value) if input.finish().is_ok() => value,
            _ => {
                send_fault(
                    packets,
                    0,
                    Some(named.as_str()),
                    FaultCode::MalformedPacket,
                    "the state value does not match the node's state type",
                );
                return;
            }
        };
        self.slots[index] = Some(Slot::Value(value));
    }
}

/// Serves the worker protocol on `input`, writing its packets to `out`, until the host shuts the
/// worker down or the input ends. The input is read with plain packets: no shared memory is asked
/// for, so this is the loop a test serves over a buffer. Returns an error when the hello cannot be
/// written, when a packet cannot be read, or when the table is not one the SDK can serve.
pub fn serve_io(
    nodes: &[DdNode],
    input: Box<dyn Read + Send>,
    out: Box<dyn Write>,
    worker_name: &str,
) -> io::Result<()> {
    serve_with(nodes, worker_name, out, |_| Ok(Transport::plain(input)))
}

/// Serves the worker protocol on the worker's stdin and stdout, with the shared memory the host
/// lends it. The packets go to `out`, the worker's packet stream.
pub fn serve_stdio(nodes: &[DdNode], worker_name: &str, out: Box<dyn Write>) -> io::Result<()> {
    serve_with(nodes, worker_name, out, |packets| {
        Transport::stdio(&mut packets.writer())
    })
}

/// The loop: the hello, then one message at a time until the host shuts the worker down or closes
/// the pipe. `start` makes the transport, after the hello is sent.
fn serve_with(
    nodes: &[DdNode],
    worker_name: &str,
    out: Box<dyn Write>,
    start: impl FnOnce(&mut Packets) -> io::Result<Transport>,
) -> io::Result<()> {
    let served = served(nodes)?;
    let hello = hello(&served, worker_name);
    let mut keys = Vec::new();
    for node in &served {
        for (_, ty) in node.inputs.iter().chain(&node.outputs) {
            opaque_keys(ty, &mut keys);
        }
        if let Some(ty) = &node.state {
            opaque_keys(ty, &mut keys);
        }
    }
    let codecs: Vec<&str> = keys.iter().map(String::as_str).collect();
    let schema = Schema::from_hello(&hello, &codecs);
    let mut packets = Packets::new(out);
    packets.send(&WorkerMsg::Hello(hello), &Schema::new())?;
    let mut transport = start(&mut packets)?;
    let mut worker = Worker {
        slots: (0..served.len()).map(|_| None).collect(),
        served,
        schema,
        cancel: transport.cancel_flag(),
    };
    while let Some(arrival) = transport.next(&mut packets.writer(), <[u8]>::to_vec)? {
        let body = match arrival {
            Arrival::Granted => {
                worker.schema.set_features(transport.lent().features());
                continue;
            }
            Arrival::Message(body) => body,
        };
        let Some((&tag, message)) = body.split_first() else {
            continue;
        };
        match tag {
            kind::PING => {
                let _ = packets.send(&WorkerMsg::Pong, &Schema::new());
            }
            kind::SHUTDOWN => return Ok(()),
            kind::RUN => handle_run(&mut worker, &mut packets, transport.lent(), message),
            kind::STATE_EXPORT => worker.export(&mut packets, message),
            kind::STATE_IMPORT => worker.import(&mut packets, message),
            // A cancel between runs has nothing to stop.
            kind::CANCEL => {}
            _ => send_fault(
                &mut packets,
                0,
                None,
                FaultCode::MalformedPacket,
                "unknown message kind",
            ),
        }
    }
    Ok(())
}

/// The served nodes, from the C table.
fn served(nodes: &[DdNode]) -> io::Result<Vec<Served>> {
    if nodes.len() > MAX_NODES {
        return Err(io::Error::other("a worker serves at most 64 nodes"));
    }
    nodes.iter().map(served_node).collect()
}

/// One node of the C table, read into its wire types.
fn served_node(node: &DdNode) -> io::Result<Served> {
    let refused = |_: Refused| io::Error::other("a node names a type the wire cannot carry");
    // SAFETY: the table is the one the caller passed in, which is valid for the whole call.
    let id = unsafe { c_text(node.id) };
    // SAFETY: as above, for the node's input ports.
    let inputs = unsafe { ports(node.inputs, node.input_count) }.map_err(refused)?;
    // SAFETY: as above, for the node's output ports.
    let outputs = unsafe { ports(node.outputs, node.output_count) }.map_err(refused)?;
    let state = if node.state.is_null() {
        None
    } else {
        // SAFETY: as above, for the node's state type.
        Some(unsafe { wire_type(node.state, 0) }.map_err(refused)?)
    };
    Ok(Served {
        id,
        run: node.run,
        inputs,
        outputs,
        state,
    })
}

/// The hello: every node's declaration.
fn hello(served: &[Served], worker_name: &str) -> Hello {
    let nodes = served
        .iter()
        .map(|node| Declaration {
            id: node.id.clone(),
            inputs: node
                .inputs
                .iter()
                .map(|(name, ty)| PortSpec::new(name, ty.clone()))
                .collect(),
            outputs: node
                .outputs
                .iter()
                .map(|(name, ty)| PortSpec::new(name, ty.clone()))
                .collect(),
            config: Vec::new(),
            state: node.state.clone(),
            capabilities: Vec::new(),
        })
        .collect();
    Hello::new(worker_name, DEFAULT_CAP, nodes)
}
