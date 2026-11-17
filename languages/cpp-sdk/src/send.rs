//! The packets the worker writes. A message is encoded with the wire's codec, then framed with a
//! length, and each write is flushed so the host sees it at once.

use std::io::{self, Write};

use daedalus_wire::codec::Writer;
use daedalus_wire::{DEFAULT_CAP, Fault, FaultCode, Schema, WorkerMsg, write_packet};

/// The packet stream the worker writes to.
pub struct Packets {
    out: Box<dyn Write>,
}

impl Packets {
    pub fn new(out: Box<dyn Write>) -> Packets {
        Packets { out }
    }

    /// Encodes and writes one message. A message the codec refuses is not written.
    pub fn send(&mut self, msg: &WorkerMsg, schema: &Schema) -> io::Result<()> {
        let body = encode(msg, schema)?;
        self.write(&body)
    }

    /// The stream the packets go to, for the transport's writes and the lent-memory sends.
    pub fn writer(&mut self) -> &mut dyn Write {
        &mut *self.out
    }

    /// Writes one already-encoded body as a packet.
    pub fn write(&mut self, body: &[u8]) -> io::Result<()> {
        write_packet(&mut self.out, body, DEFAULT_CAP).map_err(io::Error::other)
    }
}

/// A message's body, encoded with the wire's codec.
pub fn encode(msg: &WorkerMsg, schema: &Schema) -> io::Result<Vec<u8>> {
    let mut body = Writer::new();
    msg.encode(schema, &mut body).map_err(io::Error::other)?;
    Ok(body.buf)
}

/// Sends a fault. `node` names the node that failed, or none for the run.
pub fn send_fault(
    packets: &mut Packets,
    run: u64,
    node: Option<&str>,
    code: FaultCode,
    reason: &str,
) {
    let fault = WorkerMsg::Fault(Fault {
        run,
        node: node.map(str::to_owned),
        code,
        reason: reason.to_owned(),
    });
    let _ = packets.send(&fault, &Schema::new());
}
