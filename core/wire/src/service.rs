//! The service protocol: what a client and `daedalusd` say to each other. It uses the worker
//! protocol's packets and value codec, and nothing else of it.
//!
//! A connection starts with one hello each way. After that the client sends requests, each with an
//! id, and may send the next before the last is answered. Every response carries the id of its
//! request. Events arrive between responses, each with the id of the watch it belongs to.
//!
//! # What may be added, and what may not
//!
//! A side built before something was added has to keep working with a side built after, and it
//! must never take a message to mean something it doesn't.
//!
//! - A record gains fields only at its end. Every record nested in a message has its length
//!   before it, and a message is the rest of its packet: a reader takes the fields it knows and
//!   skips what follows. A reader of a newer version reads an added field only when
//!   there are bytes left ([`Reader::more`]), and has a default for when there are not.
//! - That is only for a field the other side may ignore. Something whose absence changes what a
//!   message means gets a name. A side that relies on it puts the name in its hello's `needs`,
//!   and a peer that doesn't know the name refuses the connection, naming it. A side says what
//!   it knows in `knows`, so the other can ask before it sends.
//! - A new op, event kind or error code is its own number. A server answers an op it doesn't
//!   have with `unsupported`. A client gets an event kind it doesn't have as
//!   [`EventBody::Unknown`] and an error code as [`ErrorCode::Other`], with the message.
//! - Anything else is a breaking change: it moves [`SERVICE_LAYOUT_HASH`], and the two sides
//!   refuse each other at the hello, naming both hashes. Nothing is found out mid-session.
//!
//! Graph documents are JSON text in one packet: they are configuration, not the hot path. Values
//! are binary, each with its type, and a value whose type has no codec on both sides is refused.

mod errors;
mod fields;
mod requests;
mod responses;

use std::collections::VecDeque;

pub use errors::{Diagnostic, ErrorCode, ServiceError, Severity};
pub use fields::TypedValue;
pub use requests::{
    CacheAction, EventKinds, InspectTarget, Op, OpKind, PlanForm, PlanOptions, RunId, StartConfig,
    StopMode, SubId, TelemetryLevel, WatchTarget,
};
pub use responses::{
    CacheInfo, CarryReport, EndReason, EventBody, FaultEvent, HostInfo, Inspection, Names,
    PlanSummary, PluginInfo, PortInfo, Reply, RunBrief, RunInfo, RunState, StepSummary,
    TelemetryEvent, TelemetryInfo, TelemetryRow, TraceEntry,
};

use crate::codec::{Reader, Writer};
use crate::{DEFAULT_CAP, Schema, WireError};
use fields::{get_strs, put_strs};

/// The service protocol version a hello must name. There is no range.
pub const SERVICE_VERSION: u32 = 1;
/// Most bytes a session name may have.
pub const SESSION_CAP: usize = 64;
/// The session a request names when the client chose none.
pub const DEFAULT_SESSION: &str = "default";
/// Most events a watch holds for a client that is not reading.
pub const WATCH_QUEUE: usize = 64;
/// How many served requests the trace keeps.
pub const TRACE_RING: usize = 1024;

/// The kind bytes of the service's packets. They share no value with the worker protocol's, so a
/// packet sent to the wrong kind of peer is an unknown kind there.
pub mod kind {
    pub const CLIENT_HELLO: u8 = 0x80;
    pub const REQUEST: u8 = 0x81;
    pub const SERVER_HELLO: u8 = 0xc0;
    pub const RESPONSE: u8 = 0xc1;
    pub const EVENT: u8 = 0xc2;
}

/// What every version of the service layout shares: the version, the value codec's layout, the
/// packet kinds, and that a record has its length before it. Changing any of it changes
/// [`SERVICE_LAYOUT_HASH`], and is a breaking change. What can be added without one (a field at
/// the end of a record, an op, an event kind, an error code) is not counted here.
const LAYOUT: &[u64] = &[
    SERVICE_VERSION as u64,
    crate::LAYOUT_HASH,
    SESSION_CAP as u64,
    kind::CLIENT_HELLO as u64,
    kind::REQUEST as u64,
    kind::SERVER_HELLO as u64,
    kind::RESPONSE as u64,
    kind::EVENT as u64,
    // A record has its length before it.
    0xF2A3,
];

/// What this build knows of the things a peer may rely on. A name is added here with the thing
/// it names, and is never taken away.
pub const KNOWN: &[&str] = &[];

/// The hash of the service layout. It covers the worker layout too, since values use that codec.
pub const SERVICE_LAYOUT_HASH: u64 = daedalus_abi::layout_hash(LAYOUT);

/// What a server offers.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Capabilities {
    /// The ops the server answers. One without plugins leaves out `load_plugin`.
    pub ops: Vec<OpKind>,
    /// The type keys the server has value codecs for.
    pub codecs: Vec<String>,
    /// Most runs the server holds at once. Zero is no limit.
    pub max_runs: u32,
}

impl Capabilities {
    /// Every op, and no codecs.
    pub fn all_ops() -> Capabilities {
        Capabilities {
            ops: OpKind::ALL.to_vec(),
            ..Capabilities::default()
        }
    }

    pub fn serves(&self, op: OpKind) -> bool {
        self.ops.contains(&op)
    }
}

/// The first packet each way. A client sends one with empty capabilities, and the server answers
/// with its own.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServiceHello {
    pub version: u32,
    pub layout: u64,
    /// The largest packet this side reads. Each side sends nothing larger than the other's cap.
    pub packet_cap: u32,
    pub capabilities: Capabilities,
    /// What this side knows, of the things a peer may rely on: see [`KNOWN`].
    pub knows: Vec<String>,
    /// What this side relies on its peer knowing. A peer that doesn't know one refuses.
    pub needs: Vec<String>,
}

impl ServiceHello {
    /// A client's hello.
    pub fn client() -> ServiceHello {
        ServiceHello {
            version: SERVICE_VERSION,
            layout: SERVICE_LAYOUT_HASH,
            packet_cap: DEFAULT_CAP,
            capabilities: Capabilities::default(),
            knows: KNOWN.iter().map(|name| (*name).to_owned()).collect(),
            needs: Vec::new(),
        }
    }

    /// A server's hello.
    pub fn server(capabilities: Capabilities) -> ServiceHello {
        ServiceHello {
            capabilities,
            ..ServiceHello::client()
        }
    }

    /// Whether the peer that sent this hello knows `name`. A side asks before it sends something
    /// that an older peer would misread.
    pub fn knows(&self, name: &str) -> bool {
        self.knows.iter().any(|known| known == name)
    }

    /// Refuses a peer with another protocol version or layout, naming both values, and one that
    /// relies on something this side doesn't know, naming it.
    pub fn check(&self) -> Result<(), WireError> {
        if self.version != SERVICE_VERSION {
            return Err(WireError::Protocol {
                expected: SERVICE_VERSION,
                found: self.version,
            });
        }
        if self.layout != SERVICE_LAYOUT_HASH {
            return Err(WireError::Layout {
                expected: SERVICE_LAYOUT_HASH,
                found: self.layout,
            });
        }
        if let Some(name) = self
            .needs
            .iter()
            .find(|name| !KNOWN.contains(&name.as_str()))
        {
            return Err(WireError::Needs { name: name.clone() });
        }
        Ok(())
    }

    /// The codec table both sides use after this hello: the peer's codec keys.
    pub fn codecs(&self) -> Schema {
        let mut schema = Schema::new();
        for key in &self.capabilities.codecs {
            schema.codec(key);
        }
        schema
    }

    fn encode(&self, out: &mut Writer) -> Result<(), WireError> {
        out.u32(self.version);
        out.u64(self.layout);
        out.u32(self.packet_cap);
        out.len(self.capabilities.ops.len())?;
        for op in &self.capabilities.ops {
            out.u8(*op as u8);
        }
        put_strs(out, &self.capabilities.codecs)?;
        out.u32(self.capabilities.max_runs);
        put_strs(out, &self.knows)?;
        put_strs(out, &self.needs)
    }

    fn decode(input: &mut Reader<'_>) -> Result<ServiceHello, WireError> {
        let version = input.u32()?;
        let layout = input.u64()?;
        let packet_cap = input.u32()?;
        let count = input.count()?;
        let mut ops = Vec::with_capacity(count);
        for _ in 0..count {
            // A newer server serves ops this side doesn't have. They are no use to it, and no
            // reason to refuse.
            let op = OpKind::named(input.u8()?);
            if op != OpKind::Other {
                ops.push(op);
            }
        }
        Ok(ServiceHello {
            version,
            layout,
            packet_cap,
            capabilities: Capabilities {
                ops,
                codecs: get_strs(input)?,
                max_runs: input.u32()?,
            },
            knows: get_strs(input)?,
            needs: get_strs(input)?,
        })
    }
}

/// One request in its envelope.
#[derive(Clone, Debug, PartialEq)]
pub struct Request {
    pub id: u64,
    /// The namespace the request's graph names are in. At most [`SESSION_CAP`] bytes.
    pub session: String,
    pub op: Op,
}

impl Request {
    /// A request in the default session.
    pub fn new(id: u64, op: Op) -> Request {
        Request {
            id,
            session: DEFAULT_SESSION.to_owned(),
            op,
        }
    }
}

/// What a client sends.
#[derive(Clone, Debug, PartialEq)]
pub enum ClientMsg {
    Hello(ServiceHello),
    Request(Request),
}

impl ClientMsg {
    pub fn encode(&self, codecs: &Schema) -> Result<Vec<u8>, WireError> {
        self.encode_into(Writer::new(), codecs)
    }

    /// [`ClientMsg::encode`] into a writer of the caller's, which a test pads.
    #[doc(hidden)]
    pub fn encode_into(&self, mut out: Writer, codecs: &Schema) -> Result<Vec<u8>, WireError> {
        match self {
            ClientMsg::Hello(hello) => {
                out.u8(kind::CLIENT_HELLO);
                hello.encode(&mut out)?;
            }
            ClientMsg::Request(request) => {
                if request.session.len() > SESSION_CAP {
                    return Err(WireError::TooLong(request.session.len()));
                }
                out.u8(kind::REQUEST);
                out.u64(request.id);
                out.str(&request.session)?;
                request.op.encode(&mut out, codecs)?;
            }
        }
        Ok(out.buf)
    }

    pub fn decode(body: &[u8], codecs: &Schema) -> Result<ClientMsg, WireError> {
        let mut input = Reader::new(body);
        let message = match input.u8()? {
            kind::CLIENT_HELLO => ClientMsg::Hello(ServiceHello::decode(&mut input)?),
            kind::REQUEST => {
                let id = input.u64()?;
                let session = input.str()?;
                if session.len() > SESSION_CAP {
                    return Err(WireError::TooLong(session.len()));
                }
                ClientMsg::Request(Request {
                    id,
                    session,
                    op: Op::decode(&mut input, codecs)?,
                })
            }
            other => return Err(WireError::UnknownKind(other)),
        };
        // What is left is fields from a later version, at the end of the message.
        Ok(message)
    }

    /// The id of a request whose body could not be decoded, when the envelope got that far. A
    /// server answers that id with the error, so the client's pipeline stays matched.
    pub fn request_id(body: &[u8]) -> Option<u64> {
        let mut input = Reader::new(body);
        match input.u8() {
            Ok(kind::REQUEST) => input.u64().ok(),
            _ => None,
        }
    }
}

/// The answer to one request.
#[derive(Clone, Debug, PartialEq)]
pub struct Response {
    /// The id of the request this answers.
    pub id: u64,
    pub result: Result<Reply, ServiceError>,
}

/// One event of one watch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Event {
    pub sub: SubId,
    pub body: EventBody,
}

/// What a server sends.
#[derive(Clone, Debug, PartialEq)]
pub enum ServerMsg {
    Hello(ServiceHello),
    Response(Response),
    Event(Event),
}

impl ServerMsg {
    pub fn encode(&self, codecs: &Schema) -> Result<Vec<u8>, WireError> {
        self.encode_into(Writer::new(), codecs)
    }

    /// [`ServerMsg::encode`] into a writer of the caller's, which a test pads.
    #[doc(hidden)]
    pub fn encode_into(&self, mut out: Writer, codecs: &Schema) -> Result<Vec<u8>, WireError> {
        match self {
            ServerMsg::Hello(hello) => {
                out.u8(kind::SERVER_HELLO);
                hello.encode(&mut out)?;
            }
            ServerMsg::Response(response) => {
                out.u8(kind::RESPONSE);
                out.u64(response.id);
                match &response.result {
                    Ok(reply) => {
                        out.u8(0);
                        reply.encode(&mut out, codecs)?;
                    }
                    Err(error) => {
                        out.u8(1);
                        error.encode(&mut out)?;
                    }
                }
            }
            ServerMsg::Event(event) => {
                out.u8(kind::EVENT);
                out.u64(event.sub);
                event.body.encode(&mut out)?;
            }
        }
        Ok(out.buf)
    }

    pub fn decode(body: &[u8], codecs: &Schema) -> Result<ServerMsg, WireError> {
        let mut input = Reader::new(body);
        let message = match input.u8()? {
            kind::SERVER_HELLO => ServerMsg::Hello(ServiceHello::decode(&mut input)?),
            kind::RESPONSE => {
                let id = input.u64()?;
                let result = match fields::flag(&mut input, "result flag")? {
                    false => Ok(Reply::decode(&mut input, codecs)?),
                    true => Err(ServiceError::decode(&mut input)?),
                };
                ServerMsg::Response(Response { id, result })
            }
            kind::EVENT => ServerMsg::Event(Event {
                sub: input.u64()?,
                body: EventBody::decode(&mut input)?,
            }),
            other => return Err(WireError::UnknownKind(other)),
        };
        // What is left is fields from a later version, at the end of the message.
        Ok(message)
    }
}

/// The events waiting for one watcher. It holds at most [`WATCH_QUEUE`]: a push to a full queue
/// drops the oldest and counts it, and the next pop reports the count as `lagged` before anything
/// else. A push never waits, so a watcher that does not read never holds up a step.
#[derive(Debug)]
pub struct WatchQueue {
    sub: SubId,
    events: VecDeque<EventBody>,
    dropped: u64,
}

impl WatchQueue {
    pub fn new(sub: SubId) -> WatchQueue {
        WatchQueue {
            sub,
            events: VecDeque::with_capacity(WATCH_QUEUE),
            dropped: 0,
        }
    }

    pub fn sub(&self) -> SubId {
        self.sub
    }

    pub fn len(&self) -> usize {
        self.events.len()
    }

    pub fn is_empty(&self) -> bool {
        self.events.is_empty() && self.dropped == 0
    }

    pub fn push(&mut self, body: EventBody) {
        if self.events.len() == WATCH_QUEUE {
            self.events.pop_front();
            self.dropped += 1;
        }
        self.events.push_back(body);
    }

    /// The next event to send: the count of dropped events first, if there are any.
    pub fn pop(&mut self) -> Option<Event> {
        let body = if self.dropped > 0 {
            let dropped = std::mem::take(&mut self.dropped);
            EventBody::Lagged { dropped }
        } else {
            self.events.pop_front()?
        };
        Some(Event {
            sub: self.sub,
            body,
        })
    }
}

/// The last [`TRACE_RING`] served requests.
#[derive(Debug, Default)]
pub struct TraceRing {
    entries: VecDeque<TraceEntry>,
    next_seq: u64,
}

impl TraceRing {
    pub fn new() -> TraceRing {
        TraceRing::default()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Records one served request and gives it the next sequence number.
    pub fn record(&mut self, request: &Request, error: Option<ErrorCode>) {
        if self.entries.len() == TRACE_RING {
            self.entries.pop_front();
        }
        self.entries.push_back(TraceEntry {
            seq: self.next_seq,
            id: request.id,
            session: request.session.clone(),
            op: request.op.kind(),
            target: request.op.target(),
            error,
        });
        self.next_seq += 1;
    }

    /// The newest `limit` entries, oldest first. A limit of zero is all of them.
    pub fn recent(&self, limit: u32) -> Vec<TraceEntry> {
        let keep = match limit {
            0 => self.entries.len(),
            limit => (limit as usize).min(self.entries.len()),
        };
        self.entries
            .iter()
            .skip(self.entries.len() - keep)
            .cloned()
            .collect()
    }
}
