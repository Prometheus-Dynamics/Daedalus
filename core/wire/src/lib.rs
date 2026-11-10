//! The worker wire protocol. A worker is a child process that serves nodes for a host. The two
//! talk in packets: a length, then a body. Values are typed by the port types the worker declared
//! in its hello, so nothing on the wire names a value's type.
//!
//! The decoders are total: every input gives a message or a typed error, never a panic, and no
//! allocation is made for a length the cap refuses. The protocol carries no JSON and no base64,
//! and it has no version range: a hello either names version 1 and this layout, or it is refused.

#![forbid(unsafe_code)]

pub mod codec;
mod error;
mod hello;
mod packet;
mod schema;
mod value;
mod worker;
pub use worker::kind;

pub use error::{PacketError, WireError};
pub use hello::{Declaration, Hello, PortSpec};
pub use packet::{DEFAULT_CAP, read_packet, write_packet};
pub use schema::{Dir, Schema};
pub use value::{MAX_DEPTH, WireType, WireValue, decode_value, encode_value};
pub use worker::{
    BoundaryValue, Fault, FaultCode, HostMsg, LogLevel, Outputs, Progress, REASON_CAP, Run,
    RunStatus, WorkerMsg, truncate_reason,
};

/// The protocol version a hello must name. There is no range.
pub const PROTOCOL_VERSION: u32 = 1;

/// The kind bytes and tags a layout is made of. Changing any of them changes [`LAYOUT_HASH`].
const LAYOUT: &[u64] = &[
    PROTOCOL_VERSION as u64,
    REASON_CAP as u64,
    DEFAULT_CAP as u64,
    MAX_DEPTH as u64,
    0x01,
    0x02,
    0x03,
    0x04,
    0x05,
    0x06, // host messages
    0x40,
    0x41,
    0x42,
    0x43,
    0x44,
    0x45,
    0x46, // worker messages
    // The scalar count and the type tag count.
    16,
    8,
];

/// The hash of [`LAYOUT`]. A hello carries it, and the host refuses a worker that has another.
pub const LAYOUT_HASH: u64 = daedalus_abi::layout_hash(LAYOUT);
