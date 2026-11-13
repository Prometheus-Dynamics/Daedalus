//! Typed errors. Decoding never panics: every malformed input is one of these.

use core::fmt;

/// A message, value or hello that cannot be encoded or decoded.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WireError {
    /// The input ended before a field was complete.
    Truncated { needed: usize, remaining: usize },
    /// A string field is not UTF-8.
    InvalidUtf8,
    /// A message kind this protocol does not define.
    UnknownKind(u8),
    /// A tag (type, value, status, fault code, level) this protocol does not define.
    BadTag { what: &'static str, tag: u32 },
    /// Bytes left over after a complete message.
    TrailingBytes(usize),
    /// Nesting deeper than the decoder allows.
    TooDeep,
    /// A count larger than the bytes left could hold.
    CountTooLarge { count: u32, remaining: usize },
    /// A value does not have the shape its type says.
    ValueShape {
        expected: &'static str,
        found: &'static str,
    },
    /// A type key with no codec on this side. Such values are refused.
    NoCodec(String),
    /// A port or state that the schema does not know.
    UnknownPort { node: String, port: String },
    /// A text or byte field too long for its length prefix.
    TooLong(usize),
    /// The hello names another protocol version.
    Protocol { expected: u32, found: u32 },
    /// The hello names another layout hash.
    Layout { expected: u64, found: u64 },
    /// A request for an op this side doesn't have: the other side is newer.
    UnknownOp(u8),
    /// The other side relies on something this side doesn't know, named in its hello.
    Needs { name: String },
}

impl fmt::Display for WireError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WireError::Truncated { needed, remaining } => {
                write!(
                    f,
                    "input ends early: needs {needed} bytes, {remaining} left"
                )
            }
            WireError::InvalidUtf8 => f.write_str("a string field is not UTF-8"),
            WireError::UnknownKind(kind) => write!(f, "unknown message kind {kind:#04x}"),
            WireError::BadTag { what, tag } => write!(f, "unknown {what} tag {tag}"),
            WireError::TrailingBytes(count) => write!(f, "{count} bytes after the message"),
            WireError::TooDeep => f.write_str("nesting is deeper than the decoder allows"),
            WireError::CountTooLarge { count, remaining } => {
                write!(f, "count {count} cannot fit in {remaining} bytes")
            }
            WireError::ValueShape { expected, found } => {
                write!(f, "expected a {expected} value, found a {found}")
            }
            WireError::NoCodec(key) => write!(f, "type {key} has no codec on this side"),
            WireError::UnknownPort { node, port } => {
                write!(f, "node {node} has no port {port} in the schema")
            }
            WireError::TooLong(len) => write!(f, "{len} bytes is too long for a length prefix"),
            WireError::Protocol { expected, found } => {
                write!(f, "protocol version {found}, this side speaks {expected}")
            }
            WireError::Layout { expected, found } => {
                write!(
                    f,
                    "the other side was built for another wire layout: its layout hash is \
                     {found:#018x}, this side has {expected:#018x}. One of the two has to be \
                     updated"
                )
            }
            WireError::UnknownOp(op) => write!(
                f,
                "op {op} is not one this side has: the other side is newer"
            ),
            WireError::Needs { name } => write!(
                f,
                "the other side needs `{name}`, which this side doesn't have: this side has to \
                 be updated"
            ),
        }
    }
}

impl std::error::Error for WireError {}

/// A packet that cannot be read from or written to a channel.
#[derive(Debug)]
pub enum PacketError {
    /// The channel closed cleanly between packets.
    Closed,
    /// The channel closed inside a packet.
    Truncated,
    /// The length prefix is over the cap. Nothing was allocated for it.
    TooLarge { len: u32, cap: u32 },
    /// Any other read or write failure.
    Io(std::io::Error),
}

impl fmt::Display for PacketError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PacketError::Closed => f.write_str("channel closed"),
            PacketError::Truncated => f.write_str("channel closed inside a packet"),
            PacketError::TooLarge { len, cap } => {
                write!(f, "packet of {len} bytes is over the cap of {cap}")
            }
            PacketError::Io(error) => write!(f, "channel error: {error}"),
        }
    }
}

impl std::error::Error for PacketError {}
