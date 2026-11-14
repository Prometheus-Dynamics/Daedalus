//! Shared memory between a host and a process worker: the messages that set it up, and the note
//! that a packet is in a shared buffer instead of on the stream.
//!
//! A worker that can map shared memory sends [`Shared::Ask`] right after its hello. The host
//! answers once, before any run: [`Shared::Grant`] with the buffers (their descriptors travel
//! with that packet), or [`Shared::Plain`] with the reason. From then on a packet of at least
//! the grant's threshold is written into a buffer and only [`Shared::InBuffer`] goes on the
//! stream. A host that knows none of this ignores the ask, as it ignores every kind it doesn't
//! know, and a worker that is sent a run before an answer stays on the plain path.
//!
//! These kinds are not part of [`crate::LAYOUT_HASH`]: an older peer never receives them, or
//! ignores them.

use std::string::String;
use std::vec::Vec;

use crate::WireError;
use crate::codec::{Reader, Writer};

/// The kind bytes. Host to worker below 0x40, worker to host from 0x40, as for every message.
pub mod kind {
    /// Host to worker: the answer to an ask.
    pub const ANSWER: u8 = 0x07;
    /// Host to worker: the next packet is in a buffer.
    pub const HOST_IN_BUFFER: u8 = 0x08;
    /// Worker to host: the worker maps shared memory.
    pub const ASK: u8 = 0x49;
    /// Worker to host: the next packet is in a buffer.
    pub const WORKER_IN_BUFFER: u8 = 0x4A;
}

/// The version of this exchange a side speaks.
pub const VERSION: u32 = 1;

/// One buffer of a grant: a slice of one of the descriptors that came with it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Span {
    pub descriptor: u8,
    pub offset: u64,
    pub size: u64,
}

/// One message of the exchange.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Shared {
    /// The worker maps shared memory, and speaks `version` of this exchange.
    Ask { version: u32 },
    /// The host lends buffers. The packet carries the pool page's descriptor, then each
    /// descriptor the spans name, in order. A packet of `threshold` bytes or more goes in a
    /// buffer: the host's in buffer `to_worker`, the worker's in buffer `to_host`. A later grant
    /// replaces this one, with a higher `generation`.
    Grant {
        generation: u32,
        threshold: u32,
        to_worker: u32,
        to_host: u32,
        buffers: Vec<Span>,
    },
    /// The host lends nothing, and says why. The worker stays on the plain path.
    Plain { reason: String },
    /// The next packet is the first `len` bytes of `buffer`, and its kind byte is `kind`, so a
    /// reader can tell a run from anything else without the buffer.
    InBuffer { kind: u8, buffer: u32, len: u32 },
}

/// True for a kind byte this module reads.
pub const fn is_shared(kind: u8) -> bool {
    matches!(
        kind,
        kind::ANSWER | kind::HOST_IN_BUFFER | kind::ASK | kind::WORKER_IN_BUFFER
    )
}

impl Shared {
    /// Writes the message. An `InBuffer` from the host is written with `from_host` set.
    pub fn encode(&self, from_host: bool, out: &mut Writer) -> Result<(), WireError> {
        match self {
            Shared::Ask { version } => {
                out.u8(kind::ASK);
                out.u32(*version);
            }
            Shared::Grant {
                generation,
                threshold,
                to_worker,
                to_host,
                buffers,
            } => {
                out.u8(kind::ANSWER);
                out.u8(1);
                out.u32(*generation);
                out.u32(*threshold);
                out.u32(*to_worker);
                out.u32(*to_host);
                out.len(buffers.len())?;
                for span in buffers {
                    out.u8(span.descriptor);
                    out.u64(span.offset);
                    out.u64(span.size);
                }
            }
            Shared::Plain { reason } => {
                out.u8(kind::ANSWER);
                out.u8(0);
                out.str(reason)?;
            }
            Shared::InBuffer {
                kind: inner,
                buffer,
                len,
            } => {
                out.u8(if from_host {
                    kind::HOST_IN_BUFFER
                } else {
                    kind::WORKER_IN_BUFFER
                });
                out.u8(*inner);
                out.u32(*buffer);
                out.u32(*len);
            }
        }
        Ok(())
    }

    pub fn decode(bytes: &[u8]) -> Result<Shared, WireError> {
        let mut input = Reader::new(bytes);
        let msg = match input.u8()? {
            kind::ASK => Shared::Ask {
                version: input.u32()?,
            },
            kind::ANSWER => match input.u8()? {
                0 => Shared::Plain {
                    reason: input.str()?,
                },
                1 => {
                    let generation = input.u32()?;
                    let threshold = input.u32()?;
                    let to_worker = input.u32()?;
                    let to_host = input.u32()?;
                    let count = input.count()?;
                    let mut buffers = Vec::with_capacity(count);
                    for _ in 0..count {
                        buffers.push(Span {
                            descriptor: input.u8()?,
                            offset: input.u64()?,
                            size: input.u64()?,
                        });
                    }
                    Shared::Grant {
                        generation,
                        threshold,
                        to_worker,
                        to_host,
                        buffers,
                    }
                }
                tag => {
                    return Err(WireError::BadTag {
                        what: "shared answer",
                        tag: u32::from(tag),
                    });
                }
            },
            kind::HOST_IN_BUFFER | kind::WORKER_IN_BUFFER => Shared::InBuffer {
                kind: input.u8()?,
                buffer: input.u32()?,
                len: input.u32()?,
            },
            other => return Err(WireError::UnknownKind(other)),
        };
        input.finish()?;
        Ok(msg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_message_reads_back_as_written() {
        let all = [
            (Shared::Ask { version: VERSION }, false),
            (
                Shared::Grant {
                    generation: 3,
                    threshold: 65536,
                    to_worker: 0,
                    to_host: 1,
                    buffers: vec![
                        Span {
                            descriptor: 0,
                            offset: 0,
                            size: 1 << 20,
                        },
                        Span {
                            descriptor: 0,
                            offset: 1 << 20,
                            size: 1 << 20,
                        },
                    ],
                },
                true,
            ),
            (
                Shared::Plain {
                    reason: "not on this system".into(),
                },
                true,
            ),
            (
                Shared::InBuffer {
                    kind: crate::kind::RUN,
                    buffer: 0,
                    len: 70000,
                },
                true,
            ),
            (
                Shared::InBuffer {
                    kind: crate::kind::OUTPUTS,
                    buffer: 1,
                    len: 90000,
                },
                false,
            ),
        ];
        for (msg, from_host) in all {
            let mut out = Writer::new();
            msg.encode(from_host, &mut out).unwrap();
            assert!(is_shared(out.buf[0]));
            assert_eq!(Shared::decode(&out.buf).unwrap(), msg);
        }
    }

    #[test]
    fn the_kinds_are_on_the_right_side_and_clash_with_none() {
        for host in [kind::ANSWER, kind::HOST_IN_BUFFER] {
            assert!(host < 0x40);
        }
        for worker in [kind::ASK, kind::WORKER_IN_BUFFER] {
            assert!(worker >= 0x40);
        }
        let taken = [
            crate::kind::RUN,
            crate::kind::CANCEL,
            crate::kind::STATE_EXPORT,
            crate::kind::STATE_IMPORT,
            crate::kind::PING,
            crate::kind::SHUTDOWN,
            crate::kind::HELLO,
            crate::kind::OUTPUTS,
            crate::kind::PROGRESS,
            crate::kind::LOG,
            crate::kind::FAULT,
            crate::kind::STATE_VALUE,
            crate::kind::PONG,
            crate::kind::DETAIL,
            crate::kind::NODE_CODE,
        ];
        for kind in taken {
            assert!(!is_shared(kind), "{kind:#x} is taken");
        }
    }
}
