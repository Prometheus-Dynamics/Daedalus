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
//! A worker that can also take values by reference sends [`Shared::Want`] after its ask. A host
//! that can answers each grant with [`Shared::Agree`], naming the buffers the worker writes
//! output values into. From then on the host sends a [`Shared::RunNote`] before each run, and a
//! `bytes` or opaque value can be a [`Place`] instead of its bytes: a range of a lent buffer, or of
//! a descriptor sent once and named by its slot after. An older host never answers a want, so the
//! worker keeps to the exchange above; an older worker never sends one, so it is never sent a
//! place.
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
    /// Host to worker: values by reference, agreed, and the buffers output values go in.
    pub const AGREE: u8 = 0x09;
    /// Host to worker: what the next run needs, sent just before it.
    pub const RUN_NOTE: u8 = 0x0A;
    /// Worker to host: the worker maps shared memory.
    pub const ASK: u8 = 0x49;
    /// Worker to host: the next packet is in a buffer.
    pub const WORKER_IN_BUFFER: u8 = 0x4A;
    /// Worker to host: what the worker can do beyond the plain exchange.
    pub const WANT: u8 = 0x4B;
}

/// A [`Shared::Want`] feature: the worker takes and gives values by reference.
pub const BY_REFERENCE: u32 = 1;

/// With [`BY_REFERENCE`]: a port's whole value of any type but a fixed-size scalar has a form
/// byte, so a large list, struct or string can be a place too. A place names the value's bytes as
/// the host's cell holds them: a string's or byte field's own bytes, and a compound value's wire
/// form, which is how a host holds it.
pub const ANY_TYPE: u32 = 2;

/// Whether a port's whole value of type `ty` starts with a form byte between peers that agreed
/// `features`, and so can be a place. With [`BY_REFERENCE`], a `bytes` or opaque value does; with
/// [`ANY_TYPE`] as well, any value but a fixed-size scalar, whose few bytes always cost less
/// inline. Decided by the type's shape, never by its name.
pub fn has_form(features: u32, ty: &crate::WireType) -> bool {
    use crate::WireType;
    use daedalus_types::Scalar;
    if features & BY_REFERENCE == 0 {
        return false;
    }
    let any = features & ANY_TYPE != 0;
    match ty {
        WireType::Scalar(Scalar::Bytes) | WireType::Opaque(_) => true,
        WireType::Scalar(Scalar::String) => any,
        WireType::Scalar(_) => false,
        _ => any,
    }
}

/// Where a value's bytes are, when they don't travel in the packet.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum At {
    /// A buffer of the pool the host lent.
    Lent(u32),
    /// A descriptor the host sent with a run note, by the slot it was given.
    Fd(u32),
}

/// A value by reference: `len` bytes from `offset` in what [`At`] names.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Place {
    pub at: At,
    pub offset: u64,
    pub len: u64,
}

/// The form byte of a port's value that has a form (see [`has_form`]) between peers that agreed
/// values by reference.
pub mod form {
    /// The bytes follow, as without the agreement.
    pub const INLINE: u8 = 0;
    /// A range of a lent buffer.
    pub const LENT: u8 = 1;
    /// A range of a descriptor.
    pub const FD: u8 = 2;
}

impl Place {
    /// Writes the place with its form byte.
    pub fn encode(&self, out: &mut Writer) {
        match self.at {
            At::Lent(buffer) => {
                out.u8(form::LENT);
                out.u32(buffer);
            }
            At::Fd(slot) => {
                out.u8(form::FD);
                out.u32(slot);
            }
        }
        out.u64(self.offset);
        out.u64(self.len);
    }

    /// Reads a place whose form byte, `form`, was read already.
    pub fn decode(form: u8, input: &mut Reader<'_>) -> Result<Place, WireError> {
        let at = match form {
            form::LENT => At::Lent(input.u32()?),
            form::FD => At::Fd(input.u32()?),
            tag => {
                return Err(WireError::BadTag {
                    what: "value form",
                    tag: u32::from(tag),
                });
            }
        };
        Ok(Place {
            at,
            offset: input.u64()?,
            len: input.u64()?,
        })
    }
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
    /// The worker can do these [features](BY_REFERENCE) beyond the plain exchange. Sent after
    /// the ask, before the pong.
    Want { features: u32 },
    /// The host agrees to these features for the grant just sent. An output value goes in one of
    /// `values`, the one the run's note names.
    Agree { features: u32, values: Vec<u32> },
    /// The next run, from a host that agreed values by reference. Its packet is `len` bytes at
    /// `offset` of the host's buffer when `packet` is set, and otherwise the rest of the frame
    /// this note starts, so a run and its note are one write: see [`Shared::decode_note`]. The
    /// worker writes output values into buffer `out`, or sends them in the packet when there is
    /// none. Descriptors came with this note for each slot in `attach`, in order: a later run
    /// names them by slot. The worker lets go of each slot in `forget`.
    RunNote {
        packet: Option<(u64, u32)>,
        out: Option<u32>,
        attach: Vec<u32>,
        forget: Vec<u32>,
    },
}

/// True for a kind byte this module reads.
pub const fn is_shared(kind: u8) -> bool {
    matches!(
        kind,
        kind::ANSWER
            | kind::HOST_IN_BUFFER
            | kind::AGREE
            | kind::RUN_NOTE
            | kind::ASK
            | kind::WORKER_IN_BUFFER
            | kind::WANT
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
            Shared::Want { features } => {
                out.u8(kind::WANT);
                out.u32(*features);
            }
            Shared::Agree { features, values } => {
                out.u8(kind::AGREE);
                out.u32(*features);
                out.len(values.len())?;
                for value in values {
                    out.u32(*value);
                }
            }
            Shared::RunNote {
                packet,
                out: buffer,
                attach,
                forget,
            } => {
                out.u8(kind::RUN_NOTE);
                match packet {
                    Some((offset, len)) => {
                        out.u8(1);
                        out.u64(*offset);
                        out.u32(*len);
                    }
                    None => out.u8(0),
                }
                out.u32(buffer.unwrap_or(u32::MAX));
                out.len(attach.len())?;
                for slot in attach {
                    out.u32(*slot);
                }
                out.len(forget.len())?;
                for slot in forget {
                    out.u32(*slot);
                }
            }
        }
        Ok(())
    }

    pub fn decode(bytes: &[u8]) -> Result<Shared, WireError> {
        let mut input = Reader::new(bytes);
        let msg = Shared::read(&mut input)?;
        input.finish()?;
        Ok(msg)
    }

    /// Reads a run note at the start of a frame, and gives the rest of the frame: the run's
    /// packet, when the note says it is not in the host's buffer.
    pub fn decode_note(bytes: &[u8]) -> Result<(Shared, &[u8]), WireError> {
        let mut input = Reader::new(bytes);
        let msg = Shared::read(&mut input)?;
        if !matches!(msg, Shared::RunNote { .. }) {
            return Err(WireError::UnknownKind(bytes[0]));
        }
        let at = input.pos();
        Ok((msg, &bytes[at..]))
    }

    fn read(input: &mut Reader<'_>) -> Result<Shared, WireError> {
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
            kind::WANT => Shared::Want {
                features: input.u32()?,
            },
            kind::AGREE => {
                let features = input.u32()?;
                let count = input.count()?;
                let mut values = Vec::with_capacity(count);
                for _ in 0..count {
                    values.push(input.u32()?);
                }
                Shared::Agree { features, values }
            }
            kind::RUN_NOTE => {
                let packet = match input.u8()? {
                    0 => None,
                    1 => Some((input.u64()?, input.u32()?)),
                    tag => {
                        return Err(WireError::BadTag {
                            what: "run note packet",
                            tag: u32::from(tag),
                        });
                    }
                };
                let out = Some(input.u32()?).filter(|buffer| *buffer != u32::MAX);
                let slots = |input: &mut Reader<'_>| -> Result<Vec<u32>, WireError> {
                    let count = input.count()?;
                    let mut all = Vec::with_capacity(count);
                    for _ in 0..count {
                        all.push(input.u32()?);
                    }
                    Ok(all)
                };
                let attach = slots(input)?;
                let forget = slots(input)?;
                Shared::RunNote {
                    packet,
                    out,
                    attach,
                    forget,
                }
            }
            other => return Err(WireError::UnknownKind(other)),
        };
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
            (
                Shared::Want {
                    features: BY_REFERENCE,
                },
                false,
            ),
            (
                Shared::Agree {
                    features: BY_REFERENCE,
                    values: vec![2, 3, 4, 5],
                },
                true,
            ),
            (
                Shared::RunNote {
                    packet: Some((4096, 70000)),
                    out: Some(3),
                    attach: vec![0, 1],
                    forget: vec![7],
                },
                true,
            ),
            (
                Shared::RunNote {
                    packet: None,
                    out: None,
                    attach: vec![],
                    forget: vec![],
                },
                true,
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
        for host in [
            kind::ANSWER,
            kind::HOST_IN_BUFFER,
            kind::AGREE,
            kind::RUN_NOTE,
        ] {
            assert!(host < 0x40);
        }
        for worker in [kind::ASK, kind::WORKER_IN_BUFFER, kind::WANT] {
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

    #[test]
    fn a_run_note_and_its_packet_share_one_message() {
        let note = Shared::RunNote {
            packet: None,
            out: Some(2),
            attach: vec![0],
            forget: vec![],
        };
        let mut out = Writer::new();
        note.encode(true, &mut out).unwrap();
        out.buf.extend_from_slice(&[crate::kind::RUN, 1, 2, 3]);
        let (back, rest) = Shared::decode_note(&out.buf).unwrap();
        assert_eq!(back, note);
        assert_eq!(rest, [crate::kind::RUN, 1, 2, 3]);
        assert!(
            Shared::decode(&out.buf).is_err(),
            "a whole-frame read refuses the rest"
        );
    }

    #[test]
    fn a_place_reads_back_as_written() {
        for place in [
            Place {
                at: At::Lent(3),
                offset: 4096,
                len: 1 << 20,
            },
            Place {
                at: At::Fd(9),
                offset: 0,
                len: 12,
            },
        ] {
            let mut out = Writer::new();
            place.encode(&mut out);
            let mut input = Reader::new(&out.buf);
            let form = input.u8().unwrap();
            assert_eq!(Place::decode(form, &mut input).unwrap(), place);
            input.finish().unwrap();
        }
    }
}
