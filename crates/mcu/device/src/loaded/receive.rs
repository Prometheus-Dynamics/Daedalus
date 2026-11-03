//! Receiving a plan blob in chunks (feature `receive`): a framing for any byte transport (UART,
//! USB, BLE), each chunk checked with its own CRC-32, written in order into a RAM buffer or a
//! flash partition ([`BlobSink`]). Once [`Progress::Complete`], the application loads the blob
//! ([`Interpreter::load`](super::Interpreter::load) checks the blob's own CRC-32 as well).
//!
//! A chunk frame, little-endian:
//!
//! | bytes | field |
//! | --- | --- |
//! | 1 | tag `C` |
//! | 4 | blob length |
//! | 4 | offset of the payload in the blob |
//! | 2 | payload length |
//! | n | payload |
//! | 4 | CRC-32 ([`crc32`]) of the bytes before it |
//!
//! Chunks arrive in order. A chunk at offset 0 starts a new blob, except while a blob of the same
//! length is in progress, where it is a retransmission (call [`BlobReceiver::reset`] to restart
//! one). A chunk the receiver already has is accepted and ignored; one past the received bytes is
//! a [`ReceiveError::Gap`] naming the offset to resend from. Nothing allocates.

use super::crc32;

/// The tag byte that starts a chunk frame.
pub const CHUNK_TAG: u8 = b'C';
/// Bytes of a chunk frame before its payload.
pub const CHUNK_HEADER: usize = 11;
/// Bytes a chunk frame adds to its payload (header and CRC-32).
pub const CHUNK_OVERHEAD: usize = CHUNK_HEADER + 4;

/// Why a chunk was not taken. The receiver keeps what it had.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum ReceiveError {
    /// Not a chunk frame: wrong tag, too short, or its length fields disagree.
    Frame,
    /// The frame's CRC-32 is not the one of its bytes (corrupted in transit).
    Checksum { expected: u32, found: u32 },
    /// The blob does not fit the sink.
    TooLarge { len: u32, capacity: u32 },
    /// The chunk starts past the received bytes, or belongs to another blob: resend from
    /// `resend_from`.
    Gap { resend_from: u32 },
    /// The sink refused the write (a flash error).
    Sink,
}

/// Where received bytes go: a RAM buffer (`[u8]`) or a flash partition the application wraps
/// (erasing as it needs). Writes come in increasing, contiguous offsets from 0.
pub trait BlobSink {
    /// Bytes the sink holds.
    fn capacity(&self) -> usize;
    /// Write `bytes` at `offset`.
    fn write(&mut self, offset: usize, bytes: &[u8]) -> Result<(), ReceiveError>;
}

impl BlobSink for [u8] {
    fn capacity(&self) -> usize {
        self.len()
    }

    fn write(&mut self, offset: usize, bytes: &[u8]) -> Result<(), ReceiveError> {
        self.get_mut(offset..offset + bytes.len())
            .ok_or(ReceiveError::Sink)?
            .copy_from_slice(bytes);
        Ok(())
    }
}

impl<S: BlobSink + ?Sized> BlobSink for &mut S {
    fn capacity(&self) -> usize {
        (**self).capacity()
    }

    fn write(&mut self, offset: usize, bytes: &[u8]) -> Result<(), ReceiveError> {
        (**self).write(offset, bytes)
    }
}

/// How far a blob has come.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum Progress {
    /// `received` of `len` bytes are in the sink.
    Partial { received: u32, len: u32 },
    /// All `len` bytes are in the sink.
    Complete { len: u32 },
}

/// Assembles a blob from chunk frames into a [`BlobSink`].
pub struct BlobReceiver<S> {
    sink: S,
    len: u32,
    received: u32,
}

impl<S: BlobSink> BlobReceiver<S> {
    pub fn new(sink: S) -> Self {
        Self {
            sink,
            len: 0,
            received: 0,
        }
    }

    /// Take one chunk frame.
    pub fn push(&mut self, frame: &[u8]) -> Result<Progress, ReceiveError> {
        let chunk = Chunk::decode(frame)?;
        if chunk.offset == 0 {
            let capacity = self.sink.capacity();
            if chunk.len as usize > capacity {
                return Err(ReceiveError::TooLarge {
                    len: chunk.len,
                    capacity: capacity as u32,
                });
            }
            // A new blob unless this is the first chunk of the one in progress again.
            if chunk.len != self.len || self.received == 0 || self.received == self.len {
                self.len = chunk.len;
                self.received = 0;
            }
        }
        if chunk.len != self.len || chunk.offset > self.received {
            return Err(ReceiveError::Gap {
                resend_from: self.received,
            });
        }
        let end = chunk.offset + chunk.payload.len() as u32;
        if end > self.received {
            let skip = (self.received - chunk.offset) as usize;
            self.sink
                .write(self.received as usize, &chunk.payload[skip..])?;
            self.received = end;
        }
        Ok(self.progress())
    }

    pub fn progress(&self) -> Progress {
        if self.len > 0 && self.received == self.len {
            Progress::Complete { len: self.len }
        } else {
            Progress::Partial {
                received: self.received,
                len: self.len,
            }
        }
    }

    /// Forget the blob in progress.
    pub fn reset(&mut self) {
        self.len = 0;
        self.received = 0;
    }

    pub fn sink(&self) -> &S {
        &self.sink
    }

    pub fn into_sink(self) -> S {
        self.sink
    }
}

/// A decoded chunk frame.
struct Chunk<'a> {
    len: u32,
    offset: u32,
    payload: &'a [u8],
}

impl<'a> Chunk<'a> {
    fn decode(frame: &'a [u8]) -> Result<Self, ReceiveError> {
        if frame.len() < CHUNK_OVERHEAD || frame[0] != CHUNK_TAG {
            return Err(ReceiveError::Frame);
        }
        let u32_at = |at: usize| {
            u32::from_le_bytes([frame[at], frame[at + 1], frame[at + 2], frame[at + 3]])
        };
        let len = u32_at(1);
        let offset = u32_at(5);
        let payload_len = usize::from(u16::from_le_bytes([frame[9], frame[10]]));
        if frame.len() != CHUNK_OVERHEAD + payload_len {
            return Err(ReceiveError::Frame);
        }
        let body = frame.len() - 4;
        let (expected, found) = (crc32(&frame[..body]), u32_at(body));
        if expected != found {
            return Err(ReceiveError::Checksum { expected, found });
        }
        let payload = &frame[CHUNK_HEADER..body];
        if u64::from(offset) + payload.len() as u64 > u64::from(len) {
            return Err(ReceiveError::Frame);
        }
        Ok(Self {
            len,
            offset,
            payload,
        })
    }
}

/// Write the chunk frame of `blob[offset..]`, at most `max_payload` bytes of it, into `out`, and
/// return the frame's length. The sender's side of [`BlobReceiver`]: loop with `offset` advanced
/// by the payload (`frame length - CHUNK_OVERHEAD`) until it reaches `blob.len()`.
pub fn encode_chunk(
    blob: &[u8],
    offset: usize,
    max_payload: usize,
    out: &mut [u8],
) -> Result<usize, ReceiveError> {
    let len = u32::try_from(blob.len()).map_err(|_| ReceiveError::Frame)?;
    let payload = blob
        .get(offset..)
        .ok_or(ReceiveError::Frame)?
        .get(
            ..max_payload
                .min(usize::from(u16::MAX))
                .min(blob.len() - offset),
        )
        .ok_or(ReceiveError::Frame)?;
    let frame_len = CHUNK_OVERHEAD + payload.len();
    let frame = out.get_mut(..frame_len).ok_or(ReceiveError::Frame)?;
    frame[0] = CHUNK_TAG;
    frame[1..5].copy_from_slice(&len.to_le_bytes());
    frame[5..9].copy_from_slice(&(offset as u32).to_le_bytes());
    frame[9..11].copy_from_slice(&(payload.len() as u16).to_le_bytes());
    frame[CHUNK_HEADER..frame_len - 4].copy_from_slice(payload);
    let crc = crc32(&frame[..frame_len - 4]);
    frame[frame_len - 4..].copy_from_slice(&crc.to_le_bytes());
    Ok(frame_len)
}
