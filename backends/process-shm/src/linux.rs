//! The Linux side: the lender's pool, the borrower's view of it, and reading packets that may
//! carry descriptors.

use std::fmt;
use std::fs::File;
use std::os::fd::{AsFd, BorrowedFd, OwnedFd};
use std::os::unix::fs::FileTypeExt;

use daedalus_wire::PacketError;
use daedalus_wire::codec::Writer;
use daedalus_wire::shared::{Shared, Span};
use shm_fdpass::{FdSet, MAX_FDS, RecvError};
use shm_ring_buffers::{BufferDesc, BufferIndex, Pool, PoolView};

use crate::THRESHOLD;

/// The buffer the host writes for the worker.
pub(crate) const TO_WORKER: u32 = 0;
/// The buffer the worker writes for the host.
const TO_HOST: u32 = 1;

/// Why shared memory could not be set up or used.
#[derive(Debug)]
pub struct ShmError(pub String);

impl fmt::Display for ShmError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ShmError {}

fn fail(what: &str, error: impl fmt::Debug) -> ShmError {
    ShmError(format!("{what}: {error:?}"))
}

/// True when `fd` is a socket, which is what a worker's stdin is when its host can lend memory.
pub fn is_socket(fd: BorrowedFd<'_>) -> bool {
    fd.try_clone_to_owned()
        .map(File::from)
        .and_then(|file| file.metadata())
        .is_ok_and(|meta| meta.file_type().is_socket())
}

/// Reads one packet from a socket into `buf`, with the descriptors that came with it. Returns
/// the packet's length: `buf` keeps its size between packets, and only `buf[..len]` is the
/// packet.
pub fn recv_packet(
    socket: BorrowedFd<'_>,
    cap: u32,
    buf: &mut Vec<u8>,
) -> Result<(usize, FdSet), PacketError> {
    loop {
        match shm_fdpass::recv(socket, buf, MAX_FDS, false) {
            Ok(Some(got)) => return Ok((got.len, got.fds)),
            Ok(None) => return Err(PacketError::Closed),
            Err(RecvError::TooLarge { len }) => {
                let len32 = u32::try_from(len).unwrap_or(u32::MAX);
                if len32 > cap {
                    return Err(PacketError::TooLarge { len: len32, cap });
                }
                // Grown once to the largest packet seen, then reused.
                buf.resize(len, 0);
            }
            Err(RecvError::ShortRead) => return Err(PacketError::Truncated),
            Err(RecvError::Io(error)) => return Err(PacketError::Io(error)),
            Err(other) => {
                return Err(PacketError::Io(std::io::Error::other(format!("{other:?}"))));
            }
        }
    }
}

/// The host's side: the memory it lends one worker.
pub struct Lender {
    pool: Pool,
    view: PoolView,
    size: usize,
}

impl Lender {
    /// Two buffers of at least `size` bytes each, in one new sealed memfd. `generation` must be
    /// higher than that of any lender this worker was granted before.
    pub fn new(generation: u32, size: usize) -> Result<Lender, ShmError> {
        let size = size.max(THRESHOLD).next_multiple_of(page_size());
        let pool = Pool::in_memory(generation, 2, size).map_err(|e| fail("the pool", e))?;
        let view = pool
            .writable_view()
            .map_err(|e| fail("the host's view of the pool", e))?;
        Ok(Lender { pool, view, size })
    }

    /// The size of each buffer.
    pub fn size(&self) -> usize {
        self.size
    }

    /// Sends the grant on `socket`: the message, with the pool page and the memfd attached.
    pub fn grant(&self, socket: BorrowedFd<'_>) -> Result<(), ShmError> {
        let msg = Shared::Grant {
            generation: self.pool.generation(),
            threshold: THRESHOLD as u32,
            to_worker: TO_WORKER,
            to_host: TO_HOST,
            buffers: self
                .pool
                .buffers()
                .iter()
                .map(|desc| Span {
                    descriptor: desc.descriptor,
                    offset: desc.offset,
                    size: desc.size,
                })
                .collect(),
        };
        let mut out = Writer::new();
        msg.encode(true, &mut out)
            .map_err(|e| fail("the grant", e))?;
        let page = self.pool.page_fd().map_err(|e| fail("the pool page", e))?;
        let mut fds: Vec<BorrowedFd<'_>> = vec![page.as_fd()];
        fds.extend(self.pool.descriptors());
        shm_fdpass::send(socket, &[&out.buf], &fds, false).map_err(|e| fail("sending the grant", e))
    }

    /// The buffer the host writes a packet into for the worker.
    pub fn to_worker(&mut self) -> &mut [u8] {
        self.view
            .write_bytes(BufferIndex(TO_WORKER))
            .unwrap_or_default()
    }

    /// The first `len` bytes of the buffer the worker wrote, if `len` is inside it.
    pub fn worker_wrote(&self, len: usize) -> Option<&[u8]> {
        self.view
            .read_bytes(BufferIndex(TO_HOST))
            .ok()
            .and_then(|bytes| bytes.get(..len))
    }

    /// The note that the next packet is the first `len` bytes of the worker's buffer.
    pub fn note(kind: u8, len: usize, out: &mut Writer) {
        let _ = Shared::InBuffer {
            kind,
            buffer: TO_WORKER,
            len: len as u32,
        }
        .encode(true, out);
    }
}

/// The worker's side: its view of the memory a host lent it.
pub struct Borrower {
    view: PoolView,
    threshold: usize,
    to_worker: u32,
    to_host: u32,
}

impl Borrower {
    /// Maps what a grant hands over. `msg` is the grant and `fds` the descriptors that came with
    /// it: the pool page, then the buffers' descriptors.
    pub fn attach(msg: &Shared, mut fds: FdSet) -> Result<Borrower, ShmError> {
        let Shared::Grant {
            threshold,
            to_worker,
            to_host,
            buffers,
            ..
        } = msg
        else {
            return Err(ShmError("not a grant".into()));
        };
        let page = fds
            .take(0)
            .ok_or_else(|| ShmError("the grant came without the pool page".into()))?;
        let rest: Vec<OwnedFd> = (1..fds.len()).filter_map(|index| fds.take(index)).collect();
        let descs: Vec<BufferDesc> = buffers
            .iter()
            .map(|span| BufferDesc {
                descriptor: span.descriptor,
                offset: span.offset,
                size: span.size,
            })
            .collect();
        let view = PoolView::attach_writable(page, rest, &descs)
            .map_err(|e| fail("mapping the lent buffers", e))?;
        Ok(Borrower {
            view,
            threshold: *threshold as usize,
            to_worker: *to_worker,
            to_host: *to_host,
        })
    }

    /// The smallest packet that goes in a buffer.
    pub fn threshold(&self) -> usize {
        self.threshold
    }

    /// The packet a note names: the first `len` bytes of `buffer`.
    pub fn packet(&self, buffer: u32, len: usize) -> Option<&[u8]> {
        if buffer != self.to_worker {
            return None;
        }
        self.view
            .read_bytes(BufferIndex(buffer))
            .ok()
            .and_then(|bytes| bytes.get(..len))
    }

    /// Writes `body` into the host's buffer and the note for it into `note`, when `body` is at
    /// least the threshold and fits. Returns false when it goes on the socket instead.
    pub fn put(&mut self, body: &[u8], note: &mut Writer) -> bool {
        if body.len() < self.threshold {
            return false;
        }
        let Ok(buffer) = self.view.write_bytes(BufferIndex(self.to_host)) else {
            return false;
        };
        let Some(room) = buffer.get_mut(..body.len()) else {
            return false;
        };
        room.copy_from_slice(body);
        let _ = Shared::InBuffer {
            kind: body[0],
            buffer: self.to_host,
            len: body.len() as u32,
        }
        .encode(false, note);
        true
    }
}

fn page_size() -> usize {
    // The pool rounds to its own pages too; 16 KiB covers 4 KiB and 16 KiB page kernels.
    16 * 1024
}
