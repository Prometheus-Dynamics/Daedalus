//! The Linux side: the lender's pool, the borrower's view of it, and reading packets that may
//! carry descriptors.

use std::fmt;
use std::fs::File;
use std::os::fd::{AsFd, BorrowedFd, OwnedFd};
use std::os::unix::fs::FileTypeExt;

use daedalus_abi::SharedBytes;
use daedalus_wire::PacketError;
use daedalus_wire::codec::Writer;
use daedalus_wire::shared::{At, BY_REFERENCE, Place, Shared, Span};
use shm_fdpass::{FdSet, MAX_FDS, RecvError};
use shm_ring_buffers::{BufferDesc, BufferIndex, Pool, PoolView};

use crate::THRESHOLD;
use crate::fds::FdCache;
use crate::held::Held;

/// The buffer the host writes for the worker.
pub(crate) const TO_WORKER: u32 = 0;
/// The buffer the worker writes for the host.
const TO_HOST: u32 = 1;
/// The first buffer output values go in, for a worker that agreed values by reference.
const FIRST_VALUE: u32 = 2;
/// Where values by reference start in a buffer: a cache line, so no value shares one with
/// another.
const ALIGN: usize = 64;

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
    /// The value buffers and who holds them, when the worker agreed values by reference.
    held: Option<Held>,
    /// The features agreed with the worker: none until [`Lender::agree`].
    features: u32,
    threshold: usize,
    /// Where the next value by reference goes in the host's buffer, during a run.
    cursor: usize,
    fds: FdCache,
    note: Writer,
}

impl Lender {
    /// Two buffers of at least `size` bytes each, in one new sealed memfd. `generation` must be
    /// higher than that of any lender this worker was granted before.
    pub fn new(generation: u32, size: usize) -> Result<Lender, ShmError> {
        Lender::with_values(generation, size, 0)
    }

    /// As [`Lender::new`], with `values` more buffers that output values by reference go in:
    /// for a worker that wants values by reference. Untouched, they cost address space only.
    pub fn with_values(generation: u32, size: usize, values: u32) -> Result<Lender, ShmError> {
        let size = size.max(THRESHOLD).next_multiple_of(page_size());
        let pool = Pool::in_memory(generation, FIRST_VALUE + values, size)
            .map_err(|e| fail("the pool", e))?;
        let view = pool
            .writable_view()
            .map_err(|e| fail("the host's view of the pool", e))?;
        let held = if values == 0 {
            None
        } else {
            let cells = pool
                .view()
                .map_err(|e| fail("the cells' view of the pool", e))?;
            Some(Held::new(cells, FIRST_VALUE, values))
        };
        Ok(Lender {
            pool,
            view,
            size,
            held,
            features: 0,
            threshold: THRESHOLD,
            cursor: 0,
            fds: FdCache::new(),
            note: Writer::new(),
        })
    }

    /// Whether this pool has value buffers: the worker agreed values by reference.
    pub fn by_reference(&self) -> bool {
        self.held.is_some()
    }

    /// The same, with `threshold` the smallest packet or value that goes through these buffers,
    /// in place of [`THRESHOLD`]. Set before the grant, which tells the worker.
    pub fn with_threshold(mut self, threshold: usize) -> Lender {
        self.threshold = threshold;
        self
    }

    /// The smallest packet or value that goes through these buffers, in bytes.
    pub fn threshold(&self) -> usize {
        self.threshold
    }

    /// The features agreed with the worker.
    pub fn features(&self) -> u32 {
        self.features
    }

    /// Sends the agreement that follows the grant: `features`, which include values by
    /// reference, and the value buffers.
    pub fn agree(&mut self, socket: BorrowedFd<'_>, features: u32) -> Result<(), ShmError> {
        let Some(held) = &self.held else {
            return Err(ShmError("this pool has no value buffers".into()));
        };
        let values = held.buffers().collect();
        let features = features | BY_REFERENCE;
        let mut out = Writer::new();
        Shared::Agree { features, values }
            .encode(true, &mut out)
            .map_err(|e| fail("the agreement", e))?;
        shm_fdpass::send(socket, &[&out.buf], &[], false)
            .map_err(|e| fail("sending the agreement", e))?;
        self.features = features;
        Ok(())
    }

    /// Starts a run: values by reference go in the host's buffer from its start again, and the
    /// note has no descriptors yet.
    pub fn begin_run(&mut self) {
        self.cursor = 0;
        self.fds.begin();
    }

    /// Room for `len` bytes in the host's buffer after this run's other values, if they fit.
    fn room(&mut self, len: usize, items_at: usize) -> Option<(usize, &mut [u8])> {
        let start = aligned(self.cursor, items_at);
        let end = start.checked_add(len)?;
        let room = self
            .view
            .write_bytes(BufferIndex(TO_WORKER))
            .ok()?
            .get_mut(start..end)?;
        self.cursor = end;
        Some((start, room))
    }

    /// Writes a value's bytes into the host's buffer, and gives its place. `items_at` is where
    /// its items start, for a value whose layout is an array: they are put on an aligned
    /// address, so a reader can view them as an array. None when this run's values don't leave
    /// room for it: then it travels in the packet.
    pub fn place(&mut self, bytes: &[u8], items_at: usize) -> Option<Place> {
        let (start, room) = self.room(bytes.len(), items_at)?;
        room.copy_from_slice(bytes);
        Some(Place {
            at: At::Lent(TO_WORKER),
            offset: start as u64,
            len: bytes.len() as u64,
        })
    }

    /// Writes a run's packet into the host's buffer after its values, and gives where.
    pub fn place_packet(&mut self, body: &[u8]) -> Option<(u64, u32)> {
        let (start, room) = self.room(body.len(), 0)?;
        room.copy_from_slice(body);
        Some((start as u64, body.len() as u32))
    }

    /// The place of `len` bytes from `offset` in the buffer behind `fd`: by its slot, attached
    /// to this run's note the first time the buffer is seen.
    pub fn fd_place(
        &mut self,
        fd: BorrowedFd<'_>,
        offset: u64,
        len: u64,
    ) -> Result<Place, ShmError> {
        let slot = self.fds.slot(fd)?;
        Ok(Place {
            at: At::Fd(slot),
            offset,
            len,
        })
    }

    /// How many buffers the worker has by descriptor.
    pub fn kept_fds(&self) -> usize {
        self.fds.kept()
    }

    /// A value buffer no cell holds, for the worker to write this run's output values into.
    pub fn free_out(&self) -> Option<u32> {
        self.held.as_ref()?.free_buffer()
    }

    /// An output value the worker wrote at `place`, held by the cell it goes to, with no copy.
    /// None when the place is not inside a value buffer.
    pub fn adopt(&self, place: &Place) -> Option<SharedBytes> {
        let At::Lent(buffer) = place.at else {
            return None;
        };
        self.held.as_ref()?.adopt(buffer, place.offset, place.len)
    }

    /// Sends a run: its note, then the packet in the same frame when `packet` is None, with the
    /// descriptors this run attaches. Returns how many descriptors went.
    pub fn send_run(
        &mut self,
        socket: BorrowedFd<'_>,
        out: Option<u32>,
        packet: Option<(u64, u32)>,
        run: &[u8],
    ) -> Result<usize, ShmError> {
        self.note.buf.clear();
        Shared::RunNote {
            packet,
            out,
            attach: self.fds.attach.clone(),
            forget: self.fds.forget.clone(),
        }
        .encode(true, &mut self.note)
        .map_err(|e| fail("the run note", e))?;
        let fds: Vec<BorrowedFd<'_>> = self.fds.attached().collect();
        let body: &[u8] = if packet.is_some() { &[] } else { run };
        shm_fdpass::send(socket, &[&self.note.buf, body], &fds, false)
            .map_err(|e| fail("sending the run", e))?;
        Ok(fds.len())
    }

    /// The size of each buffer.
    pub fn size(&self) -> usize {
        self.size
    }

    /// Sends the grant on `socket`: the message, with the pool page and the memfd attached.
    pub fn grant(&self, socket: BorrowedFd<'_>) -> Result<(), ShmError> {
        let msg = Shared::Grant {
            generation: self.pool.generation(),
            threshold: u32::try_from(self.threshold).unwrap_or(u32::MAX),
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
    /// The buffers output values go in, once the host agreed values by reference.
    values: Vec<u32>,
    features: u32,
    /// The value buffer this run's outputs go in, and how far it is written.
    out: Option<u32>,
    out_cursor: usize,
    fds: crate::fds::FdTable,
    /// Where the next run's packet starts in the host's buffer.
    run_offset: u64,
    /// A second view of the same buffers, which output values are written through while the
    /// node reads its inputs through the first.
    outs: PoolView,
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
        let dup = |fd: &OwnedFd| {
            fd.try_clone()
                .map_err(|e| fail("duplicating a descriptor", e))
        };
        let page_again = dup(&page)?;
        let rest_again = rest.iter().map(dup).collect::<Result<Vec<_>, _>>()?;
        let view = PoolView::attach_writable(page, rest, &descs)
            .map_err(|e| fail("mapping the lent buffers", e))?;
        let outs = PoolView::attach_writable(page_again, rest_again, &descs)
            .map_err(|e| fail("mapping the lent buffers again", e))?;
        Ok(Borrower {
            view,
            threshold: *threshold as usize,
            to_worker: *to_worker,
            to_host: *to_host,
            values: Vec::new(),
            features: 0,
            out: None,
            out_cursor: 0,
            fds: crate::fds::FdTable::new(),
            run_offset: 0,
            outs,
        })
    }

    /// Takes the host's agreement to values by reference, for this grant.
    pub fn agree(&mut self, msg: &Shared) {
        if let Shared::Agree { features, values } = msg
            && features & BY_REFERENCE != 0
        {
            self.values.clone_from(values);
            self.features = *features;
        }
    }

    /// The features the host agreed.
    pub fn features(&self) -> u32 {
        self.features
    }

    /// Whether the host agreed values by reference.
    pub fn by_reference(&self) -> bool {
        self.features & BY_REFERENCE != 0
    }

    /// Takes a run's note: lets go of the buffers it forgets, maps the ones it attaches (`fds`,
    /// in order), and starts the value buffer it names for this run's outputs.
    pub fn note(&mut self, msg: &Shared, mut fds: FdSet) -> Result<(), ShmError> {
        let Shared::RunNote {
            out,
            attach,
            forget,
            ..
        } = msg
        else {
            return Err(ShmError("not a run note".into()));
        };
        for slot in forget {
            self.fds.forget(*slot);
        }
        for (index, slot) in attach.iter().enumerate() {
            let fd = fds
                .take(index)
                .ok_or_else(|| ShmError(format!("the note attaches slot {slot} without it")))?;
            self.fds.attach(*slot, fd)?;
        }
        self.out = out.filter(|buffer| self.values.contains(buffer));
        self.out_cursor = 0;
        if let Shared::RunNote {
            packet: Some((offset, _)),
            ..
        } = msg
        {
            self.run_offset = *offset;
        }
        Ok(())
    }

    /// Where a run in the host's buffer starts: after its values with a run note, at the start
    /// without.
    pub(crate) fn run_offset(&mut self) -> u64 {
        std::mem::take(&mut self.run_offset)
    }

    /// The node's two sides at once: reading its inputs, and writing its output values.
    pub fn split(&mut self) -> (crate::Reads<'_>, crate::Writes<'_>) {
        (
            crate::Reads {
                view: Some(&self.view),
                to_worker: self.to_worker,
                fds: Some(&self.fds),
                _of: std::marker::PhantomData,
            },
            crate::Writes {
                view: Some(&mut self.outs),
                out: self.out,
                cursor: Some(&mut self.out_cursor),
                _of: std::marker::PhantomData,
            },
        )
    }

    /// The size of each lent buffer.
    pub fn buffer_size(&self) -> usize {
        self.view
            .read_bytes(BufferIndex(self.to_worker))
            .map_or(0, <[u8]>::len)
    }

    /// Begins the node's reads of the buffer in `slot`: the kernel's sync call, for a buffer
    /// that takes it.
    pub fn begin_read(&mut self, slot: u32) -> Result<(), ShmError> {
        self.fds.begin(slot)
    }

    /// Ends what [`Borrower::begin_read`] began.
    pub fn end_read(&mut self, slot: u32) {
        self.fds.end(slot);
    }

    /// What this worker did to its buffers by descriptor.
    pub fn brackets(&self) -> crate::Brackets {
        self.fds.brackets
    }

    /// A run's packet at `offset` of the host's buffer.
    pub fn packet_at(&self, offset: u64, len: usize) -> Option<&[u8]> {
        let start = usize::try_from(offset).ok()?;
        self.view
            .read_bytes(BufferIndex(self.to_worker))
            .ok()?
            .get(start..start.checked_add(len)?)
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
        let Ok(buffer) = self.outs.write_bytes(BufferIndex(self.to_host)) else {
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

/// The first offset at or after `cursor` that puts the byte `items_at` into a value on an
/// [`ALIGN`] boundary.
pub(crate) fn aligned(cursor: usize, items_at: usize) -> usize {
    let skew = items_at % ALIGN;
    (cursor + skew).next_multiple_of(ALIGN) - skew
}

#[cfg(test)]
mod tests {
    use super::{ALIGN, aligned};

    #[test]
    fn a_value_starts_where_its_items_fall_on_the_boundary() {
        for cursor in [0, 1, 60, 64, 100, 4096] {
            for items_at in [0, 4, 12, 64, 68] {
                let start = aligned(cursor, items_at);
                assert!(start >= cursor, "{cursor} {items_at}");
                assert!(
                    start - cursor < ALIGN,
                    "no more than one boundary of padding"
                );
                assert_eq!((start + items_at) % ALIGN, 0, "{cursor} {items_at}");
            }
        }
    }
}
