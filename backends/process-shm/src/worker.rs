//! The worker's side, for a worker written in Rust: asking for shared memory, reading what the
//! host sends (a packet on stdin, or a run in a lent buffer), and sending outputs back. Off Linux,
//! and with a host that lends nothing, it is the plain path: packets on stdin and stdout.
//!
//! The reading half ([`Inbox`]) and the writing half ([`Lent`]) can be on different threads: the
//! inbox maps a grant when it arrives, and both reach the mapping through [`Lent`].

use std::io::{self, Write};
use std::sync::{Arc, Mutex};
#[cfg(target_os = "linux")]
use std::sync::{MutexGuard, PoisonError};

#[cfg(target_os = "linux")]
use daedalus_wire::codec::Writer;
use daedalus_wire::{PacketError, read_packet, write_packet};

/// What a worker says when the host offered no shared memory.
pub const PLAIN_LINE: &str = "host offers no shared memory: plain path";

/// The memory a host lent this worker, if any, shared by its reading and writing halves.
#[derive(Clone, Default)]
pub struct Lent {
    #[cfg(target_os = "linux")]
    borrower: Arc<Mutex<Option<crate::Borrower>>>,
    #[cfg(not(target_os = "linux"))]
    _none: Arc<Mutex<()>>,
}

/// One thing read from the host.
#[derive(Debug, PartialEq, Eq)]
pub enum Incoming {
    /// A packet on the stream: the first `len` bytes of the buffer read into.
    Packet(usize),
    /// A run in the lent buffer, `len` bytes, for run `run`. Read it with [`Lent::run`].
    Run { len: usize, run: u64 },
    /// The host lent memory.
    Granted,
    /// The host lends nothing, and said why.
    Plain(String),
}

impl Lent {
    #[cfg(target_os = "linux")]
    fn lock(&self) -> MutexGuard<'_, Option<crate::Borrower>> {
        self.borrower.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Asks the host for shared memory, right after the hello, when stdin is a socket on Linux.
    /// Returns whether it asked.
    pub fn ask(&self, out: &mut impl Write) -> Result<bool, PacketError> {
        #[cfg(target_os = "linux")]
        {
            use std::os::fd::AsFd;
            if crate::is_socket(io::stdin().as_fd()) {
                let mut writer = Writer::new();
                daedalus_wire::shared::Shared::Ask {
                    version: daedalus_wire::shared::VERSION,
                }
                .encode(false, &mut writer)
                .map_err(|error| PacketError::Io(io::Error::other(error.to_string())))?;
                write_packet(out, &writer.buf, daedalus_wire::DEFAULT_CAP)?;
                return Ok(true);
            }
        }
        let _ = out;
        Ok(false)
    }

    /// As [`Lent::ask`], and also asks to take and give values by reference: a `bytes` or
    /// opaque input then arrives as a place in lent memory or in a buffer sent by descriptor, and
    /// an output value can be written straight into lent memory. Read and write them through
    /// [`Lent::view`].
    pub fn ask_by_reference(&self, out: &mut impl Write) -> Result<bool, PacketError> {
        if !self.ask(out)? {
            return Ok(false);
        }
        #[cfg(target_os = "linux")]
        {
            let mut writer = Writer::new();
            daedalus_wire::shared::Shared::Want {
                features: daedalus_wire::shared::BY_REFERENCE,
            }
            .encode(false, &mut writer)
            .map_err(|error| PacketError::Io(io::Error::other(error.to_string())))?;
            write_packet(out, &writer.buf, daedalus_wire::DEFAULT_CAP)?;
        }
        Ok(true)
    }

    /// Whether the host agreed values by reference: then a run's values are read with the
    /// schema's [`set_by_reference`](daedalus_wire::Schema::set_by_reference) on.
    pub fn by_reference(&self) -> bool {
        #[cfg(target_os = "linux")]
        {
            self.lock()
                .as_ref()
                .is_some_and(crate::Borrower::by_reference)
        }
        #[cfg(not(target_os = "linux"))]
        false
    }

    /// Calls `with` on the memory lent for this run: the node's inputs by reference, and room
    /// for its output values. Outside a run, or with nothing lent, the view has neither.
    pub fn view<T>(&self, with: impl FnOnce(&mut View<'_>) -> T) -> T {
        #[cfg(target_os = "linux")]
        {
            let mut guard = self.lock();
            let mut view = View {
                borrower: guard.as_mut(),
            };
            with(&mut view)
        }
        #[cfg(not(target_os = "linux"))]
        {
            with(&mut View {
                _none: std::marker::PhantomData,
            })
        }
    }

    /// Calls `with` on the run in the lent buffer, `len` bytes. None when nothing is lent or the
    /// run is not inside the buffer.
    pub fn run<T>(&self, len: usize, with: impl FnOnce(&[u8]) -> T) -> Option<T> {
        #[cfg(target_os = "linux")]
        {
            let mut guard = self.lock();
            let borrower = guard.as_mut()?;
            let offset = borrower.run_offset();
            let bytes = borrower.packet_at(offset, len)?;
            Some(with(bytes))
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = (len, with);
            None
        }
    }

    /// Sends outputs: into the host's buffer when they are large and memory is lent, on stdout
    /// otherwise. Only outputs go in a buffer: the host reads them before it sends another run.
    pub fn send_outputs(&self, body: &[u8], out: &mut impl Write) -> Result<(), PacketError> {
        #[cfg(target_os = "linux")]
        {
            let mut guard = self.lock();
            if let Some(borrower) = guard.as_mut() {
                let mut note = Writer::new();
                if borrower.put(body, &mut note) {
                    drop(guard);
                    return write_packet(out, &note.buf, daedalus_wire::DEFAULT_CAP);
                }
            }
        }
        write_packet(out, body, daedalus_wire::DEFAULT_CAP)
    }
}

/// The reading half: reads stdin.
pub struct Inbox {
    #[cfg(target_os = "linux")]
    lent: Lent,
    #[cfg(target_os = "linux")]
    socket: bool,
}

impl Inbox {
    pub fn new(lent: Lent) -> Inbox {
        #[cfg(target_os = "linux")]
        {
            use std::os::fd::AsFd;
            let socket = crate::is_socket(io::stdin().as_fd());
            Inbox { lent, socket }
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = lent;
            Inbox {}
        }
    }

    /// Reads the next thing from the host into `buf`. A grant is mapped here, before it is
    /// returned.
    pub fn next(&mut self, buf: &mut Vec<u8>) -> Result<Incoming, PacketError> {
        #[cfg(target_os = "linux")]
        if self.socket {
            use daedalus_wire::shared::{Shared, kind};
            use std::os::fd::AsFd;
            let stdin = io::stdin();
            let (len, fds) = crate::recv_packet(stdin.as_fd(), daedalus_wire::DEFAULT_CAP, buf)?;
            let malformed = |error: daedalus_wire::WireError| {
                PacketError::Io(io::Error::other(error.to_string()))
            };
            return match buf[..len].first().copied() {
                Some(kind::ANSWER) => match Shared::decode(&buf[..len]).map_err(malformed)? {
                    grant @ Shared::Grant { .. } => match crate::Borrower::attach(&grant, fds) {
                        Ok(borrower) => {
                            *self.lent.lock() = Some(borrower);
                            Ok(Incoming::Granted)
                        }
                        Err(error) => Ok(Incoming::Plain(format!(
                            "the host lent memory this worker could not map: {error}"
                        ))),
                    },
                    Shared::Plain { reason } => Ok(Incoming::Plain(reason)),
                    _ => Ok(Incoming::Packet(len)),
                },
                Some(kind::AGREE) => {
                    let msg = Shared::decode(&buf[..len]).map_err(malformed)?;
                    if let Some(borrower) = self.lent.lock().as_mut() {
                        borrower.agree(&msg);
                    }
                    Ok(Incoming::Granted)
                }
                Some(kind::RUN_NOTE) => {
                    let (msg, rest) = Shared::decode_note(&buf[..len]).map_err(malformed)?;
                    let packet = match &msg {
                        Shared::RunNote { packet, .. } => *packet,
                        _ => None,
                    };
                    let at = len - rest.len();
                    {
                        let mut guard = self.lent.lock();
                        let borrower = guard.as_mut().ok_or(PacketError::Truncated)?;
                        borrower.note(&msg, fds).map_err(|error| {
                            PacketError::Io(io::Error::other(error.to_string()))
                        })?;
                    }
                    match packet {
                        // The run came in this frame, after its note.
                        None => {
                            buf.copy_within(at..len, 0);
                            Ok(Incoming::Packet(len - at))
                        }
                        Some((offset, len)) => {
                            let len = len as usize;
                            let guard = self.lent.lock();
                            let run = guard
                                .as_ref()
                                .and_then(|borrower| borrower.packet_at(offset, len))
                                .and_then(|bytes| bytes.get(1..9))
                                .map(|raw| u64::from_le_bytes(raw.try_into().unwrap_or([0; 8])))
                                .ok_or(PacketError::Truncated)?;
                            Ok(Incoming::Run { len, run })
                        }
                    }
                }
                Some(kind::HOST_IN_BUFFER) => {
                    match Shared::decode(&buf[..len]).map_err(malformed)? {
                        Shared::InBuffer { len, .. } => {
                            let len = len as usize;
                            let run = self
                                .lent
                                .run(len, |bytes| {
                                    bytes.get(1..9).map(|raw| {
                                        u64::from_le_bytes(raw.try_into().unwrap_or([0; 8]))
                                    })
                                })
                                .flatten()
                                .ok_or(PacketError::Truncated)?;
                            Ok(Incoming::Run { len, run })
                        }
                        _ => Ok(Incoming::Packet(len)),
                    }
                }
                _ => Ok(Incoming::Packet(len)),
            };
        }
        let mut stdin = io::stdin().lock();
        read_packet(&mut stdin, daedalus_wire::DEFAULT_CAP, buf)?;
        Ok(Incoming::Packet(buf.len()))
    }
}

/// The memory lent for a run, as [`Lent::view`] gives it.
pub struct View<'a> {
    #[cfg(target_os = "linux")]
    borrower: Option<&'a mut crate::Borrower>,
    #[cfg(not(target_os = "linux"))]
    _none: std::marker::PhantomData<&'a ()>,
}

impl View<'_> {
    /// What this worker did to the buffers it was sent by descriptor.
    pub fn brackets(&self) -> crate::Brackets {
        #[cfg(target_os = "linux")]
        {
            self.borrower
                .as_ref()
                .map(|b| b.brackets())
                .unwrap_or_default()
        }
        #[cfg(not(target_os = "linux"))]
        crate::Brackets::default()
    }

    /// Whether the host agreed values by reference.
    pub fn by_reference(&self) -> bool {
        #[cfg(target_os = "linux")]
        {
            self.borrower.as_ref().is_some_and(|b| b.by_reference())
        }
        #[cfg(not(target_os = "linux"))]
        false
    }

    /// The smallest output value worth writing into lent memory. A smaller one goes in the
    /// packet, where it costs less than its place.
    pub fn threshold(&self) -> usize {
        #[cfg(target_os = "linux")]
        {
            self.borrower.as_ref().map_or(usize::MAX, |b| b.threshold())
        }
        #[cfg(not(target_os = "linux"))]
        usize::MAX
    }

    /// Begins the node's reads of the buffer sent by descriptor in `slot`, with the kernel's
    /// sync call: before the node runs. Call [`View::end_read`] after it.
    pub fn begin_read(&mut self, slot: u32) -> Result<(), String> {
        #[cfg(target_os = "linux")]
        {
            match self.borrower.as_mut() {
                Some(borrower) => borrower.begin_read(slot).map_err(|e| e.to_string()),
                None => Err("nothing is lent".into()),
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = slot;
            Err("nothing is lent".into())
        }
    }

    /// Ends what [`View::begin_read`] began.
    pub fn end_read(&mut self, slot: u32) {
        #[cfg(target_os = "linux")]
        if let Some(borrower) = self.borrower.as_mut() {
            borrower.end_read(slot);
        }
        #[cfg(not(target_os = "linux"))]
        let _ = slot;
    }

    /// The node's inputs and its outputs, to use at once.
    pub fn split(&mut self) -> (Reads<'_>, Writes<'_>) {
        #[cfg(target_os = "linux")]
        if let Some(borrower) = self.borrower.as_mut() {
            let (reads, writes) = borrower.split();
            return (reads, writes);
        }
        (
            Reads {
                #[cfg(target_os = "linux")]
                view: None,
                #[cfg(target_os = "linux")]
                to_worker: 0,
                #[cfg(target_os = "linux")]
                fds: None,
                _of: std::marker::PhantomData,
            },
            Writes {
                #[cfg(target_os = "linux")]
                view: None,
                #[cfg(target_os = "linux")]
                out: None,
                #[cfg(target_os = "linux")]
                cursor: None,
                _of: std::marker::PhantomData,
            },
        )
    }
}

/// A run's inputs by reference.
pub struct Reads<'a> {
    #[cfg(target_os = "linux")]
    pub(crate) view: Option<&'a shm_ring_buffers::PoolView>,
    #[cfg(target_os = "linux")]
    pub(crate) to_worker: u32,
    #[cfg(target_os = "linux")]
    pub(crate) fds: Option<&'a crate::fds::FdTable>,
    pub(crate) _of: std::marker::PhantomData<&'a ()>,
}

impl<'a> Reads<'a> {
    /// The bytes `place` names, read-only, for as long as the view is held.
    pub fn bytes(&self, place: &daedalus_wire::shared::Place) -> Option<&'a [u8]> {
        #[cfg(target_os = "linux")]
        {
            use daedalus_wire::shared::At;
            let start = usize::try_from(place.offset).ok()?;
            let end = start.checked_add(usize::try_from(place.len).ok()?)?;
            match place.at {
                At::Lent(buffer) if buffer == self.to_worker => self
                    .view?
                    .read_bytes(shm_ring_buffers::BufferIndex(buffer))
                    .ok()?
                    .get(start..end),
                At::Lent(_) => None,
                At::Fd(slot) => self.fds?.bytes(slot, place.offset, place.len),
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = place;
            None
        }
    }
}

/// Room for a run's output values in lent memory.
pub struct Writes<'a> {
    #[cfg(target_os = "linux")]
    pub(crate) view: Option<&'a mut shm_ring_buffers::PoolView>,
    #[cfg(target_os = "linux")]
    pub(crate) out: Option<u32>,
    #[cfg(target_os = "linux")]
    pub(crate) cursor: Option<&'a mut usize>,
    pub(crate) _of: std::marker::PhantomData<&'a ()>,
}

impl Writes<'_> {
    /// Room for an output value of `len` bytes, and the place to send for it. None when the host
    /// named no buffer for this run or the value doesn't fit: then it goes in the packet.
    pub fn room(&mut self, len: usize) -> Option<(daedalus_wire::shared::Place, &mut [u8])> {
        #[cfg(target_os = "linux")]
        {
            use daedalus_wire::shared::{At, Place};
            let buffer = self.out?;
            let cursor = self.cursor.as_mut()?;
            let start = cursor.next_multiple_of(crate::linux::VALUE_ALIGN);
            let end = start.checked_add(len)?;
            let room = self
                .view
                .as_mut()?
                .write_bytes(shm_ring_buffers::BufferIndex(buffer))
                .ok()?
                .get_mut(start..end)?;
            **cursor = end;
            Some((
                Place {
                    at: At::Lent(buffer),
                    offset: start as u64,
                    len: len as u64,
                },
                room,
            ))
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = len;
            None
        }
    }
}
