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

    /// Calls `with` on the run in the lent buffer, the first `len` bytes. None when nothing is
    /// lent or the run is not inside the buffer.
    pub fn run<T>(&self, len: usize, with: impl FnOnce(&[u8]) -> T) -> Option<T> {
        #[cfg(target_os = "linux")]
        {
            let guard = self.lock();
            let bytes = guard.as_ref()?.packet(crate::linux::TO_WORKER, len)?;
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
