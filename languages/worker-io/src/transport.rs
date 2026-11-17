//! The worker's reading side: a reader thread that takes the host's packets in order, the shared
//! memory ask, and the cancel flag. Every SDK that serves the protocol on stdio uses this one: the
//! reader forwards each packet, a cancel that names the run going on sets the flag a node polls, and
//! a run in lent memory is handed over with its bytes.
//!
//! [`Transport::stdio`] is a worker on its stdin and stdout. [`Transport::plain`] reads any stream
//! with the plain packet framing, with no shared memory: it is what a test serves over a buffer.

use std::io::{self, Read, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, channel};
use std::thread;

use daedalus_process_shm::{Inbox, Incoming, Lent, PLAIN_LINE};
use daedalus_wire::codec::Writer;
use daedalus_wire::{
    DEFAULT_CAP, LogLevel, PacketError, WorkerMsg, kind, read_packet, shared, write_packet,
};

/// The features a stdio worker asks the host for: values by reference of any type. A host that
/// agrees sends large values in lent memory; one that does not, or is older, keeps to packets.
pub const FEATURES: u32 = shared::BY_REFERENCE | shared::ANY_TYPE;

/// What the host sent, as the worker takes it.
pub enum Arrival<T> {
    /// A message, read by the caller's function from the packet's bytes.
    Message(T),
    /// The host lent memory: the caller sets its schema's features from [`Transport::lent`].
    Granted,
}

/// A packet the reader thread passes on.
enum Packet {
    Bytes(Vec<u8>),
    /// A run in the lent buffer, this many bytes.
    Run(usize),
    /// The host answered the ask: lent memory, or the reason it lends none.
    Granted,
    Plain(String),
}

/// Where the reader thread takes its packets from.
enum Source {
    Inbox(Inbox),
    Reader(Box<dyn Read + Send>),
}

impl Source {
    fn next(&mut self, buf: &mut Vec<u8>) -> Result<Incoming, PacketError> {
        match self {
            Source::Inbox(inbox) => inbox.next(buf),
            Source::Reader(reader) => {
                read_packet(reader, DEFAULT_CAP, buf)?;
                Ok(Incoming::Packet(buf.len()))
            }
        }
    }
}

/// The worker's view of the host: its packets in order, its cancel flag, and the memory lent to it.
pub struct Transport {
    lent: Lent,
    cancel: Arc<AtomicBool>,
    packets: Receiver<Result<Packet, PacketError>>,
    /// Until the host answers the ask, a run means it never will: an older host. The worker says
    /// so once, on the first run.
    unanswered: bool,
}

impl Transport {
    /// The transport of a worker on its stdio: it asks the host for [`FEATURES`] on `out`, which
    /// is the packet stream, after the hello. A stdin that is not a socket gets no shared memory.
    pub fn stdio(out: &mut impl Write) -> io::Result<Transport> {
        let lent = Lent::default();
        let unanswered = lent.ask_for(FEATURES, out).map_err(io::Error::other)?;
        let inbox = Inbox::new(lent.clone());
        Ok(Transport::spawn(lent, Source::Inbox(inbox), unanswered))
    }

    /// The transport of a worker that reads `input` with plain packets: no shared memory is asked.
    pub fn plain(input: Box<dyn Read + Send>) -> Transport {
        Transport::spawn(Lent::default(), Source::Reader(input), false)
    }

    fn spawn(lent: Lent, source: Source, unanswered: bool) -> Transport {
        let current = Arc::new(AtomicU64::new(0));
        let cancel = Arc::new(AtomicBool::new(false));
        let packets = read_in_thread(current, cancel.clone(), source);
        Transport {
            lent,
            cancel,
            packets,
            unanswered,
        }
    }

    /// The memory the host lent: a run's inputs are read through it, and outputs sent through it.
    pub fn lent(&self) -> &Lent {
        &self.lent
    }

    /// Whether the host asked the run going on to stop. A node polls it.
    pub fn cancelled(&self) -> bool {
        self.cancel.load(Ordering::SeqCst)
    }

    /// The cancel flag itself, for a node's context to hold while it runs.
    pub fn cancel_flag(&self) -> Arc<AtomicBool> {
        self.cancel.clone()
    }

    /// The next thing the host sent, or `None` when it closed the stream. A message's bytes are
    /// read by `decode`, inside the lent memory's lock when the message is a run in lent memory, so
    /// `decode` must not reach the lent memory itself. A packet the worker cannot frame is an error.
    pub fn next<T>(
        &mut self,
        out: &mut impl Write,
        mut decode: impl FnMut(&[u8]) -> T,
    ) -> io::Result<Option<Arrival<T>>> {
        loop {
            let packet = match self.packets.recv() {
                Ok(Ok(packet)) => packet,
                Ok(Err(PacketError::Closed)) | Err(_) => return Ok(None),
                Ok(Err(error)) => return Err(io::Error::other(error)),
            };
            match packet {
                Packet::Bytes(bytes) => {
                    if self.unanswered && bytes.first() == Some(&kind::RUN) {
                        self.unanswered = false;
                        log(out, PLAIN_LINE)?;
                    }
                    return Ok(Some(Arrival::Message(decode(&bytes))));
                }
                Packet::Run(len) => {
                    if let Some(message) = self.lent.run(len, &mut decode) {
                        return Ok(Some(Arrival::Message(message)));
                    }
                }
                Packet::Granted => {
                    self.unanswered = false;
                    return Ok(Some(Arrival::Granted));
                }
                Packet::Plain(reason) => {
                    self.unanswered = false;
                    log(out, &format!("{PLAIN_LINE}: {reason}"))?;
                }
            }
        }
    }
}

/// Sends one log line to the host, at info.
fn log(out: &mut impl Write, text: &str) -> io::Result<()> {
    let mut writer = Writer::new();
    WorkerMsg::Log {
        level: LogLevel::Info,
        text: text.to_owned(),
    }
    .encode(&daedalus_wire::Schema::new(), &mut writer)
    .map_err(|error| io::Error::other(error.to_string()))?;
    write_packet(out, &writer.buf, DEFAULT_CAP).map_err(io::Error::other)
}

/// The reader thread. It forwards every packet in order. A run starts a new cancel window, and a
/// cancel that names the run going on sets the cancel flag, so a cancel that arrives right after
/// its run is never lost.
fn read_in_thread(
    current: Arc<AtomicU64>,
    cancel: Arc<AtomicBool>,
    mut source: Source,
) -> Receiver<Result<Packet, PacketError>> {
    let (tx, rx) = channel();
    thread::spawn(move || {
        let mut buf = Vec::new();
        loop {
            let len = match source.next(&mut buf) {
                Ok(Incoming::Packet(len)) => len,
                Ok(Incoming::Run { len, run }) => {
                    current.store(run, Ordering::SeqCst);
                    cancel.store(false, Ordering::SeqCst);
                    if tx.send(Ok(Packet::Run(len))).is_err() {
                        return;
                    }
                    continue;
                }
                Ok(Incoming::Granted) => {
                    if tx.send(Ok(Packet::Granted)).is_err() {
                        return;
                    }
                    continue;
                }
                Ok(Incoming::Plain(reason)) => {
                    if tx.send(Ok(Packet::Plain(reason))).is_err() {
                        return;
                    }
                    continue;
                }
                Err(error) => {
                    let _ = tx.send(Err(error));
                    return;
                }
            };
            buf.truncate(len);
            let run_id = buf
                .get(1..9)
                .map(|raw| u64::from_le_bytes(raw.try_into().unwrap_or([0; 8])));
            match (buf.first(), run_id) {
                (Some(&kind::RUN), Some(run)) => {
                    current.store(run, Ordering::SeqCst);
                    cancel.store(false, Ordering::SeqCst);
                }
                (Some(&kind::CANCEL), Some(run)) if run == current.load(Ordering::SeqCst) => {
                    cancel.store(true, Ordering::SeqCst);
                }
                _ => {}
            }
            let packet = std::mem::take(&mut buf);
            if tx.send(Ok(Packet::Bytes(packet))).is_err() {
                return;
            }
        }
    });
    rx
}
