//! What the socket path allocates. The reader thread of each connection
//! counts its allocations for each request, from the read of the packet to the queued answer, by
//! the request's op; the writer thread counts them for each packet it encodes and writes. Only
//! those two threads are counted: a body that runs on a thread of its own is not.

use std::sync::atomic::{AtomicU64, Ordering};

use daedalus_wire::service::OpKind;

/// Counts kept in place, so recording one allocates nothing.
pub(crate) struct SocketProbe {
    requests: [AtomicU64; 256],
    reader: [AtomicU64; 256],
    packets: AtomicU64,
    writer: AtomicU64,
}

impl SocketProbe {
    pub(crate) fn new() -> SocketProbe {
        SocketProbe {
            requests: std::array::from_fn(|_| AtomicU64::new(0)),
            reader: std::array::from_fn(|_| AtomicU64::new(0)),
            packets: AtomicU64::new(0),
            writer: AtomicU64::new(0),
        }
    }

    pub(crate) fn request(&self, kind: Option<OpKind>, allocations: usize) {
        let index = kind.map_or(0, |kind| kind as usize);
        self.requests[index].fetch_add(1, Ordering::Relaxed);
        self.reader[index].fetch_add(allocations as u64, Ordering::Relaxed);
    }

    pub(crate) fn packet(&self, allocations: usize) {
        self.packets.fetch_add(1, Ordering::Relaxed);
        self.writer.fetch_add(allocations as u64, Ordering::Relaxed);
    }

    pub(crate) fn snapshot(&self) -> SocketAllocations {
        SocketAllocations {
            requests: std::array::from_fn(|i| self.requests[i].load(Ordering::Relaxed)),
            reader: std::array::from_fn(|i| self.reader[i].load(Ordering::Relaxed)),
            packets: self.packets.load(Ordering::Relaxed),
            writer: self.writer.load(Ordering::Relaxed),
        }
    }
}

/// What the socket path allocated, counted since the host was built.
#[derive(Clone, Debug)]
pub struct SocketAllocations {
    requests: [u64; 256],
    reader: [u64; 256],
    /// Packets the writer threads wrote.
    pub packets: u64,
    /// What the writer threads allocated, encoding and writing them.
    pub writer: u64,
}

impl SocketAllocations {
    /// Requests of this op the reader threads served.
    pub fn requests(&self, kind: OpKind) -> u64 {
        self.requests[kind as usize]
    }

    /// Requests the reader threads served, of every op and those that didn't decode.
    pub fn all_requests(&self) -> u64 {
        self.requests.iter().sum()
    }

    /// What the reader threads allocated serving requests of this op: reading, decoding,
    /// dispatching, encoding the answer and queueing it.
    pub fn reader(&self, kind: OpKind) -> u64 {
        self.reader[kind as usize]
    }

    /// The counts since `earlier`.
    pub fn since(&self, earlier: &SocketAllocations) -> SocketAllocations {
        SocketAllocations {
            requests: std::array::from_fn(|i| self.requests[i] - earlier.requests[i]),
            reader: std::array::from_fn(|i| self.reader[i] - earlier.reader[i]),
            packets: self.packets - earlier.packets,
            writer: self.writer - earlier.writer,
        }
    }
}

/// Runs `body`, counting what this thread allocates in it.
pub(crate) fn measured<R>(body: impl FnOnce() -> R) -> (R, usize) {
    daedalus_abi::counting::counted(body)
}
