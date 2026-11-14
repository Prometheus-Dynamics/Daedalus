//! Counts of what the backend did: starts, restarts, faults, runs, bytes, stderr bytes, and log
//! lines.

use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub struct Telemetry {
    spawns: AtomicU64,
    restarts: AtomicU64,
    faults: AtomicU64,
    runs: AtomicU64,
    bytes_in: AtomicU64,
    bytes_out: AtomicU64,
    stderr_bytes: AtomicU64,
    logs: AtomicU64,
    grants: AtomicU64,
    shared_in: AtomicU64,
    shared_out: AtomicU64,
    plain_workers: AtomicU64,
}

/// A copy of the counts at one time.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Snapshot {
    pub spawns: u64,
    pub restarts: u64,
    pub faults: u64,
    pub runs: u64,
    pub bytes_in: u64,
    pub bytes_out: u64,
    pub stderr_bytes: u64,
    /// Log messages the workers sent.
    pub logs: u64,
    /// Lendings of shared memory to a worker: one at each start of a worker that asked, and one
    /// each time a larger value made the host lend bigger buffers.
    pub grants: u64,
    /// Runs sent in a shared buffer instead of on the stream.
    pub shared_in: u64,
    /// Outputs a worker wrote in a shared buffer instead of on the stream.
    pub shared_out: u64,
    /// Worker starts that stayed on the plain path: the worker did not ask, or the host could
    /// not lend. A worker on a path of large values that shows up here copies them through its
    /// stream.
    pub plain_workers: u64,
}

#[derive(Clone, Copy)]
pub(crate) enum Count {
    Spawn,
    Restart,
    Fault,
    Run,
    BytesIn(u64),
    BytesOut(u64),
    Stderr(u64),
    Log,
    Grant,
    SharedIn,
    SharedOut,
    Plain,
}

impl Telemetry {
    pub(crate) fn bump(&self, count: Count) {
        let (field, by) = match count {
            Count::Spawn => (&self.spawns, 1),
            Count::Restart => (&self.restarts, 1),
            Count::Fault => (&self.faults, 1),
            Count::Run => (&self.runs, 1),
            Count::BytesIn(n) => (&self.bytes_in, n),
            Count::BytesOut(n) => (&self.bytes_out, n),
            Count::Stderr(n) => (&self.stderr_bytes, n),
            Count::Log => (&self.logs, 1),
            Count::Grant => (&self.grants, 1),
            Count::SharedIn => (&self.shared_in, 1),
            Count::SharedOut => (&self.shared_out, 1),
            Count::Plain => (&self.plain_workers, 1),
        };
        field.fetch_add(by, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> Snapshot {
        Snapshot {
            spawns: self.spawns.load(Ordering::Relaxed),
            restarts: self.restarts.load(Ordering::Relaxed),
            faults: self.faults.load(Ordering::Relaxed),
            runs: self.runs.load(Ordering::Relaxed),
            bytes_in: self.bytes_in.load(Ordering::Relaxed),
            bytes_out: self.bytes_out.load(Ordering::Relaxed),
            stderr_bytes: self.stderr_bytes.load(Ordering::Relaxed),
            logs: self.logs.load(Ordering::Relaxed),
            grants: self.grants.load(Ordering::Relaxed),
            shared_in: self.shared_in.load(Ordering::Relaxed),
            shared_out: self.shared_out.load(Ordering::Relaxed),
            plain_workers: self.plain_workers.load(Ordering::Relaxed),
        }
    }
}
