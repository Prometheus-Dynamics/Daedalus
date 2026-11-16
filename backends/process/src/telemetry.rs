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
    agreed: AtomicU64,
    placed_in: AtomicU64,
    placed_out: AtomicU64,
    passed_in: AtomicU64,
    fd_sends: AtomicU64,
    outs_full: AtomicU64,
    refused: AtomicU64,
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
    /// Lendings that also agreed values by reference: the worker asked for it.
    pub agreed: u64,
    /// Input values sent by reference: a place in lent memory or in a buffer by descriptor,
    /// instead of their bytes.
    pub placed_in: u64,
    /// Output values a worker wrote into lent memory that a cell then held, with no copy.
    pub placed_out: u64,
    /// Input values that were in lent memory already, sent as the place they are at with no copy:
    /// a node on the host wrote them in room the worker's memory lent it, or the worker wrote them
    /// as an earlier output. Counted in `placed_in` too.
    pub passed_in: u64,
    /// Descriptors sent to workers. A buffer goes once, so a steady run sends none.
    pub fd_sends: u64,
    /// Runs with every value buffer still held by cells: the worker sent that run's output
    /// values in its packet.
    pub outs_full: u64,
    /// Starts refused because the worker's files did not pass the check against the lock.
    pub refused: u64,
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
    // Lending memory is Linux only.
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    Grant,
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    SharedIn,
    SharedOut,
    Plain,
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    Agree,
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    PlacedIn,
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    PlacedOut,
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    PassedIn,
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    FdSends(u64),
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    OutsFull,
    Refused,
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
            Count::Agree => (&self.agreed, 1),
            Count::PlacedIn => (&self.placed_in, 1),
            Count::PlacedOut => (&self.placed_out, 1),
            Count::PassedIn => (&self.passed_in, 1),
            Count::FdSends(n) => (&self.fd_sends, n),
            Count::OutsFull => (&self.outs_full, 1),
            Count::Refused => (&self.refused, 1),
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
            agreed: self.agreed.load(Ordering::Relaxed),
            placed_in: self.placed_in.load(Ordering::Relaxed),
            placed_out: self.placed_out.load(Ordering::Relaxed),
            passed_in: self.passed_in.load(Ordering::Relaxed),
            fd_sends: self.fd_sends.load(Ordering::Relaxed),
            outs_full: self.outs_full.load(Ordering::Relaxed),
            refused: self.refused.load(Ordering::Relaxed),
        }
    }
}
