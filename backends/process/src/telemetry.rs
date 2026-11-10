//! Counts of what the backend did: starts, restarts, faults, runs, bytes, and stderr bytes.

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
        }
    }
}
