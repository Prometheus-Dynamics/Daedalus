//! What generated firmware runs on. No heap.

use daedalus_abi::FixedError;

pub use daedalus_runner::{FixedQueue as Queue, Overflow};

/// Why a tick or a port call stopped. Small enough to return from anywhere.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ChipError {
    /// A queue with the error policy was full.
    QueueFull {
        edge: u16,
    },
    /// A node failed. `code` is the code of its error.
    Node {
        node: u16,
        code: u32,
    },
    Port {
        port: u16,
    },
}

/// Where a graph reads the time from.
pub trait Clock {
    fn now_ns(&self) -> u64;
}

/// Pushes to edge `edge`.
#[inline]
pub fn push<T: Copy, const N: usize>(
    queue: &mut Queue<T, N>,
    value: T,
    overflow: Overflow,
    edge: u16,
) -> Result<(), ChipError> {
    queue
        .push(value, overflow)
        .map_err(|_| ChipError::QueueFull { edge })
}

/// The error a failed node returns from a tick.
#[inline]
pub fn node_error<E: FixedError>(node: u16, error: &E) -> ChipError {
    ChipError::Node {
        node,
        code: error.code(),
    }
}

/// A time for nodes that take one.
#[inline]
pub fn context(now_ns: u64) -> daedalus_abi::StepCx {
    let mut cx = daedalus_abi::StepCx::new();
    cx.now_ns = now_ns;
    cx
}
