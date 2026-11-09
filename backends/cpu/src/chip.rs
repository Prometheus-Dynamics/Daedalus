//! What generated firmware runs on. No heap.

mod clock;
mod errors;

use daedalus_abi::{FixedError, StepCx};

pub use clock::Clock;
pub use daedalus_runner::{FixedQueue as Queue, Overflow, QueueFull};
pub use errors::ChipError;

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

/// The context of a tick at time `now_ns`.
#[inline]
pub fn context(now_ns: u64) -> StepCx {
    let mut cx = StepCx::new();
    cx.now_ns = now_ns;
    cx
}
