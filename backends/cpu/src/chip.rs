//! What generated firmware runs on. No heap.

mod clock;
mod errors;
#[cfg(feature = "chip-params")]
mod param;
#[cfg(feature = "chip-params")]
mod scalar;
#[cfg(feature = "chip-params")]
pub mod wire;

use daedalus_abi::{FixedError, StepCx};

#[cfg(feature = "chip-params")]
pub use crate::scalar_kind::ScalarKind;
pub use clock::Clock;
pub use daedalus_runner::{FixedQueue as Queue, Overflow, QueueFull};
pub use errors::ChipError;
#[cfg(feature = "chip-params")]
pub use param::{ParamError, ParamSpec, ParamUpdate, Tunable, checked};
#[cfg(feature = "chip-params")]
pub use scalar::{Scalar, ScalarType};

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
