//! Keeps a panic from crossing a boundary. Entry points call these, never `catch_unwind`.

use crate::Status;

/// Runs `body`. With `std`, a panic becomes [`Status::PANIC`].
pub fn guard(body: impl FnOnce() -> Status) -> Status {
    guard_or(Status::PANIC, body)
}

/// Runs `body`. With `std`, a panic becomes `on_panic`. Without it the body just runs, since
/// those targets build with `panic = abort`.
#[cfg(feature = "std")]
#[inline]
pub fn guard_or<R>(on_panic: R, body: impl FnOnce() -> R) -> R {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(body)).unwrap_or(on_panic)
}

/// Runs `body`. With `std`, a panic becomes `on_panic`. Without it the body just runs, since
/// those targets build with `panic = abort`.
#[cfg(not(feature = "std"))]
#[inline]
pub fn guard_or<R>(_on_panic: R, body: impl FnOnce() -> R) -> R {
    body()
}
