//! Keeps a panic from crossing a boundary. Entry points call these, never `catch_unwind`.

use crate::Status;

/// What a caught panic said: the message it was raised with, or a fixed text when it carried
/// something that isn't text.
#[cfg(feature = "std")]
pub fn panic_message(payload: &(dyn core::any::Any + Send)) -> &str {
    if let Some(text) = payload.downcast_ref::<&str>() {
        text
    } else if let Some(text) = payload.downcast_ref::<std::string::String>() {
        text
    } else {
        "a panic with no message"
    }
}

/// Runs a step's body. With `std`, a panic becomes a fault whose reason is what the panic said.
/// A node the body named before it panicked stays named. Without `std` the body just runs,
/// since those targets build with `panic = abort`.
#[cfg(feature = "std")]
#[inline]
pub fn guard_step(
    cx: &mut crate::StepCx,
    body: impl FnOnce(&mut crate::StepCx) -> crate::StepEnd,
) -> crate::StepEnd {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| body(&mut *cx))) {
        Ok(end) => end,
        Err(payload) => {
            cx.fault_panic(panic_message(&*payload));
            crate::StepEnd::Fault
        }
    }
}

/// Runs a step's body. Without `std` it just runs, since those targets build with
/// `panic = abort`.
#[cfg(not(feature = "std"))]
#[inline]
pub fn guard_step(
    cx: &mut crate::StepCx,
    body: impl FnOnce(&mut crate::StepCx) -> crate::StepEnd,
) -> crate::StepEnd {
    body(cx)
}

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
