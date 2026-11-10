//! The two locks this crate's calls run under. It owns neither.

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

/// Proof that the process-wide driver lock is held. The crate that owns the lock implements this
/// for its guard, and for nothing else.
pub trait DriverHeld {}

/// The lock around every submit on one device's queue.
///
/// Vulkan wants a queue's submits serialized by the caller, and wgpu serializes its own behind
/// locks nothing outside it can take. So whoever holds a device holds one of these, and takes it
/// around each wgpu call that can submit (`Queue::submit`, `on_submitted_work_done` and
/// `map_async`, which flush pending writes) as well as each raw submit made here. Hold it only
/// around the call, and never nest it. Clones share the lock.
#[derive(Clone, Debug, Default)]
pub struct SubmitLock(Arc<Mutex<()>>);

/// Held while submitting.
#[must_use = "the lock is released when the guard drops"]
#[derive(Debug)]
pub struct SubmitGuard<'a>(#[allow(dead_code)] MutexGuard<'a, ()>);

impl SubmitLock {
    /// A panic under the lock doesn't poison it: it protects no state of its own.
    pub fn lock(&self) -> SubmitGuard<'_> {
        SubmitGuard(self.0.lock().unwrap_or_else(PoisonError::into_inner))
    }
}
