//! The lock behind every `vkQueueSubmit` on a context's wgpu queue.
//!
//! Vulkan requires a queue's submissions to be externally synchronized, and wgpu-core serializes
//! its own submits behind locks nothing outside wgpu can take. So each wgpu context owns one
//! [`SubmitLock`], and Daedalus holds it around each call that can submit on the queue:
//!
//! - `queue.submit`, through [`LockedQueue::submit`];
//! - `queue.on_submitted_work_done` and `Buffer::map_async`: both flush `write_buffer` data
//!   first, and that flush is a submit whenever such data is pending (wgpu-core 30,
//!   `flush_pending_writes` and `flush_writes_for_buffer`), so they take the lock too.
//!
//! `RawSubmitter` (see `raw_vulkan`) takes the same lock around its own `vkQueueSubmit`, so raw
//! and wgpu submissions on the queue never overlap. Hold the guard only around the call that
//! submits, and never nest it: a callback Daedalus runs under the lock must not submit.
//!
//! The lock lives in the context, not in a static, so everything holding the same
//! [`crate::GpuContextHandle`] shares it: a plugin `cdylib` has its own copy of this crate (and of
//! its statics) but reaches the host's lock through the handle. Code outside Daedalus that submits
//! on the queue takes it with [`crate::GpuContextHandle::submit`] or
//! [`crate::GpuContextHandle::submit_lock`]. It is a `std` mutex because those waits park on the
//! mutex word itself (a futex); `parking_lot` parks in a table that is per copy of the crate.

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

/// A context's queue submit lock. Clones share the lock.
#[derive(Clone, Debug, Default)]
pub struct SubmitLock(Arc<Mutex<()>>);

/// Held while submitting; dropping it releases the [`SubmitLock`].
#[must_use = "the lock is released when the guard drops"]
#[derive(Debug)]
pub struct SubmitGuard<'a>(#[allow(dead_code)] MutexGuard<'a, ()>);

impl SubmitLock {
    /// Take the lock. A panic under it does not poison it: the queue holds no state the lock
    /// protects.
    pub fn lock(&self) -> SubmitGuard<'_> {
        SubmitGuard(self.0.lock().unwrap_or_else(PoisonError::into_inner))
    }

    /// Whether `self` and `other` are the same lock.
    pub fn same_lock(&self, other: &SubmitLock) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

/// A context's wgpu queue with its submit lock: every call through it that can submit takes the
/// lock. Calls that never submit (`write_buffer`, `write_texture`) go through [`Self::raw`].
#[derive(Clone, Debug)]
pub(crate) struct LockedQueue {
    queue: wgpu::Queue,
    lock: SubmitLock,
}

impl LockedQueue {
    pub(crate) fn new(queue: wgpu::Queue, lock: SubmitLock) -> Self {
        Self { queue, lock }
    }

    /// The queue, for calls that do not submit. Submitting on it directly bypasses the lock.
    pub(crate) fn raw(&self) -> &wgpu::Queue {
        &self.queue
    }

    pub(crate) fn submit_lock(&self) -> &SubmitLock {
        &self.lock
    }

    /// `queue.submit(commands)` under the lock.
    pub(crate) fn submit(
        &self,
        commands: impl IntoIterator<Item = wgpu::CommandBuffer>,
    ) -> wgpu::SubmissionIndex {
        let _guard = self.lock.lock();
        self.queue.submit(commands)
    }

    /// `queue.on_submitted_work_done` under the lock (it may flush pending writes).
    pub(crate) fn on_submitted_work_done(&self, callback: impl FnOnce() + Send + 'static) {
        let _guard = self.lock.lock();
        self.queue.on_submitted_work_done(callback);
    }

    /// `slice.map_async` under the lock (it may flush pending writes).
    pub(crate) fn map_async(
        &self,
        slice: wgpu::BufferSlice<'_>,
        mode: wgpu::MapMode,
        callback: impl FnOnce(Result<(), wgpu::BufferAsyncError>) + wgpu::WasmNotSend + 'static,
    ) {
        let _guard = self.lock.lock();
        slice.map_async(mode, callback);
    }
}
