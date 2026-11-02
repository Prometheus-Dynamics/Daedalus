//! The lock behind every `vkQueueSubmit` Daedalus makes on its wgpu queue.
//!
//! Vulkan requires a queue's submissions to be externally synchronized, and wgpu-core serializes
//! its own submits behind locks nothing outside wgpu can take. So Daedalus holds this one
//! process-wide lock around each call that can submit on its queue:
//!
//! - `queue.submit`, through [`submit_locked`];
//! - `queue.on_submitted_work_done` and `Buffer::map_async`: both flush `write_buffer` data
//!   first, and that flush is a submit whenever such data is pending (wgpu-core 30,
//!   `flush_pending_writes` and `flush_writes_for_buffer`), so they take the lock too.
//!
//! `RawSubmitter` (see `raw_vulkan`) takes the same lock around its own `vkQueueSubmit`, so raw
//! and wgpu submissions on the queue never overlap. Hold the guard only around the call that
//! submits, and never nest it: a callback Daedalus runs under the lock must not submit.

use parking_lot::{Mutex, MutexGuard};

static QUEUE_SUBMIT: Mutex<()> = Mutex::new(());

/// The queue's submit lock.
pub(crate) fn lock() -> MutexGuard<'static, ()> {
    QUEUE_SUBMIT.lock()
}

/// `submit_locked(queue, commands)` under the queue's submit lock.
pub(crate) fn submit_locked(
    queue: &wgpu::Queue,
    commands: impl IntoIterator<Item = wgpu::CommandBuffer>,
) -> wgpu::SubmissionIndex {
    let _guard = lock();
    queue.submit(commands)
}

/// `Queue::on_submitted_work_done` under the queue's submit lock (it may flush pending writes).
pub(crate) trait WorkDoneLocked {
    fn on_submitted_work_done_locked(&self, callback: impl FnOnce() + Send + 'static);
}

impl WorkDoneLocked for wgpu::Queue {
    fn on_submitted_work_done_locked(&self, callback: impl FnOnce() + Send + 'static) {
        let _guard = lock();
        self.on_submitted_work_done(callback);
    }
}

/// `BufferSlice::map_async` under the queue's submit lock (it may flush pending writes).
pub(crate) trait MapAsyncLocked {
    fn map_async_locked(
        self,
        mode: wgpu::MapMode,
        callback: impl FnOnce(Result<(), wgpu::BufferAsyncError>) + wgpu::WasmNotSend + 'static,
    );
}

impl MapAsyncLocked for wgpu::BufferSlice<'_> {
    fn map_async_locked(
        self,
        mode: wgpu::MapMode,
        callback: impl FnOnce(Result<(), wgpu::BufferAsyncError>) + wgpu::WasmNotSend + 'static,
    ) {
        let _guard = lock();
        self.map_async(mode, callback);
    }
}
