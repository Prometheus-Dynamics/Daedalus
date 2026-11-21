//! The device's fence, attached to every dma-buf it uses in place.
//!
//! A Vulkan driver need not attach its work to an imported dma-buf (v3dv attaches nothing), so a
//! party outside the host's step ordering would see the buffer before the device is done with it:
//! a worker given it by descriptor, the host's caller, a CPU mapping inside the kernel's sync
//! bracket, another device, or the producer writing it again. So after every submit that used
//! such a buffer, the backend signals a sync fd once that submit is done and attaches it to the
//! buffer (`DMA_BUF_IOCTL_IMPORT_SYNC_FILE`): as a write where the device may have written the
//! buffer, as a read where its fd only allows reading. Everyone who honours a dma-buf's fences then
//! waits for the device, with nothing handed over beside the buffer.
//!
//! Where no sync fd can be had, the backend waits on the host for the device instead, and counts
//! it with its reason in [`SyncStats`]. That path is slower and still right.
//!
//! What the backend guarantees for a value made from an outside dma-buf: the value's owner (for a
//! frame, its lease on the producer's ring) is let go no earlier than the last submit that read or
//! wrote the buffer has finished, even when the value itself is dropped while that submit is
//! pending.

use std::any::Any;
use std::cell::RefCell;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use daedalus_dma_sys::{Access, DmaBuf, SyncFence};

use crate::context::Context;

/// A dma-buf from outside, under one value's buffer on the device.
pub(crate) struct External {
    /// The buffer, by an fd of its own.
    pub(crate) dmabuf: DmaBuf,
    /// How the device's work is attached: `Read` where the value's fd only allows reading.
    pub(crate) access: Access,
    /// What keeps the value's memory its own, such as a lease on its producer's ring. Let go
    /// only once every submit that used the buffer has finished.
    _owner: Option<Arc<dyn Any + Send + Sync>>,
    /// The device the buffer was imported on.
    context: u64,
}

impl External {
    pub(crate) fn new(
        dmabuf: DmaBuf,
        read_only: bool,
        owner: Option<Arc<dyn Any + Send + Sync>>,
        context: &Context,
    ) -> External {
        External {
            dmabuf,
            access: if read_only {
                Access::Read
            } else {
                Access::Write
            },
            _owner: owner,
            context: context.key,
        }
    }
}

thread_local! {
    /// Outside buffers this thread has given a device since its last submit there. Recording
    /// and submitting happen on one thread, so a submit attaches to exactly what it was given;
    /// one recorded and never submitted is attached at the thread's next submit, which is later
    /// and so still right.
    static TOUCHED: RefCell<Vec<Arc<External>>> = const { RefCell::new(Vec::new()) };
}

/// Notes that `external` is bound into work being recorded on this thread.
pub(crate) fn touch(external: &Arc<External>) {
    TOUCHED.with_borrow_mut(|touched| {
        if !touched.iter().any(|have| Arc::ptr_eq(have, external)) {
            touched.push(Arc::clone(external));
        }
    });
}

/// Attaches the device's fence to every outside buffer this thread gave `context` since its last
/// submit there. Called right after each submit, with the submit lock let go. Costs one
/// thread-local look when there were none.
pub(crate) fn after_submit(context: &Context) {
    let taken: Vec<Arc<External>> = TOUCHED.with_borrow_mut(|touched| {
        if touched.is_empty() {
            return Vec::new();
        }
        let (mine, rest) = std::mem::take(touched)
            .into_iter()
            .partition(|external| external.context == context.key);
        *touched = rest;
        mine
    });
    if taken.is_empty() {
        return;
    }
    let counts = &context.sync;
    // The owners stay until the device has finished what was submitted so far: a fence signal
    // covers every earlier submit on the queue, raw ones included.
    let held = taken.clone();
    context.queue.on_submitted_work_done(move || drop(held));
    if context.no_fence_attach {
        counts.skipped.fetch_add(1, Ordering::Relaxed);
        return;
    }
    let started = std::time::Instant::now();
    let signalled = daedalus_vk_sys::semaphore::signal_sync_fd(
        &context.device,
        &context.queue,
        &context.submit,
    );
    let fence = match signalled {
        Ok(Some(fd)) => SyncFence::from_fd(fd).map_err(|error| error.to_string()),
        // The work had already finished: nothing is left for anyone to wait on.
        Ok(None) => {
            counts.already_done.fetch_add(1, Ordering::Relaxed);
            counts.note_time(started);
            return;
        }
        Err(error) => Err(error.to_string()),
    };
    let attached = fence.and_then(|fence| {
        for external in &taken {
            external
                .dmabuf
                .as_ref()
                .import_fence(&fence, external.access)
                .map_err(|error| format!("the kernel refused the fence: {error}"))?;
        }
        Ok(())
    });
    match attached {
        Ok(()) => {
            counts.attaches.fetch_add(1, Ordering::Relaxed);
            counts
                .buffers
                .fetch_add(taken.len() as u64, Ordering::Relaxed);
        }
        Err(why) => {
            // Nothing to attach: the device is waited for here, before anyone sees the buffers.
            counts.host_waits.fetch_add(1, Ordering::Relaxed);
            *counts.why.lock().unwrap_or_else(PoisonError::into_inner) = Some(why);
            let _ = context.wait("a dma-buf the device used, with no fence to attach");
        }
    }
    counts.note_time(started);
}

/// Waits, on the host, for what a dma-buf's producer still has pending on it before the device
/// is given the buffer. A CPU producer has nothing pending, which costs one ioctl and one poll.
pub(crate) fn before_first_use(context: &Context, dmabuf: &DmaBuf) {
    let Ok(fence) = dmabuf.as_ref().export_fence(Access::Read) else {
        // A kernel before 6.0 has no export: there is nothing to wait on.
        return;
    };
    if fence.is_signaled().unwrap_or(true) {
        return;
    }
    context.sync.producer_waits.fetch_add(1, Ordering::Relaxed);
    let _ = fence.wait(context.wait_timeout());
}

/// The counts behind [`SyncStats`].
#[derive(Default)]
pub(crate) struct Counters {
    attaches: AtomicU64,
    buffers: AtomicU64,
    already_done: AtomicU64,
    host_waits: AtomicU64,
    producer_waits: AtomicU64,
    skipped: AtomicU64,
    nanos: AtomicU64,
    why: Mutex<Option<String>>,
}

impl Counters {
    fn note_time(&self, started: std::time::Instant) {
        let nanos = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
        self.nanos.fetch_add(nanos, Ordering::Relaxed);
    }

    pub(crate) fn stats(&self) -> SyncStats {
        SyncStats {
            attaches: self.attaches.load(Ordering::Relaxed),
            buffers: self.buffers.load(Ordering::Relaxed),
            already_done: self.already_done.load(Ordering::Relaxed),
            host_waits: self.host_waits.load(Ordering::Relaxed),
            host_wait_reason: self
                .why
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .clone(),
            producer_waits: self.producer_waits.load(Ordering::Relaxed),
            skipped: self.skipped.load(Ordering::Relaxed),
            host_nanos: self.nanos.load(Ordering::Relaxed),
        }
    }
}

/// How a device's work on dma-bufs from outside was made visible to everyone else. See
/// [`WgpuBackend::sync_stats`](crate::WgpuBackend::sync_stats).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SyncStats {
    /// Submits whose fence was attached to the buffers they used.
    pub attaches: u64,
    /// Buffers those fences were attached to, one per buffer per submit.
    pub buffers: u64,
    /// Submits that had already finished when their fence was asked for, so nothing was attached.
    pub already_done: u64,
    /// Submits whose fence couldn't be had or attached, where the host waited for the device.
    pub host_waits: u64,
    /// Why the last host wait happened.
    pub host_wait_reason: Option<String>,
    /// Buffers whose producer still had work pending when the device was given them, waited for
    /// on the host.
    pub producer_waits: u64,
    /// Submits that attached nothing because the host turned attaching off.
    pub skipped: u64,
    /// Host time spent signalling and attaching, in nanoseconds, over all submits.
    pub host_nanos: u64,
}
