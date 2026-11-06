//! The frame entry points: push a dma-buf frame into an input port, take one from an output port,
//! and release a taken frame's lease.
//!
//! A frame crosses as a `lemnos-abi` [`BufferDesc`] (one dma-buf descriptor per plane) with a
//! [`FenceDesc`], and the surface holds it as a `daedalus:frame` (v3) [`SharedFrame`], so the
//! pixels never move. Linux is the tested platform: the dma-buf work is in [`dma`], through
//! `lemnos-linux`. Elsewhere the calls return [`Status::NOT_SUPPORTED`] and touch nothing.
//!
//! Descriptor ownership follows `lemnos-abi`: a push borrows the caller's descriptors for the call
//! unless `BufferDesc::OWNED` is set, in which case the surface takes them (and closes them on any
//! failure). A take always hands out owned, duplicated descriptors.

#[cfg(target_os = "linux")]
mod dma;
#[cfg(target_os = "linux")]
mod pool;
#[cfg(target_os = "linux")]
use dma as platform;

#[cfg(all(test, target_os = "linux"))]
mod fence_board;
#[cfg(all(test, target_os = "linux"))]
mod gpu_job;
#[cfg(all(test, target_os = "linux"))]
mod tests;
#[cfg(all(test, target_os = "linux"))]
mod tests_release;

#[cfg(not(target_os = "linux"))]
mod platform {
    use lemnos_abi::{BufferDesc, FenceDesc, Handle, Status};

    /// Not supported off Linux.
    ///
    /// # Safety
    /// Nothing is read.
    pub(super) unsafe fn push(
        _graph: Handle,
        _port: *const u8,
        _port_len: usize,
        _desc: *const BufferDesc,
        _fence: *const FenceDesc,
    ) -> Result<(), Status> {
        Err(Status::NOT_SUPPORTED)
    }

    /// Not supported off Linux.
    ///
    /// # Safety
    /// Nothing is read or written.
    pub(super) unsafe fn take(
        _graph: Handle,
        _port: *const u8,
        _port_len: usize,
        _desc: *mut BufferDesc,
        _fence: *mut FenceDesc,
        _frame: *mut Handle,
    ) -> Result<(), Status> {
        Err(Status::NOT_SUPPORTED)
    }
}

#[cfg(target_os = "linux")]
pub(crate) use pool::FramePool;

use lemnos_abi::{BufferDesc, FenceDesc, Handle, Status};

use crate::ffi::run;
use crate::state::lock;

/// Pushes the dma-buf frame described by `desc` into input `port`, with an acquire `fence` (null
/// for none). The frame is shared, not copied: its pixels stay in the caller's dma-bufs.
///
/// The surface duplicates each plane's descriptor, or takes it over under `BufferDesc::OWNED`. An
/// acquire sync-file is attached to the dma-bufs as a write fence, so a consumer that syncs
/// through dma-buf (a CPU access, a GPU import) waits for it; a `SYNCOBJ` fence is
/// [`Status::NOT_SUPPORTED`]. [`Status::AGAIN`] when the input is full, [`Status::INVALID_ARGUMENT`]
/// for a descriptor that does not validate or a port that is not a frame port.
///
/// # Safety
/// `port` must be valid for `port_len` bytes, `desc` for a read, and `fence` (when not null) for a
/// read. Under `BufferDesc::OWNED` the planes' descriptors (and the fence's) are the caller's to
/// give up: the surface closes them whether or not the call succeeds.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_surface_push_frame(
    graph: Handle,
    port: *const u8,
    port_len: usize,
    desc: *const BufferDesc,
    fence: *const FenceDesc,
) -> Status {
    run(|| {
        // Safety: forwarded to the caller's contract above.
        unsafe { platform::push(graph, port, port_len, desc, fence) }
    })
}

/// Takes the next frame from output `port` into `desc` and `fence`, and writes a lease handle to
/// `frame`. The descriptor's planes are duplicated and owned by the caller (`BufferDesc::OWNED` is
/// set on it): the caller closes them. The fence is always [`FenceDesc::none`]: pending writes on
/// the frame's dma-bufs are carried by their reservations, which a dma-buf consumer waits for.
///
/// The lease keeps the frame alive until [`daedalus_surface_frame_release`]. [`Status::AGAIN`] when
/// nothing is queued; [`Status::NOT_SUPPORTED`] for a frame with no dma-buf plane or more planes
/// than a descriptor holds (the frame stays queued for the next take).
///
/// # Safety
/// `port` must be valid for `port_len` bytes; `desc`, `fence` and `frame` must be valid for
/// writes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_surface_take_frame(
    graph: Handle,
    port: *const u8,
    port_len: usize,
    desc: *mut BufferDesc,
    fence: *mut FenceDesc,
    frame: *mut Handle,
) -> Status {
    run(|| {
        // Safety: forwarded to the caller's contract above.
        unsafe { platform::take(graph, port, port_len, desc, fence, frame) }
    })
}

/// Releases a frame lease from [`daedalus_surface_take_frame`]: the surface drops its reference,
/// and the frame's memory goes once the last holder lets go. [`Status::STALE_HANDLE`] for a
/// released or never-issued lease.
///
/// # Safety
/// Any handle value is accepted; a stale or null one is [`Status::STALE_HANDLE`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_surface_frame_release(frame: Handle) -> Status {
    run(|| {
        // The frame drops here, under the lock: its descriptors close with it.
        lock().frames.remove(frame).map(drop)
    })
}
