//! The frame entry points: declared in `include/daedalus_surface.h`, built as stubs.
//!
//! A frame crosses as a `lemnos-abi` `BufferDesc` (a dma-buf by file descriptor, with its planes)
//! and a `FenceDesc`. Routing one into a graph input and out of an output needs the
//! `daedalus:frame` host-port path, which this version does not wire up yet. Until it does, both
//! return [`Status::NOT_SUPPORTED`] and touch nothing.

use lemnos_abi::{BufferDesc, FenceDesc, Handle, Status};

use crate::ffi::run;

/// Pushes a frame into input `port`. Not built yet: [`Status::NOT_SUPPORTED`].
///
/// # Safety
/// Nothing is read; the pointers are ignored.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_surface_push_frame(
    _graph: Handle,
    _port: *const u8,
    _port_len: usize,
    _desc: *const BufferDesc,
    _fence: *const FenceDesc,
) -> Status {
    run(|| Err(Status::NOT_SUPPORTED))
}

/// Takes a frame from output `port`. Not built yet: [`Status::NOT_SUPPORTED`].
///
/// # Safety
/// Nothing is read or written; the pointers are ignored.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_surface_take_frame(
    _graph: Handle,
    _port: *const u8,
    _port_len: usize,
    _desc: *mut BufferDesc,
    _fence: *mut FenceDesc,
) -> Status {
    run(|| Err(Status::NOT_SUPPORTED))
}
