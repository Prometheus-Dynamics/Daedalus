//! The `extern "C"` functions of a [`FrameSource`]'s `daedalus:frame` vtable.

use core::ffi::c_void;

use super::{FrameColorimetry, FramePlane, FrameSource, PlaneMapping};

/// # Safety
/// `data` points to a live `T` (the `ProvideForeign` contract).
unsafe fn source<'a, T>(data: *const c_void) -> &'a T {
    // Safety: guaranteed by the caller.
    unsafe { &*data.cast::<T>() }
}

macro_rules! scalar {
    ($($name:ident -> $ret:ty = $body:expr;)*) => {$(
        pub(super) unsafe extern "C" fn $name<T: FrameSource>(data: *const c_void) -> $ret {
            // Safety: the vtable is only used with data pointers to `T`.
            let f: fn(&T) -> $ret = $body;
            f(unsafe { source::<T>(data) })
        }
    )*};
}

macro_rules! plane {
    ($($name:ident -> $ret:ty = $body:expr;)*) => {$(
        pub(super) unsafe extern "C" fn $name<T: FrameSource>(
            data: *const c_void,
            index: u32,
        ) -> $ret {
            let f: fn(Option<FramePlane>) -> $ret = $body;
            // Safety: the vtable is only used with data pointers to `T`.
            f(unsafe { source::<T>(data) }.plane(index))
        }
    )*};
}

scalar! {
    width -> u32 = T::width;
    height -> u32 = T::height;
    format -> u32 = T::format;
    format_kind -> u32 = |frame| frame.format_kind() as u32;
    modifier -> u64 = T::modifier;
    timestamp_ns -> u64 = T::timestamp_ns;
    sequence -> u64 = T::sequence;
    residency -> u32 = |frame| frame.residency() as u32;
    plane_count -> u32 = T::plane_count;
    colorimetry -> FrameColorimetry = T::colorimetry;
}

plane! {
    plane_fd -> i32 = |plane| plane.and_then(|plane| plane.dmabuf_fd).unwrap_or(-1);
    plane_offset -> u64 = |plane| plane.map_or(0, |plane| plane.offset);
    plane_stride -> u64 = |plane| plane.map_or(0, |plane| plane.stride);
    plane_len -> u64 = |plane| plane.map_or(0, |plane| plane.len);
    plane_mapping -> u32 = |plane| {
        plane.map_or(PlaneMapping::Unmapped, |plane| plane.mapping) as u32
    };
}

/// Store `bytes`' length in a non-null `len` and return their address.
///
/// # Safety
/// A non-null `len` points to a writable `u64` (the caller's contract).
unsafe fn lend(bytes: &[u8], len: *mut u64) -> *const u8 {
    if !len.is_null() {
        // Safety: forwarded from the caller.
        unsafe { *len = bytes.len() as u64 };
    }
    bytes.as_ptr()
}

pub(super) unsafe extern "C" fn plane_data<T: FrameSource>(
    data: *const c_void,
    index: u32,
    len: *mut u64,
) -> *const u8 {
    // Safety: the vtable is only used with data pointers to `T`.
    match unsafe { source::<T>(data) }.plane_data(index) {
        // Safety: the caller's `len` contract.
        Some(bytes) => unsafe { lend(bytes, len) },
        None => core::ptr::null(),
    }
}

pub(super) unsafe extern "C" fn plane_end_cpu_access<T: FrameSource>(
    data: *const c_void,
    index: u32,
) {
    // Safety: the vtable is only used with data pointers to `T`.
    unsafe { source::<T>(data) }.end_cpu_access(index)
}

pub(super) unsafe extern "C" fn metadata_u64<T: FrameSource>(
    data: *const c_void,
    key: u32,
    value: *mut u64,
) -> bool {
    // Safety: the vtable is only used with data pointers to `T`.
    let Some(found) = unsafe { source::<T>(data) }.metadata_u64(key) else {
        return false;
    };
    if value.is_null() {
        return false;
    }
    // Safety: a non-null `value` points to a writable `u64` (the caller's contract).
    unsafe { *value = found };
    true
}

pub(super) unsafe extern "C" fn metadata_bytes<T: FrameSource>(
    data: *const c_void,
    key: u32,
    len: *mut u64,
) -> *const u8 {
    // Safety: the vtable is only used with data pointers to `T`.
    match unsafe { source::<T>(data) }.metadata_bytes(key) {
        // Safety: the caller's `len` contract.
        Some(bytes) => unsafe { lend(bytes, len) },
        None => core::ptr::null(),
    }
}
