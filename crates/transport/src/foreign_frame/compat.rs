//! `daedalus:frame` v2 compatibility: v3's table starts with exactly v2's fields, so a v3 frame
//! is lent to a consumer built against v2 by changing only the identity its handle carries, and
//! a frame a v2 producer made is read through v3 by a small adapter.

use core::ffi::c_void;

use super::{
    FRAME_INTERFACE_KEY, FrameFormatKind, FrameInterface, FramePlane, FrameResidency, FrameSource,
    FrameVTable, PlaneMapping,
};
use crate::portable::Arc;
use crate::{ForeignHandle, ForeignInterface, ForeignInterfaceInfo};

/// v2's field declarations, as `foreign_interface!` hashed them when v2 shipped (whitespace is
/// ignored by the hash).
const V2_SIGNATURE: &str = "\
    width: unsafe extern \"C\" fn(data: *const c_void) -> u32;\
    height: unsafe extern \"C\" fn(data: *const c_void) -> u32;\
    format: unsafe extern \"C\" fn(data: *const c_void) -> u32;\
    format_kind: unsafe extern \"C\" fn(data: *const c_void) -> u32;\
    modifier: unsafe extern \"C\" fn(data: *const c_void) -> u64;\
    timestamp_ns: unsafe extern \"C\" fn(data: *const c_void) -> u64;\
    sequence: unsafe extern \"C\" fn(data: *const c_void) -> u64;\
    residency: unsafe extern \"C\" fn(data: *const c_void) -> u32;\
    plane_count: unsafe extern \"C\" fn(data: *const c_void) -> u32;\
    plane_fd: unsafe extern \"C\" fn(data: *const c_void, index: u32) -> i32;\
    plane_offset: unsafe extern \"C\" fn(data: *const c_void, index: u32) -> u64;\
    plane_stride: unsafe extern \"C\" fn(data: *const c_void, index: u32) -> u64;\
    plane_len: unsafe extern \"C\" fn(data: *const c_void, index: u32) -> u64;\
    plane_mapping: unsafe extern \"C\" fn(data: *const c_void, index: u32) -> u32;\
    plane_data: unsafe extern \"C\" fn(data: *const c_void, index: u32, len: *mut u64) -> *const u8;\
    plane_end_cpu_access: unsafe extern \"C\" fn(data: *const c_void, index: u32);";

/// Identity of `daedalus:frame` v2, the version Daedalus 3.0.0 shipped: what a plugin built
/// against it declares and checks handles against.
pub static FRAME_INTERFACE_V2: ForeignInterfaceInfo = ForeignInterfaceInfo::new(
    FRAME_INTERFACE_KEY,
    2,
    V2_SIGNATURE,
    core::mem::size_of::<FrameVTableV2>(),
    core::mem::align_of::<FrameVTableV2>(),
);

/// The v2 identity [`frame_handle_to_v2`] gives a v3 handle. Equal to [`FRAME_INTERFACE_V2`]
/// but at its own address, so [`frame_handle_from_v2`] recognizes a lent v3 table coming back
/// and restores it without an adapter.
static V3_LENT_AS_V2: ForeignInterfaceInfo = ForeignInterfaceInfo::new(
    FRAME_INTERFACE_KEY,
    2,
    V2_SIGNATURE,
    core::mem::size_of::<FrameVTableV2>(),
    core::mem::align_of::<FrameVTableV2>(),
);

/// `daedalus:frame` v2: kept to feed and read plugins built against it (see
/// [`frame_handle_to_v2`]). New code uses [`FrameInterface`].
#[derive(Clone, Copy, Debug)]
pub enum FrameInterfaceV2 {}

/// The v2 vtable: the leading fields of [`FrameVTable`] (see its docs for each).
#[allow(missing_docs)]
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct FrameVTableV2 {
    pub width: unsafe extern "C" fn(data: *const c_void) -> u32,
    pub height: unsafe extern "C" fn(data: *const c_void) -> u32,
    pub format: unsafe extern "C" fn(data: *const c_void) -> u32,
    pub format_kind: unsafe extern "C" fn(data: *const c_void) -> u32,
    pub modifier: unsafe extern "C" fn(data: *const c_void) -> u64,
    pub timestamp_ns: unsafe extern "C" fn(data: *const c_void) -> u64,
    pub sequence: unsafe extern "C" fn(data: *const c_void) -> u64,
    pub residency: unsafe extern "C" fn(data: *const c_void) -> u32,
    pub plane_count: unsafe extern "C" fn(data: *const c_void) -> u32,
    pub plane_fd: unsafe extern "C" fn(data: *const c_void, index: u32) -> i32,
    pub plane_offset: unsafe extern "C" fn(data: *const c_void, index: u32) -> u64,
    pub plane_stride: unsafe extern "C" fn(data: *const c_void, index: u32) -> u64,
    pub plane_len: unsafe extern "C" fn(data: *const c_void, index: u32) -> u64,
    pub plane_mapping: unsafe extern "C" fn(data: *const c_void, index: u32) -> u32,
    pub plane_data:
        unsafe extern "C" fn(data: *const c_void, index: u32, len: *mut u64) -> *const u8,
    pub plane_end_cpu_access: unsafe extern "C" fn(data: *const c_void, index: u32),
}

// Safety: `FrameVTableV2` is `#[repr(C)]` with only function pointers, and the info hashes
// exactly its declaration (pinned to v2's shipped hash by a test).
unsafe impl ForeignInterface for FrameInterfaceV2 {
    type VTable = FrameVTableV2;
    const KEY: &'static str = FRAME_INTERFACE_KEY;
    const VERSION: u32 = 2;

    fn info() -> &'static ForeignInterfaceInfo {
        &FRAME_INTERFACE_V2
    }
}

/// Every v2 field sits at the same offset in v3's table, which is what makes a v3 vtable
/// pointer a valid v2 vtable pointer. Same field types are guaranteed by `frame_handle_to_v2`'s
/// test, which calls every v2 accessor through a lent v3 table.
const _: () = {
    use core::mem::offset_of;
    macro_rules! same_offsets {
        ($($field:ident),*) => {$(
            assert!(offset_of!(FrameVTable, $field) == offset_of!(FrameVTableV2, $field));
        )*};
    }
    same_offsets!(
        width,
        height,
        format,
        format_kind,
        modifier,
        timestamp_ns,
        sequence,
        residency,
        plane_count,
        plane_fd,
        plane_offset,
        plane_stride,
        plane_len,
        plane_mapping,
        plane_data,
        plane_end_cpu_access
    );
    assert!(offset_of!(FrameVTable, colorimetry) == core::mem::size_of::<FrameVTableV2>());
};

/// Whether `info` is `daedalus:frame` v2.
pub fn is_frame_v2(info: &ForeignInterfaceInfo) -> bool {
    info.same_interface(&FRAME_INTERFACE_V2)
}

/// Lend a v3 frame handle to a consumer built against v2 (a plugin whose foreign interface
/// table lists [`FRAME_INTERFACE_V2`]): the same data, vtable and owner reference under the v2
/// identity, so nothing is allocated or reference counted. Any other handle is returned as is.
pub fn frame_handle_to_v2(handle: ForeignHandle) -> ForeignHandle {
    if handle.view::<FrameInterface>().is_err() {
        return handle;
    }
    // Safety: the handle's vtable is a v3 table, whose leading fields are v2's table (same
    // order, types and offsets, see the assertions above).
    unsafe { handle.with_interface(&V3_LENT_AS_V2) }
}

/// Read a frame handle a v2 producer passed (a plugin built against v2) through v3: a v3 handle
/// [`frame_handle_to_v2`] lent comes back as itself, a frame the v2 build made is wrapped in an
/// adapter (one allocation) reporting v3's additions as unknown. Any other handle is returned
/// as is.
pub fn frame_handle_from_v2(handle: ForeignHandle) -> ForeignHandle {
    if core::ptr::eq(handle.interface(), &V3_LENT_AS_V2) {
        // Safety: only `frame_handle_to_v2` gives a handle this identity, and only a v3 one.
        return unsafe { handle.with_interface(FrameInterface::info()) };
    }
    match V2Frame::new(handle) {
        Ok(frame) => ForeignHandle::from_arc::<_, FrameInterface>(Arc::new(frame)),
        Err(handle) => handle,
    }
}

/// A v2 frame read through v3.
struct V2Frame {
    /// Keeps the frame (and the producer's code) alive.
    handle: ForeignHandle,
    vtable: *const FrameVTableV2,
}

// Safety: the handle is `Send + Sync`, and the vtable is an immutable static of the producer.
unsafe impl Send for V2Frame {}
// Safety: see above.
unsafe impl Sync for V2Frame {}

impl V2Frame {
    fn new(handle: ForeignHandle) -> Result<Self, ForeignHandle> {
        let Ok(view) = handle.view::<FrameInterfaceV2>() else {
            return Err(handle);
        };
        let vtable: *const FrameVTableV2 = view.vtable();
        Ok(Self { handle, vtable })
    }

    fn vtable(&self) -> &FrameVTableV2 {
        // Safety: checked against v2 in `new`; the vtable lives as long as the handle's owner
        // code, which `self.handle` keeps loaded.
        unsafe { &*self.vtable }
    }

    fn data(&self) -> *const c_void {
        self.handle.data()
    }
}

// Every call below: the v2 accessor with the handle's data pointer, which the handle keeps
// valid; per-plane accessors are only reached with `index < plane_count` (`FrameView` checks).
impl FrameSource for V2Frame {
    fn width(&self) -> u32 {
        unsafe { (self.vtable().width)(self.data()) }
    }
    fn height(&self) -> u32 {
        unsafe { (self.vtable().height)(self.data()) }
    }
    fn format(&self) -> u32 {
        unsafe { (self.vtable().format)(self.data()) }
    }
    fn format_kind(&self) -> FrameFormatKind {
        FrameFormatKind::from_raw(unsafe { (self.vtable().format_kind)(self.data()) })
    }
    fn modifier(&self) -> u64 {
        unsafe { (self.vtable().modifier)(self.data()) }
    }
    fn timestamp_ns(&self) -> u64 {
        unsafe { (self.vtable().timestamp_ns)(self.data()) }
    }
    fn sequence(&self) -> u64 {
        unsafe { (self.vtable().sequence)(self.data()) }
    }
    fn residency(&self) -> FrameResidency {
        FrameResidency::from_raw(unsafe { (self.vtable().residency)(self.data()) })
    }
    fn plane_count(&self) -> u32 {
        unsafe { (self.vtable().plane_count)(self.data()) }
    }
    fn plane(&self, index: u32) -> Option<FramePlane> {
        if index >= self.plane_count() {
            return None;
        }
        let (vtable, data) = (self.vtable(), self.data());
        let fd = unsafe { (vtable.plane_fd)(data, index) };
        Some(FramePlane {
            dmabuf_fd: (fd >= 0).then_some(fd),
            offset: unsafe { (vtable.plane_offset)(data, index) },
            stride: unsafe { (vtable.plane_stride)(data, index) },
            len: unsafe { (vtable.plane_len)(data, index) },
            mapping: PlaneMapping::from_raw(unsafe { (vtable.plane_mapping)(data, index) }),
        })
    }
    fn plane_data(&self, index: u32) -> Option<&[u8]> {
        if index >= self.plane_count() {
            return None;
        }
        let mut len = 0u64;
        let ptr = unsafe { (self.vtable().plane_data)(self.data(), index, &mut len) };
        // Safety: a non-null result maps `len` bytes valid until the matching end call, which
        // our caller (the v3 `plane_data` contract) makes through `end_cpu_access`.
        (!ptr.is_null()).then(|| unsafe { core::slice::from_raw_parts(ptr, len as usize) })
    }
    fn end_cpu_access(&self, index: u32) {
        unsafe { (self.vtable().plane_end_cpu_access)(self.data(), index) }
    }
}
