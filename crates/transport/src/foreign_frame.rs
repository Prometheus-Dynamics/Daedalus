//! `daedalus:frame` v2: the standard foreign interface for image frames.
//!
//! Camera-free and dependency-free: frame owners (camera stacks, decoders, image libraries)
//! implement [`FrameSource`] for their frame type in their `daedalus` integration feature, and
//! any node, including one in a separately built plugin, reads it as a [`FrameView`]. Plane
//! metadata (fd, offset, stride, length, mapping kind) never touches pixel memory; CPU bytes are
//! mapped only when a consumer asks for them ([`FrameView::plane_bytes`]). The public contract
//! is specified in `docs/foreign-frame-interface.md`.

use core::ffi::c_void;
use core::ops::Deref;

use crate::{ForeignInterfaceInfo, ForeignRef, ProvideForeign, foreign_interface};

/// Key of the frame interface.
pub const FRAME_INTERFACE_KEY: &str = "daedalus:frame";

/// DRM format modifier of linear (non-tiled) buffers.
pub const DRM_FORMAT_MOD_LINEAR: u64 = 0;
/// DRM format modifier meaning "no explicit modifier".
pub const DRM_FORMAT_MOD_INVALID: u64 = 0x00ff_ffff_ffff_ffff;
/// libcamera's MIPI CSI-2 packed Bayer modifier (`fourcc_mod_code(MIPI, 1)`). Its vendor byte
/// (0x0b) is MediaTek's in upstream `drm_fourcc.h`: it only means CSI-2 packing when the frame's
/// [`FrameFormatKind`] is [`FrameFormatKind::Bayer`] (see [`FrameView::is_csi2_packed`]).
pub const MIPI_FORMAT_MOD_CSI2_PACKED: u64 = (0x0b << 56) | 1;

/// DRM fourcc code of a four-character format name, e.g. `fourcc(b"NV12")`.
pub const fn fourcc(code: &[u8; 4]) -> u32 {
    u32::from_le_bytes(*code)
}

foreign_interface! {
    /// The `daedalus:frame` v2 interface (see [`FrameView`] and [`FrameSource`]).
    pub interface FrameInterface("daedalus:frame", version = 2);

    /// Accessors of `daedalus:frame` v2. Every function takes the handle's data pointer first;
    /// per-plane functions are only called with `index < plane_count`. Everything but
    /// `plane_data` is metadata and never maps or syncs plane memory.
    pub struct FrameVTable {
        pub width: unsafe extern "C" fn(data: *const c_void) -> u32,
        pub height: unsafe extern "C" fn(data: *const c_void) -> u32,
        /// DRM fourcc (or V4L2 fourcc for compressed formats, see [`FrameFormatKind`]).
        pub format: unsafe extern "C" fn(data: *const c_void) -> u32,
        /// [`FrameFormatKind`] as `u32`.
        pub format_kind: unsafe extern "C" fn(data: *const c_void) -> u32,
        /// DRM format modifier.
        pub modifier: unsafe extern "C" fn(data: *const c_void) -> u64,
        pub timestamp_ns: unsafe extern "C" fn(data: *const c_void) -> u64,
        pub sequence: unsafe extern "C" fn(data: *const c_void) -> u64,
        /// [`FrameResidency`] as `u32`.
        pub residency: unsafe extern "C" fn(data: *const c_void) -> u32,
        pub plane_count: unsafe extern "C" fn(data: *const c_void) -> u32,
        /// dma-buf file descriptor of plane `index` (borrowed), or -1.
        pub plane_fd: unsafe extern "C" fn(data: *const c_void, index: u32) -> i32,
        /// Offset of plane `index` in its dma-buf (or buffer).
        pub plane_offset: unsafe extern "C" fn(data: *const c_void, index: u32) -> u64,
        pub plane_stride: unsafe extern "C" fn(data: *const c_void, index: u32) -> u64,
        pub plane_len: unsafe extern "C" fn(data: *const c_void, index: u32) -> u64,
        /// [`PlaneMapping`] as `u32`.
        pub plane_mapping: unsafe extern "C" fn(data: *const c_void, index: u32) -> u32,
        /// Begin CPU access to plane `index`: map it if needed, sync it for CPU reads and return
        /// its bytes (`*len` set to their length), or null when the CPU cannot read it. Every
        /// non-null result is paired with exactly one `plane_end_cpu_access(index)`.
        pub plane_data:
            unsafe extern "C" fn(data: *const c_void, index: u32, len: *mut u64) -> *const u8,
        /// End a CPU access begun by a non-null `plane_data(index)`.
        pub plane_end_cpu_access: unsafe extern "C" fn(data: *const c_void, index: u32),
    }
}

/// Identity of the retired `daedalus:frame` v1 (13 accessors, `u32` offsets and strides, plane
/// bytes fetched with every field). No v1 vtable exists any more; hosts can use this to explain
/// a refused v1 plugin, and tests to check that v1 and v2 never mix.
pub static FRAME_INTERFACE_V1: ForeignInterfaceInfo = ForeignInterfaceInfo::new(
    FRAME_INTERFACE_KEY,
    1,
    "width: unsafe extern \"C\" fn(data: *const c_void) -> u32;\
     height: unsafe extern \"C\" fn(data: *const c_void) -> u32;\
     format: unsafe extern \"C\" fn(data: *const c_void) -> u32;\
     modifier: unsafe extern \"C\" fn(data: *const c_void) -> u64;\
     timestamp_ns: unsafe extern \"C\" fn(data: *const c_void) -> u64;\
     sequence: unsafe extern \"C\" fn(data: *const c_void) -> u64;\
     residency: unsafe extern \"C\" fn(data: *const c_void) -> u32;\
     plane_count: unsafe extern \"C\" fn(data: *const c_void) -> u32;\
     plane_data: unsafe extern \"C\" fn(data: *const c_void, index: u32) -> *const u8;\
     plane_len: unsafe extern \"C\" fn(data: *const c_void, index: u32) -> usize;\
     plane_stride: unsafe extern \"C\" fn(data: *const c_void, index: u32) -> u32;\
     plane_offset: unsafe extern \"C\" fn(data: *const c_void, index: u32) -> u32;\
     plane_fd: unsafe extern \"C\" fn(data: *const c_void, index: u32) -> i32;",
    13 * core::mem::size_of::<usize>(),
    core::mem::align_of::<usize>(),
);

/// Where a frame's memory lives.
#[repr(u32)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FrameResidency {
    /// Host memory; `plane_data` succeeds for every plane.
    Cpu = 0,
    /// Memory owned outside Daedalus (e.g. dma-buf); planes may or may not be mappable.
    External = 1,
    /// GPU memory; planes are usually not mappable.
    Gpu = 2,
}

impl FrameResidency {
    /// Decode the vtable value; unknown values are treated as [`Self::External`].
    pub fn from_raw(raw: u32) -> Self {
        match raw {
            0 => Self::Cpu,
            2 => Self::Gpu,
            _ => Self::External,
        }
    }
}

/// What `format` encodes. Providers should always set it: format 0, Bayer codes and colliding
/// modifier vendors are only unambiguous with an explicit kind.
#[repr(u32)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum FrameFormatKind {
    /// Not stated: consumers guess from `format` and `modifier` (as v1 did).
    #[default]
    Unknown = 0,
    /// Pixels: `format` is a DRM fourcc with its DRM modifier.
    Pixel = 1,
    /// Raw sensor data: `format` is a (libcamera) Bayer or mono raw DRM code, `modifier` may be
    /// [`MIPI_FORMAT_MOD_CSI2_PACKED`] or a vendor compression modifier.
    Bayer = 2,
    /// A compressed bitstream (MJPEG, H.264, ...): `format` is its V4L2 fourcc (`MJPG`, `H264`,
    /// `HEVC`), plane 0 holds the payload (`len` = payload bytes, `stride` 0).
    Compressed = 3,
}

impl FrameFormatKind {
    /// Decode the vtable value; unknown values are treated as [`Self::Unknown`].
    pub fn from_raw(raw: u32) -> Self {
        match raw {
            1 => Self::Pixel,
            2 => Self::Bayer,
            3 => Self::Compressed,
            _ => Self::Unknown,
        }
    }
}

/// How CPU reads of a plane through `plane_data` perform (whether or not it is mapped yet).
#[repr(u32)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum PlaneMapping {
    /// Cached memory: CPU reads are fast.
    #[default]
    Cached = 0,
    /// Uncached memory: readable, but every CPU read goes to DRAM; copy it out once or use the
    /// GPU instead of repeated reads.
    Uncached = 1,
    /// Write-combined memory: fine to write sequentially, very slow to read.
    WriteCombined = 2,
    /// Not CPU-accessible: `plane_data` returns null.
    Unmapped = 3,
}

impl PlaneMapping {
    /// Decode the vtable value; unknown values are treated as [`Self::Uncached`].
    pub fn from_raw(raw: u32) -> Self {
        match raw {
            0 => Self::Cached,
            2 => Self::WriteCombined,
            3 => Self::Unmapped,
            _ => Self::Uncached,
        }
    }

    /// Whether the CPU can read the plane at all.
    pub fn is_cpu_readable(self) -> bool {
        self != Self::Unmapped
    }
}

/// Metadata of one plane; reading it never maps or syncs the plane's memory.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FramePlane {
    /// dma-buf file descriptor, borrowed from the frame (duplicate it to keep it).
    pub dmabuf_fd: Option<i32>,
    /// Offset of the plane in its dma-buf (or buffer).
    pub offset: u64,
    /// Bytes per row (0 for compressed payloads).
    pub stride: u64,
    /// Plane size in bytes.
    pub len: u64,
    pub mapping: PlaneMapping,
}

impl FramePlane {
    /// A plane in cached host memory (`bytes` are what `plane_data` returns).
    pub fn cpu(bytes: &[u8], stride: u64) -> Self {
        Self {
            len: bytes.len() as u64,
            stride,
            ..Self::default()
        }
    }

    /// A dma-buf plane, [`PlaneMapping::Unmapped`] until [`Self::with_mapping`] says how the
    /// provider maps it.
    pub fn dmabuf(fd: i32, offset: u64, stride: u64, len: u64) -> Self {
        Self {
            dmabuf_fd: Some(fd),
            offset,
            stride,
            len,
            mapping: PlaneMapping::Unmapped,
        }
    }

    pub fn with_mapping(mut self, mapping: PlaneMapping) -> Self {
        self.mapping = mapping;
        self
    }
}

/// Safe owner-side implementation of `daedalus:frame` v2.
///
/// Implementing it provides the interface (`ProvideForeign<FrameInterface>`), so the owner's
/// Daedalus integration registers it with
/// `registry.register_foreign_provider::<MyFrame, FrameInterface>()`. Metadata stays unchanged
/// while the frame is shared. Methods must not panic: they are called through `extern "C"`
/// functions, where a panic aborts.
pub trait FrameSource: Send + Sync + 'static {
    fn width(&self) -> u32;
    fn height(&self) -> u32;
    /// DRM fourcc (see [`fourcc`] and [`FrameFormatKind`]).
    fn format(&self) -> u32;
    /// What [`Self::format`] encodes; set it explicitly.
    fn format_kind(&self) -> FrameFormatKind {
        FrameFormatKind::Unknown
    }
    fn modifier(&self) -> u64 {
        DRM_FORMAT_MOD_LINEAR
    }
    /// Capture timestamp in nanoseconds (clock chosen by the owner, typically monotonic).
    fn timestamp_ns(&self) -> u64 {
        0
    }
    fn sequence(&self) -> u64 {
        0
    }
    fn residency(&self) -> FrameResidency;
    fn plane_count(&self) -> u32;
    /// Metadata of plane `index`, `None` when out of range. Must not map or sync plane memory:
    /// fd-only consumers (GPU importers) read nothing else.
    fn plane(&self, index: u32) -> Option<FramePlane>;
    /// Begin CPU access to plane `index` and return its bytes, mapping lazily on first use (keep
    /// the mapping until the frame drops) and syncing for CPU reads (`DMA_BUF_IOCTL_SYNC` with
    /// `SYNC_START | SYNC_READ` for dma-bufs). `None` when the CPU cannot read the plane.
    ///
    /// Every `Some` is followed by exactly one [`Self::end_cpu_access`] for the same plane, and
    /// accesses may overlap (several consumers read one frame), so count them when syncing. The
    /// bytes must stay valid and unchanged until that end call.
    fn plane_data(&self, index: u32) -> Option<&[u8]> {
        let _ = index;
        None
    }
    /// End a CPU access begun by [`Self::plane_data`] (`SYNC_END | SYNC_READ` once the last
    /// overlapping access ends).
    fn end_cpu_access(&self, index: u32) {
        let _ = index;
    }
}

mod thunks {
    use super::*;

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

    pub(super) unsafe extern "C" fn plane_data<T: FrameSource>(
        data: *const c_void,
        index: u32,
        len: *mut u64,
    ) -> *const u8 {
        // Safety: the vtable is only used with data pointers to `T`.
        let Some(bytes) = unsafe { source::<T>(data) }.plane_data(index) else {
            return core::ptr::null();
        };
        if !len.is_null() {
            // Safety: a non-null `len` points to a writable `u64` (the caller's contract).
            unsafe { *len = bytes.len() as u64 };
        }
        bytes.as_ptr()
    }

    pub(super) unsafe extern "C" fn plane_end_cpu_access<T: FrameSource>(
        data: *const c_void,
        index: u32,
    ) {
        // Safety: the vtable is only used with data pointers to `T`.
        unsafe { source::<T>(data) }.end_cpu_access(index)
    }
}

// Safety: every function reads the `T` behind the data pointer and only calls `FrameSource`
// methods, which must not unwind.
unsafe impl<T: FrameSource> ProvideForeign<FrameInterface> for T {
    fn vtable() -> &'static FrameVTable {
        const {
            &FrameVTable {
                width: thunks::width::<T>,
                height: thunks::height::<T>,
                format: thunks::format::<T>,
                format_kind: thunks::format_kind::<T>,
                modifier: thunks::modifier::<T>,
                timestamp_ns: thunks::timestamp_ns::<T>,
                sequence: thunks::sequence::<T>,
                residency: thunks::residency::<T>,
                plane_count: thunks::plane_count::<T>,
                plane_fd: thunks::plane_fd::<T>,
                plane_offset: thunks::plane_offset::<T>,
                plane_stride: thunks::plane_stride::<T>,
                plane_len: thunks::plane_len::<T>,
                plane_mapping: thunks::plane_mapping::<T>,
                plane_data: thunks::plane_data::<T>,
                plane_end_cpu_access: thunks::plane_end_cpu_access::<T>,
            }
        }
    }
}

/// A frame seen through `daedalus:frame` v2. Take it as a node input (`frame: FrameView<'_>`);
/// it borrows the input payload, which keeps the frame alive for `'a`.
pub type FrameView<'a> = ForeignRef<'a, FrameInterface>;

impl<'a> ForeignRef<'a, FrameInterface> {
    pub fn width(&self) -> u32 {
        // Safety (all calls below): `view` checked the interface, and the provider's functions
        // accept the handle's data pointer while the handle lives.
        unsafe { (self.vtable().width)(self.data()) }
    }

    pub fn height(&self) -> u32 {
        unsafe { (self.vtable().height)(self.data()) }
    }

    /// DRM fourcc (see [`fourcc`] and [`Self::format_kind`]).
    pub fn format(&self) -> u32 {
        unsafe { (self.vtable().format)(self.data()) }
    }

    pub fn format_kind(&self) -> FrameFormatKind {
        FrameFormatKind::from_raw(unsafe { (self.vtable().format_kind)(self.data()) })
    }

    pub fn modifier(&self) -> u64 {
        unsafe { (self.vtable().modifier)(self.data()) }
    }

    /// Raw Bayer data in libcamera's MIPI CSI-2 packing (not a MediaTek-tiled pixel format,
    /// whose modifiers share the vendor byte).
    pub fn is_csi2_packed(&self) -> bool {
        self.format_kind() == FrameFormatKind::Bayer
            && self.modifier() == MIPI_FORMAT_MOD_CSI2_PACKED
    }

    pub fn timestamp_ns(&self) -> u64 {
        unsafe { (self.vtable().timestamp_ns)(self.data()) }
    }

    pub fn sequence(&self) -> u64 {
        unsafe { (self.vtable().sequence)(self.data()) }
    }

    pub fn residency(&self) -> FrameResidency {
        FrameResidency::from_raw(unsafe { (self.vtable().residency)(self.data()) })
    }

    pub fn plane_count(&self) -> u32 {
        unsafe { (self.vtable().plane_count)(self.data()) }
    }

    /// Metadata of plane `index` (never maps it), `None` when out of range.
    pub fn plane(&self, index: u32) -> Option<FramePlane> {
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

    /// Metadata of every plane (never maps them).
    pub fn planes(&self) -> impl Iterator<Item = FramePlane> + 'a {
        let view = *self;
        (0..view.plane_count()).filter_map(move |index| view.plane(index))
    }

    /// Map plane `index` for CPU reads; the access ends when the guard drops. `None` when out of
    /// range or not CPU-readable. Check [`FramePlane::mapping`] first to avoid slow reads.
    pub fn plane_bytes(&self, index: u32) -> Option<PlaneBytes<'a>> {
        if index >= self.plane_count() {
            return None;
        }
        let mut len = 0u64;
        let ptr = unsafe { (self.vtable().plane_data)(self.data(), index, &mut len) };
        if ptr.is_null() {
            return None;
        }
        Some(PlaneBytes {
            // Safety: a non-null pointer maps `len` bytes that stay valid and unchanged until
            // the matching end call, which only the guard's drop makes.
            bytes: unsafe { core::slice::from_raw_parts(ptr, len as usize) },
            frame: *self,
            index,
        })
    }

    /// [`Self::plane_bytes`] of every plane, in order (`None` for one the CPU cannot read).
    pub fn cpu_planes(&self) -> impl Iterator<Item = Option<PlaneBytes<'a>>> + 'a {
        let view = *self;
        (0..view.plane_count()).map(move |index| view.plane_bytes(index))
    }
}

/// CPU bytes of one plane, from [`FrameView::plane_bytes`]. Dropping it ends the CPU access.
pub struct PlaneBytes<'a> {
    bytes: &'a [u8],
    frame: FrameView<'a>,
    index: u32,
}

impl PlaneBytes<'_> {
    pub fn index(&self) -> u32 {
        self.index
    }
}

impl Deref for PlaneBytes<'_> {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        self.bytes
    }
}

impl AsRef<[u8]> for PlaneBytes<'_> {
    fn as_ref(&self) -> &[u8] {
        self.bytes
    }
}

impl core::fmt::Debug for PlaneBytes<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("PlaneBytes")
            .field("index", &self.index)
            .field("len", &self.bytes.len())
            .finish()
    }
}

impl Drop for PlaneBytes<'_> {
    fn drop(&mut self) {
        // Safety: ends the access this guard's non-null `plane_data` began, exactly once.
        unsafe { (self.frame.vtable().plane_end_cpu_access)(self.frame.data(), self.index) }
    }
}

#[cfg(test)]
mod tests;
