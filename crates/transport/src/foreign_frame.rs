//! `daedalus:frame` v1: the standard foreign interface for image frames.
//!
//! Camera-free and dependency-free: frame owners (camera stacks, decoders, image libraries)
//! implement [`FrameSource`] for their frame type in their `daedalus` integration feature, and
//! any node, including one in a separately built plugin, reads it as a [`FrameView`]. The
//! public contract is specified in `docs/foreign-frame-interface.md`.

use core::ffi::c_void;

use crate::{ForeignRef, ProvideForeign, foreign_interface};

/// Key of the frame interface.
pub const FRAME_INTERFACE_KEY: &str = "daedalus:frame";

/// DRM format modifier of linear (non-tiled) buffers.
pub const DRM_FORMAT_MOD_LINEAR: u64 = 0;
/// DRM format modifier meaning "no explicit modifier".
pub const DRM_FORMAT_MOD_INVALID: u64 = 0x00ff_ffff_ffff_ffff;

/// DRM fourcc code of a four-character format name, e.g. `fourcc(b"NV12")`.
pub const fn fourcc(code: &[u8; 4]) -> u32 {
    u32::from_le_bytes(*code)
}

foreign_interface! {
    /// The `daedalus:frame` v1 interface (see [`FrameView`] and [`FrameSource`]).
    pub interface FrameInterface("daedalus:frame", version = 1);

    /// Accessors of `daedalus:frame` v1. Every function takes the handle's data pointer first.
    pub struct FrameVTable {
        pub width: unsafe extern "C" fn(data: *const c_void) -> u32,
        pub height: unsafe extern "C" fn(data: *const c_void) -> u32,
        /// DRM fourcc.
        pub format: unsafe extern "C" fn(data: *const c_void) -> u32,
        /// DRM format modifier.
        pub modifier: unsafe extern "C" fn(data: *const c_void) -> u64,
        pub timestamp_ns: unsafe extern "C" fn(data: *const c_void) -> u64,
        pub sequence: unsafe extern "C" fn(data: *const c_void) -> u64,
        /// [`FrameResidency`] as `u32`.
        pub residency: unsafe extern "C" fn(data: *const c_void) -> u32,
        pub plane_count: unsafe extern "C" fn(data: *const c_void) -> u32,
        /// Mapped bytes of plane `index`, or null when the plane is not CPU-mapped.
        pub plane_data: unsafe extern "C" fn(data: *const c_void, index: u32) -> *const u8,
        pub plane_len: unsafe extern "C" fn(data: *const c_void, index: u32) -> usize,
        pub plane_stride: unsafe extern "C" fn(data: *const c_void, index: u32) -> u32,
        pub plane_offset: unsafe extern "C" fn(data: *const c_void, index: u32) -> u32,
        /// dmabuf file descriptor of plane `index`, or -1.
        pub plane_fd: unsafe extern "C" fn(data: *const c_void, index: u32) -> i32,
    }
}

/// Where a frame's memory lives.
#[repr(u32)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FrameResidency {
    /// Host memory; every plane is mapped.
    Cpu = 0,
    /// Memory owned outside Daedalus (e.g. dmabuf); planes may or may not be mapped.
    External = 1,
    /// GPU memory; planes are usually not mapped.
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

/// One plane of a frame.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FramePlane<'a> {
    /// The plane's bytes when CPU-mapped.
    pub data: Option<&'a [u8]>,
    /// Plane size in bytes (also when not mapped).
    pub len: usize,
    pub stride: u32,
    /// Offset of the plane in its dmabuf.
    pub offset: u32,
    /// dmabuf file descriptor, borrowed from the frame (duplicate it to keep it).
    pub dmabuf_fd: Option<i32>,
}

impl<'a> FramePlane<'a> {
    /// A CPU-mapped plane.
    pub fn mapped(data: &'a [u8], stride: u32) -> Self {
        Self {
            data: Some(data),
            len: data.len(),
            stride,
            offset: 0,
            dmabuf_fd: None,
        }
    }

    /// A dmabuf plane (add its mapping with [`Self::with_data`] when it has one).
    pub fn dmabuf(fd: i32, offset: u32, stride: u32, len: usize) -> Self {
        Self {
            data: None,
            len,
            stride,
            offset,
            dmabuf_fd: Some(fd),
        }
    }

    pub fn with_data(mut self, data: &'a [u8]) -> Self {
        self.len = data.len();
        self.data = Some(data);
        self
    }
}

/// Safe owner-side implementation of `daedalus:frame` v1.
///
/// Implementing it provides the interface (`ProvideForeign<FrameInterface>`), so the owner's
/// Daedalus integration registers it with
/// `registry.register_foreign_provider::<MyFrame, FrameInterface>()`. Plane memory must stay
/// valid and unchanged while the frame is shared. Methods must not panic: they are called
/// through `extern "C"` functions, where a panic aborts.
pub trait FrameSource: Send + Sync + 'static {
    fn width(&self) -> u32;
    fn height(&self) -> u32;
    /// DRM fourcc (see [`fourcc`]).
    fn format(&self) -> u32;
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
    /// Plane `index`, `None` when out of range.
    fn plane(&self, index: u32) -> Option<FramePlane<'_>>;
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
                let f: fn(Option<FramePlane<'_>>) -> $ret = $body;
                // Safety: the vtable is only used with data pointers to `T`.
                f(unsafe { source::<T>(data) }.plane(index))
            }
        )*};
    }

    scalar! {
        width -> u32 = T::width;
        height -> u32 = T::height;
        format -> u32 = T::format;
        modifier -> u64 = T::modifier;
        timestamp_ns -> u64 = T::timestamp_ns;
        sequence -> u64 = T::sequence;
        residency -> u32 = |frame| frame.residency() as u32;
        plane_count -> u32 = T::plane_count;
    }

    plane! {
        plane_data -> *const u8 = |plane| {
            plane.and_then(|plane| plane.data).map_or(core::ptr::null(), <[u8]>::as_ptr)
        };
        plane_len -> usize = |plane| plane.map_or(0, |plane| plane.len);
        plane_stride -> u32 = |plane| plane.map_or(0, |plane| plane.stride);
        plane_offset -> u32 = |plane| plane.map_or(0, |plane| plane.offset);
        plane_fd -> i32 = |plane| plane.and_then(|plane| plane.dmabuf_fd).unwrap_or(-1);
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
                modifier: thunks::modifier::<T>,
                timestamp_ns: thunks::timestamp_ns::<T>,
                sequence: thunks::sequence::<T>,
                residency: thunks::residency::<T>,
                plane_count: thunks::plane_count::<T>,
                plane_data: thunks::plane_data::<T>,
                plane_len: thunks::plane_len::<T>,
                plane_stride: thunks::plane_stride::<T>,
                plane_offset: thunks::plane_offset::<T>,
                plane_fd: thunks::plane_fd::<T>,
            }
        }
    }
}

/// A frame seen through `daedalus:frame` v1. Take it as a node input (`frame: FrameView<'_>`);
/// it borrows the input payload, so plane slices live as long as the view.
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

    /// DRM fourcc (see [`fourcc`]).
    pub fn format(&self) -> u32 {
        unsafe { (self.vtable().format)(self.data()) }
    }

    pub fn modifier(&self) -> u64 {
        unsafe { (self.vtable().modifier)(self.data()) }
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

    /// Plane `index`, `None` when out of range.
    pub fn plane(&self, index: u32) -> Option<FramePlane<'a>> {
        if index >= self.plane_count() {
            return None;
        }
        let (vtable, data) = (self.vtable(), self.data());
        let len = unsafe { (vtable.plane_len)(data, index) };
        let ptr = unsafe { (vtable.plane_data)(data, index) };
        let fd = unsafe { (vtable.plane_fd)(data, index) };
        Some(FramePlane {
            // Safety: a non-null pointer maps `len` bytes that stay valid and unchanged while
            // the frame is shared, which the borrowed handle guarantees for `'a`.
            data: (!ptr.is_null()).then(|| unsafe { core::slice::from_raw_parts(ptr, len) }),
            len,
            stride: unsafe { (vtable.plane_stride)(data, index) },
            offset: unsafe { (vtable.plane_offset)(data, index) },
            dmabuf_fd: (fd >= 0).then_some(fd),
        })
    }

    pub fn planes(&self) -> impl Iterator<Item = FramePlane<'a>> + 'a {
        let view = *self;
        (0..view.plane_count()).filter_map(move |index| view.plane(index))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ForeignHandle, ForeignInterface};
    use alloc::sync::Arc;

    struct Nv12 {
        luma: Vec<u8>,
        chroma: Vec<u8>,
    }

    impl FrameSource for Nv12 {
        fn width(&self) -> u32 {
            4
        }
        fn height(&self) -> u32 {
            2
        }
        fn format(&self) -> u32 {
            fourcc(b"NV12")
        }
        fn sequence(&self) -> u64 {
            7
        }
        fn residency(&self) -> FrameResidency {
            FrameResidency::Cpu
        }
        fn plane_count(&self) -> u32 {
            3
        }
        fn plane(&self, index: u32) -> Option<FramePlane<'_>> {
            match index {
                0 => Some(FramePlane::mapped(&self.luma, 4)),
                1 => Some(FramePlane::mapped(&self.chroma, 4)),
                2 => Some(FramePlane::dmabuf(9, 16, 4, 4)),
                _ => None,
            }
        }
    }

    #[test]
    fn frame_view_reads_the_owner_without_copying() {
        let frame = Arc::new(Nv12 {
            luma: vec![1; 8],
            chroma: vec![2; 4],
        });
        let handle = ForeignHandle::from_arc::<_, FrameInterface>(frame.clone());
        let view = handle.view::<FrameInterface>().unwrap();
        assert_eq!((view.width(), view.height()), (4, 2));
        assert_eq!(view.format(), u32::from_le_bytes(*b"NV12"));
        assert_eq!(view.modifier(), DRM_FORMAT_MOD_LINEAR);
        assert_eq!((view.sequence(), view.timestamp_ns()), (7, 0));
        assert_eq!(view.residency(), FrameResidency::Cpu);
        let planes: Vec<_> = view.planes().collect();
        assert_eq!(planes.len(), 3);
        assert_eq!(planes[0].data.unwrap().as_ptr(), frame.luma.as_ptr());
        assert_eq!(planes[1].data, Some(&[2u8; 4][..]));
        assert_eq!(planes[2], FramePlane::dmabuf(9, 16, 4, 4));
        assert!(view.plane(3).is_none());
        assert_eq!(FrameInterface::info().key(), FRAME_INTERFACE_KEY);
    }
}
