//! The declarations of `vg_lite.h` this crate uses, from the K230 Linux SDK v0.6.1
//! (`buildroot-overlay/package/vg_lite`, VGLite API 3.0, release 4.0.20, MIT), and the library
//! loaded at run time (`dlopen`; nothing links it).

#![cfg_attr(not(target_os = "linux"), allow(dead_code))]

#[cfg(target_os = "linux")]
use std::ffi::CStr;
use std::ffi::{c_int, c_void};
use std::io;

pub(crate) type VgError = c_int;
pub(crate) const SUCCESS: VgError = 0;
pub(crate) const NOT_SUPPORT: VgError = 7;

/// `vg_lite_buffer_format_t`.
pub(crate) const RGBA8888: c_int = 1 << 10;
pub(crate) const BGRA8888: c_int = 1 | (1 << 10);
pub(crate) const RGBX8888: c_int = 2 | (1 << 10);
pub(crate) const A8: c_int = 10 | (1 << 10);
pub(crate) const L8: c_int = 11 | (1 << 10);
pub(crate) const INDEX_8: c_int = 3 | (1 << 11);

/// `vg_lite_blend_t`.
pub(crate) const BLEND_NONE: c_int = 0x2000;
pub(crate) const BLEND_SRC_OVER: c_int = 0x2001;
pub(crate) const BLEND_DST_OVER: c_int = 0x2002;

/// `vg_lite_filter_t`.
pub(crate) const FILTER_POINT: c_int = 0;
pub(crate) const FILTER_BI_LINEAR: c_int = 0x2000;

/// `vg_lite_format_t` (path coordinates), `vg_lite_quality_t`, `vg_lite_fill_t`.
pub(crate) const PATH_FP32: c_int = 3;
pub(crate) const QUALITY_LOW: c_int = 3;
pub(crate) const FILL_NON_ZERO: c_int = 0x1901;
/// `vg_lite_path_type_t`, `vg_lite_cap_style_t`, `vg_lite_join_style_t`.
pub(crate) const DRAW_STROKE_PATH: c_int = 1;
pub(crate) const CAP_SQUARE: c_int = 0x1702;
pub(crate) const JOIN_MITER: c_int = 0x1800;
/// Path opcodes (`VLC_OP_*`).
pub(crate) const OP_END: u32 = 0x00;
pub(crate) const OP_MOVE: u32 = 0x02;
pub(crate) const OP_LINE: u32 = 0x04;

/// `vg_lite_map_flag_t`.
pub(crate) const MAP_DMABUF: c_int = 1;
/// `vg_lite_image_mode_t`.
pub(crate) const NORMAL_IMAGE_MODE: c_int = 0x1F00;

/// `vg_lite_matrix_t`: `m[row][column]`, a point maps to `m * (x, y, 1)`.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct Matrix {
    pub(crate) m: [[f32; 3]; 3],
}

/// `vg_lite_rectangle_t`.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Rectangle {
    pub(crate) x: i32,
    pub(crate) y: i32,
    pub(crate) width: i32,
    pub(crate) height: i32,
}

/// `vg_lite_yuvinfo_t`; unused (no YUV formats here) but part of the buffer.
#[repr(C)]
#[derive(Clone, Copy)]
struct YuvInfo {
    swizzle: c_int,
    yuv2rgb: c_int,
    planar: [u32; 6],
    heights: [u32; 2],
    pointers: [*mut c_void; 4],
}

/// `vg_lite_fc_buffer_t`; fast clear is off on the K230 (`gcFEATURE_VG_IM_FASTCLEAR` 0).
#[repr(C)]
#[derive(Clone, Copy)]
struct FcBuffer {
    width: i32,
    height: i32,
    stride: i32,
    handle: *mut c_void,
    memory: *mut c_void,
    address: u32,
    color: u32,
}

/// `vg_lite_buffer_t`.
#[repr(C)]
#[derive(Clone, Copy)]
pub(crate) struct Buffer {
    pub(crate) width: i32,
    pub(crate) height: i32,
    /// Bytes per row.
    pub(crate) stride: i32,
    tiled: c_int,
    pub(crate) format: c_int,
    pub(crate) handle: *mut c_void,
    /// The CPU's view of the pixels; `vg_lite_finish` cleans and invalidates the target's.
    pub(crate) memory: *mut c_void,
    pub(crate) address: u32,
    yuv: YuvInfo,
    pub(crate) image_mode: c_int,
    transparency_mode: c_int,
    fc_enable: i8,
    fc_buffer: [FcBuffer; 3],
    compress_mode: c_int,
    index_endian: c_int,
}

// The sizes a C compiler gives from the header (LP64; the K230's Linux is riscv64).
#[cfg(target_pointer_width = "64")]
const _: () = assert!(size_of::<Buffer>() == 264 && size_of::<Path>() == 112);

impl Buffer {
    /// A linear buffer description with no memory yet.
    pub(crate) fn new(width: u32, height: u32, stride: u32, format: c_int) -> Self {
        // SAFETY: every field is an integer, a float or a raw pointer: all zeros is valid.
        let mut buffer: Self = unsafe { std::mem::zeroed() };
        buffer.width = width as i32;
        buffer.height = height as i32;
        buffer.stride = stride as i32;
        buffer.format = format;
        buffer.image_mode = NORMAL_IMAGE_MODE;
        buffer
    }
}

/// `vg_lite_hw_memory_t`.
#[repr(C)]
#[derive(Clone, Copy)]
struct HwMemory {
    handle: *mut c_void,
    memory: *mut c_void,
    address: u32,
    bytes: u32,
    property: u32,
}

/// `vg_lite_path_t`; the library fills it (`vg_lite_init_path`, `vg_lite_set_stroke`) and frees
/// what it allocated (`vg_lite_clear_path`).
#[repr(C)]
#[derive(Clone, Copy)]
pub(crate) struct Path {
    bounding_box: [f32; 4],
    quality: c_int,
    format: c_int,
    uploaded: HwMemory,
    pub(crate) path_length: u32,
    pub(crate) path: *mut c_void,
    path_changed: i8,
    pdata_internal: i8,
    path_type: c_int,
    stroke: *mut c_void,
    stroke_path: *mut c_void,
    stroke_size: u32,
    pub(crate) stroke_color: u32,
    add_end: i8,
}

impl Path {
    pub(crate) fn zeroed() -> Self {
        // SAFETY: integers, floats and raw pointers only.
        unsafe { std::mem::zeroed() }
    }
}

pub(crate) type Init = unsafe extern "C" fn(i32, i32) -> VgError;
pub(crate) type Plain = unsafe extern "C" fn() -> VgError;
pub(crate) type WithBuffer = unsafe extern "C" fn(*mut Buffer) -> VgError;
pub(crate) type Map = unsafe extern "C" fn(*mut Buffer, c_int, i32) -> VgError;
pub(crate) type Clear = unsafe extern "C" fn(*mut Buffer, *mut Rectangle, u32) -> VgError;
/// `vg_lite_blit(target, source, matrix, blend, color, filter)`.
pub(crate) type Blit =
    unsafe extern "C" fn(*mut Buffer, *mut Buffer, *mut Matrix, c_int, u32, c_int) -> VgError;
pub(crate) type SetClut = unsafe extern "C" fn(u32, *mut u32) -> VgError;
pub(crate) type InitPath =
    unsafe extern "C" fn(*mut Path, c_int, c_int, u32, *mut c_void, f32, f32, f32, f32) -> VgError;
pub(crate) type SetStroke =
    unsafe extern "C" fn(*mut Path, c_int, c_int, f32, f32, *mut f32, u32, f32, u32) -> VgError;
pub(crate) type WithPath = unsafe extern "C" fn(*mut Path) -> VgError;
pub(crate) type SetPathType = unsafe extern "C" fn(*mut Path, c_int) -> VgError;
/// `vg_lite_draw(target, path, fill_rule, matrix, blend, color)`.
pub(crate) type Draw =
    unsafe extern "C" fn(*mut Buffer, *mut Path, c_int, *mut Matrix, c_int, u32) -> VgError;

/// The library's entry points.
#[derive(Clone, Copy)]
pub(crate) struct Api {
    pub(crate) init: Init,
    pub(crate) finish: Plain,
    pub(crate) allocate: WithBuffer,
    pub(crate) free: WithBuffer,
    pub(crate) map: Map,
    pub(crate) unmap: WithBuffer,
    pub(crate) clear: Clear,
    pub(crate) blit: Blit,
    pub(crate) set_clut: SetClut,
    /// The path entry points draw lines; a build without them makes lines `Unsupported`.
    pub(crate) paths: Option<PathApi>,
}

#[derive(Clone, Copy)]
pub(crate) struct PathApi {
    pub(crate) init_path: InitPath,
    pub(crate) set_stroke: SetStroke,
    pub(crate) set_path_type: SetPathType,
    pub(crate) update_stroke: WithPath,
    pub(crate) draw: Draw,
    pub(crate) clear_path: WithPath,
}

#[cfg(target_os = "linux")]
/// `dlopen` `libvg_lite.so` and resolve [`Api`]. The handle is never closed: the library keeps
/// process-wide state.
pub(crate) fn load() -> io::Result<Api> {
    for name in [c"libvg_lite.so", c"libvg_lite.so.0"] {
        // SAFETY: a valid name; the library's initializers are the vendor's.
        let handle = unsafe { libc::dlopen(name.as_ptr(), libc::RTLD_NOW | libc::RTLD_LOCAL) };
        if !handle.is_null() {
            // SAFETY: the handle was just opened.
            return unsafe { resolve(handle) };
        }
    }
    Err(io::Error::new(
        io::ErrorKind::NotFound,
        "libvg_lite.so not found",
    ))
}

#[cfg(target_os = "linux")]
/// # Safety
/// `handle` must be a `dlopen` handle of a VGLite build with the entry points of the header
/// this module follows.
unsafe fn resolve(handle: *mut c_void) -> io::Result<Api> {
    let symbol = |name: &CStr| {
        // SAFETY: a valid handle and name.
        let found = unsafe { libc::dlsym(handle, name.as_ptr()) };
        if found.is_null() {
            Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("libvg_lite has no {name:?}"),
            ))
        } else {
            Ok(found)
        }
    };
    macro_rules! get {
        ($name:literal, $ty:ty) => {
            // SAFETY: the symbol has the declared signature in the header.
            unsafe { std::mem::transmute::<*mut c_void, $ty>(symbol($name)?) }
        };
    }
    let paths = (|| {
        Ok::<_, io::Error>(PathApi {
            init_path: get!(c"vg_lite_init_path", InitPath),
            set_stroke: get!(c"vg_lite_set_stroke", SetStroke),
            set_path_type: get!(c"vg_lite_set_path_type", SetPathType),
            update_stroke: get!(c"vg_lite_update_stroke", WithPath),
            draw: get!(c"vg_lite_draw", Draw),
            clear_path: get!(c"vg_lite_clear_path", WithPath),
        })
    })();
    Ok(Api {
        init: get!(c"vg_lite_init", Init),
        finish: get!(c"vg_lite_finish", Plain),
        allocate: get!(c"vg_lite_allocate", WithBuffer),
        free: get!(c"vg_lite_free", WithBuffer),
        map: get!(c"vg_lite_map", Map),
        unmap: get!(c"vg_lite_unmap", WithBuffer),
        clear: get!(c"vg_lite_clear", Clear),
        blit: get!(c"vg_lite_blit", Blit),
        set_clut: get!(c"vg_lite_set_CLUT", SetClut),
        paths: paths.ok(),
    })
}

/// A VGLite status as an error: `VG_LITE_NOT_SUPPORT` is `Unsupported`.
pub(crate) fn status(what: &str, code: VgError) -> io::Result<()> {
    match code {
        SUCCESS => Ok(()),
        NOT_SUPPORT => Err(io::Error::new(
            io::ErrorKind::Unsupported,
            format!("{what}: VG_LITE_NOT_SUPPORT"),
        )),
        _ => Err(io::Error::other(format!("{what}: vg_lite_error_t {code}"))),
    }
}
