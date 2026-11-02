//! Rockchip's RGA through `librga`, the vendor's user-space library, loaded at runtime
//! (`dlopen`; nothing links it). This reaches what the mainline `rockchip-rga` V4L2 driver does
//! not: RGA3 on vendor kernels, gray input and Y-only output.
//!
//! Status: written, untested on board. The declarations follow `im2d_type.h` and
//! `im2d_buffer.h` of airockchip/librga (Apache-2.0) at commit 2b32edc; the unit tests run this
//! code against software stand-ins for the library's functions.

use std::ffi::{CStr, c_char, c_int, c_void};
use std::io;
use std::os::fd::{AsRawFd, BorrowedFd};

use crate::caps::Route;
use crate::resize::{Filter, source_size};
use crate::transform::{Transform, check_crop, check_size};

/// `im_colorkey_range`.
#[repr(C)]
#[derive(Clone, Copy, Default)]
pub(crate) struct ColorkeyRange {
    max: c_int,
    min: c_int,
}

/// `im_nn_t`.
#[repr(C)]
#[derive(Clone, Copy, Default)]
struct Nn {
    scale: [c_int; 3],
    offset: [c_int; 3],
}

/// `rga_buffer_t`; the library fills it (`wrapbuffer_*_t`) and takes it by value.
#[repr(C)]
#[derive(Clone, Copy)]
pub(crate) struct RgaBuffer {
    vir_addr: *mut c_void,
    phy_addr: *mut c_void,
    fd: c_int,
    width: c_int,
    height: c_int,
    wstride: c_int,
    hstride: c_int,
    format: c_int,
    color_space_mode: c_int,
    /// A union with `alpha_bit` (two `u16`).
    global_alpha: c_int,
    rd_mode: c_int,
    color: c_int,
    colorkey_range: ColorkeyRange,
    nn: Nn,
    rop_code: c_int,
    handle: u32,
}

// The sizes of `rga_buffer_t` a C compiler gives from the header.
#[cfg(target_pointer_width = "64")]
const _: () = assert!(size_of::<RgaBuffer>() == 96);
#[cfg(target_pointer_width = "32")]
const _: () = assert!(size_of::<RgaBuffer>() == 88);

type WrapFd = unsafe extern "C" fn(c_int, c_int, c_int, c_int, c_int, c_int) -> RgaBuffer;
type WrapAddr = unsafe extern "C" fn(*mut c_void, c_int, c_int, c_int, c_int, c_int) -> RgaBuffer;
type CvtColor = unsafe extern "C" fn(RgaBuffer, RgaBuffer, c_int, c_int, c_int, c_int) -> c_int;
type Resize = unsafe extern "C" fn(RgaBuffer, RgaBuffer, f64, f64, c_int, c_int) -> c_int;
type QueryString = unsafe extern "C" fn(c_int) -> *const c_char;
type Palette = unsafe extern "C" fn(RgaBuffer, RgaBuffer, RgaBuffer, c_int) -> c_int;
type Colorkey = unsafe extern "C" fn(RgaBuffer, RgaBuffer, ColorkeyRange, c_int, c_int) -> c_int;
type Crop = unsafe extern "C" fn(RgaBuffer, RgaBuffer, ImRect, c_int) -> c_int;
/// `imflip_t` and `imrotate_t`: a mode or a rotation, then `sync`.
type Turn = unsafe extern "C" fn(RgaBuffer, RgaBuffer, c_int, c_int) -> c_int;

/// `im_rect`.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct ImRect {
    x: c_int,
    y: c_int,
    width: c_int,
    height: c_int,
}

/// The library's entry points.
#[derive(Clone, Copy)]
pub(crate) struct Api {
    pub(crate) wrap_fd: WrapFd,
    pub(crate) wrap_addr: WrapAddr,
    pub(crate) cvtcolor: CvtColor,
    pub(crate) resize: Resize,
    pub(crate) querystring: QueryString,
    /// `impalette_t` and `imcolorkey_t`: every build we know exports them, but a missing one
    /// only makes its operation `Unsupported`.
    pub(crate) palette: Option<Palette>,
    pub(crate) colorkey: Option<Colorkey>,
    /// `imcrop_t`, `imflip_t`, `imrotate_t`: optional likewise.
    pub(crate) crop: Option<Crop>,
    pub(crate) flip: Option<Turn>,
    pub(crate) rotate: Option<Turn>,
}

/// `IM_STATUS_SUCCESS` and `IM_STATUS_NOERROR`.
const STATUS_OK: [c_int; 2] = [1, 2];
/// `IM_YUV_TO_RGB_BT601_FULL`: Y maps to R, G and B unscaled.
const YUV_TO_RGB_BT601_FULL: c_int = 2;
/// `IM_COLOR_SPACE_DEFAULT`.
const COLOR_SPACE_DEFAULT: c_int = 0;
/// `RGA_VERSION` of `IM_INFORMATION`.
const INFO_VERSION: c_int = 1;
/// The legacy `INTER_NEAREST`, also `IM_INTERP_DEFAULT`.
const INTER_NEAREST: c_int = 0;
/// `IM_INTERP_LINEAR`.
const INTERP_LINEAR: c_int = 1;
/// `IM_INTERP_AVERAGE`.
const INTERP_AVERAGE: c_int = 3;
/// `IM_ALPHA_COLORKEY_INVERTED`: by its name, the foreground pixels outside the range are
/// removed and those inside are drawn (`NORMAL` removes the range, per librga's sample).
const COLORKEY_INVERTED: c_int = 1 << 18;
/// `IM_HAL_TRANSFORM_*`.
const ROT_90: c_int = 1;
const ROT_180: c_int = 1 << 1;
const ROT_270: c_int = 1 << 2;
const FLIP_H: c_int = 1 << 3;
const FLIP_V: c_int = 1 << 4;

/// A pixel format of [`Image`], with `RkRgaSurfaceFormat`'s value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(i32)]
pub enum RgaFormat {
    /// R, G, B, A bytes.
    Rgba8888 = 0x0 << 8,
    /// R, G, B, X bytes: the fourth byte is ignored and the pixel is opaque.
    Rgbx8888 = 0x1 << 8,
    /// R, G, B bytes.
    Rgb888 = 0x2 << 8,
    /// B, G, R, A bytes.
    Bgra8888 = 0x3 << 8,
    /// B, G, R bytes.
    Bgr888 = 0x7 << 8,
    /// Y plane, then interleaved Cb, Cr at half resolution.
    Nv12 = 0xa << 8,
    /// `RK_FORMAT_YCbCr_400`: Y only.
    Gray8 = 0x15 << 8,
    /// `RK_FORMAT_BPP8`: 8-bit palette indices.
    Bpp8 = 0x13 << 8,
}

impl RgaFormat {
    /// The bytes of `pixels` pixels in the frame (the chroma of NV12 included).
    fn frame_len(self, pixels: usize) -> usize {
        match self {
            Self::Rgba8888 | Self::Rgbx8888 | Self::Bgra8888 => pixels * 4,
            Self::Rgb888 | Self::Bgr888 => pixels * 3,
            Self::Nv12 => pixels * 3 / 2,
            Self::Gray8 | Self::Bpp8 => pixels,
        }
    }
}

/// An image's size and layout. Strides are in pixels, as librga counts them. The hardware's
/// alignment limits (they differ between RGA2 and RGA3) come back as errors from the library.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Image {
    pub format: RgaFormat,
    pub width: u32,
    pub height: u32,
    pub wstride: u32,
    pub hstride: u32,
}

impl Image {
    /// Tightly packed rows.
    pub fn packed(format: RgaFormat, width: u32, height: u32) -> Self {
        Self {
            format,
            width,
            height,
            wstride: width,
            hstride: height,
        }
    }

    /// The bytes the image spans.
    pub fn frame_len(&self) -> usize {
        self.format
            .frame_len(self.wstride as usize * self.hstride as usize)
    }
}

/// `librga`, loaded. See the module docs.
pub struct Librga {
    api: Api,
    /// The `dlopen` handle, never closed: the library keeps process-wide state.
    _handle: *mut c_void,
}

// SAFETY: the handle is never used after loading; librga's blit calls are thread-safe.
unsafe impl Send for Librga {}
// SAFETY: as above.
unsafe impl Sync for Librga {}

impl Librga {
    /// Load `librga.so` from the system's library path.
    pub fn load() -> io::Result<Self> {
        for name in [c"librga.so", c"librga.so.2"] {
            // SAFETY: a valid name; the library's initializers are the vendor's.
            let handle = unsafe { libc::dlopen(name.as_ptr(), libc::RTLD_NOW | libc::RTLD_LOCAL) };
            if !handle.is_null() {
                // SAFETY: the handle was just opened.
                return unsafe { Self::from_handle(handle) };
            }
        }
        Err(io::Error::new(
            io::ErrorKind::NotFound,
            "librga.so not found",
        ))
    }

    /// # Safety
    /// `handle` must be a `dlopen` handle of a librga build with the C entry points of the
    /// header this module follows.
    unsafe fn from_handle(handle: *mut c_void) -> io::Result<Self> {
        let symbol = |name: &CStr| {
            // SAFETY: a valid handle and name.
            let found = unsafe { libc::dlsym(handle, name.as_ptr()) };
            if found.is_null() {
                Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("librga has no {name:?}"),
                ))
            } else {
                Ok(found)
            }
        };
        // SAFETY: each symbol has the declared signature in the header.
        let api = unsafe {
            Api {
                wrap_fd: std::mem::transmute::<*mut c_void, WrapFd>(symbol(c"wrapbuffer_fd_t")?),
                wrap_addr: std::mem::transmute::<*mut c_void, WrapAddr>(symbol(
                    c"wrapbuffer_virtualaddr_t",
                )?),
                cvtcolor: std::mem::transmute::<*mut c_void, CvtColor>(symbol(c"imcvtcolor_t")?),
                resize: std::mem::transmute::<*mut c_void, Resize>(symbol(c"imresize_t")?),
                querystring: std::mem::transmute::<*mut c_void, QueryString>(symbol(
                    c"querystring",
                )?),
                palette: symbol(c"impalette_t")
                    .ok()
                    .map(|f| std::mem::transmute::<*mut c_void, Palette>(f)),
                colorkey: symbol(c"imcolorkey_t")
                    .ok()
                    .map(|f| std::mem::transmute::<*mut c_void, Colorkey>(f)),
                crop: symbol(c"imcrop_t")
                    .ok()
                    .map(|f| std::mem::transmute::<*mut c_void, Crop>(f)),
                flip: symbol(c"imflip_t")
                    .ok()
                    .map(|f| std::mem::transmute::<*mut c_void, Turn>(f)),
                rotate: symbol(c"imrotate_t")
                    .ok()
                    .map(|f| std::mem::transmute::<*mut c_void, Turn>(f)),
            }
        };
        Ok(Self {
            api,
            _handle: handle,
        })
    }

    #[cfg(test)]
    pub(crate) fn with_api(api: Api) -> Self {
        Self {
            api,
            _handle: std::ptr::null_mut(),
        }
    }

    /// The library's and driver's version report.
    pub fn version(&self) -> String {
        // SAFETY: returns a NUL-terminated string the library owns, or null.
        let text = unsafe { (self.api.querystring)(INFO_VERSION) };
        if text.is_null() {
            return String::new();
        }
        // SAFETY: as above.
        unsafe { CStr::from_ptr(text) }
            .to_string_lossy()
            .into_owned()
    }

    fn wrap_fd(&self, image: Image, fd: BorrowedFd<'_>) -> RgaBuffer {
        let (w, h, ws, hs) = dims(image);
        // SAFETY: only fills a struct; the fd is used by the blit, while it is borrowed.
        unsafe { (self.api.wrap_fd)(fd.as_raw_fd(), w, h, ws, hs, image.format as c_int) }
    }

    fn wrap_ptr(&self, image: Image, ptr: *const u8) -> RgaBuffer {
        let (w, h, ws, hs) = dims(image);
        // SAFETY: only fills a struct; the blit reads or writes through it, with the caller's
        // length checked against the image.
        unsafe { (self.api.wrap_addr)(ptr.cast_mut().cast(), w, h, ws, hs, image.format as c_int) }
    }

    /// Convert the pixel format between dmabufs of the same size, without a CPU copy.
    pub fn convert_dmabuf(
        &self,
        src: Image,
        src_fd: BorrowedFd<'_>,
        dst: Image,
        dst_fd: BorrowedFd<'_>,
    ) -> io::Result<()> {
        self.cvtcolor(
            self.wrap_fd(src, src_fd),
            src,
            self.wrap_fd(dst, dst_fd),
            dst,
        )
    }

    /// Scale between dmabufs of the same format, to `dst`'s size.
    pub fn scale_dmabuf(
        &self,
        src: Image,
        src_fd: BorrowedFd<'_>,
        dst: Image,
        dst_fd: BorrowedFd<'_>,
    ) -> io::Result<()> {
        self.resize(self.wrap_fd(src, src_fd), self.wrap_fd(dst, dst_fd), 0)
    }

    /// The source image a resize reads (an area downscale crops the remainder away by
    /// narrowing the image inside its strides) and librga's interpolation for `filter`.
    fn resize_plan(src: Image, dst: Image, filter: Filter) -> io::Result<(Image, c_int)> {
        let (width, height) = source_size(
            (src.width, src.height),
            (dst.width, dst.height),
            filter,
            Route::Librga,
        )?;
        let interpolation = match filter {
            Filter::Area => INTERP_AVERAGE,
            Filter::Bilinear => INTERP_LINEAR,
        };
        Ok((
            Image {
                width,
                height,
                ..src
            },
            interpolation,
        ))
    }

    /// Resize between dmabufs of the same format, to `dst`'s size, with `filter`
    /// (`IM_INTERP_AVERAGE` or `IM_INTERP_LINEAR`). Gray8 works directly. [`Filter::Area`]
    /// must be an integer downscale; anything else is `Unsupported`.
    pub fn resize_dmabuf(
        &self,
        src: Image,
        src_fd: BorrowedFd<'_>,
        dst: Image,
        dst_fd: BorrowedFd<'_>,
        filter: Filter,
    ) -> io::Result<()> {
        let (src, interpolation) = Self::resize_plan(src, dst, filter)?;
        self.resize(
            self.wrap_fd(src, src_fd),
            self.wrap_fd(dst, dst_fd),
            interpolation,
        )
    }

    /// [`Librga::resize_dmabuf`] between byte buffers.
    pub fn resize_bytes(
        &self,
        src: Image,
        from: &[u8],
        dst: Image,
        to: &mut [u8],
        filter: Filter,
    ) -> io::Result<()> {
        check(src, from.len(), dst, to.len())?;
        let (src, interpolation) = Self::resize_plan(src, dst, filter)?;
        self.resize(
            self.wrap_ptr(src, from.as_ptr()),
            self.wrap_ptr(dst, to.as_mut_ptr()),
            interpolation,
        )
    }

    /// 8-bit indices to RGBA8 through a 256-entry palette (`impalette`, a `BPP8` source),
    /// tightly packed byte buffers.
    pub fn palette_bytes(
        &self,
        width: u32,
        height: u32,
        indices: &[u8],
        lut: &[[u8; 4]; 256],
        dst: &mut [u8],
    ) -> io::Result<()> {
        let (s, d) = (
            Image::packed(RgaFormat::Bpp8, width, height),
            Image::packed(RgaFormat::Rgba8888, width, height),
        );
        check(s, indices.len(), d, dst.len())?;
        self.palette(
            self.wrap_ptr(s, indices.as_ptr()),
            self.wrap_ptr(d, dst.as_mut_ptr()),
            lut,
        )
    }

    /// [`Librga::palette_bytes`] between dmabufs (`BPP8` indices into RGBA8); the palette is
    /// read from host memory.
    pub fn palette_dmabuf(
        &self,
        width: u32,
        height: u32,
        indices: BorrowedFd<'_>,
        lut: &[[u8; 4]; 256],
        dst: BorrowedFd<'_>,
    ) -> io::Result<()> {
        self.palette(
            self.wrap_fd(Image::packed(RgaFormat::Bpp8, width, height), indices),
            self.wrap_fd(Image::packed(RgaFormat::Rgba8888, width, height), dst),
            lut,
        )
    }

    fn palette(&self, src: RgaBuffer, dst: RgaBuffer, lut: &[[u8; 4]; 256]) -> io::Result<()> {
        let call = self.api.palette.ok_or_else(|| missing("impalette_t"))?;
        let table = self.wrap_ptr(
            Image::packed(RgaFormat::Rgba8888, 256, 1),
            lut.as_ptr().cast(),
        );
        // SAFETY: the three buffers describe live memory for the (synchronous) call.
        status(unsafe { call(src, dst, table, 1) })
    }

    /// Colour key onto RGBA8: the pixels of `src` (R, G, B, X) whose every channel lies in
    /// `min..=max` are drawn opaque onto `dst`; the others leave `dst` as it was. With `dst`
    /// cleared to zero, its alpha is the mask. Byte buffers, tightly packed; `dst` is cleared
    /// here.
    pub fn color_key_bytes(
        &self,
        width: u32,
        height: u32,
        src: &[u8],
        min: [u8; 3],
        max: [u8; 3],
        dst: &mut [u8],
    ) -> io::Result<()> {
        let (s, d) = (
            Image::packed(RgaFormat::Rgbx8888, width, height),
            Image::packed(RgaFormat::Rgba8888, width, height),
        );
        check(s, src.len(), d, dst.len())?;
        dst[..d.frame_len()].fill(0);
        self.colorkey(
            self.wrap_ptr(s, src.as_ptr()),
            self.wrap_ptr(d, dst.as_mut_ptr()),
            min,
            max,
        )
    }

    /// [`Librga::color_key_bytes`] between dmabufs. The caller clears `dst` (alpha 0) first.
    #[allow(clippy::too_many_arguments)]
    pub fn color_key_dmabuf(
        &self,
        width: u32,
        height: u32,
        src: BorrowedFd<'_>,
        min: [u8; 3],
        max: [u8; 3],
        dst: BorrowedFd<'_>,
    ) -> io::Result<()> {
        self.colorkey(
            self.wrap_fd(Image::packed(RgaFormat::Rgbx8888, width, height), src),
            self.wrap_fd(Image::packed(RgaFormat::Rgba8888, width, height), dst),
            min,
            max,
        )
    }

    /// The 8-bit mask of [`Librga::color_key_bytes`]: 255 where every channel of `src` lies in
    /// `min..=max`, else 0. The alpha is read out on the CPU.
    pub fn color_key_mask(
        &self,
        width: u32,
        height: u32,
        src: &[u8],
        min: [u8; 3],
        max: [u8; 3],
        mask: &mut [u8],
    ) -> io::Result<()> {
        let pixels = width as usize * height as usize;
        if mask.len() < pixels {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("a mask of {} bytes for {pixels} pixels", mask.len()),
            ));
        }
        let mut rgba = vec![0u8; pixels * 4];
        self.color_key_bytes(width, height, src, min, max, &mut rgba)?;
        for (m, p) in mask.iter_mut().zip(rgba.as_chunks::<4>().0) {
            *m = p[3];
        }
        Ok(())
    }

    fn colorkey(
        &self,
        src: RgaBuffer,
        dst: RgaBuffer,
        min: [u8; 3],
        max: [u8; 3],
    ) -> io::Result<()> {
        let call = self.api.colorkey.ok_or_else(|| missing("imcolorkey_t"))?;
        // Packed A, B, G, R (librga's sample); X is not compared, so alpha spans everything.
        let abgr = |[r, g, b]: [u8; 3], a: u8| i32::from_le_bytes([r, g, b, a]);
        let range = ColorkeyRange {
            max: abgr(max, 0xff),
            min: abgr(min, 0),
        };
        // SAFETY: both buffers describe live memory for the (synchronous) call.
        status(unsafe { call(src, dst, range, COLORKEY_INVERTED, 1) })
    }

    /// Copy the `rect` (left, top, width, height) of `src` into `dst`, which has the
    /// rectangle's size; a full-frame rectangle is a copy. Byte buffers.
    pub fn crop_bytes(
        &self,
        src: Image,
        from: &[u8],
        rect: (u32, u32, u32, u32),
        dst: Image,
        to: &mut [u8],
    ) -> io::Result<()> {
        check(src, from.len(), dst, to.len())?;
        self.crop(
            src,
            self.wrap_ptr(src, from.as_ptr()),
            rect,
            dst,
            self.wrap_ptr(dst, to.as_mut_ptr()),
        )
    }

    /// [`Librga::crop_bytes`] between dmabufs.
    pub fn crop_dmabuf(
        &self,
        src: Image,
        src_fd: BorrowedFd<'_>,
        rect: (u32, u32, u32, u32),
        dst: Image,
        dst_fd: BorrowedFd<'_>,
    ) -> io::Result<()> {
        self.crop(
            src,
            self.wrap_fd(src, src_fd),
            rect,
            dst,
            self.wrap_fd(dst, dst_fd),
        )
    }

    fn crop(
        &self,
        si: Image,
        src: RgaBuffer,
        rect: (u32, u32, u32, u32),
        di: Image,
        dst: RgaBuffer,
    ) -> io::Result<()> {
        check_crop(rect, si.width, si.height)?;
        check_size((di.width, di.height), (rect.2, rect.3))?;
        let call = self.api.crop.ok_or_else(|| missing("imcrop_t"))?;
        let rect = ImRect {
            x: rect.0 as c_int,
            y: rect.1 as c_int,
            width: rect.2 as c_int,
            height: rect.3 as c_int,
        };
        // SAFETY: both buffers describe live memory for the (synchronous) call.
        status(unsafe { call(src, dst, rect, 1) })
    }

    /// Flip or turn `src` into `dst` ([`Transform::output_size`] of the source). Byte buffers.
    pub fn transform_bytes(
        &self,
        src: Image,
        from: &[u8],
        dst: Image,
        to: &mut [u8],
        transform: Transform,
    ) -> io::Result<()> {
        check(src, from.len(), dst, to.len())?;
        self.transform(
            src,
            self.wrap_ptr(src, from.as_ptr()),
            dst,
            self.wrap_ptr(dst, to.as_mut_ptr()),
            transform,
        )
    }

    /// [`Librga::transform_bytes`] between dmabufs.
    pub fn transform_dmabuf(
        &self,
        src: Image,
        src_fd: BorrowedFd<'_>,
        dst: Image,
        dst_fd: BorrowedFd<'_>,
        transform: Transform,
    ) -> io::Result<()> {
        self.transform(
            src,
            self.wrap_fd(src, src_fd),
            dst,
            self.wrap_fd(dst, dst_fd),
            transform,
        )
    }

    fn transform(
        &self,
        si: Image,
        src: RgaBuffer,
        di: Image,
        dst: RgaBuffer,
        transform: Transform,
    ) -> io::Result<()> {
        check_size(
            (di.width, di.height),
            transform.output_size(si.width, si.height),
        )?;
        let (call, name, mode) = match transform {
            Transform::FlipHorizontal => (self.api.flip, "imflip_t", FLIP_H),
            Transform::FlipVertical => (self.api.flip, "imflip_t", FLIP_V),
            Transform::Rotate90 => (self.api.rotate, "imrotate_t", ROT_90),
            Transform::Rotate180 => (self.api.rotate, "imrotate_t", ROT_180),
            Transform::Rotate270 => (self.api.rotate, "imrotate_t", ROT_270),
        };
        let call = call.ok_or_else(|| missing(name))?;
        // SAFETY: both buffers describe live memory for the (synchronous) call.
        status(unsafe { call(src, dst, mode, 1) })
    }

    /// Convert the pixel format between byte buffers of the same size.
    pub fn convert(&self, src: Image, from: &[u8], dst: Image, to: &mut [u8]) -> io::Result<()> {
        check(src, from.len(), dst, to.len())?;
        self.cvtcolor(
            self.wrap_ptr(src, from.as_ptr()),
            src,
            self.wrap_ptr(dst, to.as_mut_ptr()),
            dst,
        )
    }

    fn cvtcolor(&self, src: RgaBuffer, si: Image, dst: RgaBuffer, di: Image) -> io::Result<()> {
        let mode = if si.format == RgaFormat::Gray8 {
            YUV_TO_RGB_BT601_FULL
        } else {
            COLOR_SPACE_DEFAULT
        };
        // SAFETY: both buffers describe live memory for the (synchronous) call.
        status(unsafe {
            (self.api.cvtcolor)(src, dst, si.format as c_int, di.format as c_int, mode, 1)
        })
    }

    fn resize(&self, src: RgaBuffer, dst: RgaBuffer, interpolation: c_int) -> io::Result<()> {
        // SAFETY: as in `cvtcolor`; zero factors mean "to the destination's size".
        status(unsafe { (self.api.resize)(src, dst, 0.0, 0.0, interpolation, 1) })
    }

    /// RGB8 to RGBA8, tightly packed, alpha opaque.
    pub fn rgb_to_rgba(&self, w: u32, h: u32, src: &[u8], dst: &mut [u8]) -> io::Result<()> {
        let (s, d) = (RgaFormat::Rgb888, RgaFormat::Rgba8888);
        self.convert(Image::packed(s, w, h), src, Image::packed(d, w, h), dst)
    }

    /// RGBA8 to RGB8, tightly packed.
    pub fn rgba_to_rgb(&self, w: u32, h: u32, src: &[u8], dst: &mut [u8]) -> io::Result<()> {
        let (s, d) = (RgaFormat::Rgba8888, RgaFormat::Rgb888);
        self.convert(Image::packed(s, w, h), src, Image::packed(d, w, h), dst)
    }

    /// Gray8 to RGBA8 (R = G = B = gray, alpha opaque), tightly packed.
    pub fn gray_to_rgba(&self, w: u32, h: u32, src: &[u8], dst: &mut [u8]) -> io::Result<()> {
        let (s, d) = (RgaFormat::Gray8, RgaFormat::Rgba8888);
        self.convert(Image::packed(s, w, h), src, Image::packed(d, w, h), dst)
    }

    /// The red bytes of RGBA8, tightly packed. A color conversion would give luma, not red, so
    /// the frame is relabelled: Y-only at four times the width, scaled to the width. That is red
    /// only if the hardware takes the first of each four samples. The header's interpolation 0
    /// is both the legacy `INTER_NEAREST` and `IM_INTERP_DEFAULT`, so it may instead average them
    /// or pick another. The conformance run on a board decides, and records a failure if so.
    pub fn rgba_red(&self, w: u32, h: u32, src: &[u8], dst: &mut [u8]) -> io::Result<()> {
        let from = Image::packed(RgaFormat::Gray8, w * 4, h);
        let to = Image::packed(RgaFormat::Gray8, w, h);
        check(from, src.len(), to, dst.len())?;
        self.resize(
            self.wrap_ptr(from, src.as_ptr()),
            self.wrap_ptr(to, dst.as_mut_ptr()),
            INTER_NEAREST,
        )
    }
}

fn dims(image: Image) -> (c_int, c_int, c_int, c_int) {
    (
        image.width as c_int,
        image.height as c_int,
        image.wstride as c_int,
        image.hstride as c_int,
    )
}

/// Both byte buffers cover their images.
fn check(src: Image, from: usize, dst: Image, to: usize) -> io::Result<()> {
    if from < src.frame_len() || to < dst.frame_len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "buffers of {from} and {to} bytes for frames of {} and {}",
                src.frame_len(),
                dst.frame_len()
            ),
        ));
    }
    Ok(())
}

fn missing(symbol: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::Unsupported,
        format!("this librga has no {symbol}"),
    )
}

fn status(code: c_int) -> io::Result<()> {
    if STATUS_OK.contains(&code) {
        Ok(())
    } else {
        Err(io::Error::other(format!("librga status {code}")))
    }
}

#[cfg(test)]
mod tests;
