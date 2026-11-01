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

/// `im_colorkey_range`.
#[repr(C)]
#[derive(Clone, Copy, Default)]
struct ColorkeyRange {
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

/// The library's entry points.
#[derive(Clone, Copy)]
pub(crate) struct Api {
    pub(crate) wrap_fd: WrapFd,
    pub(crate) wrap_addr: WrapAddr,
    pub(crate) cvtcolor: CvtColor,
    pub(crate) resize: Resize,
    pub(crate) querystring: QueryString,
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

/// A pixel format of [`Image`], with `RkRgaSurfaceFormat`'s value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(i32)]
pub enum RgaFormat {
    /// R, G, B, A bytes.
    Rgba8888 = 0x0 << 8,
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
}

impl RgaFormat {
    /// The bytes of `pixels` pixels in the frame (the chroma of NV12 included).
    fn frame_len(self, pixels: usize) -> usize {
        match self {
            Self::Rgba8888 | Self::Bgra8888 => pixels * 4,
            Self::Rgb888 | Self::Bgr888 => pixels * 3,
            Self::Nv12 => pixels * 3 / 2,
            Self::Gray8 => pixels,
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

fn status(code: c_int) -> io::Result<()> {
    if STATUS_OK.contains(&code) {
        Ok(())
    } else {
        Err(io::Error::other(format!("librga status {code}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    unsafe extern "C" fn wrap_addr(
        ptr: *mut c_void,
        width: c_int,
        height: c_int,
        wstride: c_int,
        hstride: c_int,
        format: c_int,
    ) -> RgaBuffer {
        RgaBuffer {
            vir_addr: ptr,
            phy_addr: std::ptr::null_mut(),
            fd: -1,
            width,
            height,
            wstride,
            hstride,
            format,
            color_space_mode: 0,
            global_alpha: 0xff,
            rd_mode: 0,
            color: 0,
            colorkey_range: ColorkeyRange::default(),
            nn: Nn::default(),
            rop_code: 0,
            handle: 0,
        }
    }

    unsafe extern "C" fn wrap_fd(
        fd: c_int,
        width: c_int,
        height: c_int,
        wstride: c_int,
        hstride: c_int,
        format: c_int,
    ) -> RgaBuffer {
        let mut buffer = unsafe {
            wrap_addr(
                std::ptr::null_mut(),
                width,
                height,
                wstride,
                hstride,
                format,
            )
        };
        buffer.fd = fd;
        buffer
    }

    /// Software `imcvtcolor_t` for the formats the helpers use.
    unsafe extern "C" fn cvtcolor(
        src: RgaBuffer,
        dst: RgaBuffer,
        sfmt: c_int,
        dfmt: c_int,
        mode: c_int,
        sync: c_int,
    ) -> c_int {
        assert_eq!(sync, 1);
        assert_eq!((src.format, dst.format), (sfmt, dfmt));
        let n = (src.wstride * src.hstride) as usize;
        let rgba = RgaFormat::Rgba8888 as c_int;
        let rgb = RgaFormat::Rgb888 as c_int;
        let gray = RgaFormat::Gray8 as c_int;
        let (s, d) = (src.vir_addr.cast::<u8>(), dst.vir_addr.cast::<u8>());
        // SAFETY: the wrapped buffers cover their images.
        unsafe {
            for i in 0..n {
                match (sfmt, dfmt) {
                    (a, b) if (a, b) == (rgb, rgba) => {
                        std::ptr::copy(s.add(i * 3), d.add(i * 4), 3);
                        *d.add(i * 4 + 3) = 0xff;
                    }
                    (a, b) if (a, b) == (rgba, rgb) => {
                        std::ptr::copy(s.add(i * 4), d.add(i * 3), 3);
                    }
                    (a, b) if (a, b) == (gray, rgba) => {
                        assert_eq!(mode, YUV_TO_RGB_BT601_FULL, "gray must keep its full range");
                        std::ptr::write_bytes(d.add(i * 4), *s.add(i), 3);
                        *d.add(i * 4 + 3) = 0xff;
                    }
                    _ => return -1,
                }
            }
        }
        1
    }

    /// Software `imresize_t`: nearest, sampling the first of each group.
    unsafe extern "C" fn resize(
        src: RgaBuffer,
        dst: RgaBuffer,
        fx: f64,
        fy: f64,
        interpolation: c_int,
        sync: c_int,
    ) -> c_int {
        assert_eq!((fx, fy, interpolation, sync), (0.0, 0.0, INTER_NEAREST, 1));
        let step = (src.width / dst.width) as usize;
        let (s, d) = (src.vir_addr.cast::<u8>(), dst.vir_addr.cast::<u8>());
        // SAFETY: the wrapped buffers cover their images.
        unsafe {
            for i in 0..(dst.wstride * dst.hstride) as usize {
                *d.add(i) = *s.add(i * step);
            }
        }
        2 // IM_STATUS_NOERROR also counts as success.
    }

    unsafe extern "C" fn querystring(name: c_int) -> *const c_char {
        assert_eq!(name, INFO_VERSION);
        c"stand-in 1.0".as_ptr()
    }

    fn stand_in() -> Librga {
        Librga::with_api(Api {
            wrap_fd,
            wrap_addr,
            cvtcolor,
            resize,
            querystring,
        })
    }

    #[test]
    fn the_helpers_convert_with_the_right_formats_and_modes() {
        let rga = stand_in();
        assert_eq!(rga.version(), "stand-in 1.0");
        let mut rgba = [0u8; 8];
        rga.rgb_to_rgba(2, 1, &[1, 2, 3, 4, 5, 6], &mut rgba)
            .unwrap();
        assert_eq!(rgba, [1, 2, 3, 255, 4, 5, 6, 255]);
        let mut rgb = [0u8; 6];
        rga.rgba_to_rgb(2, 1, &rgba, &mut rgb).unwrap();
        assert_eq!(rgb, [1, 2, 3, 4, 5, 6]);
        rga.gray_to_rgba(2, 1, &[9, 7], &mut rgba).unwrap();
        assert_eq!(rgba, [9, 9, 9, 255, 7, 7, 7, 255]);
        let mut red = [0u8; 2];
        rga.rgba_red(2, 1, &[1, 2, 3, 4, 5, 6, 7, 8], &mut red)
            .unwrap();
        assert_eq!(red, [1, 5]);
    }

    #[test]
    fn short_buffers_and_library_errors_are_refused() {
        let rga = stand_in();
        let mut short = [0u8; 7];
        let err = rga.rgb_to_rgba(2, 1, &[0; 6], &mut short).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert!(rga.rgb_to_rgba(2, 1, &[0; 5], &mut [0; 8]).is_err());
        // The stand-in does not convert BGR: its error status comes back.
        let (bgr, rgba) = (
            Image::packed(RgaFormat::Bgr888, 1, 1),
            Image::packed(RgaFormat::Rgba8888, 1, 1),
        );
        let err = rga.convert(bgr, &[0; 3], rgba, &mut [0; 4]).unwrap_err();
        assert!(err.to_string().contains("status -1"), "{err}");
    }

    #[test]
    fn frame_lengths_follow_the_format_and_strides() {
        assert_eq!(Image::packed(RgaFormat::Nv12, 4, 2).frame_len(), 12);
        let image = Image {
            format: RgaFormat::Rgb888,
            width: 2,
            height: 2,
            wstride: 4,
            hstride: 3,
        };
        assert_eq!(image.frame_len(), 36);
    }
}
