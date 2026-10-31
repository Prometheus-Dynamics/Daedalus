//! V4L2 mem2mem sessions: one device context with an output (source) and a capture
//! (destination) queue, configured once per format, size, stride and crop.
//!
//! The ioctl structs follow `linux/videodev2.h`. Their sizes are asserted on 64-bit targets and
//! the ioctl numbers are derived from them, so 32-bit builds with 64-bit `time_t` get the right
//! numbers too.

use std::ffi::CStr;
use std::fs::{File, OpenOptions};
use std::io;
use std::os::fd::{AsRawFd, BorrowedFd};
use std::path::{Path, PathBuf};

const CAP_VIDEO_M2M: u32 = 0x0000_8000;
const CAP_VIDEO_M2M_MPLANE: u32 = 0x0000_4000;
const CAP_DEVICE_CAPS: u32 = 0x8000_0000;

const BUF_TYPE_VIDEO_CAPTURE: u32 = 1;
const BUF_TYPE_VIDEO_OUTPUT: u32 = 2;
const BUF_TYPE_VIDEO_CAPTURE_MPLANE: u32 = 9;
const BUF_TYPE_VIDEO_OUTPUT_MPLANE: u32 = 10;
const MEMORY_MMAP: u32 = 1;
const MEMORY_DMABUF: u32 = 4;
const FIELD_NONE: u32 = 1;
const SEL_TGT_CROP: u32 = 0;

/// A V4L2 pixel format (`V4L2_PIX_FMT_*`); the comment gives the byte order in memory.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum PixelFormat {
    /// `RGB3`: R, G, B.
    Rgb24,
    /// `BGR3`: B, G, R.
    Bgr24,
    /// `AR24` (`ABGR32`): B, G, R, A.
    Abgr32,
    /// `XR24` (`XBGR32`): B, G, R, X.
    Xbgr32,
    /// `GREY`.
    Grey,
    /// `NV12`: Y plane, then interleaved U, V at half resolution (one buffer).
    Nv12,
    /// `NV21`: as NV12 with V, U.
    Nv21,
    /// `YU12` (I420): Y, U, V planes (one buffer).
    Yuv420,
}

impl PixelFormat {
    pub fn fourcc(self) -> u32 {
        let code = match self {
            PixelFormat::Rgb24 => *b"RGB3",
            PixelFormat::Bgr24 => *b"BGR3",
            PixelFormat::Abgr32 => *b"AR24",
            PixelFormat::Xbgr32 => *b"XR24",
            PixelFormat::Grey => *b"GREY",
            PixelFormat::Nv12 => *b"NV12",
            PixelFormat::Nv21 => *b"NV21",
            PixelFormat::Yuv420 => *b"YU12",
        };
        u32::from_le_bytes(code)
    }

    /// Bytes per pixel of the first plane.
    pub fn bytes(self) -> u32 {
        match self {
            PixelFormat::Rgb24 | PixelFormat::Bgr24 => 3,
            PixelFormat::Abgr32 | PixelFormat::Xbgr32 => 4,
            PixelFormat::Grey | PixelFormat::Nv12 | PixelFormat::Nv21 | PixelFormat::Yuv420 => 1,
        }
    }

    /// Bytes of a `height`-row frame with `stride`-byte rows (all planes).
    pub fn frame_len(self, stride: u32, height: u32) -> usize {
        let luma = stride as usize * height as usize;
        match self {
            PixelFormat::Nv12 | PixelFormat::Nv21 | PixelFormat::Yuv420 => {
                luma + stride as usize * height.div_ceil(2) as usize
            }
            _ => luma,
        }
    }
}

#[repr(C)]
struct Capability {
    driver: [u8; 16],
    card: [u8; 32],
    bus_info: [u8; 32],
    version: u32,
    capabilities: u32,
    device_caps: u32,
    reserved: [u32; 3],
}

#[repr(C)]
#[derive(Clone, Copy)]
struct PixFormat {
    width: u32,
    height: u32,
    pixelformat: u32,
    field: u32,
    bytesperline: u32,
    sizeimage: u32,
    colorspace: u32,
    priv_: u32,
    flags: u32,
    ycbcr_enc: u32,
    quantization: u32,
    xfer_func: u32,
}

#[repr(C, packed)]
#[derive(Clone, Copy)]
struct PlanePixFormat {
    sizeimage: u32,
    bytesperline: u32,
    reserved: [u16; 6],
}

#[repr(C, packed)]
#[derive(Clone, Copy)]
struct PixFormatMplane {
    width: u32,
    height: u32,
    pixelformat: u32,
    field: u32,
    colorspace: u32,
    plane_fmt: [PlanePixFormat; 8],
    num_planes: u8,
    flags: u8,
    ycbcr_enc: u8,
    quantization: u8,
    xfer_func: u8,
    reserved: [u8; 7],
}

#[repr(C)]
#[derive(Clone, Copy)]
union FormatUnion {
    pix: PixFormat,
    pix_mp: PixFormatMplane,
    raw: [u8; 200],
    // The kernel union holds pointers (`v4l2_window`): pointer alignment.
    _align: *const u8,
}

#[repr(C)]
struct Format {
    type_: u32,
    fmt: FormatUnion,
}

#[repr(C)]
struct RequestBuffers {
    count: u32,
    type_: u32,
    memory: u32,
    capabilities: u32,
    flags: u8,
    reserved: [u8; 3],
}

#[repr(C)]
#[derive(Clone, Copy)]
struct Timecode {
    type_: u32,
    flags: u32,
    frames: u8,
    seconds: u8,
    minutes: u8,
    hours: u8,
    userbits: [u8; 4],
}

#[repr(C)]
#[derive(Clone, Copy)]
union PlaneMem {
    mem_offset: u32,
    userptr: libc::c_ulong,
    fd: i32,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct Plane {
    bytesused: u32,
    length: u32,
    m: PlaneMem,
    data_offset: u32,
    reserved: [u32; 11],
}

#[repr(C)]
#[derive(Clone, Copy)]
union BufferMem {
    offset: u32,
    userptr: libc::c_ulong,
    planes: *mut Plane,
    fd: i32,
}

#[repr(C)]
struct Buffer {
    index: u32,
    type_: u32,
    bytesused: u32,
    flags: u32,
    field: u32,
    timestamp: libc::timeval,
    timecode: Timecode,
    sequence: u32,
    memory: u32,
    m: BufferMem,
    length: u32,
    reserved2: u32,
    request_fd: i32,
}

#[repr(C)]
struct Selection {
    type_: u32,
    target: u32,
    flags: u32,
    left: i32,
    top: i32,
    width: u32,
    height: u32,
    reserved: [u32; 9],
}

// The kernel's sizes on 64-bit Linux.
#[cfg(target_pointer_width = "64")]
const _: () = {
    assert!(size_of::<Capability>() == 104);
    assert!(size_of::<PixFormatMplane>() == 192);
    assert!(size_of::<Format>() == 208);
    assert!(size_of::<RequestBuffers>() == 20);
    assert!(size_of::<Plane>() == 64);
    assert!(size_of::<Buffer>() == 88);
    assert!(size_of::<Selection>() == 64);
};

/// `_IOC(dir, 'V', nr, size)` as the kernel encodes it on the targets Daedalus supports.
const fn ioc(dir: u64, nr: u64, size: usize) -> u64 {
    (dir << 30) | ((size as u64) << 16) | ((b'V' as u64) << 8) | nr
}
const READ: u64 = 2;
const WRITE: u64 = 1;
const VIDIOC_QUERYCAP: u64 = ioc(READ, 0, size_of::<Capability>());
const VIDIOC_S_FMT: u64 = ioc(READ | WRITE, 5, size_of::<Format>());
const VIDIOC_REQBUFS: u64 = ioc(READ | WRITE, 8, size_of::<RequestBuffers>());
const VIDIOC_QUERYBUF: u64 = ioc(READ | WRITE, 9, size_of::<Buffer>());
const VIDIOC_QBUF: u64 = ioc(READ | WRITE, 15, size_of::<Buffer>());
const VIDIOC_DQBUF: u64 = ioc(READ | WRITE, 17, size_of::<Buffer>());
const VIDIOC_STREAMON: u64 = ioc(WRITE, 18, size_of::<i32>());
const VIDIOC_STREAMOFF: u64 = ioc(WRITE, 19, size_of::<i32>());
const VIDIOC_S_SELECTION: u64 = ioc(READ | WRITE, 94, size_of::<Selection>());

fn ioctl<T>(file: &File, request: u64, arg: &mut T) -> io::Result<()> {
    loop {
        // SAFETY: `arg` is the struct `request` encodes (its size is part of the number), and it
        // outlives the call.
        let r = unsafe { libc::ioctl(file.as_raw_fd(), request as _, std::ptr::from_mut(arg)) };
        if r >= 0 {
            return Ok(());
        }
        let err = io::Error::last_os_error();
        if err.kind() != io::ErrorKind::Interrupted {
            return Err(err);
        }
    }
}

/// The driver name of a V4L2 device and, for a mem2mem device, whether it is multi-planar.
pub(crate) fn query(file: &File) -> io::Result<(String, Option<bool>)> {
    // SAFETY: an all-zero `v4l2_capability` is valid.
    let mut cap: Capability = unsafe { std::mem::zeroed() };
    ioctl(file, VIDIOC_QUERYCAP, &mut cap)?;
    let caps = if cap.capabilities & CAP_DEVICE_CAPS != 0 {
        cap.device_caps
    } else {
        cap.capabilities
    };
    let driver = CStr::from_bytes_until_nul(&cap.driver)
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_default();
    let m2m = if caps & CAP_VIDEO_M2M_MPLANE != 0 {
        Some(true)
    } else if caps & CAP_VIDEO_M2M != 0 {
        Some(false)
    } else {
        None
    };
    Ok((driver, m2m))
}

/// The first `/dev/video*` mem2mem device whose driver is `driver` (`"rockchip-rga"`, `"vim2m"`).
pub fn find_m2m_device(driver: &str) -> Option<PathBuf> {
    let mut nodes: Vec<PathBuf> = std::fs::read_dir("/dev")
        .ok()?
        .flatten()
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("video"))
        })
        .collect();
    nodes.sort();
    nodes.into_iter().find(|path| {
        OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .ok()
            .and_then(|f| query(&f).ok())
            .is_some_and(|(name, m2m)| name == driver && m2m.is_some())
    })
}

/// One side of a conversion: pixel format, size and row stride.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Layout {
    pub format: PixelFormat,
    pub width: u32,
    pub height: u32,
    /// Bytes per row of the first plane.
    pub stride: u32,
}

impl Layout {
    /// Tightly packed rows.
    pub fn packed(format: PixelFormat, width: u32, height: u32) -> Self {
        Self {
            format,
            width,
            height,
            stride: width * format.bytes(),
        }
    }

    /// Bytes of a frame in this layout.
    pub fn frame_len(&self) -> usize {
        self.format.frame_len(self.stride, self.height)
    }
}

/// A source rectangle to convert from (the rest of the frame is ignored).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Rect {
    pub left: i32,
    pub top: i32,
    pub width: u32,
    pub height: u32,
}

/// How a session's buffers are provided.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum Memory {
    /// Driver buffers mapped into the process: bytes are copied in and out.
    Mmap,
    /// The caller's dmabufs, queued per conversion: no copies.
    Dmabuf,
}

/// An `mmap`ed V4L2 buffer.
struct Mapped {
    ptr: *mut u8,
    len: usize,
}

impl Drop for Mapped {
    fn drop(&mut self) {
        // SAFETY: `ptr`/`len` came from a successful `mmap` of this buffer.
        unsafe { libc::munmap(self.ptr.cast(), self.len) };
    }
}

/// A configured mem2mem context: `src` into `dst`, scaling to `dst`'s size, from `crop` of the
/// source when given.
pub(crate) struct Session {
    file: File,
    mplane: bool,
    out_type: u32,
    cap_type: u32,
    memory: Memory,
    mapped: Option<(Mapped, Mapped)>,
    in_len: usize,
    out_len: usize,
}

// SAFETY: the mappings are owned by the session and only touched through `&mut self`.
unsafe impl Send for Session {}

impl Session {
    pub(crate) fn open(
        path: &Path,
        src: Layout,
        dst: Layout,
        crop: Option<Rect>,
        memory: Memory,
    ) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        let (_, m2m) = query(&file)?;
        let mplane =
            m2m.ok_or_else(|| io::Error::other(format!("{} is not mem2mem", path.display())))?;
        let (out_type, cap_type) = if mplane {
            (BUF_TYPE_VIDEO_OUTPUT_MPLANE, BUF_TYPE_VIDEO_CAPTURE_MPLANE)
        } else {
            (BUF_TYPE_VIDEO_OUTPUT, BUF_TYPE_VIDEO_CAPTURE)
        };
        let in_len = set_format(&file, out_type, mplane, src)?;
        let out_len = set_format(&file, cap_type, mplane, dst)?;
        if let Some(r) = crop {
            let mut sel = Selection {
                // The selection API takes the single-planar type for both kinds of device.
                type_: BUF_TYPE_VIDEO_OUTPUT,
                target: SEL_TGT_CROP,
                flags: 0,
                left: r.left,
                top: r.top,
                width: r.width,
                height: r.height,
                reserved: [0; 9],
            };
            ioctl(&file, VIDIOC_S_SELECTION, &mut sel)?;
            if (sel.left, sel.top, sel.width, sel.height) != (r.left, r.top, r.width, r.height) {
                return Err(io::Error::other(format!(
                    "the device adjusted crop {r:?} to {}x{}+{}+{}",
                    sel.width, sel.height, sel.left, sel.top
                )));
            }
        }
        let mem = match memory {
            Memory::Mmap => MEMORY_MMAP,
            Memory::Dmabuf => MEMORY_DMABUF,
        };
        request_buffer(&file, out_type, mem)?;
        request_buffer(&file, cap_type, mem)?;
        let mapped = match memory {
            Memory::Mmap => Some((
                map_buffer(&file, out_type, mplane)?,
                map_buffer(&file, cap_type, mplane)?,
            )),
            Memory::Dmabuf => None,
        };
        for ty in [out_type, cap_type] {
            let mut t = ty as i32;
            ioctl(&file, VIDIOC_STREAMON, &mut t)?;
        }
        Ok(Self {
            file,
            mplane,
            out_type,
            cap_type,
            memory,
            mapped,
            in_len,
            out_len,
        })
    }

    /// The frame sizes the device expects in and gives out.
    pub(crate) fn frame_lens(&self) -> (usize, usize) {
        (self.in_len, self.out_len)
    }

    /// Convert one frame between byte buffers (`Mmap` sessions); returns the bytes written.
    pub(crate) fn convert_bytes(&mut self, src: &[u8], dst: &mut [u8]) -> io::Result<usize> {
        let Some((out, cap)) = &self.mapped else {
            return Err(io::Error::other("a dmabuf session takes dmabufs"));
        };
        if src.len() < self.in_len || out.len < self.in_len {
            return Err(io::Error::other("source frame shorter than the device's"));
        }
        // SAFETY: the output buffer is mapped for `out.len >= in_len` bytes.
        unsafe { std::ptr::copy_nonoverlapping(src.as_ptr(), out.ptr, self.in_len) };
        let (cap_ptr, cap_len) = (cap.ptr, cap.len);
        self.queue(self.cap_type, None, 0)?;
        self.queue(self.out_type, None, self.in_len)?;
        self.dequeue(self.out_type)?;
        let used = self.dequeue(self.cap_type)?.min(cap_len).min(dst.len());
        // SAFETY: the capture buffer is mapped for `cap_len >= used` bytes.
        unsafe { std::ptr::copy_nonoverlapping(cap_ptr, dst.as_mut_ptr(), used) };
        Ok(used)
    }

    /// Convert one frame from dmabuf `src` into dmabuf `dst` (`Dmabuf` sessions): no copies.
    pub(crate) fn convert_dmabuf(
        &mut self,
        src: BorrowedFd<'_>,
        src_len: usize,
        dst: BorrowedFd<'_>,
        dst_len: usize,
    ) -> io::Result<usize> {
        if self.memory != Memory::Dmabuf {
            return Err(io::Error::other("a byte session takes byte buffers"));
        }
        if src_len < self.in_len || dst_len < self.out_len {
            return Err(io::Error::other(format!(
                "dmabufs of {src_len} and {dst_len} bytes; the device needs {} and {}",
                self.in_len, self.out_len
            )));
        }
        self.queue(self.cap_type, Some((dst.as_raw_fd(), dst_len)), 0)?;
        self.queue(self.out_type, Some((src.as_raw_fd(), src_len)), self.in_len)?;
        self.dequeue(self.out_type)?;
        self.dequeue(self.cap_type)
    }

    fn queue(&self, type_: u32, dmabuf: Option<(i32, usize)>, bytesused: usize) -> io::Result<()> {
        // SAFETY: all-zero `v4l2_buffer` / `v4l2_plane` are valid.
        let mut plane: Plane = unsafe { std::mem::zeroed() };
        let mut buf: Buffer = unsafe { std::mem::zeroed() };
        buf.index = 0;
        buf.type_ = type_;
        buf.field = FIELD_NONE;
        buf.memory = if dmabuf.is_some() {
            MEMORY_DMABUF
        } else {
            MEMORY_MMAP
        };
        if self.mplane {
            plane.bytesused = bytesused as u32;
            if let Some((fd, len)) = dmabuf {
                plane.m.fd = fd;
                plane.length = len as u32;
            }
            buf.m.planes = &mut plane;
            buf.length = 1;
        } else {
            buf.bytesused = bytesused as u32;
            if let Some((fd, len)) = dmabuf {
                buf.m.fd = fd;
                buf.length = len as u32;
            }
        }
        ioctl(&self.file, VIDIOC_QBUF, &mut buf)
    }

    fn dequeue(&self, type_: u32) -> io::Result<usize> {
        // SAFETY: as in `queue`.
        let mut plane: Plane = unsafe { std::mem::zeroed() };
        let mut buf: Buffer = unsafe { std::mem::zeroed() };
        buf.type_ = type_;
        buf.memory = match self.memory {
            Memory::Mmap => MEMORY_MMAP,
            Memory::Dmabuf => MEMORY_DMABUF,
        };
        if self.mplane {
            buf.m.planes = &mut plane;
            buf.length = 1;
        }
        ioctl(&self.file, VIDIOC_DQBUF, &mut buf)?;
        Ok(if self.mplane {
            plane.bytesused
        } else {
            buf.bytesused
        } as usize)
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        for ty in [self.out_type, self.cap_type] {
            let mut t = ty as i32;
            let _ = ioctl(&self.file, VIDIOC_STREAMOFF, &mut t);
        }
    }
}

/// Set a queue's format; returns the frame size the driver chose.
fn set_format(file: &File, type_: u32, mplane: bool, layout: Layout) -> io::Result<usize> {
    // SAFETY: an all-zero `v4l2_format` is valid.
    let mut f: Format = unsafe { std::mem::zeroed() };
    f.type_ = type_;
    let fourcc = layout.format.fourcc();
    if mplane {
        // SAFETY: an all-zero `v4l2_pix_format_mplane` is valid.
        let mut mp: PixFormatMplane = unsafe { std::mem::zeroed() };
        mp.width = layout.width;
        mp.height = layout.height;
        mp.pixelformat = fourcc;
        mp.field = FIELD_NONE;
        mp.num_planes = 1;
        mp.plane_fmt[0].bytesperline = layout.stride;
        mp.plane_fmt[0].sizeimage = layout.frame_len() as u32;
        f.fmt.pix_mp = mp;
    } else {
        f.fmt.pix = PixFormat {
            width: layout.width,
            height: layout.height,
            pixelformat: fourcc,
            field: FIELD_NONE,
            bytesperline: layout.stride,
            sizeimage: layout.frame_len() as u32,
            colorspace: 0,
            priv_: 0,
            flags: 0,
            ycbcr_enc: 0,
            quantization: 0,
            xfer_func: 0,
        };
    }
    ioctl(file, VIDIOC_S_FMT, &mut f)?;
    // SAFETY: the driver filled the member for `type_`.
    let (got, w, h, size) = unsafe {
        if mplane {
            let mp = f.fmt.pix_mp;
            (
                mp.pixelformat,
                mp.width,
                mp.height,
                mp.plane_fmt[0].sizeimage as usize,
            )
        } else {
            let p = f.fmt.pix;
            (p.pixelformat, p.width, p.height, p.sizeimage as usize)
        }
    };
    if got != fourcc || (w, h) != (layout.width, layout.height) {
        return Err(io::Error::other(format!(
            "the device does not take {:?} at {}x{} (it chose {w}x{h} of {got:#x})",
            layout.format, layout.width, layout.height
        )));
    }
    Ok(size)
}

fn request_buffer(file: &File, type_: u32, memory: u32) -> io::Result<()> {
    let mut req = RequestBuffers {
        count: 1,
        type_,
        memory,
        capabilities: 0,
        flags: 0,
        reserved: [0; 3],
    };
    ioctl(file, VIDIOC_REQBUFS, &mut req)?;
    if req.count < 1 {
        return Err(io::Error::other("the device gave no buffer"));
    }
    Ok(())
}

/// Map buffer 0 of a queue.
fn map_buffer(file: &File, type_: u32, mplane: bool) -> io::Result<Mapped> {
    // SAFETY: all-zero `v4l2_buffer` / `v4l2_plane` are valid.
    let mut plane: Plane = unsafe { std::mem::zeroed() };
    let mut buf: Buffer = unsafe { std::mem::zeroed() };
    buf.type_ = type_;
    buf.memory = MEMORY_MMAP;
    if mplane {
        buf.m.planes = &mut plane;
        buf.length = 1;
    }
    ioctl(file, VIDIOC_QUERYBUF, &mut buf)?;
    // SAFETY: the driver filled the offset and length of buffer 0.
    let (offset, len) = unsafe {
        if mplane {
            (plane.m.mem_offset, plane.length as usize)
        } else {
            (buf.m.offset, buf.length as usize)
        }
    };
    // SAFETY: mapping the buffer the driver just described, shared with the device.
    let ptr = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            len,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED,
            file.as_raw_fd(),
            libc::off_t::from(offset),
        )
    };
    if ptr == libc::MAP_FAILED {
        return Err(io::Error::last_os_error());
    }
    Ok(Mapped {
        ptr: ptr.cast(),
        len,
    })
}
