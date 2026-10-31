//! Fixed-function conversions through a V4L2 mem2mem device: the kernel's `rockchip-rga` driver
//! (Rockchip's 2D engine) on boards, and `vim2m` (the kernel's test driver) here. Feature `v4l2`,
//! Linux only.
//!
//! The ioctl structs follow `linux/videodev2.h`. Their sizes are checked at compile time on 64-bit
//! targets, and the ioctl numbers are derived from them, so 32-bit builds with 64-bit `time_t`
//! get the right numbers too. Buffers are `MMAP`: each conversion copies the frame in and the
//! result out.

use std::ffi::CStr;
use std::format;
use std::fs::{File, OpenOptions};
use std::io;
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::string::String;
use std::vec::Vec;

const CAP_VIDEO_M2M: u32 = 0x0000_8000;
const CAP_VIDEO_M2M_MPLANE: u32 = 0x0000_4000;
const CAP_DEVICE_CAPS: u32 = 0x8000_0000;

const BUF_TYPE_VIDEO_CAPTURE: u32 = 1;
const BUF_TYPE_VIDEO_OUTPUT: u32 = 2;
const BUF_TYPE_VIDEO_CAPTURE_MPLANE: u32 = 9;
const BUF_TYPE_VIDEO_OUTPUT_MPLANE: u32 = 10;
const MEMORY_MMAP: u32 = 1;
const FIELD_NONE: u32 = 1;

/// A V4L2 pixel format (`V4L2_PIX_FMT_*`), byte order in memory as the comment says.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
}

impl PixelFormat {
    fn fourcc(self) -> u32 {
        let code = match self {
            PixelFormat::Rgb24 => *b"RGB3",
            PixelFormat::Bgr24 => *b"BGR3",
            PixelFormat::Abgr32 => *b"AR24",
            PixelFormat::Xbgr32 => *b"XR24",
            PixelFormat::Grey => *b"GREY",
        };
        u32::from_le_bytes(code)
    }

    /// Bytes per pixel.
    pub fn bytes(self) -> u32 {
        match self {
            PixelFormat::Rgb24 | PixelFormat::Bgr24 => 3,
            PixelFormat::Abgr32 | PixelFormat::Xbgr32 => 4,
            PixelFormat::Grey => 1,
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

// The kernel's sizes on 64-bit Linux (`pahole` of videodev2.h).
#[cfg(target_pointer_width = "64")]
const _: () = {
    assert!(size_of::<Capability>() == 104);
    assert!(size_of::<PixFormatMplane>() == 192);
    assert!(size_of::<Format>() == 208);
    assert!(size_of::<RequestBuffers>() == 20);
    assert!(size_of::<Plane>() == 64);
    assert!(size_of::<Buffer>() == 88);
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

/// The driver name of a V4L2 device, and whether it is a mem2mem device (single- or
/// multi-planar).
fn query(file: &File) -> io::Result<(String, Option<bool>)> {
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

/// A frame-size converter on a mem2mem device: `width x height` frames from `src` to `dst`
/// format, one buffer per queue.
pub struct V4l2Converter {
    file: File,
    mplane: bool,
    out_type: u32,
    cap_type: u32,
    out: Mapped,
    cap: Mapped,
    in_len: usize,
    out_len: usize,
}

// SAFETY: the mappings are owned by the converter and only touched through `&mut self`.
unsafe impl Send for V4l2Converter {}

impl V4l2Converter {
    /// Open the mem2mem device at `path` for `width x height` frames, `src` into `dst`.
    pub fn open(
        path: &Path,
        width: u32,
        height: u32,
        src: PixelFormat,
        dst: PixelFormat,
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
        let in_len = set_format(&file, out_type, mplane, width, height, src)?;
        let out_len = set_format(&file, cap_type, mplane, width, height, dst)?;
        let out = map_buffer(&file, out_type, mplane)?;
        let cap = map_buffer(&file, cap_type, mplane)?;
        for ty in [out_type, cap_type] {
            let mut t = ty as i32;
            ioctl(&file, VIDIOC_STREAMON, &mut t)?;
        }
        Ok(Self {
            file,
            mplane,
            out_type,
            cap_type,
            out,
            cap,
            in_len,
            out_len,
        })
    }

    /// The frame sizes the device expects in and gives out (its strides included).
    pub fn frame_lens(&self) -> (usize, usize) {
        (self.in_len, self.out_len)
    }

    /// Convert one frame: `src` (at least [`frame_lens`](Self::frame_lens)`.0` bytes) into
    /// `dst`; returns the bytes written.
    pub fn convert(&mut self, src: &[u8], dst: &mut [u8]) -> io::Result<usize> {
        if src.len() < self.in_len || self.out.len < self.in_len {
            return Err(io::Error::other("source frame shorter than the device's"));
        }
        // SAFETY: the output buffer is mapped for `self.out.len >= in_len` bytes.
        unsafe { std::ptr::copy_nonoverlapping(src.as_ptr(), self.out.ptr, self.in_len) };
        self.queue(self.cap_type, 0)?;
        self.queue(self.out_type, self.in_len)?;
        self.dequeue(self.out_type)?;
        let used = self
            .dequeue(self.cap_type)?
            .min(self.cap.len)
            .min(dst.len());
        // SAFETY: the capture buffer is mapped for `self.cap.len >= used` bytes.
        unsafe { std::ptr::copy_nonoverlapping(self.cap.ptr, dst.as_mut_ptr(), used) };
        Ok(used)
    }

    fn queue(&self, type_: u32, bytesused: usize) -> io::Result<()> {
        // SAFETY: all-zero `v4l2_buffer` / `v4l2_plane` are valid.
        let mut plane: Plane = unsafe { std::mem::zeroed() };
        let mut buf: Buffer = unsafe { std::mem::zeroed() };
        buf.index = 0;
        buf.type_ = type_;
        buf.memory = MEMORY_MMAP;
        buf.field = FIELD_NONE;
        if self.mplane {
            plane.bytesused = bytesused as u32;
            buf.m.planes = &mut plane;
            buf.length = 1;
        } else {
            buf.bytesused = bytesused as u32;
        }
        ioctl(&self.file, VIDIOC_QBUF, &mut buf)
    }

    fn dequeue(&self, type_: u32) -> io::Result<usize> {
        // SAFETY: as in `queue`.
        let mut plane: Plane = unsafe { std::mem::zeroed() };
        let mut buf: Buffer = unsafe { std::mem::zeroed() };
        buf.type_ = type_;
        buf.memory = MEMORY_MMAP;
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

impl Drop for V4l2Converter {
    fn drop(&mut self) {
        for ty in [self.out_type, self.cap_type] {
            let mut t = ty as i32;
            let _ = ioctl(&self.file, VIDIOC_STREAMOFF, &mut t);
        }
    }
}

/// Set a queue's format; returns the frame size the driver chose.
fn set_format(
    file: &File,
    type_: u32,
    mplane: bool,
    width: u32,
    height: u32,
    format: PixelFormat,
) -> io::Result<usize> {
    // SAFETY: an all-zero `v4l2_format` is valid.
    let mut f: Format = unsafe { std::mem::zeroed() };
    f.type_ = type_;
    if mplane {
        let mut mp: PixFormatMplane = unsafe { std::mem::zeroed() };
        mp.width = width;
        mp.height = height;
        mp.pixelformat = format.fourcc();
        mp.field = FIELD_NONE;
        mp.num_planes = 1;
        mp.plane_fmt[0].bytesperline = width * format.bytes();
        f.fmt.pix_mp = mp;
    } else {
        f.fmt.pix = PixFormat {
            width,
            height,
            pixelformat: format.fourcc(),
            field: FIELD_NONE,
            bytesperline: width * format.bytes(),
            sizeimage: 0,
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
    let (got, size) = unsafe {
        if mplane {
            let mp = f.fmt.pix_mp;
            (mp.pixelformat, mp.plane_fmt[0].sizeimage as usize)
        } else {
            (f.fmt.pix.pixelformat, f.fmt.pix.sizeimage as usize)
        }
    };
    if got != format.fourcc() {
        return Err(io::Error::other(format!(
            "the device does not take {format:?}"
        )));
    }
    Ok(size)
}

/// Request one `MMAP` buffer on a queue and map it.
fn map_buffer(file: &File, type_: u32, mplane: bool) -> io::Result<Mapped> {
    let mut req = RequestBuffers {
        count: 1,
        type_,
        memory: MEMORY_MMAP,
        capabilities: 0,
        flags: 0,
        reserved: [0; 3],
    };
    ioctl(file, VIDIOC_REQBUFS, &mut req)?;
    if req.count < 1 {
        return Err(io::Error::other("the device gave no buffer"));
    }
    // SAFETY: as in `V4l2Converter::queue`.
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

/// Rockchip's RGA through the mainline `rockchip-rga` mem2mem driver, for
/// `daedalus.pixel.rgb_to_rgba` and `rgba_to_rgb`.
///
/// The driver has no R,G,B,A byte order, so the frames are relabelled: RGB bytes read as
/// `BGR24`, converted to `ABGR32` (B,G,R,A), give R,G,B,A, and the reverse for RGBA to RGB. The
/// alpha the engine writes is checked by the board conformance run. Written, untested on a
/// board: `find` returns `None` without the driver.
pub struct Rga {
    path: PathBuf,
    converters: std::collections::HashMap<(u32, u32, bool), V4l2Converter>,
}

impl Rga {
    /// The RGA device, when the kernel exposes `rockchip-rga`.
    pub fn find() -> Option<Self> {
        Some(Self {
            path: find_m2m_device("rockchip-rga")?,
            converters: std::collections::HashMap::new(),
        })
    }

    fn converter(&mut self, w: u32, h: u32, to_rgba: bool) -> io::Result<&mut V4l2Converter> {
        let key = (w, h, to_rgba);
        if !self.converters.contains_key(&key) {
            let (src, dst) = if to_rgba {
                (PixelFormat::Bgr24, PixelFormat::Abgr32)
            } else {
                (PixelFormat::Abgr32, PixelFormat::Bgr24)
            };
            let c = V4l2Converter::open(&self.path, w, h, src, dst)?;
            self.converters.insert(key, c);
        }
        Ok(self.converters.get_mut(&key).expect("inserted above"))
    }

    /// RGB8 to RGBA8 on the RGA, `width x height`, tightly packed.
    pub fn rgb_to_rgba(
        &mut self,
        width: u32,
        height: u32,
        src: &[u8],
        dst: &mut [u8],
    ) -> io::Result<()> {
        self.converter(width, height, true)?
            .convert(src, dst)
            .map(drop)
    }

    /// RGBA8 to RGB8 on the RGA.
    pub fn rgba_to_rgb(
        &mut self,
        width: u32,
        height: u32,
        src: &[u8],
        dst: &mut [u8],
    ) -> io::Result<()> {
        self.converter(width, height, false)?
            .convert(src, dst)
            .map(drop)
    }
}
