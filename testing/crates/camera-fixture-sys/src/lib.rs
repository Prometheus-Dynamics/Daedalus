//! The V4L2 requests a camera test fixture makes, one function a request. Test-only, Linux only:
//! it exists so a board test can get a real frame as a dma-buf. The structs are the kernel's
//! `videodev2.h` layouts for 64-bit Linux, and each request number is the kernel's `_IOWR`/`_IOW`
//! encoding of its struct, checked against the bindings of the `v4l` crate on the host.

#![cfg(target_os = "linux")]

use std::fs::File;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

const BUF_TYPE_CAPTURE: u32 = 1;
const MEMORY_MMAP: u32 = 1;
const FIELD_NONE: u32 = 1;
const CAP_VIDEO_CAPTURE: u32 = 0x1;
const CAP_STREAMING: u32 = 0x0400_0000;
const CAP_DEVICE_CAPS: u32 = 0x8000_0000;

/// asm-generic ioctl numbers: direction in the top two bits, then the argument size, the type
/// letter and the number. aarch64 and x86-64 Linux use this encoding.
const fn ioc(dir: u32, nr: u32, size: usize) -> libc::c_ulong {
    ((dir << 30) | ((size as u32) << 16) | ((b'V' as u32) << 8) | nr) as libc::c_ulong
}
const READ: u32 = 2;
const WRITE: u32 = 1;
const QUERYCAP: libc::c_ulong = ioc(READ, 0, 104);
const S_FMT: libc::c_ulong = ioc(READ | WRITE, 5, 208);
const REQBUFS: libc::c_ulong = ioc(READ | WRITE, 8, 20);
const QBUF: libc::c_ulong = ioc(READ | WRITE, 15, 88);
const DQBUF: libc::c_ulong = ioc(READ | WRITE, 17, 88);
const EXPBUF: libc::c_ulong = ioc(READ | WRITE, 16, 64);
const STREAMON: libc::c_ulong = ioc(WRITE, 18, 4);
const STREAMOFF: libc::c_ulong = ioc(WRITE, 19, 4);

#[repr(C)]
#[derive(Default)]
struct Capability {
    driver: [u8; 16],
    card: [u8; 32],
    bus_info: [u8; 32],
    version: u32,
    capabilities: u32,
    device_caps: u32,
    reserved: [u32; 3],
}

/// `struct v4l2_pix_format`: the member of the format union a capture request uses.
#[repr(C)]
#[derive(Default, Clone, Copy)]
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

/// `struct v4l2_format`: the type, then a union of 200 bytes aligned to 8 (its `win` member
/// holds pointers).
#[repr(C)]
struct Format {
    type_: u32,
    fmt: FormatUnion,
}

#[repr(C, align(8))]
struct FormatUnion {
    raw: [u8; 200],
}

#[repr(C)]
#[derive(Default)]
struct RequestBuffers {
    count: u32,
    type_: u32,
    memory: u32,
    capabilities: u32,
    flags: u8,
    reserved: [u8; 3],
}

/// `struct v4l2_buffer`: 88 bytes on 64-bit Linux.
#[repr(C)]
#[derive(Default)]
struct Buffer {
    index: u32,
    type_: u32,
    bytesused: u32,
    flags: u32,
    field: u32,
    _align: u32,
    timestamp: [i64; 2],
    timecode: [u8; 16],
    sequence: u32,
    memory: u32,
    m: [u64; 1],
    length: u32,
    reserved2: u32,
    request_fd: u32,
    _pad: u32,
}

#[repr(C)]
#[derive(Default)]
struct ExportBuffer {
    type_: u32,
    index: u32,
    plane: u32,
    flags: u32,
    fd: i32,
    reserved: [u32; 11],
}

/// One ioctl with the argument `arg`, which the kernel reads and writes. An error is the errno.
fn request<T>(file: &File, code: libc::c_ulong, arg: &mut T) -> io::Result<()> {
    let fd = file.as_raw_fd();
    // SAFETY: `fd` is open for the life of `file`, and `arg` is a `T` laid out the way the
    // request's argument is: each caller passes the struct its request takes.
    let rc = unsafe { libc::ioctl(fd, code as _, arg as *mut T) };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// The capabilities the device reports for capture, and whether it streams.
pub fn capture_caps(file: &File) -> io::Result<(u32, bool)> {
    let mut cap = Capability::default();
    request(file, QUERYCAP, &mut cap)?;
    let caps = if cap.capabilities & CAP_DEVICE_CAPS != 0 {
        cap.device_caps
    } else {
        cap.capabilities
    };
    let streams = caps & CAP_VIDEO_CAPTURE != 0 && caps & CAP_STREAMING != 0;
    Ok((caps, streams))
}

/// The format the device settled on, as the kernel reports it after the request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Pix {
    pub fourcc: u32,
    pub width: u32,
    pub height: u32,
    pub stride: u32,
    pub size: u32,
}

/// Asks for `fourcc` at `width` by `height`, and reads back what the device gave.
pub fn set_format(file: &File, fourcc: u32, width: u32, height: u32) -> io::Result<Pix> {
    let mut pix = PixFormat {
        width,
        height,
        pixelformat: fourcc,
        field: FIELD_NONE,
        ..PixFormat::default()
    };
    let mut fmt = Format {
        type_: BUF_TYPE_CAPTURE,
        fmt: FormatUnion { raw: [0; 200] },
    };
    // The capture member is the first 48 bytes of the union.
    // SAFETY: `pix` is plain integers, so its bytes are valid to read as `u8`s.
    let bytes = unsafe { std::slice::from_raw_parts(&pix as *const PixFormat as *const u8, 48) };
    fmt.fmt.raw[..48].copy_from_slice(bytes);
    request(file, S_FMT, &mut fmt)?;
    // SAFETY: a capture request writes the pixel format into the union's first 48 bytes, and
    // `PixFormat` is plain integers, for which any bit pattern is valid.
    pix = unsafe { std::ptr::read_unaligned(fmt.fmt.raw.as_ptr() as *const PixFormat) };
    Ok(Pix {
        fourcc: pix.pixelformat,
        width: pix.width,
        height: pix.height,
        stride: pix.bytesperline,
        size: pix.sizeimage,
    })
}

/// Allocates `count` capture buffers of the device's own memory. `EBUSY` means another user of
/// the device holds it.
pub fn request_buffers(file: &File, count: u32) -> io::Result<u32> {
    let mut req = RequestBuffers {
        count,
        type_: BUF_TYPE_CAPTURE,
        memory: MEMORY_MMAP,
        ..RequestBuffers::default()
    };
    request(file, REQBUFS, &mut req)?;
    Ok(req.count)
}

/// Frees the buffers: a request for none.
pub fn release_buffers(file: &File) -> io::Result<()> {
    let mut req = RequestBuffers {
        type_: BUF_TYPE_CAPTURE,
        memory: MEMORY_MMAP,
        ..RequestBuffers::default()
    };
    request(file, REQBUFS, &mut req)
}

fn buffer(index: u32) -> Buffer {
    Buffer {
        index,
        type_: BUF_TYPE_CAPTURE,
        memory: MEMORY_MMAP,
        ..Buffer::default()
    }
}

/// Queues buffer `index` for the device to fill.
pub fn queue(file: &File, index: u32) -> io::Result<()> {
    request(file, QBUF, &mut buffer(index))
}

/// A filled buffer: its index, how many bytes the device wrote, and its sequence number.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Filled {
    pub index: u32,
    pub bytes: u32,
    pub sequence: u32,
}

/// Takes the next filled buffer, waiting for it.
pub fn dequeue(file: &File) -> io::Result<Filled> {
    let mut buf = buffer(0);
    request(file, DQBUF, &mut buf)?;
    Ok(Filled {
        index: buf.index,
        bytes: buf.bytesused,
        sequence: buf.sequence,
    })
}

/// Exports buffer `index` as a dma-buf, which the caller owns.
pub fn export(file: &File, index: u32) -> io::Result<OwnedFd> {
    let mut exp = ExportBuffer {
        type_: BUF_TYPE_CAPTURE,
        index,
        flags: (libc::O_CLOEXEC | libc::O_RDONLY) as u32,
        ..ExportBuffer::default()
    };
    request(file, EXPBUF, &mut exp)?;
    // SAFETY: a successful EXPBUF gives a new descriptor in `fd`, which nothing else owns yet.
    Ok(unsafe { OwnedFd::from_raw_fd(exp.fd) })
}

/// Starts or stops the stream.
pub fn stream(file: &File, on: bool) -> io::Result<()> {
    let mut kind = BUF_TYPE_CAPTURE as libc::c_int;
    let code = if on { STREAMON } else { STREAMOFF };
    request(file, code, &mut kind)
}

const _: () = {
    assert!(std::mem::size_of::<Capability>() == 104);
    assert!(std::mem::size_of::<Format>() == 208);
    assert!(std::mem::size_of::<RequestBuffers>() == 20);
    assert!(std::mem::size_of::<Buffer>() == 88);
    assert!(std::mem::size_of::<ExportBuffer>() == 64);
};
