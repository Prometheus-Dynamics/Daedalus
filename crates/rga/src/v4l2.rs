//! V4L2 mem2mem sessions: one device context with an output (source) and a capture
//! (destination) queue, configured once per format, size, stride and crop. The device calls are
//! Lemnos's (`lemnos_linux::v4l2::m2m::M2mSession`); this module maps a conversion onto them.

use std::io;
use std::os::fd::BorrowedFd;
use std::path::{Path, PathBuf};
use std::time::Duration;

use lemnos_linux::v4l2::m2m::{M2mSession, QueueSetup, Ready};
use lemnos_linux::v4l2::{
    self as lv, Format, PixFormat, PixFormatMplane, PlaneFormat, PlaneMemory, QueuePlane,
};

/// `V4L2_FIELD_NONE`: progressive frames.
const FIELD_NONE: u32 = 1;

/// How long one conversion may take before it is an error.
const CONVERT_TIMEOUT: Duration = Duration::from_secs(5);

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

/// The first `/dev/video*` mem2mem device whose driver is `driver` (`"rockchip-rga"`, `"vim2m"`).
pub fn find_m2m_device(driver: &str) -> Option<PathBuf> {
    lv::find_m2m(driver).ok().flatten().map(|(path, _)| path)
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

/// A configured mem2mem context: `src` into `dst`, scaling to `dst`'s size, from `crop` of the
/// source when given. One buffer per queue, index 0.
pub(crate) struct Session {
    session: M2mSession,
    memory: Memory,
    in_len: usize,
    out_len: usize,
}

impl Session {
    pub(crate) fn open(
        path: &Path,
        src: Layout,
        dst: Layout,
        crop: Option<Rect>,
        memory: Memory,
    ) -> io::Result<Self> {
        let cap = lv::query(path)?;
        if !cap.is_m2m() {
            return Err(io::Error::other(format!(
                "{} is not mem2mem",
                path.display()
            )));
        }
        let mplane = cap.is_m2m_multiplanar();
        let mem = match memory {
            Memory::Mmap => lv::Memory::Mmap,
            Memory::Dmabuf => lv::Memory::Dmabuf,
        };
        let setup = |layout: Layout, crop: Option<Rect>| QueueSetup {
            format: format(layout, mplane),
            memory: mem,
            buffers: 1,
            crop: crop.map(|r| lv::Rect {
                left: r.left,
                top: r.top,
                width: r.width,
                height: r.height,
            }),
        };
        let mut session = M2mSession::open(path, setup(src, crop), setup(dst, None))?;
        let in_len = kept(session.output_format(), src)?;
        let out_len = kept(session.capture_format(), dst)?;
        if let Some(r) = crop {
            let got = session.output_crop();
            if got.map(|g| (g.left, g.top, g.width, g.height))
                != Some((r.left, r.top, r.width, r.height))
            {
                return Err(io::Error::other(format!(
                    "the device adjusted crop {r:?} to {got:?}"
                )));
            }
        }
        session.stream_on()?;
        Ok(Self {
            session,
            memory,
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
        if self.memory != Memory::Mmap {
            return Err(io::Error::other("a dmabuf session takes dmabufs"));
        }
        let in_len = self.in_len;
        let out = self
            .session
            .output_plane_mut(0, 0)
            .ok_or_else(|| io::Error::other("the source buffer is not mapped"))?;
        if src.len() < in_len || out.len() < in_len {
            return Err(io::Error::other("source frame shorter than the device's"));
        }
        out[..in_len].copy_from_slice(&src[..in_len]);
        let mmap = |bytes_used| QueuePlane {
            memory: PlaneMemory::Mmap,
            length: 0,
            bytes_used,
            data_offset: 0,
        };
        self.session.queue_capture(0, &[mmap(0)])?;
        self.session
            .queue_output(0, &[mmap(in_len as u32)], Duration::ZERO)?;
        let used = self.finish()?;
        let cap = self
            .session
            .capture_plane(0, 0)
            .ok_or_else(|| io::Error::other("the destination buffer is not mapped"))?;
        let used = used.min(cap.len()).min(dst.len());
        dst[..used].copy_from_slice(&cap[..used]);
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
        let plane = |fd, len: usize, bytes_used| QueuePlane {
            memory: PlaneMemory::Dmabuf(fd),
            length: len as u32,
            bytes_used,
            data_offset: 0,
        };
        self.session.queue_capture(0, &[plane(dst, dst_len, 0)])?;
        self.session.queue_output(
            0,
            &[plane(src, src_len, self.in_len as u32)],
            Duration::ZERO,
        )?;
        self.finish()
    }

    /// Wait for the device to give both buffers back; returns the bytes it wrote.
    fn finish(&mut self) -> io::Result<usize> {
        let (mut source_back, mut written) = (false, None);
        while !source_back || written.is_none() {
            let want = Ready {
                output: !source_back,
                capture: written.is_none(),
                event: false,
            };
            let ready = self.session.wait(want, Some(CONVERT_TIMEOUT))?;
            if ready == Ready::default() {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "the device did not finish the frame",
                ));
            }
            if !source_back && self.session.dequeue_output()?.is_some() {
                source_back = true;
            }
            if written.is_none()
                && let Some(done) = self.session.dequeue_capture()?
            {
                written = Some(done.bytes_used[0] as usize);
            }
        }
        Ok(written.unwrap_or_default())
    }
}

/// The V4L2 format of `layout` (one memory plane).
fn format(layout: Layout, mplane: bool) -> Format {
    let size_image = layout.frame_len() as u32;
    if mplane {
        let mut planes = [PlaneFormat::default(); lv::MAX_PLANES];
        planes[0] = PlaneFormat {
            size_image,
            bytes_per_line: layout.stride,
        };
        Format::Multi(PixFormatMplane {
            width: layout.width,
            height: layout.height,
            pixelformat: layout.format.fourcc(),
            field: FIELD_NONE,
            planes,
            num_planes: 1,
            ..Default::default()
        })
    } else {
        Format::Single(PixFormat {
            width: layout.width,
            height: layout.height,
            pixelformat: layout.format.fourcc(),
            field: FIELD_NONE,
            bytes_per_line: layout.stride,
            size_image,
            ..Default::default()
        })
    }
}

/// The frame size of the format the driver kept, or an error when it did not take `layout`.
fn kept(format: &Format, layout: Layout) -> io::Result<usize> {
    let (got, w, h, size) = match format {
        Format::Single(p) => (p.pixelformat, p.width, p.height, p.size_image),
        Format::Multi(m) => (m.pixelformat, m.width, m.height, m.planes[0].size_image),
    };
    if got != layout.format.fourcc() || (w, h) != (layout.width, layout.height) {
        return Err(io::Error::other(format!(
            "the device does not take {:?} at {}x{} (it chose {w}x{h} of {got:#x})",
            layout.format, layout.width, layout.height
        )));
    }
    Ok(size as usize)
}
