//! The V4L2 mem2mem engine against the kernel's `vim2m` test driver (when loaded): conversion,
//! scaling, and dmabuf in and out with buffers from `/dev/dma_heap/system`. The RGA test runs
//! on Rockchip boards (`rockchip-rga`) and skips elsewhere.
#![cfg(target_os = "linux")]
#![allow(clippy::print_stderr)]

use std::fs::OpenOptions;
use std::os::fd::{AsFd, AsRawFd, FromRawFd, OwnedFd};

use daedalus_rga::{Layout, PixelFormat, Rect, Rga, find_m2m_device};

fn vim2m() -> Option<Rga> {
    match find_m2m_device("vim2m") {
        Some(path) => Some(Rga::on_device(&path)),
        None => {
            eprintln!("skipping: no vim2m device (modprobe vim2m)");
            None
        }
    }
}

fn frame(len: usize, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed))
        .collect()
}

fn swap_rb(rgb: &[u8]) -> Vec<u8> {
    rgb.as_chunks::<3>()
        .0
        .iter()
        .flat_map(|p| [p[2], p[1], p[0]])
        .collect()
}

#[test]
fn vim2m_converts_rgb24_to_bgr24() {
    let Some(mut rga) = vim2m() else { return };
    let (src, dst) = (
        Layout::packed(PixelFormat::Rgb24, 640, 480),
        Layout::packed(PixelFormat::Bgr24, 640, 480),
    );
    for seed in 0..3 {
        let input = frame(src.frame_len(), seed);
        let mut out = vec![0u8; dst.frame_len()];
        let n = rga
            .convert(src, &input, None, dst, &mut out)
            .expect("convert");
        assert_eq!(n, dst.frame_len());
        assert_eq!(out, swap_rb(&input), "frame {seed}");
    }
}

/// A crop the device does not take is an error, never a silent full-frame conversion. vim2m has
/// no selection support, so asking it to crop must fail; on a device that crops (the RGA) the
/// same call converts the rectangle.
#[test]
fn vim2m_refuses_a_crop_it_cannot_do() {
    let Some(mut rga) = vim2m() else { return };
    let (src, dst) = (
        Layout::packed(PixelFormat::Rgb24, 640, 480),
        Layout::packed(PixelFormat::Rgb24, 320, 240),
    );
    let crop = Rect {
        left: 16,
        top: 8,
        width: 320,
        height: 240,
    };
    let input = frame(src.frame_len(), 1);
    let mut out = vec![0u8; dst.frame_len()];
    let err = rga
        .convert(src, &input, Some(crop), dst, &mut out)
        .expect_err("vim2m cannot crop");
    eprintln!("refused as expected: {err}");
}

/// Scaling down to the destination's size: vim2m samples the nearest source pixel.
#[test]
fn vim2m_scales_to_the_destination_size() {
    let Some(mut rga) = vim2m() else { return };
    let (src, dst) = (
        Layout::packed(PixelFormat::Rgb24, 640, 480),
        Layout::packed(PixelFormat::Rgb24, 320, 240),
    );
    let input = frame(src.frame_len(), 7);
    let mut out = vec![0u8; dst.frame_len()];
    match rga.convert(src, &input, None, dst, &mut out) {
        Ok(n) => assert_eq!(n, dst.frame_len()),
        Err(err) => return eprintln!("skipping: this vim2m does not scale ({err})"),
    }
    // Every output pixel is some source pixel of its 2x2 block.
    for y in 0..240usize {
        for x in 0..320usize {
            let got = &out[(y * 320 + x) * 3..][..3];
            let found = (0..2).any(|dy| {
                (0..2).any(|dx| {
                    let (sx, sy) = (2 * x + dx, 2 * y + dy);
                    &input[(sy * 640 + sx) * 3..][..3] == got
                })
            });
            assert!(found, "pixel ({x}, {y})");
        }
    }
}

/// A dmabuf from `/dev/dma_heap/system`, mapped for filling and checking.
struct Dmabuf {
    fd: OwnedFd,
    ptr: *mut u8,
    len: usize,
}

impl Dmabuf {
    fn alloc(len: usize) -> Option<Self> {
        #[repr(C)]
        struct Alloc {
            len: u64,
            fd: u32,
            fd_flags: u32,
            heap_flags: u64,
        }
        let heap = OpenOptions::new()
            .read(true)
            .write(true)
            .open("/dev/dma_heap/system")
            .ok()?;
        let mut req = Alloc {
            len: len as u64,
            fd: 0,
            fd_flags: (libc::O_RDWR | libc::O_CLOEXEC) as u32,
            heap_flags: 0,
        };
        // DMA_HEAP_IOCTL_ALLOC = _IOWR('H', 0, struct dma_heap_allocation_data)
        let request = (3u64 << 30) | ((size_of::<Alloc>() as u64) << 16) | ((b'H' as u64) << 8);
        // SAFETY: `req` is the struct the ioctl number encodes.
        if unsafe { libc::ioctl(heap.as_raw_fd(), request as _, &mut req) } < 0 {
            return None;
        }
        // SAFETY: the kernel returned a new dmabuf fd we now own.
        let fd = unsafe { OwnedFd::from_raw_fd(req.fd as i32) };
        // SAFETY: mapping the whole dmabuf.
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                fd.as_raw_fd(),
                0,
            )
        };
        (ptr != libc::MAP_FAILED).then(|| Self {
            fd,
            ptr: ptr.cast(),
            len,
        })
    }

    fn bytes(&mut self) -> &mut [u8] {
        // SAFETY: mapped for `len` bytes, exclusively borrowed.
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
    }
}

impl Drop for Dmabuf {
    fn drop(&mut self) {
        // SAFETY: the mapping made in `alloc`.
        unsafe { libc::munmap(self.ptr.cast(), self.len) };
    }
}

/// A dmabuf in, a dmabuf out: no CPU copy in the conversion.
#[test]
fn vim2m_blits_between_dmabufs() {
    let Some(mut rga) = vim2m() else { return };
    let (src, dst) = (
        Layout::packed(PixelFormat::Rgb24, 640, 480),
        Layout::packed(PixelFormat::Bgr24, 640, 480),
    );
    let (Some(mut a), Some(mut b)) = (
        Dmabuf::alloc(src.frame_len()),
        Dmabuf::alloc(dst.frame_len()),
    ) else {
        return eprintln!("skipping: no /dev/dma_heap/system access");
    };
    let input = frame(src.frame_len(), 3);
    a.bytes().copy_from_slice(&input);
    let n = rga
        .blit(src, a.fd.as_fd(), a.len, None, dst, b.fd.as_fd(), b.len)
        .expect("blit");
    assert_eq!(n, dst.frame_len());
    assert_eq!(b.bytes(), swap_rb(&input).as_slice());
}

/// Rockchip's RGA against the references, recorded as the fixed-function `rga-v4l2` variant.
/// Written, untested on board: skips without `rockchip-rga`.
#[test]
fn rga_converts_like_the_references() {
    use daedalus_accel::{Backend, Evidence, conform_with};
    use daedalus_kernels::reference as r;
    let Some(mut rga) = Rga::find() else {
        return eprintln!("skipping: no rockchip-rga device");
    };
    let (w, h) = (640u32, 480u32);
    let rgb: Vec<u8> = (0..w * h * 3).map(|i| (i % 251) as u8).collect();
    conform_with(
        Evidence::new("daedalus.pixel.rgb_to_rgba", Backend::FixedFunction).variant("rga-v4l2"),
        || {
            let mut out = vec![0u8; (w * h * 4) as usize];
            rga.rgb_to_rgba(w, h, &rgb, &mut out)
                .map_err(|e| e.to_string())?;
            (out == r::rgb_to_rgba(&rgb))
                .then_some(())
                .ok_or_else(|| "differs (alpha?)".into())
        },
    );
    let rgba = r::rgb_to_rgba(&rgb);
    conform_with(
        Evidence::new("daedalus.pixel.rgba_to_rgb", Backend::FixedFunction).variant("rga-v4l2"),
        || {
            let mut out = vec![0u8; (w * h * 3) as usize];
            rga.rgba_to_rgb(w, h, &rgba, &mut out)
                .map_err(|e| e.to_string())?;
            (out == rgb).then_some(()).ok_or_else(|| "differs".into())
        },
    );
}
