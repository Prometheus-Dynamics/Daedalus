//! The V4L2 mem2mem engine against the kernel's `vim2m` test driver (when loaded): conversion,
//! scaling, and dmabuf in and out with buffers from `/dev/dma_heap/system`. The RGA test runs
//! on Rockchip boards (`rockchip-rga`) and skips elsewhere.
#![cfg(target_os = "linux")]
#![allow(clippy::print_stderr)]

use std::fs::OpenOptions;
use std::os::fd::{AsFd, AsRawFd, FromRawFd, OwnedFd};

use daedalus_rga::{Filter, Layout, PixelFormat, Rect, Rga, Transform, find_m2m_device};

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

/// Resize plumbing on vim2m (it samples nearest, so values are not checked), and the filters
/// the mainline driver cannot honour refused before any device call.
#[test]
fn vim2m_resizes_and_refuses_what_the_driver_cannot_do() {
    let Some(mut rga) = vim2m() else { return };
    let (src, dst) = (
        Layout::packed(PixelFormat::Rgb24, 128, 96),
        Layout::packed(PixelFormat::Rgb24, 64, 48),
    );
    let input = frame(src.frame_len(), 3);
    let mut out = vec![0u8; dst.frame_len()];
    match rga.resize_bytes(src, &input, dst, &mut out, Filter::Area) {
        Ok(n) => assert_eq!(n, dst.frame_len()),
        Err(err) => eprintln!("skipping the area resize: this vim2m does not scale ({err})"),
    }
    let refused = |rga: &mut Rga, dst: Layout, filter| {
        let mut out = vec![0u8; dst.frame_len()];
        rga.resize_bytes(src, &input, dst, &mut out, filter)
            .unwrap_err()
            .kind()
    };
    let unsupported = std::io::ErrorKind::Unsupported;
    assert_eq!(refused(&mut rga, dst, Filter::Bilinear), unsupported);
    let third = Layout::packed(PixelFormat::Rgb24, 60, 48);
    assert_eq!(refused(&mut rga, third, Filter::Area), unsupported);
}

/// The largest difference between two frames, or an error when the lengths differ.
fn max_diff(got: &[u8], want: &[u8]) -> Result<u8, String> {
    if got.len() != want.len() {
        return Err(format!("{} bytes, want {}", got.len(), want.len()));
    }
    Ok(got
        .iter()
        .zip(want)
        .map(|(a, b)| a.abs_diff(*b))
        .max()
        .unwrap_or(0))
}

/// Within the tolerance Eidos accepts (one level), printing the measured difference.
fn within_one(what: &str, got: &[u8], want: &[u8]) -> Result<(), String> {
    let diff = max_diff(got, want)?;
    eprintln!("{what}: max difference {diff}");
    (diff <= 1)
        .then_some(())
        .ok_or_else(|| format!("max difference {diff}"))
}

/// Rockchip's RGA resizes against the references (within one level), recorded as the
/// `rga-v4l2` variant. Written, untested on board: skips without `rockchip-rga`.
#[test]
fn rga_resizes_like_the_references() {
    use daedalus_accel::{Backend, Evidence, conform_with};
    use daedalus_kernels::reference as r;
    let Some(mut rga) = Rga::find() else {
        return eprintln!("skipping: no rockchip-rga device");
    };
    let evidence = |op| Evidence::new(op, Backend::FixedFunction).variant("rga-v4l2");
    let (w, h) = (640u32, 480u32);
    let luma: Vec<u8> = (0..w * h).map(|i| (i % 251) as u8).collect();
    conform_with(evidence("daedalus.image.resize_area"), || {
        // Luma through NV12, an odd size cropped, and RGB.
        let mut out = vec![0u8; (w * h / 4) as usize];
        rga.resize_luma(w, h, &luma, w / 2, h / 2, &mut out, Filter::Area)
            .map_err(|e| e.to_string())?;
        let want = r::resize_area(&luma, w as usize, h as usize, 1, 2).0;
        within_one("area luma 640x480 / 2", &out, &want)?;
        let (ow, oh) = ((w - 1) / 2, (h - 1) / 2);
        let odd = r::resize_area(&luma, w as usize, h as usize, 1, 1).0;
        let odd: Vec<u8> = odd
            .chunks(w as usize)
            .take((h - 1) as usize)
            .flat_map(|row| row[..(w - 1) as usize].to_vec())
            .collect();
        let mut out = vec![0u8; (ow * oh) as usize];
        rga.resize_luma(w - 1, h - 1, &odd, ow, oh, &mut out, Filter::Area)
            .map_err(|e| e.to_string())?;
        let want = r::resize_area(&odd, (w - 1) as usize, (h - 1) as usize, 1, 2).0;
        within_one("area luma 639x479 / 2", &out, &want)?;
        let rgb: Vec<u8> = (0..w * h * 3).map(|i| (i % 253) as u8).collect();
        let (s, d) = (
            Layout::packed(PixelFormat::Rgb24, w, h),
            Layout::packed(PixelFormat::Rgb24, w / 4, h / 4),
        );
        let mut out = vec![0u8; d.frame_len()];
        rga.resize_bytes(s, &rgb, d, &mut out, Filter::Area)
            .map_err(|e| e.to_string())?;
        let want = r::resize_area(&rgb, w as usize, h as usize, 3, 4).0;
        within_one("area rgb 640x480 / 4", &out, &want)
    });
    conform_with(evidence("daedalus.image.resize_bilinear"), || {
        let (sw, sh) = (160u32, 120u32);
        let rgb: Vec<u8> = (0..sw * sh * 3).map(|i| (i % 253) as u8).collect();
        let (s, d) = (
            Layout::packed(PixelFormat::Rgb24, sw, sh),
            Layout::packed(PixelFormat::Rgb24, sw * 2, sh * 2),
        );
        let mut out = vec![0u8; d.frame_len()];
        rga.resize_bytes(s, &rgb, d, &mut out, Filter::Bilinear)
            .map_err(|e| e.to_string())?;
        let want = r::resize_bilinear(
            &rgb,
            sw as usize,
            sh as usize,
            3,
            2 * sw as usize,
            2 * sh as usize,
        );
        within_one("bilinear rgb 160x120 x 2", &out, &want)
    });
}

/// A full-frame crop is a copy (vim2m copies RGB24 unchanged); a destination that is not the
/// rectangle's size is refused before any device call.
#[test]
fn vim2m_copies_through_a_full_frame_crop() {
    let Some(mut rga) = vim2m() else { return };
    let src = Layout::packed(PixelFormat::Rgb24, 64, 48);
    let input = frame(src.frame_len(), 11);
    let mut out = vec![0u8; src.frame_len()];
    let full = Rect {
        left: 0,
        top: 0,
        width: 64,
        height: 48,
    };
    rga.crop_bytes(src, &input, full, src, &mut out).unwrap();
    assert_eq!(out, input);
    let half = Layout::packed(PixelFormat::Rgb24, 32, 48);
    let err = rga
        .crop_bytes(src, &input, full, half, &mut out)
        .unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
}

/// Rockchip's RGA crop against the reference, recorded as the `rga-v4l2` variant. Written,
/// untested on board.
#[test]
fn rga_crops_like_the_reference() {
    use daedalus_accel::{Backend, Evidence, conform_with};
    use daedalus_kernels::reference as r;
    let Some(mut rga) = Rga::find() else {
        return eprintln!("skipping: no rockchip-rga device");
    };
    conform_with(
        Evidence::new("daedalus.image.crop", Backend::FixedFunction).variant("rga-v4l2"),
        || {
            let (w, h) = (640u32, 480u32);
            let rgb = frame((w * h * 3) as usize, 5);
            let rect = Rect {
                left: 64,
                top: 32,
                width: 320,
                height: 240,
            };
            let (s, d) = (
                Layout::packed(PixelFormat::Rgb24, w, h),
                Layout::packed(PixelFormat::Rgb24, 320, 240),
            );
            let mut out = vec![0u8; d.frame_len()];
            rga.crop_bytes(s, &rgb, rect, d, &mut out)
                .map_err(|e| e.to_string())?;
            (out == r::crop(&rgb, w as usize, 3, (64, 32, 320, 240)))
                .then_some(())
                .ok_or_else(|| "differs".into())
        },
    );
}

/// Flips through the context's controls on vim2m (it copies RGB24, so the result is exact);
/// vim2m has no rotate control, so a turn is refused as `Unsupported`.
#[test]
fn vim2m_flips_through_the_controls() {
    use daedalus_kernels::reference as r;
    let Some(mut rga) = vim2m() else { return };
    let (w, h) = (64u32, 48u32);
    let src = Layout::packed(PixelFormat::Rgb24, w, h);
    let input = frame(src.frame_len(), 17);
    for (t, horizontal, vertical) in [
        (Transform::FlipHorizontal, true, false),
        (Transform::FlipVertical, false, true),
    ] {
        let mut out = vec![0u8; src.frame_len()];
        rga.transform_bytes(src, &input, src, &mut out, t).unwrap();
        let want = r::flip(&input, w as usize, h as usize, 3, horizontal, vertical);
        assert!(out == want, "{t:?}");
    }
    let turned = Layout::packed(PixelFormat::Rgb24, h, w);
    let mut out = vec![0u8; turned.frame_len()];
    match rga.transform_bytes(src, &input, turned, &mut out, Transform::Rotate90) {
        Err(e) if e.kind() == std::io::ErrorKind::Unsupported => {}
        other => panic!("vim2m has no rotate control, got {other:?}"),
    }
    let err = rga
        .transform_bytes(src, &input, src, &mut out, Transform::Rotate90)
        .unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
}

/// Rockchip's RGA flips and turns against the references, recorded as the `rga-v4l2` variant.
/// Written, untested on board.
#[test]
fn rga_flips_and_turns_like_the_references() {
    use daedalus_accel::{Backend, Evidence, conform_with};
    use daedalus_kernels::reference as r;
    let Some(mut rga) = Rga::find() else {
        return eprintln!("skipping: no rockchip-rga device");
    };
    let (w, h) = (320u32, 240u32);
    let src = Layout::packed(PixelFormat::Rgb24, w, h);
    let input = frame(src.frame_len(), 23);
    let mut turn = |t: Transform, want: &[u8]| -> Result<(), String> {
        let (ow, oh) = t.output_size(w, h);
        let dst = Layout::packed(PixelFormat::Rgb24, ow, oh);
        let mut out = vec![0u8; dst.frame_len()];
        rga.transform_bytes(src, &input, dst, &mut out, t)
            .map_err(|e| format!("{t:?}: {e}"))?;
        (out == want)
            .then_some(())
            .ok_or_else(|| format!("{t:?} differs"))
    };
    let (wu, hu) = (w as usize, h as usize);
    let evidence = |op| Evidence::new(op, Backend::FixedFunction).variant("rga-v4l2");
    conform_with(evidence("daedalus.image.flip"), || {
        turn(
            Transform::FlipHorizontal,
            &r::flip(&input, wu, hu, 3, true, false),
        )?;
        turn(
            Transform::FlipVertical,
            &r::flip(&input, wu, hu, 3, false, true),
        )
    });
    conform_with(evidence("daedalus.image.rotate"), || {
        for (t, q) in [
            (Transform::Rotate90, 1),
            (Transform::Rotate180, 2),
            (Transform::Rotate270, 3),
        ] {
            turn(t, &r::rotate(&input, wu, hu, 3, q).0)?;
        }
        Ok(())
    });
}
