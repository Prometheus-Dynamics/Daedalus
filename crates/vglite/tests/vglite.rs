//! VGLite against the references, recorded as the fixed-function `vglite` variant. Runs on a
//! K230 or K230D with `libvg_lite.so` and `/dev/vg_lite`, and skips elsewhere. Every
//! difference is printed: the exactness of each op is what this run measures.
#![cfg(target_os = "linux")]
#![allow(clippy::print_stderr)]

use std::os::fd::AsFd;

use daedalus_accel::{Backend, Evidence, conform_with};
use daedalus_kernels::reference as r;
use daedalus_vglite::{Backing, BlendMode, Filter, Format, Frame, Image, Transform, Vglite};
use lemnos_linux::dma::{Access, DmaBuf, DmaHeap};

fn pattern(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i * 31 + 7) as u8).collect()
}

/// Exact, printing the largest difference either way.
fn same(what: &str, got: &[u8], want: &[u8]) -> Result<(), String> {
    if got.len() != want.len() {
        return Err("lengths differ".into());
    }
    let diff = got
        .iter()
        .zip(want)
        .map(|(a, b)| a.abs_diff(*b))
        .max()
        .unwrap_or(0);
    let count = got.iter().zip(want).filter(|(a, b)| a != b).count();
    eprintln!("{what}: max difference {diff}, {count} bytes differ");
    (diff == 0)
        .then_some(())
        .ok_or_else(|| format!("max difference {diff}"))
}

/// Within the tolerance Eidos accepts for resizes (one level).
fn within_one(what: &str, got: &[u8], want: &[u8]) -> Result<(), String> {
    match same(what, got, want) {
        Ok(()) => Ok(()),
        Err(_) if got.iter().zip(want).all(|(a, b)| a.abs_diff(*b) <= 1) => Ok(()),
        Err(e) => Err(e),
    }
}

fn gpu() -> Option<Vglite> {
    match Vglite::load() {
        Ok(gpu) => Some(gpu),
        Err(e) => {
            eprintln!("skipping: {e}");
            None
        }
    }
}

fn evidence(op: &'static str) -> Evidence<'static> {
    Evidence::new(op, Backend::FixedFunction).variant("vglite")
}

#[test]
fn vglite_resizes_like_the_references() {
    let Some(gpu) = gpu() else { return };
    let (w, h) = (128u32, 64u32);
    let gray = pattern((w * h) as usize);
    let image = |w, h| Image::packed(Format::L8, w, h);
    conform_with(evidence("daedalus.image.resize_area"), || {
        let mut out = vec![0; (w * h / 4) as usize];
        gpu.resize_bytes(
            image(w, h),
            &gray,
            image(w / 2, h / 2),
            &mut out,
            Filter::Area,
        )
        .map_err(|e| e.to_string())?;
        within_one(
            "area L8 128x64 / 2",
            &out,
            &r::resize_area(&gray, w as usize, h as usize, 1, 2).0,
        )
    });
    conform_with(evidence("daedalus.image.resize_bilinear"), || {
        for (ow, oh) in [(w * 2, h * 2), (w / 2, h / 2), (200, 50)] {
            let mut out = vec![0; (ow * oh) as usize];
            gpu.resize_bytes(
                image(w, h),
                &gray,
                image(ow, oh),
                &mut out,
                Filter::Bilinear,
            )
            .map_err(|e| e.to_string())?;
            let want =
                r::resize_bilinear(&gray, w as usize, h as usize, 1, ow as usize, oh as usize);
            within_one(&format!("bilinear L8 128x64 to {ow}x{oh}"), &out, &want)?;
        }
        Ok(())
    });
}

#[test]
fn vglite_crops_flips_and_turns() {
    let Some(gpu) = gpu() else { return };
    let (w, h) = (64u32, 32u32);
    let src = Image::packed(Format::Rgba8888, w, h);
    let from = pattern(src.frame_len());
    conform_with(evidence("daedalus.image.crop"), || {
        let rect = (5, 3, 40, 20);
        let dst = Image::packed(Format::Rgba8888, 40, 20);
        let mut to = vec![0; dst.frame_len()];
        gpu.crop_bytes(src, &from, rect, dst, &mut to)
            .map_err(|e| e.to_string())?;
        same(
            "crop RGBA 64x32 (5, 3, 40, 20)",
            &to,
            &r::crop(&from, 64, 4, (5, 3, 40, 20)),
        )
    });
    let turn = |t: Transform| {
        let (ow, oh) = t.output_size(w, h);
        let dst = Image::packed(Format::Rgba8888, ow, oh);
        let mut to = vec![0; dst.frame_len()];
        gpu.transform_bytes(src, &from, dst, &mut to, t)
            .map(|_| to)
            .map_err(|e| e.to_string())
    };
    conform_with(evidence("daedalus.image.flip"), || {
        same(
            "flip H",
            &turn(Transform::FlipHorizontal)?,
            &r::flip(&from, 64, 32, 4, true, false),
        )?;
        same(
            "flip V",
            &turn(Transform::FlipVertical)?,
            &r::flip(&from, 64, 32, 4, false, true),
        )
    });
    conform_with(evidence("daedalus.image.rotate"), || {
        for (t, q) in [
            (Transform::Rotate90, 1),
            (Transform::Rotate180, 2),
            (Transform::Rotate270, 3),
        ] {
            same(
                &format!("{t:?}"),
                &turn(t)?,
                &r::rotate(&from, 64, 32, 4, q).0,
            )?;
        }
        Ok(())
    });
}

#[test]
fn vglite_fills_blends_palettes_and_draws_lines() {
    let Some(gpu) = gpu() else { return };
    let img = Image::packed(Format::Rgba8888, 32, 16);
    conform_with(evidence("daedalus.image.fill"), || {
        let mut buf = pattern(img.frame_len());
        let want = r::fill(&buf, 32, (3, 2, 20, 9), &[10, 20, 30, 40]);
        gpu.fill_bytes(img, &mut buf, (3, 2, 20, 9), [10, 20, 30, 40])
            .map_err(|e| e.to_string())?;
        same("fill RGBA", &buf, &want)?;
        let mask = Image::packed(Format::A8, 32, 16);
        let mut m = vec![0; mask.frame_len()];
        gpu.fill_bytes(mask, &mut m, (3, 2, 20, 9), [0, 0, 0, 255])
            .map_err(|e| e.to_string())?;
        same(
            "fill A8 mask",
            &m,
            &r::fill(&vec![0; 512], 32, (3, 2, 20, 9), &[255]),
        )
    });
    conform_with(evidence("daedalus.image.blend"), || {
        let fg = pattern(img.frame_len());
        let under: Vec<u8> = pattern(img.frame_len()).iter().map(|v| v ^ 0x5a).collect();
        for (mode, blend) in [
            (BlendMode::SrcOver, r::Blend::SrcOver),
            (BlendMode::Src, r::Blend::Src),
            (BlendMode::DstOver, r::Blend::DstOver),
        ] {
            let mut dst = under.clone();
            gpu.blend_bytes(img, &fg, img, &mut dst, mode, false)
                .map_err(|e| e.to_string())?;
            same(
                &format!("blend {mode:?}"),
                &dst,
                &r::blend(&fg, &under, blend, false),
            )?;
        }
        Ok(())
    });
    conform_with(evidence("daedalus.image.palette"), || {
        let mut lut = [[0u8; 4]; 256];
        for (i, e) in lut.iter_mut().enumerate() {
            *e = [i as u8, (i * 7) as u8, 255 - i as u8, (i * 3) as u8];
        }
        let indices = pattern(32 * 16);
        let mut rgba = vec![0; 32 * 16 * 4];
        gpu.palette_bytes(32, 16, &indices, &lut, &mut rgba)
            .map_err(|e| e.to_string())?;
        same("palette 32x16", &rgba, &r::palette(&indices, &lut))
    });
    conform_with(evidence("daedalus.image.line"), || {
        let gray = Image::packed(Format::Rgba8888, 32, 16);
        let lines = [
            ((1, 1), (30, 1)),
            ((4, 0), (4, 15)),
            ((0, 0), (15, 15)),
            ((31, 0), (16, 15)),
            ((2, 3), (29, 12)),
        ];
        let mut failed = None;
        for (from, to) in lines {
            let mut buf = vec![0; gray.frame_len()];
            gpu.line_bytes(gray, &mut buf, from, to, [255, 255, 255, 255])
                .map_err(|e| e.to_string())?;
            let want = r::line(&vec![0; gray.frame_len()], 32, 16, from, to, &[255; 4]);
            if let Err(e) = same(&format!("line {from:?} to {to:?}"), &buf, &want) {
                failed.get_or_insert(e);
            }
        }
        failed.map_or(Ok(()), Err)
    });
}

fn frame(image: Image, buf: &DmaBuf, backing: Backing) -> Frame<'_> {
    Frame {
        image,
        fd: buf.as_fd(),
        backing,
    }
}

/// Dmabufs both ways: the `system` heap (scattered, staged) and a CMA heap when the board has
/// one (contiguous, mapped in place).
#[test]
fn vglite_takes_dmabufs() {
    let Some(gpu) = gpu() else { return };
    let img = Image::packed(Format::Rgba8888, 64, 32);
    let from = pattern(img.frame_len());
    for (heap, backing) in [
        ("system", Backing::Scattered),
        ("linux,cma", Backing::Contiguous),
    ] {
        let Ok(heap) = DmaHeap::open(heap) else {
            eprintln!("no {heap} heap");
            continue;
        };
        let (src, dst) = (
            heap.allocate(img.frame_len()).unwrap(),
            heap.allocate(img.frame_len()).unwrap(),
        );
        let mut map = src.map().unwrap();
        src.begin_cpu_access(Access::Write).unwrap();
        map.as_mut_slice()[..from.len()].copy_from_slice(&from);
        src.end_cpu_access(Access::Write).unwrap();
        let cost = gpu
            .transform_dmabuf(
                frame(img, &src, backing),
                frame(img, &dst, backing),
                Transform::Rotate180,
            )
            .unwrap();
        eprintln!(
            "{} {backing:?}: {} bytes staged",
            heap.name(),
            cost.staged_bytes
        );
        let out = dst.map().unwrap();
        dst.begin_cpu_access(Access::Read).unwrap();
        let got = out.as_slice()[..from.len()].to_vec();
        dst.end_cpu_access(Access::Read).unwrap();
        assert_eq!(got, r::rotate(&from, 64, 32, 4, 2).0, "{backing:?}");
        assert_eq!(cost.staged_bytes == 0, backing == Backing::Contiguous);
    }
}
