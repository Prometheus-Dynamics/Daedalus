//! `GpuImages` against the scalar references in `daedalus_kernels::reference`, bit for bit, with
//! inventory evidence per op: each op's GPU cell records the adapter and the time against the
//! scalar reference on a 4 MiB frame (skipped without a wgpu device, as `kernels_tests`).
#![allow(clippy::print_stderr)] // the measurements

use std::time::{Duration, Instant};

use daedalus_accel::{Backend, Evidence, conform_with};
use daedalus_kernels::reference as r;
use daedalus_kernels::reference::Blend;

use crate::kernels_tests::{adapter, bytes, out_storage, read, storage, wgpu_ctx};
use crate::shader::{ImageCall, ImageOp, ImageShape, MaskRop};
use crate::{GpuContextHandle, GpuImages, GpuStorage};

/// Frame shapes (width, height, channels) of the conformance inputs: single pixels, odd sizes
/// and rows that straddle the word and workgroup sizes.
const SHAPES: [(u32, u32, u32); 8] = [
    (1, 1, 1),
    (1, 7, 3),
    (5, 1, 4),
    (3, 5, 3),
    (7, 4, 4),
    (64, 33, 1),
    (129, 70, 3),
    (33, 17, 4),
];

/// One dispatch and the bytes the reference gives for it.
struct Case {
    op: ImageOp,
    shape: ImageShape,
    frame: Vec<u8>,
    aux: Option<Vec<u8>>,
    want: Vec<u8>,
}

fn case(op: ImageOp, shape: ImageShape, frame: &[u8], aux: Option<&[u8]>, want: Vec<u8>) -> Case {
    Case {
        op,
        shape,
        frame: frame.to_vec(),
        aux: aux.map(<[u8]>::to_vec),
        want,
    }
}

/// The scalar reference of an op: the output for a frame and its aux input.
type Reference<'a> = dyn Fn(&[u8], Option<&[u8]>) -> Vec<u8> + 'a;

/// The frame timed against the reference: a 4 MiB one (the sizes of the other kernels' runs).
struct Bench {
    op: ImageOp,
    shape: ImageShape,
    frame: Vec<u8>,
    aux: Option<Vec<u8>>,
}

fn gpu_run(ctx: &GpuContextHandle, images: &GpuImages, c: &Case) -> Result<Vec<u8>, String> {
    let src = storage(ctx, &c.frame);
    let aux = c.aux.as_deref().map(|a| storage(ctx, a));
    let out = GpuImages::output(c.op, c.shape).map_err(|e| e.to_string())?;
    let dst = out_storage(ctx, out.bytes());
    let call = ImageCall {
        op: c.op,
        shape: c.shape,
        src: &src,
        aux: aux.as_ref(),
        dst: &dst,
    };
    images.run(call).map_err(|e| e.to_string())?.wait();
    Ok(read(&dst, out.bytes() as usize))
}

fn median(mut v: Vec<Duration>) -> f64 {
    v.sort();
    v[v.len() / 2].as_secs_f64() * 1e3
}

/// The GPU pass (submit to result, five runs) against the scalar reference, on `bench`.
fn timing(
    ctx: &GpuContextHandle,
    images: &GpuImages,
    bench: &Bench,
    reference: &Reference<'_>,
) -> String {
    let src = storage(ctx, &bench.frame);
    let aux = bench.aux.as_deref().map(|a| storage(ctx, a));
    let out = GpuImages::output(bench.op, bench.shape).unwrap();
    let dst: GpuStorage = out_storage(ctx, out.bytes());
    let mut gpu = Vec::new();
    for _ in 0..5 {
        let start = Instant::now();
        let call = ImageCall {
            op: bench.op,
            shape: bench.shape,
            src: &src,
            aux: aux.as_ref(),
            dst: &dst,
        };
        images.run(call).unwrap().wait();
        gpu.push(start.elapsed());
    }
    let mut host = Vec::new();
    for _ in 0..5 {
        let start = Instant::now();
        std::hint::black_box(reference(&bench.frame, bench.aux.as_deref()));
        host.push(start.elapsed());
    }
    format!(
        "measured over a {} byte frame, data GPU-resident: gpu {:.3} ms submit-to-result vs scalar reference {:.3} ms",
        bench.shape.bytes(),
        median(gpu),
        median(host)
    )
}

/// Every case of one op matches the reference on the GPU; the evidence records the timing.
fn check(id: &str, cases: &[Case], bench: &Bench, reference: &Reference<'_>) {
    let Some(ctx) = wgpu_ctx() else { return };
    let images = GpuImages::new(&ctx).unwrap();
    let mut mismatch: Option<String> = None;
    for c in cases {
        let got = gpu_run(&ctx, &images, c);
        if got.as_ref().is_ok_and(|g| *g == c.want) {
            continue;
        }
        if mismatch.is_none() {
            let why = match got {
                Ok(_) => "bytes differ".to_string(),
                Err(e) => e,
            };
            mismatch = Some(format!("{id}: {:?} on {:?}: {why}", c.op, c.shape));
        }
    }
    let note = timing(&ctx, &images, bench, reference);
    eprintln!("{id} (compute): {note}");
    conform_with(
        Evidence::new(id, Backend::Gpu)
            .variant("compute")
            .adapter(&adapter(&ctx))
            .note(&note),
        || mismatch.map_or(Ok(()), Err),
    );
}

fn frame(shape: ImageShape, seed: u32) -> Vec<u8> {
    bytes(shape.bytes() as usize, seed)
}

#[test]
fn resize_ops_match_the_references() {
    let mut area = Vec::new();
    let mut bilinear = Vec::new();
    for (w, h, ch) in SHAPES {
        let shape = ImageShape::new(w, h, ch);
        let f = frame(shape, w * 31 + h);
        let (wu, hu, chu) = (w as usize, h as usize, ch as usize);
        for factor in [1u32, 2, 3, 4, 7] {
            let want = r::resize_area(&f, wu, hu, chu, factor as usize).0;
            area.push(case(ImageOp::ResizeArea { factor }, shape, &f, None, want));
        }
        for (ow, oh) in [
            ((w / 2).max(1), (h / 2).max(1)),
            (w * 2 + 1, h + 2),
            (1, 1),
            (w, h),
        ] {
            let want = r::resize_bilinear(&f, wu, hu, chu, ow as usize, oh as usize);
            bilinear.push(case(
                ImageOp::ResizeBilinear {
                    out_width: ow,
                    out_height: oh,
                },
                shape,
                &f,
                None,
                want,
            ));
        }
    }
    let big = ImageShape::new(1024, 1024, 4);
    let big_frame = frame(big, 5);
    let (bw, bh) = (1024usize, 1024usize);
    check(
        "daedalus.image.resize_area",
        &area,
        &Bench {
            op: ImageOp::ResizeArea { factor: 2 },
            shape: big,
            frame: big_frame.clone(),
            aux: None,
        },
        &|f, _| r::resize_area(f, bw, bh, 4, 2).0,
    );
    check(
        "daedalus.image.resize_bilinear",
        &bilinear,
        &Bench {
            op: ImageOp::ResizeBilinear {
                out_width: 1536,
                out_height: 768,
            },
            shape: big,
            frame: big_frame.clone(),
            aux: None,
        },
        &|f, _| r::resize_bilinear(f, bw, bh, 4, 1536, 768),
    );
}

#[test]
fn geometry_ops_match_the_references() {
    let (mut crop, mut flip, mut rotate, mut fill) =
        (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    for (w, h, ch) in SHAPES {
        let shape = ImageShape::new(w, h, ch);
        let f = frame(shape, w * 17 + h * 3 + 1);
        let (wu, hu, chu) = (w as usize, h as usize, ch as usize);
        for (l, t, cw, chh) in [
            (0, 0, w, h),
            (w / 2, 0, w - w / 2, h),
            (0, h / 3, w, h - h / 3),
            (w - 1, h - 1, 1, 1),
        ] {
            let want = r::crop(
                &f,
                wu,
                chu,
                (l as usize, t as usize, cw as usize, chh as usize),
            );
            let op = ImageOp::Crop {
                left: l,
                top: t,
                width: cw,
                height: chh,
            };
            crop.push(case(op, shape, &f, None, want));
        }
        for (horizontal, vertical) in [(false, false), (true, false), (false, true), (true, true)] {
            let want = r::flip(&f, wu, hu, chu, horizontal, vertical);
            let op = ImageOp::Flip {
                horizontal,
                vertical,
            };
            flip.push(case(op, shape, &f, None, want));
        }
        for quarters in 0..=4u32 {
            let want = r::rotate(&f, wu, hu, chu, quarters).0;
            rotate.push(case(ImageOp::Rotate { quarters }, shape, &f, None, want));
        }
        let value = bytes(chu, 77);
        let (l, t) = (w / 3, h / 3);
        let (rw, rh) = ((1 + w / 2).min(w - l), (1 + h / 2).min(h - t));
        for (l, t, rw, rh) in [(0, 0, w, h), (l, t, rw, rh)] {
            let rect = (l as usize, t as usize, rw as usize, rh as usize);
            let want = r::fill(&f, wu, rect, &value);
            let op = ImageOp::Fill {
                left: l,
                top: t,
                width: rw,
                height: rh,
            };
            fill.push(case(op, shape, &f, Some(&value), want));
        }
    }
    let big = ImageShape::new(1024, 1024, 4);
    let big_frame = frame(big, 6);
    let value = [9u8, 8, 7, 255];
    check(
        "daedalus.image.crop",
        &crop,
        &Bench {
            op: ImageOp::Crop {
                left: 3,
                top: 5,
                width: 800,
                height: 700,
            },
            shape: big,
            frame: big_frame.clone(),
            aux: None,
        },
        &|f, _| r::crop(f, 1024, 4, (3, 5, 800, 700)),
    );
    check(
        "daedalus.image.flip",
        &flip,
        &Bench {
            op: ImageOp::Flip {
                horizontal: true,
                vertical: true,
            },
            shape: big,
            frame: big_frame.clone(),
            aux: None,
        },
        &|f, _| r::flip(f, 1024, 1024, 4, true, true),
    );
    check(
        "daedalus.image.rotate",
        &rotate,
        &Bench {
            op: ImageOp::Rotate { quarters: 1 },
            shape: big,
            frame: big_frame.clone(),
            aux: None,
        },
        &|f, _| r::rotate(f, 1024, 1024, 4, 1).0,
    );
    check(
        "daedalus.image.fill",
        &fill,
        &Bench {
            op: ImageOp::Fill {
                left: 100,
                top: 100,
                width: 500,
                height: 400,
            },
            shape: big,
            frame: big_frame,
            aux: Some(value.to_vec()),
        },
        &|f, a| r::fill(f, 1024, (100, 100, 500, 400), a.unwrap_or(&[])),
    );
}

#[test]
fn colour_ops_match_the_references() {
    let (mut palette, mut key, mut rop, mut blend) =
        (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    let lut_bytes = bytes(1024, 21);
    let mut lut = [[0u8; 4]; 256];
    for (i, entry) in lut.iter_mut().enumerate() {
        entry.copy_from_slice(&lut_bytes[i * 4..i * 4 + 4]);
    }
    for (w, h, ch) in SHAPES {
        // Palette: index bytes, one per pixel.
        let idx = bytes((w * h) as usize, w * 13 + h);
        let shape = ImageShape::new(w, h, 1);
        let want = r::palette(&idx, &lut);
        palette.push(case(ImageOp::Palette, shape, &idx, Some(&lut_bytes), want));

        let shape = ImageShape::new(w, h, ch);
        let f = frame(shape, w * 7 + h * 11 + 2);
        let other = frame(shape, w * 5 + h + 9);
        for op in [MaskRop::And, MaskRop::Or, MaskRop::Xor] {
            let c = match op {
                MaskRop::And => '&',
                MaskRop::Or => '|',
                MaskRop::Xor => '^',
            };
            let want = r::mask_rop(&f, &other, c);
            rop.push(case(ImageOp::MaskRop(op), shape, &f, Some(&other), want));
        }
        if ch >= 3 {
            for (min, max) in [
                ([40u8, 50, 60], [200u8, 210, 220]),
                ([0, 0, 0], [255, 255, 255]),
                ([200, 200, 200], [10, 10, 10]),
            ] {
                let want = r::color_key_mask(&f, ch as usize, min, max);
                let op = ImageOp::ColorKeyMask { min, max };
                key.push(case(op, shape, &f, None, want));
            }
        }
        // Blend: RGBA8 foreground over a RGBA8 destination, every mode and both alpha forms.
        let shape = ImageShape::new(w, h, 4);
        let fg = frame(shape, w * 3 + h * 19 + 4);
        let dst = frame(shape, w * 23 + h + 6);
        for mode in [Blend::SrcOver, Blend::Src, Blend::DstOver] {
            for premultiplied in [false, true] {
                let want = r::blend(&fg, &dst, mode, premultiplied);
                let op = ImageOp::Blend {
                    mode,
                    premultiplied,
                };
                blend.push(case(op, shape, &fg, Some(&dst), want));
            }
        }
    }
    let big = ImageShape::new(1024, 1024, 4);
    let big_frame = frame(big, 8);
    let big_dst = frame(big, 9);
    let big_idx = bytes(1024 * 1024, 10);
    check(
        "daedalus.image.palette",
        &palette,
        &Bench {
            op: ImageOp::Palette,
            shape: ImageShape::new(1024, 1024, 1),
            frame: big_idx.clone(),
            aux: Some(lut_bytes.clone()),
        },
        &|f, _| r::palette(f, &lut),
    );
    check(
        "daedalus.image.color_key_mask",
        &key,
        &Bench {
            op: ImageOp::ColorKeyMask {
                min: [40, 50, 60],
                max: [200, 210, 220],
            },
            shape: big,
            frame: big_frame.clone(),
            aux: None,
        },
        &|f, _| r::color_key_mask(f, 4, [40, 50, 60], [200, 210, 220]),
    );
    check(
        "daedalus.image.mask_rop",
        &rop,
        &Bench {
            op: ImageOp::MaskRop(MaskRop::Xor),
            shape: big,
            frame: big_frame.clone(),
            aux: Some(big_dst.clone()),
        },
        &|f, a| r::mask_rop(f, a.unwrap_or(&[]), '^'),
    );
    check(
        "daedalus.image.blend",
        &blend,
        &Bench {
            op: ImageOp::Blend {
                mode: Blend::SrcOver,
                premultiplied: false,
            },
            shape: big,
            frame: big_frame,
            aux: Some(big_dst.clone()),
        },
        &|f, a| r::blend(f, a.unwrap_or(&[]), Blend::SrcOver, false),
    );
}

/// The line test of `image.wgsl` (`img_line_on`) in 64-bit host arithmetic: whether pixel `p` is
/// on the segment from `from` to `to`. The major axis is x when |dx| >= |dy|. Along it, step `i`
/// (of `|dx|`) lands on minor offset `floor((2 * minor * i + major) / (2 * major))`: the
/// reference's error rule rounds the exact minor coordinate half up, in the direction of the
/// line (its signs are taken from `to`, and `0` when a segment has no extent on that axis).
fn on_line(from: (i32, i32), to: (i32, i32), p: (i32, i32)) -> bool {
    let (x0, y0) = (i64::from(from.0), i64::from(from.1));
    let a = (i64::from(to.0) - x0).abs();
    let b = (i64::from(to.1) - y0).abs();
    let sx = if i64::from(to.0) >= x0 { 1 } else { -1 };
    let sy = if i64::from(to.1) >= y0 { 1 } else { -1 };
    let dx = i64::from(p.0) - x0;
    let dy = i64::from(p.1) - y0;
    if a == 0 && b == 0 {
        return dx == 0 && dy == 0;
    }
    if a >= b {
        let i = dx * sx;
        if !(0..=a).contains(&i) {
            return false;
        }
        dy == sy * ((2 * b * i + a) / (2 * a))
    } else {
        let j = dy * sy;
        if !(0..=b).contains(&j) {
            return false;
        }
        dx == sx * ((2 * a * j + b) / (2 * b))
    }
}

/// The closed form against the scalar reference on every segment with endpoints in a grid that
/// runs off a 8 by 8 frame on both sides: all octants, every slope up to the frame, ties,
/// zero-length segments and endpoints outside the frame. No GPU is needed for this check.
#[test]
fn closed_form_line_matches_the_reference_on_a_grid() {
    let (w, h) = (8usize, 8usize);
    let coords: Vec<i32> = (-3..=10).collect();
    let mut checked = 0u64;
    for &fx in &coords {
        for &fy in &coords {
            for &tx in &coords {
                for &ty in &coords {
                    let want = r::line(&vec![0; w * h], w, h, (fx, fy), (tx, ty), &[1]);
                    for y in 0..h {
                        for x in 0..w {
                            let got = on_line((fx, fy), (tx, ty), (x as i32, y as i32));
                            assert_eq!(
                                got,
                                want[y * w + x] == 1,
                                "line {:?} -> {:?} at ({x}, {y})",
                                (fx, fy),
                                (tx, ty)
                            );
                        }
                    }
                    checked += 1;
                }
            }
        }
    }
    assert_eq!(checked, 14u64.pow(4));
}

/// Lines against the reference on the GPU: for each shape, every segment whose end is offset by
/// -4 to 4 pixels on each axis from an anchor (all octants, the slopes between, zero length),
/// the anchor inside the frame or off it on either side, in one colour.
#[test]
fn line_matches_the_reference() {
    let mut cases = Vec::new();
    for (w, h, ch) in SHAPES {
        let shape = ImageShape::new(w, h, ch);
        let f = frame(shape, w * 41 + h * 7 + 3);
        let value = bytes(ch as usize, w + 5 * h);
        let (wi, hi) = (w as i32, h as i32);
        for anchor in [(wi / 2, hi / 2), (-3, -2), (wi + 2, hi - 1), (-7, hi + 5)] {
            for dx in -4..=4 {
                for dy in -4..=4 {
                    let to = (anchor.0 + dx, anchor.1 + dy);
                    let want = r::line(&f, w as usize, h as usize, anchor, to, &value);
                    let op = ImageOp::Line { from: anchor, to };
                    cases.push(case(op, shape, &f, Some(&value), want));
                }
            }
        }
    }
    let big = ImageShape::new(1024, 1024, 4);
    let big_frame = frame(big, 12);
    let value = [200u8, 100, 50, 255];
    check(
        "daedalus.image.line",
        &cases,
        &Bench {
            op: ImageOp::Line {
                from: (0, 0),
                to: (1023, 700),
            },
            shape: big,
            frame: big_frame,
            aux: Some(value.to_vec()),
        },
        &|f, a| r::line(f, 1024, 1024, (0, 0), (1023, 700), a.unwrap_or(&[])),
    );
}
