//! The operations against software stand-ins for the library: an allocator, a blit that
//! samples the inverse of the matrix at pixel centres (point, or bilinear at the half-pixel
//! phase with rounded weights), a clear, a colour table and a stroke of a one-pixel line. They
//! encode what this crate reads from the library's source; the board run measures the GPU.

use std::cell::RefCell;
use std::ffi::{c_int, c_void};

use daedalus_kernels::reference as r;

use crate::ffi::{self, Api, Buffer, Matrix, Path, PathApi, Rectangle, SUCCESS, VgError};
use crate::{BlendMode, Filter, Format, Image, Transform, Vglite};

thread_local! {
    static CLUT: RefCell<Vec<u32>> = const { RefCell::new(Vec::new()) };
}

fn bpp(format: c_int) -> usize {
    if format == ffi::RGBA8888 || format == ffi::BGRA8888 || format == ffi::RGBX8888 {
        4
    } else {
        1
    }
}

fn pixels<'a>(b: &Buffer) -> &'a mut [u8] {
    let len = b.stride as usize * b.height as usize;
    // SAFETY: the stand-in allocator made `len` bytes at `memory`.
    unsafe { std::slice::from_raw_parts_mut(b.memory.cast::<u8>(), len) }
}

unsafe extern "C" fn init(_: i32, _: i32) -> VgError {
    SUCCESS
}
unsafe extern "C" fn finish() -> VgError {
    SUCCESS
}
unsafe extern "C" fn allocate(b: *mut Buffer) -> VgError {
    // SAFETY: the caller's buffer.
    let b = unsafe { &mut *b };
    let row = b.width as usize * bpp(b.format);
    // gcFEATURE_VG_16PIXELS_ALIGNED: rows padded to 16 pixels.
    b.stride = row.next_multiple_of(16 * bpp(b.format)) as i32;
    let mem = vec![0xa5u8; b.stride as usize * b.height as usize].leak();
    b.memory = mem.as_mut_ptr().cast();
    b.handle = b.memory;
    SUCCESS
}
unsafe extern "C" fn free(b: *mut Buffer) -> VgError {
    // SAFETY: allocated above with this length.
    let b = unsafe { &mut *b };
    let len = b.stride as usize * b.height as usize;
    drop(unsafe {
        Box::from_raw(std::ptr::slice_from_raw_parts_mut(
            b.memory.cast::<u8>(),
            len,
        ))
    });
    SUCCESS
}
unsafe extern "C" fn map(_: *mut Buffer, _: c_int, _: i32) -> VgError {
    ffi::NOT_SUPPORT
}
unsafe extern "C" fn unmap(_: *mut Buffer) -> VgError {
    SUCCESS
}
unsafe extern "C" fn clear(t: *mut Buffer, rect: *mut Rectangle, color: u32) -> VgError {
    // SAFETY: the caller's buffer and rectangle.
    let (t, rect) = unsafe { (&mut *t, *rect) };
    let n = bpp(t.format);
    let value = if t.format == ffi::A8 {
        vec![(color >> 24) as u8]
    } else {
        color.to_le_bytes()[..n].to_vec()
    };
    let px = pixels(t);
    for y in rect.y..rect.y + rect.height {
        for x in rect.x..rect.x + rect.width {
            px[y as usize * t.stride as usize + x as usize * n..][..n].copy_from_slice(&value);
        }
    }
    SUCCESS
}

fn invert(m: &Matrix) -> [f32; 6] {
    let [[a, b, c], [d, e, f], _] = m.m;
    let det = a * e - b * d;
    [
        e / det,
        -b / det,
        (b * f - c * e) / det,
        -d / det,
        a / det,
        (c * d - a * f) / det,
    ]
}

unsafe extern "C" fn blit(
    t: *mut Buffer,
    s: *mut Buffer,
    m: *mut Matrix,
    blend: c_int,
    _: u32,
    filter: c_int,
) -> VgError {
    // SAFETY: the caller's buffers and matrix.
    let (t, s, m) = unsafe { (&mut *t, &*s, &*m) };
    let inv = invert(m);
    let (sw, sh) = (s.width as i64, s.height as i64);
    let n = bpp(t.format);
    let clut = CLUT.with(|c| c.borrow().clone());
    let texel = |x: i64, y: i64| -> Vec<u8> {
        let (x, y) = (x.clamp(0, sw - 1) as usize, y.clamp(0, sh - 1) as usize);
        let at = y * s.stride as usize + x * bpp(s.format);
        if s.format == ffi::INDEX_8 {
            clut[pixels(s)[at] as usize].to_le_bytes().to_vec()
        } else {
            pixels(s)[at..][..n].to_vec()
        }
    };
    let dst = pixels(t);
    for y in 0..t.height as i64 {
        for x in 0..t.width as i64 {
            let (cx, cy) = (x as f32 + 0.5, y as f32 + 0.5);
            let (u, v) = (
                inv[0] * cx + inv[1] * cy + inv[2],
                inv[3] * cx + inv[4] * cy + inv[5],
            );
            let value: Vec<u8> = if filter == ffi::FILTER_POINT {
                let (iu, iv) = (u.floor() as i64, v.floor() as i64);
                if iu < 0 || iv < 0 || iu >= sw || iv >= sh {
                    continue;
                }
                texel(iu, iv)
            } else {
                let (u, v) = (u - 0.5, v - 0.5);
                let (x0, y0) = (u.floor() as i64, v.floor() as i64);
                let (fx, fy) = (u - x0 as f32, v - y0 as f32);
                let (a, b, c, d) = (
                    texel(x0, y0),
                    texel(x0 + 1, y0),
                    texel(x0, y0 + 1),
                    texel(x0 + 1, y0 + 1),
                );
                (0..n)
                    .map(|k| {
                        let top = a[k] as f32 * (1.0 - fx) + b[k] as f32 * fx;
                        let bottom = c[k] as f32 * (1.0 - fx) + d[k] as f32 * fx;
                        (top * (1.0 - fy) + bottom * fy + 0.5).floor() as u8
                    })
                    .collect()
            };
            let out = &mut dst[y as usize * t.stride as usize + x as usize * n..][..n];
            if blend == ffi::BLEND_NONE {
                out.copy_from_slice(&value);
            } else {
                // Straight SRC_OVER, the reference's rounding.
                let a = value[3] as u32;
                for k in 0..4 {
                    out[k] = if k == 3 {
                        (a + (out[3] as u32 * (255 - a) + 127) / 255) as u8
                    } else {
                        ((value[k] as u32 * a + out[k] as u32 * (255 - a) + 127) / 255) as u8
                    };
                }
            }
        }
    }
    SUCCESS
}

unsafe extern "C" fn set_clut(count: u32, colors: *mut u32) -> VgError {
    // SAFETY: `count` entries.
    let table = unsafe { std::slice::from_raw_parts(colors, count as usize) }.to_vec();
    CLUT.with(|c| *c.borrow_mut() = table);
    SUCCESS
}

unsafe extern "C" fn init_path(
    p: *mut Path,
    _: c_int,
    _: c_int,
    length: u32,
    data: *mut c_void,
    _: f32,
    _: f32,
    _: f32,
    _: f32,
) -> VgError {
    // SAFETY: the caller's path.
    let p = unsafe { &mut *p };
    p.path = data;
    p.path_length = length;
    SUCCESS
}
unsafe extern "C" fn set_stroke(
    p: *mut Path,
    cap: c_int,
    _: c_int,
    width: f32,
    _: f32,
    _: *mut f32,
    _: u32,
    _: f32,
    color: u32,
) -> VgError {
    assert_eq!((cap, width), (ffi::CAP_SQUARE, 1.0));
    // SAFETY: the caller's path.
    unsafe { (*p).stroke_color = color };
    SUCCESS
}
unsafe extern "C" fn set_path_type(_: *mut Path, kind: c_int) -> VgError {
    assert_eq!(kind, ffi::DRAW_STROKE_PATH);
    SUCCESS
}
unsafe extern "C" fn with_path(_: *mut Path) -> VgError {
    SUCCESS
}
/// A 1-wide stroke with square caps: the pixels whose centre lies in the segment's rectangle
/// widened by half a pixel on every side.
unsafe extern "C" fn draw(
    t: *mut Buffer,
    p: *mut Path,
    _: c_int,
    _: *mut Matrix,
    _: c_int,
    color: u32,
) -> VgError {
    // SAFETY: the caller's buffer and path, whose data is the seven words of a line.
    let (t, p) = unsafe { (&mut *t, &*p) };
    let words = unsafe { std::slice::from_raw_parts(p.path.cast::<u32>(), 7) };
    assert_eq!(
        (words[0], words[3], words[6]),
        (ffi::OP_MOVE, ffi::OP_LINE, ffi::OP_END)
    );
    let f = |i: usize| f32::from_bits(words[i]);
    let (x0, y0, x1, y1) = (f(1), f(2), f(4), f(5));
    let len = ((x1 - x0).powi(2) + (y1 - y0).powi(2)).sqrt();
    let (dx, dy) = if len > 0.0 {
        ((x1 - x0) / len, (y1 - y0) / len)
    } else {
        (1.0, 0.0)
    };
    let n = bpp(t.format);
    let value = color.to_le_bytes();
    for y in 0..t.height {
        for x in 0..t.width {
            let (px, py) = (x as f32 + 0.5 - x0, y as f32 + 0.5 - y0);
            let along = px * dx + py * dy;
            let across = (px * dy - py * dx).abs();
            if along >= -0.5 && along <= len + 0.5 && across <= 0.5 {
                pixels(t)[y as usize * t.stride as usize + x as usize * n..][..n]
                    .copy_from_slice(&value[..n]);
            }
        }
    }
    SUCCESS
}

fn gpu() -> Vglite {
    Vglite::with_api(Api {
        init,
        finish,
        allocate,
        free,
        map,
        unmap,
        clear,
        blit,
        set_clut,
        paths: Some(PathApi {
            init_path,
            set_stroke,
            set_path_type,
            update_stroke: with_path,
            draw,
            clear_path: with_path,
        }),
    })
}

fn pattern(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i * 37 + 11) as u8).collect()
}

#[test]
fn area_2x_is_the_rounded_block_mean_at_the_half_phase() {
    let (src, dst) = (
        Image::packed(Format::L8, 33, 9),
        Image::packed(Format::L8, 16, 4),
    );
    let from = pattern(src.frame_len());
    let mut to = vec![0; dst.frame_len()];
    let cost = gpu()
        .resize_bytes(src, &from, dst, &mut to, Filter::Area)
        .unwrap();
    // The odd column and row are cropped: the reference sees 32x8.
    let even = r::crop(&from, 33, 1, (0, 0, 32, 8));
    assert_eq!(to, r::resize_area(&even, 32, 8, 1, 2).0);
    assert_eq!(cost.staged_bytes, 32 * 8 + 16 * 4);
    let quarter = Image::packed(Format::L8, 8, 2);
    let err = gpu()
        .resize_bytes(src, &from, quarter, &mut [0; 16], Filter::Area)
        .unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::Unsupported);
}

#[test]
fn bilinear_resizes_at_opencvs_phase() {
    let (src, dst) = (
        Image::packed(Format::L8, 8, 4),
        Image::packed(Format::L8, 20, 10),
    );
    let from = pattern(src.frame_len());
    let mut to = vec![0; dst.frame_len()];
    gpu()
        .resize_bytes(src, &from, dst, &mut to, Filter::Bilinear)
        .unwrap();
    let want = r::resize_bilinear(&from, 8, 4, 1, 20, 10);
    // The stand-in rounds float weights, the reference uses OpenCV's 11-bit fixed point.
    assert!(to.iter().zip(&want).all(|(a, b)| a.abs_diff(*b) <= 1));
}

#[test]
fn crops_flips_and_turns_like_the_reference() {
    let (w, h) = (5u32, 3u32);
    let src = Image::packed(Format::Rgba8888, w, h);
    let from = pattern(src.frame_len());
    let rect = (1, 1, 3, 2);
    let dst = Image::packed(Format::Rgba8888, 3, 2);
    let mut to = vec![0; dst.frame_len()];
    gpu().crop_bytes(src, &from, rect, dst, &mut to).unwrap();
    assert_eq!(to, r::crop(&from, 5, 4, (1, 1, 3, 2)));
    let turns = [
        (
            Transform::FlipHorizontal,
            r::flip(&from, 5, 3, 4, true, false),
        ),
        (
            Transform::FlipVertical,
            r::flip(&from, 5, 3, 4, false, true),
        ),
        (Transform::Rotate90, r::rotate(&from, 5, 3, 4, 1).0),
        (Transform::Rotate180, r::rotate(&from, 5, 3, 4, 2).0),
        (Transform::Rotate270, r::rotate(&from, 5, 3, 4, 3).0),
    ];
    for (t, want) in turns {
        let (ow, oh) = t.output_size(w, h);
        let dst = Image::packed(Format::Rgba8888, ow, oh);
        let mut to = vec![0; dst.frame_len()];
        gpu().transform_bytes(src, &from, dst, &mut to, t).unwrap();
        assert_eq!(to, want, "{t:?}");
    }
    let wrong = Image::packed(Format::Rgba8888, 5, 3);
    assert!(
        gpu()
            .transform_bytes(src, &from, wrong, &mut [0; 60], Transform::Rotate90)
            .is_err()
    );
}

#[test]
fn fills_palettes_and_blends() {
    let img = Image::packed(Format::Rgba8888, 4, 3);
    let mut buf = pattern(img.frame_len());
    let want = r::fill(&buf, 4, (1, 1, 2, 2), &[1, 2, 3, 4]);
    gpu()
        .fill_bytes(img, &mut buf, (1, 1, 2, 2), [1, 2, 3, 4])
        .unwrap();
    assert_eq!(buf, want);
    let mask = Image::packed(Format::A8, 4, 3);
    let mut m = vec![0; 12];
    gpu()
        .fill_bytes(mask, &mut m, (0, 0, 2, 1), [0, 0, 0, 255])
        .unwrap();
    assert_eq!(&m[..4], [255, 255, 0, 0]);
    let gray = Image::packed(Format::L8, 4, 3);
    assert!(
        gpu()
            .fill_bytes(gray, &mut m, (0, 0, 1, 1), [9; 4])
            .is_err()
    );

    let mut lut = [[0u8; 4]; 256];
    for (i, e) in lut.iter_mut().enumerate() {
        *e = [i as u8, (i * 3) as u8, 255 - i as u8, 200];
    }
    let indices = pattern(6 * 2);
    let mut rgba = vec![0; 6 * 2 * 4];
    gpu()
        .palette_bytes(6, 2, &indices, &lut, &mut rgba)
        .unwrap();
    assert_eq!(rgba, r::palette(&indices, &lut));

    let fg = pattern(img.frame_len());
    let under = vec![90; img.frame_len()];
    let mut dst = under.clone();
    gpu()
        .blend_bytes(img, &fg, img, &mut dst, BlendMode::SrcOver, false)
        .unwrap();
    assert_eq!(dst, r::blend(&fg, &under, r::Blend::SrcOver, false));
    let err = gpu()
        .blend_bytes(img, &fg, img, &mut dst, BlendMode::SrcOver, true)
        .unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::Unsupported);
}

#[test]
fn lines_cover_bresenhams_pixels_on_axes_and_diagonals() {
    let img = Image::packed(Format::L8, 9, 7);
    for (from, to) in [
        ((1, 1), (7, 1)),
        ((2, 0), (2, 6)),
        ((0, 0), (6, 6)),
        ((8, 0), (2, 6)),
        ((4, 3), (4, 3)),
    ] {
        let mut buf = vec![0; img.frame_len()];
        gpu()
            .line_bytes(img, &mut buf, from, to, [200, 0, 0, 255])
            .unwrap();
        assert_eq!(
            buf,
            r::line(&[0; 63], 9, 7, from, to, &[200]),
            "{from:?} {to:?}"
        );
    }
}
