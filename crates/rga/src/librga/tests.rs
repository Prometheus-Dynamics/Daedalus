//! librga's calls against software stand-ins for the library's functions.

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

// The interpolation of this thread's last `resize` call (each test runs on its own thread).
thread_local! {
    static LAST_INTERP: std::cell::Cell<i32> = const { std::cell::Cell::new(-1) };
}

/// Software `imresize_t`: nearest, sampling the first of each group, or the rounded block
/// mean of gray for `IM_INTERP_AVERAGE`.
unsafe extern "C" fn resize(
    src: RgaBuffer,
    dst: RgaBuffer,
    fx: f64,
    fy: f64,
    interpolation: c_int,
    sync: c_int,
) -> c_int {
    LAST_INTERP.set(interpolation);
    assert_eq!((fx, fy, sync), (0.0, 0.0, 1));
    if interpolation == INTERP_AVERAGE {
        let f = (src.width / dst.width) as usize;
        let (s, d) = (src.vir_addr.cast::<u8>(), dst.vir_addr.cast::<u8>());
        for oy in 0..dst.height as usize {
            for ox in 0..dst.width as usize {
                let mut sum = 0usize;
                for y in 0..f {
                    for x in 0..f {
                        let at = (oy * f + y) * src.wstride as usize + ox * f + x;
                        // SAFETY: the wrapped buffers cover their images.
                        sum += unsafe { *s.add(at) } as usize;
                    }
                }
                let n = f * f;
                // SAFETY: as above.
                unsafe { *d.add(oy * dst.wstride as usize + ox) = ((sum + n / 2) / n) as u8 };
            }
        }
        return 1;
    }
    if interpolation != INTER_NEAREST {
        return -1;
    }
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

/// Software `impalette_t`: index to RGBA through the 256x1 table.
unsafe extern "C" fn palette(src: RgaBuffer, dst: RgaBuffer, lut: RgaBuffer, sync: c_int) -> c_int {
    assert_eq!(sync, 1);
    assert_eq!(
        (src.format, dst.format),
        (RgaFormat::Bpp8 as c_int, RgaFormat::Rgba8888 as c_int)
    );
    assert_eq!(
        (lut.width, lut.height, lut.format),
        (256, 1, RgaFormat::Rgba8888 as c_int)
    );
    let (s, d, t) = (
        src.vir_addr.cast::<u8>(),
        dst.vir_addr.cast::<u8>(),
        lut.vir_addr.cast::<u8>(),
    );
    for i in 0..(src.width * src.height) as usize {
        // SAFETY: the wrapped buffers cover their images.
        unsafe {
            let at = *s.add(i) as usize * 4;
            for c in 0..4 {
                *d.add(i * 4 + c) = *t.add(at + c);
            }
        }
    }
    1
}

/// Software `imcolorkey_t`, `INVERTED`: draw the in-range pixels opaque.
unsafe extern "C" fn colorkey(
    src: RgaBuffer,
    dst: RgaBuffer,
    range: ColorkeyRange,
    mode: c_int,
    sync: c_int,
) -> c_int {
    assert_eq!((mode, sync), (COLORKEY_INVERTED, 1));
    assert_eq!(src.format, RgaFormat::Rgbx8888 as c_int);
    let (lo, hi) = (range.min.to_le_bytes(), range.max.to_le_bytes());
    assert_eq!((lo[3], hi[3]), (0, 0xff));
    let (s, d) = (src.vir_addr.cast::<u8>(), dst.vir_addr.cast::<u8>());
    for i in 0..(src.width * src.height) as usize {
        // SAFETY: the wrapped buffers cover their images.
        unsafe {
            let p = [*s.add(i * 4), *s.add(i * 4 + 1), *s.add(i * 4 + 2)];
            if (0..3).all(|c| lo[c] <= p[c] && p[c] <= hi[c]) {
                for (c, v) in p.into_iter().chain([255]).enumerate() {
                    *d.add(i * 4 + c) = v;
                }
            }
        }
    }
    1
}

// This thread's last flip mode or rotation.
thread_local! {
    static LAST_TURN: std::cell::Cell<i32> = const { std::cell::Cell::new(-1) };
}

/// Software `imcrop_t` on one-byte pixels.
unsafe extern "C" fn crop(src: RgaBuffer, dst: RgaBuffer, rect: ImRect, sync: c_int) -> c_int {
    assert_eq!(sync, 1);
    assert_eq!((dst.width, dst.height), (rect.width, rect.height));
    let (s, d) = (src.vir_addr.cast::<u8>(), dst.vir_addr.cast::<u8>());
    for y in 0..rect.height as usize {
        for x in 0..rect.width as usize {
            let at = (rect.y as usize + y) * src.wstride as usize + rect.x as usize + x;
            // SAFETY: the wrapped buffers cover their images.
            unsafe { *d.add(y * dst.wstride as usize + x) = *s.add(at) };
        }
    }
    1
}

/// Records the mode; the device work is the board's to check.
unsafe extern "C" fn flip(_: RgaBuffer, _: RgaBuffer, mode: c_int, sync: c_int) -> c_int {
    assert_eq!(sync, 1);
    LAST_TURN.set(mode);
    1
}

unsafe extern "C" fn rotate(_: RgaBuffer, _: RgaBuffer, rotation: c_int, sync: c_int) -> c_int {
    assert_eq!(sync, 1);
    LAST_TURN.set(rotation);
    1
}

fn stand_in() -> Librga {
    Librga::with_api(Api {
        wrap_fd,
        wrap_addr,
        cvtcolor,
        resize,
        querystring,
        palette: Some(palette),
        colorkey: Some(colorkey),
        crop: Some(crop),
        flip: Some(flip),
        rotate: Some(rotate),
    })
}

#[test]
fn palette_and_colour_key_pass_the_table_and_the_range() {
    let rga = stand_in();
    let mut lut = [[0u8; 4]; 256];
    for (i, e) in lut.iter_mut().enumerate() {
        *e = [i as u8, 255 - i as u8, 7, 255];
    }
    let mut rgba = [0u8; 8];
    rga.palette_bytes(2, 1, &[3, 250], &lut, &mut rgba).unwrap();
    assert_eq!(rgba, [3, 252, 7, 255, 250, 5, 7, 255]);
    // R, G, B, X: in range, G too high, in range at the bounds.
    let src = [10, 20, 30, 0, 10, 99, 30, 0, 5, 20, 40, 9];
    let mut mask = [7u8; 3];
    rga.color_key_mask(3, 1, &src, [5, 0, 30], [10, 20, 40], &mut mask)
        .unwrap();
    assert_eq!(mask, [255, 0, 255]);
    let bare = Librga::with_api(Api {
        palette: None,
        colorkey: None,
        crop: None,
        ..rga.api
    });
    let err = bare
        .palette_bytes(2, 1, &[0, 0], &lut, &mut rgba)
        .unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::Unsupported);
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
fn resize_crops_through_the_image_and_passes_the_filter() {
    let rga = stand_in();
    // 5x3 gray to 2x1: the trailing column and row are cropped by narrowing the image.
    let src: Vec<u8> = (0..15).map(|i| i * 10).collect();
    let (s, d) = (
        Image::packed(RgaFormat::Gray8, 5, 3),
        Image::packed(RgaFormat::Gray8, 2, 1),
    );
    let mut out = [0u8; 2];
    rga.resize_bytes(s, &src, d, &mut out, Filter::Area)
        .unwrap();
    assert_eq!(LAST_INTERP.get(), INTERP_AVERAGE);
    // (0 + 10 + 50 + 60 + 2) / 4 and (20 + 30 + 70 + 80 + 2) / 4.
    assert_eq!(out, [30, 50]);
    let err = rga
        .resize_bytes(
            s,
            &src,
            Image::packed(RgaFormat::Gray8, 3, 1),
            &mut [0; 3],
            Filter::Area,
        )
        .unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::Unsupported);
    let up = Image::packed(RgaFormat::Gray8, 10, 6);
    let err = rga
        .resize_bytes(s, &src, up, &mut [0; 60], Filter::Bilinear)
        .unwrap_err();
    assert_eq!(LAST_INTERP.get(), INTERP_LINEAR);
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

#[test]
fn crops_and_transforms_check_sizes_and_pass_the_mode() {
    let rga = stand_in();
    let gray = |w, h| Image::packed(RgaFormat::Gray8, w, h);
    let src: Vec<u8> = (0..12).collect();
    let mut out = [0u8; 4];
    rga.crop_bytes(gray(4, 3), &src, (1, 1, 2, 2), gray(2, 2), &mut out)
        .unwrap();
    assert_eq!(out, [5, 6, 9, 10]);
    for (rect, dst) in [((3, 1, 2, 2), gray(2, 2)), ((1, 1, 2, 2), gray(2, 1))] {
        let err = rga
            .crop_bytes(gray(4, 3), &src, rect, dst, &mut out)
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput, "{rect:?}");
    }
    let mut turned = [0u8; 12];
    for (t, mode, dst) in [
        (Transform::FlipHorizontal, FLIP_H, gray(4, 3)),
        (Transform::FlipVertical, FLIP_V, gray(4, 3)),
        (Transform::Rotate90, ROT_90, gray(3, 4)),
        (Transform::Rotate180, ROT_180, gray(4, 3)),
        (Transform::Rotate270, ROT_270, gray(3, 4)),
    ] {
        rga.transform_bytes(gray(4, 3), &src, dst, &mut turned, t)
            .unwrap();
        assert_eq!(LAST_TURN.get(), mode, "{t:?}");
    }
    let err = rga
        .transform_bytes(
            gray(4, 3),
            &src,
            gray(4, 3),
            &mut turned,
            Transform::Rotate90,
        )
        .unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
}
