//! The librga backend against the references, recorded as the fixed-function `librga` variant.
//! Runs on Rockchip boards with `librga.so` and the RGA driver, and skips elsewhere.
#![cfg(target_os = "linux")]
#![allow(clippy::print_stderr)]

use daedalus_accel::{Backend, Evidence, conform_with};
use daedalus_kernels::reference as r;
use daedalus_rga::{Filter, Image, Librga, RgaFormat};

fn pattern(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i * 31 + 7) as u8).collect()
}

fn same(got: &[u8], want: &[u8]) -> Result<(), String> {
    match got.iter().zip(want).position(|(a, b)| a != b) {
        Some(at) => Err(format!("byte {at}: {} != {}", got[at], want[at])),
        None if got.len() != want.len() => Err("lengths differ".into()),
        None => Ok(()),
    }
}

#[test]
fn librga_converts_like_the_references() {
    let Ok(rga) = Librga::load() else {
        return eprintln!("skipping: no librga.so");
    };
    eprintln!("librga: {}", rga.version());
    let (w, h) = (64u32, 16u32);
    let pixels = (w * h) as usize;
    let evidence = |op| Evidence::new(op, Backend::FixedFunction).variant("librga");
    conform_with(evidence("daedalus.pixel.rgb_to_rgba"), || {
        let src = pattern(pixels * 3);
        let mut dst = vec![0; pixels * 4];
        rga.rgb_to_rgba(w, h, &src, &mut dst)
            .map_err(|e| e.to_string())?;
        same(&dst, &r::rgb_to_rgba(&src))
    });
    conform_with(evidence("daedalus.pixel.rgba_to_rgb"), || {
        let src = pattern(pixels * 4);
        let mut dst = vec![0; pixels * 3];
        rga.rgba_to_rgb(w, h, &src, &mut dst)
            .map_err(|e| e.to_string())?;
        same(&dst, &r::rgba_to_rgb(&src))
    });
    conform_with(evidence("daedalus.pixel.gray_to_rgba"), || {
        let src = pattern(pixels);
        let mut dst = vec![0; pixels * 4];
        rga.gray_to_rgba(w, h, &src, &mut dst)
            .map_err(|e| e.to_string())?;
        same(&dst, &r::gray_to_rgba(&src))
    });
    conform_with(evidence("daedalus.pixel.rgba_red"), || {
        let src = pattern(pixels * 4);
        let mut dst = vec![0; pixels];
        rga.rgba_red(w, h, &src, &mut dst)
            .map_err(|e| e.to_string())?;
        same(&dst, &r::rgba_red(&src))
    });
}

/// Within the tolerance Eidos accepts (one level), printing the measured difference.
fn within_one(what: &str, got: &[u8], want: &[u8]) -> Result<(), String> {
    if got.len() != want.len() {
        return Err("lengths differ".into());
    }
    let diff = got
        .iter()
        .zip(want)
        .map(|(a, b)| a.abs_diff(*b))
        .max()
        .unwrap_or(0);
    eprintln!("{what}: max difference {diff}");
    (diff <= 1)
        .then_some(())
        .ok_or_else(|| format!("max difference {diff}"))
}

#[test]
fn librga_resizes_like_the_references() {
    let Ok(rga) = Librga::load() else {
        return eprintln!("skipping: no librga.so");
    };
    let (w, h) = (128u32, 64u32);
    let gray = pattern((w * h) as usize);
    let image = |w, h| Image::packed(RgaFormat::Gray8, w, h);
    let evidence = |op| Evidence::new(op, Backend::FixedFunction).variant("librga");
    conform_with(evidence("daedalus.image.resize_area"), || {
        let mut out = vec![0; (w * h / 4) as usize];
        rga.resize_bytes(
            image(w, h),
            &gray,
            image(w / 2, h / 2),
            &mut out,
            Filter::Area,
        )
        .map_err(|e| e.to_string())?;
        within_one(
            "area gray 128x64 / 2",
            &out,
            &r::resize_area(&gray, w as usize, h as usize, 1, 2).0,
        )
    });
    conform_with(evidence("daedalus.image.resize_bilinear"), || {
        for (ow, oh) in [(w * 2, h * 2), (w / 2, h / 2)] {
            let mut out = vec![0; (ow * oh) as usize];
            rga.resize_bytes(
                image(w, h),
                &gray,
                image(ow, oh),
                &mut out,
                Filter::Bilinear,
            )
            .map_err(|e| e.to_string())?;
            let want =
                r::resize_bilinear(&gray, w as usize, h as usize, 1, ow as usize, oh as usize);
            within_one(&format!("bilinear gray 128x64 to {ow}x{oh}"), &out, &want)?;
        }
        Ok(())
    });
}

#[test]
fn librga_palettes_and_colour_keys_like_the_references() {
    let Ok(rga) = Librga::load() else {
        return eprintln!("skipping: no librga.so");
    };
    let (w, h) = (128u32, 64u32);
    let pixels = (w * h) as usize;
    let evidence = |op| Evidence::new(op, Backend::FixedFunction).variant("librga");
    conform_with(evidence("daedalus.image.palette"), || {
        let mut lut = [[0u8; 4]; 256];
        for (i, e) in lut.iter_mut().enumerate() {
            *e = [i as u8, (i * 7) as u8, 255 - i as u8, (i * 3) as u8];
        }
        let indices = pattern(pixels);
        let mut dst = vec![0; pixels * 4];
        rga.palette_bytes(w, h, &indices, &lut, &mut dst)
            .map_err(|e| e.to_string())?;
        same(&dst, &r::palette(&indices, &lut))
    });
    conform_with(evidence("daedalus.image.color_key_mask"), || {
        let rgbx = pattern(pixels * 4);
        let (min, max) = ([40, 0, 100], [200, 160, 255]);
        let mut mask = vec![0; pixels];
        rga.color_key_mask(w, h, &rgbx, min, max, &mut mask)
            .map_err(|e| e.to_string())?;
        same(&mask, &r::color_key_mask(&rgbx, 4, min, max))
    });
}
