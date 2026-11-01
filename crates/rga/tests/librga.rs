//! The librga backend against the references, recorded as the fixed-function `librga` variant.
//! Runs on Rockchip boards with `librga.so` and the RGA driver, and skips elsewhere.
#![cfg(target_os = "linux")]
#![allow(clippy::print_stderr)]

use daedalus_accel::{Backend, Evidence, conform_with};
use daedalus_kernels::reference as r;
use daedalus_rga::Librga;

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
