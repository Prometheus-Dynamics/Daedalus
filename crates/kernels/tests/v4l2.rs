//! The V4L2 mem2mem converter against the kernel's `vim2m` test driver (RGB24 to BGR24), when
//! it is loaded. The same code drives Rockchip's RGA (`rockchip-rga`) on boards.
#![cfg(all(feature = "v4l2", target_os = "linux"))]
#![allow(clippy::print_stderr)]

use daedalus_kernels::v4l2::{PixelFormat, V4l2Converter, find_m2m_device};

#[test]
fn vim2m_converts_rgb24_to_bgr24() {
    let Some(path) = find_m2m_device("vim2m") else {
        return eprintln!("skipping: no vim2m device (modprobe vim2m)");
    };
    let (w, h) = (640u32, 480u32);
    let mut m2m = V4l2Converter::open(&path, w, h, PixelFormat::Rgb24, PixelFormat::Bgr24)
        .expect("open vim2m");
    let (in_len, out_len) = m2m.frame_lens();
    assert_eq!(in_len, (w * h * 3) as usize);
    for seed in 0..3u8 {
        let src: Vec<u8> = (0..in_len)
            .map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed))
            .collect();
        let mut dst = vec![0u8; out_len];
        let n = m2m.convert(&src, &mut dst).expect("convert");
        assert_eq!(n, out_len);
        let want: Vec<u8> = src
            .as_chunks::<3>()
            .0
            .iter()
            .flat_map(|p| [p[2], p[1], p[0]])
            .collect();
        assert_eq!(dst, want, "frame {seed}");
    }
}

/// Rockchip's RGA (`rockchip-rga`) against the references, recorded as the fixed-function
/// `rga-v4l2` variant. Written, untested on board: skips without the driver.
#[test]
fn rga_converts_like_the_references() {
    use daedalus_accel::{Backend, Evidence, conform_with};
    use daedalus_kernels::{reference as r, v4l2::Rga};
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
