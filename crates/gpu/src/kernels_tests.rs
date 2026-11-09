//! `GpuKernels` against the scalar references, bit for bit, with inventory evidence: each op's
//! GPU cell records the adapter and the measured GPU pass time against the CPU kernel on this
//! machine (skipped without a wgpu device).
#![allow(clippy::print_stderr)] // the measurements

use std::time::{Duration, Instant};

use daedalus_accel::{Backend, Evidence, conform_with};
use daedalus_kernels::reference as r;

use crate::{
    GpuBackendKind, GpuContextHandle, GpuKernel, GpuKernels, GpuOptions, GpuStorage, GpuTimer,
    select_backend,
};

/// One wgpu context for the whole test process. Opening a device per check made some opens fail
/// under load, and a check that then skipped silently lost its inventory evidence; now no adapter
/// skips every check, and a device that fails to open after another opened fails the test.
pub(crate) fn wgpu_ctx() -> Option<GpuContextHandle> {
    static CTX: std::sync::OnceLock<Option<GpuContextHandle>> = std::sync::OnceLock::new();
    CTX.get_or_init(|| {
        let ctx = select_backend(&GpuOptions {
            preferred_backend: Some(GpuBackendKind::Wgpu),
            ..Default::default()
        });
        let found = match &ctx {
            Ok(ctx) if ctx.wgpu().is_some() => None,
            Ok(ctx) => Some(format!("no wgpu backend (skipped: {:?})", ctx.skipped())),
            Err(err) => Some(err.to_string()),
        };
        if let Some(why) = found {
            // The inventory lane needs the evidence: a missing GPU there is a failure.
            assert!(
                std::env::var_os("ACCEL_REQUIRE_GPU").is_none(),
                "ACCEL_REQUIRE_GPU is set but no GPU opened: {why}"
            );
            eprintln!("skipping the GPU kernel checks: {why}");
            return None;
        }
        ctx.ok()
    })
    .clone()
}

pub(crate) fn adapter(ctx: &GpuContextHandle) -> String {
    let info = ctx.wgpu().unwrap().0.adapter_info();
    format!("{} ({})", info.name, info.driver_info)
}

pub(crate) fn bytes(len: usize, seed: u32) -> Vec<u8> {
    let mut x = seed.wrapping_mul(2_654_435_761) | 1;
    (0..len)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 17;
            x ^= x << 5;
            x as u8
        })
        .collect()
}

pub(crate) fn storage(ctx: &GpuContextHandle, data: &[u8]) -> GpuStorage {
    GpuStorage::with_data(ctx, if data.is_empty() { &[0; 4] } else { data }, None).unwrap()
}

pub(crate) fn out_storage(ctx: &GpuContextHandle, len: u64) -> GpuStorage {
    GpuStorage::new(ctx, len.next_multiple_of(4).max(4), None).unwrap()
}

pub(crate) fn read(s: &GpuStorage, len: usize) -> Vec<u8> {
    let mut out = vec![0u8; len.next_multiple_of(4)];
    s.read_into(0, &mut out).unwrap();
    out.truncate(len);
    out
}

/// Bytes as f32s for the CPU timing (the vectors come from `Vec<u8>`, 1-aligned in principle;
/// the allocator gives at least 8, checked).
fn as_f32(d: &mut [u8]) -> &mut [f32] {
    assert!(d.as_ptr().cast::<f32>().is_aligned());
    // SAFETY: aligned (checked), `len / 4` whole f32s, and every bit pattern is an f32.
    unsafe { std::slice::from_raw_parts_mut(d.as_mut_ptr().cast(), d.len() / 4) }
}

fn as_f32_ref(d: &[u8]) -> &[f32] {
    assert!(d.as_ptr().cast::<f32>().is_aligned());
    // SAFETY: as in `as_f32`.
    unsafe { std::slice::from_raw_parts(d.as_ptr().cast(), d.len() / 4) }
}

fn f32_bytes(v: &[f32]) -> Vec<u8> {
    v.iter().flat_map(|f| f.to_le_bytes()).collect()
}

/// Median GPU pass time of `kernel` over `n` elements in its compute or fragment form, and
/// median CPU time of `cpu`.
#[allow(clippy::too_many_arguments)]
fn measure(
    ctx: &GpuContextHandle,
    kernels: &GpuKernels,
    kernel: GpuKernel,
    fragment: bool,
    src: &GpuStorage,
    dst: &GpuStorage,
    n: u64,
    cpu: &mut dyn FnMut(),
) -> String {
    let (device, _) = ctx.wgpu().unwrap();
    let timer = GpuTimer::new(ctx).unwrap();
    let (mut gpu, mut total) = (Vec::new(), Vec::new());
    for _ in 0..7 {
        let start = Instant::now();
        let mut encoder = device.create_command_encoder(&Default::default());
        if fragment {
            kernels
                .encode_fragment(&mut encoder, kernel, src, dst, n, timer.render_writes())
                .unwrap();
        } else {
            kernels
                .encode(&mut encoder, kernel, src, dst, n, timer.compute_writes())
                .unwrap();
        }
        let mut timed = timer.submit(encoder);
        timed.ticket().wait();
        // Encode to completion: the pass plus every copy (the fragment form's target-to-buffer
        // copy) and the submission itself.
        total.push(start.elapsed());
        let elapsed = loop {
            if let Some(t) = timed.try_elapsed() {
                break t.unwrap();
            }
        };
        gpu.push(elapsed);
    }
    let mut host = Vec::new();
    for _ in 0..7 {
        let start = Instant::now();
        cpu();
        host.push(start.elapsed());
    }
    let median = |v: &mut Vec<Duration>| {
        v.sort();
        v[v.len() / 2].as_secs_f64() * 1e3
    };
    format!(
        "measured over {} input bytes, data GPU-resident: gpu {:.3} ms encode-to-result \
         (all copies; the pass alone {:.3} ms, {:?}) vs cpu kernel {:.3} ms",
        kernel.input_len(n),
        median(&mut total),
        median(&mut gpu),
        timer.source(),
        median(&mut host)
    )
}

/// One op: every length matches the reference on the GPU, in the compute and (where the op has
/// one) fragment forms, then a 4 MiB-ish measurement of each.
fn check(
    op: &str,
    kernel: GpuKernel,
    input: &dyn Fn(usize) -> Vec<u8>,
    want: &dyn Fn(&[u8]) -> Vec<u8>,
    cpu: &dyn Fn(&[u8], &mut [u8]),
    elements: &dyn Fn(&[u8]) -> u64,
) {
    let Some(ctx) = wgpu_ctx() else { return };
    let kernels = GpuKernels::new(&ctx).unwrap();
    let adapter = adapter(&ctx);
    let forms: &[bool] = if GpuKernels::has_fragment(kernel) {
        &[false, true]
    } else {
        &[false]
    };
    for &fragment in forms {
        let mut mismatch = None;
        for len in [1usize, 3, 7, 64, 129, 1000, 4099, 65_537] {
            let src_bytes = input(len);
            let n = elements(&src_bytes);
            let expected = want(&src_bytes);
            let (src, dst) = (
                storage(&ctx, &src_bytes),
                out_storage(&ctx, kernel.output_len(n)),
            );
            let ticket = if fragment {
                kernels.run_fragment(kernel, &src, &dst, n)
            } else {
                kernels.run(kernel, &src, &dst, n)
            };
            ticket.unwrap().wait();
            let got = read(&dst, expected.len());
            if got != expected && mismatch.is_none() {
                mismatch = Some(format!("{op}: {n} elements differ"));
            }
        }
        let big = input(4 << 20);
        let n = elements(&big);
        let (src, dst) = (storage(&ctx, &big), out_storage(&ctx, kernel.output_len(n)));
        let mut out = vec![0u8; kernel.output_len(n) as usize];
        let note = measure(&ctx, &kernels, kernel, fragment, &src, &dst, n, &mut || {
            cpu(&big, &mut out)
        });
        let variant = if fragment { "fragment" } else { "compute" };
        eprintln!("{op} ({variant}): {note}");
        conform_with(
            Evidence::new(op, Backend::Gpu)
                .variant(variant)
                .adapter(&adapter)
                .note(&note),
            || mismatch.map_or(Ok(()), Err),
        );
    }
}

#[test]
fn pixel_kernels_match_the_references() {
    check(
        "daedalus.pixel.rgb_to_rgba",
        GpuKernel::RgbToRgba,
        &|n| bytes(n * 3, 1),
        &r::rgb_to_rgba,
        &|s, d| daedalus_kernels::rgb_to_rgba(s, d),
        &|s| s.len() as u64 / 3,
    );
    check(
        "daedalus.pixel.rgba_to_rgb",
        GpuKernel::RgbaToRgb,
        &|n| bytes(n * 4, 2),
        &r::rgba_to_rgb,
        &|s, d| daedalus_kernels::rgba_to_rgb(s, d),
        &|s| s.len() as u64 / 4,
    );
    check(
        "daedalus.pixel.gray_to_rgba",
        GpuKernel::GrayToRgba,
        &|n| bytes(n, 3),
        &r::gray_to_rgba,
        &|s, d| daedalus_kernels::gray_to_rgba(s, d),
        &|s| s.len() as u64,
    );
    check(
        "daedalus.pixel.rgba_red",
        GpuKernel::RgbaRed,
        &|n| bytes(n * 4, 4),
        &r::rgba_red,
        &|s, d| daedalus_kernels::rgba_red(s, d),
        &|s| s.len() as u64 / 4,
    );
}

#[test]
fn float_kernels_match_the_references() {
    check(
        "daedalus.float.f32_from_le",
        GpuKernel::F32FromLe,
        &|n| bytes(n * 4, 5),
        &|s| f32_bytes(&r::f32_from_le(s)),
        &|s, d| daedalus_kernels::copy_bytes(s, d),
        &|s| s.len() as u64 / 4,
    );
    // Every half, then random ones.
    let all: Vec<u8> = (0..=u16::MAX).flat_map(u16::to_le_bytes).collect();
    check(
        "daedalus.float.f16_to_f32",
        GpuKernel::F16ToF32,
        &|n| {
            if n == 65_537 {
                all.clone()
            } else {
                bytes(n * 2, 6)
            }
        },
        &|s| f32_bytes(&r::f16_to_f32(s)),
        &|s, d| {
            // The output bytes as f32 (aligned scratch is allocated once, outside the timing).
            daedalus_kernels::f16_to_f32(s, as_f32(d));
        },
        &|s| s.len() as u64 / 2,
    );
    let values: Vec<f32> = (0..=200_000)
        .map(|i| -0.25 + i as f32 * (1.5 / 200_000.0))
        .chain([
            f32::NAN,
            f32::INFINITY,
            f32::NEG_INFINITY,
            -0.0,
            0.5 / 255.0,
        ])
        .chain((0..=255).map(|i| (i as f32 + 0.5) / 255.0))
        .collect();
    let floats = f32_bytes(&values);
    check(
        "daedalus.float.f32_to_unorm8",
        GpuKernel::F32ToUnorm8,
        &|n| {
            if n == 65_537 {
                floats.clone()
            } else {
                floats[..(n * 4).min(floats.len())].to_vec()
            }
        },
        &|s| r::f32_to_unorm8(&r::f32_from_le(s)),
        &|s, d| {
            // GPU-resident floats are already f32 on the host side of this comparison.
            daedalus_kernels::f32_to_unorm8(as_f32_ref(s), d);
        },
        &|s| s.len() as u64 / 4,
    );
}

#[test]
fn copy_and_codec_kernels_match_the_references() {
    check(
        "daedalus.copy.bytes",
        GpuKernel::Copy,
        &|n| bytes(n, 7),
        &|s| s.to_vec(),
        &|s, d| daedalus_kernels::copy_bytes(s, d),
        &|s| s.len() as u64,
    );
    check(
        "daedalus.codec.hex_lower",
        GpuKernel::HexLower,
        &|n| bytes(n, 8),
        &r::hex_lower,
        &|s, d| daedalus_kernels::hex_lower(s, d),
        &|s| s.len() as u64,
    );
    check(
        "daedalus.codec.base64_encode",
        GpuKernel::Base64Encode,
        &|n| bytes(n, 9),
        &r::base64_encode,
        &|s, d| {
            daedalus_kernels::base64_encode_into(s, d);
        },
        &|s| s.len() as u64,
    );
    check(
        "daedalus.codec.base64_decode",
        GpuKernel::Base64Decode,
        &|n| r::base64_encode(&bytes(n, 10)),
        &|s| r::base64_decode(s).unwrap(),
        &|s, d| {
            let _ = daedalus_kernels::base64_decode_into(s, d);
        },
        &|s| s.len() as u64,
    );
}

/// The decoder's length and errors match the CPU decoder's.
#[test]
fn gpu_base64_decode_reports_length_and_errors() {
    let Some(ctx) = wgpu_ctx() else { return };
    let kernels = GpuKernels::new(&ctx).unwrap();
    let cases: [(&[u8], Option<&[u8]>); 6] = [
        (b"TWFu", Some(b"Man")),
        (b"TWE=", Some(b"Ma")),
        (b"TQ==", Some(b"M")),
        (b"TWF=", None),
        (b"T*Fu", None),
        (b"TW=u", None),
    ];
    for (input, want) in cases {
        let src = storage(&ctx, input);
        let dst = out_storage(&ctx, 4);
        let got = kernels.base64_decode(&src, &dst, input.len() as u64);
        match want {
            Some(want) => {
                assert_eq!(got.unwrap(), want.len() as u64);
                assert_eq!(read(&dst, want.len()), want);
            }
            None => assert!(got.is_err(), "{:?}", std::str::from_utf8(input)),
        }
        assert_eq!(daedalus_kernels::base64_decode(input).ok().as_deref(), want);
    }
}

/// The copy engine (`copy_buffer_to_buffer`) for GPU-resident bytes: the fixed-function cell
/// of the byte copy.
#[test]
fn copy_engine_copies_gpu_resident_bytes() {
    let Some(ctx) = wgpu_ctx() else { return };
    let (device, queue) = ctx.wgpu_locked().unwrap();
    let kernels = GpuKernels::new(&ctx).unwrap();
    let data = bytes(4 << 20, 11);
    let (src, dst) = (storage(&ctx, &data), out_storage(&ctx, data.len() as u64));
    let timer = GpuTimer::new(&ctx).unwrap();
    let mut times = Vec::new();
    for _ in 0..7 {
        let start = Instant::now();
        let mut encoder = device.create_command_encoder(&Default::default());
        kernels
            .copy_with_engine(&mut encoder, &src, &dst, data.len() as u64)
            .unwrap();
        crate::GpuTicket::submit_on(device, &queue, [encoder.finish()]).wait();
        times.push(start.elapsed());
    }
    times.sort();
    let mut host = vec![0u8; data.len()];
    let start = Instant::now();
    daedalus_kernels::copy_bytes(&data, &mut host);
    let cpu = start.elapsed();
    let note = format!(
        "measured over {} bytes: copy engine {:.3} ms submit-to-done ({:?} timer available) vs cpu {:.3} ms",
        data.len(),
        times[3].as_secs_f64() * 1e3,
        timer.source(),
        cpu.as_secs_f64() * 1e3
    );
    eprintln!("daedalus.copy.bytes copy engine: {note}");
    let got = read(&dst, data.len());
    let adapter = adapter(&ctx);
    conform_with(
        Evidence::new("daedalus.copy.bytes", Backend::FixedFunction)
            .variant("copy_engine")
            .adapter(&adapter)
            .note(&note),
        || {
            (got == data)
                .then_some(())
                .ok_or_else(|| "copy differs".into())
        },
    );
}

/// CRC-32 on the GPU (chunk CRCs combined on the host) against the bitwise reference, measured.
#[test]
fn gpu_crc32_matches_the_reference() {
    let Some(ctx) = wgpu_ctx() else { return };
    let kernels = GpuKernels::new(&ctx).unwrap();
    let mut mismatch = None;
    for len in [1usize, 7, 4095, 4096, 4097, 65_537, 1 << 20] {
        let data = bytes(len, 18);
        let got = kernels.crc32(&storage(&ctx, &data), len as u64).unwrap();
        if got != r::crc32(&data) && mismatch.is_none() {
            mismatch = Some(format!("{len} bytes: {got:#x}"));
        }
    }
    let big = bytes(4 << 20, 19);
    let src = storage(&ctx, &big);
    let mut gpu = Vec::new();
    for _ in 0..5 {
        let start = Instant::now();
        let _ = kernels.crc32(&src, big.len() as u64).unwrap();
        gpu.push(start.elapsed());
    }
    gpu.sort();
    let start = Instant::now();
    let _ = daedalus_kernels::crc32(&big);
    let cpu = start.elapsed();
    let note = format!(
        "measured over {} bytes, data GPU-resident: gpu chunks + host combine {:.3} ms (submit to result) vs cpu kernel {:.3} ms",
        big.len(),
        gpu[2].as_secs_f64() * 1e3,
        cpu.as_secs_f64() * 1e3
    );
    eprintln!("daedalus.codec.crc32: {note}");
    let adapter = adapter(&ctx);
    conform_with(
        Evidence::new("daedalus.codec.crc32", Backend::Gpu)
            .variant("chunks+combine")
            .adapter(&adapter)
            .note(&note),
        || mismatch.map_or(Ok(()), Err),
    );
}
