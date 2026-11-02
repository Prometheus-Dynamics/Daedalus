//! Every kernel on every CPU backend this process can run, against the scalar references.
//! With `ACCEL_RESULTS_DIR` set, each check records evidence for `docs/backend-matrix.md`.
#![allow(clippy::print_stderr)]

use daedalus_accel::{Backend, conform};
use daedalus_kernels as k;
use daedalus_kernels::reference as r;

const BACKENDS: [Backend; 5] = [
    Backend::X86Sse2,
    Backend::X86Avx2,
    Backend::Neon64,
    Backend::Neon32,
    Backend::Rvv,
];

/// Deterministic bytes; lengths straddle every vector width and tail.
fn bytes(len: usize, seed: u32) -> Vec<u8> {
    let mut x = seed.wrapping_mul(2_654_435_761).wrapping_add(1);
    (0..len)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 17;
            x ^= x << 5;
            x as u8
        })
        .collect()
}

const LENGTHS: [usize; 9] = [0, 1, 7, 31, 64, 129, 1000, 4099, 65_537];

/// The scalar reference against fixed answers.
#[test]
fn scalar_references_match_known_answers() {
    let ok = r::rgb_to_rgba(&[1, 2, 3, 4, 5, 6]) == [1, 2, 3, 255, 4, 5, 6, 255]
        && r::rgba_to_rgb(&[1, 2, 3, 4]) == [1, 2, 3]
        && r::gray_to_rgba(&[9]) == [9, 9, 9, 255]
        && r::rgba_red(&[7, 1, 2, 3]) == [7]
        && r::f32_from_le(&1.5f32.to_le_bytes()) == [1.5]
        && r::f16_to_f32(&0x3c00u16.to_le_bytes()) == [1.0]
        && r::f16_to_f32(&0x0001u16.to_le_bytes()) == [5.960_464_5e-8]
        && r::f32_to_unorm8(&[f32::NAN, -1.0, 0.5, 2.0]) == [0, 0, 128, 255]
        && r::hex_lower(&[0x0f, 0xa0]) == *b"0fa0"
        && r::base64_encode(b"Man") == *b"TWFu"
        && r::base64_encode(b"Ma") == *b"TWE="
        && r::base64_decode(b"TWE=").as_deref() == Some(&b"Ma"[..])
        && r::base64_decode(b"TWF=").is_none();
    // 2x2 area: (a+b+c+d+2)>>2 per channel; the odd trailing column is dropped.
    let area = r::resize_area(&[1, 2, 9, 4, 5, 9], 3, 2, 1, 2) == (vec![3], 1, 2 / 2)
        && r::resize_area(&[0, 10, 1, 11, 2, 12, 3, 13], 2, 2, 2, 2) == (vec![2, 12], 1, 1)
        && r::resize_area(&[7], 1, 1, 1, 2) == (vec![7], 1, 1);
    // 2x up: OpenCV's quarter weights, edges clamped.
    let bilinear = r::resize_bilinear(&[0, 100], 2, 1, 1, 4, 1) == [0, 25, 75, 100]
        && r::resize_bilinear(&[0, 100, 200, 44], 2, 2, 1, 2, 2) == [0, 100, 200, 44];
    for op in [
        "daedalus.pixel.rgb_to_rgba",
        "daedalus.pixel.rgba_to_rgb",
        "daedalus.pixel.gray_to_rgba",
        "daedalus.pixel.rgba_red",
        "daedalus.float.f32_from_le",
        "daedalus.float.f16_to_f32",
        "daedalus.float.f32_to_unorm8",
        "daedalus.copy.bytes",
        "daedalus.codec.hex_lower",
        "daedalus.codec.base64_encode",
        "daedalus.codec.base64_decode",
    ] {
        conform(op, Backend::Scalar, || ok);
    }
    conform("daedalus.image.resize_area", Backend::Scalar, || area);
    conform("daedalus.image.resize_bilinear", Backend::Scalar, || {
        bilinear
    });
}

/// A test input of a given length and the reference output.
type Want<'a, T> = &'a dyn Fn(usize) -> (Vec<u8>, Vec<T>);
/// A kernel forced onto one backend; `None` when the process cannot run it.
type Run<'a, T> = &'a dyn Fn(Backend, &[u8], &mut [T]) -> Option<()>;

/// Run `kernel` on `backend` into a fresh output and compare with `want`.
fn check<T: PartialEq + Copy + Default + std::fmt::Debug>(
    op: &str,
    want: Want<'_, T>,
    kernel: Run<'_, T>,
) {
    for backend in BACKENDS {
        conform(op, backend, || {
            LENGTHS.iter().all(|&len| {
                let (input, expected) = want(len);
                let mut out = vec![T::default(); expected.len()];
                kernel(backend, &input, &mut out).is_some() && out == expected
            })
        });
    }
}

#[test]
fn pixel_conversions() {
    check(
        "daedalus.pixel.rgb_to_rgba",
        &|n| {
            let i = bytes(n * 3, 1);
            let w = r::rgb_to_rgba(&i);
            (i, w)
        },
        &|b, s, d| k::rgb_to_rgba::run(b, s, d),
    );
    check(
        "daedalus.pixel.rgba_to_rgb",
        &|n| {
            let i = bytes(n * 4, 2);
            let w = r::rgba_to_rgb(&i);
            (i, w)
        },
        &|b, s, d| k::rgba_to_rgb::run(b, s, d),
    );
    check(
        "daedalus.pixel.gray_to_rgba",
        &|n| {
            let i = bytes(n, 3);
            let w = r::gray_to_rgba(&i);
            (i, w)
        },
        &|b, s, d| k::gray_to_rgba::run(b, s, d),
    );
    check(
        "daedalus.pixel.rgba_red",
        &|n| {
            let i = bytes(n * 4, 4);
            let w = r::rgba_red(&i);
            (i, w)
        },
        &|b, s, d| k::rgba_red::run(b, s, d),
    );
}

/// Bits, so NaN payloads and -0.0 compare exactly.
#[derive(Clone, Copy, Default, PartialEq, Debug)]
struct Bits(u32);

fn bits(v: Vec<f32>) -> Vec<Bits> {
    v.into_iter().map(|f| Bits(f.to_bits())).collect()
}

#[test]
fn float_conversions() {
    // f32 LE: random bit patterns, NaNs included.
    check(
        "daedalus.float.f32_from_le",
        &|n| {
            let i = bytes(n * 4, 5);
            let w = bits(r::f32_from_le(&i));
            (i, w)
        },
        // On little-endian targets the kernel is the vector byte copy into the f32s' bytes.
        &|b, s, d: &mut [Bits]| {
            let mut out = vec![0u8; d.len() * 4];
            k::copy_bytes::run(b, s, &mut out)?;
            for (d, v) in d.iter_mut().zip(out.as_chunks::<4>().0) {
                *d = Bits(u32::from_le_bytes(*v));
            }
            Some(())
        },
    );
    // f16: all 65536 halves (plus the random lengths).
    let all: Vec<u8> = (0..=u16::MAX).flat_map(u16::to_le_bytes).collect();
    check(
        "daedalus.float.f16_to_f32",
        &|n| {
            let i = if n == 65_537 {
                all.clone()
            } else {
                bytes(n * 2, 6)
            };
            let w = bits(r::f16_to_f32(&i));
            (i, w)
        },
        &|b, s, d: &mut [Bits]| {
            let mut out = vec![0f32; d.len()];
            k::f16_to_f32_base::run(b, s, &mut out)?;
            for (d, v) in d.iter_mut().zip(out) {
                *d = Bits(v.to_bits());
            }
            Some(())
        },
    );
    // unorm8: every f32 in [-0.25, 1.25] at a fine step, plus NaN, infinities and ties.
    let mut values: Vec<f32> = (0..=600_000)
        .map(|i| -0.25 + i as f32 * (1.5 / 600_000.0))
        .collect();
    values.extend([
        f32::NAN,
        f32::INFINITY,
        f32::NEG_INFINITY,
        0.5 / 255.0,
        127.5 / 255.0,
        -0.0,
    ]);
    values.extend((0..=255).map(|i| (i as f32 + 0.5) / 255.0));
    let input: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();
    check(
        "daedalus.float.f32_to_unorm8",
        &|n| {
            let take = (n * 4).min(input.len());
            let i = if n == 65_537 {
                input.clone()
            } else {
                input[..take].to_vec()
            };
            let w = r::f32_to_unorm8(&r::f32_from_le(&i));
            (i, w)
        },
        &|b, s, d| k::f32_to_unorm8::run(b, &r::f32_from_le(s), d),
    );
}

#[test]
fn copies_and_codecs() {
    check(
        "daedalus.copy.bytes",
        &|n| {
            let i = bytes(n, 7);
            (i.clone(), i)
        },
        &|b, s, d| k::copy_bytes::run(b, s, d),
    );
    check(
        "daedalus.codec.hex_lower",
        &|n| {
            let i = bytes(n, 8);
            let w = r::hex_lower(&i);
            (i, w)
        },
        &|b, s, d| k::hex_lower::run(b, s, d),
    );
    // base64 encode: the vector part is whole groups; the public function adds the tail.
    check(
        "daedalus.codec.base64_encode",
        &|n| {
            let i = bytes(n, 9);
            let w = r::base64_encode(&i);
            (i, w)
        },
        &|b, s, d| {
            let full = s.len() / 3;
            k::encode_groups::run(b, &s[..full * 3], &mut d[..full * 4])?;
            let tail = k::base64_encode(&s[full * 3..]);
            d[full * 4..].copy_from_slice(tail.as_bytes());
            Some(())
        },
    );
    // base64 decode: valid input, and every invalid byte position is rejected.
    check(
        "daedalus.codec.base64_decode",
        &|n| {
            let raw = bytes(n, 10);
            let i = r::base64_encode(&raw);
            (i, raw)
        },
        &|b, s, d| {
            let groups = s.len() / 4;
            let body = groups.saturating_sub(1);
            if !k::decode_groups::run(b, &s[..body * 4], &mut d[..body * 3])? {
                return None;
            }
            let tail = k::base64_decode(&s[body * 4..]).ok()?;
            d[body * 3..].copy_from_slice(&tail);
            // Corrupting any body symbol must be caught by the vector path.
            let mut bad = s[..body * 4].to_vec();
            for (pos, byte) in [(0usize, b'*'), (bad.len().saturating_sub(1), b'=')] {
                if let Some(slot) = bad.get_mut(pos) {
                    let keep = *slot;
                    *slot = byte;
                    let mut scratch = vec![0u8; body * 3];
                    if k::decode_groups::run(b, &bad, &mut scratch)? {
                        return None;
                    }
                    bad[pos] = keep;
                }
            }
            Some(())
        },
    );
}

/// CRC-32: slice-by-8, the extension kernels (PCLMULQDQ, AArch64 CRC, Zvbc) and the ARMv7
/// `vmull.p8` folding against the bitwise reference, at every length up to 300 bytes and a few large ones.
#[test]
fn crc32_kernels() {
    use daedalus_accel::{Evidence, Ext, conform_with};
    let check = |backend: Backend| -> Result<(), String> {
        for len in (0..300).chain([1000, 4099, 65_537, 1 << 20]) {
            let data = bytes(len, 14);
            let want = r::crc32(&data);
            match k::crc32::run(backend, &data) {
                Some(got) if got == want => {}
                Some(got) => return Err(format!("{len} bytes: {got:#x} != {want:#x}")),
                None => return Err("not runnable".into()),
            }
        }
        Ok(())
    };
    assert_eq!(r::crc32(b"123456789"), 0xCBF4_3926, "the IEEE check value");
    conform_with(
        Evidence::new("daedalus.codec.crc32", Backend::Scalar),
        || check(Backend::Scalar),
    );
    conform_with(
        Evidence::new("daedalus.codec.crc32", Backend::X86Sse2).extensions(&[Ext::Pclmulqdq]),
        || check(Backend::X86Sse2),
    );
    conform_with(
        Evidence::new("daedalus.codec.crc32", Backend::Neon64).extensions(&[Ext::Crc32]),
        || check(Backend::Neon64),
    );
    conform_with(
        Evidence::new("daedalus.codec.crc32", Backend::Rvv).extensions(&[Ext::Zvbc]),
        || check(Backend::Rvv),
    );
    conform_with(
        Evidence::new("daedalus.codec.crc32", Backend::Neon32),
        || check(Backend::Neon32),
    );
    assert_eq!(k::crc32(b"123456789"), 0xCBF4_3926);
    // Combining per-chunk CRCs gives the whole buffer's.
    let data = bytes(10_000, 17);
    for split in [0, 1, 7, 4096, 9_999, 10_000] {
        let (a, b) = data.split_at(split);
        assert_eq!(
            k::crc32_combine(k::crc32(a), k::crc32(b), b.len() as u64),
            r::crc32(&data),
            "split {split}"
        );
    }
}

/// binary16 with F16C (`x86_avx2+f16c`): every half, bit for bit.
#[test]
fn f16_to_f32_with_f16c() {
    use daedalus_accel::{Evidence, Ext, conform_with};
    let all: Vec<u8> = (0..=u16::MAX).flat_map(u16::to_le_bytes).collect();
    conform_with(
        Evidence::new("daedalus.float.f16_to_f32", Backend::X86Avx2).extensions(&[Ext::F16c]),
        || {
            for input in [all.clone(), bytes(2 * 1001, 15), bytes(14, 16)] {
                let want = r::f16_to_f32(&input);
                let mut got = vec![0f32; want.len()];
                k::f16_to_f32_with_f16c(&input, &mut got).ok_or("no F16C")?;
                if got
                    .iter()
                    .zip(&want)
                    .any(|(a, b)| a.to_bits() != b.to_bits())
                {
                    return Err(format!("{} halves differ", input.len() / 2));
                }
            }
            Ok(())
        },
    );
}

/// The public entry points (dispatching) agree with the references, including errors.
#[test]
fn public_entry_points() {
    for len in LENGTHS {
        let raw = bytes(len, 11);
        let enc = k::base64_encode(&raw);
        assert_eq!(enc.as_bytes(), r::base64_encode(&raw).as_slice());
        assert_eq!(k::base64_decode(enc.as_bytes()).unwrap(), raw);
    }
    for bad in [&b"TWF="[..], b"TWE", b"TW=u", b"T*Fu", b"====", b"TWFuTW=u"] {
        assert!(
            k::base64_decode(bad).is_err(),
            "{:?}",
            std::str::from_utf8(bad)
        );
        assert!(r::base64_decode(bad).is_none());
    }
    let le = bytes(4 * 1001, 13);
    let mut f = vec![0f32; 1001];
    k::f32_from_le(&le, &mut f);
    assert!(
        f.iter()
            .zip(r::f32_from_le(&le))
            .all(|(a, b)| a.to_bits() == b.to_bits())
    );
    let src = bytes(3 * 100, 12);
    let mut dst = vec![0u8; 5 * 100];
    k::copy_rows(&src, 3, &mut dst, 5, 3, 100);
    for row in 0..100 {
        assert_eq!(dst[row * 5..row * 5 + 3], src[row * 3..row * 3 + 3]);
    }
}

/// `ACCEL_DUMP=<path>` writes the registry for `accel-matrix render --registry`.
#[test]
fn registry_dump() {
    if let Some(path) = std::env::var_os("ACCEL_DUMP") {
        daedalus_accel::dump(path, &[&k::ACCEL]).expect("write the registry dump");
    }
    let json = daedalus_accel::dump_json(&[&k::ACCEL]);
    assert!(json.contains("daedalus.codec.base64_decode"));
}

/// The `core::simd` kernels (nightly, `--cfg daedalus_portable_simd`) against the references.
#[cfg(daedalus_portable_simd)]
#[test]
fn portable_simd_kernels() {
    use daedalus_kernels::portable as p;
    let pb = Backend::PortableSimd;
    let all_lengths = |want: &dyn Fn(usize) -> (Vec<u8>, Vec<u8>),
                       run: &dyn Fn(&[u8], &mut [u8])| {
        LENGTHS.iter().all(|&n| {
            let (input, expected) = want(n);
            let mut out = vec![0u8; expected.len()];
            run(&input, &mut out);
            out == expected
        })
    };
    conform("daedalus.pixel.rgb_to_rgba", pb, || {
        all_lengths(
            &|n| {
                let i = bytes(n * 3, 1);
                let w = r::rgb_to_rgba(&i);
                (i, w)
            },
            &p::rgb_to_rgba,
        )
    });
    conform("daedalus.pixel.rgba_to_rgb", pb, || {
        all_lengths(
            &|n| {
                let i = bytes(n * 4, 2);
                let w = r::rgba_to_rgb(&i);
                (i, w)
            },
            &p::rgba_to_rgb,
        )
    });
    conform("daedalus.pixel.gray_to_rgba", pb, || {
        all_lengths(
            &|n| {
                let i = bytes(n, 3);
                let w = r::gray_to_rgba(&i);
                (i, w)
            },
            &p::gray_to_rgba,
        )
    });
    conform("daedalus.pixel.rgba_red", pb, || {
        all_lengths(
            &|n| {
                let i = bytes(n * 4, 4);
                let w = r::rgba_red(&i);
                (i, w)
            },
            &p::rgba_red,
        )
    });
    conform("daedalus.copy.bytes", pb, || {
        all_lengths(
            &|n| {
                let i = bytes(n, 7);
                (i.clone(), i)
            },
            &p::copy_bytes,
        )
    });
    conform("daedalus.float.f32_from_le", pb, || {
        all_lengths(
            &|n| {
                let i = bytes(n * 4, 5);
                (i.clone(), i)
            },
            &p::copy_bytes,
        )
    });
    conform("daedalus.codec.hex_lower", pb, || {
        all_lengths(
            &|n| {
                let i = bytes(n, 8);
                let w = r::hex_lower(&i);
                (i, w)
            },
            &p::hex_lower,
        )
    });
    conform("daedalus.codec.base64_encode", pb, || {
        all_lengths(
            &|n| {
                let i = bytes(n, 9);
                let w = r::base64_encode(&i);
                (i, w)
            },
            &|s, d| {
                let full = s.len() / 3;
                p::base64_encode_groups(&s[..full * 3], &mut d[..full * 4]);
                d[full * 4..].copy_from_slice(k::base64_encode(&s[full * 3..]).as_bytes());
            },
        )
    });
    conform("daedalus.codec.base64_decode", pb, || {
        LENGTHS.iter().all(|&n| {
            let raw = bytes(n, 10);
            let enc = r::base64_encode(&raw);
            let body = (enc.len() / 4).saturating_sub(1);
            let mut out = vec![0u8; raw.len()];
            let ok = p::base64_decode_groups(&enc[..body * 4], &mut out[..body * 3]);
            let tail = k::base64_decode(&enc[body * 4..]).unwrap();
            out[body * 3..].copy_from_slice(&tail);
            let mut bad = enc[..body * 4].to_vec();
            let caught = match bad.first_mut() {
                None => true,
                Some(c) => {
                    *c = b'*';
                    !p::base64_decode_groups(&bad, &mut vec![0u8; body * 3])
                }
            };
            ok && out == raw && caught
        })
    });
    let halves: Vec<u8> = (0..=u16::MAX).flat_map(u16::to_le_bytes).collect();
    conform("daedalus.float.f16_to_f32", pb, || {
        let want = r::f16_to_f32(&halves);
        let mut got = vec![0f32; want.len()];
        p::f16_to_f32(&halves, &mut got);
        got.iter()
            .zip(&want)
            .all(|(a, b)| a.to_bits() == b.to_bits())
    });
    conform("daedalus.float.f32_to_unorm8", pb, || {
        let mut values: Vec<f32> = (0..=300_000)
            .map(|i| -0.25 + i as f32 * (1.5 / 300_000.0))
            .collect();
        values.extend([f32::NAN, f32::INFINITY, f32::NEG_INFINITY, -0.0]);
        values.extend((0..=255).map(|i| (i as f32 + 0.5) / 255.0));
        let want = r::f32_to_unorm8(&values);
        let mut got = vec![0u8; values.len()];
        p::f32_to_unorm8(&values, &mut got);
        got == want
    });
}

/// CRC-32 throughput of every backend this process can run, for choosing `crc32`'s dispatch:
/// `cargo test -p daedalus-kernels --release --test conformance crc32_throughput -- --ignored
/// --nocapture` (on the device itself; qemu timings mean nothing).
#[test]
#[ignore = "timing; run manually on the device"]
fn crc32_throughput() {
    use std::time::Instant;
    for len in [64usize, 1024, 64 << 10, 4 << 20] {
        let data = bytes(len, 21);
        let runs = ((64 << 20) / len).max(8);
        for backend in [Backend::Scalar].into_iter().chain(BACKENDS) {
            if k::crc32::run(backend, &data).is_none() {
                continue;
            }
            let start = Instant::now();
            let mut sink = 0u32;
            for _ in 0..runs {
                sink ^= k::crc32::run(backend, std::hint::black_box(&data)).unwrap();
            }
            let secs = start.elapsed().as_secs_f64();
            std::hint::black_box(sink);
            eprintln!(
                "crc32 {len:>8} B {:>7}: {:>8.0} MB/s",
                backend.name(),
                (len * runs) as f64 / secs / 1e6
            );
        }
    }
}
