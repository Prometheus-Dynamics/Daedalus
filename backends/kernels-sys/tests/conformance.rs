//! Every kernel on every CPU ISA this process can run, against the scalar references. An ISA the
//! process cannot run (its `run` returns `None` on an empty input) is skipped.

use daedalus_kernels_sys as k;
use daedalus_kernels_sys::Isa;
use daedalus_kernels_sys::reference as r;

const ISAS: [Isa; 5] = [
    Isa::X86Sse2,
    Isa::X86Avx2,
    Isa::Neon64,
    Isa::Neon32,
    Isa::Rvv,
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
    assert_eq!(r::f32_from_le(&1.5f32.to_le_bytes()), [1.5],);
    assert_eq!(r::f16_to_f32(&0x3c00u16.to_le_bytes()), [1.0]);
    assert_eq!(r::f16_to_f32(&0x0001u16.to_le_bytes()), [5.960_464_5e-8]);
    assert_eq!(
        r::f32_to_unorm8(&[f32::NAN, -1.0, 0.5, 2.0]),
        [0, 0, 128, 255]
    );
    assert_eq!(r::hex_lower(&[0x0f, 0xa0]), *b"0fa0");
    assert_eq!(r::base64_encode(b"Man"), *b"TWFu");
    assert_eq!(r::base64_encode(b"Ma"), *b"TWE=");
    assert_eq!(r::base64_decode(b"TWE=").as_deref(), Some(&b"Ma"[..]));
    assert!(r::base64_decode(b"TWF=").is_none());
}

/// A test input of a given length and the reference output.
type Want<'a, T> = &'a dyn Fn(usize) -> (Vec<u8>, Vec<T>);
/// A kernel forced onto one ISA; `None` when the process cannot run it.
type Run<'a, T> = &'a dyn Fn(Isa, &[u8], &mut [T]) -> Option<()>;

/// Runs `kernel` on each runnable ISA at every length and compares with `want`.
fn check<T: PartialEq + Copy + Default + std::fmt::Debug>(
    op: &str,
    want: Want<'_, T>,
    kernel: Run<'_, T>,
) {
    for isa in ISAS {
        let (empty_in, empty_want) = want(0);
        let mut empty_out = vec![T::default(); empty_want.len()];
        if kernel(isa, &empty_in, &mut empty_out).is_none() {
            continue;
        }
        for len in LENGTHS {
            let (input, expected) = want(len);
            let mut out = vec![T::default(); expected.len()];
            assert!(
                kernel(isa, &input, &mut out).is_some(),
                "{op} on {isa:?} at {len}"
            );
            assert!(out == expected, "{op} on {isa:?} at {len} bytes");
        }
    }
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
        &|isa, s, d: &mut [Bits]| {
            let mut out = vec![0u8; d.len() * 4];
            k::copy_bytes::run(isa, s, &mut out)?;
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
        &|isa, s, d: &mut [Bits]| {
            let mut out = vec![0f32; d.len()];
            k::f16_to_f32_base::run(isa, s, &mut out)?;
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
        &|isa, s, d| k::f32_to_unorm8::run(isa, &r::f32_from_le(s), d),
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
        &|isa, s, d| k::copy_bytes::run(isa, s, d),
    );
    check(
        "daedalus.codec.hex_lower",
        &|n| {
            let i = bytes(n, 8);
            let w = r::hex_lower(&i);
            (i, w)
        },
        &|isa, s, d| k::hex_lower::run(isa, s, d),
    );
    // base64 encode: the vector part is whole groups; the public function adds the tail.
    check(
        "daedalus.codec.base64_encode",
        &|n| {
            let i = bytes(n, 9);
            let w = r::base64_encode(&i);
            (i, w)
        },
        &|isa, s, d| {
            let full = s.len() / 3;
            k::encode_groups::run(isa, &s[..full * 3], &mut d[..full * 4])?;
            let tail = k::base64_encode(&s[full * 3..]);
            d[full * 4..].copy_from_slice(tail.as_bytes());
            Some(())
        },
    );
    // base64 decode: valid input, and a corrupted body symbol must be caught by the vector path.
    check(
        "daedalus.codec.base64_decode",
        &|n| {
            let raw = bytes(n, 10);
            let i = r::base64_encode(&raw);
            (i, raw)
        },
        &|isa, s, d| {
            let groups = s.len() / 4;
            let body = groups.saturating_sub(1);
            if !k::decode_groups::run(isa, &s[..body * 4], &mut d[..body * 3])? {
                return None;
            }
            let tail = k::base64_decode(&s[body * 4..]).ok()?;
            d[body * 3..].copy_from_slice(&tail);
            let mut bad = s[..body * 4].to_vec();
            for (pos, byte) in [(0usize, b'*'), (bad.len().saturating_sub(1), b'=')] {
                if let Some(slot) = bad.get_mut(pos) {
                    let keep = *slot;
                    *slot = byte;
                    let mut scratch = vec![0u8; body * 3];
                    if k::decode_groups::run(isa, &bad, &mut scratch)? {
                        return None;
                    }
                    bad[pos] = keep;
                }
            }
            Some(())
        },
    );
}

/// binary16 with F16C (`x86_avx2+f16c`): every half, bit for bit. Skipped without F16C.
#[test]
fn f16_to_f32_with_f16c() {
    let all: Vec<u8> = (0..=u16::MAX).flat_map(u16::to_le_bytes).collect();
    for input in [all, bytes(2 * 1001, 15), bytes(14, 16)] {
        let want = r::f16_to_f32(&input);
        let mut got = vec![0f32; want.len()];
        if k::f16_to_f32_with_f16c(&input, &mut got).is_none() {
            return;
        }
        assert!(
            got.iter()
                .zip(&want)
                .all(|(a, b)| a.to_bits() == b.to_bits()),
            "{} halves differ",
            input.len() / 2
        );
    }
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

/// The `core::simd` kernels (nightly, `--cfg daedalus_portable_simd`) against the references.
#[cfg(daedalus_portable_simd)]
#[test]
fn portable_simd_kernels() {
    use daedalus_kernels_sys::portable as p;
    let all_lengths = |want: &dyn Fn(usize) -> (Vec<u8>, Vec<u8>),
                       run: &dyn Fn(&[u8], &mut [u8])| {
        LENGTHS.iter().all(|&n| {
            let (input, expected) = want(n);
            let mut out = vec![0u8; expected.len()];
            run(&input, &mut out);
            out == expected
        })
    };
    assert!(all_lengths(
        &|n| {
            let i = bytes(n, 7);
            (i.clone(), i)
        },
        &p::copy_bytes,
    ));
    assert!(all_lengths(
        &|n| {
            let i = bytes(n * 4, 5);
            (i.clone(), i)
        },
        &p::copy_bytes,
    ));
    assert!(all_lengths(
        &|n| {
            let i = bytes(n, 8);
            let w = r::hex_lower(&i);
            (i, w)
        },
        &p::hex_lower,
    ));
    assert!(all_lengths(
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
    ));
    assert!(LENGTHS.iter().all(|&n| {
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
    }));
    let halves: Vec<u8> = (0..=u16::MAX).flat_map(u16::to_le_bytes).collect();
    let want = r::f16_to_f32(&halves);
    let mut got = vec![0f32; want.len()];
    p::f16_to_f32(&halves, &mut got);
    assert!(
        got.iter()
            .zip(&want)
            .all(|(a, b)| a.to_bits() == b.to_bits())
    );
    let mut values: Vec<f32> = (0..=300_000)
        .map(|i| -0.25 + i as f32 * (1.5 / 300_000.0))
        .collect();
    values.extend([f32::NAN, f32::INFINITY, f32::NEG_INFINITY, -0.0]);
    values.extend((0..=255).map(|i| (i as f32 + 0.5) / 255.0));
    let want = r::f32_to_unorm8(&values);
    let mut got = vec![0u8; values.len()];
    p::f32_to_unorm8(&values, &mut got);
    assert!(got == want);
}
