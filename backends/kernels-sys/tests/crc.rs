//! CRC-32: every carried path against the bitwise reference, and `crc32_combine` against the
//! CRC of the concatenation. A path the process cannot run (`run` returns `None` on an empty
//! input) is skipped.

use daedalus_accel::{Backend, Evidence, Ext, conform_with};
use daedalus_kernels_sys as k;
use daedalus_kernels_sys::Isa;
use daedalus_kernels_sys::reference as r;

const ISAS: [Isa; 5] = [
    Isa::Scalar,
    Isa::X86Sse2,
    Isa::Neon64,
    Isa::Neon32,
    Isa::Rvv,
];

const LENGTHS: [usize; 9] = [0, 1, 7, 31, 64, 129, 1000, 4099, 65_537];

/// Deterministic bytes.
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

#[test]
fn ieee_check_value() {
    assert_eq!(r::crc32(b"123456789"), 0xCBF4_3926);
    assert_eq!(k::crc32(b"123456789"), 0xCBF4_3926);
}

/// The inventory column and the extension each path needs (`None` for the base build).
fn column(isa: Isa) -> (Backend, &'static [Ext]) {
    match isa {
        Isa::Scalar => (Backend::Scalar, &[]),
        Isa::X86Sse2 => (Backend::X86Sse2, &[Ext::Pclmulqdq]),
        Isa::Neon64 => (Backend::Neon64, &[Ext::Crc32]),
        Isa::Neon32 => (Backend::Neon32, &[]),
        Isa::Rvv => (Backend::Rvv, &[Ext::Zvbc]),
        _ => (Backend::Scalar, &[]),
    }
}

#[test]
fn every_runnable_path_agrees_with_the_reference() {
    for isa in ISAS {
        let (backend, exts) = column(isa);
        conform_with(
            Evidence::new("daedalus.codec.crc32", backend).extensions(exts),
            || {
                for len in LENGTHS {
                    let data = bytes(len, 14);
                    let want = r::crc32(&data);
                    match k::crc32::run(isa, &data) {
                        Some(got) if got == want => {}
                        Some(got) => return Err(format!("{len} bytes: {got:#x} != {want:#x}")),
                        None => return Err("not runnable".into()),
                    }
                }
                Ok(())
            },
        );
    }
}

#[test]
fn dispatching_crc32_agrees_with_the_reference() {
    for len in LENGTHS {
        let data = bytes(len, 15);
        assert_eq!(k::crc32(&data), r::crc32(&data), "{len} bytes");
    }
}

#[test]
fn combine_matches_the_crc_of_the_concatenation() {
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
