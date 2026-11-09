//! Scalar conversions.

#![cfg(feature = "chip-params")]

use daedalus_cpu::chip::{Scalar, ScalarKind};
use daedalus_cpu::scalar_kind::f64_le;

#[test]
fn integers_convert_when_they_fit() {
    assert_eq!(
        Scalar::I64(255).coerce(ScalarKind::U8),
        Some(Scalar::U8(255))
    );
    assert_eq!(Scalar::I64(256).coerce(ScalarKind::U8), None);
    assert_eq!(Scalar::I64(-1).coerce(ScalarKind::U64), None);
    assert_eq!(Scalar::U64(u64::MAX).coerce(ScalarKind::I64), None);
    assert_eq!(
        Scalar::I8(-3).coerce(ScalarKind::I64),
        Some(Scalar::I64(-3))
    );
}

#[test]
fn an_integer_becomes_a_float_only_when_exact() {
    assert_eq!(
        Scalar::I64(1 << 24).coerce(ScalarKind::F32),
        Some(Scalar::F32(16_777_216.0))
    );
    assert_eq!(Scalar::I64((1 << 24) + 1).coerce(ScalarKind::F32), None);
    assert_eq!(
        Scalar::I64(-(1 << 24)).coerce(ScalarKind::F32),
        Some(Scalar::F32(-16_777_216.0))
    );
}

#[cfg(feature = "f64")]
#[test]
fn an_integer_becomes_an_f64_only_when_exact() {
    assert_eq!(
        Scalar::I64(1 << 53).coerce(ScalarKind::F64),
        Some(Scalar::F64(9_007_199_254_740_992.0))
    );
    assert_eq!(Scalar::I64((1 << 53) + 1).coerce(ScalarKind::F64), None);
    assert_eq!(
        Scalar::F32(0.25).coerce(ScalarKind::F64),
        Some(Scalar::F64(0.25))
    );
}

#[test]
fn other_conversions_are_refused() {
    assert_eq!(Scalar::F64(0.5).coerce(ScalarKind::I32), None);
    assert_eq!(Scalar::Bool(true).coerce(ScalarKind::U8), None);
    assert_eq!(Scalar::U8(1).coerce(ScalarKind::Bool), None);
    assert_eq!(Scalar::F64(1e300).coerce(ScalarKind::F32), None);
    assert_eq!(Scalar::F64(-1e300).coerce(ScalarKind::F32), None);
}

/// A spread of bit patterns, so the integer conversion is checked against the hardware's.
fn sample_bits() -> impl Iterator<Item = u64> {
    let mut state = 0x9e37_79b9_7f4a_7c15_u64;
    let random = std::iter::repeat_with(move || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    });
    let special = [
        0,
        1 << 63,
        f64::MIN_POSITIVE.to_bits(),
        1,
        0x36a0_0000_0000_0000, // half the smallest f32
        0x36a0_0000_0000_0001,
        0x36b0_0000_0000_0000,
        0x47ef_ffff_e000_0000, // f32::MAX
        f64::INFINITY.to_bits(),
        f64::NEG_INFINITY.to_bits(),
    ];
    // Values near f32's range, where the rounding happens.
    let near = random.take(200_000).map(|bits| {
        let exponent = 0x340 + (bits >> 52) % 0x140;
        (bits & !(0x7ff << 52)) | (exponent << 52)
    });
    special.into_iter().chain(random.take(200_000)).chain(near)
}

#[test]
fn f64_to_f32_matches_the_hardware() {
    for bits in sample_bits() {
        let value = f64::from_bits(bits);
        let converted = Scalar::F64(value).coerce(ScalarKind::F32);
        if value.is_nan() {
            assert!(matches!(converted, Some(Scalar::F32(out)) if out.is_nan()));
        } else if value.is_finite() && value.abs() > f64::from(f32::MAX) {
            assert_eq!(converted, None, "{value:e}");
        } else {
            let Some(Scalar::F32(out)) = converted else {
                panic!("{value:e}");
            };
            assert_eq!(out.to_bits(), (value as f32).to_bits(), "{value:e}");
        }
    }
}

#[test]
fn comparing_f64s_matches_the_hardware() {
    let all: Vec<f64> = sample_bits().take(2000).map(f64::from_bits).collect();
    let tricky = [
        0.0,
        -0.0,
        f64::NAN,
        f64::INFINITY,
        f64::NEG_INFINITY,
        1.0,
        -1.0,
    ];
    for a in all.iter().chain(&tricky) {
        for b in all.iter().take(100).chain(&tricky) {
            assert_eq!(f64_le(*a, *b), a <= b, "{a:e} <= {b:e}");
        }
    }
}

#[test]
fn ordering_needs_one_kind() {
    assert!(Scalar::U8(3).le(Scalar::U8(3)));
    assert!(!Scalar::U8(3).le(Scalar::I8(3)));
    assert!(!Scalar::F32(f32::NAN).le(Scalar::F32(1.0)));
}

#[test]
fn bits_round_trip_for_every_kind() {
    for kind in ScalarKind::ALL {
        let (low, high) = Scalar::bounds(kind);
        for value in [low, high] {
            assert_eq!(Scalar::from_bits(kind, value.bits()), value, "{kind:?}");
            assert_eq!(value.kind(), kind);
        }
    }
}

#[test]
fn the_kind_numbers_are_fixed() {
    assert_eq!(
        ScalarKind::ALL.map(|kind| kind as u8),
        [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    );
    assert_eq!(ScalarKind::Bool as u8, 0);
    assert_eq!(ScalarKind::F32 as u8, 9);
    assert_eq!(ScalarKind::from_u8(11), None);
}
