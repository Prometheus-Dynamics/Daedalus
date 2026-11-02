//! Typed scalar values: graph constants, tunable parameters and widened edge values.

/// The scalar types a constant, a parameter or a widening edge can have (`isize`/`usize` are
/// target-sized and excluded). The discriminant is the wire tag and the loaded-mode type id.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
#[repr(u8)]
pub enum ScalarKind {
    Bool,
    I8,
    I16,
    I32,
    I64,
    U8,
    U16,
    U32,
    U64,
    F32,
    F64,
}

/// A value of a [`ScalarKind`].
#[derive(Clone, Copy, Debug, PartialEq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum Scalar {
    Bool(bool),
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    F32(f32),
    F64(f64),
}

/// Calls `$m!(Kind, rust_type)` for every scalar kind.
macro_rules! for_each_scalar {
    ($m:ident) => {
        $m! {
            Bool: bool, I8: i8, I16: i16, I32: i32, I64: i64, U8: u8, U16: u16, U32: u32,
            U64: u64, F32: f32, F64: f64,
        }
    };
}
pub(crate) use for_each_scalar;

impl ScalarKind {
    pub const ALL: [Self; 11] = [
        Self::Bool,
        Self::I8,
        Self::I16,
        Self::I32,
        Self::I64,
        Self::U8,
        Self::U16,
        Self::U32,
        Self::U64,
        Self::F32,
        Self::F64,
    ];

    pub const fn from_u8(tag: u8) -> Option<Self> {
        if (tag as usize) < Self::ALL.len() {
            Some(Self::ALL[tag as usize])
        } else {
            None
        }
    }

    /// The Rust type name (`f32`, ...).
    pub const fn rust_name(self) -> &'static str {
        macro_rules! names {
            ($($kind:ident: $ty:ty),* $(,)?) => {
                match self { $(Self::$kind => stringify!($ty)),* }
            };
        }
        for_each_scalar!(names)
    }

    pub const fn is_float(self) -> bool {
        matches!(self, Self::F32 | Self::F64)
    }

    /// The inclusive range of an integer kind.
    pub const fn int_range(self) -> Option<(i128, i128)> {
        Some(match self {
            Self::I8 => (i8::MIN as i128, i8::MAX as i128),
            Self::I16 => (i16::MIN as i128, i16::MAX as i128),
            Self::I32 => (i32::MIN as i128, i32::MAX as i128),
            Self::I64 => (i64::MIN as i128, i64::MAX as i128),
            Self::U8 => (0, u8::MAX as i128),
            Self::U16 => (0, u16::MAX as i128),
            Self::U32 => (0, u32::MAX as i128),
            Self::U64 => (0, u64::MAX as i128),
            _ => return None,
        })
    }

    /// The smallest and largest value (`-inf..=inf` for floats).
    pub const fn bounds(self) -> (Scalar, Scalar) {
        macro_rules! bounds {
            ($($kind:ident: $ty:ty),* $(,)?) => {
                match self {
                    Self::Bool => (Scalar::Bool(false), Scalar::Bool(true)),
                    Self::F32 => (Scalar::F32(f32::NEG_INFINITY), Scalar::F32(f32::INFINITY)),
                    Self::F64 => (Scalar::F64(f64::NEG_INFINITY), Scalar::F64(f64::INFINITY)),
                    $(Self::$kind => (Scalar::$kind(<$ty>::MIN), Scalar::$kind(<$ty>::MAX)),)*
                }
            };
        }
        bounds!(I8: i8, I16: i16, I32: i32, I64: i64, U8: u8, U16: u16, U32: u32, U64: u64)
    }

    /// Whether the builtin numeric widening adapter of the runtime converts `self` to `to` (the
    /// only adapter the MCU profile runs; `daedalus-mcu-build` tests the table against it).
    pub const fn widens_to(self, to: Self) -> bool {
        use ScalarKind::*;
        matches!(
            (self, to),
            (I8, I16 | I32 | I64 | F32 | F64)
                | (I16, I32 | I64 | F32 | F64)
                | (I32, I64 | F64)
                | (U8, U16 | U32 | U64 | I16 | I32 | I64 | F32 | F64)
                | (U16, U32 | U64 | I32 | I64 | F32 | F64)
                | (U32, U64 | I64 | F64)
                | (F32, F64)
        )
    }
}

impl Scalar {
    pub const fn kind(&self) -> ScalarKind {
        macro_rules! kind {
            ($($kind:ident: $ty:ty),* $(,)?) => {
                match self { $(Self::$kind(_) => ScalarKind::$kind),* }
            };
        }
        for_each_scalar!(kind)
    }

    fn int(self) -> Option<i128> {
        Some(match self {
            Self::I8(v) => v.into(),
            Self::I16(v) => v.into(),
            Self::I32(v) => v.into(),
            Self::I64(v) => v.into(),
            Self::U8(v) => v.into(),
            Self::U16(v) => v.into(),
            Self::U32(v) => v.into(),
            Self::U64(v) => v.into(),
            _ => return None,
        })
    }

    /// Convert to `kind` with the planner's rules for graph constants (`ValueType::check_value`):
    /// integers to any integer kind they fit and to floats that represent them exactly (up to
    /// 2^24 for `f32`, 2^53 for `f64`), floats to floats (`f32` rounds; a finite value beyond
    /// `f32::MAX` fails), `bool` to `bool`. `None` when the value does not convert.
    pub fn coerce(self, kind: ScalarKind) -> Option<Self> {
        if self.kind() == kind {
            return Some(self);
        }
        if let Some(v) = self.int() {
            if let Some((min, max)) = kind.int_range() {
                if v < min || v > max {
                    return None;
                }
                // In range: the casts are exact.
                return Some(match kind {
                    ScalarKind::I8 => Self::I8(v as i8),
                    ScalarKind::I16 => Self::I16(v as i16),
                    ScalarKind::I32 => Self::I32(v as i32),
                    ScalarKind::I64 => Self::I64(v as i64),
                    ScalarKind::U8 => Self::U8(v as u8),
                    ScalarKind::U16 => Self::U16(v as u16),
                    ScalarKind::U32 => Self::U32(v as u32),
                    _ => Self::U64(v as u64),
                });
            }
            let exact: u128 = if kind == ScalarKind::F32 {
                1 << 24
            } else {
                1 << 53
            };
            // Within +-2^53 the value is an `i64` (and converts without the i128 routines).
            return match kind {
                _ if v.unsigned_abs() > exact => None,
                ScalarKind::F32 => Some(Self::F32(v as i64 as f32)),
                #[cfg(feature = "f64")]
                ScalarKind::F64 => Some(Self::F64(v as i64 as f64)),
                _ => None,
            };
        }
        match (self, kind) {
            #[cfg(feature = "f64")]
            (Self::F32(v), ScalarKind::F64) => Some(Self::F64(v.into())),
            (Self::F64(v), ScalarKind::F32) => {
                let narrowed = f64_to_f32(v);
                (narrowed.is_finite() || !v.is_finite()).then_some(Self::F32(narrowed))
            }
            _ => None,
        }
    }

    /// `self <= other` for two values of the same kind (`false` for NaN or different kinds).
    pub fn le(self, other: Self) -> bool {
        match (self, other) {
            (Self::Bool(a), Self::Bool(b)) => a <= b,
            (Self::F32(a), Self::F32(b)) => a <= b,
            #[cfg(feature = "f64")]
            (Self::F64(a), Self::F64(b)) => a <= b,
            (a, b) if a.kind() == b.kind() => a.int() <= b.int(),
            _ => false,
        }
    }

    /// Read a value of `kind` from `src`.
    ///
    /// # Safety
    /// `src` points to an initialised value of `kind`'s Rust type (alignment not required).
    pub unsafe fn read(kind: ScalarKind, src: *const u8) -> Self {
        macro_rules! read {
            ($($kind:ident: $ty:ty),* $(,)?) => {
                // SAFETY: the caller's contract.
                match kind { $(ScalarKind::$kind => Self::$kind(unsafe { src.cast::<$ty>().read_unaligned() })),* }
            };
        }
        for_each_scalar!(read)
    }

    /// Write the value as its Rust type to `dst`.
    ///
    /// # Safety
    /// `dst` is valid for a write of the value's size (alignment not required).
    pub unsafe fn write(self, dst: *mut u8) {
        macro_rules! write {
            ($($kind:ident: $ty:ty),* $(,)?) => {
                // SAFETY: the caller's contract.
                match self { $(Self::$kind(v) => unsafe { dst.cast::<$ty>().write_unaligned(v) }),* }
            };
        }
        for_each_scalar!(write)
    }
}

/// `v as f32` (round to nearest, ties to even; out of range to infinity) in integer arithmetic, so
/// an `f32` parameter accepts an `f64` value without linking `f64` soft-float.
fn f64_to_f32(v: f64) -> f32 {
    /// `m >> shift`, rounded to nearest, ties to even.
    fn round_shift(m: u64, shift: u32) -> u64 {
        if shift >= 64 {
            return 0;
        }
        let (q, rem, half) = (m >> shift, m & ((1 << shift) - 1), 1 << (shift - 1));
        if rem > half || (rem == half && q & 1 == 1) {
            q + 1
        } else {
            q
        }
    }
    let bits = v.to_bits();
    let sign = (bits >> 32) as u32 & 0x8000_0000;
    let exp = ((bits >> 52) & 0x7ff) as i32;
    let man = bits & ((1 << 52) - 1);
    let out = if exp == 0x7ff {
        // Infinity, or a quiet NaN keeping the payload's top bits.
        0x7f80_0000
            | if man != 0 {
                0x0040_0000 | (man >> 29) as u32
            } else {
                0
            }
    } else if exp == 0 {
        // `f64` subnormals are below half the smallest `f32` subnormal.
        0
    } else {
        let e = exp - 1023 + 127;
        let m = man | (1 << 52);
        if e >= 0xff {
            0x7f80_0000
        } else if e <= 0 {
            // An `f32` subnormal; rounding up may reach the smallest normal, encoded alike.
            round_shift(m, (30 - e) as u32) as u32
        } else {
            // A carry out of the mantissa moves into the exponent, up to infinity.
            ((e as u32) << 23) + round_shift(m, 29) as u32 - (1 << 23)
        }
    };
    f32::from_bits(sign | out)
}

/// Rust scalar types usable as parameter values.
pub trait ScalarType: Copy {
    const KIND: ScalarKind;
    fn into_scalar(self) -> Scalar;
    /// The value when `scalar` has exactly this type.
    fn from_scalar(scalar: Scalar) -> Option<Self>;
}

macro_rules! scalar_types {
    ($($kind:ident: $ty:ty),* $(,)?) => {$(
        impl ScalarType for $ty {
            const KIND: ScalarKind = ScalarKind::$kind;
            fn into_scalar(self) -> Scalar {
                Scalar::$kind(self)
            }
            fn from_scalar(scalar: Scalar) -> Option<Self> {
                match scalar {
                    Scalar::$kind(v) => Some(v),
                    _ => None,
                }
            }
        }
        impl From<$ty> for Scalar {
            fn from(value: $ty) -> Self {
                Scalar::$kind(value)
            }
        }
    )*};
}
scalar_types! {
    Bool: bool, I8: i8, I16: i16, I32: i32, I64: i64, U8: u8, U16: u16, U32: u32, U64: u64,
    F32: f32,
}
// Without `f64`, a compiled graph cannot have an `f64` parameter: it fails to build here.
#[cfg(feature = "f64")]
scalar_types! { F64: f64 }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coerce_follows_the_constant_rules() {
        assert_eq!(
            Scalar::I64(255).coerce(ScalarKind::U8),
            Some(Scalar::U8(255))
        );
        assert_eq!(Scalar::I64(256).coerce(ScalarKind::U8), None);
        assert_eq!(Scalar::I64(-1).coerce(ScalarKind::U64), None);
        assert_eq!(
            Scalar::I64(1 << 24).coerce(ScalarKind::F32),
            Some(Scalar::F32(16777216.0))
        );
        assert_eq!(Scalar::I64((1 << 24) + 1).coerce(ScalarKind::F32), None);
        assert_eq!(Scalar::F64(0.5).coerce(ScalarKind::I32), None);
        assert_eq!(Scalar::F64(1e300).coerce(ScalarKind::F32), None);
        let to_f64 = cfg!(feature = "f64").then_some(Scalar::F64(0.25));
        assert_eq!(Scalar::F32(0.25).coerce(ScalarKind::F64), to_f64);
        assert_eq!(
            Scalar::I32(1).coerce(ScalarKind::F64).is_some(),
            cfg!(feature = "f64")
        );
        assert_eq!(
            Scalar::F64(0.25).coerce(ScalarKind::F32),
            Some(Scalar::F32(0.25))
        );
        assert_eq!(Scalar::Bool(true).coerce(ScalarKind::U8), None);
        assert!(Scalar::U8(3).le(Scalar::U8(3)) && !Scalar::F32(f32::NAN).le(Scalar::F32(1.0)));
    }

    #[test]
    fn f64_to_f32_matches_the_cast() {
        let mut state = 0x9e37_79b9_7f4a_7c15_u64;
        let edges = [
            0.0,
            -0.0,
            1.0,
            f64::MIN_POSITIVE,
            f64::MAX,
            f64::INFINITY,
            f64::NAN,
            f32::MAX as f64,
            f32::MIN_POSITIVE as f64,
            1e-45,
            7e-46,
            1.4e-45,
            3.4028235677973366e38,
            3.4028235e38,
            1.0000000596046448,
            1.0000001788139343,
        ];
        let random = (0..200_000).map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            // Mostly exponents near the f32 range, where rounding and subnormals happen.
            let exp = 1023 - 160 + (state >> 52) % 320;
            f64::from_bits((state & 0x800f_ffff_ffff_ffff) | (exp << 52))
        });
        for v in edges.into_iter().flat_map(|v| [v, -v]).chain(random) {
            let (ours, cast) = (f64_to_f32(v), v as f32);
            assert!(
                ours.to_bits() == cast.to_bits() || (ours.is_nan() && cast.is_nan()),
                "{v:e}: {ours:e} vs {cast:e}"
            );
        }
    }

    #[test]
    fn widening_pairs_coerce() {
        for from in ScalarKind::ALL {
            let converts = |to: &ScalarKind| cfg!(feature = "f64") || *to != ScalarKind::F64;
            for to in ScalarKind::ALL
                .into_iter()
                .filter(|to| from.widens_to(*to) && converts(to))
            {
                let (min, max) = from.bounds();
                if !from.is_float() {
                    assert!(
                        min.coerce(to).is_some() && max.coerce(to).is_some(),
                        "{from:?}->{to:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn raw_round_trip() {
        let mut buf = [0u8; 8];
        for value in [
            Scalar::Bool(true),
            Scalar::I16(-3),
            Scalar::F64(1.5),
            Scalar::U64(9),
        ] {
            // SAFETY: an 8-byte buffer holds every scalar.
            unsafe {
                value.write(buf.as_mut_ptr());
                assert_eq!(Scalar::read(value.kind(), buf.as_ptr()), value);
            }
        }
    }
}
