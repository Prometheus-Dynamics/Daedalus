//! Typed scalar values: constants that can change while a graph runs.

use crate::scalar_kind::{ScalarKind, f64_le};

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

impl Scalar {
    pub const fn kind(&self) -> ScalarKind {
        match self {
            Scalar::Bool(_) => ScalarKind::Bool,
            Scalar::I8(_) => ScalarKind::I8,
            Scalar::I16(_) => ScalarKind::I16,
            Scalar::I32(_) => ScalarKind::I32,
            Scalar::I64(_) => ScalarKind::I64,
            Scalar::U8(_) => ScalarKind::U8,
            Scalar::U16(_) => ScalarKind::U16,
            Scalar::U32(_) => ScalarKind::U32,
            Scalar::U64(_) => ScalarKind::U64,
            Scalar::F32(_) => ScalarKind::F32,
            Scalar::F64(_) => ScalarKind::F64,
        }
    }

    fn int(self) -> Option<i128> {
        Some(match self {
            Scalar::I8(v) => v.into(),
            Scalar::I16(v) => v.into(),
            Scalar::I32(v) => v.into(),
            Scalar::I64(v) => v.into(),
            Scalar::U8(v) => v.into(),
            Scalar::U16(v) => v.into(),
            Scalar::U32(v) => v.into(),
            Scalar::U64(v) => v.into(),
            _ => return None,
        })
    }

    /// The lowest and highest values of `kind`. For a float, its infinities.
    pub const fn bounds(kind: ScalarKind) -> (Scalar, Scalar) {
        match kind {
            ScalarKind::Bool => (Scalar::Bool(false), Scalar::Bool(true)),
            ScalarKind::I8 => (Scalar::I8(i8::MIN), Scalar::I8(i8::MAX)),
            ScalarKind::I16 => (Scalar::I16(i16::MIN), Scalar::I16(i16::MAX)),
            ScalarKind::I32 => (Scalar::I32(i32::MIN), Scalar::I32(i32::MAX)),
            ScalarKind::I64 => (Scalar::I64(i64::MIN), Scalar::I64(i64::MAX)),
            ScalarKind::U8 => (Scalar::U8(u8::MIN), Scalar::U8(u8::MAX)),
            ScalarKind::U16 => (Scalar::U16(u16::MIN), Scalar::U16(u16::MAX)),
            ScalarKind::U32 => (Scalar::U32(u32::MIN), Scalar::U32(u32::MAX)),
            ScalarKind::U64 => (Scalar::U64(u64::MIN), Scalar::U64(u64::MAX)),
            ScalarKind::F32 => (Scalar::F32(f32::NEG_INFINITY), Scalar::F32(f32::INFINITY)),
            ScalarKind::F64 => (Scalar::F64(f64::NEG_INFINITY), Scalar::F64(f64::INFINITY)),
        }
    }

    /// Converts to `kind` by the planner's rules for constants: an integer to any integer type
    /// it fits and to a float that holds it exactly, a float to a float (a finite value beyond
    /// `f32::MAX` doesn't fit), `bool` to `bool` only.
    pub fn coerce(self, kind: ScalarKind) -> Option<Scalar> {
        if self.kind() == kind {
            return Some(self);
        }
        if let Some(value) = self.int() {
            return int_to(value, kind);
        }
        match (self, kind) {
            #[cfg(feature = "f64")]
            (Scalar::F32(value), ScalarKind::F64) => Some(Scalar::F64(value.into())),
            (Scalar::F64(value), ScalarKind::F32) => f64_to_f32(value).map(Scalar::F32),
            _ => None,
        }
    }

    /// `self <= other` for two values of one kind. `false` for NaN or different kinds.
    pub fn le(self, other: Scalar) -> bool {
        match (self, other) {
            (Scalar::Bool(a), Scalar::Bool(b)) => a <= b,
            (Scalar::F32(a), Scalar::F32(b)) => a <= b,
            (Scalar::F64(a), Scalar::F64(b)) => f64_le(a, b),
            (a, b) if a.kind() == b.kind() => a.int() <= b.int(),
            _ => false,
        }
    }

    /// The value as the bits a stored plan keeps.
    pub const fn bits(self) -> u64 {
        match self {
            Scalar::Bool(v) => v as u64,
            Scalar::I8(v) => v as u8 as u64,
            Scalar::I16(v) => v as u16 as u64,
            Scalar::I32(v) => v as u32 as u64,
            Scalar::I64(v) => v as u64,
            Scalar::U8(v) => v as u64,
            Scalar::U16(v) => v as u64,
            Scalar::U32(v) => v as u64,
            Scalar::U64(v) => v,
            Scalar::F32(v) => v.to_bits() as u64,
            Scalar::F64(v) => v.to_bits(),
        }
    }

    /// A value of `kind` from the bits [`Scalar::bits`] gave.
    pub const fn from_bits(kind: ScalarKind, bits: u64) -> Scalar {
        match kind {
            ScalarKind::Bool => Scalar::Bool(bits != 0),
            ScalarKind::I8 => Scalar::I8(bits as i8),
            ScalarKind::I16 => Scalar::I16(bits as i16),
            ScalarKind::I32 => Scalar::I32(bits as i32),
            ScalarKind::I64 => Scalar::I64(bits as i64),
            ScalarKind::U8 => Scalar::U8(bits as u8),
            ScalarKind::U16 => Scalar::U16(bits as u16),
            ScalarKind::U32 => Scalar::U32(bits as u32),
            ScalarKind::U64 => Scalar::U64(bits),
            ScalarKind::F32 => Scalar::F32(f32::from_bits(bits as u32)),
            ScalarKind::F64 => Scalar::F64(f64::from_bits(bits)),
        }
    }
}

fn int_to(value: i128, kind: ScalarKind) -> Option<Scalar> {
    if let Some((low, high)) = kind.int_range() {
        if value < low || value > high {
            return None;
        }
        return Some(match kind {
            ScalarKind::I8 => Scalar::I8(value as i8),
            ScalarKind::I16 => Scalar::I16(value as i16),
            ScalarKind::I32 => Scalar::I32(value as i32),
            ScalarKind::I64 => Scalar::I64(value as i64),
            ScalarKind::U8 => Scalar::U8(value as u8),
            ScalarKind::U16 => Scalar::U16(value as u16),
            ScalarKind::U32 => Scalar::U32(value as u32),
            _ => Scalar::U64(value as u64),
        });
    }
    match kind {
        ScalarKind::F32 if value.unsigned_abs() <= 1 << 24 => {
            Some(Scalar::F32(value as i32 as f32))
        }
        #[cfg(feature = "f64")]
        ScalarKind::F64 if value.unsigned_abs() <= 1 << 53 => {
            Some(Scalar::F64(value as i64 as f64))
        }
        _ => None,
    }
}

/// The nearest `f32`, rounding ties to even, in integer arithmetic so no `f64` support is linked.
/// `None` if a finite value is beyond `f32::MAX`.
fn f64_to_f32(value: f64) -> Option<f32> {
    /// `f32::MAX` as an `f64`.
    const LIMIT: u64 = 0x47ef_ffff_e000_0000;
    const INFINITY: u64 = 0x7ff0_0000_0000_0000;
    let bits = value.to_bits();
    let sign = ((bits >> 63) as u32) << 31;
    let size = bits & !(1 << 63);
    if size >= INFINITY {
        // Infinity stays infinity and NaN stays NaN.
        let quiet = if size > INFINITY { 0x0040_0000 } else { 0 };
        return Some(f32::from_bits(sign | 0x7f80_0000 | quiet));
    }
    if size > LIMIT {
        return None;
    }
    let exponent = (size >> 52) as i32 - 1023 + 127;
    if (size >> 52) == 0 {
        // Below the smallest `f32`, even with subnormals.
        return Some(f32::from_bits(sign));
    }
    let mantissa = (1_u64 << 52) | (size & ((1 << 52) - 1));
    let shift = if exponent >= 1 {
        29
    } else {
        29 + (1 - exponent) as u32
    };
    if shift >= 64 {
        return Some(f32::from_bits(sign));
    }
    let kept = mantissa >> shift;
    let rest = mantissa & ((1 << shift) - 1);
    let half = 1_u64 << (shift - 1);
    let rounded = kept + u64::from(rest > half || (rest == half && kept & 1 == 1));
    let size = if exponent >= 1 {
        ((exponent as u32 - 1) << 23) + rounded as u32
    } else {
        rounded as u32
    };
    Some(f32::from_bits(sign | size))
}

/// A Rust type a parameter can have.
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
                    Scalar::$kind(value) => Some(value),
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
    F32: f32, F64: f64,
}
