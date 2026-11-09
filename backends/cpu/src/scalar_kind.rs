//! The kinds of value a constant or a parameter can have.

/// A value a constant or parameter can hold. The numbers are stored in plans.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum ScalarKind {
    Bool = 0,
    I8 = 1,
    I16 = 2,
    I32 = 3,
    I64 = 4,
    U8 = 5,
    U16 = 6,
    U32 = 7,
    U64 = 8,
    F32 = 9,
    F64 = 10,
}

impl ScalarKind {
    pub const ALL: [ScalarKind; 11] = [
        ScalarKind::Bool,
        ScalarKind::I8,
        ScalarKind::I16,
        ScalarKind::I32,
        ScalarKind::I64,
        ScalarKind::U8,
        ScalarKind::U16,
        ScalarKind::U32,
        ScalarKind::U64,
        ScalarKind::F32,
        ScalarKind::F64,
    ];

    /// The kind stored as tag `tag`.
    pub const fn from_u8(tag: u8) -> Option<ScalarKind> {
        if (tag as usize) < Self::ALL.len() {
            Some(Self::ALL[tag as usize])
        } else {
            None
        }
    }

    pub const fn is_float(self) -> bool {
        matches!(self, ScalarKind::F32 | ScalarKind::F64)
    }

    /// The inclusive range of an integer kind.
    pub const fn int_range(self) -> Option<(i128, i128)> {
        Some(match self {
            ScalarKind::I8 => (i8::MIN as i128, i8::MAX as i128),
            ScalarKind::I16 => (i16::MIN as i128, i16::MAX as i128),
            ScalarKind::I32 => (i32::MIN as i128, i32::MAX as i128),
            ScalarKind::I64 => (i64::MIN as i128, i64::MAX as i128),
            ScalarKind::U8 => (0, u8::MAX as i128),
            ScalarKind::U16 => (0, u16::MAX as i128),
            ScalarKind::U32 => (0, u32::MAX as i128),
            ScalarKind::U64 => (0, u64::MAX as i128),
            _ => return None,
        })
    }

    pub fn from_word(word: u32) -> Option<ScalarKind> {
        Self::ALL.get(word as usize).copied()
    }

    pub const fn key(self) -> &'static str {
        match self {
            ScalarKind::Bool => "bool",
            ScalarKind::I8 => "i8",
            ScalarKind::I16 => "i16",
            ScalarKind::I32 => "i32",
            ScalarKind::I64 => "i64",
            ScalarKind::U8 => "u8",
            ScalarKind::U16 => "u16",
            ScalarKind::U32 => "u32",
            ScalarKind::U64 => "u64",
            ScalarKind::F32 => "f32",
            ScalarKind::F64 => "f64",
        }
    }

    pub const fn size(self) -> u16 {
        match self {
            ScalarKind::Bool | ScalarKind::I8 | ScalarKind::U8 => 1,
            ScalarKind::I16 | ScalarKind::U16 => 2,
            ScalarKind::I32 | ScalarKind::U32 | ScalarKind::F32 => 4,
            ScalarKind::I64 | ScalarKind::U64 | ScalarKind::F64 => 8,
        }
    }

    pub fn from_key(key: &str) -> Option<ScalarKind> {
        Self::ALL.into_iter().find(|kind| kind.key() == key)
    }

    /// Whether `bits` is a value of this kind. NaN isn't in any range, so it isn't one for a
    /// parameter either.
    pub fn in_order(self, low: u64, value: u64, high: u64) -> bool {
        match (self.rank(low), self.rank(value), self.rank(high)) {
            (Some(low), Some(value), Some(high)) => low <= value && value <= high,
            _ => false,
        }
    }

    /// A number that sorts as `bits` does as a value of this kind. `None` for NaN.
    fn rank(self, bits: u64) -> Option<u64> {
        match self {
            ScalarKind::F32 => float_rank(bits & 0xffff_ffff, 1 << 31, 0x7f80_0000),
            ScalarKind::F64 => float_rank(bits, 1 << 63, 0x7ff0_0000_0000_0000),
            ScalarKind::I8 | ScalarKind::I16 | ScalarKind::I32 | ScalarKind::I64 => {
                let shift = 64 - u32::from(self.size()) * 8;
                Some((((bits << shift) as i64) >> shift) as u64 ^ (1 << 63))
            }
            _ => Some(bits),
        }
    }
}

/// With no floating-point arithmetic, so a chip needs no support for it. The two zeros are equal.
fn float_rank(bits: u64, sign: u64, infinity: u64) -> Option<u64> {
    let size = bits & !sign;
    if size > infinity {
        None
    } else if bits & sign != 0 {
        Some(sign - size)
    } else {
        Some(sign + size)
    }
}

/// `a <= b`, with no floating-point arithmetic, so a chip needs no `f64` support for it.
/// `false` if either is NaN. The two zeros are equal.
pub fn f64_le(a: f64, b: f64) -> bool {
    ScalarKind::F64.in_order(a.to_bits(), a.to_bits(), b.to_bits())
}
