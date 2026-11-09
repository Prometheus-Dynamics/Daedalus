//! What a port declaration reads from a Rust type, in const context.

use crate::{Scalar, TypeExpr, TypeKey};

pub trait Typed {
    const KEY: TypeKey;
    const EXPR: TypeExpr;
}

macro_rules! scalar {
    ($($ty:ty => $scalar:ident),* $(,)?) => {$(
        impl Typed for $ty {
            const KEY: TypeKey = Scalar::$scalar.key();
            const EXPR: TypeExpr = TypeExpr::Scalar(Scalar::$scalar);
        }
    )*};
}

scalar!(
    () => Unit, bool => Bool, i8 => I8, i16 => I16, i32 => I32, i64 => Int, isize => ISize,
    u8 => U8, u16 => U16, u32 => U32, u64 => U64, usize => USize, f32 => F32, f64 => Float,
    &'static str => String, &'static [u8] => Bytes,
);

#[cfg(feature = "alloc")]
scalar!(alloc::string::String => String, alloc::vec::Vec<u8> => Bytes);
