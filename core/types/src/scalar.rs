//! The types every graph has.

use crate::TypeKey;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum Scalar {
    Unit,
    Bool,
    I8,
    I16,
    I32,
    /// `i64`.
    Int,
    ISize,
    U8,
    U16,
    U32,
    U64,
    USize,
    F32,
    /// `f64`.
    Float,
    /// By name only: the bytes of a value live in a memory space.
    String,
    /// By name only: the bytes of a value live in a memory space.
    Bytes,
}

impl Scalar {
    /// The Rust type this scalar stands for.
    pub const fn rust_name(self) -> &'static str {
        match self {
            Scalar::Unit => "()",
            Scalar::Bool => "bool",
            Scalar::I8 => "i8",
            Scalar::I16 => "i16",
            Scalar::I32 => "i32",
            Scalar::Int => "i64",
            Scalar::ISize => "isize",
            Scalar::U8 => "u8",
            Scalar::U16 => "u16",
            Scalar::U32 => "u32",
            Scalar::U64 => "u64",
            Scalar::USize => "usize",
            Scalar::F32 => "f32",
            Scalar::Float => "f64",
            Scalar::String => "String",
            Scalar::Bytes => "Vec<u8>",
        }
    }

    pub const fn key(self) -> TypeKey {
        TypeKey::new(match self {
            Scalar::Unit => "unit",
            Scalar::Bool => "bool",
            Scalar::I8 => "i8",
            Scalar::I16 => "i16",
            Scalar::I32 => "i32",
            Scalar::Int => "i64",
            Scalar::ISize => "isize",
            Scalar::U8 => "u8",
            Scalar::U16 => "u16",
            Scalar::U32 => "u32",
            Scalar::U64 => "u64",
            Scalar::USize => "usize",
            Scalar::F32 => "f32",
            Scalar::Float => "f64",
            Scalar::String => "string",
            Scalar::Bytes => "bytes",
        })
    }
}
