//! Constants known while planning.

use crate::{Code, Error};

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Literal {
    Unit,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(&'static str),
    Bytes(&'static [u8]),
    List(&'static [Literal]),
    Struct(&'static [(&'static str, Literal)]),
    Enum(&'static str, Option<&'static Literal>),
}

pub trait FromLiteral: Sized {
    fn from_literal(value: &Literal) -> Result<Self, Error>;
}

const fn wrong_kind(expected: &'static str) -> Error {
    Error::new(Code::InvalidLiteral, expected)
}

macro_rules! integer {
    ($($ty:ty),*) => {$(
        impl FromLiteral for $ty {
            fn from_literal(value: &Literal) -> Result<Self, Error> {
                let Literal::Int(int) = value else {
                    return Err(wrong_kind(concat!("expected an integer for ", stringify!($ty))));
                };
                <$ty>::try_from(*int).map_err(|_| {
                    Error::new(
                        Code::OutOfRange,
                        concat!("constant does not fit ", stringify!($ty)),
                    )
                })
            }
        }
    )*};
}

integer!(i8, i16, i32, i64, isize, u8, u16, u32, u64, usize);

impl FromLiteral for f64 {
    fn from_literal(value: &Literal) -> Result<Self, Error> {
        match value {
            Literal::Float(float) => Ok(*float),
            Literal::Int(int) if (*int as f64) as i64 == *int => Ok(*int as f64),
            Literal::Int(_) => Err(Error::new(Code::OutOfRange, "integer is not exact in f64")),
            _ => Err(wrong_kind("expected a number for f64")),
        }
    }
}

impl FromLiteral for f32 {
    fn from_literal(value: &Literal) -> Result<Self, Error> {
        match value {
            Literal::Float(float) => Ok(*float as f32),
            Literal::Int(int) if (*int as f32) as i64 == *int => Ok(*int as f32),
            Literal::Int(_) => Err(Error::new(Code::OutOfRange, "integer is not exact in f32")),
            _ => Err(wrong_kind("expected a number for f32")),
        }
    }
}

#[cfg(feature = "alloc")]
impl FromLiteral for alloc::string::String {
    fn from_literal(value: &Literal) -> Result<Self, Error> {
        match value {
            Literal::Str(value) => Ok((*value).into()),
            _ => Err(wrong_kind("str")),
        }
    }
}

impl FromLiteral for bool {
    fn from_literal(value: &Literal) -> Result<Self, Error> {
        match value {
            Literal::Bool(flag) => Ok(*flag),
            _ => Err(wrong_kind("expected a bool")),
        }
    }
}

impl FromLiteral for &'static str {
    fn from_literal(value: &Literal) -> Result<Self, Error> {
        match value {
            Literal::Str(text) => Ok(text),
            _ => Err(wrong_kind("expected a string")),
        }
    }
}

impl FromLiteral for &'static [u8] {
    fn from_literal(value: &Literal) -> Result<Self, Error> {
        match value {
            Literal::Bytes(bytes) => Ok(bytes),
            _ => Err(wrong_kind("expected bytes")),
        }
    }
}
