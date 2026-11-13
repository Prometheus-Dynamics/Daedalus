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

/// `Unit` is `None`, and anything else is the value.
impl<T: FromLiteral> FromLiteral for Option<T> {
    fn from_literal(value: &Literal) -> Result<Self, Error> {
        match value {
            Literal::Unit => Ok(None),
            value => T::from_literal(value).map(Some),
        }
    }
}

/// A list, each item decoded in turn. The first item that doesn't fit is the error.
#[cfg(feature = "alloc")]
impl<T: FromLiteral> FromLiteral for alloc::vec::Vec<T> {
    fn from_literal(value: &Literal) -> Result<Self, Error> {
        let Literal::List(items) = value else {
            return Err(wrong_kind("expected a list"));
        };
        items.iter().map(T::from_literal).collect()
    }
}

/// A list of exactly `N` items.
impl<T: FromLiteral, const N: usize> FromLiteral for [T; N] {
    fn from_literal(value: &Literal) -> Result<Self, Error> {
        let Literal::List(items) = value else {
            return Err(wrong_kind("expected a list"));
        };
        if items.len() != N {
            return Err(Error::new(
                Code::OutOfRange,
                "the list is not the array's length",
            ));
        }
        let mut refused = None;
        let decoded: [Option<T>; N] =
            core::array::from_fn(|at| match items.get(at).map(T::from_literal) {
                Some(Ok(item)) => Some(item),
                Some(Err(error)) => {
                    refused.get_or_insert(error);
                    None
                }
                None => None,
            });
        if let Some(error) = refused {
            return Err(error);
        }
        Ok(decoded.map(|item| match item {
            Some(item) => item,
            None => unreachable!("every item decoded, or the error above was returned"),
        }))
    }
}

/// The field `name` of a struct constant, for `#[derive(FromLiteral)]`.
#[doc(hidden)]
pub fn literal_field<T: FromLiteral>(
    fields: &[(&'static str, Literal)],
    name: &str,
) -> Result<T, Error> {
    match fields.iter().find(|(have, _)| *have == name) {
        Some((_, value)) => T::from_literal(value),
        None => Err(Error::new(
            Code::InvalidLiteral,
            "the struct constant is missing a field",
        )),
    }
}
