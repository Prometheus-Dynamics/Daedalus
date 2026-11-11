//! What a port declaration reads from a Rust type, in const context.

use crate::{Scalar, TypeExpr, TypeKey};

pub trait Typed {
    /// The key a named type is compared by. A type made of others ([`Typed::STRUCTURAL`]) has
    /// no name of its own, and this is a placeholder for it.
    const KEY: TypeKey;
    /// What the type is made of.
    const EXPR: TypeExpr;
    /// The type has no key of its own and is compared by [`Typed::EXPR`]: a list, an optional
    /// value, a tuple or a map. A port of such a type is declared with the expression.
    const STRUCTURAL: bool = false;
    /// The type is `u8`. A list of bytes is the bytes scalar and not a list.
    #[doc(hidden)]
    const BYTE: bool = false;
}

/// The key of every structural type. It is never compared: a structural type is compared by
/// its expression.
pub const STRUCTURAL_KEY: TypeKey = TypeKey::new("structural");

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
    u16 => U16, u32 => U32, u64 => U64, usize => USize, f32 => F32, f64 => Float,
    &'static str => String, &'static [u8] => Bytes,
);

impl Typed for u8 {
    const KEY: TypeKey = Scalar::U8.key();
    const EXPR: TypeExpr = TypeExpr::Scalar(Scalar::U8);
    const BYTE: bool = true;
}

#[cfg(feature = "alloc")]
scalar!(alloc::string::String => String);

/// A list. A `Vec<u8>` is the bytes scalar, with its key, as it always was. Any other `Vec<T>`
/// is a list of `T`, compared by what it is a list of.
#[cfg(feature = "alloc")]
impl<T: Typed> Typed for alloc::vec::Vec<T> {
    const KEY: TypeKey = if T::BYTE {
        Scalar::Bytes.key()
    } else {
        STRUCTURAL_KEY
    };
    const EXPR: TypeExpr = if T::BYTE {
        TypeExpr::Scalar(Scalar::Bytes)
    } else {
        TypeExpr::List(&T::EXPR)
    };
    const STRUCTURAL: bool = !T::BYTE;
}

/// A value that may be absent, as a value. A node parameter written `Option<T>` is something
/// else: an optional port of type `T`.
impl<T: Typed> Typed for Option<T> {
    const KEY: TypeKey = STRUCTURAL_KEY;
    const EXPR: TypeExpr = TypeExpr::Optional(&T::EXPR);
    const STRUCTURAL: bool = true;
}

/// A map from `K` to `V`, with its keys in order.
#[cfg(feature = "alloc")]
impl<K: Typed, V: Typed> Typed for alloc::collections::BTreeMap<K, V> {
    const KEY: TypeKey = STRUCTURAL_KEY;
    const EXPR: TypeExpr = TypeExpr::Map(&K::EXPR, &V::EXPR);
    const STRUCTURAL: bool = true;
}

macro_rules! tuples {
    ($(($($name:ident),+))*) => {$(
        impl<$($name: Typed),+> Typed for ($($name,)+) {
            const KEY: TypeKey = STRUCTURAL_KEY;
            const EXPR: TypeExpr = TypeExpr::Tuple(&[$($name::EXPR),+]);
            const STRUCTURAL: bool = true;
        }
    )*};
}

tuples!((A, B)(A, B, C)(A, B, C, D));

/// A handle to a value is of the value's type: a node that takes or returns `Arc<T>` has a
/// port of type `T`, and what moves is the handle.
#[cfg(feature = "alloc")]
impl<T: Typed> Typed for alloc::sync::Arc<T> {
    const KEY: TypeKey = T::KEY;
    const EXPR: TypeExpr = T::EXPR;
}
