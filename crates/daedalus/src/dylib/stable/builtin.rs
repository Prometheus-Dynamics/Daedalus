//! Builtin scalars (`bool`, integers, floats, `()`, `String`, `Vec<u8>`) crossing the stable
//! boundary without a [`Value`] detour: encoded from a payload's `dyn Any` by borrowing, and
//! decoded into a typed payload of the port's Rust type.

use std::any::Any;
use std::sync::Arc;

use super::value::{StableValue, tag, to_value};
use crate::data::model::{TypeExpr, Value, ValueType};
use crate::data::typing::coerce_builtin_const_value;
use crate::registry::typeexpr_transport_key;
use crate::transport::{Payload, TypeKey};

/// Encode a builtin Rust value, borrowing strings and bytes from `any`.
pub fn encode(any: &dyn Any) -> Option<StableValue> {
    macro_rules! scalars {
        ($($ty:ty => $make:expr),* $(,)?) => {
            $(if let Some(value) = any.downcast_ref::<$ty>() {
                #[allow(clippy::redundant_closure_call)]
                return Some(($make)(*value));
            })*
        };
    }
    scalars! {
        i64 => StableValue::int,
        f64 => StableValue::float,
        bool => StableValue::bool,
        i32 => |v: i32| StableValue::int(v.into()),
        u32 => |v: u32| StableValue::int(v.into()),
        f32 => |v: f32| StableValue::float(v.into()),
        u64 => StableValue::uint,
        usize => |v: usize| StableValue::uint(v as u64),
        isize => |v: isize| StableValue::int(v as i64),
        i8 => |v: i8| StableValue::int(v.into()),
        i16 => |v: i16| StableValue::int(v.into()),
        u8 => |v: u8| StableValue::int(v.into()),
        u16 => |v: u16| StableValue::int(v.into()),
        () => |()| StableValue::UNIT,
    }
    if let Some(text) = any.downcast_ref::<String>() {
        return Some(StableValue::string(text));
    }
    if let Some(bytes) = any.downcast_ref::<Vec<u8>>() {
        return Some(StableValue::bytes(bytes));
    }
    any.downcast_ref::<Arc<[u8]>>()
        .map(|bytes| StableValue::bytes(bytes))
}

/// The builtin scalar a port stands for: its type (`Optional` peeled) or else its key.
pub fn scalar_of(ty: &TypeExpr, key: &TypeKey) -> Option<ValueType> {
    match ty {
        TypeExpr::Scalar(scalar) => Some(*scalar),
        TypeExpr::Optional(inner) => scalar_of(inner, key),
        _ => crate::data::typing::BUILTIN_VALUE_TYPES
            .iter()
            .copied()
            .find(|scalar| typeexpr_transport_key(&TypeExpr::Scalar(*scalar)) == *key),
    }
}

/// A payload holding the Rust type of `scalar` under `key`.
///
/// # Safety
/// Every pointer in `value` must be valid for the call.
pub unsafe fn decode(
    scalar: ValueType,
    key: TypeKey,
    value: &StableValue,
) -> Result<Payload, String> {
    // Safety (slices): forwarded from the caller.
    match (scalar, value.tag) {
        (ValueType::String, tag::STRING) => {
            return Ok(Payload::owned(key, unsafe { value.as_str() }?.to_owned()));
        }
        (ValueType::Bytes, tag::BYTES) => {
            return Ok(Payload::owned(key, unsafe { value.as_bytes() }.to_vec()));
        }
        (ValueType::U64, tag::UINT) => return Ok(Payload::owned(key, value.scalar)),
        (ValueType::USize, tag::UINT) => {
            let value = usize::try_from(value.scalar).map_err(|err| err.to_string())?;
            return Ok(Payload::owned(key, value));
        }
        _ => {}
    }
    // Safety: forwarded from the caller.
    let value = unsafe { to_value(value) }?;
    fn typed<T: Send + Sync + 'static>(
        key: TypeKey,
        scalar: ValueType,
        value: &Value,
    ) -> Result<Payload, String> {
        coerce_builtin_const_value::<T>(value)
            .map(|typed| Payload::owned(key, typed))
            .ok_or_else(|| format!("expected {}, found {value:?}", scalar.rust_name()))
    }
    match scalar {
        ValueType::Unit => typed::<()>(key, scalar, &value),
        ValueType::Bool => typed::<bool>(key, scalar, &value),
        ValueType::I8 => typed::<i8>(key, scalar, &value),
        ValueType::I16 => typed::<i16>(key, scalar, &value),
        ValueType::I32 => typed::<i32>(key, scalar, &value),
        ValueType::Int => typed::<i64>(key, scalar, &value),
        ValueType::ISize => typed::<isize>(key, scalar, &value),
        ValueType::U8 => typed::<u8>(key, scalar, &value),
        ValueType::U16 => typed::<u16>(key, scalar, &value),
        ValueType::U32 => typed::<u32>(key, scalar, &value),
        ValueType::U64 => typed::<u64>(key, scalar, &value),
        ValueType::USize => typed::<usize>(key, scalar, &value),
        ValueType::F32 => typed::<f32>(key, scalar, &value),
        ValueType::Float => typed::<f64>(key, scalar, &value),
        ValueType::String => typed::<String>(key, scalar, &value),
        ValueType::Bytes => typed::<Vec<u8>>(key, scalar, &value),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builtins_cross_without_values() {
        let text = String::from("hi");
        let encoded = encode(&text).unwrap();
        assert_eq!(
            encoded.ptr.cast::<u8>(),
            text.as_ptr(),
            "borrowed, not copied"
        );
        let payload = unsafe { decode(ValueType::String, "string".into(), &encoded) }.unwrap();
        assert_eq!(payload.get_ref::<String>().map(String::as_str), Some("hi"));

        let payload =
            unsafe { decode(ValueType::I32, "i32".into(), &encode(&7_u8).unwrap()) }.unwrap();
        assert_eq!(payload.get_ref::<i32>(), Some(&7));
        let big = encode(&u64::MAX).unwrap();
        let payload = unsafe { decode(ValueType::U64, "u64".into(), &big) }.unwrap();
        assert_eq!(payload.get_ref::<u64>(), Some(&u64::MAX));
        let err = unsafe { decode(ValueType::I8, "i8".into(), &encode(&1000_i64).unwrap()) };
        assert!(err.is_err());
        assert!(encode(&Value::Unit).is_none());
    }
}
