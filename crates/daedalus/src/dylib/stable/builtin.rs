//! Builtin scalars (`bool`, integers, floats, `()`, `String`, `Vec<u8>`) crossing the stable
//! boundary without a [`Value`] detour: encoded from a payload's `dyn Any` by borrowing, and
//! decoded into a typed payload of the port's Rust type.

use std::any::Any;
use std::sync::Arc;

use super::value::{StableValue, tag, to_value};
use crate::data::model::{TypeExpr, Value, ValueType};
use crate::data::typing::coerce_builtin_const_value;
use crate::registry::typeexpr_transport_key;
use crate::transport::{Payload, PayloadSlot, TypeKey};

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

/// The common scalar and text cases, without the `Value` detour (`None`: take it).
///
/// # Safety
/// As [`decode`].
unsafe fn decode_direct(
    scalar: ValueType,
    key: &TypeKey,
    slot: &mut PayloadSlot,
    value: &StableValue,
) -> Option<Result<Payload, String>> {
    fn owned<T: Default + Send + Sync + 'static>(
        slot: &mut PayloadSlot,
        key: &TypeKey,
        value: T,
    ) -> Option<Result<Payload, String>> {
        Some(Ok(slot.owned(key.clone(), value)))
    }
    let bits = value.scalar;
    let int = bits as i64;
    // Safety (slices): forwarded from the caller.
    match (scalar, value.tag) {
        (ValueType::String, tag::STRING) => {
            Some(unsafe { value.as_str() }.map(|text| slot.owned(key.clone(), text.to_owned())))
        }
        (ValueType::Bytes, tag::BYTES) => owned(slot, key, unsafe { value.as_bytes() }.to_vec()),
        (ValueType::Bool, tag::BOOL) => owned(slot, key, bits != 0),
        (ValueType::Float, tag::FLOAT) => owned(slot, key, f64::from_bits(bits)),
        (ValueType::F32, tag::FLOAT) => owned(slot, key, f64::from_bits(bits) as f32),
        (ValueType::U64, tag::UINT) => owned(slot, key, bits),
        (ValueType::USize, tag::UINT) => Some(
            usize::try_from(bits)
                .map(|value| slot.owned(key.clone(), value))
                .map_err(|err| err.to_string()),
        ),
        (_, tag::INT) => match scalar {
            ValueType::Int => owned(slot, key, int),
            ValueType::I32 => int_in::<i32>(slot, key, scalar, int),
            ValueType::U32 => int_in::<u32>(slot, key, scalar, int),
            ValueType::I16 => int_in::<i16>(slot, key, scalar, int),
            ValueType::U16 => int_in::<u16>(slot, key, scalar, int),
            ValueType::I8 => int_in::<i8>(slot, key, scalar, int),
            ValueType::U8 => int_in::<u8>(slot, key, scalar, int),
            ValueType::U64 => int_in::<u64>(slot, key, scalar, int),
            ValueType::ISize => int_in::<isize>(slot, key, scalar, int),
            ValueType::USize => int_in::<usize>(slot, key, scalar, int),
            _ => None,
        },
        _ => None,
    }
}

/// `value` as `T` in `slot`, or the range error.
fn int_in<T: TryFrom<i64> + Default + Send + Sync + 'static>(
    slot: &mut PayloadSlot,
    key: &TypeKey,
    scalar: ValueType,
    value: i64,
) -> Option<Result<Payload, String>> {
    Some(
        T::try_from(value)
            .map(|value| slot.owned(key.clone(), value))
            .map_err(|_| format!("expected {}, found {value}", scalar.rust_name())),
    )
}

/// A payload holding the Rust type of `scalar` under `key`, built in `slot`'s allocation when
/// it is free.
///
/// # Safety
/// Every pointer in `value` must be valid for the call.
pub unsafe fn decode(
    scalar: ValueType,
    key: TypeKey,
    slot: &mut PayloadSlot,
    value: &StableValue,
) -> Result<Payload, String> {
    if let Some(payload) = unsafe { decode_direct(scalar, &key, slot, value) } {
        return payload;
    }
    // Safety: forwarded from the caller.
    let value = unsafe { to_value(value) }?;
    fn typed<T: Default + Send + Sync + 'static>(
        key: TypeKey,
        scalar: ValueType,
        slot: &mut PayloadSlot,
        value: &Value,
    ) -> Result<Payload, String> {
        coerce_builtin_const_value::<T>(value)
            .map(|typed| slot.owned(key, typed))
            .ok_or_else(|| format!("expected {}, found {value:?}", scalar.rust_name()))
    }
    match scalar {
        ValueType::Unit => typed::<()>(key, scalar, slot, &value),
        ValueType::Bool => typed::<bool>(key, scalar, slot, &value),
        ValueType::I8 => typed::<i8>(key, scalar, slot, &value),
        ValueType::I16 => typed::<i16>(key, scalar, slot, &value),
        ValueType::I32 => typed::<i32>(key, scalar, slot, &value),
        ValueType::Int => typed::<i64>(key, scalar, slot, &value),
        ValueType::ISize => typed::<isize>(key, scalar, slot, &value),
        ValueType::U8 => typed::<u8>(key, scalar, slot, &value),
        ValueType::U16 => typed::<u16>(key, scalar, slot, &value),
        ValueType::U32 => typed::<u32>(key, scalar, slot, &value),
        ValueType::U64 => typed::<u64>(key, scalar, slot, &value),
        ValueType::USize => typed::<usize>(key, scalar, slot, &value),
        ValueType::F32 => typed::<f32>(key, scalar, slot, &value),
        ValueType::Float => typed::<f64>(key, scalar, slot, &value),
        ValueType::String => typed::<String>(key, scalar, slot, &value),
        ValueType::Bytes => typed::<Vec<u8>>(key, scalar, slot, &value),
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
        let payload = unsafe {
            decode(
                ValueType::String,
                "string".into(),
                &mut PayloadSlot::default(),
                &encoded,
            )
        }
        .unwrap();
        assert_eq!(payload.get_ref::<String>().map(String::as_str), Some("hi"));

        let payload = unsafe {
            decode(
                ValueType::I32,
                "i32".into(),
                &mut PayloadSlot::default(),
                &encode(&7_u8).unwrap(),
            )
        }
        .unwrap();
        assert_eq!(payload.get_ref::<i32>(), Some(&7));
        let big = encode(&u64::MAX).unwrap();
        let payload = unsafe {
            decode(
                ValueType::U64,
                "u64".into(),
                &mut PayloadSlot::default(),
                &big,
            )
        }
        .unwrap();
        assert_eq!(payload.get_ref::<u64>(), Some(&u64::MAX));
        let err = unsafe {
            decode(
                ValueType::I8,
                "i8".into(),
                &mut PayloadSlot::default(),
                &encode(&1000_i64).unwrap(),
            )
        };
        assert!(err.is_err());
        assert!(encode(&Value::Unit).is_none());
    }
}
