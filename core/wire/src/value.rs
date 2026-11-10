//! Typed values. A value is encoded by its type, and a type is encoded as a tree of tags. Both
//! sides rebuild the same [`WireType`] from a hello, so the wire carries no names for values.

use daedalus_types::{Scalar, TypeExpr};

use crate::WireError;
use crate::codec::{Reader, Writer};
use crate::schema::Schema;

/// How deep a type or value may nest. Deeper input is refused, so a hostile message cannot
/// exhaust the stack.
pub const MAX_DEPTH: u32 = 32;

/// Every scalar, in tag order: a scalar's wire tag is its discriminant.
const SCALARS: [Scalar; 16] = [
    Scalar::Unit,
    Scalar::Bool,
    Scalar::I8,
    Scalar::I16,
    Scalar::I32,
    Scalar::Int,
    Scalar::ISize,
    Scalar::U8,
    Scalar::U16,
    Scalar::U32,
    Scalar::U64,
    Scalar::USize,
    Scalar::F32,
    Scalar::Float,
    Scalar::String,
    Scalar::Bytes,
];

/// An owned type: the wire's copy of an A4 [`TypeExpr`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WireType {
    Scalar(Scalar),
    /// A type key. Its values are opaque bytes that a codec on each side reads and writes.
    Opaque(String),
    Optional(Box<WireType>),
    List(Box<WireType>),
    Map(Box<WireType>, Box<WireType>),
    Tuple(Vec<WireType>),
    Struct(Vec<(String, WireType)>),
    Enum(Vec<(String, Option<WireType>)>),
}

impl WireType {
    /// The wire copy of a type declared in code.
    pub fn from_expr(expr: &TypeExpr) -> WireType {
        match expr {
            TypeExpr::Scalar(scalar) => WireType::Scalar(*scalar),
            TypeExpr::Opaque(key) => WireType::Opaque(key.as_str().to_owned()),
            TypeExpr::Optional(inner) => WireType::Optional(Box::new(Self::from_expr(inner))),
            TypeExpr::List(inner) => WireType::List(Box::new(Self::from_expr(inner))),
            TypeExpr::Map(key, value) => WireType::Map(
                Box::new(Self::from_expr(key)),
                Box::new(Self::from_expr(value)),
            ),
            TypeExpr::Tuple(items) => WireType::Tuple(items.iter().map(Self::from_expr).collect()),
            TypeExpr::Struct(fields) => WireType::Struct(
                fields
                    .iter()
                    .map(|field| (field.name.to_owned(), Self::from_expr(&field.ty)))
                    .collect(),
            ),
            TypeExpr::Enum(variants) => WireType::Enum(
                variants
                    .iter()
                    .map(|variant| {
                        (
                            variant.name.to_owned(),
                            variant.payload.as_ref().map(Self::from_expr),
                        )
                    })
                    .collect(),
            ),
        }
    }

    /// A short name for error messages.
    pub fn name(&self) -> &'static str {
        match self {
            WireType::Scalar(scalar) => scalar_name(*scalar),
            WireType::Opaque(_) => "opaque",
            WireType::Optional(_) => "optional",
            WireType::List(_) => "list",
            WireType::Map(..) => "map",
            WireType::Tuple(_) => "tuple",
            WireType::Struct(_) => "struct",
            WireType::Enum(_) => "enum",
        }
    }

    pub fn encode(&self, out: &mut Writer) -> Result<(), WireError> {
        match self {
            WireType::Scalar(scalar) => {
                out.u8(0);
                out.u8(*scalar as u8);
            }
            WireType::Opaque(key) => {
                out.u8(1);
                out.str(key)?;
            }
            WireType::Optional(inner) => {
                out.u8(2);
                inner.encode(out)?;
            }
            WireType::List(inner) => {
                out.u8(3);
                inner.encode(out)?;
            }
            WireType::Map(key, value) => {
                out.u8(4);
                key.encode(out)?;
                value.encode(out)?;
            }
            WireType::Tuple(items) => {
                out.u8(5);
                out.len(items.len())?;
                for item in items {
                    item.encode(out)?;
                }
            }
            WireType::Struct(fields) => {
                out.u8(6);
                out.len(fields.len())?;
                for (name, ty) in fields {
                    out.str(name)?;
                    ty.encode(out)?;
                }
            }
            WireType::Enum(variants) => {
                out.u8(7);
                out.len(variants.len())?;
                for (name, payload) in variants {
                    out.str(name)?;
                    match payload {
                        None => out.u8(0),
                        Some(ty) => {
                            out.u8(1);
                            ty.encode(out)?;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub fn decode(input: &mut Reader<'_>, depth: u32) -> Result<WireType, WireError> {
        if depth > MAX_DEPTH {
            return Err(WireError::TooDeep);
        }
        let deeper = depth + 1;
        Ok(match input.u8()? {
            0 => {
                let tag = input.u8()?;
                let scalar = SCALARS
                    .iter()
                    .copied()
                    .find(|scalar| *scalar as u8 == tag)
                    .ok_or(WireError::BadTag {
                        what: "scalar",
                        tag: u32::from(tag),
                    })?;
                WireType::Scalar(scalar)
            }
            1 => WireType::Opaque(input.str()?),
            2 => WireType::Optional(Box::new(Self::decode(input, deeper)?)),
            3 => WireType::List(Box::new(Self::decode(input, deeper)?)),
            4 => WireType::Map(
                Box::new(Self::decode(input, deeper)?),
                Box::new(Self::decode(input, deeper)?),
            ),
            5 => {
                let count = input.count()?;
                let mut items = Vec::with_capacity(count);
                for _ in 0..count {
                    items.push(Self::decode(input, deeper)?);
                }
                WireType::Tuple(items)
            }
            6 => {
                let count = input.count()?;
                let mut fields = Vec::with_capacity(count);
                for _ in 0..count {
                    let name = input.str()?;
                    fields.push((name, Self::decode(input, deeper)?));
                }
                WireType::Struct(fields)
            }
            7 => {
                let count = input.count()?;
                let mut variants = Vec::with_capacity(count);
                for _ in 0..count {
                    let name = input.str()?;
                    let payload = match input.u8()? {
                        0 => None,
                        1 => Some(Self::decode(input, deeper)?),
                        tag => {
                            return Err(WireError::BadTag {
                                what: "variant payload",
                                tag: u32::from(tag),
                            });
                        }
                    };
                    variants.push((name, payload));
                }
                WireType::Enum(variants)
            }
            tag => {
                return Err(WireError::BadTag {
                    what: "type",
                    tag: u32::from(tag),
                });
            }
        })
    }
}

fn scalar_name(scalar: Scalar) -> &'static str {
    match scalar {
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
    }
}

/// A value on the wire. Unsigned integers are native: a `U64` holds all of 0 to `u64::MAX`.
#[derive(Clone, Debug, PartialEq)]
pub enum WireValue {
    Unit,
    Bool(bool),
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    ISize(i64),
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    USize(u64),
    F32(f32),
    F64(f64),
    String(String),
    Bytes(Vec<u8>),
    Optional(Option<Box<WireValue>>),
    List(Vec<WireValue>),
    Map(Vec<(WireValue, WireValue)>),
    Tuple(Vec<WireValue>),
    /// Field values in the type's field order.
    Struct(Vec<WireValue>),
    Enum {
        variant: u32,
        payload: Option<Box<WireValue>>,
    },
    /// The bytes a codec wrote for an opaque type.
    Opaque(Vec<u8>),
}

impl WireValue {
    /// A short name for error messages.
    pub fn name(&self) -> &'static str {
        match self {
            WireValue::Unit => "unit",
            WireValue::Bool(_) => "bool",
            WireValue::I8(_) => "i8",
            WireValue::I16(_) => "i16",
            WireValue::I32(_) => "i32",
            WireValue::I64(_) => "i64",
            WireValue::ISize(_) => "isize",
            WireValue::U8(_) => "u8",
            WireValue::U16(_) => "u16",
            WireValue::U32(_) => "u32",
            WireValue::U64(_) => "u64",
            WireValue::USize(_) => "usize",
            WireValue::F32(_) => "f32",
            WireValue::F64(_) => "f64",
            WireValue::String(_) => "string",
            WireValue::Bytes(_) => "bytes",
            WireValue::Optional(_) => "optional",
            WireValue::List(_) => "list",
            WireValue::Map(_) => "map",
            WireValue::Tuple(_) => "tuple",
            WireValue::Struct(_) => "struct",
            WireValue::Enum { .. } => "enum",
            WireValue::Opaque(_) => "opaque",
        }
    }
}

/// Writes `value` as `ty`. A value that does not have the shape of its type is refused, and so is
/// an opaque type whose key has no codec in `schema`.
pub fn encode_value(
    ty: &WireType,
    value: &WireValue,
    schema: &Schema,
    out: &mut Writer,
) -> Result<(), WireError> {
    encode_at(ty, value, schema, out, 0)
}

fn shape(expected: &WireType, found: &WireValue) -> WireError {
    WireError::ValueShape {
        expected: expected.name(),
        found: found.name(),
    }
}

fn encode_at(
    ty: &WireType,
    value: &WireValue,
    schema: &Schema,
    out: &mut Writer,
    depth: u32,
) -> Result<(), WireError> {
    if depth > MAX_DEPTH {
        return Err(WireError::TooDeep);
    }
    let deeper = depth + 1;
    match (ty, value) {
        (WireType::Scalar(Scalar::Unit), WireValue::Unit) => {}
        (WireType::Scalar(Scalar::Bool), WireValue::Bool(v)) => out.bool(*v),
        (WireType::Scalar(Scalar::I8), WireValue::I8(v)) => out.i8(*v),
        (WireType::Scalar(Scalar::I16), WireValue::I16(v)) => out.i16(*v),
        (WireType::Scalar(Scalar::I32), WireValue::I32(v)) => out.i32(*v),
        (WireType::Scalar(Scalar::Int), WireValue::I64(v)) => out.i64(*v),
        (WireType::Scalar(Scalar::ISize), WireValue::ISize(v)) => out.i64(*v),
        (WireType::Scalar(Scalar::U8), WireValue::U8(v)) => out.u8(*v),
        (WireType::Scalar(Scalar::U16), WireValue::U16(v)) => out.u16(*v),
        (WireType::Scalar(Scalar::U32), WireValue::U32(v)) => out.u32(*v),
        (WireType::Scalar(Scalar::U64), WireValue::U64(v)) => out.u64(*v),
        (WireType::Scalar(Scalar::USize), WireValue::USize(v)) => out.u64(*v),
        (WireType::Scalar(Scalar::F32), WireValue::F32(v)) => out.f32(*v),
        (WireType::Scalar(Scalar::Float), WireValue::F64(v)) => out.f64(*v),
        (WireType::Scalar(Scalar::String), WireValue::String(v)) => out.str(v)?,
        (WireType::Scalar(Scalar::Bytes), WireValue::Bytes(v)) => out.bytes(v)?,
        (WireType::Optional(inner), WireValue::Optional(v)) => match v {
            None => out.u8(0),
            Some(item) => {
                out.u8(1);
                encode_at(inner, item, schema, out, deeper)?;
            }
        },
        (WireType::List(inner), WireValue::List(items)) => {
            out.len(items.len())?;
            for item in items {
                encode_at(inner, item, schema, out, deeper)?;
            }
        }
        (WireType::Map(key_ty, value_ty), WireValue::Map(pairs)) => {
            out.len(pairs.len())?;
            for (key, item) in pairs {
                encode_at(key_ty, key, schema, out, deeper)?;
                encode_at(value_ty, item, schema, out, deeper)?;
            }
        }
        (WireType::Tuple(types), WireValue::Tuple(items)) if types.len() == items.len() => {
            for (ty, item) in types.iter().zip(items) {
                encode_at(ty, item, schema, out, deeper)?;
            }
        }
        (WireType::Struct(fields), WireValue::Struct(items)) if fields.len() == items.len() => {
            for ((_, ty), item) in fields.iter().zip(items) {
                encode_at(ty, item, schema, out, deeper)?;
            }
        }
        (WireType::Enum(variants), WireValue::Enum { variant, payload }) => {
            let (_, payload_ty) = variants.get(*variant as usize).ok_or(WireError::BadTag {
                what: "variant",
                tag: *variant,
            })?;
            out.u32(*variant);
            match (payload_ty, payload) {
                (None, None) => {}
                (Some(ty), Some(item)) => encode_at(ty, item, schema, out, deeper)?,
                _ => return Err(shape(ty, value)),
            }
        }
        (WireType::Opaque(key), WireValue::Opaque(bytes)) => {
            if !schema.has_codec(key) {
                return Err(WireError::NoCodec(key.clone()));
            }
            out.bytes(bytes)?;
        }
        _ => return Err(shape(ty, value)),
    }
    Ok(())
}

/// Reads one value of `ty` from `input`.
pub fn decode_value(
    ty: &WireType,
    input: &mut Reader<'_>,
    schema: &Schema,
) -> Result<WireValue, WireError> {
    decode_at(ty, input, schema, 0)
}

fn decode_at(
    ty: &WireType,
    input: &mut Reader<'_>,
    schema: &Schema,
    depth: u32,
) -> Result<WireValue, WireError> {
    if depth > MAX_DEPTH {
        return Err(WireError::TooDeep);
    }
    let deeper = depth + 1;
    Ok(match ty {
        WireType::Scalar(scalar) => match scalar {
            Scalar::Unit => WireValue::Unit,
            Scalar::Bool => WireValue::Bool(input.bool()?),
            Scalar::I8 => WireValue::I8(input.i8()?),
            Scalar::I16 => WireValue::I16(input.i16()?),
            Scalar::I32 => WireValue::I32(input.i32()?),
            Scalar::Int => WireValue::I64(input.i64()?),
            Scalar::ISize => WireValue::ISize(input.i64()?),
            Scalar::U8 => WireValue::U8(input.u8()?),
            Scalar::U16 => WireValue::U16(input.u16()?),
            Scalar::U32 => WireValue::U32(input.u32()?),
            Scalar::U64 => WireValue::U64(input.u64()?),
            Scalar::USize => WireValue::USize(input.u64()?),
            Scalar::F32 => WireValue::F32(input.f32()?),
            Scalar::Float => WireValue::F64(input.f64()?),
            Scalar::String => WireValue::String(input.str()?),
            Scalar::Bytes => WireValue::Bytes(input.bytes()?),
        },
        WireType::Optional(inner) => match input.u8()? {
            0 => WireValue::Optional(None),
            1 => WireValue::Optional(Some(Box::new(decode_at(inner, input, schema, deeper)?))),
            tag => {
                return Err(WireError::BadTag {
                    what: "optional",
                    tag: u32::from(tag),
                });
            }
        },
        WireType::List(inner) => {
            let count = input.count()?;
            let mut items = Vec::with_capacity(count);
            for _ in 0..count {
                items.push(decode_at(inner, input, schema, deeper)?);
            }
            WireValue::List(items)
        }
        WireType::Map(key_ty, value_ty) => {
            let count = input.count()?;
            let mut pairs = Vec::with_capacity(count);
            for _ in 0..count {
                let key = decode_at(key_ty, input, schema, deeper)?;
                pairs.push((key, decode_at(value_ty, input, schema, deeper)?));
            }
            WireValue::Map(pairs)
        }
        WireType::Tuple(types) => {
            let mut items = Vec::with_capacity(types.len());
            for ty in types {
                items.push(decode_at(ty, input, schema, deeper)?);
            }
            WireValue::Tuple(items)
        }
        WireType::Struct(fields) => {
            let mut items = Vec::with_capacity(fields.len());
            for (_, ty) in fields {
                items.push(decode_at(ty, input, schema, deeper)?);
            }
            WireValue::Struct(items)
        }
        WireType::Enum(variants) => {
            let variant = input.u32()?;
            let (_, payload_ty) = variants.get(variant as usize).ok_or(WireError::BadTag {
                what: "variant",
                tag: variant,
            })?;
            let payload = match payload_ty {
                None => None,
                Some(ty) => Some(Box::new(decode_at(ty, input, schema, deeper)?)),
            };
            WireValue::Enum { variant, payload }
        }
        WireType::Opaque(key) => {
            if !schema.has_codec(key) {
                return Err(WireError::NoCodec(key.clone()));
            }
            WireValue::Opaque(input.bytes()?)
        }
    })
}
