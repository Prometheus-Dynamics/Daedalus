use serde::{Deserialize, Serialize};
use std::borrow::Cow;

/// Concrete runtime value.
///
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value")]
pub enum Value {
    Unit,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(Cow<'static, str>),
    Bytes(Cow<'static, [u8]>),
    List(Vec<Value>),
    Map(Vec<(Value, Value)>),
    Tuple(Vec<Value>),
    Struct(Vec<StructFieldValue>),
    Enum(EnumValue),
}

impl Value {
    /// Field `name` of a `Struct` value.
    pub fn field(&self, name: &str) -> Option<&Value> {
        match self {
            Value::Struct(fields) => fields
                .iter()
                .find(|field| field.name == name)
                .map(|field| &field.value),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::String(value) => Some(value),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(value) => Some(*value),
            _ => None,
        }
    }

    /// Non-negative `Int` value.
    pub fn as_u64(&self) -> Option<u64> {
        match self {
            Value::Int(value) => u64::try_from(*value).ok(),
            _ => None,
        }
    }

    pub fn as_list(&self) -> Option<&[Value]> {
        match self {
            Value::List(items) => Some(items),
            _ => None,
        }
    }

    /// String items of a `List` value; non-string items are skipped.
    pub fn as_string_list(&self) -> Option<Vec<String>> {
        Some(
            self.as_list()?
                .iter()
                .filter_map(|item| item.as_str().map(str::to_string))
                .collect(),
        )
    }

    /// A `Map` whose keys are all strings, as an ordered map. `None` if any key is not a string.
    pub fn as_string_map(&self) -> Option<std::collections::BTreeMap<String, Value>> {
        let Value::Map(entries) = self else {
            return None;
        };
        entries
            .iter()
            .map(|(key, value)| Some((key.as_str()?.to_string(), value.clone())))
            .collect()
    }
}

/// Borrowed view of a value to avoid cloning large payloads.
///
#[derive(Clone, Debug, PartialEq)]
pub enum ValueRef<'a> {
    Unit,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(&'a str),
    Bytes(&'a [u8]),
    List(&'a [Value]),
    Map(&'a [(Value, Value)]),
    Tuple(&'a [Value]),
    Struct(&'a [StructFieldValue]),
    Enum {
        name: &'a str,
        value: Option<&'a Value>,
    },
}

impl<'a> From<&'a Value> for ValueRef<'a> {
    fn from(v: &'a Value) -> Self {
        match v {
            Value::Unit => ValueRef::Unit,
            Value::Bool(b) => ValueRef::Bool(*b),
            Value::Int(i) => ValueRef::Int(*i),
            Value::Float(f) => ValueRef::Float(*f),
            Value::String(s) => ValueRef::String(s),
            Value::Bytes(b) => ValueRef::Bytes(b),
            Value::List(items) => ValueRef::List(items),
            Value::Map(entries) => ValueRef::Map(entries),
            Value::Tuple(items) => ValueRef::Tuple(items),
            Value::Struct(fields) => ValueRef::Struct(fields),
            Value::Enum(ev) => ValueRef::Enum {
                name: &ev.name,
                value: ev.value.as_deref(),
            },
        }
    }
}

impl<'a> ValueRef<'a> {
    /// Convert a borrowed view into an owned value.
    ///
    pub fn into_owned(self) -> Value {
        match self {
            ValueRef::Unit => Value::Unit,
            ValueRef::Bool(b) => Value::Bool(b),
            ValueRef::Int(i) => Value::Int(i),
            ValueRef::Float(f) => Value::Float(f),
            ValueRef::String(s) => Value::String(Cow::Owned(s.to_string())),
            ValueRef::Bytes(b) => Value::Bytes(Cow::Owned(b.to_vec())),
            ValueRef::List(items) => Value::List(items.to_vec()),
            ValueRef::Map(entries) => Value::Map(entries.to_vec()),
            ValueRef::Tuple(items) => Value::Tuple(items.to_vec()),
            ValueRef::Struct(fields) => Value::Struct(fields.to_vec()),
            ValueRef::Enum { name, value } => Value::Enum(EnumValue {
                name: name.to_string(),
                value: value.map(|v| Box::new(v.clone())),
            }),
        }
    }
}

/// Static value type.
///
/// Every numeric variant is one Rust type (`Int` is `i64`, `Float` is `f64`), so the transport
/// key of a builtin scalar names exactly one Rust type. Graph values carry integers as
/// `Value::Int` and floats as `Value::Float` whatever the width.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, PartialOrd, Ord)]
pub enum ValueType {
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
    String,
    Bytes,
}

impl ValueType {
    /// The Rust type this scalar stands for (`Vec<u8>` for `Bytes`, `()` for `Unit`).
    pub const fn rust_name(self) -> &'static str {
        match self {
            Self::Unit => "()",
            Self::Bool => "bool",
            Self::I8 => "i8",
            Self::I16 => "i16",
            Self::I32 => "i32",
            Self::Int => "i64",
            Self::ISize => "isize",
            Self::U8 => "u8",
            Self::U16 => "u16",
            Self::U32 => "u32",
            Self::U64 => "u64",
            Self::USize => "usize",
            Self::F32 => "f32",
            Self::Float => "f64",
            Self::String => "String",
            Self::Bytes => "Vec<u8>",
        }
    }

    /// The inclusive range of an integer type (`isize`/`usize` at this target's width).
    pub fn int_range(self) -> Option<(i128, i128)> {
        Some(match self {
            Self::I8 => (i8::MIN.into(), i8::MAX.into()),
            Self::I16 => (i16::MIN.into(), i16::MAX.into()),
            Self::I32 => (i32::MIN.into(), i32::MAX.into()),
            Self::Int => (i64::MIN.into(), i64::MAX.into()),
            Self::ISize => (isize::MIN as i128, isize::MAX as i128),
            Self::U8 => (0, u8::MAX.into()),
            Self::U16 => (0, u16::MAX.into()),
            Self::U32 => (0, u32::MAX.into()),
            Self::U64 => (0, u64::MAX.into()),
            Self::USize => (0, usize::MAX as i128),
            _ => return None,
        })
    }

    pub const fn is_float(self) -> bool {
        matches!(self, Self::F32 | Self::Float)
    }

    pub fn is_numeric(self) -> bool {
        self.is_float() || self.int_range().is_some()
    }

    /// Check that a graph value converts to this numeric type exactly: an integer in range, or
    /// for floats any `Value::Float` within range (`f32` rounds) and integers it represents
    /// exactly. Other types accept anything; their conversions check shape themselves.
    pub fn check_value(self, value: &Value) -> Result<(), String> {
        let rust = self.rust_name();
        match (self.int_range(), value) {
            (Some((min, max)), Value::Int(v)) if (min..=max).contains(&i128::from(*v)) => Ok(()),
            (Some((min, max)), Value::Int(v)) => {
                Err(format!("{v} is out of range for {rust} ({min}..={max})"))
            }
            (Some(_), other) => Err(format!("expected an integer for {rust}, found {other:?}")),
            (None, Value::Float(v)) if self == Self::F32 && v.is_finite() => {
                if v.abs() <= f64::from(f32::MAX) {
                    Ok(())
                } else {
                    Err(format!("{v} is out of range for f32"))
                }
            }
            (None, Value::Float(_)) if self.is_float() => Ok(()),
            (None, Value::Int(v)) if self.is_float() => {
                let exact = if self == Self::F32 { 1 << 24 } else { 1 << 53 };
                if v.unsigned_abs() <= exact {
                    Ok(())
                } else {
                    Err(format!("{v} is not exactly representable as {rust}"))
                }
            }
            (None, other) if self.is_float() => {
                Err(format!("expected a number for {rust}, found {other:?}"))
            }
            _ => Ok(()),
        }
    }
}

/// Type expression to describe structured types.
///
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, PartialOrd, Ord)]
pub enum TypeExpr {
    Scalar(ValueType),
    /// Opaque, named type identifier (e.g. plugin-defined types).
    ///
    /// This is useful when a type's internal structure isn't expressed in the graph
    /// type system, but you still want strong matching and a meaningful label in UIs.
    Opaque(String),
    Optional(Box<TypeExpr>),
    List(Box<TypeExpr>),
    Map(Box<TypeExpr>, Box<TypeExpr>),
    Tuple(Vec<TypeExpr>),
    Struct(Vec<StructField>),
    Enum(Vec<EnumVariant>),
}

impl TypeExpr {
    /// Construct a scalar type expression.
    pub fn scalar(t: ValueType) -> Self {
        TypeExpr::Scalar(t)
    }

    /// Construct an opaque type expression.
    pub fn opaque(name: impl Into<String>) -> Self {
        TypeExpr::Opaque(name.into())
    }

    /// Wrap an optional type.
    pub fn optional(inner: TypeExpr) -> Self {
        TypeExpr::Optional(Box::new(inner))
    }

    /// Wrap a list type.
    pub fn list(inner: TypeExpr) -> Self {
        TypeExpr::List(Box::new(inner))
    }

    /// Wrap a map type.
    pub fn map(key: TypeExpr, value: TypeExpr) -> Self {
        TypeExpr::Map(Box::new(key), Box::new(value))
    }

    /// Construct a struct type.
    pub fn r#struct(fields: Vec<StructField>) -> Self {
        TypeExpr::Struct(fields)
    }

    /// Construct an enum type.
    pub fn r#enum(variants: Vec<EnumVariant>) -> Self {
        TypeExpr::Enum(variants)
    }

    /// Decode a type expression stored as a JSON string value (the planner metadata encoding).
    #[cfg(feature = "json")]
    pub fn from_json_value(value: &Value) -> Option<Self> {
        serde_json::from_str(value.as_str()?).ok()
    }

    /// Produce a canonically ordered representation for deterministic equality/ordering.
    pub fn normalize(self) -> Self {
        match self {
            TypeExpr::Scalar(v) => TypeExpr::Scalar(v),
            TypeExpr::Opaque(name) => TypeExpr::Opaque(name),
            TypeExpr::Optional(inner) => TypeExpr::Optional(Box::new(inner.normalize())),
            TypeExpr::List(inner) => TypeExpr::List(Box::new(inner.normalize())),
            TypeExpr::Map(k, v) => TypeExpr::Map(Box::new(k.normalize()), Box::new(v.normalize())),
            TypeExpr::Tuple(items) => {
                TypeExpr::Tuple(items.into_iter().map(|t| t.normalize()).collect())
            }
            TypeExpr::Struct(mut fields) => {
                for f in &mut fields {
                    f.ty = f.ty.clone().normalize();
                }
                fields.sort_by(|a, b| a.name.cmp(&b.name));
                TypeExpr::Struct(fields)
            }
            TypeExpr::Enum(mut variants) => {
                for v in &mut variants {
                    if let Some(t) = &v.ty {
                        v.ty = Some(t.clone().normalize());
                    }
                }
                variants.sort_by(|a, b| a.name.cmp(&b.name));
                TypeExpr::Enum(variants)
            }
        }
    }
}

/// Named field for struct types.
///
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, PartialOrd, Ord)]
pub struct StructField {
    pub name: String,
    pub ty: TypeExpr,
}

/// Struct field value pairing.
///
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StructFieldValue {
    pub name: String,
    pub value: Value,
}

/// Enum variant with optional payload type.
///
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, PartialOrd, Ord)]
pub struct EnumVariant {
    pub name: String,
    pub ty: Option<TypeExpr>,
}

/// Enum value with optional payload.
///
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EnumValue {
    pub name: String,
    pub value: Option<Box<Value>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn struct_and_enum_ordering_are_deterministic() {
        let a = TypeExpr::Struct(vec![
            StructField {
                name: "b".into(),
                ty: TypeExpr::Scalar(ValueType::Bool),
            },
            StructField {
                name: "a".into(),
                ty: TypeExpr::Scalar(ValueType::Int),
            },
        ])
        .normalize();
        let fields = match a {
            TypeExpr::Struct(f) => f,
            _ => unreachable!(),
        };
        assert_eq!(fields[0].name, "a");

        let variants = TypeExpr::Enum(vec![
            EnumVariant {
                name: "z".into(),
                ty: None,
            },
            EnumVariant {
                name: "a".into(),
                ty: Some(TypeExpr::Scalar(ValueType::String)),
            },
        ])
        .normalize();
        let variants = match variants {
            TypeExpr::Enum(v) => v,
            _ => unreachable!(),
        };
        assert_eq!(variants[0].name, "a");
    }

    #[test]
    fn value_ref_round_trip_owned() {
        let v = Value::Struct(vec![
            StructFieldValue {
                name: "a".into(),
                value: Value::Int(1),
            },
            StructFieldValue {
                name: "b".into(),
                value: Value::String("hi".into()),
            },
        ]);
        let view = ValueRef::from(&v);
        let owned = view.into_owned();
        assert_eq!(v, owned);
    }
}
