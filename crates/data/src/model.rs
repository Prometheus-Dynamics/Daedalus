use serde::{Deserialize, Serialize};
use std::borrow::Cow;

/// Concrete runtime value.
///
/// ```
/// use daedalus_data::model::Value;
/// let v = Value::Int(42);
/// assert_eq!(v, Value::Int(42));
/// ```
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

/// Borrowed view of a value to avoid cloning large payloads.
///
/// ```
/// use daedalus_data::model::{Value, ValueRef};
/// let value = Value::String("hi".into());
/// let view = ValueRef::from(&value);
/// assert!(matches!(view, ValueRef::String("hi")));
/// ```
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
    /// ```
    /// use daedalus_data::model::{Value, ValueRef};
    /// let value = Value::Bool(true);
    /// let owned = ValueRef::from(&value).into_owned();
    /// assert_eq!(owned, Value::Bool(true));
    /// ```
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
/// ```
/// use daedalus_data::model::ValueType;
/// let ty = ValueType::String;
/// assert_eq!(ty, ValueType::String);
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, PartialOrd, Ord)]
pub enum ValueType {
    Unit,
    Bool,
    /// 32-bit signed integer (stored in `Value::Int`).
    I32,
    /// 32-bit unsigned integer (stored in `Value::Int`).
    U32,
    Int,
    /// 32-bit float (stored in `Value::Float`).
    F32,
    Float,
    String,
    Bytes,
    // Future: more primitives can be added.
}

/// Type expression to describe structured types.
///
/// ```
/// use daedalus_data::model::{TypeExpr, ValueType};
/// let ty = TypeExpr::List(Box::new(TypeExpr::Scalar(ValueType::Int)));
/// assert!(matches!(ty, TypeExpr::List(_)));
/// ```
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
/// ```
/// use daedalus_data::model::{StructField, TypeExpr, ValueType};
/// let field = StructField { name: "count".into(), ty: TypeExpr::Scalar(ValueType::Int) };
/// assert_eq!(field.name, "count");
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, PartialOrd, Ord)]
pub struct StructField {
    pub name: String,
    pub ty: TypeExpr,
}

/// Struct field value pairing.
///
/// ```
/// use daedalus_data::model::{StructFieldValue, Value};
/// let field = StructFieldValue { name: "ok".into(), value: Value::Bool(true) };
/// assert_eq!(field.name, "ok");
/// ```
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StructFieldValue {
    pub name: String,
    pub value: Value,
}

/// Enum variant with optional payload type.
///
/// ```
/// use daedalus_data::model::{EnumVariant, TypeExpr, ValueType};
/// let variant = EnumVariant { name: "Ready".into(), ty: Some(TypeExpr::Scalar(ValueType::Unit)) };
/// assert_eq!(variant.name, "Ready");
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, PartialOrd, Ord)]
pub struct EnumVariant {
    pub name: String,
    pub ty: Option<TypeExpr>,
}

/// Enum value with optional payload.
///
/// ```
/// use daedalus_data::model::{EnumValue, Value};
/// let value = EnumValue { name: "Done".into(), value: Some(Box::new(Value::Unit)) };
/// assert_eq!(value.name, "Done");
/// ```
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
