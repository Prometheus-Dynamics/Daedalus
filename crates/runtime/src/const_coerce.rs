//! Building a port's Rust type from a graph [`Value`] (a constant, a port default, or a `Value`
//! payload).
//!
//! Builtin scalars convert without registration. For other types the node macros register a
//! coercer per input and config field type when a plugin installs
//! ([`register_default_const_coercer`]): the type's [`DaedalusTypeExpr::from_value`] (unit enums
//! deriving `DaedalusTypeExpr`), else serde (types implementing `Deserialize`). A coercer
//! registered explicitly (`PluginRegistry::register_const_coercer`, `register_enum`) wins.

use std::any::{Any, type_name};

use daedalus_data::daedalus_type::DaedalusTypeExpr;
use daedalus_data::model::Value;
use daedalus_data::typing::builtin_type_expr;
use serde::de::DeserializeOwned;
use serde_json::Value as JsonValue;

use crate::io::ConstCoercerMap;

/// A function building `T` from a graph value.
pub type CoerceFn<T> = fn(&Value) -> Option<T>;

/// Register the coercer of `T` built from `schema` (tried first) and `serde`, unless `T` is a
/// builtin, neither is given, or `T` already has a coercer.
pub fn register_default_const_coercer<T>(
    coercers: &ConstCoercerMap,
    schema: Option<CoerceFn<T>>,
    serde: Option<CoerceFn<T>>,
) where
    T: Send + Sync + 'static,
{
    if (schema.is_none() && serde.is_none()) || builtin_type_expr::<T>().is_some() {
        return;
    }
    coercers.write().entry(type_name::<T>()).or_insert_with(|| {
        Box::new(move |value| {
            schema
                .and_then(|coerce| coerce(value))
                .or_else(|| serde.and_then(|coerce| coerce(value)))
                .map(|typed| Box::new(typed) as Box<dyn Any + Send + Sync>)
        })
    });
}

/// Deserialize `T` from `value`. Enums use serde's external tagging: a unit variant is its name
/// (`Value::String` or `Value::Enum`), a payload variant `Value::Enum { name, value }`.
pub fn deserialize_value<T: DeserializeOwned>(value: &Value) -> Option<T> {
    serde_json::from_value(serde_json_of(value)).ok()
}

fn serde_json_of(value: &Value) -> JsonValue {
    match value {
        Value::Unit => JsonValue::Null,
        Value::Bool(b) => JsonValue::Bool(*b),
        Value::Int(i) => JsonValue::from(*i),
        Value::Float(f) => JsonValue::from(*f),
        Value::String(s) => JsonValue::String(s.to_string()),
        Value::Bytes(bytes) => {
            JsonValue::Array(bytes.iter().map(|b| JsonValue::from(*b)).collect())
        }
        Value::Enum(ev) => match &ev.value {
            None => JsonValue::String(ev.name.clone()),
            Some(inner) => JsonValue::Object(
                [(ev.name.clone(), serde_json_of(inner))]
                    .into_iter()
                    .collect(),
            ),
        },
        Value::List(items) | Value::Tuple(items) => {
            JsonValue::Array(items.iter().map(serde_json_of).collect())
        }
        Value::Struct(fields) => JsonValue::Object(
            fields
                .iter()
                .map(|field| (field.name.clone(), serde_json_of(&field.value)))
                .collect(),
        ),
        Value::Map(entries) if entries.iter().all(|(k, _)| matches!(k, Value::String(_))) => {
            JsonValue::Object(
                entries
                    .iter()
                    .filter_map(|(k, v)| match k {
                        Value::String(key) => Some((key.to_string(), serde_json_of(v))),
                        _ => None,
                    })
                    .collect(),
            )
        }
        Value::Map(entries) => JsonValue::Array(
            entries
                .iter()
                .map(|(k, v)| JsonValue::Array(vec![serde_json_of(k), serde_json_of(v)]))
                .collect(),
        ),
    }
}

/// Support code for the node macros; not a public API.
#[doc(hidden)]
pub mod derive_support {
    use core::marker::PhantomData;

    use super::{CoerceFn, DaedalusTypeExpr, DeserializeOwned, Value, deserialize_value};
    use daedalus_data::to_value::ToValue;

    /// Autoref-specialization probe: `(&Probe::<T>(PhantomData)).schema_coercer()` is
    /// `Some(T::from_value)` when `T: DaedalusTypeExpr`, `serde_coercer()` is `Some` when
    /// `T: Deserialize`; both are `None` otherwise.
    pub struct Probe<T>(pub PhantomData<T>);

    pub trait SchemaCoerce<T> {
        fn schema_coercer(&self) -> Option<CoerceFn<T>>;
    }

    impl<T: DaedalusTypeExpr> SchemaCoerce<T> for Probe<T> {
        fn schema_coercer(&self) -> Option<CoerceFn<T>> {
            Some(T::from_value)
        }
    }

    pub trait NoSchemaCoerce<T> {
        fn schema_coercer(&self) -> Option<CoerceFn<T>> {
            None
        }
    }

    impl<T> NoSchemaCoerce<T> for &Probe<T> {}

    pub trait SerdeCoerce<T> {
        fn serde_coercer(&self) -> Option<CoerceFn<T>>;
    }

    impl<T: DeserializeOwned> SerdeCoerce<T> for Probe<T> {
        fn serde_coercer(&self) -> Option<CoerceFn<T>> {
            Some(deserialize_value::<T>)
        }
    }

    pub trait NoSerdeCoerce<T> {
        fn serde_coercer(&self) -> Option<CoerceFn<T>> {
            None
        }
    }

    impl<T> NoSerdeCoerce<T> for &Probe<T> {}

    /// `(&Probe::<T>(PhantomData)).value_encoder()` is `Some(T::to_value)` when `T: ToValue`,
    /// else `None`.
    pub trait ToValueProbe<T> {
        fn value_encoder(&self) -> Option<fn(&T) -> Value>;
    }

    impl<T: ToValue> ToValueProbe<T> for Probe<T> {
        fn value_encoder(&self) -> Option<fn(&T) -> Value> {
            Some(T::to_value)
        }
    }

    pub trait NoToValueProbe<T> {
        fn value_encoder(&self) -> Option<fn(&T) -> Value> {
            None
        }
    }

    impl<T> NoToValueProbe<T> for &Probe<T> {}

    /// `(&Probe::<T>(PhantomData)).cloner()` is `Some(T::clone)` when `T: Clone`, else `None`.
    pub trait CloneProbe<T> {
        fn cloner(&self) -> Option<fn(&T) -> T>;
    }

    impl<T: Clone> CloneProbe<T> for Probe<T> {
        fn cloner(&self) -> Option<fn(&T) -> T> {
            Some(T::clone)
        }
    }

    pub trait NoCloneProbe<T> {
        fn cloner(&self) -> Option<fn(&T) -> T> {
            None
        }
    }

    impl<T> NoCloneProbe<T> for &Probe<T> {}
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use daedalus_data::model::{EnumValue, StructFieldValue};

    use super::*;

    #[derive(Debug, PartialEq, serde::Deserialize)]
    #[serde(rename_all = "snake_case")]
    enum Shape {
        Dot,
        Circle(f64),
        Rect { w: i64, h: i64 },
    }

    fn named(name: &str, value: Option<Value>) -> Value {
        Value::Enum(EnumValue {
            name: name.into(),
            value: value.map(Box::new),
        })
    }

    #[test]
    fn values_deserialize_with_external_enum_tagging() {
        assert_eq!(deserialize_value(&named("dot", None)), Some(Shape::Dot));
        assert_eq!(
            deserialize_value(&Value::String(Cow::from("dot"))),
            Some(Shape::Dot)
        );
        assert_eq!(
            deserialize_value(&named("circle", Some(Value::Float(1.5)))),
            Some(Shape::Circle(1.5))
        );
        let rect = Value::Struct(vec![
            StructFieldValue {
                name: "w".into(),
                value: Value::Int(2),
            },
            StructFieldValue {
                name: "h".into(),
                value: Value::Int(3),
            },
        ]);
        assert_eq!(
            deserialize_value(&named("rect", Some(rect))),
            Some(Shape::Rect { w: 2, h: 3 })
        );
        assert_eq!(deserialize_value::<Shape>(&Value::Int(0)), None);
    }

    #[test]
    fn default_coercers_never_replace_registered_ones_or_cover_builtins() {
        let coercers = crate::io::new_const_coercer_map();
        register_default_const_coercer::<i64>(&coercers, None, Some(deserialize_value));
        assert!(coercers.read().is_empty());
        register_default_const_coercer::<Shape>(&coercers, None, None);
        assert!(coercers.read().is_empty());

        register_default_const_coercer::<Shape>(&coercers, None, Some(|_| Some(Shape::Dot)));
        register_default_const_coercer::<Shape>(&coercers, None, Some(deserialize_value));
        let guard = coercers.read();
        let coerce = guard.get(type_name::<Shape>()).expect("registered");
        let coerced = coerce(&Value::Unit).expect("first coercer kept");
        assert_eq!(coerced.downcast_ref::<Shape>(), Some(&Shape::Dot));
    }
}
