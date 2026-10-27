//! JSON codecs and errors with deterministic representations.
use alloc::borrow::Cow;
use alloc::boxed::Box;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64;
use serde_json::Value as JsonValue;

use crate::errors::{DataError, DataErrorCode, DataResult};
use crate::model::{EnumValue, StructFieldValue, Value};

/// Encode a `Value` into a deterministic JSON string.
pub fn to_json(value: &Value) -> DataResult<String> {
    let json = encode_value(value)?;
    serde_json::to_string(&json)
        .map_err(|e| DataError::new(DataErrorCode::Serialization, e.to_string()))
}

/// Decode a `Value` from a JSON string.
pub fn from_json(s: &str) -> DataResult<Value> {
    let parsed: JsonValue = serde_json::from_str(s).map_err(|e| {
        DataError::new(
            DataErrorCode::Serialization,
            format!("parse error at {}: {}", e.line(), e.column()),
        )
    })?;
    decode_value(&parsed)
}

/// Encode to a structured JSON value.
#[doc(hidden)]
pub fn encode_value(value: &Value) -> DataResult<JsonValue> {
    let mut obj = serde_json::Map::new();
    match value {
        Value::Unit => {
            obj.insert("type".into(), JsonValue::String("unit".into()));
        }
        Value::Bool(b) => {
            obj.insert("type".into(), JsonValue::String("bool".into()));
            obj.insert("value".into(), JsonValue::Bool(*b));
        }
        Value::Int(i) => {
            obj.insert("type".into(), JsonValue::String("int".into()));
            obj.insert("value".into(), JsonValue::Number((*i).into()));
        }
        Value::Float(f) => {
            if !f.is_finite() {
                return Err(DataError::new(
                    DataErrorCode::Serialization,
                    "non-finite floats are not supported in JSON",
                ));
            }
            obj.insert("type".into(), JsonValue::String("float".into()));
            let number = serde_json::Number::from_f64(*f).ok_or_else(|| {
                DataError::new(
                    DataErrorCode::Serialization,
                    "float could not be represented as a JSON number",
                )
            })?;
            obj.insert("value".into(), JsonValue::Number(number));
        }
        Value::String(s) => {
            obj.insert("type".into(), JsonValue::String("string".into()));
            obj.insert("value".into(), JsonValue::String(s.to_string()));
        }
        Value::Bytes(bytes) => {
            obj.insert("type".into(), JsonValue::String("bytes".into()));
            obj.insert("value".into(), JsonValue::String(B64.encode(bytes)));
        }
        Value::List(items) => {
            obj.insert("type".into(), JsonValue::String("list".into()));
            let mut arr = Vec::new();
            for v in items {
                arr.push(encode_value(v)?);
            }
            obj.insert("value".into(), JsonValue::Array(arr));
        }
        Value::Map(entries) => {
            obj.insert("type".into(), JsonValue::String("map".into()));
            let mut arr = Vec::new();
            for (k, v) in entries {
                arr.push(JsonValue::Array(vec![encode_value(k)?, encode_value(v)?]));
            }
            obj.insert("value".into(), JsonValue::Array(arr));
        }
        Value::Tuple(items) => {
            obj.insert("type".into(), JsonValue::String("tuple".into()));
            let mut arr = Vec::new();
            for v in items {
                arr.push(encode_value(v)?);
            }
            obj.insert("value".into(), JsonValue::Array(arr));
        }
        Value::Struct(fields) => {
            obj.insert("type".into(), JsonValue::String("struct".into()));
            let mut props = serde_json::Map::new();
            let mut sorted = fields.clone();
            sorted.sort_by(|a, b| a.name.cmp(&b.name));
            for StructFieldValue { name, value } in sorted {
                props.insert(name, encode_value(&value)?);
            }
            obj.insert("value".into(), JsonValue::Object(props));
        }
        Value::Enum(ev) => {
            obj.insert("type".into(), JsonValue::String("enum".into()));
            let mut inner = serde_json::Map::new();
            inner.insert("name".into(), JsonValue::String(ev.name.clone()));
            if let Some(v) = &ev.value {
                inner.insert("value".into(), encode_value(v)?);
            }
            obj.insert("value".into(), JsonValue::Object(inner));
        }
    }
    Ok(JsonValue::Object(obj))
}

/// Convert a `Value` into plain (untagged) JSON for display and host-facing APIs.
///
/// Unlike [`encode_value`], this is lossy and not meant to round-trip: it renders values the way a
/// host UI or log would expect them.
///
/// - `Unit` becomes `null`; non-finite floats also become `null`.
/// - `List` and `Tuple` become arrays; `Bytes` becomes an array of byte numbers.
/// - `Struct` becomes an object keyed by field name.
/// - `Map` becomes an object when every key is a string, otherwise an array of `[key, value]` pairs.
/// - `Enum` becomes `{"name": ..., "value": ...}` (`value` omitted for unit variants).
pub fn to_plain_json(value: &Value) -> JsonValue {
    to_plain_json_with(value, PlainJsonOptions::default())
}

/// How [`to_plain_json_with`] renders `Value::Bytes`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum BytesEncoding {
    /// An array of byte numbers (what [`to_plain_json`] emits).
    #[default]
    Numbers,
    /// One standard-alphabet, padded base64 string: about a third of the size of
    /// [`BytesEncoding::Numbers`] for large buffers and far cheaper to parse. The value's type
    /// says it is bytes, so the string carries no wrapper.
    Base64,
}

/// Rendering options for [`to_plain_json_with`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PlainJsonOptions {
    pub bytes: BytesEncoding,
}

/// [`to_plain_json`] with options: the same rendering, except for what `options` changes.
pub fn to_plain_json_with(value: &Value, options: PlainJsonOptions) -> JsonValue {
    let recurse = |v: &Value| to_plain_json_with(v, options);
    match value {
        Value::Unit => JsonValue::Null,
        Value::Bool(b) => JsonValue::Bool(*b),
        Value::Int(i) => JsonValue::Number((*i).into()),
        Value::Float(f) => serde_json::Number::from_f64(*f)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        Value::String(s) => JsonValue::String(s.to_string()),
        Value::Bytes(bytes) => match options.bytes {
            BytesEncoding::Numbers => JsonValue::Array(
                bytes
                    .iter()
                    .map(|byte| JsonValue::Number((*byte).into()))
                    .collect(),
            ),
            BytesEncoding::Base64 => JsonValue::String(B64.encode(bytes)),
        },
        Value::List(items) | Value::Tuple(items) => {
            JsonValue::Array(items.iter().map(recurse).collect())
        }
        Value::Struct(fields) => {
            let mut obj = serde_json::Map::new();
            for field in fields {
                obj.insert(field.name.clone(), recurse(&field.value));
            }
            JsonValue::Object(obj)
        }
        Value::Enum(ev) => {
            let mut obj = serde_json::Map::new();
            obj.insert("name".into(), JsonValue::String(ev.name.clone()));
            if let Some(v) = &ev.value {
                obj.insert("value".into(), recurse(v));
            }
            JsonValue::Object(obj)
        }
        Value::Map(entries) => {
            if entries.iter().all(|(k, _)| matches!(k, Value::String(_))) {
                let mut obj = serde_json::Map::new();
                for (k, v) in entries {
                    if let Value::String(key) = k {
                        obj.insert(key.to_string(), recurse(v));
                    }
                }
                JsonValue::Object(obj)
            } else {
                JsonValue::Array(
                    entries
                        .iter()
                        .map(|(k, v)| JsonValue::Array(vec![recurse(k), recurse(v)]))
                        .collect(),
                )
            }
        }
    }
}

/// Convert plain (untagged) JSON into a `Value`; the inverse of [`to_plain_json`] where it is
/// unambiguous.
///
/// - `null` becomes `Unit`; booleans and strings map directly.
/// - Integral numbers that fit `i64` become `Int`, every other number becomes `Float`.
/// - Arrays become `List` and objects become `Map`s with `String` keys.
///
/// Values that [`to_plain_json`] renders lossily (`Bytes`, `Tuple`, `Struct`, `Enum`, non-string
/// map keys) come back in these generic shapes rather than their original variant.
pub fn from_plain_json(value: &JsonValue) -> DataResult<Value> {
    Ok(match value {
        JsonValue::Null => Value::Unit,
        JsonValue::Bool(b) => Value::Bool(*b),
        JsonValue::Number(n) => match (n.as_i64(), n.as_f64()) {
            (Some(i), _) => Value::Int(i),
            (None, Some(f)) => Value::Float(f),
            (None, None) => {
                return Err(DataError::new(
                    DataErrorCode::Serialization,
                    format!("unsupported JSON number {n}"),
                ));
            }
        },
        JsonValue::String(s) => Value::String(Cow::Owned(s.clone())),
        JsonValue::Array(items) => Value::List(
            items
                .iter()
                .map(from_plain_json)
                .collect::<DataResult<_>>()?,
        ),
        JsonValue::Object(map) => Value::Map(
            map.iter()
                .map(|(k, v)| Ok((Value::String(Cow::Owned(k.clone())), from_plain_json(v)?)))
                .collect::<DataResult<_>>()?,
        ),
    })
}

/// Decode from a structured JSON value.
#[doc(hidden)]
pub fn decode_value(value: &JsonValue) -> DataResult<Value> {
    let obj = value.as_object().ok_or_else(|| {
        DataError::new(
            DataErrorCode::Serialization,
            "expected object with fields `type` and optional `value`",
        )
    })?;
    let ty = obj.get("type").and_then(|v| v.as_str()).ok_or_else(|| {
        DataError::new(
            DataErrorCode::Serialization,
            "missing or invalid `type` field",
        )
    })?;
    match ty {
        "unit" => Ok(Value::Unit),
        "bool" => obj
            .get("value")
            .and_then(|v| v.as_bool())
            .map(Value::Bool)
            .ok_or_else(|| {
                DataError::new(DataErrorCode::Serialization, "expected boolean in `value`")
            }),
        "int" => obj
            .get("value")
            .and_then(|v| v.as_i64())
            .map(Value::Int)
            .ok_or_else(|| {
                DataError::new(DataErrorCode::Serialization, "expected integer in `value`")
            }),
        "float" => {
            let f = obj.get("value").and_then(|v| v.as_f64()).ok_or_else(|| {
                DataError::new(DataErrorCode::Serialization, "expected number in `value`")
            })?;
            if !f.is_finite() {
                return Err(DataError::new(
                    DataErrorCode::Serialization,
                    "non-finite floats are not supported in JSON",
                ));
            }
            Ok(Value::Float(f))
        }
        "string" => obj
            .get("value")
            .and_then(|v| v.as_str())
            .map(|s| Value::String(Cow::Owned(s.to_string())))
            .ok_or_else(|| {
                DataError::new(DataErrorCode::Serialization, "expected string in `value`")
            }),
        "bytes" => {
            let s = obj.get("value").and_then(|v| v.as_str()).ok_or_else(|| {
                DataError::new(
                    DataErrorCode::Serialization,
                    "expected base64 string in `value`",
                )
            })?;
            let decoded = B64.decode(s.as_bytes()).map_err(|e| {
                DataError::new(
                    DataErrorCode::Serialization,
                    format!("invalid base64 for bytes: {e}"),
                )
            })?;
            Ok(Value::Bytes(Cow::Owned(decoded)))
        }
        "list" => {
            let arr = obj.get("value").and_then(|v| v.as_array()).ok_or_else(|| {
                DataError::new(DataErrorCode::Serialization, "expected array in `value`")
            })?;
            let mut items = Vec::new();
            for v in arr {
                items.push(decode_value(v)?);
            }
            Ok(Value::List(items))
        }
        "map" => {
            let arr = obj.get("value").and_then(|v| v.as_array()).ok_or_else(|| {
                DataError::new(
                    DataErrorCode::Serialization,
                    "expected array of pairs in `value`",
                )
            })?;
            let mut entries = Vec::new();
            for pair in arr {
                let elems = pair.as_array().ok_or_else(|| {
                    DataError::new(
                        DataErrorCode::Serialization,
                        "expected array pair in map entry",
                    )
                })?;
                if elems.len() != 2 {
                    return Err(DataError::new(
                        DataErrorCode::Serialization,
                        "map entry must have two elements",
                    ));
                }
                let key = decode_value(&elems[0])?;
                let val = decode_value(&elems[1])?;
                entries.push((key, val));
            }
            Ok(Value::Map(entries))
        }
        "tuple" => {
            let arr = obj.get("value").and_then(|v| v.as_array()).ok_or_else(|| {
                DataError::new(DataErrorCode::Serialization, "expected array in `value`")
            })?;
            let mut items = Vec::new();
            for v in arr {
                items.push(decode_value(v)?);
            }
            Ok(Value::Tuple(items))
        }
        "struct" => {
            let m = obj
                .get("value")
                .and_then(|v| v.as_object())
                .ok_or_else(|| {
                    DataError::new(DataErrorCode::Serialization, "expected object in `value`")
                })?;
            let mut fields = Vec::new();
            for (name, val) in m {
                fields.push(StructFieldValue {
                    name: name.clone(),
                    value: decode_value(val)?,
                });
            }
            fields.sort_by(|a, b| a.name.cmp(&b.name));
            Ok(Value::Struct(fields))
        }
        "enum" => {
            let m = obj
                .get("value")
                .and_then(|v| v.as_object())
                .ok_or_else(|| {
                    DataError::new(DataErrorCode::Serialization, "expected object in `value`")
                })?;
            let name = m
                .get("name")
                .and_then(|v| v.as_str())
                .ok_or_else(|| DataError::new(DataErrorCode::Serialization, "missing enum name"))?;
            let val = if let Some(v) = m.get("value") {
                Some(Box::new(decode_value(v)?))
            } else {
                None
            };
            Ok(Value::Enum(EnumValue {
                name: name.to_string(),
                value: val,
            }))
        }
        other => Err(DataError::new(
            DataErrorCode::Serialization,
            format!("unknown value type tag `{other}`"),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_json_with_base64_bytes_encodes_bytes_only() {
        let value = Value::Struct(vec![
            StructFieldValue {
                name: "xyz".into(),
                value: Value::Bytes(vec![0u8, 1, 2, 250, 255].into()),
            },
            StructFieldValue {
                name: "count".into(),
                value: Value::Int(5),
            },
            StructFieldValue {
                name: "nested".into(),
                value: Value::List(vec![Value::Bytes(Vec::<u8>::new().into())]),
            },
        ]);
        let base64 = to_plain_json_with(
            &value,
            PlainJsonOptions {
                bytes: BytesEncoding::Base64,
            },
        );
        assert_eq!(base64["xyz"], JsonValue::String("AAEC+v8=".into()));
        assert_eq!(base64["count"], JsonValue::from(5));
        assert_eq!(base64["nested"][0], JsonValue::String(String::new()));
        // The default options are `to_plain_json` exactly.
        assert_eq!(
            to_plain_json_with(&value, PlainJsonOptions::default()),
            to_plain_json(&value)
        );
        assert_eq!(
            to_plain_json(&value)["xyz"],
            serde_json::json!([0, 1, 2, 250, 255])
        );
    }

    #[test]
    fn plain_json_renders_compound_values() {
        let value = Value::Struct(vec![
            StructFieldValue {
                name: "label".into(),
                value: Value::String("cam".into()),
            },
            StructFieldValue {
                name: "size".into(),
                value: Value::Tuple(vec![Value::Int(640), Value::Int(480)]),
            },
            StructFieldValue {
                name: "meta".into(),
                value: Value::Map(vec![(Value::String("fps".into()), Value::Float(30.0))]),
            },
            StructFieldValue {
                name: "mode".into(),
                value: Value::Enum(EnumValue {
                    name: "Auto".into(),
                    value: None,
                }),
            },
            StructFieldValue {
                name: "nan".into(),
                value: Value::Float(f64::NAN),
            },
            StructFieldValue {
                name: "pairs".into(),
                value: Value::Map(vec![(Value::Int(1), Value::Bool(true))]),
            },
        ]);
        assert_eq!(
            to_plain_json(&value),
            serde_json::json!({
                "label": "cam",
                "size": [640, 480],
                "meta": {"fps": 30.0},
                "mode": {"name": "Auto"},
                "nan": null,
                "pairs": [[1, true]],
            })
        );
        assert_eq!(to_plain_json(&Value::Unit), serde_json::Value::Null);
        assert_eq!(
            to_plain_json(&Value::Bytes(vec![1, 2].into())),
            serde_json::json!([1, 2])
        );
    }

    #[test]
    fn plain_json_round_trips_scalars_lists_and_string_maps() {
        let value = Value::Map(vec![
            (Value::String("fps".into()), Value::Float(29.5)),
            (Value::String("label".into()), Value::String("cam".into())),
            (Value::String("n".into()), Value::Int(-3)),
            (
                Value::String("tags".into()),
                Value::List(vec![Value::Bool(true), Value::Unit]),
            ),
        ]);
        let json = to_plain_json(&value);
        assert_eq!(from_plain_json(&json).expect("decode"), value);
        assert_eq!(
            from_plain_json(&serde_json::json!(2.0)).expect("float"),
            Value::Float(2.0)
        );
    }

    #[test]
    fn round_trip_value() {
        let v = Value::String("hi".into());
        let json = to_json(&v).expect("json");
        let back = from_json(&json).expect("back");
        assert_eq!(v, back);
    }

    #[test]
    fn round_trip_bytes() {
        let v = Value::Bytes(vec![1, 2, 3, 42].into());
        let json = to_json(&v).expect("json");
        let back = from_json(&json).expect("back");
        assert_eq!(v, back);
        assert!(json.contains("AQIDKg"));
    }

    #[test]
    fn errors_on_invalid_json() {
        let err = from_json("not-json").unwrap_err();
        assert_eq!(err.code(), DataErrorCode::Serialization);
    }

    #[test]
    fn errors_on_wrong_type_tag() {
        let err = decode_value(&serde_json::json!({"type":"nope","value":1})).unwrap_err();
        assert_eq!(err.code(), DataErrorCode::Serialization);
    }

    #[test]
    fn rejects_non_finite_float() {
        let err = to_json(&Value::Float(f64::INFINITY)).unwrap_err();
        assert_eq!(err.code(), DataErrorCode::Serialization);
    }

    #[test]
    fn rejects_bad_base64() {
        let err = decode_value(&serde_json::json!({"type":"bytes","value":"!!!"})).unwrap_err();
        assert_eq!(err.code(), DataErrorCode::Serialization);
    }

    #[test]
    fn deterministic_field_order() {
        let json = to_json(&Value::Int(5)).unwrap();
        // Expect "type" before "value" to keep goldens stable.
        assert!(json.find("\"type\"").unwrap() < json.find("\"value\"").unwrap());
    }

    #[test]
    fn round_trip_struct_and_enum() {
        let v = Value::Struct(vec![
            StructFieldValue {
                name: "a".into(),
                value: Value::Int(1),
            },
            StructFieldValue {
                name: "b".into(),
                value: Value::Bool(true),
            },
        ]);
        let json = to_json(&v).unwrap();
        let back = from_json(&json).unwrap();
        assert_eq!(back, v);

        let e = Value::Enum(EnumValue {
            name: "ok".into(),
            value: Some(Box::new(Value::Int(2))),
        });
        let json = to_json(&e).unwrap();
        let back = from_json(&json).unwrap();
        assert_eq!(back, e);
    }
}
