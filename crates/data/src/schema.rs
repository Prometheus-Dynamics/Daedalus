//! Schema emission (feature-gated).

use crate::errors::{DataError, DataErrorCode, DataResult};
use crate::model::{EnumVariant, StructField, TypeExpr, ValueType};

/// Convert a `TypeExpr` into a JSON Schema fragment.
pub fn type_to_json_schema(ty: &TypeExpr) -> DataResult<serde_json::Value> {
    use serde_json::json;

    let schema = match ty {
        TypeExpr::Scalar(ValueType::Unit) => json!({ "type": "null" }),
        TypeExpr::Scalar(ValueType::Bool) => json!({ "type": "boolean" }),
        TypeExpr::Scalar(ValueType::I32 | ValueType::U32 | ValueType::Int) => {
            json!({ "type": "integer" })
        }
        TypeExpr::Scalar(ValueType::F32 | ValueType::Float) => json!({ "type": "number" }),
        TypeExpr::Scalar(ValueType::String) => json!({ "type": "string" }),
        TypeExpr::Scalar(ValueType::Bytes) => {
            json!({ "type": "string", "contentEncoding": "base64" })
        }
        TypeExpr::Opaque(name) => json!({
            "type": "object",
            "description": format!("opaque type: {name}"),
        }),
        TypeExpr::Optional(inner) => {
            json!({ "anyOf": [ type_to_json_schema(inner)?, { "type": "null" } ] })
        }
        TypeExpr::List(inner) => json!({
            "type": "array",
            "items": type_to_json_schema(inner)?,
        }),
        TypeExpr::Map(key, value) => {
            if **key != TypeExpr::Scalar(ValueType::String) {
                return Err(DataError::new(
                    DataErrorCode::UnsupportedFeature,
                    "JSON Schema map keys must be strings",
                ));
            }
            json!({
                "type": "object",
                "additionalProperties": type_to_json_schema(value)?,
            })
        }
        TypeExpr::Tuple(items) => {
            let mut prefix = Vec::new();
            for item in items {
                prefix.push(type_to_json_schema(item)?);
            }
            json!({
                "type": "array",
                "prefixItems": prefix,
                "items": false
            })
        }
        TypeExpr::Struct(fields) => {
            let mut sorted = fields.clone();
            sorted.sort_by(|a, b| a.name.cmp(&b.name));
            let mut props = serde_json::Map::new();
            let mut required = Vec::new();
            for StructField { name, ty } in sorted {
                props.insert(name.clone(), type_to_json_schema(&ty)?);
                required.push(name);
            }
            json!({
                "type": "object",
                "properties": props,
                "required": required,
                "additionalProperties": false
            })
        }
        TypeExpr::Enum(variants) => {
            let mut sorted = variants.clone();
            sorted.sort_by(|a, b| a.name.cmp(&b.name));
            let mut choices = Vec::new();
            for EnumVariant { name, ty } in sorted {
                match ty {
                    Some(payload) => {
                        choices.push(json!({
                            "type": "object",
                            "properties": {
                                "tag": { "const": name },
                                "value": type_to_json_schema(&payload)?
                            },
                            "required": ["tag", "value"],
                            "additionalProperties": false
                        }));
                    }
                    None => {
                        choices.push(json!({ "type": "string", "const": name }));
                    }
                }
            }
            json!({ "anyOf": choices })
        }
    };
    Ok(schema)
}

/// JSON Schema for the serde form of [`Value`](crate::model::Value)
/// (`{"type": "<Variant>", "value": <payload>}`).
///
/// Nested values refer back to this schema through `self_ref`, the JSON pointer where the caller
/// embeds it (for example `"#/$defs/value"`).
pub fn value_json_schema(self_ref: &str) -> serde_json::Value {
    use serde_json::json;

    let value = json!({ "$ref": self_ref });
    let values = json!({ "type": "array", "items": value });
    let variant = |tag: &str, payload: Option<serde_json::Value>| {
        let mut schema = json!({
            "type": "object",
            "properties": { "type": { "const": tag } },
            "required": ["type"],
        });
        if let Some(payload) = payload {
            schema["properties"]["value"] = payload;
            schema["required"] = json!(["type", "value"]);
        }
        schema
    };
    json!({
        "description": "A Daedalus value in its tagged form: {\"type\": <variant>, \"value\": <payload>}.",
        "oneOf": [
            variant("Unit", None),
            variant("Bool", Some(json!({ "type": "boolean" }))),
            variant("Int", Some(json!({ "type": "integer" }))),
            variant("Float", Some(json!({ "type": "number" }))),
            variant("String", Some(json!({ "type": "string" }))),
            variant("Bytes", Some(json!({
                "type": "array",
                "items": { "type": "integer", "minimum": 0, "maximum": 255 },
            }))),
            variant("List", Some(values.clone())),
            variant("Map", Some(json!({
                "type": "array",
                "items": { "type": "array", "prefixItems": [value, value], "items": false, "minItems": 2 },
            }))),
            variant("Tuple", Some(values)),
            variant("Struct", Some(json!({
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": { "name": { "type": "string" }, "value": value },
                    "required": ["name", "value"],
                },
            }))),
            variant("Enum", Some(json!({
                "type": "object",
                "properties": {
                    "name": { "type": "string" },
                    "value": { "anyOf": [value, { "type": "null" }] },
                },
                "required": ["name"],
            }))),
        ],
    })
}

/// Pretty-printed JSON Schema string.
pub fn to_json_schema_string(ty: &TypeExpr) -> DataResult<String> {
    let schema = type_to_json_schema(ty)?;
    serde_json::to_string_pretty(&schema)
        .map_err(|e| DataError::new(DataErrorCode::Serialization, e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn value_schema_covers_every_serialized_variant() {
        use crate::model::{EnumValue, Value};

        let schema = value_json_schema("#");
        let tags: Vec<&str> = schema["oneOf"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v["properties"]["type"]["const"].as_str().unwrap())
            .collect();
        let samples = [
            Value::Unit,
            Value::Bool(true),
            Value::Int(1),
            Value::Float(1.5),
            Value::String("s".into()),
            Value::Bytes(vec![1u8].into()),
            Value::List(vec![]),
            Value::Map(vec![]),
            Value::Tuple(vec![]),
            Value::Struct(vec![]),
            Value::Enum(EnumValue {
                name: "e".into(),
                value: None,
            }),
        ];
        let serialized: Vec<String> = samples
            .iter()
            .map(|v| {
                serde_json::to_value(v).unwrap()["type"]
                    .as_str()
                    .unwrap()
                    .to_owned()
            })
            .collect();
        assert_eq!(tags, serialized);
        let bytes = serde_json::to_value(Value::Bytes(vec![7u8].into())).unwrap();
        assert_eq!(bytes["value"], serde_json::json!([7]));
    }

    #[test]
    fn emits_basic_schema() {
        let schema = type_to_json_schema(&TypeExpr::Scalar(ValueType::Bool)).unwrap();
        assert_eq!(schema, serde_json::json!({"type": "boolean"}));
    }

    #[test]
    fn rejects_non_string_map_keys() {
        let err = type_to_json_schema(&TypeExpr::Map(
            Box::new(TypeExpr::Scalar(ValueType::Int)),
            Box::new(TypeExpr::Scalar(ValueType::String)),
        ))
        .unwrap_err();
        assert_eq!(err.code(), DataErrorCode::UnsupportedFeature);
    }

    #[test]
    fn emits_struct_schema_sorted_fields() {
        let schema = type_to_json_schema(&TypeExpr::Struct(vec![
            StructField {
                name: "b".into(),
                ty: TypeExpr::Scalar(ValueType::Bool),
            },
            StructField {
                name: "a".into(),
                ty: TypeExpr::Scalar(ValueType::Int),
            },
        ]))
        .unwrap();
        assert_eq!(
            schema,
            serde_json::json!({
                "type": "object",
                "properties": {
                    "a": { "type": "integer" },
                    "b": { "type": "boolean" }
                },
                "required": ["a", "b"],
                "additionalProperties": false
            })
        );
    }

    #[test]
    fn emits_enum_schema_with_payload() {
        let schema = type_to_json_schema(&TypeExpr::Enum(vec![
            EnumVariant {
                name: "unit".into(),
                ty: None,
            },
            EnumVariant {
                name: "payload".into(),
                ty: Some(TypeExpr::Scalar(ValueType::String)),
            },
        ]))
        .unwrap();
        // Ensure both variants are represented
        let any_of = schema.get("anyOf").unwrap().as_array().unwrap();
        assert_eq!(any_of.len(), 2);
    }
}
