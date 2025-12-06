//! Proto emission (feature-gated).

use crate::errors::{DataError, DataErrorCode, DataResult};
use crate::model::{EnumVariant, StructField, TypeExpr, ValueType};

/// Render a `TypeExpr` into a proto3 type string. Map keys must be strings;
/// tuples are not yet supported and return `UnsupportedFeature`.
pub fn to_proto_type(expr: &TypeExpr) -> DataResult<String> {
    match expr {
        TypeExpr::Scalar(ValueType::Unit) => Ok("google.protobuf.Empty".into()),
        TypeExpr::Scalar(ValueType::Bool) => Ok("bool".into()),
        TypeExpr::Scalar(ValueType::I32) => Ok("int32".into()),
        TypeExpr::Scalar(ValueType::U32) => Ok("uint32".into()),
        TypeExpr::Scalar(ValueType::Int) => Ok("int64".into()),
        TypeExpr::Scalar(ValueType::F32) => Ok("float".into()),
        TypeExpr::Scalar(ValueType::Float) => Ok("double".into()),
        TypeExpr::Scalar(ValueType::String) => Ok("string".into()),
        TypeExpr::Scalar(ValueType::Bytes) => Ok("bytes".into()),
        TypeExpr::Opaque(_) => Ok("google.protobuf.Any".into()),
        TypeExpr::Optional(inner) => Ok(format!("optional {}", to_proto_type(inner)?)),
        TypeExpr::List(inner) => Ok(format!("repeated {}", to_proto_type(inner)?)),
        TypeExpr::Map(key, value) => {
            if **key != TypeExpr::Scalar(ValueType::String) {
                return Err(DataError::new(
                    DataErrorCode::UnsupportedFeature,
                    "proto map keys must be strings",
                ));
            }
            Ok(format!("map<string, {}>", to_proto_type(value)?))
        }
        TypeExpr::Tuple(_) => Err(DataError::new(
            DataErrorCode::UnsupportedFeature,
            "tuple proto emission is not supported yet",
        )),
        TypeExpr::Struct(fields) => {
            let mut sorted = fields.clone();
            sorted.sort_by(|a, b| a.name.cmp(&b.name));
            let mut parts = Vec::new();
            for (idx, StructField { name, ty }) in sorted.into_iter().enumerate() {
                parts.push(format!("{}: {} = {}", name, to_proto_type(&ty)?, idx + 1));
            }
            Ok(format!("message {{ {} }}", parts.join("; ")))
        }
        TypeExpr::Enum(variants) => {
            let mut sorted = variants.clone();
            sorted.sort_by(|a, b| a.name.cmp(&b.name));
            let mut parts = Vec::new();
            for EnumVariant { name, ty } in sorted {
                match ty {
                    Some(t) => parts.push(format!("{}: {};", name, to_proto_type(&t)?)),
                    None => parts.push(name.to_string()),
                }
            }
            Ok(format!("oneof {{ {} }}", parts.join(" ")))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_repeated_type() {
        let ty = TypeExpr::List(Box::new(TypeExpr::Scalar(ValueType::Bool)));
        assert_eq!(to_proto_type(&ty).unwrap(), "repeated bool");
    }

    #[test]
    fn rejects_non_string_keys() {
        let ty = TypeExpr::Map(
            Box::new(TypeExpr::Scalar(ValueType::Int)),
            Box::new(TypeExpr::Scalar(ValueType::String)),
        );
        let err = to_proto_type(&ty).unwrap_err();
        assert_eq!(err.code(), DataErrorCode::UnsupportedFeature);
    }

    #[test]
    fn renders_struct_message() {
        let ty = TypeExpr::Struct(vec![
            StructField {
                name: "b".into(),
                ty: TypeExpr::Scalar(ValueType::Float),
            },
            StructField {
                name: "a".into(),
                ty: TypeExpr::Scalar(ValueType::Int),
            },
        ]);
        let rendered = to_proto_type(&ty).unwrap();
        assert!(rendered.contains("message"));
        assert!(rendered.contains("a: int64"));
        assert!(rendered.contains("b: double"));
    }

    #[test]
    fn renders_enum_oneof_like() {
        let ty = TypeExpr::Enum(vec![
            EnumVariant {
                name: "ready".into(),
                ty: None,
            },
            EnumVariant {
                name: "error".into(),
                ty: Some(TypeExpr::Scalar(ValueType::String)),
            },
        ]);
        let rendered = to_proto_type(&ty).unwrap();
        assert!(rendered.contains("oneof"));
        assert!(rendered.contains("ready"));
        assert!(rendered.contains("error"));
    }
}
