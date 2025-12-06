use serde::{Deserialize, Serialize};

use crate::errors::{DataError, DataErrorCode, DataResult};
use crate::model::{TypeExpr, Value, ValueType};

/// GPU-related hints carried on descriptors.
///
/// ```
/// use daedalus_data::descriptor::{GpuHints, MemoryLocation};
/// let hints = GpuHints { requires_gpu: false, preferred_memory: Some(MemoryLocation::Host) };
/// assert_eq!(hints.preferred_memory, Some(MemoryLocation::Host));
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct GpuHints {
    pub requires_gpu: bool,
    pub preferred_memory: Option<MemoryLocation>,
}

/// Memory location hint for GPU-aware values.
///
/// ```
/// use daedalus_data::descriptor::MemoryLocation;
/// let loc = MemoryLocation::Device;
/// assert_eq!(loc, MemoryLocation::Device);
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MemoryLocation {
    Host,
    Device,
    Shared,
}

/// Descriptor for values/types.
///
/// ```
/// use daedalus_data::descriptor::{DataDescriptor, DescriptorId, DescriptorVersion};
/// let desc = DataDescriptor {
///     id: DescriptorId::new("example"),
///     version: DescriptorVersion::new("1.0.0"),
///     label: None,
///     settable: false,
///     default: None,
///     schema: None,
///     codecs: vec![],
///     converters: vec![],
///     feature_flags: vec![],
///     gpu: None,
///     type_expr: None,
/// };
/// assert_eq!(desc.id.0, "example");
/// ```
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DataDescriptor {
    pub id: DescriptorId,
    pub version: DescriptorVersion,
    pub label: Option<String>,
    pub settable: bool,
    pub default: Option<Value>,
    pub schema: Option<String>,
    pub codecs: Vec<String>,
    pub converters: Vec<String>,
    pub feature_flags: Vec<String>,
    pub gpu: Option<GpuHints>,
    pub type_expr: Option<TypeExpr>,
}

impl DataDescriptor {
    /// Validate the descriptor, including type/default compatibility.
    ///
    /// ```
    /// use daedalus_data::descriptor::{DataDescriptor, DescriptorId, DescriptorVersion};
    /// let desc = DataDescriptor {
    ///     id: DescriptorId::new("example"),
    ///     version: DescriptorVersion::new("1.0"),
    ///     label: None,
    ///     settable: false,
    ///     default: None,
    ///     schema: None,
    ///     codecs: vec![],
    ///     converters: vec![],
    ///     feature_flags: vec![],
    ///     gpu: None,
    ///     type_expr: None,
    /// };
    /// desc.validate().unwrap();
    /// ```
    pub fn validate(&self) -> DataResult<()> {
        self.id.validate()?;
        self.version.validate()?;
        if let Some(default) = &self.default {
            if self.type_expr.is_none() {
                return Err(DataError::new(
                    DataErrorCode::InvalidDescriptor,
                    "type_expr is required when default is present",
                ));
            }
            validate_default(self.type_expr.as_ref().unwrap(), default)?;
        }
        Ok(())
    }

    /// Deterministic ordering for codecs/converters/feature flags.
    ///
    /// ```
    /// use daedalus_data::descriptor::{DataDescriptor, DescriptorId, DescriptorVersion};
    /// let desc = DataDescriptor {
    ///     id: DescriptorId::new("id"),
    ///     version: DescriptorVersion::new("1.0"),
    ///     label: None,
    ///     settable: false,
    ///     default: None,
    ///     schema: None,
    ///     codecs: vec!["b".into(), "a".into()],
    ///     converters: vec!["y".into(), "x".into()],
    ///     feature_flags: vec!["b".into(), "a".into()],
    ///     gpu: None,
    ///     type_expr: None,
    /// };
    /// let sorted = desc.normalize();
    /// assert_eq!(sorted.codecs, vec!["a", "b"]);
    /// ```
    pub fn normalize(mut self) -> Self {
        self.codecs.sort();
        self.converters.sort();
        self.feature_flags.sort();
        self
    }
}

/// Builder to construct descriptors with deterministic ordering.
///
/// ```
/// use daedalus_data::descriptor::{DescriptorBuilder, GpuHints, MemoryLocation};
/// use daedalus_data::errors::DataResult;
/// use daedalus_data::model::{TypeExpr, Value, ValueType};
///
/// fn build_descriptor() -> DataResult<()> {
///     let desc = DescriptorBuilder::new("example", "1.0.0")
///         .label("Example")
///         .settable(true)
///         .type_expr(TypeExpr::Scalar(ValueType::String))
///         .default(Value::String("hi".into()))
///         .codec("json")
///         .feature_flag("core")
///         .gpu_hints(GpuHints { requires_gpu: false, preferred_memory: Some(MemoryLocation::Host) })
///         .build()?;
///     assert_eq!(desc.codecs, vec!["json"]);
///     Ok(())
/// }
/// ```
pub struct DescriptorBuilder {
    inner: DataDescriptor,
}

impl DescriptorBuilder {
    pub fn new(id: impl Into<String>, version: impl Into<String>) -> Self {
        Self {
            inner: DataDescriptor {
                id: DescriptorId::new(id.into()),
                version: DescriptorVersion::new(version.into()),
                label: None,
                settable: false,
                default: None,
                schema: None,
                codecs: Vec::new(),
                converters: Vec::new(),
                feature_flags: Vec::new(),
                gpu: None,
                type_expr: None,
            },
        }
    }

    pub fn label(mut self, label: impl Into<String>) -> Self {
        self.inner.label = Some(label.into());
        self
    }

    pub fn settable(mut self, settable: bool) -> Self {
        self.inner.settable = settable;
        self
    }

    pub fn default(mut self, default: Value) -> Self {
        self.inner.default = Some(default);
        self
    }

    pub fn schema(mut self, schema: impl Into<String>) -> Self {
        self.inner.schema = Some(schema.into());
        self
    }

    pub fn codec(mut self, codec: impl Into<String>) -> Self {
        self.inner.codecs.push(codec.into());
        self
    }

    pub fn converter(mut self, conv: impl Into<String>) -> Self {
        self.inner.converters.push(conv.into());
        self
    }

    pub fn feature_flag(mut self, flag: impl Into<String>) -> Self {
        self.inner.feature_flags.push(flag.into());
        self
    }

    pub fn gpu_hints(mut self, hints: GpuHints) -> Self {
        self.inner.gpu = Some(hints);
        self
    }

    pub fn type_expr(mut self, ty: TypeExpr) -> Self {
        self.inner.type_expr = Some(ty.normalize());
        self
    }

    pub fn build(self) -> DataResult<DataDescriptor> {
        let desc = self.inner.normalize();
        desc.validate()?;
        Ok(desc)
    }
}

/// Descriptor for a type expression with associated metadata.
///
/// ```
/// use daedalus_data::descriptor::{DataDescriptor, DescriptorId, DescriptorVersion, TypeDescriptor};
/// use daedalus_data::model::{TypeExpr, ValueType};
/// let desc = DataDescriptor {
///     id: DescriptorId::new("demo"),
///     version: DescriptorVersion::new("1.0.0"),
///     label: None,
///     settable: false,
///     default: None,
///     schema: None,
///     codecs: vec![],
///     converters: vec![],
///     feature_flags: vec![],
///     gpu: None,
///     type_expr: None,
/// };
/// let typed = TypeDescriptor { ty: TypeExpr::Scalar(ValueType::Int), descriptor: desc };
/// assert!(matches!(typed.ty, TypeExpr::Scalar(_)));
/// ```
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TypeDescriptor {
    pub ty: TypeExpr,
    pub descriptor: DataDescriptor,
}

/// Strongly typed descriptor id with basic namespace validation.
///
/// ```
/// use daedalus_data::descriptor::DescriptorId;
/// let id = DescriptorId::namespaced("sensor", "temp");
/// assert_eq!(id.0, "sensor.temp");
/// ```
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Hash)]
pub struct DescriptorId(pub String);

impl DescriptorId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn namespaced(namespace: impl Into<String>, name: impl Into<String>) -> Self {
        let ns = namespace.into();
        let name = name.into();
        if ns.is_empty() {
            return Self(name);
        }
        Self(format!("{ns}.{name}"))
    }
    pub fn validate(&self) -> DataResult<()> {
        if self.0.is_empty() {
            return Err(DataError::new(
                DataErrorCode::InvalidDescriptor,
                "id must not be empty",
            ));
        }
        if !self.0.chars().all(|c| {
            c.is_ascii_lowercase() || c.is_ascii_digit() || c == '.' || c == '_' || c == '-'
        }) {
            return Err(DataError::new(
                DataErrorCode::InvalidDescriptor,
                "id must be lowercase/digit/._-",
            ));
        }
        Ok(())
    }
}

/// Strongly typed semantic version string.
///
/// ```
/// use daedalus_data::descriptor::DescriptorVersion;
/// let ver = DescriptorVersion::new("1.2.3");
/// assert_eq!(ver.0, "1.2.3");
/// ```
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Hash)]
pub struct DescriptorVersion(pub String);

impl DescriptorVersion {
    pub fn new(v: impl Into<String>) -> Self {
        Self(v.into())
    }
    pub fn validate(&self) -> DataResult<()> {
        let parts: Vec<_> = self.0.split('.').collect();
        if parts.len() < 2 {
            return Err(DataError::new(
                DataErrorCode::InvalidDescriptor,
                "version must be at least major.minor",
            ));
        }
        if parts
            .iter()
            .any(|p| p.is_empty() || p.chars().any(|c| !c.is_ascii_digit()))
        {
            return Err(DataError::new(
                DataErrorCode::InvalidDescriptor,
                "version segments must be numeric",
            ));
        }
        Ok(())
    }
}

fn validate_default(ty: &TypeExpr, value: &Value) -> DataResult<()> {
    match (ty, value) {
        (TypeExpr::Scalar(ValueType::Unit), Value::Unit) => Ok(()),
        (TypeExpr::Scalar(ValueType::Bool), Value::Bool(_)) => Ok(()),
        (TypeExpr::Scalar(ValueType::I32 | ValueType::U32 | ValueType::Int), Value::Int(_)) => {
            Ok(())
        }
        (TypeExpr::Scalar(ValueType::F32 | ValueType::Float), Value::Float(_)) => Ok(()),
        (TypeExpr::Scalar(ValueType::String), Value::String(_)) => Ok(()),
        (TypeExpr::Scalar(ValueType::Bytes), Value::Bytes(_)) => Ok(()),
        (TypeExpr::Optional(inner), v) => validate_default(inner, v),
        (TypeExpr::List(inner), Value::List(items)) => {
            for v in items {
                validate_default(inner, v)?;
            }
            Ok(())
        }
        (TypeExpr::Map(k_ty, v_ty), Value::Map(entries)) => {
            for (k, v) in entries {
                validate_default(k_ty, k)?;
                validate_default(v_ty, v)?;
            }
            Ok(())
        }
        (TypeExpr::Tuple(types), Value::Tuple(values)) => {
            if types.len() != values.len() {
                return Err(DataError::new(
                    DataErrorCode::InvalidType,
                    "tuple length mismatch",
                ));
            }
            for (t, v) in types.iter().zip(values.iter()) {
                validate_default(t, v)?;
            }
            Ok(())
        }
        (TypeExpr::Struct(fields), Value::Struct(values)) => {
            if fields.len() != values.len() {
                return Err(DataError::new(
                    DataErrorCode::InvalidType,
                    "struct field count mismatch",
                ));
            }
            for (field, val) in fields.iter().zip(values.iter()) {
                if field.name != val.name {
                    return Err(DataError::new(
                        DataErrorCode::InvalidType,
                        "struct field name mismatch",
                    ));
                }
                validate_default(&field.ty, &val.value)?;
            }
            Ok(())
        }
        (TypeExpr::Enum(variants), Value::Enum(ev)) => {
            let variant = variants.iter().find(|v| v.name == ev.name).ok_or_else(|| {
                DataError::new(DataErrorCode::InvalidType, "enum variant not found")
            })?;
            match (&variant.ty, &ev.value) {
                (None, None) => Ok(()),
                (Some(t), Some(v)) => validate_default(t, v),
                (None, Some(_)) | (Some(_), None) => Err(DataError::new(
                    DataErrorCode::InvalidType,
                    "enum payload mismatch",
                )),
            }
        }
        _ => Err(DataError::new(
            DataErrorCode::InvalidType,
            "default does not match type_expr",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{StructField, StructFieldValue};

    #[test]
    fn normalize_sorts_fields() {
        let desc = DataDescriptor {
            id: DescriptorId::new("id"),
            version: DescriptorVersion::new("v1"),
            label: None,
            settable: true,
            default: None,
            schema: None,
            codecs: vec!["b".into(), "a".into()],
            converters: vec!["y".into(), "x".into()],
            feature_flags: vec!["f2".into(), "f1".into()],
            gpu: None,
            type_expr: None,
        }
        .normalize();
        assert_eq!(desc.codecs, vec!["a", "b"]);
        assert_eq!(desc.converters, vec!["x", "y"]);
        assert_eq!(desc.feature_flags, vec!["f1", "f2"]);
    }

    #[test]
    fn serde_preserves_sorted_order() {
        let desc = DescriptorBuilder::new("id", "1.0")
            .codec("z")
            .codec("a")
            .converter("b")
            .converter("a")
            .feature_flag("beta")
            .feature_flag("alpha")
            .build()
            .expect("build");
        let json = serde_json::to_string(&desc).unwrap();
        assert!(json.find("a").unwrap() < json.find("z").unwrap());
        assert!(json.find("alpha").unwrap() < json.find("beta").unwrap());
    }

    /// Minimal registry fixture showing `(id, version)` uniqueness and conflict diagnostics.
    ///
    /// ```
    /// use std::collections::HashMap;
    /// use daedalus_data::descriptor::{DataDescriptor, DescriptorBuilder, DescriptorId, DescriptorVersion};
    ///
    /// #[derive(Default)]
    /// struct Registry {
    ///     entries: HashMap<(DescriptorId, DescriptorVersion), DataDescriptor>,
    /// }
    ///
    /// impl Registry {
    ///     fn register(&mut self, desc: DataDescriptor) -> Result<(), String> {
    ///         let key = (desc.id.clone(), desc.version.clone());
    ///         if self.entries.contains_key(&key) {
    ///             return Err(format!("duplicate descriptor {:?},{}", key.0, key.1));
    ///         }
    ///         self.entries.insert(key, desc);
    ///         Ok(())
    ///     }
    /// }
    ///
    /// let mut reg = Registry::default();
    /// let desc = DescriptorBuilder::new("sensor.temp", "1.0.0").build().unwrap();
    /// reg.register(desc.clone()).unwrap();
    /// assert!(reg.register(desc).is_err());
    /// ```
    #[test]
    fn registry_fixture_compiles() {
        // Doc-test above is the primary fixture; keep this test as a placeholder.
    }

    #[test]
    fn golden_descriptor_serialization_is_stable() {
        let desc = DescriptorBuilder::new("id", "1.0")
            .label("Example")
            .settable(true)
            .codec("json")
            .converter("int_to_string")
            .feature_flag("core")
            .build()
            .expect("build");
        let json = serde_json::to_string(&desc).unwrap();
        assert_eq!(
            json,
            r#"{"id":"id","version":"1.0","label":"Example","settable":true,"default":null,"schema":null,"codecs":["json"],"converters":["int_to_string"],"feature_flags":["core"],"gpu":null,"type_expr":null}"#
        );
    }

    #[test]
    fn validates_default_against_type() {
        let desc = DescriptorBuilder::new("id", "1.0")
            .type_expr(TypeExpr::Scalar(ValueType::String))
            .default(Value::String("ok".into()))
            .build()
            .unwrap();
        assert_eq!(desc.id.0, "id");

        let err = DescriptorBuilder::new("id2", "1.0")
            .type_expr(TypeExpr::Scalar(ValueType::Int))
            .default(Value::Bool(true))
            .build()
            .unwrap_err();
        assert_eq!(err.code(), DataErrorCode::InvalidType);

        let err = DescriptorBuilder::new("id3", "1.0")
            .type_expr(TypeExpr::Struct(vec![StructField {
                name: "a".into(),
                ty: TypeExpr::Scalar(ValueType::Int),
            }]))
            .default(Value::Struct(vec![StructFieldValue {
                name: "a".into(),
                value: Value::Int(1),
            }]))
            .build()
            .unwrap();
        assert_eq!(err.id.0, "id3");
    }
}
