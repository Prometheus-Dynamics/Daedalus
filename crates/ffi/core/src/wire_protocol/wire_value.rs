use std::borrow::Cow;
use std::collections::BTreeMap;

use daedalus_data::model::{EnumValue, StructFieldValue, Value};
use daedalus_transport::{AccessMode, Layout, Payload, Residency, TypeKey};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::InvokeContractError;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum WireValue {
    Unit,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Bytes(BytePayload),
    Image(ImagePayload),
    Handle(WirePayloadHandle),
    List(Vec<WireValue>),
    Record(BTreeMap<String, WireValue>),
    Enum(WireEnumValue),
}

impl WireValue {
    pub fn validate_contract(&self) -> Result<(), InvokeContractError> {
        match self {
            WireValue::Unit
            | WireValue::Bool(_)
            | WireValue::Int(_)
            | WireValue::Float(_)
            | WireValue::String(_)
            | WireValue::Bytes(_) => Ok(()),
            WireValue::Image(image) => {
                if image.width == 0 {
                    return Err(InvokeContractError::InvalidImage { field: "width" });
                }
                if image.height == 0 {
                    return Err(InvokeContractError::InvalidImage { field: "height" });
                }
                if image.channels == 0 {
                    return Err(InvokeContractError::InvalidImage { field: "channels" });
                }
                Ok(())
            }
            WireValue::Handle(handle) => {
                if handle.id.trim().is_empty() {
                    return Err(InvokeContractError::EmptyField { field: "handle.id" });
                }
                Ok(())
            }
            WireValue::List(items) => {
                for item in items {
                    item.validate_contract()?;
                }
                Ok(())
            }
            WireValue::Record(fields) => {
                for (name, value) in fields {
                    if name.trim().is_empty() {
                        return Err(InvokeContractError::EmptyField {
                            field: "record.field",
                        });
                    }
                    value.validate_contract()?;
                }
                Ok(())
            }
            WireValue::Enum(value) => {
                if value.name.trim().is_empty() {
                    return Err(InvokeContractError::EmptyField { field: "enum.name" });
                }
                if let Some(value) = &value.value {
                    value.validate_contract()?;
                }
                Ok(())
            }
        }
    }

    pub fn into_value(self) -> Result<Value, WireValueConversionError> {
        self.try_into()
    }

    pub fn from_value(value: Value) -> Result<Self, WireValueConversionError> {
        value.try_into()
    }

    pub fn into_payload(
        self,
        type_key: impl Into<TypeKey>,
    ) -> Result<Payload, WireValueConversionError> {
        let type_key = type_key.into();
        match self {
            WireValue::Bytes(payload) => Ok(Payload::bytes_with_type_key(
                type_key,
                std::sync::Arc::from(payload.data),
            )),
            value => Ok(Payload::owned(type_key, value.into_value()?)),
        }
    }

    pub fn payload_ref_from_payload(
        id: impl Into<String>,
        payload: &Payload,
        access: AccessMode,
    ) -> Self {
        WireValue::Handle(WirePayloadHandle::from_payload(id, payload, access))
    }

    pub fn from_payload(payload: &Payload) -> Result<Self, WireValueConversionError> {
        if let Some(value) = payload.get_ref::<Value>() {
            return WireValue::from_value(value.clone());
        }
        if let Some(bytes) = payload
            .value_any()
            .and_then(|value| value.downcast_ref::<std::sync::Arc<[u8]>>())
        {
            return Ok(WireValue::Bytes(BytePayload {
                data: bytes.as_ref().to_vec(),
                encoding: ByteEncoding::Raw,
            }));
        }
        Err(WireValueConversionError::UnsupportedPayload {
            type_key: payload.type_key().to_string(),
            rust_type_name: payload.storage_rust_type_name(),
        })
    }
}

impl TryFrom<WireValue> for Value {
    type Error = WireValueConversionError;

    fn try_from(value: WireValue) -> Result<Self, Self::Error> {
        Ok(match value {
            WireValue::Unit => Value::Unit,
            WireValue::Bool(value) => Value::Bool(value),
            WireValue::Int(value) => Value::Int(value),
            WireValue::Float(value) => Value::Float(value),
            WireValue::String(value) => Value::String(Cow::Owned(value)),
            WireValue::Bytes(payload) => Value::Bytes(Cow::Owned(payload.data)),
            WireValue::Image(_) => {
                return Err(WireValueConversionError::UnsupportedWireValue {
                    kind: "image",
                    target: "daedalus_data::model::Value",
                });
            }
            WireValue::Handle(_) => {
                return Err(WireValueConversionError::UnsupportedWireValue {
                    kind: "handle",
                    target: "daedalus_data::model::Value",
                });
            }
            WireValue::List(items) => Value::List(
                items
                    .into_iter()
                    .map(Value::try_from)
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            WireValue::Record(fields) => Value::Struct(
                fields
                    .into_iter()
                    .map(|(name, value)| {
                        Ok(StructFieldValue {
                            name,
                            value: value.try_into()?,
                        })
                    })
                    .collect::<Result<Vec<_>, WireValueConversionError>>()?,
            ),
            WireValue::Enum(value) => Value::Enum(EnumValue {
                name: value.name,
                value: value
                    .value
                    .map(|value| Value::try_from(*value).map(Box::new))
                    .transpose()?,
            }),
        })
    }
}

impl TryFrom<Value> for WireValue {
    type Error = WireValueConversionError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        Ok(match value {
            Value::Unit => WireValue::Unit,
            Value::Bool(value) => WireValue::Bool(value),
            Value::Int(value) => WireValue::Int(value),
            Value::Float(value) => WireValue::Float(value),
            Value::String(value) => WireValue::String(value.into_owned()),
            Value::Bytes(value) => WireValue::Bytes(BytePayload {
                data: value.into_owned(),
                encoding: ByteEncoding::Raw,
            }),
            Value::List(items) | Value::Tuple(items) => WireValue::List(
                items
                    .into_iter()
                    .map(WireValue::try_from)
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            Value::Map(entries) => {
                let mut record = BTreeMap::new();
                for (key, value) in entries {
                    let Value::String(key) = key else {
                        return Err(WireValueConversionError::UnsupportedValue {
                            kind: "map with non-string key",
                            target: "WireValue::Record",
                        });
                    };
                    record.insert(key.into_owned(), value.try_into()?);
                }
                WireValue::Record(record)
            }
            Value::Struct(fields) => WireValue::Record(
                fields
                    .into_iter()
                    .map(|field| Ok((field.name, field.value.try_into()?)))
                    .collect::<Result<BTreeMap<_, _>, WireValueConversionError>>()?,
            ),
            Value::Enum(value) => WireValue::Enum(WireEnumValue {
                name: value.name,
                value: value
                    .value
                    .map(|value| WireValue::try_from(*value).map(Box::new))
                    .transpose()?,
            }),
        })
    }
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum WireValueConversionError {
    #[error("unsupported wire value kind `{kind}` for target `{target}`")]
    UnsupportedWireValue {
        kind: &'static str,
        target: &'static str,
    },
    #[error("unsupported runtime value kind `{kind}` for target `{target}`")]
    UnsupportedValue {
        kind: &'static str,
        target: &'static str,
    },
    #[error(
        "unsupported payload `{type_key}` with storage `{rust_type_name:?}` for target `WireValue`"
    )]
    UnsupportedPayload {
        type_key: String,
        rust_type_name: Option<&'static str>,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BytePayload {
    pub data: Vec<u8>,
    #[serde(default)]
    pub encoding: ByteEncoding,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ByteEncoding {
    #[default]
    Raw,
    Base64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ImagePayload {
    pub data: Vec<u8>,
    pub width: u32,
    pub height: u32,
    pub channels: u8,
    #[serde(default)]
    pub dtype: ScalarDType,
    #[serde(default)]
    pub layout: ImageLayout,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WirePayloadHandle {
    pub id: String,
    pub type_key: TypeKey,
    #[serde(default)]
    pub access: AccessMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub residency: Option<Residency>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub layout: Option<Layout>,
    #[serde(default)]
    pub capabilities: Vec<String>,
    #[serde(default)]
    pub metadata: BTreeMap<String, serde_json::Value>,
}

impl WirePayloadHandle {
    pub fn from_payload(id: impl Into<String>, payload: &Payload, access: AccessMode) -> Self {
        let mut metadata = BTreeMap::new();
        if let Some(bytes) = payload.bytes_estimate() {
            metadata.insert("bytes_estimate".into(), serde_json::json!(bytes));
        }
        if let Some(contract) = payload.boundary_contract() {
            metadata.insert(
                "boundary_layout_hash".into(),
                serde_json::json!(contract.layout_hash.to_string()),
            );
        }
        Self {
            id: id.into(),
            type_key: payload.type_key().clone(),
            access,
            residency: Some(payload.residency()),
            layout: payload.layout().cloned(),
            capabilities: Vec::new(),
            metadata,
        }
    }
}

#[cfg(feature = "image-payload")]
impl ImagePayload {
    pub fn validate(&self) -> Result<(), ImagePayloadValidationError> {
        if self.width == 0 {
            return Err(ImagePayloadValidationError::InvalidDimension {
                field: "width",
                value: self.width,
            });
        }
        if self.height == 0 {
            return Err(ImagePayloadValidationError::InvalidDimension {
                field: "height",
                value: self.height,
            });
        }
        if !(1..=4).contains(&self.channels) {
            return Err(ImagePayloadValidationError::InvalidChannels {
                channels: self.channels,
            });
        }
        let expected = self.expected_data_len()?;
        if self.data.len() != expected {
            return Err(ImagePayloadValidationError::InvalidDataLength {
                expected,
                actual: self.data.len(),
            });
        }
        Ok(())
    }

    pub fn expected_data_len(&self) -> Result<usize, ImagePayloadValidationError> {
        let width =
            usize::try_from(self.width).map_err(|_| ImagePayloadValidationError::Overflow)?;
        let height =
            usize::try_from(self.height).map_err(|_| ImagePayloadValidationError::Overflow)?;
        width
            .checked_mul(height)
            .and_then(|pixels| pixels.checked_mul(usize::from(self.channels)))
            .and_then(|samples| samples.checked_mul(self.dtype.bytes_per_sample()))
            .ok_or(ImagePayloadValidationError::Overflow)
    }
}

#[cfg(feature = "image-payload")]
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum ImagePayloadValidationError {
    #[error("image {field} must be greater than zero, found {value}")]
    InvalidDimension { field: &'static str, value: u32 },
    #[error("image channels must be between 1 and 4, found {channels}")]
    InvalidChannels { channels: u8 },
    #[error("image data length mismatch: expected {expected} bytes, found {actual}")]
    InvalidDataLength { expected: usize, actual: usize },
    #[error("image byte length overflow")]
    Overflow,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ScalarDType {
    #[default]
    U8,
    U16,
    F32,
}

#[cfg(feature = "image-payload")]
impl ScalarDType {
    pub fn bytes_per_sample(self) -> usize {
        match self {
            ScalarDType::U8 => 1,
            ScalarDType::U16 => 2,
            ScalarDType::F32 => 4,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ImageLayout {
    #[default]
    Hwc,
    Chw,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct WireEnumValue {
    pub name: String,
    #[serde(default)]
    pub value: Option<Box<WireValue>>,
}
