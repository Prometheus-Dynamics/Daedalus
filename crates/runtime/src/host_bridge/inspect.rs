//! Turn arbitrary host-bridge payloads into inspectable `Value`s.
//!
//! Hosts that display graph outputs (dashboards, logs, debuggers) should not need to know every
//! concrete Rust type flowing through a graph. [`inspect_payload`] consults a
//! [`ValueSerializerMap`] (typically `PluginRegistry::value_serializers`) keyed by the payload's
//! concrete `TypeId`, and falls back to a structured [`PayloadSummary`] for unregistered types.

use crate::prelude::*;
use core::any::Any;

use daedalus_data::model::{StructFieldValue, Value};
use daedalus_transport::{Payload, Residency, TypeKey};

use super::serializers::ValueSerializerMap;

/// Metadata describing a payload independent of whether its value could be serialized.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PayloadSummary {
    /// Transport type key carried by the payload.
    pub type_key: TypeKey,
    /// Concrete Rust type stored in the payload, when the storage exposes it.
    pub rust_type: Option<&'static str>,
    /// Where the payload currently lives.
    pub residency: Residency,
    /// Optional layout identity (for image/tensor style payloads).
    pub layout: Option<String>,
    /// Storage-provided size estimate in bytes.
    pub bytes_estimate: Option<u64>,
}

impl PayloadSummary {
    /// Capture the summary fields of a payload.
    pub fn of(payload: &Payload) -> Self {
        Self {
            type_key: payload.type_key().clone(),
            rust_type: payload.storage_rust_type_name(),
            residency: payload.residency(),
            layout: payload.layout().map(|layout| layout.as_str().to_string()),
            bytes_estimate: payload.bytes_estimate(),
        }
    }

    /// Render the summary as a deterministic `Value::Struct`.
    ///
    /// Field order is fixed: `type_key`, `rust_type`, `residency`, `layout`, `bytes_estimate`.
    /// Missing optional fields are rendered as `Value::Unit`.
    pub fn to_value(&self) -> Value {
        let opt_string = |value: Option<&str>| {
            value
                .map(|value| Value::String(value.to_string().into()))
                .unwrap_or(Value::Unit)
        };
        Value::Struct(vec![
            StructFieldValue {
                name: "type_key".to_string(),
                value: Value::String(self.type_key.as_str().to_string().into()),
            },
            StructFieldValue {
                name: "rust_type".to_string(),
                value: opt_string(self.rust_type),
            },
            StructFieldValue {
                name: "residency".to_string(),
                value: Value::String(self.residency.as_str().into()),
            },
            StructFieldValue {
                name: "layout".to_string(),
                value: opt_string(self.layout.as_deref()),
            },
            StructFieldValue {
                name: "bytes_estimate".to_string(),
                value: self
                    .bytes_estimate
                    .map(|bytes| Value::Int(i64::try_from(bytes).unwrap_or(i64::MAX)))
                    .unwrap_or(Value::Unit),
            },
        ])
    }
}

/// Result of inspecting a payload.
#[derive(Clone, Debug, PartialEq)]
pub enum PayloadInspection {
    /// The payload was converted into a `Value` (it already was one, or a serializer matched).
    Value {
        value: Value,
        summary: PayloadSummary,
    },
    /// No serializer is registered for the payload's concrete type.
    Opaque(PayloadSummary),
}

impl PayloadInspection {
    pub fn summary(&self) -> &PayloadSummary {
        match self {
            PayloadInspection::Value { summary, .. } | PayloadInspection::Opaque(summary) => {
                summary
            }
        }
    }

    /// Serialized value, if the payload type had a serializer.
    pub fn value(&self) -> Option<&Value> {
        match self {
            PayloadInspection::Value { value, .. } => Some(value),
            PayloadInspection::Opaque(_) => None,
        }
    }

    pub fn into_value(self) -> Option<Value> {
        match self {
            PayloadInspection::Value { value, .. } => Some(value),
            PayloadInspection::Opaque(_) => None,
        }
    }

    pub fn is_opaque(&self) -> bool {
        matches!(self, PayloadInspection::Opaque(_))
    }

    /// Serialized value, or the structured summary for opaque payloads.
    pub fn to_display_value(&self) -> Value {
        match self {
            PayloadInspection::Value { value, .. } => value.clone(),
            PayloadInspection::Opaque(summary) => summary.to_value(),
        }
    }

    /// Plain JSON rendering (see [`daedalus_data::json::to_plain_json`]).
    ///
    /// Opaque payloads render as an object with `type_key`, `rust_type`, `residency`, `layout`,
    /// and `bytes_estimate` fields.
    pub fn to_json(&self) -> serde_json::Value {
        daedalus_data::json::to_plain_json(&self.to_display_value())
    }
}

/// Serialize a payload's value using `serializers`, without the structured fallback.
///
/// `Value` payloads are returned as-is. Other payloads are looked up by the `TypeId` of the stored
/// value (the inner `T` for `Arc<T>`-backed payloads), so registrations made with
/// `register_value_serializer_in::<T, _>` match both owned and shared payloads.
pub fn serialize_payload_value(
    payload: &Payload,
    serializers: &ValueSerializerMap,
) -> Option<Value> {
    let value = payload.value_any_sync()?;
    if let Some(value) = value.downcast_ref::<Value>() {
        return Some(value.clone());
    }
    let type_id = <dyn Any>::type_id(value);
    let guard = serializers.read();
    guard.get(&type_id).and_then(|serializer| serializer(value))
}

/// Inspect a payload using `serializers`, falling back to a structured summary.
pub fn inspect_payload(payload: &Payload, serializers: &ValueSerializerMap) -> PayloadInspection {
    let summary = PayloadSummary::of(payload);
    match serialize_payload_value(payload, serializers) {
        Some(value) => PayloadInspection::Value { value, summary },
        None => {
            tracing::trace!(
                target: "daedalus_runtime::host_bridge",
                type_key = %summary.type_key,
                rust_type = summary.rust_type.unwrap_or("unknown"),
                "payload has no registered value serializer"
            );
            PayloadInspection::Opaque(summary)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::portable::Arc;

    use daedalus_transport::{BoundaryCapabilities, Layout};

    use super::*;
    use crate::host_bridge::{
        new_value_serializer_map, primitive_value_serializer_map, register_value_serializer_in,
    };

    #[derive(Clone, Debug)]
    struct Pose {
        x: i64,
        y: i64,
    }

    struct Frame;

    #[test]
    fn values_pass_through_without_registration() {
        let map = new_value_serializer_map();
        let payload = Payload::owned("value", Value::Int(3));
        let inspection = inspect_payload(&payload, &map);
        assert_eq!(inspection.value(), Some(&Value::Int(3)));
        assert_eq!(inspection.summary().type_key.as_str(), "value");
    }

    #[test]
    fn primitives_and_registered_types_serialize_for_owned_and_shared_payloads() {
        let map = primitive_value_serializer_map();
        register_value_serializer_in::<Pose, _>(&map, |pose| {
            Value::Struct(vec![
                StructFieldValue {
                    name: "x".into(),
                    value: Value::Int(pose.x),
                },
                StructFieldValue {
                    name: "y".into(),
                    value: Value::Int(pose.y),
                },
            ])
        });

        let text = Payload::owned("string", String::from("hello"));
        assert_eq!(
            inspect_payload(&text, &map).into_value(),
            Some(Value::String("hello".into()))
        );
        let float = Payload::shared("f64", Arc::new(1.5f64));
        assert_eq!(
            inspect_payload(&float, &map).into_value(),
            Some(Value::Float(1.5))
        );

        let pose = Payload::shared("pose", Arc::new(Pose { x: 1, y: 2 }));
        let inspection = inspect_payload(&pose, &map);
        assert_eq!(inspection.to_json(), serde_json::json!({"x": 1, "y": 2}));
    }

    #[test]
    fn boundary_payloads_with_borrow_ref_are_serialized() {
        let map = primitive_value_serializer_map();
        let payload = Payload::boundary_owned("test:i64", 9i64, BoundaryCapabilities::rust_value());
        assert_eq!(
            inspect_payload(&payload, &map).into_value(),
            Some(Value::Int(9))
        );
    }

    #[test]
    fn unregistered_types_fall_back_to_summary() {
        let map = primitive_value_serializer_map();
        let payload = Payload::shared_with(
            "camera:frame",
            Arc::new(Frame),
            Residency::Gpu,
            Some(Layout::new("rgba8")),
            Some(1024),
        );
        let inspection = inspect_payload(&payload, &map);
        assert!(inspection.is_opaque());
        let summary = inspection.summary();
        assert_eq!(summary.type_key.as_str(), "camera:frame");
        assert_eq!(summary.residency, Residency::Gpu);
        assert_eq!(summary.bytes_estimate, Some(1024));
        assert!(
            summary
                .rust_type
                .is_some_and(|name| name.ends_with("Frame"))
        );
        let json = inspection.to_json();
        assert_eq!(json["type_key"], "camera:frame");
        assert_eq!(json["residency"], "gpu");
        assert_eq!(json["layout"], "rgba8");
        assert_eq!(json["bytes_estimate"], 1024);
    }
}
