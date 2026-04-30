use std::borrow::Cow;
use std::collections::BTreeMap;

use daedalus_data::model::{TypeExpr, Value};
use daedalus_registry::capability::NodeDecl;

pub use daedalus_core::metadata::{
    DYNAMIC_INPUT_LABELS_KEY, DYNAMIC_INPUT_TYPES_KEY, DYNAMIC_INPUTS_KEY,
    DYNAMIC_OUTPUT_LABELS_KEY, DYNAMIC_OUTPUT_TYPES_KEY, DYNAMIC_OUTPUTS_KEY, EMBEDDED_GROUP_KEY,
    GROUP_ID_KEY, GROUP_LABEL_KEY, HOST_BRIDGE_META_KEY, PLAN_APPLIED_LOWERINGS_KEY,
    PLAN_CONVERTER_METADATA_PREFIX, PLAN_EDGE_EXPLANATIONS_KEY, PLAN_GPU_SEGMENTS_KEY,
    PLAN_GPU_WHY_KEY, PLAN_OVERLOAD_RESOLUTIONS_KEY, PLAN_SCHEDULE_ORDER_KEY,
    PLAN_SCHEDULE_PRIORITY_KEY, PLAN_TOPO_ORDER_KEY,
};

pub fn metadata_bool(metadata: &BTreeMap<String, Value>, key: &str) -> bool {
    matches!(metadata.get(key), Some(Value::Bool(true)))
}

pub fn metadata_string<'a>(metadata: &'a BTreeMap<String, Value>, key: &str) -> Option<&'a str> {
    let Value::String(value) = metadata.get(key)? else {
        return None;
    };
    let trimmed = value.trim();
    (!trimmed.is_empty()).then_some(trimmed)
}

pub fn descriptor_metadata_value(desc: &NodeDecl, key: &str) -> Option<Value> {
    desc.metadata_json
        .get(key)
        .and_then(|raw| serde_json::from_str::<Value>(raw).ok())
}

pub fn descriptor_metadata_string(desc: &NodeDecl, key: &str) -> Option<String> {
    let Some(Value::String(value)) = descriptor_metadata_value(desc, key) else {
        return None;
    };
    let trimmed = value.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

pub fn is_host_bridge_metadata(metadata: &BTreeMap<String, Value>) -> bool {
    metadata_bool(metadata, HOST_BRIDGE_META_KEY)
}

pub fn descriptor_dynamic_port_type(desc: &NodeDecl, is_input: bool) -> Option<String> {
    descriptor_metadata_string(
        desc,
        if is_input {
            DYNAMIC_INPUTS_KEY
        } else {
            DYNAMIC_OUTPUTS_KEY
        },
    )
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct GroupMetadata {
    pub id: Option<String>,
    pub label: Option<String>,
    pub embedded_group: Option<String>,
}

impl GroupMetadata {
    pub fn from_node_metadata(metadata: &BTreeMap<String, Value>) -> Self {
        Self {
            id: metadata_string(metadata, GROUP_ID_KEY).map(ToOwned::to_owned),
            label: metadata_string(metadata, GROUP_LABEL_KEY).map(ToOwned::to_owned),
            embedded_group: metadata_string(metadata, EMBEDDED_GROUP_KEY).map(ToOwned::to_owned),
        }
    }

    pub fn preferred_id(&self) -> Option<&str> {
        self.id.as_deref().or(self.embedded_group.as_deref())
    }

    pub fn write_to_node_metadata(&self, metadata: &mut BTreeMap<String, Value>) {
        write_optional_string(metadata, GROUP_ID_KEY, self.id.as_deref());
        write_optional_string(metadata, GROUP_LABEL_KEY, self.label.as_deref());
        write_optional_string(metadata, EMBEDDED_GROUP_KEY, self.embedded_group.as_deref());
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DynamicPortMetadata {
    pub input_types: BTreeMap<String, TypeExpr>,
    pub output_types: BTreeMap<String, TypeExpr>,
    pub input_labels: BTreeMap<String, String>,
    pub output_labels: BTreeMap<String, String>,
}

impl DynamicPortMetadata {
    pub fn from_node_metadata(metadata: &BTreeMap<String, Value>) -> Self {
        Self {
            input_types: decode_type_map(metadata.get(DYNAMIC_INPUT_TYPES_KEY)),
            output_types: decode_type_map(metadata.get(DYNAMIC_OUTPUT_TYPES_KEY)),
            input_labels: decode_string_map(metadata.get(DYNAMIC_INPUT_LABELS_KEY)),
            output_labels: decode_string_map(metadata.get(DYNAMIC_OUTPUT_LABELS_KEY)),
        }
    }

    pub fn resolved_type(&self, is_input: bool, port: &str) -> Option<TypeExpr> {
        let key = normalize_port(port);
        if is_input {
            self.input_types.get(&key)
        } else {
            self.output_types.get(&key)
        }
        .cloned()
    }

    pub fn set_resolved_type(&mut self, is_input: bool, port: &str, ty: TypeExpr) {
        if is_input {
            &mut self.input_types
        } else {
            &mut self.output_types
        }
        .insert(normalize_port(port), ty);
    }

    pub fn set_label(&mut self, is_input: bool, port: &str, label: String) {
        if is_input {
            &mut self.input_labels
        } else {
            &mut self.output_labels
        }
        .insert(normalize_port(port), label);
    }

    pub fn write_to_node_metadata(&self, metadata: &mut BTreeMap<String, Value>) {
        write_type_map(metadata, DYNAMIC_INPUT_TYPES_KEY, &self.input_types);
        write_type_map(metadata, DYNAMIC_OUTPUT_TYPES_KEY, &self.output_types);
        write_string_map(metadata, DYNAMIC_INPUT_LABELS_KEY, &self.input_labels);
        write_string_map(metadata, DYNAMIC_OUTPUT_LABELS_KEY, &self.output_labels);
    }
}

fn normalize_port(port: &str) -> String {
    port.to_ascii_lowercase()
}

fn decode_type_map(value: Option<&Value>) -> BTreeMap<String, TypeExpr> {
    decode_string_map(value)
        .into_iter()
        .filter_map(|(port, json)| {
            serde_json::from_str::<TypeExpr>(&json)
                .ok()
                .map(|ty| (port, ty))
        })
        .collect()
}

fn decode_string_map(value: Option<&Value>) -> BTreeMap<String, String> {
    let Some(Value::Map(entries)) = value else {
        return BTreeMap::new();
    };
    entries
        .iter()
        .filter_map(|(key, value)| {
            let Value::String(key) = key else {
                return None;
            };
            let Value::String(value) = value else {
                return None;
            };
            Some((normalize_port(key), value.to_string()))
        })
        .collect()
}

fn write_type_map(
    metadata: &mut BTreeMap<String, Value>,
    key: &str,
    types: &BTreeMap<String, TypeExpr>,
) {
    let entries = types
        .iter()
        .filter_map(|(port, ty)| {
            serde_json::to_string(ty)
                .ok()
                .map(|json| (port.clone(), json))
        })
        .collect::<BTreeMap<_, _>>();
    write_string_map(metadata, key, &entries);
}

fn write_string_map(
    metadata: &mut BTreeMap<String, Value>,
    key: &str,
    values: &BTreeMap<String, String>,
) {
    if values.is_empty() {
        metadata.remove(key);
        return;
    }
    metadata.insert(
        key.to_string(),
        Value::Map(
            values
                .iter()
                .map(|(port, value)| {
                    (
                        Value::String(Cow::Owned(normalize_port(port))),
                        Value::String(Cow::Owned(value.clone())),
                    )
                })
                .collect(),
        ),
    );
}

fn write_optional_string(metadata: &mut BTreeMap<String, Value>, key: &str, value: Option<&str>) {
    let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        metadata.remove(key);
        return;
    };
    metadata.insert(
        key.to_string(),
        Value::String(Cow::Owned(value.to_string())),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dynamic_port_metadata_round_trips_with_normalized_ports() {
        let mut dynamic = DynamicPortMetadata::default();
        dynamic.set_resolved_type(true, "Input", TypeExpr::Opaque("frame".to_string()));
        dynamic.set_resolved_type(false, "Out", TypeExpr::Opaque("result".to_string()));
        dynamic.set_label(true, "Input", "Frame".to_string());
        dynamic.set_label(false, "Out", "Result".to_string());

        let mut metadata = BTreeMap::new();
        dynamic.write_to_node_metadata(&mut metadata);

        let decoded = DynamicPortMetadata::from_node_metadata(&metadata);
        assert_eq!(
            decoded.resolved_type(true, "input"),
            Some(TypeExpr::Opaque("frame".to_string()))
        );
        assert_eq!(
            decoded.resolved_type(false, "OUT"),
            Some(TypeExpr::Opaque("result".to_string()))
        );
        assert_eq!(
            metadata
                .get(DYNAMIC_INPUT_TYPES_KEY)
                .and_then(|value| match value {
                    Value::Map(entries) => entries.first(),
                    _ => None,
                })
                .and_then(|(key, _)| match key {
                    Value::String(key) => Some(key.as_ref()),
                    _ => None,
                }),
            Some("input")
        );
    }

    #[test]
    fn group_metadata_round_trips_and_prefers_id() {
        let group = GroupMetadata {
            id: Some("group-1".to_string()),
            label: Some("Group 1".to_string()),
            embedded_group: Some("fallback".to_string()),
        };
        let mut metadata = BTreeMap::new();
        group.write_to_node_metadata(&mut metadata);

        let decoded = GroupMetadata::from_node_metadata(&metadata);
        assert_eq!(decoded.preferred_id(), Some("group-1"));
        assert_eq!(decoded.label.as_deref(), Some("Group 1"));
        assert_eq!(decoded.embedded_group.as_deref(), Some("fallback"));
    }

    #[test]
    fn host_bridge_metadata_requires_true_bool() {
        let mut metadata = BTreeMap::new();
        metadata.insert(HOST_BRIDGE_META_KEY.to_string(), Value::Bool(false));
        assert!(!is_host_bridge_metadata(&metadata));

        metadata.insert(HOST_BRIDGE_META_KEY.to_string(), Value::Bool(true));
        assert!(is_host_bridge_metadata(&metadata));
    }
}
