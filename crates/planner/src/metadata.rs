use alloc::borrow::{Cow, ToOwned};
use alloc::collections::BTreeMap;
use alloc::string::{String, ToString};
use alloc::vec::Vec;

use daedalus_data::model::{TypeExpr, Value};
use daedalus_registry::capability::NodeDecl;

pub use daedalus_core::metadata::{
    DYNAMIC_INPUT_LABELS_KEY, DYNAMIC_INPUT_TYPES_KEY, DYNAMIC_INPUTS_KEY,
    DYNAMIC_OUTPUT_LABELS_KEY, DYNAMIC_OUTPUT_TYPES_KEY, DYNAMIC_OUTPUTS_KEY, EMBEDDED_GROUP_KEY,
    GROUP_ID_KEY, GROUP_LABEL_KEY, HOST_BRIDGE_META_KEY, HOST_HELD_INPUTS_KEY,
    HOST_INPUT_TYPES_KEY, HOST_OUTPUT_TYPES_KEY, HOST_SHARED_INPUTS_KEY,
    PLAN_APPLIED_LOWERINGS_KEY, PLAN_CONVERTER_METADATA_PREFIX, PLAN_EDGE_EXPLANATIONS_KEY,
    PLAN_GPU_SEGMENTS_KEY, PLAN_GPU_WHY_KEY, PLAN_OVERLOAD_RESOLUTIONS_KEY,
    PLAN_SCHEDULE_ORDER_KEY, PLAN_SCHEDULE_PRIORITY_KEY, PLAN_TOPO_ORDER_KEY,
};

/// Opaque type name the planner treats as a type variable, inferred from connected edges.
pub const GENERIC_TYPE_NAME: &str = "generic";

/// Whether `ty` is the generic type marker (`Opaque("generic")`, case-insensitive).
pub fn is_generic_marker(ty: &TypeExpr) -> bool {
    matches!(ty, TypeExpr::Opaque(name) if name.eq_ignore_ascii_case(GENERIC_TYPE_NAME))
}

/// Node metadata for a host bridge: the host-bridge marker plus generic dynamic inputs and
/// outputs, so arbitrary host ports are allowed and the planner infers their types from edges.
pub fn host_bridge_metadata() -> BTreeMap<String, Value> {
    let generic = Value::String(Cow::Borrowed(GENERIC_TYPE_NAME));
    BTreeMap::from([
        (HOST_BRIDGE_META_KEY.to_string(), Value::Bool(true)),
        (DYNAMIC_INPUTS_KEY.to_string(), generic.clone()),
        (DYNAMIC_OUTPUTS_KEY.to_string(), generic),
    ])
}

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

/// How a host input delivers the values the host pushes.
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum HostInputPolicy {
    /// Each push is queued and consumed once by the tick that takes it, under the port's
    /// pressure and freshness policy.
    #[default]
    Queued,
    /// The last pushed value persists and is delivered to every tick until a push replaces it
    /// (`HostBridgeHandle::set_held_input`). Held pushes never trigger a tick by themselves.
    Held,
}

/// The held host inputs a host-bridge node declares (`HOST_HELD_INPUTS_KEY`, a list of port
/// names).
pub fn host_held_inputs(metadata: &BTreeMap<String, Value>) -> impl Iterator<Item = &str> {
    let ports = match metadata.get(HOST_HELD_INPUTS_KEY) {
        Some(Value::List(ports)) => ports.as_slice(),
        _ => &[],
    };
    ports.iter().filter_map(Value::as_str)
}

/// The host inputs a host-bridge node declares shared with other graphs
/// (`HOST_SHARED_INPUTS_KEY`, a list of port names).
pub fn host_shared_inputs(metadata: &BTreeMap<String, Value>) -> impl Iterator<Item = &str> {
    let ports = match metadata.get(HOST_SHARED_INPUTS_KEY) {
        Some(Value::List(ports)) => ports.as_slice(),
        _ => &[],
    };
    ports.iter().filter_map(Value::as_str)
}

/// Declare host input `port` of a host-bridge node's metadata shared with other graphs
/// (`HOST_SHARED_INPUTS_KEY`): by-value consumers get a planned copy.
pub fn set_host_input_shared(metadata: &mut BTreeMap<String, Value>, port: &str) {
    if host_shared_inputs(metadata).any(|shared| shared.eq_ignore_ascii_case(port)) {
        return;
    }
    let mut ports = match metadata.remove(HOST_SHARED_INPUTS_KEY) {
        Some(Value::List(ports)) => ports,
        _ => Vec::new(),
    };
    ports.push(Value::String(Cow::Owned(port.to_string())));
    metadata.insert(HOST_SHARED_INPUTS_KEY.to_string(), Value::List(ports));
}

/// The policy of host input `port` in a host-bridge node's metadata (ports match
/// case-insensitively, as the planner matches them).
pub fn host_input_policy(metadata: &BTreeMap<String, Value>, port: &str) -> HostInputPolicy {
    if host_held_inputs(metadata).any(|held| held.eq_ignore_ascii_case(port)) {
        HostInputPolicy::Held
    } else {
        HostInputPolicy::Queued
    }
}

/// Record host input `port`'s policy in a host-bridge node's metadata (`HOST_HELD_INPUTS_KEY`,
/// the single source of truth) and return the previous one. The key is removed once no input is
/// held. Ports are not validated here; see `Graph::set_host_input_policy`.
pub fn set_host_input_policy(
    metadata: &mut BTreeMap<String, Value>,
    port: &str,
    policy: HostInputPolicy,
) -> HostInputPolicy {
    let previous = host_input_policy(metadata, port);
    if previous == policy {
        return previous;
    }
    let mut held = match metadata.remove(HOST_HELD_INPUTS_KEY) {
        Some(Value::List(ports)) => ports,
        _ => Vec::new(),
    };
    match policy {
        HostInputPolicy::Held => held.push(Value::String(Cow::Owned(port.to_string()))),
        HostInputPolicy::Queued => {
            held.retain(|value| !value.as_str().is_some_and(|p| p.eq_ignore_ascii_case(port)));
        }
    }
    if !held.is_empty() {
        metadata.insert(HOST_HELD_INPUTS_KEY.to_string(), Value::List(held));
    }
    previous
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

/// Host port types declared by the graph author on a host-bridge node (see
/// `GraphBuilder::input_as`). Unlike the planner-owned [`DynamicPortMetadata`], these are graph
/// inputs: the planner seeds the bridge's resolved port types from them before type checking.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HostPortTypes {
    pub inputs: BTreeMap<String, TypeExpr>,
    pub outputs: BTreeMap<String, TypeExpr>,
}

impl HostPortTypes {
    pub fn from_node_metadata(metadata: &BTreeMap<String, Value>) -> Self {
        Self {
            inputs: decode_type_map(metadata.get(HOST_INPUT_TYPES_KEY)),
            outputs: decode_type_map(metadata.get(HOST_OUTPUT_TYPES_KEY)),
        }
    }

    pub fn declare(&mut self, is_host_input: bool, port: &str, ty: TypeExpr) {
        if is_host_input {
            &mut self.inputs
        } else {
            &mut self.outputs
        }
        .insert(normalize_port(port), ty);
    }

    pub fn write_to_node_metadata(&self, metadata: &mut BTreeMap<String, Value>) {
        write_type_map(metadata, HOST_INPUT_TYPES_KEY, &self.inputs);
        write_type_map(metadata, HOST_OUTPUT_TYPES_KEY, &self.outputs);
    }

    /// The bridge node's resolved port types: host inputs are bridge outputs and vice versa.
    pub fn to_dynamic(&self) -> DynamicPortMetadata {
        DynamicPortMetadata {
            input_types: self.outputs.clone(),
            output_types: self.inputs.clone(),
            ..DynamicPortMetadata::default()
        }
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
            Some((normalize_port(key.as_str()?), value.as_str()?.to_string()))
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
