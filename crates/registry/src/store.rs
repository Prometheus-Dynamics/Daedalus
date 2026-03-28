use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::diagnostics::{RegistryError, RegistryErrorCode, RegistryResult};
use crate::ids::{GroupId, NodeId};
use daedalus_core::compute::ComputeAffinity;
use daedalus_core::sync::SyncGroup;
use daedalus_data::convert::{ConversionResolution, ConverterGraph, ConverterId};
use daedalus_data::descriptor::{DataDescriptor, DescriptorId, DescriptorVersion};
use daedalus_data::model::{TypeExpr, Value};

/// Immutable view of registry contents for deterministic iteration/serialization.
///
/// ```
/// use daedalus_registry::store::Registry;
/// let registry = Registry::new();
/// let view = registry.view();
/// assert!(view.nodes.is_empty());
/// ```
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RegistryView {
    pub values: BTreeMap<(DescriptorId, DescriptorVersion), DataDescriptor>,
    pub nodes: BTreeMap<NodeId, NodeDescriptor>,
    pub groups: BTreeMap<GroupId, GroupDescriptor>,
    pub converters: BTreeMap<ConverterId, (TypeExpr, TypeExpr)>,
}

/// Flattened snapshot for deterministic comparisons/goldens.
///
/// ```
/// use daedalus_registry::store::Registry;
/// let registry = Registry::new();
/// let snapshot = registry.snapshot();
/// assert!(snapshot.values.is_empty());
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegistrySnapshot {
    pub values: Vec<String>,
    pub nodes: Vec<String>,
    pub groups: Vec<String>,
    pub converters: Vec<String>,
}

/// Registry for value descriptors and converters.
///
/// ```
/// use daedalus_registry::store::Registry;
/// let mut registry = Registry::new();
/// assert!(registry.view().nodes.is_empty());
/// ```
pub struct Registry {
    values: BTreeMap<(DescriptorId, DescriptorVersion), DataDescriptor>,
    nodes: BTreeMap<NodeId, NodeDescriptor>,
    groups: BTreeMap<GroupId, GroupDescriptor>,
    converters: BTreeMap<ConverterId, (TypeExpr, TypeExpr)>,
    graph: ConverterGraph,
}

impl std::fmt::Debug for Registry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Registry")
            .field("values", &self.values.len())
            .field("nodes", &self.nodes.len())
            .field("groups", &self.groups.len())
            .field("converters", &self.converters.len())
            .finish()
    }
}

impl Registry {
    /// Construct an empty registry.
    pub fn new() -> Self {
        Self {
            values: BTreeMap::new(),
            nodes: BTreeMap::new(),
            groups: BTreeMap::new(),
            converters: BTreeMap::new(),
            graph: ConverterGraph::new(),
        }
    }

    /// Register a value descriptor.
    pub fn register_value(&mut self, desc: DataDescriptor) -> RegistryResult<()> {
        let key = (desc.id.clone(), desc.version.clone());
        if self.values.contains_key(&key) {
            return Err(RegistryError::new(
                RegistryErrorCode::Conflict,
                format!("duplicate value {:?}", key),
            )
            .with_conflict_key(format!("{}@{}", key.0.0, key.1.0))
            .with_conflict_kind(crate::diagnostics::ConflictKind::Value)
            .with_payload(crate::diagnostics::RegistryErrorCompute::Conflict {
                key: format!("{}@{}", key.0.0, key.1.0),
                kind: crate::diagnostics::ConflictKind::Value,
            }));
        }
        self.values.insert(key, desc.normalize());
        Ok(())
    }

    /// Register a converter edge.
    pub fn register_converter(
        &mut self,
        converter: Box<dyn daedalus_data::convert::Converter>,
    ) -> RegistryResult<()> {
        let id = converter.id();
        if self.converters.contains_key(&id) {
            return Err(RegistryError::new(
                RegistryErrorCode::Conflict,
                format!("duplicate converter {:?}", id),
            )
            .with_conflict_key(id.0.clone())
            .with_conflict_kind(crate::diagnostics::ConflictKind::Converter)
            .with_payload(crate::diagnostics::RegistryErrorCompute::Conflict {
                key: id.0.clone(),
                kind: crate::diagnostics::ConflictKind::Converter,
            }));
        }
        let input = converter.input().clone();
        let output = converter.output().clone();
        self.graph.register(converter);
        self.converters.insert(id, (input, output));
        Ok(())
    }

    /// Register a node descriptor.
    pub fn register_node(&mut self, node: NodeDescriptor) -> RegistryResult<()> {
        node.validate()
            .map_err(|e| RegistryError::new(RegistryErrorCode::Conflict, e))?;
        let key = node.id.clone();
        if self.nodes.contains_key(&key) {
            return Err(RegistryError::new(
                RegistryErrorCode::Conflict,
                format!("duplicate node {:?}", key),
            )
            .with_conflict_key(key.0.clone())
            .with_conflict_kind(crate::diagnostics::ConflictKind::Node)
            .with_payload(crate::diagnostics::RegistryErrorCompute::Conflict {
                key: key.0.clone(),
                kind: crate::diagnostics::ConflictKind::Node,
            }));
        }
        self.nodes.insert(key, node);
        Ok(())
    }

    /// Register a node group descriptor.
    pub fn register_group(&mut self, group: GroupDescriptor) -> RegistryResult<()> {
        group
            .validate()
            .map_err(|e| RegistryError::new(RegistryErrorCode::Conflict, e))?;
        let key = group.id.clone();
        if self.groups.contains_key(&key) {
            return Err(RegistryError::new(
                RegistryErrorCode::Conflict,
                format!("duplicate group {:?}", key),
            )
            .with_conflict_key(key.0.clone())
            .with_conflict_kind(crate::diagnostics::ConflictKind::Group)
            .with_payload(crate::diagnostics::RegistryErrorCompute::Conflict {
                key: key.0.clone(),
                kind: crate::diagnostics::ConflictKind::Group,
            }));
        }
        self.groups.insert(key, group);
        Ok(())
    }

    /// Resolve a conversion path without feature/GPU context.
    pub fn resolve_converter(
        &self,
        from: &TypeExpr,
        to: &TypeExpr,
    ) -> RegistryResult<ConversionResolution> {
        self.graph
            .resolve(from, to)
            .map_err(|e| RegistryError::new(RegistryErrorCode::ConverterError, e.to_string()))
    }

    /// Resolve a conversion path with feature/GPU context.
    pub fn resolve_converter_with_context(
        &self,
        from: &TypeExpr,
        to: &TypeExpr,
        active_features: &[String],
        allow_gpu: bool,
    ) -> RegistryResult<ConversionResolution> {
        self.graph
            .resolve_with_context(from, to, active_features, allow_gpu)
            .map_err(|e| RegistryError::new(RegistryErrorCode::ConverterError, e.to_string()))
    }

    /// Return an immutable view of registry contents.
    pub fn view(&self) -> RegistryView {
        RegistryView {
            values: self.values.clone(),
            nodes: self.nodes.clone(),
            groups: self.groups.clone(),
            converters: self.converters.clone(),
        }
    }

    /// Filtered view by active feature flags; converters are not filtered here (handled by resolve_with_context).
    /// Return a view filtered to active features.
    pub fn filtered_view(&self, active_features: &[String]) -> RegistryView {
        let values = self
            .values
            .iter()
            .filter(|(_, desc)| {
                desc.feature_flags
                    .iter()
                    .all(|f| active_features.contains(f))
            })
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let nodes = self
            .nodes
            .iter()
            .filter(|(_, desc)| {
                desc.feature_flags
                    .iter()
                    .all(|f| active_features.contains(f))
            })
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let groups = self
            .groups
            .iter()
            .filter(|(_, desc)| {
                desc.feature_flags
                    .iter()
                    .all(|f| active_features.contains(f))
            })
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        RegistryView {
            values,
            nodes,
            groups,
            converters: self.converters.clone(),
        }
    }

    /// Return a flattened snapshot for deterministic comparisons.
    pub fn snapshot(&self) -> RegistrySnapshot {
        let mut values: Vec<String> = self
            .values
            .keys()
            .map(|(id, ver)| format!("{}@{}", id.0, ver.0))
            .collect();
        values.sort();
        let mut nodes: Vec<String> = self
            .nodes
            .iter()
            .map(|(id, node)| {
                format!(
                    "{}:{} inputs={} outputs={}",
                    id.0,
                    node.label.clone().unwrap_or_default(),
                    node.inputs.len(),
                    node.outputs.len()
                )
            })
            .collect();
        nodes.sort();
        let mut groups: Vec<String> = self
            .groups
            .iter()
            .map(|(id, group)| format!("{}:{}", id.0, group.label.clone().unwrap_or_default()))
            .collect();
        groups.sort();
        let mut converters: Vec<String> = self.converters.keys().map(|id| id.0.clone()).collect();
        converters.sort();
        RegistrySnapshot {
            values,
            nodes,
            groups,
            converters,
        }
    }
}

impl Default for Registry {
    fn default() -> Self {
        Self::new()
    }
}

/// Minimal node descriptor placeholder.
///
/// ```
/// use daedalus_registry::store::{NodeDescriptor, Port};
/// use daedalus_registry::ids::NodeId;
/// use daedalus_core::compute::ComputeAffinity;
/// use daedalus_data::model::{TypeExpr, ValueType};
///
/// let node = NodeDescriptor {
///     id: NodeId::new("demo.node"),
///     feature_flags: vec![],
///     label: None,
///     group: None,
///     inputs: vec![Port { name: "in".into(), ty: TypeExpr::Scalar(ValueType::Int), access: Default::default(), source: None, const_value: None }],
///     fanin_inputs: vec![],
///     outputs: vec![],
///     default_compute: ComputeAffinity::CpuOnly,
///     sync_groups: vec![],
///     metadata: Default::default(),
/// };
/// assert_eq!(node.id.0, "demo.node");
/// ```
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NodeDescriptor {
    pub id: NodeId,
    pub feature_flags: Vec<String>,
    pub label: Option<String>,
    /// Optional reference to a registered node group implementation (subgraph).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<GroupId>,
    pub inputs: Vec<Port>,
    /// Indexed fan-in port groups (e.g. `ins0`, `ins1`, ...), matched by numeric suffix.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub fanin_inputs: Vec<FanInPort>,
    pub outputs: Vec<Port>,
    #[serde(default)]
    pub default_compute: ComputeAffinity,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sync_groups: Vec<SyncGroup>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, daedalus_data::model::Value>,
}

/// Descriptor for a node group (subgraph).
///
/// Groups are registry-level entities that can be referenced by node descriptors and expanded by the planner.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GroupDescriptor {
    pub id: GroupId,
    pub feature_flags: Vec<String>,
    pub label: Option<String>,
    /// Serialized planner graph (`daedalus_planner::Graph`) as JSON.
    pub graph: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, daedalus_data::model::Value>,
}

/// Builder for `GroupDescriptor`.
pub struct GroupDescriptorBuilder {
    id: GroupId,
    feature_flags: Vec<String>,
    label: Option<String>,
    graph: String,
    metadata: BTreeMap<String, daedalus_data::model::Value>,
}

impl GroupDescriptorBuilder {
    pub fn new(id: impl Into<String>, graph_json: impl Into<String>) -> Self {
        Self {
            id: GroupId::new(id.into()),
            feature_flags: Vec::new(),
            label: None,
            graph: graph_json.into(),
            metadata: BTreeMap::new(),
        }
    }

    pub fn label(mut self, label: impl Into<String>) -> Self {
        self.label = Some(label.into());
        self
    }

    pub fn feature_flag(mut self, flag: impl Into<String>) -> Self {
        self.feature_flags.push(flag.into());
        self
    }

    pub fn metadata(mut self, key: impl Into<String>, value: daedalus_data::model::Value) -> Self {
        self.metadata.insert(key.into(), value);
        self
    }

    pub fn metadata_map<
        K: Into<String>,
        V: IntoIterator<Item = (K, daedalus_data::model::Value)>,
    >(
        mut self,
        entries: V,
    ) -> Self {
        for (k, v) in entries {
            self.metadata.insert(k.into(), v);
        }
        self
    }

    pub fn build(mut self) -> Result<GroupDescriptor, &'static str> {
        self.feature_flags.sort();
        let desc = GroupDescriptor {
            id: self.id,
            feature_flags: self.feature_flags,
            label: self.label,
            graph: self.graph,
            metadata: self.metadata,
        };
        desc.validate().map(|_| desc)
    }
}

impl GroupDescriptor {
    pub fn validate(&self) -> Result<(), &'static str> {
        self.id.validate()?;
        if self.graph.trim().is_empty() {
            return Err("group graph must not be empty");
        }
        Ok(())
    }
}

/// Port-group metadata for indexed fan-in inputs.
///
/// ```
/// use daedalus_registry::store::FanInPort;
/// use daedalus_data::model::{TypeExpr, ValueType};
/// let fanin = FanInPort { prefix: "ins".into(), start: 0, ty: TypeExpr::Scalar(ValueType::Int) };
/// assert_eq!(fanin.prefix, "ins");
/// ```
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FanInPort {
    pub prefix: String,
    #[serde(default)]
    pub start: u32,
    pub ty: TypeExpr,
}

/// Builder for `NodeDescriptor` with deterministic ordering.
///
/// ```
/// use daedalus_registry::store::NodeDescriptorBuilder;
/// use daedalus_data::model::{TypeExpr, ValueType};
/// let node = NodeDescriptorBuilder::new("demo.node")
///     .input("in", TypeExpr::Scalar(ValueType::Int))
///     .output("out", TypeExpr::Scalar(ValueType::Int))
///     .build()
///     .unwrap();
/// assert_eq!(node.inputs.len(), 1);
/// ```
pub struct NodeDescriptorBuilder {
    id: NodeId,
    feature_flags: Vec<String>,
    label: Option<String>,
    group: Option<GroupId>,
    inputs: Vec<Port>,
    fanin_inputs: Vec<FanInPort>,
    outputs: Vec<Port>,
    default_compute: ComputeAffinity,
    sync_groups: Vec<SyncGroup>,
    metadata: BTreeMap<String, daedalus_data::model::Value>,
}

impl NodeDescriptorBuilder {
    /// Start a new node descriptor builder.
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: NodeId::new(id.into()),
            feature_flags: Vec::new(),
            label: None,
            group: None,
            inputs: Vec::new(),
            fanin_inputs: Vec::new(),
            outputs: Vec::new(),
            default_compute: ComputeAffinity::CpuOnly,
            sync_groups: Vec::new(),
            metadata: BTreeMap::new(),
        }
    }

    /// Set a display label.
    pub fn label(mut self, label: impl Into<String>) -> Self {
        self.label = Some(label.into());
        self
    }

    /// Add a required feature flag.
    pub fn feature_flag(mut self, flag: impl Into<String>) -> Self {
        self.feature_flags.push(flag.into());
        self
    }

    /// Add an input port.
    pub fn input(mut self, name: impl Into<String>, ty: TypeExpr) -> Self {
        self = self.input_with_access(name, ty, PortAccessMode::Borrowed);
        self
    }

    /// Add an input port with an explicit access mode.
    pub fn input_with_access(
        mut self,
        name: impl Into<String>,
        ty: TypeExpr,
        access: PortAccessMode,
    ) -> Self {
        self.inputs.push(Port {
            name: name.into(),
            ty,
            access,
            source: None,
            const_value: None,
        });
        self
    }

    /// Declare an indexed fan-in input group. Ports are matched as `{prefix}{N}` where `N >= start`.
    pub fn fanin_input(mut self, prefix: impl Into<String>, start: u32, ty: TypeExpr) -> Self {
        self.fanin_inputs.push(FanInPort {
            prefix: prefix.into(),
            start,
            ty,
        });
        self
    }

    /// Add an output port.
    pub fn output(mut self, name: impl Into<String>, ty: TypeExpr) -> Self {
        self.outputs.push(Port {
            name: name.into(),
            ty,
            access: PortAccessMode::Borrowed,
            source: None,
            const_value: None,
        });
        self
    }

    /// Set the default compute affinity.
    pub fn default_compute(mut self, compute: ComputeAffinity) -> Self {
        self.default_compute = compute;
        self
    }

    /// Add a sync group for this node.
    pub fn sync_group(mut self, group: SyncGroup) -> Self {
        self.sync_groups.push(group);
        self
    }

    /// Add metadata for this node.
    pub fn metadata(mut self, key: impl Into<String>, value: daedalus_data::model::Value) -> Self {
        self.metadata.insert(key.into(), value);
        self
    }

    /// Reference a node group implementation by id.
    pub fn group(mut self, id: impl Into<String>) -> Self {
        self.group = Some(GroupId::new(id.into()));
        self
    }

    /// Extend metadata from an iterator.
    pub fn metadata_map<
        K: Into<String>,
        V: IntoIterator<Item = (K, daedalus_data::model::Value)>,
    >(
        mut self,
        entries: V,
    ) -> Self {
        for (k, v) in entries {
            self.metadata.insert(k.into(), v);
        }
        self
    }

    /// Build and validate the descriptor.
    pub fn build(mut self) -> Result<NodeDescriptor, &'static str> {
        self.inputs.sort_by(|a, b| a.name.cmp(&b.name));
        self.outputs.sort_by(|a, b| a.name.cmp(&b.name));
        self.fanin_inputs.sort_by(|a, b| a.prefix.cmp(&b.prefix));
        let desc = NodeDescriptor {
            id: self.id,
            feature_flags: {
                self.feature_flags.sort();
                self.feature_flags
            },
            label: self.label,
            group: self.group,
            inputs: self.inputs,
            fanin_inputs: self.fanin_inputs,
            outputs: self.outputs,
            default_compute: self.default_compute,
            sync_groups: {
                self.sync_groups.sort_by(|a, b| a.name.cmp(&b.name));
                self.sync_groups
            },
            metadata: self.metadata,
        };
        desc.validate().map(|_| desc)
    }
}

impl NodeDescriptor {
    /// Validate the descriptor for uniqueness and consistency.
    pub fn validate(&self) -> Result<(), &'static str> {
        self.id.validate()?;
        if let Some(group) = &self.group {
            group.validate()?;
        }
        // Ensure deterministic ordering and uniqueness of ports by name.
        let mut inputs = self.inputs.clone();
        inputs.sort_by(|a, b| a.name.cmp(&b.name));
        inputs.dedup_by(|a, b| a.name == b.name);
        if inputs.len() != self.inputs.len() {
            return Err("duplicate input port name");
        }
        let mut fanin = self.fanin_inputs.clone();
        fanin.sort_by(|a, b| a.prefix.cmp(&b.prefix));
        fanin.dedup_by(|a, b| a.prefix == b.prefix);
        if fanin.len() != self.fanin_inputs.len() {
            return Err("duplicate fanin input prefix");
        }
        let mut outputs = self.outputs.clone();
        outputs.sort_by(|a, b| a.name.cmp(&b.name));
        outputs.dedup_by(|a, b| a.name == b.name);
        if outputs.len() != self.outputs.len() {
            return Err("duplicate output port name");
        }
        // Ensure sync group port membership references known ports and is unique.
        for group in &self.sync_groups {
            for p in &group.ports {
                if !self.inputs.iter().any(|inp| &inp.name == p) {
                    return Err("sync group references unknown port");
                }
            }
            let mut ports = group.ports.clone();
            ports.sort();
            ports.dedup();
            if ports.len() != group.ports.len() {
                return Err("duplicate port in sync group");
            }
        }
        // Ensure no port appears in more than one group.
        let mut all_ports: Vec<String> = self
            .sync_groups
            .iter()
            .flat_map(|g| g.ports.clone())
            .collect();
        let total = all_ports.len();
        all_ports.sort();
        all_ports.dedup();
        if all_ports.len() != total {
            return Err("port appears in multiple sync groups");
        }
        Ok(())
    }

    /// Find the declared type for a port name, including fan-in prefixes.
    pub fn input_ty_for(&self, port: &str) -> Option<&TypeExpr> {
        if let Some(p) = self.inputs.iter().find(|p| p.name == port) {
            return Some(&p.ty);
        }
        for spec in &self.fanin_inputs {
            if let Some(idx) = port.strip_prefix(&spec.prefix) {
                if idx.is_empty() || !idx.bytes().all(|b| b.is_ascii_digit()) {
                    continue;
                }
                if let Ok(n) = idx.parse::<u32>()
                    && n >= spec.start
                {
                    return Some(&spec.ty);
                }
            }
        }
        None
    }

    /// Find the declared access mode for an input port name.
    ///
    /// Fan-in prefixes (e.g. `ins0`, `ins1`) default to `Borrowed`.
    pub fn input_access_for(&self, port: &str) -> PortAccessMode {
        if let Some(p) = self.inputs.iter().find(|p| p.name == port) {
            return p.access;
        }
        // Fan-in inputs are implicitly borrowed unless we add access metadata to FanInPort.
        PortAccessMode::Borrowed
    }
}

/// Node port metadata.
///
/// ```
/// use daedalus_registry::store::Port;
/// use daedalus_data::model::{TypeExpr, ValueType};
/// let port = Port { name: "out".into(), ty: TypeExpr::Scalar(ValueType::Bool), access: Default::default(), source: None, const_value: None };
/// assert_eq!(port.name, "out");
/// ```
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum PortAccessMode {
    /// Input is read-only; aliasing/fanout is allowed.
    #[default]
    Borrowed,
    /// Node intends to take ownership of the input value (may still be COW under the hood).
    Owned,
    /// Node intends to mutate the input in-place. This requires exclusivity (no fanout).
    MutBorrowed,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Port {
    pub name: String,
    pub ty: TypeExpr,
    #[serde(default)]
    pub access: PortAccessMode,
    #[serde(default)]
    pub source: Option<String>,
    #[serde(default)]
    pub const_value: Option<Value>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use daedalus_data::descriptor::DescriptorBuilder;
    use daedalus_data::model::{TypeExpr, ValueType};

    #[test]
    fn registers_value_uniquely() {
        let mut reg = Registry::new();
        let desc = DescriptorBuilder::new("a", "1.0")
            .type_expr(TypeExpr::Scalar(ValueType::Int))
            .build()
            .unwrap();
        reg.register_value(desc.clone()).unwrap();
        let err = reg.register_value(desc).unwrap_err();
        assert_eq!(err.code(), RegistryErrorCode::Conflict);
        assert_eq!(
            err.conflict_kind(),
            Some(crate::diagnostics::ConflictKind::Value)
        );
        assert!(matches!(
            err.payload(),
            Some(crate::diagnostics::RegistryErrorCompute::Conflict { .. })
        ));
    }

    #[test]
    fn registers_node_uniquely() {
        let mut reg = Registry::new();
        let node = NodeDescriptorBuilder::new("node")
            .label("Node")
            .input("in", TypeExpr::Scalar(ValueType::Int))
            .output("out", TypeExpr::Scalar(ValueType::Bool))
            .build()
            .unwrap();
        reg.register_node(node.clone()).unwrap();
        let err = reg.register_node(node).unwrap_err();
        assert_eq!(err.code(), RegistryErrorCode::Conflict);
    }

    #[test]
    fn registers_converter_uniquely_and_resolves() {
        struct BoolToInt;
        impl daedalus_data::convert::Converter for BoolToInt {
            fn id(&self) -> daedalus_data::convert::ConverterId {
                daedalus_data::convert::ConverterId("bool_to_int".into())
            }
            fn input(&self) -> &TypeExpr {
                static TY: once_cell::sync::Lazy<TypeExpr> =
                    once_cell::sync::Lazy::new(|| TypeExpr::Scalar(ValueType::Bool));
                &TY
            }
            fn output(&self) -> &TypeExpr {
                static TY: once_cell::sync::Lazy<TypeExpr> =
                    once_cell::sync::Lazy::new(|| TypeExpr::Scalar(ValueType::Int));
                &TY
            }
            fn cost(&self) -> u64 {
                1
            }
            fn convert(
                &self,
                v: daedalus_data::model::Value,
            ) -> daedalus_data::errors::DataResult<daedalus_data::model::Value> {
                match v {
                    daedalus_data::model::Value::Bool(b) => {
                        Ok(daedalus_data::model::Value::Int(if b { 1 } else { 0 }))
                    }
                    _ => Err(daedalus_data::errors::DataError::new(
                        daedalus_data::errors::DataErrorCode::InvalidType,
                        "expected bool",
                    )),
                }
            }
        }

        let mut reg = Registry::new();
        reg.register_converter(Box::new(BoolToInt)).unwrap();
        let err = reg.register_converter(Box::new(BoolToInt)).unwrap_err();
        assert_eq!(err.code(), RegistryErrorCode::Conflict);

        let from = TypeExpr::Scalar(ValueType::Bool);
        let to = TypeExpr::Scalar(ValueType::Int);
        let res = reg.resolve_converter(&from, &to).unwrap();
        assert_eq!(res.provenance.steps.len(), 1);
    }

    #[test]
    fn view_is_deterministic() {
        let mut reg = Registry::new();
        let d1 = DescriptorBuilder::new("b", "1.0")
            .type_expr(TypeExpr::Scalar(ValueType::Int))
            .build()
            .unwrap();
        let d2 = DescriptorBuilder::new("a", "1.0")
            .type_expr(TypeExpr::Scalar(ValueType::Int))
            .build()
            .unwrap();
        reg.register_value(d1).unwrap();
        reg.register_value(d2).unwrap();
        let snap = reg.snapshot();
        assert_eq!(snap.values, vec!["a@1.0".to_string(), "b@1.0".to_string()]);
    }

    #[test]
    fn snapshot_is_deterministic() {
        let mut reg = Registry::new();
        reg.register_value(
            DescriptorBuilder::new("b", "1.0")
                .type_expr(TypeExpr::Scalar(ValueType::Int))
                .feature_flag("feat")
                .build()
                .unwrap(),
        )
        .unwrap();
        reg.register_value(
            DescriptorBuilder::new("a", "1.0")
                .type_expr(TypeExpr::Scalar(ValueType::Bool))
                .build()
                .unwrap(),
        )
        .unwrap();
        let node = NodeDescriptor {
            id: NodeId::new("n2"),
            feature_flags: vec![],
            label: None,
            group: None,
            inputs: Vec::new(),
            fanin_inputs: Vec::new(),
            outputs: Vec::new(),
            default_compute: ComputeAffinity::CpuOnly,
            sync_groups: Vec::new(),
            metadata: Default::default(),
        };
        reg.register_node(node).unwrap();
        let snap = reg.snapshot();
        assert_eq!(snap.values, vec!["a@1.0".to_string(), "b@1.0".to_string()]);
        assert_eq!(snap.nodes, vec!["n2: inputs=0 outputs=0".to_string()]);
    }
}
