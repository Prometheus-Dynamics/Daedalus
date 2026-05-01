use std::collections::BTreeMap;

use daedalus_data::model::{TypeExpr, Value};
use daedalus_transport::{
    AccessMode, AdaptCost, AdaptKind, AdaptRequest, AdapterId, BoundaryTypeContract, Layout,
    Residency, TypeKey,
};
use serde::{Deserialize, Serialize};

use crate::ids::NodeId;

use super::support::{ActiveFeatureSet, features_enabled};

/// Plugin-level capability manifest.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PluginManifest {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub dependencies: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub provided_types: Vec<TypeKey>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub provided_nodes: Vec<NodeId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub provided_adapters: Vec<AdapterId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub provided_serializers: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub provided_devices: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub boundary_contracts: Vec<BoundaryTypeContract>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required_host_capabilities: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub feature_flags: Vec<String>,
}

impl PluginManifest {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            ..Self::default()
        }
    }

    pub fn version(mut self, version: impl Into<String>) -> Self {
        self.version = Some(version.into());
        self
    }

    pub fn dependency(mut self, id: impl Into<String>) -> Self {
        self.dependencies.push(id.into());
        self
    }

    pub fn provided_type(mut self, key: impl Into<TypeKey>) -> Self {
        self.provided_types.push(key.into());
        self
    }

    pub fn provided_node(mut self, id: impl Into<String>) -> Self {
        self.provided_nodes.push(NodeId::new(id.into()));
        self
    }

    pub fn provided_adapter(mut self, id: impl Into<String>) -> Self {
        self.provided_adapters.push(AdapterId::new(id.into()));
        self
    }

    pub fn provided_serializer(mut self, id: impl Into<String>) -> Self {
        self.provided_serializers.push(id.into());
        self
    }

    pub fn provided_device(mut self, id: impl Into<String>) -> Self {
        self.provided_devices.push(id.into());
        self
    }

    pub fn boundary_contract(mut self, contract: BoundaryTypeContract) -> Self {
        self.boundary_contracts.push(contract);
        self
    }

    pub fn required_host_capability(mut self, capability: impl Into<String>) -> Self {
        self.required_host_capabilities.push(capability.into());
        self
    }

    pub fn feature_flag(mut self, flag: impl Into<String>) -> Self {
        self.feature_flags.push(flag.into());
        self
    }

    pub(super) fn normalize(mut self) -> Self {
        self.dependencies.sort();
        self.dependencies.dedup();
        self.provided_types.sort();
        self.provided_types.dedup();
        self.provided_nodes.sort();
        self.provided_nodes.dedup();
        self.provided_adapters.sort();
        self.provided_adapters.dedup();
        self.provided_serializers.sort();
        self.provided_serializers.dedup();
        self.provided_devices.sort();
        self.provided_devices.dedup();
        self.boundary_contracts
            .sort_by(|a, b| a.type_key.cmp(&b.type_key));
        self.boundary_contracts
            .dedup_by(|a, b| a.type_key == b.type_key);
        self.required_host_capabilities.sort();
        self.required_host_capabilities.dedup();
        self.feature_flags.sort();
        self.feature_flags.dedup();
        self
    }
}

/// Host/boundary export policy for a transport type.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExportPolicy {
    #[default]
    None,
    Value,
    Bytes,
    Handle,
    Native,
}

/// Stable type declaration used by planner, tooling, plugins, and host boundaries.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TypeDecl {
    pub key: TypeKey,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rust: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<TypeExpr>,
    #[serde(default)]
    pub export: ExportPolicy,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub capabilities: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub feature_flags: Vec<String>,
}

impl TypeDecl {
    pub fn new(key: impl Into<TypeKey>) -> Self {
        Self {
            key: key.into(),
            rust: None,
            schema: None,
            export: ExportPolicy::None,
            capabilities: Vec::new(),
            feature_flags: Vec::new(),
        }
    }

    pub fn rust<T: 'static>(mut self) -> Self {
        self.rust = Some(std::any::type_name::<T>().to_string());
        self
    }

    pub fn schema(mut self, schema: TypeExpr) -> Self {
        self.schema = Some(schema);
        self
    }

    pub fn export(mut self, export: ExportPolicy) -> Self {
        self.export = export;
        self
    }

    pub fn capability(mut self, capability: impl Into<String>) -> Self {
        self.capabilities.push(capability.into());
        self
    }

    pub fn feature_flag(mut self, flag: impl Into<String>) -> Self {
        self.feature_flags.push(flag.into());
        self
    }

    pub(super) fn normalize(mut self) -> Self {
        self.capabilities.sort();
        self.capabilities.dedup();
        self.feature_flags.sort();
        self.feature_flags.dedup();
        self
    }
}

/// Serializable adapter declaration. Runtime executable functions live in host-owned tables.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdapterDecl {
    pub id: AdapterId,
    pub from: TypeKey,
    pub to: TypeKey,
    pub kind: AdaptKind,
    pub cost: AdaptCost,
    pub access: AccessMode,
    #[serde(default)]
    pub requires_gpu: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub residency: Option<Residency>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub layout: Option<Layout>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub feature_flags: Vec<String>,
}

/// Resolved adapter path with deterministic total cost.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdapterPathResolution {
    pub steps: Vec<AdapterId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub step_details: Vec<AdapterPathStep>,
    pub total_cost: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_target: Option<TypeKey>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdapterPathStep {
    pub adapter: AdapterId,
    pub from: TypeKey,
    pub to: TypeKey,
    pub kind: AdaptKind,
    pub access: AccessMode,
    pub cost: AdaptCost,
    #[serde(default, skip_serializing_if = "is_false")]
    pub requires_gpu: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub residency: Option<Residency>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub layout: Option<Layout>,
}

impl AdapterPathStep {
    pub(super) fn from_decl(adapter: &AdapterDecl) -> Self {
        Self {
            adapter: adapter.id.clone(),
            from: adapter.from.clone(),
            to: adapter.to.clone(),
            kind: adapter.kind,
            access: adapter.access,
            cost: adapter.cost,
            requires_gpu: adapter.requires_gpu,
            residency: adapter.residency,
            layout: adapter.layout.clone(),
        }
    }
}

fn is_false(value: &bool) -> bool {
    !*value
}

impl AdapterDecl {
    pub fn new(id: impl Into<String>, from: impl Into<TypeKey>, to: impl Into<TypeKey>) -> Self {
        let cost = AdaptCost::default();
        Self {
            id: AdapterId::new(id),
            from: from.into(),
            to: to.into(),
            kind: cost.kind,
            cost,
            access: AccessMode::Read,
            requires_gpu: false,
            residency: None,
            layout: None,
            feature_flags: Vec::new(),
        }
    }

    pub fn kind(mut self, kind: AdaptKind) -> Self {
        self.kind = kind;
        self.cost.kind = kind;
        self
    }

    pub fn cost(mut self, cost: AdaptCost) -> Self {
        self.kind = cost.kind;
        self.cost = cost;
        self
    }

    pub fn access(mut self, access: AccessMode) -> Self {
        self.access = access;
        self
    }

    pub fn requires_gpu(mut self, requires_gpu: bool) -> Self {
        self.requires_gpu = requires_gpu;
        self
    }

    pub fn residency(mut self, residency: Residency) -> Self {
        self.residency = Some(residency);
        self
    }

    pub fn layout(mut self, layout: impl Into<Layout>) -> Self {
        self.layout = Some(layout.into());
        self
    }

    pub fn feature_flag(mut self, flag: impl Into<String>) -> Self {
        self.feature_flags.push(flag.into());
        self
    }

    pub(super) fn normalize(mut self) -> Self {
        self.feature_flags.sort();
        self.feature_flags.dedup();
        self
    }
}

/// Access-aware node port declaration for the new transport model.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PortDecl {
    pub name: String,
    pub type_key: TypeKey,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<TypeExpr>,
    #[serde(default)]
    pub access: AccessMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub residency: Option<Residency>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub layout: Option<Layout>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub const_value_json: Option<String>,
}

/// Indexed fan-in input declaration for ports named `{prefix}{N}`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FanInDecl {
    pub prefix: String,
    #[serde(default)]
    pub start: u32,
    pub type_key: TypeKey,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<TypeExpr>,
}

impl FanInDecl {
    pub fn new(prefix: impl Into<String>, start: u32, type_key: impl Into<TypeKey>) -> Self {
        Self {
            prefix: prefix.into(),
            start,
            type_key: type_key.into(),
            schema: None,
        }
    }

    pub fn schema(mut self, schema: TypeExpr) -> Self {
        self.schema = Some(schema);
        self
    }
}

impl PortDecl {
    pub fn new(name: impl Into<String>, type_key: impl Into<TypeKey>) -> Self {
        Self {
            name: name.into(),
            type_key: type_key.into(),
            schema: None,
            access: AccessMode::Read,
            residency: None,
            layout: None,
            source: None,
            const_value_json: None,
        }
    }

    pub fn schema(mut self, schema: TypeExpr) -> Self {
        self.schema = Some(schema);
        self
    }

    pub fn access(mut self, access: AccessMode) -> Self {
        self.access = access;
        self
    }

    pub fn residency(mut self, residency: Residency) -> Self {
        self.residency = Some(residency);
        self
    }

    pub fn layout(mut self, layout: impl Into<Layout>) -> Self {
        self.layout = Some(layout.into());
        self
    }

    pub fn source(mut self, source: impl Into<String>) -> Self {
        self.source = Some(source.into());
        self
    }

    pub fn const_value(mut self, value: daedalus_data::model::Value) -> Self {
        self.const_value_json = serde_json::to_string(&value).ok();
        self
    }
}

pub const NODE_EXECUTION_KIND_META_KEY: &str = "daedalus.node.execution";

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeExecutionKind {
    #[default]
    HandlerRequired,
    HostBridge,
    External,
    NoOp,
}

impl NodeExecutionKind {
    pub fn as_str(self) -> &'static str {
        match self {
            NodeExecutionKind::HandlerRequired => "handler_required",
            NodeExecutionKind::HostBridge => "host_bridge",
            NodeExecutionKind::External => "external",
            NodeExecutionKind::NoOp => "no_op",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "handler_required" => Some(NodeExecutionKind::HandlerRequired),
            "host_bridge" => Some(NodeExecutionKind::HostBridge),
            "external" => Some(NodeExecutionKind::External),
            "no_op" => Some(NodeExecutionKind::NoOp),
            _ => None,
        }
    }

    pub fn from_metadata_value(value: &Value) -> Option<Self> {
        match value {
            Value::String(value) => Self::parse(value),
            _ => None,
        }
    }

    pub fn from_metadata(metadata: &BTreeMap<String, Value>) -> Option<Self> {
        metadata
            .get(NODE_EXECUTION_KIND_META_KEY)
            .and_then(Self::from_metadata_value)
    }

    pub fn write_to_metadata(self, metadata: &mut BTreeMap<String, Value>) {
        metadata.insert(
            NODE_EXECUTION_KIND_META_KEY.to_string(),
            Value::String(self.as_str().into()),
        );
    }

    pub fn write_default_to_metadata(self, metadata: &mut BTreeMap<String, Value>) {
        metadata
            .entry(NODE_EXECUTION_KIND_META_KEY.to_string())
            .or_insert_with(|| Value::String(self.as_str().into()));
    }
}

#[cfg(test)]
mod node_execution_kind_tests {
    use super::*;

    #[test]
    fn node_execution_kind_round_trips_through_typed_metadata_helpers() {
        let mut metadata = BTreeMap::new();

        NodeExecutionKind::External.write_to_metadata(&mut metadata);
        assert_eq!(
            NodeExecutionKind::from_metadata(&metadata),
            Some(NodeExecutionKind::External)
        );

        NodeExecutionKind::NoOp.write_default_to_metadata(&mut metadata);
        assert_eq!(
            NodeExecutionKind::from_metadata(&metadata),
            Some(NodeExecutionKind::External)
        );
    }
}

/// New transport-aware node declaration.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeDecl {
    pub id: NodeId,
    #[serde(
        default,
        skip_serializing_if = "NodeExecutionKind::is_handler_required"
    )]
    pub execution_kind: NodeExecutionKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub inputs: Vec<PortDecl>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub fanin_inputs: Vec<FanInDecl>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub outputs: Vec<PortDecl>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub feature_flags: Vec<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata_json: BTreeMap<String, String>,
}

impl NodeDecl {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: NodeId::new(id.into()),
            execution_kind: NodeExecutionKind::HandlerRequired,
            label: None,
            inputs: Vec::new(),
            fanin_inputs: Vec::new(),
            outputs: Vec::new(),
            feature_flags: Vec::new(),
            metadata_json: BTreeMap::new(),
        }
    }

    pub fn label(mut self, label: impl Into<String>) -> Self {
        self.label = Some(label.into());
        self
    }

    pub fn execution_kind(mut self, kind: NodeExecutionKind) -> Self {
        self.execution_kind = kind;
        self
    }

    pub fn external(mut self) -> Self {
        self.execution_kind = NodeExecutionKind::External;
        self
    }

    pub fn no_op(mut self) -> Self {
        self.execution_kind = NodeExecutionKind::NoOp;
        self
    }

    pub fn input(mut self, port: PortDecl) -> Self {
        self.inputs.push(port);
        self
    }

    pub fn fanin_input(mut self, fanin: FanInDecl) -> Self {
        self.fanin_inputs.push(fanin);
        self
    }

    pub fn output(mut self, port: PortDecl) -> Self {
        self.outputs.push(port);
        self
    }

    pub fn feature_flag(mut self, flag: impl Into<String>) -> Self {
        self.feature_flags.push(flag.into());
        self
    }

    pub fn metadata(mut self, key: impl Into<String>, value: daedalus_data::model::Value) -> Self {
        if let Ok(json) = serde_json::to_string(&value) {
            self.metadata_json.insert(key.into(), json);
        }
        self
    }

    pub(super) fn normalize(mut self) -> Self {
        self.inputs.sort_by(|a, b| a.name.cmp(&b.name));
        self.fanin_inputs.sort_by(|a, b| a.prefix.cmp(&b.prefix));
        self.outputs.sort_by(|a, b| a.name.cmp(&b.name));
        self.feature_flags.sort();
        self.feature_flags.dedup();
        self
    }
}

impl NodeExecutionKind {
    fn is_handler_required(kind: &Self) -> bool {
        matches!(kind, NodeExecutionKind::HandlerRequired)
    }
}

/// Boundary serializer declaration. Runtime functions live in host-owned tables.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SerializerDecl {
    pub id: String,
    pub type_key: TypeKey,
    pub export: ExportPolicy,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub feature_flags: Vec<String>,
}

impl SerializerDecl {
    pub fn new(id: impl Into<String>, type_key: impl Into<TypeKey>, export: ExportPolicy) -> Self {
        Self {
            id: id.into(),
            type_key: type_key.into(),
            export,
            feature_flags: Vec::new(),
        }
    }

    pub fn feature_flag(mut self, flag: impl Into<String>) -> Self {
        self.feature_flags.push(flag.into());
        self
    }

    pub(super) fn normalize(mut self) -> Self {
        self.feature_flags.sort();
        self.feature_flags.dedup();
        self
    }
}

/// Device capability declaration for upload/download/branch operations.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeviceDecl {
    pub id: String,
    pub cpu: TypeKey,
    pub device: TypeKey,
    pub upload: AdapterId,
    pub download: AdapterId,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub capabilities: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub feature_flags: Vec<String>,
}

impl DeviceDecl {
    pub fn new(
        id: impl Into<String>,
        cpu: impl Into<TypeKey>,
        device: impl Into<TypeKey>,
        upload: impl Into<String>,
        download: impl Into<String>,
    ) -> Self {
        Self {
            id: id.into(),
            cpu: cpu.into(),
            device: device.into(),
            upload: AdapterId::new(upload),
            download: AdapterId::new(download),
            capabilities: Vec::new(),
            feature_flags: Vec::new(),
        }
    }

    pub fn capability(mut self, capability: impl Into<String>) -> Self {
        self.capabilities.push(capability.into());
        self
    }

    pub fn feature_flag(mut self, flag: impl Into<String>) -> Self {
        self.feature_flags.push(flag.into());
        self
    }

    pub(super) fn normalize(mut self) -> Self {
        self.capabilities.sort();
        self.capabilities.dedup();
        self.feature_flags.sort();
        self.feature_flags.dedup();
        self
    }
}

impl AdapterDecl {
    pub(super) fn enabled_in_context(
        &self,
        active_features: &ActiveFeatureSet<'_>,
        allow_gpu: bool,
    ) -> bool {
        (!self.requires_gpu || allow_gpu) && features_enabled(&self.feature_flags, active_features)
    }

    pub(super) fn matches_request(&self, request: &AdaptRequest) -> bool {
        if request.exclusive
            && matches!(
                self.kind,
                AdaptKind::Identity
                    | AdaptKind::Reinterpret
                    | AdaptKind::View
                    | AdaptKind::SharedView
                    | AdaptKind::MetadataOnly
            )
        {
            return false;
        }
        if !self.access.satisfies(request.access) {
            return false;
        }
        if let Some(required) = request.residency
            && self.residency != Some(required)
        {
            return false;
        }
        if let Some(required) = &request.layout
            && self.layout.as_ref() != Some(required)
        {
            return false;
        }
        true
    }
}
