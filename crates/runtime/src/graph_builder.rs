mod connection;
mod context;
mod edge_policy;
mod metadata;
mod nested;
mod scope;
mod spec;

pub use context::GraphCtx;
pub use nested::{NestedGraph, NestedGraphHandle};
pub use scope::GraphScope;
pub use spec::{GraphBuildError, IntoPortSpec, NodeSpec, PortSpec};

use crate::handles::{NodeHandleLike, PortHandle};
use crate::host_bridge::HOST_BRIDGE_META_KEY;
use daedalus_core::metadata::{DYNAMIC_INPUTS_KEY, DYNAMIC_OUTPUTS_KEY};
use daedalus_data::model::Value;
use daedalus_planner::{ComputeAffinity, Edge, Graph, NodeInstance, NodeRef, PortRef};
use daedalus_registry::{capability::CapabilityRegistry, ids::NodeId};
use std::collections::{BTreeMap, HashMap};

use self::metadata::{const_value_from_port_decl, metadata_from_node_decl};
use self::nested::is_host_bridge;
use crate::host_bridge::HOST_BRIDGE_ID;
use crate::plan::RuntimeEdgePolicy;

/// Graph builder with alias and basic port validation using native transport capabilities.
///
/// Prefer the fallible `try_*` methods for release-facing code and tooling that accepts external
/// graph definitions. The shorter convenience methods remain available for examples and fixtures,
/// but user-caused wiring errors such as unknown aliases, duplicate nested aliases, or missing
/// nested ports may panic with an actionable message.
#[derive(Clone)]
pub struct GraphBuilder {
    capabilities: CapabilityRegistry,
    nodes: Vec<NodeInstance>,
    edges: Vec<Edge>,
    const_overrides: HashMap<String, HashMap<String, Option<Value>>>,
    node_metadata_overrides: HashMap<String, BTreeMap<String, Value>>,
    graph_metadata: BTreeMap<String, Value>,
    injected_node_metadata: BTreeMap<String, Value>,
    injected_node_metadata_overwrite: BTreeMap<String, Value>,
    host_bridge_alias: Option<String>,
    host_bridge_added: bool,
    nested: HashMap<String, NestedGraphHandle>,
}

impl GraphBuilder {
    pub fn new(capabilities: CapabilityRegistry) -> Self {
        Self {
            capabilities,
            nodes: Vec::new(),
            edges: Vec::new(),
            const_overrides: HashMap::new(),
            node_metadata_overrides: HashMap::new(),
            graph_metadata: BTreeMap::new(),
            injected_node_metadata: BTreeMap::new(),
            injected_node_metadata_overwrite: BTreeMap::new(),
            host_bridge_alias: Some("host".to_string()),
            host_bridge_added: false,
            nested: HashMap::new(),
        }
    }

    pub fn new_with_capabilities(capabilities: CapabilityRegistry) -> Self {
        Self::new(capabilities)
    }

    pub fn named(capabilities: CapabilityRegistry, name: impl Into<String>) -> Self {
        Self::new(capabilities).graph_metadata("name", name)
    }

    /// Attach graph-level metadata to the built graph (`Graph.metadata`).
    pub fn graph_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        let key = key.into();
        let value = value.into();
        self.graph_metadata.insert(key, Value::String(value.into()));
        self
    }

    /// Attach graph-level metadata (typed `Value`) to the built graph (`Graph.metadata`).
    ///
    /// Nodes can read this via `ExecutionContext.graph_metadata` at runtime.
    pub fn graph_metadata_value(mut self, key: impl Into<String>, value: Value) -> Self {
        self.graph_metadata.insert(key.into(), value);
        self
    }

    /// Inject a key/value into every node's metadata, without overwriting existing keys.
    ///
    /// This is the ergonomic way to "broadcast" host-provided metadata to all nodes so it shows up
    /// in each node's `ExecutionContext.metadata` at runtime.
    pub fn inject_node_metadata(mut self, key: impl Into<String>, value: Value) -> Self {
        self.injected_node_metadata.insert(key.into(), value);
        self
    }

    /// Inject a key/value into every node's metadata, overwriting existing keys.
    pub fn inject_node_metadata_overwrite(mut self, key: impl Into<String>, value: Value) -> Self {
        self.injected_node_metadata_overwrite
            .insert(key.into(), value);
        self
    }

    /// Add a node via a pre-built spec (useful with plugin helpers).
    pub fn node_spec(self, spec: NodeSpec, alias: &str) -> Self {
        self.node_id(&spec.id, alias)
    }

    /// Fallible version of [`Self::node_spec`] that requires a registered node declaration.
    pub fn try_node_spec(self, spec: NodeSpec, alias: &str) -> Result<Self, GraphBuildError> {
        self.try_node_id(&spec.id, alias)
    }

    /// Add a node using any handle that exposes id/alias.
    pub fn node_handle_like(self, handle: &dyn NodeHandleLike) -> Self {
        self.node_id(handle.id(), handle.alias())
    }

    /// Fallible version of [`Self::node_handle_like`] that requires a registered node declaration.
    pub fn try_node_handle_like(
        self,
        handle: &dyn NodeHandleLike,
    ) -> Result<Self, GraphBuildError> {
        self.try_node_id(handle.id(), handle.alias())
    }

    /// Add a node using a typed handle (preferred).
    pub fn node<H>(self, handle: H) -> Self
    where
        H: NodeHandleLike,
    {
        self.node_id(handle.id(), handle.alias())
    }

    /// Fallible version of [`Self::node`] that requires a registered node declaration.
    pub fn try_node<H>(self, handle: H) -> Result<Self, GraphBuildError>
    where
        H: NodeHandleLike,
    {
        self.try_node_id(handle.id(), handle.alias())
    }

    /// Add a node by id.
    pub fn node_from_id(self, id: &str, alias: &str) -> Self {
        self.node_id(id, alias)
    }

    /// Fallible version of [`Self::node_from_id`] that requires a registered node declaration.
    pub fn try_node_from_id(self, id: &str, alias: &str) -> Result<Self, GraphBuildError> {
        self.try_node_id(id, alias)
    }

    /// Add a node by id and report a typed error when the capability registry does not know it.
    pub fn try_node_id(self, id: &str, alias: &str) -> Result<Self, GraphBuildError> {
        if self.capabilities.nodes().contains_key(&NodeId::new(id)) {
            Ok(self.node_id(id, alias))
        } else {
            Err(GraphBuildError::MissingNodeId { id: id.to_string() })
        }
    }

    /// Add a node using its descriptor default compute affinity (or override via `node_with_compute`).
    pub fn node_id(mut self, id: &str, alias: &str) -> Self {
        let decl = self.capabilities.nodes().get(&NodeId::new(id));
        let ports = decl
            .map(|decl| {
                (
                    decl.inputs.iter().map(|p| p.name.clone()).collect(),
                    decl.outputs.iter().map(|p| p.name.clone()).collect(),
                )
            })
            .unwrap_or_default();

        // Base consts from native node declaration, if any.
        let mut const_inputs = Vec::new();
        let compute = ComputeAffinity::CpuOnly;
        let mut metadata: BTreeMap<String, Value> = BTreeMap::new();
        let sync_groups = Vec::new();
        if let Some(decl) = decl {
            metadata = metadata_from_node_decl(decl);
            for port in &decl.inputs {
                if let Some(v) = const_value_from_port_decl(port) {
                    const_inputs.push((port.name.clone(), v));
                }
            }
        }
        if let Some(over) = self.const_overrides.get(alias) {
            for (p, v) in over {
                match v {
                    Some(val) => {
                        const_inputs.retain(|(name, _)| name != p);
                        const_inputs.push((p.clone(), val.clone()));
                    }
                    None => const_inputs.retain(|(name, _)| name != p),
                }
            }
        }
        if let Some(overrides) = self.node_metadata_overrides.get(alias) {
            for (k, v) in overrides {
                metadata.insert(k.clone(), v.clone());
            }
        }

        self.nodes.push(NodeInstance {
            id: daedalus_registry::ids::NodeId::new(id),
            bundle: Some(id.to_string()),
            label: Some(alias.to_string()),
            inputs: ports.0,
            outputs: ports.1,
            compute,
            const_inputs,
            sync_groups,
            metadata,
        });
        self
    }

    /// Add a node with an explicit compute affinity override.
    pub fn node_with_compute(mut self, id: &str, alias: &str, compute: ComputeAffinity) -> Self {
        self = self.node_id(id, alias);
        if let Some(last) = self.nodes.last_mut() {
            last.compute = compute;
        }
        self
    }

    /// Attach sync groups metadata to the most recently added node.
    pub fn sync_groups(mut self, groups: Vec<daedalus_core::sync::SyncGroup>) -> Self {
        if let Some(last) = self.nodes.last_mut() {
            last.sync_groups = groups;
        }
        self
    }

    /// Attach or override metadata for a node handle. Values can store arbitrary UI hints
    /// (e.g. positions, styles) without changing core types.
    pub fn node_metadata(
        self,
        handle: &impl NodeHandleLike,
        key: impl Into<String>,
        value: Value,
    ) -> Self {
        self.node_metadata_by_id(handle.alias(), key, value)
    }

    /// Attach or override metadata for a node alias. Values can store arbitrary UI hints
    /// (e.g. positions, styles) without changing core types.
    pub fn node_metadata_by_id(
        mut self,
        node_alias: impl Into<String>,
        key: impl Into<String>,
        value: Value,
    ) -> Self {
        let alias = node_alias.into();
        let key = key.into();
        let entry = self
            .node_metadata_overrides
            .entry(alias.clone())
            .or_default();
        entry.insert(key.clone(), value.clone());
        if let Some(node) = self
            .nodes
            .iter_mut()
            .find(|n| n.label.as_deref() == Some(alias.as_str()))
        {
            node.metadata.insert(key, value);
        }
        self
    }

    /// Bulk metadata helper for a node handle.
    pub fn node_metadata_map<H, K, I>(self, handle: &H, metadata: I) -> Self
    where
        H: NodeHandleLike,
        K: Into<String>,
        I: IntoIterator<Item = (K, Value)>,
    {
        self.node_metadata_map_by_id(handle.alias(), metadata)
    }

    /// Bulk metadata helper for a node alias.
    pub fn node_metadata_map_by_id<K, I>(
        mut self,
        node_alias: impl Into<String>,
        metadata: I,
    ) -> Self
    where
        K: Into<String>,
        I: IntoIterator<Item = (K, Value)>,
    {
        let alias = node_alias.into();
        for (k, v) in metadata {
            self = self.node_metadata_by_id(alias.clone(), k.into(), v);
        }
        self
    }

    /// Set or unset a constant value for a node alias/port. None unsets/ignores default.
    pub fn const_input(mut self, port: &PortHandle, value: Option<Value>) -> Self {
        let alias = port.node_alias().to_string();
        let port_name = port.port().to_string();
        let entry = self.const_overrides.entry(alias.clone()).or_default();
        entry.insert(port_name.clone(), value.clone());
        if let Some(node) = self
            .nodes
            .iter_mut()
            .find(|n| n.label.as_deref() == Some(alias.as_str()))
        {
            match value {
                Some(v) => {
                    node.const_inputs.retain(|(name, _)| name != &port_name);
                    node.const_inputs.push((port_name, v));
                }
                None => node.const_inputs.retain(|(name, _)| name != &port_name),
            }
        }
        self
    }

    /// Ensure a host-bridge node exists with the provided alias (one per graph).
    pub fn host_bridge(mut self, alias: impl Into<String>) -> Self {
        let alias = alias.into();
        self.host_bridge_alias = Some(alias.clone());
        self.ensure_host_bridge(Some(alias))
    }

    fn ensure_host_bridge(mut self, alias: Option<String>) -> Self {
        let alias = alias
            .or_else(|| self.host_bridge_alias.clone())
            .unwrap_or_else(|| "host".to_string());
        if self.host_bridge_added {
            return self;
        }
        let exists = self
            .nodes
            .iter()
            .any(|n| n.label.as_deref() == Some(alias.as_str()) || n.id.0 == HOST_BRIDGE_ID);
        if exists {
            self.host_bridge_added = true;
            return self;
        }
        self.host_bridge_added = true;
        self.nodes.push(NodeInstance {
            id: daedalus_registry::ids::NodeId::new(HOST_BRIDGE_ID),
            bundle: None,
            label: Some(alias),
            inputs: Vec::new(),
            outputs: Vec::new(),
            compute: ComputeAffinity::CpuOnly,
            const_inputs: Vec::new(),
            sync_groups: Vec::new(),
            metadata: BTreeMap::from([
                (HOST_BRIDGE_META_KEY.to_string(), Value::Bool(true)),
                // Allow arbitrary host ports without registry-declared schemas.
                // The planner treats `Opaque("generic")` as a type variable and infers
                // concrete types from graph edges.
                (
                    DYNAMIC_INPUTS_KEY.to_string(),
                    Value::String(std::borrow::Cow::from("generic")),
                ),
                (
                    DYNAMIC_OUTPUTS_KEY.to_string(),
                    Value::String(std::borrow::Cow::from("generic")),
                ),
            ]),
        });
        self
    }

    pub(crate) fn ensure_host_bridge_port(mut self, is_output: bool, port: &str) -> Self {
        let host_alias = self
            .host_bridge_alias
            .clone()
            .unwrap_or_else(|| "host".to_string());
        if let Some(host) = self.nodes.iter_mut().find(|n| {
            is_host_bridge(n)
                && (n.label.as_deref() == Some(host_alias.as_str()) || n.id.0 == HOST_BRIDGE_ID)
        }) {
            let ports = if is_output {
                &mut host.outputs
            } else {
                &mut host.inputs
            };
            if !ports.iter().any(|p| p == port) {
                ports.push(port.to_string());
            }
        }
        self
    }

    /// Set or unset a constant value by explicit id/port tuple.
    pub fn const_input_by_id(
        mut self,
        node_alias: impl Into<String>,
        port: impl Into<String>,
        value: Option<Value>,
    ) -> Self {
        let node_alias = node_alias.into();
        let port = port.into();
        let entry = self.const_overrides.entry(node_alias.clone()).or_default();
        entry.insert(port.clone(), value.clone());
        if let Some(node) = self
            .nodes
            .iter_mut()
            .find(|n| n.label.as_deref() == Some(node_alias.as_str()))
        {
            match value {
                Some(v) => {
                    node.const_inputs.retain(|(name, _)| name != &port);
                    node.const_inputs.push((port, v));
                }
                None => node.const_inputs.retain(|(name, _)| name != &port),
            }
        }
        self
    }

    /// Inline another graph by prefixing node labels with `alias` and wiring its host bridge
    /// to return a handle representing the nested inputs/outputs.
    ///
    /// # Panics
    ///
    /// Panics when `alias` is already used by a node or nested graph in this
    /// builder.
    pub fn nest(self, nested: &NestedGraph, alias: impl Into<String>) -> (Self, NestedGraphHandle) {
        self.try_nest(nested, alias)
            .unwrap_or_else(|err| panic!("{err}"))
    }

    pub fn try_nest(
        mut self,
        nested: &NestedGraph,
        alias: impl Into<String>,
    ) -> Result<(Self, NestedGraphHandle), GraphBuildError> {
        let alias = alias.into();
        if self.nested.contains_key(&alias)
            || self
                .nodes
                .iter()
                .any(|n| n.label.as_deref() == Some(alias.as_str()))
        {
            return Err(GraphBuildError::DuplicateNestedAlias { alias });
        }
        let prefix = format!("{alias}::");
        let mut index_map: Vec<Option<usize>> = vec![None; nested.graph.nodes.len()];

        for (idx, node) in nested.graph.nodes.iter().enumerate() {
            if idx == nested.host_index {
                continue;
            }
            let mut cloned = node.clone();
            let base_label = cloned.label.clone().unwrap_or_else(|| cloned.id.0.clone());
            cloned.label = Some(format!("{prefix}{base_label}"));
            let new_idx = self.nodes.len();
            self.nodes.push(cloned);
            index_map[idx] = Some(new_idx);
        }

        let mut inputs: BTreeMap<String, Vec<PortRef>> = BTreeMap::new();
        let mut outputs: BTreeMap<String, Vec<PortRef>> = BTreeMap::new();

        for edge in &nested.graph.edges {
            let from_is_host = edge.from.node.0 == nested.host_index;
            let to_is_host = edge.to.node.0 == nested.host_index;

            match (from_is_host, to_is_host) {
                (true, false) => {
                    if let Some(target_idx) = index_map[edge.to.node.0] {
                        inputs
                            .entry(edge.from.port.clone())
                            .or_default()
                            .push(PortRef {
                                node: NodeRef(target_idx),
                                port: edge.to.port.clone(),
                            });
                    }
                }
                (false, true) => {
                    if let Some(source_idx) = index_map[edge.from.node.0] {
                        outputs
                            .entry(edge.to.port.clone())
                            .or_default()
                            .push(PortRef {
                                node: NodeRef(source_idx),
                                port: edge.from.port.clone(),
                            });
                    }
                }
                (false, false) => {
                    let Some(from_idx) = index_map[edge.from.node.0] else {
                        continue;
                    };
                    let Some(to_idx) = index_map[edge.to.node.0] else {
                        continue;
                    };

                    self.edges.push(Edge {
                        from: PortRef {
                            node: NodeRef(from_idx),
                            port: edge.from.port.clone(),
                        },
                        to: PortRef {
                            node: NodeRef(to_idx),
                            port: edge.to.port.clone(),
                        },
                        metadata: edge.metadata.clone(),
                    });
                }
                (true, true) => {}
            }
        }

        let handle = NestedGraphHandle {
            alias: alias.clone(),
            inputs,
            outputs,
        };

        self.nested.insert(alias.clone(), handle.clone());
        Ok((self, handle))
    }
}
pub fn graph_to_json(graph: &Graph) -> Result<String, serde_json::Error> {
    serde_json::to_string(graph)
}

#[cfg(test)]
#[path = "graph_builder_scope_tests.rs"]
mod scope_tests;
#[cfg(test)]
#[path = "graph_builder_tests.rs"]
mod tests;
