use daedalus_data::model::Value;
use daedalus_planner::{ComputeAffinity, Edge, Graph, NodeInstance, NodeRef, PortRef};
use daedalus_registry::{ids::NodeId, store::Registry};
use crate::handles::{NodeHandleLike, PortHandle};
use crate::host_bridge::HOST_BRIDGE_META_KEY;
use std::collections::{BTreeMap, HashMap};

use crate::host_bridge::HOST_BRIDGE_ID;

/// Convenience wrapper so callers can pre-prefix ids (e.g. via a plugin helper)
/// and pass them into `GraphBuilder::node_spec`.
#[derive(Clone, Debug)]
pub struct NodeSpec {
    pub id: String,
}

impl NodeSpec {
    pub fn new(id: impl Into<String>) -> Self {
        Self { id: id.into() }
    }

    pub fn prefixed(prefix: &str, id: &str) -> Self {
        Self {
            id: format!("{prefix}:{id}"),
        }
    }
}

impl From<(String, String)> for NodeSpec {
    fn from(value: (String, String)) -> Self {
        NodeSpec { id: value.0 }
    }
}

impl<'a> From<(&'a str, &'a str)> for NodeSpec {
    fn from(value: (&'a str, &'a str)) -> Self {
        NodeSpec {
            id: value.0.to_string(),
        }
    }
}

/// Internal representation of a port reference, from either strings or handles.
#[derive(Clone, Debug)]
pub struct PortSpec {
    pub node: String,
    pub port: String,
}

pub trait IntoPortSpec {
    fn into_spec(self) -> PortSpec;
}

impl IntoPortSpec for &str {
    fn into_spec(self) -> PortSpec {
        let mut parts = self.split(':');
        PortSpec {
            node: parts.next().unwrap_or("").to_string(),
            port: parts.next().unwrap_or("").to_string(),
        }
    }
}

impl IntoPortSpec for (&str, &str) {
    fn into_spec(self) -> PortSpec {
        PortSpec {
            node: self.0.to_string(),
            port: self.1.to_string(),
        }
    }
}

impl IntoPortSpec for (String, String) {
    fn into_spec(self) -> PortSpec {
        PortSpec {
            node: self.0,
            port: self.1,
        }
    }
}

impl IntoPortSpec for &PortHandle {
    fn into_spec(self) -> PortSpec {
        PortSpec {
            node: self.node_alias.clone(),
            port: self.port.clone(),
        }
    }
}

fn is_host_bridge(node: &NodeInstance) -> bool {
    matches!(
        node.metadata.get(HOST_BRIDGE_META_KEY),
        Some(Value::Bool(true))
    )
}

/// A graph paired with the host-bridge alias used to expose its inputs/outputs.
#[derive(Clone, Debug)]
pub struct NestedGraph {
    graph: Graph,
    host_alias: String,
    host_index: usize,
}

impl NestedGraph {
    /// Build a nested graph using the provided host-bridge alias.
    pub fn new(graph: Graph, host_alias: impl Into<String>) -> Result<Self, &'static str> {
        let host_alias = host_alias.into();
        let host_index = graph
            .nodes
            .iter()
            .position(|n| is_host_bridge(n) && n.label.as_deref() == Some(host_alias.as_str()))
            .ok_or("host bridge alias not found in nested graph")?;

        Ok(Self {
            graph,
            host_alias,
            host_index,
        })
    }

    /// Build a nested graph using the first host bridge found (by metadata flag).
    pub fn first_host(graph: Graph) -> Result<Self, &'static str> {
        let (host_index, host_alias) = graph
            .nodes
            .iter()
            .enumerate()
            .find_map(|(idx, n)| {
                is_host_bridge(n).then(|| (idx, n.label.clone().unwrap_or_else(|| n.id.0.clone())))
            })
            .ok_or("nested graph missing host bridge")?;

        Ok(Self {
            graph,
            host_alias,
            host_index,
        })
    }

    pub fn host_alias(&self) -> &str {
        &self.host_alias
    }

    pub fn graph(&self) -> &Graph {
        &self.graph
    }
}

/// Interface for a nested graph once it has been inlined into another graph.
#[derive(Clone, Debug)]
pub struct NestedGraphHandle {
    pub alias: String,
    pub inputs: BTreeMap<String, Vec<PortRef>>, // host -> inner targets
    pub outputs: BTreeMap<String, Vec<PortRef>>, // inner sources -> host
}

impl NestedGraphHandle {
    /// Port handle for a nested graph input (outer -> nested).
    pub fn input(&self, port: impl Into<String>) -> PortHandle {
        PortHandle::new(self.alias.clone(), port)
    }

    /// Port handle for a nested graph output (nested -> outer).
    pub fn output(&self, port: impl Into<String>) -> PortHandle {
        PortHandle::new(self.alias.clone(), port)
    }

    pub fn input_ports(&self) -> impl Iterator<Item = &str> {
        self.inputs.keys().map(|k| k.as_str())
    }

    pub fn output_ports(&self) -> impl Iterator<Item = &str> {
        self.outputs.keys().map(|k| k.as_str())
    }
}

/// Graph builder with alias and basic port validation using the registry.
pub struct GraphBuilder<'r> {
    reg: &'r Registry,
    nodes: Vec<NodeInstance>,
    edges: Vec<Edge>,
    const_overrides: HashMap<String, HashMap<String, Option<Value>>>,
    node_metadata_overrides: HashMap<String, BTreeMap<String, Value>>,
    graph_metadata: BTreeMap<String, String>,
    graph_metadata_values: BTreeMap<String, Value>,
    injected_node_metadata: BTreeMap<String, Value>,
    injected_node_metadata_overwrite: BTreeMap<String, Value>,
    host_bridge_alias: Option<String>,
    host_bridge_added: bool,
    nested: HashMap<String, NestedGraphHandle>,
}

impl<'r> GraphBuilder<'r> {
    pub fn new(registry: &'r Registry) -> Self {
        Self {
            reg: registry,
            nodes: Vec::new(),
            edges: Vec::new(),
            const_overrides: HashMap::new(),
            node_metadata_overrides: HashMap::new(),
            graph_metadata: BTreeMap::new(),
            graph_metadata_values: BTreeMap::new(),
            injected_node_metadata: BTreeMap::new(),
            injected_node_metadata_overwrite: BTreeMap::new(),
            host_bridge_alias: Some("host".to_string()),
            host_bridge_added: false,
            nested: HashMap::new(),
        }
    }

    /// Attach string graph-level metadata to the built graph (`Graph.metadata`).
    ///
    /// For typed graph metadata visible to nodes at runtime, prefer `graph_metadata_value`.
    pub fn graph_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        let key = key.into();
        let value = value.into();
        self.graph_metadata.insert(key.clone(), value.clone());
        self.graph_metadata_values
            .insert(key, Value::String(value.into()));
        self
    }

    /// Attach graph-level metadata (typed `Value`) to the built graph (`Graph.metadata_values`).
    ///
    /// Nodes can read this via `ExecutionContext.graph_metadata` at runtime.
    pub fn graph_metadata_value(mut self, key: impl Into<String>, value: Value) -> Self {
        self.graph_metadata_values.insert(key.into(), value);
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

    /// Add a node from an id/version pair (e.g., produced by a plugin helper).
    pub fn node_pair<T>(self, pair: T, alias: &str) -> Self
    where
        T: Into<NodeSpec>,
    {
        let spec: NodeSpec = pair.into();
        self.node_spec(spec, alias)
    }

    /// Add a node using any handle that exposes id/alias.
    pub fn node_handle_like(self, handle: &dyn NodeHandleLike) -> Self {
        self.node_id(handle.id(), handle.alias())
    }

    /// Add a node using a typed handle (preferred).
    pub fn node<H>(self, handle: H) -> Self
    where
        H: NodeHandleLike,
    {
        self.node_id(handle.id(), handle.alias())
    }

    /// Add a node by id.
    pub fn node_from_id(self, id: &str, alias: &str) -> Self {
        self.node_id(id, alias)
    }

    /// Add a node using its descriptor default compute affinity (or override via `node_with_compute`).
    pub fn node_id(mut self, id: &str, alias: &str) -> Self {
        let view = self.reg.view();
        let desc = view.nodes.get(&NodeId::new(id));
        let ports = desc
            .map(|desc| {
                (
                    desc.inputs.iter().map(|p| p.name.clone()).collect(),
                    desc.outputs.iter().map(|p| p.name.clone()).collect(),
                )
            })
            .unwrap_or_default();

        // Base consts from registry descriptor, if any.
        let mut const_inputs = Vec::new();
        let mut compute = ComputeAffinity::CpuOnly;
        let mut metadata: BTreeMap<String, Value> = BTreeMap::new();
        let mut sync_groups = Vec::new();
        if let Some(desc) = desc {
            compute = desc.default_compute;
            metadata = desc.metadata.clone();
            sync_groups = desc.sync_groups.clone();
            for port in &desc.inputs {
                if let Some(v) = &port.const_value {
                    const_inputs.push((port.name.clone(), v.clone()));
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
        let alias = port.node_alias.clone();
        let port_name = port.port.clone();
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
                    "dynamic_inputs".to_string(),
                    Value::String(std::borrow::Cow::from("generic")),
                ),
                (
                    "dynamic_outputs".to_string(),
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
    pub fn nest(
        mut self,
        nested: &NestedGraph,
        alias: impl Into<String>,
    ) -> (Self, NestedGraphHandle) {
        let alias = alias.into();
        if self.nested.contains_key(&alias)
            || self
                .nodes
                .iter()
                .any(|n| n.label.as_deref() == Some(alias.as_str()))
        {
            panic!("nested alias '{}' already in use", alias);
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
        (self, handle)
    }

    pub fn connect<F, T>(self, from: F, to: T) -> Self
    where
        F: IntoPortSpec,
        T: IntoPortSpec,
    {
        self.connect_ports(from, to)
    }

    /// Connect two ports and attach metadata to the edge.
    pub fn connect_with_metadata<F, T, K>(
        mut self,
        from: F,
        to: T,
        metadata: impl IntoIterator<Item = (K, Value)>,
    ) -> Self
    where
        F: IntoPortSpec,
        T: IntoPortSpec,
        K: Into<String>,
    {
        let edge_idx = self.edges.len();
        self = self.connect_ports(from, to);
        let meta: Vec<(String, Value)> = metadata.into_iter().map(|(k, v)| (k.into(), v)).collect();
        for e in self.edges.iter_mut().skip(edge_idx) {
            for (k, v) in &meta {
                e.metadata.insert(k.clone(), v.clone());
            }
        }
        self
    }

    /// Attach/override metadata for an existing connection edge.
    pub fn edge_metadata<F, T>(
        mut self,
        from: F,
        to: T,
        key: impl Into<String>,
        value: Value,
    ) -> Self
    where
        F: IntoPortSpec,
        T: IntoPortSpec,
    {
        let from_spec = from.into_spec();
        let to_spec = to.into_spec();
        let f_idx = self.find_index(&from_spec.node);
        let t_idx = self.find_index(&to_spec.node);
        let key = key.into();
        for edge in &mut self.edges {
            if edge.from.node.0 == f_idx
                && edge.to.node.0 == t_idx
                && edge.from.port == from_spec.port
                && edge.to.port == to_spec.port
            {
                edge.metadata.insert(key.clone(), value.clone());
            }
        }
        self
    }

    /// Connect using PortHandle pairs for string-free wiring.
    pub fn connect_handles(mut self, from: &PortHandle, to: &PortHandle) -> Self {
        self = self.connect_ports(from, to);
        self
    }

    /// Connect by explicit id/port tuples (old-style, but structured).
    pub fn connect_by_id(
        mut self,
        from: (impl Into<String>, impl Into<String>),
        to: (impl Into<String>, impl Into<String>),
    ) -> Self {
        self = self.connect_ports((from.0.into(), from.1.into()), (to.0.into(), to.1.into()));
        self
    }

    /// No-op helpers to mirror the desired API shape for host I/O handles.
    pub fn inputs(self, _ports: &[PortHandle]) -> Self {
        self
    }

    pub fn outputs(self, _ports: &[PortHandle]) -> Self {
        self
    }

    /// Connect using explicit tuple arguments instead of a colon string, e.g.
    /// `.connect_ports(("src", "out"), ("dst", "inp"))`.
    pub fn connect_ports<F, T>(mut self, from: F, to: T) -> Self
    where
        F: IntoPortSpec,
        T: IntoPortSpec,
    {
        let from_spec = from.into_spec();
        let to_spec = to.into_spec();
        let host_alias = self
            .host_bridge_alias
            .clone()
            .unwrap_or_else(|| "host".to_string());
        if from_spec.node == host_alias || to_spec.node == host_alias {
            self = self.ensure_host_bridge(Some(host_alias.clone()));
        }
        if from_spec.node == host_alias {
            self = self.ensure_host_bridge_port(true, &from_spec.port);
        }
        if to_spec.node == host_alias {
            self = self.ensure_host_bridge_port(false, &to_spec.port);
        }
        let from_nested = self.nested.get(&from_spec.node).cloned();
        let to_nested = self.nested.get(&to_spec.node).cloned();

        if from_nested.is_some() && to_nested.is_some() {
            panic!(
                "cannot connect nested graph '{}' directly to nested graph '{}'",
                from_spec.node, to_spec.node
            );
        }
        if let Some(nested) = from_nested {
            return self.connect_from_nested_spec(&nested, &from_spec.port, to_spec);
        }
        if let Some(nested) = to_nested {
            return self.connect_to_nested_spec(from_spec, &nested, &to_spec.port);
        }

        let f_idx = self.find_index(&from_spec.node);
        let t_idx = self.find_index(&to_spec.node);
        self.edges.push(Edge {
            from: PortRef {
                node: NodeRef(f_idx),
                port: from_spec.port,
            },
            to: PortRef {
                node: NodeRef(t_idx),
                port: to_spec.port,
            },
            metadata: BTreeMap::new(),
        });
        self
    }

    /// Connect an outer node/port to a nested graph input port.
    pub fn connect_to_nested<F>(
        mut self,
        from: F,
        nested: &NestedGraphHandle,
        port: impl AsRef<str>,
    ) -> Self
    where
        F: IntoPortSpec,
    {
        let from_spec = from.into_spec();
        let host_alias = self
            .host_bridge_alias
            .clone()
            .unwrap_or_else(|| "host".to_string());
        if from_spec.node == host_alias {
            self = self.ensure_host_bridge(Some(host_alias));
            self = self.ensure_host_bridge_port(true, &from_spec.port);
        }
        let lookup = |name: &str, nodes: &[NodeInstance]| {
            nodes
                .iter()
                .position(|n| n.id.0 == name || n.label.as_deref() == Some(name))
                .unwrap_or_else(|| panic!("node alias '{}' not found", name))
        };
        let f_idx = lookup(&from_spec.node, &self.nodes);
        let port = port.as_ref();
        let targets = nested
            .inputs
            .get(port)
            .unwrap_or_else(|| panic!("nested input '{}' not found", port));

        for target in targets {
            self.edges.push(Edge {
                from: PortRef {
                    node: NodeRef(f_idx),
                    port: from_spec.port.clone(),
                },
                to: target.clone(),
                metadata: BTreeMap::new(),
            });
        }
        self
    }

    /// Connect a nested graph output port to a node/port in the outer graph.
    pub fn connect_from_nested<T>(
        mut self,
        nested: &NestedGraphHandle,
        port: impl AsRef<str>,
        to: T,
    ) -> Self
    where
        T: IntoPortSpec,
    {
        let to_spec = to.into_spec();
        let host_alias = self
            .host_bridge_alias
            .clone()
            .unwrap_or_else(|| "host".to_string());
        if to_spec.node == host_alias {
            self = self.ensure_host_bridge(Some(host_alias));
            self = self.ensure_host_bridge_port(false, &to_spec.port);
        }
        let lookup = |name: &str, nodes: &[NodeInstance]| {
            nodes
                .iter()
                .position(|n| n.id.0 == name || n.label.as_deref() == Some(name))
                .unwrap_or_else(|| panic!("node alias '{}' not found", name))
        };
        let t_idx = lookup(&to_spec.node, &self.nodes);
        let port = port.as_ref();
        let sources = nested
            .outputs
            .get(port)
            .unwrap_or_else(|| panic!("nested output '{}' not found", port));

        for source in sources {
            self.edges.push(Edge {
                from: source.clone(),
                to: PortRef {
                    node: NodeRef(t_idx),
                    port: to_spec.port.clone(),
                },
                metadata: BTreeMap::new(),
            });
        }
        self
    }

    fn connect_from_nested_spec(
        mut self,
        nested: &NestedGraphHandle,
        port: &str,
        to: PortSpec,
    ) -> Self {
        let t_idx = self.find_index(&to.node);
        let sources = nested
            .outputs
            .get(port)
            .unwrap_or_else(|| panic!("nested output '{}' not found", port));

        for source in sources {
            self.edges.push(Edge {
                from: source.clone(),
                to: PortRef {
                    node: NodeRef(t_idx),
                    port: to.port.clone(),
                },
                metadata: BTreeMap::new(),
            });
        }
        self
    }

    fn connect_to_nested_spec(
        mut self,
        from: PortSpec,
        nested: &NestedGraphHandle,
        port: &str,
    ) -> Self {
        let f_idx = self.find_index(&from.node);
        let targets = nested
            .inputs
            .get(port)
            .unwrap_or_else(|| panic!("nested input '{}' not found", port));

        for target in targets {
            self.edges.push(Edge {
                from: PortRef {
                    node: NodeRef(f_idx),
                    port: from.port.clone(),
                },
                to: target.clone(),
                metadata: BTreeMap::new(),
            });
        }
        self
    }

    fn find_index(&self, name: &str) -> usize {
        self.nodes
            .iter()
            .position(|n| n.id.0 == name || n.label.as_deref() == Some(name))
            .unwrap_or_else(|| panic!("node alias '{}' not found", name))
    }

    pub fn build(self) -> Graph {
        let mut nodes = self.nodes;
        if !self.injected_node_metadata.is_empty()
            || !self.injected_node_metadata_overwrite.is_empty()
        {
            for node in &mut nodes {
                for (k, v) in &self.injected_node_metadata {
                    node.metadata.entry(k.clone()).or_insert_with(|| v.clone());
                }
                for (k, v) in &self.injected_node_metadata_overwrite {
                    node.metadata.insert(k.clone(), v.clone());
                }
            }
        }
        Graph {
            nodes,
            edges: self.edges,
            metadata: self.graph_metadata,
            metadata_values: self.graph_metadata_values,
        }
    }
}

/// Graph definition context for graph-backed nodes.
pub struct GraphCtx<'r> {
    builder: GraphBuilder<'r>,
    host_alias: String,
    expected_inputs: Vec<String>,
    expected_outputs: Vec<String>,
}

impl<'r> GraphCtx<'r> {
    fn take_builder(&mut self) -> GraphBuilder<'r> {
        let reg = self.builder.reg;
        std::mem::replace(&mut self.builder, GraphBuilder::new(reg))
    }

    pub fn new(registry: &'r Registry, inputs: &[&str], outputs: &[&str]) -> Self {
        Self {
            builder: GraphBuilder::new(registry),
            host_alias: "host".to_string(),
            expected_inputs: inputs.iter().map(|v| v.to_string()).collect(),
            expected_outputs: outputs.iter().map(|v| v.to_string()).collect(),
        }
    }

    pub fn node(&mut self, id: &str) -> crate::handles::NodeHandle {
        self.node_as(id, id)
    }

    pub fn node_as(&mut self, id: &str, alias: &str) -> crate::handles::NodeHandle {
        let builder = self.take_builder();
        self.builder = builder.node_id(id, alias);
        crate::handles::NodeHandle {
            id: id.to_string(),
            alias: alias.to_string(),
        }
    }

    pub fn connect(&mut self, from: &PortHandle, to: &PortHandle) {
        let builder = self.take_builder();
        self.builder = builder.connect_handles(from, to);
    }

    pub fn const_input(&mut self, port: &PortHandle, value: Value) {
        let builder = self.take_builder();
        self.builder = builder.const_input(port, Some(value));
    }

    pub fn input(&self, name: &str) -> PortHandle {
        PortHandle::new(self.host_alias.clone(), name)
    }

    pub fn output(&self, name: &str) -> PortHandle {
        PortHandle::new(self.host_alias.clone(), name)
    }

    pub fn bind_output(&mut self, name: &str, from: &PortHandle) {
        let host = self.output(name);
        let builder = self.take_builder();
        self.builder = builder.connect_handles(from, &host);
    }

    pub fn build(mut self) -> Graph {
        self.builder = self.builder.host_bridge(self.host_alias.clone());
        for name in &self.expected_inputs {
            self.builder = self.builder.ensure_host_bridge_port(false, name);
        }
        for name in &self.expected_outputs {
            self.builder = self.builder.ensure_host_bridge_port(true, name);
        }
        self.builder.build()
    }
}

pub fn graph_to_json(graph: &Graph) -> Result<String, serde_json::Error> {
    serde_json::to_string(graph)
}

#[cfg(test)]
mod tests {
    use super::*;
    use daedalus_data::model::{TypeExpr, Value, ValueType};
    use daedalus_registry::store::{NodeDescriptorBuilder, Registry};

    #[test]
    fn applies_metadata_overrides() {
        let mut reg = Registry::new();
        let desc = NodeDescriptorBuilder::new("demo.node")
            .metadata("from_desc", Value::Bool(true))
            .build()
            .unwrap();
        reg.register_node(desc).unwrap();

        let graph = GraphBuilder::new(&reg)
            .node_from_id("demo.node", "alias")
            .node_metadata_by_id("alias", "pos_x", Value::Int(10))
            .build();

        let meta = &graph.nodes[0].metadata;
        assert_eq!(meta.get("from_desc"), Some(&Value::Bool(true)));
        assert_eq!(meta.get("pos_x"), Some(&Value::Int(10)));
    }

    #[test]
    fn can_inject_graph_metadata_and_broadcast_to_nodes() {
        let mut reg = Registry::new();
        let desc = NodeDescriptorBuilder::new("demo.node")
            .metadata("existing", Value::String("keep".into()))
            .build()
            .unwrap();
        reg.register_node(desc).unwrap();

        let graph = GraphBuilder::new(&reg)
            .graph_metadata("graph_run_id", "run-123")
            .graph_metadata_value("multiplier", Value::Int(3))
            .inject_node_metadata("trace_id", Value::String("trace-abc".into()))
            .inject_node_metadata_overwrite("existing", Value::String("overwrite".into()))
            .node_from_id("demo.node", "alias")
            .build();

        assert_eq!(graph.metadata.get("graph_run_id"), Some(&"run-123".into()));
        assert_eq!(
            graph.metadata_values.get("graph_run_id"),
            Some(&Value::String("run-123".into()))
        );
        assert_eq!(
            graph.metadata_values.get("multiplier"),
            Some(&Value::Int(3))
        );
        let meta = &graph.nodes[0].metadata;
        assert_eq!(
            meta.get("trace_id"),
            Some(&Value::String("trace-abc".into()))
        );
        assert_eq!(
            meta.get("existing"),
            Some(&Value::String("overwrite".into()))
        );
    }

    #[test]
    fn nests_graph_and_exposes_ports() {
        let reg = Registry::new();

        let inner = GraphBuilder::new(&reg)
            .host_bridge("inner")
            .node_from_id("demo.add", "add")
            .connect_by_id(("inner", "lhs"), ("add", "lhs"))
            .connect_by_id(("inner", "rhs"), ("add", "rhs"))
            .connect_by_id(("add", "sum"), ("inner", "sum"))
            .build();
        let nested = NestedGraph::new(inner, "inner").expect("inner host bridge missing");

        let (builder, nested_handle) = GraphBuilder::new(&reg)
            .node_from_id("demo.src", "src")
            .nest(&nested, "adder");

        let graph = builder
            .node_from_id("demo.sink", "sink")
            .connect(("src", "out_lhs"), &nested_handle.input("lhs"))
            .connect(("src", "out_rhs"), &nested_handle.input("rhs"))
            .connect(&nested_handle.output("sum"), ("sink", "in"))
            .build();

        assert!(nested_handle.inputs.contains_key("lhs"));
        assert!(nested_handle.inputs.contains_key("rhs"));
        assert!(nested_handle.outputs.contains_key("sum"));

        let find = |name: &str| {
            graph
                .nodes
                .iter()
                .position(|n| n.label.as_deref() == Some(name))
                .unwrap()
        };
        let src_idx = find("src");
        let sink_idx = find("sink");
        let add_idx = find("adder::add");

        let has_inbound = graph
            .edges
            .iter()
            .any(|e| e.from.node.0 == src_idx && e.to.node.0 == add_idx && e.to.port == "lhs");
        let has_outbound = graph
            .edges
            .iter()
            .any(|e| e.from.node.0 == add_idx && e.to.node.0 == sink_idx && e.from.port == "sum");

        assert!(has_inbound, "nested inputs should target inner nodes");
        assert!(has_outbound, "nested outputs should feed outer nodes");
    }

    #[test]
    fn applies_edge_metadata() {
        let mut reg = Registry::new();
        reg.register_node(
            NodeDescriptorBuilder::new("demo.src")
                .output("out", TypeExpr::Scalar(ValueType::Bool))
                .build()
                .unwrap(),
        )
        .unwrap();
        reg.register_node(
            NodeDescriptorBuilder::new("demo.sink")
                .input("in", TypeExpr::Scalar(ValueType::Bool))
                .build()
                .unwrap(),
        )
        .unwrap();

        let graph = GraphBuilder::new(&reg)
            .node_from_id("demo.src", "a")
            .node_from_id("demo.sink", "b")
            .connect_with_metadata(
                ("a", "out"),
                ("b", "in"),
                [("ui.color", Value::String("red".into()))],
            )
            .build();
        assert_eq!(graph.edges.len(), 1);
        assert!(matches!(
            graph.edges[0].metadata.get("ui.color"),
            Some(Value::String(s)) if s.as_ref() == "red"
        ));
    }
}
