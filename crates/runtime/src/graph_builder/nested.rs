use crate::handles::PortHandle;
use crate::host_bridge::HOST_BRIDGE_META_KEY;
use daedalus_data::model::Value;
use daedalus_planner::{Graph, NodeInstance, PortRef};
use std::collections::BTreeMap;

use super::GraphBuildError;

pub(super) fn is_host_bridge(node: &NodeInstance) -> bool {
    matches!(
        node.metadata.get(HOST_BRIDGE_META_KEY),
        Some(Value::Bool(true))
    )
}

/// A graph paired with the host-bridge alias used to expose its inputs/outputs.
#[derive(Clone, Debug)]
pub struct NestedGraph {
    pub(super) graph: Graph,
    pub(super) host_alias: String,
    pub(super) host_index: usize,
}

impl NestedGraph {
    /// Build a nested graph using the provided host-bridge alias.
    pub fn new(graph: Graph, host_alias: impl Into<String>) -> Result<Self, GraphBuildError> {
        let host_alias = host_alias.into();
        let host_index = graph
            .nodes
            .iter()
            .position(|n| is_host_bridge(n) && n.label.as_deref() == Some(host_alias.as_str()))
            .ok_or_else(|| GraphBuildError::MissingHostBridgeAlias {
                alias: host_alias.clone(),
            })?;

        Ok(Self {
            graph,
            host_alias,
            host_index,
        })
    }

    /// Build a nested graph using the first host bridge found (by metadata flag).
    pub fn first_host(graph: Graph) -> Result<Self, GraphBuildError> {
        let (host_index, host_alias) = graph
            .nodes
            .iter()
            .enumerate()
            .find_map(|(idx, n)| {
                is_host_bridge(n).then(|| (idx, n.label.clone().unwrap_or_else(|| n.id.0.clone())))
            })
            .ok_or(GraphBuildError::MissingHostBridge)?;

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
