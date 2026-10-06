//! Host input policies on a graph or graph document, before planning.
//!
//! The policy lives in the host-bridge node's metadata (`HOST_HELD_INPUTS_KEY`), so it
//! round-trips through [`GraphDocument`] JSON and the planner sees it: a held input feeding a
//! by-value (`move`/`modify`) consumer is planned as a fan-out, which a policy set on a running
//! bridge cannot do.

use alloc::string::{String, ToString};
use alloc::vec::Vec;

use daedalus_data::model::Value;
use thiserror::Error;

use crate::document::GraphDocument;
use crate::graph::{Graph, NodeInstance, NodeRef};
use crate::metadata::{
    HOST_INPUT_TYPES_KEY, HostInputPolicy, host_held_inputs, host_input_policy,
    is_host_bridge_metadata, set_host_input_policy,
};

/// Names a host-bridge node: by alias (its label, or its id when unlabelled) or by index.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HostNode<'a> {
    Alias(&'a str),
    Node(NodeRef),
}

impl<'a> From<&'a str> for HostNode<'a> {
    fn from(alias: &'a str) -> Self {
        Self::Alias(alias)
    }
}

impl<'a> From<&'a String> for HostNode<'a> {
    fn from(alias: &'a String) -> Self {
        Self::Alias(alias)
    }
}

impl From<NodeRef> for HostNode<'_> {
    fn from(node: NodeRef) -> Self {
        Self::Node(node)
    }
}

/// A host input policy could not be read or set.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum HostInputError {
    #[error("no host bridge `{host}` in the graph (host bridges: {available:?})")]
    UnknownHost {
        host: String,
        available: Vec<String>,
    },
    #[error("node {node} is not a host bridge")]
    NotAHostBridge { node: usize },
    #[error("host bridge `{host}` has no input `{port}` (host inputs: {available:?})")]
    UnknownPort {
        host: String,
        port: String,
        available: Vec<String>,
    },
}

fn host_alias(node: &NodeInstance) -> &str {
    node.label.as_deref().unwrap_or(node.id.0.as_str())
}

impl Graph {
    /// The host-bridge nodes of this graph with their aliases.
    pub fn host_bridges(&self) -> impl Iterator<Item = (NodeRef, &str)> {
        self.nodes
            .iter()
            .enumerate()
            .filter(|(_, node)| is_host_bridge_metadata(&node.metadata))
            .map(|(index, node)| (NodeRef(index), host_alias(node)))
    }

    fn host_node<'h>(&self, host: impl Into<HostNode<'h>>) -> Result<usize, HostInputError> {
        match host.into() {
            HostNode::Node(NodeRef(index)) => self
                .nodes
                .get(index)
                .filter(|node| is_host_bridge_metadata(&node.metadata))
                .map(|_| index)
                .ok_or(HostInputError::NotAHostBridge { node: index }),
            HostNode::Alias(alias) => self
                .host_bridges()
                .find(|(_, name)| *name == alias)
                .map(|(node, _)| node.0)
                .ok_or_else(|| HostInputError::UnknownHost {
                    host: alias.to_string(),
                    available: self.host_bridges().map(|(_, a)| a.to_string()).collect(),
                }),
        }
    }

    /// The host inputs (values the host pushes) of a host bridge: its output ports, the ports
    /// its edges leave from, declared input types, and held inputs, each named once.
    pub fn host_input_ports<'h>(
        &self,
        host: impl Into<HostNode<'h>>,
    ) -> Result<Vec<&str>, HostInputError> {
        let index = self.host_node(host)?;
        let node = &self.nodes[index];
        let declared = match node.metadata.get(HOST_INPUT_TYPES_KEY) {
            Some(Value::Map(entries)) => entries.as_slice(),
            _ => &[],
        };
        let mut ports: Vec<&str> = Vec::new();
        let names = node
            .outputs
            .iter()
            .map(String::as_str)
            .chain(
                self.edges
                    .iter()
                    .filter(|edge| edge.from.node.0 == index)
                    .map(|edge| edge.from.port.as_str()),
            )
            .chain(host_held_inputs(&node.metadata))
            .chain(declared.iter().filter_map(|(port, _)| port.as_str()));
        for name in names {
            if !ports.iter().any(|seen| seen.eq_ignore_ascii_case(name)) {
                ports.push(name);
            }
        }
        Ok(ports)
    }

    /// Resolve host input `port` of `host` to its spelling in the graph.
    fn host_input(&self, index: usize, port: &str) -> Result<String, HostInputError> {
        let ports = self.host_input_ports(NodeRef(index))?;
        ports
            .iter()
            .find(|name| **name == port)
            .or_else(|| ports.iter().find(|name| name.eq_ignore_ascii_case(port)))
            .map(|name| name.to_string())
            .ok_or_else(|| HostInputError::UnknownPort {
                host: host_alias(&self.nodes[index]).to_string(),
                port: port.to_string(),
                available: ports.iter().map(|name| name.to_string()).collect(),
            })
    }

    /// The policy of host input `port` on `host` (an alias or a [`NodeRef`]).
    pub fn host_input_policy<'h>(
        &self,
        host: impl Into<HostNode<'h>>,
        port: &str,
    ) -> Result<HostInputPolicy, HostInputError> {
        let index = self.host_node(host)?;
        let port = self.host_input(index, port)?;
        Ok(host_input_policy(&self.nodes[index].metadata, &port))
    }

    /// Set the policy of host input `port` on `host` (an alias or a [`NodeRef`]) and return the
    /// previous one. Set it before planning: a held input feeding a by-value consumer needs the
    /// fan-out the planner inserts for it. Recorded in the bridge's `HOST_HELD_INPUTS_KEY`
    /// metadata, the same place `GraphBuilder::held_input` writes.
    pub fn set_host_input_policy<'h>(
        &mut self,
        host: impl Into<HostNode<'h>>,
        port: &str,
        policy: HostInputPolicy,
    ) -> Result<HostInputPolicy, HostInputError> {
        let index = self.host_node(host)?;
        let port = self.host_input(index, port)?;
        Ok(set_host_input_policy(
            &mut self.nodes[index].metadata,
            &port,
            policy,
        ))
    }
}

impl GraphDocument {
    /// [`Graph::host_input_policy`] on the document's graph.
    pub fn host_input_policy<'h>(
        &self,
        host: impl Into<HostNode<'h>>,
        port: &str,
    ) -> Result<HostInputPolicy, HostInputError> {
        self.graph.host_input_policy(host, port)
    }

    /// [`Graph::set_host_input_policy`] on the document's graph, so a loaded document can mark
    /// inputs held before it is compiled.
    pub fn set_host_input_policy<'h>(
        &mut self,
        host: impl Into<HostNode<'h>>,
        port: &str,
        policy: HostInputPolicy,
    ) -> Result<HostInputPolicy, HostInputError> {
        self.graph.set_host_input_policy(host, port, policy)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::Edge;
    use crate::metadata::{HOST_HELD_INPUTS_KEY, host_bridge_metadata};

    fn graph() -> Graph {
        Graph {
            nodes: alloc::vec![
                NodeInstance {
                    metadata: host_bridge_metadata(),
                    ..NodeInstance::new("io.host_bridge")
                        .with_label("host")
                        .with_outputs(["frame"])
                },
                NodeInstance::new("sink").with_inputs(["imu", "frame"]),
            ],
            edges: alloc::vec![
                Edge::new(0, "frame", 1, "frame"),
                Edge::new(0, "IMU", 1, "imu")
            ],
            ..Graph::default()
        }
    }

    #[test]
    fn policies_round_trip_through_document_json() {
        let mut doc = GraphDocument::new(graph());
        assert_eq!(
            doc.set_host_input_policy("host", "imu", HostInputPolicy::Held),
            Ok(HostInputPolicy::Queued)
        );
        let held = doc.graph.nodes[0].metadata.get(HOST_HELD_INPUTS_KEY);
        assert_eq!(
            held.and_then(|v| v.as_list()).map(<[_]>::len),
            Some(1),
            "stored once under the edge's spelling"
        );
        assert_eq!(
            host_held_inputs(&doc.graph.nodes[0].metadata).collect::<Vec<_>>(),
            ["IMU"]
        );
        let loaded = GraphDocument::from_json(&doc.to_json().unwrap()).unwrap();
        assert_eq!(loaded, doc);
        assert_eq!(
            loaded.host_input_policy(NodeRef(0), "imu"),
            Ok(HostInputPolicy::Held)
        );
        assert_eq!(
            loaded.host_input_policy("host", "frame"),
            Ok(HostInputPolicy::Queued)
        );

        doc.set_host_input_policy("host", "IMU", HostInputPolicy::Queued)
            .unwrap();
        assert!(
            !doc.graph.nodes[0]
                .metadata
                .contains_key(HOST_HELD_INPUTS_KEY)
        );
    }

    #[test]
    fn unknown_hosts_and_ports_are_typed_errors() {
        let mut graph = graph();
        assert!(matches!(
            graph.set_host_input_policy("cam", "imu", HostInputPolicy::Held),
            Err(HostInputError::UnknownHost { available, .. }) if available == ["host"]
        ));
        assert_eq!(
            graph.host_input_policy(NodeRef(1), "imu"),
            Err(HostInputError::NotAHostBridge { node: 1 })
        );
        assert!(matches!(
            graph.set_host_input_policy("host", "gps", HostInputPolicy::Held),
            Err(HostInputError::UnknownPort { port, available, .. })
                if port == "gps" && available == ["frame", "IMU"]
        ));
    }
}
