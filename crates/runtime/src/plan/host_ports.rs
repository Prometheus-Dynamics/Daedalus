//! Typed introspection of host-bridge ports in a [`RuntimePlan`].
//!
//! Host inputs are the host-bridge node's *output* ports (edges leaving the bridge), host outputs
//! are its *input* ports (edges entering the bridge). Port types are taken from planner-owned
//! data, in order: the bridge node's resolved dynamic port types, the planner edge explanations
//! for the connecting edges, and the runtime edge transports.

use std::collections::BTreeMap;

use daedalus_data::model::TypeExpr;
use daedalus_planner::{DynamicPortMetadata, NodeRef, is_generic_marker, is_host_bridge_metadata};
use daedalus_transport::TypeKey;
use serde::{Deserialize, Serialize};

use super::RuntimePlan;
use super::transports::EdgeExplanations;
use crate::handles::PortId;

/// Direction of a host port, from the host's point of view.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HostPortDirection {
    /// The host pushes payloads into the graph through this port.
    Input,
    /// The graph delivers payloads to the host through this port.
    Output,
}

/// A graph node port wired to a host port.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostPortConnection {
    /// Index of the runtime edge carrying this connection.
    pub edge_index: usize,
    pub node: NodeRef,
    pub node_id: String,
    pub node_label: Option<String>,
    /// Port on the connected graph node.
    pub port: PortId,
    /// Type of the connected graph node port, when known.
    pub type_expr: Option<TypeExpr>,
}

/// Description of one host-bridge port.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostPortDescriptor {
    /// Host bridge alias (node label, or node id when unlabeled).
    pub alias: String,
    pub host_node: NodeRef,
    pub direction: HostPortDirection,
    /// Port name to use with `push`/`feed_payload` (inputs) or `drain`/`try_pop` (outputs).
    pub name: PortId,
    /// Host-side type of the port, when the planner resolved one.
    pub type_expr: Option<TypeExpr>,
    /// Transport key derived from `type_expr`.
    pub type_key: Option<TypeKey>,
    /// Graph node ports wired to this host port, in edge order.
    pub connections: Vec<HostPortConnection>,
}

impl HostPortDescriptor {
    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    pub fn is_input(&self) -> bool {
        self.direction == HostPortDirection::Input
    }
}

fn concrete(ty: Option<TypeExpr>) -> Option<TypeExpr> {
    ty.filter(|ty| !is_generic_marker(ty))
}

impl RuntimePlan {
    /// Aliases of every host-bridge node in the plan, in node order.
    pub fn host_bridge_aliases(&self) -> Vec<String> {
        self.nodes
            .iter()
            .filter(|node| is_host_bridge_metadata(&node.metadata))
            .map(|node| node.host_alias().to_string())
            .collect()
    }

    /// Describe every host port of every host-bridge node.
    ///
    /// Ordering is deterministic: host nodes in plan order, inputs before outputs, then ports by
    /// name.
    pub fn host_ports(&self) -> Vec<HostPortDescriptor> {
        let explanations = EdgeExplanations::from_metadata(&self.graph_metadata);
        let mut out = Vec::new();
        for (host_idx, host) in self.nodes.iter().enumerate() {
            if !is_host_bridge_metadata(&host.metadata) {
                continue;
            }
            let dynamic = DynamicPortMetadata::from_node_metadata(&host.metadata);
            for direction in [HostPortDirection::Input, HostPortDirection::Output] {
                out.extend(self.host_ports_for_node(host_idx, direction, &dynamic, &explanations));
            }
        }
        out
    }

    /// Describe the host ports of the host bridge with `alias` (exact match on label, or id when
    /// the node is unlabeled — the same rule used to create bridge handles).
    pub fn host_ports_for(&self, alias: &str) -> Vec<HostPortDescriptor> {
        self.host_ports()
            .into_iter()
            .filter(|port| port.alias == alias)
            .collect()
    }

    /// Ports the host can push into for the bridge with `alias`.
    pub fn host_inputs(&self, alias: &str) -> Vec<HostPortDescriptor> {
        self.host_ports_for(alias)
            .into_iter()
            .filter(|port| port.direction == HostPortDirection::Input)
            .collect()
    }

    /// Ports the host can drain for the bridge with `alias`.
    pub fn host_outputs(&self, alias: &str) -> Vec<HostPortDescriptor> {
        self.host_ports_for(alias)
            .into_iter()
            .filter(|port| port.direction == HostPortDirection::Output)
            .collect()
    }

    fn host_ports_for_node(
        &self,
        host_idx: usize,
        direction: HostPortDirection,
        dynamic: &DynamicPortMetadata,
        explanations: &EdgeExplanations,
    ) -> Vec<HostPortDescriptor> {
        let host = &self.nodes[host_idx];
        let is_input = direction == HostPortDirection::Input;
        let mut ports: BTreeMap<PortId, HostPortDescriptor> = BTreeMap::new();
        for (edge_index, edge) in self.edges.iter().enumerate() {
            let (host_port, other, other_port) = if is_input {
                if edge.from().0 != host_idx {
                    continue;
                }
                (edge.source_port_id(), edge.to(), edge.target_port_id())
            } else {
                if edge.to().0 != host_idx {
                    continue;
                }
                (edge.target_port_id(), edge.from(), edge.source_port_id())
            };
            let Some(other_node) = self.nodes.get(other.0) else {
                continue;
            };
            let (from, to) = if is_input {
                (host, other_node)
            } else {
                (other_node, host)
            };
            let explained = explanations
                .get(from, to, edge)
                .map(|explained| (&explained.from_type, &explained.to_type));
            let transport = self
                .edge_transports
                .get(edge_index)
                .and_then(Option::as_ref);
            // Host-side and graph-side types for this edge.
            let (host_side, other_side) = if is_input {
                (
                    explained
                        .map(|(from, _)| from.clone())
                        .or_else(|| transport.map(|t| t.from_type.clone())),
                    explained
                        .map(|(_, to)| to.clone())
                        .or_else(|| transport.map(|t| t.to_type.clone())),
                )
            } else {
                (
                    explained
                        .map(|(_, to)| to.clone())
                        .or_else(|| transport.map(|t| t.to_type.clone())),
                    explained
                        .map(|(from, _)| from.clone())
                        .or_else(|| transport.map(|t| t.from_type.clone())),
                )
            };
            let other_side = concrete(other_side);
            let descriptor = ports
                .entry(host_port.clone())
                .or_insert_with(|| HostPortDescriptor {
                    alias: host.host_alias().to_string(),
                    host_node: NodeRef(host_idx),
                    direction,
                    name: host_port.clone(),
                    // Host-bridge dynamic metadata is keyed by the node's own port direction.
                    type_expr: concrete(dynamic.resolved_type(!is_input, host_port.as_str())),
                    type_key: None,
                    connections: Vec::new(),
                });
            if descriptor.type_expr.is_none() {
                descriptor.type_expr = concrete(host_side);
            }
            descriptor.connections.push(HostPortConnection {
                edge_index,
                node: other,
                node_id: other_node.id.clone(),
                node_label: other_node.label.clone(),
                port: other_port.clone(),
                type_expr: other_side,
            });
        }
        ports
            .into_values()
            .map(|mut descriptor| {
                if descriptor.type_expr.is_none() {
                    descriptor.type_expr = unanimous_connection_type(&descriptor.connections);
                }
                descriptor.type_key = descriptor
                    .type_expr
                    .as_ref()
                    .map(daedalus_registry::typeexpr_transport_key);
                descriptor
            })
            .collect()
    }
}

fn unanimous_connection_type(connections: &[HostPortConnection]) -> Option<TypeExpr> {
    let first = connections.first()?.type_expr.clone()?;
    connections
        .iter()
        .all(|connection| connection.type_expr.as_ref() == Some(&first))
        .then_some(first)
}
