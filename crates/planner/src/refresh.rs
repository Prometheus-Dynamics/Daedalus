//! Bring a stored graph's node port lists up to date with the registry.
//!
//! Graphs persist each node's port names (and the per-port `inputs.<port>.*` /
//! `outputs.<port>.*` metadata the declaration generated). When a plugin adds a port to a node, a
//! graph stored earlier lacks it and the planner rejects the node as stale (`PortMissing`).
//! [`Graph::refresh_ports`] adds what the registry now declares and drops what it no longer
//! declares, unless an edge or const input still uses it (that stays for the planner to report).
//! [`Graph::refresh_ports_with`] can also drop const inputs on ports the plugin no longer declares
//! ([`RefreshPolicy::drop_stale_consts`]): a stale setting rather than a wiring error.

use alloc::collections::BTreeSet;
use alloc::string::String;
use alloc::vec::Vec;
use daedalus_data::model::Value;
use daedalus_registry::capability::{CapabilityRegistry, NodeDecl};
use daedalus_registry::ids::NodeId;
use serde::{Deserialize, Serialize};

use crate::document::GraphDocument;
use crate::graph::{Graph, NodeInstance};
use crate::metadata::descriptor_dynamic_port_type;

/// How [`Graph::refresh_ports_with`] treats stale state beyond port lists.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RefreshPolicy {
    /// Drop const inputs on input ports the declaration no longer has (and no edge uses), with
    /// the port; the dropped values are listed in [`NodePortRefresh::dropped_consts`]. Off by
    /// default: such a port is then kept and the planner reports it (`PortExtra`).
    pub drop_stale_consts: bool,
}

/// What [`Graph::refresh_ports`] changed on one node.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NodePortRefresh {
    /// The node's index in [`Graph::nodes`].
    pub node: usize,
    pub id: NodeId,
    pub added_inputs: Vec<String>,
    pub added_outputs: Vec<String>,
    pub removed_inputs: Vec<String>,
    pub removed_outputs: Vec<String>,
    /// Undeclared ports kept because an edge or const input uses them; the planner reports them.
    pub kept_inputs: Vec<String>,
    pub kept_outputs: Vec<String>,
    /// Const inputs dropped because their port is no longer declared
    /// ([`RefreshPolicy::drop_stale_consts`]), as (port, value).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub dropped_consts: Vec<(String, Value)>,
}

impl NodePortRefresh {
    fn is_empty(&self) -> bool {
        self.added_inputs.is_empty()
            && self.added_outputs.is_empty()
            && self.removed_inputs.is_empty()
            && self.removed_outputs.is_empty()
            && self.kept_inputs.is_empty()
            && self.kept_outputs.is_empty()
            && self.dropped_consts.is_empty()
    }
}

/// Every node [`Graph::refresh_ports`] touched or found stale, in node order.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PortRefreshReport {
    pub nodes: Vec<NodePortRefresh>,
}

impl PortRefreshReport {
    /// Whether the graph's port lists were already current (nothing added, removed or kept).
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// Whether ports were added or removed or const inputs dropped (the graph changed and may be
    /// re-saved).
    pub fn changed(&self) -> bool {
        self.nodes.iter().any(|node| {
            !(node.added_inputs.is_empty()
                && node.added_outputs.is_empty()
                && node.removed_inputs.is_empty()
                && node.removed_outputs.is_empty()
                && node.dropped_consts.is_empty())
        })
    }
}

impl Graph {
    /// Refresh each node's port lists from its declaration in `registry`:
    ///
    /// - declared ports the node lacks are appended (in declaration order), with the
    ///   declaration's `inputs.<port>.*` / `outputs.<port>.*` metadata the node does not already
    ///   set;
    /// - ports the declaration no longer has are dropped with that metadata, unless an edge or a
    ///   const input uses them: those stay, are listed as kept, and the planner reports them.
    ///
    /// Nodes the registry does not know, nodes that list no ports at all (their ports come from
    /// the registry when planned), dynamic port directions and fan-in ports (`prefix<N>`) are left
    /// as they are. Const inputs, labels, edges and other metadata are untouched. Idempotent: a
    /// second call reports nothing.
    pub fn refresh_ports(&mut self, registry: &CapabilityRegistry) -> PortRefreshReport {
        self.refresh_ports_with(registry, RefreshPolicy::default())
    }

    /// [`Graph::refresh_ports`] under `policy`. With [`RefreshPolicy::drop_stale_consts`], a
    /// const input on an input port the declaration no longer has, and no edge uses, is dropped
    /// (on any known node without dynamic inputs, including nodes that list no ports) and its
    /// port removed; edges on undeclared ports still keep them.
    pub fn refresh_ports_with(
        &mut self,
        registry: &CapabilityRegistry,
        policy: RefreshPolicy,
    ) -> PortRefreshReport {
        let mut report = PortRefreshReport::default();
        for index in 0..self.nodes.len() {
            let Some(decl) = registry.nodes().get(&self.nodes[index].id) else {
                continue;
            };
            let dynamic_inputs = descriptor_dynamic_port_type(decl, true).is_some();
            let declared_inputs: Vec<&str> = decl.inputs.iter().map(|p| p.name.as_str()).collect();
            let mut dropped_consts = Vec::new();
            if policy.drop_stale_consts && !dynamic_inputs {
                let wired = self.wired_ports(index, true);
                self.nodes[index].const_inputs.retain(|(port, value)| {
                    if declares_input(decl, &declared_inputs, port)
                        || wired.contains(&port.trim().to_ascii_lowercase())
                    {
                        return true;
                    }
                    dropped_consts.push((port.clone(), value.clone()));
                    false
                });
            }
            let node = &self.nodes[index];
            let mut change = NodePortRefresh {
                node: index,
                id: node.id.clone(),
                added_inputs: Vec::new(),
                added_outputs: Vec::new(),
                removed_inputs: Vec::new(),
                removed_outputs: Vec::new(),
                kept_inputs: Vec::new(),
                kept_outputs: Vec::new(),
                dropped_consts,
            };
            if node.inputs.is_empty() && node.outputs.is_empty() {
                if !change.is_empty() {
                    report.nodes.push(change);
                }
                continue;
            }
            let used_inputs = self.used_ports(index, true);
            let used_outputs = self.used_ports(index, false);
            let node = &mut self.nodes[index];
            if !dynamic_inputs {
                let (added, removed, kept) = refresh_direction(
                    node,
                    decl,
                    Direction::Inputs,
                    &declared_inputs,
                    &used_inputs,
                );
                change.added_inputs = added;
                change.removed_inputs = removed;
                change.kept_inputs = kept;
            }
            if descriptor_dynamic_port_type(decl, false).is_none() {
                let declared: Vec<&str> = decl.outputs.iter().map(|p| p.name.as_str()).collect();
                let (added, removed, kept) =
                    refresh_direction(node, decl, Direction::Outputs, &declared, &used_outputs);
                change.added_outputs = added;
                change.removed_outputs = removed;
                change.kept_outputs = kept;
            }
            if !change.is_empty() {
                report.nodes.push(change);
            }
        }
        report
    }

    /// Lowercased names of node `index`'s ports an edge uses.
    fn wired_ports(&self, index: usize, inputs: bool) -> BTreeSet<String> {
        self.edges
            .iter()
            .filter_map(|edge| {
                let end = if inputs { &edge.to } else { &edge.from };
                (end.node.0 == index).then(|| end.port.trim().to_ascii_lowercase())
            })
            .collect()
    }

    /// Lowercased names of node `index`'s ports an edge (or, for inputs, a const input) uses.
    fn used_ports(&self, index: usize, inputs: bool) -> BTreeSet<String> {
        let mut used = self.wired_ports(index, inputs);
        if inputs {
            used.extend(
                self.nodes[index]
                    .const_inputs
                    .iter()
                    .map(|(port, _)| port.trim().to_ascii_lowercase()),
            );
        }
        used
    }
}

impl GraphDocument {
    /// [`Graph::refresh_ports`] on the document's graph.
    pub fn refresh_ports(&mut self, registry: &CapabilityRegistry) -> PortRefreshReport {
        self.graph.refresh_ports(registry)
    }
}

#[derive(Clone, Copy)]
enum Direction {
    Inputs,
    Outputs,
}

impl Direction {
    fn metadata_prefix(self, port: &str) -> String {
        match self {
            Direction::Inputs => alloc::format!("inputs.{port}."),
            Direction::Outputs => alloc::format!("outputs.{port}."),
        }
    }
}

/// Refresh one direction of `node`; (added, removed, kept) port names.
fn refresh_direction(
    node: &mut NodeInstance,
    decl: &NodeDecl,
    direction: Direction,
    declared: &[&str],
    used: &BTreeSet<String>,
) -> (Vec<String>, Vec<String>, Vec<String>) {
    let ports = match direction {
        Direction::Inputs => &mut node.inputs,
        Direction::Outputs => &mut node.outputs,
    };
    let is_declared = |port: &str| match direction {
        Direction::Inputs => declares_input(decl, declared, port),
        Direction::Outputs => declared
            .iter()
            .any(|name| name.eq_ignore_ascii_case(port.trim())),
    };

    let (mut removed, mut kept) = (Vec::new(), Vec::new());
    ports.retain(|port| {
        if port.trim().is_empty() || is_declared(port) {
            return true;
        }
        if used.contains(&port.trim().to_ascii_lowercase()) {
            kept.push(port.clone());
            return true;
        }
        removed.push(port.clone());
        false
    });

    let mut added = Vec::new();
    for name in declared {
        if !ports
            .iter()
            .any(|port| port.trim().eq_ignore_ascii_case(name))
        {
            ports.push(String::from(*name));
            added.push(String::from(*name));
        }
    }

    for port in &removed {
        let prefix = direction.metadata_prefix(port);
        node.metadata.retain(|key, _| !key.starts_with(&prefix));
    }
    for port in &added {
        let prefix = direction.metadata_prefix(port);
        for (key, json) in &decl.metadata_json {
            if !key.starts_with(&prefix) || node.metadata.contains_key(key) {
                continue;
            }
            if let Ok(value) = serde_json::from_str::<Value>(json) {
                node.metadata.insert(key.clone(), value);
            }
        }
    }
    (added, removed, kept)
}

/// Whether `decl` declares input `port`: by name (`declared`) or as a fan-in instance.
fn declares_input(decl: &NodeDecl, declared: &[&str], port: &str) -> bool {
    let port = port.trim();
    declared.iter().any(|name| name.eq_ignore_ascii_case(port)) || is_fanin_port(decl, port)
}

/// Whether `port` is an instance of one of `decl`'s fan-in inputs (`prefix<N>`, N >= start).
fn is_fanin_port(decl: &NodeDecl, port: &str) -> bool {
    decl.fanin_inputs.iter().any(|spec| {
        port.strip_prefix(spec.prefix.as_str())
            .and_then(|rest| rest.parse::<u32>().ok())
            .is_some_and(|index| index >= spec.start)
    })
}

#[cfg(test)]
mod tests;
