//! Reading and setting a host bridge's input policy on a graph, before planning.

use std::fmt;

use daedalus_graph::{GraphBody, HostInputPolicy, NodeDoc, NodeKindDoc};

/// Names a host bridge: by alias (its label, or its id when it has none) or by index.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HostNode<'a> {
    Alias(&'a str),
    Node(u32),
}

impl<'a> From<&'a str> for HostNode<'a> {
    fn from(alias: &'a str) -> HostNode<'a> {
        HostNode::Alias(alias)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum HostInputError {
    UnknownHost {
        host: String,
        available: Vec<String>,
    },
    NotAHostBridge {
        node: u32,
    },
    UnknownPort {
        host: String,
        port: String,
        available: Vec<String>,
    },
}

impl fmt::Display for HostInputError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HostInputError::UnknownHost { host, available } => {
                write!(
                    f,
                    "no host bridge `{host}` (host bridges: {})",
                    available.join(", ")
                )
            }
            HostInputError::NotAHostBridge { node } => {
                write!(f, "node {node} is not a host bridge")
            }
            HostInputError::UnknownPort {
                host,
                port,
                available,
            } => {
                write!(
                    f,
                    "host bridge `{host}` has no input `{port}` (inputs: {})",
                    available.join(", ")
                )
            }
        }
    }
}

impl std::error::Error for HostInputError {}

fn alias(node: &NodeDoc) -> &str {
    node.label.as_deref().unwrap_or(&node.id)
}

fn is_bridge(node: &NodeDoc) -> bool {
    node.kind == NodeKindDoc::HostBridge
}

fn find_host(graph: &GraphBody, host: HostNode<'_>) -> Result<usize, HostInputError> {
    match host {
        HostNode::Node(index) => match graph.nodes.get(index as usize) {
            Some(node) if is_bridge(node) => Ok(index as usize),
            _ => Err(HostInputError::NotAHostBridge { node: index }),
        },
        HostNode::Alias(name) => graph
            .nodes
            .iter()
            .position(|node| is_bridge(node) && alias(node) == name)
            .ok_or_else(|| HostInputError::UnknownHost {
                host: name.to_string(),
                available: graph
                    .nodes
                    .iter()
                    .filter(|node| is_bridge(node))
                    .map(|node| alias(node).to_string())
                    .collect(),
            }),
    }
}

/// The values the host pushes into this bridge, each named once.
pub(crate) fn host_input_ports<'a>(
    graph: &'a GraphBody,
    host: impl Into<HostNode<'a>>,
) -> Result<Vec<&'a str>, HostInputError> {
    let index = find_host(graph, host.into())?;
    let node = &graph.nodes[index];
    let edges = graph
        .edges
        .iter()
        .filter(|edge| edge.from.node as usize == index)
        .map(|edge| edge.from.port.as_str());
    let mut ports: Vec<&str> = node
        .outputs
        .iter()
        .map(String::as_str)
        .chain(edges)
        .chain(node.host_inputs.keys().map(String::as_str))
        .chain(node.port_types.keys().map(String::as_str))
        .collect();
    ports.sort_unstable();
    ports.dedup();
    Ok(ports)
}

fn checked_port(graph: &GraphBody, index: usize, port: &str) -> Result<(), HostInputError> {
    let ports = host_input_ports(graph, HostNode::Node(index as u32))?;
    if ports.contains(&port) {
        return Ok(());
    }
    Err(HostInputError::UnknownPort {
        host: alias(&graph.nodes[index]).to_string(),
        port: port.to_string(),
        available: ports.iter().map(|name| name.to_string()).collect(),
    })
}

pub fn host_input_policy<'a>(
    graph: &'a GraphBody,
    host: impl Into<HostNode<'a>>,
    port: &str,
) -> Result<HostInputPolicy, HostInputError> {
    let index = find_host(graph, host.into())?;
    checked_port(graph, index, port)?;
    Ok(graph.nodes[index]
        .host_inputs
        .get(port)
        .copied()
        .unwrap_or_default())
}

/// Sets the policy and returns the old one. Do this before planning: a held input feeding a
/// by-value consumer needs the copy the planner inserts for it.
pub fn set_host_input_policy<'a>(
    graph: &mut GraphBody,
    host: impl Into<HostNode<'a>>,
    port: &str,
    policy: HostInputPolicy,
) -> Result<HostInputPolicy, HostInputError> {
    let index = find_host(graph, host.into())?;
    checked_port(graph, index, port)?;
    let inputs = &mut graph.nodes[index].host_inputs;
    let old = if policy == HostInputPolicy::Consume {
        inputs.remove(port)
    } else {
        inputs.insert(port.to_string(), policy)
    };
    Ok(old.unwrap_or_default())
}
