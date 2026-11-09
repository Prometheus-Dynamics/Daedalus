//! Works out each node's ports and checks that the graph only names ports that exist.

use std::collections::BTreeSet;
use std::fmt;

use daedalus_graph::{NodeDoc, NodeKindDoc};
use daedalus_registry::{Entry, FanInDecl, PortDecl, TypeRef};
use daedalus_value::Access;

use super::dynamic::instance_type;
use super::port_types::generic_var;
use super::{Graph, Node, Port, Work};
use crate::pipeline::SolveError;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Direction {
    Input,
    Output,
}

impl fmt::Display for Direction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Direction::Input => "input",
            Direction::Output => "output",
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum PortError {
    PortMissing {
        node: String,
        direction: Direction,
        port: String,
    },
    PortExtra {
        node: String,
        direction: Direction,
        port: String,
    },
    PortDuplicate {
        node: String,
        direction: Direction,
        port: String,
    },
    /// A port's type in the graph isn't a type key.
    TypeInvalid {
        node: String,
        port: String,
        text: String,
    },
    /// An edge names a node index outside the graph.
    NodeOutOfRange { index: u32 },
}

impl fmt::Display for PortError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PortError::PortMissing {
                node,
                direction,
                port,
            } => {
                write!(f, "node `{node}` has no {direction} port `{port}`")
            }
            PortError::PortExtra {
                node,
                direction,
                port,
            } => {
                write!(
                    f,
                    "node `{node}` lists {direction} port `{port}`, which isn't declared"
                )
            }
            PortError::PortDuplicate {
                node,
                direction,
                port,
            } => {
                write!(f, "node `{node}` lists {direction} port `{port}` twice")
            }
            PortError::TypeInvalid { node, port, text } => {
                write!(f, "node `{node}` port `{port}`: `{text}` is not a type key")
            }
            PortError::NodeOutOfRange { index } => {
                write!(f, "an edge names node {index}, which isn't in the graph")
            }
        }
    }
}

impl std::error::Error for PortError {}

fn matches_fanin(fanin: &FanInDecl, port: &str) -> bool {
    port.strip_prefix(fanin.prefix)
        .and_then(|digits| digits.parse::<u32>().ok())
        .is_some_and(|number| number >= fanin.start)
}

fn solved(entry: &Entry, port: &PortDecl, doc: &NodeDoc, errors: &mut Vec<PortError>) -> Port {
    let mut ty = entry.port_type(port);
    if generic_var(&ty).is_some() {
        match instance_type(doc, port.name) {
            Ok(Some(instance)) => ty = instance,
            Ok(None) => {}
            Err(text) => errors.push(PortError::TypeInvalid {
                node: doc.id.clone(),
                port: port.name.to_string(),
                text,
            }),
        }
    }
    Port {
        name: port.name.to_string(),
        ty,
        access: port.access,
        optional: port.optional,
        default: port.default,
    }
}

fn check_lists(
    doc: &NodeDoc,
    direction: Direction,
    listed: &[String],
    declared: &[Port],
    fanin: &[FanInDecl],
    strict: bool,
    errors: &mut Vec<PortError>,
) {
    let mut seen = BTreeSet::new();
    for name in listed {
        let error = if !seen.insert(name.as_str()) {
            PortError::PortDuplicate {
                node: doc.id.clone(),
                direction,
                port: name.clone(),
            }
        } else if declared.iter().any(|port| port.name == *name)
            || (direction == Direction::Input && fanin.iter().any(|f| matches_fanin(f, name)))
        {
            continue;
        } else {
            PortError::PortExtra {
                node: doc.id.clone(),
                direction,
                port: name.clone(),
            }
        };
        errors.push(error);
    }
    if strict {
        for port in declared
            .iter()
            .filter(|port| !seen.contains(port.name.as_str()))
        {
            errors.push(PortError::PortMissing {
                node: doc.id.clone(),
                direction,
                port: port.name.clone(),
            });
        }
    }
}

fn bridge_port(name: &str, text: &str, doc: &NodeDoc, errors: &mut Vec<PortError>) -> Option<Port> {
    match instance_type(doc, name) {
        Ok(Some(ty)) => Some(Port {
            name: name.to_string(),
            ty,
            access: Access::default(),
            optional: false,
            default: None,
        }),
        _ => {
            errors.push(PortError::TypeInvalid {
                node: doc.id.clone(),
                port: name.to_string(),
                text: text.to_string(),
            });
            None
        }
    }
}

/// A host bridge's ports are what the graph says they are, on whichever side edges use them.
fn fill_bridge(graph: &mut Graph, index: usize, errors: &mut Vec<PortError>) {
    let doc = graph.nodes[index].doc.clone();
    let mut inputs = Vec::new();
    let mut outputs = Vec::new();
    for (name, text) in &doc.port_types {
        let Some(port) = bridge_port(name, text, &doc, errors) else {
            continue;
        };
        let at = index as u32;
        let is_input = graph
            .edges
            .iter()
            .any(|e| e.to.node == at && e.to.port == *name);
        let is_output = graph
            .edges
            .iter()
            .any(|e| e.from.node == at && e.from.port == *name);
        if is_input {
            inputs.push(port.clone());
        }
        if is_output || !is_input {
            outputs.push(port);
        }
    }
    let node = &mut graph.nodes[index];
    node.inputs = inputs;
    node.outputs = outputs;
}

fn fill_node(node: &mut Node, entry: &Entry, strict: bool, errors: &mut Vec<PortError>) {
    let doc = node.doc.clone();
    node.inputs = entry
        .decl
        .inputs
        .iter()
        .map(|p| solved(entry, p, &doc, errors))
        .collect();
    node.outputs = entry
        .decl
        .outputs
        .iter()
        .map(|p| solved(entry, p, &doc, errors))
        .collect();
    for name in doc.port_types.keys() {
        let known = node
            .inputs
            .iter()
            .chain(&node.outputs)
            .any(|port| port.name == *name);
        if !known && !entry.decl.fanin.iter().any(|f| matches_fanin(f, name)) {
            errors.push(PortError::PortExtra {
                node: doc.id.clone(),
                direction: Direction::Input,
                port: name.clone(),
            });
        }
    }
    let fanin = entry.decl.fanin;
    check_lists(
        &doc,
        Direction::Input,
        &doc.inputs,
        &node.inputs,
        fanin,
        strict,
        errors,
    );
    check_lists(
        &doc,
        Direction::Output,
        &doc.outputs,
        &node.outputs,
        &[],
        strict,
        errors,
    );
}

/// Adds the fan-in port an edge names, once.
fn add_fanin(node: &mut Node, entry: &Entry, port: &str) -> bool {
    let Some(fanin) = entry.decl.fanin.iter().find(|f| matches_fanin(f, port)) else {
        return false;
    };
    if !node.inputs.iter().any(|known| known.name == port) {
        node.inputs.push(Port {
            name: port.to_string(),
            ty: fanin.ty.clone(),
            access: Access::default(),
            optional: false,
            default: None,
        });
    }
    true
}

pub fn run(work: &mut Work<'_>) {
    let mut errors = Vec::new();
    let strict = work.config.strict_ports;
    for index in 0..work.graph.nodes.len() {
        if work.graph.nodes[index].doc.kind == NodeKindDoc::HostBridge {
            fill_bridge(&mut work.graph, index, &mut errors);
        } else if let Some(entry) = work.catalog.entry(&work.graph.nodes[index].doc.id) {
            fill_node(&mut work.graph.nodes[index], entry, strict, &mut errors);
        }
    }
    let edges = std::mem::take(&mut work.graph.edges);
    for edge in &edges {
        let count = work.graph.nodes.len() as u32;
        if edge.from.node >= count || edge.to.node >= count {
            errors.push(PortError::NodeOutOfRange {
                index: edge.from.node.max(edge.to.node),
            });
            continue;
        }
        check_end(
            work,
            edge.from.node,
            &edge.from.port,
            Direction::Output,
            &mut errors,
        );
        check_end(
            work,
            edge.to.node,
            &edge.to.port,
            Direction::Input,
            &mut errors,
        );
    }
    work.graph.edges = edges;
    work.errors
        .extend(errors.into_iter().map(SolveError::Ports));
}

fn check_end(
    work: &mut Work<'_>,
    index: u32,
    port: &str,
    direction: Direction,
    errors: &mut Vec<PortError>,
) {
    let catalog = work.catalog;
    let node = &mut work.graph.nodes[index as usize];
    let entry = catalog.entry(&node.doc.id);
    if node.doc.kind == NodeKindDoc::Node && entry.is_none() {
        return; // Hydrate already reported it.
    }
    let ports = match direction {
        Direction::Input => &node.inputs,
        Direction::Output => &node.outputs,
    };
    if ports.iter().any(|known| known.name == port) {
        return;
    }
    if direction == Direction::Input && entry.is_some_and(|entry| add_fanin(node, entry, port)) {
        return;
    }
    errors.push(PortError::PortMissing {
        node: node.doc.id.clone(),
        direction,
        port: port.to_string(),
    });
}

/// The type of a port, once the port stage has run.
pub fn type_of<'a>(
    graph: &'a Graph,
    index: u32,
    port: &str,
    direction: Direction,
) -> Option<&'a TypeRef> {
    let node = graph.nodes.get(index as usize)?;
    let ports = match direction {
        Direction::Input => &node.inputs,
        Direction::Output => &node.outputs,
    };
    ports
        .iter()
        .find(|known| known.name == port)
        .map(|known| &known.ty)
}
