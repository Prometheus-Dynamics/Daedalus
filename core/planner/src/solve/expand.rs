//! Replaces each group node with its body, until none remain.

use std::collections::BTreeMap;
use std::fmt;

use daedalus_graph::{
    EdgeDoc, EdgePolicyDoc, GraphBody, GroupDef, GroupDoc, Lit, NodeDoc, NodeKindDoc, PortRefDoc,
    StageConstDoc, StagePortDoc,
};
use daedalus_types::Literal;

use super::catalog::Catalog;
use super::{Node, Work};
use crate::pipeline::SolveError;

const MAX_DEPTH: usize = 16;

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ExpandError {
    /// Groups nested past the limit, outermost first.
    GroupDepth {
        chain: Vec<String>,
    },
    GroupMissing {
        id: String,
    },
    /// An edge or a const names a port the group doesn't expose.
    PortMissing {
        group: String,
        node: String,
        port: String,
    },
    /// An edge names a node index outside the graph.
    NodeOutOfRange {
        index: u32,
    },
}

impl fmt::Display for ExpandError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExpandError::GroupDepth { chain } => {
                write!(f, "groups are nested too deeply: {}", chain.join(" > "))
            }
            ExpandError::GroupMissing { id } => write!(f, "no group `{id}` is defined"),
            ExpandError::PortMissing { group, node, port } => {
                write!(f, "group `{group}` (node `{node}`) has no port `{port}`")
            }
            ExpandError::NodeOutOfRange { index } => {
                write!(f, "an edge names node {index}, which isn't in the graph")
            }
        }
    }
}

impl std::error::Error for ExpandError {}

/// A port of an expanded node, in the flat graph.
type Flat = (u32, String);

/// How an original node's ports map to the flat graph.
enum Mapping {
    Single(u32),
    Group {
        id: String,
        label: String,
        inputs: BTreeMap<String, Vec<Flat>>,
        /// `None` where the stage behind the port already failed.
        outputs: BTreeMap<String, Option<Flat>>,
    },
    /// Couldn't be expanded. The error is already reported.
    Failed,
}

struct Expander<'a> {
    catalog: Catalog<'a>,
    groups: &'a [GroupDoc],
    nodes: Vec<Node>,
    edges: Vec<EdgeDoc>,
    errors: Vec<ExpandError>,
}

fn from_def(def: &GroupDef) -> GroupDoc {
    let port = |p: &daedalus_graph::StagePort| StagePortDoc {
        stage: p.stage.to_string(),
        port: p.port.to_string(),
    };
    GroupDoc {
        id: def.id.to_string(),
        stages: def
            .stages
            .iter()
            .map(|(s, id)| (s.to_string(), id.to_string()))
            .collect(),
        connects: def
            .connects
            .iter()
            .map(|(a, b)| (port(a), port(b)))
            .collect(),
        inputs: def
            .inputs
            .iter()
            .map(|(name, ports)| (name.to_string(), ports.iter().map(port).collect()))
            .collect(),
        outputs: def
            .outputs
            .iter()
            .map(|(name, p)| (name.to_string(), port(p)))
            .collect(),
        consts: def
            .consts
            .iter()
            .map(|(at, value)| StageConstDoc {
                at: port(at),
                value: lit_of(value),
            })
            .collect(),
    }
}

/// A declaration's constant in its document form.
pub(crate) fn lit_of(value: &Literal) -> Lit {
    match value {
        Literal::Unit => Lit::Unit,
        Literal::Bool(v) => Lit::Bool(*v),
        Literal::Int(v) => Lit::Int(*v),
        Literal::Float(v) => Lit::Float(*v),
        Literal::Str(v) => Lit::Str(v.to_string()),
        Literal::Bytes(v) => Lit::Bytes(v.to_vec()),
        Literal::List(items) => Lit::List(items.iter().map(lit_of).collect()),
        Literal::Struct(fields) => Lit::Struct(
            fields
                .iter()
                .map(|(name, value)| daedalus_graph::LitField {
                    name: name.to_string(),
                    value: lit_of(value),
                })
                .collect(),
        ),
        Literal::Enum(name, payload) => Lit::Enum {
            name: name.to_string(),
            payload: payload.map(|value| Box::new(lit_of(value))),
        },
    }
}

impl Expander<'_> {
    fn definition(&self, id: &str) -> Option<GroupDoc> {
        if let Some(group) = self.groups.iter().find(|group| group.id == id) {
            return Some(group.clone());
        }
        self.catalog
            .decl(id)
            .and_then(|decl| decl.group)
            .map(from_def)
    }

    fn is_group(&self, node: &NodeDoc) -> bool {
        node.kind == NodeKindDoc::Group
            || self
                .catalog
                .decl(&node.id)
                .is_some_and(|decl| decl.group.is_some())
    }

    /// Expands one group body and returns how its ports map.
    fn group(&mut self, node: &NodeDoc, outer: Option<&str>, chain: &[String]) -> Mapping {
        let mut chain = chain.to_vec();
        chain.push(node.id.clone());
        if chain.len() > MAX_DEPTH {
            self.errors.push(ExpandError::GroupDepth { chain });
            return Mapping::Failed;
        }
        let Some(body) = self.definition(&node.id) else {
            self.errors.push(ExpandError::GroupMissing {
                id: node.id.clone(),
            });
            return Mapping::Failed;
        };
        let label = node.label.clone().unwrap_or_else(|| node.id.clone());
        let outermost = outer.unwrap_or(&node.id).to_string();
        let names: Vec<&String> = body.stages.keys().collect();
        let index_of = |name: &str| names.iter().position(|known| *known == name);
        let mut stages = Vec::new();
        for (name, id) in &body.stages {
            let mut stage = NodeDoc::new(id.clone());
            stage.label = Some(format!("{label}::{name}"));
            for constant in body.consts.iter().filter(|c| c.at.stage == *name) {
                stage
                    .consts
                    .insert(constant.at.port.clone(), constant.value.clone());
            }
            if self.groups.iter().any(|group| group.id == *id) {
                stage.kind = NodeKindDoc::Group;
            }
            stages.push(stage);
        }
        let mappings = self.flatten(&stages, Some(&outermost), &chain);
        let missing = |port: &StagePortDoc| ExpandError::PortMissing {
            group: node.id.clone(),
            node: port.stage.clone(),
            port: port.port.clone(),
        };
        let mut connects = Vec::new();
        for (a, b) in &body.connects {
            match (index_of(&a.stage), index_of(&b.stage)) {
                (Some(from), Some(to)) => {
                    connects.push(EdgeDoc {
                        from: PortRefDoc {
                            node: from as u32,
                            port: a.port.clone(),
                        },
                        to: PortRefDoc {
                            node: to as u32,
                            port: b.port.clone(),
                        },
                        policy: EdgePolicyDoc::default(),
                    });
                }
                (None, _) => self.errors.push(missing(a)),
                (_, None) => self.errors.push(missing(b)),
            }
        }
        self.connect(&connects, &mappings);
        let mut inputs = BTreeMap::new();
        for (name, targets) in &body.inputs {
            let mut flat = Vec::new();
            for target in targets {
                match index_of(&target.stage) {
                    Some(at) => flat.extend(self.input_ports(&mappings[at], &target.port)),
                    None => self.errors.push(missing(target)),
                }
            }
            inputs.insert(name.clone(), flat);
        }
        let mut outputs = BTreeMap::new();
        for (name, source) in &body.outputs {
            match index_of(&source.stage) {
                Some(at) => {
                    let flat = self.output_port(&mappings[at], &source.port);
                    outputs.insert(name.clone(), flat);
                }
                None => {
                    self.errors.push(missing(source));
                    outputs.insert(name.clone(), None);
                }
            }
        }
        Mapping::Group {
            id: node.id.clone(),
            label,
            inputs,
            outputs,
        }
    }

    fn input_ports(&mut self, mapping: &Mapping, port: &str) -> Vec<Flat> {
        match mapping {
            Mapping::Single(index) => vec![(*index, port.to_string())],
            Mapping::Failed => Vec::new(),
            Mapping::Group {
                id, label, inputs, ..
            } => match inputs.get(port) {
                Some(flat) => flat.clone(),
                None => {
                    self.errors.push(ExpandError::PortMissing {
                        group: id.clone(),
                        node: label.clone(),
                        port: port.to_string(),
                    });
                    Vec::new()
                }
            },
        }
    }

    fn output_port(&mut self, mapping: &Mapping, port: &str) -> Option<Flat> {
        match mapping {
            Mapping::Single(index) => Some((*index, port.to_string())),
            Mapping::Failed => None,
            Mapping::Group {
                id, label, outputs, ..
            } => {
                let flat = outputs.get(port).cloned();
                if flat.is_none() {
                    self.errors.push(ExpandError::PortMissing {
                        group: id.clone(),
                        node: label.clone(),
                        port: port.to_string(),
                    });
                }
                flat.flatten()
            }
        }
    }

    /// Writes edges between already-flattened nodes.
    fn connect(&mut self, edges: &[EdgeDoc], mappings: &[Mapping]) {
        for edge in edges {
            let (Some(from), Some(to)) = (
                mappings.get(edge.from.node as usize),
                mappings.get(edge.to.node as usize),
            ) else {
                let index = if (edge.from.node as usize) < mappings.len() {
                    edge.to.node
                } else {
                    edge.from.node
                };
                self.errors.push(ExpandError::NodeOutOfRange { index });
                continue;
            };
            let Some((node, port)) = self.output_port(from, &edge.from.port) else {
                continue;
            };
            for (target, target_port) in self.input_ports(to, &edge.to.port) {
                self.edges.push(EdgeDoc {
                    from: PortRefDoc {
                        node,
                        port: port.clone(),
                    },
                    to: PortRefDoc {
                        node: target,
                        port: target_port,
                    },
                    policy: edge.policy.clone(),
                });
            }
        }
    }

    fn flatten(
        &mut self,
        nodes: &[NodeDoc],
        outer: Option<&str>,
        chain: &[String],
    ) -> Vec<Mapping> {
        let mut mappings = Vec::with_capacity(nodes.len());
        for node in nodes {
            if !self.is_group(node) {
                self.nodes
                    .push(Node::new(node.clone(), outer.map(str::to_string)));
                mappings.push(Mapping::Single(self.nodes.len() as u32 - 1));
                continue;
            }
            let mapping = self.group(node, outer, chain);
            for (port, value) in &node.consts {
                for (target, target_port) in self.input_ports(&mapping, port) {
                    self.nodes[target as usize]
                        .doc
                        .consts
                        .insert(target_port, value.clone());
                }
            }
            mappings.push(mapping);
        }
        mappings
    }
}

pub fn run(work: &mut Work<'_>, graph: &GraphBody) {
    let mut expander = Expander {
        catalog: work.catalog,
        groups: &graph.groups,
        nodes: Vec::new(),
        edges: Vec::new(),
        errors: Vec::new(),
    };
    let mappings = expander.flatten(&graph.nodes, None, &[]);
    expander.connect(&graph.edges, &mappings);
    work.graph.nodes = expander.nodes;
    work.graph.edges = expander.edges;
    work.errors
        .extend(expander.errors.into_iter().map(SolveError::Expand));
}
