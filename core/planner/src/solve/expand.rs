//! Replaces each group node with its body, until none remain.

use std::collections::BTreeMap;
use std::fmt;

use daedalus_graph::{
    EdgeDoc, EdgePolicyDoc, GraphBody, GroupDef, GroupDoc, Lit, NodeDoc, NodeKindDoc, PortRefDoc,
    StageConstDoc, StagePolicyDoc, StagePortDoc,
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
    /// An edge policy of a group names a stage input no connect of the group leads into.
    EdgePolicyUnmatched {
        group: String,
        stage: String,
        port: String,
    },
    /// A group gives one stage input two edge policies.
    EdgePolicyTwice {
        group: String,
        stage: String,
        port: String,
    },
    /// An edge into a group's input carries a policy of its own that differs from the one the
    /// group declares for a stage behind that input.
    EdgePolicyConflict {
        group: String,
        stage: String,
        port: String,
        group_policy: EdgePolicyDoc,
        edge_policy: EdgePolicyDoc,
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
            ExpandError::EdgePolicyUnmatched { group, stage, port } => write!(
                f,
                "group `{group}` gives `{stage}.{port}` an edge policy, but no connect or group \
                 input leads into it"
            ),
            ExpandError::EdgePolicyConflict {
                group,
                stage,
                port,
                group_policy,
                edge_policy,
            } => write!(
                f,
                "group `{group}` gives `{stage}.{port}` the edge policy {group_policy:?}, but the \
                 edge into it carries {edge_policy:?}: drop that edge's own policy, or make it \
                 {group_policy:?}"
            ),
            ExpandError::EdgePolicyTwice { group, stage, port } => {
                write!(
                    f,
                    "group `{group}` gives `{stage}.{port}` two edge policies"
                )
            }
        }
    }
}

impl std::error::Error for ExpandError {}

/// A port of an expanded node, in the flat graph.
type Flat = (u32, String);

/// A group's edge policy for one stage input, and where it was declared.
#[derive(Clone)]
struct Declared {
    policy: EdgePolicyDoc,
    group: String,
    stage: String,
    port: String,
}

/// A port a group input leads to, with the policy the group nearest it declares for its edge.
#[derive(Clone)]
struct Target {
    flat: Flat,
    declared: Option<Declared>,
}

impl Declared {
    /// The policy an edge carrying `incoming` gets into this stage input: the group's. `None`
    /// when the edge's own policy isn't the default and differs from it, which is refused.
    fn over(&self, incoming: &EdgePolicyDoc) -> Option<EdgePolicyDoc> {
        (*incoming == EdgePolicyDoc::Fifo || *incoming == self.policy).then(|| self.policy.clone())
    }

    fn conflict(&self, incoming: &EdgePolicyDoc) -> ExpandError {
        ExpandError::EdgePolicyConflict {
            group: self.group.clone(),
            stage: self.stage.clone(),
            port: self.port.clone(),
            group_policy: self.policy.clone(),
            edge_policy: incoming.clone(),
        }
    }
}

/// How an original node's ports map to the flat graph.
enum Mapping {
    Single(u32),
    Group {
        id: String,
        label: String,
        inputs: BTreeMap<String, Vec<Target>>,
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
        metadata: {
            let mut by_stage: BTreeMap<String, BTreeMap<String, Lit>> = BTreeMap::new();
            for (stage, key, value) in def.metadata {
                by_stage
                    .entry(stage.to_string())
                    .or_default()
                    .insert(key.to_string(), lit_of(value));
            }
            by_stage
        },
        edge_policies: def
            .edge_policies
            .iter()
            .map(|(at, policy)| StagePolicyDoc {
                at: port(at),
                policy: (*policy).into(),
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
            // A stage carries what the group's own node was given, so a setting on a group
            // reaches every node inside it, and then its own, which wins. A stage that is a
            // group passes the result on to its stages the same way.
            stage.metadata = node.metadata.clone();
            if let Some(own) = body.metadata.get(name) {
                stage
                    .metadata
                    .extend(own.iter().map(|(key, value)| (key.clone(), value.clone())));
            }
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
        let policies = self.edge_policies(&node.id, &body);
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
                        policy: policies
                            .get(&(b.stage.as_str(), b.port.as_str()))
                            .cloned()
                            .unwrap_or_default(),
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
                let Some(at) = index_of(&target.stage) else {
                    self.errors.push(missing(target));
                    continue;
                };
                let own = policies
                    .get(&(target.stage.as_str(), target.port.as_str()))
                    .map(|policy| Declared {
                        policy: policy.clone(),
                        group: node.id.clone(),
                        stage: target.stage.clone(),
                        port: target.port.clone(),
                    });
                for mut reached in self.input_ports(&mappings[at], &target.port) {
                    // A nested group's own policy for the stage is nearer, so it wins; this
                    // group's is then an incoming edge's, checked against it.
                    match (&reached.declared, own.clone()) {
                        (Some(inner), Some(own)) => {
                            if inner.over(&own.policy).is_none() {
                                self.errors.push(inner.conflict(&own.policy));
                            }
                        }
                        (None, own) => reached.declared = own,
                        (Some(_), None) => {}
                    }
                    flat.push(reached);
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

    /// The group's edge policies by the stage input they're for. One that no connect or group
    /// input leads into, or a second for the same input, is refused.
    fn edge_policies<'g>(
        &mut self,
        group: &str,
        body: &'g GroupDoc,
    ) -> BTreeMap<(&'g str, &'g str), EdgePolicyDoc> {
        let mut policies = BTreeMap::new();
        for entry in &body.edge_policies {
            let key = (entry.at.stage.as_str(), entry.at.port.as_str());
            let (group, stage, port) = (group.to_string(), key.0.to_string(), key.1.to_string());
            let connected = body.connects.iter().any(|(_, to)| *to == entry.at);
            let input = body.inputs.values().flatten().any(|to| *to == entry.at);
            if !connected && !input {
                self.errors
                    .push(ExpandError::EdgePolicyUnmatched { group, stage, port });
            } else if policies.insert(key, entry.policy.clone()).is_some() {
                self.errors
                    .push(ExpandError::EdgePolicyTwice { group, stage, port });
            }
        }
        policies
    }

    fn input_ports(&mut self, mapping: &Mapping, port: &str) -> Vec<Target> {
        match mapping {
            Mapping::Single(index) => vec![Target {
                flat: (*index, port.to_string()),
                declared: None,
            }],
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
            for target in self.input_ports(to, &edge.to.port) {
                let policy = match &target.declared {
                    None => edge.policy.clone(),
                    Some(declared) => match declared.over(&edge.policy) {
                        Some(policy) => policy,
                        None => {
                            self.errors.push(declared.conflict(&edge.policy));
                            continue;
                        }
                    },
                };
                let (target, target_port) = target.flat;
                self.edges.push(EdgeDoc {
                    from: PortRefDoc {
                        node,
                        port: port.clone(),
                    },
                    to: PortRefDoc {
                        node: target,
                        port: target_port,
                    },
                    policy,
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
                for Target {
                    flat: (target, target_port),
                    ..
                } in self.input_ports(&mapping, port)
                {
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
