//! Building the upstream and the remainders from the classes.

use std::collections::{BTreeMap, BTreeSet};

use daedalus_graph::{EdgeDoc, GraphBody, NodeDoc, NodeKindDoc, PortRefDoc};

use super::classes::{Classes, alias};
use super::{HostAlias, Remainder, SharedNode, Split, UPSTREAM_HOST};
use crate::solve::convert::key_of;
use crate::solve::{Graph, Node};

fn bridge(id: &str) -> NodeDoc {
    let mut doc = NodeDoc::new(id);
    doc.kind = NodeKindDoc::HostBridge;
    doc
}

fn port_ref(node: u32, port: &str) -> PortRefDoc {
    PortRefDoc {
        node,
        port: port.to_string(),
    }
}

fn output_type(node: &Node, port: &str) -> String {
    let output = node.outputs.iter().find(|candidate| candidate.name == port);
    output
        .and_then(|port| key_of(&port.ty))
        .map_or_else(String::new, |key| key.as_str().to_string())
}

struct Cut<'a> {
    graphs: &'a [&'a Graph],
    classes: &'a Classes,
    /// Upstream position of each shared class.
    position: BTreeMap<u32, u32>,
    /// The first instance of each shared class.
    first: BTreeMap<u32, (usize, u32)>,
    /// The upstream's output port for each `(class, port)` read outside it, with its type.
    outputs: BTreeMap<(u32, String), (String, String)>,
}

impl Cut<'_> {
    fn shared_class(&self, graph: usize, node: u32) -> Option<u32> {
        self.classes.of[graph][node as usize].filter(|&class| self.classes.shared[class as usize])
    }

    fn is_bridge(&self, graph: usize, node: u32) -> bool {
        self.graphs[graph].nodes[node as usize].doc.kind == NodeKindDoc::HostBridge
    }

    fn find_outputs(&mut self) {
        for (number, graph) in self.graphs.iter().enumerate() {
            for edge in &graph.edges {
                let Some(class) = self.shared_class(number, edge.from.node) else {
                    continue;
                };
                if self.shared_class(number, edge.to.node).is_some() {
                    continue;
                }
                let position = self.position[&class];
                let source = &graph.nodes[edge.from.node as usize];
                self.outputs
                    .entry((class, edge.from.port.clone()))
                    .or_insert_with(|| {
                        let name = format!("n{position}_{}", edge.from.port);
                        (name, output_type(source, &edge.from.port))
                    });
            }
        }
    }

    fn upstream(&self) -> GraphBody {
        let mut hosts: BTreeMap<String, (u32, NodeDoc)> = BTreeMap::new();
        let mut host_edges = Vec::new();
        let mut body = GraphBody::default();
        for (&class, &(number, node)) in &self.first {
            let graph = self.graphs[number];
            let mut doc = graph.nodes[node as usize].doc.clone();
            doc.shareable = true;
            body.nodes.push(doc);
            let to = self.position[&class];
            for edge in graph.edges.iter().filter(|edge| edge.to.node == node) {
                let target = port_ref(to, &edge.to.port);
                if let Some(parent) = self.shared_class(number, edge.from.node) {
                    let from = port_ref(self.position[&parent], &edge.from.port);
                    body.edges.push(EdgeDoc {
                        from,
                        to: target,
                        policy: edge.policy.clone(),
                    });
                    continue;
                }
                let source = &graph.nodes[edge.from.node as usize];
                let next = hosts.len() as u32;
                let (host, doc) = hosts.entry(alias(source).to_string()).or_insert_with(|| {
                    let mut host = source.doc.clone();
                    host.port_types.clear();
                    host.host_inputs.clear();
                    host.inputs.clear();
                    host.outputs.clear();
                    (next, host)
                });
                copy_port(doc, &source.doc, &edge.from.port);
                host_edges.push((*host, edge.from.port.clone(), target, edge.policy.clone()));
            }
        }
        let shift = body.nodes.len() as u32;
        let mut ordered: Vec<(u32, NodeDoc)> = hosts.into_values().collect();
        ordered.sort_by_key(|(at, _)| *at);
        body.nodes.extend(ordered.into_iter().map(|(_, doc)| doc));
        for (host, port, to, policy) in host_edges {
            body.edges.push(EdgeDoc {
                from: port_ref(host + shift, &port),
                to,
                policy,
            });
        }
        if !self.outputs.is_empty() {
            let out = body.nodes.len() as u32;
            let mut doc = bridge(UPSTREAM_HOST);
            for ((class, port), (name, ty)) in &self.outputs {
                doc.port_types.insert(name.clone(), ty.clone());
                body.edges.push(EdgeDoc {
                    from: port_ref(self.position[class], port),
                    to: port_ref(out, name),
                    policy: Default::default(),
                });
            }
            body.nodes.push(doc);
        }
        body
    }

    fn remainder(&self, number: usize) -> Remainder {
        let graph = self.graphs[number];
        let survives = |edge: &&EdgeDoc| {
            self.shared_class(number, edge.from.node).is_none()
                && self.shared_class(number, edge.to.node).is_none()
        };
        let mut live = BTreeSet::new();
        for edge in graph.edges.iter().filter(survives) {
            live.insert((edge.from.node, edge.from.port.as_str()));
            live.insert((edge.to.node, edge.to.port.as_str()));
        }
        let mut emptied = BTreeSet::new();
        let mut had_edge = BTreeSet::new();
        for edge in &graph.edges {
            for end in [&edge.from, &edge.to] {
                if self.is_bridge(number, end.node) {
                    had_edge.insert(end.node);
                    if !live.contains(&(end.node, end.port.as_str())) {
                        emptied.insert((end.node, end.port.as_str()));
                    }
                }
            }
        }
        let mut nodes = Vec::new();
        let mut remap = vec![None; graph.nodes.len()];
        for (index, node) in graph.nodes.iter().enumerate() {
            let at = index as u32;
            if self.shared_class(number, at).is_some() {
                continue;
            }
            if self.is_bridge(number, at)
                && had_edge.contains(&at)
                && !live.iter().any(|(node, _)| *node == at)
            {
                continue;
            }
            let mut doc = node.doc.clone();
            for &(_, port) in emptied.iter().filter(|(node, _)| *node == at) {
                doc.port_types.remove(port);
                doc.host_inputs.remove(port);
                doc.inputs.retain(|name| name != port);
                doc.outputs.retain(|name| name != port);
            }
            remap[index] = Some(nodes.len() as u32);
            nodes.push(doc);
        }
        let has_nodes = nodes.iter().any(|doc| doc.kind != NodeKindDoc::HostBridge);
        let mut body = GraphBody {
            nodes,
            edges: Vec::new(),
            groups: Vec::new(),
        };
        let mut links = BTreeMap::new();
        let mut aliases = Vec::new();
        let host_at = body.nodes.len() as u32;
        for edge in &graph.edges {
            let (from, to) = (edge.from.node as usize, edge.to.node as usize);
            let policy = edge.policy.clone();
            let target = remap[to].map(|node| port_ref(node, &edge.to.port));
            let Some(class) = self.shared_class(number, edge.from.node) else {
                if let (Some(source), Some(to)) = (remap[from], target) {
                    let from = port_ref(source, &edge.from.port);
                    body.edges.push(EdgeDoc { from, to, policy });
                }
                continue;
            };
            if self.shared_class(number, edge.to.node).is_some() {
                continue;
            }
            let (name, ty) = &self.outputs[&(class, edge.from.port.clone())];
            if self.is_bridge(number, edge.to.node) {
                aliases.push(HostAlias {
                    host: alias(&graph.nodes[to]).to_string(),
                    port: edge.to.port.clone(),
                    upstream_port: name.clone(),
                });
                continue;
            }
            links.insert(name.clone(), ty.clone());
            if let Some(to) = target {
                body.edges.push(EdgeDoc {
                    from: port_ref(host_at, name),
                    to,
                    policy,
                });
            }
        }
        if !links.is_empty() {
            let mut doc = bridge(UPSTREAM_HOST);
            doc.port_types = links.clone();
            body.nodes.push(doc);
        }
        Remainder {
            graph: body,
            has_nodes,
            links: links.into_keys().collect(),
            aliases,
        }
    }
}

fn copy_port(host: &mut NodeDoc, from: &NodeDoc, port: &str) {
    if let Some(ty) = from.port_types.get(port) {
        host.port_types.insert(port.to_string(), ty.clone());
    }
    if let Some(policy) = from.host_inputs.get(port) {
        host.host_inputs.insert(port.to_string(), *policy);
    }
    if from.outputs.iter().any(|name| name == port) && !host.outputs.iter().any(|name| name == port)
    {
        host.outputs.push(port.to_string());
    }
}

pub(super) fn build(graphs: &[&Graph], classes: &Classes) -> Split {
    // Classes are numbered as first met in topological order, so a parent comes before its child.
    let mut replaces: BTreeMap<u32, Vec<(u32, u32)>> = BTreeMap::new();
    for (number, of) in classes.of.iter().enumerate() {
        for (node, class) in of.iter().enumerate() {
            if let Some(class) = class.filter(|&class| classes.shared[class as usize]) {
                replaces
                    .entry(class)
                    .or_default()
                    .push((number as u32, node as u32));
            }
        }
    }
    let position: BTreeMap<u32, u32> = replaces
        .keys()
        .enumerate()
        .map(|(at, &class)| (class, at as u32))
        .collect();
    let first = replaces
        .iter()
        .map(|(&class, all)| (class, (all[0].0 as usize, all[0].1)))
        .collect();
    let mut cut = Cut {
        graphs,
        classes,
        position,
        first,
        outputs: BTreeMap::new(),
    };
    cut.find_outputs();
    let upstream = (!replaces.is_empty()).then(|| cut.upstream());
    let remainders = (0..graphs.len())
        .map(|number| cut.remainder(number))
        .collect();
    let shared = replaces
        .into_iter()
        .map(|(class, replaces)| SharedNode {
            upstream_node: cut.position[&class],
            replaces,
        })
        .collect();
    Split {
        upstream,
        remainders,
        shared,
    }
}
