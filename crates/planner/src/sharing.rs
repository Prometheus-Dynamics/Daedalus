//! Common-upstream detection across graphs: structurally identical node subgraphs that several
//! graphs compute from the same host inputs (a camera's mask and pyramid feeding an ArUco and an
//! AprilTag detector) are cut out into one upstream graph, and each graph keeps the rest, fed by
//! the upstream's outputs through new host inputs.
//!
//! Two nodes are the same when they have the same registry id, bundle, ports, compute affinity,
//! constants, sync groups and metadata (labels and the UI node id are ignored), and each input
//! port is wired to the same sources: the same host input (name, declared type, held policy) or
//! an equal node's same output port, with the same edge metadata. Equality is exact (on the
//! canonical serialized form, sources by class id), never a hash. Only nodes `shareable` accepts
//! are considered, and a node is shared only when all its sources are host inputs or shared
//! nodes, so the upstream always forms a prefix of every graph that uses it. Sharing a node is
//! only correct for deterministic, side-effect-free nodes: the caller decides which (the engine
//! requires `NODE_SHAREABLE_META_KEY`).

use alloc::collections::{BTreeMap, BTreeSet};
use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;

use daedalus_data::model::Value;
use serde::Serialize;

use crate::graph::{Edge, Graph, NodeInstance, NodeRef};
use crate::metadata::{
    HostInputPolicy, HostPortTypes, host_bridge_metadata, host_input_policy,
    is_host_bridge_metadata, set_host_input_policy,
};
use daedalus_core::metadata::UI_NODE_ID_KEY;

/// Host-bridge label of the upstream graph [`split_shared_upstream`] builds.
pub const SHARED_UPSTREAM_HOST: &str = "host";

/// The result of [`split_shared_upstream`].
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SharedSplit {
    /// One instance of every node two or more graphs share, with host inputs named as in the
    /// graphs and one host output per shared output port another node or the host reads;
    /// `None` when nothing is shared.
    pub upstream: Option<Graph>,
    /// Per input graph, in order: what it computes besides the upstream.
    pub parts: Vec<SharedPart>,
    /// The shared nodes, in upstream order.
    pub nodes: Vec<SharedNode>,
}

/// One graph after [`split_shared_upstream`] cut the shared nodes out of it.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SharedPart {
    /// The remaining nodes and its host bridge (the original graph when nothing was shared).
    pub graph: Graph,
    /// Whether any node besides the host bridge remains.
    pub has_nodes: bool,
    /// Upstream output `upstream_port` feeds this part's new host input `port`.
    pub links: Vec<SharedPort>,
    /// This graph's host output `port` is upstream output `upstream_port` (no node of the part
    /// produces it any more).
    pub aliases: Vec<SharedPort>,
    /// The original host inputs the part still reads.
    pub inputs: Vec<String>,
}

/// An upstream output and the part port it serves.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SharedPort {
    pub upstream_port: String,
    pub port: String,
}

/// A node class the upstream computes once for several graphs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SharedNode {
    /// Its node in the upstream graph.
    pub upstream_node: NodeRef,
    /// Its upstream label (the first graph's label, made unique).
    pub label: String,
    /// Registry node id.
    pub node_id: String,
    /// Indices of the graphs that contain it.
    pub graphs: Vec<usize>,
}

/// Where an input port of a node gets its values, for class keys.
#[derive(Serialize)]
enum Source<'a> {
    Host {
        port: String,
        held: bool,
        declared: Option<&'a daedalus_data::model::TypeExpr>,
    },
    Node(usize),
}

#[derive(Serialize)]
struct ClassKey<'a> {
    node: &'a NodeInstance,
    incoming: Vec<(&'a str, Source<'a>, &'a str, &'a BTreeMap<String, Value>)>,
}

/// Per graph: its bridge, incoming edges per node and each node's class.
struct Analysis {
    bridge: Option<usize>,
    incoming: Vec<Vec<usize>>,
    class: Vec<Option<usize>>,
}

/// Cut the node subgraphs that two or more of `graphs` compute identically from the same host
/// inputs out into one upstream graph (see the module docs); `shareable` picks the candidate
/// nodes. Graphs without exactly one host bridge take part unchanged.
pub fn split_shared_upstream(
    graphs: &[Graph],
    shareable: impl Fn(&NodeInstance) -> bool,
) -> SharedSplit {
    let mut interner: BTreeMap<Vec<u8>, usize> = BTreeMap::new();
    let analyses: Vec<Analysis> = graphs
        .iter()
        .map(|graph| analyze(graph, &shareable, &mut interner))
        .collect();
    let mut users: Vec<BTreeSet<usize>> = vec![BTreeSet::new(); interner.len()];
    for (index, analysis) in analyses.iter().enumerate() {
        for class in analysis.class.iter().flatten() {
            users[*class].insert(index);
        }
    }
    let shared = |class: Option<usize>| class.filter(|class| users[*class].len() > 1);
    // Upstream nodes in first-appearance order: (graph, node) representatives.
    let mut upstream_of: BTreeMap<usize, usize> = BTreeMap::new();
    let mut representatives: Vec<(usize, usize, usize)> = Vec::new();
    for (index, analysis) in analyses.iter().enumerate() {
        for (node, class) in analysis.class.iter().enumerate() {
            if let Some(class) = shared(*class)
                && !upstream_of.contains_key(&class)
            {
                upstream_of.insert(class, representatives.len() + 1);
                representatives.push((index, node, class));
            }
        }
    }
    if representatives.is_empty() {
        return SharedSplit {
            upstream: None,
            parts: graphs.iter().map(SharedPart::unchanged).collect(),
            nodes: Vec::new(),
        };
    }

    let mut upstream = Graph {
        metadata: graphs[representatives[0].0].metadata.clone(),
        ..Graph::default()
    };
    let mut bridge = NodeInstance {
        metadata: host_bridge_metadata(),
        ..NodeInstance::new("io.host_bridge").with_label(SHARED_UPSTREAM_HOST)
    };
    let mut declared = HostPortTypes::default();
    let mut labels = BTreeSet::new();
    let mut nodes = Vec::with_capacity(representatives.len());
    upstream.nodes.push(NodeInstance::new(""));
    for &(index, node, class) in &representatives {
        let graph = &graphs[index];
        let original = &graph.nodes[node];
        let base = original.label.clone().unwrap_or_else(|| {
            original
                .id
                .0
                .rsplit(':')
                .next()
                .unwrap_or("node")
                .to_string()
        });
        let label = unique(&mut labels, base);
        let at = upstream.nodes.len();
        upstream.nodes.push(NodeInstance {
            label: Some(label.clone()),
            ..original.clone()
        });
        nodes.push(SharedNode {
            upstream_node: NodeRef(at),
            label,
            node_id: original.id.0.clone(),
            graphs: users[class].iter().copied().collect(),
        });
        let host = analyses[index].bridge;
        for &edge in &analyses[index].incoming[node] {
            let edge = &graph.edges[edge];
            let from = if Some(edge.from.node.0) == host {
                let port = edge.from.port.clone();
                let host_meta = &graph.nodes[edge.from.node.0].metadata;
                if !bridge.outputs.contains(&port) {
                    if host_input_policy(host_meta, &port) == HostInputPolicy::Held {
                        set_host_input_policy(&mut bridge.metadata, &port, HostInputPolicy::Held);
                    }
                    let types = HostPortTypes::from_node_metadata(host_meta);
                    if let Some(ty) = types.inputs.get(&port.to_ascii_lowercase()) {
                        declared.declare(true, &port, ty.clone());
                    }
                    bridge.outputs.push(port.clone());
                }
                (0, port)
            } else {
                let class = analyses[index].class[edge.from.node.0].unwrap_or(usize::MAX);
                (upstream_of[&class], edge.from.port.clone())
            };
            upstream.edges.push(Edge {
                metadata: edge.metadata.clone(),
                ..Edge::new(from.0, from.1, at, edge.to.port.clone())
            });
        }
    }

    // Upstream outputs: one per shared (node, port) read outside the upstream.
    let mut outputs: BTreeMap<(usize, String), String> = BTreeMap::new();
    let mut output_names = BTreeSet::new();
    let mut parts = Vec::with_capacity(graphs.len());
    for (index, graph) in graphs.iter().enumerate() {
        let analysis = &analyses[index];
        let is_shared = |node: usize| shared(analysis.class[node]).is_some();
        if !analysis.class.iter().any(|class| shared(*class).is_some()) {
            parts.push(SharedPart::unchanged(graph));
            continue;
        }
        let host = analysis.bridge.unwrap_or(usize::MAX);
        let mut remap = vec![usize::MAX; graph.nodes.len()];
        let mut part = Graph {
            metadata: graph.metadata.clone(),
            ..Graph::default()
        };
        for (node, instance) in graph.nodes.iter().enumerate() {
            if !is_shared(node) {
                remap[node] = part.nodes.len();
                part.nodes.push(instance.clone());
            }
        }
        let (mut links, mut aliases) = (Vec::new(), Vec::new());
        for edge in &graph.edges {
            let (from, to) = (edge.from.node.0, edge.to.node.0);
            match (is_shared(from), is_shared(to)) {
                (false, false) => part.edges.push(Edge {
                    metadata: edge.metadata.clone(),
                    ..Edge::new(
                        remap[from],
                        edge.from.port.clone(),
                        remap[to],
                        edge.to.port.clone(),
                    )
                }),
                (true, false) => {
                    let upstream_node = upstream_of[&analysis.class[from].unwrap_or(usize::MAX)];
                    let name = outputs
                        .entry((upstream_node, edge.from.port.clone()))
                        .or_insert_with(|| {
                            let label = upstream.nodes[upstream_node].label.as_deref();
                            let name = format!("{}.{}", label.unwrap_or("shared"), edge.from.port);
                            let name = unique(&mut output_names, name);
                            bridge.inputs.push(name.clone());
                            upstream.edges.push(Edge::new(
                                upstream_node,
                                edge.from.port.clone(),
                                0,
                                name.clone(),
                            ));
                            name
                        })
                        .clone();
                    if to == host {
                        push_unique(&mut aliases, &name, &edge.to.port);
                    } else {
                        push_unique(&mut links, &name, &name);
                        part.edges.push(Edge {
                            metadata: edge.metadata.clone(),
                            ..Edge::new(remap[host], name, remap[to], edge.to.port.clone())
                        });
                    }
                }
                _ => {}
            }
        }
        let inputs = prune_host_inputs(graph, host, &mut part, remap[host], &links);
        let has_nodes = part.nodes.len() > 1;
        parts.push(SharedPart {
            graph: part,
            has_nodes,
            links,
            aliases,
            inputs,
        });
    }
    declared.write_to_node_metadata(&mut bridge.metadata);
    upstream.nodes[0] = bridge;
    SharedSplit {
        upstream: Some(upstream),
        parts,
        nodes,
    }
}

impl SharedPart {
    fn unchanged(graph: &Graph) -> Self {
        let inputs = graph
            .host_bridges()
            .next()
            .map(|(host, _)| graph.nodes[host.0].outputs.clone())
            .unwrap_or_default();
        Self {
            has_nodes: graph
                .nodes
                .iter()
                .any(|node| !is_host_bridge_metadata(&node.metadata)),
            graph: graph.clone(),
            links: Vec::new(),
            aliases: Vec::new(),
            inputs,
        }
    }
}

/// Drop the part's host inputs only shared nodes read (their declared types too) and add the
/// link inputs; returns the original host inputs the part still reads.
fn prune_host_inputs(
    original: &Graph,
    host: usize,
    part: &mut Graph,
    part_host: usize,
    links: &[SharedPort],
) -> Vec<String> {
    let read = |graph: &Graph, at: usize, port: &str| {
        graph
            .edges
            .iter()
            .any(|edge| edge.from.node.0 == at && edge.from.port == port)
    };
    let Some(bridge) = original.nodes.get(host) else {
        return Vec::new();
    };
    let dropped: Vec<String> = bridge
        .outputs
        .iter()
        .filter(|port| read(original, host, port) && !read(part, part_host, port))
        .cloned()
        .collect();
    let node = &mut part.nodes[part_host];
    node.outputs.retain(|port| !dropped.contains(port));
    let inputs = node.outputs.clone();
    node.outputs
        .extend(links.iter().map(|link| link.port.clone()));
    let mut types = HostPortTypes::from_node_metadata(&node.metadata);
    for port in &dropped {
        types.inputs.remove(&port.to_ascii_lowercase());
    }
    types.write_to_node_metadata(&mut node.metadata);
    inputs
}

fn analyze(
    graph: &Graph,
    shareable: &impl Fn(&NodeInstance) -> bool,
    interner: &mut BTreeMap<Vec<u8>, usize>,
) -> Analysis {
    let mut bridges = graph.host_bridges().map(|(node, _)| node.0);
    let bridge = match (bridges.next(), bridges.next()) {
        (Some(bridge), None) => Some(bridge),
        _ => None,
    };
    let mut incoming = vec![Vec::new(); graph.nodes.len()];
    for (index, edge) in graph.edges.iter().enumerate() {
        if let Some(list) = incoming.get_mut(edge.to.node.0) {
            list.push(index);
        }
    }
    let mut analysis = Analysis {
        bridge,
        incoming,
        class: vec![None; graph.nodes.len()],
    };
    if bridge.is_none() {
        return analysis;
    }
    // Classes in topological order; nodes on cycles or behind unshareable nodes get none.
    let mut done = vec![false; graph.nodes.len()];
    loop {
        let mut progressed = false;
        for node in 0..graph.nodes.len() {
            if done[node] || Some(node) == bridge {
                continue;
            }
            let sources = &analysis.incoming[node];
            let ready = sources.iter().all(|&edge| {
                let from = graph.edges[edge].from.node.0;
                Some(from) == bridge || done.get(from).copied().unwrap_or(true)
            });
            if !ready {
                continue;
            }
            done[node] = true;
            progressed = true;
            analysis.class[node] = class_of(graph, &analysis, node, shareable, interner);
        }
        if !progressed {
            return analysis;
        }
    }
}

fn class_of(
    graph: &Graph,
    analysis: &Analysis,
    node: usize,
    shareable: &impl Fn(&NodeInstance) -> bool,
    interner: &mut BTreeMap<Vec<u8>, usize>,
) -> Option<usize> {
    let instance = &graph.nodes[node];
    if !shareable(instance) {
        return None;
    }
    // Declared host input types (owned per bridge, so resolved after the sources).
    let types = analysis
        .bridge
        .map(|bridge| HostPortTypes::from_node_metadata(&graph.nodes[bridge].metadata))
        .unwrap_or_default();
    let mut incoming = Vec::with_capacity(analysis.incoming[node].len());
    for &edge in &analysis.incoming[node] {
        let edge = &graph.edges[edge];
        let from = edge.from.node.0;
        let source = if Some(from) == analysis.bridge {
            let meta = &graph.nodes[from].metadata;
            let port = edge.from.port.to_ascii_lowercase();
            Source::Host {
                held: host_input_policy(meta, &port) == HostInputPolicy::Held,
                declared: None,
                port,
            }
        } else {
            Source::Node(analysis.class[from]?)
        };
        incoming.push((
            edge.to.port.as_str(),
            source,
            edge.from.port.as_str(),
            &edge.metadata,
        ));
    }
    for (_, source, _, _) in &mut incoming {
        if let Source::Host { port, declared, .. } = source {
            *declared = types.inputs.get(port.as_str());
        }
    }
    incoming.sort_by(|a, b| {
        let key = |entry: &(&str, Source<'_>, &str, &BTreeMap<String, Value>)| {
            serde_json::to_vec(&(entry.0, &entry.1, entry.2, entry.3)).unwrap_or_default()
        };
        key(a).cmp(&key(b))
    });
    let mut canonical = instance.clone();
    canonical.label = None;
    canonical.metadata.remove(UI_NODE_ID_KEY);
    canonical.const_inputs.sort_by(|a, b| a.0.cmp(&b.0));
    let key = serde_json::to_vec(&ClassKey {
        node: &canonical,
        incoming,
    })
    .ok()?;
    let next = interner.len();
    Some(*interner.entry(key).or_insert(next))
}

fn unique(taken: &mut BTreeSet<String>, base: String) -> String {
    let mut name = base.clone();
    let mut suffix = 2;
    while !taken.insert(name.clone()) {
        name = format!("{base}#{suffix}");
        suffix += 1;
    }
    name
}

fn push_unique(list: &mut Vec<SharedPort>, upstream_port: &str, port: &str) {
    if !list
        .iter()
        .any(|entry| entry.upstream_port == upstream_port && entry.port == port)
    {
        list.push(SharedPort {
            upstream_port: upstream_port.to_string(),
            port: port.to_string(),
        });
    }
}
