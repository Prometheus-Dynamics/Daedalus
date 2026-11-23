//! Why a plan came out the way it did.

pub mod json;

use daedalus_backend::{Compiled, PortRef};
use daedalus_graph::{EdgeDoc, EdgePolicyDoc};
use daedalus_types::{NodeIndex, PortIndex};
use serde::{Deserialize, Serialize};

use crate::api::Planned;
use crate::assign::{Placed, Reason};
use crate::solve::{Graph, Inserted};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PlaceReason {
    HostBridge,
    Forced,
    OnlyCandidate,
    Chosen { crossings: u32, rank: u32 },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeExplain {
    pub node: u32,
    pub id: String,
    /// `None` for a host bridge.
    pub backend: Option<String>,
    pub reason: PlaceReason,
}

/// An edge between two nodes the graph asked for. Conversions the planner added show up in
/// `conversion` rather than as edges of their own.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EdgeExplain {
    pub from_node: u32,
    pub from_port: String,
    pub to_node: u32,
    pub to_port: String,
    pub conversion: Vec<String>,
    pub transfer: Vec<String>,
    /// The memory space the producer writes the value in. `None` when the planner gave the edge
    /// no value of its own, as inside a region a backend merged into one step.
    pub written_in: Option<String>,
    /// The memory space the consumer reads it from.
    pub read_in: Option<String>,
    /// The consumer reads the value where the producer wrote it: nothing moves it and nothing
    /// copies it. With an empty `transfer`, this is what tells a read in place from an edge the
    /// planner knows nothing about.
    pub in_place: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PortExplain {
    pub node: u32,
    pub port: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegionExplain {
    pub region: u32,
    pub backend: String,
    pub nodes: Vec<u32>,
    /// Consumption, then production, each as numerator and denominator.
    pub rate: [[u64; 2]; 2],
    pub inputs: Vec<PortExplain>,
    pub outputs: Vec<PortExplain>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnitExplain {
    pub region: u32,
    pub unit: u32,
    pub covers: Vec<u32>,
}

/// An input a step converts as it reads it: from the form the value arrived in to the type its
/// node reads. Listed like a transfer, so the cost shows.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecodeExplain {
    pub node: u32,
    pub port: String,
    /// The type the node reads.
    pub ty: String,
    pub reason: String,
}

/// An edge that queues more than one value into a node that waits for its other inputs. While
/// the node waits, the edge fills: each queued value is held, and a value that holds something
/// scarce (a camera frame from a small pool) pins it until it is read.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueuedExplain {
    pub node: u32,
    pub port: String,
    /// How many values the edge holds. `None` for a FIFO, whose depth the host sets when it
    /// loads the plan (64 unless told otherwise).
    pub depth: Option<u32>,
    /// The node's other connected inputs, which it waits for.
    pub waits_on: Vec<String>,
    pub note: String,
}

/// A node the planner added: a conversion between two types, or a copy for a reader that takes
/// its value. It is added once for an output however many readers need it, and they share what
/// it makes, so its cost is paid once a value and not once a reader.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AddedExplain {
    pub node: u32,
    pub id: String,
    /// The output it reads.
    pub from_node: u32,
    pub from_port: String,
    /// Every reader its value reaches.
    pub readers: Vec<PortExplain>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Explain {
    pub nodes: Vec<NodeExplain>,
    pub edges: Vec<EdgeExplain>,
    pub regions: Vec<RegionExplain>,
    pub units: Vec<UnitExplain>,
    /// Left out when there are none, as in a record written before steps listed them.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub decodes: Vec<DecodeExplain>,
    /// Left out when there are none.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub queued: Vec<QueuedExplain>,
    /// The nodes the planner added, each once with its readers. Left out when there are none.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub added: Vec<AddedExplain>,
}

fn reason(reason: &Reason) -> PlaceReason {
    match *reason {
        Reason::HostBridge => PlaceReason::HostBridge,
        Reason::Forced => PlaceReason::Forced,
        Reason::OnlyCandidate => PlaceReason::OnlyCandidate,
        Reason::Chosen { crossings, rank } => PlaceReason::Chosen { crossings, rank },
    }
}

fn input_ref(graph: &Graph, edge: &EdgeDoc) -> Option<PortRef> {
    let node = graph.nodes.get(edge.to.node as usize)?;
    let port = node
        .inputs
        .iter()
        .position(|port| port.name == edge.to.port)?;
    Some(PortRef {
        node: NodeIndex(edge.to.node),
        port: PortIndex(u16::try_from(port).ok()?),
    })
}

/// Where the value on `edge` is written and where its consumer reads it, as space names.
fn spaces_of(planned: &Planned, edge: &EdgeDoc) -> Option<(String, String)> {
    let routing = &planned.regions.routing;
    let reader = input_ref(&planned.graph, edge)
        .and_then(|at| routing.readers.get(&at))
        // A host bridge's input is what the host takes.
        .or_else(|| {
            routing
                .host_outputs
                .iter()
                .find(|out| out.node == edge.to.node && out.port == edge.to.port)
                .map(|out| &out.reader)
        })?;
    let written = routing.values.get(reader.value as usize)?.home;
    Some((
        routing.spaces.get(written)?.clone(),
        routing.spaces.get(reader.space)?.clone(),
    ))
}

fn transfers_of(planned: &Planned, edge: &EdgeDoc, out: &mut Vec<String>) {
    let routing = &planned.regions.routing;
    let Some(reader) = input_ref(&planned.graph, edge).and_then(|at| routing.readers.get(&at))
    else {
        return;
    };
    out.extend(
        reader
            .path
            .iter()
            .map(|&step| routing.table[step].id.clone()),
    );
}

/// The edge into a planner-added node. It has one input, so there is one.
fn edge_into(graph: &Graph, node: u32) -> Option<&EdgeDoc> {
    graph.edges.iter().find(|edge| edge.to.node == node)
}

/// The edge the graph asked for that ends at `last`'s reader: back from the reader through the
/// nodes the planner added, to the output the graph named. Several readers may share an added
/// node, so the walk goes from the reader, which has one way back.
fn edge_explain(planned: &Planned, last: &EdgeDoc) -> EdgeExplain {
    let graph = &planned.graph;
    let (mut conversion, mut transfer) = (Vec::new(), Vec::new());
    let mut first = last;
    let mut hops = vec![last];
    while graph.nodes[first.from.node as usize].inserted != Inserted::No {
        let at = first.from.node;
        conversion.push(graph.nodes[at as usize].doc.id.clone());
        let Some(before) = edge_into(graph, at) else {
            break;
        };
        first = before;
        hops.push(before);
    }
    conversion.reverse();
    for hop in hops.iter().rev() {
        transfers_of(planned, hop, &mut transfer);
    }
    // Where the first edge's value is written, and where the last edge's consumer reads.
    let written_in = spaces_of(planned, first).map(|(written, _)| written);
    let read_in = spaces_of(planned, last).map(|(_, read)| read);
    let in_place = transfer.is_empty() && written_in.is_some() && written_in == read_in;
    EdgeExplain {
        from_node: first.from.node,
        from_port: first.from.port.clone(),
        to_node: last.to.node,
        to_port: last.to.port.clone(),
        conversion,
        transfer,
        written_in,
        read_in,
        in_place,
    }
}

/// Each node the planner added, once, with the output it reads and every reader its value
/// reaches, through any nodes added after it.
fn added(graph: &Graph) -> Vec<AddedExplain> {
    let mut out = Vec::new();
    for (at, node) in graph.nodes.iter().enumerate() {
        if node.inserted == Inserted::No {
            continue;
        }
        let Some(source) = edge_into(graph, at as u32) else {
            continue;
        };
        // The readers downstream: follow every edge out, through added nodes.
        let (mut readers, mut open) = (Vec::new(), vec![at as u32]);
        while let Some(from) = open.pop() {
            for edge in graph.edges.iter().filter(|edge| edge.from.node == from) {
                if graph.nodes[edge.to.node as usize].inserted == Inserted::No {
                    readers.push(PortExplain {
                        node: edge.to.node,
                        port: edge.to.port.clone(),
                    });
                } else {
                    open.push(edge.to.node);
                }
            }
        }
        readers.sort_by(|a, b| (a.node, &a.port).cmp(&(b.node, &b.port)));
        out.push(AddedExplain {
            node: at as u32,
            id: node.doc.id.clone(),
            from_node: source.from.node,
            from_port: source.from.port.clone(),
            readers,
        });
    }
    out
}

fn port_name(graph: &Graph, port: &PortRef, inputs: bool) -> PortExplain {
    let node = port.node.0;
    let on = &graph.nodes[node as usize];
    let list = if inputs { &on.inputs } else { &on.outputs };
    PortExplain {
        node,
        port: list[usize::from(port.port.0)].name.clone(),
    }
}

fn decodes(graph: &Graph, compiled: &Compiled, out: &mut Vec<DecodeExplain>) {
    let Compiled::Steps(units) = compiled else {
        return;
    };
    for decode in units.iter().flat_map(|unit| &unit.info.decodes) {
        out.push(DecodeExplain {
            node: decode.input.node.0,
            port: port_name(graph, &decode.input, true).port,
            ty: decode.ty.to_string(),
            reason: decode.reason.to_string(),
        });
    }
}

fn covers(compiled: &Compiled) -> Vec<Vec<u32>> {
    let of = |covers: &[NodeIndex]| covers.iter().map(|n| n.0).collect();
    match compiled {
        Compiled::Steps(units) => units.iter().map(|unit| of(&unit.info.covers)).collect(),
        Compiled::Blob { units, .. } | Compiled::Source { units, .. } => {
            units.iter().map(|unit| of(&unit.covers)).collect()
        }
    }
}

/// The edges that queue more than one value into a node that fires only when every connected
/// input has a new value, and has another one to wait for.
fn queued(graph: &Graph) -> Vec<QueuedExplain> {
    let mut out = Vec::new();
    for edge in &graph.edges {
        let depth = match &edge.policy {
            EdgePolicyDoc::Fifo => None,
            EdgePolicyDoc::Bounded { capacity, .. } if *capacity > 1 => Some(*capacity),
            _ => continue,
        };
        let node = &graph.nodes[edge.to.node as usize];
        if node.fire != daedalus_registry::Fire::All
            || node.doc.kind == daedalus_graph::NodeKindDoc::HostBridge
        {
            continue;
        }
        let mut waits_on: Vec<String> = graph
            .edges
            .iter()
            .filter(|other| other.to.node == edge.to.node && other.to.port != edge.to.port)
            .map(|other| other.to.port.clone())
            .collect();
        waits_on.sort();
        waits_on.dedup();
        if waits_on.is_empty() {
            continue;
        }
        let held = match depth {
            Some(depth) => format!("up to {depth} values"),
            None => "up to the FIFO depth (64 by default)".to_string(),
        };
        out.push(QueuedExplain {
            node: edge.to.node,
            port: edge.to.port.clone(),
            depth,
            note: format!(
                "`{}` runs only when {} also has a new value, and until then this edge holds \
                 {held}; if the value holds something scarce, give the edge latest_only or \
                 per_tick",
                node.doc.id,
                waits_on.join(", ")
            ),
            waits_on,
        });
    }
    out
}

/// The record of how a finished plan was reached.
pub fn explain_plan(planned: &Planned) -> Explain {
    let (graph, regions) = (&planned.graph, &planned.regions);
    let name =
        |index: daedalus_types::BackendIndex| planned.backend_ids[usize::from(index.0)].clone();
    let nodes = graph
        .nodes
        .iter()
        .enumerate()
        .map(|(index, node)| NodeExplain {
            node: index as u32,
            id: node.doc.id.clone(),
            backend: match regions.assignment.placed[index] {
                Placed::Host => None,
                Placed::Backend(backend) => Some(name(backend)),
            },
            reason: reason(&regions.assignment.reasons[index]),
        })
        .collect();
    let edges = graph
        .edges
        .iter()
        .filter(|edge| graph.nodes[edge.to.node as usize].inserted == Inserted::No)
        .map(|edge| edge_explain(planned, edge))
        .collect();
    let mut units = Vec::new();
    let mut converted = Vec::new();
    let mut described = Vec::new();
    for (at, region) in regions.regions.iter().enumerate() {
        let formed = &region.formed;
        let (consume, produce) = regions.rates[formed.rate.0 as usize];
        described.push(RegionExplain {
            region: at as u32,
            backend: name(formed.backend),
            nodes: formed.nodes.clone(),
            rate: [[consume.num, consume.den], [produce.num, produce.den]],
            inputs: formed
                .inputs
                .iter()
                .map(|p| port_name(graph, p, true))
                .collect(),
            outputs: formed
                .outputs
                .iter()
                .map(|p| port_name(graph, p, false))
                .collect(),
        });
        decodes(graph, &region.compiled, &mut converted);
        for (unit, covers) in covers(&region.compiled).into_iter().enumerate() {
            units.push(UnitExplain {
                region: at as u32,
                unit: unit as u32,
                covers,
            });
        }
    }
    Explain {
        nodes,
        edges,
        regions: described,
        units,
        decodes: converted,
        queued: queued(graph),
        added: added(graph),
    }
}
