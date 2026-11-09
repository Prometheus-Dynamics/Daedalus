//! Why a plan came out the way it did.

pub mod json;

use daedalus_backend::{Compiled, PortRef};
use daedalus_graph::EdgeDoc;
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Explain {
    pub nodes: Vec<NodeExplain>,
    pub edges: Vec<EdgeExplain>,
    pub regions: Vec<RegionExplain>,
    pub units: Vec<UnitExplain>,
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

fn edge_explain(planned: &Planned, first: &EdgeDoc) -> EdgeExplain {
    let graph = &planned.graph;
    let (mut conversion, mut transfer) = (Vec::new(), Vec::new());
    let mut edge = first;
    transfers_of(planned, edge, &mut transfer);
    // Each planner-added node has one input and one output, so its one outgoing edge continues.
    while graph.nodes[edge.to.node as usize].inserted != Inserted::No {
        let at = edge.to.node;
        conversion.push(graph.nodes[at as usize].doc.id.clone());
        let Some(next) = graph.edges.iter().find(|e| e.from.node == at) else {
            break;
        };
        edge = next;
        transfers_of(planned, edge, &mut transfer);
    }
    EdgeExplain {
        from_node: first.from.node,
        from_port: first.from.port.clone(),
        to_node: edge.to.node,
        to_port: edge.to.port.clone(),
        conversion,
        transfer,
    }
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

fn covers(compiled: &Compiled) -> Vec<Vec<u32>> {
    let of = |covers: &[NodeIndex]| covers.iter().map(|n| n.0).collect();
    match compiled {
        Compiled::Steps(units) => units.iter().map(|unit| of(&unit.info.covers)).collect(),
        Compiled::Blob { units, .. } | Compiled::Source { units, .. } => {
            units.iter().map(|unit| of(&unit.covers)).collect()
        }
    }
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
        .filter(|edge| graph.nodes[edge.from.node as usize].inserted == Inserted::No)
        .map(|edge| edge_explain(planned, edge))
        .collect();
    let mut units = Vec::new();
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
    }
}
