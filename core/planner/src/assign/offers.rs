//! Asks each backend which nodes it can run.

use std::collections::BTreeMap;

use daedalus_backend::{Backend, BodyIndex, Offer};

use crate::control::CONTROL_BACKEND;
use daedalus_graph::NodeKindDoc;
use daedalus_registry::Registry;
use daedalus_types::NodeIndex;

use super::error::{AssignError, BodyState, BodyStatus};
use crate::solve::Graph;
use crate::solve::type_table::TypeTable;

/// Backends a compile refused for a node, and why. Keyed by (node, backend rank).
pub type Removed = BTreeMap<(u32, u8), String>;

/// What the backends said, per node.
pub(super) struct Offers {
    /// Backend ranks that took the node, lowest first. Forced nodes hold just the forced one.
    pub candidates: Vec<Vec<u8>>,
    pub bodies: Vec<BTreeMap<u8, BodyIndex>>,
}

fn ask(
    backend: &dyn Backend,
    registry: &Registry,
    graph: &Graph,
    nodes: &[(NodeIndex, &str)],
    types: &TypeTable,
) -> Vec<(u32, BodyIndex)> {
    let data = registry.offer_data(backend.id(), nodes, |node, port, declared| {
        let solved = &graph.nodes[node.0 as usize];
        let found = solved
            .inputs
            .iter()
            .chain(&solved.outputs)
            .find(|p| p.name == port);
        types.get(found.map_or(declared, |p| &p.ty))
    });
    let nodes = data.nodes();
    backend
        .can_take(&Offer { nodes: &nodes })
        .into_iter()
        .map(|taken| (taken.node.0, taken.body))
        .collect()
}

fn statuses(
    registry: &Registry,
    id: &str,
    index: u32,
    backends: &[&dyn Backend],
    took: &BTreeMap<u8, BodyIndex>,
    removed: &Removed,
) -> Vec<BodyStatus> {
    let Some(entry) = registry.entry(id) else {
        return Vec::new();
    };
    entry
        .bodies
        .iter()
        .map(|body| {
            let rank = backends.iter().position(|b| b.id() == body.backend);
            let state = match rank.map(|rank| rank as u8) {
                None => BodyState::NotSupplied,
                Some(rank) => match removed.get(&(index, rank)) {
                    Some(reason) => BodyState::Refused(reason.clone()),
                    None if took.contains_key(&rank) => BodyState::Took,
                    None => BodyState::Declined,
                },
            };
            BodyStatus {
                backend: body.backend.to_string(),
                state,
            }
        })
        .collect()
}

pub(super) fn gather(
    graph: &Graph,
    registry: &Registry,
    backends: &[&dyn Backend],
    types: &TypeTable,
    removed: &Removed,
) -> Result<Offers, Vec<AssignError>> {
    let count = graph.nodes.len();
    let mut bodies: Vec<BTreeMap<u8, BodyIndex>> = vec![BTreeMap::new(); count];
    let offered: Vec<(NodeIndex, &str)> = graph
        .nodes
        .iter()
        .enumerate()
        .filter(|(_, node)| {
            !matches!(
                node.doc.kind,
                NodeKindDoc::HostBridge | NodeKindDoc::Control
            )
        })
        .map(|(index, node)| (NodeIndex(index as u32), node.doc.id.as_str()))
        .collect();
    for (rank, backend) in backends.iter().enumerate() {
        for (node, body) in ask(*backend, registry, graph, &offered, types) {
            if (node as usize) < count {
                bodies[node as usize].insert(rank as u8, body);
            }
        }
    }
    let mut errors = Vec::new();
    let mut candidates = vec![Vec::new(); count];
    let control = backends
        .iter()
        .position(|backend| backend.id() == CONTROL_BACKEND);
    for (index, node) in graph.nodes.iter().enumerate() {
        if node.doc.kind == NodeKindDoc::Control {
            match control {
                Some(rank) => candidates[index].push(rank as u8),
                None => errors.push(AssignError::NodeUnplaceable {
                    node: node.doc.id.clone(),
                    bodies: Vec::new(),
                }),
            }
        }
    }
    for &(NodeIndex(index), id) in &offered {
        let at = index as usize;
        let list = || statuses(registry, id, index, backends, &bodies[at], removed);
        let open: Vec<u8> = bodies[at]
            .keys()
            .copied()
            .filter(|&rank| !removed.contains_key(&(index, rank)))
            .collect();
        let forced = graph.nodes[at].doc.backend.as_deref();
        candidates[at] = if let Some(name) = forced {
            let has_body = registry.entry(id).is_some_and(|e| e.body(name).is_some());
            let rank = backends
                .iter()
                .position(|b| b.id() == name)
                .map(|rank| rank as u8);
            match rank.filter(|rank| open.contains(rank)) {
                Some(rank) => vec![rank],
                None if !has_body || rank.is_none() => {
                    errors.push(AssignError::ForcedUnknown {
                        node: id.to_string(),
                        backend: name.to_string(),
                        bodies: list(),
                    });
                    Vec::new()
                }
                None => {
                    errors.push(AssignError::ForcedUnavailable {
                        node: id.to_string(),
                        backend: name.to_string(),
                        bodies: list(),
                    });
                    Vec::new()
                }
            }
        } else {
            if open.is_empty() {
                errors.push(AssignError::NodeUnplaceable {
                    node: id.to_string(),
                    bodies: list(),
                });
            }
            open
        };
    }
    if errors.is_empty() {
        Ok(Offers { candidates, bodies })
    } else {
        Err(errors)
    }
}
