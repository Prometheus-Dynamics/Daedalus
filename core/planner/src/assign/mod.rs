//! Decides which backend runs each node: fewest crossings, then the Host's priority.

mod components;
pub mod error;
mod offers;
mod search;

use daedalus_backend::{Backend, BodyIndex};
use daedalus_graph::NodeKindDoc;
use daedalus_registry::Registry;
use daedalus_types::BackendIndex;

use crate::solve::Graph;
use crate::solve::type_table::TypeTable;
use components::{components, wiring};
use error::AssignError;
pub use offers::Removed;
use search::{OverBudget, State, search};

/// Search steps allowed per coupled component.
pub const ASSIGN_BUDGET: u64 = 1_000_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Placed {
    /// A host bridge.
    Host,
    Backend(BackendIndex),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reason {
    HostBridge,
    Forced,
    OnlyCandidate,
    /// Picked by the search. `crossings` is its coupled component's.
    Chosen {
        crossings: u32,
        rank: u32,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Assignment {
    pub placed: Vec<Placed>,
    pub reasons: Vec<Reason>,
    /// The chosen body. `None` for a host bridge.
    pub bodies: Vec<Option<BodyIndex>>,
    pub crossings: u32,
    pub expansions: u64,
}

/// One assignment problem.
pub struct Input<'a> {
    pub graph: &'a Graph,
    pub registry: &'a Registry,
    /// Highest priority first.
    pub backends: &'a [&'a dyn Backend],
    pub types: &'a TypeTable,
    pub budget: u64,
    pub removed: &'a Removed,
}

/// Places a fixed node and counts crossings between fixed nodes.
fn place_fixed(state: &mut State<'_>, fixed: &[(u32, u8)]) -> u32 {
    fixed
        .iter()
        .map(|&(node, place)| state.place(node, place))
        .sum()
}

pub fn assign(input: &Input<'_>) -> Result<Assignment, Vec<AssignError>> {
    let Input {
        graph,
        registry,
        backends,
        types,
        budget,
        removed,
    } = *input;
    if backends.len() >= usize::from(u8::MAX) {
        return Err(vec![AssignError::TooManyBackends {
            count: backends.len(),
        }]);
    }
    let offers = offers::gather(graph, registry, backends, types, removed)?;
    let wiring = wiring(graph);
    let count = graph.nodes.len();
    let host = backends.len() as u8;
    let is_host = |node: usize| graph.nodes[node].doc.kind == NodeKindDoc::HostBridge;
    let free: Vec<bool> = (0..count)
        .map(|n| !is_host(n) && offers.candidates[n].len() > 1)
        .collect();
    let mut state = State::new(&wiring, host + 1, count);
    let fixed: Vec<(u32, u8)> = (0..count)
        .filter(|&n| !free[n])
        .map(|n| {
            (
                n as u32,
                if is_host(n) {
                    host
                } else {
                    offers.candidates[n][0]
                },
            )
        })
        .collect();
    let mut crossings = place_fixed(&mut state, &fixed);
    let mut reasons: Vec<Reason> = (0..count)
        .map(|n| {
            if is_host(n) {
                Reason::HostBridge
            } else if graph.nodes[n].doc.backend.is_some() {
                Reason::Forced
            } else {
                Reason::OnlyCandidate
            }
        })
        .collect();
    let mut expansions = 0;
    let mut errors = Vec::new();
    for group in components(&wiring, &free) {
        match search(
            &mut state,
            &group,
            &offers.candidates,
            budget,
            &mut expansions,
        ) {
            Ok(best) => {
                crossings += best.crossings;
                for (&node, &place) in group.iter().zip(&best.choice) {
                    state.place(node, place);
                    reasons[node as usize] = Reason::Chosen {
                        crossings: best.crossings,
                        rank: u32::from(place),
                    };
                }
            }
            Err(OverBudget) => errors.push(AssignError::AssignmentBudget {
                nodes: group
                    .iter()
                    .map(|&n| graph.nodes[n as usize].doc.id.clone())
                    .collect(),
                budget,
            }),
        }
    }
    if !errors.is_empty() {
        return Err(errors);
    }
    let placed = (0..count)
        .map(|n| match state.placed[n] {
            Some(place) if place != host => Placed::Backend(BackendIndex(place)),
            _ => Placed::Host,
        })
        .collect::<Vec<_>>();
    let bodies = (0..count)
        .map(|n| match placed[n] {
            Placed::Backend(BackendIndex(rank)) => offers.bodies[n].get(&rank).copied(),
            Placed::Host => None,
        })
        .collect();
    Ok(Assignment {
        placed,
        reasons,
        bodies,
        crossings,
        expansions,
    })
}
