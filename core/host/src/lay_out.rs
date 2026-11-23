//! The layout of a plan's steps: each step's slots, its wake edges, its join rule and the pressure
//! each edge declared. Built once, when a plan loads, and nothing here runs on a tick.

#![cfg(feature = "std")]

use std::collections::{BTreeMap, HashMap};
use std::mem;
use std::vec;
use std::vec::Vec;

use daedalus_abi::{NO_SLOT, Step};
use daedalus_backend::{StepUnit, UnitInfo};
use daedalus_graph::{EdgePolicyDoc, OverflowDoc};
use daedalus_made::{Bound, Item, Stitched};
use daedalus_plan::Span;
use daedalus_runner::{Fire, Recovery, SlotKind, SlotSpec, SlotStamp, StepMeta};
use daedalus_types::{SpaceIndex, TypeIndex};
use daedalus_value::{Keep, Overflow, Pressure};

use crate::intern::intern;
use crate::load::{LoadError, LoadOptions, NodeEntry, node_entries, take_steps};
use crate::recovery::step_recovery;

pub(crate) struct Built {
    pub(crate) steps: Vec<Step>,
    pub(crate) meta: Vec<StepMeta>,
    pub(crate) table: Vec<u32>,
    pub(crate) slots: Vec<SlotSpec>,
    pub(crate) edges: EdgeRules,
    pub(crate) covers: Vec<Vec<u32>>,
    pub(crate) same_as: Vec<Option<String>>,
    pub(crate) transfers: Vec<Option<&'static str>>,
}

/// The pressure each edge declared, by the slot it lands on.
pub(crate) type EdgeRules = BTreeMap<u32, EdgeRule>;

#[derive(Clone, Copy)]
pub(crate) struct EdgeRule {
    pub(crate) pressure: Pressure,
    pub(crate) blocking: bool,
    /// The slot's value lives for the tick it was written in.
    pub(crate) per_tick: bool,
}

/// One step's wiring while it's being laid out.
struct Draft {
    reads: Vec<u32>,
    writes: Vec<u32>,
    may_pend: bool,
    fire: Fire,
    required: Vec<u32>,
    reuse: bool,
    always: bool,
    recovery: Recovery,
}

fn span(table: &mut Vec<u32>, values: &[u32]) -> Span {
    let start = table.len() as u32;
    table.extend_from_slice(values);
    Span {
        start,
        len: values.len() as u32,
    }
}

/// Builds the plan's steps in layout order, with each step's slots and join rules.
pub(crate) fn lay_out(bound: &mut Bound, options: LoadOptions) -> Result<Built, LoadError> {
    let mut units = take_steps(bound);
    let mut move_steps = mem::take(&mut bound.move_steps);
    let stitched = &bound.facts.stitched;
    let mut steps = Vec::new();
    let mut drafts = Vec::new();
    let mut step_covers = Vec::new();
    let mut same_as = Vec::new();
    let mut transfers = Vec::new();
    let entries = node_entries(bound);
    let mut edges = EdgeRules::new();
    let host_spaces = host_spaces(stitched);
    for item in &stitched.layout {
        match item {
            Item::Run {
                unit,
                covers,
                reads,
                writes,
            } => {
                let Some(StepUnit { step, info }) = units[unit.region as usize]
                    .get_mut(unit.unit as usize)
                    .and_then(Option::take)
                else {
                    return Err(LoadError::NotSteps);
                };
                steps.push(step);
                step_covers.push(covers.clone());
                same_as.push(Some(same_step(bound, &entries, covers, &info)));
                transfers.push(None);
                let recovery = step_recovery(bound, covers)?;
                let connected = connected_reads(bound, &info, reads);
                let (fire, required) = join_for(bound, covers, &connected);
                for (slot, edge) in &connected {
                    apply_edge(&mut edges, *slot, &bound.facts.edges[*edge].policy)?;
                }
                drafts.push(Draft {
                    reads: reads.clone(),
                    writes: writes.clone(),
                    may_pend: info.may_pend,
                    always: reads.iter().all(|slot| *slot == NO_SLOT),
                    fire,
                    required,
                    reuse: covers.len() == 1
                        && bound.facts.nodes[covers[0] as usize].reusable
                        && writes.iter().filter(|slot| **slot != NO_SLOT).all(|slot| {
                            host_spaces.contains(&stitched.slots[*slot as usize].space)
                        }),
                    recovery,
                });
            }
            Item::Move {
                from_slot,
                to_slot,
                transfer,
            } => {
                let id = &stitched.transfers[*transfer as usize].id;
                let Some(step) = move_steps
                    .get_mut(*transfer as usize)
                    .and_then(Option::take)
                else {
                    return Err(LoadError::TransferUnbound { id: id.clone() });
                };
                steps.push(step);
                step_covers.push(Vec::new());
                same_as.push(None);
                transfers.push(Some(intern(id)));
                drafts.push(Draft {
                    reads: vec![*from_slot],
                    writes: vec![*to_slot],
                    may_pend: false,
                    fire: Fire::Any,
                    required: Vec::new(),
                    reuse: false,
                    always: false,
                    recovery: Recovery::Fail,
                });
            }
            // A step that returned `Pending` already holds its consumers back until its fence
            // is ready, so a wait needs no step of its own.
            Item::Wait { .. } => {}
        }
    }
    bound.move_steps = move_steps;
    let slots = slot_specs(bound, options, &edges);
    let (meta, table) = wire(&drafts);
    Ok(Built {
        steps,
        meta,
        table,
        slots,
        edges,
        covers: step_covers,
        same_as,
        transfers,
    })
}

/// What has to be equal for a step of one plan to stand in for a step of another: the nodes it
/// covers, at the same places in the graph, with the same bodies and constants, and the same
/// ports in the same order. A step holds its nodes' state and constants, so a step that matches
/// is moved across a reload as it is, and one that doesn't starts fresh.
fn same_step(bound: &Bound, entries: &[NodeEntry], covers: &[u32], info: &UnitInfo) -> String {
    let nodes: Vec<_> = covers
        .iter()
        .map(|&node| {
            let entry = &entries[node as usize];
            let consts = &bound.facts.nodes[node as usize].consts;
            (node, &entry.key, &entry.body, consts)
        })
        .collect();
    std::format!("{nodes:?} {info:?}")
}

/// The spaces a host port lives in. Reuse keeps an output between runs, which only a value the
/// process owns can do, so a reusable node stays reusable only when it writes into one of these.
fn host_spaces(stitched: &Stitched) -> Vec<u32> {
    stitched
        .host_inputs
        .iter()
        .chain(&stitched.host_outputs)
        .map(|port| stitched.slots[port.slot as usize].space)
        .collect()
}

/// The unit's reads that have a slot and an edge, as (slot, edge). The layout's `reads` line up
/// with the unit's, with `NO_SLOT` at a port nothing feeds.
fn connected_reads(bound: &Bound, info: &UnitInfo, reads: &[u32]) -> Vec<(u32, usize)> {
    let edges = &bound.facts.edges;
    let edge_of = |input: &daedalus_backend::PortRef| {
        let node = input.node.0;
        let port = bound.facts.nodes[node as usize]
            .inputs
            .get(usize::from(input.port.0))?;
        edges
            .iter()
            .position(|edge| edge.to_node == node && edge.to_port == *port)
    };
    info.reads
        .iter()
        .zip(reads)
        .filter(|(_, slot)| **slot != NO_SLOT)
        .filter_map(|(input, slot)| Some((*slot, edge_of(input)?)))
        .collect()
}

/// The join rule of a unit: whether a partial set waits, and the slots its nodes require.
fn join_for(bound: &Bound, covers: &[u32], connected: &[(u32, usize)]) -> (Fire, Vec<u32>) {
    let nodes = &bound.facts.nodes;
    let fire = if covers.iter().any(|&node| nodes[node as usize].fire_all) {
        Fire::All
    } else {
        Fire::Any
    };
    let required = connected
        .iter()
        .filter(|(_, edge)| {
            let to = &bound.facts.edges[*edge];
            covers.contains(&to.to_node)
                && nodes[to.to_node as usize].required.contains(&to.to_port)
        })
        .map(|(slot, _)| *slot)
        .collect();
    (fire, required)
}

/// Records what an edge declared, for the slot it lands on. FIFO is the plain default. A bounded
/// edge whose overflow word names no rule is refused: the planner refuses it first, so this is a
/// plan that was changed by hand.
fn apply_edge(rules: &mut EdgeRules, slot: u32, policy: &EdgePolicyDoc) -> Result<(), LoadError> {
    let mut blocking = false;
    let pressure = match policy {
        EdgePolicyDoc::Fifo => return Ok(()),
        EdgePolicyDoc::LatestOnly | EdgePolicyDoc::PerTick => Pressure::LatestOnly,
        EdgePolicyDoc::Bounded { capacity, overflow } => {
            let rule = OverflowDoc::parse(overflow).ok_or_else(|| LoadError::Overflow {
                overflow: overflow.clone(),
            })?;
            blocking = rule == OverflowDoc::Block;
            Pressure::Bounded {
                cap: (*capacity).max(1),
                overflow: match rule {
                    OverflowDoc::DropOldest => Overflow::DropOldest,
                    OverflowDoc::DropNewest => Overflow::DropNewest,
                    OverflowDoc::ErrorOnFull | OverflowDoc::Block => Overflow::ErrorOnFull,
                    OverflowDoc::Coalesce => Overflow::Coalesce {
                        keep: Keep::Newest,
                        window_ns: 0,
                    },
                },
            }
        }
    };
    let per_tick = matches!(policy, EdgePolicyDoc::PerTick);
    let rule = EdgeRule {
        pressure,
        blocking,
        per_tick,
    };
    rules.insert(slot, rule);
    Ok(())
}

/// The slots that keep their value: one with several readers keeps it for all of them, and a held
/// or shared host input keeps it for every tick, whatever number of readers it has.
pub(crate) fn kept_slots(stitched: &Stitched) -> Vec<u32> {
    let mut kept: Vec<u32> = stitched
        .slots
        .iter()
        .enumerate()
        .filter(|(_, info)| info.consumers > 1)
        .map(|(at, _)| at as u32)
        .chain(
            stitched
                .host_inputs
                .iter()
                .filter(|port| port.policy.is_context())
                .map(|port| port.slot),
        )
        .collect();
    kept.sort_unstable();
    kept.dedup();
    kept
}

/// Each slot's capacity and read rule. A kept slot ([`kept_slots`]) has one cell, and an edge
/// that declared a bound gets exactly that many cells.
fn slot_specs(bound: &Bound, options: LoadOptions, edges: &EdgeRules) -> Vec<SlotSpec> {
    let kept = kept_slots(&bound.facts.stitched);
    bound
        .facts
        .stitched
        .slots
        .iter()
        .enumerate()
        .map(|(at, _)| {
            if kept.binary_search(&(at as u32)).is_ok() {
                return SlotSpec {
                    cap: 1,
                    kind: SlotKind::Held,
                };
            }
            let cap = match edges.get(&(at as u32)).map(|rule| rule.pressure) {
                Some(Pressure::Bounded { cap, .. }) => cap,
                Some(Pressure::LatestOnly) => 1,
                _ => options.fifo_capacity.max(1),
            };
            SlotSpec {
                cap,
                kind: SlotKind::Fifo,
            }
        })
        .collect()
}

/// The step records, with a wake edge from each producer to the readers of what it wrote.
fn wire(drafts: &[Draft]) -> (Vec<StepMeta>, Vec<u32>) {
    let mut last_writer: HashMap<u32, usize> = HashMap::new();
    let mut wakes: Vec<Vec<u32>> = vec![Vec::new(); drafts.len()];
    for (at, draft) in drafts.iter().enumerate() {
        for slot in draft.reads.iter().filter(|slot| **slot != NO_SLOT) {
            if let Some(&writer) = last_writer.get(slot)
                && writer != at
            {
                wakes[writer].push(at as u32);
            }
        }
        for slot in draft.writes.iter().filter(|slot| **slot != NO_SLOT) {
            last_writer.insert(*slot, at);
        }
    }
    let mut table = Vec::new();
    let meta = drafts
        .iter()
        .zip(wakes)
        .map(|(draft, mut wake)| {
            wake.sort_unstable();
            wake.dedup();
            StepMeta {
                wake: span(&mut table, &wake),
                inputs: span(&mut table, &draft.reads),
                outputs: span(&mut table, &draft.writes),
                always: draft.always,
                may_pend: draft.may_pend,
                recovery: draft.recovery,
                fire: draft.fire,
                required: span(&mut table, &draft.required),
                reuse: draft.reuse,
            }
        })
        .collect();
    (meta, table)
}

/// The sorted port table. Interned, so loading the same graph again adds nothing.
/// What each slot's values are. A compiled step doesn't know these numbers, so the runner writes
/// them on what the step publishes.
pub(crate) fn slot_stamps(stitched: &Stitched) -> Vec<SlotStamp> {
    stitched
        .slots
        .iter()
        .map(|info| SlotStamp {
            ty: TypeIndex(info.ty.unwrap_or(0)),
            space: SpaceIndex(info.space as u16),
        })
        .collect()
}
