//! Writes the flat list.

use std::collections::{BTreeMap, BTreeSet};

use daedalus_backend::{Compiled, PortRef, UnitInfo};
use daedalus_plan::{PlanData, Span, StepKind, StepRecord};

use super::fences::Fences;
use super::slots::{Live, number};
use super::transfers::{Reader, Routing, Source};
use daedalus_abi::NO_SLOT;

use super::{HostPort, Item, SlotInfo, Stitched, TransferUse, UnitId};
use crate::regions::CompiledRegion;
use crate::regions::Regions;
use crate::solve::type_table::TypeTable;

/// An item before slots are numbered.
pub(super) enum Draft {
    Run {
        unit: UnitId,
        covers: Vec<u32>,
        reads: Vec<Option<Live>>,
        writes: Vec<Option<Live>>,
    },
    Move {
        from: Live,
        to: Live,
        transfer: usize,
        covers: u32,
    },
    Wait {
        unit: UnitId,
    },
}

impl Draft {
    pub fn reads(&self) -> Vec<Live> {
        match self {
            Draft::Run { reads, .. } => reads.iter().flatten().copied().collect(),
            Draft::Move { from, .. } => vec![*from],
            Draft::Wait { .. } => Vec::new(),
        }
    }

    pub fn writes(&self) -> Vec<Live> {
        match self {
            Draft::Run { writes, .. } => writes.iter().flatten().copied().collect(),
            Draft::Move { to, .. } => vec![*to],
            Draft::Wait { .. } => Vec::new(),
        }
    }
}

fn units(compiled: &Compiled) -> Vec<&UnitInfo> {
    match compiled {
        Compiled::Steps(steps) => steps.iter().map(|step| &step.info).collect(),
        Compiled::Blob { units, .. } | Compiled::Source { units, .. } => units.iter().collect(),
    }
}

/// Regions with producers before consumers, lowest number first among the ready ones.
fn region_order(regions: &[CompiledRegion], routing: &Routing) -> Vec<usize> {
    let mut writer = BTreeMap::new();
    for (at, region) in regions.iter().enumerate() {
        for output in &region.formed.outputs {
            writer.insert((output.node.0, output.port.0), at);
        }
    }
    let count = regions.len();
    let mut waiting = vec![BTreeSet::new(); count];
    for (at, region) in regions.iter().enumerate() {
        for input in &region.formed.inputs {
            if let Some(Source::Node { node, port }) = routing
                .readers
                .get(input)
                .map(|r| &routing.values[r.value as usize].source)
                && let Some(&from) = writer.get(&(*node, *port))
            {
                waiting[at].insert(from);
            }
        }
    }
    let mut placed = vec![false; count];
    let mut order = Vec::with_capacity(count);
    while order.len() < count {
        let next = (0..count).find(|&at| !placed[at] && waiting[at].iter().all(|&w| placed[w]));
        let Some(next) = next else {
            break; // Convex regions of a DAG can't form a cycle.
        };
        placed[next] = true;
        order.push(next);
    }
    order
}

struct Walk<'a> {
    routing: &'a Routing,
    draft: Vec<Draft>,
    delivered: BTreeSet<Live>,
    fences: Fences,
}

impl Walk<'_> {
    /// Emits the moves that bring a value to its reader, each once.
    fn moves(&mut self, reader: &Reader, covers: u32) {
        let routing = self.routing;
        let home = routing.values[reader.value as usize].home;
        let mut at = home;
        for &hop in &reader.path {
            let to = routing.table[hop].to;
            let live = (reader.value, to);
            if self.delivered.insert(live) {
                self.draft.push(Draft::Move {
                    from: (reader.value, at),
                    to: live,
                    transfer: hop,
                    covers,
                });
            }
            at = to;
        }
    }
}

fn value_of_output(routing: &Routing, output: &PortRef) -> Vec<u32> {
    (0..routing.values.len() as u32)
        .filter(|&value| {
            routing.values[value as usize].source
                == Source::Node {
                    node: output.node.0,
                    port: output.port.0,
                }
        })
        .collect()
}

fn walk_region(walk: &mut Walk<'_>, at: usize, region: &CompiledRegion) {
    let routing = walk.routing;
    let incoming = &routing.incoming[at];
    for (number, info) in units(&region.compiled).into_iter().enumerate() {
        let unit = UnitId {
            region: at as u32,
            unit: number as u32,
        };
        let mut reads = Vec::new();
        let absorbed: Vec<PortRef> = info
            .absorbed
            .iter()
            .filter_map(|&i| incoming.get(i as usize).copied())
            .collect();
        let covers = info.covers.first().map_or(0, |node| node.0);
        for input in &info.reads {
            let Some(reader) = routing.readers.get(input) else {
                reads.push(None);
                continue;
            };
            if let Some(producer) = walk.fences.wait_for(reader.value, at as u32) {
                walk.draft.push(Draft::Wait { unit: producer });
            }
            if absorbed.contains(input) {
                reads.push(Some((
                    reader.value,
                    routing.values[reader.value as usize].home,
                )));
            } else {
                walk.moves(reader, covers);
                reads.push(Some((reader.value, reader.space)));
            }
        }
        let mut writes = Vec::new();
        for output in &info.writes {
            // Each output has one place in the frame, and at most one value.
            let value = value_of_output(routing, output).first().copied();
            writes.push(value.map(|value| (value, routing.values[value as usize].home)));
            if let Some(value) = value.filter(|_| info.may_pend) {
                walk.fences.record_write(value, unit);
            }
        }
        walk.draft.push(Draft::Run {
            unit,
            covers: info.covers.iter().map(|node| node.0).collect(),
            reads,
            writes,
        });
    }
}

struct Encoder {
    plan: PlanData,
}

impl Encoder {
    fn nodes(&mut self, list: &[u32]) -> Span {
        let span = Span {
            start: self.plan.nodes.len() as u32,
            len: list.len() as u32,
        };
        self.plan.nodes.extend_from_slice(list);
        span
    }

    fn slots(&mut self, list: &[u32]) -> Span {
        let span = Span {
            start: self.plan.slots.len() as u32,
            len: list.len() as u32,
        };
        self.plan.slots.extend_from_slice(list);
        span
    }
}

/// Lays out compiled regions as one list.
pub(super) fn lay_out(regions: &Regions, types: &TypeTable) -> Stitched {
    let routing = &regions.routing;
    let mut walk = Walk {
        routing,
        draft: Vec::new(),
        delivered: BTreeSet::new(),
        fences: Fences::default(),
    };
    let mut flat: BTreeMap<UnitId, u32> = BTreeMap::new();
    for at in region_order(&regions.regions, routing) {
        let count = units(&regions.regions[at].compiled).len() as u32;
        for unit in 0..count {
            flat.insert(
                UnitId {
                    region: at as u32,
                    unit,
                },
                flat.len() as u32,
            );
        }
        walk_region(&mut walk, at, &regions.regions[at]);
    }
    let mut at_start: Vec<Live> = Vec::new();
    for (index, value) in routing.values.iter().enumerate() {
        if matches!(value.source, Source::Host { .. }) {
            at_start.push((index as u32, value.home));
        }
    }
    let mut at_end = Vec::new();
    for output in &routing.host_outputs {
        walk.moves(&output.reader, output.node);
        at_end.push((output.reader.value, output.reader.space));
    }
    let draft = walk.draft;
    let numbered = number(&draft, &at_start, &at_end);
    let slot_of = |live: &Live| numbered.of[live];
    let slot_or_none = |live: &Option<Live>| live.as_ref().map_or(NO_SLOT, slot_of);
    let mut uses: Vec<TransferUse> = Vec::new();
    let mut layout = Vec::with_capacity(draft.len());
    let mut encoder = Encoder {
        plan: PlanData::default(),
    };
    for item in &draft {
        match item {
            Draft::Run {
                unit,
                covers,
                reads,
                writes,
            } => {
                let (reads, writes): (Vec<u32>, Vec<u32>) = (
                    reads.iter().map(slot_or_none).collect(),
                    writes.iter().map(slot_or_none).collect(),
                );
                let record = StepRecord {
                    kind: StepKind::Call,
                    body: flat[unit],
                    covers: encoder.nodes(covers),
                    reads: encoder.slots(&reads),
                    writes: encoder.slots(&writes),
                };
                encoder.plan.steps.push(record);
                layout.push(Item::Run {
                    unit: *unit,
                    covers: covers.clone(),
                    reads,
                    writes,
                });
            }
            Draft::Move {
                from,
                to,
                transfer,
                covers,
            } => {
                let (from, to) = (slot_of(from), slot_of(to));
                let index = uses
                    .iter()
                    .position(|u| u.table == *transfer)
                    .unwrap_or_else(|| {
                        uses.push(TransferUse {
                            table: *transfer,
                            id: routing.table[*transfer].id.clone(),
                        });
                        uses.len() - 1
                    }) as u32;
                let record = StepRecord {
                    kind: StepKind::Transfer,
                    body: index,
                    covers: encoder.nodes(&[*covers]),
                    reads: encoder.slots(&[from]),
                    writes: encoder.slots(&[to]),
                };
                encoder.plan.steps.push(record);
                layout.push(Item::Move {
                    from_slot: from,
                    to_slot: to,
                    transfer: index,
                });
            }
            Draft::Wait { unit } => {
                encoder.plan.steps.push(StepRecord {
                    kind: StepKind::Wait,
                    body: flat[unit],
                    covers: Span::default(),
                    reads: Span::default(),
                    writes: Span::default(),
                });
                layout.push(Item::Wait { unit: *unit });
            }
        }
    }
    let mut slots = vec![
        SlotInfo {
            space: 0,
            ty: None,
            consumers: 0
        };
        numbered.count as usize
    ];
    for (live, &slot) in &numbered.of {
        let value = &routing.values[live.0 as usize];
        slots[slot as usize] = SlotInfo {
            space: live.1 as u32,
            ty: types.get(&value.ty).map(|ty| ty.0),
            consumers: numbered.readers.get(live).copied().unwrap_or(0),
        };
    }
    for slot in &slots {
        encoder.plan.spaces.push(slot.space);
        encoder.plan.types.push(slot.ty.unwrap_or(u32::MAX));
    }
    let mut bodies = vec![0; flat.len()];
    for (unit, &index) in &flat {
        bodies[index as usize] = unit.region;
    }
    encoder.plan.bodies = bodies;
    let host_inputs = host_inputs(routing, &numbered.of);
    let host_outputs = routing
        .host_outputs
        .iter()
        .map(|o| HostPort {
            node: o.node,
            port: o.port.clone(),
            slot: numbered.of[&(o.reader.value, o.reader.space)],
        })
        .collect();
    Stitched {
        layout,
        slots,
        spaces: routing.spaces.clone(),
        transfers: uses,
        host_inputs,
        host_outputs,
        plan: encoder.plan,
    }
}

fn host_inputs(routing: &Routing, slots: &BTreeMap<Live, u32>) -> Vec<HostPort> {
    routing
        .values
        .iter()
        .enumerate()
        .filter_map(|(index, value)| match &value.source {
            Source::Host { node, port } => Some(HostPort {
                node: *node,
                port: port.clone(),
                slot: slots[&(index as u32, value.home)],
            }),
            Source::Node { .. } => None,
        })
        .collect()
}
