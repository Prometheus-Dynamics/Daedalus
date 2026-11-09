//! Merging: nodes that always run one after the other become one step, and values pass between
//! them without going through a queue.
//!
//! A node joins the one before it when that node is the only thing feeding it, so a merged step
//! fires exactly when its first node would and behaves the same as the separate steps. Nodes that
//! wait on more than one input start a step of their own, and so does the node after one with a
//! second output in use.

use alloc::vec;
use alloc::vec::Vec;
use core::ops::Range;

use daedalus_abi::{NO_SLOT, OwnedStep, Slots, Step, StepCx, StepEnd};
use daedalus_backend::{CompileCx, PortRef, Refusal, Region, StepUnit, UnitInfo};
use daedalus_runner::{Nested, NestedStep};
use daedalus_types::{NodeIndex, PortIndex};

use crate::backend::{body_of, step_of, unit};

fn wired_inputs(node: NodeIndex, region: &Region<'_>, cx: &CompileCx<'_>) -> Vec<PortRef> {
    let mut wired: Vec<PortRef> = region
        .inputs
        .iter()
        .copied()
        .chain(cx.edges.iter().map(|edge| edge.to))
        .filter(|port| port.node == node)
        .collect();
    wired.sort_unstable();
    wired.dedup();
    wired
}

/// The edge that merges `next` onto `previous`, if there is one.
fn merged_edge(
    previous: NodeIndex,
    next: NodeIndex,
    region: &Region<'_>,
    cx: &CompileCx<'_>,
) -> Result<Option<PortRef>, Refusal> {
    let (producer, consumer) = (body_of(previous, cx)?, body_of(next, cx)?);
    if !producer.merges || !consumer.merges {
        return Ok(None);
    }
    let [only] = wired_inputs(next, region, cx)[..] else {
        return Ok(None);
    };
    let Some(edge) = cx.edges.iter().find(|edge| edge.to == only) else {
        return Ok(None);
    };
    let fan_out = cx.edges.iter().filter(|e| e.from == edge.from).count();
    let leaves = region.outputs.contains(&edge.from);
    // A fault drops everything the step wrote, so an earlier node may not have a value on its way
    // anywhere else.
    let only_output = (0..producer.outputs).all(|port| {
        let port = PortRef {
            node: previous,
            port: PortIndex(port),
        };
        port == edge.from || !wired_output(port, region, cx)
    });
    let joins = edge.from.node == previous && !edge.queued && fan_out == 1 && !leaves;
    Ok((joins && only_output).then_some(edge.from))
}

/// A run of nodes that merge, and the output each node after the first takes its value from.
type Group = (Range<usize>, Vec<PortRef>);

/// The region's nodes as runs that merge.
fn groups(region: &Region<'_>, cx: &CompileCx<'_>) -> Result<Vec<Group>, Refusal> {
    let mut groups = Vec::new();
    let mut start = 0;
    let mut links = Vec::new();
    for at in 1..=region.nodes.len() {
        let link = match region.nodes.get(at) {
            Some(&next) => merged_edge(region.nodes[at - 1], next, region, cx)?,
            None => None,
        };
        match link {
            Some(link) => links.push(link),
            None => {
                groups.push((start..at, core::mem::take(&mut links)));
                start = at;
            }
        }
    }
    Ok(groups)
}

/// One step that runs several nodes.
struct Merged {
    nested: Nested,
    reads: usize,
    /// The nested cell of the first output. The rest follow it.
    first_write: u32,
}

impl OwnedStep for Merged {
    fn run(&mut self, slots: &mut Slots, cx: &mut StepCx) -> StepEnd {
        let (inputs, outputs) = slots.as_mut_slice().split_at_mut(self.reads);
        let end = self.nested.run_chain(inputs, cx);
        for (at, input) in inputs.iter_mut().enumerate() {
            if self.nested.moved(at as u32) {
                input.state = daedalus_abi::SlotState::Moved as u32;
            }
        }
        if matches!(end, StepEnd::Done) {
            for (at, output) in outputs.iter_mut().enumerate() {
                if let Some(value) = self.nested.take(self.first_write + at as u32) {
                    *output = value;
                }
            }
        }
        self.nested.reset();
        end
    }
}

fn wired_output(port: PortRef, region: &Region<'_>, cx: &CompileCx<'_>) -> bool {
    region.outputs.contains(&port) || cx.edges.iter().any(|edge| edge.from == port)
}

fn merged_unit(
    nodes: &[NodeIndex],
    links: &[PortRef],
    region: &Region<'_>,
    cx: &CompileCx<'_>,
) -> Result<StepUnit, Refusal> {
    let head = nodes[0];
    let reads = wired_inputs(head, region, cx);
    let locals_at = reads.len() as u32;
    let first_write = locals_at + links.len() as u32;
    let local = |port: PortRef| links.iter().position(|link| *link == port);

    let mut writes = Vec::new();
    let mut steps = Vec::with_capacity(nodes.len());
    for (at, &node) in nodes.iter().enumerate() {
        let body = body_of(node, cx)?;
        let port_of = |port: u16| PortRef {
            node,
            port: PortIndex(port),
        };
        let bound_inputs = (0..body.inputs).map(|port| {
            if at == 0 {
                let found = reads.iter().position(|read| *read == port_of(port));
                return found.map_or(NO_SLOT, |view| view as u32);
            }
            // The first node's output feeds the second, and so on, so this is that node's link.
            if wired_inputs(node, region, cx).contains(&port_of(port)) {
                locals_at + at as u32 - 1
            } else {
                NO_SLOT
            }
        });
        let bound_inputs = bound_inputs.collect();
        let mut bound_outputs = vec![NO_SLOT; body.outputs as usize];
        for port in 0..body.outputs {
            let here = port_of(port);
            bound_outputs[port as usize] = if let Some(link) = local(here) {
                locals_at + link as u32
            } else if wired_output(here, region, cx) {
                writes.push(here);
                first_write + writes.len() as u32 - 1
            } else {
                NO_SLOT
            };
        }
        steps.push(NestedStep {
            step: step_of(node, body, cx)?,
            reads: bound_inputs,
            writes: bound_outputs,
        });
    }
    let cells = first_write as usize + writes.len();
    let views = (0..locals_at).collect();
    let merged = Merged {
        nested: Nested::new(steps, cells, views),
        reads: reads.len(),
        first_write,
    };
    Ok(StepUnit {
        step: Step::boxed(merged),
        info: UnitInfo {
            covers: nodes.to_vec(),
            reads,
            writes,
            absorbed: Vec::new(),
            may_pend: false,
        },
    })
}

/// Compiles a region, merging what can be merged.
pub(crate) fn compile(region: &Region<'_>, cx: &CompileCx<'_>) -> Result<Vec<StepUnit>, Refusal> {
    let mut units = Vec::new();
    for (range, links) in groups(region, cx)? {
        let nodes = &region.nodes[range];
        units.push(if let [node] = nodes {
            unit(*node, cx)?
        } else {
            merged_unit(nodes, &links, region, cx)?
        });
    }
    Ok(units)
}
