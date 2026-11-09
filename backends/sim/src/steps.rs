//! The steps: each bumps a counter, and faults if told to.

use daedalus_abi::{OwnedStep, Slots, Step, StepCx, StepEnd, StepWord};
use daedalus_backend::{CompileCx, Refusal};

use crate::counters::{bump_node, bump_transfer};

const FAULT: usize = 1 << (usize::BITS - 1);
const REASON: &str = "sim fault";

fn fail(node: u32, cx: &mut StepCx) -> StepEnd {
    cx.fault(Some(node), REASON);
    StepEnd::Fault
}

struct NodeStep;

impl StepWord for NodeStep {
    fn run(word: usize, _: &mut Slots, cx: &mut StepCx) -> StepEnd {
        let node = (word & !FAULT) as u32;
        bump_node(node);
        if word & FAULT != 0 {
            fail(node, cx)
        } else {
            StepEnd::Done
        }
    }
}

struct MergedStep {
    nodes: Box<[u32]>,
    faults: &'static [u32],
}

impl OwnedStep for MergedStep {
    fn run(&mut self, _: &mut Slots, cx: &mut StepCx) -> StepEnd {
        for &node in &self.nodes {
            bump_node(node);
            if self.faults.contains(&node) {
                return fail(node, cx);
            }
        }
        StepEnd::Done
    }
}

pub(crate) fn step_for(nodes: &[u32], faults: &'static [u32]) -> Step {
    match nodes {
        [node] => {
            let fault = if faults.contains(node) { FAULT } else { 0 };
            Step::of::<NodeStep>(*node as usize | fault)
        }
        _ => Step::boxed(MergedStep {
            nodes: nodes.into(),
            faults,
        }),
    }
}

struct TransferStep;

impl StepWord for TransferStep {
    fn run(word: usize, _: &mut Slots, _: &mut StepCx) -> StepEnd {
        bump_transfer(word);
        StepEnd::Done
    }
}

pub(crate) fn make_transfer<const INDEX: usize>(_: &CompileCx<'_>) -> Result<Step, Refusal> {
    Ok(Step::of::<TransferStep>(INDEX))
}
