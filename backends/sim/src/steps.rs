//! The steps: each bumps a counter, and faults if told to.

use daedalus_abi::{OwnedStep, Slots, Step, StepCx, StepEnd, StepWord};
use daedalus_backend::{CompileCx, Refusal};

use crate::counters::{bump_node, bump_transfer};

const FAULT: usize = 1 << (usize::BITS - 1);
const REASON: &str = "sim fault";

/// How many lines of detail a failing node writes. Zero, the default, writes none.
static DETAIL_LINES: core::sync::atomic::AtomicUsize = core::sync::atomic::AtomicUsize::new(0);

/// Has every sim node say that it ran with [`StepCx::detail`], and a failing one follow that with
/// `lines` lines of what it found. For tests of what a host keeps. Zero turns it off again.
pub fn write_details(lines: usize) {
    DETAIL_LINES.store(lines, core::sync::atomic::Ordering::Relaxed);
}

fn ran(node: u32, cx: &StepCx) {
    let lines = DETAIL_LINES.load(core::sync::atomic::Ordering::Relaxed);
    if lines != 0 {
        cx.detail_fmt(format_args!("node {node} ran\n"));
    }
}

fn fail(node: u32, cx: &mut StepCx) -> StepEnd {
    for line in 0..DETAIL_LINES.load(core::sync::atomic::Ordering::Relaxed) {
        cx.detail_fmt(format_args!("node {node} found line {line}\n"));
    }
    cx.fault(Some(node), REASON);
    StepEnd::Fault
}

struct NodeStep;

impl StepWord for NodeStep {
    fn run(word: usize, _: &mut Slots, cx: &mut StepCx) -> StepEnd {
        let node = (word & !FAULT) as u32;
        bump_node(node);
        ran(node, cx);
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
            ran(node, cx);
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
