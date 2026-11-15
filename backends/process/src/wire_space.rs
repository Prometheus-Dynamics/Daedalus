//! How values move between a worker and the CPU backend: through `cpu.wire`, the CPU backend's
//! space whose cells hold their type's wire form. A worker's values are that form already, so
//! each move hands the cell on as it is. Nothing here reads a value or knows its type: the CPU
//! node that wrote it into `cpu.wire` wrote its wire form, and the one that reads it there reads
//! that form back, each through its type's own `WireForm`.
//!
//! There is no move between a worker and `cpu.host`, where cells hold Rust values in memory:
//! such a value never reaches a worker as its memory's bytes.

use std::borrow::Cow;

use daedalus_abi::{OwnedStep, Slots, Step, StepCx, StepEnd};
use daedalus_backend::{CompileCx, Refusal, Transfer, TransferCost};

use crate::region::HOST;

/// The CPU backend's space whose cells hold their type's wire form.
pub const CPU_WIRE: &str = "cpu.wire";

pub const TO_WORKER: &str = "process.from_cpu_wire";
pub const FROM_WORKER: &str = "process.to_cpu_wire";

/// The two moves, for any type.
pub(crate) fn transfers() -> Vec<Transfer> {
    let entry = |id, from, to| Transfer {
        id,
        key: None,
        from: Cow::Borrowed(from),
        to: Cow::Borrowed(to),
        cost: TransferCost::Handoff,
        make: hand,
    };
    vec![
        entry(TO_WORKER, CPU_WIRE, HOST),
        entry(FROM_WORKER, HOST, CPU_WIRE),
    ]
}

fn hand(_: &CompileCx<'_>) -> Result<Step, Refusal> {
    Ok(Step::boxed(Hand))
}

/// Hands a cell on from one space to the other, as it is.
struct Hand;

impl OwnedStep for Hand {
    fn run(&mut self, slots: &mut Slots, cx: &mut StepCx) -> StepEnd {
        let cells = slots.as_mut_slice();
        let [input, out] = cells else {
            cx.fault(None, "a wire move has one input and one output");
            return StepEnd::Fault;
        };
        if daedalus_runner::hand_on(input, out) {
            StepEnd::Done
        } else {
            cx.fault(None, "a wire move was given no value");
            StepEnd::Fault
        }
    }
}
