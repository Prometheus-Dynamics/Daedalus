//! How values move between a worker and the CPU backend: through `cpu.wire`, the CPU backend's
//! space whose cells hold their type's wire form. A worker's values are that form already, so
//! each move hands the cell on as it is. Nothing here reads a value or knows its type: the CPU
//! node that wrote it into `cpu.wire` wrote its wire form, and the one that reads it there reads
//! that form back, each through its type's own `WireForm`.
//!
//! There is no move between a worker and `cpu.host`, where cells hold Rust values in memory:
//! such a value never reaches a worker as its memory's bytes.
//!
//! A CPU node's value in `cpu.wire` is copied into the worker's lent memory, or into the run's
//! packet, on its way in, so that move costs a copy. A value written in `process.lent` (see
//! [`crate::LENT`]) is in the worker's memory already: a worker reads it there, and a CPU node
//! reads it through a hand-on to `cpu.wire`.

use std::borrow::Cow;

use daedalus_abi::{OwnedStep, Slots, Step, StepCx, StepEnd};
use daedalus_backend::{CompileCx, Refusal, Transfer, TransferCost};

use crate::region::{HOST, LENT};

/// The CPU backend's space whose cells hold their type's wire form.
pub const CPU_WIRE: &str = "cpu.wire";

pub const TO_WORKER: &str = "process.from_cpu_wire";
pub const FROM_WORKER: &str = "process.to_cpu_wire";
pub const LENT_TO_CPU: &str = "process.lent_to_cpu_wire";

/// The two moves, for any type.
pub(crate) fn transfers() -> Vec<Transfer> {
    let entry = |id, from, to, cost| Transfer {
        id,
        key: None,
        from: Cow::Borrowed(from),
        to: Cow::Borrowed(to),
        cost,
        make: hand,
    };
    let mut moves = vec![
        entry(TO_WORKER, CPU_WIRE, HOST, TransferCost::Copy),
        entry(FROM_WORKER, HOST, CPU_WIRE, TransferCost::Handoff),
    ];
    if cfg!(target_os = "linux") {
        moves.push(entry(LENT_TO_CPU, LENT, CPU_WIRE, TransferCost::Handoff));
    }
    moves
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
