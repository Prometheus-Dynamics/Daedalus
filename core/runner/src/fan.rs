//! Fan-out: an output that several readers share goes to a queue for each of them. A step with
//! such an output has more output slots than its body has outputs. After the body ran, each added
//! output cell takes the value of the output it fans, with a reference of its own, and from
//! there it is an output like any other: published, hidden, replayed and held back as one. Nothing
//! is copied but an inline value's bytes, when the outputs are published, as any inline output's
//! are. A plan with no fan pays one test a step.

use daedalus_abi::{Slot, SlotState};

use crate::Runner;
use crate::error::LoadError;
use crate::owned;

/// One added output of a step: its output `to` takes the value its body wrote to output `from`.
/// A step's added outputs come after its body's own.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FanOut {
    pub step: u32,
    pub from: u16,
    pub to: u16,
}

/// The entries of `step` in `fans`, which are sorted by step.
#[inline]
pub(crate) fn of_step(fans: &[FanOut], step: usize) -> &[FanOut] {
    if fans.is_empty() {
        return fans;
    }
    let start = fans.partition_point(|fan| (fan.step as usize) < step);
    let len = fans[start..].partition_point(|fan| fan.step as usize == step);
    &fans[start..start + len]
}

/// Fills the added output cells of a step from the ones its body wrote. `outputs` are the step's
/// output cells, the body's own and then the added ones.
pub(crate) fn fill(fans: &[FanOut], outputs: &mut [Slot]) {
    for fan in fans {
        let (Some(&from), true) = (
            outputs.get(usize::from(fan.from)),
            usize::from(fan.to) < outputs.len(),
        ) else {
            continue;
        };
        if from.state != SlotState::Full as u32 {
            continue;
        }
        // SAFETY: the cell is full, so it owns a live reference, and the added cell takes one of
        // its own, which its queue or a discard releases once.
        unsafe { owned::retain(&from) };
        outputs[usize::from(fan.to)] = from;
    }
}

impl<'a> Runner<'a> {
    /// Attaches the plan's fan-outs, sorted by step. Each names two outputs of its step, the
    /// added one after every output the body writes.
    pub fn set_fans(&mut self, fans: &'a [FanOut]) -> Result<(), LoadError> {
        let sorted = fans.windows(2).all(|pair| pair[0].step <= pair[1].step);
        let inside = fans.iter().all(|fan| {
            self.meta
                .get(fan.step as usize)
                .is_some_and(|meta| fan.from < fan.to && u32::from(fan.to) < meta.outputs.len)
        });
        if !sorted || !inside {
            return Err(LoadError::Length);
        }
        self.fans = fans;
        Ok(())
    }
}
