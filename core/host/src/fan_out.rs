//! Fan-out: an output that several readers share gets a queue for each of them. The planner gives
//! such an output one slot. Here every reader but one is pointed at a new slot of its own, and the
//! producer gets those slots as added outputs, which the runner fills from the one its body
//! writes (`daedalus_runner::FanOut`). Every reader then has an ordinary edge, with its own depth
//! and policy, and takes its value once; the value is shared by its reference count and never
//! copied. A host input is left alone: what it holds is the host's to feed and replace.

use std::collections::BTreeSet;
use std::vec::Vec;

use daedalus_abi::NO_SLOT;
use daedalus_made::Stitched;
use daedalus_runner::FanOut;

use crate::lay_out::Draft;

/// What [`fan_out`] added to a plan's layout.
pub(crate) struct Fanned {
    /// For each added slot, the plan slot whose values it carries. An added slot's number is the
    /// plan's slot count plus its place here.
    pub(crate) extra: Vec<u32>,
    /// The plan slots that were fanned, which no longer keep a value for several readers.
    pub(crate) slots: Vec<u32>,
    /// The added outputs, sorted by step.
    pub(crate) fans: Vec<FanOut>,
}

/// Every read of `slot` by a step after `producer`, as (step, position among its reads).
fn readers(drafts: &[Draft], producer: usize, slot: u32) -> Vec<(usize, usize)> {
    let mut found = Vec::new();
    for (step, draft) in drafts.iter().enumerate().skip(producer + 1) {
        for (at, read) in draft.reads.iter().enumerate() {
            if *read == slot {
                found.push((step, at));
            }
        }
    }
    found
}

/// Gives every reader of a work output a slot of its own, the host counted as one reader.
pub(crate) fn fan_out(stitched: &Stitched, drafts: &mut [Draft]) -> Fanned {
    let host_inputs: BTreeSet<u32> = stitched.host_inputs.iter().map(|port| port.slot).collect();
    let host_outputs: BTreeSet<u32> = stitched.host_outputs.iter().map(|port| port.slot).collect();
    let mut fanned = Fanned {
        extra: Vec::new(),
        slots: Vec::new(),
        fans: Vec::new(),
    };
    for producer in 0..drafts.len() {
        for at in 0..drafts[producer].writes.len() {
            let slot = drafts[producer].writes[at];
            if slot == NO_SLOT || host_inputs.contains(&slot) {
                continue;
            }
            let readers = readers(drafts, producer, slot);
            // The host, when it reads the output, keeps the plan's slot, where its port is.
            // Otherwise the first reader does.
            let keeps = usize::from(!host_outputs.contains(&slot));
            if readers.len() <= keeps {
                continue;
            }
            for &(step, read) in &readers[keeps..] {
                fanned.extra.push(slot);
                let own = (stitched.slots.len() + fanned.extra.len() - 1) as u32;
                drafts[step].reads[read] = own;
                drafts[producer].writes.push(own);
                fanned.fans.push(FanOut {
                    step: producer as u32,
                    from: at as u16,
                    to: (drafts[producer].writes.len() - 1) as u16,
                });
            }
            fanned.slots.push(slot);
        }
    }
    fanned
}
