#![allow(dead_code)] // Each test binary uses part of this module.

//! Fixtures shared by the runner tests: buffers for a plan, and small step bodies.

use daedalus_abi::{Slot, SlotState, Slots, StepCx, StepEnd, StepWord};
use daedalus_plan::Span;
use daedalus_runner::{
    Fire, Memory, Pending, Plan, Queue, Recovery, Runner, SlotSpec, StepHealth, StepMeta,
};

/// Builds the flat table and the per-step records.
#[derive(Default)]
pub struct Wiring {
    pub table: Vec<u32>,
    pub meta: Vec<StepMeta>,
}

impl Wiring {
    fn span(&mut self, values: &[u32]) -> Span {
        let start = self.table.len() as u32;
        self.table.extend_from_slice(values);
        Span {
            start,
            len: values.len() as u32,
        }
    }

    pub fn step(&mut self, wake: &[u32], inputs: &[u32], outputs: &[u32]) {
        self.record(wake, inputs, outputs, false);
    }

    /// A step that may return `Pending`, so it gets a pending entry.
    pub fn pending_step(&mut self, wake: &[u32], inputs: &[u32], outputs: &[u32]) {
        self.record(wake, inputs, outputs, true);
    }

    /// Sets how `step` fires, and the input slots it cannot run without.
    pub fn set_join(&mut self, step: usize, fire: Fire, required: &[u32]) {
        self.meta[step].required = self.span(required);
        self.meta[step].fire = fire;
    }

    /// Opts `step` into output reuse.
    pub fn set_reuse(&mut self, step: usize) {
        self.meta[step].reuse = true;
    }

    /// Sets what a fault of `step` does.
    pub fn set_recovery(&mut self, step: usize, recovery: Recovery) {
        self.meta[step].recovery = recovery;
    }

    fn record(&mut self, wake: &[u32], inputs: &[u32], outputs: &[u32], may_pend: bool) {
        let wake = self.span(wake);
        let inputs = self.span(inputs);
        let outputs = self.span(outputs);
        let required = self.span(&[]);
        self.meta.push(StepMeta {
            wake,
            inputs,
            outputs,
            always: false,
            may_pend,
            recovery: Recovery::Fail,
            fire: Fire::Any,
            required,
            reuse: false,
        });
    }
}

/// Owns every buffer a runner borrows.
pub struct Fixture {
    pub steps: Vec<daedalus_abi::Step>,
    pub wiring: Wiring,
    pub specs: Vec<SlotSpec>,
    pub cells: Vec<Slot>,
    pub queues: Vec<Queue>,
    pub storage: Vec<Slot>,
    pub dirty: Vec<u32>,
    pub carry: Vec<u32>,
    pub always: Vec<u32>,
    pub pending: Vec<Pending>,
    pub health: Vec<StepHealth>,
    pub activity: Vec<u32>,
    pub inline: Vec<[u8; 16]>,
}

impl Fixture {
    pub fn new(steps: Vec<daedalus_abi::Step>, wiring: Wiring, specs: Vec<SlotSpec>) -> Fixture {
        let words = steps.len().div_ceil(32);
        let storage_len: u32 = specs.iter().map(|spec| spec.cap).sum();
        let may_pend = wiring.meta.iter().filter(|meta| meta.may_pend).count();
        let widest = wiring
            .meta
            .iter()
            .map(|meta| (meta.inputs.len + meta.outputs.len) as usize)
            .max()
            .unwrap_or(0);
        Fixture {
            cells: vec![Slot::EMPTY; widest],
            queues: vec![Queue::default(); specs.len()],
            storage: vec![Slot::EMPTY; storage_len as usize],
            dirty: vec![0; words],
            carry: vec![0; words],
            always: vec![0; words],
            pending: vec![Pending::EMPTY; may_pend],
            health: vec![StepHealth::default(); steps.len()],
            activity: vec![0; words],
            inline: vec![[0; 16]; storage_len as usize],
            steps,
            wiring,
            specs,
        }
    }

    pub fn load(&mut self) -> Result<Runner<'_>, daedalus_runner::LoadError> {
        Runner::load(
            Plan {
                steps: &mut self.steps,
                meta: &self.wiring.meta,
                table: &self.wiring.table,
                slots: &self.specs,
            },
            Memory {
                cells: &mut self.cells,
                queues: &mut self.queues,
                storage: &mut self.storage,
                dirty: &mut self.dirty,
                carry: &mut self.carry,
                always: &mut self.always,
                pending: &mut self.pending,
                health: &mut self.health,
                activity: &mut self.activity,
                inline: &mut self.inline,
            },
        )
    }
}

/// A value of `len` as a tag, so a test can see which value a body received.
pub fn tagged(len: usize) -> Slot {
    Slot {
        state: SlotState::Full as u32,
        len,
        ..Slot::EMPTY
    }
}

/// The cell at `index` of the step's frame: its inputs, then its outputs.
pub fn cell(slots: &mut Slots, index: usize) -> &mut Slot {
    slots.cell(index).expect("slot in range")
}

/// Reads the step's one input and, when `word` is not zero, writes its one output.
pub struct Link;

impl StepWord for Link {
    fn run(word: usize, slots: &mut Slots, _: &mut StepCx) -> StepEnd {
        let value = *cell(slots, 0);
        if word != 0 {
            *cell(slots, 1) = value;
        }
        StepEnd::Done
    }
}

/// The word that makes [`Link`] write its output.
pub fn link_word(writes: bool) -> usize {
    usize::from(writes)
}

/// Adds a chain of `len` steps: step `i` reads slot `i` and writes slot `i + 1`, and the last
/// step reads and writes nothing more. Returns the wiring and the step words.
pub fn chain(len: usize) -> (Wiring, Vec<usize>) {
    let mut wiring = Wiring::default();
    let mut words = Vec::new();
    for index in 0..len {
        let last = index + 1 == len;
        let wake: Vec<u32> = if last { vec![] } else { vec![index as u32 + 1] };
        let output: Option<usize> = if last { None } else { Some(index + 1) };
        let outputs: Vec<u32> = output.map(|o| vec![o as u32]).unwrap_or_default();
        wiring.step(&wake, &[index as u32], &outputs);
        words.push(link_word(output.is_some()));
    }
    (wiring, words)
}
