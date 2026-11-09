//! `host.in -> inc -> host.out` through a [`Host`] and its serial runner, with no standard library
//! and no operating system clock. `scripts/ci.d/nostd.sh` checks this for chip targets, and its
//! tests run the same code natively.

#![no_std]
#![forbid(unsafe_code)]

extern crate alloc;

use alloc::vec;
use alloc::vec::Vec;

use daedalus_abi::{OwnedStep, Slot, Slots, Step, StepCx, StepEnd};
use daedalus_host::{Dir, Host, HostError, PortSpec, Ports};
use daedalus_plan::Span;
use daedalus_runner::{
    Fire, INLINE_BYTES, InlineCell, Memory, Pending, Plan, Queue, Recovery, Runner, SlotKind,
    SlotSpec, StepHealth, StepMeta, read_inline, write_inline,
};
use daedalus_types::{SpaceIndex, TypeIndex, TypeKey};
use daedalus_value::{InlineBytes, Owner, Value};

const TY: TypeIndex = TypeIndex(0);
const SPACE: SpaceIndex = SpaceIndex(0);
static KEY: TypeKey = TypeKey::new("i64");
static TYPES: [TypeKey; 1] = [TypeKey::new("i64")];
static PORTS: [PortSpec<'static>; 2] = [
    PortSpec {
        name: "in",
        dir: Dir::Input,
        slot: 0,
        ty: TY,
        space: SPACE,
        key: &KEY,
        wake: &[0],
    },
    PortSpec {
        name: "out",
        dir: Dir::Output,
        slot: 1,
        ty: TY,
        space: SPACE,
        key: &KEY,
        wake: &[],
    },
];

/// Adds one to its input.
struct Inc {
    out: InlineCell,
}

impl OwnedStep for Inc {
    fn run(&mut self, slots: &mut Slots, _: &mut StepCx) -> StepEnd {
        let Some(x) = slots.cell(0).and_then(|cell| read_inline::<i64>(cell)) else {
            return StepEnd::Fault;
        };
        let Some(cell) = slots.cell(1) else {
            return StepEnd::Fault;
        };
        write_inline(&(x + 1), &mut self.out, cell);
        StepEnd::Done
    }
}

/// The memory of one graph, all of it made once.
pub struct Smoke {
    steps: Vec<Step>,
    meta: Vec<StepMeta>,
    table: Vec<u32>,
    specs: Vec<SlotSpec>,
    cells: Vec<Slot>,
    queues: Vec<Queue>,
    storage: Vec<Slot>,
    dirty: Vec<u32>,
    carry: Vec<u32>,
    always: Vec<u32>,
    pending: Vec<Pending>,
    health: Vec<StepHealth>,
    activity: Vec<u32>,
    inline: Vec<InlineCell>,
}

impl Default for Smoke {
    fn default() -> Self {
        Self::new()
    }
}

impl Smoke {
    pub fn new() -> Smoke {
        let span = |start, len| Span { start, len };
        let queue = SlotSpec {
            cap: 2,
            kind: SlotKind::Fifo,
        };
        Smoke {
            steps: vec![Step::boxed(Inc {
                out: [0; INLINE_BYTES],
            })],
            meta: vec![StepMeta {
                wake: span(0, 0),
                inputs: span(0, 1),
                outputs: span(1, 1),
                always: false,
                may_pend: false,
                recovery: Recovery::Fail,
                fire: Fire::Any,
                required: span(0, 0),
                reuse: false,
            }],
            table: vec![0, 1],
            specs: vec![queue, queue],
            cells: vec![Slot::EMPTY; 2],
            queues: vec![Queue::default(); 2],
            storage: vec![Slot::EMPTY; 4],
            dirty: vec![0],
            carry: vec![0],
            always: vec![0],
            pending: Vec::new(),
            health: vec![StepHealth::default()],
            activity: vec![0],
            inline: vec![[0; INLINE_BYTES]; 4],
        }
    }

    /// A host over this memory.
    ///
    /// # Panics
    ///
    /// If the fixed tables above stop agreeing with each other.
    pub fn host(&mut self) -> Host<'_> {
        let runner = Runner::load(
            Plan {
                steps: &mut self.steps,
                meta: &self.meta,
                table: &self.table,
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
        .expect("the tables agree");
        let ports = Ports::new(&PORTS).expect("the ports are sorted");
        Host::new(runner, ports, &TYPES)
    }
}

/// Runs one tick with `x` and the time `clock` gives, and returns the output.
#[allow(clippy::result_large_err)] // The host's own error, passed straight through.
pub fn increment(
    host: &mut Host<'_>,
    x: i64,
    clock: &mut impl FnMut() -> u64,
) -> Result<Option<i64>, HostError> {
    let owner = InlineBytes::from_slice(&x.to_le_bytes()).map_or(Owner::Static(&[]), Owner::Inline);
    host.feed(
        "in",
        Value {
            ty: TY,
            space: SPACE,
            owner,
        },
    )?;
    host.set_clock(clock());
    host.step()?;
    Ok(host
        .take_value("out")?
        .and_then(|value| <[u8; 8]>::try_from(value.bytes()?).ok())
        .map(i64::from_le_bytes))
}
