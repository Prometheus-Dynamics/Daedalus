//! A plugin the loader tests open. `pass` forwards its input cell to its output cell. `boom`
//! panics, to prove a panic in a node is a fault. `abort` ends the process.

use std::sync::atomic::{AtomicU32, Ordering};

use daedalus_abi::{Fence, FencePoll, FenceWord, Slot, SlotState, Slots, StepCx, StepEnd};
use daedalus_loader::export::{NodeDef, PluginDef, PortDef};

/// How many polls the gated node's fence waits for before it is ready. The runner polls once a
/// tick, so its output appears on the fourth tick after the node ran.
const POLLS_TO_READY: u32 = 3;
static POLLS: AtomicU32 = AtomicU32::new(0);

/// The fence `gated` returns: ready once it has been polled enough times.
struct Polls;

impl FenceWord for Polls {
    fn poll(_: usize) -> FencePoll {
        if POLLS.fetch_add(1, Ordering::SeqCst) + 1 >= POLLS_TO_READY {
            FencePoll::Ready
        } else {
            FencePoll::Pending
        }
    }
}

/// Forwards its input, as `pass` does, but its work is still running on a device: it returns
/// `Pending` until the fence has been polled enough times.
fn gated(slots: &mut Slots, cx: &mut StepCx) -> StepEnd {
    POLLS.store(0, Ordering::SeqCst);
    match pass(slots, cx) {
        StepEnd::Done => StepEnd::Pending(Fence::of::<Polls>(0)),
        other => other,
    }
}

fn pass(slots: &mut Slots, _cx: &mut StepCx) -> StepEnd {
    let Some(input) = slots.cell(0).map(|cell| *cell) else {
        return StepEnd::Fault;
    };
    match slots.cell(1) {
        Some(output) => {
            *output = Slot {
                state: SlotState::Full as u32,
                ..input
            };
            StepEnd::Done
        }
        None => StepEnd::Fault,
    }
}

fn boom(_slots: &mut Slots, _cx: &mut StepCx) -> StepEnd {
    panic!("boom: the node panicked on purpose");
}

/// Takes the whole process down, as a crashing plugin would.
fn abort(_slots: &mut Slots, _cx: &mut StepCx) -> StepEnd {
    std::process::abort();
}

static PASS_IN: [PortDef; 1] = [PortDef {
    name: "in",
    key: "u32",
    optional: false,
}];
static PASS_OUT: [PortDef; 1] = [PortDef {
    name: "out",
    key: "u32",
    optional: false,
}];

static NODES: [NodeDef; 4] = [
    NodeDef {
        id: "fixture.pass",
        source: "dylib-fixture",
        backend: "fixture.cpu",
        text: "pass",
        inputs: &PASS_IN,
        outputs: &PASS_OUT,
        run: pass,
    },
    NodeDef {
        id: "fixture.boom",
        source: "dylib-fixture",
        backend: "fixture.cpu",
        text: "boom",
        inputs: &PASS_IN,
        outputs: &PASS_OUT,
        run: boom,
    },
    NodeDef {
        id: "fixture.abort",
        source: "dylib-fixture",
        backend: "fixture.cpu",
        text: "abort",
        inputs: &PASS_IN,
        outputs: &PASS_OUT,
        run: abort,
    },
    NodeDef {
        id: "fixture.gated",
        source: "dylib-fixture",
        backend: "fixture.cpu",
        text: "gated",
        inputs: &PASS_IN,
        outputs: &PASS_OUT,
        run: gated,
    },
];

daedalus_loader::export_plugin!(PluginDef {
    name: "dylib-fixture",
    backends: &["fixture.cpu"],
    nodes: &NODES,
});
