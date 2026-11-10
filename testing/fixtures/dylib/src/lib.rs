//! A plugin the loader tests open. `pass` forwards its input cell to its output cell. `boom`
//! panics, to prove a panic in a node is a fault. `abort` ends the process.

use daedalus_abi::{Slot, SlotState, Slots, StepCx, StepEnd};
use daedalus_loader::export::{NodeDef, PluginDef, PortDef};

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

static NODES: [NodeDef; 3] = [
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
];

daedalus_loader::export_plugin!(PluginDef {
    name: "dylib-fixture",
    backends: &["fixture.cpu"],
    nodes: &NODES,
});
