//! A dylib plugin that carries the fake NPU backend and three nodes. A host loads it through
//! the loader like any other plugin: it exports the one entry symbol and nothing else.

#![forbid(unsafe_code)]

use std::cell::RefCell;

use daedalus_abi::{Slots, StepCx, StepEnd};
use daedalus_fake_npu::{BACKEND_ID, run_op};
use daedalus_loader::export::{NodeDef, PluginDef, PortDef};
use daedalus_runner::INLINE_BYTES;

pub const PLUGIN_NAME: &str = "fake-npu-plugin";

thread_local! {
    /// Where a node's output bytes are kept until the runner has taken them. A plugin's step
    /// is a function with no data of its own, and one step's output is taken before the next
    /// step runs.
    static OUT: RefCell<[u8; INLINE_BYTES]> = const { RefCell::new([0; INLINE_BYTES]) };
}

fn run(op: &str, slots: &mut Slots) -> StepEnd {
    OUT.with(|out| run_op(op, slots, &mut out.borrow_mut()))
}

fn scale(slots: &mut Slots, _: &mut StepCx) -> StepEnd {
    run("scale2", slots)
}

fn add(slots: &mut Slots, _: &mut StepCx) -> StepEnd {
    run("add1", slots)
}

fn negate(slots: &mut Slots, _: &mut StepCx) -> StepEnd {
    run("negate", slots)
}

static IN: [PortDef; 1] = [PortDef {
    name: "x",
    key: "u32",
    optional: false,
}];
static OUT_PORT: [PortDef; 1] = [PortDef {
    name: "y",
    key: "u32",
    optional: false,
}];

const fn node(
    id: &'static str,
    text: &'static str,
    run: fn(&mut Slots, &mut StepCx) -> StepEnd,
) -> NodeDef {
    NodeDef {
        id,
        source: PLUGIN_NAME,
        backend: BACKEND_ID,
        text,
        inputs: &IN,
        outputs: &OUT_PORT,
        run,
    }
}

static NODES: [NodeDef; 3] = [
    node("npu.scale", "scale2", scale),
    node("npu.add", "add1", add),
    node("npu.negate", "negate", negate),
];

daedalus_loader::export_plugin!(PluginDef {
    name: PLUGIN_NAME,
    backends: &[BACKEND_ID],
    nodes: &NODES,
});
