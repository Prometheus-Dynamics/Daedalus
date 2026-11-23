//! A dylib plugin that carries the fake NPU backend, its three nodes, and two that only hand a
//! value on: one of a type the plugin knows by key alone, one of another plugin's plain-data type. A host loads it through
//! the loader like any other plugin: it exports the one entry symbol and nothing else.

#![forbid(unsafe_code)]

use std::cell::RefCell;

use daedalus_abi::{Slots, StepCx, StepEnd};
use daedalus_fake_npu::{BACKEND_ID, run_op};
use daedalus_loader::export::{NodeDef, PluginDef, PortDef};
use daedalus_runner::INLINE_BYTES;

pub(crate) const PLUGIN_NAME: &str = "fake-npu-plugin";

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

/// Hands its input on by reference, whatever it is.
fn hand_on(slots: &mut Slots, _: &mut StepCx) -> StepEnd {
    match slots.cell(1) {
        Some(output) => {
            daedalus_abi::forward(output, 0);
            StepEnd::Done
        }
        None => StepEnd::Fault,
    }
}

const fn port(name: &'static str, key: &'static str) -> [PortDef; 1] {
    [PortDef {
        name,
        key,
        optional: false,
    }]
}

/// A type only its key says anything about: a host's own.
static KEPT_IN: [PortDef; 1] = port("x", "host.kept");
static KEPT_OUT: [PortDef; 1] = port("y", "host.kept");
/// The dylib fixture's plain-data type, which that plugin's codec converts.
static NUMBER_IN: [PortDef; 1] = port("x", "dylib.number");
static NUMBER_OUT: [PortDef; 1] = port("y", "dylib.number");

const fn handing(
    id: &'static str,
    inputs: &'static [PortDef],
    outputs: &'static [PortDef],
) -> NodeDef {
    NodeDef {
        id,
        source: PLUGIN_NAME,
        backend: BACKEND_ID,
        text: "hand_on",
        inputs,
        outputs,
        run: hand_on,
    }
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

static NODES: [NodeDef; 5] = [
    node("npu.scale", "scale2", scale),
    node("npu.add", "add1", add),
    node("npu.negate", "negate", negate),
    handing("npu.keep", &KEPT_IN, &KEPT_OUT),
    handing("npu.number", &NUMBER_IN, &NUMBER_OUT),
];

daedalus_loader::export_plugin!(PluginDef {
    name: PLUGIN_NAME,
    backends: &[BACKEND_ID],
    nodes: &NODES,
    made: &[],
    codecs: &[],
    produce: None,
});
