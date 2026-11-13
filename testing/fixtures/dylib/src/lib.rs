//! A plugin the loader tests open. `pass` forwards its input cell to its output cell. `boom`
//! panics, to prove a panic in a node is a fault. `abort` ends the process.

use std::sync::atomic::{AtomicU32, Ordering};

use daedalus_abi::{
    BytesC, Fence, FencePoll, FenceWord, HANDOVER_PENDING, OWNER_HANDED, PlainData, Slot, SlotState,
    Slots, StepCx, StepEnd,
};
use daedalus_derive::PlainData;
use daedalus_loader::export::{CodecDef, NodeDef, PluginDef, PortDef};

/// A plain-data type the fixture's codec converts.
#[repr(C)]
#[derive(Clone, Copy, PlainData)]
#[plain(key = "dylib.number")]
struct Number {
    value: u32,
}


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

fn boom(_slots: &mut Slots, cx: &mut StepCx) -> StepEnd {
    // More than the reason holds goes to the host's detail, when it keeps one.
    cx.detail("boom: what led up to it, ");
    cx.detail_fmt(format_args!("in {} pieces", 2));
    panic!("boom: the node panicked on purpose");
}

/// Takes the whole process down, as a crashing plugin would.
fn abort(_slots: &mut Slots, _cx: &mut StepCx) -> StepEnd {
    std::process::abort();
}

/// The megabyte the handover node hands out. It is made once, and every hand-over points at it, so
/// a tick costs the hand-over alone.
static HANDED: std::sync::OnceLock<Box<[u8]>> = std::sync::OnceLock::new();

/// Hands a megabyte over without a copy. Output cell 1 is the hand-over: the bytes are described in
/// `cx.handover[0]`, and the loader turns the cell into a record once the step returns.
fn handover(slots: &mut Slots, cx: &mut StepCx) -> StepEnd {
    let Some(out) = slots.cell(1) else {
        return StepEnd::Fault;
    };
    let bytes = HANDED.get_or_init(|| vec![7u8; 1 << 20].into_boxed_slice());
    let (ptr, len) = (bytes.as_ptr(), bytes.len());
    cx.handover[0] = BytesC {
        ptr,
        len,
        data: std::ptr::null_mut(),
        release: Some(release_handed),
    };
    *out = Slot {
        state: SlotState::Full as u32,
        owner: OWNER_HANDED,
        reserved: HANDOVER_PENDING,
        data: ptr.cast_mut(),
        len,
        ..Slot::EMPTY
    };
    StepEnd::Done
}

/// Called once when the last value of a hand-over is dropped. The megabyte is static, so there is
/// nothing to free: the release is where a plugin would free its own bytes.
///
/// # Safety
/// Takes no pointer it reads.
unsafe extern "C" fn release_handed(_data: *mut ()) {}

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

static HANDED_OUT: [PortDef; 1] = [PortDef {
    name: "out",
    key: "bytes",
    optional: false,
}];

static NODES: [NodeDef; 5] = [
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
        id: "fixture.handover",
        source: "dylib-fixture",
        backend: "fixture.cpu",
        text: "handover",
        inputs: &PASS_IN,
        outputs: &HANDED_OUT,
        run: handover,
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

/// Plugin bytes to the host value: one more than the number's value.
fn number_to_value(input: &[u8]) -> Option<Vec<u8>> {
    let value = u32::from_ne_bytes(input.try_into().ok()?);
    Some(value.checked_add(1)?.to_ne_bytes().to_vec())
}

/// The host value to plugin bytes. A value of zero has no number, so it is refused.
fn value_to_number(input: &[u8]) -> Option<Vec<u8>> {
    let value = u32::from_ne_bytes(input.try_into().ok()?);
    Some(value.checked_sub(1)?.to_ne_bytes().to_vec())
}

/// Plugin bytes to the wire form, big-endian.
fn number_to_wire(input: &[u8]) -> Option<Vec<u8>> {
    let value = u32::from_ne_bytes(input.try_into().ok()?);
    Some(value.to_be_bytes().to_vec())
}

/// The wire form to plugin bytes.
fn wire_to_number(input: &[u8]) -> Option<Vec<u8>> {
    let value = u32::from_be_bytes(input.try_into().ok()?);
    Some(value.to_ne_bytes().to_vec())
}

static CODECS: [CodecDef; 1] = [CodecDef {
    key: Number::KEY,
    value_key: "u32",
    wire_key: "bytes",
    backend: "fixture.cpu",
    layout: Number::LAYOUT,
    cost: 1,
    lossy: false,
    to_value: number_to_value,
    from_value: value_to_number,
    to_bytes: number_to_wire,
    from_bytes: wire_to_number,
}];

/// The bytes the fixture hands over without a copy: one megabyte, for index 0. Other indexes make none.
fn make_bytes(index: u32) -> Option<Vec<u8>> {
    (index == 0).then(|| vec![7u8; 1 << 20])
}

daedalus_loader::export_plugin!(PluginDef {
    name: "dylib-fixture",
    backends: &["fixture.cpu"],
    nodes: &NODES,
    codecs: &CODECS,
    produce: Some(make_bytes),
});
