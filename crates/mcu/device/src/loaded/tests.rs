//! The interpreter on a hand-built library and blob: a fan-in into one input (the first edge in
//! plan order that holds a value supplies it, the others are dropped) and a user adapter on one
//! of its edges. The blob is written here, not by the host compiler, so these tests stay in this
//! crate.

use super::{
    AdapterEntry, FORMAT_VERSION, InputEntry, Interpreter, Io, Library, LoadError, MAGIC,
    NodeEntry, StateEntry, crc32,
};
use crate::wire::Write;
use crate::{Ctx, McuError, NodeError, name_hash};

/// Type ids: `bool` is `ScalarKind` 0 and `u8` is 5.
const BOOL: u16 = 0;
const U8: u16 = 5;

/// `pass(x: bool) -> bool`, which forwards its input.
unsafe fn pass_run(_state: *mut u8, _ctx: &Ctx, io: &mut Io) -> Result<(), NodeError> {
    // SAFETY: input 0 is a required `bool` and output 0 a `bool`, as the library declares.
    unsafe {
        let value: bool = io.input(0);
        io.output(0, Some(value));
    }
    Ok(())
}

/// `u8 -> bool`: any nonzero command is `true`.
unsafe fn command_to_bool(input: *const u8, output: *mut u8) {
    // SAFETY: the interpreter passes a `u8` input and a `bool` slot, as the adapter declares.
    unsafe { output.cast::<bool>().write(input.read() != 0) }
}

static NODES: [NodeEntry; 1] = [NodeEntry {
    inputs: &[InputEntry {
        ty: BOOL,
        optional: false,
    }],
    outputs: &[BOOL],
    state: StateEntry::of::<()>(),
    run: pass_run,
}];

static ADAPTERS: [AdapterEntry; 1] = [AdapterEntry {
    input: U8,
    output: BOOL,
    run: command_to_bool,
}];

pub(super) static LIBRARY: Library = Library {
    hash: 0x51ab,
    types: &[],
    nodes: &NODES,
    adapters: &ADAPTERS,
};

/// Postcard-style writer for the test blob.
struct Blob(Vec<u8>);

impl Write for Blob {
    fn put(&mut self, bytes: &[u8]) {
        self.0.extend_from_slice(bytes);
    }
}

/// One host edge into the fan-in: `port` of the host inputs, `convert` the adapter index (or
/// none). Capacity 4, overflow error.
fn host_edge(w: &mut Blob, port: u64, convert: Option<u64>) {
    w.put(&[0]);
    w.varint(port);
    w.varint(4);
    w.put(&[2]);
    match convert {
        None => w.put(&[0]),
        Some(index) => {
            w.put(&[1]);
            w.varint(index);
        }
    }
}

/// A plan with host inputs `a: bool` and `b: u8`, one `pass` node whose input is a fan-in of
/// `edges` (host port, adapter) and whose output goes to the host output `out: bool`.
fn fan_in_blob(fan: u64, edges: &[(u64, Option<u64>)]) -> Vec<u8> {
    let mut w = Blob(Vec::new());
    w.put(&MAGIC);
    w.put(&[FORMAT_VERSION]);
    w.varint(LIBRARY.hash);
    w.varint(7);
    w.varint(1);
    w.put(&[0]); // one node, library entry 0
    w.varint(2);
    w.varint(u64::from(name_hash("a")));
    w.varint(u64::from(BOOL));
    w.varint(u64::from(name_hash("b")));
    w.varint(u64::from(U8));
    w.varint(1);
    w.varint(u64::from(name_hash("out")));
    w.varint(u64::from(BOOL));
    w.varint(1); // nodes
    w.put(&[0]); // not fire mode `all`
    w.varint(1); // one input
    w.put(&[4]); // fan-in
    w.varint(fan);
    for &(port, convert) in edges {
        host_edge(&mut w, port, convert);
    }
    // The output edge: from node 0's output 0, no conversion.
    w.varint(1);
    w.put(&[1]);
    w.varint(0);
    w.put(&[0]);
    w.varint(4);
    w.put(&[2]);
    w.put(&[0]);
    let crc = crc32(&w.0);
    w.0.extend_from_slice(&crc.to_le_bytes());
    w.0
}

pub(super) fn plan() -> Vec<u8> {
    fan_in_blob(2, &[(0, None), (1, Some(0))])
}

#[test]
fn fan_in_takes_the_first_edge_with_a_value_and_drops_the_rest() {
    let blob = plan();
    let mut interp = Box::new(Interpreter::<256>::new(&LIBRARY));
    interp.load(&blob).unwrap();
    let (a, b) = (interp.input("a").unwrap(), interp.input("b").unwrap());
    let out = interp.output("out").unwrap();

    // Only `b` holds a value: the adapter turns the command into `bool`.
    interp.push(b, 7u8).unwrap();
    interp.tick().unwrap();
    assert_eq!(interp.pop::<bool>(out), Ok(Some(true)));

    // `a` is the first edge in plan order: its value wins over `b`'s.
    interp.push(a, false).unwrap();
    interp.push(b, 7u8).unwrap();
    interp.tick().unwrap();
    assert_eq!(interp.pop::<bool>(out), Ok(Some(false)));

    // Two values on `a` in one tick: the oldest one is read, the other is dropped.
    interp.push(a, true).unwrap();
    interp.push(a, false).unwrap();
    interp.tick().unwrap();
    assert_eq!(interp.pop::<bool>(out), Ok(Some(true)));
    interp.tick().unwrap();
    assert_eq!(interp.pop::<bool>(out), Ok(None));

    // A zero command through the adapter is `false`; nothing pushed runs nothing.
    interp.push(b, 0u8).unwrap();
    interp.tick().unwrap();
    assert_eq!(interp.pop::<bool>(out), Ok(Some(false)));
    interp.tick().unwrap();
    assert_eq!(interp.pop::<bool>(out), Ok(None));
    // The host port has the type it was declared with.
    assert_eq!(interp.push(a, 1u8), Err(McuError::Port { port: a }));
}

#[test]
fn rejects_blobs_the_fan_in_and_adapter_rules_forbid() {
    let mut interp = Interpreter::<256>::new(&LIBRARY);
    // Another version is refused before anything else (a version 2 blob is the old layout).
    let mut old = plan();
    old[4] = 2;
    assert_eq!(interp.load(&old), Err(LoadError::Version { found: 2 }));
    // A fan-in needs at least two edges.
    assert_eq!(
        interp.load(&fan_in_blob(1, &[(0, None)])),
        Err(LoadError::Input { node: 0, input: 0 })
    );
    // The adapter index must name a library adapter.
    assert_eq!(
        interp.load(&fan_in_blob(2, &[(0, None), (1, Some(3))])),
        Err(LoadError::Edge { edge: 1 })
    );
    // An adapter converts its own input type only: `a` is a `bool`.
    assert_eq!(
        interp.load(&fan_in_blob(2, &[(0, Some(0)), (1, None)])),
        Err(LoadError::Type { edge: 0 })
    );
    // A plan without an adapter on a `u8` edge into a `bool` input does not widen either.
    assert_eq!(
        interp.load(&fan_in_blob(2, &[(0, None), (1, None)])),
        Err(LoadError::Type { edge: 1 })
    );
    // The rejected loads left no plan running; a good one loads.
    assert!(interp.plan_hash().is_none());
    interp.load(&plan()).unwrap();
    assert_eq!(interp.plan_hash(), Some(7));
}
