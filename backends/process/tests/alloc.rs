//! A steady-state run allocates nothing on the host's thread once it has run once.

mod support;

use daedalus_abi::counting::{CountingAlloc, counted};
use daedalus_abi::testing::{release, run_once};
use daedalus_abi::{Slot, SlotState, Slots, StepCx, StepEnd};
use daedalus_backend::{
    Backend, BodyIndex, CompileCx, Compiled, NO_METADATA, NodeBody, PortRef, RateId, Region,
};
use daedalus_backend_process::ProcessBackend;
use daedalus_types::{BackendIndex, NodeIndex, PortIndex};
use daedalus_worker_conformance::runner::registry;

#[global_allocator]
static ALLOCATOR: CountingAlloc = CountingAlloc;

#[test]
fn a_steady_state_run_allocates_nothing_on_the_host_thread() {
    let mut backend = ProcessBackend::new();
    backend
        .add_worker(support::spec(&[]), &registry())
        .expect("registers");
    let inputs = [
        PortRef {
            node: NodeIndex(0),
            port: PortIndex(0),
        },
        PortRef {
            node: NodeIndex(0),
            port: PortIndex(1),
        },
    ];
    let outputs = [PortRef {
        node: NodeIndex(0),
        port: PortIndex(0),
    }];
    let nodes = [NodeIndex(0)];
    let region = Region {
        backend: BackendIndex(0),
        nodes: &nodes,
        inputs: &inputs,
        outputs: &outputs,
        incoming: &[],
        placed: &[],
        rate: RateId(0),
    };
    let bodies = [NodeBody {
        node: NodeIndex(0),
        id: "fixture.scalar_add",
        body: BodyIndex(0),
        inputs: 2,
        outputs: 1,
        merges: false,
        fire_all: false,
        label: None,
        metadata: &NO_METADATA,
        code: &(),
        text: "fixture.scalar_add",
    }];
    let cx = CompileCx {
        types: &[],
        consts: &[],
        spaces: &[],
        bodies: &bodies,
        edges: &[],
        host_inputs: &[],
        host_outputs: &[],
        fifo_capacity: 64,
        merge_allowed: false,
        freeze_params: false,
        sharing: &[],
    };
    let mut step = match backend.compile(&region, &cx) {
        Ok(Compiled::Steps(mut units)) => units.pop().expect("a unit").step,
        other => panic!("compile: {other:?}"),
    };
    let mut a = 2i64.to_le_bytes();
    let mut b = 40i64.to_le_bytes();
    let mut cells = [
        Slot {
            state: SlotState::Full as u32,
            owner: 0,
            data: a.as_mut_ptr(),
            len: 8,
            cap: 8,
            ..Slot::EMPTY
        },
        Slot {
            state: SlotState::Full as u32,
            owner: 0,
            data: b.as_mut_ptr(),
            len: 8,
            cap: 8,
            ..Slot::EMPTY
        },
        Slot::EMPTY,
    ];
    let mut cx_step = StepCx::new();
    // The first run sizes every buffer. Later runs reuse them.
    assert!(matches!(
        run_once(&mut step, &mut Slots::of(&mut cells), &mut cx_step),
        StepEnd::Done
    ));
    let (ends, allocations) = counted(|| {
        let mut all_done = true;
        for _ in 0..200 {
            let end = run_once(&mut step, &mut Slots::of(&mut cells), &mut cx_step);
            all_done &= matches!(end, StepEnd::Done);
        }
        all_done
    });
    assert!(ends, "every steady run is done");
    assert_eq!(
        allocations, 0,
        "a steady run allocates nothing on the host thread"
    );
    release(step);
}

/// A megabyte handed to a worker and back (reversed, by this fixture's node) is a value too long to sit in a cell, so it is handed
/// on as shared bytes. Once its consumer has let the last one go, the same memory is used for
/// the next: a steady run of large values allocates nothing on the host's thread either.
#[test]
fn a_steady_run_of_megabyte_values_allocates_nothing_on_the_host_thread() {
    const LEN: usize = 1 << 20;
    let mut backend = ProcessBackend::new();
    backend
        .add_worker(support::spec(&[]), &registry())
        .expect("registers");
    let port = [PortRef {
        node: NodeIndex(0),
        port: PortIndex(0),
    }];
    let nodes = [NodeIndex(0)];
    let region = Region {
        backend: BackendIndex(0),
        nodes: &nodes,
        inputs: &port,
        outputs: &port,
        incoming: &[],
        placed: &[],
        rate: RateId(0),
    };
    let bodies = [NodeBody {
        node: NodeIndex(0),
        id: "fixture.raw_io",
        body: BodyIndex(0),
        inputs: 1,
        outputs: 1,
        merges: false,
        fire_all: false,
        label: None,
        metadata: &NO_METADATA,
        code: &(),
        text: "fixture.raw_io",
    }];
    let cx = CompileCx {
        types: &[],
        consts: &[],
        spaces: &[],
        bodies: &bodies,
        edges: &[],
        host_inputs: &[],
        host_outputs: &[],
        fifo_capacity: 64,
        merge_allowed: false,
        freeze_params: false,
        sharing: &[],
    };
    let mut step = match backend.compile(&region, &cx) {
        Ok(Compiled::Steps(mut units)) => units.pop().expect("a unit").step,
        other => panic!("compile: {other:?}"),
    };
    let mut payload: Vec<u8> = (0..LEN).map(|at| (at * 31 + 7) as u8).collect();
    let mut cells = [
        Slot {
            state: SlotState::Full as u32,
            owner: 0,
            data: payload.as_mut_ptr(),
            len: LEN,
            cap: LEN,
            ..Slot::EMPTY
        },
        Slot::EMPTY,
    ];
    let mut cx_step = StepCx::new();
    let mut run = |cells: &mut [Slot; 2]| {
        // The value handed back last time is let go, as its consumer would have.
        daedalus_runner::drop_owned(core::mem::replace(&mut cells[1], Slot::EMPTY));
        let end = run_once(&mut step, &mut Slots::of(cells), &mut cx_step);
        matches!(end, StepEnd::Done) && cells[1].len == LEN
    };
    // The first runs size every buffer, the worker's and the host's.
    for _ in 0..4 {
        assert!(run(&mut cells));
    }
    // The fixture's node gives its payload back reversed.
    let reversed: Vec<u8> = payload.iter().rev().copied().collect();
    let (all_done, allocations) = counted(|| (0..16).all(|_| run(&mut cells)));
    assert!(all_done);
    assert_eq!(
        cells[1].bytes().map(|bytes| bytes == reversed.as_slice()),
        Some(true),
        "the whole payload came back"
    );
    assert_eq!(
        allocations, 0,
        "sixteen megabytes through a worker allocate nothing on the host's thread"
    );
    // A consumer that keeps a value keeps its memory: the next one gets memory of its own.
    let held = cells[1];
    cells[1] = Slot::EMPTY;
    assert!(run(&mut cells));
    assert_ne!(held.data, cells[1].data, "a held value is not written over");
    assert_eq!(
        held.bytes().map(|bytes| bytes == reversed.as_slice()),
        Some(true)
    );
    daedalus_runner::drop_owned(held);
    daedalus_runner::drop_owned(cells[1]);
    release(step);
}
