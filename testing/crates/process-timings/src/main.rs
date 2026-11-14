//! The process backend's one-node round trip, which the G4 board run records. It starts the worker
//! binary it is given, compiles the `fixture.scalar_add` node as a one-node region, and times one
//! step run per operation: the host sends the two `i64` inputs, the worker adds them and sends the
//! sum back. Run by hand, not in the gate:
//! `process-timings WORKER [BATCHES] [MEGABYTES]`, where WORKER is the `daedalus-worker-fixture`
//! binary. With MEGABYTES it also times a payload of that size handed to the worker and back.
//!
//! It warms up, then times BATCHES batches of 1,000 operations and prints the median, minimum and
//! maximum of the per-operation times, in nanoseconds. It then counts the allocations of 200 more
//! runs on this thread, which must be zero.
#![allow(clippy::print_stdout, clippy::print_stderr)] // the measurements and usage are this program's output

use std::path::PathBuf;
use std::time::Instant;

use daedalus_abi::counting::{CountingAlloc, counted};
use daedalus_abi::testing::{release, run_once};
use daedalus_abi::{Slot, SlotState, Slots, StepCx, StepEnd};
use daedalus_backend::{
    Backend, BodyIndex, CompileCx, Compiled, NO_METADATA, NodeBody, PortRef, RateId, Region,
};
use daedalus_backend_process::{ProcessBackend, WorkerSpec};
use daedalus_types::{BackendIndex, NodeIndex, PortIndex};
use daedalus_worker_conformance::runner::registry;

#[global_allocator]
static ALLOCATOR: CountingAlloc = CountingAlloc;

const PER_BATCH: usize = 1_000;
const WARM_UP: usize = 10_000;

fn usage() -> ! {
    eprintln!("usage: process-timings WORKER [BATCHES] [MEGABYTES]");
    std::process::exit(2);
}

fn median(sorted: &[f64]) -> f64 {
    sorted[sorted.len() / 2]
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let Some(worker) = args.first() else { usage() };
    let batches: usize = args
        .get(1)
        .map_or(101, |text| text.parse().expect("BATCHES is a number"));

    let mut backend = ProcessBackend::new();
    backend
        .add_worker(WorkerSpec::new(PathBuf::from(worker)), &registry())
        .expect("the worker registers its nodes");

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
    let mut run = || {
        let end = run_once(&mut step, &mut Slots::of(&mut cells), &mut cx_step);
        assert!(matches!(end, StepEnd::Done), "the worker answers every run");
    };

    // The first runs size every buffer and start the worker's own loop, so the timed runs are warm.
    for _ in 0..WARM_UP {
        run();
    }
    let mut times = Vec::with_capacity(batches);
    for _ in 0..batches {
        let start = Instant::now();
        for _ in 0..PER_BATCH {
            run();
        }
        times.push(start.elapsed().as_nanos() as f64 / PER_BATCH as f64);
    }
    times.sort_by(f64::total_cmp);
    let (_, allocations) = counted(|| {
        for _ in 0..200 {
            run();
        }
    });
    println!(
        "round_trip median_ns {:.1} min_ns {:.1} max_ns {:.1} batches {} calls_per_batch {PER_BATCH} allocations_per_200_runs {allocations}",
        median(&times),
        times[0],
        times[times.len() - 1],
        times.len()
    );
    release(step);

    // A value's bytes handed to the worker and back: `fixture.raw_io` returns its payload as it
    // is. Across a pipe that is one copy in and one out, and this is what it costs.
    if let Some(megabytes) = args.get(2) {
        let megabytes: usize = megabytes.parse().expect("MEGABYTES is a number");
        hand_over(&backend, megabytes, batches.min(31));
    }
}

/// Times a payload of `megabytes` MiB through the worker and back, one run per operation.
fn hand_over(backend: &ProcessBackend, megabytes: usize, batches: usize) {
    let inputs = [PortRef {
        node: NodeIndex(0),
        port: PortIndex(0),
    }];
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
    let len = megabytes << 20;
    let mut payload: Vec<u8> = (0..len).map(|at| (at * 31 + 7) as u8).collect();
    let mut cells = [
        Slot {
            state: SlotState::Full as u32,
            owner: 0,
            data: payload.as_mut_ptr(),
            len,
            cap: len,
            ..Slot::EMPTY
        },
        Slot::EMPTY,
    ];
    let mut cx_step = StepCx::new();
    let mut run = || {
        cells[1] = Slot::EMPTY;
        let end = run_once(&mut step, &mut Slots::of(&mut cells), &mut cx_step);
        assert!(
            matches!(end, StepEnd::Done),
            "the worker answers: {}",
            cx_step.reason()
        );
        assert_eq!(cells[1].len, len, "the whole payload comes back");
    };
    for _ in 0..8 {
        run();
    }
    // A megabyte takes long enough that one run is an operation.
    let mut times = Vec::with_capacity(batches);
    for _ in 0..batches {
        let start = Instant::now();
        run();
        times.push(start.elapsed().as_nanos() as f64);
    }
    times.sort_by(f64::total_cmp);
    let (_, allocations) = counted(|| {
        for _ in 0..8 {
            run();
        }
    });
    println!(
        "hand_over megabytes {megabytes} median_ns {:.0} min_ns {:.0} max_ns {:.0} ns_per_megabyte {:.0} runs {} allocations_per_8_runs {allocations}",
        median(&times),
        times[0],
        times[times.len() - 1],
        median(&times) / megabytes as f64,
        times.len()
    );
    release(step);
}
