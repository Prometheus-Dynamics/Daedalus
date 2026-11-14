//! The process backend's one-node round trip, which the G4 board run records. It starts the worker
//! binary it is given, compiles the `fixture.scalar_add` node as a one-node region, and times one
//! step run per operation: the host sends the two `i64` inputs, the worker adds them and sends the
//! sum back. Run by hand, not in the gate:
//! `process-timings WORKER [BATCHES] [MEGABYTES]`, where WORKER is the `daedalus-worker-fixture`
//! binary. With MEGABYTES it also times a payload of that size handed to the worker and back, and
//! the node's own work on it (the worker reverses it) timed in this process, so the transport's
//! share is the difference. It says which transport the worker got: values by reference (inputs as
//! places, outputs held where the worker wrote them), shared memory for whole packets, or plain.
//! The fixture picks by its environment: `DAEDALUS_FIXTURE_NO_REFERENCE` asks for shared memory
//! alone, `DAEDALUS_FIXTURE_PLAIN` for nothing.
//!
//! `process-timings sweep SWEEP_WORKER [RUNS]` instead finds where placing a value starts to
//! pay, with the `daedalus-sweep-worker` binary this crate builds: see [`sweep`].
//!
//! It warms up, then times BATCHES batches of 1,000 operations and prints the median, minimum and
//! maximum of the per-operation times, in nanoseconds. It then counts the allocations of 200 more
//! runs on this thread, which must be zero.
#![allow(clippy::print_stdout, clippy::print_stderr)] // the measurements and usage are this program's output

mod step;
mod sweep;

use std::path::{Path, PathBuf};
use std::time::Instant;

use daedalus_abi::counting::{CountingAlloc, counted};
use daedalus_abi::testing::{release, run_once};
use daedalus_abi::{Slot, Slots, StepCx, StepEnd};
use daedalus_backend_process::{ProcessBackend, WorkerSpec};
use daedalus_worker_conformance::runner::registry;

#[global_allocator]
static ALLOCATOR: CountingAlloc = CountingAlloc;

const PER_BATCH: usize = 1_000;
const WARM_UP: usize = 10_000;

fn usage() -> ! {
    eprintln!("usage: process-timings WORKER [BATCHES] [MEGABYTES]");
    eprintln!("       process-timings sweep SWEEP_WORKER [RUNS]");
    std::process::exit(2);
}

fn median(sorted: &[f64]) -> f64 {
    sorted[sorted.len() / 2]
}

/// The 99th percentile of sorted times: the slowest with fewer than 100 runs.
fn p99(sorted: &[f64]) -> f64 {
    sorted[(sorted.len() * 99 / 100).min(sorted.len() - 1)]
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let Some(worker) = args.first() else { usage() };
    if worker == "sweep" {
        let Some(program) = args.get(1) else { usage() };
        let runs = args
            .get(2)
            .map_or(101, |text| text.parse().expect("RUNS is a number"));
        sweep::sweep(Path::new(program), runs);
        return;
    }
    let batches: usize = args
        .get(1)
        .map_or(101, |text| text.parse().expect("BATCHES is a number"));

    let mut backend = ProcessBackend::new();
    backend
        .add_worker(WorkerSpec::new(PathBuf::from(worker)), &registry())
        .expect("the worker registers its nodes");

    let mut step = step::one_node(&backend, "fixture.scalar_add", 2, 1);

    let mut a = 2i64.to_le_bytes();
    let mut b = 40i64.to_le_bytes();
    let mut cells = [step::full(&mut a), step::full(&mut b), Slot::EMPTY];
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

    // A value's bytes handed to the worker and back: `fixture.raw_io` returns its payload
    // reversed. Across a pipe that is one copy in and one out, and this is what it costs.
    if let Some(megabytes) = args.get(2) {
        let megabytes: usize = megabytes.parse().expect("MEGABYTES is a number");
        hand_over(&backend, megabytes, batches.min(101));
    }
}

/// Times a payload of `megabytes` MiB through the worker and back, one run per operation.
fn hand_over(backend: &ProcessBackend, megabytes: usize, batches: usize) {
    let mut step = step::one_node(backend, "fixture.raw_io", 1, 1);
    let len = megabytes << 20;
    let mut payload: Vec<u8> = (0..len).map(|at| (at * 31 + 7) as u8).collect();
    let mut cells = [step::full(&mut payload), Slot::EMPTY];
    let mut cx_step = StepCx::new();
    let mut run = || {
        // The value handed back last time is let go, as its consumer would have.
        daedalus_runner::drop_owned(core::mem::replace(&mut cells[1], Slot::EMPTY));
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
    // The node's own work (the worker reverses the bytes), timed here on the same machine,
    // so the transport's share is the hand-over less this.
    let mut work = Vec::with_capacity(batches);
    for _ in 0..batches {
        let start = Instant::now();
        payload.reverse();
        core::hint::black_box(&payload);
        work.push(start.elapsed().as_nanos() as f64);
    }
    work.sort_by(f64::total_cmp);
    let per_mb = median(&times) / megabytes as f64;
    let telemetry = backend.telemetry();
    let transport = if telemetry.agreed > 0 {
        "by_reference"
    } else if telemetry.grants > 0 {
        "shared"
    } else {
        "plain"
    };
    let work_per_mb = median(&work) / megabytes as f64;
    println!(
        "hand_over transport {transport} megabytes {megabytes} median_ns {:.0} p99_ns {:.0} min_ns {:.0} max_ns {:.0} ns_per_megabyte {per_mb:.0} p99_ns_per_megabyte {:.0} node_work_ns_per_megabyte {work_per_mb:.0} transport_ns_per_megabyte {:.0} runs {} allocations_per_8_runs {allocations} placed_in {} placed_out {} fd_sends {}",
        median(&times),
        p99(&times),
        times[0],
        times[times.len() - 1],
        p99(&times) / megabytes as f64,
        per_mb - work_per_mb,
        times.len(),
        telemetry.placed_in,
        telemetry.placed_out,
        telemetry.fd_sends
    );
    release(step);
}
