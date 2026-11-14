//! Where placing a value starts to pay: a sweep of list sizes, each handed to a worker three ways.
//!
//! - `view`: the list goes as a place and the node reads its items where the host put them.
//! - `decode`: the list goes as a place and the node decodes it first, as an SDK with no view does.
//! - `socket`: the list goes in the run's packet on the socket, which is what a value below the
//!   threshold does.
//!
//! Two shapes: a `list<f32>`, and a list of 28-byte records (a keypoint's shape). Each node gives
//! back a checksum, so only the list crosses. For each way that places, the threshold is the
//! smallest size from which it is no slower than `socket` (within [`TOLERANCE`]) at every larger
//! size: from there placing never costs, for a node that views or for one that decodes.

use std::path::Path;
use std::time::Instant;

use daedalus_abi::testing::{release, run_once};
use daedalus_abi::{Slot, Slots, Step, StepCx, StepEnd};
use daedalus_backend_process::{ProcessBackend, WorkerSpec};

use crate::step::{full, one_node};
use crate::{median, p99};

/// How much slower than the socket a placed run may be and still count as no slower: the runs'
/// own spread.
const TOLERANCE: f64 = 1.05;

/// The sizes swept, in bytes of items: 512 bytes to 4 MiB, doubling.
const SIZES: [usize; 14] = [
    512,
    1 << 10,
    2 << 10,
    4 << 10,
    8 << 10,
    16 << 10,
    32 << 10,
    64 << 10,
    128 << 10,
    256 << 10,
    512 << 10,
    1 << 20,
    2 << 20,
    4 << 20,
];

struct Shape {
    name: &'static str,
    item: usize,
    view: &'static str,
    decode: &'static str,
}

const SHAPES: [Shape; 2] = [
    Shape {
        name: "f32",
        item: 4,
        view: "sweep.floats_view",
        decode: "sweep.floats_decode",
    },
    Shape {
        name: "point28",
        item: 28,
        view: "sweep.points_view",
        decode: "sweep.points_decode",
    },
];

/// A worker started from `program` that places values of any size, or none.
fn backend(program: &Path, shared_from: usize) -> ProcessBackend {
    let mut backend = ProcessBackend::new();
    let mut registry = daedalus_registry::Registry::new();
    backend
        .add_worker_declaring(
            WorkerSpec::new(program).shared_from(shared_from),
            &mut registry,
        )
        .expect("the sweep worker registers");
    backend
}

/// A list of `count` items of `item` bytes as its cell holds it: the count, then the items, every
/// item's bytes a small finite float (or a small integer) so any reading is valid.
fn list(count: usize, item: usize) -> Vec<u8> {
    let mut bytes = (count as u32).to_le_bytes().to_vec();
    for at in 0..count {
        for word in 0..item / 4 {
            let value = if item == 28 && word >= 5 {
                (at % 7) as u32
            } else {
                ((at + word) as f32 * 0.5).to_bits()
            };
            bytes.extend_from_slice(&value.to_le_bytes());
        }
    }
    bytes
}

/// The median and 99th percentile of `runs` runs of `step` on `value`, in nanoseconds.
fn time(step: &mut Step, value: &mut [u8], runs: usize) -> (f64, f64) {
    let mut cells = [full(value), Slot::EMPTY];
    let mut cx = StepCx::new();
    let mut run = || {
        daedalus_runner::drop_owned(core::mem::replace(&mut cells[1], Slot::EMPTY));
        let end = run_once(step, &mut Slots::of(&mut cells), &mut cx);
        assert!(
            matches!(end, StepEnd::Done),
            "the worker answers: {}",
            cx.reason()
        );
    };
    for _ in 0..runs / 10 + 4 {
        run();
    }
    let mut times = Vec::with_capacity(runs);
    for _ in 0..runs {
        let start = Instant::now();
        run();
        times.push(start.elapsed().as_nanos() as f64);
    }
    daedalus_runner::drop_owned(core::mem::replace(&mut cells[1], Slot::EMPTY));
    times.sort_by(f64::total_cmp);
    (median(&times), p99(&times))
}

/// Runs the sweep with the worker at `program`, `runs` timed runs per size and way, and prints
/// one line per size and shape, then the threshold for each shape.
pub fn sweep(program: &Path, runs: usize) {
    let placing = backend(program, 0);
    let socket = backend(program, usize::MAX);
    for shape in &SHAPES {
        let mut view_step = one_node(&placing, shape.view, 1, 1);
        let mut decode_step = one_node(&placing, shape.decode, 1, 1);
        let mut inline = one_node(&socket, shape.decode, 1, 1);
        let mut rows = Vec::with_capacity(SIZES.len());
        for size in SIZES {
            let mut value = list(size / shape.item, shape.item);
            let placed_before = placing.telemetry().placed_in;
            let (view_ns, view_p99) = time(&mut view_step, &mut value, runs);
            let (decode_ns, decode_p99) = time(&mut decode_step, &mut value, runs);
            let placed = placing.telemetry().placed_in - placed_before;
            let (socket_ns, socket_p99) = time(&mut inline, &mut value, runs);
            rows.push((value.len(), view_ns, decode_ns, socket_ns));
            println!(
                "sweep shape {} bytes {} items {} view_ns {view_ns:.0} view_p99_ns {view_p99:.0} decode_ns {decode_ns:.0} decode_p99_ns {decode_p99:.0} socket_ns {socket_ns:.0} socket_p99_ns {socket_p99:.0} placed {placed}",
                shape.name,
                value.len(),
                size / shape.item,
            );
        }
        let view = threshold(&rows, |row| row.1);
        let decode = threshold(&rows, |row| row.2);
        println!(
            "threshold shape {} view_bytes {view} decode_bytes {decode}",
            shape.name
        );
        release(view_step);
        release(decode_step);
        release(inline);
    }
    let telemetry = socket.telemetry();
    assert_eq!(telemetry.placed_in, 0, "nothing placed on the socket path");
}

/// The smallest size from which `placed` is no slower than the socket at every larger size, or
/// `none` when the largest is slower.
fn threshold(
    rows: &[(usize, f64, f64, f64)],
    placed: fn(&(usize, f64, f64, f64)) -> f64,
) -> String {
    let mut from = None;
    for row in rows.iter().rev() {
        if placed(row) > row.3 * TOLERANCE {
            break;
        }
        from = Some(row.0);
    }
    from.map_or("none".to_owned(), |size| size.to_string())
}
