//! The GPU budgets that are counted, not timed: how many submits, passes, shader runs, pipelines, transfers
//! and allocations the wgpu backend makes for a piece of work. Each is a constant here, and
//! `tests/budgets.rs` measures the backend against it on whatever device the lane has, which in
//! CI is lavapipe. A count doesn't depend on the device's speed, so it is the same everywhere.
//!
//! The timed budgets are the tool in `main.rs`, which runs on boards and not in the gate.

#![forbid(unsafe_code)]

use std::any::Any;
use std::sync::Arc;

use daedalus_abi::testing::{release, run_once};
use daedalus_abi::{Slot, Slots, Step, StepCx, StepEnd};
use daedalus_backend::{Backend, CompileCx};
use daedalus_runner::{drop_owned, put_shared, put_typed, take_typed};
use daedalus_types::{SpaceIndex, TypeIndex};
use daedalus_wgpu::merge::PixelSpec;
use daedalus_wgpu::{Storage, WgpuBackend};
use daedalus_wgpu_conformance::{
    AFFINE, Counted, FLIP, In, Net, TRIPLE, filled, measure, port, run, words,
};

/// A region is one submit, whatever is in it.
pub const SUBMITS_PER_REGION: u64 = 1;
/// A region of shader nodes is one compute pass: its nodes' dispatches share it.
pub const PASSES_PER_REGION: u64 = 1;
/// A whole-shader node runs once.
pub const DISPATCHES_PER_SHADER_NODE: u64 = 1;
/// A chain of pixel bodies runs once, whatever its length.
pub const DISPATCHES_PER_MERGED_CHAIN: u64 = 1;
/// And is one pipeline, built once.
pub const PIPELINES_PER_MERGED_CHAIN: u64 = 1;
/// Running builds nothing: every pipeline is built when the region compiles.
pub const PIPELINES_PER_RUN: u64 = 0;
/// A value that goes from the host to the device is uploaded once.
pub const UPLOADS_PER_BOUNDARY: u64 = 1;
/// And one that comes back is copied once.
pub const DOWNLOADS_PER_BOUNDARY: u64 = 1;
/// A region moves nothing between the host and the device by itself.
pub const TRANSFERS_PER_REGION: u64 = 0;
/// Allocations by one run of a region's step after warm-up, however many shaders run in it.
/// A region of shaders that bind buffers is recorded with Vulkan itself into a command buffer
/// it uses again, so nothing is allocated.
pub const ALLOCATIONS_PER_STEP: u64 = 0;
/// A measure that is over its budget, or under one that has to be exact.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Miss {
    pub what: &'static str,
    pub measured: u64,
    pub budget: u64,
}

impl std::fmt::Display for Miss {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}: measured {}, and the budget is {}",
            self.what, self.measured, self.budget
        )
    }
}

/// A count that has to be exactly its budget. Fewer is a miss too: the budget is then stale,
/// and is lowered in the PR that earned it.
pub fn exactly(what: &'static str, measured: u64, budget: u64) -> Result<(), Miss> {
    match measured == budget {
        true => Ok(()),
        false => Err(Miss {
            what,
            measured,
            budget,
        }),
    }
}

/// A count that may be at most its budget.
pub fn at_most(what: &'static str, measured: u64, budget: u64) -> Result<(), Miss> {
    match measured <= budget {
        true => Ok(()),
        false => Err(Miss {
            what,
            measured,
            budget,
        }),
    }
}

/// What a region of shader nodes may ask of the device in one run, by how many times a
/// shader runs in it.
pub fn region(counted: &Counted, dispatches: u64) -> Result<(), Miss> {
    exactly("submits per region", counted.submits, SUBMITS_PER_REGION)?;
    exactly("passes per region", counted.passes, PASSES_PER_REGION)?;
    exactly("shader runs per region", counted.dispatches, dispatches)?;
    exactly(
        "pipelines built by a run",
        counted.pipelines,
        PIPELINES_PER_RUN,
    )?;
    exactly(
        "transfers by a region",
        counted.uploads + counted.downloads,
        TRANSFERS_PER_REGION,
    )?;
    at_most(
        "allocations per step",
        counted.allocations,
        ALLOCATIONS_PER_STEP,
    )
}

/// One warm run of a line of `nodes` whole-shader nodes over `len` words.
pub fn shader_line(backend: &WgpuBackend, nodes: usize, len: u32) -> Counted {
    let codes: Vec<&'static (dyn Any + Send + Sync)> = vec![&AFFINE; nodes];
    let net = Net::line(&codes);
    let mut unit = net
        .compile(backend, &[port(nodes as u32 - 1, 0)])
        .expect("compiles");
    let input = filled(backend, &(0..len).collect::<Vec<u32>>());
    let mut last = Counted::default();
    // The first runs make the buffers and bind groups. The last is what is counted.
    for _ in 0..3 {
        let ran = run(backend, &mut unit, &[In::Buffer(&input)], 1);
        assert!(matches!(ran.end, StepEnd::Pending(_)), "{}", ran.reason);
        // Reading the output waits for the device, so the next run has a free buffer.
        let out = ran.outputs.into_iter().next().flatten().expect("an output");
        let _ = words(&out);
        last = ran.counted;
    }
    release(unit.step);
    last
}

/// The chain the merge budgets run: two small pixel bodies, `pairs` times over.
pub fn chain(pairs: usize) -> Vec<&'static PixelSpec> {
    (0..pairs).flat_map(|_| [&TRIPLE, &FLIP]).collect()
}

/// A chain of pixel bodies as a region: what compiling it built, and one warm run.
pub fn merged_chain(backend: &WgpuBackend, pairs: usize, len: u32) -> (Counted, Counted) {
    let codes: Vec<&'static (dyn Any + Send + Sync)> = chain(pairs)
        .into_iter()
        .map(|spec| spec as &'static (dyn Any + Send + Sync))
        .collect();
    let net = Net::line(&codes);
    let last = port(codes.len() as u32 - 1, 0);
    let (unit, built) = measure(backend, || net.compile(backend, &[last]).expect("compiles"));
    let mut unit = unit;
    let input = filled(backend, &(0..len).collect::<Vec<u32>>());
    let mut warm = Counted::default();
    for _ in 0..3 {
        let ran = run(backend, &mut unit, &[In::Buffer(&input)], 1);
        assert!(matches!(ran.end, StepEnd::Pending(_)), "{}", ran.reason);
        let out = ran.outputs.into_iter().next().flatten().expect("an output");
        let _ = words(&out);
        warm = ran.counted;
    }
    release(unit.step);
    (built, warm)
}

pub fn transfer(backend: &WgpuBackend, id: &str) -> Step {
    let cx = CompileCx {
        types: &[],
        consts: &[],
        spaces: &[],
        bodies: &[],
        edges: &[],
        host_inputs: &[],
        host_outputs: &[],
        fifo_capacity: 1,
        merge_allowed: true,
        freeze_params: false,
        sharing: &[],
    };
    let table = backend.transfers();
    let found = table.iter().find(|t| t.id == id).expect("offered");
    (found.make)(&cx).expect("a step")
}

pub fn through(step: &mut Step, input: Slot) -> Slot {
    let mut cells = [input, Slot::EMPTY];
    let mut cx = StepCx::new();
    let end = run_once(step, &mut Slots::of(&mut cells), &mut cx);
    assert!(!matches!(end, StepEnd::Fault), "{}", cx.reason());
    drop_owned(cells[0]);
    cells[1]
}

/// One value of `bytes` bytes taken across each boundary once: host to device, and device back
/// to memory the host reads.
pub fn boundaries(backend: &WgpuBackend, bytes: usize) -> Counted {
    let (ty, space) = (TypeIndex(0), SpaceIndex(0));
    let mut upload = transfer(backend, daedalus_wgpu::UPLOAD);
    let mut readback = transfer(backend, daedalus_wgpu::READBACK);
    let payload: Arc<[u8]> = (0..bytes).map(|at| at as u8).collect();
    let (_, counted) = measure(backend, || {
        let mut cell = Slot::EMPTY;
        put_shared(&mut cell, ty, space, payload);
        let mut up = through(&mut upload, cell);
        let on_device = take_typed::<Storage>(&mut up).expect("a buffer");
        let mut cell = Slot::EMPTY;
        put_typed(&mut cell, ty, space, on_device);
        let mut back = through(&mut readback, cell);
        drop(take_typed::<Storage>(&mut back));
    });
    release(upload);
    release(readback);
    counted
}
