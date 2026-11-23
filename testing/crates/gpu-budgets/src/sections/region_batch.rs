//! The region test's own nodes run as a batch: eight of them, the add and the triple shaders of
//! `backends/wgpu/tests/region.rs` chained the way that test chains them, over 1000 words. The
//! region runs as one step, one submit and one pass. The same eight are also submitted one by
//! one, waited for once at the end, to compare.

use std::any::Any;
use std::sync::Arc;
use std::time::Instant;

use daedalus_abi::counting::counted;
use daedalus_abi::testing::{release, run_once, wait_fence};
use daedalus_abi::{Slot, Slots, StepCx, StepEnd};
use daedalus_backend::{
    Backend, BodyIndex, CompileCx, Compiled, InnerEdge, NO_METADATA, NodeBody, PortRef, RateId,
    Region, StepUnit,
};
use daedalus_graph::EdgePolicyDoc;
use daedalus_runner::{put_typed, take_typed};
use daedalus_types::{BackendIndex, NodeIndex, PortIndex, SpaceIndex, TypeIndex};
use daedalus_wgpu::shader::{BindingKind, BindingSpec, Bound, Dispatch, Run, Shader, ShaderSpec};
use daedalus_wgpu::{Storage, Ticket, WgpuBackend};

use super::{line_ms, measure, summary};

/// Elements per node, as the region test uses them.
const ELEMENTS: u32 = 1000;
/// Nodes in the batch, as the region test's eight-node chain.
const NODES: u32 = 8;
/// Warm-up runs before the timed ones, and the timed runs: the median of 200 after 20.
const WARM: usize = 20;
const TIMED: usize = 200;

const fn storage(index: u32, name: &'static str, read_only: bool) -> BindingSpec {
    BindingSpec {
        index,
        name,
        kind: BindingKind::Storage {
            read_only,
            stride: 4,
        },
    }
}

/// `out = a + b`, element by element. The same source as the region test's `ADD`.
pub static ADD: ShaderSpec = ShaderSpec {
    name: "add",
    entry: "main",
    source: "
        @group(0) @binding(0) var<storage, read> a: array<u32>;
        @group(0) @binding(1) var<storage, read> b: array<u32>;
        @group(0) @binding(2) var<storage, read_write> out: array<u32>;
        @compute @workgroup_size(64)
        fn main(@builtin(global_invocation_id) id: vec3<u32>) {
            if id.x < arrayLength(&out) { out[id.x] = a[id.x] + b[id.x]; }
        }
    ",
    workgroup: [64, 1, 1],
    bindings: &[
        storage(0, "a", true),
        storage(1, "b", true),
        storage(2, "out", false),
    ],
    dispatch: Dispatch::PerElement {
        binding: 2,
        stride: 4,
    },
    inputs: &[0, 1],
    outputs: &[2],
};

/// `out = src * 3`, element by element. The same source as the region test's `TRIPLE`.
pub static TRIPLE: ShaderSpec = ShaderSpec {
    name: "triple",
    entry: "main",
    source: "
        @group(0) @binding(0) var<storage, read> src: array<u32>;
        @group(0) @binding(1) var<storage, read_write> out: array<u32>;
        @compute @workgroup_size(64)
        fn main(@builtin(global_invocation_id) id: vec3<u32>) {
            if id.x < arrayLength(&out) { out[id.x] = src[id.x] * 3u; }
        }
    ",
    workgroup: [64, 1, 1],
    bindings: &[storage(0, "src", true), storage(1, "out", false)],
    dispatch: Dispatch::PerElement {
        binding: 1,
        stride: 4,
    },
    inputs: &[0],
    outputs: &[1],
};

fn port(node: u32, port: u16) -> PortRef {
    PortRef {
        node: NodeIndex(node),
        port: PortIndex(port),
    }
}

fn body(node: u32, spec: &'static ShaderSpec) -> NodeBody<'static> {
    NodeBody {
        node: NodeIndex(node),
        id: spec.name,
        body: BodyIndex(0),
        inputs: spec.inputs.len() as u16,
        outputs: spec.outputs.len() as u16,
        merges: true,
        fire_all: true,
        metrics: 0,
        label: None,
        metadata: &NO_METADATA,
        code: spec as &(dyn Any + Send + Sync),
        text: spec.source,
    }
}

/// Node 0 adds the two region inputs; each later node is `add(previous, x)` when its number is
/// even and `triple(previous)` when odd. This is the region test's `chain`.
fn node_specs() -> Vec<&'static ShaderSpec> {
    (0..NODES)
        .map(|node| match node {
            0 => &ADD,
            n if n % 2 == 0 => &ADD,
            _ => &TRIPLE,
        })
        .collect()
}

/// What the chain computes for one element, as the region test's `reference`.
fn reference(x: u32, y: u32) -> u32 {
    let mut value = x.wrapping_add(y);
    for node in 1..NODES {
        value = if node % 2 == 0 {
            value.wrapping_add(x)
        } else {
            value.wrapping_mul(3)
        };
    }
    value
}

/// The region as one compiled step, with its inputs and its output port.
fn compile_region(backend: &WgpuBackend) -> StepUnit {
    let specs = node_specs();
    let bodies: Vec<NodeBody<'static>> = specs
        .iter()
        .enumerate()
        .map(|(node, spec)| body(node as u32, spec))
        .collect();
    let nodes: Vec<NodeIndex> = (0..NODES).map(NodeIndex).collect();
    let edges_list: Vec<(PortRef, PortRef)> = (1..NODES)
        .map(|node| (port(node - 1, 0), port(node, 0)))
        .collect();
    let mut inputs = vec![port(0, 0), port(0, 1)];
    for node in 1..NODES {
        if node % 2 == 0 {
            inputs.push(port(node, 1));
        }
    }
    let policy = EdgePolicyDoc::default();
    let edges: Vec<InnerEdge<'_>> = edges_list
        .iter()
        .map(|&(from, to)| InnerEdge {
            from,
            to,
            queued: false,
            policy: &policy,
        })
        .collect();
    let outputs = [port(NODES - 1, 0)];
    let region = Region {
        backend: BackendIndex(0),
        nodes: &nodes,
        inputs: &inputs,
        outputs: &outputs,
        incoming: &[],
        placed: &[],
        read_in: &[],
        rate: RateId(0),
    };
    let cx = CompileCx {
        types: &[],
        consts: &[],
        spaces: &[],
        bodies: &bodies,
        edges: &edges,
        host_inputs: &[],
        host_outputs: &[],
        fifo_capacity: 1,
        merge_allowed: true,
        freeze_params: false,
        sharing: &[],
        lent: &[],
        offered: &[],
    };
    let Compiled::Steps(mut units) = backend.compile(&region, &cx).expect("compiles") else {
        panic!("a region of shaders compiles to steps");
    };
    assert_eq!(units.len(), 1, "one step for the whole region");
    units.remove(0)
}

/// Fills the inputs, runs the step once and waits for its fence. Returns the output cell, which
/// the caller takes from after the clock stops.
fn step_once(unit: &mut StepUnit, cells: &mut [Slot]) {
    let mut cx = StepCx::new();
    let end = run_once(&mut unit.step, &mut Slots::of(cells), &mut cx);
    match end {
        StepEnd::Pending(fence) => {
            wait_fence(&fence, u64::MAX);
        }
        _ => panic!("the region did not pend: {}", cx.reason()),
    }
}

fn input_cells(x: &Arc<Storage>, y: &Arc<Storage>, inputs: usize) -> Vec<Slot> {
    let mut cells = vec![Slot::EMPTY; inputs + 1];
    for (at, cell) in cells.iter_mut().take(inputs).enumerate() {
        let value = if at == 1 { y } else { x };
        put_typed(cell, TypeIndex(0), SpaceIndex(0), Arc::clone(value));
    }
    cells
}

fn words(storage: &Storage) -> Vec<u32> {
    let mut bytes = vec![0; storage.size() as usize];
    storage.read_into(0, &mut bytes).expect("a readback");
    bytes
        .as_chunks::<4>()
        .0
        .iter()
        .map(|word| u32::from_le_bytes(*word))
        .collect()
}

fn filled(backend: &WgpuBackend, values: &[u32]) -> Arc<Storage> {
    let storage = Storage::new(backend, values.len() as u64 * 4).expect("a buffer");
    let bytes: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();
    storage.write(0, &bytes).expect("in range");
    Arc::new(storage)
}

/// One node of the eight, as its own shader and its own output, for the submit-by-submit run.
struct Solo {
    shader: Shader,
    out: Storage,
}

/// Submits one run of a shader. A `Busy` answer waits for the oldest run, then submits again.
fn submit(shader: &Shader, bound: &[Bound<'_>]) -> Ticket {
    loop {
        match shader.run(bound).expect("runs") {
            Run::Started(ticket) => return ticket,
            Run::Busy(oldest) => oldest.wait().expect("finishes"),
        }
    }
}

/// The eight nodes, each submitted on its own, and one wait for the last at the end. The device
/// finishes work in the order it was submitted, so the last wait covers all eight.
fn submit_each(solos: &[Solo], x: &Storage, y: &Storage) {
    let mut last = None;
    for (node, solo) in solos.iter().enumerate() {
        let bound: Vec<Bound<'_>> = match node {
            0 => vec![
                Bound::Storage(x),
                Bound::Storage(y),
                Bound::Storage(&solo.out),
            ],
            n if n % 2 == 0 => vec![
                Bound::Storage(&solos[n - 1].out),
                Bound::Storage(x),
                Bound::Storage(&solo.out),
            ],
            n => vec![Bound::Storage(&solos[n - 1].out), Bound::Storage(&solo.out)],
        };
        last = Some(submit(&solo.shader, &bound));
    }
    if let Some(ticket) = last {
        ticket.wait().expect("finishes");
    }
}

/// The batch of eight, as one region step and as eight submits, with the allocations of one warm
/// step run. The region's result is checked against the region test's arithmetic.
pub fn region_batch(backend: &WgpuBackend) {
    let xs: Vec<u32> = (0..ELEMENTS).map(|at| at * 7 + 1).collect();
    let ys: Vec<u32> = (0..ELEMENTS).map(|at| at * 11 + 5).collect();
    let (x, y) = (filled(backend, &xs), filled(backend, &ys));
    let want: Vec<u32> = xs.iter().zip(&ys).map(|(&x, &y)| reference(x, y)).collect();

    let mut unit = compile_region(backend);
    let reads = unit.info.reads.len();
    let mut cells = input_cells(&x, &y, reads);
    step_once(&mut unit, &mut cells);
    let region_ok = words(&take_output(&mut cells, reads)) == want;

    let mut times = Vec::with_capacity(TIMED);
    for round in 0..(WARM + TIMED) {
        let mut cells = input_cells(&x, &y, reads);
        let start = Instant::now();
        step_once(&mut unit, &mut cells);
        let took = start.elapsed();
        drop(take_output(&mut cells, reads));
        if round >= WARM {
            times.push(took);
        }
    }
    // The allocations of one warm run, with the inputs already on the device.
    let mut cells = input_cells(&x, &y, reads);
    let ((), allocations) = counted(|| step_once(&mut unit, &mut cells));
    drop(take_output(&mut cells, reads));
    release(unit.step);
    println!(
        "region-batch, 8 nodes over {ELEMENTS} words as one step (results match: {region_ok}): {}",
        line_ms(summary(&mut times))
    );
    println!("region-batch, allocations in one warm step run: {allocations}");

    let solos: Vec<Solo> = node_specs()
        .into_iter()
        .map(|spec| Solo {
            shader: Shader::load(backend, spec).expect("loads"),
            out: Storage::new(backend, ELEMENTS as u64 * 4).expect("a buffer"),
        })
        .collect();
    let apart = measure(WARM, TIMED, || submit_each(&solos, &x, &y));
    println!(
        "region-batch, the same 8 nodes as 8 submits, one wait: {}",
        line_ms(apart)
    );
}

/// Takes the output of a step run out of its cells, read as a device value.
fn take_output(cells: &mut [Slot], reads: usize) -> Arc<Storage> {
    take_typed::<Storage>(&mut cells[reads]).expect("an output on the device")
}
