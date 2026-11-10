//! A region of shader nodes compiled into one step: one submit, one pass per node, values that
//! stay on the device between the nodes, and results that match the same arithmetic here.

mod common;

use std::any::Any;
use std::collections::BTreeMap;
use std::sync::Arc;

use daedalus_abi::counting::{CountingAlloc, counted};
use daedalus_abi::testing::{release, run_once};
use daedalus_abi::{Slot, Slots, StepCx, StepEnd};
use daedalus_backend::{
    Backend, BodyIndex, CompileCx, Compiled, InnerEdge, NO_METADATA, NodeBody, PortRef, RateId,
    Region, StepUnit,
};
use daedalus_graph::{EdgePolicyDoc, Lit};
use daedalus_runner::{put_typed, take_typed};
use daedalus_types::{BackendIndex, NodeIndex, PortIndex, SpaceIndex, TypeIndex};
use daedalus_wgpu::shader::{BindingKind, BindingSpec, Dispatch, ShaderSpec};
use daedalus_wgpu::{Options, Storage, WgpuBackend};

#[global_allocator]
static ALLOC: CountingAlloc = CountingAlloc;

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

/// `out = a + b`, element by element.
static ADD: ShaderSpec = ShaderSpec {
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

/// `out = src * 3`, element by element.
static TRIPLE: ShaderSpec = ShaderSpec {
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

fn body<'a>(
    node: u32,
    spec: &'static ShaderSpec,
    metadata: &'a BTreeMap<String, Lit>,
) -> NodeBody<'a> {
    NodeBody {
        node: NodeIndex(node),
        id: spec.name,
        body: BodyIndex(0),
        inputs: spec.inputs.len() as u16,
        outputs: spec.outputs.len() as u16,
        merges: true,
        fire_all: true,
        label: None,
        metadata,
        code: spec as &(dyn Any + Send + Sync),
        text: spec.source,
    }
}

/// A chain: node 0 adds the two region inputs, and each later node is `add(previous, x)` when
/// its number is even and `triple(previous)` when odd.
struct Chain {
    bodies: Vec<NodeBody<'static>>,
    edges: Vec<(PortRef, PortRef)>,
    nodes: Vec<NodeIndex>,
    inputs: Vec<PortRef>,
}

fn chain(len: u32) -> Chain {
    let mut made = Chain {
        bodies: vec![body(0, &ADD, &NO_METADATA)],
        edges: Vec::new(),
        nodes: vec![NodeIndex(0)],
        // Cell 0 is x and cell 1 is y. Later nodes that add read x again, through a port of
        // their own.
        inputs: vec![port(0, 0), port(0, 1)],
    };
    for node in 1..len {
        let adds = node % 2 == 0;
        made.bodies
            .push(body(node, if adds { &ADD } else { &TRIPLE }, &NO_METADATA));
        made.nodes.push(NodeIndex(node));
        made.edges.push((port(node - 1, 0), port(node, 0)));
        if adds {
            made.inputs.push(port(node, 1));
        }
    }
    made
}

/// What the chain computes for one element.
fn reference(len: u32, x: u32, y: u32) -> Vec<u32> {
    let mut value = x.wrapping_add(y);
    let mut seen = vec![value];
    for node in 1..len {
        value = if node % 2 == 0 {
            value.wrapping_add(x)
        } else {
            value.wrapping_mul(3)
        };
        seen.push(value);
    }
    seen
}

fn compile(backend: &WgpuBackend, chain: &Chain, outputs: &[PortRef]) -> StepUnit {
    let policy = EdgePolicyDoc::default();
    let edges: Vec<InnerEdge<'_>> = chain
        .edges
        .iter()
        .map(|&(from, to)| InnerEdge {
            from,
            to,
            queued: false,
            policy: &policy,
        })
        .collect();
    let region = Region {
        backend: BackendIndex(0),
        nodes: &chain.nodes,
        inputs: &chain.inputs,
        outputs,
        incoming: &[],
        rate: RateId(0),
    };
    let cx = CompileCx {
        types: &[],
        consts: &[],
        spaces: &[],
        bodies: &chain.bodies,
        edges: &edges,
        host_inputs: &[],
        host_outputs: &[],
        fifo_capacity: 1,
        merge_allowed: true,
        freeze_params: false,
    };
    let Compiled::Steps(mut units) = backend.compile(&region, &cx).expect("compiles") else {
        panic!("a region of shaders compiles to steps");
    };
    assert_eq!(units.len(), 1, "one step for the whole region");
    let unit = units.remove(0);
    assert_eq!(unit.info.covers, chain.nodes, "the step covers every node");
    assert_eq!(unit.info.reads, chain.inputs);
    assert_eq!(unit.info.writes, outputs);
    assert!(unit.info.may_pend);
    unit
}

fn words(storage: &Storage) -> Vec<u32> {
    let mut bytes = vec![0; storage.size() as usize];
    // A readback waits for the work that fills the buffer.
    storage.read_into(0, &mut bytes).expect("a readback");
    bytes
        .as_chunks::<4>()
        .0
        .iter()
        .map(|word| u32::from_le_bytes(*word))
        .collect()
}

fn filled(backend: &WgpuBackend, words: &[u32]) -> Arc<Storage> {
    let storage = Storage::new(backend, words.len() as u64 * 4).expect("a buffer");
    let bytes: Vec<u8> = words.iter().flat_map(|word| word.to_le_bytes()).collect();
    storage.write(0, &bytes).expect("in range");
    Arc::new(storage)
}

/// One run of the step: x and y go in every input cell that reads them, and the outputs come
/// back out of the cells.
fn run(
    unit: &mut StepUnit,
    inputs: usize,
    outputs: usize,
    x: &Arc<Storage>,
    y: &Arc<Storage>,
) -> Vec<Arc<Storage>> {
    let mut cells = vec![Slot::EMPTY; inputs + outputs];
    for (at, cell) in cells.iter_mut().take(inputs).enumerate() {
        let value = if at == 1 { y } else { x };
        put_typed(cell, TypeIndex(0), SpaceIndex(0), Arc::clone(value));
    }
    let mut cx = StepCx::new();
    let end = run_once(&mut unit.step, &mut Slots::of(&mut cells), &mut cx);
    assert!(matches!(end, StepEnd::Pending(_)), "{}", cx.reason());
    let (read, written) = cells.split_at_mut(inputs);
    for cell in read {
        drop(take_typed::<Storage>(cell));
    }
    written
        .iter_mut()
        .map(|cell| take_typed::<Storage>(cell).expect("an output on the device"))
        .collect()
}

fn private() -> Option<WgpuBackend> {
    let record = common::adapter()?;
    Some(WgpuBackend::open(Options {
        adapter: Some(record.adapter.name),
        ..Options::default()
    }))
}

#[test]
fn eight_nodes_run_as_one_submit_and_eight_passes() {
    let Some(backend) = private() else { return };
    let made = chain(8);
    let mut unit = compile(&backend, &made, &[port(7, 0)]);
    let xs: Vec<u32> = (0..1000).map(|at| at * 7 + 1).collect();
    let ys: Vec<u32> = (0..1000).map(|at| at * 11 + 5).collect();
    let (x, y) = (filled(&backend, &xs), filled(&backend, &ys));

    let before = backend.shader_stats().expect("open");
    let out = run(&mut unit, made.inputs.len(), 1, &x, &y);
    let after = backend.shader_stats().expect("open");
    assert_eq!(
        after.submits - before.submits,
        1,
        "one submit for the region"
    );
    assert_eq!(after.passes - before.passes, 8, "one pass per node");

    let want: Vec<u32> = xs
        .iter()
        .zip(&ys)
        .map(|(&x, &y)| reference(8, x, y)[7])
        .collect();
    assert_eq!(words(&out[0]), want);

    // A second run with other values gives a buffer of its own while the first is still held.
    let (x2, y2) = (filled(&backend, &ys), filled(&backend, &xs));
    let again = run(&mut unit, made.inputs.len(), 1, &x2, &y2);
    assert_eq!(words(&out[0]), want, "the held output wasn't written over");
    let want2: Vec<u32> = ys
        .iter()
        .zip(&xs)
        .map(|(&x, &y)| reference(8, x, y)[7])
        .collect();
    assert_eq!(words(&again[0]), want2);
    release(unit.step);
}

#[test]
fn a_value_read_inside_and_outside_the_region_is_one_buffer() {
    let Some(backend) = private() else { return };
    let made = chain(4);
    // Node 1's output feeds node 2, and also leaves the region.
    let mut unit = compile(&backend, &made, &[port(1, 0), port(3, 0)]);
    let (x, y) = (
        filled(&backend, &[5, 6, 7, 8]),
        filled(&backend, &[1, 1, 1, 1]),
    );
    let before = backend.shader_stats().expect("open");
    let out = run(&mut unit, made.inputs.len(), 2, &x, &y);
    assert_eq!(
        backend.shader_stats().expect("open").submits - before.submits,
        1
    );
    let middle: Vec<u32> = [5u32, 6, 7, 8]
        .iter()
        .map(|&x| reference(4, x, 1)[1])
        .collect();
    let last: Vec<u32> = [5u32, 6, 7, 8]
        .iter()
        .map(|&x| reference(4, x, 1)[3])
        .collect();
    assert_eq!(words(&out[0]), middle);
    assert_eq!(words(&out[1]), last);
    release(unit.step);
}

#[test]
fn what_cannot_run_is_refused_or_faults_and_names_the_node() {
    let Some(backend) = private() else { return };
    // A body that isn't a shader is refused, by node.
    let mut made = chain(2);
    static NOT_A_SHADER: u32 = 7;
    made.bodies[1].code = &NOT_A_SHADER;
    let policy = EdgePolicyDoc::default();
    let edges = [InnerEdge {
        from: port(0, 0),
        to: port(1, 0),
        queued: false,
        policy: &policy,
    }];
    let outputs = [port(1, 0)];
    let region = Region {
        backend: BackendIndex(0),
        nodes: &made.nodes,
        inputs: &made.inputs,
        outputs: &outputs,
        incoming: &[],
        rate: RateId(0),
    };
    let cx = CompileCx {
        types: &[],
        consts: &[],
        spaces: &[],
        bodies: &made.bodies,
        edges: &edges,
        host_inputs: &[],
        host_outputs: &[],
        fifo_capacity: 1,
        merge_allowed: true,
        freeze_params: false,
    };
    let refused = backend.compile(&region, &cx).expect_err("not a shader");
    assert_eq!(refused.nodes, [NodeIndex(1)]);
    assert!(
        refused.reason.contains("isn't a shader"),
        "{}",
        refused.reason
    );

    // An input that isn't on the device faults the step, names the node, and submits nothing.
    let made = chain(2);
    let mut unit = compile(&backend, &made, &[port(1, 0)]);
    let before = backend.shader_stats().expect("open");
    let mut cells = vec![Slot::EMPTY; 3];
    let mut step_cx = StepCx::new();
    let end = run_once(&mut unit.step, &mut Slots::of(&mut cells), &mut step_cx);
    assert!(matches!(end, StepEnd::Fault));
    assert_eq!(step_cx.at, 0, "the first node is the one with no input");
    assert!(step_cx.reason().contains("missing") || step_cx.reason().contains("isn't a buffer"));
    assert_eq!(
        backend.shader_stats().expect("open").submits,
        before.submits
    );
    release(unit.step);
}

#[test]
fn a_region_steps_allocations_dont_grow_with_its_nodes() {
    let Some(backend) = private() else { return };
    let (x, y) = (filled(&backend, &[1; 256]), filled(&backend, &[2; 256]));
    let cost = |len: u32| {
        let made = chain(len);
        let mut unit = compile(&backend, &made, &[port(len - 1, 0)]);
        let inputs = made.inputs.len();
        // Warm: the buffers exist and the bind groups are kept. Each output is dropped before
        // the next run, as a consumer that has finished with it does.
        for _ in 0..8 {
            drop(run(&mut unit, inputs, 1, &x, &y));
        }
        let cheapest = (0..16)
            .map(|_| {
                let mut cells = vec![Slot::EMPTY; inputs + 1];
                for (at, cell) in cells.iter_mut().take(inputs).enumerate() {
                    let value = if at == 1 { &y } else { &x };
                    put_typed(cell, TypeIndex(0), SpaceIndex(0), Arc::clone(value));
                }
                let mut cx = StepCx::new();
                let (end, allocated) =
                    counted(|| run_once(&mut unit.step, &mut Slots::of(&mut cells), &mut cx));
                assert!(matches!(end, StepEnd::Pending(_)));
                for cell in &mut cells {
                    drop(take_typed::<Storage>(cell));
                }
                allocated
            })
            .min()
            .unwrap_or(0);
        release(unit.step);
        cheapest
    };
    let (one, eight) = (cost(1), cost(8));
    // wgpu allocates for the encoder and the submit once, and a little for each pass it
    // records. What this backend adds on top of that doesn't depend on the node count.
    let per_pass = (eight.saturating_sub(one)) / 7;
    assert!(
        per_pass <= 12,
        "one node: {one}, eight nodes: {eight}, so {per_pass} per extra pass"
    );
    assert!(one <= 64, "one node: {one}");
}

/// `out = src * gain`, with the gain in a uniform that a port feeds.
static GAIN: ShaderSpec = ShaderSpec {
    name: "gain",
    entry: "main",
    source: "
        struct Config { gain: u32 }
        @group(0) @binding(0) var<storage, read> src: array<u32>;
        @group(0) @binding(1) var<uniform> config: Config;
        @group(0) @binding(2) var<storage, read_write> out: array<u32>;
        @compute @workgroup_size(64)
        fn main(@builtin(global_invocation_id) id: vec3<u32>) {
            if id.x < arrayLength(&out) { out[id.x] = src[id.x] * config.gain; }
        }
    ",
    workgroup: [64, 1, 1],
    bindings: &[
        storage(0, "src", true),
        BindingSpec {
            index: 1,
            name: "config",
            kind: BindingKind::Uniform { size: 4 },
        },
        storage(2, "out", false),
    ],
    dispatch: Dispatch::PerElement {
        binding: 2,
        stride: 4,
    },
    inputs: &[0, 1],
    outputs: &[2],
};

#[test]
fn a_uniform_takes_plain_bytes_or_bytes_already_on_the_device() {
    let Some(backend) = private() else { return };
    let made = Chain {
        bodies: vec![body(0, &GAIN, &NO_METADATA)],
        edges: Vec::new(),
        nodes: vec![NodeIndex(0)],
        inputs: vec![port(0, 0), port(0, 1)],
    };
    let mut unit = compile(&backend, &made, &[port(0, 0)]);
    let src = filled(&backend, &[1, 2, 3, 4]);
    let mut go = |gain: Slot| {
        let mut cells = [Slot::EMPTY, gain, Slot::EMPTY];
        put_typed(&mut cells[0], TypeIndex(0), SpaceIndex(0), Arc::clone(&src));
        let mut cx = StepCx::new();
        let end = run_once(&mut unit.step, &mut Slots::of(&mut cells), &mut cx);
        assert!(matches!(end, StepEnd::Pending(_)), "{}", cx.reason());
        drop(take_typed::<Storage>(&mut cells[0]));
        daedalus_runner::drop_owned(cells[1]);
        words(&take_typed::<Storage>(&mut cells[2]).expect("an output"))
    };
    // As a CPU node writes a small value: inline bytes.
    // `put_inline` keeps a pointer to the bytes it is given, so they live until the run is over.
    let five = 5u32.to_le_bytes();
    let mut inline = Slot::EMPTY;
    daedalus_runner::put_inline(&mut inline, TypeIndex(0), SpaceIndex(0), &five).expect("fits");
    assert_eq!(go(inline), [5, 10, 15, 20]);
    // As an upload leaves it: a buffer on the device, copied into the uniform in the same submit.
    let mut uploaded = Slot::EMPTY;
    put_typed(
        &mut uploaded,
        TypeIndex(0),
        SpaceIndex(0),
        filled(&backend, &[7]),
    );
    assert_eq!(go(uploaded), [7, 14, 21, 28]);
    release(unit.step);
}

#[test]
fn a_lost_device_faults_the_region_then_is_reopened_and_the_region_runs_again() {
    let Some(backend) = private() else { return };
    let made = chain(3);
    let mut unit = compile(&backend, &made, &[port(2, 0)]);
    let want: Vec<u32> = [3u32, 4].iter().map(|&x| reference(3, x, 9)[2]).collect();
    let (x, y) = (filled(&backend, &[3, 4]), filled(&backend, &[9, 9]));
    assert_eq!(
        words(&run(&mut unit, made.inputs.len(), 1, &x, &y)[0]),
        want
    );
    let old_device = x.device_key();

    // The device is lost. The region's next run faults and submits nothing, and what covers it
    // is still every node, so the fault can be laid at the right nodes.
    backend.mark_lost();
    let mut cells = vec![Slot::EMPTY; made.inputs.len() + 1];
    for (at, cell) in cells.iter_mut().take(made.inputs.len()).enumerate() {
        let value = if at == 1 { &y } else { &x };
        put_typed(cell, TypeIndex(0), SpaceIndex(0), Arc::clone(value));
    }
    let mut cx = StepCx::new();
    let end = run_once(&mut unit.step, &mut Slots::of(&mut cells), &mut cx);
    assert!(matches!(end, StepEnd::Fault));
    assert!(cx.reason().contains("lost"), "{}", cx.reason());
    assert_eq!(unit.info.covers, made.nodes);
    for cell in &mut cells {
        drop(take_typed::<Storage>(cell));
    }

    // That request reopened the device, with no wait before the first attempt, and the region
    // was built again on it. Values made on the new device run through it as before.
    assert_eq!(backend.retry_in(), None);
    let (x, y) = (filled(&backend, &[3, 4]), filled(&backend, &[9, 9]));
    assert_ne!(x.device_key(), old_device, "the backend is on a new device");
    assert_eq!(
        words(&run(&mut unit, made.inputs.len(), 1, &x, &y)[0]),
        want
    );
    release(unit.step);

    // Off is the host's doing, and only the host's. Nothing opens while it's off.
    backend.turn_off();
    assert_eq!(backend.start(), Err(daedalus_wgpu::OpenError::Off));
    assert!(Storage::new(&backend, 16).is_err());
    backend.turn_on();
    assert!(backend.start().is_ok());
}

/// A recording body: copies its input to its output with a copy command of its own.
fn record_copy(cx: &mut daedalus_wgpu::record::RecordCx<'_>) -> Result<(), String> {
    let (from, to) = (
        cx.input(0).ok_or("no input")?,
        cx.output(0).ok_or("no output")?,
    );
    let bytes = from.size().min(to.size());
    cx.encoder()
        .copy_buffer_to_buffer(from.wgpu(), 0, to.wgpu(), 0, bytes);
    Ok(())
}

fn record_refuses(_: &mut daedalus_wgpu::record::RecordCx<'_>) -> Result<(), String> {
    Err("the author's recording said no".into())
}

static COPY_BODY: daedalus_wgpu::record::RecordBody =
    daedalus_wgpu::record::RecordBody::new(record_copy);
static HALF_BODY: daedalus_wgpu::record::RecordBody =
    daedalus_wgpu::record::RecordBody::new(record_copy).sized(|inputs, _| inputs[0] / 2);
static REFUSING_BODY: daedalus_wgpu::record::RecordBody =
    daedalus_wgpu::record::RecordBody::new(record_refuses);

fn recording(node: u32, record: &'static daedalus_wgpu::record::RecordBody) -> NodeBody<'static> {
    NodeBody {
        node: NodeIndex(node),
        id: "recording",
        body: BodyIndex(0),
        inputs: 1,
        outputs: 1,
        merges: true,
        fire_all: true,
        label: None,
        metadata: &NO_METADATA,
        code: record as &(dyn Any + Send + Sync),
        text: "",
    }
}

#[test]
fn a_recording_body_records_into_the_regions_one_submit() {
    let Some(backend) = private() else { return };
    // add(x, y) -> the author's copy -> triple, and the copy's output leaves the region too.
    let made = |record| Chain {
        bodies: vec![
            body(0, &ADD, &NO_METADATA),
            recording(1, record),
            body(2, &TRIPLE, &NO_METADATA),
        ],
        edges: vec![(port(0, 0), port(1, 0)), (port(1, 0), port(2, 0))],
        nodes: vec![NodeIndex(0), NodeIndex(1), NodeIndex(2)],
        inputs: vec![port(0, 0), port(0, 1)],
    };
    let chain = made(&COPY_BODY);
    let mut unit = compile(&backend, &chain, &[port(1, 0), port(2, 0)]);
    let (x, y) = (
        filled(&backend, &[1, 2, 3, 4]),
        filled(&backend, &[10, 20, 30, 40]),
    );
    let before = backend.shader_stats().expect("open");
    let out = run(&mut unit, 2, 2, &x, &y);
    let after = backend.shader_stats().expect("open");
    assert_eq!(
        after.submits - before.submits,
        1,
        "the author never submits"
    );
    assert_eq!(
        after.passes - before.passes,
        2,
        "the two shaders: the copy opens no pass"
    );
    assert_eq!(words(&out[0]), [11, 22, 33, 44]);
    assert_eq!(words(&out[1]), [33, 66, 99, 132]);
    release(unit.step);

    // The body says how large its output is.
    let chain = made(&HALF_BODY);
    let mut unit = compile(&backend, &chain, &[port(1, 0), port(2, 0)]);
    let out = run(&mut unit, 2, 2, &x, &y);
    assert_eq!(words(&out[0]), [11, 22]);
    assert_eq!(
        words(&out[1]),
        [33, 66],
        "and what follows it is sized from that"
    );
    release(unit.step);

    // An error from the body faults the step at that node, and nothing is submitted.
    let chain = made(&REFUSING_BODY);
    let mut unit = compile(&backend, &chain, &[port(2, 0)]);
    let before = backend.shader_stats().expect("open");
    let mut cells = vec![Slot::EMPTY; 3];
    put_typed(&mut cells[0], TypeIndex(0), SpaceIndex(0), Arc::clone(&x));
    put_typed(&mut cells[1], TypeIndex(0), SpaceIndex(0), Arc::clone(&y));
    let mut cx = StepCx::new();
    let end = run_once(&mut unit.step, &mut Slots::of(&mut cells), &mut cx);
    assert!(matches!(end, StepEnd::Fault));
    assert_eq!((cx.at, cx.reason()), (1, "the author's recording said no"));
    assert_eq!(
        backend.shader_stats().expect("open").submits,
        before.submits
    );
    for cell in &mut cells {
        drop(take_typed::<Storage>(cell));
    }
    release(unit.step);
}

#[cfg(target_os = "linux")]
#[test]
fn a_raw_submitter_is_made_on_the_backends_device() {
    let Some(backend) = private() else { return };
    let storage = Storage::new(&backend, 64).expect("a buffer");
    match backend.raw_submitter(3) {
        Ok(submitter) => {
            assert_eq!(submitter.slots(), 3);
            assert!(
                storage.raw_buffer().is_some(),
                "a Vulkan device has Vulkan buffers"
            );
            // The feature is a fact about the device, asked for without making a pipeline.
            let _ = backend.storage_buffer_8bit();
        }
        // Off Vulkan there is no ring to make, and the error says so.
        Err(error) => assert!(storage.raw_buffer().is_none(), "{error}"),
    }
}

#[test]
fn a_region_keeps_what_it_read_until_its_next_run() {
    let Some(backend) = private() else { return };
    let made = chain(2);
    let mut unit = compile(&backend, &made, &[port(1, 0)]);
    let (x, y) = (filled(&backend, &[1, 2]), filled(&backend, &[3, 4]));
    let alone = Arc::strong_count(&x);
    // The step has returned and its cells are released, and the device may still be reading
    // the inputs: the region holds them. The runner doesn't run it again until its fence has
    // resolved, so that is exactly long enough.
    drop(run(&mut unit, made.inputs.len(), 1, &x, &y));
    assert_eq!(Arc::strong_count(&x), alone + 1, "held by the region");
    assert_eq!(Arc::strong_count(&y), alone + 1);
    // The next run lets go of what the last one read, and holds what it reads itself.
    let (other_x, other_y) = (filled(&backend, &[5, 6]), filled(&backend, &[7, 8]));
    drop(run(&mut unit, made.inputs.len(), 1, &other_x, &other_y));
    assert_eq!(
        (Arc::strong_count(&x), Arc::strong_count(&y)),
        (alone, alone)
    );
    assert_eq!(Arc::strong_count(&other_x), alone + 1);
    // And the step lets go of everything when it goes.
    release(unit.step);
    assert_eq!(Arc::strong_count(&other_x), alone);
}
