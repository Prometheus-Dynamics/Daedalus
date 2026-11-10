//! Per-pixel bodies as an author writes them, merged by the backend when it compiles a region:
//! which nodes become one shader, where a chain is cut, and that the merged shader gives what
//! the nodes give one at a time.

use std::any::Any;
use std::sync::Arc;

use daedalus_abi::testing::{release, run_once};
use daedalus_abi::{Slot, Slots, StepCx, StepEnd};
use daedalus_backend::{
    Backend, BodyIndex, CompileCx, Compiled, InnerEdge, NO_METADATA, NodeBody, PortRef, RateId,
    Region, StepUnit,
};
use daedalus_graph::EdgePolicyDoc;
use daedalus_macros::node;
use daedalus_registry::NodeRecord;
use daedalus_runner::{drop_owned, put_inline, put_typed, take_typed};
use daedalus_types::{BackendIndex, NodeIndex, PortIndex, SpaceIndex, TypeIndex};
use daedalus_wgpu::merge::{Form, PixelSpec, chain_key, fragment_pass, source_hash};
use daedalus_wgpu::{Format, Options, Storage, Texture, TextureDesc, WgpuBackend};

#[node(id = "p.inc", inputs(c), outputs(out), backend = daedalus_wgpu_author(pixel, c: u32, out: u32))]
const INC: &str = "
    const STEP: u32 = 1u;
    fn helper(x: u32) -> u32 { return x + STEP; }
    fn apply(c: u32) -> u32 { return helper(c); }
";

/// The same top-level names as `INC`, meaning other things: the bodies are put side by side.
#[node(id = "p.triple", inputs(c), outputs(out), backend = daedalus_wgpu_author(pixel, c: u32, out: u32))]
const TRIPLE: &str = "
    const STEP: u32 = 3u;
    fn helper(x: u32) -> u32 { return x * STEP; }
    fn apply(c: u32) -> u32 { return helper(c); }
";

#[node(id = "p.mask", inputs(c, cfg), outputs(out), backend = daedalus_wgpu_author(pixel, c: u32, cfg: u32, out: u32))]
const MASK: &str = "
    struct Config { mask: u32 }
    fn apply(c: u32, cfg: Config) -> u32 { return c ^ cfg.mask; }
";

#[node(id = "p.plus", inputs(c, other), outputs(out), backend = daedalus_wgpu_author(pixel, c: u32, other: u32, out: u32))]
const PLUS: &str = "fn apply(c: u32, other: u32) -> u32 { return c + other; }";

/// Reads its neighbour, so it is a whole shader and can't join a chain.
#[node(id = "p.neighbour", inputs(src), outputs(dst), backend = daedalus_wgpu_author(src: u32, dst: u32))]
const NEIGHBOUR: &str = "
    @group(0) @binding(0) var<storage, read> src: array<u32>;
    @group(0) @binding(1) var<storage, read_write> dst: array<u32>;
    @compute @workgroup_size(64)
    fn main(@builtin(global_invocation_id) id: vec3<u32>) {
        let n = arrayLength(&dst);
        if id.x < n { dst[id.x] = src[id.x] + src[(id.x + 1u) % n]; }
    }
";

#[node(id = "p.inc4", inputs(c), outputs(out), backend = daedalus_wgpu_author(pixel, c: u32, out: u32))]
const INC4: &str = "fn apply(c: vec4<u32>) -> vec4<u32> { return c + vec4<u32>(1u, 2u, 3u, 4u); }";

#[node(id = "p.twice4", inputs(c), outputs(out), backend = daedalus_wgpu_author(pixel, c: u32, out: u32))]
const TWICE4: &str = "fn apply(c: vec4<u32>) -> vec4<u32> { return c * 2u; }";

fn port(node: u32, port: u16) -> PortRef {
    PortRef {
        node: NodeIndex(node),
        port: PortIndex(port),
    }
}

/// A region under construction: nodes in order, the edges between them, and its boundary.
#[derive(Default)]
struct Net {
    bodies: Vec<NodeBody<'static>>,
    edges: Vec<(PortRef, PortRef)>,
    nodes: Vec<NodeIndex>,
    inputs: Vec<PortRef>,
}

impl Net {
    /// Adds a node and returns its number.
    fn add(&mut self, record: &'static NodeRecord) -> u32 {
        let node = self.nodes.len() as u32;
        self.bodies.push(NodeBody {
            node: NodeIndex(node),
            id: record.decl.id,
            body: BodyIndex(0),
            inputs: record.decl.inputs.len() as u16,
            outputs: record.decl.outputs.len() as u16,
            merges: true,
            fire_all: true,
            label: None,
            metadata: &NO_METADATA,
            code: record.body.code,
            text: record.body.text,
        });
        self.nodes.push(NodeIndex(node));
        node
    }

    /// A chain of nodes, each fed on its first port by the one before. The first node's first
    /// port is a region input.
    fn chain(records: &[&'static NodeRecord]) -> Net {
        let mut net = Net::default();
        for record in records {
            let node = net.add(record);
            match node {
                0 => net.inputs.push(port(0, 0)),
                _ => net.edges.push((port(node - 1, 0), port(node, 0))),
            }
        }
        net
    }

    fn compile(&self, backend: &WgpuBackend, outputs: &[PortRef]) -> StepUnit {
        let policy = EdgePolicyDoc::default();
        let edges: Vec<InnerEdge<'_>> = self
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
            nodes: &self.nodes,
            inputs: &self.inputs,
            outputs,
            incoming: &[],
            rate: RateId(0),
        };
        let cx = CompileCx {
            types: &[],
            consts: &[],
            spaces: &[],
            bodies: &self.bodies,
            edges: &edges,
            host_inputs: &[],
            host_outputs: &[],
            fifo_capacity: 1,
            merge_allowed: true,
            freeze_params: false,
        };
        let Compiled::Steps(mut units) = backend.compile(&region, &cx).expect("compiles") else {
            panic!("steps");
        };
        assert_eq!(
            units[0].info.covers, self.nodes,
            "the step still covers every node"
        );
        units.remove(0)
    }
}

fn backend() -> Option<WgpuBackend> {
    let shared = WgpuBackend::open(Options::default());
    let record = match shared.start() {
        Ok(record) => record,
        Err(error) => {
            assert!(
                std::env::var_os("DAEDALUS_GPU_REQUIRED").is_none(),
                "{error}"
            );
            return None;
        }
    };
    // A device of this test's own, so the pass and pipeline counters are its alone.
    Some(WgpuBackend::open(Options {
        adapter: Some(record.adapter.name),
        ..Options::default()
    }))
}

fn filled(backend: &WgpuBackend, words: &[u32]) -> Arc<Storage> {
    let storage = Storage::new(backend, words.len() as u64 * 4).expect("a buffer");
    let bytes: Vec<u8> = words.iter().flat_map(|word| word.to_le_bytes()).collect();
    storage.write(0, &bytes).expect("in range");
    Arc::new(storage)
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

/// What goes in one input cell.
enum In<'a> {
    Buffer(&'a Arc<Storage>),
    Word(u32),
}

/// Runs the step once and returns its outputs, with how many passes and submits it took.
fn run(
    backend: &WgpuBackend,
    unit: &mut StepUnit,
    inputs: &[In<'_>],
    outputs: usize,
) -> (Vec<Vec<u32>>, u64, u64) {
    // `put_inline` keeps a pointer to the bytes it is given, so they live until the run is over.
    let held: Vec<[u8; 4]> = inputs
        .iter()
        .map(|input| match input {
            In::Word(word) => word.to_le_bytes(),
            In::Buffer(_) => [0; 4],
        })
        .collect();
    let mut cells = vec![Slot::EMPTY; inputs.len() + outputs];
    for ((cell, input), word) in cells.iter_mut().zip(inputs).zip(&held) {
        match input {
            In::Buffer(storage) => {
                put_typed(cell, TypeIndex(0), SpaceIndex(0), Arc::clone(storage))
            }
            In::Word(_) => {
                put_inline(cell, TypeIndex(0), SpaceIndex(0), word).expect("fits");
            }
        }
    }
    let before = backend.shader_stats().expect("open");
    let mut cx = StepCx::new();
    let end = run_once(&mut unit.step, &mut Slots::of(&mut cells), &mut cx);
    assert!(matches!(end, StepEnd::Pending(_)), "{}", cx.reason());
    let after = backend.shader_stats().expect("open");
    let (read, written) = cells.split_at_mut(inputs.len());
    for cell in read {
        if take_typed::<Storage>(cell).is_none() {
            drop_owned(*cell);
        }
    }
    let out = written
        .iter_mut()
        .map(|cell| words(&take_typed::<Storage>(cell).expect("an output on the device")))
        .collect();
    (
        out,
        after.passes - before.passes,
        after.submits - before.submits,
    )
}

fn spec(record: &'static NodeRecord) -> &'static PixelSpec {
    (record.body.code as &dyn Any)
        .downcast_ref()
        .expect("a pixel body")
}

#[test]
fn the_record_holds_the_function_and_what_it_takes() {
    let mask = spec(&MASK_NODE);
    assert_eq!((mask.name, mask.input, mask.output), ("MASK", "u32", "u32"));
    assert_eq!((mask.input_stride, mask.output_stride), (4, 4));
    assert_eq!(mask.config, Some(("Config", 4)));
    assert_eq!(mask.params, ["c", "cfg"]);
    assert_eq!(
        mask.inputs,
        [0, 1],
        "each port feeds the parameter of its name"
    );
    assert_eq!(mask.names, ["Config", "apply"]);
    assert_eq!(mask.source_hash, source_hash(MASK));
    let plus = spec(&PLUS_NODE);
    assert_eq!(
        (plus.sides.len(), plus.sides[0].name, plus.sides[0].stride),
        (1, "other", 4)
    );
    assert_eq!(spec(&INC_NODE).names, ["STEP", "apply", "helper"]);
    assert_eq!(
        (MASK_NODE.body.backend, MASK_NODE.body.text),
        ("wgpu", MASK)
    );
}

#[test]
fn a_chain_of_four_is_one_pass_and_equals_the_nodes_run_one_at_a_time() {
    let Some(backend) = backend() else { return };
    let records: [&'static NodeRecord; 4] = [&INC_NODE, &TRIPLE_NODE, &INC_NODE, &TRIPLE_NODE];
    let input: Vec<u32> = (0..1000).map(|at| at * 37 + 5).collect();
    let x = filled(&backend, &input);

    // Merged: the two bodies' `helper` and `STEP` don't clash, and nothing is written between.
    let net = Net::chain(&records);
    let mut unit = net.compile(&backend, &[port(3, 0)]);
    let pipelines = backend.shader_stats().expect("open").pipelines;
    let (merged, passes, submits) = run(&backend, &mut unit, &[In::Buffer(&x)], 1);
    assert_eq!((passes, submits), (1, 1), "four nodes, one pass");
    release(unit.step);

    // One at a time: each node a region of its own, and its output fed to the next.
    let mut value = Arc::clone(&x);
    for record in records {
        let alone = Net::chain(&[record]);
        let mut unit = alone.compile(&backend, &[port(0, 0)]);
        let (out, passes, _) = run(&backend, &mut unit, &[In::Buffer(&value)], 1);
        assert_eq!(passes, 1);
        value = filled(&backend, &out[0]);
        release(unit.step);
    }
    assert_eq!(merged[0], words(&value), "bit for bit");
    let by_hand: Vec<u32> = input.iter().map(|x| ((x + 1) * 3 + 1) * 3).collect();
    assert_eq!(merged[0], by_hand);

    // The same chain compiled again is the same pipeline.
    let again = Net::chain(&records).compile(&backend, &[port(3, 0)]);
    assert_eq!(
        backend.shader_stats().expect("open").pipelines,
        pipelines + 2
    );
    let once_more = Net::chain(&records).compile(&backend, &[port(3, 0)]);
    assert_eq!(
        backend.shader_stats().expect("open").pipelines,
        pipelines + 2,
        "found, not built"
    );
    release(again.step);
    release(once_more.step);
}

#[test]
fn a_node_that_reads_its_neighbour_cuts_the_chain_around_it() {
    let Some(backend) = backend() else { return };
    let net = Net::chain(&[
        &INC_NODE,
        &TRIPLE_NODE,
        &NEIGHBOUR_NODE,
        &INC_NODE,
        &TRIPLE_NODE,
    ]);
    let mut unit = net.compile(&backend, &[port(4, 0)]);
    let input = [1u32, 2, 3, 4, 5];
    let (out, passes, submits) = run(
        &backend,
        &mut unit,
        &[In::Buffer(&filled(&backend, &input))],
        1,
    );
    assert_eq!(
        (passes, submits),
        (3, 1),
        "two merged pairs and the whole shader between them"
    );
    let first: Vec<u32> = input.iter().map(|x| (x + 1) * 3).collect();
    let want: Vec<u32> = (0..5)
        .map(|at| (first[at] + first[(at + 1) % 5] + 1) * 3)
        .collect();
    assert_eq!(out[0], want);
    release(unit.step);
}

#[test]
fn a_second_consumer_ends_the_chain() {
    let Some(backend) = backend() else { return };
    let net = Net::chain(&[&INC_NODE, &TRIPLE_NODE, &INC_NODE]);
    // The second node's output also leaves the region, so it has to be in a buffer.
    let mut unit = net.compile(&backend, &[port(1, 0), port(2, 0)]);
    let x = filled(&backend, &[10, 20, 30]);
    let (out, passes, _) = run(&backend, &mut unit, &[In::Buffer(&x)], 2);
    assert_eq!(passes, 2, "the first two merge, and the third runs alone");
    assert_eq!(out[0], [33, 63, 93]);
    assert_eq!(out[1], [34, 64, 94]);
    release(unit.step);

    // A second consumer inside the region ends it too.
    let mut net = Net::chain(&[&INC_NODE, &TRIPLE_NODE]);
    let plus = net.add(&PLUS_NODE);
    net.edges.push((port(1, 0), port(plus, 0)));
    net.edges.push((port(0, 0), port(plus, 1)));
    let mut unit = net.compile(&backend, &[port(plus, 0)]);
    let (out, passes, _) = run(&backend, &mut unit, &[In::Buffer(&x)], 1);
    assert_eq!(
        passes, 2,
        "the first node's output is read twice, so it stands alone"
    );
    assert_eq!(out[0], [44, 84, 124], "(x + 1) * 3 + (x + 1)");
    release(unit.step);
}

#[test]
fn a_long_chain_is_cut_into_pieces_that_each_fit_any_device() {
    let Some(backend) = backend() else { return };
    // Nine nodes, each with a buffer of its own beside the pixel.
    let mut net = Net::default();
    for at in 0..9 {
        let node = net.add(&PLUS_NODE);
        if at == 0 {
            net.inputs.push(port(0, 0));
        } else {
            net.edges.push((port(node - 1, 0), port(node, 0)));
        }
    }
    for node in 0..9 {
        net.inputs.push(port(node, 1));
    }
    let mut unit = net.compile(&backend, &[port(8, 0)]);
    let x = filled(&backend, &[100, 200]);
    let sides: Vec<Arc<Storage>> = (1..=9).map(|at| filled(&backend, &[at, at * 10])).collect();
    let mut inputs = vec![In::Buffer(&x)];
    inputs.extend(sides.iter().map(In::Buffer));
    let (out, passes, submits) = run(&backend, &mut unit, &inputs, 1);
    // Two for the chain and one for each side: six nodes make eight storage buffers.
    assert_eq!((passes, submits), (2, 1), "six nodes, then three");
    assert_eq!(out[0], [145, 650]);
    release(unit.step);
}

#[test]
fn a_config_is_a_port_or_zeroes() {
    let Some(backend) = backend() else { return };
    let x = filled(&backend, &[0b1100, 0b1010]);
    // Fed by a port, as a small value from a CPU node.
    let mut net = Net::chain(&[&INC_NODE, &MASK_NODE, &TRIPLE_NODE]);
    net.inputs.push(port(1, 1));
    let mut unit = net.compile(&backend, &[port(2, 0)]);
    let (out, passes, _) = run(&backend, &mut unit, &[In::Buffer(&x), In::Word(0b0110)], 1);
    assert_eq!(passes, 1, "a config doesn't end a chain");
    assert_eq!(out[0], [(0b1101 ^ 0b0110) * 3, (0b1011 ^ 0b0110) * 3]);
    release(unit.step);
    // With nothing connected the config is zeroes.
    let net = Net::chain(&[&INC_NODE, &MASK_NODE]);
    let mut unit = net.compile(&backend, &[port(1, 0)]);
    let (out, _, _) = run(&backend, &mut unit, &[In::Buffer(&x)], 1);
    assert_eq!(out[0], [0b1101, 0b1011]);
    release(unit.step);
}

#[test]
fn the_chain_key_is_pinned() {
    // FNV-1a over each node's name and source hash, the binding table, the workgroup size and
    // the form. A change here changes every merged pipeline's identity, so it has to be meant.
    let chain = [spec(&INC_NODE), spec(&TRIPLE_NODE)];
    assert_eq!(chain_key(&chain, Form::Compute), 0x219a_70a7_16f9_a453);
    assert_ne!(
        chain_key(&chain, Form::Compute),
        chain_key(&chain, Form::Fragment)
    );
    let other = [spec(&TRIPLE_NODE), spec(&INC_NODE)];
    assert_ne!(
        chain_key(&chain, Form::Compute),
        chain_key(&other, Form::Compute),
        "order counts"
    );
}

#[test]
fn the_fragment_form_gives_what_the_compute_form_gives() {
    let Some(backend) = backend() else { return };
    let records: [&'static NodeRecord; 4] = [&INC4_NODE, &TWICE4_NODE, &INC4_NODE, &TWICE4_NODE];
    let (width, height) = (9u32, 5u32);
    let pixels: Vec<[u32; 4]> = (0..width * height)
        .map(|at| [at, at * 2 + 1, 7, at % 5])
        .collect();

    // The compute form, through a region, on a buffer of the same pixels.
    let flat: Vec<u32> = pixels.iter().flatten().copied().collect();
    let mut unit = Net::chain(&records).compile(&backend, &[port(3, 0)]);
    let (compute, passes, _) = run(
        &backend,
        &mut unit,
        &[In::Buffer(&filled(&backend, &flat))],
        1,
    );
    assert_eq!(passes, 1);
    release(unit.step);
    let by_hand: Vec<u32> = pixels
        .iter()
        .flat_map(|pixel| {
            let add = [1, 2, 3, 4];
            std::array::from_fn::<u32, 4, _>(|at| ((pixel[at] + add[at]) * 2 + add[at]) * 2)
        })
        .collect();
    assert_eq!(compute[0], by_hand);

    // The fragment form: one render pass over a texture of them.
    let chain: Vec<&PixelSpec> = records.iter().map(|record| spec(record)).collect();
    let pass = fragment_pass(&backend, &chain, Format::Rgba16Uint).expect("a pass");
    let desc = TextureDesc {
        format: Format::Rgba16Uint,
        width,
        height,
    };
    let (source, target) = (
        Texture::new(&backend, desc).unwrap(),
        Texture::new(&backend, desc).unwrap(),
    );
    let texels: Vec<u8> = flat
        .iter()
        .flat_map(|&word| (word as u16).to_le_bytes())
        .collect();
    source.write(&texels, width * 8).expect("valid bytes");
    let before = backend.shader_stats().expect("open");
    let binding = pass.bind(&[&source], &[]).expect("binds");
    pass.run_bound(&binding, &[&target])
        .expect("draws")
        .wait()
        .expect("finishes");
    let after = backend.shader_stats().expect("open");
    assert_eq!(
        (after.passes - before.passes, after.submits - before.submits),
        (1, 1)
    );
    let mut read = vec![0; texels.len()];
    target.read_into(&mut read).expect("a readback");
    let fragment: Vec<u32> = read
        .as_chunks::<2>()
        .0
        .iter()
        .map(|word| u32::from(u16::from_le_bytes(*word)))
        .collect();
    assert_eq!(fragment, compute[0], "bit for bit");

    // A chain of another pixel type can't be drawn to this target.
    let wrong = fragment_pass(&backend, &[spec(&INC_NODE)], Format::Rgba16Uint);
    assert!(wrong.is_err());
}

#[node(id = "p.mask4", inputs(c, cfg), outputs(out), backend = daedalus_wgpu_author(pixel, c: u32, cfg: u32, out: u32))]
const MASK4: &str = "
    struct Config { mask: u32 }
    fn apply(c: vec4<u32>, cfg: Config) -> vec4<u32> { return c ^ vec4<u32>(cfg.mask); }
";

#[node(id = "p.plus4", inputs(c, other), outputs(out), backend = daedalus_wgpu_author(pixel, c: u32, other: u32, out: u32))]
const PLUS4: &str = "fn apply(c: vec4<u32>, other: vec4<u32>) -> vec4<u32> { return c + other; }";

fn texture_of(backend: &WgpuBackend, desc: TextureDesc, words: &[u32]) -> Arc<Texture> {
    let texture = Texture::new(backend, desc).expect("a texture");
    let texels: Vec<u8> = words
        .iter()
        .flat_map(|&word| (word as u16).to_le_bytes())
        .collect();
    texture.write(&texels, desc.width * 8).expect("valid bytes");
    Arc::new(texture)
}

fn texels(texture: &Texture) -> Vec<u32> {
    let desc = texture.desc();
    let mut read = vec![0; (desc.width * desc.height * 8) as usize];
    texture.read_into(&mut read).expect("a readback");
    read.as_chunks::<2>()
        .0
        .iter()
        .map(|word| u32::from(u16::from_le_bytes(*word)))
        .collect()
}

/// Runs a step whose first inputs are textures and whose last is a config word, and returns
/// the texture it made with how many passes that took.
fn draw(
    backend: &WgpuBackend,
    unit: &mut StepUnit,
    textures: &[&Arc<Texture>],
    word: Option<u32>,
) -> (Arc<Texture>, u64) {
    let inputs = textures.len() + usize::from(word.is_some());
    let mut cells = vec![Slot::EMPTY; inputs + 1];
    for (cell, texture) in cells.iter_mut().zip(textures) {
        put_typed(cell, TypeIndex(0), SpaceIndex(0), Arc::clone(texture));
    }
    // `put_inline` keeps a pointer to the bytes it is given, so they live until the run is over.
    let word = word.map(u32::to_le_bytes);
    if let Some(word) = &word {
        put_inline(
            &mut cells[textures.len()],
            TypeIndex(0),
            SpaceIndex(0),
            word,
        )
        .expect("fits");
    }
    let before = backend.shader_stats().expect("open");
    let mut cx = StepCx::new();
    let end = run_once(&mut unit.step, &mut Slots::of(&mut cells), &mut cx);
    assert!(matches!(end, StepEnd::Pending(_)), "{}", cx.reason());
    let after = backend.shader_stats().expect("open");
    assert_eq!(after.submits - before.submits, 1);
    let (read, written) = cells.split_at_mut(inputs);
    for cell in read {
        if take_typed::<Texture>(cell).is_none() {
            drop_owned(*cell);
        }
    }
    let out = take_typed::<Texture>(&mut written[0]).expect("a texture out");
    (out, after.passes - before.passes)
}

#[test]
fn a_chain_given_a_texture_draws_one() {
    let Some(backend) = backend() else { return };
    let records: [&'static NodeRecord; 4] = [&INC4_NODE, &MASK4_NODE, &PLUS4_NODE, &TWICE4_NODE];
    let desc = TextureDesc {
        format: Format::Rgba16Uint,
        width: 9,
        height: 5,
    };
    let count = desc.width * desc.height * 4;
    let pixels: Vec<u32> = (0..count).map(|at| at * 3 + 1).collect();
    let others: Vec<u32> = (0..count).map(|at| 1000 - at).collect();
    let mask = 0b0101_1010;
    let by_hand = |mask: u32| -> Vec<u32> {
        let add = [1, 2, 3, 4];
        pixels
            .iter()
            .zip(&others)
            .enumerate()
            .map(|(at, (pixel, other))| (((pixel + add[at % 4]) ^ mask) + other) * 2)
            .collect()
    };

    // Inputs: the chain's pixels, the mask's config, the other side of the sum.
    let net = || {
        let mut net = Net::chain(&records);
        net.inputs.push(port(2, 1));
        net.inputs.push(port(1, 1));
        net
    };
    let mut unit = net().compile(&backend, &[port(3, 0)]);
    let (source, other) = (
        texture_of(&backend, desc, &pixels),
        texture_of(&backend, desc, &others),
    );
    let (first, passes) = draw(&backend, &mut unit, &[&source, &other], Some(mask));
    assert_eq!(passes, 1, "four nodes, one render pass");
    assert_eq!(first.desc(), desc);
    assert_eq!(texels(&first), by_hand(mask), "what the nodes would give");

    // The same step, with buffers, is the compute form and gives the same numbers.
    let (compute, passes, _) = run(
        &backend,
        &mut unit,
        &[
            In::Buffer(&filled(&backend, &pixels)),
            In::Buffer(&filled(&backend, &others)),
            In::Word(mask),
        ],
        1,
    );
    assert_eq!(passes, 1);
    assert_eq!(compute[0], texels(&first), "bit for bit");

    // A texture still held isn't drawn over; one given back is drawn to again.
    let (second, _) = draw(&backend, &mut unit, &[&source, &other], Some(0));
    assert_eq!(texels(&second), by_hand(0), "the config is read each run");
    assert_eq!(texels(&first), by_hand(mask), "the held one is untouched");
    let kept = Arc::as_ptr(&first);
    drop(first);
    let (third, _) = draw(&backend, &mut unit, &[&source, &other], Some(mask));
    assert_eq!(Arc::as_ptr(&third), kept, "drawn to again, not made again");
    assert_eq!(texels(&third), by_hand(mask));

    // A texture for the pixels and a buffer for the other side is refused, by name.
    let mut cells = vec![Slot::EMPTY; 4];
    put_typed(
        &mut cells[0],
        TypeIndex(0),
        SpaceIndex(0),
        Arc::clone(&source),
    );
    put_typed(
        &mut cells[1],
        TypeIndex(0),
        SpaceIndex(0),
        filled(&backend, &others),
    );
    let mask_bytes = mask.to_le_bytes();
    put_inline(&mut cells[2], TypeIndex(0), SpaceIndex(0), &mask_bytes).expect("fits");
    let mut cx = StepCx::new();
    let end = run_once(&mut unit.step, &mut Slots::of(&mut cells), &mut cx);
    assert!(matches!(end, StepEnd::Fault), "a mixed chain");
    assert!(cx.reason().contains("textures"), "{}", cx.reason());
    assert!(take_typed::<Texture>(&mut cells[0]).is_some());
    assert!(take_typed::<Storage>(&mut cells[1]).is_some());
    drop_owned(cells[2]);
    drop((second, third));
    release(unit.step);
}
