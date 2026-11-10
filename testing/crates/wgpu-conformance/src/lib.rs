//! What the wgpu backend's conformance tests and the GPU budgets share: a device of a test's
//! own, a region built by hand, and a run that says what the device was asked to do.
//!
//! The cases are in `tests/`, and they need a real adapter. The lavapipe lane
//! (`scripts/gpu-lavapipe.sh`) runs them with `DAEDALUS_GPU_REQUIRED=1`, where no adapter is a
//! failure and not a skip.

#![forbid(unsafe_code)]

use std::any::Any;
use std::sync::Arc;

use daedalus_abi::counting::counted;
use daedalus_abi::testing::run_once;
use daedalus_abi::{Slot, Slots, StepCx, StepEnd};
use daedalus_backend::{
    Backend, BodyIndex, CompileCx, Compiled, InnerEdge, NO_METADATA, NodeBody, PortRef, RateId,
    Refusal, Region, StepUnit,
};
use daedalus_graph::EdgePolicyDoc;
use daedalus_runner::{drop_owned, put_inline, put_typed, take_typed};
use daedalus_types::{BackendIndex, NodeIndex, PortIndex, SpaceIndex, TypeIndex};
use daedalus_wgpu::shader::{BindingKind, BindingSpec, Dispatch, ShaderSpec};
use daedalus_wgpu::{Options, Storage, WgpuBackend};

/// A backend with a device of its own, so what it counts is this caller's alone. `None` when
/// the machine has no adapter, unless `DAEDALUS_GPU_REQUIRED` is set, where that is a failure.
pub fn private() -> Option<WgpuBackend> {
    let shared = WgpuBackend::open(Options::default());
    let record = match shared.start() {
        Ok(record) => record,
        Err(error) => {
            assert!(
                std::env::var_os("DAEDALUS_GPU_REQUIRED").is_none(),
                "this lane needs an adapter: {error}"
            );
            return None;
        }
    };
    let backend = WgpuBackend::open(Options {
        adapter: Some(record.adapter.name),
        ..Options::default()
    });
    backend.start().expect("the adapter that just opened");
    Some(backend)
}

pub fn port(node: u32, port: u16) -> PortRef {
    PortRef {
        node: NodeIndex(node),
        port: PortIndex(port),
    }
}

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

const UNARY: &[BindingSpec] = &[storage(0, "src", true), storage(1, "dst", false)];

const fn unary(name: &'static str, source: &'static str) -> ShaderSpec {
    ShaderSpec {
        name,
        entry: "main",
        source,
        workgroup: [64, 1, 1],
        bindings: UNARY,
        dispatch: Dispatch::PerElement {
            binding: 1,
            stride: 4,
        },
        inputs: &[0],
        outputs: &[1],
    }
}

/// A whole shader, `dst = src * 2 + 1`, word by word. It reads its neighbour's bounds, so it
/// is never merged.
pub const AFFINE: ShaderSpec = unary(
    "conformance.affine",
    "
    @group(0) @binding(0) var<storage, read> src: array<u32>;
    @group(0) @binding(1) var<storage, read_write> dst: array<u32>;
    @compute @workgroup_size(64)
    fn main(@builtin(global_invocation_id) id: vec3<u32>) {
        if id.x < arrayLength(&dst) { dst[id.x] = src[id.x] * 2u + 1u; }
    }
",
);

/// A shader with no bindings and so no ports: what the backend rules compile, which offer
/// nodes with no payload to bind.
pub const IDLE: ShaderSpec = ShaderSpec {
    name: "conformance.idle",
    entry: "main",
    source: "@compute @workgroup_size(1) fn main() {}",
    workgroup: [1, 1, 1],
    bindings: &[],
    dispatch: Dispatch::Counts([1, 1, 1]),
    inputs: &[],
    outputs: &[],
};

/// A region under construction: nodes in order, the edges between them, and its inputs.
#[derive(Default)]
pub struct Net {
    bodies: Vec<NodeBody<'static>>,
    pub edges: Vec<(PortRef, PortRef)>,
    nodes: Vec<NodeIndex>,
    pub inputs: Vec<PortRef>,
}

impl Net {
    /// Adds a node whose body is `code`, with this many ports, and returns its number.
    pub fn add(
        &mut self,
        id: &'static str,
        code: &'static (dyn Any + Send + Sync),
        inputs: u16,
        outputs: u16,
    ) -> u32 {
        let node = self.nodes.len() as u32;
        self.bodies.push(NodeBody {
            node: NodeIndex(node),
            id,
            body: BodyIndex(0),
            inputs,
            outputs,
            merges: true,
            fire_all: true,
            label: None,
            metadata: &NO_METADATA,
            code,
            text: "",
        });
        self.nodes.push(NodeIndex(node));
        node
    }

    /// A line of one-in, one-out nodes, each fed by the one before. The first node's input is
    /// the region's.
    pub fn line(codes: &[&'static (dyn Any + Send + Sync)]) -> Net {
        let mut net = Net::default();
        for code in codes {
            let node = net.add("conformance.node", *code, 1, 1);
            match node {
                0 => net.inputs.push(port(0, 0)),
                _ => net.edges.push((port(node - 1, 0), port(node, 0))),
            }
        }
        net
    }

    pub fn nodes(&self) -> &[NodeIndex] {
        &self.nodes
    }

    /// Compiles the region with these outputs into its one step.
    pub fn compile(&self, backend: &WgpuBackend, outputs: &[PortRef]) -> Result<StepUnit, Refusal> {
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
        match backend.compile(&region, &cx)? {
            Compiled::Steps(mut units) if units.len() == 1 => Ok(units.remove(0)),
            _ => Err(Refusal::new(self.nodes.clone(), "a region is one step")),
        }
    }
}

/// What goes in one input cell.
pub enum In<'a> {
    Buffer(&'a Arc<Storage>),
    Word(u32),
}

/// What one run of a step asked of the device and of the allocator.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Counted {
    pub submits: u64,
    pub passes: u64,
    pub pipelines: u64,
    pub uploads: u64,
    pub downloads: u64,
    pub allocations: u64,
}

/// What the device was asked between `before` and now.
fn since(backend: &WgpuBackend, before: &Counted, allocations: usize) -> Counted {
    let now = snapshot(backend);
    Counted {
        submits: now.submits - before.submits,
        passes: now.passes - before.passes,
        pipelines: now.pipelines - before.pipelines,
        uploads: now.uploads - before.uploads,
        downloads: now.downloads - before.downloads,
        allocations: allocations as u64,
    }
}

fn snapshot(backend: &WgpuBackend) -> Counted {
    let shaders = backend.shader_stats().expect("an open device");
    let moved = backend.transfer_stats().expect("an open device");
    Counted {
        submits: shaders.submits,
        passes: shaders.passes,
        pipelines: shaders.pipelines,
        uploads: moved.uploads,
        downloads: moved.downloads,
        allocations: 0,
    }
}

/// Counts what `body` asks of `backend`'s device and of the allocator. The allocations are
/// zero unless the test binary installs the counting allocator.
pub fn measure<R>(backend: &WgpuBackend, body: impl FnOnce() -> R) -> (R, Counted) {
    let before = snapshot(backend);
    let (out, allocations) = counted(body);
    (out, since(backend, &before, allocations))
}

/// How a run ended, the buffers it put in its output cells, and what it cost.
pub struct Ran {
    pub end: StepEnd,
    pub reason: String,
    pub at: u32,
    pub outputs: Vec<Option<Arc<Storage>>>,
    pub counted: Counted,
}

/// Runs `unit` once on `inputs`, with `outputs` cells to write.
pub fn run(backend: &WgpuBackend, unit: &mut StepUnit, inputs: &[In<'_>], outputs: usize) -> Ran {
    let (ty, space) = (TypeIndex(0), SpaceIndex(0));
    let mut cells = vec![Slot::EMPTY; inputs.len() + outputs];
    for (cell, input) in cells.iter_mut().zip(inputs) {
        match input {
            In::Buffer(storage) => put_typed(cell, ty, space, Arc::clone(storage)),
            In::Word(word) => put_inline(cell, ty, space, &word.to_le_bytes()).expect("fits"),
        }
    }
    let mut cx = StepCx::new();
    let (end, counted) = measure(backend, || {
        run_once(&mut unit.step, &mut Slots::of(&mut cells), &mut cx)
    });
    let (read, written) = cells.split_at_mut(inputs.len());
    for cell in read {
        if take_typed::<Storage>(cell).is_none() {
            drop_owned(*cell);
        }
    }
    Ran {
        end,
        reason: cx.reason().to_string(),
        at: cx.at,
        outputs: written.iter_mut().map(take_typed::<Storage>).collect(),
        counted,
    }
}

/// `words` in a device buffer.
pub fn filled(backend: &WgpuBackend, words: &[u32]) -> Arc<Storage> {
    let storage = Storage::new(backend, words.len() as u64 * 4).expect("a buffer");
    let bytes: Vec<u8> = words.iter().flat_map(|word| word.to_le_bytes()).collect();
    storage.write(0, &bytes).expect("in range");
    Arc::new(storage)
}

/// A buffer's words. This waits for the device, so the work before it is done.
pub fn words(storage: &Storage) -> Vec<u32> {
    let mut bytes = vec![0; storage.size() as usize];
    storage.read_into(0, &mut bytes).expect("a readback");
    bytes
        .as_chunks::<4>()
        .0
        .iter()
        .map(|word| u32::from_le_bytes(*word))
        .collect()
}
