//! A software model of an NPU, as a backend. Nothing here touches hardware. It has what a real
//! one has and the core has never heard of: a memory space of its own, a table of transfers to
//! and from host memory, a fixed list of ops it can run, and a fixed cost for each.
//!
//! It is built against `core/backend` and nothing else of Daedalus changes for it, which is
//! what it is here to show. [`FakeNpu`] is the backend linked in. [`PluginNpu`] is the same
//! backend loaded from a dylib plugin.

#![forbid(unsafe_code)]

mod plugin;

use std::borrow::Cow;
use std::sync::atomic::{AtomicU64, Ordering};

use daedalus_abi::{OwnedStep, Slots, Step, StepCx, StepEnd};
use daedalus_backend::{
    Backend, CompileCx, Compiled, MemorySpace, NodeRef, Offer, Refusal, Region, SpaceKind,
    StepUnit, Transfer, TransferCost, UnitInfo,
};
use daedalus_runner::{INLINE_BYTES, put_inline};
use daedalus_types::{NodeIndex, SpaceIndex, TypeIndex};

pub use plugin::PluginNpu;

pub const BACKEND_ID: &str = "fake-npu";
/// The device's own memory. A value a node here reads or writes is in it.
pub const SRAM: &str = "fake-npu:sram";
/// Host memory, as the CPU backend names it.
pub const HOST: &str = "cpu.host";
pub const UPLOAD: &str = "fake-npu.upload";
pub const DOWNLOAD: &str = "fake-npu.download";
/// What the device can run. Anything else is refused when a region is compiled.
pub const OPS: [&str; 3] = ["scale2", "add1", "negate"];
/// What one op costs, in the device's own time.
pub const OP_COST_NS: u64 = 1_000;

static OPS_RUN: AtomicU64 = AtomicU64::new(0);
static UPLOADS: AtomicU64 = AtomicU64::new(0);
static DOWNLOADS: AtomicU64 = AtomicU64::new(0);

/// What the device has done since the process started.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Counters {
    pub ops: u64,
    pub uploads: u64,
    pub downloads: u64,
    /// The device time the ops took: [`OP_COST_NS`] each.
    pub busy_ns: u64,
}

pub fn counters() -> Counters {
    let ops = OPS_RUN.load(Ordering::Relaxed);
    Counters {
        ops,
        uploads: UPLOADS.load(Ordering::Relaxed),
        downloads: DOWNLOADS.load(Ordering::Relaxed),
        busy_ns: ops * OP_COST_NS,
    }
}

/// What an op computes. `None` for an op the device does not have.
pub fn apply(op: &str, x: u32) -> Option<u32> {
    match op {
        "scale2" => Some(x.wrapping_mul(2)),
        "add1" => Some(x.wrapping_add(1)),
        "negate" => Some(x.wrapping_neg()),
        _ => None,
    }
}

/// Reads the number in cell 0, runs `op` on it, and writes the result to cell 1 from `out`,
/// which has to outlive the step. This is the whole of a node on this device.
pub fn run_op(op: &str, slots: &mut Slots, out: &mut [u8; INLINE_BYTES]) -> StepEnd {
    let input = slots
        .cell(0)
        .and_then(|cell| cell.bytes())
        .and_then(|bytes| <[u8; 4]>::try_from(bytes).ok())
        .map(u32::from_le_bytes);
    let Some(input) = input else {
        return StepEnd::Skipped;
    };
    let (Some(result), Some(cell)) = (apply(op, input), slots.cell(1)) else {
        return StepEnd::Fault;
    };
    out[..4].copy_from_slice(&result.to_le_bytes());
    if put_inline(cell, TypeIndex(0), SpaceIndex(0), &out[..4]).is_none() {
        return StepEnd::Fault;
    }
    OPS_RUN.fetch_add(1, Ordering::Relaxed);
    StepEnd::Done
}

struct OpStep {
    op: &'static str,
    node: u32,
    out: [u8; INLINE_BYTES],
}

impl OwnedStep for OpStep {
    fn run(&mut self, slots: &mut Slots, cx: &mut StepCx) -> StepEnd {
        let end = run_op(self.op, slots, &mut self.out);
        if matches!(end, StepEnd::Fault) {
            cx.fault(Some(self.node), "the fake NPU could not run the op");
        }
        end
    }
}

/// A move between host memory and the device's: the bytes are copied, and counted.
struct Move {
    count: &'static AtomicU64,
    held: [u8; INLINE_BYTES],
}

impl OwnedStep for Move {
    fn run(&mut self, slots: &mut Slots, cx: &mut StepCx) -> StepEnd {
        let Some(bytes) = slots.cell(0).and_then(|cell| cell.bytes()) else {
            return StepEnd::Skipped;
        };
        let len = bytes.len();
        if len > INLINE_BYTES {
            cx.fault(None, "the fake NPU moves plain data of 16 bytes or less");
            return StepEnd::Fault;
        }
        self.held[..len].copy_from_slice(bytes);
        let Some(cell) = slots.cell(1) else {
            return StepEnd::Fault;
        };
        if put_inline(cell, TypeIndex(0), SpaceIndex(0), &self.held[..len]).is_none() {
            return StepEnd::Fault;
        }
        self.count.fetch_add(1, Ordering::Relaxed);
        StepEnd::Done
    }
}

fn upload(_: &CompileCx<'_>) -> Result<Step, Refusal> {
    Ok(Step::boxed(Move {
        count: &UPLOADS,
        held: [0; INLINE_BYTES],
    }))
}

fn download(_: &CompileCx<'_>) -> Result<Step, Refusal> {
    Ok(Step::boxed(Move {
        count: &DOWNLOADS,
        held: [0; INLINE_BYTES],
    }))
}

pub(crate) fn memory() -> Vec<MemorySpace> {
    vec![MemorySpace {
        id: Cow::Borrowed(SRAM),
        kind: SpaceKind::Device,
        readable: true,
    }]
}

/// The transfer table: host memory to the device's and back, each a copy.
pub(crate) fn transfers(have: bool) -> Vec<Transfer> {
    if !have {
        return Vec::new();
    }
    vec![
        Transfer {
            id: UPLOAD,
            key: None,
            from: Cow::Borrowed(HOST),
            to: Cow::Borrowed(SRAM),
            cost: TransferCost::Copy,
            make: upload,
        },
        Transfer {
            id: DOWNLOAD,
            key: None,
            from: Cow::Borrowed(SRAM),
            to: Cow::Borrowed(HOST),
            cost: TransferCost::Copy,
            make: download,
        },
    ]
}

pub(crate) fn take_all(offer: &Offer<'_>) -> Vec<NodeRef> {
    offer
        .nodes
        .iter()
        .map(|node| NodeRef {
            node: node.node,
            body: node.body,
        })
        .collect()
}

/// The backend, linked in. A node's body is the name of a model: one of [`OPS`].
#[derive(Clone, Copy, Debug)]
pub struct FakeNpu {
    transfers: bool,
}

impl FakeNpu {
    pub fn new() -> FakeNpu {
        FakeNpu { transfers: true }
    }

    /// The device with an empty transfer table: nothing can reach its memory.
    pub fn without_transfers() -> FakeNpu {
        FakeNpu { transfers: false }
    }
}

impl Default for FakeNpu {
    fn default() -> FakeNpu {
        FakeNpu::new()
    }
}

impl Backend for FakeNpu {
    fn id(&self) -> &str {
        BACKEND_ID
    }

    fn memory(&self) -> Vec<MemorySpace> {
        memory()
    }

    fn transfers(&self) -> Vec<Transfer> {
        transfers(self.transfers)
    }

    /// Every node with a body for this backend is taken. An offer does not show a body's text,
    /// so an op the device lacks is refused when its region is compiled, and the planner then
    /// assigns that node again without this backend.
    fn can_take(&self, offer: &Offer<'_>) -> Vec<NodeRef> {
        take_all(offer)
    }

    fn compile(&self, region: &Region<'_>, cx: &CompileCx<'_>) -> Result<Compiled, Refusal> {
        let body = |node: NodeIndex| cx.bodies.iter().find(|body| body.node == node);
        let unknown: Vec<NodeIndex> = region
            .nodes
            .iter()
            .copied()
            .filter(|node| !body(*node).is_some_and(|body| OPS.contains(&body.text)))
            .collect();
        if let Some(first) = unknown.first() {
            let op = body(*first).map_or("", |body| body.text);
            return Err(Refusal::new(
                unknown,
                &format!("the fake NPU has no op `{op}`: it runs {}", OPS.join(", ")),
            ));
        }
        // One step a node, each naming its node's own ports: the region's boundary, and both
        // ends of every edge inside it.
        let ports = |node: NodeIndex,
                     boundary: &[daedalus_backend::PortRef],
                     inner: Vec<daedalus_backend::PortRef>| {
            let mut ports: Vec<_> = boundary
                .iter()
                .copied()
                .chain(inner)
                .filter(|port| port.node == node)
                .collect();
            ports.sort_by_key(|port| port.port.0);
            ports.dedup();
            ports
        };
        let units = region
            .nodes
            .iter()
            .map(|&node| {
                let op = OPS
                    .iter()
                    .copied()
                    .find(|op| body(node).is_some_and(|body| body.text == *op))
                    .unwrap_or(OPS[0]);
                StepUnit {
                    step: Step::boxed(OpStep {
                        op,
                        node: node.0,
                        out: [0; INLINE_BYTES],
                    }),
                    info: UnitInfo {
                        covers: vec![node],
                        reads: ports(
                            node,
                            region.inputs,
                            cx.edges.iter().map(|edge| edge.to).collect(),
                        ),
                        writes: ports(
                            node,
                            region.outputs,
                            cx.edges.iter().map(|edge| edge.from).collect(),
                        ),
                        absorbed: Vec::new(),
                        may_pend: false,
                    },
                }
            })
            .collect();
        Ok(Compiled::Steps(units))
    }
}
