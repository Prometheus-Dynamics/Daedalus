//! An author's `#[transfer]` runs. The producer writes its output in a device's memory, and the
//! consumer reads the host's, so the only move between them is the author's function. The device
//! also offers an any-type move that is cheaper, and the planner still takes the author's.

use std::borrow::Cow;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use daedalus_abi::{Slots, Step, StepCx, StepEnd, StepWord};
use daedalus_backend::{
    Backend, CompileCx, Compiled, MemorySpace, NodeRef, Offer, Refusal, Region, SpaceKind,
    Transfer, TransferCost,
};
use daedalus_cpu::CpuBackend;
use daedalus_graph::{EdgeDoc, EdgePolicyDoc, GraphBody, NodeDoc, NodeKindDoc, PortRefDoc};
use daedalus_host::{LoadOptions, LoadedHost};
use daedalus_macros::{node, transfer};
use daedalus_planner::{Config, Planned, plan};
use daedalus_registry::{NodeRecord, Registry, TransferRecord};

const DEVICE: &str = "test.device";

/// Runs the CPU's nodes in the device's memory. Its own outputs land there, and only the host's
/// memory is readable by its nodes.
struct Device;

impl Backend for Device {
    fn id(&self) -> &str {
        "cpu"
    }

    fn memory(&self) -> Vec<MemorySpace> {
        vec![
            MemorySpace {
                id: Cow::Borrowed(DEVICE),
                kind: SpaceKind::Device,
                readable: false,
            },
            MemorySpace {
                id: Cow::Borrowed("cpu.host"),
                kind: SpaceKind::Host,
                readable: true,
            },
        ]
    }

    fn transfers(&self) -> Vec<Transfer> {
        vec![Transfer {
            id: "device.copy_any",
            key: None,
            from: Cow::Borrowed(DEVICE),
            to: Cow::Borrowed("cpu.host"),
            cost: TransferCost::Handoff,
            make: make_any,
        }]
    }

    fn can_take(&self, offer: &Offer<'_>) -> Vec<NodeRef> {
        CpuBackend.can_take(offer)
    }

    fn compile(&self, region: &Region<'_>, cx: &CompileCx<'_>) -> Result<Compiled, Refusal> {
        CpuBackend.compile(region, cx)
    }

    fn bind_transfer(
        &self,
        code: &(dyn std::any::Any + Send + Sync),
        cx: &CompileCx<'_>,
    ) -> Option<Result<Step, Refusal>> {
        CpuBackend.bind_transfer(code, cx)
    }
}

/// How many times the any-type move ran. The planner must never pick it here.
static ANY_RUNS: AtomicU32 = AtomicU32::new(0);

struct AnyMove;

impl StepWord for AnyMove {
    fn run(_: usize, _: &mut Slots, _: &mut StepCx) -> StepEnd {
        ANY_RUNS.fetch_add(1, Ordering::SeqCst);
        StepEnd::Done
    }
}

fn make_any(_: &CompileCx<'_>) -> Result<Step, Refusal> {
    Ok(Step::of::<AnyMove>(0))
}

/// How many times the author's transfer ran.
static CALLS: AtomicU32 = AtomicU32::new(0);

#[node(id = "t.produce", inputs(a), outputs(out))]
fn produce(a: u32) -> u32 {
    a
}

#[node(id = "t.read", inputs(a), outputs(out))]
fn read(a: u32) -> u32 {
    a
}

#[transfer(from = "test.device", to = "cpu.host", cost = copy)]
fn to_host(value: &u32) -> u32 {
    CALLS.fetch_add(1, Ordering::SeqCst);
    value + 100
}

#[transfer(from = "test.device", to = "cpu.host", cost = copy)]
fn refuse_zero(value: &u32) -> Result<u32, &'static str> {
    if *value == 0 {
        Err("a zero can't move")
    } else {
        Ok(*value)
    }
}

const NODES: [&NodeRecord; 2] = [&PRODUCE_NODE, &READ_NODE];

fn edge(from: u32, from_port: &str, to: u32, to_port: &str) -> EdgeDoc {
    EdgeDoc {
        from: PortRefDoc {
            node: from,
            port: from_port.to_string(),
        },
        to: PortRefDoc {
            node: to,
            port: to_port.to_string(),
        },
        policy: EdgePolicyDoc::default(),
    }
}

/// `host.x -> produce (device) -> read (host) -> host.out`.
fn graph() -> GraphBody {
    let mut bridge = NodeDoc::new("host");
    bridge.label = Some("io".to_string());
    bridge.kind = NodeKindDoc::HostBridge;
    for port in ["x", "out"] {
        bridge
            .port_types
            .insert(port.to_string(), "u32".to_string());
    }
    GraphBody {
        nodes: vec![bridge, NodeDoc::new("t.produce"), NodeDoc::new("t.read")],
        edges: vec![
            edge(0, "x", 1, "a"),
            edge(1, "out", 2, "a"),
            edge(2, "out", 0, "out"),
        ],
        groups: Vec::new(),
    }
}

fn planned(transfer: &'static TransferRecord) -> Planned {
    let mut registry = Registry::new();
    registry.register_all(&NODES).unwrap();
    registry.register_transfer(transfer).unwrap();
    // Merging off: each node is its own step.
    let config = Config {
        merge: false,
        ..Config::default()
    };
    plan(&graph(), &registry, &[&Device], &config).unwrap()
}

fn run(host: &mut LoadedHost, x: u32) -> Option<u32> {
    let io = host.host();
    io.feed_typed("x", Arc::new(x)).unwrap();
    io.step().unwrap();
    io.take_copy::<u32>("out").unwrap()
}

#[test]
fn an_author_transfer_moves_each_value_once_and_the_planner_prefers_it() {
    let planned = planned(&TO_HOST_TRANSFER);
    let moves: Vec<&str> = planned
        .stitched
        .transfers
        .iter()
        .map(|used| used.id.as_str())
        .collect();
    assert_eq!(
        moves,
        [TO_HOST_TRANSFER.id],
        "the any-type move is not used"
    );

    let mut host = LoadedHost::load(planned, LoadOptions::default()).unwrap();
    assert_eq!(run(&mut host, 1), Some(101));
    assert_eq!(run(&mut host, 2), Some(102));
    assert_eq!(CALLS.load(Ordering::SeqCst), 2, "one call per value");
    assert_eq!(ANY_RUNS.load(Ordering::SeqCst), 0);
}

#[test]
fn a_transfer_that_returns_an_error_is_a_fault_naming_the_transfer() {
    let mut host =
        LoadedHost::load(planned(&REFUSE_ZERO_TRANSFER), LoadOptions::default()).unwrap();
    assert_eq!(run(&mut host, 4), Some(4));
    assert_eq!(run(&mut host, 0), None, "the zero never reaches the reader");

    let report = host
        .fault_reports()
        .find(|report| report.transfer == Some(REFUSE_ZERO_TRANSFER.id))
        .expect("a fault naming the transfer");
    assert_eq!(report.reason(), "a zero can't move");
    assert!(report.covers.is_empty(), "a move runs no node");
}
