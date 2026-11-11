//! A value is not there, for the host or for another step, until the step that writes it has
//! finished. A step that returns `Pending` has stored its output but its fence hasn't resolved,
//! so no reader sees that value until it does, and a failed fence takes it back. Values the
//! step finished earlier stay readable meanwhile.

use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use daedalus_abi::testing::run_once;
use daedalus_abi::{Fence, FencePoll, FenceWord, OwnedStep, Slots, Step, StepCx, StepEnd};
use daedalus_backend::{
    Backend, CompileCx, Compiled, MemorySpace, NodeRef, Offer, Refusal, Region, StepUnit, Transfer,
    UnitInfo,
};
use daedalus_cpu::CpuBackend;
use daedalus_graph::{EdgeDoc, EdgePolicyDoc, GraphBody, NodeDoc, NodeKindDoc, PortRefDoc};
use daedalus_host::{LoadOptions, LoadedHost, WaitOutcome};
use daedalus_macros::node;
use daedalus_planner::{Config, plan};
use daedalus_registry::Registry;

/// One gate per test: 0 is closed, 1 is open, 2 has failed.
static GATES: [AtomicU8; 4] = [const { AtomicU8::new(0) }; 4];
const OPEN: u8 = 1;
const FAILED: u8 = 2;

/// The fence a held step waits on. Its word is the gate's index.
struct Gate;

impl FenceWord for Gate {
    fn poll(word: usize) -> FencePoll {
        match GATES[word].load(Ordering::Acquire) {
            OPEN => FencePoll::Ready,
            FAILED => FencePoll::Failed,
            _ => FencePoll::Pending,
        }
    }
}

/// Runs the CPU's step, and returns `Pending` on its gate. The step has stored its output by then.
struct Held {
    gate: usize,
    inner: Option<Step>,
}

impl OwnedStep for Held {
    fn run(&mut self, slots: &mut Slots, cx: &mut StepCx) -> StepEnd {
        match self.inner.as_mut().map(|step| run_once(step, slots, cx)) {
            Some(StepEnd::Done) => StepEnd::Pending(Fence::of::<Gate>(self.gate)),
            Some(other) => other,
            None => StepEnd::Fault,
        }
    }
}

impl Drop for Held {
    fn drop(&mut self) {
        if let Some(step) = self.inner.take() {
            step.discard();
        }
    }
}

struct Waiting {
    gate: usize,
    /// The node whose step is held. Every step, when there is none.
    only: Option<u32>,
}

impl Backend for Waiting {
    fn id(&self) -> &str {
        "cpu"
    }

    fn memory(&self) -> Vec<MemorySpace> {
        CpuBackend.memory()
    }

    fn transfers(&self) -> Vec<Transfer> {
        Vec::new()
    }

    fn can_take(&self, offer: &Offer<'_>) -> Vec<NodeRef> {
        CpuBackend.can_take(offer)
    }

    fn compile(&self, region: &Region<'_>, cx: &CompileCx<'_>) -> Result<Compiled, Refusal> {
        match CpuBackend.compile(region, cx)? {
            Compiled::Steps(units) => Ok(Compiled::Steps(
                units
                    .into_iter()
                    .map(|unit| {
                        let held = self
                            .only
                            .is_none_or(|node| unit.info.covers.iter().any(|have| have.0 == node));
                        if !held {
                            return unit;
                        }
                        StepUnit {
                            step: Step::boxed(Held {
                                gate: self.gate,
                                inner: Some(unit.step),
                            }),
                            info: UnitInfo {
                                may_pend: true,
                                ..unit.info
                            },
                        }
                    })
                    .collect(),
            )),
            other => Ok(other),
        }
    }
}

#[node(id = "k.inc", inputs(a), outputs(out))]
fn inc(a: u32) -> u32 {
    a + 1
}

fn graph() -> GraphBody {
    let mut bridge = NodeDoc::new("host");
    bridge.label = Some("io".to_string());
    bridge.kind = NodeKindDoc::HostBridge;
    for port in ["x", "out"] {
        bridge
            .port_types
            .insert(port.to_string(), "u32".to_string());
    }
    let edge = |from: u32, from_port: &str, to: u32, to_port: &str| EdgeDoc {
        from: PortRefDoc {
            node: from,
            port: from_port.to_string(),
        },
        to: PortRefDoc {
            node: to,
            port: to_port.to_string(),
        },
        policy: EdgePolicyDoc::default(),
    };
    GraphBody {
        nodes: vec![bridge, NodeDoc::new("k.inc")],
        edges: vec![edge(0, "x", 1, "a"), edge(1, "out", 0, "out")],
        groups: Vec::new(),
    }
}

fn load(gate: usize) -> LoadedHost {
    let mut registry = Registry::new();
    registry.register_all(&[&INC_NODE]).unwrap();
    let config = Config {
        merge: false,
        ..Config::default()
    };
    let waiting = Waiting { gate, only: None };
    let planned = plan(&graph(), &registry, &[&waiting], &config).unwrap();
    LoadedHost::load(planned, LoadOptions::default()).unwrap()
}

#[node(id = "k.add", inputs(a, b), outputs(out))]
fn add(a: u32, b: u32) -> u32 {
    a + b
}

/// `x` goes through the held `k.inc` to `k.add`, whose other input is the host's `y`.
fn load_with_reader(gate: usize) -> LoadedHost {
    let mut bridge = NodeDoc::new("host");
    bridge.label = Some("io".to_string());
    bridge.kind = NodeKindDoc::HostBridge;
    for port in ["x", "y", "out"] {
        bridge
            .port_types
            .insert(port.to_string(), "u32".to_string());
    }
    let edge = |from: u32, from_port: &str, to: u32, to_port: &str| EdgeDoc {
        from: PortRefDoc {
            node: from,
            port: from_port.to_string(),
        },
        to: PortRefDoc {
            node: to,
            port: to_port.to_string(),
        },
        policy: EdgePolicyDoc::default(),
    };
    let graph = GraphBody {
        nodes: vec![bridge, NodeDoc::new("k.inc"), NodeDoc::new("k.add")],
        edges: vec![
            edge(0, "x", 1, "a"),
            edge(1, "out", 2, "a"),
            edge(0, "y", 2, "b"),
            edge(2, "out", 0, "out"),
        ],
        groups: Vec::new(),
    };
    let mut registry = Registry::new();
    registry.register_all(&[&INC_NODE, &ADD_NODE]).unwrap();
    let config = Config {
        merge: false,
        ..Config::default()
    };
    let waiting = Waiting {
        gate,
        only: Some(1),
    };
    let planned = plan(&graph, &registry, &[&waiting], &config).unwrap();
    LoadedHost::load(planned, LoadOptions::default()).unwrap()
}

#[test]
fn a_take_gets_nothing_while_the_fence_is_pending_and_the_value_once_it_resolves() {
    let mut host = load(0);
    host.host().feed_typed("x", Arc::new(20u32)).unwrap();
    host.step().unwrap();
    assert_eq!(host.host().pending(), 1);
    assert_eq!(host.host().take_copy::<u32>("out").unwrap(), None);
    assert!(host.host().take_value("out").unwrap().is_none());
    host.step().unwrap();
    assert_eq!(host.host().take_copy::<u32>("out").unwrap(), None);

    GATES[0].store(OPEN, Ordering::Release);
    let waited = host
        .step_until_settled(std::time::Duration::from_secs(10))
        .unwrap();
    assert_eq!(waited.outcome, WaitOutcome::Settled);
    assert_eq!(host.host().take_copy::<u32>("out").unwrap(), Some(21));
    assert_eq!(
        host.host().take_copy::<u32>("out").unwrap(),
        None,
        "taken once"
    );
}

#[test]
fn a_failed_fence_takes_its_output_back_and_nothing_is_handed_out() {
    let mut host = load(1);
    host.host().feed_typed("x", Arc::new(5u32)).unwrap();
    host.step().unwrap();
    assert_eq!(host.host().take_copy::<u32>("out").unwrap(), None);

    GATES[1].store(FAILED, Ordering::Release);
    let report = host.step().unwrap();
    assert_eq!(report.faults, 1, "the failed fence is a fault");
    assert_eq!(host.host().pending(), 0);
    assert_eq!(host.host().take_copy::<u32>("out").unwrap(), None);
    let waited = host
        .step_until_settled(std::time::Duration::from_secs(10))
        .unwrap();
    assert_eq!(waited.outcome, WaitOutcome::Settled);
    assert_eq!(host.host().take_copy::<u32>("out").unwrap(), None);
}

#[test]
fn a_value_finished_earlier_stays_readable_while_the_next_one_is_pending() {
    let mut host = load(2);
    let settle = |host: &mut LoadedHost| {
        let waited = host
            .step_until_settled(std::time::Duration::from_secs(10))
            .unwrap();
        assert_eq!(waited.outcome, WaitOutcome::Settled);
    };
    host.host().feed_typed("x", Arc::new(20u32)).unwrap();
    host.step().unwrap();
    GATES[2].store(OPEN, Ordering::Release);
    settle(&mut host);
    // The first value is done and left where it is. The step starts on a second.
    GATES[2].store(0, Ordering::Release);
    host.host().feed_typed("x", Arc::new(30u32)).unwrap();
    host.step().unwrap();
    assert_eq!(host.host().pending(), 1);
    assert_eq!(host.host().take_copy::<u32>("out").unwrap(), Some(21));
    assert_eq!(
        host.host().take_copy::<u32>("out").unwrap(),
        None,
        "the second isn't there yet"
    );
    GATES[2].store(OPEN, Ordering::Release);
    settle(&mut host);
    assert_eq!(host.host().take_copy::<u32>("out").unwrap(), Some(31));
    assert_eq!(host.host().take_copy::<u32>("out").unwrap(), None);
}

#[test]
fn a_step_woken_by_another_input_does_not_read_a_value_that_is_still_pending() {
    let mut host = load_with_reader(3);
    host.host().feed_typed("x", Arc::new(1u32)).unwrap();
    host.step().unwrap();
    assert_eq!(host.host().pending(), 1);
    // The reader's other input arrives and wakes it. Its first input is still being made.
    host.host().feed_typed("y", Arc::new(10u32)).unwrap();
    host.step().unwrap();
    host.step().unwrap();
    assert_eq!(
        host.host().take_copy::<u32>("out").unwrap(),
        None,
        "the reader didn't run on a value that wasn't finished"
    );
    GATES[3].store(OPEN, Ordering::Release);
    let waited = host
        .step_until_settled(std::time::Duration::from_secs(10))
        .unwrap();
    assert_eq!(waited.outcome, WaitOutcome::Settled);
    assert_eq!(host.host().take_copy::<u32>("out").unwrap(), Some(12));
    assert_eq!(host.host().take_copy::<u32>("out").unwrap(), None);
}
