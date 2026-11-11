//! A host output is not there until the step that writes it has finished. A step that returns
//! `Pending` has stored its output but its fence hasn't resolved, so every take gives nothing
//! until it does, and a failed fence takes its output back.

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
static GATES: [AtomicU8; 2] = [AtomicU8::new(0), AtomicU8::new(0)];
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
                    .map(|unit| StepUnit {
                        step: Step::boxed(Held {
                            gate: self.gate,
                            inner: Some(unit.step),
                        }),
                        info: UnitInfo {
                            may_pend: true,
                            ..unit.info
                        },
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
    let planned = plan(&graph(), &registry, &[&Waiting { gate }], &config).unwrap();
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
