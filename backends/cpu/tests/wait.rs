//! A host waits for work a device hasn't finished. The step's first run ends in a pending fence,
//! and the fence completes once a thread opens its gate. One call to `step_until_settled` returns
//! after that, with the output present, and the ticks it ran stay few. A wait that times out
//! leaves the step held, and the next wait finishes it.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

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

/// One gate for each test, so tests running at once don't share a fence.
static GATES: [AtomicBool; 2] = [AtomicBool::new(false), AtomicBool::new(false)];

/// The fence a held step waits on. Its word is the gate's index.
struct Gate;

impl FenceWord for Gate {
    fn poll(word: usize) -> FencePoll {
        if GATES[word].load(Ordering::Acquire) {
            FencePoll::Ready
        } else {
            FencePoll::Pending
        }
    }
}

/// Runs the CPU's step, and returns `Pending` on its fence. A pending step has done its work and
/// stored its outputs, as a device step does when it submits work. Its consumers wait until the
/// gate opens.
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

/// The CPU backend, with every step made to wait on a gate first.
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

#[node(id = "w.inc", inputs(a), outputs(out))]
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
        nodes: vec![bridge, NodeDoc::new("w.inc")],
        edges: vec![edge(0, "x", 1, "a"), edge(1, "out", 0, "out")],
        groups: Vec::new(),
    }
}

fn load(gate: usize) -> LoadedHost {
    let mut registry = Registry::new();
    registry.register_all(&[&INC_NODE]).unwrap();
    // Merging off: each node is its own step.
    let config = Config {
        merge: false,
        ..Config::default()
    };
    let planned = plan(&graph(), &registry, &[&Waiting { gate }], &config).unwrap();
    LoadedHost::load(planned, LoadOptions::default()).unwrap()
}

/// A tick count a wait may run. Each sleep lasts at least as long as it asked, and the sleeps
/// are at most 2 ms once they have grown, so a wait that spun would run far more than this.
fn ticks_allowed(elapsed: Duration) -> u32 {
    10 + elapsed.as_millis() as u32 / 2
}

#[test]
fn one_call_returns_once_the_fence_completes_with_the_output_present() {
    let mut host = load(0);
    host.host().feed_typed("x", Arc::new(20u32)).unwrap();
    let opener = std::thread::spawn(|| {
        std::thread::sleep(Duration::from_millis(30));
        GATES[0].store(true, Ordering::Release);
    });
    let start = Instant::now();
    let waited = host.step_until_settled(Duration::from_secs(10)).unwrap();
    let elapsed = start.elapsed();
    opener.join().unwrap();

    assert_eq!(waited.outcome, WaitOutcome::Settled);
    assert_eq!(waited.pending, 0);
    assert!(elapsed >= Duration::from_millis(30), "{elapsed:?}");
    assert!(
        waited.ticks > 1,
        "the wait ticked while the fence was pending"
    );
    assert!(
        waited.ticks <= ticks_allowed(elapsed),
        "{waited:?} in {elapsed:?}"
    );
    assert_eq!(host.host().take_copy::<u32>("out").unwrap(), Some(21));
}

#[test]
fn a_timed_out_wait_leaves_the_step_held_and_the_next_wait_finishes_it() {
    let mut host = load(1);
    host.host().feed_typed("x", Arc::new(1u32)).unwrap();

    let start = Instant::now();
    let waited = host.step_until_settled(Duration::from_millis(50)).unwrap();
    let elapsed = start.elapsed();
    assert_eq!(waited.outcome, WaitOutcome::TimedOut);
    assert_eq!(waited.pending, 1);
    assert!(elapsed >= Duration::from_millis(50), "{elapsed:?}");
    assert!(elapsed < Duration::from_secs(2), "{elapsed:?}");
    assert!(
        waited.ticks <= ticks_allowed(elapsed),
        "{waited:?} in {elapsed:?}"
    );

    GATES[1].store(true, Ordering::Release);
    let waited = host.step_until_settled(Duration::from_secs(10)).unwrap();
    assert_eq!(waited.outcome, WaitOutcome::Settled);
    assert_eq!(host.host().take_copy::<u32>("out").unwrap(), Some(2));
}
