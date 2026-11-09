//! Host telemetry: levels, the clock, the event log, the reports, and no allocation at Basic.

use daedalus_abi::counting::{CountingAlloc, counted};
use daedalus_abi::{Slots, Step, StepCx, StepEnd, StepWord};
use daedalus_host::{Event, Level, Telemetry};
use daedalus_plan::Span;
use daedalus_runner::{
    Memory, Pending, Plan, Queue, Runner, SlotKind, SlotSpec, StepHealth, StepMeta,
};

#[global_allocator]
static ALLOC: CountingAlloc = CountingAlloc;

struct Nop;

impl StepWord for Nop {
    fn run(_: usize, _: &mut Slots, _: &mut StepCx) -> StepEnd {
        StepEnd::Done
    }
}

/// One step with no ports, woken by hand.
struct Graph {
    steps: Vec<Step>,
    meta: Vec<StepMeta>,
    specs: Vec<SlotSpec>,
    cells: Vec<daedalus_abi::Slot>,
    queues: Vec<Queue>,
    storage: Vec<daedalus_abi::Slot>,
    dirty: Vec<u32>,
    carry: Vec<u32>,
    always: Vec<u32>,
    pending: Vec<Pending>,
    health: Vec<StepHealth>,
    activity: Vec<u32>,
    inline: Vec<[u8; 16]>,
}

impl Graph {
    fn new() -> Graph {
        let empty = Span { start: 0, len: 0 };
        Graph {
            steps: vec![Step::of::<Nop>(0)],
            meta: vec![StepMeta {
                wake: empty,
                inputs: empty,
                outputs: empty,
                always: false,
                may_pend: false,
                recovery: daedalus_runner::Recovery::Fail,
            }],
            specs: vec![SlotSpec {
                cap: 1,
                kind: SlotKind::Fifo,
            }],
            cells: vec![daedalus_abi::Slot::EMPTY],
            queues: vec![Queue::default()],
            storage: vec![daedalus_abi::Slot::EMPTY],
            dirty: vec![0],
            carry: vec![0],
            always: vec![0],
            pending: vec![],
            health: vec![StepHealth::default()],
            activity: vec![0],
            inline: vec![[0; 16]],
        }
    }

    fn load(&mut self) -> Runner<'_> {
        Runner::load(
            Plan {
                steps: &mut self.steps,
                meta: &self.meta,
                table: &[],
                slots: &self.specs,
            },
            Memory {
                cells: &mut self.cells,
                queues: &mut self.queues,
                storage: &mut self.storage,
                dirty: &mut self.dirty,
                carry: &mut self.carry,
                always: &mut self.always,
                pending: &mut self.pending,
                health: &mut self.health,
                activity: &mut self.activity,
                inline: &mut self.inline,
            },
        )
        .unwrap()
    }
}

fn fake_clock() -> impl FnMut() -> u64 {
    let mut now = 100u64;
    move || {
        now += 5;
        now
    }
}

#[test]
fn basic_counts_ticks_and_times_them() {
    let mut graph = Graph::new();
    let mut runner = graph.load();
    let mut telemetry = Telemetry::new(Level::Basic);
    let mut clock = fake_clock();
    runner.wake(0);
    telemetry.tick(&mut runner, &mut clock).unwrap();
    telemetry.tick(&mut runner, &mut clock).unwrap();
    let report = telemetry.report(&runner);
    assert_eq!(report.counters.ticks, 2);
    assert_eq!(report.counters.steps_called, 1);
    assert_eq!(report.ticks_timed, 2);
    assert_eq!(report.total_ns, 10);
    assert_eq!(report.max_ns, 5);
    assert!(report.nodes.is_empty(), "rows come at Detailed");
    assert!(report.table().contains("ticks        2"));
}

#[test]
fn detailed_adds_one_row_per_step() {
    let mut graph = Graph::new();
    let mut runner = graph.load();
    let mut telemetry = Telemetry::new(Level::Detailed);
    let mut clock = fake_clock();
    runner.wake(0);
    telemetry.tick(&mut runner, &mut clock).unwrap();
    let report = telemetry.report(&runner);
    assert_eq!(report.nodes.len(), 1);
    assert_eq!(report.nodes[0].calls, 1);
    assert!(report.json().contains("\"calls\":1"));
}

#[test]
fn trace_logs_each_tick_only_while_the_log_is_on() {
    let mut graph = Graph::new();
    let mut runner = graph.load();
    let mut telemetry = Telemetry::new(Level::Trace);
    let mut clock = fake_clock();
    runner.wake(0);
    telemetry.tick(&mut runner, &mut clock).unwrap();
    assert_eq!(
        telemetry.log().recent().count(),
        0,
        "the log is off by default"
    );
    telemetry.log_mut().set_on(true);
    runner.wake(0);
    telemetry.tick(&mut runner, &mut clock).unwrap();
    let events: Vec<&Event> = telemetry.log().recent().collect();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].step_calls, 1);
}

#[test]
fn warm_ticks_at_basic_allocate_nothing() {
    let mut graph = Graph::new();
    let mut runner = graph.load();
    let mut telemetry = Telemetry::new(Level::Basic);
    let mut clock = fake_clock();
    runner.wake(0);
    telemetry.tick(&mut runner, &mut clock).unwrap();
    let ((), allocations) = counted(|| {
        for _ in 0..1_000 {
            runner.wake(0);
            telemetry.tick(&mut runner, &mut clock).unwrap();
        }
    });
    assert_eq!(allocations, 0);
}
