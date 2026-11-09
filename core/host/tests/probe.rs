//! The tick probe, the overhead window, diagnostics JSON, and allocation attribution.

use std::sync::atomic::{AtomicU64, Ordering};

use daedalus_abi::counting::{CountingAlloc, counted};
use daedalus_abi::{Slots, Step, StepCx, StepEnd, StepWord};
use daedalus_host::{Host, PlanReport, Ports, TickOverheadWindow};
use daedalus_plan::Span;
use daedalus_runner::{
    Memory, Plan, Recovery, Runner, SlotKind, SlotSpec, StepHistogram, StepMeta, TickProbe,
    TickSample,
};
use daedalus_types::TypeKey;

#[global_allocator]
static ALLOC: CountingAlloc = CountingAlloc;

static TYPES: [TypeKey; 0] = [];

fn no_clock() -> u64 {
    0
}

/// Advances 100 ns per read, so a probe's sums are exact.
static NOW: AtomicU64 = AtomicU64::new(0);
fn ticking_clock() -> u64 {
    NOW.fetch_add(100, Ordering::Relaxed)
}

struct Idle;

impl StepWord for Idle {
    fn run(_: usize, _: &mut Slots, _: &mut StepCx) -> StepEnd {
        StepEnd::Done
    }
}

fn leak<T>(values: Vec<T>) -> &'static mut [T] {
    Box::leak(values.into_boxed_slice())
}

fn runner(steps: usize) -> Runner<'static> {
    let specs: &'static [SlotSpec] = leak(vec![SlotSpec {
        cap: 1,
        kind: SlotKind::Fifo,
    }]);
    let span = Span { start: 0, len: 0 };
    let meta: &'static [StepMeta] = leak(vec![
        StepMeta {
            wake: span,
            inputs: span,
            outputs: span,
            always: true,
            may_pend: false,
            recovery: Recovery::Fail,
            fire: daedalus_runner::Fire::Any,
            required: span,
            reuse: false,
        };
        steps
    ]);
    let step_list = leak((0..steps).map(|_| Step::of::<Idle>(0)).collect::<Vec<_>>());
    let words = steps.div_ceil(32);
    let memory = Memory {
        cells: leak(vec![daedalus_abi::Slot::EMPTY; 1]),
        queues: leak(vec![daedalus_runner::Queue::default(); 1]),
        storage: leak(vec![daedalus_abi::Slot::EMPTY; 1]),
        dirty: leak(vec![0; words]),
        carry: leak(vec![0; words]),
        always: leak(vec![0; words]),
        pending: leak(Vec::new()),
        health: leak(vec![daedalus_runner::StepHealth::default(); steps]),
        activity: leak(vec![0; words]),
        inline: leak(vec![[0u8; 16]; 1]),
    };
    Runner::load(
        Plan {
            steps: step_list,
            meta,
            table: &[],
            slots: specs,
        },
        memory,
    )
    .unwrap()
}

#[test]
fn the_window_keeps_the_last_ticks_and_derives_the_remainder() {
    let probe: &'static TickProbe<'static> = Box::leak(Box::new(TickProbe::new(ticking_clock)));
    let mut runner = runner(2);
    runner.set_probe(probe);
    let mut window = TickOverheadWindow::new(4);
    for _ in 0..6 {
        runner.tick().unwrap();
        assert!(probe.has_tick());
        window.record(probe);
        assert!(!probe.has_tick());
    }
    assert_eq!((window.len(), window.recorded()), (4, 6));
    let latest = window.latest().unwrap();
    assert_eq!(latest.steps, 2);
    assert!(latest.tick_ns >= latest.body_ns);
    assert_eq!(latest.overhead_ns(), latest.tick_ns - latest.body_ns);
    let report = window.report();
    assert_eq!(report.ticks, 4);
    assert!(report.p50_ns > 0 && report.max_ns >= report.p99_ns);
    window.clear();
    assert!(window.is_empty() && window.latest().is_none());
}

#[test]
fn take_tick_resets_the_probe_and_peek_does_not() {
    let probe: &'static TickProbe<'static> = Box::leak(Box::new(TickProbe::new(ticking_clock)));
    let mut runner = runner(1);
    runner.set_probe(probe);
    runner.tick().unwrap();
    let mut sample = TickSample::default();
    probe.peek_tick(&mut sample);
    assert_eq!(sample.steps, 1);
    assert!(probe.has_tick());
    probe.take_tick(&mut sample);
    assert_eq!(sample.steps, 1);
    assert!(!probe.has_tick());
    let mut after = TickSample::default();
    probe.take_tick(&mut after);
    assert_eq!(after, TickSample::default());
}

#[test]
fn a_warm_tick_with_a_probe_attached_allocates_nothing() {
    let histograms: &'static [StepHistogram] = leak(vec![StepHistogram::new()]);
    let probe: &'static TickProbe<'static> =
        Box::leak(Box::new(TickProbe::with_histograms(no_clock, histograms)));
    let mut runner = runner(1);
    runner.set_probe(probe);
    runner.tick().unwrap();
    let mut window = TickOverheadWindow::new(8);
    let ((), allocations) = counted(|| {
        for _ in 0..2_000 {
            runner.tick().unwrap();
            window.record(probe);
        }
    });
    assert_eq!(allocations, 0);
    let total: u32 = probe.histogram(0).unwrap().counts().iter().sum();
    assert!(total >= 2_000);
}

#[test]
fn diagnostics_round_trip_as_json() {
    let host = Host::new(runner(2), Ports::new(&[]).unwrap(), &TYPES);
    let report = host.render_plan();
    assert_eq!(report.steps.len(), 2);
    assert!(report.steps[0].always);
    let text = report.to_json();
    assert_eq!(PlanReport::from_json(&text), Some(report));
}

#[cfg(feature = "alloc-probe")]
#[test]
fn the_allocation_probe_attributes_to_a_scope() {
    use daedalus_host::alloc_probe::{Scope, counts, reset, scoped};
    reset();
    let boxed = scoped(Scope::Body, || Box::new(1u32));
    drop(boxed);
    assert_eq!(counts()[1], 1);
    assert_eq!(counts()[0], 0);
}
