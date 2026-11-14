//! The tick probe, the overhead window, diagnostics JSON, and allocation attribution.

use std::sync::atomic::{AtomicU64, Ordering};

use daedalus_abi::counting::{CountingAlloc, counted};
use daedalus_abi::{Slots, Step, StepCx, StepEnd, StepWord};
use daedalus_host::{Host, PlanReport, Ports, TickOverheadWindow};
use daedalus_plan::Span;
use daedalus_runner::{
    Memory, Plan, ProbeClock, Recovery, Runner, SlotKind, SlotSpec, StepHistogram, StepMeta,
    TickProbe, TickSample, best_clock,
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
fn each_steps_time_is_summed_and_a_step_that_publishes_nothing_has_no_bytes() {
    static CLOCK: AtomicU64 = AtomicU64::new(0);
    fn clock() -> u64 {
        CLOCK.fetch_add(100, Ordering::Relaxed)
    }
    let steps: &'static [StepHistogram] = leak(vec![StepHistogram::new(), StepHistogram::new()]);
    let probe: &'static TickProbe<'static> =
        Box::leak(Box::new(TickProbe::new(clock).with_steps(steps)));
    let mut runner = runner(2);
    assert_eq!(runner.step_figures(0), None, "no probe, no figures");
    runner.set_probe(probe);
    for _ in 0..5 {
        runner.tick().unwrap();
    }
    // The clock moves 100 ns a reading, and a step's time is one reading less the one before.
    for step in 0..2 {
        let figures = runner.step_figures(step).expect("a probe that keeps them");
        assert_eq!(
            (figures.timed_runs, figures.time_ns, figures.bytes),
            (5, 500, 0)
        );
    }
    assert_eq!(runner.step_figures(2), None);

    // Keeping them costs no allocation.
    let ((), allocations) = counted(|| {
        for _ in 0..2_000 {
            runner.tick().unwrap();
            let _ = runner.step_figures(1);
        }
    });
    assert_eq!(allocations, 0);
    assert_eq!(
        runner.step_figures(1).map(|figures| figures.timed_runs),
        Some(2_005)
    );
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

/// A probe timing every `every`th tick, on a chain of two steps.
fn sampled(every: u32) -> &'static TickProbe<'static> {
    Box::leak(Box::new(TickProbe::sampled(ticking_clock, every)))
}

#[test]
fn one_times_every_tick_and_a_window_of_them_is_all_timed() {
    let probe = sampled(1);
    let mut runner = runner(2);
    runner.set_probe(probe);
    let mut window = TickOverheadWindow::new(8);
    for _ in 0..5 {
        runner.tick().unwrap();
        window.record(probe);
    }
    let report = window.report();
    assert_eq!((report.ticks, report.timed_ticks, report.steps), (5, 5, 10));
    assert!(report.p50_ns > 0);
}

#[test]
fn above_one_times_every_kth_tick_and_counts_every_tick() {
    let probe = sampled(4);
    let mut runner = runner(2);
    runner.set_probe(probe);
    let mut window = TickOverheadWindow::new(16);
    for n in 0..12 {
        runner.tick().unwrap();
        let mut sample = TickSample::default();
        probe.peek_tick(&mut sample);
        // Tick n is timed when n is a multiple of 4, and an untimed tick has no time, only steps.
        assert_eq!(sample.timed, n % 4 == 0, "tick {n}");
        if !sample.timed {
            assert_eq!((sample.tick_ns, sample.body_ns), (0, 0), "tick {n}");
        }
        assert_eq!(sample.steps, 2, "tick {n}: every tick counts its steps");
        window.record(probe);
    }
    let report = window.report();
    assert_eq!((report.ticks, report.timed_ticks), (12, 3));
    assert_eq!(report.steps, 24, "the step count is exact over every tick");
    assert!(report.p50_ns > 0 && report.mean_overhead_ns >= 0.0);
}

#[test]
fn a_window_with_no_timed_tick_reports_counts_and_no_times() {
    let probe = sampled(1000);
    let mut runner = runner(2);
    runner.set_probe(probe);
    // Tick 0 is timed. Take it out of the probe, so the window starts with untimed ticks only.
    runner.tick().unwrap();
    let mut first = TickSample::default();
    probe.take_tick(&mut first);
    assert!(first.timed);
    let mut window = TickOverheadWindow::new(8);
    for _ in 0..5 {
        runner.tick().unwrap();
        window.record(probe);
    }
    let report = window.report();
    assert_eq!((report.ticks, report.timed_ticks, report.steps), (5, 0, 10));
    assert_eq!(
        (report.p50_ns, report.p99_ns, report.max_ns),
        (0, 0, 0),
        "no timed tick, no timing figures"
    );
    assert_eq!((report.mean_ns, report.mean_overhead_ns), (0.0, 0.0));
}

#[test]
fn changing_k_between_windows_applies_from_the_next_tick() {
    let probe = sampled(1);
    let mut runner = runner(2);
    runner.set_probe(probe);
    let mut first = TickOverheadWindow::new(8);
    for _ in 0..3 {
        runner.tick().unwrap();
        first.record(probe);
    }
    assert_eq!(first.report().timed_ticks, 3);

    // The next window times every second tick, counting on from tick 3: 4 and 6 are timed.
    probe.set_every(2);
    assert_eq!(probe.every(), 2);
    let mut second = TickOverheadWindow::new(8);
    for _ in 0..4 {
        runner.tick().unwrap();
        second.record(probe);
    }
    let report = second.report();
    assert_eq!((report.ticks, report.timed_ticks, report.steps), (4, 2, 8));
}

/// A clock of its own, so these tests do not share `NOW`: 100 ns a read.
static EXACT: AtomicU64 = AtomicU64::new(0);
fn exact_clock() -> u64 {
    EXACT.fetch_add(100, Ordering::Relaxed)
}

#[test]
fn a_serial_tick_adds_up_exactly_on_a_fake_clock() {
    // Reads: the tick's start, the end of each of two steps, and the tick's end. The steps take
    // 100 ns each, and the runner's own work is the 100 ns after the last step.
    let probe: &'static TickProbe<'static> = Box::leak(Box::new(TickProbe::new(exact_clock)));
    let mut runner = runner(2);
    runner.set_probe(probe);
    runner.tick().unwrap();
    let mut sample = TickSample::default();
    probe.take_tick(&mut sample);
    assert_eq!(sample.steps, 2);
    assert_eq!(sample.tick_ns, 300);
    assert_eq!(sample.body_ns, 200);
    assert_eq!(sample.overhead_ns(), 100);
}

#[test]
fn a_serial_tick_on_the_best_clock_is_monotonic_and_adds_up() {
    use std::sync::OnceLock;
    use std::time::Instant;
    fn fallback() -> u64 {
        static START: OnceLock<Instant> = OnceLock::new();
        START.get_or_init(Instant::now).elapsed().as_nanos() as u64
    }
    let clock: ProbeClock = best_clock(fallback, true);
    let probe: &'static TickProbe<'static> = Box::leak(Box::new(TickProbe::sampled_by(clock, 1)));
    let mut runner = runner(4);
    runner.set_probe(probe);
    let mut window = TickOverheadWindow::new(64);
    let mut last_end = 0u64;
    for _ in 0..64 {
        runner.tick().unwrap();
        let mut sample = TickSample::default();
        probe.peek_tick(&mut sample);
        // A body is inside its tick, and the tick's total is its body plus the runner's own time.
        assert!(sample.body_ns <= sample.tick_ns, "{sample:?}");
        assert_eq!(sample.overhead_ns(), sample.tick_ns - sample.body_ns);
        assert_eq!(sample.steps, 4);
        last_end = last_end.max(sample.tick_ns);
        window.record(probe);
    }
    assert!(last_end > 0, "the best clock gives times");
    assert!(window.report().p50_ns > 0);
}

#[test]
fn the_probe_clock_runs_at_the_rate_of_the_monotonic_clock() {
    use std::time::{Duration, Instant};
    let clock = daedalus_host::probe_clock();
    let mut last = clock.now();
    for _ in 0..10_000 {
        let now = clock.now();
        assert!(now >= last, "the probe clock never goes back");
        last = now;
    }
    // Both clocks time the same sleep, so its length cancels out of the comparison.
    let (probe_start, wall_start) = (clock.now(), Instant::now());
    std::thread::sleep(Duration::from_millis(50));
    let (probe_ns, wall_ns) = (
        clock.now() - probe_start,
        wall_start.elapsed().as_nanos() as u64,
    );
    assert!(
        probe_ns.abs_diff(wall_ns) * 20 <= wall_ns,
        "probe clock {probe_ns} ns against monotonic {wall_ns} ns, more than 5% apart"
    );
}
