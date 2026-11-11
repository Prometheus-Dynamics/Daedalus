//! The tick probe: relaxed-atomic time sums and counts for each tick, behind one `None` check when
//! no probe is attached. `take_tick` moves a finished tick out. The probe never allocates, and the
//! clock is a function the host supplies, because the runner has no clock of its own.

use daedalus_abi::Word;

use crate::Runner;

/// A sum that many steps add to. It is 64-bit where the target has that, and otherwise a 32-bit
/// word on chips that run one step at a time.
#[cfg(target_has_atomic = "64")]
mod sum {
    use core::sync::atomic::{AtomicU64, Ordering};

    pub struct Sum(AtomicU64);

    impl Sum {
        pub const fn new() -> Sum {
            Sum(AtomicU64::new(0))
        }
        pub fn get(&self) -> u64 {
            self.0.load(Ordering::Relaxed)
        }
        pub fn add(&self, value: u64) {
            self.0.fetch_add(value, Ordering::Relaxed);
        }
        pub fn set(&self, value: u64) {
            self.0.store(value, Ordering::Relaxed);
        }
    }
}

#[cfg(not(target_has_atomic = "64"))]
mod sum {
    use daedalus_abi::Word;

    pub struct Sum(Word);

    impl Sum {
        pub const fn new() -> Sum {
            Sum(Word::new(0))
        }
        pub fn get(&self) -> u64 {
            u64::from(self.0.get())
        }
        pub fn add(&self, value: u64) {
            self.0.set(self.0.get().saturating_add(value as u32));
        }
        pub fn set(&self, value: u64) {
            self.0.set(value as u32);
        }
    }
}

use sum::Sum;

/// Log-scale buckets of a step's run time: bucket `i` counts runs under `2^(i+8)` ns.
pub const HISTOGRAM_BUCKETS: usize = 16;

/// One step's run-time histogram.
pub struct StepHistogram {
    buckets: [Sum; HISTOGRAM_BUCKETS],
}

impl Default for StepHistogram {
    fn default() -> StepHistogram {
        StepHistogram::new()
    }
}

impl StepHistogram {
    pub const fn new() -> StepHistogram {
        StepHistogram {
            buckets: [const { Sum::new() }; HISTOGRAM_BUCKETS],
        }
    }

    fn record(&self, nanos: u64) {
        let bucket = ((64 - (nanos >> 8).leading_zeros()) as usize).min(HISTOGRAM_BUCKETS - 1);
        self.buckets[bucket].add(1);
    }

    /// The counts, one per bucket.
    pub fn counts(&self) -> [u32; HISTOGRAM_BUCKETS] {
        core::array::from_fn(|at| self.buckets[at].get() as u32)
    }
}

/// The finished figures of one tick, or of a tick still being measured.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TickSample {
    /// Whether this tick was timed. A tick the probe skipped has no clock readings: its `tick_ns`
    /// and `body_ns` are zero, and only `steps` is counted.
    pub timed: bool,
    /// The whole tick.
    pub tick_ns: u64,
    /// Time inside step bodies, measured from one step's end to the next: each step's own call,
    /// and the runner's work between it and the step before. A tick reads the clock once at its
    /// start and once after each step that runs, so the time the runner spends between steps is
    /// counted in the body time of the step after it.
    pub body_ns: u64,
    /// Step bodies called.
    pub steps: u32,
}

impl TickSample {
    /// Time that was the runner's own: the tick minus the bodies.
    pub const fn overhead_ns(&self) -> u64 {
        self.tick_ns.saturating_sub(self.body_ns)
    }
}

/// Reads the host's clock, in nanoseconds.
pub type ClockFn = fn() -> u64;

/// One graph's probe. A runner borrows it for as long as it is attached.
pub struct TickProbe<'a> {
    clock: ClockFn,
    tick_ns: Sum,
    body_ns: Sum,
    steps: Sum,
    /// The last clock reading the probe took in this tick: the start of the tick, or the end of
    /// the last step that ran. Each step's time runs from here to its own end.
    mark: Sum,
    /// Set when a tick has finished and not yet been taken.
    finished: Word,
    /// Ticks begun. Tick `n` is timed when `n` is a multiple of `every`.
    ticks: Sum,
    /// How often a tick is timed: every `every`th tick. Always at least 1.
    every: Word,
    /// Whether the tick now running is timed.
    running_timed: Word,
    /// Whether the tick last finished was timed.
    last_timed: Word,
    /// One per step, for the profile level. Empty means no histograms.
    histograms: &'a [StepHistogram],
}

impl<'a> TickProbe<'a> {
    pub const fn new(clock: ClockFn) -> TickProbe<'a> {
        TickProbe::sampled(clock, 1)
    }

    /// A probe that times every `every`th tick, and counts the steps of every tick. `every` of 1
    /// times every tick, which is what `new` does. A value of 0 is taken as 1.
    pub const fn sampled(clock: ClockFn, every: u32) -> TickProbe<'a> {
        TickProbe {
            clock,
            tick_ns: Sum::new(),
            body_ns: Sum::new(),
            steps: Sum::new(),
            mark: Sum::new(),
            finished: Word::new(0),
            ticks: Sum::new(),
            every: Word::new(if every == 0 { 1 } else { every }),
            running_timed: Word::new(0),
            last_timed: Word::new(0),
            histograms: &[],
        }
    }

    /// Changes how often ticks are timed, from the next tick on. Zero is taken as one.
    pub fn set_every(&self, every: u32) {
        self.every.set(every.max(1));
    }

    /// How often ticks are timed.
    pub fn every(&self) -> u32 {
        self.every.get()
    }

    /// A probe that also fills one histogram per step.
    pub const fn with_histograms(clock: ClockFn, histograms: &'a [StepHistogram]) -> TickProbe<'a> {
        TickProbe {
            clock,
            tick_ns: Sum::new(),
            body_ns: Sum::new(),
            steps: Sum::new(),
            mark: Sum::new(),
            finished: Word::new(0),
            ticks: Sum::new(),
            every: Word::new(1),
            running_timed: Word::new(0),
            last_timed: Word::new(0),
            histograms,
        }
    }

    /// Starts a tick. Returns the clock reading at its start when the tick is timed, and `None`
    /// when it is not, in which case the clock is not read.
    pub(crate) fn begin_tick(&self) -> Option<u64> {
        let number = self.ticks.get();
        self.ticks.add(1);
        let timed = number.is_multiple_of(u64::from(self.every.get().max(1)));
        self.running_timed.set(u32::from(timed));
        timed.then(|| {
            let now = (self.clock)();
            self.mark.set(now);
            now
        })
    }

    /// Counts a step that ran. On a timed tick it also takes the step's time: from the previous
    /// reading to this step's end, which is the step's own call and the runner's work since the
    /// step before it. That is one clock read a step rather than two.
    pub(crate) fn add_step(&self, step: usize) {
        self.steps.add(1);
        if self.running_timed.get() == 0 {
            return;
        }
        let now = (self.clock)();
        let nanos = now.saturating_sub(self.mark.get());
        self.mark.set(now);
        self.body_ns.add(nanos);
        if let Some(histogram) = self.histograms.get(step) {
            histogram.record(nanos);
        }
    }

    /// Finishes a tick that `begin_tick` started with `start`.
    pub(crate) fn finish_tick(&self, start: Option<u64>) {
        if let Some(start) = start {
            self.tick_ns.set((self.clock)().saturating_sub(start));
        }
        self.last_timed.set(u32::from(start.is_some()));
        self.finished.set(1);
    }

    /// True when a tick has finished and hasn't been taken.
    pub fn has_tick(&self) -> bool {
        self.finished.get() != 0
    }

    /// Moves the finished tick into `sample` and resets the probe.
    pub fn take_tick(&self, sample: &mut TickSample) {
        self.peek_tick(sample);
        self.tick_ns.set(0);
        self.body_ns.set(0);
        self.steps.set(0);
        self.finished.set(0);
        self.last_timed.set(0);
    }

    /// Copies the current figures into `sample`, leaving the probe as it is.
    pub fn peek_tick(&self, sample: &mut TickSample) {
        *sample = TickSample {
            timed: self.last_timed.get() != 0,
            tick_ns: self.tick_ns.get(),
            body_ns: self.body_ns.get(),
            steps: self.steps.get() as u32,
        };
    }

    /// One histogram, for reports.
    pub fn histogram(&self, step: usize) -> Option<&StepHistogram> {
        self.histograms.get(step)
    }
}

impl<'a> Runner<'a> {
    /// Attaches `probe`. Every tick from now on fills it.
    pub fn set_probe(&mut self, probe: &'a TickProbe<'a>) {
        self.probe = Some(probe);
    }

    /// Detaches the probe.
    pub fn clear_probe(&mut self) {
        self.probe = None;
    }
}
