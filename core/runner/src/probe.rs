//! The tick probe: relaxed-atomic time sums and counts for each tick, behind one `None` check when
//! no probe is attached. `take_tick` moves a finished tick out. The probe never allocates. Its
//! clock is the host's function, or the platform's counter where `best_clock` finds one.

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

/// One step's figures: how long its runs took, as a histogram and a sum, and the bytes it
/// published. They cost a step one clock read and three additions when its tick is timed, and
/// one addition for each output it publishes. Nothing here allocates.
pub struct StepHistogram {
    buckets: [Sum; HISTOGRAM_BUCKETS],
    /// The time of every timed run, added up.
    total: Sum,
    /// The lengths of the values the step published, added up.
    bytes: Sum,
}

/// One step's figures, read out.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StepFigures {
    /// Runs that were timed: all of them when every tick is, one in `every` otherwise.
    pub timed_runs: u32,
    /// The time those runs took together. A run's time is from the end of the step before it
    /// to its own end, so it holds the runner's work between the two.
    pub time_ns: u64,
    /// The bytes the step published: the lengths of its outputs as their cells give them. A
    /// value handed on by reference counts its length and is not copied. A typed value, whose
    /// size the runner doesn't know, counts as nothing.
    pub bytes: u64,
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
            total: Sum::new(),
            bytes: Sum::new(),
        }
    }

    fn record(&self, nanos: u64) {
        let bucket = ((64 - (nanos >> 8).leading_zeros()) as usize).min(HISTOGRAM_BUCKETS - 1);
        self.buckets[bucket].add(1);
        self.total.add(nanos);
    }

    /// The step's figures as of now.
    pub fn figures(&self) -> StepFigures {
        StepFigures {
            timed_runs: self.counts().iter().sum(),
            time_ns: self.total.get(),
            bytes: self.bytes.get(),
        }
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

/// Where a probe's times come from. `Host` is the host's clock function. The other variant is the
/// platform's cheapest reading, which a probe takes where the platform allows it from user space.
#[derive(Clone, Copy)]
pub enum ProbeClock {
    /// The host's clock, in nanoseconds from some fixed point.
    Host(ClockFn),
    /// The aarch64 virtual counter: one register read, scaled to nanoseconds by `mult`, which is
    /// `(1e9 << 32) / frequency` for the counter's frequency.
    #[cfg(all(target_arch = "aarch64", target_os = "linux"))]
    Counter { mult: u64 },
    /// The x86_64 time stamp counter, which the kernel reads as its clocksource: one `rdtsc`,
    /// scaled to nanoseconds by `mult` as for `Counter`.
    #[cfg(all(target_arch = "x86_64", target_os = "linux"))]
    Tsc { mult: u64 },
}

impl ProbeClock {
    /// Reads the clock, in nanoseconds from its own starting point.
    #[inline(always)]
    pub fn now(self) -> u64 {
        match self {
            ProbeClock::Host(clock) => clock(),
            #[cfg(all(target_arch = "aarch64", target_os = "linux"))]
            ProbeClock::Counter { mult } => ticks_to_ns(counter_ticks(), mult),
            #[cfg(all(target_arch = "x86_64", target_os = "linux"))]
            ProbeClock::Tsc { mult } => ticks_to_ns(tsc_ticks(), mult),
        }
    }
}

/// The cheapest clock this platform has for a probe, and `fallback` where it has none. A probe
/// takes only differences of readings, so the clock's starting point does not matter.
///
/// `kernel_clock_ok` says whether the kernel's clocksource is one the probe may read directly. It
/// matters on x86_64 Linux only: there the probe uses the time stamp counter only when the host
/// has found the kernel's clocksource to be `tsc`. Anything in doubt gives `fallback`.
pub fn best_clock(fallback: ClockFn, kernel_clock_ok: bool) -> ProbeClock {
    #[cfg(all(target_arch = "x86_64", target_os = "linux"))]
    if kernel_clock_ok && let Some(frequency) = tsc_frequency(fallback) {
        let mult = ((1_000_000_000u128 << 32) / u128::from(frequency)) as u64;
        if mult != 0 {
            return ProbeClock::Tsc { mult };
        }
    }
    #[cfg(all(target_arch = "aarch64", target_os = "linux"))]
    {
        let frequency = counter_frequency();
        if frequency != 0 {
            let mult = ((1_000_000_000u128 << 32) / u128::from(frequency)) as u64;
            if mult != 0 {
                return ProbeClock::Counter { mult };
            }
        }
    }
    let _ = kernel_clock_ok;
    ProbeClock::Host(fallback)
}

#[cfg(all(target_arch = "aarch64", target_os = "linux"))]
#[inline(always)]
fn counter_ticks() -> u64 {
    let ticks: u64;
    // SAFETY: `cntvct_el0` is the virtual counter. Linux lets user space read it, and the read
    // changes no state and touches no memory.
    unsafe {
        core::arch::asm!("mrs {}, cntvct_el0", out(reg) ticks, options(nomem, nostack, preserves_flags))
    };
    ticks
}

/// One read of the time stamp counter.
#[cfg(all(target_arch = "x86_64", target_os = "linux"))]
#[inline(always)]
fn tsc_ticks() -> u64 {
    // SAFETY: `rdtsc` reads the time stamp counter. It needs no feature beyond the baseline and
    // changes no state; the kernel leaves it readable from user space.
    unsafe { core::arch::x86_64::_rdtsc() }
}

/// The TSC frequency in hertz, or `None` when it cannot be trusted: the counter must be invariant
/// (CPUID 0x80000007 EDX bit 8), and the frequency comes from CPUID 0x15 or 0x16 when they give it,
/// and otherwise from one calibration against `fallback`.
#[cfg(all(target_arch = "x86_64", target_os = "linux"))]
fn tsc_frequency(fallback: ClockFn) -> Option<u64> {
    if !tsc_invariant() {
        return None;
    }
    let hertz = cpuid_tsc_hertz()
        .or_else(cpuid_base_hertz)
        .or_else(|| calibrate(fallback))?;
    // A frequency outside what any part runs at means a misread, and then the host clock is kept.
    (500_000_000..=10_000_000_000)
        .contains(&hertz)
        .then_some(hertz)
}

#[cfg(all(target_arch = "x86_64", target_os = "linux"))]
fn tsc_invariant() -> bool {
    if core::arch::x86_64::__cpuid(0x8000_0000).eax < 0x8000_0007 {
        return false;
    }
    core::arch::x86_64::__cpuid(0x8000_0007).edx & (1 << 8) != 0
}

/// CPUID 0x15: the TSC ratio and the crystal frequency, when the part reports all three.
#[cfg(all(target_arch = "x86_64", target_os = "linux"))]
fn cpuid_tsc_hertz() -> Option<u64> {
    if core::arch::x86_64::__cpuid(0).eax < 0x15 {
        return None;
    }
    let leaf = core::arch::x86_64::__cpuid_count(0x15, 0);
    if leaf.eax == 0 || leaf.ebx == 0 || leaf.ecx == 0 {
        return None;
    }
    Some(u64::from(leaf.ecx) * u64::from(leaf.ebx) / u64::from(leaf.eax))
}

/// CPUID 0x16: the base frequency in MHz, when the part reports it.
#[cfg(all(target_arch = "x86_64", target_os = "linux"))]
fn cpuid_base_hertz() -> Option<u64> {
    if core::arch::x86_64::__cpuid(0).eax < 0x16 {
        return None;
    }
    let base = core::arch::x86_64::__cpuid(0x16).eax;
    (base != 0).then(|| u64::from(base) * 1_000_000)
}

/// The TSC frequency, measured against `fallback` over 10 ms. The frequency is an estimate, and
/// its error scales the probe's times but never reorders them.
#[cfg(all(target_arch = "x86_64", target_os = "linux"))]
fn calibrate(fallback: ClockFn) -> Option<u64> {
    const SPAN_NS: u64 = 10_000_000;
    // A clock that stops advancing gives up after this many reads, and the host clock is kept.
    const MAX_READS: u32 = 50_000_000;
    let start = fallback();
    let start_ticks = tsc_ticks();
    for _ in 0..MAX_READS {
        let now = fallback();
        if now.saturating_sub(start) >= SPAN_NS {
            let elapsed = now - start;
            let ticks = tsc_ticks().wrapping_sub(start_ticks);
            return (ticks > 0)
                .then(|| (u128::from(ticks) * 1_000_000_000 / u128::from(elapsed)) as u64);
        }
    }
    None
}

#[cfg(all(target_arch = "aarch64", target_os = "linux"))]
fn counter_frequency() -> u64 {
    let frequency: u64;
    // SAFETY: `cntfrq_el0` holds the counter's frequency. Linux lets user space read it, and the
    // read changes no state and touches no memory.
    unsafe {
        core::arch::asm!("mrs {}, cntfrq_el0", out(reg) frequency, options(nomem, nostack, preserves_flags))
    };
    frequency
}

/// Nanoseconds for `ticks` of a counter, with `mult` as in [`ProbeClock::Counter`].
#[cfg(any(
    test,
    all(
        any(target_arch = "aarch64", target_arch = "x86_64"),
        target_os = "linux"
    )
))]
#[inline(always)]
fn ticks_to_ns(ticks: u64, mult: u64) -> u64 {
    ((u128::from(ticks) * u128::from(mult)) >> 32) as u64
}

/// One graph's probe. A runner borrows it for as long as it is attached.
pub struct TickProbe<'a> {
    clock: ProbeClock,
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
    /// Whether the tick now running runs its steps one after another. Then a step's time is the
    /// difference of two readings and the body time is the last reading less the first, so no
    /// step adds to it.
    serial: Word,
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
        TickProbe::sampled_by(ProbeClock::Host(clock), every)
    }

    /// `sampled`, with the clock given as a [`ProbeClock`], such as `best_clock` returns.
    pub const fn sampled_by(clock: ProbeClock, every: u32) -> TickProbe<'a> {
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
            serial: Word::new(0),
            histograms: &[],
        }
    }

    /// This probe, also keeping each step's figures in `steps`: one entry per step of the plan.
    pub const fn with_steps(mut self, steps: &'a [StepHistogram]) -> TickProbe<'a> {
        self.histograms = steps;
        self
    }

    /// Counts `len` bytes that step `step` published. One addition, on every tick.
    pub(crate) fn add_bytes(&self, step: usize, len: u64) {
        if let Some(histogram) = self.histograms.get(step) {
            histogram.bytes.add(len);
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
            clock: ProbeClock::Host(clock),
            tick_ns: Sum::new(),
            body_ns: Sum::new(),
            steps: Sum::new(),
            mark: Sum::new(),
            finished: Word::new(0),
            ticks: Sum::new(),
            every: Word::new(1),
            running_timed: Word::new(0),
            last_timed: Word::new(0),
            serial: Word::new(0),
            histograms,
        }
    }

    /// Starts a tick. Returns the clock reading at its start when the tick is timed, and `None`
    /// when it is not, in which case the clock is not read. `serial` is true for a tick whose
    /// steps run one after another, which is the only case where a step's time can be taken from
    /// the readings alone.
    pub(crate) fn begin_tick(&self, serial: bool) -> Option<u64> {
        let number = self.ticks.get();
        self.ticks.add(1);
        let timed = number.is_multiple_of(u64::from(self.every.get().max(1)));
        self.running_timed.set(u32::from(timed));
        self.serial.set(u32::from(serial));
        timed.then(|| {
            let now = self.clock.now();
            self.mark.set(now);
            now
        })
    }

    /// Takes the time of a step that ran, on a timed tick: from the previous reading to this
    /// step's end, which is the step's own call and the runner's work since the step before it.
    /// That is one clock read and one store a step. A parallel tick also adds the time to the
    /// body sum, since its steps overlap and the readings cannot be telescoped.
    pub(crate) fn add_step(&self, step: usize) {
        if self.running_timed.get() == 0 {
            return;
        }
        let now = self.clock.now();
        let nanos = now.saturating_sub(self.mark.get());
        self.mark.set(now);
        if self.serial.get() == 0 {
            self.body_ns.add(nanos);
        }
        if let Some(histogram) = self.histograms.get(step) {
            histogram.record(nanos);
        }
    }

    /// Finishes a tick that `begin_tick` started with `start`, and that called `steps` step bodies.
    /// Every tick counts its steps, timed or not. A serial tick's body time is the last reading
    /// less the first, which is the sum of its steps' times exactly.
    pub(crate) fn finish_tick(&self, start: Option<u64>, steps: u32) {
        if steps != 0 {
            self.steps.add(u64::from(steps));
        }
        if let Some(start) = start {
            let end = self.clock.now();
            self.tick_ns.set(end.saturating_sub(start));
            if self.serial.get() != 0 {
                self.body_ns.add(self.mark.get().saturating_sub(start));
            }
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

    /// The time and bytes of `step`, when a probe that keeps each step's figures is attached.
    pub fn step_figures(&self, step: u32) -> Option<StepFigures> {
        Some(self.probe?.histogram(step as usize)?.figures())
    }
}

#[cfg(test)]
mod tests {
    use super::ticks_to_ns;

    #[test]
    fn a_counter_at_54_mhz_reads_in_nanoseconds_to_within_one() {
        let mult = ((1_000_000_000u128 << 32) / 54_000_000) as u64;
        assert!(ticks_to_ns(54_000_000, mult).abs_diff(1_000_000_000) <= 1);
        assert!(ticks_to_ns(27, mult).abs_diff(500) <= 1);
        let mut last = 0;
        for ticks in (0..10_000u64).map(|n| n * 37) {
            let ns = ticks_to_ns(ticks, mult);
            assert!(ns >= last, "monotonic at {ticks}");
            last = ns;
        }
    }
}
