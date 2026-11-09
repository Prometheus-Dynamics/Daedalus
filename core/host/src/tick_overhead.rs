//! The tick-overhead window: the last N ticks a probe finished, and what they cost.

use std::vec::Vec;

use daedalus_runner::{TickProbe, TickSample};

/// A window of the most recent ticks. Recording allocates nothing; the window is sized once.
#[derive(Debug)]
pub struct TickOverheadWindow {
    samples: Vec<TickSample>,
    limit: usize,
    next: usize,
    recorded: u64,
}

/// What the window's ticks cost.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct TickOverheadReport {
    pub ticks: usize,
    pub p50_ns: u64,
    pub p99_ns: u64,
    pub max_ns: u64,
    pub mean_ns: f64,
    /// Mean of the time outside step bodies.
    pub mean_overhead_ns: f64,
}

impl TickOverheadWindow {
    pub fn new(limit: usize) -> TickOverheadWindow {
        TickOverheadWindow {
            samples: Vec::with_capacity(limit.max(1)),
            limit: limit.max(1),
            next: 0,
            recorded: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.samples.len()
    }

    pub fn is_empty(&self) -> bool {
        self.samples.is_empty()
    }

    /// How many ticks have been recorded in all.
    pub fn recorded(&self) -> u64 {
        self.recorded
    }

    /// Folds in the tick `probe` has finished, if there is one.
    pub fn record(&mut self, probe: &TickProbe<'_>) {
        if !probe.has_tick() {
            return;
        }
        let mut sample = TickSample::default();
        probe.take_tick(&mut sample);
        if self.samples.len() < self.limit {
            self.samples.push(sample);
        } else {
            self.samples[self.next] = sample;
        }
        self.next = (self.next + 1) % self.limit;
        self.recorded += 1;
    }

    /// The latest tick.
    pub fn latest(&self) -> Option<TickSample> {
        let at = (self.next + self.limit - 1) % self.limit;
        self.samples.get(at).copied()
    }

    pub fn clear(&mut self) {
        self.samples.clear();
        self.next = 0;
    }

    /// Aggregates the window: percentiles of the tick time, and the mean overhead.
    pub fn report(&self) -> TickOverheadReport {
        let ticks = self.samples.len();
        if ticks == 0 {
            return TickOverheadReport::default();
        }
        let mut ordered: Vec<u64> = self.samples.iter().map(|sample| sample.tick_ns).collect();
        ordered.sort_unstable();
        let percentile = |rank: usize| ordered[(rank * (ticks - 1)).div_ceil(100)];
        let total: u64 = ordered.iter().sum();
        let overhead: u64 = self.samples.iter().map(TickSample::overhead_ns).sum();
        TickOverheadReport {
            ticks,
            p50_ns: percentile(50),
            p99_ns: percentile(99),
            max_ns: ordered[ticks - 1],
            mean_ns: total as f64 / ticks as f64,
            mean_overhead_ns: overhead as f64 / ticks as f64,
        }
    }
}
