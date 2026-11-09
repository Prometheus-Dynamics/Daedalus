//! Fixed runner counters. They are plain integers, so counting never allocates. The host reads
//! them through [`Runner::counters`] and turns them into reports.

use crate::Runner;
use crate::tick::TickReport;

/// What the runner has done since it loaded. The size is fixed, and a test checks it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Counters {
    /// Ticks run.
    pub ticks: u64,
    /// Ticks that found nothing dirty and no fence waiting.
    pub idle_ticks: u64,
    /// Step bodies called, recovery retries included.
    pub steps_called: u64,
    /// Calls that returned `Skipped`.
    pub skipped: u64,
    /// Calls that returned `Fault`.
    pub faults: u64,
    /// Steps not called because they were in backoff.
    pub backoff_skips: u64,
    /// Dirty steps held back because the activity mask cleared them.
    pub deferrals: u64,
    /// Steps not called because a blocking output was full.
    pub backpressure_deferrals: u64,
    /// Values a bounded edge rejected.
    pub backpressure_events: u64,
    /// Steps that went pending.
    pub pending: u64,
    /// Dirty steps a cancel left unrun, counted when the cancel stopped a tick.
    pub cancelled: u64,
    /// Reusable steps whose stamps were compared.
    pub reuse_checks: u64,
    /// Compared steps that replayed their stored outputs instead of running.
    pub reuse_replays: u64,
    /// Outputs given up under their slot's pressure policy.
    pub dropped: u64,
}

impl Counters {
    /// Adds one tick's report.
    pub(crate) fn absorb(&mut self, report: &TickReport) {
        self.ticks += 1;
        self.idle_ticks += u64::from(report.idle);
        self.steps_called += u64::from(report.step_calls);
        self.skipped += u64::from(report.skipped);
        self.faults += u64::from(report.faults);
    }
}

impl Runner<'_> {
    /// The counters since the runner loaded.
    pub fn counters(&self) -> Counters {
        self.counters
    }
}

impl Runner<'_> {
    /// How many steps the plan has.
    pub fn step_count(&self) -> usize {
        self.steps.len()
    }
}
