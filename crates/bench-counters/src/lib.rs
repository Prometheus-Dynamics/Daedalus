//! Hardware performance counters around a closure, for benches.
//!
//! [`Counters::new`] opens a group of events on the calling thread (`perf_event_open`, user space
//! only, on any CPU). [`Counters::measure`] runs a closure `n` times with the group enabled and
//! returns a [`Sample`]: totals, per-call values, and whether the group was multiplexed.
//!
//! Only Linux has the syscall. On other systems the same API compiles and every call returns
//! [`Error::Unsupported`].
//!
//! ```
//! use daedalus_bench_counters::{Counters, Event};
//!
//! // Counters are often unavailable (CI, containers, VMs): skip the measurement then.
//! if let Ok(mut counters) = Counters::new(&[Event::Cycles, Event::Instructions]) {
//!     let sample = counters.measure(1_000, || {
//!         std::hint::black_box((0..100u64).sum::<u64>());
//!     });
//!     if let Ok(sample) = sample {
//!         let instructions = sample.per_call(Event::Instructions);
//!         assert!(instructions.is_none_or(|n| n > 0.0));
//!     }
//! }
//! ```

use std::fmt;
use std::time::Duration;

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "linux")]
pub use linux::Counters;

#[cfg(not(target_os = "linux"))]
mod other;
#[cfg(not(target_os = "linux"))]
pub use other::Counters;

/// One hardware event. Each is a generic perf event, so the same name works on any PMU that
/// implements it; a missing one is reported, not guessed at.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Event {
    /// CPU cycles (`PERF_COUNT_HW_CPU_CYCLES`).
    Cycles,
    /// Retired instructions (`PERF_COUNT_HW_INSTRUCTIONS`).
    Instructions,
    /// Mispredicted branches (`PERF_COUNT_HW_BRANCH_MISSES`).
    BranchMisses,
    /// L1 data-cache read misses (`PERF_COUNT_HW_CACHE_L1D`, read, miss).
    L1dReadMisses,
    /// Last-level cache read misses (`PERF_COUNT_HW_CACHE_LL`, read, miss).
    LlcReadMisses,
}

impl Event {
    /// Every event this crate knows, in a fixed order.
    pub const ALL: [Event; 5] = [
        Event::Cycles,
        Event::Instructions,
        Event::BranchMisses,
        Event::L1dReadMisses,
        Event::LlcReadMisses,
    ];

    /// A short name for tables and logs (`cycles`, `instructions`, ...).
    pub fn name(self) -> &'static str {
        match self {
            Event::Cycles => "cycles",
            Event::Instructions => "instructions",
            Event::BranchMisses => "branch_misses",
            Event::L1dReadMisses => "l1d_read_misses",
            Event::LlcReadMisses => "llc_read_misses",
        }
    }
}

/// Why counters could not be opened or read.
#[derive(Debug)]
pub enum Error {
    /// The platform has no `perf_event_open` (not Linux, or the kernel lacks the syscall).
    Unsupported(&'static str),
    /// The kernel refused user-space counting. `hint` names the knob:
    /// `/proc/sys/kernel/perf_event_paranoid` (2 or lower allows counting your own threads).
    PermissionDenied { hint: String },
    /// No requested event could be opened: the PMU does not implement it. The first one is named.
    NoSuchEvent(Event),
    /// Any other failure from the kernel, with the OS error.
    Io(std::io::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Unsupported(why) => write!(f, "hardware counters unsupported: {why}"),
            Error::PermissionDenied { hint } => write!(f, "hardware counters denied: {hint}"),
            Error::NoSuchEvent(event) => {
                write!(
                    f,
                    "no requested event is available (first: {})",
                    event.name()
                )
            }
            Error::Io(err) => write!(f, "perf_event_open failed: {err}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(err) => Some(err),
            _ => None,
        }
    }
}

/// The result of [`Counters::measure`]: totals over all calls, and how well the group ran.
#[derive(Clone, Debug)]
pub struct Sample {
    iterations: u64,
    /// Totals in the order the events were opened.
    totals: Vec<(Event, u64)>,
    missing: Vec<Event>,
    time_enabled: u64,
    time_running: u64,
    elapsed: Duration,
}

impl Sample {
    /// How many times the closure ran.
    pub fn iterations(&self) -> u64 {
        self.iterations
    }

    /// The total count of `event` over the whole measurement, or `None` if it was not measured.
    pub fn total(&self, event: Event) -> Option<u64> {
        self.totals
            .iter()
            .find(|(e, _)| *e == event)
            .map(|(_, v)| *v)
    }

    /// The count of `event` per call, or `None` if it was not measured or no call ran.
    pub fn per_call(&self, event: Event) -> Option<f64> {
        let total = self.total(event)?;
        (self.iterations > 0).then(|| total as f64 / self.iterations as f64)
    }

    /// The events that were measured, in the order they were opened.
    pub fn events(&self) -> impl Iterator<Item = Event> + '_ {
        self.totals.iter().map(|(e, _)| *e)
    }

    /// Events that could not be opened on this PMU. Never silently dropped.
    pub fn missing(&self) -> &[Event] {
        &self.missing
    }

    /// Whether the group was multiplexed: it did not run for its whole enabled time, so the counts
    /// cover only part of the run. Compare with [`Sample::running_fraction`].
    pub fn multiplexed(&self) -> bool {
        self.time_running < self.time_enabled
    }

    /// The fraction of the enabled time the group was scheduled on the PMU (1.0 when not
    /// multiplexed). Counts are not scaled: divide by this to estimate the full-run count.
    pub fn running_fraction(&self) -> f64 {
        if self.time_enabled == 0 {
            1.0
        } else {
            self.time_running as f64 / self.time_enabled as f64
        }
    }

    /// Wall time of the measured loop, from enable to disable.
    pub fn elapsed(&self) -> Duration {
        self.elapsed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(iterations: u64, totals: Vec<(Event, u64)>) -> Sample {
        Sample {
            iterations,
            totals,
            missing: vec![Event::LlcReadMisses],
            time_enabled: 100,
            time_running: 100,
            elapsed: Duration::from_nanos(7),
        }
    }

    #[test]
    fn per_call_divides_the_total_and_skips_missing_events() {
        let s = sample(4, vec![(Event::Cycles, 100), (Event::Instructions, 10)]);
        assert_eq!(s.per_call(Event::Cycles), Some(25.0));
        assert_eq!(s.total(Event::Instructions), Some(10));
        assert_eq!(s.per_call(Event::BranchMisses), None);
        assert_eq!(s.missing(), &[Event::LlcReadMisses]);
        assert_eq!(
            s.events().collect::<Vec<_>>(),
            [Event::Cycles, Event::Instructions]
        );
    }

    #[test]
    fn zero_iterations_have_no_per_call_value() {
        let s = sample(0, vec![(Event::Cycles, 0)]);
        assert_eq!(s.per_call(Event::Cycles), None);
    }

    #[test]
    fn multiplexing_is_reported_from_enabled_and_running_time() {
        let mut s = sample(1, vec![]);
        assert!(!s.multiplexed());
        assert_eq!(s.running_fraction(), 1.0);
        s.time_running = 25;
        assert!(s.multiplexed());
        assert_eq!(s.running_fraction(), 0.25);
    }

    #[test]
    fn every_event_has_a_distinct_name() {
        let mut names: Vec<_> = Event::ALL.iter().map(|e| e.name()).collect();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), Event::ALL.len());
    }

    #[test]
    fn errors_name_what_went_wrong() {
        let text = Error::NoSuchEvent(Event::LlcReadMisses).to_string();
        assert!(text.contains("llc_read_misses"), "{text}");
        let text = Error::PermissionDenied {
            hint: "paranoid=4".into(),
        }
        .to_string();
        assert!(text.contains("paranoid=4"), "{text}");
    }
}
