//! How much the host records. Each level includes the one before it.

/// The host's telemetry level. Dev's Hardware level (perf counters) is not carried.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub enum Level {
    /// Nothing is recorded.
    Off,
    /// The runner's counters, and the tick timing.
    #[default]
    Basic,
    /// Also one row per step, with its calls and failure record.
    Detailed,
    /// Also one event per tick, in a bounded log.
    Trace,
}
