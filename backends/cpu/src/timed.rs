//! Timing a stage of a body for one of its node's metrics.

use daedalus_abi::{MetricId, StepCx};

/// `ctx.timed(id, || work)`: runs the work and reports how long it took as the node's metric
/// `id`. In scope wherever `daedalus_cpu_author::Timed` is imported.
pub trait Timed {
    /// Runs `work` and reports its duration, in nanoseconds, as the node's metric `id`. No clock
    /// is read when the host reads no metrics.
    fn timed<R>(&self, id: impl MetricId, work: impl FnOnce() -> R) -> R;
}

impl Timed for StepCx {
    #[inline]
    fn timed<R>(&self, id: impl MetricId, work: impl FnOnce() -> R) -> R {
        if !self.wants_metrics() {
            return work();
        }
        let start = std::time::Instant::now();
        let out = work();
        self.metric(id, start.elapsed().as_nanos() as u64);
        out
    }
}
