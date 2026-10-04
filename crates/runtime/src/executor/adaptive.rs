//! Cost model behind `run_adaptive_in_place`: run a frame in parallel only when the measured
//! work that could overlap outweighs what dispatching it to the worker pool costs.
//!
//! Parallel frames, and every [`SERIAL_SAMPLE_EVERY`]th serial frame, record each segment's wall
//! time (one `Instant` read per node serially, per segment in parallel) into a per-segment moving
//! average `c`; the mode is re-decided after each such frame. With `T = Σc`, critical
//! path `L` (longest chain of dependent segments), `W` workers and `S` segments per frame:
//!
//! ```text
//! serial   ≈ T
//! parallel ≈ max(L, T / W) + S · dispatch
//! ```
//!
//! `dispatch` starts at [`DEFAULT_DISPATCH_OVERHEAD`] (or the configured value) and tracks the
//! overhead parallel frames actually show. Parallel needs a predicted gain of at least
//! [`ENTER_GAIN`] of `T`; it is kept while the gain stays above [`EXIT_GAIN`], and no switch
//! happens within [`MIN_DWELL`] frames of the last one. Before the first measurement, segments
//! with a heavy hint (GPU affinity or [`NODE_COST_META_KEY`] `"heavy"`) count as
//! [`HEAVY_PRIOR`], others as free, so graphs of cheap nodes start, and stay, serial.

use crate::prelude::*;
use core::time::Duration;

use crate::plan::{NODE_COST_META_KEY, RuntimeNode};
use daedalus_planner::ComputeAffinity;

use super::CompiledSchedule;

/// Initial estimate of what parallel dispatch costs per segment.
pub const DEFAULT_DISPATCH_OVERHEAD: Duration = Duration::from_micros(4);
/// Predicted gain, as a fraction of serial time, needed to switch to parallel.
const ENTER_GAIN: f64 = 0.25;
/// Predicted gain below which a parallel executor goes back to serial.
const EXIT_GAIN: f64 = 0.05;
/// Frames to stay in a mode after switching.
const MIN_DWELL: u32 = 8;
/// Assumed cost of a hinted segment before it is measured.
const HEAVY_PRIOR: f64 = 1_000_000.0;
/// Weight of a new measurement in the moving averages.
const ALPHA: f64 = 0.25;
/// Serial frames between two timed ones.
const SERIAL_SAMPLE_EVERY: u32 = 4;

#[derive(Debug, Default)]
pub(crate) struct AdaptiveState {
    parallel: bool,
    measured: bool,
    /// A measurement arrived since the last decision.
    fresh: bool,
    /// Frames since the last switch.
    dwell: u32,
    /// Serial frames counted modulo `SERIAL_SAMPLE_EVERY`; the first of each round is timed.
    serial_frames: u32,
    /// Dispatch overhead per segment (ns); `None` until configured or first used.
    dispatch_ns: Option<f64>,
    /// Moving average of each segment's wall time (ns).
    cost_ns: Vec<f64>,
    /// This frame's segment wall times (ns).
    frame_ns: Vec<u64>,
    /// Scratch: earliest finish per segment for the critical path.
    finish_ns: Vec<f64>,
}

impl AdaptiveState {
    pub(crate) fn set_dispatch_overhead(&mut self, overhead: Duration) {
        self.dispatch_ns = Some(overhead.as_nanos() as f64);
    }

    /// Whether the next frame should run in parallel on `workers` threads.
    pub(crate) fn choose(
        &mut self,
        schedule: &CompiledSchedule,
        nodes: &[RuntimeNode],
        workers: usize,
    ) -> bool {
        if !can_run_parallel(schedule) || workers.min(schedule.host_deferred_graph.width) <= 1 {
            return false;
        }
        if self.cost_ns.is_empty() {
            self.init(schedule, nodes);
        }
        if self.dwell < MIN_DWELL {
            self.dwell += 1;
        }
        if self.dwell < MIN_DWELL || !core::mem::take(&mut self.fresh) {
            return self.parallel;
        }
        let (serial, parallel) = self.estimate(schedule, workers, false);
        let gain = serial - parallel;
        let threshold = if self.parallel { EXIT_GAIN } else { ENTER_GAIN };
        let parallel = gain > serial * threshold;
        if parallel != self.parallel {
            self.parallel = parallel;
            self.dwell = 0;
        }
        self.parallel
    }

    /// Zeroed per-segment slots for this frame's measurements, or `None` when this serial frame
    /// goes untimed.
    pub(crate) fn frame_costs(&mut self) -> Option<&mut [u64]> {
        if !self.parallel {
            self.serial_frames = (self.serial_frames + 1) % SERIAL_SAMPLE_EVERY;
            if self.serial_frames != 1 && self.measured {
                return None;
            }
        }
        self.frame_ns.fill(0);
        Some(&mut self.frame_ns)
    }

    /// Fold this frame's measurements in; `wall` is the parallel run's wall time.
    pub(crate) fn observe(
        &mut self,
        schedule: &CompiledSchedule,
        workers: usize,
        wall: Option<Duration>,
    ) {
        for (avg, &frame) in self.cost_ns.iter_mut().zip(&self.frame_ns) {
            let frame = frame as f64;
            *avg = if self.measured {
                *avg + ALPHA * (frame - *avg)
            } else {
                frame
            };
        }
        self.measured = true;
        self.fresh = true;
        if let Some(wall) = wall {
            let (_, span) = self.estimate(schedule, workers, true);
            let segments = schedule.host_deferred_graph.total_segments.max(1) as f64;
            let observed = (wall.as_nanos() as f64 - span).max(0.0) / segments;
            let dispatch = self.dispatch();
            self.dispatch_ns = Some(dispatch + ALPHA * (observed - dispatch));
        }
    }

    fn init(&mut self, schedule: &CompiledSchedule, nodes: &[RuntimeNode]) {
        let segments = schedule.host_deferred_graph.indegree.len();
        self.cost_ns = vec![0.0; segments];
        self.frame_ns = vec![0; segments];
        self.finish_ns = vec![0.0; segments];
        for (node, &segment) in nodes.iter().zip(schedule.segment_of.iter()) {
            if let Some(cost) = self.cost_ns.get_mut(segment)
                && is_heavy(node)
            {
                *cost = HEAVY_PRIOR;
            }
        }
        // Let the first frame decide.
        self.dwell = MIN_DWELL;
        self.fresh = true;
    }

    fn dispatch(&self) -> f64 {
        self.dispatch_ns
            .unwrap_or(DEFAULT_DISPATCH_OVERHEAD.as_nanos() as f64)
    }

    /// Predicted `(serial, parallel)` frame time from the averages, or from this frame's
    /// measurements without dispatch overhead when `frame`.
    fn estimate(&mut self, schedule: &CompiledSchedule, workers: usize, frame: bool) -> (f64, f64) {
        let graph = &schedule.host_deferred_graph;
        let cost = |segment: usize| -> f64 {
            if frame {
                self.frame_ns.get(segment).map_or(0.0, |&ns| ns as f64)
            } else {
                self.cost_ns.get(segment).copied().unwrap_or(0.0)
            }
        };
        self.finish_ns.fill(0.0);
        let (mut total, mut critical) = (0.0f64, 0.0f64);
        for &segment in graph.topo_order.iter() {
            let finish = self.finish_ns[segment] + cost(segment);
            total += cost(segment);
            critical = critical.max(finish);
            for &next in &graph.adjacency[segment] {
                self.finish_ns[next] = self.finish_ns[next].max(finish);
            }
        }
        let workers = workers.min(graph.width).max(1) as f64;
        let mut parallel = critical.max(total / workers);
        if !frame {
            parallel += graph.total_segments as f64 * self.dispatch();
        }
        (total, parallel)
    }
}

/// The segment graph has independent work: more than one ready segment, or fan-out.
pub(crate) fn can_run_parallel(schedule: &CompiledSchedule) -> bool {
    let graph = &schedule.host_deferred_graph;
    !schedule.linear_segment_flow
        && (graph.ready_segments.len() > 1 || graph.adjacency.iter().any(|next| next.len() > 1))
}

fn is_heavy(node: &RuntimeNode) -> bool {
    matches!(
        node.compute,
        ComputeAffinity::GpuPreferred | ComputeAffinity::GpuRequired
    ) || node
        .metadata
        .get(NODE_COST_META_KEY)
        .and_then(|value| value.as_str())
        == Some("heavy")
}

/// One adaptive frame on `exec` in the chosen mode, timed into `adaptive`.
pub(crate) fn run_adaptive_on<H>(
    exec: &mut super::Executor<'_, H>,
    adaptive: &mut AdaptiveState,
    parallel: bool,
    workers: usize,
) -> Result<super::ExecutionTelemetry, super::ExecuteError>
where
    H: super::NodeHandler + Send + Sync + 'static,
{
    if !can_run_parallel(&exec.schedule) {
        return super::serial::run_with_boundaries(exec);
    }
    let schedule = exec.schedule.clone();
    let Some(costs) = adaptive.frame_costs() else {
        return super::serial::run_with_boundaries(exec);
    };
    // Without threads `choose` never picks parallel (one worker).
    let (result, wall) = match parallel {
        #[cfg(feature = "threads")]
        true => {
            let clock = exec.core.clock.clone();
            let start = clock.now();
            let result = super::parallel::run(exec, Some(costs));
            (result, Some(clock.elapsed(start)))
        }
        _ => {
            let costs = super::serial::SegmentCosts {
                segment_of: &schedule.segment_of,
                costs,
            };
            (
                super::serial::run_with_boundaries_timed(exec, Some(costs)),
                None,
            )
        }
    };
    if result.is_ok() {
        adaptive.observe(&schedule, workers, wall);
    }
    result
}
