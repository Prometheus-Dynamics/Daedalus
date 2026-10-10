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
//! overhead parallel frames actually show. A frame's overhead is its wall time minus what its
//! segments already forced: the critical path `L` of the frame's measured segment times, or the
//! busiest thread's segment time, whichever is larger, divided over the frame's `S` segments. The
//! calling thread's segments are segment work, so when late workers leave it most of a frame that
//! time is not counted as dispatch. Parallel needs a predicted gain of at least
//! [`ENTER_GAIN`] of `T`; it is kept while the gain stays above [`EXIT_GAIN`], and no switch
//! happens within [`MIN_DWELL`] frames of the last one. Before the first measurement, segments
//! with a heavy hint (GPU affinity or [`NODE_COST_META_KEY`] `"heavy"`) count as
//! [`HEAVY_PRIOR`], others as free, so graphs of cheap nodes start, and stay, serial.

use crate::prelude::*;
use core::time::Duration;

use crate::plan::{NODE_COST_META_KEY, RuntimeNode};
use daedalus_planner::ComputeAffinity;

use super::CompiledSchedule;
use super::schedule_compile::CompiledSegmentGraph;

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
    /// Drop what was measured per segment (the segments changed), keeping the dispatch cost.
    pub(crate) fn forget_segments(&mut self) {
        *self = Self {
            dispatch_ns: self.dispatch_ns,
            ..Self::default()
        };
    }

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
        self.decide(&schedule.host_deferred_graph, workers)
    }

    /// The mode hysteresis: switch to parallel when the predicted gain exceeds [`ENTER_GAIN`] of
    /// serial time, back to serial below [`EXIT_GAIN`], and hold each mode for [`MIN_DWELL`]
    /// frames. Re-decides only after a fresh measurement.
    fn decide(&mut self, graph: &CompiledSegmentGraph, workers: usize) -> bool {
        if self.dwell < MIN_DWELL {
            self.dwell += 1;
        }
        if self.dwell < MIN_DWELL || !core::mem::take(&mut self.fresh) {
            return self.parallel;
        }
        let (serial, parallel) = self.estimate(graph, workers, None);
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

    /// Fold this frame's measurements in. `timing` is a parallel run's wall time and the segment
    /// time of its busiest thread; serial frames pass `None` and leave the dispatch cost alone.
    pub(crate) fn observe(
        &mut self,
        graph: &CompiledSegmentGraph,
        workers: usize,
        timing: Option<(Duration, u64)>,
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
        if let Some((wall, busiest)) = timing {
            let (_, span) = self.estimate(graph, workers, Some(busiest as f64));
            let segments = graph.total_segments.max(1) as f64;
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

    /// Predicted `(serial, parallel)` frame time. From the averages when `busiest` is `None`,
    /// with dispatch overhead added. Otherwise from this frame's measurements, with `busiest` the
    /// busiest thread's segment time (ns) and no overhead: see [`AdaptiveState::observe`].
    fn estimate(
        &mut self,
        graph: &CompiledSegmentGraph,
        workers: usize,
        busiest: Option<f64>,
    ) -> (f64, f64) {
        let cost = |segment: usize| -> f64 {
            if busiest.is_some() {
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
        let parallel = match busiest {
            // The frame's wall time covers at least its critical path and its busiest thread.
            Some(busiest) => critical.max(busiest),
            // An ideal split of the work over the workers, plus what dispatch costs.
            None => {
                let workers = workers.min(graph.width).max(1) as f64;
                critical.max(total / workers) + graph.total_segments as f64 * self.dispatch()
            }
        };
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
    let (result, timing) = match parallel {
        #[cfg(feature = "threads")]
        true => {
            let clock = exec.core.clock.clone();
            let start = clock.now();
            let result = super::parallel::run(exec, Some(costs));
            let wall = clock.elapsed(start);
            match result {
                Ok((telemetry, busiest)) => (Ok(telemetry), Some((wall, busiest))),
                Err(error) => (Err(error), None),
            }
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
        adaptive.observe(&schedule.host_deferred_graph, workers, timing);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::portable::Arc;

    const MS: u64 = 1_000_000;

    /// `n` independent segments, all ready at once.
    fn independent(n: usize) -> CompiledSegmentGraph {
        CompiledSegmentGraph {
            adjacency: Arc::new(vec![Vec::new(); n]),
            indegree: Arc::new(vec![0; n]),
            ready_segments: Arc::new((0..n).collect()),
            total_segments: n,
            topo_order: Arc::new((0..n).collect()),
            width: n,
        }
    }

    /// State with `avg` (ns) as each segment's average and `frame` as this frame's measurements.
    fn state(avg: &[f64], frame: &[u64]) -> AdaptiveState {
        AdaptiveState {
            measured: true,
            cost_ns: avg.to_vec(),
            frame_ns: frame.to_vec(),
            finish_ns: vec![0.0; avg.len()],
            ..AdaptiveState::default()
        }
    }

    #[test]
    fn late_worker_frame_has_no_dispatch_overhead() {
        let graph = independent(4);
        // The calling thread ran three 3 ms segments and the late worker one free segment. The
        // wall time (9 ms) is the caller's own work, not dispatch.
        let frame = [3 * MS, 3 * MS, 3 * MS, 0];
        let mut state = state(&[0.0; 4], &frame);
        for _ in 0..40 {
            state.observe(&graph, 2, Some((Duration::from_nanos(9 * MS), 9 * MS)));
        }
        assert!(state.dispatch() < 1.0, "dispatch {} ns", state.dispatch());
    }

    #[test]
    fn wall_beyond_critical_path_and_busiest_thread_is_overhead() {
        let graph = independent(4);
        // Four 3 ms segments, the busiest thread ran 9 ms, the frame took 15 ms: 6 ms is not
        // covered by any thread's segment work, so it is 1.5 ms of dispatch per segment.
        let frame = [3 * MS; 4];
        let mut state = state(&[0.0; 4], &frame);
        for _ in 0..60 {
            state.observe(&graph, 2, Some((Duration::from_nanos(15 * MS), 9 * MS)));
        }
        let expected = 1.5 * MS as f64;
        assert!(
            (state.dispatch() - expected).abs() < 1_000.0,
            "dispatch {} ns, expected {expected} ns",
            state.dispatch()
        );
    }

    /// The decision for four segments of `per_segment` ns each, on two workers, starting in
    /// `mode`, with `overhead` per segment.
    fn decision(mode: bool, per_segment: u64, overhead: Duration) -> bool {
        let graph = independent(4);
        let mut state = state(&[per_segment as f64; 4], &[per_segment; 4]);
        state.parallel = mode;
        state.dwell = MIN_DWELL;
        state.fresh = true;
        state.set_dispatch_overhead(overhead);
        state.decide(&graph, 2)
    }

    #[test]
    fn switches_to_parallel_when_the_gain_clears_enter_gain() {
        // Serial 12 ms; parallel max(3, 12 / 2) = 6 ms with no overhead: a 50% gain.
        assert!(decision(false, 3 * MS, Duration::ZERO));
    }

    #[test]
    fn hysteresis_holds_the_mode_between_the_thresholds() {
        // Overhead 1 ms per segment: parallel 6 + 4 = 10 ms, a gain of 2 of 12 ms (16.7%). Below
        // ENTER_GAIN (25%) serial stays serial; above EXIT_GAIN (5%) parallel stays parallel.
        let overhead = Duration::from_millis(1);
        assert!(
            !decision(false, 3 * MS, overhead),
            "entered parallel below ENTER_GAIN"
        );
        assert!(
            decision(true, 3 * MS, overhead),
            "left parallel above EXIT_GAIN"
        );
    }

    #[test]
    fn returns_to_serial_once_segments_are_cheap_and_overhead_dominates() {
        // 0.1 ms segments: serial 0.4 ms; parallel max(0.1, 0.2) + 4 x 1 ms = 4.2 ms.
        assert!(!decision(true, MS / 10, Duration::from_millis(1)));
    }
}
