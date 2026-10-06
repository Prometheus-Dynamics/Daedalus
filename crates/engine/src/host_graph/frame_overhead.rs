//! Frame-path overhead recording for [`HostGraph`]: where each tick's time goes between frame
//! arrival and results (see "Frame-path overhead" in `docs/runtime-diagnostics.md`).
//!
//! The executor times each run into the [`FrameProbe`]; the graph moves a finished run into the
//! window when the next tick starts (and folds it in when reporting), so the tick paths only gain
//! one check while recording is off.

use crate::portable::Arc;
use crate::prelude::*;

use daedalus_runtime::executor::NodeHandler;
use daedalus_runtime::{
    EdgeTickSample, FrameOverheadReport, FrameOverheadWindow, FrameProbe, FrameTickSample,
};

use super::HostGraph;

/// Ticks [`HostGraph::enable_frame_overhead`] keeps when configured without a size.
pub const DEFAULT_FRAME_OVERHEAD_WINDOW: usize = 512;

pub(crate) struct FrameOverheadState {
    probe: Arc<FrameProbe>,
    window: FrameOverheadWindow,
    host: HostSide,
}

/// Host-side values of the run the probe holds.
#[derive(Clone, Copy, Default)]
struct HostSide {
    /// Feeds since the previous tick, taken from the bridge when the run started.
    push_ns: u64,
    /// Host-scope allocation count when the run started.
    #[cfg(feature = "alloc-probe")]
    allocs: u64,
}

impl HostSide {
    #[cfg(feature = "alloc-probe")]
    fn alloc_count() -> u64 {
        daedalus_runtime::alloc_probe::counts().host
    }

    /// The probe's run (taken when `reset`, else peeked) plus the host-side values.
    fn fill(
        self,
        probe: &FrameProbe,
        reset: bool,
        take_ns: u64,
        sample: &mut FrameTickSample,
        edges: &mut [EdgeTickSample],
    ) {
        if reset {
            probe.take_tick(sample, edges);
        } else {
            probe.peek_tick(sample, edges);
        }
        sample.push_ns = self.push_ns;
        sample.take_ns = take_ns;
        #[cfg(feature = "alloc-probe")]
        {
            sample.host_allocs = Self::alloc_count().saturating_sub(self.allocs);
        }
    }
}

impl<H: NodeHandler + Send + Sync + 'static> HostGraph<H> {
    /// Record each tick's frame-path overhead over a rolling window of `window` ticks, at any
    /// metrics level: host-bridge push and take time, input collection, adapters (zero-copy vs
    /// copying), handlers, node framing, output drain, dispatch, per-edge queue time, copies and,
    /// with the `alloc-probe` allocator installed, runtime/node/host allocations. Recording is
    /// allocation-free; the window is allocated here. Read it with [`Self::frame_overhead`].
    /// Restarts recording when already enabled.
    pub fn enable_frame_overhead(&mut self, window: usize) {
        let probe = Arc::new(FrameProbe::for_plan(self.runtime_plan()));
        let window = FrameOverheadWindow::new(window, &probe);
        self.runner.executor.set_frame_probe(Some(probe.clone()));
        self.host.set_io_timing(true);
        self.frame_overhead = Some(Box::new(FrameOverheadState {
            probe,
            window,
            host: HostSide {
                push_ns: 0,
                #[cfg(feature = "alloc-probe")]
                allocs: HostSide::alloc_count(),
            },
        }));
    }

    /// [`Self::enable_frame_overhead`] when configured (`EngineConfig::with_frame_overhead`).
    pub(crate) fn with_configured_frame_overhead(mut self, window: Option<usize>) -> Self {
        if let Some(window) = window {
            self.enable_frame_overhead(window);
        }
        self
    }

    /// Stop recording frame-path overhead and drop the window.
    pub fn disable_frame_overhead(&mut self) {
        self.frame_overhead = None;
        self.runner.executor.set_frame_probe(None);
        self.host.set_io_timing(false);
    }

    pub fn frame_overhead_enabled(&self) -> bool {
        self.frame_overhead.is_some()
    }

    /// Start the window over (e.g. after warm-up), keeping recording on.
    pub fn reset_frame_overhead(&mut self) {
        if let Some(window) = self
            .frame_overhead
            .as_ref()
            .map(|state| state.window.capacity())
        {
            self.enable_frame_overhead(window);
        }
    }

    /// p50/p99/max/mean of every stage and counter over the recorded window (the latest tick
    /// included), plus per-edge queue and adapter time; `None` unless
    /// [`Self::enable_frame_overhead`] was called. Print it with `{}` (a table) or serialize it.
    pub fn frame_overhead(&self) -> Option<FrameOverheadReport> {
        let state = self.frame_overhead.as_ref()?;
        let labels = self.edge_labels();
        if !state.probe.has_tick() {
            return Some(state.window.report(&labels));
        }
        let mut window = state.window.clone();
        let take_ns = self.host.pending_take_time().as_nanos() as u64;
        window.record(|sample, edges| state.host.fill(&state.probe, false, take_ns, sample, edges));
        Some(window.report(&labels))
    }

    /// The most recent tick's breakdown (its take time so far included).
    pub fn last_frame_tick(&self) -> Option<FrameTickSample> {
        let state = self.frame_overhead.as_ref()?;
        if !state.probe.has_tick() {
            return state.window.latest().copied();
        }
        let mut sample = FrameTickSample::default();
        let mut edges = vec![EdgeTickSample::default(); state.probe.edge_count()];
        let take_ns = self.host.pending_take_time().as_nanos() as u64;
        state
            .host
            .fill(&state.probe, false, take_ns, &mut sample, &mut edges);
        Some(sample)
    }

    /// `from.port -> to.port` per plan edge, with node labels.
    fn edge_labels(&self) -> Vec<String> {
        let label = |idx: usize| self.node_labels.get(idx).map(String::as_str).unwrap_or("?");
        self.runtime_plan()
            .edges
            .iter()
            .map(|edge| {
                format!(
                    "{}.{} -> {}.{}",
                    label(edge.from().0),
                    edge.source_port(),
                    label(edge.to().0),
                    edge.target_port()
                )
            })
            .collect()
    }

    /// Before a tick: move the previous run into the window and claim the feeds since then for
    /// the coming one. One check while recording is off.
    #[inline]
    pub(super) fn frame_commit(&mut self) {
        if self.frame_overhead.is_some() {
            self.frame_commit_recorded();
        }
    }

    // Out of line so the tick paths keep their size.
    #[inline(never)]
    fn frame_commit_recorded(&mut self) {
        let io = self.host.take_io_time();
        let Some(state) = self.frame_overhead.as_deref_mut() else {
            return;
        };
        let FrameOverheadState {
            probe,
            window,
            host,
        } = state;
        if probe.has_tick() {
            let take_ns = io.take.as_nanos() as u64;
            window.record(|sample, edges| host.fill(probe, true, take_ns, sample, edges));
        }
        *host = HostSide {
            push_ns: io.push.as_nanos() as u64,
            #[cfg(feature = "alloc-probe")]
            allocs: HostSide::alloc_count(),
        };
    }
}
