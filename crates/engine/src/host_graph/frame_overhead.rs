//! Frame-path overhead recording for [`HostGraph`]: where each tick's time goes between frame
//! arrival and results (see "Frame-path overhead" in `docs/runtime-diagnostics.md`).

use crate::portable::Arc;
use crate::prelude::*;

use daedalus_runtime::executor::NodeHandler;
use daedalus_runtime::{FrameOverheadReport, FrameOverheadWindow, FrameProbe, FrameTickSample};

use super::HostGraph;
use crate::error::EngineError;

/// Ticks [`HostGraph::enable_frame_overhead`] keeps when configured without a size.
pub const DEFAULT_FRAME_OVERHEAD_WINDOW: usize = 512;

pub(crate) struct FrameOverheadState {
    probe: Arc<FrameProbe>,
    window: FrameOverheadWindow,
    /// Counters at the end of the previous tick, to attribute host allocations in between.
    #[cfg(feature = "alloc-probe")]
    allocs: daedalus_runtime::alloc_probe::AllocCounts,
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
            #[cfg(feature = "alloc-probe")]
            allocs: daedalus_runtime::alloc_probe::counts(),
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

    /// Clear the recorded window (e.g. after warm-up), keeping recording on.
    pub fn reset_frame_overhead(&mut self) {
        if let Some(state) = self.frame_overhead.as_mut() {
            state.window.clear();
            let _ = self.host.take_io_time();
            #[cfg(feature = "alloc-probe")]
            {
                state.allocs = daedalus_runtime::alloc_probe::counts();
            }
        }
    }

    /// p50/p99/max/mean of every stage and counter over the recorded window, plus per-edge
    /// queue and adapter time; `None` unless [`Self::enable_frame_overhead`] was called. Print it
    /// with `{}` (a table) or serialize it.
    pub fn frame_overhead(&self) -> Option<FrameOverheadReport> {
        let state = self.frame_overhead.as_ref()?;
        let labels = self.edge_labels();
        Some(
            state
                .window
                .report(&labels, self.host.pending_take_time().as_nanos() as u64),
        )
    }

    /// The most recent tick's breakdown (its take time so far included).
    pub fn last_frame_tick(&self) -> Option<FrameTickSample> {
        let mut sample = *self.frame_overhead.as_ref()?.window.latest()?;
        sample.take_ns += self.host.pending_take_time().as_nanos() as u64;
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

    /// Run one tick through `run`, recording it when frame overhead is enabled.
    pub(super) fn frame_tick<T>(
        &mut self,
        run: impl FnOnce(&mut Self) -> Result<T, EngineError>,
    ) -> Result<T, EngineError> {
        let Some(state) = self.frame_overhead.as_mut() else {
            return run(self);
        };
        // Feeds since the last tick feed this one; takes since then belong to the last tick.
        let io = self.host.take_io_time();
        if let Some(latest) = state.window.latest_mut() {
            latest.take_ns += io.take.as_nanos() as u64;
        }
        #[cfg(feature = "alloc-probe")]
        let before = daedalus_runtime::alloc_probe::counts();
        let clock = self.runner.executor.clock().clone();
        let start = clock.now();
        let result = run(self);
        let tick = clock.elapsed(start);
        #[cfg(feature = "alloc-probe")]
        let after = daedalus_runtime::alloc_probe::counts();
        if let Some(state) = self.frame_overhead.as_mut() {
            let FrameOverheadState { probe, window, .. } = &mut **state;
            window.record(|sample, edges| {
                probe.finish_tick(tick, sample, edges);
                sample.push_ns = io.push.as_nanos() as u64;
            });
            #[cfg(feature = "alloc-probe")]
            {
                let host = before.since(&state.allocs).host;
                let during = after.since(&before);
                if let Some(sample) = state.window.latest_mut() {
                    sample.runtime_allocs = during.runtime;
                    sample.node_allocs = during.node;
                    sample.runtime_alloc_bytes = during.runtime_bytes;
                    sample.node_alloc_bytes = during.node_bytes;
                    sample.host_allocs = host;
                }
                state.allocs = after;
            }
        }
        result
    }
}
