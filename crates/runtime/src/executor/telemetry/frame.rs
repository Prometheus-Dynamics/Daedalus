//! Frame-path overhead probe: per-tick counters the executor fills while a probe is attached
//! (`OwnedExecutor::set_frame_probe`, `HostGraph::enable_frame_overhead`), independent of the
//! metrics level. Recording is a few relaxed atomic adds per node and edge and never allocates;
//! without a probe each site is one `None` check. The owner moves each finished run out with
//! [`FrameProbe::take_tick`].

use crate::portable::AtomicU64;
use crate::prelude::*;
use core::fmt;
use core::sync::atomic::Ordering::Relaxed;
use core::time::Duration;

use daedalus_core::platform::{Clock, Instant};

use crate::plan::RuntimePlan;

/// Executor time a probe sums per tick.
#[derive(Clone, Copy)]
pub(crate) enum ProbeTime {
    /// Wall time of the executor run (`OwnedExecutor::run_*`), drain included.
    Tick,
    /// Host inputs fanned out to graph edges.
    Inject,
    /// Input collection per node: queue pops and adapters.
    Collect,
    /// Node handler calls.
    Handlers,
    /// Whole node runs: io setup, handler, flush and output publishing.
    NodeRuns,
    /// Graph outputs handed to the host bridge.
    Drain,
    ZeroCopyAdapters,
    CopyingAdapters,
    /// Enqueue to dequeue, over every edge.
    QueueWait,
}

const TIMES: usize = 9;

/// Executor events a probe counts per tick.
#[derive(Clone, Copy)]
pub(crate) enum ProbeCount {
    /// Executor runs since the last [`FrameProbe::take_tick`].
    Ticks,
    Nodes,
    ZeroCopyAdapts,
    Copies,
    CopiedBytes,
    /// Payload clones for fan-out: an `Arc` increment, no data copy.
    SharedClones,
    GpuUploads,
    GpuDownloads,
    RuntimeAllocs,
    NodeAllocs,
    RuntimeAllocBytes,
    NodeAllocBytes,
}

const COUNTS: usize = 12;

/// What an edge's adapter path does to the payload (see `RuntimeEdgeTransport::copies_data`).
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum EdgeAdapterClass {
    /// No adapter path.
    #[default]
    None,
    /// Only zero-copy steps (identity, views, metadata-only, in-place).
    ZeroCopy,
    /// At least one step that may copy (copy-on-write, branch, materialize, device transfer,
    /// (de)serialization, custom).
    Copying,
}

impl EdgeAdapterClass {
    pub fn as_str(self) -> &'static str {
        match self {
            EdgeAdapterClass::None => "none",
            EdgeAdapterClass::ZeroCopy => "zero_copy",
            EdgeAdapterClass::Copying => "copying",
        }
    }
}

#[derive(Clone, Copy, Default)]
struct EdgeClass {
    class: EdgeAdapterClass,
    uploads: u64,
    downloads: u64,
}

#[derive(Default)]
struct EdgeCells {
    wait_ns: AtomicU64,
    adapter_ns: AtomicU64,
    adapts: AtomicU64,
}

/// One tick's per-edge overhead.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct EdgeTickSample {
    /// Enqueue to dequeue, summed over the payloads the edge delivered.
    pub wait_ns: u64,
    /// Adapter path time.
    pub adapter_ns: u64,
    /// Adapter path runs.
    pub adapts: u64,
}

/// One tick of a host-driven graph, broken down by where its time went (ns) plus its copy and
/// allocation counters.
///
/// `tick_ns = inject + inputs + adapters + node_io + handlers + drain + dispatch` (serial runs;
/// parallel node runs overlap, so `dispatch` saturates at zero). `push_ns` and `take_ns` are host
/// bridge calls outside the tick: feeds since the previous tick and takes after it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FrameTickSample {
    /// Host-bridge feeds (`push*`, bound inputs) since the previous tick.
    pub push_ns: u64,
    /// Wall time of the tick (`HostGraph::tick*`).
    pub tick_ns: u64,
    /// Host inputs fanned out to graph edges.
    pub inject_ns: u64,
    /// Input collection (queue pops, direct slots), adapters excluded.
    pub inputs_ns: u64,
    pub adapters_zero_copy_ns: u64,
    pub adapters_copying_ns: u64,
    /// Node handler calls.
    pub handlers_ns: u64,
    /// Node framing around handlers: io setup, flush, output publishing and fan-out.
    pub node_io_ns: u64,
    /// Graph outputs handed to the host bridge.
    pub drain_ns: u64,
    /// The rest of the tick: run setup, scheduling and readiness checks between nodes.
    pub dispatch_ns: u64,
    /// Host-bridge takes, drains and `inspect_*` after the tick.
    pub take_ns: u64,
    /// `tick_ns - handlers_ns`: everything the runtime adds to the handlers' own work.
    pub graph_overhead_ns: u64,
    /// Enqueue to dequeue over every edge (latency; overlaps the stages above).
    pub queue_wait_ns: u64,
    pub nodes: u64,
    /// Zero-copy adapter path runs.
    pub zero_copy_adapts: u64,
    /// Copying adapter path runs.
    pub copies: u64,
    /// Estimated bytes the copying adapter paths produced.
    pub copied_bytes: u64,
    /// Payload clones for fan-out (`Arc` increments).
    pub shared_clones: u64,
    pub gpu_uploads: u64,
    pub gpu_downloads: u64,
    /// Allocations by the runtime during the tick (`alloc-probe`).
    pub runtime_allocs: u64,
    /// Allocations inside node handlers during the tick (`alloc-probe`).
    pub node_allocs: u64,
    /// Allocations by host-bridge feeds and takes between this tick and the next (its takes and
    /// the next tick's feeds; `alloc-probe`).
    pub host_allocs: u64,
    pub runtime_alloc_bytes: u64,
    pub node_alloc_bytes: u64,
}

/// Per-tick overhead counters shared by an executor and its host (see the module docs).
pub struct FrameProbe {
    times: [AtomicU64; TIMES],
    counts: [AtomicU64; COUNTS],
    edges: Box<[EdgeCells]>,
    classes: Box<[EdgeClass]>,
}

impl fmt::Debug for FrameProbe {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FrameProbe")
            .field("edges", &self.edges.len())
            .finish_non_exhaustive()
    }
}

impl FrameProbe {
    /// A probe for `plan`'s edges, classifying each edge's adapter path once.
    pub fn for_plan(plan: &RuntimePlan) -> Self {
        let classes = (0..plan.edges.len())
            .map(|idx| {
                let Some(transport) = plan.edge_transports.get(idx).and_then(Option::as_ref) else {
                    return EdgeClass::default();
                };
                if transport.adapter_steps.is_empty() {
                    return EdgeClass::default();
                }
                let (uploads, downloads) = transport.device_transfers();
                EdgeClass {
                    class: if transport.copies_data() {
                        EdgeAdapterClass::Copying
                    } else {
                        EdgeAdapterClass::ZeroCopy
                    },
                    uploads,
                    downloads,
                }
            })
            .collect();
        Self {
            times: [const { AtomicU64::new(0) }; TIMES],
            counts: [const { AtomicU64::new(0) }; COUNTS],
            edges: (0..plan.edges.len())
                .map(|_| EdgeCells::default())
                .collect(),
            classes,
        }
    }

    pub fn edge_count(&self) -> usize {
        self.edges.len()
    }

    /// The adapter class of edge `edge_idx`.
    pub fn edge_class(&self, edge_idx: usize) -> EdgeAdapterClass {
        self.classes
            .get(edge_idx)
            .map(|class| class.class)
            .unwrap_or_default()
    }

    #[inline]
    pub(crate) fn add_time(&self, time: ProbeTime, duration: Duration) {
        self.times[time as usize].fetch_add(duration.as_nanos() as u64, Relaxed);
    }

    #[inline]
    pub(crate) fn add_count(&self, count: ProbeCount, n: u64) {
        self.counts[count as usize].fetch_add(n, Relaxed);
    }

    pub(crate) fn record_queue_wait(&self, edge_idx: usize, waited: Duration) {
        let nanos = waited.as_nanos() as u64;
        self.times[ProbeTime::QueueWait as usize].fetch_add(nanos, Relaxed);
        if let Some(edge) = self.edges.get(edge_idx) {
            edge.wait_ns.fetch_add(nanos, Relaxed);
        }
    }

    /// One adapter path run on `edge_idx`; `bytes` estimates its output when it copies.
    pub(crate) fn record_adapter(
        &self,
        edge_idx: usize,
        duration: Duration,
        bytes: impl FnOnce() -> Option<u64>,
    ) {
        let class = self.classes.get(edge_idx).copied().unwrap_or_default();
        if class.class == EdgeAdapterClass::Copying {
            self.add_time(ProbeTime::CopyingAdapters, duration);
            self.add_count(ProbeCount::Copies, 1);
            self.add_count(ProbeCount::CopiedBytes, bytes().unwrap_or(0));
        } else {
            self.add_time(ProbeTime::ZeroCopyAdapters, duration);
            self.add_count(ProbeCount::ZeroCopyAdapts, 1);
        }
        self.add_count(ProbeCount::GpuUploads, class.uploads);
        self.add_count(ProbeCount::GpuDownloads, class.downloads);
        if let Some(edge) = self.edges.get(edge_idx) {
            edge.adapter_ns
                .fetch_add(duration.as_nanos() as u64, Relaxed);
            edge.adapts.fetch_add(1, Relaxed);
        }
    }

    /// Start timing an executor run (and counting its allocations with `alloc-probe`).
    #[inline]
    pub(crate) fn begin_tick(&self, clock: &Clock) -> TickStart {
        TickStart {
            at: clock.now(),
            #[cfg(feature = "alloc-probe")]
            allocs: crate::alloc_probe::counts(),
        }
    }

    /// Finish the run [`Self::begin_tick`] started.
    #[inline]
    pub(crate) fn end_tick(&self, clock: &Clock, start: TickStart) {
        self.add_time(ProbeTime::Tick, clock.elapsed(start.at));
        self.add_count(ProbeCount::Ticks, 1);
        #[cfg(feature = "alloc-probe")]
        {
            let during = crate::alloc_probe::counts().since(&start.allocs);
            self.add_count(ProbeCount::RuntimeAllocs, during.runtime);
            self.add_count(ProbeCount::NodeAllocs, during.node);
            self.add_count(ProbeCount::RuntimeAllocBytes, during.runtime_bytes);
            self.add_count(ProbeCount::NodeAllocBytes, during.node_bytes);
        }
    }

    /// Whether a run finished since the last [`Self::take_tick`].
    pub fn has_tick(&self) -> bool {
        self.counts[ProbeCount::Ticks as usize].load(Relaxed) > 0
    }

    /// Move what the runs since the last call recorded into `sample` (every field but
    /// `push_ns`, `take_ns` and `host_allocs`, which the host fills) and `edges` (one per plan
    /// edge), deriving the remainder stages from the tick wall time.
    pub fn take_tick(&self, sample: &mut FrameTickSample, edges: &mut [EdgeTickSample]) {
        self.read_tick(true, sample, edges);
    }

    /// [`Self::take_tick`] without resetting the counters.
    pub fn peek_tick(&self, sample: &mut FrameTickSample, edges: &mut [EdgeTickSample]) {
        self.read_tick(false, sample, edges);
    }

    fn read_tick(&self, reset: bool, sample: &mut FrameTickSample, edges: &mut [EdgeTickSample]) {
        let read = |cell: &AtomicU64| {
            if reset {
                cell.swap(0, Relaxed)
            } else {
                cell.load(Relaxed)
            }
        };
        let time = |time: ProbeTime| read(&self.times[time as usize]);
        let count = |count: ProbeCount| read(&self.counts[count as usize]);
        let tick_ns = time(ProbeTime::Tick);
        let collect = time(ProbeTime::Collect);
        let runs = time(ProbeTime::NodeRuns);
        let _ = count(ProbeCount::Ticks);
        sample.tick_ns = tick_ns;
        sample.inject_ns = time(ProbeTime::Inject);
        sample.adapters_zero_copy_ns = time(ProbeTime::ZeroCopyAdapters);
        sample.adapters_copying_ns = time(ProbeTime::CopyingAdapters);
        sample.inputs_ns =
            collect.saturating_sub(sample.adapters_zero_copy_ns + sample.adapters_copying_ns);
        sample.handlers_ns = time(ProbeTime::Handlers);
        sample.node_io_ns = runs.saturating_sub(sample.handlers_ns);
        sample.drain_ns = time(ProbeTime::Drain);
        sample.dispatch_ns =
            tick_ns.saturating_sub(sample.inject_ns + collect + runs + sample.drain_ns);
        sample.graph_overhead_ns = tick_ns.saturating_sub(sample.handlers_ns);
        sample.queue_wait_ns = time(ProbeTime::QueueWait);
        sample.nodes = count(ProbeCount::Nodes);
        sample.zero_copy_adapts = count(ProbeCount::ZeroCopyAdapts);
        sample.copies = count(ProbeCount::Copies);
        sample.copied_bytes = count(ProbeCount::CopiedBytes);
        sample.shared_clones = count(ProbeCount::SharedClones);
        sample.gpu_uploads = count(ProbeCount::GpuUploads);
        sample.gpu_downloads = count(ProbeCount::GpuDownloads);
        sample.runtime_allocs = count(ProbeCount::RuntimeAllocs);
        sample.node_allocs = count(ProbeCount::NodeAllocs);
        sample.runtime_alloc_bytes = count(ProbeCount::RuntimeAllocBytes);
        sample.node_alloc_bytes = count(ProbeCount::NodeAllocBytes);
        for (cells, out) in self.edges.iter().zip(edges.iter_mut()) {
            *out = EdgeTickSample {
                wait_ns: read(&cells.wait_ns),
                adapter_ns: read(&cells.adapter_ns),
                adapts: read(&cells.adapts),
            };
        }
    }
}

/// An executor run being timed ([`FrameProbe::begin_tick`]).
pub(crate) struct TickStart {
    at: Instant,
    #[cfg(feature = "alloc-probe")]
    allocs: crate::alloc_probe::AllocCounts,
}
