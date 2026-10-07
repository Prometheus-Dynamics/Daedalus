//! Frame-path overhead harness for host-driven Daedalus graphs.
//!
//! Measures what a graph adds between frame arrival and results: a [`SyntheticFrameSource`]
//! hands out frames in external memory (a dma-buf from `/dev/dma_heap` when available, else a
//! `memfd` mapping) through the `daedalus:frame` interface, [`run_frame_bench`] pushes one per
//! tick, ticks and takes the outputs, and returns the wall time per frame together with the
//! graph's [`FrameOverheadReport`] and allocation
//! counts. [`compile_frame_chain`] builds the reference graph: a chain of no-op stages.
//!
//! # Recipe: your own nodes
//!
//! Compile a graph whose host input `frame` is typed `daedalus:frame` (or your frame type with
//! its provider) with frame overhead enabled, then drive it with the harness:
//!
//! ```ignore
//! use daedalus_frame_bench::*;
//!
//! #[global_allocator]
//! static ALLOC: CountingAllocator = CountingAllocator::system();
//!
//! let mut registry = frame_bench_registry()?;     // synthetic frames + their provider
//! registry.install(&my_plugin)?;                  // e.g. a detector taking `FrameView<'_>`
//! let graph = registry
//!     .graph_builder()?
//!     .input_as("frame", TypeExpr::opaque(FRAME_INTERFACE_KEY))
//!     .try_node(&detector)?
//!     .try_connect("frame", &detector.input("frame"))?
//!     .try_connect(&detector.output("detections"), "detections")?
//!     .build();
//! let config = EngineConfig::default().with_frame_overhead(1024);
//! let mut host = Engine::new(config)?.compile_registry(&registry, graph)?;
//! let mut source = SyntheticFrameSource::new(FrameSourceConfig::default())?;
//! let run = run_frame_bench(&mut host, &mut source, &FrameBenchConfig::new(["detections"]))?;
//! println!("{run}");
//! ```

mod buffer;
mod chain;
mod detector;
mod perf;
mod pin;
mod source;

use core::fmt;
use std::time::Instant;

pub use buffer::{FrameBacking, FrameBuffer};
pub use chain::{
    CHAIN_INPUT, CHAIN_OUTPUT, FrameBenchPlugin, STAGE_NODE_ID, compile_frame_chain,
    compile_frame_fanout, frame_bench_registry,
};
pub use daedalus::alloc_probe::{AllocCounts, CountingAllocator};
pub use daedalus::transport::FRAME_INTERFACE_KEY;
pub use detector::{
    DETECTOR_GROUP_ID, DETECTOR_OUTPUTS, DetectorPlugin, DetectorShape, compile_detector,
    detector_graph, detector_registry, register_detector_group,
};
pub use perf::InstructionCounter;
pub use pin::{PIN_CPU_ENV, pin_from_env};
pub use source::{
    FrameFeed, FrameSourceConfig, SYNTHETIC_FRAME_KEY, SyntheticFrame, SyntheticFrameSource,
};

use daedalus::engine::{EngineError, FrameOverheadReport, HostGraph};
use daedalus::runtime::executor::NodeHandler;

/// What [`run_frame_bench`] drives.
#[derive(Clone, Debug)]
pub struct FrameBenchConfig {
    /// Frames run before measuring (queues, caches and the window settle).
    pub warmup: usize,
    /// Measured frames.
    pub ticks: usize,
    /// Host input the frames are pushed to.
    pub input: String,
    /// Host outputs taken after every tick.
    pub outputs: Vec<String>,
}

impl FrameBenchConfig {
    /// 200 warm-up and 2000 measured frames into `frame`, taking `outputs`.
    pub fn new<S: Into<String>>(outputs: impl IntoIterator<Item = S>) -> Self {
        Self {
            warmup: 200,
            ticks: 2000,
            input: CHAIN_INPUT.to_string(),
            outputs: outputs.into_iter().map(Into::into).collect(),
        }
    }

    pub fn with_ticks(mut self, warmup: usize, ticks: usize) -> Self {
        self.warmup = warmup;
        self.ticks = ticks.max(1);
        self
    }
}

/// One harness run: per-frame wall time from the host's side (push + tick + take), the graph's
/// overhead report and allocations per frame.
#[derive(Clone, Debug)]
pub struct FrameBenchRun {
    pub ticks: usize,
    pub frame_p50_ns: u64,
    pub frame_p99_ns: u64,
    pub frame_mean_ns: f64,
    /// `None` unless the graph records frame overhead (`EngineConfig::with_frame_overhead`).
    pub overhead: Option<FrameOverheadReport>,
    /// Allocations during the measured frames, per frame; `None` without the
    /// [`CountingAllocator`] installed.
    pub allocs_per_frame: Option<[f64; 4]>,
    /// User-space instructions per frame (push + tick + take and the timing around them);
    /// `None` where no instruction counter opens ([`InstructionCounter`]).
    pub instructions_per_frame: Option<f64>,
}

impl FrameBenchRun {
    /// p50 of a stage of the overhead report, in ns.
    pub fn stage_p50(&self, name: &str) -> Option<u64> {
        Some(self.overhead.as_ref()?.stage(name)?.p50)
    }

    /// Largest per-tick value of a counter of the overhead report.
    pub fn counter_max(&self, name: &str) -> Option<u64> {
        Some(self.overhead.as_ref()?.counter(name)?.max)
    }
}

impl fmt::Display for FrameBenchRun {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "frame (push + tick + take): p50 {} ns, p99 {} ns, mean {:.0} ns over {} frames",
            self.frame_p50_ns, self.frame_p99_ns, self.frame_mean_ns, self.ticks
        )?;
        if let Some(instructions) = self.instructions_per_frame {
            writeln!(f, "instructions per frame: {instructions:.0}")?;
        }
        if let Some([runtime, node, host, other]) = self.allocs_per_frame {
            writeln!(
                f,
                "allocations per frame: runtime {runtime:.2}, node {node:.2}, host {host:.2}, other {other:.2}"
            )?;
        }
        match &self.overhead {
            Some(report) => write!(f, "{report}"),
            None => writeln!(f, "(frame overhead recording off)"),
        }
    }
}

/// Drive `host` with frames from `source`: warm up, then per frame push, tick and take every
/// output, timing each frame. The overhead window is reset after warm-up, so the report covers
/// the measured frames (the last `window` of them).
pub fn run_frame_bench<H: NodeHandler + Send + Sync + 'static>(
    host: &mut HostGraph<H>,
    source: &mut SyntheticFrameSource,
    config: &FrameBenchConfig,
) -> Result<FrameBenchRun, EngineError> {
    let input = daedalus::runtime::PortId::new(config.input.as_str());
    let frame = |host: &mut HostGraph<H>, source: &mut SyntheticFrameSource| {
        host.push_payload(input.clone(), source.next_payload());
        host.tick()?;
        for output in &config.outputs {
            while host.take_payload(output).is_some() {}
        }
        Ok::<(), EngineError>(())
    };
    for _ in 0..config.warmup {
        frame(host, source)?;
    }
    host.reset_frame_overhead();
    let mut wall = vec![0u64; config.ticks.max(1)];
    let before = daedalus::alloc_probe::counts();
    let counter = InstructionCounter::start();
    for slot in wall.iter_mut() {
        let start = Instant::now();
        frame(host, source)?;
        *slot = start.elapsed().as_nanos() as u64;
    }
    let instructions = counter.as_ref().map(InstructionCounter::read);
    let during = daedalus::alloc_probe::counts().since(&before);
    let ticks = wall.len();
    let mean = wall.iter().sum::<u64>() as f64 / ticks as f64;
    wall.sort_unstable();
    let per = |count: u64| count as f64 / ticks as f64;
    Ok(FrameBenchRun {
        ticks,
        frame_p50_ns: percentile(&wall, 50),
        frame_p99_ns: percentile(&wall, 99),
        frame_mean_ns: mean,
        overhead: host.frame_overhead(),
        instructions_per_frame: instructions.map(|count| count as f64 / ticks as f64),
        allocs_per_frame: daedalus::alloc_probe::is_installed().then(|| {
            [
                per(during.runtime),
                per(during.node),
                per(during.host),
                per(during.other),
            ]
        }),
    })
}

/// `DAEDALUS_NODE_FUSION` (`0`/`off` turns node fusion off; on by default), for comparing fused
/// and unfused runs of the bench examples (`EngineConfig::with_node_fusion`).
pub fn node_fusion_from_env() -> bool {
    std::env::var("DAEDALUS_NODE_FUSION").map_or(true, |value| {
        !matches!(
            value.to_ascii_lowercase().as_str(),
            "0" | "off" | "false" | "no"
        )
    })
}

/// Nearest-rank percentile `q` of sorted `values`.
fn percentile(values: &[u64], q: usize) -> u64 {
    values
        .get((values.len() * q).div_ceil(100).saturating_sub(1))
        .copied()
        .unwrap_or(0)
}

/// Least-squares `fixed + per_node * nodes` through `(nodes, ns)` points: the fixed per-tick
/// cost and the cost each extra node adds.
pub fn fit_per_node(points: &[(usize, f64)]) -> (f64, f64) {
    let n = points.len() as f64;
    if points.len() < 2 {
        return (points.first().map_or(0.0, |point| point.1), 0.0);
    }
    let mean_x = points.iter().map(|p| p.0 as f64).sum::<f64>() / n;
    let mean_y = points.iter().map(|p| p.1).sum::<f64>() / n;
    let cov: f64 = points
        .iter()
        .map(|p| (p.0 as f64 - mean_x) * (p.1 - mean_y))
        .sum();
    let var: f64 = points.iter().map(|p| (p.0 as f64 - mean_x).powi(2)).sum();
    let slope = if var == 0.0 { 0.0 } else { cov / var };
    (mean_y - slope * mean_x, slope)
}
