//! Per-frame overhead of a detector-shaped graph (five stages, the frame fanned out to four of
//! them, config-struct constants, `Arc`'d struct outputs), as the flat per-stage graph and as one
//! group node the planner expands. Prints wall time, allocations and the frame-overhead table.
//!
//! ```text
//! cargo run --release -p daedalus-frame-bench --example detector
//! FRAME_CHAIN_TICKS=20000 cargo run --release -p daedalus-frame-bench --example detector
//! DAEDALUS_NODE_FUSION=0 cargo run --release -p daedalus-frame-bench --example detector
//! ```
// The report is this example's output.
#![allow(clippy::print_stdout)]

use daedalus::engine::{EngineConfig, MetricsLevel};
use daedalus_frame_bench::{
    CountingAllocator, DETECTOR_OUTPUTS, DetectorShape, FrameBenchConfig, FrameFeed,
    FrameSourceConfig, SyntheticFrameSource, compile_detector, run_frame_bench,
};

#[global_allocator]
static ALLOC: CountingAllocator = CountingAllocator::system();

type Error = Box<dyn std::error::Error + Send + Sync>;

fn main() -> Result<(), Error> {
    if let Some(cpu) = daedalus_frame_bench::pin_from_env()? {
        println!("pinned to CPU {cpu}");
    }
    let ticks = std::env::var("FRAME_CHAIN_TICKS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(5000);
    let bench = FrameBenchConfig::new(DETECTOR_OUTPUTS).with_ticks(ticks / 5, ticks);
    // `DETECTOR_ONLY=flat|group` runs one shape with recording off (for profilers).
    let only = std::env::var("DETECTOR_ONLY").ok();
    let records: &[bool] = if only.is_some() {
        &[false]
    } else {
        &[false, true]
    };
    for &record in records {
        for shape in [DetectorShape::Flat, DetectorShape::Group] {
            if only.as_deref().is_some_and(|only| only != shape.as_str()) {
                continue;
            }
            let mut config = EngineConfig::default()
                .with_metrics_level(MetricsLevel::Off)
                .with_node_fusion(daedalus_frame_bench::node_fusion_from_env());
            if record {
                config = config.with_frame_overhead(ticks);
            }
            let mut host = compile_detector(shape, config)?;
            let mut source = SyntheticFrameSource::new(FrameSourceConfig {
                feed: FrameFeed::Owner,
                ..FrameSourceConfig::default()
            })?;
            let run = run_frame_bench(&mut host, &mut source, &bench)?;
            println!(
                "== {} ({} nodes), recording {} ==\n{run}",
                shape.as_str(),
                host.node_labels().len(),
                if record { "on" } else { "off" }
            );
        }
    }
    Ok(())
}
