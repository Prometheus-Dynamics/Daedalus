//! Per-frame overhead of a host-driven graph: a synthetic external frame source (dma-heap or
//! memfd) feeds a chain of N no-op `daedalus:frame` stages (N = 1, 4, 16). Prints the fixed and
//! per-node cost, copy/allocation counters and the frame-overhead table.
//!
//! ```text
//! cargo run --release -p daedalus-frame-bench --example frame_chain
//! FRAME_CHAIN_TICKS=20000 cargo run --release -p daedalus-frame-bench --example frame_chain
//! ```
// The report is this example's output.
#![allow(clippy::print_stdout)]

use daedalus::engine::{EngineConfig, MetricsLevel};
use daedalus_frame_bench::{
    CHAIN_OUTPUT, CountingAllocator, FrameBenchConfig, FrameBenchRun, FrameFeed, FrameSourceConfig,
    SyntheticFrameSource, compile_frame_chain, fit_per_node, run_frame_bench,
};

#[global_allocator]
static ALLOC: CountingAllocator = CountingAllocator::system();

const STAGES: [usize; 3] = [1, 4, 16];

type Error = Box<dyn std::error::Error + Send + Sync>;

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn run(
    stages: usize,
    feed: FrameFeed,
    record: bool,
    bench: &FrameBenchConfig,
) -> Result<FrameBenchRun, Error> {
    let mut config = EngineConfig::default()
        .with_metrics_level(MetricsLevel::Off)
        .with_node_fusion(daedalus_frame_bench::node_fusion_from_env());
    if record {
        config = config.with_frame_overhead(bench.ticks);
    }
    let mut host = compile_frame_chain(stages, feed, config)?;
    let mut source = SyntheticFrameSource::new(FrameSourceConfig {
        feed,
        ..FrameSourceConfig::default()
    })?;
    Ok(run_frame_bench(&mut host, &mut source, bench)?)
}

fn main() -> Result<(), Error> {
    if let Some(cpu) = daedalus_frame_bench::pin_from_env()? {
        println!("pinned to CPU {cpu}");
    }
    let ticks = env_usize("FRAME_CHAIN_TICKS", 5000);
    let bench = FrameBenchConfig::new([CHAIN_OUTPUT]).with_ticks(ticks / 5, ticks);
    let source = SyntheticFrameSource::new(FrameSourceConfig::default())?;
    println!(
        "frame_chain: {} {}, 640x480 GRAY8 frames on {}, {} measured frames per run",
        std::env::consts::ARCH,
        std::env::consts::OS,
        source.backing().as_str(),
        ticks
    );
    drop(source);
    // Untimed run so the first measured one does not pay for cold caches and clock ramp-up.
    run(4, FrameFeed::Interface, false, &bench)?;

    for feed in [FrameFeed::Interface, FrameFeed::Owner] {
        println!("\n== feed: {} ==", feed.as_str());
        println!(
            "{:>6} {:>12} {:>12} {:>12} {:>12} {:>12} {:>14} {:>8} {:>10} {:>10}",
            "stages",
            "instructions",
            "frame p50",
            "frame p99",
            "tick p50",
            "handlers",
            "graph_overhead",
            "copies",
            "rt allocs",
            "node allocs"
        );
        let mut wall = Vec::new();
        let mut overhead = Vec::new();
        let mut table = None;
        for stages in STAGES {
            // Recording off: the plain cost; recording on: the breakdown.
            let plain = run(stages, feed, false, &bench)?;
            let recorded = run(stages, feed, true, &bench)?;
            let p50 = |name| recorded.stage_p50(name).unwrap_or(0);
            let [runtime, node, ..] = recorded.allocs_per_frame.unwrap_or_default();
            println!(
                "{stages:>6} {:>12.0} {:>10}ns {:>10}ns {:>10}ns {:>10}ns {:>12}ns {:>8} {:>10.2} {:>10.2}",
                plain.instructions_per_frame.unwrap_or(0.0),
                plain.frame_p50_ns,
                plain.frame_p99_ns,
                p50("tick"),
                p50("handlers"),
                p50("graph_overhead"),
                recorded.counter_max("copies").unwrap_or(0),
                runtime,
                node
            );
            wall.push((stages, plain.frame_p50_ns as f64));
            overhead.push((stages, p50("graph_overhead") as f64));
            if stages == 4 {
                table = Some(recorded);
            }
        }
        let (fixed, per_node) = fit_per_node(&wall);
        println!("frame p50 fit: {fixed:.0} ns fixed + {per_node:.0} ns per stage");
        let (fixed, per_node) = fit_per_node(&overhead);
        println!("graph_overhead p50 fit: {fixed:.0} ns fixed + {per_node:.0} ns per stage");
        if let Some(run) = table {
            println!("\n4 stages, frame overhead recorded:\n{run}");
        }
    }

    let host = compile_frame_chain(2, FrameFeed::Owner, EngineConfig::default())?;
    println!(
        "explain_plan (owner feed, 2 stages):\n{}",
        host.explain_plan()
    );
    Ok(())
}
