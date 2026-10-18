//! Per-frame cost of one frame fanned out to N parallel no-op stages (N = `FRAME_BENCH_FANOUT`,
//! default 16), each feeding its own host output. The stages do no work, so the frame time is
//! the runtime's dispatch cost; `FRAME_BENCH_MODE` picks the runtime mode (serial when unset).
//! Prints the frame p50 and p99 and the frame-overhead table.
//!
//! ```text
//! FRAME_BENCH_MODE=parallel cargo run --release -p daedalus-frame-bench --example fanout
//! FRAME_BENCH_FANOUT=4 FRAME_CHAIN_TICKS=20000 cargo run --release -p daedalus-frame-bench --example fanout
//! ```
// The report is this example's output.
#![allow(clippy::print_stdout)]

use daedalus::engine::{EngineConfig, MetricsLevel};
use daedalus_frame_bench::{
    CHAIN_OUTPUT, CountingAllocator, FrameBenchConfig, FrameFeed, FrameSourceConfig,
    SyntheticFrameSource, compile_frame_fanout, run_frame_bench, runtime_mode_from_env,
};

#[global_allocator]
static ALLOC: CountingAllocator = CountingAllocator::system();

type Error = Box<dyn std::error::Error + Send + Sync>;

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn main() -> Result<(), Error> {
    if let Some(cpu) = daedalus_frame_bench::pin_from_env()? {
        println!("pinned to CPU {cpu}");
    }
    let ticks = env_usize("FRAME_CHAIN_TICKS", 5000);
    let width = env_usize("FRAME_BENCH_FANOUT", 16).max(1);
    let mode = runtime_mode_from_env()?;
    let outputs: Vec<String> = (0..width).map(|idx| format!("{CHAIN_OUTPUT}_{idx}")).collect();
    let bench = FrameBenchConfig::new(outputs).with_ticks(ticks / 5, ticks);
    let config = EngineConfig::default()
        .with_metrics_level(MetricsLevel::Off)
        .with_node_fusion(daedalus_frame_bench::node_fusion_from_env())
        .with_runtime_mode(mode.clone());
    let mut host = compile_frame_fanout(width, FrameFeed::Owner, config)?;
    let mut source = SyntheticFrameSource::new(FrameSourceConfig {
        feed: FrameFeed::Owner,
        ..FrameSourceConfig::default()
    })?;
    let run = run_frame_bench(&mut host, &mut source, &bench)?;
    println!(
        "fanout {width} stages, mode {mode:?}, {} nodes\n{run}",
        host.node_labels().len()
    );
    Ok(())
}
