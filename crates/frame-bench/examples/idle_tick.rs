// The report is this example's output.
#![allow(clippy::print_stdout)]

//! Idle tick cost: a host graph of N no-op `daedalus:frame` stages (default 16), ticked with no
//! host input pushed, so every node is skipped as not ready. Prints the wall time per tick and the
//! executed-node count (zero).
//!
//! `idle_tick ITERS [STAGES]`. Instruction counts (`perf stat -e instructions:u`) do not depend
//! on host load: run it with ITERS = 0 and with ITERS = N, and divide the difference by N.
//!
//! ```text
//! perf stat -e instructions:u -- target/release/examples/idle_tick 0
//! perf stat -e instructions:u -- target/release/examples/idle_tick 200000
//! ```

use std::hint::black_box;
use std::time::Instant;

use daedalus::engine::{EngineConfig, MetricsLevel};
use daedalus_frame_bench::{FrameFeed, compile_frame_chain};

type Error = Box<dyn std::error::Error + Send + Sync>;

fn main() -> Result<(), Error> {
    let args: Vec<String> = std::env::args().collect();
    let iters: usize = args.get(1).map_or(Ok(0), |arg| arg.parse())?;
    let stages: usize = args.get(2).map_or(Ok(16), |arg| arg.parse())?;
    let config = EngineConfig::default().with_metrics_level(MetricsLevel::Off);
    let mut host = compile_frame_chain(stages, FrameFeed::Interface, config)?;
    for _ in 0..1000.min(iters) {
        let telemetry = black_box(host.tick()?);
        host.recycle_telemetry(telemetry);
    }
    let start = Instant::now();
    let mut executed = 0usize;
    for _ in 0..iters {
        let telemetry = black_box(host.tick()?);
        executed += telemetry.nodes_executed as usize;
        host.recycle_telemetry(telemetry);
    }
    let elapsed = start.elapsed();
    let per_tick = if iters == 0 {
        0.0
    } else {
        elapsed.as_nanos() as f64 / iters as f64
    };
    println!(
        "idle_tick: {stages} stages, {iters} ticks, {per_tick:.1} ns per tick, {executed} nodes executed"
    );
    Ok(())
}
