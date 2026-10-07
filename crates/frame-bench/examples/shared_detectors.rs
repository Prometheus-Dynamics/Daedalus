//! Per-frame cost of N detector graphs on one camera: each a full detector (preprocessing
//! included) against one shared preprocessing graph fanned out to N detector tails, hand-laid
//! and found by structural sharing. Prints wall time, instructions, allocations and the
//! domain's sharing counters.
//!
//! ```text
//! cargo run --release -p daedalus-frame-bench --example shared_detectors
//! FRAME_CHAIN_TICKS=20000 SHARED_DETECTORS=4 cargo run --release -p daedalus-frame-bench --example shared_detectors
//! ```
// The report is this example's output.
#![allow(clippy::print_stdout)]

use daedalus::engine::{EngineConfig, MetricsLevel};
use daedalus_frame_bench::{
    CountingAllocator, DETECTOR_OUTPUTS, Dictionary, FrameBenchConfig, FrameFeed,
    FrameSourceConfig, SyntheticFrameSource, compile_separate_detectors, compile_shared_detectors,
    compile_structural_detectors, dictionary_name, run_domain_bench,
};

#[global_allocator]
static ALLOC: CountingAllocator = CountingAllocator::system();

type Error = Box<dyn std::error::Error + Send + Sync>;

const ALL: [Dictionary; 4] = [
    Dictionary::AprilTag36h11,
    Dictionary::Aruco4x4_50,
    Dictionary::AprilTag16h5,
    Dictionary::Aruco6x6_250,
];

fn main() -> Result<(), Error> {
    if let Some(cpu) = daedalus_frame_bench::pin_from_env()? {
        println!("pinned to CPU {cpu}");
    }
    let env = |name: &str, default: usize| {
        std::env::var(name)
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(default)
    };
    let ticks = env("FRAME_CHAIN_TICKS", 5000);
    let detectors = &ALL[..env("SHARED_DETECTORS", 2).clamp(1, ALL.len())];
    let bench = FrameBenchConfig::new(DETECTOR_OUTPUTS).with_ticks(ticks / 5, ticks);
    let outputs: Vec<(&str, &str)> = detectors
        .iter()
        .flat_map(|&d| DETECTOR_OUTPUTS.map(|port| (dictionary_name(d), port)))
        .collect();
    for record in [false, true] {
        for layout in ["separate", "shared", "structural"] {
            let config = EngineConfig::default().with_metrics_level(MetricsLevel::Off);
            let mut domain = match layout {
                "separate" => compile_separate_detectors(detectors, config)?,
                "shared" => compile_shared_detectors(detectors, config)?,
                _ => compile_structural_detectors(detectors, config)?,
            };
            if record {
                domain.enable_frame_overhead(ticks);
            }
            let mut source = SyntheticFrameSource::new(FrameSourceConfig {
                feed: FrameFeed::Owner,
                ..FrameSourceConfig::default()
            })?;
            let run = run_domain_bench(&mut domain, &mut source, &bench, &outputs)?;
            println!(
                "== {layout}, {} detectors, recording {} ==\n{}",
                detectors.len(),
                if record { "on" } else { "off" },
                if record {
                    run.to_string()
                } else {
                    format!("{run}{}", domain.stats())
                }
            );
            if layout == "structural" && !record {
                println!("{}", domain.explain());
            }
        }
    }
    Ok(())
}
