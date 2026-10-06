//! Steady-state frame-path overhead of the no-op `daedalus:frame` chain: no copies, no runtime or
//! node allocations, and the overhead report accounts for every stage.

use std::sync::Mutex;

use daedalus::engine::{EngineConfig, FrameOverheadReport, MetricsLevel};
use daedalus_frame_bench::{
    CHAIN_OUTPUT, CountingAllocator, FrameBenchConfig, FrameBenchRun, FrameFeed, FrameSourceConfig,
    SyntheticFrameSource, compile_frame_chain, run_frame_bench,
};

#[global_allocator]
static ALLOC: CountingAllocator = CountingAllocator::system();

/// Allocation counters are process-wide: one measurement at a time.
static SERIAL: Mutex<()> = Mutex::new(());

fn run(stages: usize, feed: FrameFeed, metrics: MetricsLevel) -> FrameBenchRun {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let config = EngineConfig::default()
        .with_metrics_level(metrics)
        .with_frame_overhead(256);
    let mut host = compile_frame_chain(stages, feed, config).expect("compile chain");
    let mut source = SyntheticFrameSource::new(FrameSourceConfig {
        width: 64,
        height: 48,
        feed,
        ..FrameSourceConfig::default()
    })
    .expect("frame source");
    run_frame_bench(
        &mut host,
        &mut source,
        &FrameBenchConfig::new([CHAIN_OUTPUT]).with_ticks(64, 256),
    )
    .expect("frame bench")
}

fn max(report: &FrameOverheadReport, counter: &str) -> u64 {
    report.counter(counter).expect(counter).max
}

#[test]
fn external_frame_chain_has_no_copies_and_no_runtime_allocations() {
    for stages in [1, 4, 16] {
        let run = run(stages, FrameFeed::Interface, MetricsLevel::Off);
        let report = run.overhead.as_ref().expect("overhead report");
        assert!(report.alloc_probe, "the counting allocator is installed");
        assert_eq!(report.ticks, 256);
        assert_eq!(report.counter("nodes").expect("nodes").p50, stages as u64);
        for counter in [
            "copies",
            "copied_bytes",
            "zero_copy_adapts",
            "gpu_uploads",
            "gpu_downloads",
            "runtime_allocs",
            "node_allocs",
            "host_allocs",
        ] {
            assert_eq!(max(report, counter), 0, "{stages} stages: {counter}\n{run}");
        }
        let [runtime, node, host, _other] = run.allocs_per_frame.expect("alloc probe");
        assert_eq!((runtime, node, host), (0.0, 0.0, 0.0), "{run}");
    }
}

#[test]
fn owner_feed_adds_one_zero_copy_view_per_frame() {
    let run = run(4, FrameFeed::Owner, MetricsLevel::Off);
    let report = run.overhead.as_ref().expect("overhead report");
    assert_eq!(max(report, "copies"), 0, "{run}");
    assert_eq!(report.counter("zero_copy_adapts").expect("adapts").p50, 1);
    let edge = report
        .edges
        .iter()
        .find(|edge| edge.adapts_per_tick > 0.0)
        .expect("adapting edge");
    assert_eq!(edge.adapter.as_str(), "zero_copy");
    assert!(edge.label.ends_with("stage_0.frame"), "{}", edge.label);
}

#[test]
fn stages_add_up_to_the_tick() {
    let run = run(4, FrameFeed::Interface, MetricsLevel::Off);
    let report = run.overhead.as_ref().expect("overhead report");
    let mean = |name: &str| report.stage(name).expect(name).mean;
    let parts: f64 = [
        "inject",
        "inputs",
        "adapters_zero_copy",
        "adapters_copying",
        "handlers",
        "node_io",
        "drain",
        "dispatch",
    ]
    .into_iter()
    .map(mean)
    .sum();
    let tick = mean("tick");
    assert!(
        (parts - tick).abs() <= tick * 0.01,
        "{parts} vs {tick}\n{run}"
    );
    assert!(mean("graph_overhead") <= tick);
    assert!(mean("push") > 0.0 && mean("take") > 0.0, "{run}");
    // Every edge, the host output included, delivered the frame each tick.
    assert_eq!(report.edges.len(), 5, "{run}");
}

#[test]
fn detailed_metrics_keep_the_chain_copy_free() {
    let run = run(4, FrameFeed::Interface, MetricsLevel::Detailed);
    let report = run.overhead.as_ref().expect("overhead report");
    assert_eq!(max(report, "copies"), 0);
    assert_eq!(max(report, "node_allocs"), 0);
}
