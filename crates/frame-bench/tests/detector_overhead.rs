//! Steady-state frame path of the detector-shaped graph (`compile_detector`), flat and as a group
//! node: typed `#[node]` handlers with config structs, state and `Arc`'d outputs allocate
//! nothing, copy nothing, and the expanded group runs the same nodes and edges as the flat graph.

use std::sync::Mutex;

use daedalus::engine::{EngineConfig, MetricsLevel};
use daedalus_frame_bench::{
    CountingAllocator, DETECTOR_OUTPUTS, DetectorShape, FrameBenchConfig, FrameBenchRun, FrameFeed,
    FrameSourceConfig, SyntheticFrameSource, compile_detector, run_frame_bench,
};

#[global_allocator]
static ALLOC: CountingAllocator = CountingAllocator::system();

/// Allocation counters are process-wide: one measurement at a time.
static SERIAL: Mutex<()> = Mutex::new(());

fn run(shape: DetectorShape, metrics: MetricsLevel) -> FrameBenchRun {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let config = EngineConfig::default()
        .with_metrics_level(metrics)
        .with_frame_overhead(256);
    let mut host = compile_detector(shape, config).expect("compile detector");
    let mut source = SyntheticFrameSource::new(FrameSourceConfig {
        width: 64,
        height: 48,
        feed: FrameFeed::Owner,
        ..FrameSourceConfig::default()
    })
    .expect("frame source");
    run_frame_bench(
        &mut host,
        &mut source,
        &FrameBenchConfig::new(DETECTOR_OUTPUTS).with_ticks(64, 256),
    )
    .expect("frame bench")
}

#[test]
fn detector_frames_allocate_nothing() {
    for shape in [DetectorShape::Flat, DetectorShape::Group] {
        let run = run(shape, MetricsLevel::Off);
        let report = run.overhead.as_ref().expect("overhead report");
        let context = format!("{}\n{run}", shape.as_str());
        assert!(report.alloc_probe, "the counting allocator is installed");
        assert_eq!(report.counter("nodes").expect("nodes").p50, 5, "{context}");
        for counter in ["copies", "runtime_allocs", "node_allocs", "host_allocs"] {
            let max = report.counter(counter).expect(counter).max;
            assert_eq!(max, 0, "{counter}: {context}");
        }
        // Mask prep's `FrameView` is the provider's view of the owner frame, lent in place.
        let adapts = report.counter("zero_copy_adapts").expect("adapts");
        assert_eq!((adapts.p50, adapts.max), (1, 1), "{context}");
        // mask prep -> quads -> decode -> validate run as one fused unit.
        let fused = report.counter("fused_handoffs").expect("fused");
        assert_eq!((fused.p50, fused.max), (3, 3), "{context}");
        let [runtime, node, host, _other] = run.allocs_per_frame.expect("alloc probe");
        assert_eq!((runtime, node, host), (0.0, 0.0, 0.0), "{context}");
    }
}

#[test]
fn group_expands_to_the_flat_graph() {
    let edges = |shape| {
        let run = run(shape, MetricsLevel::Off);
        let mut labels: Vec<String> = run
            .overhead
            .expect("overhead report")
            .edges
            .iter()
            .map(|edge| edge.label.replace("detector::", ""))
            .collect();
        labels.sort();
        labels
    };
    assert_eq!(edges(DetectorShape::Group), edges(DetectorShape::Flat));
}
