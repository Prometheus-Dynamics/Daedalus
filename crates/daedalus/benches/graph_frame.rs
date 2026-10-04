//! One frame of the detector-like graph (`tests/support/detector_graph.rs`): push a frame, tick,
//! drain four host outputs.
//!
//! Run with `cargo bench -p daedalus-rs --features engine-full,plugins --bench graph_frame`.
#[path = "../tests/support/detector_graph.rs"]
mod detector_graph;

use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use daedalus::engine::{MetricsLevel, RuntimeMode};
use detector_graph::{Frame, compile, drive_frame};

fn bench_graph_frame(c: &mut Criterion) {
    let mut group = c.benchmark_group("graph_frame");
    group.throughput(Throughput::Elements(1));
    let frame = Arc::new(Frame::new(7));
    for (name, mode, metrics) in [
        ("serial_metrics_off", RuntimeMode::Serial, MetricsLevel::Off),
        (
            "serial_metrics_basic",
            RuntimeMode::Serial,
            MetricsLevel::Basic,
        ),
        (
            "parallel_metrics_off",
            RuntimeMode::Parallel,
            MetricsLevel::Off,
        ),
        (
            "adaptive_metrics_off",
            RuntimeMode::Adaptive,
            MetricsLevel::Off,
        ),
    ] {
        let mut host = compile(mode, metrics).expect("compile detector graph");
        group.bench_function(name, |b| {
            b.iter(|| black_box(drive_frame(&mut host, &frame)))
        });
    }
    group.finish();
}

fn config() -> Criterion {
    Criterion::default()
        .sample_size(30)
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(3))
}

criterion_group! {
    name = benches;
    config = config();
    targets = bench_graph_frame
}
criterion_main!(benches);
