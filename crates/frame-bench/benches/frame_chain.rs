//! One frame through a chain of N no-op `daedalus:frame` stages: push a synthetic external frame,
//! tick, take the output. `interface` feeds `daedalus:frame` handles (no adapter), `owner` feeds
//! the frame type (one `View` adapter on the first edge); `+overhead` records frame overhead.
//!
//! Run with `cargo bench -p daedalus-frame-bench --bench frame_chain`; for the per-stage
//! breakdown run the `frame_chain` example.

use std::hint::black_box;
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use daedalus::engine::{EngineConfig, MetricsLevel};
use daedalus_frame_bench::{
    CHAIN_INPUT, CHAIN_OUTPUT, FrameFeed, FrameSourceConfig, SyntheticFrameSource,
    compile_frame_chain,
};

fn bench_frame_chain(c: &mut Criterion) {
    let mut group = c.benchmark_group("frame_chain");
    group.throughput(Throughput::Elements(1));
    let cases = [
        (FrameFeed::Interface, false),
        (FrameFeed::Owner, false),
        (FrameFeed::Interface, true),
    ];
    for (feed, overhead) in cases {
        for stages in [1usize, 4, 16] {
            let mut config = EngineConfig::default().with_metrics_level(MetricsLevel::Off);
            if overhead {
                config = config.with_frame_overhead(1024);
            }
            let mut host = compile_frame_chain(stages, feed, config).expect("compile chain");
            let mut source = SyntheticFrameSource::new(FrameSourceConfig {
                feed,
                ..FrameSourceConfig::default()
            })
            .expect("frame source");
            let name = format!(
                "{}{}",
                feed.as_str(),
                if overhead { "+overhead" } else { "" }
            );
            group.bench_function(BenchmarkId::new(name, stages), |b| {
                b.iter(|| {
                    host.push_payload(CHAIN_INPUT, source.next_payload());
                    host.tick().expect("tick");
                    black_box(host.take_payload(CHAIN_OUTPUT))
                })
            });
        }
    }
    group.finish();
}

fn config() -> Criterion {
    Criterion::default()
        .sample_size(30)
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(2))
}

criterion_group! {
    name = benches;
    config = config();
    targets = bench_frame_chain
}
criterion_main!(benches);
