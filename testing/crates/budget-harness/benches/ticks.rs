//! Wall time of the budget table's benchmarks, for the bench workflow's comparison against the
//! previous run. Timings depend on the machine, so nothing here gates a PR.

use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use daedalus_budget_harness::detector::{detector, frame, picture};
use daedalus_budget_harness::{Stage, chain, chain_tick, idle_tick};
use daedalus_handoff_bench as handoff;

fn ticks(c: &mut Criterion) {
    let mut idle = chain(16, Stage::Plain, true);
    chain_tick(&mut idle, 1);
    c.bench_function("idle_tick_16", |b| {
        b.iter(|| black_box(idle_tick(&mut idle)))
    });

    let mut fused = chain(16, Stage::Plain, true);
    c.bench_function("fused_chain_16_tick", |b| {
        b.iter(|| black_box(chain_tick(&mut fused, 7)))
    });

    let mut unfused = chain(16, Stage::Plain, false);
    c.bench_function("unfused_chain_16_tick", |b| {
        b.iter(|| black_box(chain_tick(&mut unfused, 7)))
    });

    let mut host = detector();
    let picture = picture();
    c.bench_function("detector_frame", |b| {
        b.iter(|| black_box(frame(&mut host, &picture)))
    });

    // The large-value handoff (I3): a megabyte, read by 8 nodes and handed on through 8 stages.
    for path in handoff::Path::ALL {
        for (shape, name) in [
            (handoff::Shape::Readers(8), "read"),
            (handoff::Shape::Chain(8), "chain"),
        ] {
            let mut host = handoff::graph(path, shape);
            let kept = handoff::value(path, 1 << 20);
            c.bench_function(&format!("handoff_{}_{name}_tick", path.name()), |b| {
                b.iter_batched(
                    // A chain takes the value, so it gets a new one, made outside the timing.
                    || match shape {
                        handoff::Shape::Readers(_) => kept.again(),
                        _ => handoff::value(path, 1 << 20),
                    },
                    |fed| {
                        handoff::feed(&mut host, fed);
                        black_box(handoff::tick(&mut host))
                    },
                    criterion::BatchSize::LargeInput,
                )
            });
        }
    }
}

/// The I3 round trip: a 3,110,400-byte value handed to one node and back per tick
/// (testing/board-runs/I3.md). `static`: the node runs in this process, with no stable-ABI plugin.
fn i3_round_trip(c: &mut Criterion) {
    for path in handoff::Path::ALL {
        let mut round = handoff::RoundTrip::new(path, handoff::I3_BYTES);
        c.bench_function(&format!("i3_{}_static_round_tick", path.name()), |b| {
            b.iter(|| black_box(round.tick()))
        });
    }
}

criterion_group!(benches, ticks, i3_round_trip);
criterion_main!(benches);
