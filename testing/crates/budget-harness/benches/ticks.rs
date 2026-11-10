//! Wall time of the budget table's benchmarks, for the bench workflow's comparison against the
//! previous run. Timings depend on the machine, so nothing here gates a PR.

use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use daedalus_budget_harness::detector::{detector, frame, picture};
use daedalus_budget_harness::{Stage, chain, chain_tick, idle_tick};

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
}

criterion_group!(benches, ticks);
criterion_main!(benches);
