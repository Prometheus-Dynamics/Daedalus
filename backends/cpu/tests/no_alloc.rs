//! A step allocates nothing once it's built.

use crate::common;

use common::{call, inline, step_of, value};
use daedalus_abi::counting::counted;
use daedalus_abi::{Slot, StepCx};
use daedalus_macros::node;

#[node(id = "t.mix", inputs(a, b), outputs(sum, flag))]
fn mix(a: u32, b: &u64) -> (u64, bool) {
    (u64::from(a) + b, a > 3)
}

#[test]
fn a_thousand_runs_allocate_nothing() {
    let mut step = step_of(&MIX_NODE);
    let mut cx = StepCx::new();
    let ((), allocations) = counted(|| {
        for round in 0..1000_u32 {
            let (mut a, mut b) = ([0; 16], [0; 16]);
            let mut cells = [
                inline(round, &mut a),
                inline(u64::from(round), &mut b),
                Slot::EMPTY,
                Slot::EMPTY,
            ];
            call(&mut step, &mut cells, &mut cx);
            assert_eq!(value::<u64>(&cells[2]), u64::from(round) * 2);
        }
    });
    assert_eq!(allocations, 0);
}

#[node(
    id = "t.staged",
    inputs(a),
    outputs(out),
    metrics(stage = "ns", found = "count")
)]
fn staged(a: u32, ctx: &daedalus_cpu::Ctx) -> u32 {
    use daedalus_cpu::Timed;
    let out = ctx.timed(StagedMetric::Stage, || a + 1);
    ctx.metric(StagedMetric::Found, u64::from(a));
    out
}

#[test]
fn reporting_a_metric_allocates_nothing_with_cells_or_without() {
    let cells = [
        daedalus_abi::MetricCell::new(),
        daedalus_abi::MetricCell::new(),
    ];
    for on in [false, true] {
        let mut step = step_of(&STAGED_NODE);
        let mut cx = StepCx::new();
        if on {
            cx.metrics = cells.as_ptr();
            cx.metrics_len = 2;
        }
        let ((), allocations) = counted(|| {
            for round in 0..1000_u32 {
                let mut a = [0; 16];
                let mut slots = [inline(round, &mut a), Slot::EMPTY];
                call(&mut step, &mut slots, &mut cx);
                assert_eq!(value::<u32>(&slots[1]), round + 1);
            }
        });
        assert_eq!(allocations, 0, "cells {on}");
    }
    // Only the run with cells wrote: a thousand values, 0 to 999.
    let (sum, count, max) = cells[1].read();
    assert_eq!((sum, count, max), (499_500, 1000, 999));
    assert_eq!(cells[0].read().1, 1000);
}
