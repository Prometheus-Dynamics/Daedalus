//! A step allocates nothing once it's built.

mod common;

use common::{call, inline, step_of, value};
use daedalus_abi::counting::{CountingAlloc, counted};
use daedalus_abi::{Slot, StepCx};
use daedalus_macros::node;

#[global_allocator]
static ALLOC: CountingAlloc = CountingAlloc;

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
