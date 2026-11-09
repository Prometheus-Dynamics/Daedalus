//! Generated firmware doesn't touch the heap.

#![cfg(feature = "source")]

#[path = "shared/source_fixture.rs"]
mod nodes;

#[allow(dead_code)]
mod generated {
    include!("fixtures/blink.rs");
}

use daedalus_abi::counting::{CountingAlloc, counted};

#[global_allocator]
static ALLOC: CountingAlloc = CountingAlloc;

#[test]
fn push_tick_and_pop_allocate_nothing() {
    let mut graph = generated::Graph::new();
    let ((), allocated) = counted(|| {
        for tick in 0..1000_u64 {
            graph.push_sample((tick % 4096) as u16).unwrap();
            graph.tick_at(tick).unwrap();
            let _ = (graph.pop_level(), graph.pop_led(), graph.pop_rises());
        }
    });
    assert_eq!(allocated, 0);
}
