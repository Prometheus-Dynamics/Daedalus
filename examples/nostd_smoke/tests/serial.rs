//! The graph runs, on the clock it's given, and a steady-state tick allocates nothing.

use daedalus_abi::counting::{CountingAlloc, counted};
use daedalus_nostd_smoke::{Smoke, increment};

#[global_allocator]
static ALLOC: CountingAlloc = CountingAlloc;

#[test]
fn it_adds_one_on_the_clock_it_is_given() {
    let mut smoke = Smoke::new();
    let mut host = smoke.host();
    let mut readings = 0;
    let mut clock = || {
        readings += 1;
        readings
    };
    assert_eq!(increment(&mut host, 41, &mut clock).unwrap(), Some(42));
    assert_eq!(increment(&mut host, -1, &mut clock).unwrap(), Some(0));
    assert_eq!(readings, 2);
}

#[test]
fn a_steady_state_tick_allocates_nothing() {
    let mut smoke = Smoke::new();
    let mut host = smoke.host();
    let mut clock = || 0;
    increment(&mut host, 0, &mut clock).unwrap();
    let ((), allocated) = counted(|| {
        for x in 0..1000 {
            assert_eq!(increment(&mut host, x, &mut clock).unwrap(), Some(x + 1));
        }
    });
    assert_eq!(allocated, 0);
}
