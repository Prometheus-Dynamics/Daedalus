//! Four threads, each with its own host, allocate nothing per tick either.

use std::thread;

use daedalus_abi::counting::{CountingAlloc, counted};
use daedalus_nostd_smoke::{Smoke, increment};

#[global_allocator]
static ALLOC: CountingAlloc = CountingAlloc;

fn steady(thread: i64) -> usize {
    let mut smoke = Smoke::new();
    let mut host = smoke.host();
    let mut clock = || 0;
    increment(&mut host, thread, &mut clock).unwrap();
    let ((), allocated) = counted(|| {
        for x in 0..1000 {
            let expected = thread * 10_000 + x;
            let out = increment(&mut host, expected, &mut clock).unwrap();
            assert_eq!(out, Some(expected + 1));
        }
    });
    allocated
}

#[test]
fn four_threads_each_with_a_host_allocate_nothing() {
    let threads: Vec<_> = (0..4)
        .map(|thread| thread::spawn(move || steady(thread)))
        .collect();
    for thread in threads {
        assert_eq!(thread.join().unwrap(), 0);
    }
}
