//! The cost of polling a ticket.

use std::time::Instant;

use daedalus_wgpu::{Storage, WgpuBackend};

use super::{MIB, RUNS, WARM, median_of, us};

pub fn run(backend: &WgpuBackend) {
    let copy = daedalus_wgpu_conformance::Op::new(backend, &daedalus_wgpu_conformance::COPY);
    // Polling a ticket that is still pending: a 16 MiB copy, polled once straight after submit.
    let n = 16 * MIB;
    let src = Storage::new(backend, n).expect("a buffer");
    let dst = Storage::new(backend, n).expect("a buffer");
    src.write(0, &vec![7u8; n as usize]).expect("in range");
    let mut pending_polls = Vec::with_capacity(RUNS);
    let mut pending_count = 0usize;
    for round in 0..(WARM + RUNS) {
        let ticket = copy.run(&src, &dst);
        let start = Instant::now();
        let done = ticket.is_done();
        let took = start.elapsed();
        if round >= WARM {
            pending_polls.push(took);
            if !done {
                pending_count += 1;
            }
        }
        ticket.wait().expect("finishes");
    }
    // Polling a ticket that has finished, which a step pays on every later run.
    let finished = copy.run(&src, &dst);
    finished.wait().expect("finishes");
    let mut done_polls = Vec::with_capacity(10_000);
    for _ in 0..10_000 {
        let start = Instant::now();
        std::hint::black_box(finished.is_done());
        done_polls.push(start.elapsed());
    }
    println!(
        "poll a 16 MiB copy straight after submit: median {:.3} us, still pending on {pending_count} of {RUNS} runs",
        us(median_of(&mut pending_polls))
    );
    println!(
        "poll a finished ticket, median of 10000: {:.3} us",
        us(median_of(&mut done_polls))
    );
}
