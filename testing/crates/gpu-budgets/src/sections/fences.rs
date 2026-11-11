//! The cost of a submit that has nothing to wait for, and of polling a ticket.

use std::time::Instant;

use daedalus_wgpu::kernels::{Kernel, Kernels};
use daedalus_wgpu::{Storage, WgpuBackend};

use super::{MIB, RUNS, WARM, median, median_of, us};

pub fn run(backend: &WgpuBackend) {
    let kernels = Kernels::new(backend).expect("a device");
    // An empty submit, with no pass recorded, and an empty batch, with its pass opened and closed.
    let bare = median(WARM, RUNS, || {
        kernels.submit(kernels.encoder()).wait().expect("finishes");
    });
    let empty_pass = median(WARM, RUNS, || {
        kernels.batch().submit().wait().expect("finishes");
    });
    println!(
        "submit to done, nothing recorded: {:.1} us; an empty batch (one pass): {:.1} us",
        us(bare),
        us(empty_pass)
    );

    // Polling a ticket that is still pending: a 16 MiB copy, polled once straight after submit.
    let n = 16 * MIB;
    let src = Storage::new(backend, n).expect("a buffer");
    let dst = Storage::new(backend, n).expect("a buffer");
    src.write(0, &vec![7u8; n as usize]).expect("in range");
    let mut pending_polls = Vec::with_capacity(RUNS);
    let mut pending_count = 0usize;
    for round in 0..(WARM + RUNS) {
        let ticket = kernels.run(Kernel::Copy, &src, &dst, n).expect("runs");
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
    let finished = kernels.run(Kernel::Copy, &src, &dst, 4096).expect("runs");
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
