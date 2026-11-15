//! Allocating a 16 MiB buffer, with each memory hint, with a first write (which pays the device's
//! zero-fill), and uninitialised. Each is the median of 50 timed runs after 5 warm-up allocations,
//! with the p99 and the best of the same runs.

use std::time::{Duration, Instant};

use daedalus_wgpu::{MemoryHints, Options, Storage};

use super::{MIB, RUNS, Stats, WARM, device_with, line_ms, summary};

const SIZE: u64 = 16 * MIB;

/// The stats of `RUNS` times that `body` reports, after `WARM` untimed calls. The buffer is
/// dropped inside `body`, so each run pays for its own allocation.
fn sample(mut body: impl FnMut() -> Duration) -> Stats {
    for _ in 0..WARM {
        body();
    }
    let mut times: Vec<Duration> = (0..RUNS).map(|_| body()).collect();
    summary(&mut times)
}

pub fn run() {
    for (label, hints) in [
        ("MemoryUsage", MemoryHints::MemoryUsage),
        ("Performance", MemoryHints::Performance),
    ] {
        let Some(backend) = device_with(|options| Options { hints, ..options }) else {
            println!("alloc 16 MiB, hint {label}: skipped, no device");
            continue;
        };
        let new = sample(|| {
            let start = Instant::now();
            let storage = Storage::new(&backend, SIZE).expect("a buffer");
            let took = start.elapsed();
            drop(storage);
            took
        });
        let first_write = sample(|| {
            let bytes = vec![1u8; SIZE as usize];
            let start = Instant::now();
            let storage = Storage::new(&backend, SIZE).expect("a buffer");
            storage.write(0, &bytes).expect("in range");
            let took = start.elapsed();
            drop(storage);
            took
        });
        println!("Storage::new 16 MiB, hint {label}: {}", line_ms(new));
        println!(
            "Storage::new then a first write 16 MiB, hint {label}: {}",
            line_ms(first_write)
        );
        if label == "MemoryUsage" {
            let uninit = sample(|| {
                let start = Instant::now();
                let storage = Storage::new_uninit(&backend, SIZE).expect("a buffer");
                let took = start.elapsed();
                drop(storage);
                took
            });
            println!("Storage::new_uninit 16 MiB: {}", line_ms(uninit));
            // The device's first use of a buffer nobody has written: a 4-byte copy out of it, as
            // the reference's `first_use` times it. The device clears the buffer on that first use.
            let copy =
                daedalus_wgpu_conformance::Op::new(&backend, &daedalus_wgpu_conformance::COPY);
            let sink = Storage::new(&backend, 4).expect("a buffer");
            let first_use = sample(|| {
                let storage = Storage::new(&backend, SIZE).expect("a buffer");
                let start = Instant::now();
                copy.run(&storage, &sink).wait().expect("finishes");
                let took = start.elapsed();
                drop(storage);
                took
            });
            println!(
                "16 MiB first use of an unwritten buffer (device clear): {}",
                line_ms(first_use)
            );
        }
    }
}
